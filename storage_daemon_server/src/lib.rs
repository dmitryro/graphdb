// storage_daemon_server/src/lib.rs
// Fixed: 2025-08-10 - Corrected StorageConfig initialization to match expected fields and types
// Fixed: 2025-08-10 - Converted Map<String, Value> to HashMap<String, Value> for engine_specific_config
// Fixed: 2025-08-10 - Removed unnecessary Some wrapper for data_directory (E0308)
// Fixed: 2025-08-10 - Added missing StorageConfig fields (E0063)
// Fixed: 2025-08-10 - Converted log_directory to String (E0308)
// Fixed: 2025-08-10 - Removed invalid max_disk_space_mb field (E0560)

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use tokio::sync::oneshot;
use tokio::sync::mpsc::Sender;
use tokio::signal;

use lib::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngineType, create_storage};
use std::sync::{Arc, Mutex};
use openraft::{Config as RaftConfig, Raft, BasicNode};
use openraft::network::{RaftNetwork, RaftNetworkFactory, RPCOption};
use openraft::storage::{RaftLogReader, RaftSnapshotBuilder, RaftStorage, LogState, Adaptor};
use openraft::error::{RaftError, RPCError, ClientWriteError, InstallSnapshotError};
use openraft::{Entry, LogId, RaftTypeConfig, SnapshotPolicy, Vote, TokioRuntime, StoredMembership};
use openraft::raft::{ClientWriteResponse, responder::Responder};
use openraft::{ErrorSubject, ErrorVerb};
use openraft::storage::SnapshotMeta;
use std::str::FromStr;
use serde_yaml2 as serde_yaml;
use std::io::Cursor;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use log::{info, error, warn, debug};
use simplelog::{CombinedLogger, TermLogger, WriteLogger, LevelFilter, Config, ConfigBuilder, TerminalMode, ColorChoice};
use serde_json::{Value, Map};

// Declare the storage_client module.
pub mod storage_client;

// Re-export the necessary items from the storage_client module.
pub use storage_client::StorageClient;

// Define a custom TypeConfig for RaftTypeConfig
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypeConfig;

impl RaftTypeConfig for TypeConfig {
    type D = StorageRequest;
    type R = StorageResponse;
    type NodeId = u64;
    type Node = BasicNode;
    type Entry = Entry<Self>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = StorageResponse;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageRequest {
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Set { key: String, value: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageResponse {
    pub success: bool,
    pub message: String,
    pub data: Option<String>,
}

impl Responder<TypeConfig> for StorageResponse {
    type Receiver = Sender<Result<ClientWriteResponse<TypeConfig>, ClientWriteError<u64, BasicNode>>>;
    
    fn from_app_data(data: StorageRequest) -> (StorageRequest, Self, Self::Receiver) {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let response = StorageResponse {
            success: true,
            message: "Request received".to_string(),
            data: None,
        };
        (data, response, tx)
    }

    fn send(self, _result: Result<ClientWriteResponse<TypeConfig>, ClientWriteError<u64, BasicNode>>) {
        // Placeholder: Real implementation should send result to client
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSpecificConfig {
    pub tiering: Option<TieringConfig>,
    pub secondary_backend: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    pub hot_ttl_hours: u32,
    pub warm_ttl_days: u32,
    pub cold_backend: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettings {
    pub config_root_directory: PathBuf,
    pub data_directory: PathBuf,
    pub log_directory: PathBuf,
    #[serde(default = "StorageSettings::default_default_port")]
    pub default_port: u16,
    pub cluster_range: String,
    pub max_disk_space_gb: u64,
    pub min_disk_space_gb: u64,
    pub use_raft_for_scale: bool,
    pub storage_engine_type: String,
    // Corrected: Use a HashMap to handle arbitrary key-value pairs from the YAML file.
    #[serde(default)]
    pub engine_specific_config: HashMap<String, serde_json::Value>,
    pub max_open_files: u64,
}

impl Default for StorageSettings {
    fn default() -> Self {
        StorageSettings {
            config_root_directory: PathBuf::from("./storage_daemon_server"),
            data_directory: PathBuf::from("/opt/graphdb/storage_data"), // Corrected to match your YAML
            log_directory: PathBuf::from("/opt/graphdb/logs"),
            default_port: 8083,
            cluster_range: "8083".to_string(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".to_string(),
            engine_specific_config: HashMap::new(), // Initialize with an empty HashMap
            max_open_files: 100,
        }
    }
}

impl StorageSettings {
    fn default_default_port() -> u16 {
        8083
    }

    pub fn load_from_yaml(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file from {:?}", path))?;
        let wrapper: StorageSettingsWrapper = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", path))?;
        Ok(wrapper.storage)
    }
}

// The `StorageSettingsWrapper` struct can remain as is.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSettingsWrapper {
    pub storage: StorageSettings,
}

pub struct StorageDaemon {
    pub storage: Arc<dyn GraphStorageEngine>,
    pub shutdown_tx: oneshot::Sender<()>,
}

impl StorageDaemon {
    pub fn get_storage(&self) -> Arc<dyn GraphStorageEngine> {
        self.storage.clone()
    }
}

// Mock network implementation
#[derive(Clone)]
pub struct MockRaftNetwork;

impl RaftNetwork<TypeConfig> for MockRaftNetwork {
    async fn append_entries(
        &mut self,
        _rpc: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Ok(openraft::raft::AppendEntriesResponse::Success)
    }

    async fn install_snapshot(
        &mut self,
        _rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::InstallSnapshotResponse<u64>, RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>> {
        Ok(openraft::raft::InstallSnapshotResponse {
            vote: Vote::new(0, 0),
        })
    }

    async fn vote(
        &mut self,
        _rpc: openraft::raft::VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Ok(openraft::raft::VoteResponse {
            vote: Vote::new(0, 0),
            vote_granted: true,
            last_log_id: None,
        })
    }
}

#[derive(Clone)]
pub struct MockRaftNetworkFactory;

impl RaftNetworkFactory<TypeConfig> for MockRaftNetworkFactory {
    type Network = MockRaftNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        MockRaftNetwork
    }
}

// State machine and storage implementation
#[derive(Debug, Default, Clone)]
pub struct StoreData {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    pub kvs: BTreeMap<String, String>,
}

impl Serialize for StoreData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("StoreData", 3)?;
        state.serialize_field("last_applied_log", &self.last_applied_log)?;
        state.serialize_field("last_membership", &self.last_membership)?;
        state.serialize_field("kvs", &self.kvs)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for StoreData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct StoreDataRaw {
            last_applied_log: Option<LogId<u64>>,
            last_membership: StoredMembership<u64, BasicNode>,
            kvs: BTreeMap<String, String>,
        }
        let raw = StoreDataRaw::deserialize(deserializer)?;
        Ok(StoreData {
            last_applied_log: raw.last_applied_log,
            last_membership: raw.last_membership,
            kvs: raw.kvs,
        })
    }
}

#[derive(Clone)]
pub struct InMemoryRaftStorage {
    storage: Arc<dyn GraphStorageEngine>,
    log_entries: Arc<Mutex<Vec<Entry<TypeConfig>>>>,
    vote: Arc<Mutex<Option<Vote<u64>>>>,
    state_machine: Arc<Mutex<StoreData>>,
}

impl InMemoryRaftStorage {
    pub fn new(storage: Arc<dyn GraphStorageEngine>) -> Self {
        let membership = StoredMembership::new(
            None,
            openraft::Membership::new(vec![BTreeSet::from([1])], BTreeMap::new()),
        );
        InMemoryRaftStorage {
            storage,
            log_entries: Arc::new(Mutex::new(Vec::new())),
            vote: Arc::new(Mutex::new(None)),
            state_machine: Arc::new(Mutex::new(StoreData {
                last_applied_log: None,
                last_membership: membership,
                kvs: BTreeMap::new(),
            })),
        }
    }
}

impl RaftLogReader<TypeConfig> for InMemoryRaftStorage {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, openraft::StorageError<u64>>
    where
        RB: std::ops::RangeBounds<u64> + Clone + Send,
    {
        let log_entries = self.log_entries.lock().unwrap();
        let start = match range.start_bound() {
            std::ops::Bound::Included(&x) => x as usize,
            std::ops::Bound::Excluded(&x) => x as usize + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&x) => x as usize + 1,
            std::ops::Bound::Excluded(&x) => x as usize,
            std::ops::Bound::Unbounded => log_entries.len(),
        };
        let entries = log_entries.get(start..end).map(|x| x.to_vec()).unwrap_or_default();
        Ok(entries)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for InMemoryRaftStorage {
    async fn build_snapshot(
        &mut self
    ) -> Result<openraft::storage::Snapshot<TypeConfig>, openraft::StorageError<u64>> {
        let data = self.state_machine.lock().unwrap();
        let bytes = serde_json::to_vec(&*data).map_err(|e| {
            openraft::StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )
        })?;
        Ok(openraft::storage::Snapshot {
            meta: SnapshotMeta {
                last_log_id: data.last_applied_log,
                last_membership: data.last_membership.clone(),
                snapshot_id: "default".to_string(),
            },
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

impl RaftStorage<TypeConfig> for InMemoryRaftStorage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), openraft::StorageError<u64>> {
        *self.vote.lock().unwrap() = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, openraft::StorageError<u64>> {
        Ok(self.vote.lock().unwrap().clone())
    }

    async fn save_committed(&mut self, _committed: Option<LogId<u64>>) -> Result<(), openraft::StorageError<u64>> {
        // Note: This is a no-op; real implementation should persist
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, openraft::StorageError<u64>> {
        Ok(self.state_machine.lock().unwrap().last_applied_log)
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut log_entries = self.log_entries.lock().unwrap();
        log_entries.extend(entries);
        Ok(())
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> Result<(), openraft::StorageError<u64>> {
        let mut log_entries = self.log_entries.lock().unwrap();
        log_entries.retain(|e| e.log_id.index < log_id.index);
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), openraft::StorageError<u64>> {
        let mut log_entries = self.log_entries.lock().unwrap();
        log_entries.retain(|e| e.log_id.index >= log_id.index);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), openraft::StorageError<u64>> {
        let data = self.state_machine.lock().unwrap();
        Ok((data.last_applied_log, data.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<StorageResponse>, openraft::StorageError<u64>> {
        let mut data = self.state_machine.lock().unwrap();
        let mut results = Vec::with_capacity(entries.len());

        for entry in entries {
            let response = match &entry.payload {
                openraft::EntryPayload::Normal(cmd) => match &cmd.command {
                    Command::Set { key, value } => {
                        data.kvs.insert(key.clone(), value.clone());
                        StorageResponse {
                            success: true,
                            message: format!("Set {} = {}", key, value),
                            data: Some(format!("Set {} = {}", key, value)),
                        }
                    }
                },
                openraft::EntryPayload::Membership(membership) => {
                    data.last_membership = StoredMembership::new(Some(entry.log_id), membership.clone());
                    StorageResponse {
                        success: true,
                        message: "Updated membership".to_string(),
                        data: None,
                    }
                }
                openraft::EntryPayload::Blank => {
                    StorageResponse {
                        success: true,
                        message: "Blank entry".to_string(),
                        data: None,
                    }
                }
            };
            data.last_applied_log = Some(entry.log_id);
            results.push(response);
        }
        Ok(results)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, openraft::StorageError<u64>> {
        let log_entries = self.log_entries.lock().unwrap();
        let last_log_id = log_entries.last().map(|e| e.log_id);
        let last_purged_log_id = log_entries.first().map(|e| e.log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, openraft::StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<u64>> {
        let new_data: StoreData = serde_json::from_slice(snapshot.get_ref()).map_err(|e| {
            openraft::StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )
        })?;
        let mut data = self.state_machine.lock().unwrap();
        data.last_applied_log = meta.last_log_id;
        data.last_membership = meta.last_membership.clone();
        data.kvs = new_data.kvs;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<openraft::storage::Snapshot<TypeConfig>>, openraft::StorageError<u64>> {
        let data = self.state_machine.lock().unwrap();
        Ok(data.last_applied_log.map(|last_applied| {
            let meta = SnapshotMeta {
                last_log_id: Some(last_applied),
                last_membership: data.last_membership.clone(),
                snapshot_id: "default".to_string(),
            };
            let data_bytes = serde_json::to_vec(&*data).unwrap();
            openraft::storage::Snapshot {
                meta,
                snapshot: Box::new(Cursor::new(data_bytes)),
            }
        }))
    }
}

// --- Corrected `start_storage_daemon_server_real` function ---
pub async fn start_storage_daemon_server_real(
    port: u16,
    settings: StorageSettings,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<StorageDaemon> {
    // Initialize logger
    let log_file_path = format!("/tmp/graphdb-storage-{}.out", port);
    let log_file = File::create(&log_file_path)
        .with_context(|| format!("Failed to create log file at {}", log_file_path))?;
    let log_config = ConfigBuilder::new()
        .set_time_format_rfc3339()
        .set_thread_level(LevelFilter::Off)
        .build();
    CombinedLogger::init(vec![
        TermLogger::new(LevelFilter::Info, log_config.clone(), TerminalMode::Mixed, ColorChoice::Auto),
        WriteLogger::new(LevelFilter::Info, log_config, log_file),
    ]).with_context(|| "Failed to initialize logger")?;

    info!("[Storage Daemon] Starting real storage daemon server on port {}", port);
    info!("[Storage Daemon] Data directory: {:?}", settings.data_directory);
    info!("[Storage Daemon] Log directory: {:?}", settings.log_directory);
    info!("[Storage Daemon] Effective listening port: {}", port);
    info!("[Storage Daemon] Default port from config: {}", settings.default_port);
    info!("[Storage Daemon] Cluster range: {}", settings.cluster_range);
    info!("[Storage Daemon] Max disk space: {} GB", settings.max_disk_space_gb);
    info!("[Storage Daemon] Min disk space: {} GB", settings.min_disk_space_gb);
    info!("[Storage Daemon] Use Raft for scale: {}", settings.use_raft_for_scale);
    info!("[Storage Daemon] Storage engine type: {}", settings.storage_engine_type);
    info!("[Storage Daemon] Engine specific config: {:?}", settings.engine_specific_config);

    // Verify RocksDB path if engine is RocksDB
    if settings.storage_engine_type.to_lowercase() == "rocksdb" {
        if !settings.engine_specific_config.is_empty() {
            if let Some(path) = settings.engine_specific_config.get("path").and_then(|p| p.as_str()) {
                let rocksdb_path = PathBuf::from(path);
                if !rocksdb_path.exists() {
                    error!("[Storage Daemon] RocksDB path does not exist: {:?}", rocksdb_path);
                    return Err(anyhow!("RocksDB path does not exist: {:?}", rocksdb_path));
                }
                if !rocksdb_path.is_dir() {
                    error!("[Storage Daemon] RocksDB path is not a directory: {:?}", rocksdb_path);
                    return Err(anyhow!("RocksDB path is not a directory: {:?}", rocksdb_path));
                }
                debug!("[Storage Daemon] Verified RocksDB path: {:?}", rocksdb_path);
            } else {
                error!("[Storage Daemon] RocksDB path not specified in engine_specific_config: {:?}", settings.engine_specific_config);
                return Err(anyhow!("RocksDB path not specified in engine_specific_config"));
            }
        } else {
            error!("[Storage Daemon] No engine_specific_config for RocksDB");
            return Err(anyhow!("No engine_specific_config for RocksDB"));
        }
    }

    let cluster_range_str = settings.cluster_range.clone();

    let storage_config = StorageConfig {
        storage_engine_type: StorageEngineType::from_str(&settings.storage_engine_type)
            .map_err(|e| anyhow::anyhow!("Invalid storage engine type: {}", e))?,
        data_directory: settings.data_directory,
        config_root_directory: settings.config_root_directory,
        log_directory: settings.log_directory.display().to_string(),
        default_port: settings.default_port,
        cluster_range: settings.cluster_range,
        use_raft_for_scale: settings.use_raft_for_scale,
        max_disk_space_gb: 10,
        min_disk_space_gb: 1,
        engine_specific_config: Some(settings.engine_specific_config),
        max_open_files: Some(settings.max_open_files as i32),
        connection_string: match settings.storage_engine_type.as_str() {
            "redis" | "postgresql" | "mysql" => Some(format!("{}:{}", cluster_range_str, port)),
            _ => None,
        },
    };
    let storage = match create_storage(&storage_config) {
        Ok(storage) => {
            info!("[Storage Daemon] Initialized storage backend: {}", settings.storage_engine_type);
            storage
        },
        Err(e) => {
            error!("[Storage Daemon] Failed to initialize storage backend {}: {}", settings.storage_engine_type, e);
            return Err(anyhow!("Failed to create storage engine {}: {}", settings.storage_engine_type, e));
        },
    };

    // Initialize Raft if enabled
    if settings.use_raft_for_scale {
        let raft_config = RaftConfig {
            cluster_name: "graphdb-cluster".to_string(),
            heartbeat_interval: 250,
            election_timeout_min: 1000,
            election_timeout_max: 2000,
            install_snapshot_timeout: 3000,
            snapshot_policy: SnapshotPolicy::Never,
            max_payload_entries: 1000,
            enable_elect: true,
            enable_heartbeat: true,
            enable_tick: true,
            max_in_snapshot_log_to_keep: 1000,
            snapshot_max_chunk_size: 1000000,
            purge_batch_size: 100,
            ..Default::default()
        };
        let node_id = 1;
        let _node = BasicNode { addr: format!("0.0.0.0:{}", port) };
        let network = MockRaftNetworkFactory;
        let raft_storage = InMemoryRaftStorage::new(storage.clone());
        let (log_store, state_machine) = Adaptor::new(raft_storage);
        let _raft = Raft::new(node_id, Arc::new(raft_config.validate()?), network, log_store, state_machine).await?;
        info!("[Storage Daemon] Initialized Raft cluster");
    }

    // Bind to the port
    let _listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await
        .with_context(|| format!("Failed to bind storage daemon to port {}", port))?;
    info!("[Storage Daemon] Successfully bound to port {}. Now listening for connections.", port);

    // Placeholder for serving handlers
    tokio::spawn(async move {
        // Integrate with handlers.rs, e.g., handlers::serve(storage.clone(), listener).await;
    });

    // Set up shutdown handling
    let (shutdown_tx, _shutdown_rx_task) = oneshot::channel();
    tokio::spawn(async move {
        shutdown_rx.await.ok();
        info!("[Storage Daemon] Shutdown signal received. Exiting.");
    });

    Ok(StorageDaemon { storage, shutdown_tx })
}

pub fn get_default_storage_port_from_config() -> u16 {
    let config_path = PathBuf::from("storage_daemon_server/storage_config.yaml");
    match StorageSettings::load_from_yaml(&config_path) {
        Ok(settings) => {
            info!("[Storage Daemon Config] Loaded default port {} from {:?}", settings.default_port, config_path);
            settings.default_port
        },
        Err(e) => {
            error!("[Storage Daemon Config] Could not load or parse storage_config.yaml for default port: {}. Using application's hardcoded default {}.", e, StorageSettings::default().default_port);
            StorageSettings::default().default_port
        }
    }
}

pub async fn run_storage_daemon(
    cli_port_override: Option<u16>,
    config_file_path: PathBuf,
) -> Result<()> {
    let settings = match StorageSettings::load_from_yaml(&config_file_path) {
        Ok(s) => {
            info!("[Storage Daemon] Loaded storage settings from {}.", config_file_path.display());
            s
        },
        Err(e) => {
            error!("[Storage Daemon] Error loading configuration from {}: {}. Falling back to default settings.", config_file_path.display(), e);
            StorageSettings::default()
        }
    };

    let effective_port = if let Some(p) = cli_port_override {
        info!("[Storage Daemon] Using port from CLI argument: {}", p);
        p
    } else {
        info!("[Storage Daemon] Using default port from config/default: {}", settings.default_port);
        settings.default_port
    };

    if effective_port < 1024 || effective_port > 65535 {
        return Err(anyhow::anyhow!(
            "Invalid effective port: {}. Port must be between 1024 and 65535.",
            effective_port
        ));
    }

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("[Storage Daemon] Ctrl-C received. Initiating graceful shutdown...");
        let _ = tx.send(());
    });

    start_storage_daemon_server_real(effective_port, settings, rx).await?;
    Ok(())
}

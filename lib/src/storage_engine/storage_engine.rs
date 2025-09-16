use std::any::Any;
use std::pin::Pin;
use std::collections::{HashMap, BTreeSet};
use async_trait::async_trait;
use models::errors::{GraphError, GraphResult, ValidationError};
use uuid::Uuid;
use models::{Edge, Identifier, Vertex};
use tokio::sync::{OnceCell, RwLock, Mutex as TokioMutex};
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use surrealdb::engine::local::{Db, Mem, RocksDb};
use tikv_client::{Config, Transaction, TransactionClient as TiKvClient, RawClient as KvClient};
use surrealdb::Surreal;
use sled::Db as SledDB;
use surrealdb::engine::any::Any as SurrealAny;
use surrealdb::sql::Thing;
use tokio::fs;
use tokio::net::TcpStream;
use tokio::time::{self, sleep, timeout, Duration as TokioDuration};
use reqwest::Client;
use std::process;
use anyhow::{Result, Context, anyhow};
use std::io::Error;
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value, from_value};
use serde::{Deserialize, Serialize, Deserializer};
use log::{info, debug, warn, error, trace};
#[cfg(unix)]
use nix::unistd::{Pid, getpid, getuid};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, RefreshKind};
use nix::sys::signal::{self, kill, Signal};
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, MetadataExt};
use std::net::SocketAddr; // Added for TcpStream::connect
use openraft::{
    Raft, Config as RaftConfig, error::RaftError, NodeId,
    raft::{AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse},
    network::{RaftNetworkFactory, RaftNetwork, RPCOption},
    error::{InstallSnapshotError, CheckIsLeaderError, ForwardToLeader},
    storage::Adaptor, LogId, LeaderId, StoredMembership, RaftTypeConfig, storage::RaftStateMachine, storage::RaftSnapshotBuilder,
    OptionalSend, OptionalSync,
};
use futures::future::Future; 
use futures::TryFutureExt;
// Try to import RPCError from different possible locations
use openraft::error::RPCError;
use openraft_memstore::MemStore;    // from the openraft-memstore crate
use openraft_memstore::TypeConfig as RaftMemStoreTypeConfig;
// If the above doesn't work, try:
// use openraft::network::RPCError;
// Or if it's named differently:
// use openraft::error::NetworkError as RPCError;
use crate::daemon::daemon_config::DAEMON_REGISTRY_DB_PATH; // Corrected import
use crate::config::{DEFAULT_DATA_DIRECTORY, DEFAULT_LOG_DIRECTORY, LOCK_FILE_PATH,
                                 DEFAULT_STORAGE_PORT, StorageConfig, SledConfig, RocksdbConfig, TikvConfig,
                                 RedisConfig, MySQLConfig, PostgreSQLConfig, TypeConfig, QueryPlan, QueryResult,
                                 StorageConfigInner, SelectedStorageConfig, StorageConfigWrapper, 
                                 load_storage_config_from_yaml,
                                 create_default_storage_yaml_config,
                                 load_engine_specific_config};
use crate::daemon::daemon_utils::{find_pid_by_port, stop_process, parse_cluster_range};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY,  NonBlockingDaemonRegistry, DaemonMetadata};
use crate::storage_engine::inmemory_storage::{InMemoryStorage};
use crate::storage_engine::storage_utils::{cleanup_legacy_sled_paths, copy_dir};
use crate::storage_engine::raft_storage::{RaftStorage};
pub use crate::config::{ StorageEngineType };

pub use crate::storage_engine::inmemory_storage::InMemoryStorage as InMemoryGraphStorage;
#[cfg(feature = "with-sled")]
pub use crate::storage_engine::sled_storage::{SledStorage, SLED_DB};
#[cfg(feature = "with-rocksdb")]
pub use crate::storage_engine::rocksdb_storage::RocksdbStorage;
#[cfg(feature = "with-tikv")]
pub use crate::storage_engine::tikv_storage::TikvStorage;
#[cfg(feature = "redis-datastore")]
use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;
#[cfg(any(feature = "sled", feature = "rocksdb", feature = "tikv"))]
use crate::storage_engine::hybrid_storage::HybridStorage;

pub static CLEANUP_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
pub static GLOBAL_STORAGE_ENGINE_MANAGER: OnceCell<Arc<AsyncStorageEngineManager>> = OnceCell::const_new();

#[cfg(feature = "with-sled")]
static SLED_SINGLETON: TokioMutex<Option<Arc<SledStorage>>> = TokioMutex::const_new(None);
#[cfg(feature = "with-rocksdb")]
static ROCKSDB_SINGLETON: TokioMutex<Option<Arc<RocksdbStorage>>> = TokioMutex::const_new(None);
#[cfg(feature = "with-tikv")]
static TIKV_SINGLETON: TokioMutex<Option<Arc<TikvStorage>>> = TokioMutex::const_new(None);
#[cfg(feature = "redis-datastore")]
static REDIS_SINGLETON: OnceCell<Arc<RedisStorage>> = OnceCell::const_new();
#[cfg(feature = "postgres-datastore")]
static POSTGRES_SINGLETON: OnceCell<Arc<PostgresStorage>> = OnceCell::const_new();
#[cfg(feature = "mysql-datastore")]
static MYSQL_SINGLETON: OnceCell<Arc<MySQLStorage>> = OnceCell::const_new();

// The NodeId must be `u64` as per the `openraft` crate's definition.
type NodeIdType = u64;

// Define Raft application's request and response types.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppRequest {
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppResponse {
    pub message: String,
}

impl RaftTypeConfig for TypeConfig {
    type D = AppRequest;
    type R = AppResponse;
    type NodeId = NodeIdType;
    type Node = NodeIdType; // Simplified to use NodeIdType directly
    type Entry = openraft::Entry<Self>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::raft::responder::OneshotResponder<Self>;
}


const MAX_RETRIES: u32 = 5;
const RETRY_DELAY_MS: u64 = 500;


#[derive(Debug, Default)]
pub struct ApplicationStateMachine {
    last_applied: Option<LogId<NodeIdType>>,
    data: HashMap<String, String>,
}

#[derive(Clone, Debug, Default)]
pub struct ExampleNetwork;

// Helper method to get string values from StorageConfig, similar to get_config_port
fn get_config_value(config: &StorageConfig, key: &str, default: &str) -> String {
    config.engine_specific_config
        .as_ref()
        .and_then(|selected_config| match key {
            "host" => selected_config.storage.host.clone(),
            _ => None,
        })
        .unwrap_or_else(|| {
            warn!("No {} specified in config, using default: {}", key, default);
            default.to_string()
        })
}

impl RaftNetwork<RaftMemStoreTypeConfig> for ExampleNetwork {
    fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RaftMemStoreTypeConfig>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<
            AppendEntriesResponse<<RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId>,
            RPCError<
                <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId,
                <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::Node,
                RaftError<<RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId>,
            >,
        >,
    > + Send {
        async move {
            let _ = rpc;
            todo!("Implement append_entries")
        }
    }

    fn vote(
        &mut self,
        rpc: VoteRequest<<RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<
            VoteResponse<<RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId>,
            RPCError<
                <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId,
                <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::Node,
                RaftError<<RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId>,
            >,
        >,
    > + Send {
        async move {
            let _ = rpc;
            todo!("Implement vote")
        }
    }

    fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<RaftMemStoreTypeConfig>,
        _option: RPCOption,
    ) -> impl Future<
        Output = Result<
            InstallSnapshotResponse<<RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId>,
            RPCError<
                <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId,
                <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::Node,
                RaftError<
                    <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId,
                    InstallSnapshotError,
                >,
            >,
        >,
    > + Send {
        async move {
            let _ = rpc;
            todo!("Implement install_snapshot")
        }
    }
}

impl RaftNetworkFactory<RaftMemStoreTypeConfig> for ExampleNetwork {
    type Network = ExampleNetwork;

    fn new_client(
        &mut self,
        _target: <RaftMemStoreTypeConfig as openraft::RaftTypeConfig>::NodeId,
        _node: &(),
    ) -> impl Future<Output = Self::Network> + Send {
        async move { ExampleNetwork::default() }
    }
}

// New struct to wrap the surrealdb client and implement GraphStorageEngine and StorageEngine
#[derive(Debug, Clone)]
pub struct SurrealdbGraphStorage {
    pub db: Surreal<Db>,
    pub backend_type: StorageEngineType,
}


#[cfg(feature = "with-rocksdb")]
impl SurrealdbGraphStorage {
    pub async fn force_unlock(path: &PathBuf) -> Result<(), GraphError> {
        let lock_file = path.join("LOCK");
        if lock_file.exists() {
            info!("Removing RocksDB lock file at {:?}", lock_file);
            tokio::fs::remove_file(&lock_file).await
                .map_err(|e| GraphError::StorageError(format!("Failed to remove RocksDB lock file at {:?}: {}", lock_file, e)))?;
        }
        // Verify the database can be opened to ensure no other locks
        match Surreal::new::<RocksDb>(path.clone()).await {
            Ok(_) => {
                info!("RocksDB database at {:?} is accessible after lock removal", path);
                Ok(())
            }
            Err(e) => Err(GraphError::StorageError(format!("RocksDB database at {:?} still locked or inaccessible: {}", path, e)))
        }
    }
}

/// A simple struct to represent the key-value data we're storing.
/// This helps SurrealDB serialize and deserialize the data correctly.
#[derive(Debug, Serialize, Deserialize)]
struct StoredValue {
    value: String,
}

#[async_trait]
impl StorageEngine for SurrealdbGraphStorage {
    /// Connects to the SurrealDB server. The client manages connections internally.
    async fn connect(&self) -> Result<(), GraphError> {
        Ok(())
    }

    /// Inserts a key-value pair into the SurrealDB `storage` table.
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        let key_str = String::from_utf8(key)
            .map_err(|e| GraphError::StorageError(format!("Invalid key for generic insert: {}", e)))?;
        let value_str = String::from_utf8(value)
            .map_err(|e| GraphError::StorageError(format!("Invalid value for generic insert: {}", e)))?;
        
        // Create an instance of our StoredValue struct to pass as content.
        let data_to_store = StoredValue { value: value_str };

        // The create method for a specific ID returns an Option, not a Vec.
        // We handle the potential error from the async call first, then check the Option.
        let created: Option<StoredValue> = self.db.create(("storage", key_str))
            .content(data_to_store)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        // Ensure that a record was actually created.
        if created.is_none() {
            return Err(GraphError::StorageError("Failed to create record".to_string()));
        }

        Ok(())
    }

    /// Retrieves a value for a given key from the SurrealDB `storage` table.
    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        let key_str = String::from_utf8(key.to_vec())
            .map_err(|e| GraphError::StorageError(format!("Invalid key for generic retrieve: {}", e)))?;

        // The select method for a specific ID returns an Option, not a Vec.
        let result: Option<StoredValue> = self.db.select(("storage", key_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        // Return the value if a record was found.
        Ok(result.map(|sv| sv.value.into_bytes()))
    }

    /// Deletes a record from the SurrealDB `storage` table.
    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        let key_str = String::from_utf8(key.to_vec())
            .map_err(|e| GraphError::StorageError(format!("Invalid key for generic delete: {}", e)))?;
            
        // Explicitly annotate the type for the delete method.
        let deleted: Option<StoredValue> = self.db.delete(("storage", key_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    /// SurrealDB handles flushing internally.
    async fn flush(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SurrealdbGraphStorage {
    async fn start(&self) -> Result<(), GraphError> {
        info!("SurrealDB store started.");
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        info!("SurrealDB store stopped.");
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        match self.backend_type {
            StorageEngineType::Sled => "sled",
            StorageEngineType::RocksDB => "rocksdb",
            StorageEngineType::TiKV => "tikv",
            StorageEngineType::InMemory => "in-memory",
            _ => "unknown",
        }
    }

    async fn is_running(&self) -> bool {
        true
    }

    /// Executes a raw SurrealQL query and returns a JSON value.
    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        debug!("Executing query: {}", query_string);
        let mut result = self.db.query(query_string).await
            .map_err(|e| GraphError::QueryError(e.to_string()))?;
        
        // The `take` method with an index returns a Result<Vec<Value>, _>
        // for that specific query statement.
        let values: Vec<Value> = result.take(0)
            .map_err(|e| GraphError::QueryError(e.to_string()))?;
        
        // We get the first value from the returned vector.
        let value = values.into_iter().next()
            .ok_or_else(|| GraphError::QueryError("Query returned no values".to_string()))?;
        
        Ok(value)
    }


    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on Surreal (returning null as not implemented)");
        Ok(QueryResult::Null)
    }       

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let created: Option<Vertex> = self.db.create(("vertices", vertex.id.0.to_string()))
            .content(vertex)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if created.is_none() {
            return Err(GraphError::StorageError("Failed to create vertex".to_string()));
        }
        
        trace!("Created vertex: {:?}", created);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let result: Option<Vertex> = self.db.select(("vertices", id.to_string()))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        Ok(result)
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let updated: Option<Vertex> = self.db.update(("vertices", vertex.id.0.to_string()))
            .content(vertex)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if updated.is_none() {
            return Err(GraphError::StorageError("Failed to update vertex".to_string()));
        }

        trace!("Updated vertex: {:?}", updated);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let deleted: Option<Vertex> = self.db.delete(("vertices", id.to_string()))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        trace!("Deleted vertex: {:?}", deleted);
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let results: Vec<Vertex> = self.db.select("vertices")
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(results)
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let edge_id_str = format!("{}:{}:{}", edge.outbound_id.0, edge.t, edge.inbound_id.0);
        
        let created: Option<Edge> = self.db.create(("edges", edge_id_str))
            .content(edge)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if created.is_none() {
            return Err(GraphError::StorageError("Failed to create edge".to_string()));
        }

        trace!("Created edge: {:?}", created);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        let edge_id_str = format!("{}:{}:{}", outbound_id, edge_type, inbound_id);

        let result: Option<Edge> = self.db.select(("edges", &edge_id_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        Ok(result)
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let edge_id_str = format!("{}:{}:{}", edge.outbound_id.0, edge.t, edge.inbound_id.0);

        let updated: Option<Edge> = self.db.update(("edges", edge_id_str))
            .content(edge)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if updated.is_none() {
            return Err(GraphError::StorageError("Failed to update edge".to_string()));
        }

        trace!("Updated edge: {:?}", updated);
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let edge_id_str = format!("{}:{}:{}", outbound_id, edge_type, inbound_id);
        
        let deleted: Option<Edge> = self.db.delete(("edges", &edge_id_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        trace!("Deleted edge: {:?}", deleted);
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let results: Vec<Edge> = self.db.select("edges")
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        Ok(results)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.db.query("REMOVE TABLE vertices").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.query("REMOVE TABLE edges").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

// Wrapper for StorageEngineManager to provide async compatibility
#[derive(Debug)]
pub struct AsyncStorageEngineManager {
    manager: Arc<TokioMutex<StorageEngineManager>>,
}

impl AsyncStorageEngineManager {
    pub fn from_manager(manager: StorageEngineManager) -> Self {
        AsyncStorageEngineManager {
            manager: Arc::new(TokioMutex::new(manager)),
        }
    }

    pub fn get_manager(&self) -> Arc<TokioMutex<StorageEngineManager>> {
        Arc::clone(&self.manager)
    }

    pub async fn get_persistent_engine(&self) -> Arc<dyn GraphStorageEngine + Send + Sync> {
        let manager = self.manager.lock().await;
        manager.get_persistent_engine()
    }

    pub async fn use_storage(&self, config: StorageConfig, permanent: bool) -> Result<(), GraphError> {
        let mut manager = self.manager.lock().await;
        manager.use_storage(config, permanent).await
    }

    pub async fn current_engine_type(&self) -> StorageEngineType {
        let manager = self.manager.lock().await;
        manager.current_engine_type().await
    }

    pub async fn get_current_engine_data_path(&self) -> Option<PathBuf> {
        let manager = self.manager.lock().await;
        manager.get_current_engine_data_path().await
    }
}

/// Helper function to check if a lock file exists
#[cfg(feature = "with-sled")]
pub async fn lock_file_exists(lock_path: PathBuf) -> Result<bool, GraphError> {
    let exists = fs::metadata(&lock_path).await.is_ok();
    debug!("lock_file_exists({:?}) -> {:?}", lock_path, exists);
    Ok(exists)
}

/// Initializes the global StorageEngineManager
pub async fn init_storage_engine_manager(config_path_yaml: PathBuf) -> Result<(), GraphError> {
    info!("Initializing StorageEngineManager with YAML: {:?}", config_path_yaml);
    
    if let Some(parent) = config_path_yaml.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create directory for YAML config: {:?}", parent))?;
    }
    
    // Check if already initialized
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_some() {
        info!("StorageEngineManager already initialized, reusing existing instance");
        return Ok(());
    }
    
    // Load configuration from YAML to get storage_engine_type and port
    info!("Loading config from {:?}", config_path_yaml);
    let mut config = load_storage_config_from_yaml(Some(config_path_yaml.clone())).await
        .map_err(|e| {
            error!("Failed to load YAML config from {:?}: {}", config_path_yaml, e);
            GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
        })?;
    
    let storage_engine = config.storage_engine_type.clone();
    let port = config.engine_specific_config
        .as_ref()
        .and_then(|c| c.storage.port)
        .unwrap_or_else(|| match storage_engine {
            StorageEngineType::TiKV => 2380,
            _ => 8052, // Default for Sled and others
        });
    
    // Normalize Sled path to remove port suffixes - use clean engine name
    if storage_engine == StorageEngineType::Sled {
        if let Some(ref mut engine_config) = config.engine_specific_config {
            let base_data_dir = config.data_directory
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
            
            // Use clean path without port suffix
            let normalized_path = base_data_dir.join("sled");
            
            // Check if path needs updating (remove port suffixes)
            let needs_update = engine_config.storage.path.as_ref()
                .map(|current_path| current_path != &normalized_path)
                .unwrap_or(true);
                
            if needs_update {
                info!("Normalizing Sled path from {:?} to {:?}", 
                      engine_config.storage.path, normalized_path);
                
                // Clean up any old port-suffixed directories
                cleanup_legacy_sled_paths(&base_data_dir, port).await;
                
                engine_config.storage.path = Some(normalized_path);
                engine_config.storage.port = Some(port);
                
                config.save().await
                    .map_err(|e| GraphError::ConfigurationError(
                        format!("Failed to save updated StorageConfig with normalized Sled path: {}", e)
                    ))?;
                    
                info!("Updated and saved config with normalized Sled path");
            }
        }
    }
    
    debug!("Loaded storage_engine_type: {:?}, port: {:?}", storage_engine, port);
    // Initialize StorageEngineManager with the loaded storage_engine_type and port
    let manager = StorageEngineManager::new(storage_engine, &config_path_yaml, false, Some(port)).await
        .map_err(|e| {
            error!("Failed to create StorageEngineManager: {}", e);
            GraphError::StorageError(format!("Failed to create StorageEngineManager: {}", e))
        })?;

    GLOBAL_STORAGE_ENGINE_MANAGER
        .set(Arc::new(AsyncStorageEngineManager::from_manager(manager)))
        .map_err(|_| GraphError::StorageError("Failed to set StorageEngineManager: already initialized".to_string()))?;

    info!("StorageEngineManager initialized successfully with engine: {:?} on port {:?}", storage_engine, port);
    Ok(())
}


#[cfg(feature = "with-sled")]
pub async fn log_lock_file_diagnostics(lock_path: PathBuf) -> Result<(), GraphError> {
    debug!("Running log_lock_file_diagnostics for {:?}", lock_path);
    match fs::metadata(&lock_path).await {
        Ok(metadata) => {
            debug!("Lock file diagnostics for {:?}:", lock_path);
            debug!("  Size: {} bytes", metadata.len());
            debug!("  Modified: {:?}", metadata.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH));
            debug!("  Read-only: {}", metadata.permissions().readonly());
            Ok(())
        }
        Err(e) => {
            warn!("Failed to get lock file metadata for {:?}: {}", lock_path, e);
            Err(GraphError::Io(e))
        }
    }
}

#[cfg(feature = "with-sled")]
async fn handle_sled_retry_error(sled_lock_path: &PathBuf, _sled_path: &PathBuf, attempt: u32) {
    warn!(
        "Sled lock contention (attempt {}/5) — another process may hold the lock",
        attempt + 1
    );
    if sled_lock_path.exists() {
        warn!("Lock file exists at {:?}", sled_lock_path);
        if let Err(e) = std::fs::remove_file(&sled_lock_path) {
            error!("Failed to remove lock file at {:?}: {}", sled_lock_path, e);
        } else {
            info!("Successfully removed lock file at {:?}", sled_lock_path);
        }
    }
}

#[cfg(feature = "with-sled")]
pub async fn recover_sled(lock_path: PathBuf) -> Result<(), GraphError> {
    debug!("Starting recover_sled for {:?}", lock_path);
    if let Some(parent) = lock_path.parent() {
        match fs::metadata(parent).await {
            Ok(metadata) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let permissions = metadata.permissions();
                    if permissions.mode() & 0o200 == 0 {
                        warn!("Parent directory {:?} lacks write permissions (mode: {:o})", parent, permissions.mode());
                    }
                    debug!("Parent directory owned by UID: {}, current process UID: {}", metadata.uid(), nix::unistd::getuid().as_raw());
                }
            }
            Err(e) => {
                warn!("Failed to check parent directory metadata for {:?}: {}", parent, e);
            }
        }
    }
    
    if lock_file_exists(lock_path.clone()).await? {
        warn!("Attempting to remove stale Sled lock file at {:?}", lock_path);
        match fs::metadata(&lock_path).await {
            Ok(metadata) => {
                debug!("Lock file permissions: {:?}", metadata.permissions());
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let permissions = metadata.permissions();
                    debug!("Lock file mode: {:o}, UID: {}, current UID: {}", permissions.mode(), metadata.uid(), nix::unistd::getuid().as_raw());
                    if permissions.readonly() {
                        let mut new_perms = permissions.clone();
                        new_perms.set_mode(0o600);
                        if let Err(e) = fs::set_permissions(&lock_path, new_perms).await {
                            warn!("Failed to make lock file writable at {:?}: {}", lock_path, e);
                        }
                    }
                }
                #[cfg(unix)]
                {
                    if let Ok(output) = std::process::Command::new("lsof").arg(lock_path.to_str().unwrap()).output() {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        if !stdout.is_empty() {
                            warn!("Processes holding lock file {:?}: {}", lock_path, stdout);
                        } else {
                            debug!("No processes currently holding lock file {:?}", lock_path);
                        }
                    } else {
                        warn!("Failed to run lsof on {:?}", lock_path);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to check lock file permissions for {:?}: {}", lock_path, e);
            }
        }
        
        match fs::remove_file(&lock_path).await {
            Ok(()) => {
                info!("Successfully removed stale Sled lock file at {:?}", lock_path);
                if lock_file_exists(lock_path.clone()).await? {
                    error!("Lock file at {:?} still exists after removal attempt", lock_path);
                    return Err(GraphError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to ensure Sled lock file removal at {:?}", lock_path),
                    )));
                }
                Ok(())
            }
            Err(e) => {
                error!("Failed to remove Sled lock file at {:?}: {}", lock_path, e);
                match std::fs::remove_file(&lock_path) {
                    Ok(()) => {
                        info!("Successfully removed stale Sled lock file (sync) at {:?}", lock_path);
                        if lock_file_exists(lock_path.clone()).await? {
                            error!("Lock file at {:?} still exists after sync removal attempt", lock_path);
                            return Err(GraphError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Failed to ensure Sled lock file removal at {:?}", lock_path),
                            )));
                        }
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to remove Sled lock file (sync) at {:?}: {}", lock_path, e);
                        Err(GraphError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to remove Sled lock file: {}", e),
                        )))
                    }
                }
            }
        }
    } else {
        debug!("No lock file found for Sled at {:?}", lock_path);
        Ok(())
    }
}

/// Recovers a RocksDB database by clearing stale lock file
async fn recover_rocksdb(data_dir: &PathBuf) -> Result<(), GraphError> {
    warn!("Checking for RocksDB lock file at {:?}", data_dir);
    let lock_file = data_dir.join("LOCK");
    const MAX_RETRIES: u32 = 3;
    let mut retries = 0;

    while lock_file.exists() && retries < MAX_RETRIES {
        trace!("Lock file found: {:?}", lock_file);
        
        // Prevent reentrant cleanup
        if CLEANUP_IN_PROGRESS.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                match fs::metadata(&lock_file).await {
                    Ok(metadata) => {
                        let acquire_time = metadata.mtime();
                        let current_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(i64::MAX);
                        trace!("Lock file age: {}s (current_time: {}, acquire_time: {})", current_time - acquire_time, current_time, acquire_time);
                        
                        // Check if lock is older than 1 minute
                        if current_time - acquire_time > 60 {
                            warn!("Removing stale RocksDB lock file (age {}s, retry {}): {:?}", current_time - acquire_time, retries, lock_file);
                            fs::remove_file(&lock_file)
                                .await
                                .map_err(|e| GraphError::Io(e))
                                .with_context(|| format!("Failed to remove stale RocksDB lock file {:?}", lock_file))?;
                            info!("Successfully removed stale RocksDB lock file");
                            CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                            break;
                        } else {
                            // Attempt to shut down existing engine using async operations
                            if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                                trace!("Attempting to shut down existing engine (retry {})", retries);
                                let manager_arc = Arc::clone(manager);
                                
                                match async {
                                    let manager = manager_arc.get_manager();
                                    let mgr = manager.lock().await;
                                    let engine = mgr.engine.lock().await;
                                    (*engine).stop().await
                                }.await {
                                    Ok(()) => {
                                        info!("Shut down existing engine before lock removal");
                                    }
                                    Err(e) => {
                                        warn!("Failed to shut down existing manager: {:?}", e);
                                    }
                                }
                            } else {
                                trace!("No existing storage engine manager found to shut down");
                            }
                            
                            // Wait to ensure resources are released
                            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                            
                            // Check if lock is held by current process
                            let current_pid = process::id();
                            trace!("Checking if lock is held by current process (PID: {})", current_pid);
                            
                            match tokio::process::Command::new("lsof")
                                .arg(lock_file.to_str().unwrap())
                                .output()
                                .await
                            {
                                Ok(lsof_output) => {
                                    let output = String::from_utf8_lossy(&lsof_output.stdout);
                                    trace!("lsof output for lock file: {}", output);
                                    let pid_lines: Vec<&str> = output.lines()
                                        .filter(|line| line.contains(&lock_file.to_str().unwrap()))
                                        .collect();
                                    let mut lock_held_by_current = false;
                                    
                                    for line in pid_lines {
                                        let fields: Vec<&str> = line.split_whitespace().collect();
                                        if fields.len() > 1 {
                                            if let Ok(pid) = fields[1].parse::<u32>() {
                                                if pid == current_pid {
                                                    lock_held_by_current = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    
                                    if lock_held_by_current || output.is_empty() {
                                        warn!("Lock file likely held by current process or stale, removing (retry {}): {:?}", retries, lock_file);
                                        fs::remove_file(&lock_file)
                                            .await
                                            .map_err(|e| GraphError::Io(e))
                                            .with_context(|| format!("Failed to remove RocksDB lock file {:?}", lock_file))?;
                                        info!("Successfully removed RocksDB lock file");
                                        CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                                        break;
                                    } else {
                                        error!("RocksDB lock file is held by another process: {:?}", lock_file);
                                        CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                                        return Err(GraphError::StorageError(format!(
                                            "RocksDB lock file is held by another process: {:?}", lock_file
                                        )));
                                    }
                                }
                                Err(_) => {
                                    warn!("Failed to run lsof, assuming lock is stale (retry {}): {:?}", retries, lock_file);
                                    fs::remove_file(&lock_file)
                                        .await
                                        .map_err(|e| GraphError::Io(e))
                                        .with_context(|| format!("Failed to remove RocksDB lock file {:?}", lock_file))?;
                                    info!("Successfully removed RocksDB lock file");
                                    CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read metadata for RocksDB lock file {:?}: {}", lock_file, e);
                        CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                        return Err(GraphError::Io(e));
                    }
                }
            }
            
            #[cfg(not(unix))]
            {
                // Attempt to shut down existing engine
                if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                    trace!("Attempting to shut down existing engine (retry {})", retries);
                    let manager_arc = Arc::clone(manager);
                    let manager = manager_arc.get_manager();
                    let mgr = manager.lock().await;
                    let engine = mgr.engine.lock().await;
                    if let Err(e) = (*engine).stop().await {
                        warn!("Failed to shut down existing manager: {:?}", e);
                    } else {
                        info!("Shut down existing engine before lock removal");
                    }
                }
                
                // Wait to ensure resources are released
                tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                
                warn!("Removing RocksDB lock file (non-Unix system, retry {}): {:?}", retries, lock_file);
                fs::remove_file(&lock_file)
                    .await
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("Failed to remove RocksDB lock file {:?}", lock_file))?;
                info!("Successfully removed RocksDB lock file");
                CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                break;
            }
        } else {
            trace!("Cleanup already in progress, skipping retry {}", retries);
        }
        
        retries += 1;
        if retries < MAX_RETRIES {
            trace!("Retrying lock file cleanup after 3s delay (attempt {}/{})", retries + 1, MAX_RETRIES);
            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
        }
    }

    if lock_file.exists() {
        error!("Failed to remove RocksDB lock file after {} retries: {:?}", MAX_RETRIES, lock_file);
        warn!("Terminating process to release lock file as a last resort");
        process::exit(1); // Force exit to release resources
    }

    if !data_dir.exists() {
        info!("Creating RocksDB directory: {:?}", data_dir);
        fs::create_dir_all(data_dir)
            .await
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create RocksDB directory {:?}", data_dir))?;
    }
    
    info!("RocksDB directory ready: {:?}", data_dir);
    CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
    Ok(())
}

/// Performs emergency cleanup of the storage engine manager
/// Performs emergency cleanup of the storage engine manager
pub async fn emergency_cleanup_storage_engine_manager() -> Result<(), anyhow::Error> {
    info!("Performing emergency cleanup for StorageEngineManager");
    
    // Clean up FileLock
    let lock_path = PathBuf::from(LOCK_FILE_PATH);
    if lock_path.exists() {
        if let Err(e) = fs::remove_file(&lock_path).await {
            warn!("Failed to remove lock file at {:?}: {}", lock_path, e);
        } else {
            info!("Removed lock file at {:?}", lock_path);
        }
    }
    
    // Clean up GLOBAL_STORAGE_ENGINE_MANAGER
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        let mutex = manager.get_manager();
        let mut locked_manager = mutex.lock().await;
        if let Err(e) = locked_manager.shutdown().await {
            warn!("Failed to shutdown StorageEngineManager: {}", e);
        }
        drop(locked_manager);
    }
    
    // Additional Sled-specific cleanup
    #[cfg(feature = "with-sled")]
    {
        let sled_path = PathBuf::from("/opt/graphdb/storage_data/sled");
       /*
        if sled_path.exists() {
            // Call SledStorage::force_unlock (needs to be made public in sled_storage.rs)
            if let Err(e) = SledStorage::force_unlock(&sled_path).await {
                warn!("Failed to force unlock Sled database at {:?}: {}", sled_path, e);
            } else {
                info!("Successfully forced unlock on Sled database at {:?}", sled_path);
            }
        }
        */
        // Kill any processes holding file descriptors
        if let Ok(output) = tokio::process::Command::new("lsof")
            .arg("-t")
            .arg(sled_path.to_str().ok_or_else(|| anyhow!("Invalid sled path"))?)
            .output()
            .await
        {
            let pids = String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|pid| pid.trim().parse::<u32>().ok())
                .collect::<Vec<u32>>();
            
            for pid in pids {
                if let Err(e) = tokio::process::Command::new("kill")
                    .arg("-9")
                    .arg(pid.to_string())
                    .status()
                    .await
                {
                    warn!("Failed to kill process {}: {}", pid, e);
                } else {
                    info!("Killed process {} holding Sled database", pid);
                }
            }
        }
    }
    
    Ok(())
}

// StorageEngine and GraphStorageEngine traits
#[async_trait]
pub trait StorageEngine: Send + Sync + Debug + 'static {
    async fn connect(&self) -> Result<(), GraphError>;
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError>;
    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError>;
    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError>;
    async fn flush(&self) -> Result<(), GraphError>;
}

#[async_trait]
pub trait GraphStorageEngine: StorageEngine + Send + Sync + Debug + 'static {
    async fn start(&self) -> Result<(), GraphError>;
    async fn stop(&self) -> Result<(), GraphError>;
    fn get_type(&self) -> &'static str;
    async fn is_running(&self) -> bool;
    async fn query(&self, query_string: &str) -> Result<Value, GraphError>;
    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError>;
    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError>;
    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError>;
    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError>;
    async fn clear_data(&self) -> Result<(), GraphError>;
    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError>;
    fn as_any(&self) -> &dyn Any;
    async fn close(&self) -> Result<(), GraphError>;
}

#[derive(Debug)]
pub struct StorageEngineManager {
    pub engine: Arc<TokioMutex<HybridStorage>>,
    persistent_engine: Arc<dyn GraphStorageEngine + Send + Sync>,
    session_engine_type: Option<StorageEngineType>,
    config: StorageConfig,
    config_path: PathBuf,
    // The key of a HashMap must be a concrete type that implements the `Eq` and `Hash` traits.
    // Assuming NodeId is a concrete type (e.g., a struct or integer) and not a trait.
    raft_instances: Arc<TokioMutex<HashMap<u64, Arc<dyn Any + Send + Sync>>>>,
}

impl StorageEngineManager {
    pub async fn new(
        storage_engine_type: StorageEngineType,
        config_path: &PathBuf,
        use_temp: bool,
        port: Option<u16>,
    ) -> Result<Self, GraphError> {
        info!("Creating StorageEngineManager with engine: {:?}", storage_engine_type);
        println!("===> CREATING NEW STORAGE ENGINE MANAGER WITH TYPE {:?}", storage_engine_type);

        let mut config = load_storage_config_from_yaml(Some(config_path.clone())).await
            .map_err(|e| GraphError::ConfigurationError(format!("Failed to load storage config: {}", e)))?;
        let port = port.unwrap_or(config.default_port);

        let engine_path_name = storage_engine_type.to_string().to_lowercase();
        let base_data_dir = config.data_directory
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
        let base_engine_path = base_data_dir.join(&engine_path_name);
        let engine_path = base_engine_path.join(port.to_string());

        // Check and release any existing locks before proceeding
        match storage_engine_type {
            #[cfg(feature = "with-sled")]
            StorageEngineType::Sled => {
                if engine_path.exists() {
                    if let Err(e) = SledStorage::force_unlock(&engine_path).await {
                        warn!("Failed to unlock Sled database at {:?}: {}", engine_path, e);
                        return Err(GraphError::StorageError(format!("Failed to unlock Sled database at {:?}: {}", engine_path, e)));
                    } else {
                        info!("Successfully unlocked Sled database at {:?}", engine_path);
                        println!("===> SUCCESSFULLY UNLOCKED SLED DATABASE AT {:?}", engine_path);
                    }
                    // Verify no lock file exists
                    let lock_file = engine_path.join("db.lck");
                    if lock_file.exists() {
                        return Err(GraphError::StorageError(format!("Lock file still exists at {:?} after unlock attempt", lock_file)));
                    }
                    println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
                }
            }
            #[cfg(feature = "with-rocksdb")]
            StorageEngineType::RocksDB => {
                if engine_path.exists() {
                    if let Err(e) = SurrealdbGraphStorage::force_unlock(&engine_path).await {
                        warn!("Failed to unlock RocksDB database at {:?}: {}", engine_path, e);
                        return Err(GraphError::StorageError(format!("Failed to unlock RocksDB database at {:?}: {}", engine_path, e)));
                    } else {
                        info!("Successfully unlocked RocksDB database at {:?}", engine_path);
                        println!("===> SUCCESSFULLY UNLOCKED ROCKSDB DATABASE AT {:?}", engine_path);
                    }
                    // Verify no lock file exists
                    let lock_file = engine_path.join("LOCK");
                    if lock_file.exists() {
                        return Err(GraphError::StorageError(format!("Lock file still exists at {:?} after unlock attempt", lock_file)));
                    }
                    println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
                }
            }
            _ => {}
        }

        if let Some(ref mut engine_config) = config.engine_specific_config {
            engine_config.storage.path = Some(engine_path.clone());
            engine_config.storage.port = Some(port);
        }

        // Get or create metadata
        let metadata = match GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
            Some(mut existing) => {
                if existing.data_dir != Some(engine_path.clone()) {
                    warn!("Updating daemon data_dir from {:?} to {:?}", existing.data_dir, engine_path);
                    println!("===> UPDATING DAEMON DATA_DIR FROM {:?} TO {:?}", existing.data_dir, engine_path);
                    existing.data_dir = Some(engine_path.clone());
                    GLOBAL_DAEMON_REGISTRY.update_daemon_metadata(existing.clone()).await?;
                }
                existing
            }
            None => {
                let new_metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: std::process::id(),
                    ip_address: "127.0.0.1".to_string(),
                    data_dir: Some(engine_path.clone()),
                    config_path: Some(config_path.clone()),
                    engine_type: Some(storage_engine_type.to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                };
                GLOBAL_DAEMON_REGISTRY.register_daemon(new_metadata.clone()).await?;
                println!("===> REGISTERED NEW DAEMON ENTRY FOR PORT {}", port);
                new_metadata
            }
        };

        if !use_temp {
            config.save().await.map_err(|e| GraphError::ConfigurationError(format!("Failed to save config: {}", e)))?;
        }

        let engine = Self::initialize_storage_engine(storage_engine_type, &config).await?;

        Self::create_manager(
            storage_engine_type,
            config,
            config_path,
            engine,
            !use_temp,
            Arc::new(TokioMutex::new(HashMap::new())),
        )
        .await
    }

    #[cfg(feature = "with-sled")]
    async fn init_sled(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing Sled engine: {:?}", config);
        println!("===> INITIALIZING SLED ENGINE: {:?}", config);

        let engine_specific = config.engine_specific_config.as_ref()
            .ok_or_else(|| GraphError::ConfigurationError("Missing engine_specific_config for Sled".to_string()))?;

        let sled_path = engine_specific.storage.path.clone()
            .ok_or_else(|| GraphError::ConfigurationError("Missing path for Sled".to_string()))?;

        let port = engine_specific.storage.port.unwrap_or(DEFAULT_STORAGE_PORT);

        // Check and release any existing locks
        if sled_path.exists() {
            if let Err(e) = SledStorage::force_unlock(&sled_path).await {
                return Err(GraphError::StorageError(format!("Failed to unlock Sled database at {:?}: {}", sled_path, e)));
            }
            info!("Successfully unlocked Sled database at {:?}", sled_path);
            println!("===> SUCCESSFULLY UNLOCKED SLED DATABASE AT {:?}", sled_path);
            // Verify no lock file exists
            let lock_file = sled_path.join("db.lck");
            if lock_file.exists() {
                return Err(GraphError::StorageError(format!("Lock file still exists at {:?} after unlock attempt", lock_file)));
            }
            println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
        }

        // Attempt to get existing metadata
        let metadata = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?;

        if let Some(mut existing_metadata) = metadata {
            if existing_metadata.data_dir != Some(sled_path.clone()) {
                warn!("Path mismatch for Sled: registry shows {:?}, but config specifies {:?}", existing_metadata.data_dir, sled_path);
                println!("===> PATH MISMATCH FOR SLED: REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", existing_metadata.data_dir, sled_path);
                
                // Update registry with correct path
                existing_metadata.data_dir = Some(sled_path.clone());
                GLOBAL_DAEMON_REGISTRY.update_daemon_metadata(existing_metadata).await?;
                info!("Updated daemon registry data_dir to {:?}", sled_path);
                println!("===> UPDATED DAEMON REGISTRY DATA_DIR TO {:?}", sled_path);
            }
        } else {
            // Create new metadata if not exists
            let new_metadata = DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: std::process::id(),
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(sled_path.clone()),
                config_path: None,
                engine_type: Some("Sled".to_string()),
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0),
            };
            GLOBAL_DAEMON_REGISTRY.register_daemon(new_metadata).await?;
            info!("Created new daemon registry entry for port {} with path {:?}", port, sled_path);
            println!("===> CREATED NEW DAEMON REGISTRY ENTRY FOR PORT {} WITH PATH {:?}", port, sled_path);
        }

        let sled_config = SledConfig {
            storage_engine_type: StorageEngineType::Sled,
            path: sled_path,
            host: engine_specific.storage.host.clone(),
            port: engine_specific.storage.port,
            temporary: false,
            use_compression: engine_specific.storage.use_compression,
            cache_capacity: engine_specific.storage.cache_capacity,
        };

        // Attempt to open Sled database
        SledStorage::new(&sled_config, config).await
            .map(|s| Arc::new(s) as Arc<dyn GraphStorageEngine + Send + Sync>)
            .map_err(|e| GraphError::StorageError(format!("Failed to initialize Sled: {}", e)))
    }

    async fn cleanup_legacy_sled_directories_during_reset(base_data_dir: &Path, current_port: u16) {
        info!("Cleaning up legacy port-suffixed Sled directories during reset in {:?}", base_data_dir);
        
        if !base_data_dir.exists() {
            return;
        }
        
        if let Ok(entries) = tokio::fs::read_dir(base_data_dir).await {
            let mut entries = entries;
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    if name.starts_with("sled_") && name != "sled" {
                        if let Some(suffix) = name.strip_prefix("sled_") {
                            if let Ok(old_port) = suffix.parse::<u16>() {
                                if old_port != current_port {
                                    let old_path = entry.path();
                                    info!("Found legacy Sled directory during reset: {:?} (port {})", old_path, old_port);
                                    
                                    if old_path.exists() {
                                        if let Err(e) = SledStorage::force_unlock(&old_path).await {
                                            warn!("Failed to force unlock Sled database during reset at {:?}: {}", old_path, e);
                                        } else {
                                            info!("Successfully unlocked Sled database during reset at {:?}", old_path);
                                            println!("===> SUCCESSFULLY UNLOCKED SLED DATABASE DURING RESET AT {:?}", old_path);
                                        }
                                        // Verify no lock file exists
                                        let lock_file = old_path.join("db.lck");
                                        if lock_file.exists() {
                                            warn!("Lock file still exists at {:?} after unlock attempt during reset", lock_file);
                                        } else {
                                            println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
                                        }
                                    }
                                    
                                    match tokio::fs::remove_dir_all(&old_path).await {
                                        Ok(_) => {
                                            info!("Successfully removed legacy Sled directory during reset: {:?}", old_path);
                                            if let Ok(_) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(old_port).await {
                                                info!("Unregistered daemon registry entry for legacy port {} during reset", old_port);
                                            }
                                        }
                                        Err(e) => {
                                            warn!("Failed to remove legacy Sled directory during reset {:?}: {}", old_path, e);
                                            if let Ok(_) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(old_port).await {
                                                info!("Unregistered daemon registry entry for legacy port {} during reset (directory removal failed)", old_port);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            warn!("Could not read directory entries from {:?} during reset", base_data_dir);
        }
        
        info!("Completed cleanup of legacy port-suffixed Sled directories during reset");
    }

    async fn load_and_configure(
        storage_engine: StorageEngineType,
        config_path_yaml: &Path,
        permanent: bool,
    ) -> Result<StorageConfig, GraphError> {
        info!("Loading config from {:?}", config_path_yaml);
        
        let mut config = if config_path_yaml.exists() {
            Self::load_existing_config(config_path_yaml).await?
        } else {
            Self::create_default_config(storage_engine, config_path_yaml).await?
        };

        if config.storage_engine_type != storage_engine {
            Self::override_engine_config(&mut config, storage_engine, config_path_yaml, permanent).await?;
        }

        Self::validate_config(&config, storage_engine).await?;
        Ok(config)
    }

    pub async fn initialize_raft_instances(
        config: &StorageConfig,
        raft_instances: &Arc<TokioMutex<HashMap<NodeIdType, Arc<dyn Any + Send + Sync>>>>,
    ) -> Result<(), GraphError> {
        info!("Initializing Raft instances for clustering");
        let mut instances = raft_instances.lock().await;

        // Example node ID
        let node_id: NodeIdType = 1;

        // Raft runtime configuration
        let raft_config = Arc::new(RaftConfig {
            cluster_name: "graphdb-cluster".to_string(),
            ..Default::default()
        });

        // RaftStorage wrapper
        let raft_graph_store = RaftStorage::new(config);

        // Create the OpenRaft adaptor for log & state machine
        let store: Arc<MemStore> = MemStore::new_async().await;
        let (log_store, state_machine) =
            Adaptor::<RaftMemStoreTypeConfig, Arc<MemStore>>::new(store.clone());

        let raft_network: ExampleNetwork = ExampleNetwork::default();

        let raft_node = Raft::<RaftMemStoreTypeConfig>::new(
            node_id,
            raft_config.clone(),
            raft_network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to create Raft instance: {}", e)))?;

        // Store the node in the shared map
        instances.insert(node_id, Arc::new(raft_node) as Arc<dyn Any + Send + Sync>);

        info!("Initialized Raft node with ID: {}", node_id);
        Ok(())
    }

    pub async fn get_raft_instance(&self, node_id: u64) -> Result<Arc<dyn Any + Send + Sync>, GraphError> {
        let instances = self.raft_instances.lock().await;
        instances.get(&node_id)
            .cloned()
            .ok_or_else(|| GraphError::StorageError(format!("No Raft instance found for node_id {}", node_id)))
    }

    pub async fn reset_config(&mut self, config: StorageConfig) -> Result<(), GraphError> {
        // Update the internal configuration state
        self.config = config;
        info!("StorageEngineManager configuration reset: {:?}", self.config);
        Ok(())
    }

    async fn shutdown_existing_manager() {
        if let Some(existing_manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            trace!("Shutting down existing StorageEngineManager before initialization");
            
            // BEGIN FIX: Explicitly drop the persistent engine handle to release the file lock.
            {
                let manager = existing_manager.get_manager();
                let mgr_locked = manager.lock().await;
                // Get a mutable reference to the persistent engine and take ownership.
                // This drops the old handle and releases the lock.
                let _ = mgr_locked.persistent_engine;
            }
            info!("Old persistent engine handle dropped. File lock released.");
            // END FIX

            const MAX_RETRIES: u32 = 3;
            for retry in 0..MAX_RETRIES {
                let stop_result = {
                    let manager = existing_manager.get_manager();
                    let mgr = manager.lock().await;
                    let engine = mgr.engine.lock().await;
                    
                    if (*engine).is_running().await {
                        (*engine).stop().await
                    } else {
                        info!("Existing engine is already stopped.");
                        return;
                    }
                };

                match stop_result {
                    Ok(()) => {
                        info!("Shut down existing StorageEngineManager successfully");
                        return;
                    }
                    Err(e) => {
                        warn!("Failed to shut down existing manager (retry {}): {:?}", retry, e);
                        if retry < MAX_RETRIES - 1 {
                            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                        }
                    }
                }
            }
            warn!("Failed to shut down existing StorageEngineManager after {} retries", MAX_RETRIES);
        } else {
            trace!("No existing storage engine manager found to shut down");
        }
    }

    async fn load_existing_config(config_path_yaml: &Path) -> Result<StorageConfig, GraphError> {
        let content = tokio::fs::read_to_string(config_path_yaml)
            .await
            .map_err(|e| {
                error!("Failed to read YAML file at {:?}: {}", config_path_yaml, e);
                GraphError::Io(e)
            })?;
        
        debug!("Raw YAML content from {:?}:\n{}", config_path_yaml, content);
        
        load_storage_config_from_yaml(Some(config_path_yaml.to_path_buf())).await
            .map_err(|e| {
                error!("Failed to deserialize YAML config from {:?}: {}", config_path_yaml, e);
                GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
            })
    }

    // Helper method to clean up old port-suffixed directories
    async fn cleanup_old_port_directories(base_dir: &Path, engine_prefix: &str, current_port: u16) {
        if let Ok(entries) = tokio::fs::read_dir(base_dir).await {
            let mut entries = entries;
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    // Look for directories matching pattern like "sled_8052", "sled_8053", etc.
                    if name.starts_with(&format!("{}_", engine_prefix)) && name != format!("{}_{}", engine_prefix, current_port) {
                        if let Ok(path) = entry.path().canonicalize() {
                            // Check if this looks like an old port directory
                            if let Some(suffix) = name.strip_prefix(&format!("{}_", engine_prefix)) {
                                if suffix.parse::<u16>().is_ok() {
                                    // This looks like an old port directory
                                    match tokio::fs::remove_dir_all(&path).await {
                                        Ok(_) => info!("Cleaned up old storage directory: {:?}", path),
                                        Err(e) => warn!("Failed to clean up old storage directory {:?}: {}", path, e),
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Helper method to handle port conflicts with reduced lock contention
    async fn handle_port_conflicts(
        storage_engine: StorageEngineType,
        config: &StorageConfig,
        selected_port: u16,
    ) -> Result<(), GraphError> {
        if let Some(metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(selected_port).await? {
            if !NonBlockingDaemonRegistry::is_pid_running(metadata.pid).await.unwrap_or(false) {
                warn!("Stale storage process registered on port {}. Attempting cleanup.", selected_port);
                GLOBAL_DAEMON_REGISTRY.unregister_daemon(selected_port).await?;
                
                if storage_engine == StorageEngineType::Sled {
                    // Get the actual configured path (without port suffix)
                    let sled_path = if let Some(ref engine_config) = config.engine_specific_config {
                        engine_config.storage.path.clone()
                            .unwrap_or_else(|| {
                                config.data_directory
                                    .clone()
                                    .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                                    .join("sled")
                            })
                    } else {
                        config.data_directory
                            .clone()
                            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                            .join("sled")
                    };
                    
                    // Try to unlock the current configured path
                    if sled_path.exists() {
                        if let Err(e) = SledStorage::force_unlock(&sled_path).await {
                            warn!("Failed to unlock Sled storage at {:?}: {}", sled_path, e);
                        } else {
                            info!("Successfully unlocked Sled storage at {:?}", sled_path);
                        }
                    }
                    
                    // Also check for and clean up any legacy port-suffixed paths
                    let base_data_dir = config.data_directory
                        .clone()
                        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
                    
                    Self::cleanup_legacy_sled_paths(&base_data_dir, selected_port).await;
                }
            } else {
                info!("Active storage process found on port {}. Skipping cleanup.", selected_port);
            }
        }
        Ok(())
    }


    /// Helper function to clean up legacy port-suffixed Sled directories
    async fn cleanup_legacy_sled_paths(base_data_dir: &Path, current_port: u16) {
        info!("Cleaning up legacy port-suffixed Sled directories in {:?}", base_data_dir);
        
        if !base_data_dir.exists() {
            return;
        }
        
        if let Ok(entries) = tokio::fs::read_dir(base_data_dir).await {
            let mut entries = entries;
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    // Look for directories matching pattern like "sled_8052", "sled_8053", etc.
                    if name.starts_with("sled_") && name != "sled" {
                        if let Some(suffix) = name.strip_prefix("sled_") {
                            if let Ok(old_port) = suffix.parse::<u16>() {
                                let old_path = entry.path();
                                info!("Found legacy Sled directory: {:?} (port {})", old_path, old_port);
                                
                                // Try to force unlock any database locks first
                                if old_path.exists() {
                                    if let Err(e) = SledStorage::force_unlock(&old_path).await {
                                        warn!("Failed to force unlock Sled database at {:?}: {}", old_path, e);
                                    } else {
                                        info!("Successfully unlocked Sled database at {:?}", old_path);
                                    }
                                }
                                
                                // Attempt to remove the entire legacy directory
                                match tokio::fs::remove_dir_all(&old_path).await {
                                    Ok(_) => {
                                        info!("Successfully removed legacy Sled directory: {:?}", old_path);
                                        
                                        // Also clean up daemon registry entry for the old port if it exists
                                        if let Ok(_) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(old_port).await {
                                            info!("Unregistered daemon registry entry for legacy port {}", old_port);
                                        }
                                    }
                                    Err(e) => {
                                        warn!("Failed to remove legacy Sled directory {:?}: {}", old_path, e);
                                        // If we can't remove it, at least try to clean up the registry
                                        if let Ok(_) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(old_port).await {
                                            info!("Unregistered daemon registry entry for legacy port {} (directory removal failed)", old_port);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else {
            warn!("Could not read directory entries from {:?}", base_data_dir);
        }
        
        info!("Completed cleanup of legacy port-suffixed Sled directories");
    }

    // Helper method to clean up legacy port-suffixed database locks
    async fn cleanup_legacy_port_conflicts(base_data_dir: &Path, current_port: u16) {
        if !base_data_dir.exists() {
            return;
        }
        
        info!("Checking for legacy port-suffixed Sled databases to unlock in {:?}", base_data_dir);
        
        if let Ok(entries) = tokio::fs::read_dir(base_data_dir).await {
            let mut entries = entries;
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    // Look for directories matching pattern like "sled_8052", "sled_8053", etc.
                    if name.starts_with("sled_") && name != "sled" {
                        if let Some(suffix) = name.strip_prefix("sled_") {
                            if let Ok(legacy_port) = suffix.parse::<u16>() {
                                let legacy_sled_path = entry.path();
                                let legacy_db_path = legacy_sled_path;
                                
                                if legacy_db_path.exists() {
                                    info!("Found legacy Sled database at {:?} (port {})", legacy_db_path, legacy_port);
                                    
                                    // Try to force unlock the legacy database
                                    if let Err(e) = SledStorage::force_unlock(&legacy_db_path).await {
                                        warn!("Failed to unlock legacy Sled storage at {:?}: {}", legacy_db_path, e);
                                    } else {
                                        info!("Successfully unlocked legacy Sled storage at {:?}", legacy_db_path);
                                        
                                        // If we successfully unlocked it, try to remove the entire legacy directory
                                        match tokio::fs::remove_dir_all(&legacy_db_path).await {
                                            Ok(_) => info!("Removed legacy Sled directory: {:?}", legacy_db_path),
                                            Err(e) => warn!("Failed to remove legacy Sled directory {:?}: {}", legacy_db_path, e),
                                        }
                                    }
                                    
                                    // Also clean up daemon registry for the legacy port
                                    if let Ok(_) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(legacy_port).await {
                                        info!("Unregistered legacy daemon registry entry for port {}", legacy_port);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Helper method to ensure daemon registry directory exists
    async fn ensure_daemon_registry_directory() -> Result<(), GraphError> {
        let registry_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);
        debug!("Attempting to create daemon registry directory for path: {:?}", registry_path);
        
        if let Some(parent) = registry_path.parent() {
            debug!("Checking parent directory: {:?}", parent);
            if !parent.exists() {
                debug!("Parent directory does not exist, attempting to create: {:?}", parent);
                let max_retries = 3;
                
                for attempt in 1..=max_retries {
                    match fs::create_dir_all(parent).await {
                        Ok(_) => {
                            debug!("Successfully created daemon registry directory: {:?}", parent);
                            break;
                        }
                        Err(e) => {
                            error!(
                                "Failed to create daemon registry directory {:?} on attempt {}/{}: {}",
                                parent, attempt, max_retries, e
                            );
                            
                            if parent.exists() {
                                if let Ok(metadata) = fs::metadata(parent).await {
                                    error!("Parent directory exists with permissions: {:?}", metadata.permissions());
                                }
                            } else {
                                error!("Parent directory does not exist: {:?}", parent);
                            }
                            
                            if attempt == max_retries {
                                let fallback_path = PathBuf::from("/tmp/graphdb/daemon_registry.db");
                                warn!("Falling back to registry path: {:?}", fallback_path);
                                
                                if let Some(fallback_parent) = fallback_path.parent() {
                                    fs::create_dir_all(fallback_parent)
                                        .await
                                        .map_err(|e| {
                                            error!("Failed to create fallback registry directory {:?}: {}", fallback_parent, e);
                                            GraphError::Io(e)
                                        })?;
                                    debug!("Successfully created fallback registry directory: {:?}", fallback_parent);
                                }
                                break;
                            }
                            
                            sleep(TokioDuration::from_millis(500 * attempt as u64)).await;
                        }
                    }
                }
            } else {
                if let Ok(metadata) = fs::metadata(parent).await {
                    if metadata.permissions().readonly() {
                        error!("Parent directory {:?} is not writable", parent);
                        let fallback_path = PathBuf::from("/tmp/graphdb/daemon_registry.db");
                        warn!("Falling back to registry path: {:?}", fallback_path);
                        
                        if let Some(fallback_parent) = fallback_path.parent() {
                            fs::create_dir_all(fallback_parent)
                                .await
                                .map_err(|e| {
                                    error!("Failed to create fallback registry directory {:?}: {}", fallback_parent, e);
                                    GraphError::Io(e)
                                })?;
                            debug!("Successfully created fallback registry directory: {:?}", fallback_parent);
                        }
                    } else {
                        debug!("Parent directory is writable: {:?}", parent);
                    }
                }
            }
        } else {
            error!("Registry path has no parent: {:?}", registry_path);
            return Err(GraphError::ConfigurationError(format!(
                "Invalid registry path: {:?}", registry_path
            )));
        }
        
        debug!("Ensured daemon registry directory exists: {:?}", registry_path);
        Ok(())
    }

    async fn create_default_config(
        storage_engine: StorageEngineType,
        config_path_yaml: &Path,
    ) -> Result<StorageConfig, GraphError> {
        warn!("Config file not found at {:?}", config_path_yaml);
        
        create_default_storage_yaml_config(&config_path_yaml.to_path_buf(), storage_engine).await?;
        
        load_storage_config_from_yaml(Some(config_path_yaml.to_path_buf())).await
            .map_err(|e| {
                error!("Failed to load newly created YAML config from {:?}: {}", config_path_yaml, e);
                GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
            })
    }

    async fn override_engine_config(
        config: &mut StorageConfig,
        storage_engine: StorageEngineType,
        config_path_yaml: &Path,
        permanent: bool,
    ) -> Result<(), GraphError> {
        info!(
            "Overriding YAML storage_engine_type ({:?}) with passed engine: {:?}", 
            config.storage_engine_type, 
            storage_engine
        );
        println!("=====> WE ARE OVERRIDING {:?}", storage_engine);
        config.storage_engine_type = storage_engine;
        config.engine_specific_config = Some(
            load_engine_specific_config(storage_engine, config_path_yaml)
                .map_err(|e| {
                    error!("Failed to load engine-specific config for {:?}: {}", storage_engine, e);
                    GraphError::ConfigurationError(format!("Failed to load engine-specific config: {}", e))
                })?
        );

        if permanent {
            Self::save_config_permanently(config, config_path_yaml).await?;
        }

        debug!(
            "Config after override: storage_engine_type={:?}, default_port={}, cluster_range={}",
            config.storage_engine_type, config.default_port, config.cluster_range
        );
        
        Ok(())
    }

    async fn save_config_permanently(
        config: &StorageConfig,
        config_path_yaml: &Path,
    ) -> Result<(), GraphError> {
        config.save().await.map_err(|e| {
            error!("Failed to save updated config to {:?}: {}", config_path_yaml, e);
            GraphError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Verify saved config
        let saved_content = tokio::fs::read_to_string(config_path_yaml)
            .await
            .map_err(|e| {
                error!("Failed to read saved YAML config at {:?}: {}", config_path_yaml, e);
                GraphError::Io(e)
            })?;
        
        debug!("Saved YAML content at {:?}:\n{}", config_path_yaml, saved_content);
        info!("Updated YAML config at {:?} with storage_engine_type: {:?}", config_path_yaml, config.storage_engine_type);
        
        Ok(())
    }

    async fn initialize_storage_engine(
        engine_type: StorageEngineType,
        config: &StorageConfig,
    ) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing storage engine: {:?}", engine_type);
        
        match engine_type {
            StorageEngineType::Hybrid => Self::init_hybrid(config),
            StorageEngineType::InMemory => Self::init_inmemory(config),
            StorageEngineType::Sled => Self::init_sled(config).await,
            StorageEngineType::RocksDB => Self::init_rocksdb(config).await,
            StorageEngineType::Redis => Self::init_redis(config).await,
            StorageEngineType::PostgreSQL => Self::init_postgresql(config).await,
            StorageEngineType::MySQL => Self::init_mysql(config).await,
            StorageEngineType::TiKV => Self::init_tikv(config).await,
        }
    }

    fn init_hybrid(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing Hybrid engine");
        Ok(Arc::new(InMemoryGraphStorage::new(config)))
    }

    fn init_inmemory(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing InMemory engine");
        Ok(Arc::new(InMemoryGraphStorage::new(config)))
    }

    async fn cleanup_legacy_sled_directories(parent_dir: &Path, current_port: u16) {
        info!("Checking for legacy Sled directories in {:?}", parent_dir);
        println!("===> CHECKING FOR LEGACY SLED DIRECTORIES IN {:?}", parent_dir);

        match fs::read_dir(parent_dir).await {
            Ok(mut entries) => {
                let mut errors = Vec::new();
                while let Ok(Some(entry)) = entries.next_entry().await {
                    let path = entry.path();
                    if path.is_dir() {
                        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            if name.starts_with("sled_") && name != "sled" {
                                if let Some(suffix) = name.strip_prefix("sled_") {
                                    if let Ok(port) = suffix.parse::<u16>() {
                                        if port == current_port {
                                            info!("Skipping active directory for port {}: {:?}", port, path);
                                            println!("===> SKIPPING ACTIVE DIRECTORY FOR PORT {}: {:?}", port, path);
                                            continue;
                                        }
                                        info!("Found legacy Sled directory for port {}: {:?}", port, path);
                                        println!("===> FOUND LEGACY SLED DIRECTORY FOR PORT {}: {:?}", port, path);

                                        // Clean up lock file
                                        let lock_file = path.join("db.lck");
                                        if lock_file.exists() {
                                            match fs::remove_file(&lock_file).await {
                                                Ok(_) => {
                                                    info!("Successfully removed stale lock file at {:?}", lock_file);
                                                    println!("===> SUCCESSFULLY REMOVED STALE LOCK FILE AT {:?}", lock_file);
                                                }
                                                Err(e) => {
                                                    warn!("Failed to remove lock file at {:?}: {}", lock_file, e);
                                                    println!("===> WARNING: FAILED TO REMOVE LOCK FILE AT {:?}: {}", lock_file, e);
                                                    errors.push(format!("Failed to remove lock file at {:?}: {}", lock_file, e));
                                                }
                                            }
                                        }

                                        // Clean up invalid /db subdirectory or file
                                        let invalid_db_path = path.join("db");
                                        if invalid_db_path.exists() {
                                            if invalid_db_path.is_dir() {
                                                match fs::remove_dir_all(&invalid_db_path).await {
                                                    Ok(_) => {
                                                        info!("Removed invalid /db directory at {:?}", invalid_db_path);
                                                        println!("===> REMOVED INVALID /db DIRECTORY AT {:?}", invalid_db_path);
                                                    }
                                                    Err(e) => {
                                                        warn!("Failed to remove invalid /db directory at {:?}: {}", invalid_db_path, e);
                                                        println!("===> WARNING: FAILED TO REMOVE INVALID /db DIRECTORY AT {:?}: {}", invalid_db_path, e);
                                                        errors.push(format!("Failed to remove invalid /db directory at {:?}: {}", invalid_db_path, e));
                                                    }
                                                }
                                            } else {
                                                match fs::remove_file(&invalid_db_path).await {
                                                    Ok(_) => {
                                                        info!("Removed invalid /db file at {:?}", invalid_db_path);
                                                        println!("===> REMOVED INVALID /db FILE AT {:?}", invalid_db_path);
                                                    }
                                                    Err(e) => {
                                                        warn!("Failed to remove invalid /db file at {:?}: {}", invalid_db_path, e);
                                                        println!("===> WARNING: FAILED TO REMOVE INVALID /db FILE AT {:?}: {}", invalid_db_path, e);
                                                        errors.push(format!("Failed to remove invalid /db file at {:?}: {}", invalid_db_path, e));
                                                    }
                                                }
                                            }
                                        }

                                        // Remove the legacy directory
                                        match fs::remove_dir_all(&path).await {
                                            Ok(_) => {
                                                info!("Successfully cleaned up legacy Sled directory: {:?}", path);
                                                println!("===> SUCCESSFULLY CLEANED UP LEGACY SLED DIRECTORY: {:?}", path);
                                            }
                                            Err(e) => {
                                                warn!("Failed to clean up legacy Sled directory {:?}: {}", path, e);
                                                println!("===> WARNING: FAILED TO CLEAN UP LEGACY SLED DIRECTORY {:?}: {}", path, e);
                                                errors.push(format!("Failed to clean up legacy Sled directory {:?}: {}", path, e));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if !errors.is_empty() {
                    warn!("Encountered errors during legacy directory cleanup: {:?}", errors);
                    println!("===> WARNING: ENCOUNTERED ERRORS DURING LEGACY DIRECTORY CLEANUP: {:?}", errors);
                }
            }
            Err(e) => {
                warn!("Failed to read directory {:?}: {}", parent_dir, e);
                println!("===> WARNING: FAILED TO READ DIRECTORY {:?}: {}", parent_dir, e);
            }
        }
    }

    #[cfg(feature = "with-tikv")]
    pub async fn init_tikv(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing TiKV engine");
        
        let mut tikv_singleton = TIKV_SINGLETON.lock().await;
        
        if let Some(engine) = &*tikv_singleton {
            info!("TiKV storage engine already initialized, reusing existing instance.");
            return Ok(Arc::clone(engine) as Arc<dyn GraphStorageEngine + Send + Sync>);
        }

        // Extract configuration directly from engine_specific_config
        let engine_specific = config.engine_specific_config.as_ref().ok_or_else(|| {
            error!("Missing engine_specific_config for TiKV");
            GraphError::ConfigurationError("Missing engine_specific_config for TiKV".to_string())
        })?;

        let tikv_config = TikvConfig {
            storage_engine_type: StorageEngineType::TiKV,
            path: engine_specific.storage.path
                .clone()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/tikv")),
            host: engine_specific.storage.host.clone(),
            port: engine_specific.storage.port,
            pd_endpoints: engine_specific.storage.pd_endpoints
                .clone()
                .or_else(|| {
                    warn!("pd_endpoints missing in TiKV configuration, using default: 127.0.0.1:2379");
                    Some("127.0.0.1:2379".to_string())
                }),
            username: engine_specific.storage.username.clone(),
            password: engine_specific.storage.password.clone(),
        };

        debug!("TiKV config: {:?}", tikv_config);

        // Validate pd_endpoints
        if tikv_config.pd_endpoints.is_none() || tikv_config.pd_endpoints.as_ref().map_or(true, |s| s.is_empty()) {
            error!("Missing or empty pd_endpoints in TiKV configuration");
            return Err(GraphError::ConfigurationError("Missing or empty pd_endpoints in TiKV configuration".to_string()));
        }

        let engine = TikvStorage::new(&tikv_config).await
            .map_err(|e| {
                error!("Failed to initialize TiKV: {}", e);
                GraphError::StorageError(format!("Failed to initialize TiKV: {}", e))
            })?;

        let arc_engine = Arc::new(engine);
        *tikv_singleton = Some(arc_engine.clone());
        
        info!("TiKV storage engine initialized successfully");
        Ok(arc_engine as Arc<dyn GraphStorageEngine + Send + Sync>)
    }

    #[cfg(feature = "with-rocksdb")]
    async fn init_rocksdb(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        use tokio::time::{self, Duration as TokioDuration};
        println!("IT'S TIME TO INITIALIZE ROCKSDB");
        info!("Initializing RocksDB engine with SurrealDB backend");

        // Get the path for the RocksDB engine
        let engine_path = Self::get_engine_path(config, StorageEngineType::RocksDB)?;
        
        // Ensure the directory exists to avoid filesystem errors
        info!("Ensuring directory exists: {:?}", engine_path);
        Self::ensure_directory_exists(&engine_path).await?;

        // Check and release any existing locks
        if engine_path.exists() {
            if let Err(e) = SurrealdbGraphStorage::force_unlock(&engine_path).await {
                return Err(GraphError::StorageError(format!("Failed to unlock RocksDB database at {:?}: {}", engine_path, e)));
            }
            info!("Successfully unlocked RocksDB database at {:?}", engine_path);
            println!("===> SUCCESSFULLY UNLOCKED ROCKSDB DATABASE AT {:?}", engine_path);
            // Verify no lock file exists
            let lock_file = engine_path.join("LOCK");
            if lock_file.exists() {
                return Err(GraphError::StorageError(format!("Lock file still exists at {:?} after unlock attempt", lock_file)));
            }
            println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
        }

        let mut db_instance = None;
        let mut attempt = 0;
        let max_attempts = 5;
        let base_delay_ms = 100;

        while attempt < max_attempts {
            debug!("Attempting to connect to SurrealDB RocksDB backend... (Attempt {} of {})", attempt + 1, max_attempts);
            
            match Surreal::new::<RocksDb>(engine_path.clone()).await {
                Ok(db) => {
                    info!("Successfully connected to SurrealDB RocksDB backend on attempt {}.", attempt + 1);
                    db_instance = Some(db);
                    break; // Connection successful, exit the loop
                },
                Err(e) => {
                    let error_string = e.to_string();
                    
                    // Check for lock-related errors
                    if error_string.contains("lock") || error_string.contains("occupied") {
                        warn!("Detected a potential RocksDB lock conflict. Attempting retry after a delay. Error: {}", error_string);
                        let delay = TokioDuration::from_millis(base_delay_ms * 2u64.pow(attempt));
                        info!("Retrying in {:?}...", delay);
                        time::sleep(delay).await;
                        attempt += 1;
                    } else {
                        error!("A non-retryable error occurred while connecting to RocksDB: {}", error_string);
                        return Err(GraphError::StorageError(format!("Failed to connect to SurrealDB RocksDB backend: {}", error_string)));
                    }
                }
            }
        }

        // After the loop, check if we have a valid database instance
        let db = db_instance.ok_or_else(|| {
            error!("Failed to connect to RocksDB after {} attempts.", max_attempts);
            GraphError::StorageError(format!("Failed to open RocksDB after {} attempts due to lock error.", max_attempts))
        })?;
        
        db.use_ns("graphdb").use_db("graph").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        println!("SUCCESSFULLY INITIALIZED ROCKSDB");
        Ok(Arc::new(SurrealdbGraphStorage { db, backend_type: StorageEngineType::RocksDB }))
    }

    #[cfg(not(feature = "redis-datastore"))]
    async fn init_redis(_config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        Err(GraphError::StorageError(
            "Redis support is not enabled. Please enable the 'redis-datastore' feature.".to_string()
        ))
    }

    #[cfg(feature = "postgres-datastore")]
    async fn init_postgresql(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        let postgres_config = Self::build_postgresql_config(config)?;
        
        let postgres_instance = POSTGRES_SINGLETON.get_or_init(|| async {
            trace!("Creating new PostgresStorage singleton");
            let storage = PostgresStorage::new(&postgres_config).await
                .expect("Failed to initialize PostgreSQL singleton");
            Arc::new(storage)
        }).await;
        
        Ok(postgres_instance.clone())
    }

    #[cfg(not(feature = "postgres-datastore"))]
    async fn init_postgresql(_config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        Err(GraphError::StorageError(
            "PostgreSQL support is not enabled. Please enable the 'postgres-datastore' feature.".to_string()
        ))
    }

    #[cfg(feature = "mysql-datastore")]
    async fn init_mysql(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        let mysql_config = Self::build_mysql_config(config)?;
        
        let mysql_instance = MYSQL_SINGLETON.get_or_init(|| async {
            trace!("Creating new MySQLStorage singleton");
            let storage = MySQLStorage::new(&mysql_config).await
                .expect("Failed to initialize MySQL singleton");
            Arc::new(storage)
        }).await;
        
        Ok(mysql_instance.clone())
    }

    #[cfg(not(feature = "mysql-datastore"))]
    async fn init_mysql(_config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        Err(GraphError::StorageError(
            "MySQL support is not enabled. Please enable the 'mysql-datastore' feature.".to_string()
        ))
    }

    // Helper methods for configuration building
    fn get_engine_path(config: &StorageConfig, engine_type: StorageEngineType) -> Result<PathBuf, GraphError> {
        match engine_type {
            StorageEngineType::Sled => {
                Ok(config.engine_specific_config
                    .as_ref()
                    .and_then(|selected_config| selected_config.storage.path.clone())
                    .unwrap_or_else(|| {
                        let path = PathBuf::from(format!("{}/sled", DEFAULT_DATA_DIRECTORY));
                        warn!("No path specified for Sled, using default: {:?}", path);
                        path
                    }))
            }
            StorageEngineType::RocksDB => {
                config.engine_specific_config
                    .as_ref()
                    .and_then(|selected_config| selected_config.storage.path.clone())
                    .ok_or_else(|| {
                        error!("RocksDB path is missing in engine_specific_config: {:?}", config.engine_specific_config);
                        GraphError::ConfigurationError(
                            "RocksDB path is missing in engine_specific_config".to_string()
                        )
                    })
            }
            _ => {
                config.data_directory.clone()
                    .ok_or_else(|| {
                        error!("No data_directory specified for engine type: {:?}", engine_type);
                        GraphError::ConfigurationError(
                            "No data_directory specified in config".to_string()
                        )
                    })
            }
        }
    }

    #[cfg(feature = "with-sled")]
    fn build_sled_config(config: &StorageConfig, path: PathBuf) -> Result<SledConfig, GraphError> {
        let engine_config = config.engine_specific_config.as_ref().ok_or_else(|| {
            GraphError::ConfigurationError("Missing engine_specific_config".to_string())
        })?;
        Ok(SledConfig {
            storage_engine_type: StorageEngineType::Sled,
            path,
            host: Some(get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, config.default_port)),
            // Note: temporary, use_compression, and cache_capacity are not in StorageConfigInner.
            // Using SledConfig defaults as they are defined in the struct.
            temporary: false,
            use_compression: true,
            cache_capacity: None,
        })
    }

    #[cfg(feature = "with-rocksdb")]
    fn build_rocksdb_config(config: &StorageConfig, path: PathBuf) -> Result<RocksdbConfig, GraphError> {
        Ok(RocksdbConfig {
            storage_engine_type: StorageEngineType::RocksDB,
            path,
            host: Some(get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, config.default_port)),
        })
    }

    #[cfg(feature = "redis-datastore")]
    fn build_redis_config(config: &StorageConfig) -> Result<RedisConfig, GraphError> {
        let _config_map = config.engine_specific_config.as_ref()
            .ok_or_else(|| GraphError::ConfigurationError("Redis config is missing".to_string()))?;

        Ok(RedisConfig {
            storage_engine_type: StorageEngineType::Redis,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, 6379)),
            database: Some(Self::get_config_value(config, "database", "0")),
            username: None,
            password: None,
        })
    }

    #[cfg(feature = "postgres-datastore")]
    fn build_postgresql_config(config: &StorageConfig) -> Result<PostgreSQLConfig, GraphError> {
        let _config_map = config.engine_specific_config.as_ref()
            .ok_or_else(|| GraphError::ConfigurationError("PostgreSQL config is missing".to_string()))?;

        Ok(PostgreSQLConfig {
            storage_engine_type: StorageEngineType::PostgreSQL,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, 5432)),
            username: Some(Self::get_config_value(config, "username", "graphdb_user")),
            password: Some(Self::get_config_value(config, "password", "secure_password")),
            database: Some(Self::get_config_value(config, "database", "graphdb")),
        })
    }

    #[cfg(feature = "mysql-datastore")]
    fn build_mysql_config(config: &StorageConfig) -> Result<MySQLConfig, GraphError> {
        let _config_map = config.engine_specific_config.as_ref()
            .ok_or_else(|| GraphError::ConfigurationError("MySQL config is missing".to_string()))?;

        Ok(MySQLConfig {
            storage_engine_type: StorageEngineType::MySQL,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, 3306)),
            username: Some(Self::get_config_value(config, "username", "graphdb_user")),
            password: Some(Self::get_config_value(config, "password", "secure_password")),
            database: Some(Self::get_config_value(config, "database", "graphdb")),
        })
    }

    // Utility helper methods
    fn get_config_port(config: &StorageConfig, default: u16) -> u16 {
        config.engine_specific_config
            .as_ref()
            .and_then(|selected_config| selected_config.storage.port)
            .unwrap_or(default)
    }

    async fn ensure_directory_exists(path: &PathBuf) -> Result<(), GraphError> {
        if !path.exists() {
            debug!("Creating data directory at {:?}", path);
            tokio::fs::create_dir_all(path).await.map_err(|e| {
                error!("Failed to create data directory at {:?}: {}", path, e);
                GraphError::Io(e)
            })?;
        }
        Ok(())
    }

    pub async fn create_manager(
        engine_type: StorageEngineType,
        config: StorageConfig,
        config_path_yaml: &Path,
        persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
        permanent: bool,
        raft_instances: Arc<TokioMutex<HashMap<u64, Arc<dyn Any + Send + Sync>>>>,
    ) -> Result<StorageEngineManager, GraphError> {
        let engine = Arc::new(TokioMutex::new(HybridStorage {
            inmemory: Arc::new(InMemoryGraphStorage::new(&config)),
            persistent: persistent.clone(),
            running: Arc::new(TokioMutex::new(false)),
            engine_type,
        }));

        let manager = StorageEngineManager {
            engine,
            persistent_engine: persistent,
            session_engine_type: if permanent { None } else { Some(engine_type) },
            config,
            config_path: config_path_yaml.to_path_buf(),
            raft_instances,
        };
        Ok(manager)
    }

    // Missing Sled-specific helper methods
    #[cfg(feature = "with-sled")]
    async fn handle_sled_lock_file(engine_path: &PathBuf) -> Result<(), GraphError> {
        let sled_lock_path = engine_path.join("db.lck");
        
        // Fix: Remove the borrow and use the `?` operator to handle the `Result`.
        if lock_file_exists(sled_lock_path.clone()).await? {
            warn!("Lock file exists before Sled initialization: {:?}", sled_lock_path);
            Self::log_lock_file_diagnostics(&sled_lock_path).await;
            
            // Fix: Remove the borrow and use the `?` operator.
            recover_sled(sled_lock_path).await?;
        } else {
            debug!("No lock file found for Sled at {:?}", sled_lock_path);
        }
        Ok(())
    }

    #[cfg(feature = "with-sled")]
    async fn handle_sled_retry_error(sled_lock_path: &PathBuf, sled_path: &PathBuf, attempt: u32) {
        // Check if the lock file exists, handling the Result and cloning the path.
        if let Ok(true) = lock_file_exists(sled_lock_path.clone()).await {
            warn!("Lock file still exists after retry {}: {:?}", attempt, sled_lock_path);
            
            // Assuming this function takes a reference.
            Self::log_lock_file_diagnostics(sled_lock_path).await;
            
            // Try to recover the lock file, also cloning the path to satisfy ownership.
            if let Err(e) = recover_sled(sled_lock_path.clone()).await {
                warn!("Failed to recover Sled lock file on retry {}: {}", attempt, e);
            }
        } else {
            warn!("No lock file found on retry {}, but Sled initialization still failed", attempt);
        }
    }

    #[cfg(feature = "with-sled")]
    async fn log_final_sled_error(sled_lock_path: &PathBuf) {
        // Log the initial error message.
        error!("Failed to initialize Sled after all retries");

        // Use `if let` to handle the `Result` returned by `lock_file_exists`.
        // We clone the `sled_lock_path` to satisfy the function's ownership requirement.
        if let Ok(true) = lock_file_exists(sled_lock_path.clone()).await {
            error!("Lock file still exists after all retries: {:?}", sled_lock_path);
            
            // This function call is also fixed to pass a PathBuf by reference
            // if its signature expects that.
            // Assuming Self::log_lock_file_diagnostics takes a reference.
            Self::log_lock_file_diagnostics(sled_lock_path).await;
        }
    }


    #[cfg(feature = "with-sled")]
    pub async fn log_lock_file_diagnostics(lock_path: &PathBuf) {
        match fs::metadata(lock_path).await {
            Ok(metadata) => {
                debug!("Lock file diagnostics for {:?}:", lock_path);
                debug!("  Size: {} bytes", metadata.len());
                debug!("  Modified: {:?}", metadata.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH));
                debug!("  Read-only: {}", metadata.permissions().readonly());
            }
            Err(e) => {
                warn!("Failed to get lock file metadata for {:?}: {}", lock_path, e);
            }
        }
    }

    /// Validates the storage configuration for the specified engine type
    async fn validate_config(config: &StorageConfig, engine_type: StorageEngineType) -> Result<(), GraphError> {
        use tokio::fs;
        use anyhow::Context;

        info!("Validating config for engine type: {:?}", engine_type);
        debug!("Full config: {:?}", config);
        
        match engine_type {
            StorageEngineType::Sled | StorageEngineType::RocksDB | StorageEngineType::TiKV | StorageEngineType::Hybrid => {
                let path = config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("engine_specific_config for {:?}: {:?}", engine_type, map);
                        map.storage.path.clone().map(PathBuf::from)
                    })
                    .unwrap_or_else(|| {
                        let default_path = match engine_type {
                            StorageEngineType::Sled => PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY)),
                            StorageEngineType::RocksDB => PathBuf::from(&format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY)),
                            _ => config.data_directory
                                .clone()
                                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
                        };
                        warn!("No path specified in engine_specific_config, using default: {:?}", default_path);
                        default_path
                    });
                
                info!("Validating path for {:?}: {:?}", engine_type, path);
                
                if !path.exists() {
                    info!("Creating path: {:?}", path);
                    fs::create_dir_all(&path)
                        .await
                        .map_err(|e| GraphError::Io(e))
                        .with_context(|| format!("Failed to create engine-specific path: {:?}", path))?;
                }
                
                if !path.is_dir() {
                    return Err(GraphError::ConfigurationError(format!(
                        "Path for {:?} is not a directory: {:?}", engine_type, path
                    )));
                }
                
                // Test write permissions
                let test_file = path.join(".write_test");
                fs::write(&test_file, "")
                    .await
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("No write permissions for engine-specific path: {:?}", path))?;
                
                fs::remove_file(&test_file)
                    .await
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("Failed to remove test file in {:?}", path))?;
            }
            
            StorageEngineType::Redis | StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                let map = config.engine_specific_config
                    .as_ref()
                    .ok_or_else(|| {
                        error!("engine_specific_config is missing for {:?}", engine_type);
                        GraphError::ConfigurationError(format!("Engine-specific config required for {:?}", engine_type))
                    })?;
                
                debug!("engine_specific_config for {:?}: {:?}", engine_type, map);
                
                if map.storage.host.is_none() || map.storage.port.is_none() {
                    return Err(GraphError::ConfigurationError(format!(
                        "Host and port are required for {:?}", engine_type
                    )));
                }
                
                if matches!(engine_type, StorageEngineType::PostgreSQL | StorageEngineType::MySQL) {
                    if map.storage.username.is_none() || map.storage.password.is_none() || map.storage.database.is_none() {
                        return Err(GraphError::ConfigurationError(format!(
                            "Username, password, and database are required for {:?}", engine_type
                        )));
                    }
                }
            }
            
            StorageEngineType::InMemory => {
                info!("No specific validation required for InMemory engine");
            }
        }
        
        info!("Config validation successful for {:?}", engine_type);
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), GraphError> {
        info!("Shutting down StorageEngineManager");
        
        // Stop the hybrid engine if it's running
        {
            let engine = self.engine.lock().await;
            if (*engine).is_running().await {
                info!("Stopping running hybrid engine");
                (*engine).stop().await
                    .map_err(|e| {
                        error!("Failed to stop hybrid engine: {}", e);
                        GraphError::StorageError(format!("Failed to stop hybrid engine: {}", e))
                    })?;
            }
        }
        
        // Close all connections
        self.close_connections().await
            .map_err(|e| {
                error!("Failed to close connections during shutdown: {}", e);
                GraphError::StorageError(format!("Failed to close connections: {}", e))
            })?;
        
        // Ensure RocksDB singleton is closed
        #[cfg(feature = "with-rocksdb")]
        {
            let mut rocksdb_singleton = ROCKSDB_SINGLETON.lock().await;
            if let Some(rocksdb_instance) = rocksdb_singleton.as_ref() {
                rocksdb_instance.close().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB singleton: {}", e)))?;
                info!("RocksDB singleton closed during shutdown");
                *rocksdb_singleton = None;
            }
        }

        info!("StorageEngineManager shutdown completed successfully");
        Ok(())
    }

    pub async fn close_connections(&mut self) -> Result<(), GraphError> {
        let engine_type = self.current_engine_type().await;
        info!("Closing connections for engine type: {:?}", engine_type);

        match engine_type {
            StorageEngineType::Hybrid => {
                if let Some(hybrid_storage) = self.persistent_engine.as_any().downcast_ref::<HybridStorage>() {
                    hybrid_storage.close().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to close Hybrid storage: {}", e)))?;
                    info!("Hybrid storage connections closed");
                } else {
                    warn!("Failed to downcast persistent_engine to HybridStorage");
                }
            }
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    if let Some(sled_storage) = self.persistent_engine.as_any().downcast_ref::<SledStorage>() {
                        sled_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close Sled database: {}", e)))?;
                        info!("Sled database connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to SledStorage");
                    }
                }
                #[cfg(not(feature = "with-sled"))]
                {
                    warn!("Sled support is not enabled, skipping close");
                }
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    if let Some(rocksdb_storage) = self.persistent_engine.as_any().downcast_ref::<RocksdbStorage>() {
                        rocksdb_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB database: {}", e)))?;
                        info!("RocksDB database connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to RocksdbStorage");
                    }
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    warn!("RocksDB support is not enabled, skipping close");
                }
            }
            StorageEngineType::TiKV => {
                #[cfg(feature = "with-tikv")]
                {
                    if let Some(tikv_storage) = self.persistent_engine.as_any().downcast_ref::<TikvStorage>() {
                        tikv_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close TiKV storage: {}", e)))?;
                        info!("TiKV storage connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to TikvStorage");
                    }
                }
                #[cfg(not(feature = "with-tikv"))]
                {
                    warn!("TiKV support is not enabled, skipping close");
                }
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    if let Some(redis_storage) = self.persistent_engine.as_any().downcast_ref::<RedisStorage>() {
                        redis_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close Redis connection: {}", e)))?;
                        info!("Redis connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to RedisStorage");
                    }
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    warn!("Redis support is not enabled, skipping close");
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    if let Some(postgres_storage) = self.persistent_engine.as_any().downcast_ref::<PostgresStorage>() {
                        postgres_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close PostgreSQL connection: {}", e)))?;
                        info!("PostgreSQL connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to PostgresStorage");
                    }
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    warn!("PostgreSQL support is not enabled, skipping close");
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    if let Some(mysql_storage) = self.persistent_engine.as_any().downcast_ref::<MySQLStorage>() {
                        mysql_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close MySQL connection: {}", e)))?;
                        info!("MySQL connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to MySQLStorage");
                    }
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    warn!("MySQL support is not enabled, skipping close");
                }
            }
            StorageEngineType::InMemory => {
                if let Some(inmemory_storage) = self.persistent_engine.as_any().downcast_ref::<InMemoryGraphStorage>() {
                    inmemory_storage.close().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to close InMemory storage: {}", e)))?;
                    info!("InMemory storage flushed");
                } else {
                    warn!("Failed to downcast persistent_engine to InMemoryGraphStorage");
                }
            }
        }

        let engine = self.engine.lock().await;
        (*engine).close().await
            .map_err(|e| GraphError::StorageError(format!("Failed to close HybridStorage: {}", e)))?;
        Ok(())
    }

    /// Resets the StorageEngineManager
    pub async fn reset(&mut self) -> Result<(), GraphError> {
        info!("Resetting StorageEngineManager");
        let engine = self.engine.lock().await;
        if (*engine).is_running().await {
            (*engine).stop().await?;
        }
        let engine_type = engine.engine_type.clone(); // Extract engine_type before dropping the lock
        drop(engine); // Release the lock
        
        // Extract port from config or use default
        let port = self.config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.port)
            .unwrap_or_else(|| match engine_type {
                StorageEngineType::TiKV => 2380,
                _ => 8052,
            });
        
        // Normalize Sled path to remove port suffixes
        if engine_type == StorageEngineType::Sled {
            if let Some(ref mut engine_config) = self.config.engine_specific_config {
                let base_data_dir = self.config.data_directory
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
                
                // Use clean path without port suffix
                let normalized_path = base_data_dir.join("sled");
                
                // Check if path needs updating (remove port suffixes)
                let needs_update = engine_config.storage.path.as_ref()
                    .map(|current_path| current_path != &normalized_path)
                    .unwrap_or(true);
                    
                if needs_update {
                    info!("Normalizing Sled path during reset from {:?} to {:?}", 
                          engine_config.storage.path, normalized_path);
                    
                    // Clean up any legacy port-suffixed directories
                    Self::cleanup_legacy_sled_directories_during_reset(&base_data_dir, port).await;
                    
                    engine_config.storage.path = Some(normalized_path);
                    engine_config.storage.port = Some(port);
                    
                    self.config.save().await
                        .map_err(|e| GraphError::ConfigurationError(
                            format!("Failed to save updated StorageConfig with normalized Sled path during reset: {}", e)
                        ))?;
                        
                    info!("Updated and saved config with normalized Sled path during reset");
                }
            }
        }
        
        let new_manager = StorageEngineManager::new(engine_type, &self.config_path, self.session_engine_type.is_none(), Some(port)).await
            .map_err(|e| {
                error!("Failed to create new StorageEngineManager: {}", e);
                GraphError::StorageError(format!("Failed to reset StorageEngineManager: {}", e))
            })?;
        
        self.engine = new_manager.engine.clone();
        self.persistent_engine = new_manager.persistent_engine.clone();
        self.session_engine_type = new_manager.session_engine_type;
        self.config = new_manager.config.clone();
        
        info!("StorageEngineManager reset completed with engine: {:?} on port {:?}", engine_type, port);
        Ok(())
    }

    pub fn get_persistent_engine(&self) -> Arc<dyn GraphStorageEngine + Send + Sync> {
        Arc::clone(&self.persistent_engine)
    }

    pub async fn get_runtime_config(&self) -> Result<StorageConfig, GraphError> {
        let mut config = self.config.clone();
        if let Some(session_engine) = self.session_engine_type {
            config.storage_engine_type = session_engine;
        }
        Ok(config)
    }

    pub fn get_current_engine_type(&self) -> StorageEngineType {
        tokio::runtime::Runtime::new()
            .expect("Failed to create Tokio runtime")
            .block_on(self.current_engine_type())
    }

    pub async fn current_engine_type(&self) -> StorageEngineType {
        if let Some(engine_type) = &self.session_engine_type {
            engine_type.clone()
        } else {
            let engine = self.engine.lock().await;
            (*engine).engine_type.clone()
        }
    }

    pub async fn get_current_engine_data_path(&self) -> Option<PathBuf> {
        self.current_engine_data_path().await
    }

    pub async fn current_engine_data_path(&self) -> Option<PathBuf> {
        let engine_type = self.current_engine_type().await;
        match engine_type {
            StorageEngineType::Sled => {
                Some(self.config.engine_specific_config
                    .as_ref()
                    .and_then(|map| map.storage.path.clone().map(PathBuf::from))
                    .unwrap_or_else(|| PathBuf::from("./storage_daemon_server/data/sled")))
            }
            StorageEngineType::RocksDB => {
                Some(self.config.engine_specific_config
                    .as_ref()
                    .and_then(|map| map.storage.path.clone().map(PathBuf::from))
                    .unwrap_or_else(|| PathBuf::from("./storage_daemon_server/data/rocksdb")))
            }
            _ => self.config.data_directory.clone(),
        }
    }

    fn get_engine_config_path(&self, engine_type: StorageEngineType) -> PathBuf {
        let parent = self.config_path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
        match engine_type {
            StorageEngineType::Hybrid => parent.join("storage_config_hybrid.yaml"),
            StorageEngineType::Sled => parent.join("storage_config_sled.yaml"),
            StorageEngineType::RocksDB => parent.join("storage_config_rocksdb.yaml"),
            StorageEngineType::InMemory => parent.join("storage_config_inmemory.yaml"),
            StorageEngineType::Redis => parent.join("storage_config_redis.yaml"),
            StorageEngineType::PostgreSQL => parent.join("storage_config_postgres.yaml"),
            StorageEngineType::MySQL => parent.join("storage_config_mysql.yaml"),
            StorageEngineType::TiKV => parent.join("storage_config_tykv.yaml"),

        }
    }

    async fn migrate_data(&self, old_engine: &Arc<dyn GraphStorageEngine + Send + Sync>, new_engine: &Arc<dyn GraphStorageEngine + Send + Sync>) -> Result<(), GraphError> {
        info!("Migrating data from {} to {}", old_engine.get_type(), new_engine.get_type());
        let start_time = Instant::now();

        let vertices = old_engine.get_all_vertices().await?;
        for vertex in vertices {
            new_engine.create_vertex(vertex).await?;
        }

        let edges = old_engine.get_all_edges().await?;
        for edge in edges {
            new_engine.create_edge(edge).await?;
        }

        info!("Data migration completed in {}ms", start_time.elapsed().as_millis());
        Ok(())
    }

    pub fn available_engines() -> Vec<StorageEngineType> {
        let mut engines = vec![StorageEngineType::InMemory];
        #[cfg(feature = "with-sled")]
        engines.push(StorageEngineType::Sled);
        #[cfg(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv"))]
        engines.push(StorageEngineType::Hybrid);
        #[cfg(feature = "with-rocksdb")]
        engines.push(StorageEngineType::RocksDB);
        #[cfg(feature = "with-tikv")]
        engines.push(StorageEngineType::TiKV);
        #[cfg(feature = "redis-datastore")]
        engines.push(StorageEngineType::Redis);
        #[cfg(feature = "postgres-datastore")]
        engines.push(StorageEngineType::PostgreSQL);
        #[cfg(feature = "mysql-datastore")]
        engines.push(StorageEngineType::MySQL);
        engines
    }

    pub async fn use_storage(&mut self, new_config: StorageConfig, permanent: bool) -> Result<(), GraphError> {
        info!("=== Starting use_storage for engine: {:?}, permanent: {} ===", new_config.storage_engine_type, permanent);
        trace!("use_storage called with engine_type: {:?}", new_config.storage_engine_type);
        let start_time = Instant::now();
        println!("===> USE STORAGE HANDLER - STEP 1");

        // Check if requested engine is available
        let available_engines = Self::available_engines();
        trace!("Available engines: {:?}", available_engines);
        if !available_engines.contains(&new_config.storage_engine_type) {
            error!("Storage engine {:?} is not supported in this build. Available: {:?}", new_config.storage_engine_type, available_engines);
            return Err(GraphError::InvalidStorageEngine(format!(
                "Storage engine {:?} is not supported. Available engines: {:?}", new_config.storage_engine_type, available_engines
            )));
        }

        // Get old state
        let (was_running, old_persistent_arc, old_engine_type) = {
            let engine_guard = self.engine.lock().await;
            let was_running = (*engine_guard).is_running().await;
            trace!("Current engine state - running: {}, type: {:?}", was_running, (*engine_guard).engine_type);
            debug!("Current engine: {:?}", (*engine_guard).engine_type);
            (was_running, Arc::clone(&engine_guard.persistent), (*engine_guard).engine_type)
        };

        // Get paths from config
        let old_path = new_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.path.clone())
            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data"));
        let new_path = new_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.path.clone())
            .unwrap_or_else(|| PathBuf::from(format!("/opt/graphdb/storage_data/{}", new_config.storage_engine_type.to_string().to_lowercase())));

        // Get port
        let port = new_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.port)
            .unwrap_or(new_config.default_port);

        // Check for running daemon on the port
        let daemon_running = match find_pid_by_port(port).await {
            Ok(Some(pid)) => NonBlockingDaemonRegistry::is_pid_running(pid).await.unwrap_or(false),
            _ => false,
        };

        // If daemon is running and engine/path are the same, update metadata and reuse
        if daemon_running && old_engine_type == new_config.storage_engine_type && old_path == new_path {
            info!("Valid daemon running on port {}, same engine and path, updating metadata.", port);
            let meta = DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: find_pid_by_port(port).await?.unwrap_or(std::process::id()),
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(new_path.clone()),
                config_path: Some(self.config_path.clone()),
                engine_type: Some(new_config.storage_engine_type.to_string()),
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| GraphError::StorageError(format!("System time error: {}", e)))?
                    .as_nanos() as i64,
            };
            GLOBAL_DAEMON_REGISTRY.register_daemon(meta).await
                .map_err(|e| GraphError::StorageError(format!("Failed to update daemon metadata: {}", e)))?;
            info!("Metadata updated for daemon on port {}.", port);
            return Ok(());
        }

        // Skip if no change needed
        if old_engine_type == new_config.storage_engine_type && old_path == new_path && self.session_engine_type.is_none() {
            info!("No switch needed: same engine {:?} and path {:?}", old_engine_type, old_path);
            trace!("Skipping switch: current engine matches requested and no session override. Elapsed: {}ms", start_time.elapsed().as_millis());
            return Ok(());
        }

        // Stop and close the current engine if running, unless Sled-to-Sled with same path
        if was_running && !(old_engine_type == StorageEngineType::Sled && new_config.storage_engine_type == StorageEngineType::Sled && old_path == new_path) {
            info!("Stopping current engine {:?} before switch", old_engine_type);
            let engine = self.engine.lock().await;
            (*engine).stop().await
                .map_err(|e| GraphError::StorageError(format!("Failed to stop current engine: {}", e)))?;
        }
        if !(old_engine_type == StorageEngineType::Sled && new_config.storage_engine_type == StorageEngineType::Sled && old_path == new_path) {
            info!("Closing old persistent engine to release resources");
            old_persistent_arc.close().await
                .map_err(|e| GraphError::StorageError(format!("Failed to close old persistent engine: {}", e)))?;
        }

        // Engine-specific cleanup, skip if daemon is running
        if !daemon_running && !(old_engine_type == StorageEngineType::Sled && new_config.storage_engine_type == StorageEngineType::Sled && old_path == new_path) {
            match old_engine_type {
                StorageEngineType::RocksDB => {
                    #[cfg(feature = "with-rocksdb")]
                    {
                        let mut singleton = ROCKSDB_SINGLETON.lock().await;
                        if let Some(rocksdb_instance) = singleton.as_ref() {
                            info!("Closing existing RocksDB instance before switching");
                            rocksdb_instance.close().await
                                .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB: {}", e)))?;
                            *singleton = None;
                        }
                        if old_path.exists() {
                            warn!("Cleaning up RocksDB directory at {:?}", old_path);
                            if let Err(e) = recover_rocksdb(&old_path).await {
                                warn!("Failed to clean RocksDB locks: {}", e);
                            } else {
                                info!("Successfully cleaned RocksDB locks at {:?}", old_path);
                            }
                        }
                    }
                }
                StorageEngineType::Sled => {
                    #[cfg(feature = "with-sled")]
                    {
                        let mut singleton = SLED_SINGLETON.lock().await;
                        if let Some(sled_instance) = singleton.as_ref() {
                            info!("Closing existing Sled instance before switching");
                            sled_instance.close().await
                                .map_err(|e| GraphError::StorageError(format!("Failed to close Sled: {}", e)))?;
                            *singleton = None;
                        }
                        if old_path.exists() {
                            warn!("Cleaning up Sled locks at {:?}", old_path);
                            let lock_path = old_path.join("db.lck");
                            if lock_path.exists() {
                                if let Err(e) = fs::remove_file(&lock_path).await {
                                    warn!("Failed to remove Sled lock file at {:?}: {}", lock_path, e);
                                } else {
                                    info!("Successfully removed Sled lock file at {:?}", lock_path);
                                }
                            }
                        }
                    }
                }
                StorageEngineType::TiKV => {
                    #[cfg(feature = "with-tikv")]
                    {
                        let mut singleton = TIKV_SINGLETON.lock().await;
                        if let Some(tikv_instance) = singleton.as_ref() {
                            info!("Closing existing TiKV instance before switching");
                            tikv_instance.close().await
                                .map_err(|e| GraphError::StorageError(format!("Failed to close TiKV: {}", e)))?;
                            *singleton = None;
                        }
                    }
                }
                StorageEngineType::Redis => {
                    #[cfg(feature = "redis-datastore")]
                    {
                        let redis_instance = REDIS_SINGLETON.get().await;
                        if let Ok(redis_instance) = redis_instance {
                            info!("Closing existing Redis instance before switching");
                            redis_instance.close().await
                                .map_err(|e| GraphError::StorageError(format!("Failed to close Redis: {}", e)))?;
                        }
                    }
                }
                StorageEngineType::PostgreSQL => {
                    #[cfg(feature = "postgres-datastore")]
                    {
                        let postgres_instance = POSTGRES_SINGLETON.get().await;
                        if let Ok(postgres_instance) = postgres_instance {
                            info!("Closing existing PostgreSQL instance before switching");
                            postgres_instance.close().await
                                .map_err(|e| GraphError::StorageError(format!("Failed to close PostgreSQL: {}", e)))?;
                        }
                    }
                }
                StorageEngineType::MySQL => {
                    #[cfg(feature = "mysql-datastore")]
                    {
                        let mysql_instance = MYSQL_SINGLETON.get().await;
                        if let Ok(mysql_instance) = mysql_instance {
                            info!("Closing existing MySQL instance before switching");
                            mysql_instance.close().await
                                .map_err(|e| GraphError::StorageError(format!("Failed to close MySQL: {}", e)))?;
                        }
                    }
                }
                _ => {}
            }
        }

        println!("===> USE STORAGE HANDLER - STEP 2: Loading configuration...");

        // Determine config path
        let config_path = self.config_path.clone();
        info!("Using main config path: {:?}", config_path);
        trace!("Resolved config path: {:?}", config_path);

        // Load or create configuration
        let mut loaded_config = if config_path.exists() {
            info!("Loading existing config from {:?}", config_path);
            trace!("Reading config file: {:?}", config_path);
            load_storage_config_from_yaml(Some(config_path.clone())).await
                .map_err(|e| {
                    error!("Failed to deserialize YAML config from {:?}: {}", config_path, e);
                    GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
                })?
        } else {
            warn!("Config file not found at {:?}", config_path);
            create_default_storage_yaml_config(&config_path, new_config.storage_engine_type).await?;
            load_storage_config_from_yaml(Some(config_path.clone())).await
                .map_err(|e| {
                    error!("Failed to load newly created YAML config from {:?}: {}", config_path, e);
                    GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
                })?
        };
        println!("===> Configuration loaded successfully.");
        debug!("Loaded storage config: {:?}", loaded_config);

        // Update configuration with provided new_config
        loaded_config.storage_engine_type = new_config.storage_engine_type;
        loaded_config.engine_specific_config = new_config.engine_specific_config.clone();
        println!("===> USE STORAGE HANDLER - STEP 3: Loading engine-specific configuration...");
        debug!("Using config root directory: {:?}", loaded_config.config_root_directory);
        println!("===> Engine-specific configuration loaded successfully.");

        // Validate new configuration
        Self::validate_config(&loaded_config, new_config.storage_engine_type)
            .await
            .map_err(|e| {
                error!("Configuration validation failed for new engine {:?}: {}", new_config.storage_engine_type, e);
                GraphError::ConfigurationError(format!("Configuration validation failed: {}", e))
            })?;

        // If permanent, save the new configuration
        if permanent {
            info!("Saving new configuration for permanent switch to {:?}", new_config.storage_engine_type);
            loaded_config.save().await
                .map_err(|e| {
                    error!("Failed to save new config to {:?}: {}", config_path, e);
                    GraphError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                })?;
            self.session_engine_type = None;
        } else {
            self.session_engine_type = Some(new_config.storage_engine_type);
        }
        println!("===> USE STORAGE HANDLER - STEP 4: Saving and reloading config");
        debug!("Final loaded_config before saving: {:?}", loaded_config);
        println!("===> Saving configuration to disk...");
        println!("===> Configuration saved successfully.");

        // Stop existing daemon only if engine or path differs
        if daemon_running && !(old_engine_type == new_config.storage_engine_type && old_path == new_path) {
            println!("===> USE STORAGE HANDLER - STEP 5: Managing daemon on port {}...", port);
            let max_attempts = 5;
            let mut attempt = 0;
            while attempt < max_attempts {
                match find_pid_by_port(port).await {
                    Ok(Some(found_pid)) => {
                        debug!("Found PID {} for port {} on attempt {}", found_pid, port, attempt);
                        if NonBlockingDaemonRegistry::is_pid_running(found_pid).await.unwrap_or(false) {
                            info!("Active daemon found on port {}, stopping and updating registry.", port);
                            stop_process(found_pid).await
                                .map_err(|e| GraphError::StorageError(format!("Failed to stop daemon on PID {}: {}", found_pid, e)))?;
                            GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", port).await?;
                            sleep(TokioDuration::from_millis(500)).await;
                        }
                        break;
                    }
                    Ok(None) => {
                        debug!("No process found on port {} on attempt {}", port, attempt);
                        println!("===> No daemon found on port {}.", port);
                        break;
                    }
                    Err(e) => {
                        warn!("Failed to check port {} on attempt {}: {}", port, attempt, e);
                        attempt += 1;
                        sleep(TokioDuration::from_millis(100)).await;
                    }
                }
            }
        }

        // Initialize new engine only if no running daemon or engine/path differs
        let new_persistent = if daemon_running && old_engine_type == new_config.storage_engine_type && old_path == new_path {
            info!("Reusing existing Sled engine for port {}", port);
            #[cfg(feature = "with-sled")]
            {
                let sled_db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Failed to access existing Sled DB".to_string()))?;
                let sled_db_guard = sled_db.lock().await;
                info!("Reusing existing Sled database at {:?}", sled_db_guard.path);
                let storage = SledStorage::new_with_db(
                    &SledConfig {
                        storage_engine_type: StorageEngineType::Sled,
                        path: new_path.clone(),
                        host: new_config.engine_specific_config.as_ref().and_then(|c| c.storage.host.clone()),
                        port: Some(port),
                        temporary: false,
                        use_compression: new_config.engine_specific_config.as_ref().map_or(false, |c| c.storage.use_compression),
                        cache_capacity: new_config.engine_specific_config.as_ref().and_then(|c| c.storage.cache_capacity),
                    },
                    &new_config,
                    Arc::clone(&sled_db_guard.db),
                ).await?;
                Arc::new(storage) as Arc<dyn GraphStorageEngine + Send + Sync>
            }
            #[cfg(not(feature = "with-sled"))]
            return Err(GraphError::StorageError("Sled support is not enabled".to_string()));
        } else {
            async fn retry_init_engine<F, Fut>(init_fn: F, max_retries: u32) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError>
            where
                F: Fn() -> Fut + Send + 'static,
                Fut: futures::Future<Output = Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError>> + Send,
            {
                let mut attempt = 0;
                while attempt < max_retries {
                    match init_fn().await {
                        Ok(engine) => return Ok(engine),
                        Err(e) if e.to_string().contains("WouldBlock") || e.to_string().contains("Resource temporarily unavailable") => {
                            warn!("Lock contention during init (attempt {}/{}), retrying in 1s...", attempt + 1, max_retries);
                            sleep(TokioDuration::from_secs(1)).await;
                            attempt += 1;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(GraphError::StorageError("Max retries exceeded for engine init due to lock contention".to_string()))
            }

            println!("===> USE STORAGE HANDLER - STEP 6: Initializing StorageEngineManager...");
            let config_for_closure = new_config.clone(); // Clone new_config for use in closure
            retry_init_engine(
                move || {
                    let config = config_for_closure.clone();
                    async move {
                        match config.storage_engine_type {
                            StorageEngineType::InMemory => {
                                info!("Initializing InMemory engine");
                                Ok(Arc::new(InMemoryGraphStorage::new(&config)) as Arc<dyn GraphStorageEngine + Send + Sync>)
                            }
                            StorageEngineType::Hybrid => {
                                #[cfg(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv"))]
                                {
                                    let persistent_engine = "sled";
                                    let persistent: Arc<dyn GraphStorageEngine + Send + Sync> = match persistent_engine {
                                        "sled" => {
                                            #[cfg(feature = "with-sled")]
                                            {
                                                let sled_config = SledConfig {
                                                    storage_engine_type: StorageEngineType::Sled,
                                                    path: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.path.clone())
                                                        .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/sled")),
                                                    host: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.host.clone()),
                                                    port: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.port),
                                                    cache_capacity: None,
                                                    temporary: false,
                                                    use_compression: true,
                                                };
                                                match SledStorage::new(&sled_config, &config).await {
                                                    Ok(storage) => Arc::new(storage),
                                                    Err(e) => {
                                                        error!("Failed to create Sled storage for Hybrid: {}", e);
                                                        return Err(e);
                                                    }
                                                }
                                            }
                                            #[cfg(not(feature = "with-sled"))]
                                            return Err(GraphError::ConfigurationError("Sled support is not enabled for Hybrid.".to_string()));
                                        }
                                        "rocksdb" => {
                                            #[cfg(feature = "with-rocksdb")]
                                            {
                                                let rocksdb_config = RocksdbConfig {
                                                    storage_engine_type: StorageEngineType::RocksDB,
                                                    path: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.path.clone())
                                                        .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/rocksdb")),
                                                    host: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.host.clone()),
                                                    port: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.port),
                                                };
                                                match RocksdbStorage::new(&rocksdb_config) {
                                                    Ok(storage) => Arc::new(storage),
                                                    Err(e) => {
                                                        error!("Failed to create RocksDB storage for Hybrid: {}", e);
                                                        return Err(e);
                                                    }
                                                }
                                            }
                                            #[cfg(not(feature = "with-rocksdb"))]
                                            return Err(GraphError::ConfigurationError("RocksDB support is not enabled for Hybrid.".to_string()));
                                        }
                                        "tikv" => {
                                            #[cfg(feature = "with-tikv")]
                                            {
                                                let tikv_config = TikvConfig {
                                                    storage_engine_type: StorageEngineType::TiKV,
                                                    path: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.path.clone())
                                                        .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/tikv")),
                                                    host: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.host.clone()),
                                                    port: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.port),
                                                    pd_endpoints: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.pd_endpoints.clone()),
                                                    username: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.username.clone()),
                                                    password: config.engine_specific_config
                                                        .as_ref()
                                                        .and_then(|map| map.storage.password.clone()),
                                                };
                                                match TikvStorage::new(&tikv_config).await {
                                                    Ok(storage) => Arc::new(storage),
                                                    Err(e) => {
                                                        error!("Failed to create TiKV storage for Hybrid: {}", e);
                                                        return Err(e);
                                                    }
                                                }
                                            }
                                            #[cfg(not(feature = "with-tikv"))]
                                            return Err(GraphError::ConfigurationError("TiKV support is not enabled for Hybrid.".to_string()));
                                        }
                                        _ => {
                                            error!("Unsupported persistent engine for Hybrid: {}", persistent_engine);
                                            return Err(GraphError::ConfigurationError(format!("Unsupported persistent engine for Hybrid: {}", persistent_engine)));
                                        }
                                    };
                                    info!("Created Hybrid storage with persistent engine: {}", persistent_engine);
                                    Ok(Arc::new(HybridStorage::new(persistent)) as Arc<dyn GraphStorageEngine + Send + Sync>)
                                }
                                #[cfg(not(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")))]
                                {
                                    error!("No persistent storage engines enabled for Hybrid");
                                    Err(GraphError::ConfigurationError("No persistent storage engines enabled for Hybrid. Enable 'with-sled', 'with-rocksdb', or 'with-tikv'.".to_string()))
                                }
                            }
                            StorageEngineType::Sled => {
                                #[cfg(feature = "with-sled")]
                                {
                                    let engine_specific = config.engine_specific_config
                                        .as_ref()
                                        .ok_or_else(|| GraphError::ConfigurationError("Sled configuration missing from `engine_specific_config`".to_string()))?;
                                    
                                    let sled_config = SledConfig {
                                        storage_engine_type: StorageEngineType::Sled,
                                        path: engine_specific.storage.path.clone()
                                            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/sled")),
                                        host: engine_specific.storage.host.clone(),
                                        port: engine_specific.storage.port,
                                        temporary: false,
                                        use_compression: engine_specific.storage.use_compression,
                                        cache_capacity: engine_specific.storage.cache_capacity,
                                    };
                                    
                                    info!("Initializing Sled engine with path: {:?}", sled_config.path);
                                    let mut sled_singleton = SLED_SINGLETON.lock().await;
                                    let sled_instance = match sled_singleton.as_ref() {
                                        Some(instance) => {
                                            info!("Reusing existing Sled instance");
                                            instance.clone()
                                        }
                                        None => {
                                            trace!("Creating new SledStorage singleton");
                                            let storage = SledStorage::new(&sled_config, &config).await?;
                                            let instance = Arc::new(storage);
                                            *sled_singleton = Some(instance.clone());
                                            instance
                                        }
                                    };
                                    Ok(sled_instance as Arc<dyn GraphStorageEngine + Send + Sync>)
                                }
                                #[cfg(not(feature = "with-sled"))]
                                {
                                    error!("Sled support is not enabled in this build");
                                    Err(GraphError::StorageError("Sled support is not enabled. Please enable the 'with-sled' feature.".to_string()))
                                }
                            }
                            StorageEngineType::RocksDB => {
                                #[cfg(feature = "with-rocksdb")]
                                {
                                    let rocksdb_path = config.engine_specific_config
                                        .as_ref()
                                        .and_then(|map| map.storage.path.clone())
                                        .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/rocksdb"));
                                    if lock_file_exists(rocksdb_path.join("LOCK")).await? {
                                        warn!("Lock file exists for RocksDB: {:?}", rocksdb_path.join("LOCK"));
                                        recover_rocksdb(&rocksdb_path).await?;
                                    }
                                    let rocksdb_config = RocksdbConfig {
                                        storage_engine_type: StorageEngineType::RocksDB,
                                        path: rocksdb_path,
                                        host: Some(config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.host.clone())
                                            .unwrap_or("127.0.0.1".to_string())),
                                        port: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.port),
                                    };
                                    info!("Initializing RocksDB engine with path: {:?}", rocksdb_config.path);
                                    let mut rocksdb_singleton = ROCKSDB_SINGLETON.lock().await;
                                    let rocksdb_instance = match rocksdb_singleton.as_ref() {
                                        Some(instance) => {
                                            info!("Reusing existing RocksDB instance");
                                            instance.clone()
                                        }
                                        None => {
                                            trace!("Creating new RocksdbStorage singleton");
                                            let storage = RocksdbStorage::new(&rocksdb_config)?;
                                            let instance = Arc::new(storage);
                                            *rocksdb_singleton = Some(instance.clone());
                                            instance
                                        }
                                    };
                                    Ok(rocksdb_instance as Arc<dyn GraphStorageEngine + Send + Sync>)
                                }
                                #[cfg(not(feature = "with-rocksdb"))]
                                {
                                    error!("RocksDB support is not enabled in this build");
                                    Err(GraphError::StorageError("RocksDB support is not enabled. Please enable the 'with-rocksdb' feature.".to_string()))
                                }
                            }
                            StorageEngineType::TiKV => {
                                #[cfg(feature = "with-tikv")]
                                {
                                    let tikv_config = TikvConfig {
                                        storage_engine_type: StorageEngineType::TiKV,
                                        path: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.path.clone())
                                            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/tikv")),
                                        host: Some(config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.host.clone())
                                            .unwrap_or("127.0.0.1".to_string())),
                                        port: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.port),
                                        pd_endpoints: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.pd_endpoints.clone()),
                                        username: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.username.clone()),
                                        password: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.password.clone()),
                                    };
                                    let mut tikv_singleton = TIKV_SINGLETON.lock().await;
                                    let tikv_instance = if let Some(instance) = tikv_singleton.as_ref() {
                                        info!("Reusing existing TiKV instance");
                                        instance.clone()
                                    } else {
                                        trace!("Creating new TiKV instance");
                                        let storage = TikvStorage::new(&tikv_config).await?;
                                        let instance = Arc::new(storage);
                                        *tikv_singleton = Some(instance.clone());
                                        instance
                                    };
                                    Ok(tikv_instance as Arc<dyn GraphStorageEngine + Send + Sync>)
                                }
                                #[cfg(not(feature = "with-tikv"))]
                                {
                                    error!("TiKV support is not enabled in this build");
                                    Err(GraphError::StorageError("TiKV support is not enabled. Please enable the 'with-tikv' feature.".to_string()))
                                }
                            }
                            StorageEngineType::Redis => {
                                #[cfg(feature = "redis-datastore")]
                                {
                                    let redis_config = RedisConfig {
                                        storage_engine_type: StorageEngineType::Redis,
                                        path: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.path.clone())
                                            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/redis")),
                                        host: Some(config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.host.clone())
                                            .unwrap_or("127.0.0.1".to_string())),
                                        port: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.port)
                                            .unwrap_or(6379),
                                    };
                                    let redis_instance = REDIS_SINGLETON.get_or_init(|| async {
                                        trace!("Creating new RedisStorage singleton");
                                        let storage = RedisStorage::new(&redis_config).await?;
                                        Ok(Arc::new(storage))
                                    }).await?;
                                    Ok(redis_instance.clone() as Arc<dyn GraphStorageEngine + Send + Sync>)
                                }
                                #[cfg(not(feature = "redis-datastore"))]
                                {
                                    error!("Redis support is not enabled in this build");
                                    Err(GraphError::StorageError("Redis support is not enabled. Please enable the 'redis-datastore' feature.".to_string()))
                                }
                            }
                            StorageEngineType::PostgreSQL => {
                                #[cfg(feature = "postgres-datastore")]
                                {
                                    let postgres_config = PostgreSQLConfig {
                                        storage_engine_type: StorageEngineType::PostgreSQL,
                                        path: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.path.clone())
                                            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/postgresql")),
                                        host: Some(config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.host.clone())
                                            .unwrap_or("127.0.0.1".to_string())),
                                        port: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.port)
                                            .unwrap_or(5432),
                                    };
                                    let postgres_instance = POSTGRES_SINGLETON.get_or_init(|| async {
                                        trace!("Creating new PostgresStorage singleton");
                                        let storage = PostgresStorage::new(&postgres_config).await?;
                                        Ok(Arc::new(storage))
                                    }).await?;
                                    Ok(postgres_instance.clone() as Arc<dyn GraphStorageEngine + Send + Sync>)
                                }
                                #[cfg(not(feature = "postgres-datastore"))]
                                {
                                    error!("PostgreSQL support is not enabled in this build");
                                    Err(GraphError::StorageError("PostgreSQL support is not enabled. Please enable the 'postgres-datastore' feature.".to_string()))
                                }
                            }
                            StorageEngineType::MySQL => {
                                #[cfg(feature = "mysql-datastore")]
                                {
                                    let mysql_config = MySQLConfig {
                                        storage_engine_type: StorageEngineType::MySQL,
                                        path: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.path.clone())
                                            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/mysql")),
                                        host: Some(config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.host.clone())
                                            .unwrap_or("127.0.0.1".to_string())),
                                        port: config.engine_specific_config
                                            .as_ref()
                                            .and_then(|map| map.storage.port)
                                            .unwrap_or(3306),
                                    };
                                    let mysql_instance = MYSQL_SINGLETON.get_or_init(|| async {
                                        trace!("Creating new MySQLStorage singleton");
                                        let storage = MySQLStorage::new(&mysql_config).await?;
                                        Ok(Arc::new(storage))
                                    }).await?;
                                    Ok(mysql_instance.clone() as Arc<dyn GraphStorageEngine + Send + Sync>)
                                }
                                #[cfg(not(feature = "mysql-datastore"))]
                                {
                                    error!("MySQL support is not enabled in this build");
                                    Err(GraphError::StorageError("MySQL support is not enabled. Please enable the 'mysql-datastore' feature.".to_string()))
                                }
                            }
                        }
                    }
                },
                5,
            ).await?
        };

        // Migrate or copy data if needed
        if old_engine_type != new_config.storage_engine_type || old_path != new_path {
            info!("Handling data transfer from old {:?} (path {:?}) to new {:?} (path {:?})", 
                  old_engine_type, old_path, new_config.storage_engine_type, new_path);
            if old_engine_type != new_config.storage_engine_type {
                info!("Migrating data from {} to {}", old_persistent_arc.get_type(), new_persistent.get_type());
                self.migrate_data(&old_persistent_arc, &new_persistent).await?;
            } else if let (Some(old_p), Some(new_p)) = (old_path.to_str(), new_path.to_str()) {
                if old_p != new_p {
                    info!("Copying data directory for same-engine path change");
                    copy_dir(&old_path, &new_path).await
                        .map_err(|e| GraphError::Io(e))?;
                }
            }
        }

        // Update the engine
        self.engine = Arc::new(TokioMutex::new(HybridStorage {
            inmemory: Arc::new(InMemoryGraphStorage::new(&new_config)),
            persistent: new_persistent.clone(),
            running: Arc::new(TokioMutex::new(false)),
            engine_type: new_config.storage_engine_type,
        }));
        self.persistent_engine = new_persistent;

        // Start the new engine and register daemon
        if !daemon_running || old_engine_type != new_config.storage_engine_type || old_path != new_path {
            let engine = self.engine.lock().await;
            (*engine).start().await
                .map_err(|e| GraphError::StorageError(format!("Failed to start new engine: {}", e)))?;
            let meta = DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: std::process::id(),
                ip_address: "127.0.0.1".to_string(),
                data_dir: new_config.engine_specific_config
                    .as_ref()
                    .and_then(|c| c.storage.path.clone()),
                config_path: Some(config_path),
                engine_type: Some(new_config.storage_engine_type.to_string()),
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| GraphError::StorageError(format!("System time error: {}", e)))?
                    .as_nanos() as i64,
            };
            GLOBAL_DAEMON_REGISTRY.register_daemon(meta).await
                .map_err(|e| GraphError::StorageError(format!("Failed to register daemon: {}", e)))?;
            info!("Registered new daemon for engine {:?} on port {}", new_config.storage_engine_type, port);
        }

        info!("Successfully switched to storage engine: {:?}", new_config.storage_engine_type);
        trace!("use_storage completed in {}ms", start_time.elapsed().as_millis());
        Ok(())
    }
}

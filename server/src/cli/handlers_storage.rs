use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use once_cell::sync::Lazy;
use std::path::{PathBuf, Path};
use std::net::{IpAddr, SocketAddr};
use std::fs::{self, OpenOptions, File};
use std::collections::HashMap;
use tokio::fs::{remove_file};
use tokio::fs as tokio_fs;
use std::io::Write; // <-- this is the missing import
use tokio::io::AsyncReadExt; // Add this import at the top of the file
use std::io::ErrorKind;
use fs2::FileExt;
use chrono::Utc;
use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{self, sleep, timeout, Duration as TokioDuration};
use log::{info, debug, warn, error, trace};
use futures::stream::StreamExt;
use serde_json::{Value, Map};
use serde_yaml2 as serde_yaml;
use rocksdb::{DB, Options};
use reqwest::Client;
use nix::sys::signal::{kill, Signal};
use sysinfo::{System, Process, Pid as NixPid, ProcessesToUpdate, RefreshKind, ProcessRefreshKind};
use nix::unistd::Pid;
use std::time::Instant;
use lib::commands::{CommandType, Commands, StartAction, StorageAction, UseAction};
use lib::config::{
    CliConfig,
    StorageConfig,
    SelectedStorageConfig,
    StorageConfigInner,
    StorageConfigWrapper,
    TiKVConfigWrapper,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_STORAGE_CONFIG_PATH_POSTGRES,
    DEFAULT_STORAGE_CONFIG_PATH_MYSQL,
    DEFAULT_STORAGE_CONFIG_PATH_REDIS,
    DEFAULT_STORAGE_CONFIG_PATH_SLED,
    DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB,
    DEFAULT_STORAGE_CONFIG_PATH_TIKV,
    DEFAULT_STORAGE_CONFIG_PATH,
    TIKV_DAEMON_ENGINE_TYPE_NAME,
    STORAGE_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_DIR,
    DEFAULT_DATA_DIRECTORY,
    DEFAULT_STORAGE_PORT,
    MAX_SHUTDOWN_RETRIES,
    SHUTDOWN_RETRY_DELAY_MS,
    SelectedStorageConfigWrapper,
    CliTomlStorageConfig,
    load_storage_config_from_yaml,
    load_engine_specific_config,
    load_cli_config,
    daemon_api_storage_engine_type_to_string,
    create_default_selected_storage_config,
};
use lib::config::config_helpers::{ validate_cluster_range };
use crate::cli::handlers_utils::{format_engine_config, write_registry_fallback, execute_storage_query, 
                                 convert_hashmap_to_selected_config, retry_operation, selected_storage_config_to_hashmap };
use daemon_api::start_daemon;
pub use models::errors::GraphError;
use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::config::{StorageEngineType, StorageConfig as EngineStorageConfig, TikvConfig,
                                  RedisConfig, MySQLConfig, PostgreSQLConfig, RocksDBConfig, SledConfig,
                                  };
use lib::storage_engine::{AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER, 
                          emergency_cleanup_storage_engine_manager,  SledStorage, RocksDBStorage, TikvStorage, 
                          log_lock_file_diagnostics, SurrealdbGraphStorage};

use lib::daemon::daemon_management::{
    is_port_free,
    find_pid_by_port,
    check_process_status_by_port,
    stop_process_by_port,
    parse_cluster_range,
    is_port_in_cluster_range,
    is_storage_daemon_running,
    get_pid_for_port,
    check_pid_validity,
    stop_process_by_pid,
    check_daemon_health,
};
use lib::query_parser::{parse_query_from_string, QueryType};

// --- Add this new global static mutex to control concurrent access ---
// Use a TokioMutex for async locking.
// The Mutex<()> is a common pattern when you just need a lock and no data.
static USE_STORAGE_LOCK: Lazy<TokioMutex<()>> = Lazy::new(|| {
    info!("Initializing global use storage command lock.");
    TokioMutex::new(())
});

// A constant for the file-based lock path. This ensures all processes
// are looking for the same file to coordinate.
const LOCK_FILE_PATH: &str = "/tmp/graphdb_storage_lock";

// A struct to manage the file-based lock. It acquires the lock upon creation
// and ensures it's removed when the program exits gracefully.
struct FileLock {
    path: PathBuf,
}

impl FileLock {
    async fn acquire() -> Result<Self, anyhow::Error> {
        let path = PathBuf::from(LOCK_FILE_PATH);
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(_) => {
                info!("Successfully acquired inter-process lock at {:?}", path);
                Ok(FileLock { path })
            },
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                warn!("Lock file exists at {:?}", path);
                // Attempt to clean up stale lock file if it's older than a threshold
                if let Ok(metadata) = fs::metadata(&path) {
                    if let Ok(modified) = metadata.modified() {
                        let age = SystemTime::now().duration_since(modified).unwrap_or(Duration::from_secs(0));
                        if age > Duration::from_secs(60) { // Consider lock stale after 60 seconds
                            warn!("Removing stale lock file (age: {:?})", age);
                            if let Err(e) = fs::remove_file(&path) {
                                error!("Failed to remove stale lock file: {}", e);
                            } else {
                                // Retry acquiring the lock
                                match OpenOptions::new().write(true).create_new(true).open(&path) {
                                    Ok(_) => {
                                        info!("Successfully acquired lock after removing stale file");
                                        return Ok(FileLock { path });
                                    },
                                    Err(e) => return Err(anyhow!("Failed to acquire lock after removing stale file: {}", e)),
                                }
                            }
                        }
                    }
                }
                Err(anyhow!("Another process is already using the storage engine. Please wait for it to finish."))
            },
            Err(e) => Err(anyhow!("Failed to create lock file: {}", e)),
        }
    }

    async fn release(&self) -> Result<(), anyhow::Error> {
        if self.path.exists() {
            remove_file(&self.path).await?;
            info!("Successfully released inter-process lock at {:?}", self.path);
        } else {
            warn!("Lock file at {:?} does not exist during release", self.path);
        }
        Ok(())
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if self.path.exists() {
            if let Err(e) = std::fs::remove_file(&self.path) {
                error!("Failed to release lock file {:?} in Drop: {}", self.path, e);
            } else {
                info!("Released lock file {:?} in Drop", self.path);
            }
        }
    }
}

/// A new struct to hold the information for a single daemon process.
/// This makes it easier to pass around all the relevant details.
#[derive(Debug)]
pub struct DaemonInfo {
    pub pid: u32,
    pub port: u16,
    pub status: String,
    pub engine: String,
    pub config_path: String,
    pub max_open_files: Option<u64>,
    pub max_disk_space: Option<u64>,
    pub min_disk_space: Option<u64>,
    pub use_raft: Option<bool>,
    pub data_directory: Option<PathBuf>,
    pub log_directory: Option<PathBuf>,
    pub cluster_range: String,
}

pub mod storage {
    pub mod api {
        use anyhow::Result;
        pub async fn check_storage_daemon_status(_port: u16) -> Result<String> { Ok("Running".to_string()) }
    }
}

// Function to check if engine-specific config is complete
fn is_engine_specific_config_complete(config: &Option<SelectedStorageConfig>) -> bool {
    if let Some(engine_config) = config {
        engine_config.storage.path.is_some() || engine_config.storage.host.is_some()
    } else {
        false
    }
}

/// Helper function to synchronize the daemon registry with the latest config from the StorageEngineManager.
/// This ensures the 'status' command always shows the correct engine type.
// Updated sync_daemon_registry_with_manager function
// Updated sync_daemon_registry_with_manager function
async fn sync_daemon_registry_with_manager(
    port: u16,
    config: &StorageConfig,
    _shutdown_tx_opt: &Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
) -> Result<(), anyhow::Error> {
    let manager = GLOBAL_STORAGE_ENGINE_MANAGER.get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?;
    let engine_type = manager.current_engine_type().await;
    
    let base_data_dir = config.data_directory
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
    let engine_path_name = daemon_api_storage_engine_type_to_string(&engine_type).to_lowercase();
    let instance_path = base_data_dir.join(&engine_path_name).join(port.to_string());

    let daemon_metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port,
        pid: std::process::id(),
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(instance_path.clone()),
        config_path: config.config_root_directory.clone(),
        engine_type: Some(engine_type.to_string()),
        last_seen_nanos: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
        zmq_ready: false,
    };

    // Check if daemon is already registered
    if let Some(existing_metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
        if existing_metadata.data_dir != Some(instance_path.clone()) || existing_metadata.engine_type != Some(engine_type.to_string()) {
            warn!("Updating daemon metadata for port {} from path {:?} to {:?}", port, existing_metadata.data_dir, instance_path);
            GLOBAL_DAEMON_REGISTRY.update_daemon_metadata(daemon_metadata).await?;
            info!("Updated daemon metadata for port {} with path {:?}", port, instance_path);
        } else {
            info!("Daemon metadata for port {} already correct", port);
        }
    } else {
        timeout(Duration::from_secs(5), GLOBAL_DAEMON_REGISTRY.register_daemon(daemon_metadata))
            .await
            .map_err(|_| anyhow!("Timeout registering daemon on port {}", port))??;
        info!("Registered daemon on port {} with path {:?}", port, instance_path);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    new_config: Option<StorageConfig>,
    _cluster_opt: Option<String>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<(), anyhow::Error> {
    trace!("Entering start_storage_interactive with port: {:?}", port);
    info!("handlers_storage.rs version: 2025-09-29");

    let command = CommandType::StartStorage {
        port,
        config_file: config_file.clone(),
        cluster: _cluster_opt.clone(),
        storage_port: port,
        storage_cluster: _cluster_opt,
    };
    let cli_config = CliConfig::load(Some(command))
        .with_context(|| "Failed to load CLI configuration. Check file permissions or path.")?;
    info!("SUCCESS: CLI config loaded: {:?}", cli_config);
    println!("SUCCESS: CLI config loaded.");

    let get_config_path = |cli_config_file: Option<PathBuf>| {
        cli_config_file.or(config_file.clone()).unwrap_or_else(|| {
            PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)
        })
    };

    let config_path = match &cli_config.command {
        Commands::Start { action: Some(StartAction::All { storage_config, .. }), .. } => get_config_path(storage_config.clone()),
        Commands::Start { action: Some(StartAction::Storage { config_file, .. }), .. } => get_config_path(config_file.clone()),
        Commands::Storage(StorageAction::Start { config_file, .. }) => get_config_path(config_file.clone()),
        _ => get_config_path(None),
    };
    debug!("Determined config path: {:?}", config_path);
    println!("==> STARTING STORAGE - STEP 3");

    let mut storage_config = if let Some(config) = new_config {
        info!("Using provided StorageConfig object, ignoring config_file.");
        config
    } else {
        match load_storage_config_from_yaml(Some(config_path.clone())).await {
            Ok(config) => {
                debug!("Loaded storage config from {:?}", config_path);
                println!("Loaded storage config from {:?}", config_path);
                config
            },
            Err(e) => {
                error!("Failed to load storage config from {:?}: {}", config_path, e);
                warn!("Using default StorageConfig due to config load failure");
                StorageConfig::default()
            }
        }
    };
    println!("=========> IN STEP 3 CONFIG IS {:?}", storage_config);

    if storage_config.config_root_directory.is_none() {
        storage_config.config_root_directory = Some(config_path.parent().unwrap_or(&PathBuf::from("./storage_daemon_server")).to_path_buf());
        debug!("Set config_root_directory to {:?}", storage_config.config_root_directory);
    }
    info!("Loaded Storage Config: {:?}", storage_config);
    info!("Storage engine type from config: {:?}", storage_config.storage_engine_type);
    println!("=========> BEFORE STEP 3.1 CONFIG IS {:?}", storage_config);
    println!("==> STARTING STORAGE - STEP 3.1");

    let engine_specific_config_path = match daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type).to_lowercase().as_str() {
        "sled" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED)),
        "rocksdb" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB)),
        "postgres" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES)),
        "mysql" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL)),
        "redis" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS)),
        "tikv" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV)),
        _ => None,
    };

    if let Some(engine_config_path) = engine_specific_config_path {
        if engine_config_path.exists() {
            debug!("Loading engine-specific config from {:?}", engine_config_path);
            match tokio_fs::read_to_string(&engine_config_path).await {
                Ok(content) => {
                    match serde_yaml::from_str::<SelectedStorageConfigWrapper>(&content) {
                        Ok(engine_specific) => {
                            let should_override = storage_config.engine_specific_config.is_none() ||
                                storage_config.engine_specific_config.as_ref()
                                    .map(|c| c.storage_engine_type != storage_config.storage_engine_type)
                                    .unwrap_or(true);
                            if should_override {
                                println!("Overriding engine-specific config with values from {:?}", engine_config_path);
                                storage_config.engine_specific_config = Some(engine_specific.storage.clone());
                            } else {
                                println!("Loaded engine-specific config from {:?}", engine_config_path);
                                info!("Merged engine-specific config from {:?}", engine_config_path);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to deserialize engine-specific config from {:?}: {}. Using default config.", engine_config_path, e);
                            storage_config.engine_specific_config = Some(create_default_selected_storage_config(&storage_config.storage_engine_type));
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read engine-specific config file {:?}: {}. Using default config.", engine_config_path, e);
                    storage_config.engine_specific_config = Some(create_default_selected_storage_config(&storage_config.storage_engine_type));
                }
            }
        } else {
            debug!("No engine-specific config found at {:?}", engine_config_path);
            storage_config.engine_specific_config = Some(create_default_selected_storage_config(&storage_config.storage_engine_type));
        }
    } else {
        debug!("No engine-specific config path defined for engine {:?}", storage_config.storage_engine_type);
        storage_config.engine_specific_config = Some(create_default_selected_storage_config(&storage_config.storage_engine_type));
    }
    println!("=========> BEFORE STEP 3.2 CONFIG IS {:?}", storage_config);

    let selected_port = port.unwrap_or(storage_config.default_port);
    if let Some(ref mut engine_config) = storage_config.engine_specific_config {
        let engine_path_name = storage_config.storage_engine_type.to_string().to_lowercase();
        let data_dir_path = storage_config.data_directory.as_ref().map_or(
            PathBuf::from(DEFAULT_DATA_DIRECTORY),
            |p| p.clone()
        );
        let engine_data_path = data_dir_path.join(&engine_path_name).join(selected_port.to_string());

        if engine_config.storage.path.is_none() || engine_config.storage.path != Some(engine_data_path.clone()) {
            info!("Setting engine-specific path to {:?}", engine_data_path);
            engine_config.storage.path = Some(engine_data_path.clone());
        }

        if engine_config.storage.port != Some(selected_port) {
            info!("Updating engine-specific port from {:?} to {}", engine_config.storage.port, selected_port);
            engine_config.storage.port = Some(selected_port);
        }
    }
    debug!("Storage config after path normalization: {:?}", storage_config);
    println!("==> STARTING STORAGE - STEP 3.2 {:?}", storage_config);

    let is_permanent = matches!(
        &cli_config.command,
        Commands::Use(UseAction::Storage { permanent: true, .. })
    );
    if is_permanent {
        debug!("--permanent flag detected, setting storage engine to {:?}", storage_config.storage_engine_type);
        info!("Saved storage config with engine {:?}", storage_config.storage_engine_type);
        storage_config.save().await.context("Failed to save storage config")?;
    }
    println!("==> STARTING STORAGE - STEP 4 - and engine specific config here is {:?}", storage_config.engine_specific_config);

    info!("===> SELECTED PORT {} FOR DAEMON COMMUNICATION (CLI: {:?}, Default: {})",
        selected_port, port, storage_config.default_port);
    println!("===> SELECTED PORT {} FOR DAEMON COMMUNICATION", selected_port);
    println!("===> USING ENGINE-SPECIFIC PORT {} FROM CONFIG", selected_port);

    let mut current_ports = parse_port_cluster_range(&storage_config.cluster_range)?;
    if storage_config.storage_engine_type != StorageEngineType::TiKV {
        if !current_ports.contains(&selected_port) {
            current_ports.push(selected_port);
            current_ports.sort();
            current_ports.dedup();
            storage_config.cluster_range = if current_ports.len() == 1 {
                selected_port.to_string()
            } else {
                format!("{}-{}", current_ports.iter().min().unwrap_or(&selected_port), current_ports.iter().max().unwrap_or(&selected_port))
            };
            info!("Updated cluster range to {} to include selected port {}", storage_config.cluster_range, selected_port);
        }
    }

    let tikv_pd_port = if storage_config.storage_engine_type == StorageEngineType::TiKV {
        storage_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.pd_endpoints.as_deref())
            .and_then(|pd| pd.split(':').last())
            .and_then(|p| p.parse::<u16>().ok())
    } else {
        let tikv_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if tikv_config_path.exists() {
            match std::fs::read_to_string(&tikv_config_path) {
                Ok(content) => {
                    match serde_yaml::from_str::<TiKVConfigWrapper>(&content) {
                        Ok(wrapper) => wrapper.storage.pd_endpoints.and_then(|pd| pd.split(':').last().and_then(|p| p.parse::<u16>().ok())),
                        Err(e) => {
                            warn!("Failed to parse previous TiKV config for PD port: {}", e);
                            None
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read TiKV config file: {}", e);
                    None
                }
            }
        } else {
            None
        }
    };
    debug!("TiKV PD port: {:?}", tikv_pd_port);

    if storage_config.storage_engine_type == StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if Some(selected_port) == Some(pd_port) {
                info!("Skipping daemon startup on TiKV PD port {}", selected_port);
                println!("Skipping daemon startup on TiKV PD port {}.", selected_port);
                return Ok(());
            }
        }
    }

    if storage_config.storage_engine_type != StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if selected_port == pd_port {
                return Err(anyhow!("Selected port {} is reserved for TiKV PD and cannot be used for engine {:?}", selected_port, storage_config.storage_engine_type));
            }
        }
    }

    let ip = "127.0.0.1";
    let addr = format!("ipc:///opt/graphdb/graphdb-{}.ipc", selected_port);
    println!("==> STARTING STORAGE - STEP 5");

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry state before startup for port {}: {:?}", selected_port, all_daemons);
    let is_graphdb_process = all_daemons.iter().any(|d| d.port == selected_port && d.service_type == "storage");

    if is_graphdb_process {
        info!("Port {} is in use by an existing GraphDB storage daemon. Attempting cleanup...", selected_port);
        if let Some(metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(selected_port).await? {
            let engine_type = metadata.engine_type.as_ref().map(|s| s.as_str());
            let engine_path = metadata.data_dir.as_ref();
            let pid = metadata.pid;

            if pid > 0 && check_pid_validity(pid).await {
                info!("Active process with PID {} found for port {}. Attempting to terminate...", selected_port, pid);
                match stop_process_by_pid("Storage Daemon", pid).await {
                    Ok(_) => {
                        info!("Successfully terminated process with PID {} for port {}", pid, selected_port);
                        println!("===> SUCCESSFULLY TERMINATED PROCESS WITH PID {} FOR PORT {}", pid, selected_port);
                    }
                    Err(e) if e.to_string().contains("No such process") => {
                        info!("Process with PID {} for port {} already terminated", pid, selected_port);
                        println!("===> PROCESS WITH PID {} FOR PORT {} ALREADY TERMINATED", pid, selected_port);
                    }
                    Err(e) => {
                        warn!("Failed to terminate process with PID {} for port {}: {}", pid, selected_port, e);
                        println!("===> ERROR: FAILED TO TERMINATE PROCESS WITH PID {} FOR PORT {}", pid, selected_port);
                    }
                }
            } else {
                info!("No active process found for PID {} on port {}", pid, selected_port);
                println!("===> NO ACTIVE PROCESS FOUND FOR PID {} ON PORT {}", pid, selected_port);
            }

            if let (Some(engine_type), Some(engine_path)) = (engine_type, engine_path) {
                match engine_type.to_lowercase().as_str() {
                    "rocksdb" => {
                        if engine_path.exists() {
                            if let Err(e) = RocksDBStorage::force_unlock(engine_path).await {
                                warn!("Failed to unlock RocksDB database at {:?}: {}", engine_path, e);
                                println!("===> ERROR: FAILED TO UNLOCK ROCKSDB DATABASE AT {:?}", engine_path);
                            } else {
                                info!("Successfully unlocked RocksDB database at {:?}", engine_path);
                                println!("===> SUCCESSFULLY UNLOCKED ROCKSDB DATABASE AT {:?}", engine_path);
                            }
                            let lock_file = engine_path.join("LOCK");
                            if lock_file.exists() {
                                warn!("Lock file still exists at {:?} after unlock attempt", lock_file);
                                println!("===> ERROR: LOCK FILE STILL EXISTS AT {:?}", lock_file);
                            } else {
                                println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
                            }
                        }
                    }
                    "sled" => {
                        if engine_path.exists() {
                            if let Err(e) = SledStorage::force_unlock(engine_path).await {
                                warn!("Failed to unlock Sled database at {:?}: {}", engine_path, e);
                                println!("===> ERROR: FAILED TO UNLOCK SLED DATABASE AT {:?}", engine_path);
                            } else {
                                info!("Successfully unlocked Sled database at {:?}", engine_path);
                                println!("===> SUCCESSFULLY UNLOCKED SLED DATABASE AT {:?}", engine_path);
                            }
                            let lock_file = engine_path.join("db.lck");
                            if lock_file.exists() {
                                warn!("Lock file still exists at {:?} after unlock attempt", lock_file);
                                println!("===> ERROR: LOCK FILE STILL EXISTS AT {:?}", lock_file);
                            } else {
                                println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
                            }
                        }
                    }
                    _ => {
                        warn!("Unknown engine type {} for port {}", engine_type, selected_port);
                        println!("===> WARNING: UNKNOWN ENGINE TYPE {} FOR PORT {}", engine_type, selected_port);
                    }
                }
            }
            GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", selected_port).await?;
            info!("Successfully removed stale storage daemon on port {} from registry", selected_port);
            println!("===> REMOVED STALE DAEMON ON PORT {} FROM REGISTRY", selected_port);
        }
    }

    if check_process_status_by_port("Storage Daemon", selected_port).await {
        info!("Port {} is in use by a storage process. Attempting to stop it.", selected_port);
        for attempt in 1..=3 {
            match stop_process_by_port("Storage Daemon", selected_port).await {
                Ok(_) => {
                    info!("Successfully stopped storage process on port {} on attempt {}", selected_port, attempt);
                    println!("Storage Daemon on port {} stopped.", selected_port);
                    break;
                }
                Err(e) if e.to_string().contains("No such process") => {
                    info!("Storage process on port {} already terminated on attempt {}", selected_port, attempt);
                    println!("Storage Daemon on port {} stopped.", selected_port);
                    break;
                }
                Err(e) if attempt < 3 => {
                    warn!("Failed to stop storage process on port {} on attempt {}: {}. Retrying.", selected_port, attempt, e);
                    tokio::time::sleep(TokioDuration::from_millis(500 * attempt as u64)).await;
                }
                Err(e) => {
                    error!("Failed to stop storage process on port {} after 3 attempts: {}", selected_port, e);
                    return Err(anyhow!("Failed to stop existing storage process on port {}: {}", selected_port, e));
                }
            }
        }
    } else {
        info!("No storage process found running on port {}", selected_port);
        println!("===> NO STORAGE PROCESS FOUND ON PORT {}", selected_port);
    }

    async fn is_port_free(port: u16) -> bool {
        !check_process_status_by_port("Storage Daemon", port).await
    }

    println!("==> STARTING STORAGE - STEP 5.1 {}", daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type));
    if !is_port_free(selected_port).await {
        warn!("Port {} is still in use after cleanup attempt.", selected_port);
        return Err(anyhow!("Port {} is not free after cleanup", selected_port));
    }

    let engine_type = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);
    debug!("Converted engine_type: {}", engine_type);
    println!("==> STARTING STORAGE - STEP 5.2 {}", engine_type);

    debug!("storage_config: {:?}", storage_config);
    let daemon_config_string = serde_yaml::to_string(&storage_config)
        .context("Failed to serialize daemon config to YAML")?;
    debug!("daemon_config_string: {}", daemon_config_string);
    println!("==> STARTING STORAGE - STEP 5.3");

    // Start the daemon and ensure it's running before proceeding
    let max_attempts = 3;
    let retry_interval = TokioDuration::from_millis(1000);
    let mut pid = None;

    for attempt in 1..=max_attempts {
        debug!("Attempt {}/{} to start storage daemon on port {}", attempt, max_attempts, selected_port);
        match start_daemon(
            Some(selected_port),
            Some(daemon_config_string.clone()),
            vec![],
            "storage",
            Some(storage_config.clone()),
        ).await {
            Ok(_) => {
                debug!("start_daemon succeeded on attempt {} for port {}", attempt, selected_port);
                match find_pid_by_port(selected_port).await {
                    Some(p) if p > 0 && check_pid_validity(p).await => {
                        pid = Some(p);
                        info!("Storage daemon started with PID {} on port {}", p, selected_port);
                        println!("Daemon (PID {}) is listening on {}:{}", p, ip, selected_port);
                        break;
                    }
                    _ => {
                        warn!("No valid PID found for port {} after starting daemon on attempt {}", selected_port, attempt);
                        if attempt < max_attempts {
                            info!("Retrying storage daemon startup in {:?}", retry_interval);
                            tokio::time::sleep(retry_interval).await;
                        } else {
                            return Err(anyhow!("No valid PID found for port {} after {} attempts", selected_port, max_attempts));
                        }
                    }
                }
            }
            Err(e) => {
                error!("Storage daemon start attempt {}/{} failed on port {}: {}", attempt, max_attempts, selected_port, e);
                if attempt < max_attempts {
                    info!("Retrying storage daemon startup in {:?}", retry_interval);
                    tokio::time::sleep(retry_interval).await;
                } else {
                    return Err(anyhow!("Failed to start storage daemon on port {} after {} attempts: {}", selected_port, max_attempts, e));
                }
            }
        }
    }

    let pid = pid.ok_or_else(|| anyhow!("Failed to start storage daemon on port {} after {} attempts", selected_port, max_attempts))?;

    // Register daemon metadata before engine initialization
    println!("==> STARTING STORAGE - STEP 6 (PREPARE PATHS)");
    let engine_type_str = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);
    let engine_path_name = engine_type_str.to_lowercase();

    let base_data_dir = storage_config.data_directory
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
    let base_engine_path = base_data_dir.join(&engine_path_name);
    let instance_path = base_engine_path.join(selected_port.to_string());

    let mut manager_config = storage_config.clone();
    if let Some(ref mut engine_config) = manager_config.engine_specific_config {
        engine_config.storage.path = Some(instance_path.clone());
        engine_config.storage.port = Some(selected_port);
    }
    manager_config.data_directory = Some(base_engine_path.clone());

    info!("Prepared paths - base: {:?}", base_engine_path);
    info!("Prepared paths - instance: {:?}", instance_path);
    println!("=====> Instance path for daemon: {:?}", instance_path);

    let daemon_metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port: selected_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(instance_path.clone()),
        config_path: Some(config_path.clone()),
        engine_type: Some(engine_type_str.clone()),
        last_seen_nanos: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
        zmq_ready: false,
    };

    let pre_register_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry state before registering daemon on port {}: {:?}", selected_port, pre_register_daemons);

    if let Some(existing) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(selected_port).await? {
        let update_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port: selected_port,
            pid,
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(instance_path.clone()),
            config_path: Some(config_path.clone()),
            engine_type: Some(engine_type_str.clone()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
            zmq_ready: false,
        };
        GLOBAL_DAEMON_REGISTRY.update_daemon_metadata(update_metadata).await?;
        info!("Updated existing daemon metadata on port {}", selected_port);
        println!("===> UPDATED DAEMON ON PORT {} WITH PATH {:?}", selected_port, instance_path);
    } else {
        timeout(TokioDuration::from_secs(5), GLOBAL_DAEMON_REGISTRY.register_daemon(daemon_metadata))
            .await
            .map_err(|_| {
                error!("Timeout registering daemon on port {}", selected_port);
                println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", selected_port);
                anyhow!("Timeout registering daemon on port {}", selected_port)
            })??;
        info!("Registered daemon on port {} with path {:?}", selected_port, instance_path);
        println!("===> REGISTERED DAEMON ON PORT {} WITH PATH {:?}", selected_port, instance_path);
    }

    // Verify daemon registration
    let post_register_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry state after registering daemon on port {}: {:?}", selected_port, post_register_daemons);
    if !post_register_daemons.iter().any(|d| d.port == selected_port && d.service_type == "storage") {
        error!("Daemon on port {} not found in registry after registration!", selected_port);
        println!("===> ERROR: DAEMON ON PORT {} NOT FOUND IN REGISTRY AFTER REGISTRATION!", selected_port);
        return Err(anyhow!("Daemon on port {} not found in registry after registration", selected_port));
    }

    // Initialize StorageEngineManager after daemon is confirmed running
    println!("==> STARTING STORAGE - STEP 7 (INITIALIZE MANAGER)");
    trace!("Checking if GLOBAL_STORAGE_ENGINE_MANAGER is initialized");

    let max_init_attempts = 3;
    let mut engine_initialized = false;
    let mut attempt = 0;

    while attempt < max_init_attempts && !engine_initialized {
        attempt += 1;
        info!("Attempt {}/{} to initialize StorageEngineManager on port {}", attempt, max_init_attempts, selected_port);
        println!("===> ATTEMPT {}/{} TO INITIALIZE STORAGEENGINEMANAGER ON PORT {}", attempt, max_init_attempts, selected_port);

        // Check if manager is already initialized
        if let Some(async_manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            info!("Existing StorageEngineManager found, updating with new config");
            async_manager
                .use_storage(manager_config.clone(), is_permanent)
                .await
                .context("Failed to update existing StorageEngineManager")?;
            engine_initialized = true;
            println!("===> SUCCESSFULLY UPDATED STORAGEENGINEMANAGER ON ATTEMPT {}", attempt);
        } else {
            match StorageEngineManager::new(
                storage_config.storage_engine_type.clone(),
                &config_path,
                is_permanent,
                Some(selected_port),
            ).await {
                Ok(manager) => {
                    engine_initialized = true;
                    let arc_manager = Arc::new(AsyncStorageEngineManager::from_manager(manager));
                    GLOBAL_STORAGE_ENGINE_MANAGER
                        .set(arc_manager.clone())
                        .map_err(|_| anyhow!("Failed to set StorageEngineManager"))?;
                    info!("Successfully initialized StorageEngineManager on attempt {}", attempt);
                    println!("===> SUCCESSFULLY INITIALIZED STORAGEENGINEMANAGER ON ATTEMPT {}", attempt);
                }
                Err(e) if e.to_string().contains("WouldBlock") && attempt < max_init_attempts => {
                    warn!("Failed to initialize StorageEngineManager due to WouldBlock error: {}. Retrying after delay...", e);
                    println!("===> ERROR: WOULDBLOCK ON STORAGEENGINEMANAGER INITIALIZATION, RETRYING");
                    tokio::time::sleep(TokioDuration::from_millis(1000)).await;
                    continue;
                }
                Err(e) => {
                    error!("Failed to initialize StorageEngineManager: {}", e);
                    println!("===> ERROR: FAILED TO INITIALIZE STORAGEENGINEMANAGER: {}", e);
                    return Err(anyhow!("Failed to initialize StorageEngineManager: {}", e));
                }
            }
        }
    }

    if !engine_initialized {
        error!("Failed to initialize StorageEngineManager on port {} after {} attempts", selected_port, max_init_attempts);
        println!("===> ERROR: FAILED TO INITIALIZE STORAGEENGINEMANAGER ON PORT {} AFTER {} ATTEMPTS", selected_port, max_init_attempts);
        return Err(anyhow!("Failed to initialize StorageEngineManager on port {} after {} attempts", selected_port, max_init_attempts));
    }

    info!("Initialized StorageEngineManager with engine type: {:?} on port {}", storage_config.storage_engine_type, selected_port);
    println!("===> STORAGE ENGINE MANAGER INITIALIZED SUCCESSFULLY FOR PORT {}", selected_port);

    // Perform engine-specific cleanup and verification
    if storage_config.storage_engine_type == StorageEngineType::Sled {
        let selected_config = storage_config.engine_specific_config.clone()
            .unwrap_or_else(|| create_default_selected_storage_config(&StorageEngineType::Sled));
        let sled_path = selected_config.storage.path.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY).join("sled").join(selected_port.to_string()));
        info!("Sled path set to {:?}", sled_path);
        println!("===> USING SLED PATH: {:?}", sled_path);

        // Verify Sled database by checking directory existence
        if sled_path.exists() && sled_path.is_dir() {
            info!("Sled database directory exists at {:?}", sled_path);
            println!("===> SLED DATABASE DIRECTORY VERIFIED AT {:?}", sled_path);
        } else {
            warn!("Sled database directory does not exist at {:?}", sled_path);
            println!("===> WARNING: SLED DATABASE DIRECTORY NOT FOUND AT {:?}", sled_path);
        }
    } else if storage_config.storage_engine_type == StorageEngineType::RocksDB {
        let selected_config = storage_config.engine_specific_config.clone()
            .unwrap_or_else(|| create_default_selected_storage_config(&StorageEngineType::RocksDB));
        let rocksdb_path = selected_config.storage.path.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY).join("rocksdb").join(selected_port.to_string()));
        info!("RocksDB path set to {:?}", rocksdb_path);
        println!("===> USING ROCKSDB PATH: {:?}", rocksdb_path);

        let max_reset_attempts = 3;
        let mut attempt = 0;
        let mut rocksdb_initialized = false;

        while attempt < max_reset_attempts && !rocksdb_initialized {
            attempt += 1;
            info!("Attempt {}/{} to initialize RocksDB database at {:?}", attempt, max_reset_attempts, rocksdb_path);
            println!("===> ATTEMPT {}/{} TO INITIALIZE ROCKSDB DATABASE AT {:?}", attempt, max_reset_attempts, rocksdb_path);

            match RocksDBStorage::force_unlock(&rocksdb_path).await {
                Ok(_) => {
                    info!("Successfully performed RocksDB force unlock at {:?}", rocksdb_path);
                    println!("===> SUCCESSFULLY UNLOCKED ROCKSDB DATABASE AT {:?}", rocksdb_path);
                }
                Err(e) => {
                    warn!("Failed to force unlock RocksDB at {:?}: {}", rocksdb_path, e);
                    println!("===> ERROR: FAILED TO UNLOCK ROCKSDB DATABASE AT {:?}", rocksdb_path);
                }
            }

            tokio::time::sleep(TokioDuration::from_millis(1000)).await;

            let lock_file = rocksdb_path.join("LOCK");
            if lock_file.exists() {
                warn!("Lock file still exists at {:?} after unlock attempt", lock_file);
                println!("===> WARNING: LOCK FILE STILL EXISTS AT {:?}", lock_file);
            } else {
                info!("No lock file found at {:?}", lock_file);
                println!("===> NO LOCK FILE FOUND AT {:?}", lock_file);
            }

            rocksdb_initialized = true;
        }

        if !rocksdb_initialized {
            return Err(anyhow!("Failed to initialize RocksDB database at {:?} after {} attempts", rocksdb_path, max_reset_attempts));
        }
    } else if storage_config.storage_engine_type == StorageEngineType::TiKV {
        let pd_endpoints = storage_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.pd_endpoints.clone())
            .unwrap_or("127.0.0.1:2379".to_string());
        let pd_port = pd_endpoints.split(':').last().and_then(|p| p.parse::<u16>().ok());
        if let Some(pd_port) = pd_port {
            if check_process_status_by_port("TiKV PD", pd_port).await {
                info!("TiKV PD process detected on port {}. Skipping termination to avoid disrupting cluster.", pd_port);
                println!("Skipping termination for TiKV PD port {} (process: Storage Daemon).", pd_port);
            }
        }
        if let Err(e) = TikvStorage::force_unlock().await {
            warn!("Failed to force unlock TiKV: {}", e);
        } else {
            info!("Successfully performed TiKV force unlock");
            println!("===> Lock files cleaned successfully.");
        }
    }

    sync_daemon_registry_with_manager(selected_port, &storage_config, &storage_daemon_shutdown_tx_opt).await?;
    println!("==> STEP 7 COMPLETE: Registry synced with manager paths");

    // Set up shutdown channel and handle
    *storage_daemon_port_arc.lock().await = Some(selected_port);
    let (tx, rx) = oneshot::channel();
    *storage_daemon_shutdown_tx_opt.lock().await = Some(tx);
    let handle = tokio::spawn(async move {
        rx.await.ok();
        info!("Storage daemon on port {} shutting down", selected_port);
    });
    *storage_daemon_handle.lock().await = Some(handle);
    debug!("Completed storage daemon startup");

    let final_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Final registry state for port {}: {:?}", selected_port, final_daemons);
    if !final_daemons.iter().any(|d| d.port == selected_port && d.service_type == "storage") {
        error!("Daemon on port {} still not found in registry after startup!", selected_port);
        println!("===> ERROR: DAEMON ON PORT {} STILL NOT FOUND IN REGISTRY AFTER STARTUP!", selected_port);
    }

    info!("Storage daemon startup completed successfully on port {}", selected_port);
    println!("Storage daemon startup completed successfully on port {}.", selected_port);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn stop_storage_interactive(
    port: Option<u16>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    daemon_port: Arc<TokioMutex<Option<u16>>>,
) -> Result<(), anyhow::Error> {
    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;

    // If no port is specified, stop all storage daemons
    let target_ports = match port {
        Some(p) => vec![p],
        None => {
            let daemons = daemon_registry
                .get_all_daemon_metadata()
                .await
                .context("Failed to access daemon registry")?;
            daemons
                .into_iter()
                .filter(|d| d.service_type == "storage")
                .map(|d| d.port)
                .collect::<Vec<u16>>()
        }
    };

    if target_ports.is_empty() && port.is_none() {
        info!("No storage daemons found in registry to stop.");
        println!("===> NO STORAGE DAEMONS FOUND IN REGISTRY TO STOP");
        return Ok(());
    }

    let mut failed_ports = Vec::new();

    for port in target_ports {
        // Find the daemon for the specified port
        let daemon = daemon_registry
            .get_all_daemon_metadata()
            .await
            .context("Failed to access daemon registry")?
            .into_iter()
            .find(|d| d.service_type == "storage" && d.port == port);

        // Get the database path and socket path for cleanup
        let db_path = PathBuf::from(DEFAULT_DATA_DIRECTORY)
            .join("rocksdb")
            .join(port.to_string());
        let lock_path = db_path.join("LOCK");
        let socket_path = format!("/tmp/graphdb-{}.ipc", port);

        match daemon {
            Some(d) if d.pid != 0 => {
                // Validate PID before attempting to terminate
                if check_pid_validity(d.pid).await {
                    info!("Attempting to stop Storage Daemon on port {} (PID {})...", port, d.pid);
                    println!("===> ATTEMPTING TO STOP STORAGE DAEMON ON PORT {} (PID {})", port, d.pid);

                    // Try SIGTERM first with a longer timeout for graceful shutdown
                    if let Err(e) = kill(Pid::from_raw(d.pid as i32), Signal::SIGTERM) {
                        warn!("Failed to send SIGTERM to PID {} for port {}: {}", d.pid, port, e);
                        println!("===> WARNING: FAILED TO SEND SIGTERM TO PID {} FOR PORT {}", d.pid, port);
                    } else {
                        info!("Sent SIGTERM to PID {} for Storage Daemon on port {}.", d.pid, port);
                        println!("===> SENT SIGTERM TO PID {} FOR STORAGE DAEMON ON PORT {}", d.pid, port);
                    }

                    // Wait for process to exit (up to 5 seconds with retries)
                    let max_attempts = 10;
                    let mut attempts = 0;
                    while check_pid_validity(d.pid).await && attempts < max_attempts {
                        sleep(TokioDuration::from_millis(500)).await;
                        attempts += 1;
                    }

                    // If process is still running, escalate to SIGKILL
                    if check_pid_validity(d.pid).await {
                        warn!("Process (PID {}) on port {} did not exit after SIGTERM. Sending SIGKILL...", d.pid, port);
                        println!("===> WARNING: PROCESS (PID {}) ON PORT {} DID NOT EXIT AFTER SIGTERM. SENDING SIGKILL...", d.pid, port);
                        if let Err(e) = kill(Pid::from_raw(d.pid as i32), Signal::SIGKILL) {
                            warn!("Failed to send SIGKILL to PID {} for port {}: {}", d.pid, port, e);
                            println!("===> WARNING: FAILED TO SEND SIGKILL TO PID {} FOR PORT {}", d.pid, port);
                        } else {
                            info!("Sent SIGKILL to PID {} for Storage Daemon on port {}.", d.pid, port);
                            println!("===> SENT SIGKILL TO PID {} FOR STORAGE DAEMON ON PORT {}", d.pid, port);
                            // Wait briefly after SIGKILL
                            sleep(TokioDuration::from_millis(1000)).await;
                        }
                    } else {
                        info!("Process (PID {}) exited gracefully after SIGTERM.", d.pid);
                        println!("===> PROCESS (PID {}) EXITED GRACEFULLY AFTER SIGTERM.", d.pid);
                    }
                } else {
                    warn!("Stale PID {} found for Storage Daemon on port {}. Cleaning up...", d.pid, port);
                    println!("===> STALE PID {} FOUND FOR STORAGE DAEMON ON PORT {}. CLEANING UP...", d.pid, port);
                }
            }
            Some(_) => {
                warn!("No valid PID found for Storage Daemon on port {}. Cleaning up registry and lock file...", port);
                println!("===> NO VALID PID FOUND FOR STORAGE DAEMON ON PORT {}. CLEANING UP REGISTRY AND LOCK FILE...", port);
            }
            None => {
                info!("No Storage Daemon found on port {} in registry. Checking for stray processes...", port);
                println!("===> NO STORAGE DAEMON FOUND ON PORT {} IN REGISTRY. CHECKING FOR STRAY PROCESSES...", port);
            }
        }

        // Clean up lock file
        if lock_path.exists() {
            RocksDBStorage::force_unlock(&db_path).await
                .context(format!("Failed to clean up lock file at {:?}", lock_path))?;
            info!("Successfully cleaned up lock file at {:?}", lock_path);
            println!("===> SUCCESSFULLY CLEANED UP LOCK FILE AT {:?}", lock_path);
        }

        // Clean up ZMQ socket file
        if std::path::Path::new(&socket_path).exists() {
            std::fs::remove_file(&socket_path)
                .context(format!("Failed to remove ZMQ socket file at {}", socket_path))?;
            info!("Successfully removed ZMQ socket file at {}", socket_path);
            println!("===> SUCCESSFULLY REMOVED ZMQ SOCKET FILE AT {}", socket_path);
        }

        // Remove from registry
        daemon_registry
            .unregister_daemon(port)
            .await
            .context(format!("Failed to remove daemon on port {} from registry", port))?;
        info!("Storage daemon on port {} removed from registry.", port);
        println!("===> STORAGE DAEMON ON PORT {} REMOVED FROM REGISTRY", port);

        // Robust port check with extended retries
        let max_attempts_port_check = 10; // Total 10 seconds wait
        let mut attempts_port_check = 0;
        while !is_port_free(port).await && attempts_port_check < max_attempts_port_check {
            warn!(
                "Port {} still in use after cleanup. Waiting 1000ms before re-check (Attempt {}).",
                port, attempts_port_check + 1
            );
            println!(
                "===> WARNING: PORT {} IS STILL IN USE AFTER CLEANUP. WAITING 1000MS BEFORE RE-CHECK (ATTEMPT {}).",
                port, attempts_port_check + 1
            );

            // Check for stray processes and terminate them
            let system = System::new_with_specifics(RefreshKind::everything().with_processes(ProcessRefreshKind::everything()));
            if let Some(new_pid) = find_pid_by_port(port).await {
                warn!("Stray process (PID {}) found on port {}. Sending SIGKILL...", new_pid, port);
                println!("===> STRAY PROCESS (PID {}) FOUND ON PORT {}. SENDING SIGKILL...", new_pid, port);
                if let Err(e) = kill(Pid::from_raw(new_pid as i32), Signal::SIGKILL) {
                    warn!("Failed to send SIGKILL to stray PID {} for port {}: {}", new_pid, port, e);
                    println!("===> WARNING: FAILED TO SEND SIGKILL TO STRAY PID {} FOR PORT {}", new_pid, port);
                } else {
                    info!("Sent SIGKILL to stray PID {} for port {}.", new_pid, port);
                    println!("===> SENT SIGKILL TO STRAY PID {} FOR PORT {}", new_pid, port);
                }
                sleep(TokioDuration::from_millis(1000)).await;
            }

            sleep(TokioDuration::from_millis(1000)).await;
            attempts_port_check += 1;
        }

        // Final port check
        if is_port_free(port).await {
            info!("Port {} is now free.", port);
            println!("===> PORT {} IS NOW FREE", port);
        } else {
            // Get the new PID and process name for detailed error reporting
            let system = System::new_with_specifics(RefreshKind::everything().with_processes(ProcessRefreshKind::everything()));
            let error_msg = if let Some(new_pid) = find_pid_by_port(port).await {
                let process_name = system
                    .process(NixPid::from_u32(new_pid))
                    .map(|p| p.name().to_string_lossy().to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                format!("Port {} still in use by PID {} ({}) after cleanup", port, new_pid, process_name)
            } else {
                format!("Port {} still in use after cleanup, but no PID found", port)
            };
            warn!("{}", error_msg);
            println!("===> WARNING: {}", error_msg);
            failed_ports.push(error_msg);
        }
    }

    // Clear shutdown_tx and daemon_handle
    let mut shutdown_tx_guard = shutdown_tx.lock().await;
    if shutdown_tx_guard.is_some() {
        if let Some(tx) = shutdown_tx_guard.take() {
            let _ = tx.send(());
            info!("Sent shutdown signal for storage daemon(s).");
            println!("===> SENT SHUTDOWN SIGNAL FOR STORAGE DAEMON(S)");
        }
    }

    let mut daemon_handle_guard = daemon_handle.lock().await;
    if let Some(handle) = daemon_handle_guard.take() {
        let _ = handle.await;
        info!("Daemon handle cleared.");
        println!("===> DAEMON HANDLE CLEARED");
    }

    let mut daemon_port_guard = daemon_port.lock().await;
    *daemon_port_guard = None;
    info!("Daemon port cleared.");
    println!("===> DAEMON PORT CLEARED");

    if failed_ports.is_empty() {
        info!(
            "Storage daemon(s) stopped successfully for port(s): {:?}",
            port.unwrap_or(0)
        );
        println!(
            "===> STORAGE DAEMON(S) STOPPED SUCCESSFULLY FOR PORT(S): {:?}",
            port.unwrap_or(0)
        );
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Failed to stop one or more storage daemons: {:?}", failed_ports
        ))
    }
}

/// Displays status of storage daemons only.
pub async fn display_storage_daemon_status(
    port_arg: Option<u16>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) {
    // Load CLI storage config
    let cli_storage_config = match load_storage_config_from_yaml(None).await {
        Ok(config) => {
            info!("Loaded storage config: {:?}", config);
            Some(config)
        }
        Err(e) => {
            warn!("Failed to load CLI storage config from YAML: {}", e);
            None
        }
    };

    // Get all daemons and filter for storage type
    let all_daemons = GLOBAL_DAEMON_REGISTRY
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default();
    let storage_daemons: Vec<_> = all_daemons.iter().filter(|d| d.service_type == "storage").collect();

    // Collect running ports from cluster configuration
    let mut running_ports = Vec::new();
    if let Some(config) = &cli_storage_config {
        let cluster_ports = parse_port_cluster_range(&config.cluster_range).unwrap_or_default();
        for port in cluster_ports {
            if check_process_status_by_port("Storage Daemon", port).await {
                running_ports.push(port);
            }
        }
    }

    // Include port_arg if specified and running
    if let Some(p) = port_arg {
        if check_process_status_by_port("Storage Daemon", p).await && !running_ports.contains(&p) {
            running_ports.push(p);
        }
    }

    // Update registry for running daemons not present
    if let Some(config) = &cli_storage_config {
        for port in &running_ports {
            if !storage_daemons.iter().any(|d| d.port == *port) {
                warn!("Storage daemon running on port {} but not in registry. Updating metadata...", port);
                let engine_type = config.storage_engine_type.clone();
                let instance_path = config
                    .data_directory
                    .as_ref()
                    .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                    .join(daemon_api_storage_engine_type_to_string(&engine_type).to_lowercase())
                    .join(port.to_string());
                let pid = find_pid_by_port(*port).await.unwrap_or(0);

                let update_metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port: *port,
                    pid,
                    ip_address: "127.0.0.1".to_string(),
                    data_dir: Some(instance_path.clone()),
                    config_path: config.config_root_directory.clone(),
                    engine_type: Some(engine_type.to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    zmq_ready: false,
                };

                if let Err(e) = GLOBAL_DAEMON_REGISTRY.update_daemon_metadata(update_metadata).await {
                    error!("Failed to update daemon metadata on port {}: {}", port, e);
                    println!("===> ERROR: Failed to update daemon metadata on port {}: {}", port, e);
                } else {
                    info!("Successfully updated daemon metadata on port {}", port);
                    println!("===> Successfully updated daemon metadata on port {}", port);
                }
            }
        }
    }

    // Refresh storage daemon list
    let all_daemons = GLOBAL_DAEMON_REGISTRY
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default();
    let storage_daemons: Vec<_> = all_daemons.iter().filter(|d| d.service_type == "storage").collect();

    // Determine ports to display
    let ports_to_display = match port_arg {
        Some(p) => {
            if storage_daemons.iter().any(|d| d.port == p) || running_ports.contains(&p) {
                vec![p]
            } else {
                println!("No Storage Daemon found on port {}.", p);
                vec![]
            }
        }
        None => running_ports.clone(),
    };

    // Display header
    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<50}", "Status", "Port", "Configuration Details");
    println!("{:-<15} {:-<10} {:-<50}", "", "", "");

    if ports_to_display.is_empty() {
        println!("{:<15} {:<10} {:<50}", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for (i, &port) in ports_to_display.iter().enumerate() {
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running"
            } else {
                "Down"
            };

            let metadata = storage_daemons.iter().find(|d| d.port == port);

            // Prioritize configuration for engine type and data path
            let (engine_type_str, data_path_display) = if let Some(config) = &cli_storage_config {
                let engine = config.storage_engine_type.clone();
                let data_path = config
                    .data_directory
                    .as_ref()
                    .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                    .join(daemon_api_storage_engine_type_to_string(&engine).to_lowercase())
                    .join(port.to_string());
                (
                    daemon_api_storage_engine_type_to_string(&engine),
                    data_path.display().to_string(),
                )
            } else if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                let engine = manager.current_engine_type().await;
                let data_path = manager.get_current_engine_data_path().await;
                let data_path_str = data_path.map_or("N/A".to_string(), |p| p.display().to_string());
                (
                    daemon_api_storage_engine_type_to_string(&engine),
                    data_path_str,
                )
            } else {
                warn!("Falling back to daemon metadata for port {}", port);
                let engine = metadata
                    .as_ref()
                    .and_then(|meta| meta.engine_type.clone())
                    .unwrap_or_else(|| "unknown".to_string());
                let data_path_str = metadata
                    .as_ref()
                    .and_then(|meta| meta.data_dir.clone())
                    .map_or("N/A".to_string(), |p| p.display().to_string());
                (engine, data_path_str)
            };

            let pid_info = metadata.map_or("PID: Unknown".to_string(), |m| format!("PID: {}", m.pid));

            if let Some(storage_config) = &cli_storage_config {
                println!("{:<15} {:<10} {:<50}", storage_daemon_status, port, format!("{} | Engine: {}", pid_info, engine_type_str));
                println!("{:<15} {:<10} {:<50}", "", "", "");
                println!("{:<15} {:<10} {:<50}", "", "", "=== Storage Configuration ===");
                println!("{:<15} {:<10} {:<50}", "", "", format!("Data Path: {}", data_path_display));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Host: {}", storage_config.engine_specific_config.as_ref().map_or("N/A", |c| c.storage.host.as_deref().unwrap_or("N/A"))));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Port: {}", port));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Max Open Files: {}", storage_config.max_open_files));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Max Disk Space: {} GB", storage_config.max_disk_space_gb));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Min Disk Space: {} GB", storage_config.min_disk_space_gb));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Use Raft: {}", storage_config.use_raft_for_scale));
            } else {
                println!("{:<15} {:<10} {:<50}", storage_daemon_status, port, format!("{} | Engine: {} (Configuration not loaded)", pid_info, engine_type_str));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Data Path: {}", data_path_display));
            }

            // Print separator between multiple ports
            if ports_to_display.len() > 1 && i < ports_to_display.len() - 1 {
                println!("{:-<15} {:-<10} {:-<50}", "", "", "");
            }
        }
    }

    if port_arg.is_none() && !ports_to_display.is_empty() {
        println!("\nTo check a specific Storage, use 'status storage --port <port>'.");
    }

    *storage_daemon_port_arc.lock().await = ports_to_display.first().copied();
    println!("--------------------------------------------------");
}

pub async fn show_storage() -> Result<()> {
    println!("--- Storage Engine Configuration ---");
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone())).await
        .unwrap_or_else(|e| {
            warn!("Failed to load storage config from {:?}: {}, using default", config_path, e);
            StorageConfig::default()
        });

    // Get the running storage daemon port from the registry
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let storage_daemons: Vec<_> = all_daemons.iter().filter(|d| d.service_type == "storage").collect();
    let daemon_port = if !storage_daemons.is_empty() {
        // Find the first running storage daemon
        let mut running_port = None;
        for daemon in &storage_daemons {
            if check_process_status_by_port("Storage Daemon", daemon.port).await {
                running_port = Some(daemon.port);
                break;
            }
        }
        running_port.unwrap_or(storage_config.default_port)
    } else {
        storage_config.default_port
    };

    // Ensure storage daemon is running
    ensure_storage_daemon_running(
        Some(daemon_port),
        Some(config_path.clone()),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
    ).await?;

    // Get current engine type and data path from StorageEngineManager
    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?;
    let current_engine = manager.current_engine_type().await;
    let current_data_path = manager.get_current_engine_data_path().await;

    // Format engine configuration with the actual daemon port
    let mut engine_config_lines = format_engine_config(&storage_config, daemon_port);
    engine_config_lines.retain(|line| !line.starts_with("Engine:"));

    // Display configuration
    println!("{:<30} {}", "Current Engine", daemon_api_storage_engine_type_to_string(&current_engine));
    println!("{:<30} {}", "Config File", config_path.display());
    println!("{:-<30} {}", "", "");
    println!("{:<30} {}", "Configuration Details", "");
    for line in engine_config_lines {
        println!("{:<30} {}", "", line);
    }

    println!("{:<30} {}", "Data Directory", current_data_path.map_or("N/A".to_string(), |p| p.display().to_string()));
    println!("{:<30} {}", "Log Directory", storage_config.log_directory.map_or("N/A".to_string(), |p| p.display().to_string()));
    println!("{:<30} {}", "Config Root", storage_config.config_root_directory.map_or("N/A".to_string(), |p| p.display().to_string()));
    println!("{:<30} {}", "Default Port", storage_config.default_port);
    println!("{:<30} {}", "Cluster Range", storage_config.cluster_range);
    println!("{:<30} {}", "Use Raft for Scale", storage_config.use_raft_for_scale);
    println!("-----------------------------------");

    Ok(())
}

/// Handles `storage` subcommand for direct CLI execution.
pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file, cluster, storage_port, storage_cluster } => {
            let is_start_storage = storage_port.is_some() || storage_cluster.is_some() || (port.is_some() && storage_port.is_none() && cluster.is_some() && storage_cluster.is_none());
            if is_start_storage {
                if port.is_some() && storage_port.is_some() {
                    log::error!("Cannot specify both --port and --storage-port in `start storage`");
                    return Err(anyhow!("Cannot specify both --port and --storage-port"));
                }
                if cluster.is_some() && storage_cluster.is_some() {
                    log::error!("Cannot specify both --cluster and --storage-cluster in `start storage`");
                    return Err(anyhow!("Cannot specify both --cluster and --storage-cluster"));
                }
            }
            let actual_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let actual_cluster = storage_cluster.or(cluster);
            let actual_config_file = config_file.unwrap_or_else(|| PathBuf::from("./storage_daemon_server/storage_config.yaml"));
            log::info!("Starting storage daemon with port: {:?}, cluster: {:?}, config: {:?}", actual_port, actual_cluster, actual_config_file.display());
            if let Some(cluster) = actual_cluster {
                let ports = parse_cluster_range(&cluster)?;
                log::info!("Parsed cluster range for storage daemon: {:?}", ports);
                for p in ports {
                    start_storage_interactive(
                        Some(p),
                        Some(actual_config_file.clone()),
                        None,
                        None,
                        Arc::new(TokioMutex::new(None)),
                        Arc::new(TokioMutex::new(None)),
                        Arc::new(TokioMutex::new(None)),
                    ).await?;
                }
            } else {
                start_storage_interactive(
                    Some(actual_port),
                    Some(actual_config_file),
                    None,
                    None,
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                ).await?;
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(
                port,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await
        }
        StorageAction::Status { port, cluster } => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_storage_daemon_status(Some(p), Arc::new(TokioMutex::new(None))).await;
                }
            } else {
                display_storage_daemon_status(port, Arc::new(TokioMutex::new(None))).await;
            }
            Ok(())
        }
        StorageAction::StorageQuery => {
            execute_storage_query().await;
            Ok(())
        }
        StorageAction::Show => {
            show_storage().await
        }
        StorageAction::Health => {
            println!("Performing Storage Health Check (simulated, non-interactive mode)...");
            Ok(())
        }
        StorageAction::Version => {
            println!("Retrieving Storage Version (simulated, non-interactive mode)...");
            Ok(())
        }
        StorageAction::List => {
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();
            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}, Engine: {:?}", 
                             daemon.port, daemon.pid, daemon.data_dir, daemon.engine_type);
                }
            }
            Ok(())
        }
    }
}

/// Handles `StorageAction` variants in interactive mode.
pub async fn handle_storage_command_interactive(
    action: StorageAction,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match action {
        StorageAction::Start { port, config_file, cluster, storage_port, storage_cluster } => {
            let is_start_storage = storage_port.is_some() || storage_cluster.is_some() || (port.is_some() && storage_port.is_none() && cluster.is_some() && storage_cluster.is_none());
            if is_start_storage {
                if port.is_some() && storage_port.is_some() {
                    log::error!("Cannot specify both --port and --storage-port in `start storage`");
                    return Err(anyhow!("Cannot specify both --port and --storage-port"));
                }
                if cluster.is_some() && storage_cluster.is_some() {
                    log::error!("Cannot specify both --cluster and --storage-cluster in `start storage`");
                    return Err(anyhow!("Cannot specify both --cluster and --storage-cluster"));
                }
            }
            let actual_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let actual_cluster = storage_cluster.or(cluster);
            let actual_config_file = config_file.unwrap_or_else(|| PathBuf::from("./storage_daemon_server/storage_config.yaml"));
            log::info!("Starting storage daemon interactively with port: {:?}, cluster: {:?}, config: {:?}", actual_port, actual_cluster, actual_config_file.display());
            start_storage_interactive(
                Some(actual_port),
                Some(actual_config_file),
                None,
                actual_cluster,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port, .. } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::StorageQuery => {
            execute_storage_query().await;
            Ok(())
        }
        StorageAction::Health => {
            println!("Performing Storage Health Check (simulated, interactive mode)...");
            Ok(())
        }
        StorageAction::Version => {
            println!("Retrieving Storage Version (simulated, interactive mode)...");
            Ok(())
        }
        StorageAction::Show => {
            show_storage().await
        }
        StorageAction::List => {
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();
            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}, Engine: {:?}", 
                             daemon.port, daemon.pid, daemon.data_dir, daemon.engine_type);
                }
            }
            Ok(())
        }
    }
}

/// Stops a standalone storage daemon. This is the non-interactive version.
pub async fn stop_storage(
    port: Option<u16>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);

    println!("Attempting to stop Storage daemon on port {}...", actual_port);
    stop_process_by_port("Storage Daemon", actual_port).await?;
    println!("Standalone Storage daemon on port {} stopped.", actual_port);
    Ok(())
}

/// Updates the storage engine configuration and applies it via StorageEngineManager.
pub async fn use_storage_engine(engine_type_str: &str, permanent: bool) -> Result<(), anyhow::Error> {
    // Map string to StorageEngineType
    let engine_type = match engine_type_str.to_lowercase().as_str() {
        "sled" => StorageEngineType::Sled,
        "rocksdb" => StorageEngineType::RocksDB,
        "inmemory" => StorageEngineType::InMemory,
        "redis" => StorageEngineType::Redis,
        "postgresql" => StorageEngineType::PostgreSQL,
        "mysql" => StorageEngineType::MySQL,
        _ => return Err(anyhow!("Unknown storage engine: {}", engine_type_str)),
    };
    println!("===> WE GONNA TRY TO CHANGE ENGINE HERE!");

    // Validate engine type against available engines
    let available_engines = StorageEngineManager::available_engines();
    if !available_engines.contains(&engine_type) {
        return Err(anyhow!("Storage engine {} is not enabled. Available engines: {:?}", engine_type_str, available_engines));
    }

    // Load storage configuration
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let mut storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
        .await
        .unwrap_or_else(|_| StorageConfig::default());

    // Update storage_config with the new engine type
    storage_config.storage_engine_type = engine_type.clone();
    // Ensure engine-specific config is initialized
    if storage_config.engine_specific_config.is_none() {
        storage_config.engine_specific_config = Some(create_default_selected_storage_config(&engine_type));
    }

    // Ensure storage daemon is running
    let shutdown_tx_opt = Arc::new(TokioMutex::new(None));
    let handle = Arc::new(TokioMutex::new(None));
    let port_arc = Arc::new(TokioMutex::new(None));
    ensure_storage_daemon_running(
        Some(storage_config.default_port),
        Some(config_path.clone()),
        shutdown_tx_opt.clone(),
        handle,
        port_arc,
    ).await?;

    // Update StorageEngineManager
    info!("Executing use storage command for {} (permanent: {})", engine_type_str, permanent);
    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized. Run 'graphdb-cli storage start' to initialize it."))?;

    manager.use_storage(storage_config.clone(), permanent)
        .await
        .context(format!("Failed to update storage engine to {}", engine_type_str))?;

    // After successfully switching the engine, synchronize the daemon registry with the new state.
    if permanent {
        let port = storage_config.default_port;
        if let Err(e) = sync_daemon_registry_with_manager(port, &storage_config, &shutdown_tx_opt).await {
            warn!("Failed to synchronize daemon registry: {}", e);
        } else {
            info!("Successfully synchronized daemon registry for port {}", port);
            println!("===> Successfully synchronized daemon registry for port {}", port);
        }
    }

    println!("==> Switched to storage engine {} (persisted: {})", engine_type_str, permanent);
    Ok(())
}

pub async fn handle_save_storage() -> Result<()> {
    println!("Saved storage...");
    Ok(())
}

/// Handles the interactive 'reload storage' command.
pub async fn reload_storage_interactive(
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    println!("Reloading standalone Storage daemon...");
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let absolute_config_path = config_path.canonicalize()
        .context(format!("Failed to get absolute path for config file: {}", config_path.display()))?;

    log::info!("Attempting to reload with absolute config path: {:?}", absolute_config_path);

    // Pass the actual port if it's currently known, otherwise None to let it default
    let current_storage_port = storage_daemon_port_arc.lock().await.unwrap_or(DEFAULT_STORAGE_PORT);

    stop_storage_interactive(
        Some(current_storage_port),
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone(),
    ).await?;

    start_storage_interactive(
        Some(current_storage_port),
        Some(absolute_config_path),
        None,
        None,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc,
    ).await?;

    Ok(())
}

async fn force_cleanup_engine_lock(engine: StorageEngineType, data_directory: &Option<PathBuf>) -> Result<()> {
    info!("Attempting to force cleanup lock files for engine: {:?}", engine);

    let engine_path = match data_directory.as_ref() {
        Some(p) => p.join(daemon_api_storage_engine_type_to_string(&engine)),
        None => {
            warn!("Data directory not specified, skipping cleanup for engine: {:?}", engine);
            return Ok(());
        }
    };

    // Determine the lock file path based on the engine type.
    let lock_file_path = match engine {
        StorageEngineType::RocksDB => engine_path.join("LOCK"),
        StorageEngineType::Sled => engine_path.join("db.lck"),
        _ => {
            warn!("No specific cleanup logic for engine: {:?}", engine);
            return Ok(());
        }
    };

    // Check if the directory is writable
    if let Some(parent) = lock_file_path.parent() {
        if !parent.exists() {
            debug!("Creating directory for engine: {:?}", parent);
            fs::create_dir_all(parent)
                .context(format!("Failed to create directory for engine at {:?}", parent))?;
        }
        // Test write access by attempting to create a temporary file
        let test_file = parent.join(".test_write_access");
        match OpenOptions::new().write(true).create(true).open(&test_file) {
            Ok(_) => {
                debug!("Write access confirmed for directory: {:?}", parent);
                let _ = fs::remove_file(&test_file); // Clean up test file
            }
            Err(e) => {
                warn!("No write access to directory {:?}: {}", parent, e);
                return Err(anyhow!("Insufficient permissions to write to directory {:?}", parent));
            }
        }
    }

    // Retry logic for removing the lock file
    let max_attempts = 10;
    let mut attempt = 1;
    let mut delay = Duration::from_millis(200);

    while lock_file_path.exists() && attempt <= max_attempts {
        info!(
            "Attempt {} of {}: Found lingering lock file for {:?} at {:?}. Attempting to remove it.",
            attempt, max_attempts, engine, lock_file_path
        );
        match fs::remove_file(&lock_file_path) {
            Ok(_) => {
                info!("Successfully removed lock file for {:?} at {:?}", engine, lock_file_path);
                return Ok(());
            }
            Err(e) => {
                warn!(
                    "Attempt {} failed to remove lock file for {:?} at {:?}: {}",
                    attempt, engine, lock_file_path, e
                );
                if attempt == max_attempts && engine == StorageEngineType::RocksDB {
                    info!("Attempting RocksDB repair to release stale lock at {:?}", engine_path);
                    let mut opts = Options::default();
                    opts.create_if_missing(true);
                    match DB::repair(&opts, &engine_path) {
                        Ok(_) => {
                            info!("Successfully repaired RocksDB at {:?}", engine_path);
                            return Ok(());
                        }
                        Err(e) => {
                            warn!("Failed to repair RocksDB at {:?}: {}", engine_path, e);
                            return Err(anyhow!("Failed to repair RocksDB or remove lock file for {:?} at {:?} after {} attempts: {}", engine, lock_file_path, max_attempts, e));
                        }
                    }
                }
                // Wait before retrying, with exponential backoff
                tokio::time::sleep(delay).await;
                delay = delay.checked_mul(2).unwrap_or(Duration::from_millis(5000)); // Cap the delay
                attempt += 1;
            }
        }
    }

    if !lock_file_path.exists() {
        info!("No lock file found for {:?} at {:?}", engine, lock_file_path);
    } else {
        return Err(anyhow!("Lock file for {:?} at {:?} still exists after cleanup attempts.", engine, lock_file_path));
    }

    Ok(())
}

/// Ensure a process with the given PID is terminated
async fn ensure_process_terminated(pid: u32) -> Result<()> {
    let mut system = System::new_all();
    let max_attempts = 5;
    let mut attempt = 1;
    
    while attempt <= max_attempts {
        system.refresh_all();
        if let Some(process) = system.process(NixPid::from(pid as usize)) {
            warn!("Process {} still running, attempting to terminate (attempt {})", pid, attempt);
            process.kill();
            tokio::time::sleep(Duration::from_millis(200)).await;
            attempt += 1;
        } else {
            info!("Process {} is no longer running", pid);
            return Ok(());
        }
    }
    Err(anyhow!("Failed to terminate process {} after {} attempts", pid, max_attempts))
}


/// Handles the 'use storage' command for managing the storage daemon.
pub async fn handle_use_storage_command(
    engine: StorageEngineType,
    permanent: bool,
) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();
    info!("=== Starting handle_use_storage_command for engine: {:?}, permanent: {} ===", engine, permanent);
    println!("===> SEE WHAT PERMANENT IS {} SEE WHAT'S ENGINE engine: {:?}", permanent, engine);

    // Load TiKV PD port from storage_config_tikv.yaml if it exists
    let tikv_pd_port = {
        let tikv_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if tikv_config_path.exists() {
            match std::fs::read_to_string(&tikv_config_path) {
                Ok(content) => {
                    match serde_yaml::from_str::<TiKVConfigWrapper>(&content) {
                        Ok(wrapper) => wrapper.storage.pd_endpoints.and_then(|pd| pd.split(':').last().and_then(|p| p.parse::<u16>().ok())),
                        Err(e) => {
                            warn!("Failed to parse TiKV config: {}", e);
                            None
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read TiKV config: {}", e);
                    None
                }
            }
        } else {
            None
        }
    };
    debug!("TiKV PD port from config: {:?}", tikv_pd_port);

    // Capture all existing storage daemons
    let all_existing_storage_daemons = GLOBAL_DAEMON_REGISTRY
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|daemon| daemon.service_type == "storage")
        .collect::<Vec<DaemonMetadata>>();

    let existing_daemon_ports: Vec<u16> = all_existing_storage_daemons.iter().map(|d| d.port).collect();
    info!("Captured existing storage daemons on ports: {:?}", existing_daemon_ports);

    println!("===> USE STORAGE HANDLER - STEP 1");
    let cwd = std::env::current_dir()
        .map_err(|e| anyhow!("Failed to get current working directory: {}", e))?;

    let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let absolute_config_path = cwd.join(&config_path);
    debug!("Attempting to load storage config from {:?}", absolute_config_path);

    // Load configuration
    println!("===> USE STORAGE HANDLER - STEP 2: Loading configuration...");
    let mut current_config: StorageConfig = match load_storage_config_from_yaml(Some(absolute_config_path.clone())).await {
        Ok(config) => {
            info!("Successfully loaded existing storage config: {:?}", config);
            println!("===> Configuration loaded successfully.");
            config
        },
        Err(e) => {
            warn!("Failed to load existing config from {:?}, using default values. Error: {}", absolute_config_path, e);
            println!("===> Configuration file not found, using default settings.");
            StorageConfig::default()
        }
    };
    debug!("Initial loaded config: {:?}", current_config);
    println!(
        "Loaded storage config: default_port={}, cluster_range={}, data_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, log_directory={:?}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        current_config.default_port,
        current_config.cluster_range,
        current_config.data_directory,
        current_config.storage_engine_type,
        current_config.engine_specific_config,
        current_config.log_directory,
        current_config.max_disk_space_gb,
        current_config.min_disk_space_gb,
        current_config.use_raft_for_scale
    );

    // Load engine-specific config
    println!("===> USE STORAGE HANDLER - STEP 3: Loading engine-specific configuration...");
    let config_root_directory = match current_config.config_root_directory.as_ref() {
        Some(path) => {
            println!("===> Using config root directory: {:?}", path);
            path
        },
        None => {
            return Err(anyhow!("'config_root_directory' is missing from the loaded configuration. Check your storage_config.yaml file."));
        }
    };

    let engine_types = vec![StorageEngineType::RocksDB, StorageEngineType::Sled, StorageEngineType::TiKV];
    for other_engine in engine_types.iter().filter(|&e| e != &engine) {
        let stale_path = PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", other_engine.to_string().to_lowercase(), current_config.default_port));
        if stale_path.exists() {
            warn!("Found stale directory for engine {} at {:?}", other_engine, stale_path);
            if let Err(e) = std::fs::remove_dir_all(&stale_path) {
                warn!("Failed to remove stale directory at {:?}: {}", stale_path, e);
            } else {
                info!("Removed stale directory at {:?}", stale_path);
                println!("===> REMOVED STALE {} DIRECTORY AT {:?}", other_engine.to_string().to_uppercase(), stale_path);
            }
        }
    }

    let engine_specific_config = if engine == StorageEngineType::TiKV {
        let tikv_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if tikv_config_path.exists() {
            let content = std::fs::read_to_string(&tikv_config_path)
                .map_err(|e| anyhow!("Failed to read TiKV config file: {}", e))?;
            debug!("TiKV config content: {}", content);
            match serde_yaml::from_str::<TiKVConfigWrapper>(&content) {
                Ok(wrapper) => {
                    let tikv_config = SelectedStorageConfig {
                        storage_engine_type: StorageEngineType::TiKV,
                        storage: StorageConfigInner {
                            path: wrapper.storage.path,
                            host: wrapper.storage.host,
                            port: wrapper.storage.port,
                            username: wrapper.storage.username,
                            password: wrapper.storage.password,
                            pd_endpoints: wrapper.storage.pd_endpoints,
                            database: wrapper.storage.database,
                            use_compression: true,
                            cache_capacity: Some(1024*1024*1024),
                            temporary: false,
                            use_raft_for_scale: false,
                        },
                    };
                    info!("Successfully parsed TiKV config: {:?}", tikv_config);
                    selected_storage_config_to_hashmap(&tikv_config)
                },
                Err(e) => {
                    error!("Failed to parse TiKV config at {:?}: {}. Content: {}", tikv_config_path, e, content);
                    let mut map = HashMap::new();
                    map.insert("storage_engine_type".to_string(), Value::String("tikv".to_string()));
                    map.insert("path".to_string(), Value::String("/opt/graphdb/storage_data/tikv".to_string()));
                    map.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                    map.insert("port".to_string(), Value::Number(2380.into()));
                    map.insert("username".to_string(), Value::String("tikv".to_string()));
                    map.insert("password".to_string(), Value::String("tikv".to_string()));
                    map.insert("pd_endpoints".to_string(), Value::String("127.0.0.1:2379".to_string()));
                    map
                }
            }
        } else {
            warn!("TiKV config file not found at {:?}, using default TiKV configuration", tikv_config_path);
            let mut map = HashMap::new();
            map.insert("storage_engine_type".to_string(), Value::String("tikv".to_string()));
            map.insert("path".to_string(), Value::String("/opt/graphdb/storage_data/tikv".to_string()));
            map.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
            map.insert("port".to_string(), Value::Number(2380.into()));
            map.insert("username".to_string(), Value::String("tikv".to_string()));
            map.insert("password".to_string(), Value::String("tikv".to_string()));
            map.insert("pd_endpoints".to_string(), Value::String("127.0.0.1:2379".to_string()));
            map
        }
    } else {
        let config = lib::config::load_engine_specific_config(engine.clone(), config_root_directory)
            .map_err(|e| anyhow!("Failed to load engine-specific config: {}", e))?;
        let mut map = selected_storage_config_to_hashmap(&config);
        map.insert("storage_engine_type".to_string(), Value::String(engine.to_string().to_lowercase()));
        map
    };
    println!("===> Engine-specific configuration loaded successfully.");

    // Extract port from engine-specific config, prioritizing it over defaults
    let new_port = engine_specific_config.get("port")
        .and_then(|v| match v {
            Value::Number(n) => n.as_u64().map(|p| p as u16),
            Value::String(s) => s.parse::<u16>().ok(),
            _ => None,
        })
        .unwrap_or_else(|| {
            if engine == StorageEngineType::TiKV {
                2380 // TiKV default port
            } else if engine == StorageEngineType::RocksDB {
                current_config.default_port // Use YAML's default_port (8051) for RocksDB
            } else {
                8052 // Default for Sled and other non-TiKV engines
            }
        });

    // Validate port to ensure it’s not reserved for TiKV PD when using non-TiKV engine
    if engine != StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if new_port == pd_port {
                return Err(anyhow!("Selected port {} is reserved for TiKV PD and cannot be used for engine {:?}", new_port, engine));
            }
        }
    }

    let path = engine_specific_config.get("path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", engine.to_string().to_lowercase(), new_port))
        });
    let host = engine_specific_config.get("host")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let username = engine_specific_config.get("username")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if engine == StorageEngineType::TiKV { "tikv".to_string() } else { "".to_string() });
    let password = engine_specific_config.get("password")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if engine == StorageEngineType::TiKV { "tikv".to_string() } else { "".to_string() });
    let pd_endpoints = engine_specific_config.get("pd_endpoints")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if engine == StorageEngineType::TiKV { "127.0.0.1:2379".to_string() } else { "".to_string() });

    let engine_config = SelectedStorageConfig {
        storage_engine_type: engine.clone(),
        storage: StorageConfigInner {
            path: Some(path),
            host: Some(host),
            port: Some(new_port),
            database: engine_specific_config.get("database").and_then(|v| v.as_str()).map(String::from),
            username: Some(username),
            password: Some(password),
            pd_endpoints: if engine == StorageEngineType::TiKV { Some(pd_endpoints) } else { None },
            cache_capacity: Some(1024*1024*1024),
            use_compression: true,
            temporary: false,
            use_raft_for_scale: false,
        },
    };

    // Update new_config with engine-specific values
    let mut new_config = current_config.clone();
    new_config.storage_engine_type = engine.clone();
    new_config.default_port = new_port;
    new_config.engine_specific_config = Some(engine_config.clone());

    // Create updated_engine_config for compatibility
    let mut updated_engine_config = HashMap::new();
    updated_engine_config.insert(
        "path".to_string(),
        Value::String(engine_config.storage.path.as_ref().unwrap().to_string_lossy().to_string())
    );
    updated_engine_config.insert(
        "host".to_string(),
        Value::String(engine_config.storage.host.as_ref().map(|s| s.clone()).unwrap_or_default())
    );
    updated_engine_config.insert(
        "port".to_string(),
        Value::Number(new_port.into())
    );
    updated_engine_config.insert(
        "username".to_string(),
        Value::String(engine_config.storage.username.as_ref().map(|s| s.clone()).unwrap_or_default())
    );
    updated_engine_config.insert(
        "password".to_string(),
        Value::String(engine_config.storage.password.as_ref().map(|s| s.clone()).unwrap_or_default())
    );
    if let Some(pd_endpoints_ref) = engine_config.storage.pd_endpoints.as_ref() {
        updated_engine_config.insert("pd_endpoints".to_string(), Value::String(pd_endpoints_ref.clone()));
    }
    updated_engine_config.insert(
        "storage_engine_type".to_string(),
        Value::String(engine.to_string().to_lowercase())
    );

    debug!("Updated storage config: {:?}", new_config);
    let expected_engine_type = new_config.storage_engine_type.clone();
    let config_port = new_config.default_port;

    println!("===> USE STORAGE HANDLER - STEP 4: Saving and reloading config");
    println!("===> Final new_config before saving: {:?}", new_config);
    if permanent {
        info!("Saving updated storage configuration using StorageConfig::save");
        println!("===> Saving configuration to disk...");
        new_config.save().await.context("Failed to save updated StorageConfig")?;
        debug!("Successfully saved storage configuration to {:?}", absolute_config_path);
        println!("===> Configuration saved successfully.");
    }
    println!("Reloaded storage config for daemon management: {:?}", new_config);

    // Stop all running storage daemons
    println!("===> USE STORAGE HANDLER - STEP 5: Attempting to stop all existing storage daemons...");
    let mut successfully_stopped_ports: Vec<u16> = Vec::new();
    let mut failed_to_stop_ports: Vec<u16> = Vec::new();

    for daemon in all_existing_storage_daemons {
        let current_engine_str = daemon.engine_type.as_deref().unwrap_or("unknown");
        let expected_engine_str = daemon_api_storage_engine_type_to_string(&expected_engine_type);

        if current_engine_str == expected_engine_str && daemon.port == config_port {
            info!("Existing daemon on port {} already uses engine type {}, reusing it.", daemon.port, expected_engine_str);
            successfully_stopped_ports.push(daemon.port);
            println!("Skipping shutdown of Storage Daemon on port {} (already using {}).", daemon.port, expected_engine_str);
            println!("===> Daemon on port {} stopped successfully.", daemon.port);
            continue;
        }

        if let Some(pd_port) = tikv_pd_port {
            if daemon.port == pd_port && current_engine_str == TIKV_DAEMON_ENGINE_TYPE_NAME {
                info!("Skipping shutdown of TiKV PD daemon on port {}. Considered handled.", daemon.port);
                println!("Skipping termination for TiKV PD port {} (process: Storage Daemon).", daemon.port);
                successfully_stopped_ports.push(daemon.port);
                println!("===> Daemon on port {} stopped successfully.", daemon.port);
                continue;
            }
        }

        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", daemon.port);
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            info!("Removing stale ZeroMQ socket file: {}", socket_path);
            tokio::fs::remove_file(&socket_path)
                .await
                .context(format!("Failed to remove stale ZeroMQ socket file {}", socket_path))?;
        }

        let mut is_daemon_stopped = false;
        let pid_file_path = PathBuf::from(STORAGE_PID_FILE_DIR).join(format!("{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, daemon.port));
        let mut last_pid: Option<u32> = None;
        if tokio::fs::File::open(&pid_file_path).await.is_ok() {
            if let Ok(mut file) = tokio::fs::File::open(&pid_file_path).await {
                let mut contents = String::new();
                if file.read_to_string(&mut contents).await.is_ok() {
                    if let Ok(pid) = contents.trim().parse::<u32>() {
                        last_pid = Some(pid);
                    }
                }
            }
        }

        for attempt in 0..MAX_SHUTDOWN_RETRIES {
            debug!("Attempting to stop storage daemon on port {} (Attempt {} of {})", daemon.port, attempt + 1, MAX_SHUTDOWN_RETRIES);

            stop_storage_interactive(
                Some(daemon.port),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await.ok();

            if let Some(pid) = last_pid {
                let pid_nix = nix::unistd::Pid::from_raw(pid as i32);
                if let Err(e) = kill(pid_nix, Signal::SIGTERM) {
                    warn!("Failed to send TERM signal to PID {}: {}", pid, e);
                } else {
                    info!("Sent SIGTERM to PID {} for Storage Daemon on port {}.", pid, daemon.port);
                    println!("Sent SIGTERM to PID {} for Storage Daemon on port {}.", pid, daemon.port);
                }
            }

            if !is_storage_daemon_running(daemon.port).await {
                info!("Storage daemon on port {} is confirmed stopped.", daemon.port);
                println!("Storage Daemon on port {} stopped.", daemon.port);
                is_daemon_stopped = true;
                if pid_file_path.exists() {
                    tokio_fs::remove_file(&pid_file_path).await.ok();
                    println!("Storage daemon on port {} stopped.", daemon.port);
                }
                break;
            }

            info!("Storage daemon still running on port {}, retrying stop in {}ms...", daemon.port, SHUTDOWN_RETRY_DELAY_MS);
            tokio::time::sleep(TokioDuration::from_millis(SHUTDOWN_RETRY_DELAY_MS)).await;
        }

        if is_daemon_stopped {
            successfully_stopped_ports.push(daemon.port);
            println!("===> Daemon on port {} stopped successfully.", daemon.port);
        } else {
            error!("Failed to stop existing storage daemon on port {} after {} attempts.", daemon.port, MAX_SHUTDOWN_RETRIES);
            failed_to_stop_ports.push(daemon.port);
        }
    }

    if failed_to_stop_ports.is_empty() {
        println!("===> All necessary daemons stopped successfully.");
    } else {
        warn!("Failed to stop daemons on ports: {:?}", failed_to_stop_ports);
        println!("===> Failed to stop {} storage daemons.", failed_to_stop_ports.len());
        for port in &failed_to_stop_ports {
            println!("     Port {}: Failed to stop daemon.", port);
        }
    }

    // Initialize StorageEngineManager
    println!("===> USE STORAGE HANDLER - STEP 6: Initializing StorageEngineManager...");
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_none() {
        debug!("StorageEngineManager not initialized, creating new instance");
        println!("===> Creating new instance of StorageEngineManager...");
        let manager = StorageEngineManager::new(
            expected_engine_type.clone(),
            &absolute_config_path,
            permanent,
            Some(config_port),
        )
        .await
        .context("Failed to initialize StorageEngineManager")?;

        debug!(
            "StorageEngineManager created: engine_type={:?}, config_path={:?}, port={:?}",
            expected_engine_type, absolute_config_path, config_port
        );

        let async_manager = AsyncStorageEngineManager::from_manager(manager);
        GLOBAL_STORAGE_ENGINE_MANAGER
            .set(Arc::new(async_manager))
            .context("Failed to set GLOBAL_STORAGE_ENGINE_MANAGER")?;
        println!("===> StorageEngineManager initialized successfully.");
    } else {
        println!("===> Updating existing StorageEngineManager...");
        let async_manager = GLOBAL_STORAGE_ENGINE_MANAGER
            .get()
            .ok_or_else(|| GraphError::ConfigurationError("StorageEngineManager not accessible".to_string()))?;
        debug!("Calling use_storage on existing StorageEngineManager: config={:?}, permanent={}", new_config, permanent);
        async_manager
            .use_storage(new_config.clone(), permanent)
            .await
            .context("Failed to update StorageEngineManager with new engine")?;
        println!("===> StorageEngineManager updated successfully.");
    }

    // Restart storage daemons
    println!("===> USE STORAGE HANDLER - STEP 7: Discovering and restarting storage daemon cluster...");
    let mut daemon_ports_to_restart = successfully_stopped_ports.clone();
    if !daemon_ports_to_restart.contains(&config_port) {
        daemon_ports_to_restart.push(config_port);
    }
    if let Some(pd_port) = tikv_pd_port {
        daemon_ports_to_restart.retain(|&p| p != pd_port);
        info!("Excluding TiKV PD port {} from daemon restart list", pd_port);
    }
    daemon_ports_to_restart.sort();
    daemon_ports_to_restart.dedup();

    let mut successful_restarts: Vec<u16> = Vec::new();
    let mut failed_restarts: Vec<(u16, String)> = Vec::new();

    if daemon_ports_to_restart.is_empty() {
        info!("No storage daemons to restart, starting single daemon on port {}", config_port);
        println!("===> No existing storage cluster found, starting single daemon...");

        let mut port_specific_config = new_config.clone();
        if let Some(ref mut engine_config) = port_specific_config.engine_specific_config {
            engine_config.storage.port = Some(config_port);
            engine_config.storage.path = Some(PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", engine.to_string().to_lowercase(), config_port)));
        }

        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", config_port);
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            info!("Removing stale ZeroMQ socket file: {}", socket_path);
            tokio::fs::remove_file(&socket_path)
                .await
                .context(format!("Failed to remove stale ZeroMQ socket file {}", socket_path))?;
        }

        start_storage_interactive(
            Some(config_port),
            Some(absolute_config_path.clone()),
            Some(port_specific_config),
            Some("skip_manager_set".to_string()),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
        ).await.context("Failed to start storage daemon")?;

        info!("Skipping health check for daemon on port {}", config_port);
        successful_restarts.push(config_port);
        println!("===> ✓ Storage daemon started successfully with {} engine on port {}", 
                 daemon_api_storage_engine_type_to_string(&expected_engine_type), config_port);
    } else {
        info!("Restarting {} storage daemons on ports: {:?}", daemon_ports_to_restart.len(), daemon_ports_to_restart);
        println!("===> Found existing storage cluster on ports: {:?}", daemon_ports_to_restart);
        println!("===> Restarting {} storage daemons with {} engine...", daemon_ports_to_restart.len(), 
                 daemon_api_storage_engine_type_to_string(&expected_engine_type));

        for (index, port) in daemon_ports_to_restart.iter().enumerate() {
            println!("===> Restarting storage daemon {} of {} on port {}...", 
                     index + 1, daemon_ports_to_restart.len(), port);

            let mut port_specific_config = new_config.clone();
            if let Some(ref mut engine_config) = port_specific_config.engine_specific_config {
                engine_config.storage.port = Some(*port);
                engine_config.storage.path = Some(PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", engine.to_string().to_lowercase(), port)));
            }

            let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
            if tokio::fs::metadata(&socket_path).await.is_ok() {
                info!("Removing stale ZeroMQ socket file: {}", socket_path);
                tokio::fs::remove_file(&socket_path)
                    .await
                    .context(format!("Failed to remove stale ZeroMQ socket file {}", socket_path))?;
            }

            match start_storage_interactive(
                Some(*port),
                Some(absolute_config_path.clone()),
                Some(port_specific_config),
                Some("skip_manager_set".to_string()),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await {
                Ok(()) => {
                    info!("Skipping health check for daemon on port {}", port);
                    successful_restarts.push(*port);
                    println!("===> ✓ Storage daemon on port {} restarted successfully", port);
                },
                Err(e) => {
                    failed_restarts.push((*port, e.to_string()));
                    error!("Failed to restart storage daemon on port {}: {}", port, e);
                    println!("===> ✗ Failed to restart storage daemon on port {}: {}", port, e);
                }
            }

            if index < daemon_ports_to_restart.len() - 1 {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            }
        }

        if !successful_restarts.is_empty() {
            info!("Successfully restarted {} storage daemons on ports: {:?}", 
                 successful_restarts.len(), successful_restarts);
            println!("===> ✓ Successfully restarted {} storage daemons with {} engine", 
                     successful_restarts.len(), daemon_api_storage_engine_type_to_string(&expected_engine_type));
        }

        if !failed_restarts.is_empty() {
            warn!("Failed to restart {} storage daemons: {:?}", failed_restarts.len(), failed_restarts);
            println!("===> ✗ Failed to restart {} storage daemons", failed_restarts.len());
            for (port, error) in &failed_restarts {
                println!("     Port {}: {}", port, error);
            }

            if successful_restarts.is_empty() {
                return Err(anyhow!("Failed to restart all storage daemons with new engine"));
            } else {
                warn!("Partial success: {} succeeded, {} failed", successful_restarts.len(), failed_restarts.len());
            }
        }
    }

    // Verify cluster
    println!("===> Verifying storage daemon cluster status...");
    tokio::time::sleep(Duration::from_millis(2000)).await;

    let current_storage_daemons = GLOBAL_DAEMON_REGISTRY
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|daemon| daemon.service_type == "storage")
        .collect::<Vec<DaemonMetadata>>();

    let running_ports: Vec<u16> = current_storage_daemons.iter().map(|d| d.port).collect();
    let expected_engine_str = daemon_api_storage_engine_type_to_string(&expected_engine_type);

    info!("Storage cluster verification: {} daemons running on ports {:?} with engine type {}", 
          current_storage_daemons.len(), running_ports, expected_engine_str);

    let correct_engine_count = current_storage_daemons
        .iter()
        .filter(|daemon| daemon.engine_type.as_deref() == Some(&expected_engine_str))
        .count();

    if correct_engine_count == current_storage_daemons.len() {
        println!("===> ✓ Storage cluster verified: {} daemons running with {} engine on ports {:?}", 
                 current_storage_daemons.len(), expected_engine_str, running_ports);
    } else {
        warn!("Engine type mismatch: {}/{} daemons have correct engine type", 
              correct_engine_count, current_storage_daemons.len());
        println!("===> ⚠ Warning: Some daemons may not have correct engine type");
    }

    info!("Successfully initialized storage engine {:?}", expected_engine_type);
    println!("Switched to storage engine {} (persisted: {})", daemon_api_storage_engine_type_to_string(&expected_engine_type), permanent);
    info!("=== Completed handle_use_storage_command for engine: {:?}, permanent: {}. Elapsed: {}ms ===", engine, permanent, start_time.elapsed().as_millis());

    Ok(())
}

// Helper function to display configuration
pub fn display_config(config: &StorageConfig, config_path: &PathBuf) {
    println!("Current Storage Configuration (from {:?}):", config_path);
    println!("- storage_engine_type: {}", daemon_api_storage_engine_type_to_string(&config.storage_engine_type));
    println!("- config_root_directory: {:?}", config.config_root_directory);
    println!("- data_directory: {:?}", config.data_directory);
    println!("- log_directory: {:?}", config.log_directory);
    println!("- default_port: {}", config.default_port);
    println!("- cluster_range: {}", config.cluster_range);
    println!("- max_disk_space_gb: {}", config.max_disk_space_gb);
    println!("- min_disk_space_gb: {}", config.min_disk_space_gb);
    println!("- use_raft_for_scale: {}", config.use_raft_for_scale);
    println!("- max_open_files: {}", config.max_open_files);

    if let Some(engine_specific) = &config.engine_specific_config {
        println!("- engine_specific_config:");
        if let Some(path) = &engine_specific.storage.path {
            println!("  - path: {:?}", path);
        }
        if let Some(host) = &engine_specific.storage.host {
            println!("  - host: {}", host);
        }
        if let Some(port) = &engine_specific.storage.port {
            println!("  - port: {}", port);
        }
        if let Some(database) = &engine_specific.storage.database {
            println!("  - database: {}", database);
        }
        if let Some(username) = &engine_specific.storage.username {
            println!("  - username: {}", username);
        }
        if let Some(password) = &engine_specific.storage.password {
            println!("  - password: {}", password);
        }
    } else {
        println!("- engine_specific_config: Not available");
    }
}

/// Handles the 'show storage' command. This function
/// displays the current storage configuration by reading it
pub async fn handle_show_storage_command() -> Result<(), GraphError> {
    // Determine the current working directory to resolve config paths
    let cwd = std::env::current_dir()
        .map_err(|e| GraphError::ConfigurationError(format!("Failed to get current working directory: {}", e)))?;
    debug!("Current working directory: {:?}", cwd);

    // Determine the path to the config file
    let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
    let absolute_config_path = cwd.join(&config_path);
    debug!("Attempting to load storage config from {:?}", absolute_config_path);

    // Load the config from the file
    let config = load_storage_config_from_yaml(Some(absolute_config_path.clone())).await
        .map_err(|e| GraphError::ConfigurationError(format!("Failed to load storage config from {:?}: {}", absolute_config_path, e)))?;
    debug!("Loaded config from file: {:?}", config);

    // Check if the daemon is running
    let is_daemon_running = GLOBAL_STORAGE_ENGINE_MANAGER.get().is_some();

    if is_daemon_running {
        info!("Storage daemon is running. Fetching runtime configuration.");
        let manager = GLOBAL_STORAGE_ENGINE_MANAGER
            .get()
            .ok_or_else(|| GraphError::ConfigurationError("StorageEngineManager not initialized".to_string()))?;

        let runtime_config_raw = manager.get_manager().lock().await.get_runtime_config().await
            .map_err(|e| GraphError::ConfigurationError(format!("Failed to get runtime config: {}", e)))?;
        
        let runtime_config: StorageConfig = runtime_config_raw.into();

        // Issue a warning if the runtime engine type differs from the file configuration
        if runtime_config.storage_engine_type != config.storage_engine_type {
            warn!(
                "Daemon engine ({:?}) differs from config file ({:?}). Displaying runtime configuration.",
                runtime_config.storage_engine_type, config.storage_engine_type
            );
        }

        // Display runtime configuration if daemon is running
        println!("Storage Daemon Running on Port {}:", runtime_config.default_port);
        let registry = GLOBAL_DAEMON_REGISTRY.get().await;
        if let Ok(Some(metadata)) = registry.get_daemon_metadata(runtime_config.default_port).await {
            println!("Daemon Status: Running");
            println!("Daemon PID: {}", metadata.pid);
            if let Some(engine_type_str) = &metadata.engine_type {
                println!("Daemon Engine Type (Runtime): {}", engine_type_str);
            }
        } else {
            println!("Daemon Status: Running (No registry metadata available)");
        }
        display_config(&runtime_config, &absolute_config_path);
    } else {
        // Daemon is not running, display file configuration
        println!("Storage daemon is not running.");
        println!("Displaying configuration from storage_config.yaml.");
        display_config(&config, &absolute_config_path);
    }

    Ok(())
}


/// Handles the interactive 'show storage' command.
pub async fn handle_show_storage_command_interactive() -> Result<()> {
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
        .await // Added .await here
        .unwrap_or_else(|e| {
            warn!("Failed to load storage config from {:?}: {}, using default", config_path, e);
            StorageConfig::default()
        });
    // Ensure storage daemon is running
    ensure_storage_daemon_running(
        Some(storage_config.default_port),
        Some(config_path),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
    ).await?;
    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?;
    let engine_type = manager.current_engine_type().await;
    println!("Current Storage Engine (Interactive Mode): {}", daemon_api_storage_engine_type_to_string(&engine_type));
    Ok(())
}

/// Handles the interactive 'use storage' command.
pub async fn handle_use_storage_interactive(
    engine: StorageEngineType,
    permanent: bool,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    handle_use_storage_command(engine.clone(), permanent).await?;
    reload_storage_interactive(
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc,
    ).await?;
    Ok(())
}

/// Handles the "show storage config" command, printing the current configuration.
pub async fn handle_show_storage_config_command() -> Result<(), anyhow::Error> {
    let storage_config = load_storage_config_from_yaml(Some(PathBuf::from("./storage_daemon_server/storage_config.yaml")))
        .await
        .map_err(|e| anyhow!("Failed to load storage config from YAML: {}", e))?;
    // Ensure storage daemon is running
    ensure_storage_daemon_running(
        Some(storage_config.default_port),
        Some(PathBuf::from("./storage_daemon_server/storage_config.yaml")),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
    ).await?;
    let engine_config = EngineStorageConfig {
        storage_engine_type: storage_config.storage_engine_type, // Fixed: removed .unwrap_or()
        data_directory: storage_config.data_directory,
        max_open_files: storage_config.max_open_files,
        max_disk_space_gb: storage_config.max_disk_space_gb,
        min_disk_space_gb: storage_config.min_disk_space_gb,
        engine_specific_config: storage_config.engine_specific_config,
        default_port: storage_config.default_port,
        log_directory: storage_config.log_directory,
        config_root_directory: storage_config.config_root_directory,
        cluster_range: storage_config.cluster_range.to_string(),
        use_raft_for_scale: storage_config.use_raft_for_scale,
    };
    println!("Current Storage Configuration:");
    println!("- storage_engine_type: {:?}", engine_config.storage_engine_type);
    println!(
        "- data_directory: {}",
        engine_config.data_directory.map(|p| p.display().to_string()).unwrap_or("None".to_string())
    );
    println!("- default_port: {}", engine_config.default_port);
    println!(
        "- log_directory: {}",
        engine_config.log_directory.map(|p| p.display().to_string()).unwrap_or("None".to_string())
    );
    println!(
        "- config_root_directory: {}",
        engine_config.config_root_directory.map(|p| p.display().to_string()).unwrap_or("None".to_string())
    );
    println!("- cluster_range: {}", engine_config.cluster_range);
    println!("- use_raft_for_scale: {}", engine_config.use_raft_for_scale);
    println!("- max_open_files: {}", engine_config.max_open_files);
    println!("- max_disk_space_gb: {}", engine_config.max_disk_space_gb);
    println!("- min_disk_space_gb: {}", engine_config.min_disk_space_gb);
    Ok(())
}

/// Ensures the storage daemon is running on the specified port, starting it if necessary.
pub async fn ensure_storage_daemon_running(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let config_path = config_file.unwrap_or_else(|| PathBuf::from("./storage_daemon_server/storage_config.yaml"));
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone())).await
        .map_err(|e| {
            warn!("Failed to load storage config from {:?}: {}", config_path, e);
            anyhow!("Failed to load storage config: {}", e)
        })?;
    let selected_port = port.unwrap_or(storage_config.default_port);
    debug!("Ensuring storage daemon on port {} with config {:?}", selected_port, config_path);

    // Check if the daemon is running and StorageEngineManager is initialized
    if check_process_status_by_port("Storage Daemon", selected_port).await {
        if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            let current_engine = manager.current_engine_type().await;
            debug!("Current engine from StorageEngineManager: {:?}", current_engine);
            info!("Storage daemon already running on port {} with engine {:?}", selected_port, current_engine);
            return Ok(());
        } else {
            warn!("Storage daemon running on port {} but StorageEngineManager not initialized. Restarting.", selected_port);
        }
    }

    // Start the storage daemon
    info!("Starting storage daemon on port {}", selected_port);
    start_storage_interactive(
        Some(selected_port),
        Some(config_path),
        None,
        None,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc,
    ).await?;

    // Verify StorageEngineManager is initialized
    let max_attempts = 5;
    for attempt in 0..max_attempts {
        if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            let current_engine = manager.current_engine_type().await;
            debug!("StorageEngineManager initialized on attempt {} with engine {:?}", attempt + 1, current_engine);
            return Ok(());
        }
        warn!("StorageEngineManager not initialized on attempt {}. Retrying...", attempt + 1);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(anyhow!("Failed to initialize StorageEngineManager after {} attempts", max_attempts))
}


// Helper function to parse cluster range
fn parse_port_cluster_range(range: &str) -> Result<Vec<u16>, anyhow::Error> {
    if range.is_empty() {
        return Ok(vec![]);
    }
    if range.contains('-') {
        let parts: Vec<&str> = range.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid cluster range format: {}", range));
        }
        let start: u16 = parts[0].parse().context("Failed to parse cluster range start")?;
        let end: u16 = parts[1].parse().context("Failed to parse cluster range end")?;
        Ok((start..=end).collect())
    } else {
        let port: u16 = range.parse().context("Failed to parse single port")?;
        Ok(vec![port])
    }
}

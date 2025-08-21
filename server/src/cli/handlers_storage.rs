use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use once_cell::sync::Lazy;
use std::path::{PathBuf, Path};
use std::net::{IpAddr, SocketAddr};
use std::fs::{self, OpenOptions};
use tokio::fs::{remove_file};
use std::io::ErrorKind;
use fs2::FileExt;
use chrono::Utc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use log::{info, debug, warn, error, trace};
use futures::stream::StreamExt;
use serde_json::Value;
use serde_yaml2 as serde_yaml;
use std::time::Instant;
use crate::cli::commands::{CommandType, Commands, StartAction, StorageAction, UseAction};
use crate::cli::config::{
    CliConfig,
    StorageConfig,
    SelectedStorageConfig,
    StorageConfigInner,
    StorageConfigWrapper,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_STORAGE_CONFIG_PATH_POSTGRES,
    DEFAULT_STORAGE_CONFIG_PATH_MYSQL,
    DEFAULT_STORAGE_CONFIG_PATH_REDIS,
    DEFAULT_STORAGE_CONFIG_PATH_SLED,
    DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB,
    DEFAULT_STORAGE_CONFIG_PATH,
    DEFAULT_STORAGE_PORT,
    CliTomlStorageConfig,
    load_storage_config_from_yaml,
    load_engine_specific_config,
    load_cli_config,
    daemon_api_storage_engine_type_to_string,
};
use crate::cli::handlers_utils::{format_engine_config, write_registry_fallback, execute_storage_query, convert_hashmap_to_selected_config};
use daemon_api::start_daemon;
use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::storage_engine::config::{StorageEngineType, StorageConfig as EngineStorageConfig, TikvConfig,
                                  RedisConfig, MySQLConfig, PostgreSQLConfig, RocksdbConfig, SledConfig,
                                 };
use lib::storage_engine::{AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER, 
                          emergency_cleanup_storage_engine_manager};

use crate::cli::daemon_management::{
    is_port_free,
    find_pid_by_port,
    check_process_status_by_port,
    stop_process_by_port,
    parse_cluster_range,
    is_port_in_cluster_range,
    is_storage_daemon_running,
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

/// Helper function to synchronize the daemon registry with the latest config from the StorageEngineManager.
/// This ensures the 'status' command always shows the correct engine type.
async fn sync_daemon_registry_with_manager(port: u16) -> Result<()> {
    trace!("Starting synchronization of daemon registry for port {}", port);

    // First, read the latest configuration from the YAML file, which is the source of truth.
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let storage_config = load_storage_config_from_yaml(Some(config_path))
        .map_err(|e| {
            error!("Failed to load storage config for synchronization: {}", e);
            anyhow!("Failed to load storage config for synchronization: {}", e)
        })?;

    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?;

    // Get the current engine type and data path from the manager
    let engine_type = manager.current_engine_type().await;
    let engine_type_str = daemon_api_storage_engine_type_to_string(&engine_type);
    let data_path = manager.get_current_engine_data_path().await;

    // Get the existing metadata from the registry
    if let Ok(Some(mut metadata)) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await {
        // Update the engine_type and last_seen_nanos fields
        metadata.engine_type = Some(engine_type_str.clone());
        metadata.data_dir = data_path;
        metadata.last_seen_nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        info!("Synchronizing daemon registry: Updating engine type to '{}' for port {}", engine_type_str, port);

        // Re-register the daemon with the updated metadata
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await {
            warn!("Failed to update DaemonMetadata for port {} during sync: {}", port, e);
        } else {
            info!("Successfully synchronized DaemonMetadata engine_type to '{}' for port {}", engine_type_str, port);
        }
    } else {
        warn!("Could not find daemon metadata for port {} to sync. This may not be an issue if the daemon is not running.", port);
    }

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
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
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

pub async fn ensure_storage_daemon_is_running(
    new_config: Option<StorageConfig>,
    port: Option<u16>,
    config_file: Option<PathBuf>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    // Safely unwrap the new_config or return a clear error if it is not provided.
    let config = match new_config {
        Some(conf) => conf,
        None => return Err(anyhow!("A storage configuration is required to ensure the daemon is running.")),
    };

    // Now, we can safely access the fields on the unwrapped `config` object.
    let selected_port = port.unwrap_or(config.default_port);
    let engine_type = config.storage_engine_type.clone();
    // Fix: Clone config_file before unwrapping to avoid move error.
    let config_path = config_file.clone().unwrap_or_else(|| PathBuf::from("./storage_daemon_server/storage_config.yaml"));
    debug!("Ensuring storage daemon on port {} with engine {:?}", selected_port, engine_type);

    // Check if the daemon is running and StorageEngineManager is initialized
    if check_process_status_by_port("Storage Daemon", selected_port).await {
        if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            let current_engine = manager.current_engine_type().await;
            if current_engine == engine_type {
                info!("Storage daemon already running on port {} with engine {:?}, no action needed.", selected_port, current_engine);
                return Ok(());
            } else {
                info!("Stopping storage daemon on port {} with engine {:?} to switch to {:?}", selected_port, current_engine, engine_type);
                // Gracefully stop the daemon to switch engines.
                stop_process_by_port("Storage Daemon", selected_port).await?;
            }
        } else {
            warn!("Storage daemon running on port {} but StorageEngineManager not initialized. Restarting.", selected_port);
            stop_process_by_port("Storage Daemon", selected_port).await?;
        }
    }

    // Start the storage daemon with the provided new_config
    info!("Starting storage daemon on port {} with engine {:?} (attempt 1)", selected_port, engine_type);

    // Call the correct function as requested, passing the required arguments.
    start_storage_interactive(
        Some(selected_port),
        config_file,
        Some(config),
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
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    Err(anyhow!("Failed to initialize StorageEngineManager after {} attempts", max_attempts))
}

pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    new_config: Option<StorageConfig>,
    _cluster_opt: Option<String>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<(), anyhow::Error> {
    // --- STEP 0: CONFIRM FUNCTION EXECUTION ---
    trace!("Entering start_storage_interactive with port: {:?}", port);
    info!("handlers_storage.rs version: 2025-08-16");

    // --- STEP 1: LOAD CLI CONFIGURATION ---
    let command = CommandType::StartStorage {
        port,
        config_file: config_file.clone(),
        cluster: _cluster_opt.clone(),
        storage_port: port,
        storage_cluster: _cluster_opt,
    };
    let cli_config = CliConfig::load(Some(command))
        .with_context(|| "Failed to load CLI configuration. Check file permissions or path.")?;
    println!("SUCCESS: CLI config loaded.");
    trace!("CLI config loaded: {:?}", cli_config);

    // --- STEP 2: DETERMINE CONFIG FILE PATH ---
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
    
    // --- STEP 3: LOAD STORAGE CONFIGURATION ---
    let mut storage_config = if let Some(config) = new_config {
        // New logic: If a config object is provided, use it directly.
        info!("Using provided StorageConfig object, ignoring config_file.");
        config
    } else {
        // Original logic: If no config object is provided, load from file.
        match load_storage_config_from_yaml(Some(config_path.clone())) {
            Ok(config) => {
                debug!("Loaded storage config from {:?}", config_path);
                config
            },
            Err(e) => {
                error!("Failed to load storage config from {:?}: {}", config_path, e);
                if config_path.exists() {
                    match std::fs::read_to_string(&config_path) {
                        Ok(content) => error!("Config file content: {}", content),
                        Err(e) => error!("Config file exists but cannot be read at {:?}: {}", config_path, e),
                    }
                } else {
                    error!("Config file does not exist at {:?}", config_path);
                }
                warn!("Using default StorageConfig due to config load failure");
                StorageConfig::default()
            }
        }
    };

    // If a config root directory isn't specified, derive it from the config file's parent directory.
    if storage_config.config_root_directory.is_none() {
        storage_config.config_root_directory = Some(config_path.parent().unwrap_or(&PathBuf::from("./storage_daemon_server")).to_path_buf());
        debug!("Set config_root_directory to {:?}", storage_config.config_root_directory);
    }
    info!("Loaded Storage Config: {:?}", storage_config);
    info!("Storage engine type from config: {:?}", storage_config.storage_engine_type);

    println!("==> STARTING STORAGE - STEP 3.1");
    // --- STEP 3.1: LOAD ENGINE-SPECIFIC CONFIG IF MISSING ---
    if storage_config.engine_specific_config.is_none() {
        let engine_specific_config_path = match storage_config.storage_engine_type.to_string().to_lowercase().as_str() {
            "rocksdb" => storage_config.config_root_directory.as_ref().map(|p| p.join("storage_config_rocksdb.yaml")),
            "sled" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED)),
            "postgres" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES)),
            "mysql" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL)),
            "redis" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS)),
            _ => None,
        };

        if let Some(engine_config_path) = engine_specific_config_path {
            if engine_config_path.exists() {
                debug!("Loading engine-specific config from {:?}", engine_config_path);
                match load_storage_config_from_yaml(Some(engine_config_path.clone())) {
                    Ok(engine_config) => {
                        // Handle data_directory override case
                        if let Some(data_directory) = engine_config.data_directory {
                            match engine_config.engine_specific_config {
                                Some(mut engine_specific) => {
                                    // Override path with data_directory and use other fields from engine_specific_config
                                    engine_specific.storage.path = Some(data_directory);
                                    storage_config.engine_specific_config = Some(engine_specific);
                                }
                                None => {
                                    // Create new config with just the data_directory path
                                    storage_config.engine_specific_config = Some(SelectedStorageConfig {
                                        storage: StorageConfigInner {
                                            path: Some(data_directory),
                                            host: None,
                                            port: None,
                                            username: None,
                                            password: None,
                                            database: None,
                                        },
                                    });
                                }
                            }
                        } else {
                            // No data_directory, just use engine_specific_config as-is
                            storage_config.engine_specific_config = engine_config.engine_specific_config;
                        }
                        info!("Merged engine-specific config from {:?}", engine_config_path);
                    }
                    Err(e) => {
                        warn!("Failed to load engine-specific config from {:?}: {}. Using existing config.", engine_config_path, e);
                        if engine_config_path.exists() {
                            match std::fs::read_to_string(&engine_config_path) {
                                Ok(content) => warn!("Engine-specific config file content: {}", content),
                                Err(e) => warn!("Engine-specific config file exists but cannot be read at {:?}: {}", engine_config_path, e),
                            }
                        }
                    }
                }
            } else {
                debug!("No engine-specific config found at {:?}", engine_config_path);
            }
        }
    }

    // --- STEP 3.2: HANDLE --permanent FLAG FOR STORAGE ENGINE TYPE ---
    let is_permanent = matches!(
        &cli_config.command,
        Commands::Use(UseAction::Storage { permanent: true, .. })
    );
    if is_permanent {
        debug!("--permanent flag detected, setting storage engine to rocksdb");
        storage_config.storage_engine_type = StorageEngineType::RocksDB;
        info!("Updated storage config to use rocksdb; persistence will be handled by storage engine manager");
    }
    println!("==> STARTING STORAGE - STEP 4");
    // --- STEP 4: DETERMINE PORT ---
    let selected_port = port.or_else(|| {
        match &cli_config.command {
            Commands::Start { action: Some(StartAction::All { storage_port, port: cmd_port, .. }), .. } => storage_port.or(*cmd_port),
            Commands::Start { action: Some(StartAction::Storage { storage_port, port: cmd_port, .. }), .. } => storage_port.or(*cmd_port),
            Commands::Storage(StorageAction::Start { storage_port, port: cmd_port, .. }) => storage_port.or(*cmd_port),
            _ => None,
        }
    }).unwrap_or(storage_config.default_port);

    info!("===> SELECTED PORT {}", selected_port);

    println!("==> STARTING STORAGE - STEP 5");
    // --- STEP 5: START STORAGE DAEMON ---
    let ip = "127.0.0.1";
    let ip_addr: IpAddr = ip.parse().with_context(|| format!("Invalid IP address: {}", ip))?;
    let addr = SocketAddr::new(ip_addr, selected_port);
    info!("Starting storage daemon on {}", addr);

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let is_graphdb_process = all_daemons.iter().any(|d| d.port == selected_port && d.service_type == "storage");

    if is_graphdb_process {
        info!("Port {} is in use by an existing GraphDB storage daemon. Removing stale registry entry.", selected_port);
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", selected_port).await {
            warn!("Failed to remove stale storage daemon from registry: {}", e);
        }
    } else if check_process_status_by_port("Storage Daemon", selected_port).await {
        info!("Port {} is in use by a process. Attempting to stop it.", selected_port);
        stop_process_by_port("Storage Daemon", selected_port).await
            .with_context(|| format!("Failed to stop existing process on port {}", selected_port))?;
    } else {
        info!("No process found running on port {}", selected_port);
    }
    println!("==> STARTING STORAGE - STEP 5.1 {}", storage_config.storage_engine_type);
    if !is_port_free(selected_port).await {
        warn!("Port {} is still in use after cleanup attempt. This may cause issues.", selected_port);
    }

    let engine_type = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);
    debug!("Converted engine_type: {}", engine_type);
    println!("==> STARTING STORAGE - STEP 5.2 {}", engine_type);
    debug!("Calling start_daemon with port: {:?}", Some(selected_port));
    start_daemon(Some(selected_port), None, vec![], "storage").await
        .with_context(|| format!("Failed to start storage daemon on port {}", selected_port))?;

    println!("==> STARTING STORAGE - STEP 6");
    // --- STEP 6: INITIALIZE STORAGE ENGINE MANAGER ---
    trace!("Clearing GLOBAL_STORAGE_ENGINE_MANAGER before initialization");
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
             let mutex = manager.get_manager();
             let mut locked_manager = mutex.lock().await;
             if let Err(e) = locked_manager.shutdown().await {
                 warn!("Failed to shutdown existing StorageEngineManager: {}", e);
             }
         drop(locked_manager); // Explicitly drop to release the mutex
         trace!("Released lock on existing storage engine manager");
    } else {
        trace!("GLOBAL_STORAGE_ENGINE_MANAGER not yet initialized, nothing to clear");
    }

    trace!("Checking current GLOBAL_STORAGE_ENGINE_MANAGER state: {:?}", GLOBAL_STORAGE_ENGINE_MANAGER.get());
    let manager = StorageEngineManager::new(storage_config.storage_engine_type, &config_path, is_permanent).await
        .map_err(|e| {
            error!("Failed to create StorageEngineManager with config path {:?}: {}", config_path, e);
            if config_path.exists() {
                match std::fs::read_to_string(&config_path) {
                    Ok(content) => error!("Config file content: {}", content),
                    Err(e) => error!("Config file exists but cannot be read at {:?}: {}", config_path, e),
                }
            } else {
                error!("Config file does not exist at {:?}", config_path);
            }
            anyhow!("Failed to create StorageEngineManager: {}", e)
        })?;

    // Unwrap Arc<StorageEngineManager> to get StorageEngineManager for from_manager
    let unwrapped_manager = Arc::try_unwrap(manager)
        .map_err(|_| anyhow!("Failed to unwrap Arc<StorageEngineManager>: multiple references exist"))?;
    let async_manager = AsyncStorageEngineManager::from_manager(unwrapped_manager);
    GLOBAL_STORAGE_ENGINE_MANAGER.set(Arc::new(async_manager))
        .map_err(|_| anyhow!("Failed to set StorageEngineManager"))?;
    trace!("GLOBAL_STORAGE_ENGINE_MANAGER has been initialized");
    info!("Initialized StorageEngineManager with engine type: {:?}", storage_config.storage_engine_type);
    trace!("StorageEngineManager initialized with config: {:?}", storage_config);

    // Update DaemonMetadata with correct engine_type and data_dir from StorageEngineManager
    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?;
    let engine_type = manager.current_engine_type().await;
    let current_data_path = manager.get_current_engine_data_path().await;
    let engine_type_str = daemon_api_storage_engine_type_to_string(&engine_type);
    println!("==> STARTING STORAGE - STEP 7");
    // --- STEP 7: REGISTER DAEMON ---
    let pid = match find_pid_by_port(selected_port).await {
        Some(p) if p > 0 => p,
        _ => {
            log::warn!("No valid PID found via lsof for port {}. Checking registry.", selected_port);
            match GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(selected_port).await {
                Ok(Some(metadata)) if metadata.service_type == "storage" => metadata.pid,
                _ => {
                    log::error!("Failed to find PID for Storage Daemon on port {}.", selected_port);
                    return Err(anyhow!("Failed to find PID for Storage Daemon on port {}. Ensure the process is binding to the port.", selected_port));
                }
            }
        }
    };

    let metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port: selected_port,
        pid,
        ip_address: ip.to_string(),
        data_dir: current_data_path,
        config_path: Some(config_path.clone()),
        engine_type: Some(engine_type_str.clone()),
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    async fn retry_operation<F, Fut, T>(operation: F, max_attempts: u8, desc: &str) -> Result<T, anyhow::Error>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
    {
        for attempt in 0..max_attempts {
            match operation().await {
                Ok(result) => {
                    debug!("Successfully completed {} on attempt {}", desc, attempt + 1);
                    return Ok(result);
                }
                Err(e) => {
                    error!("Failed to {} (attempt {}/{}): {}", desc, attempt + 1, max_attempts, e);
                    if attempt + 1 >= max_attempts {
                        return Err(e).context(format!("Failed to {} after {} attempts", desc, max_attempts));
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
        unreachable!()
    }

    let fallback_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.json");
    let fallback_dir = fallback_path.parent().ok_or_else(|| anyhow!("Invalid fallback path: {:?}", fallback_path))?;

    retry_operation(|| async {
        tokio::fs::create_dir_all(fallback_dir).await.map_err(anyhow::Error::from)
    }, 3, "create fallback directory").await?;

    retry_operation(|| async {
        GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await.map_err(anyhow::Error::from)
    }, 3, "register daemon").await?;

    info!("Successfully registered storage daemon with PID {} on port {} with engine type {}", pid, selected_port, engine_type_str);
    println!("==> Successfully registered storage daemon with PID {} on port {}", pid, selected_port);

    let lock_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.lock");
    let new_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry state after registering storage daemon: {:?}", new_daemons);

    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("Failed to open lock file {:?}", lock_path))?;

    retry_operation(|| async {
        lock_file.try_lock_exclusive().map_err(anyhow::Error::from).map(|_| ())
    }, 3, "acquire exclusive lock").await?;

    retry_operation(|| async {
        write_registry_fallback(&new_daemons, &fallback_path).await
    }, 3, "write registry fallback").await?;

    fs2::FileExt::unlock(&lock_file).with_context(|| format!("Failed to release lock on {:?}", lock_path))?;
    let _ = std::fs::remove_file(&lock_path);

    // --- STEP 8: HEALTH CHECK ---
    let health_check_timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(500);
    let start_time = tokio::time::Instant::now();

    while start_time.elapsed() < health_check_timeout {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            info!("Completed storage daemon health check on port {} (PID {}) with engine type {}", selected_port, pid, engine_type_str);
            *storage_daemon_port_arc.lock().await = Some(selected_port);
            let (tx, rx) = oneshot::channel();
            *storage_daemon_shutdown_tx_opt.lock().await = Some(tx);
            let handle = tokio::spawn(async move {
                rx.await.ok();
                info!("Storage daemon on port {} shutting down", selected_port);
            });
            *storage_daemon_handle.lock().await = Some(handle);
            debug!("Completed storage daemon startup");
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    error!("Storage daemon on port {} failed to become reachable within {} seconds", selected_port, health_check_timeout.as_secs());
    Err(anyhow!("Storage daemon failed to start on port {}", selected_port))
}

/// Stops the Storage Daemon on the specified port or all storage daemons if no port is provided.
pub async fn stop_storage_interactive(
    port: Option<u16>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
        let storage_config = load_storage_config_from_yaml(Some(config_path))
            .unwrap_or_else(|_| StorageConfig::default());
        let default_port = if storage_config.default_port != 0 {
            storage_config.default_port
        } else {
            DEFAULT_STORAGE_PORT
        };
        let mut ports = GLOBAL_DAEMON_REGISTRY
            .get_all_daemon_metadata()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|d| d.service_type == "storage")
            .map(|d| d.port)
            .collect::<Vec<u16>>();
        if !ports.contains(&default_port) {
            ports.push(default_port);
        }
        ports
    };

    let mut errors = Vec::new();
    for actual_port in ports_to_stop {
        info!("Attempting to stop storage daemon on port {}", actual_port);

        // Check if the port is managed
        let is_managed_port = storage_daemon_port_arc.lock().await.as_ref() == Some(&actual_port);
        if is_managed_port {
            let mut shutdown_tx_lock = storage_daemon_shutdown_tx_opt.lock().await;
            let mut handle_lock = storage_daemon_handle.lock().await;
            if let Some(tx) = shutdown_tx_lock.take() {
                if tx.send(()).is_ok() {
                    info!("Sent shutdown signal to storage daemon on port {}", actual_port);
                } else {
                    warn!("Failed to send shutdown signal to storage daemon on port {}", actual_port);
                }
            }
            if let Some(handle) = handle_lock.take() {
                if let Err(e) = handle.await {
                    warn!("Failed to join storage daemon handle on port {}: {}", actual_port, e);
                }
                *storage_daemon_port_arc.lock().await = None;
            }
        }

        // Check if the daemon is running
        let is_running = check_process_status_by_port("Storage Daemon", actual_port).await;
        if !is_running {
            info!("No storage daemon running on port {}", actual_port);
            println!("No storage daemon running on port {}.", actual_port);
        } else {
            // Stop the process
            if let Err(e) = stop_process_by_port("Storage Daemon", actual_port).await {
                errors.push(anyhow!("Failed to stop storage daemon process on port {}: {}", actual_port, e));
            }
        }

        // Always attempt to deregister the daemon
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", actual_port).await {
            warn!("Failed to remove storage daemon (port: {}) from registry: {}", actual_port, e);
        } else {
            info!("Successfully removed storage daemon on port {} from registry", actual_port);
        }

        // Verify port is free with retries
        let mut attempts = 0;
        let max_attempts = 7;
        let mut port_free = false;
        while attempts < max_attempts {
            if is_port_free(actual_port).await {
                port_free = true;
                info!("Storage daemon on port {} stopped successfully", actual_port);
                println!("Storage daemon on port {} stopped.", actual_port);
                break;
            }
            debug!("Port {} still in use after attempt {}", actual_port, attempts + 1);
            tokio::time::sleep(Duration::from_millis(500)).await;
            attempts += 1;
        }

        if !port_free {
            errors.push(anyhow!("Port {} is still in use after attempting to stop storage daemon", actual_port));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to stop storage daemon: {:?}", errors))
    }
}

/// Displays status of storage daemons only.
pub async fn display_storage_daemon_status(
    port_arg: Option<u16>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) {
    let cli_storage_config = match load_storage_config_from_yaml(None) {
        Ok(config) => Some(config),
        Err(e) => {
            warn!("Failed to load CLI storage config from YAML: {}", e);
            None
        }
    };

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry contents for storage status: {:?}", all_daemons);

    let storage_daemons: Vec<_> = all_daemons.iter().filter(|d| d.service_type == "storage").collect();

    let ports_to_display = match port_arg {
        Some(p) => {
            if storage_daemons.iter().any(|d| d.port == p) {
                vec![p]
            } else {
                println!("No Storage Daemon found on port {}.", p);
                vec![]
            }
        },
        None => {
            let mut running_ports: Vec<u16> = Vec::new();
            for daemon in &storage_daemons {
                if check_process_status_by_port("Storage Daemon", daemon.port).await {
                    running_ports.push(daemon.port);
                }
            }
            running_ports
        }
    };

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<50}", "Status", "Port", "Configuration Details");
    println!("{:-<15} {:-<10} {:-<50}", "", "", "");

    if ports_to_display.is_empty() {
        println!("{:<15} {:<10} {:<50}", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for &port in &ports_to_display {
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running".to_string()
            } else {
                "Down".to_string()
            };

            let metadata = storage_daemons.iter().find(|d| d.port == port);

            if let Some(storage_config) = &cli_storage_config {
                let mut pid_info = "PID: Unknown".to_string();
                if let Some(meta) = metadata {
                    pid_info = format!("PID: {}", meta.pid);
                }

                let (engine_type_str, data_path_display) = if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                    let engine = manager.current_engine_type().await;
                    let data_path = manager.get_current_engine_data_path().await;
                    let data_path_str = data_path.map_or("N/A".to_string(), |p| p.display().to_string());
                    (daemon_api_storage_engine_type_to_string(&engine), data_path_str)
                } else {
                    warn!("StorageEngineManager not initialized; falling back to daemon metadata.");
                    let engine = metadata.as_ref().and_then(|meta| meta.engine_type.clone()).unwrap_or_else(|| "unknown".to_string());
                    let data_path = metadata.as_ref().and_then(|meta| meta.data_dir.clone());
                    let data_path_str = data_path.map_or("N/A".to_string(), |p| p.display().to_string());
                    (engine, data_path_str)
                };

                println!("{:<15} {:<10} {:<50}", storage_daemon_status, port, format!("{} | Engine: {}", pid_info, engine_type_str));
                println!("{:<15} {:<10} {:<50}", "", "", "");
                println!("{:<15} {:<10} {:<50}", "", "", "=== Storage Configuration ===");
                println!("{:<15} {:<10} {:<50}", "", "", format!("Data Path: {}", data_path_display));

                let mut engine_config_lines = format_engine_config(storage_config);
                engine_config_lines.retain(|line| !line.starts_with("Engine:") && !line.starts_with("Port:") && !line.starts_with("Data Path:"));

                for config_line in engine_config_lines {
                    println!("{:<15} {:<10} {:<50}", "", "", config_line);
                }

                println!("{:<15} {:<10} {:<50}", "", "", "");
                println!("{:<15} {:<10} {:<50}", "", "", "=== Directory Configuration ===");
                let data_dir_display = storage_config.data_directory
                    .as_ref()
                    .map_or("N/A".to_string(), |p| p.display().to_string());
                println!("{:<15} {:<10} {:<50}", "", "", format!("Data Directory: {}", data_dir_display));
                let log_dir_display = storage_config.log_directory
                    .as_ref()
                    .map_or("N/A".to_string(), |p| p.display().to_string());
                println!("{:<15} {:<10} {:<50}", "", "", format!("Log Directory: {}", log_dir_display));

                let config_root_display = storage_config.config_root_directory
                    .as_ref()
                    .map_or("N/A".to_string(), |p| p.display().to_string());
                let final_config_root = if config_root_display == "N/A" {
                    DEFAULT_CONFIG_ROOT_DIRECTORY_STR.to_string()
                } else {
                    config_root_display
                };
                println!("{:<15} {:<10} {:<50}", "", "", format!("Config Root: {}", final_config_root));

                println!("{:<15} {:<10} {:<50}", "", "", "");
                println!("{:<15} {:<10} {:<50}", "", "", "=== Network & Scaling ===");
                println!("{:<15} {:<10} {:<50}", "", "", format!("Default Port: {}", storage_config.default_port));

                let cluster_range = &storage_config.cluster_range;
                if is_port_in_cluster_range(port, cluster_range) {
                    println!("{:<15} {:<10} {:<50}", "", "", format!("Cluster Range: {}", cluster_range));
                } else {
                    println!("{:<15} {:<10} {:<50}", "", "", format!("Cluster Range: {} (Port {} is outside this range!)", cluster_range, port));
                }
                println!("{:<15} {:<10} {:<50}", "", "", format!("Use Raft for Scale: {}", storage_config.use_raft_for_scale));
            } else {
                println!("{:<15} {:<10} {:<50}", "", "", "Configuration not loaded due to error");
            }
            if ports_to_display.len() > 1 && port != *ports_to_display.last().unwrap() {
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

/// Displays the current storage engine configuration.
pub async fn show_storage() -> Result<()> {
    println!("AND I WILL TRY");
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
        .unwrap_or_else(|e| {
            warn!("Failed to load storage config from {:?}: {}, using default", config_path, e);
            StorageConfig::default()
        });

    println!("OF COURSE I WILL TRY");
    // Ensure storage daemon is running
    ensure_storage_daemon_running(
        Some(storage_config.default_port),
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

    // Format engine configuration
    let mut engine_config_lines = format_engine_config(&storage_config);
    engine_config_lines.retain(|line| !line.starts_with("Engine:"));
    println!("NEVER CAME HERE");
    // Display configuration
    println!("\n--- Storage Engine Configuration ---");
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
pub async fn use_storage_engine(engine_type_str: &str, permanent: bool) -> Result<()> {
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



    // Ensure storage daemon is running
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
        .unwrap_or_else(|_| StorageConfig::default());
    ensure_storage_daemon_running(
        Some(storage_config.default_port),
        Some(config_path.clone()),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
    ).await?;
    // Update StorageEngineManager
    info!("Executing use storage command for {} (permanent: {})", engine_type_str, permanent);
    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized. Run 'graphdb-cli storage start' to initialize it."))?;


   
    // After successfully switching the engine, synchronize the daemon registry with the new state.
    if permanent {
        let port = storage_config.default_port;
        if let Err(e) = sync_daemon_registry_with_manager(port).await {
            warn!("Failed to synchronize daemon registry: {}", e);
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

// Assuming the following declarations are correct from storage_engine.rs
// pub static GLOBAL_STORAGE_ENGINE_MANAGER: OnceCell<Arc<AsyncStorageEngineManager>> = OnceCell::const_new();
// const DEFAULT_STORAGE_CONFIG_PATH_RELATIVE: &str = "config/storage.yaml";
// fn ensure_storage_daemon_running(...) -> Result<(), anyhow::Error> { ... }
// fn emergency_cleanup_storage_engine_manager() -> Result<(), anyhow::Error> { ... }
// fn sync_daemon_registry_with_manager(...) -> Result<(), anyhow::Error> { ... }
// fn daemon_api_storage_engine_type_to_string(...) -> String { ... }
// This is the revised version of the `handle_use_storage_command` function.
// This is the revised version of the `handle_use_storage_command` function.
pub async fn handle_use_storage_command(engine: StorageEngineType, permanent: bool) -> Result<(), anyhow::Error> {
    // Acquire intra-process lock
    let _guard = USE_STORAGE_LOCK.lock().await;
    info!("Acquired intra-process USE_STORAGE_LOCK");

    // Acquire the file-based lock. This prevents multiple CLI processes from running
    // this command at the same time.
    let lock = FileLock::acquire().await.context("Failed to acquire process lock")?;

    let start_time = Instant::now();
    info!("=== Starting handle_use_storage_command for engine: {:?}, permanent: {} ===", engine, permanent);
    println!("===> SEE WHAT PERMANENT IS {} SEE WHAT'S ENGINE engine: {:?}", permanent, engine);

    println!("===> USE STORAGE HANDLER - STEP 1");

    // Resolve absolute config path
    let cwd = std::env::current_dir()
        .map_err(|e| anyhow!("Failed to get current working directory: {}", e))?;
    let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let absolute_config_path = cwd.join(&config_path);
    debug!("Attempting to load storage config from {:?}", absolute_config_path);
    println!("===> USE STORAGE HANDLER - STEP 2");

    // Load the existing main configuration file
    let mut current_config: StorageConfig = match load_storage_config_from_yaml(Some(absolute_config_path.clone())) {
        Ok(config) => {
            info!("Successfully loaded existing storage config: {:?}", config);
            config
        },
        Err(e) => {
            warn!("Failed to load existing config from {:?}, using default values. Error: {}", absolute_config_path, e);
            StorageConfig::default()
        }
    };

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
    println!("===> USE STORAGE HANDLER - STEP 3");

    // Load the clean, engine-specific configuration
    let engine_specific_config = load_engine_specific_config(engine, current_config.config_root_directory.as_ref().unwrap())
        .context("Failed to load engine-specific config")?;

    // Create a new StorageConfig instance by merging the two
    let mut new_config = current_config.clone();
    new_config.storage_engine_type = engine;
    let engine_config = convert_hashmap_to_selected_config(engine_specific_config)?;
    // --- CRUCIAL FIX HERE: Update the default_port from the engine-specific config ---
    if let Some(port) = engine_config.storage.port {
        new_config.default_port = port;
    }
    new_config.engine_specific_config = Some(engine_config);
    new_config.cluster_range = new_config.default_port.to_string();
    debug!("Updated storage config: {:?}", new_config);

    // Store all values we'll need BEFORE new_config gets moved
    let expected_engine_type = new_config.storage_engine_type.clone();
    let config_port = new_config.default_port;

    // --- FIX APPLIED HERE ---
    // The previous logic saved the config too late. This crucial change moves
    // the save operation to happen before any daemon management.
    println!("===> USE STORAGE HANDLER - STEP 4: Saving and reloading config");
    // Print the config right before saving to prove it has the correct values.
    println!("===> Final new_config before saving: {:?}", new_config);
    if permanent {
        info!("Saving updated storage configuration using StorageConfig::save");
        new_config.save()
            .context("Failed to save updated StorageConfig")?;
        debug!("Successfully saved storage configuration to {:?}", absolute_config_path);
    }
  

    // Crucial step: Reload the configuration from the file to ensure all subsequent
    // functions (like `is_storage_daemon_running` and `ensure_storage_daemon_running`)
    // use the freshly saved, correct values from the disk. This prevents stale data.
    //let reloaded_config: StorageConfig = load_storage_config_from_yaml(Some(absolute_config_path.clone()))
    //   .context("Failed to reload configuration from file after saving")?;
    
    //let reloaded_config = new_config;
    println!("------------------------------> SEE IT <--------------------------------");
    println!("Reloaded storage config for daemon management: {:?}", new_config);
    // --- END FIX ---

    println!("===> USE STORAGE HANDLER - STEP 5");

    // Step 1: Forcefully stop the existing daemon and ensure it's gone.
    // We use a retry loop to handle a scenario where the daemon might take a moment to
    // be visible to the `stop` command after a previous run.
    const MAX_SHUTDOWN_RETRIES: u32 = 5;
    const SHUTDOWN_RETRY_DELAY_MS: u64 = 1000;
    let mut is_daemon_stopped = false;

    for attempt in 0..MAX_SHUTDOWN_RETRIES {
        // Attempt to stop the daemon
        if let Err(e) = stop_storage_interactive(
            Some(config_port),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
        ).await {
            warn!("Failed to stop storage daemon on attempt {}: {}", attempt + 1, e);
        }

        // Now, we need to poll and wait for the process to truly be gone.
        if !is_storage_daemon_running(config_port).await {
            info!("Storage daemon on port {} is confirmed stopped.", config_port);
            is_daemon_stopped = true;
            break;
        }

        info!("Daemon still running on port {}, retrying stop in {}ms...", config_port, SHUTDOWN_RETRY_DELAY_MS);
        tokio::time::sleep(Duration::from_millis(SHUTDOWN_RETRY_DELAY_MS)).await;
    }

    if !is_daemon_stopped {
        error!("Failed to stop existing storage daemon after {} attempts. The new daemon will likely fail to start due to a locked database.", MAX_SHUTDOWN_RETRIES);
        // We can't proceed safely here.
        return Err(anyhow!("Failed to stop existing storage daemon. Aborting."));
    }

    // Step 2: Initialize or reuse StorageEngineManager after the old daemon is guaranteed to be gone.
    // This part is simplified and a lot of the complex logic is removed.
    let manager = match GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        Some(existing_manager) => {
            // If the manager already exists, we should just use it.
            // The `stop` and `start` logic above handles the daemon lifecycle.
            info!("Reusing existing GLOBAL_STORAGE_ENGINE_MANAGER.");
            Arc::clone(existing_manager)
        },
        None => {
            info!("Initializing new StorageEngineManager.");
            // Pass the reloaded config, not the old `new_config` object.
            let new_manager = StorageEngineManager::new(expected_engine_type.clone(), &absolute_config_path, permanent)
                .await
                .context("Failed to create new StorageEngineManager")?;
            let unwrapped_manager = Arc::try_unwrap(new_manager)
                .map_err(|_| anyhow!("Failed to unwrap Arc: multiple references exist"))?;
            let async_manager = Arc::new(AsyncStorageEngineManager::from_manager(unwrapped_manager));
            GLOBAL_STORAGE_ENGINE_MANAGER.set(async_manager)
                .map_err(|_| anyhow!("Failed to set GLOBAL_STORAGE_ENGINE_MANAGER"))?;
            GLOBAL_STORAGE_ENGINE_MANAGER.get().unwrap().clone()
        }
    };
  
    println!("===> USE STORAGE HANDLER - STEP 6");

    // Start the new daemon with retries
    const MAX_STARTUP_RETRIES: u32 = 5;
    let mut last_error = None;

    for attempt in 0..MAX_STARTUP_RETRIES {
        info!("Starting storage daemon on port {} with engine {:?} (attempt {})", config_port, expected_engine_type, attempt + 1);
        println!("Starting storage daemon on port {} with engine {:?} (attempt {})", config_port, expected_engine_type, attempt + 1);
        match ensure_storage_daemon_is_running(
            Some(new_config.clone()),  // Clone for each attempt to avoid move issues
            Some(config_port),
            Some(absolute_config_path.clone()),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
        ).await {
            Ok(()) => {
                println!("===> USE STORAGE HANDLER - STEP 7");
                last_error = None;
                break;
            },
            Err(e) => {
                warn!("Failed to start storage daemon on attempt {}: {}", attempt + 1, e);
                last_error = Some(e);
                if attempt < MAX_STARTUP_RETRIES - 1 {
                    warn!("Emergency cleanup before retry...");
                    if let Err(cleanup_err) = emergency_cleanup_storage_engine_manager().await {
                        warn!("Emergency cleanup failed: {}", cleanup_err);
                    }
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                }
            }
        }
    }
    if last_error.is_some() {
        return Err(last_error.unwrap().context("Failed to ensure storage daemon is running after retries"));
    }

    // Switch the engine - use expected_engine_type instead of new_config.storage_engine_type
    manager
        .use_storage(expected_engine_type.clone(), permanent)
        .await
        .context("Failed to switch storage engine")?;
    println!("===> USE STORAGE HANDLER - STEP 8");

    // Store port before any potential moves - remove duplicate
    // let config_port = new_config.default_port;  // Already stored above

    // Synchronize daemon registry
    if let Err(e) = sync_daemon_registry_with_manager(config_port).await {
        warn!("Failed to synchronize daemon registry: {}", e);
    }

    // Verify the current engine
    let current_engine = manager.current_engine_type().await;
    debug!("Current engine type after switch: {:?}", current_engine);

    // Use the stored expected_engine_type instead of trying to access moved new_config
    if current_engine != expected_engine_type {
        warn!(
            "Engine mismatch after switch: expected {:?}, found {:?}",
            expected_engine_type, current_engine
        );
        return Err(anyhow!("Failed to switch to correct storage engine"));
    }

    info!("=== Completed handle_use_storage_command for engine: {:?}, permanent: {}. Elapsed: {}ms ===", engine, permanent, start_time.elapsed().as_millis());
    println!("And We Switched to storage engine {} (persisted: {})", daemon_api_storage_engine_type_to_string(&current_engine), permanent);

    // Lock is automatically released by Drop or explicitly here
    lock.release().await?;

    Ok(())
}

pub async fn handle_show_storage_command() -> Result<()> {
    // Log the current working directory
    let cwd = std::env::current_dir()
        .map_err(|e| anyhow!("Failed to get current working directory: {}", e))?;
    debug!("Current working directory: {:?}", cwd);

    // Load the main storage configuration
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let absolute_config_path = cwd.join(&config_path);
    debug!("Attempting to load storage config from {:?}", absolute_config_path);

    let storage_config = match load_storage_config_from_yaml(Some(absolute_config_path.clone())) {
        Ok(config) => {
            debug!("Successfully loaded storage config: {:?}", config);
            config
        },
        Err(e) => {
            error!("Failed to load storage config from {:?}: {}", absolute_config_path, e);
            if absolute_config_path.exists() {
                match fs::read_to_string(&absolute_config_path) {
                    Ok(content) => {
                        error!("Config file content: {}", content);
                        error!("Deserialization failed with error: {}", e);
                    },
                    Err(e) => error!("Config file exists but cannot be read: {}", e),
                }
            } else {
                error!("Config file does not exist at {:?}", absolute_config_path);
            }
            warn!("Using default storage configuration");
            StorageConfig::default()
        },
    };

    // Check if the storage daemon is running
    debug!("Checking if storage daemon is running: GLOBAL_STORAGE_ENGINE_MANAGER.get() = {:?}", GLOBAL_STORAGE_ENGINE_MANAGER.get());
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        // Daemon is running: display the current engine type
        let engine_type = manager.current_engine_type().await;
        println!("Current Storage Engine: {}", daemon_api_storage_engine_type_to_string(&engine_type));
        if engine_type != storage_config.storage_engine_type {
            warn!("Daemon engine ({:?}) differs from config file ({:?})", engine_type, storage_config.storage_engine_type);
        }
    } else {
        // Daemon is not running: display configuration from files
        warn!("Storage daemon is not running. Showing configuration from storage_config.yaml and engine-specific config.");

        println!("Current Storage Configuration:");
        println!("- storage_engine_type: {:?}", storage_config.storage_engine_type);
        println!("- config_root_directory: {:?}", storage_config.config_root_directory);
        println!("- data_directory: {:?}", storage_config.data_directory);
        println!("- log_directory: {:?}", storage_config.log_directory);
        println!("- default_port: {}", storage_config.default_port);
        println!("- cluster_range: {}", storage_config.cluster_range);
        println!("- max_disk_space_gb: {}", storage_config.max_disk_space_gb);
        println!("- min_disk_space_gb: {}", storage_config.min_disk_space_gb);
        println!("- use_raft_for_scale: {}", storage_config.use_raft_for_scale);
        println!("- max_open_files: {}", storage_config.max_open_files.to_string());

        // Load engine-specific configuration
        let engine_specific_config = load_engine_specific_config(storage_config.storage_engine_type.clone(), &absolute_config_path)
            .map_err(|e| {
                warn!("Failed to load engine-specific config for {:?}: {}", storage_config.storage_engine_type, e);
                e
            });

        if let Ok(engine_specific_map) = engine_specific_config {
            let engine_config_value = serde_json::Value::Object(serde_json::Map::from_iter(engine_specific_map.into_iter()));
            match storage_config.storage_engine_type {
                StorageEngineType::Sled => {
                    if let Ok(sled_config) = serde_json::from_value::<SledConfig>(engine_config_value) {
                        println!("- engine_path: {:?}", sled_config.path);
                        if let Some(host) = sled_config.host {
                            println!("- host: {}", host);
                        }
                        if let Some(port) = sled_config.port {
                            println!("- port: {}", port);
                        }
                    } else {
                        warn!("Failed to deserialize SledConfig from engine-specific config");
                        println!("- engine-specific config: Failed to deserialize");
                    }
                },
                StorageEngineType::RocksDB => {
                    if let Ok(rocksdb_config) = serde_json::from_value::<RocksdbConfig>(engine_config_value) {
                        println!("- engine_path: {:?}", rocksdb_config.path);
                        if let Some(host) = rocksdb_config.host {
                            println!("- host: {}", host);
                        }
                        if let Some(port) = rocksdb_config.port {
                            println!("- port: {}", port);
                        }
                    } else {
                        warn!("Failed to deserialize RocksdbConfig from engine-specific config");
                        println!("- engine-specific config: Failed to deserialize");
                    }
                },
                StorageEngineType::TiKV => {
                     if let Ok(tikv_config) = serde_json::from_value::<TikvConfig>(engine_config_value) {
                        println!("- engine_path: {:?}", tikv_config.path);
                        if let Some(host) = tikv_config.host {
                            println!("- host: {}", host);
                        }
                        if let Some(port) = tikv_config.port {
                            println!("- port: {}", port);
                        }
                    } else {
                        warn!("Failed to deserialize TikvConfig from engine-specific config");
                        println!("- engine-specific config: Failed to deserialize");
                    }                   
                }
                StorageEngineType::Redis => {
                    if let Ok(redis_config) = serde_json::from_value::<RedisConfig>(engine_config_value) {
                        println!("- engine_path: {:?}", redis_config.path);
                        if let Some(host) = redis_config.host {
                            println!("- host: {}", host);
                        }
                        if let Some(port) = redis_config.port {
                            println!("- port: {}", port);
                        }
                    } else {
                        warn!("Failed to deserialize RedisConfig from engine-specific config");
                        println!("- engine-specific config: Failed to deserialize");
                    }
                },
                StorageEngineType::MySQL => {
                    if let Ok(mysql_config) = serde_json::from_value::<MySQLConfig>(engine_config_value) {
                        println!("- engine_path: {:?}", mysql_config.path);
                        if let Some(host) = mysql_config.host {
                            println!("- host: {}", host);
                        }
                        if let Some(port) = mysql_config.port {
                            println!("- port: {}", port);
                        }
                    } else {
                        warn!("Failed to deserialize MySQLConfig from engine-specific config");
                        println!("- engine-specific config: Failed to deserialize");
                    }
                },
                StorageEngineType::PostgreSQL => {
                    if let Ok(postgresql_config) = serde_json::from_value::<PostgreSQLConfig>(engine_config_value) {
                        println!("- engine_path: {:?}", postgresql_config.path);
                        if let Some(host) = postgresql_config.host {
                            println!("- host: {}", host);
                        }
                        if let Some(port) = postgresql_config.port {
                            println!("- port: {}", port);
                        }
                    } else {
                        warn!("Failed to deserialize PostgreSQLConfig from engine-specific config");
                        println!("- engine-specific config: Failed to deserialize");
                    }
                },
                StorageEngineType::InMemory => {
                    println!("- engine_path: None (InMemory engine)");
                },
            }
        } else {
            println!("- engine-specific config: Not available");
        }
    }

    Ok(())
}

/// Handles the interactive 'show storage' command.
pub async fn handle_show_storage_command_interactive() -> Result<()> {
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
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
pub async fn handle_show_storage_config_command() -> Result<()> {
    let storage_config = load_storage_config_from_yaml(Some(PathBuf::from("./storage_daemon_server/storage_config.yaml")))
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
        storage_engine_type: storage_config.storage_engine_type,
        data_directory: storage_config.data_directory.unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data")),
        connection_string: None,
        max_open_files: Some(storage_config.max_open_files.try_into().unwrap_or_default()),
        max_disk_space_gb: storage_config.max_disk_space_gb,
        min_disk_space_gb: storage_config.min_disk_space_gb,
        engine_specific_config: match storage_config.engine_specific_config {
            Some(config) => {
                let serialized = serde_json::to_value(&config)
                    .map_err(|e| anyhow!("Failed to serialize engine config: {}", e))?;
                match serialized {
                    Value::Object(map) => Some(map.into_iter().collect()),
                    _ => None,
                }
            },
            None => None,
        },
        default_port: storage_config.default_port,
        log_directory: storage_config.log_directory.unwrap_or_else(|| PathBuf::from("/var/log/graphdb")).display().to_string(),
        config_root_directory: storage_config.config_root_directory.unwrap_or_else(|| PathBuf::from("./storage_daemon_server")),
        cluster_range: storage_config.cluster_range.to_string(),
        use_raft_for_scale: storage_config.use_raft_for_scale,
    };

    println!("Current Storage Configuration:");
    println!("- storage_engine_type: {:?}", engine_config.storage_engine_type);
    println!("- data_directory: {}", engine_config.data_directory.display());
    println!("- default_port: {}", engine_config.default_port);
    println!("- log_directory: {}", engine_config.log_directory);
    println!("- config_root_directory: {}", engine_config.config_root_directory.display());
    println!("- cluster_range: {}", engine_config.cluster_range);
    println!("- use_raft_for_scale: {}", engine_config.use_raft_for_scale);
    println!(
        "- max_open_files: {}",
        engine_config.max_open_files
            .map(|v| v.to_string())
            .unwrap_or_else(|| "None".into())
    );
    println!(
        "- max_disk_space_gb: {}",
        engine_config.max_disk_space_gb
    );
    println!(
        "- min_disk_space_gb: {}",
        engine_config.min_disk_space_gb
    );
    Ok(())
}

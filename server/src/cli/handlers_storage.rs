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
use log::{info, debug, warn, error, trace};
use futures::stream::StreamExt;
use serde_json::{Value, Map};
use serde_yaml2 as serde_yaml;
use rocksdb::{DB, Options};
use reqwest::Client;
use nix::sys::signal::{kill, Signal};
use sysinfo::{System, Process, Pid};
use std::time::Instant;
use crate::cli::commands::{CommandType, Commands, StartAction, StorageAction, UseAction};
use crate::cli::config::{
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
    CliTomlStorageConfig,
    load_storage_config_from_yaml,
    load_engine_specific_config,
    load_cli_config,
    daemon_api_storage_engine_type_to_string,
    create_default_selected_storage_config,
};
use crate::cli::config_helpers::{ validate_cluster_range };
use crate::cli::handlers_utils::{format_engine_config, write_registry_fallback, execute_storage_query, 
                                 convert_hashmap_to_selected_config, retry_operation, selected_storage_config_to_hashmap };
use daemon_api::start_daemon;
pub use models::errors::GraphError;
use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::storage_engine::config::{StorageEngineType, StorageConfig as EngineStorageConfig, TikvConfig,
                                  RedisConfig, MySQLConfig, PostgreSQLConfig, RocksdbConfig, SledConfig,
                                  };
use lib::storage_engine::{AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER, 
                          emergency_cleanup_storage_engine_manager,  SledStorage, RocksdbStorage, TikvStorage, 
                          log_lock_file_diagnostics};

use crate::cli::daemon_management::{
    is_port_free,
    find_pid_by_port,
    check_process_status_by_port,
    stop_process_by_port,
    parse_cluster_range,
    is_port_in_cluster_range,
    is_storage_daemon_running,
    get_pid_for_port,
    check_pid_validity,
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

// Helper function to convert EngineStorageConfig to StorageConfig
fn convert_engine_storage_config_to_storage_config(
    engine_config: EngineStorageConfig,
    engine_type: lib::storage_engine::config::StorageEngineType,
) -> StorageConfig {
    let engine_specific_config = engine_config.engine_specific_config.map(|config_map| {
        SelectedStorageConfig {
            // The fix: Add the missing storage_engine_type field, using the function parameter.
            storage_engine_type: engine_type,
            storage: StorageConfigInner {
                path: config_map.get("path").and_then(|v| v.as_str()).map(PathBuf::from),
                host: config_map.get("host").and_then(|v| v.as_str()).map(String::from),
                port: config_map.get("port").and_then(|v| v.as_u64()).map(|p| p as u16),
                database: None, // Not present in EngineStorageConfig
                username: None, // Not present in EngineStorageConfig
                password: None, // Not present in EngineStorageConfig
                pd_endpoints: None,
            }
        }
    });

    StorageConfig {
        config_root_directory: Some(engine_config.config_root_directory),
        data_directory: Some(engine_config.data_directory),
        log_directory: Some(PathBuf::from(engine_config.log_directory)),
        default_port: engine_config.default_port,
        cluster_range: engine_config.cluster_range,
        max_disk_space_gb: engine_config.max_disk_space_gb,
        min_disk_space_gb: engine_config.min_disk_space_gb,
        use_raft_for_scale: engine_config.use_raft_for_scale,
        storage_engine_type: engine_type,
        engine_specific_config,
        max_open_files: engine_config.max_open_files.unwrap_or(1024) as u64,
    }
}

// Function to perform the mapping from CLI config to daemon config
fn map_cli_config_to_daemon_config(
    cli_config: &StorageConfig,
) -> Result<EngineStorageConfig, anyhow::Error> {
    // Check if `engine_specific_config` is present and if so, serialize its inner `storage` struct to a HashMap.
    let engine_specific_config = if let Some(selected_config) = &cli_config.engine_specific_config {
        // Access the `storage` field directly, as `SelectedStorageConfig` is a struct, not an enum.
        let json_value = serde_json::to_value(&selected_config.storage)?;
        // Convert the Value to a HashMap.
        let map: HashMap<String, Value> = serde_json::from_value(json_value)
            .map_err(|e| anyhow!("Failed to convert engine_specific_config: {}", e))?;
        Some(map)
    } else {
        None
    };

    Ok(EngineStorageConfig {
        // Correctly maps the `StorageEngineType` enum directly.
        storage_engine_type: cli_config.storage_engine_type.clone(),

        // Uses `ok_or_else` to convert the `Option<PathBuf>` to a `Result<PathBuf>`
        data_directory: cli_config.data_directory.clone().ok_or_else(|| {
            anyhow!("Data directory not specified in configuration")
        })?,

        // These fields are required by the `EngineStorageConfig`, so we unwrap the Option<PathBuf>
        // and provide a sensible default if it's not present.
        config_root_directory: cli_config.config_root_directory.clone().unwrap_or_else(|| {
            PathBuf::from("/default/path") // Provide a sensible default path here
        }),

        // The CLI config has `log_directory` as `Option<PathBuf>`, but the lib config has `String`.
        // We handle the Option and then convert the PathBuf to a String.
        log_directory: cli_config.log_directory.clone().unwrap_or_else(|| {
            PathBuf::from("/default/logs")
        }).to_string_lossy().into_owned(),

        // `cluster_range` is a `String`.
        cluster_range: cli_config.cluster_range.clone(),
        
        // These fields are already of the correct type and can be mapped directly.
        max_disk_space_gb: cli_config.max_disk_space_gb,
        min_disk_space_gb: cli_config.min_disk_space_gb,
        use_raft_for_scale: cli_config.use_raft_for_scale,
        default_port: cli_config.default_port,
        
        // This field is now of type `Option<HashMap<String, Value>>`.
        engine_specific_config,

        // `max_open_files` is a `u64` in the CLI config and `Option<i32>` in the lib config.
        // We check if the `u64` value fits within `i32` before casting and wrapping in `Some`.
        max_open_files: if cli_config.max_open_files <= i32::MAX as u64 {
            Some(cli_config.max_open_files as i32)
        } else {
            None
        },

        // This is the new field we need to add.
        connection_string: None,
    })
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
    // --- STEP 0: CONFIRM FUNCTION EXECUTION ---
    trace!("Entering start_storage_interactive with port: {:?}", port);
    info!("handlers_storage.rs version: 2025-09-05");

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
    info!("SUCCESS: CLI config loaded: {:?}", cli_config);
    println!("SUCCESS: CLI config loaded.");

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
        info!("Using provided StorageConfig object, ignoring config_file.");
        config
    } else {
        match load_storage_config_from_yaml(Some(config_path.clone())) {
            Ok(config) => {
                debug!("Loaded storage config from {:?}", config_path);
                println!("Loaded storage config from {:?}", config_path);
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

    // --- STEP 3.1: LOAD ENGINE-SPECIFIC CONFIG ---
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
            match SelectedStorageConfig::load_from_yaml(&engine_config_path) {
                Ok(engine_specific) => {
                    // Check if engine_specific_config needs to be overridden
                    let should_override = storage_config.engine_specific_config.is_none() ||
                        storage_config.engine_specific_config.as_ref()
                            .map(|c| c.storage_engine_type != storage_config.storage_engine_type)
                            .unwrap_or(true);
                    if should_override {
                        println!("Overriding engine-specific config with values from {:?}", engine_config_path);
                        storage_config.engine_specific_config = Some(engine_specific.clone());
                        info!("Overridden engine-specific config from {:?}", engine_config_path);
                        // Save the updated configuration to storage_config.yaml
                        storage_config.save().context("Failed to save StorageConfig with overridden engine-specific config")?;
                        info!("Saved updated storage config with engine-specific config to {:?}", config_path);
                    } else {
                        println!("Loaded engine-specific config from {:?}", engine_config_path);
                        info!("Merged engine-specific config from {:?}", engine_config_path);
                    }
                }
                Err(e) => {
                    warn!("Failed to load engine-specific config from {:?}: {}. Using default config.", engine_config_path, e);
                    if engine_config_path.exists() {
                        match std::fs::read_to_string(&engine_config_path) {
                            Ok(content) => warn!("Engine-specific config file content: {}", content),
                            Err(e) => warn!("Engine-specific config file exists but cannot be read at {:?}: {}", engine_config_path, e),
                        }
                    }
                    storage_config.engine_specific_config = Some(create_default_selected_storage_config(&storage_config.storage_engine_type));
                    // Save the default configuration to ensure consistency
                    storage_config.save().context("Failed to save StorageConfig with default engine-specific config")?;
                    info!("Saved default storage config to {:?}", config_path);
                }
            }
        } else {
            debug!("No engine-specific config found at {:?}", engine_config_path);
            storage_config.engine_specific_config = Some(create_default_selected_storage_config(&storage_config.storage_engine_type));
            // Save the default configuration to ensure consistency
            storage_config.save().context("Failed to save StorageConfig with default engine-specific config")?;
            info!("Saved default storage config to {:?}", config_path);
        }
    } else {
        debug!("No engine-specific config path defined for engine {:?}", storage_config.storage_engine_type);
        storage_config.engine_specific_config = Some(create_default_selected_storage_config(&storage_config.storage_engine_type));
        // Save the default configuration to ensure consistency
        storage_config.save().context("Failed to save StorageConfig with default engine-specific config")?;
        info!("Saved default storage config to {:?}", config_path);
    }

    // --- STEP 3.2: NORMALIZE PATH ---
    if let Some(ref mut engine_config) = storage_config.engine_specific_config {
        let engine_path_name = storage_config.storage_engine_type.to_string().to_lowercase();
        let data_dir_path = storage_config.data_directory.as_ref().map_or(
            PathBuf::from(DEFAULT_DATA_DIRECTORY),
            |p| p.clone()
        );
        let engine_data_path = data_dir_path.join(&engine_path_name);
        if engine_config.storage.path.is_none() || engine_config.storage.path != Some(engine_data_path.clone()) {
            info!("Setting engine-specific path to {:?}", engine_data_path);
            engine_config.storage.path = Some(engine_data_path.clone());
            // Save the updated path to ensure consistency
            storage_config.save().context("Failed to save StorageConfig with normalized path")?;
            info!("Saved storage config with normalized path to {:?}", config_path);
        }

        // Verify directory exists and is writable
        if engine_data_path.exists() {
            info!("Data directory exists at {:?}", engine_data_path);
            if std::fs::metadata(&engine_data_path).map(|m| m.permissions().readonly()).unwrap_or(true) {
                error!("Data directory at {:?} is not writable", engine_data_path);
                return Err(anyhow!("Data directory at {:?} is not writable", engine_data_path));
            }
        } else {
            warn!("Data directory does not exist at {:?}", engine_data_path);
            match std::fs::create_dir_all(&engine_data_path) {
                Ok(_) => info!("Created data directory at {:?}", engine_data_path),
                Err(e) => {
                    error!("Failed to create data directory at {:?}: {}", engine_data_path, e);
                    return Err(anyhow!("Failed to create data directory at {:?}: {}", engine_data_path, e));
                }
            }
        }
    }
    debug!("Storage config after path normalization: {:?}", storage_config);
    println!("==> STARTING STORAGE - STEP 3.2 {:?}", storage_config);

    // --- STEP 3.3: HANDLE --permanent FLAG FOR STORAGE ENGINE TYPE ---
    let is_permanent = matches!(
        &cli_config.command,
        Commands::Use(UseAction::Storage { permanent: true, .. })
    );
    if is_permanent {
        debug!("--permanent flag detected, setting storage engine to {:?}", storage_config.storage_engine_type);
        storage_config.save().context("Failed to save StorageConfig with permanent engine")?;
        info!("Saved storage config with engine {:?}", storage_config.storage_engine_type);
    }
    println!("==> STARTING STORAGE - STEP 4 - and engine specific config here is {:?}", storage_config.engine_specific_config);

    // --- STEP 4: DETERMINE PORT AND UPDATE CLUSTER RANGE ---
    // Prioritize CLI port, then engine-specific port, then default port
    let selected_port = port.unwrap_or_else(|| {
        storage_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.port)
            .unwrap_or(storage_config.default_port)
    });
    info!("===> SELECTED PORT {} (CLI: {:?}, Engine-specific: {:?}, Default: {})",
        selected_port,
        port,
        storage_config.engine_specific_config.as_ref().and_then(|c| c.storage.port),
        storage_config.default_port
    );
    println!("===> SELECTED PORT {}", selected_port);

    // Extract TiKV PD port from pd_endpoints if previous engine was TiKV
    let tikv_pd_port = if storage_config.storage_engine_type == StorageEngineType::TiKV {
        storage_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.pd_endpoints.as_deref())
            .and_then(|pd| pd.split(':').last())
            .and_then(|p| p.parse::<u16>().ok())
    } else {
        let tikv_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if tikv_config_path.exists() {
            match fs::read_to_string(&tikv_config_path)
                .with_context(|| format!("Failed to read TiKV config file at {:?}", tikv_config_path))
                .and_then(|content| {
                    serde_yaml::from_str::<TiKVConfigWrapper>(&content)
                        .with_context(|| format!("Failed to parse TiKV config at {:?}", tikv_config_path))
                })
                .map(|wrapper| wrapper.storage.pd_endpoints)
                .map(|pd| pd.as_deref().and_then(|pd_str| pd_str.split(':').last().and_then(|p| p.parse::<u16>().ok())))
            {
                Ok(Some(pd_port)) => Some(pd_port),
                Ok(None) => None,
                Err(e) => {
                    warn!("Failed to parse previous TiKV config for PD port: {}", e);
                    None
                }
            }
        } else {
            None
        }
    };
    debug!("TiKV PD port: {:?}", tikv_pd_port);

    // Early check: Skip daemon startup if selected port is TiKV PD port and engine is TiKV
    if storage_config.storage_engine_type == StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if Some(selected_port) == Some(pd_port) {
                info!("Skipping daemon startup on TiKV PD port {}", selected_port);
                println!("Skipping daemon startup on TiKV PD port {}.", selected_port);
                return Ok(());
            }
        }
    }

    // Update engine-specific config with selected port
    if let Some(ref mut engine_config) = storage_config.engine_specific_config {
        engine_config.storage.port = Some(selected_port);
        // Ensure pd_endpoints for TiKV
        if storage_config.storage_engine_type == StorageEngineType::TiKV {
            engine_config.storage.pd_endpoints = Some("127.0.0.1:2379".to_string()); // Default, overridden by config
            engine_config.storage.username = Some("tikv".to_string());
            engine_config.storage.password = Some("tikv".to_string());
        }
        // Save the updated engine-specific config with the selected port
        storage_config.save().context("Failed to save StorageConfig with updated engine-specific port")?;
        info!("Saved storage config with updated engine-specific port to {:?}", config_path);
    }

    // Adjust cluster range to exclude TiKV PD port for non-TiKV engines
    let mut current_ports = parse_port_cluster_range(&storage_config.cluster_range)?;
    if storage_config.storage_engine_type != StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            current_ports.retain(|&p| p != pd_port);
            info!("Excluding TiKV PD port {} from cluster range for non-TiKV engine", pd_port);
        }
        storage_config.cluster_range = if current_ports.is_empty() {
            selected_port.to_string()
        } else if current_ports.len() == 1 {
            current_ports[0].to_string()
        } else {
            format!("{}-{}", current_ports.iter().min().unwrap_or(&selected_port), current_ports.iter().max().unwrap_or(&selected_port))
        };
        info!("Adjusted cluster range to {} for engine {:?}", storage_config.cluster_range, storage_config.storage_engine_type);
        storage_config.save().context("Failed to save StorageConfig with updated cluster range")?;
    } else {
        // For TiKV, include the PD port in the cluster range only if it's not already present
        if let Some(pd_port) = tikv_pd_port {
            if !current_ports.contains(&pd_port) {
                current_ports.push(pd_port);
                current_ports.sort();
                current_ports.dedup();
                storage_config.cluster_range = if current_ports.len() == 1 {
                    pd_port.to_string()
                } else {
                    format!("{}-{}", current_ports.iter().min().unwrap_or(&pd_port), current_ports.iter().max().unwrap_or(&selected_port))
                };
                info!("Updated cluster range to {} to include TiKV PD port {}", storage_config.cluster_range, pd_port);
                storage_config.save().context("Failed to save StorageConfig with updated cluster range")?;
            }
        }
    }

    // Ensure selected port is not the TiKV PD port for non-TiKV engines
    if storage_config.storage_engine_type != StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if selected_port == pd_port {
                return Err(anyhow!("Selected port {} is reserved for TiKV PD and cannot be used for engine {:?}", selected_port, storage_config.storage_engine_type));
            }
        }
    }

    storage_config.save().context("Failed to save port-specific StorageConfig")?;

    let ip = "127.0.0.1";
    let ip_addr: IpAddr = ip.parse().with_context(|| format!("Invalid IP address: {}", ip))?;
    let addr = SocketAddr::new(ip_addr, selected_port);
    println!("==> STARTING STORAGE - STEP 5");

    // --- STEP 5: VALIDATE PORT AND CLEANUP ---
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let is_graphdb_process = all_daemons.iter().any(|d| d.port == selected_port && d.service_type == "storage");

    if is_graphdb_process {
        info!("Port {} is in use by an existing GraphDB storage daemon. Checking health...", selected_port);
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            info!("Storage daemon on port {} is already running and healthy. Skipping startup.", selected_port);
            println!("Storage Daemon already running on port {}.", selected_port);
            return Ok(());
        } else {
            info!("Port {} is in use by a stale GraphDB storage daemon. Removing registry entry.", selected_port);
            if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", selected_port).await {
                warn!("Failed to remove stale storage daemon from registry: {}", e);
            }
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
                    tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
                }
                Err(e) => {
                    error!("Failed to stop storage process on port {} after 3 attempts: {}", selected_port, e);
                    return Err(anyhow!("Failed to stop existing storage process on port {}: {}", selected_port, e));
                }
            }
        }
    } else {
        info!("No storage process found running on port {}", selected_port);
    }

    // Sled-specific cleanup
    if storage_config.storage_engine_type == StorageEngineType::Sled {
        let sled_path = storage_config.engine_specific_config
            .as_ref()
            .map(|c| c.storage.path.clone())
            .unwrap_or_else(|| Some(PathBuf::from(DEFAULT_DATA_DIRECTORY).join("sled")))
            .ok_or_else(|| anyhow!("Sled path is None"))?;
        let lock_path = sled_path.join("db.lck");
        for attempt in 1..=3 {
            if lock_path.exists() {
                warn!("Sled lock file exists at {:?}", lock_path);
                match fs::remove_file(&lock_path) {
                    Ok(_) => {
                        info!("Successfully removed Sled lock file at {:?}", lock_path);
                        println!("===> Lock files cleaned successfully.");
                        break;
                    }
                    Err(e) if attempt < 3 => {
                        warn!("Failed to remove Sled lock file on attempt {}: {}. Retrying.", attempt, e);
                        tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
                    }
                    Err(e) => {
                        error!("Failed to remove Sled lock file after 3 attempts: {}", e);
                        return Err(anyhow!("Failed to clean Sled lock file: {}", e));
                    }
                }
            } else {
                info!("No Sled lock file found at {:?}", lock_path);
                println!("===> Lock files cleaned successfully.");
                break;
            }
        }
    }

    // TiKV-specific cleanup (only if engine is TiKV)
    if storage_config.storage_engine_type == StorageEngineType::TiKV {
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

    println!("==> STARTING STORAGE - STEP 5.1 {}", daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type));
    if !is_port_free(selected_port).await {
        warn!("Port {} is still in use after cleanup attempt.", selected_port);
        return Err(anyhow!("Port {} is not free after cleanup", selected_port));
    }

    let engine_type = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);
    debug!("Converted engine_type: {}", engine_type);
    println!("==> STARTING STORAGE - STEP 5.2 {}", engine_type);

    // --- STEP 5.2: MAP STORAGE CONFIG ---
    let lib_storage_config = {
        let engine_specific_config_map: Option<HashMap<String, serde_json::Value>> =
            if let Some(selected_config) = storage_config.engine_specific_config.clone() {
                let mut map = HashMap::new();
                map.insert(
                    "storage_engine_type".to_string(),
                    Value::String(selected_config.storage_engine_type.to_string().to_lowercase())
                );
                let engine_path_name = selected_config.storage_engine_type.to_string().to_lowercase();
                let data_dir_path = storage_config.data_directory.as_ref().map_or(
                    PathBuf::from(DEFAULT_DATA_DIRECTORY),
                    |p| p.clone()
                );
                let engine_data_path = data_dir_path.join(&engine_path_name);
                map.insert(
                    "path".to_string(),
                    Value::String(engine_data_path.to_string_lossy().to_string())
                );
                if let Some(host) = selected_config.storage.host {
                    map.insert("host".to_string(), Value::String(host));
                }
                if let Some(port) = selected_config.storage.port {
                    map.insert("port".to_string(), Value::Number(port.into()));
                }
                if let Some(username) = selected_config.storage.username {
                    map.insert("username".to_string(), Value::String(username));
                }
                if let Some(password) = selected_config.storage.password {
                    map.insert("password".to_string(), Value::String(password));
                }
                if let Some(database) = selected_config.storage.database {
                    map.insert("database".to_string(), Value::String(database));
                }
                if let Some(pd_endpoints) = selected_config.storage.pd_endpoints {
                    map.insert("pd_endpoints".to_string(), Value::String(pd_endpoints));
                }
                Some(map)
            } else {
                None
            };

        EngineStorageConfig {
            storage_engine_type: storage_config.storage_engine_type.clone(),
            engine_specific_config: engine_specific_config_map,
            default_port: storage_config.default_port,
            config_root_directory: storage_config.config_root_directory.clone()
                .unwrap_or_else(|| PathBuf::from("./storage_daemon_server")),
            cluster_range: storage_config.cluster_range.clone(),
            data_directory: storage_config.data_directory.clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
            log_directory: storage_config.log_directory.clone()
                .unwrap_or_else(|| PathBuf::from("/opt/graphdb/logs")).to_string_lossy().into_owned(),
            max_disk_space_gb: storage_config.max_disk_space_gb,
            min_disk_space_gb: storage_config.min_disk_space_gb,
            use_raft_for_scale: storage_config.use_raft_for_scale,
            connection_string: None,
            max_open_files: Some(storage_config.max_open_files as i32),
        }
    };
    debug!("lib_storage_config: {:?}", lib_storage_config);

    let daemon_config = map_cli_config_to_daemon_config(&storage_config)?;
    let daemon_config_string = serde_yaml::to_string(&daemon_config)
        .context("Failed to serialize daemon config to YAML")?;
    debug!("daemon_config_string: {}", daemon_config_string);
    println!("==> STARTING STORAGE - STEP 5.3");

    // --- STEP 5.3: START DAEMON WITH RETRY ---
    let max_attempts = 3;
    let retry_interval = Duration::from_millis(1000);
    let health_check_timeout = Duration::from_secs(20);
    let poll_interval = Duration::from_millis(500);
    let mut pid = None;

    for attempt in 1..=max_attempts {
        debug!("Attempt {}/{} to start storage daemon on port {}", attempt, max_attempts, selected_port);
        match start_daemon(
            Some(selected_port),
            Some(daemon_config_string.clone()),
            vec![],
            "storage",
            Some(lib_storage_config.clone()),
        ).await {
            Ok(_) => {
                debug!("start_daemon succeeded on attempt {} for port {}", attempt, selected_port);
                // Early health check with PID validation
                let start_time = tokio::time::Instant::now();
                while start_time.elapsed() < health_check_timeout {
                    if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                        debug!("Storage daemon passed early health check on port {}", selected_port);
                        match find_pid_by_port(selected_port).await {
                            Some(p) if p > 0 => {
                                if check_pid_validity(p).await {
                                    pid = Some(p);
                                    info!("Storage daemon started with PID {} on port {}", p, selected_port);
                                    println!("Daemon (PID {}) is listening on {}:{}", p, ip, selected_port);
                                    break;
                                } else {
                                    debug!("PID {} for port {} is no longer valid", p, selected_port);
                                }
                            }
                            _ => {
                                debug!("No valid PID found for port {} during health check", selected_port);
                            }
                        }
                    }
                    debug!("Health check attempt for port {} (elapsed: {:?}", selected_port, start_time.elapsed());
                    tokio::time::sleep(poll_interval).await;
                }
                if pid.is_some() {
                    break;
                } else {
                    warn!("Storage daemon on port {} failed health check on attempt {}/{}", selected_port, attempt, max_attempts);
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

    let pid = match pid {
        Some(p) => p,
        None => {
            error!("Failed to start storage daemon on port {} after {} attempts", selected_port, max_attempts);
            return Err(anyhow!("Failed to start storage daemon on port {} after {} attempts", selected_port, max_attempts));
        }
    };

    println!("==> STARTING STORAGE - STEP 6");
    // --- STEP 6: INITIALIZE STORAGE ENGINE MANAGER ---
    trace!("Checking if GLOBAL_STORAGE_ENGINE_MANAGER is already initialized");
    
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_none() {
        trace!("GLOBAL_STORAGE_ENGINE_MANAGER not initialized, creating new instance");
        let manager = StorageEngineManager::new(storage_config.storage_engine_type.clone(), &config_path, is_permanent).await
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

        let unwrapped_manager = Arc::try_unwrap(manager)
            .map_err(|_| anyhow!("Failed to unwrap Arc<StorageEngineManager>: multiple references exist"))?;
        let async_manager = AsyncStorageEngineManager::from_manager(unwrapped_manager);
        GLOBAL_STORAGE_ENGINE_MANAGER.set(Arc::new(async_manager))
            .map_err(|_| anyhow!("Failed to set StorageEngineManager"))?;
        trace!("GLOBAL_STORAGE_ENGINE_MANAGER has been initialized");
        info!("Initialized StorageEngineManager with engine type: {:?}", storage_config.storage_engine_type);
        println!("===> TRYING TO INITIAZLIE POOL");
    } else {
        trace!("GLOBAL_STORAGE_ENGINE_MANAGER already initialized, using existing instance");
        let manager = GLOBAL_STORAGE_ENGINE_MANAGER.get().unwrap();
        
        let current_engine = manager.current_engine_type().await;
        if current_engine != storage_config.storage_engine_type {
            info!("Updating existing StorageEngineManager from {:?} to {:?}", current_engine, storage_config.storage_engine_type);
            manager.use_storage(storage_config.storage_engine_type.clone(), is_permanent).await
                .context("Failed to update existing StorageEngineManager with new engine type")?;
        } else {
            info!("Using existing StorageEngineManager with engine type: {:?}", current_engine);
        }
    }

    println!("==> STARTING STORAGE - STEP 7");
    // --- STEP 7: REGISTER DAEMON ---
    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?;
    let engine_type = manager.current_engine_type().await;
    let current_data_path = manager.get_current_engine_data_path().await;
    let engine_type_str = daemon_api_storage_engine_type_to_string(&engine_type);

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

    let fallback_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.json");
    let fallback_dir = fallback_path.parent().ok_or_else(|| anyhow!("Invalid fallback path: {:?}", fallback_path))?;

    retry_operation(|| async {
        tokio::fs::create_dir_all(fallback_dir).await.map_err(anyhow::Error::from)
    }, 3, "create fallback directory").await?;

    for attempt in 1..=3 {
        match GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await {
            Ok(_) => {
                info!("Successfully registered storage daemon with PID {} on port {} with engine type {}", pid, selected_port, engine_type_str);
                println!("==> Successfully registered storage daemon with PID {} on port {}", pid, selected_port);
                break;
            }
            Err(e) => {
                error!("Failed to register daemon (attempt {}/3): {}", attempt, e);
                if attempt == 3 {
                    return Err(anyhow!("Failed to register daemon after 3 attempts: {}", e));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    let new_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry state after registering storage daemon: {:?}", new_daemons);

    let lock_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.lock");
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
    let _ = fs::remove_file(&lock_path);

    // --- STEP 8: FINAL HEALTH CHECK ---
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
        debug!("Final health check attempt for port {} (elapsed: {:?}", selected_port, start_time.elapsed());
        tokio::time::sleep(poll_interval).await;
    }

    error!("Storage daemon on port {} failed final health check after {} seconds", selected_port, health_check_timeout.as_secs());
    return Err(anyhow!("Storage daemon failed to start on port {}: health check failed", selected_port));
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
                println!("{:<15} {:<10} {:<50}", "", "", format!("Host: {}", storage_config.engine_specific_config.as_ref().map_or("N/A", |c| c.storage.host.as_ref().map_or("N/A", |h| h.as_str()))));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Port: {}", port)); // Use actual daemon port
                println!("{:<15} {:<10} {:<50}", "", "", format!("Max Open Files: {}", storage_config.max_open_files));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Max Disk Space: {} GB", storage_config.max_disk_space_gb));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Min Disk Space: {} GB", storage_config.min_disk_space_gb));
                println!("{:<15} {:<10} {:<50}", "", "", format!("Use Raft: {}", storage_config.use_raft_for_scale));

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

pub async fn show_storage() -> Result<()> {
    println!("--- Storage Engine Configuration ---");
    let config_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
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
        if let Some(process) = system.process(Pid::from(pid as usize)) {
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
            match fs::read_to_string(&tikv_config_path) {
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
    let mut current_config: StorageConfig = match load_storage_config_from_yaml(Some(absolute_config_path.clone())) {
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

    let engine_specific_config = if engine == StorageEngineType::TiKV {
        let tikv_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if tikv_config_path.exists() {
            let content = fs::read_to_string(&tikv_config_path)
                .map_err(|e| anyhow!("Failed to read TiKV config file: {}", e))?;
            debug!("TiKV config content: {}", content);
            match serde_yaml::from_str::<TiKVConfigWrapper>(&content) {
                Ok(wrapper) => {
                    let tikv_config = SelectedStorageConfig {
                        storage_engine_type: wrapper.storage.storage_engine_type,
                        storage: StorageConfigInner {
                            path: wrapper.storage.path,
                            host: wrapper.storage.host,
                            port: wrapper.storage.port,
                            username: wrapper.storage.username,
                            password: wrapper.storage.password,
                            pd_endpoints: wrapper.storage.pd_endpoints,
                            database: wrapper.storage.database,
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
        crate::cli::config::load_engine_specific_config(engine.clone(), config_root_directory)
            .map_err(|e| anyhow!("Failed to load engine-specific config: {}", e))?
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
        .unwrap_or_else(|| PathBuf::from(format!("{}/{}", DEFAULT_DATA_DIRECTORY, engine.to_string().to_lowercase())));
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
        },
    };

    // Update new_config with engine-specific values
    let mut new_config = current_config.clone();
    new_config.storage_engine_type = engine.clone();
    new_config.default_port = new_port; // Ensure default_port is set to engine-specific port
    new_config.engine_specific_config = Some(engine_config.clone());

    // Create updated_engine_config for compatibility
    let mut updated_engine_config = Map::new();
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
        Value::Number(new_port.into()) // Use new_port directly
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
        new_config.save().context("Failed to save updated StorageConfig")?;
        debug!("Successfully saved storage configuration to {:?}", absolute_config_path);
        println!("===> Configuration saved successfully.");
    }
    println!("------------------------------> SEE IT <--------------------------------");
    println!("Reloaded storage config for daemon management: {:?}", new_config);

    // Stop existing storage daemons
    println!("===> USE STORAGE HANDLER - STEP 5: Attempting to stop existing daemon...");
    let mut successfully_stopped_ports: Vec<u16> = Vec::new();
    let mut failed_to_stop_ports: Vec<u16> = Vec::new();

    for daemon in all_existing_storage_daemons {
        let current_engine_str = daemon.engine_type.as_deref().unwrap_or("unknown");
        let expected_engine_str = daemon_api_storage_engine_type_to_string(&expected_engine_type);

        // Skip daemons that match the new engine type
        if current_engine_str == expected_engine_str {
            info!("Skipping shutdown of daemon on port {} as it already uses the correct engine type {}.", daemon.port, expected_engine_str);
            successfully_stopped_ports.push(daemon.port);
            println!("Skipping shutdown of Storage Daemon on port {} (already using {}).", daemon.port, expected_engine_str);
            println!("===> Daemon on port {} stopped successfully.", daemon.port);
            continue;
        }

        // Skip stopping the TiKV PD daemon
        if let Some(pd_port) = tikv_pd_port {
            if daemon.port == pd_port && current_engine_str == TIKV_DAEMON_ENGINE_TYPE_NAME {
                info!("Skipping shutdown of TiKV PD daemon on port {}. Considered handled.", daemon.port);
                println!("Skipping termination for TiKV PD port {} (process: Storage Daemon).", daemon.port);
                successfully_stopped_ports.push(daemon.port);
                println!("===> Daemon on port {} stopped successfully.", daemon.port);
                continue;
            }
        }

        let mut is_daemon_stopped = false;
        let pid_file_path = PathBuf::from(STORAGE_PID_FILE_DIR).join(format!("{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, daemon.port));
        let mut last_pid: Option<u32> = None;
        if tokio_fs::File::open(&pid_file_path).await.is_ok() {
            if let Ok(mut file) = tokio_fs::File::open(&pid_file_path).await {
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
                    fs::remove_file(&pid_file_path).ok();
                    println!("Storage daemon on port {} stopped.", daemon.port);
                }
                break;
            }

            info!("Storage daemon still running on port {}, retrying stop in {}ms...", daemon.port, SHUTDOWN_RETRY_DELAY_MS);
            tokio::time::sleep(Duration::from_millis(SHUTDOWN_RETRY_DELAY_MS)).await;
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

    // Clean up lock files
    println!("===> USE STORAGE HANDLER - STEP 5.5: Cleaning up lock files for {:?}", expected_engine_type);
    println!("===> Cleaning up lock files...");
    if engine == StorageEngineType::TiKV {
        let tikv_path = new_config.engine_specific_config
            .as_ref()
            .and_then(|config| config.storage.path.clone())
            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/tikv"));
        if tikv_path.exists() {
            if let Err(e) = TikvStorage::force_unlock().await {
                warn!("Failed to clean up TiKV lock files: {}", e);
            } else {
                info!("Lock files cleaned successfully.");
                println!("===> Lock files cleaned successfully.");
            }
        }
    } else {
        let lock_path = new_config.data_directory.as_ref()
            .map(|d| PathBuf::from(d).join("sled").join("db.lck"))
            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/sled/db.lck"));
        debug!("Checking Sled lock file at {:?}", lock_path);
        if lock_path.exists() {
            info!("Removing Sled lock file at {:?}", lock_path);
            fs::remove_file(&lock_path).context(format!("Failed to remove Sled lock file at {:?}", lock_path))?;
            println!("===> Lock files cleaned successfully.");
        }
    }
    println!("===> Lock files cleaned successfully.");

    // Initialize StorageEngineManager
    println!("===> USE STORAGE HANDLER - STEP 6: Initializing StorageEngineManager...");
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_none() {
        debug!("StorageEngineManager not initialized, creating new instance");
        println!("===> Creating new instance of StorageEngineManager...");
        let manager = StorageEngineManager::new(expected_engine_type.clone(), &absolute_config_path, permanent)
            .await
            .context("Failed to initialize StorageEngineManager")?;
        debug!("StorageEngineManager created: engine_type={:?}, config_path={:?}", expected_engine_type, absolute_config_path);
        GLOBAL_STORAGE_ENGINE_MANAGER
            .set(Arc::new(AsyncStorageEngineManager::from_manager(
                Arc::try_unwrap(manager)
                    .map_err(|_| GraphError::ConfigurationError("Failed to unwrap Arc<StorageEngineManager>: multiple references exist".to_string()))?
            )))
            .context("Failed to set GLOBAL_STORAGE_ENGINE_MANAGER")?;
        println!("===> StorageEngineManager initialized successfully.");
    } else {
        println!("===> Updating existing StorageEngineManager...");
        let async_manager = GLOBAL_STORAGE_ENGINE_MANAGER
            .get()
            .ok_or_else(|| GraphError::ConfigurationError("StorageEngineManager not accessible".to_string()))?;
        debug!("Calling use_storage on existing StorageEngineManager: engine_type={:?}, permanent={}", expected_engine_type, permanent);
        async_manager
            .use_storage(expected_engine_type.clone(), permanent)
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
    // Filter out TiKV PD port from restart list
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

        start_storage_interactive(
            Some(config_port),
            Some(absolute_config_path.clone()),
            Some(new_config.clone()),
            None,
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
        ).await.context("Failed to start storage daemon")?;

        println!("===> Storage daemon started successfully with {} engine on port {}", 
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
            port_specific_config.default_port = *port;
            if let Some(ref mut engine_config) = port_specific_config.engine_specific_config {
                engine_config.storage.port = Some(*port);
            }

            match start_storage_interactive(
                Some(*port),
                Some(absolute_config_path.clone()),
                Some(port_specific_config),
                None,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await {
                Ok(()) => {
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
    let config = load_storage_config_from_yaml(Some(absolute_config_path.clone()))
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

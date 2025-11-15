use anyhow::{Result, Context, anyhow};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use once_cell::sync::Lazy;
use std::path::{PathBuf, Path};
use std::net::{IpAddr, SocketAddr};
use std::fs::{self, OpenOptions, File};
use std::collections::HashMap;
use tokio::fs::{self as tokio_fs, remove_file};
use std::io::Write; // <-- this is the missing import
use tokio::io::{self, AsyncReadExt}; // Add this import at the top of the file
use std::io::ErrorKind;
use fs2::FileExt;
use chrono::Utc;
use std::os::unix::fs::PermissionsExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::{self, sleep, timeout, Duration as TokioDuration};
use log::{info, debug, warn, error, trace};
use futures::stream::{ StreamExt, TryStreamExt };
use serde_json::{Value, Map};
use serde_yaml2 as serde_yaml;
use rocksdb::{DB, Options};
use reqwest::Client;
use nix::sys::signal::{kill, Signal};
use sysinfo::{System, Process, Pid as NixPid, ProcessesToUpdate, RefreshKind, ProcessRefreshKind};
use nix::unistd::Pid;
use std::time::Instant;
use walkdir::WalkDir;
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
    default_tikv_map,
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
    cleanup_daemon_registry_stale_entries,
    get_running_storage_daemons,
    is_socket_used_by_cli,
    is_zmq_ipc_in_use,
    stop_specific_storage_daemon,
    sync_daemon_registry_with_manager,
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
                        let age = SystemTime::now().duration_since(modified).unwrap_or(TokioDuration::from_secs(0));
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

async fn migrate_data_if_needed(old_port: Option<u16>, new_port: u16) -> Result<()> {
    let base = PathBuf::from("/opt/graphdb/storage_data/sled");
    let old_path = base.join(old_port.map(|p| p.to_string()).unwrap_or_default());
    let new_path = base.join(new_port.to_string());

    if old_port.is_none() || old_port == Some(new_port) {
        return Ok(()); // No migration needed
    }

    if !old_path.exists() {
        info!("No data to migrate from port {}", old_port.unwrap());
        return Ok(());
    }

    if new_path.exists() {
        warn!("Target path {} already exists. Removing...", new_path.display());
        tokio_fs::remove_dir_all(&new_path).await.with_context(|| format!("Failed to remove {}", new_path.display()))?;
    }

    println!("Migrating data from {} to {}", old_path.display(), new_path.display());
    info!("Migrating storage data: {} → {}", old_path.display(), new_path.display());

    tokio_fs::create_dir_all(&new_path).await.with_context(|| format!("Failed to create {}", new_path.display()))?;

    for entry in WalkDir::new(&old_path).into_iter().filter_map(|e| e.ok()) {
        let src = entry.path();
        let dest = new_path.join(src.strip_prefix(&old_path).unwrap());

        if src.is_dir() {
            tokio_fs::create_dir_all(&dest).await.with_context(|| format!("Failed to create dir {}", dest.display()))?;
        } else {
            std::fs::copy(src, &dest).with_context(|| format!("Failed to copy {} to {}", src.display(), dest.display()))?;
        }
    }

    // Optional: clean up old data
    tokio_fs::remove_dir_all(&old_path).await.with_context(|| format!("Failed to remove old data at {}", old_path.display()))?;
    info!("Migration completed and old data removed.");

    Ok(())
}

// ============================================================================
// Helper Functions - Fixed & Preserved
// ============================================================================

/// Load CLI configuration from command-line arguments
fn load_cli_config_from_cli(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    cluster_opt: Option<String>,
) -> Result<CliConfig, anyhow::Error> {
    trace!("Loading CLI configuration with port: {:?}", port);
   
    let command = CommandType::StartStorage {
        port,
        config_file: config_file.clone(),
        cluster: cluster_opt.clone(),
        storage_port: port,
        storage_cluster: cluster_opt,
    };
   
    let cli_cfg = CliConfig::load(Some(command))
        .with_context(|| "Failed to load CLI configuration. Check file permissions or path.")?;
   
    info!("SUCCESS: CLI config loaded: {:?}", cli_cfg);
    println!("SUCCESS: CLI config loaded.");
    Ok(cli_cfg)
}

/// Resolve storage configuration file path from CLI config
fn resolve_storage_config_path(
    cli_cfg: &CliConfig,
    config_file: Option<PathBuf>,
) -> PathBuf {
    let get_config_path = |cli_config_file: Option<PathBuf>| {
        cli_config_file
            .or(config_file.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH))
    };
    let cfg_path = match &cli_cfg.command {
        Commands::Start { action: Some(StartAction::All { storage_config, .. }), .. } => {
            get_config_path(storage_config.clone())
        }
        Commands::Start { action: Some(StartAction::Storage { config_file, .. }), .. } => {
            get_config_path(config_file.clone())
        }
        Commands::Storage(StorageAction::Start { config_file, .. }) => {
            get_config_path(config_file.clone())
        }
        _ => get_config_path(None),
    };
   
    debug!("Determined config path: {:?}", cfg_path);
    println!("==> STARTING STORAGE - STEP 3");
    cfg_path
}

/// Load main storage configuration from YAML file or use provided config
async fn load_main_storage_config(
    new_config: Option<StorageConfig>,
    cfg_path: &PathBuf,
) -> Result<StorageConfig, anyhow::Error> {
    let mut storage_cfg = if let Some(config) = new_config {
        info!("Using provided StorageConfig object, ignoring config_file.");
        config
    } else {
        match load_storage_config_from_yaml(Some(cfg_path.clone())).await {
            Ok(config) => {
                debug!("Loaded storage config from {:?}", cfg_path);
                println!("Loaded storage config from {:?}", cfg_path);
                config
            }
            Err(e) => {
                error!("Failed to load storage config from {:?}: {}", cfg_path, e);
                warn!("Using default StorageConfig due to config load failure");
                StorageConfig::default()
            }
        }
    };
   
    println!("=========> IN STEP 3 CONFIG IS {:?}", storage_cfg);
    if storage_cfg.config_root_directory.is_none() {
        storage_cfg.config_root_directory = Some(
            cfg_path
                .parent()
                .unwrap_or(&PathBuf::from("./storage_daemon_server"))
                .to_path_buf()
        );
        debug!("Set config_root_directory to {:?}", storage_cfg.config_root_directory);
    }
   
    info!("Loaded Storage Config: {:?}", storage_cfg);
    info!("Storage engine type from config: {:?}", storage_cfg.storage_engine_type);
    Ok(storage_cfg)
}

/// Load engine-specific configuration section
async fn load_engine_specific_section(storage_cfg: &mut StorageConfig) {
    println!("=========> BEFORE STEP 3.1 CONFIG IS {:?}", storage_cfg);
    println!("==> STARTING STORAGE - STEP 3.1");
    let engine_specific_config_path = match daemon_api_storage_engine_type_to_string(&storage_cfg.storage_engine_type)
        .to_lowercase()
        .as_str()
    {
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
                            let should_override = storage_cfg.engine_specific_config.is_none()
                                || storage_cfg
                                    .engine_specific_config
                                    .as_ref()
                                    .map(|c| c.storage_engine_type != storage_cfg.storage_engine_type)
                                    .unwrap_or(true);
                           
                            if should_override {
                                println!("Overriding engine-specific config with values from {:?}", engine_config_path);
                                storage_cfg.engine_specific_config = Some(engine_specific.storage.clone());
                            } else {
                                println!("Loaded engine-specific config from {:?}", engine_config_path);
                                info!("Merged engine-specific config from {:?}", engine_config_path);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to deserialize engine-specific config from {:?}: {}. Using default config.", engine_config_path, e);
                            storage_cfg.engine_specific_config = Some(create_default_selected_storage_config(&storage_cfg.storage_engine_type));
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read engine-specific config file {:?}: {}. Using default config.", engine_config_path, e);
                    storage_cfg.engine_specific_config = Some(create_default_selected_storage_config(&storage_cfg.storage_engine_type));
                }
            }
        } else {
            debug!("No engine-specific config found at {:?}", engine_config_path);
            storage_cfg.engine_specific_config = Some(create_default_selected_storage_config(&storage_cfg.storage_engine_type));
        }
    } else {
        debug!("No engine-specific config path defined for engine {:?}", storage_cfg.storage_engine_type);
        storage_cfg.engine_specific_config = Some(create_default_selected_storage_config(&storage_cfg.storage_engine_type));
    }
}

/// Normalize paths and ports in storage configuration
fn normalise_paths_and_ports(
    port: Option<u16>,
    storage_cfg: &mut StorageConfig,
) -> u16 {
    println!("=========> BEFORE STEP 3.2 CONFIG IS {:?}", storage_cfg);
    let selected_port = port.unwrap_or(storage_cfg.default_port);
   
    if let Some(ref mut engine_config) = storage_cfg.engine_specific_config {
        let engine_path_name = storage_cfg.storage_engine_type.to_string().to_lowercase();
        let data_dir_path = storage_cfg
            .data_directory
            .as_ref()
            .map_or(PathBuf::from(DEFAULT_DATA_DIRECTORY), |p| p.clone());
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
   
    debug!("Storage config after path normalization: {:?}", storage_cfg);
    println!("==> STARTING STORAGE - STEP 3.2 {:?}", storage_cfg);
   
    selected_port
}

/// Handle permanent and migrate flags from CLI
async fn handle_permanent_and_migrate_flags(
    cli_cfg: &CliConfig,
    storage_cfg: &mut StorageConfig,
) -> Result<(bool, bool), anyhow::Error> {
    let is_permanent = matches!(
        &cli_cfg.command,
        Commands::Use(UseAction::Storage { permanent: true, .. })
    );
    let must_migrate = matches!(
        &cli_cfg.command,
        Commands::Use(UseAction::Storage { migrate: true, .. })
    );
    if is_permanent {
        debug!("--permanent flag detected, setting storage engine to {:?}", storage_cfg.storage_engine_type);
        info!("Saved storage config with engine {:?}", storage_cfg.storage_engine_type);
        storage_cfg.save().await.context("Failed to save storage config")?;
    }
   
    println!("==> STARTING STORAGE - STEP 4 - and engine specific config here is {:?}", storage_cfg.engine_specific_config);
    Ok((is_permanent, must_migrate))
}

/// Check if a healthy daemon already exists for this port and engine
async fn try_reuse_existing_daemon(
    target_port: u16,
    storage_cfg: &StorageConfig,
    is_use_cmd: bool,
    port_arc: Arc<TokioMutex<Option<u16>>>,
    shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    handle_arc: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<bool, anyhow::Error> {
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let target_engine_str = daemon_api_storage_engine_type_to_string(&storage_cfg.storage_engine_type).to_lowercase();
    
    if let Some(existing) = all_daemons.iter().find(|d| d.port == target_port && d.service_type == "storage") {
        let existing_engine = existing.engine_type.as_deref().unwrap_or("unknown").to_lowercase();

        // Check if PID is alive and engine matches
        if existing.pid > 0
            && check_pid_validity(existing.pid).await
            && existing_engine == target_engine_str
        {
            // ✅ Use registry metadata, not ZMQ ping
            if existing.zmq_ready && existing.engine_synced {
                info!("Healthy {} daemon (PID {}) already running on port {} — reusing.", target_engine_str, existing.pid, target_port);
                println!("===> REUSING EXISTING {} DAEMON ON PORT {} (PID {})", target_engine_str.to_uppercase(), target_port, existing.pid);

                *port_arc.lock().await = Some(target_port);
                let (tx, rx) = oneshot::channel();
                *shutdown_tx_opt.lock().await = Some(tx);
                let handle = tokio::spawn(async move {
                    let _ = rx.await;
                    info!("Storage daemon on port {} shutting down", target_port);
                });
                *handle_arc.lock().await = Some(handle);

                sync_daemon_registry_with_manager(target_port, storage_cfg, &shutdown_tx_opt).await?;
                return Ok(true);
            } else {
                // Daemon is running but not ready yet — don't reuse
                info!("Daemon PID {} on port {} is running but not ready (zmq_ready={}, engine_synced={}).", 
                      existing.pid, target_port, existing.zmq_ready, existing.engine_synced);
            }
        }
    }
    
    Ok(false)
}

/// Start storage daemon with retry logic
async fn start_daemon_with_retry(
    target_port: u16,
    storage_cfg: &StorageConfig,
) -> Result<u32, anyhow::Error> {
    println!("==> STARTING STORAGE - STEP 5.1 {}",
             daemon_api_storage_engine_type_to_string(&storage_cfg.storage_engine_type));
    println!("==> STARTING STORAGE - STEP 5.2 {}",
             daemon_api_storage_engine_type_to_string(&storage_cfg.storage_engine_type));
    let daemon_config_string = serde_yaml::to_string(&storage_cfg)
        .context("Failed to serialize daemon config to YAML")?;
   
    println!("==> STARTING STORAGE - STEP 5.3");
    let max_attempts = 3;
    let retry_interval = TokioDuration::from_millis(1000);
    let mut pid = None;
    for attempt in 1..=max_attempts {
        debug!("Attempt {}/{} to start storage daemon on port {}", attempt, max_attempts, target_port);
       
        let start_result = start_daemon(
            Some(target_port),
            Some(daemon_config_string.clone()),
            vec![],
            "storage",
            Some(storage_cfg.clone()),
        ).await;
        match start_result {
            Ok(_) => {
                debug!("start_daemon succeeded on attempt {} for port {}", attempt, target_port);
                match find_pid_by_port(target_port).await {
                    Some(p) if p > 0 && check_pid_validity(p).await => {
                        pid = Some(p);
                        info!("Storage daemon started with PID {} on port {}", p, target_port);
                        println!("Daemon (PID {}) is listening on 127.0.0.1:{}", p, target_port);
                        break;
                    }
                    _ => {
                        warn!("No valid PID found for port {} after starting daemon on attempt {}", target_port, attempt);
                        if attempt < max_attempts {
                            info!("Retrying storage daemon startup in {:?}", retry_interval);
                            sleep(retry_interval).await;
                        }
                    }
                }
            }
            Err(e) => {
                let err_msg = e.to_string();
                if err_msg.contains("thread::join failed") && err_msg.contains("No such process") {
                    warn!("Ignoring harmless daemon detach error on attempt {}", attempt);
                    if let Some(p) = find_pid_by_port(target_port).await {
                        if p > 0 && check_pid_validity(p).await {
                            pid = Some(p);
                            info!("Storage daemon recovered with PID {} on port {} despite detach error", p, target_port);
                            println!("Daemon (PID {}) is listening on 127.0.0.1:{}", p, target_port);
                            break;
                        }
                    }
                }
                error!("Storage daemon start attempt {}/{} failed on port {}: {}", attempt, max_attempts, target_port, e);
                if attempt < max_attempts {
                    info!("Retrying storage daemon startup in {:?}", retry_interval);
                    sleep(retry_interval).await;
                }
            }
        }
    }
    pid.ok_or_else(|| anyhow!("Failed to start storage daemon on port {} after {} attempts", target_port, max_attempts))
}

/// Register daemon in global registry
async fn register_daemon(
    target_port: u16,
    pid: u32,
    storage_cfg: &StorageConfig,
    cfg_path: &PathBuf,
) -> Result<(), anyhow::Error> {
    println!("==> STARTING STORAGE - STEP 6 (PREPARE PATHS)");
   
    let engine_type_str = daemon_api_storage_engine_type_to_string(&storage_cfg.storage_engine_type);
    let engine_path_name = engine_type_str.to_lowercase();
    let base_data_dir = storage_cfg
        .data_directory
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
    let base_engine_path = base_data_dir.join(&engine_path_name);
    let instance_path = base_engine_path.join(target_port.to_string());
    info!("Prepared paths - instance: {:?}", instance_path);
    println!("=====> Instance path for daemon: {:?}", instance_path);
    let daemon_metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port: target_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(instance_path.clone()),
        config_path: Some(cfg_path.clone()),
        engine_type: Some(engine_type_str.clone()),
        last_seen_nanos: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
        zmq_ready: false,
        engine_synced: false,
    };
    if let Some(_existing) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(target_port).await? {
        let update_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port: target_port,
            pid,
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(instance_path.clone()),
            config_path: Some(cfg_path.clone()),
            engine_type: Some(engine_type_str.clone()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
            zmq_ready: false,
            engine_synced: false,
        };
        GLOBAL_DAEMON_REGISTRY.update_daemon_metadata(update_metadata).await?;
        info!("Updated existing daemon metadata on port {}", target_port);
        println!("===> UPDATED DAEMON ON PORT {} WITH PATH {:?}", target_port, instance_path);
    } else {
        timeout(
            TokioDuration::from_secs(5),
            GLOBAL_DAEMON_REGISTRY.register_daemon(daemon_metadata),
        )
        .await
        .map_err(|_| anyhow!("Timeout registering daemon on port {}", target_port))??;
       
        info!("Registered daemon on port {} with path {:?}", target_port, instance_path);
        println!("===> REGISTERED DAEMON ON PORT {} WITH PATH {:?}", target_port, instance_path);
    }
    Ok(())
}

/// Initialize storage engine manager
async fn initialise_storage_manager(
    target_port: u16,
    storage_cfg: &StorageConfig,
    cfg_path: &PathBuf,
    is_permanent: bool,
    must_migrate: bool,
) -> Result<(), anyhow::Error> {
    println!("==> STARTING STORAGE - STEP 7 (INITIALIZE MANAGER)");
    let engine_type_str = daemon_api_storage_engine_type_to_string(&storage_cfg.storage_engine_type);
    let engine_path_name = engine_type_str.to_lowercase();
    let base_data_dir = storage_cfg
        .data_directory
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
    let base_engine_path = base_data_dir.join(&engine_path_name);
    let instance_path = base_engine_path.join(target_port.to_string());
    let mut manager_config = storage_cfg.clone();
    if let Some(ref mut engine_config) = manager_config.engine_specific_config {
        engine_config.storage.path = Some(instance_path.clone());
        engine_config.storage.port = Some(target_port);
    }
    manager_config.data_directory = Some(base_engine_path.clone());
    let max_init_attempts = 3;
    let mut engine_initialized = false;
    let mut attempt = 0;
    while attempt < max_init_attempts && !engine_initialized {
        attempt += 1;
        println!("===> ATTEMPT {}/{} TO INITIALIZE STORAGEENGINEMANAGER ON PORT {}",
                 attempt, max_init_attempts, target_port);
        if let Some(async_manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            info!("Existing StorageEngineManager found, updating with new config");
            async_manager
                .use_storage(manager_config.clone(), is_permanent, must_migrate)
                .await
                .context("Failed to update existing StorageEngineManager")?;
            engine_initialized = true;
            println!("===> SUCCESSFULLY UPDATED STORAGEENGINEMANAGER ON ATTEMPT {}", attempt);
        } else {
            match StorageEngineManager::new(
                storage_cfg.storage_engine_type.clone(),
                cfg_path,
                is_permanent,
                Some(target_port),
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
                    warn!("Failed to initialize StorageEngineManager due to WouldBlock error. Retrying...");
                    println!("===> ERROR: WOULDBLOCK ON STORAGEENGINEMANAGER INITIALIZATION, RETRYING");
                    sleep(TokioDuration::from_millis(1000)).await;
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
        return Err(anyhow!("Failed to initialize StorageEngineManager on port {} after {} attempts",
                          target_port, max_init_attempts));
    }
    println!("===> STORAGE ENGINE MANAGER INITIALIZED SUCCESSFULLY FOR PORT {}", target_port);
    Ok(())
}

/// Engine-specific post-initialization tasks
async fn engine_specific_post_init(
    target_port: u16,
    storage_cfg: &StorageConfig,
) -> Result<(), anyhow::Error> {
    match storage_cfg.storage_engine_type {
        StorageEngineType::Sled => {
            let selected_config = storage_cfg
                .engine_specific_config
                .clone()
                .unwrap_or_else(|| create_default_selected_storage_config(&StorageEngineType::Sled));
            let sled_path = selected_config
                .storage
                .path
                .unwrap_or_else(|| {
                    PathBuf::from(DEFAULT_DATA_DIRECTORY)
                        .join("sled")
                        .join(target_port.to_string())
                });
           
            info!("Sled path set to {:?}", sled_path);
            println!("===> USING SLED PATH: {:?}", sled_path);
            if sled_path.exists() && sled_path.is_dir() {
                info!("Sled database directory exists at {:?}", sled_path);
                println!("===> SLED DATABASE DIRECTORY VERIFIED AT {:?}", sled_path);
            } else {
                warn!("Sled database directory does not exist at {:?}", sled_path);
                println!("===> WARNING: SLED DATABASE DIRECTORY NOT FOUND AT {:?}", sled_path);
            }
        }
        StorageEngineType::RocksDB => {
            let selected_config = storage_cfg
                .engine_specific_config
                .clone()
                .unwrap_or_else(|| create_default_selected_storage_config(&StorageEngineType::RocksDB));
            let rocksdb_path = selected_config
                .storage
                .path
                .unwrap_or_else(|| {
                    PathBuf::from(DEFAULT_DATA_DIRECTORY)
                        .join("rocksdb")
                        .join(target_port.to_string())
                });
           
            info!("RocksDB path set to {:?}", rocksdb_path);
            println!("===> USING ROCKSDB PATH: {:?}", rocksdb_path);
            const MAX_RESET_ATTEMPTS: usize = 3;
            let mut attempt = 0;
            let mut rocksdb_initialized = false;
            while attempt < MAX_RESET_ATTEMPTS && !rocksdb_initialized {
                attempt += 1;
                println!("===> ATTEMPT {}/{} TO INITIALIZE ROCKSDB DATABASE AT {:?}",
                        attempt, MAX_RESET_ATTEMPTS, rocksdb_path);
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
                sleep(TokioDuration::from_millis(1000)).await;
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
                return Err(anyhow!("Failed to initialize RocksDB database at {:?} after {} attempts",
                                  rocksdb_path, MAX_RESET_ATTEMPTS));
            }
        }
        StorageEngineType::TiKV => {
            let pd_endpoints = storage_cfg
                .engine_specific_config
                .as_ref()
                .and_then(|c| c.storage.pd_endpoints.clone())
                .unwrap_or("127.0.0.1:2379".to_string());
            let pd_port = pd_endpoints.split(':').last().and_then(|p| p.parse::<u16>().ok());
           
            if let Some(pd_port) = pd_port {
                if check_process_status_by_port("TiKV PD", pd_port).await {
                    info!("TiKV PD process detected on port {}. Skipping termination.", pd_port);
                    println!("Skipping termination for TiKV PD port {}.", pd_port);
                }
            }
           
            if let Err(e) = TikvStorage::force_unlock().await {
                warn!("Failed to force unlock TiKV: {}", e);
            } else {
                info!("Successfully performed TiKV force unlock");
                println!("===> Lock files cleaned successfully.");
            }
        }
        _ => {}
    }
   
    Ok(())
}

/// Finalize startup by syncing registry and setting up shutdown channels
async fn finalise_startup(
    target_port: u16,
    storage_cfg: &StorageConfig,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<(), anyhow::Error> {
    sync_daemon_registry_with_manager(
        target_port,
        storage_cfg,
        &storage_daemon_shutdown_tx_opt,
    ).await?;
   
    println!("==> STEP 7 COMPLETE: Registry synced with manager paths");
    *storage_daemon_port_arc.lock().await = Some(target_port);
    let (tx, rx) = oneshot::channel();
    *storage_daemon_shutdown_tx_opt.lock().await = Some(tx);
    let handle = tokio::spawn(async move {
        rx.await.ok();
        info!("Storage daemon on port {} shutting down", target_port);
    });
    *storage_daemon_handle.lock().await = Some(handle);
    info!("Storage daemon startup completed successfully on port {}", target_port);
    println!("Storage daemon startup completed successfully on port {}.", target_port);
    Ok(())
}

async fn copy_dir_all(src: &Path, dst: &Path) -> Result<(), io::Error> {
    tokio_fs::create_dir_all(dst).await?;

    let mut stack = vec![(src.to_path_buf(), dst.to_path_buf())];

    while let Some((current_src, current_dst)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current_src).await?;

        while let Some(entry) = entries.next_entry().await? {
            let src_path = entry.path();
            let relative = src_path.strip_prefix(src).unwrap();
            let dst_path = dst.join(relative);

            if src_path.is_dir() {
                tokio::fs::create_dir_all(&dst_path).await?;
                stack.push((src_path, dst_path));
            } else {
                tokio::fs::copy(&src_path, &dst_path).await?;
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    new_config: Option<StorageConfig>,
    flag: Option<String>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<(), anyhow::Error> {
    trace!("Entering start_storage_interactive with port: {:?}, flag: {:?}", port, flag);
    info!("handlers_storage.rs version: 2025-11-08");
    let force_port = flag.as_deref() == Some("force_port");

    // -----------------------------------------------------------------
    // STEP 1: CLI config & source detection
    // -----------------------------------------------------------------
    let cli_cfg = load_cli_config_from_cli(port, config_file.clone(), None)?;
    let _is_use_cmd = matches!(&cli_cfg.command, Commands::Use(_));

    // -----------------------------------------------------------------
    // STEP 2: Resolve config path
    // -----------------------------------------------------------------
    let cfg_path = resolve_storage_config_path(&cli_cfg, config_file.clone());

    // -----------------------------------------------------------------
    // STEP 3: Load storage config
    // -----------------------------------------------------------------
    let mut storage_cfg = load_main_storage_config(new_config, &cfg_path).await?;

    // -----------------------------------------------------------------
    // STEP 3.1: Load engine-specific config
    // -----------------------------------------------------------------
    load_engine_specific_section(&mut storage_cfg).await;

    // -----------------------------------------------------------------
    // STEP 3.2: Normalize paths & ports
    // -----------------------------------------------------------------
    let requested_port = normalise_paths_and_ports(port, &mut storage_cfg);

    // -----------------------------------------------------------------
    // STEP 4: Permanent / migrate flags
    // -----------------------------------------------------------------
    let (is_permanent, must_migrate) =
        handle_permanent_and_migrate_flags(&cli_cfg, &mut storage_cfg).await?;

    // -----------------------------------------------------------------
    // STEP 5: TiKV PD handling
    // -----------------------------------------------------------------
    let tikv_pd_port = if storage_cfg.storage_engine_type == StorageEngineType::TiKV {
        storage_cfg.engine_specific_config.as_ref()
            .and_then(|c| c.storage.pd_endpoints.as_ref())
            .and_then(|s| s.split(':').last())
            .and_then(|p| p.parse::<u16>().ok())
    } else {
        let p = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if p.exists() {
            if let Ok(txt) = tokio_fs::read_to_string(&p).await {
                if let Ok(w) = serde_yaml::from_str::<TiKVConfigWrapper>(&txt) {
                    w.storage.pd_endpoints.as_ref()
                        .and_then(|s| s.split(':').last())
                        .and_then(|p| p.parse::<u16>().ok())
                } else { None }
            } else { None }
        } else { None }
    };
    if storage_cfg.storage_engine_type == StorageEngineType::TiKV {
        if let Some(pd) = tikv_pd_port {
            if Some(requested_port) == Some(pd) {
                info!("Skipping daemon startup on TiKV PD port {}", requested_port);
                return Ok(());
            }
        }
    } else if let Some(pd) = tikv_pd_port {
        if requested_port == pd {
            return Err(anyhow!(
                "Port {} is reserved for TiKV PD and cannot be used for engine {:?}",
                requested_port,
                storage_cfg.storage_engine_type
            ));
        }
    }
    println!("==> STARTING STORAGE - STEP 5");

    // -----------------------------------------------------------------
    // STEP 6: TRY TO REUSE EXISTING DAEMON OF SAME ENGINE
    // -----------------------------------------------------------------
    let target_engine = daemon_api_storage_engine_type_to_string(&storage_cfg.storage_engine_type);
    if try_reuse_existing_daemon(
        requested_port,
        &storage_cfg,
        _is_use_cmd,
        storage_daemon_port_arc.clone(),
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
    ).await? {
        info!("Successfully reused existing daemon on port {}", requested_port);
        println!("===> SUCCESSFULLY REUSED EXISTING DAEMON ON PORT {}", requested_port);
        return Ok(()); // EARLY RETURN — reuse succeeded
    }

    // -----------------------------------------------------------------
    // STEP 6.5: MIGRATE FROM LATEST VALID DATA DIR + WAL — UNCONDITIONAL
    // -----------------------------------------------------------------
    let base_dir = PathBuf::from("/opt/graphdb/storage_data/sled");
    let target_path = base_dir.join(requested_port.to_string());
    let target_wal_dir = base_dir.join(format!("wal_{}", requested_port));

    // Always scan for latest valid DB + WAL
    let mut candidates: Vec<(PathBuf, SystemTime, u64, u16)> = Vec::new(); // (path, modified, started_at, port)

    if let Ok(mut entries) = tokio_fs::read_dir(&base_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.is_dir()
                && path.file_name().and_then(|n| n.to_str()).map(|s| s.parse::<u16>().is_ok()).unwrap_or(false)
                && path != target_path
            {
                let port_num: u16 = path.file_name().unwrap().to_string_lossy().parse().unwrap();
                if let Ok(metadata) = entry.metadata().await {
                    if let Ok(modified) = metadata.modified() {
                        let meta_path = path.join("metadata.json");
                        if tokio_fs::metadata(&meta_path).await.is_ok() {
                            if let Ok(meta_content) = tokio_fs::read_to_string(&meta_path).await {
                                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_content) {
                                    if meta.get("engine_synced").and_then(|v| v.as_bool()) == Some(true) {
                                        if let Some(started_at) = meta.get("started_at").and_then(|v| v.as_u64()) {
                                            candidates.push((path.clone(), modified, started_at, port_num));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if !candidates.is_empty() {
        candidates.sort_by_key(|&(_, modified, _, _)| std::cmp::Reverse(modified));
        if let Some((src_path, _, _, src_port)) = candidates.first() {
            let src_wal_dir = base_dir.join(format!("wal_{}", src_port));

            println!("Migrating DB + WAL from port {} → {}", src_port, requested_port);
            info!("Migrating: {} + wal_{} → {} + wal_{}", src_path.display(), src_port, target_path.display(), requested_port);

            // Ensure target dirs
            tokio_fs::create_dir_all(&target_path).await?;
            if src_wal_dir.exists() {
                tokio_fs::create_dir_all(&target_wal_dir).await?;
            }

            // Remove old target DB (but not wal)
            if target_path.exists() {
                tokio_fs::remove_dir_all(&target_path).await?;
            }

            // Copy DB
            copy_dir_all(src_path, &target_path).await?;

            // Copy WAL
            if src_wal_dir.exists() {
                if target_wal_dir.exists() {
                    tokio_fs::remove_dir_all(&target_wal_dir).await?;
                }
                copy_dir_all(&src_wal_dir, &target_wal_dir).await?;
                info!("WAL migrated: wal_{} → wal_{}", src_port, requested_port);
            }

            println!("Migration complete: port {} → {}", src_port, requested_port);
        }
    }

    // -----------------------------------------------------------------
    // STEP 7: PORT RESOLUTION
    // -----------------------------------------------------------------
    let mut target_port = requested_port;
    if storage_cfg.use_raft_for_scale {
        let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
        if let Some(existing) = all_daemons.iter().find(|d| d.port == requested_port && d.service_type == "storage") {
            if existing.pid > 0 && check_pid_validity(existing.pid).await {
                let existing_engine = existing.engine_type.as_deref().unwrap_or("unknown");
                let target_engine_lower = target_engine.to_lowercase();
                let existing_engine_lower = existing_engine.to_lowercase();
                if existing_engine_lower != target_engine_lower {
                    warn!("Port {} occupied by {} - scanning cluster_range", requested_port, existing_engine);
                    let current_ports = parse_cluster_range(&storage_cfg.cluster_range)?;
                    let mut candidates: Vec<u16> = current_ports.iter().filter(|&&p| p != requested_port).copied().collect();
                    candidates.sort_unstable();
                    let mut free_port: Option<u16> = None;
                    for &p in &candidates {
                        if !check_process_status_by_port("Storage Daemon", p).await {
                            free_port = Some(p);
                            break;
                        }
                    }
                    if let Some(free) = free_port {
                        target_port = free;
                        info!("Using free port {} for {} daemon", target_port, target_engine);
                    } else {
                        return Err(anyhow!("No free port in cluster range {}", storage_cfg.cluster_range));
                    }
                }
            }
        }
    } else {
        let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
        if let Some(existing) = all_daemons.iter().find(|d| d.port == requested_port && d.service_type == "storage") {
            if existing.pid > 0 && check_pid_validity(existing.pid).await {
                let existing_engine = existing.engine_type.as_deref().unwrap_or("unknown");
                let target_engine_lower = target_engine.to_lowercase();
                let existing_engine_lower = existing_engine.to_lowercase();
                if existing_engine_lower != target_engine_lower {
                    return Err(anyhow!(
                        "Port {} is in use by {} daemon. Stop it first.",
                        requested_port, existing_engine
                    ));
                }
            }
        }
        target_port = requested_port;
    }

    // -----------------------------------------------------------------
    // STEP 8-12: Startup
    // -----------------------------------------------------------------
    let pid = start_daemon_with_retry(target_port, &storage_cfg).await?;
    register_daemon(target_port, pid, &storage_cfg, &cfg_path).await?;
    initialise_storage_manager(target_port, &storage_cfg, &cfg_path, is_permanent, must_migrate).await?;
    engine_specific_post_init(target_port, &storage_cfg).await?;
    finalise_startup(
        target_port,
        &storage_cfg,
        storage_daemon_port_arc,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
    ).await?;

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

    // === GET TARGET PORTS: SPECIFIC OR ALL STORAGE DAEMONS ===
    let target_ports = match port {
        Some(p) => vec![p],
        None => daemon_registry
            .list_storage_daemons()
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|d| d.port)
            .collect::<Vec<u16>>(),
    };

    if target_ports.is_empty() && port.is_none() {
        info!("No storage daemons found in registry to stop.");
        println!("===> NO STORAGE DAEMONS FOUND IN REGISTRY TO STOP");
        return Ok(());
    }

    let mut failed_ports = Vec::new();

    for port in target_ports {
        let socket_path = format!("/tmp/graphdb-{}.ipc", port);

        // === SKIP IF CLI IS USING SOCKET ===
        if is_socket_used_by_cli(&socket_path).await.unwrap_or(false) {
            warn!("Port {} is used by CLI process. Skipping shutdown to avoid self-termination.", port);
            println!("===> WARNING: Port {} is used by CLI. Skipping shutdown.", port);
            failed_ports.push(format!("Port {} used by CLI", port));
            continue;
        }

        // === GET DAEMON METADATA ===
        let daemon = daemon_registry
            .get_daemon_metadata(port)
            .await
            .ok()
            .flatten();

        // === DYNAMIC DB PATH FROM REGISTRY OR FALLBACK ===
        let db_path = daemon
            .as_ref()
            .and_then(|d| d.data_dir.clone())
            .unwrap_or_else(|| {
                PathBuf::from(DEFAULT_DATA_DIRECTORY)
                    .join("sled")
                    .join(port.to_string())
            });

        let lock_path = if db_path.to_str().unwrap_or("").contains("rocksdb") {
            db_path.join("LOCK")
        } else {
            db_path.join("db.lck")  // Sled lock file
        };

        // === STOP DAEMON ===
        match &daemon {  // Use reference to avoid moving
            Some(d) if d.pid != 0 && d.pid != std::process::id() => {
                if check_pid_validity(d.pid).await {
                    info!("Attempting to stop Storage Daemon on port {} (PID {})...", port, d.pid);
                    println!("===> ATTEMPTING TO STOP STORAGE DAEMON ON PORT {} (PID {})", port, d.pid);

                    if let Err(e) = stop_specific_storage_daemon(port, true).await {
                        warn!("Failed to stop daemon on port {} (PID {}): {}", port, d.pid, e);
                        println!("===> WARNING: Failed to stop daemon on port {} (PID {}): {}", port, d.pid, e);
                        failed_ports.push(format!("Failed to stop daemon on port {}: {}", port, e));
                    } else {
                        info!("Successfully stopped daemon on port {} (PID {}).", port, d.pid);
                        println!("===> Successfully stopped daemon on port {} (PID {}).", port, d.pid);
                    }
                } else {
                    warn!("Stale PID {} found for Storage Daemon on port {}. Cleaning up...", d.pid, port);
                    println!("===> STALE PID {} FOUND FOR STORAGE DAEMON ON PORT {}. CLEANING UP...", d.pid, port);
                }
            }
            Some(d) if d.pid == std::process::id() => {
                warn!("PID {} for port {} matches CLI process. Skipping shutdown.", d.pid, port);
                println!("===> WARNING: PID {} for port {} matches CLI process. Skipping shutdown.", d.pid, port);
                // Don't treat this as an error - it means the daemon is the CLI process itself
                info!("Port {} matches CLI process, treating as already handled.", port);
                println!("===> PORT {} MATCHES CLI PROCESS, TREATING AS ALREADY HANDLED", port);
                // Continue with cleanup but don't try to stop the daemon
            }
            Some(_) => {
                warn!("No valid PID found for Storage Daemon on port {}. Checking for stray processes...", port);
                println!("===> NO VALID PID FOUND FOR STORAGE DAEMON ON PORT {}. CHECKING FOR STRAY PROCESSES...", port);
            }
            None => {
                info!("No Storage Daemon found on port {} in registry. Checking for stray processes...", port);
                println!("===> NO STORAGE DAEMON FOUND ON PORT {} IN REGISTRY. CHECKING FOR STRAY PROCESSES...", port);

                if let Some(new_pid) = find_pid_by_port(port).await {
                    if new_pid != std::process::id() && check_pid_validity(new_pid).await {
                        info!("Found stray process (PID {}) on port {}. Stopping...", new_pid, port);
                        println!("===> Found stray process (PID {}) on port {}. Stopping...", new_pid, port);
                        if let Err(e) = stop_specific_storage_daemon(port, true).await {
                            warn!("Failed to stop stray process on port {} (PID {}): {}", port, new_pid, e);
                            println!("===> WARNING: Failed to stop stray process on port {} (PID {}): {}", port, new_pid, e);
                            failed_ports.push(format!("Failed to stop stray process on port {}: {}", port, e));
                        }
                    } else {
                        warn!("Stray PID {} on port {} is invalid or matches CLI. Skipping.", new_pid, port);
                        println!("===> WARNING: Stray PID {} on port {} is invalid or matches CLI. Skipping.", new_pid, port);
                        // Don't add to failed_ports if it matches CLI process, only if it's an invalid PID
                        if new_pid == std::process::id() {
                            info!("Stray PID {} matches CLI process on port {}, treating as handled.", new_pid, port);
                            println!("===> STRAY PID {} MATCHES CLI PROCESS ON PORT {}, TREATING AS HANDLED", new_pid, port);
                        } else {
                            failed_ports.push(format!("Stray PID {} on port {} is invalid", new_pid, port));
                        }
                    }
                }
            }
        }

        // === CLEANUP: LOCK FILE ===
        if lock_path.exists() {
            if db_path.to_str().unwrap_or("").contains("rocksdb") {
                if let Err(e) = RocksDBStorage::force_unlock(&db_path).await {
                    warn!("Failed to clean up RocksDB lock at {:?}: {}", lock_path, e);
                    println!("===> WARNING: Failed to clean up lock at {:?}", lock_path);
                    failed_ports.push(format!("Failed to clean lock at {:?}: {}", lock_path, e));
                } else {
                    info!("Successfully cleaned up lock file at {:?}", lock_path);
                    println!("===> SUCCESSFULLY CLEANED UP LOCK FILE AT {:?}", lock_path);
                }
            } else {
                // Sled: just remove db.lck
                if let Err(e) = std::fs::remove_file(&lock_path) {
                    warn!("Failed to remove Sled lock file at {:?}: {}", lock_path, e);
                    println!("===> WARNING: Failed to remove lock file at {:?}", lock_path);
                    failed_ports.push(format!("Failed to remove lock at {:?}: {}", lock_path, e));
                } else {
                    info!("Successfully removed Sled lock file at {:?}", lock_path);
                    println!("===> SUCCESSFULLY REMOVED SLED LOCK FILE AT {:?}", lock_path);
                }
            }
        }

        // === UNREGISTER FROM REGISTRY ===
        // Only unregister if the daemon wasn't the CLI process
        if daemon.as_ref().map_or(true, |d| d.pid != std::process::id()) {
            if let Err(e) = daemon_registry.unregister_daemon(port).await {
                warn!("Failed to remove daemon on port {} from registry: {}", port, e);
                println!("===> WARNING: Failed to remove daemon on port {} from registry", port);
                failed_ports.push(format!("Failed to unregister port {}: {}", port, e));
            } else {
                info!("Storage daemon on port {} removed from registry.", port);
                println!("===> STORAGE DAEMON ON PORT {} REMOVED FROM REGISTRY", port);
            }
        } else {
            info!("Daemon on port {} was CLI process, skipping registry removal.", port);
            println!("===> DAEMON ON PORT {} WAS CLI PROCESS, SKIPPING REGISTRY REMOVAL", port);
        }

        // === CLEANUP: ZMQ SOCKET (only if daemon wasn't CLI process) ===
        if daemon.as_ref().map_or(true, |d| d.pid != std::process::id()) {
            if Path::new(&socket_path).exists() && !is_socket_used_by_cli(&socket_path).await.unwrap_or(false) {
                if let Err(e) = std::fs::remove_file(&socket_path) {
                    warn!("Failed to remove ZMQ socket file at {}: {}", socket_path, e);
                    println!("===> WARNING: Failed to remove ZMQ socket at {}", socket_path);
                    failed_ports.push(format!("Failed to remove socket {}: {}", socket_path, e));
                } else {
                    info!("Successfully removed ZMQ socket file at {}", socket_path);
                    println!("===> SUCCESSFULLY REMOVED ZMQ SOCKET FILE AT {}", socket_path);
                }
            }
        } else {
            info!("Daemon on port {} was CLI process, keeping socket file.", port);
            println!("===> DAEMON ON PORT {} WAS CLI PROCESS, KEEPING SOCKET FILE", port);
        }

        // === FINAL PORT CHECK WITH RETRIES ===
        let max_attempts = 10;
        let mut attempt = 0;
        while !is_port_free(port).await && attempt < max_attempts {
            warn!("Port {} still in use after cleanup. Waiting 1000ms (Attempt {})", port, attempt + 1);
            println!("===> WARNING: PORT {} IS STILL IN USE AFTER CLEANUP. WAITING 1000MS (ATTEMPT {})", port, attempt + 1);

            if let Some(pid) = find_pid_by_port(port).await {
                if pid != std::process::id() && check_pid_validity(pid).await {
                    warn!("Stray process (PID {}) found on port {}. Sending SIGKILL...", pid, port);
                    println!("===> STRAY PROCESS (PID {}) FOUND ON PORT {}. SENDING SIGKILL...", pid, port);
                    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
                    sleep(TokioDuration::from_millis(1000)).await;
                } else if pid == std::process::id() {
                    // If the port is still in use by the CLI process, that's expected and not an error
                    info!("Port {} still in use by CLI process, which is expected.", port);
                    println!("===> PORT {} STILL IN USE BY CLI PROCESS, WHICH IS EXPECTED", port);
                    break; // Exit the retry loop since this is expected behavior
                }
            }
            sleep(TokioDuration::from_millis(1000)).await;
            attempt += 1;
        }

        if is_port_free(port).await {
            info!("Port {} is now free.", port);
            println!("===> PORT {} IS NOW FREE", port);
        } else if find_pid_by_port(port).await.map_or(false, |pid| pid == std::process::id()) {
            // If port is still in use by CLI process, that's expected and not an error
            info!("Port {} still in use by CLI process, which is expected.", port);
            println!("===> PORT {} STILL IN USE BY CLI PROCESS, WHICH IS EXPECTED", port);
        } else {
            let msg = if let Some(pid) = find_pid_by_port(port).await {
                let system = System::new_with_specifics(RefreshKind::everything().with_processes(ProcessRefreshKind::everything()));
                let name = system
                    .process(NixPid::from_u32(pid))
                    .map(|p| p.name().to_string_lossy().to_string())
                    .unwrap_or("unknown".to_string());
                format!("Port {} still in use by PID {} ({})", port, pid, name)
            } else {
                format!("Port {} still in use, no PID found", port)
            };
            warn!("{}", msg);
            println!("===> WARNING: {}", msg);
            failed_ports.push(msg);
        }
    }

    // === CLEAR HANDLES ===
    let mut tx_guard = shutdown_tx.lock().await;
    if tx_guard.is_some() {
        if let Some(tx) = tx_guard.take() {
            let _ = tx.send(());
            info!("Sent shutdown signal for storage daemon(s).");
            println!("===> in stop_storage_interactive: SENT SHUTDOWN SIGNAL FOR STORAGE DAEMON(S)");
        }
    }

    let mut handle_guard = daemon_handle.lock().await;
    if let Some(handle) = handle_guard.take() {
        let _ = handle.await;
        info!("Daemon handle cleared.");
        println!("===> DAEMON HANDLE CLEARED");
    }

    let mut port_guard = daemon_port.lock().await;
    *port_guard = None;
    info!("Daemon port cleared.");
    println!("===> DAEMON PORT CLEARED");

    if failed_ports.is_empty() {
        info!("Storage daemon(s) stopped successfully for port(s): {:?}", port);
        println!("===> STORAGE DAEMON(S) STOPPED SUCCESSFULLY FOR PORT(S): {:?}", port);
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
                    engine_synced: false,
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
pub async fn use_storage_engine(engine_type_str: &str, permanent: bool, migrate: bool) -> Result<(), anyhow::Error> {
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

    manager.use_storage(storage_config.clone(), permanent, migrate)
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
                sleep(delay).await;
                delay = delay.checked_mul(2).unwrap_or(TokioDuration::from_millis(5000)); // Cap the delay
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
            sleep(TokioDuration::from_millis(200)).await;
            attempt += 1;
        } else {
            info!("Process {} is no longer running", pid);
            return Ok(());
        }
    }
    Err(anyhow!("Failed to terminate process {} after {} attempts", pid, max_attempts))
}

/// Handles the 'migrate' command for migrating data between storage engines.
pub async fn handle_migrate_command(
    from_engine: StorageEngineType,
    to_engine: StorageEngineType,
) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();
    info!("=== Starting handle_migrate_command from engine: {:?} to engine: {:?}", from_engine, to_engine);
    println!("===> MIGRATING FROM ENGINE: {:?} TO ENGINE: {:?}", from_engine, to_engine);

    // Clean up stale registry entries
    cleanup_daemon_registry_stale_entries()
        .await
        .context("Failed to clean daemon registry")?;
    println!("===> Cleaned daemon registry");

    // Debug registry state
    let initial_registry = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Initial registry state: {:?}", initial_registry);

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
    debug!("All existing storage daemons: {:?}", all_existing_storage_daemons);

    // Get active ports
    let active_ports = get_running_storage_daemons()
        .await
        .context("Failed to get running storage daemons")?;
    debug!("Active ports from get_running_storage_daemons: {:?}", active_ports);

    // Filter active storage daemons
    let storage_daemons: Vec<DaemonMetadata> = all_existing_storage_daemons
        .into_iter()
        .filter(|daemon| active_ports.contains(&daemon.port))
        .collect();

    println!("======> STORAGE DAEMON {:?}", storage_daemons.len());
    let existing_daemon_ports: Vec<u16> = storage_daemons.iter().map(|d| d.port).collect();
    info!("Captured active storage daemons on ports: {:?}", existing_daemon_ports);
    println!("===> Active storage daemons on ports: {:?}", existing_daemon_ports);

    if storage_daemons.is_empty() {
        warn!("No active storage daemons found for migration. Registry state: {:?}", initial_registry);
        println!("===> WARNING: No active storage daemons found");
        return Err(anyhow!("No active storage daemons to migrate"));
    }

    println!("===> MIGRATE HANDLER - STEP 1: Loading configuration...");
    let cwd = std::env::current_dir()
        .map_err(|e| anyhow!("Failed to get current working directory: {}", e))?;
    let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let absolute_config_path = cwd.join(&config_path);
    debug!("Attempting to load storage config from {:?}", absolute_config_path);

    // Load base configuration
    let base_config: StorageConfig = match load_storage_config_from_yaml(Some(absolute_config_path.clone())).await {
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
    debug!("Initial loaded config: {:?}", base_config);
    println!(
        "Loaded storage config: default_port={}, cluster_range={}, data_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, log_directory={:?}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        base_config.default_port,
        base_config.cluster_range,
        base_config.data_directory,
        base_config.storage_engine_type,
        base_config.engine_specific_config,
        base_config.log_directory,
        base_config.max_disk_space_gb,
        base_config.min_disk_space_gb,
        base_config.use_raft_for_scale
    );

    // Load config root directory
    println!("===> MIGRATE HANDLER - STEP 2: Loading config root directory...");
    let config_root_directory = match base_config.config_root_directory.as_ref() {
        Some(path) => {
            println!("===> Using config root directory: {:?}", path);
            path
        },
        None => {
            return Err(anyhow!("'config_root_directory' is missing from the loaded configuration. Check your storage_config.yaml file."));
        }
    };

    // Preserve directories for migration
    let engine_types = vec![StorageEngineType::Sled, StorageEngineType::RocksDB, StorageEngineType::TiKV, StorageEngineType::InMemory, StorageEngineType::Redis, StorageEngineType::PostgreSQL, StorageEngineType::MySQL];
    let directories_to_preserve: Vec<PathBuf> = engine_types
        .iter()
        .filter(|&e| e != &to_engine)
        .map(|e| PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", e.to_string().to_lowercase(), base_config.default_port)))
        .filter(|p| p.exists())
        .collect();
    info!("Preserving directories for migration: {:?}", directories_to_preserve);
    println!("===> PRESERVING DIRECTORIES FOR MIGRATION: {:?}", directories_to_preserve);

    // Load engine-specific config for from_engine
    println!("===> MIGRATE HANDLER - STEP 3: Loading engine-specific configuration for from_engine...");
    let from_engine_specific_config = if from_engine == StorageEngineType::TiKV {
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
                            use_compression: false,
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
        let config = load_engine_specific_config(from_engine.clone(), config_root_directory)
            .map_err(|e| anyhow!("Failed to load engine-specific config for from_engine: {}", e))?;
        let mut map = selected_storage_config_to_hashmap(&config);
        map.insert("storage_engine_type".to_string(), Value::String(from_engine.to_string().to_lowercase()));
        map
    };
    println!("===> From engine-specific configuration loaded successfully.");

    // Load engine-specific config for to_engine
    println!("===> MIGRATE HANDLER - STEP 4: Loading engine-specific configuration for to_engine...");
    let to_engine_specific_config = if to_engine == StorageEngineType::TiKV {
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
                            use_compression: false,
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
        let config = load_engine_specific_config(to_engine.clone(), config_root_directory)
            .map_err(|e| anyhow!("Failed to load engine-specific config for to_engine: {}", e))?;
        let mut map = selected_storage_config_to_hashmap(&config);
        map.insert("storage_engine_type".to_string(), Value::String(to_engine.to_string().to_lowercase()));
        map
    };
    println!("===> To engine-specific configuration loaded successfully.");

    // Extract port for from_engine
    let from_port = from_engine_specific_config.get("port")
        .and_then(|v| match v {
            Value::Number(n) => n.as_u64().map(|p| p as u16),
            Value::String(s) => s.parse::<u16>().ok(),
            _ => None,
        })
        .unwrap_or_else(|| {
            if from_engine == StorageEngineType::TiKV {
                2380
            } else if from_engine == StorageEngineType::RocksDB {
                base_config.default_port
            } else {
                8052
            }
        });

    if from_engine != StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if from_port == pd_port {
                return Err(anyhow!("Selected from_port {} is reserved for TiKV PD and cannot be used for engine {:?}", from_port, from_engine));
            }
        }
    }

    // Extract port for to_engine
    let to_port = to_engine_specific_config.get("port")
        .and_then(|v| match v {
            Value::Number(n) => n.as_u64().map(|p| p as u16),
            Value::String(s) => s.parse::<u16>().ok(),
            _ => None,
        })
        .unwrap_or_else(|| {
            if to_engine == StorageEngineType::TiKV {
                2380
            } else if to_engine == StorageEngineType::RocksDB {
                base_config.default_port
            } else {
                8052
            }
        });

    if to_engine != StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if to_port == pd_port {
                return Err(anyhow!("Selected to_port {} is reserved for TiKV PD and cannot be used for engine {:?}", to_port, to_engine));
            }
        }
    }

    // Construct from_config
    let from_path = from_engine_specific_config.get("path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", from_engine.to_string().to_lowercase(), from_port))
        });
    let from_host = from_engine_specific_config.get("host")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let from_username = from_engine_specific_config.get("username")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if from_engine == StorageEngineType::TiKV { "tikv".to_string() } else { "".to_string() });
    let from_password = from_engine_specific_config.get("password")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if from_engine == StorageEngineType::TiKV { "tikv".to_string() } else { "".to_string() });
    let from_pd_endpoints = from_engine_specific_config.get("pd_endpoints")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if from_engine == StorageEngineType::TiKV { "127.0.0.1:2379".to_string() } else { "".to_string() });

    let from_engine_config = SelectedStorageConfig {
        storage_engine_type: from_engine.clone(),
        storage: StorageConfigInner {
            path: Some(from_path),
            host: Some(from_host),
            port: Some(from_port),
            database: from_engine_specific_config.get("database").and_then(|v| v.as_str()).map(String::from),
            username: Some(from_username),
            password: Some(from_password),
            pd_endpoints: if from_engine == StorageEngineType::TiKV { Some(from_pd_endpoints) } else { None },
            cache_capacity: Some(1024*1024*1024),
            use_compression: false,
            temporary: false,
            use_raft_for_scale: false,
        },
    };

    let mut from_config = base_config.clone();
    from_config.storage_engine_type = from_engine.clone();
    from_config.default_port = from_port;
    from_config.engine_specific_config = Some(from_engine_config.clone());

    // Construct to_config
    let to_path = to_engine_specific_config.get("path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", to_engine.to_string().to_lowercase(), to_port))
        });
    let to_host = to_engine_specific_config.get("host")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let to_username = to_engine_specific_config.get("username")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if to_engine == StorageEngineType::TiKV { "tikv".to_string() } else { "".to_string() });
    let to_password = to_engine_specific_config.get("password")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if to_engine == StorageEngineType::TiKV { "tikv".to_string() } else { "".to_string() });
    let to_pd_endpoints = to_engine_specific_config.get("pd_endpoints")
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| if to_engine == StorageEngineType::TiKV { "127.0.0.1:2379".to_string() } else { "".to_string() });

    let to_engine_config = SelectedStorageConfig {
        storage_engine_type: to_engine.clone(),
        storage: StorageConfigInner {
            path: Some(to_path),
            host: Some(to_host),
            port: Some(to_port),
            database: to_engine_specific_config.get("database").and_then(|v| v.as_str()).map(String::from),
            username: Some(to_username),
            password: Some(to_password),
            pd_endpoints: if to_engine == StorageEngineType::TiKV { Some(to_pd_endpoints) } else { None },
            cache_capacity: Some(1024*1024*1024),
            use_compression: false,
            temporary: false,
            use_raft_for_scale: false,
        },
    };

    let mut to_config = base_config.clone();
    to_config.storage_engine_type = to_engine.clone();
    to_config.default_port = to_port;
    to_config.engine_specific_config = Some(to_engine_config.clone());

    // Initialize or update StorageEngineManager for each active daemon
    println!("===> MIGRATE HANDLER - STEP 5: Initializing StorageEngineManager for {} daemons...", storage_daemons.len());
    let mut successful_migrations: Vec<u16> = Vec::new();
    let mut failed_migrations: Vec<(u16, String)> = Vec::new();

    for daemon in storage_daemons.iter() {
        let port = daemon.port;
        info!("Migrating storage daemon on port {}", port);
        println!("===> Migrating storage daemon on port {}", port);

        // Check if CLI is using the IPC socket
        let socket_path = format!("/tmp/graphdb-{}.ipc", port);
        if is_socket_used_by_cli(&socket_path).await.unwrap_or(false) {
            warn!("Port {} is used by CLI process. Skipping migration to avoid self-termination.", port);
            println!("===> WARNING: Port {} is used by CLI. Skipping.", port);
            failed_migrations.push((port, "Port used by CLI".to_string()));
            continue;
        }

        // Create port-specific configs
        let mut port_from_config = from_config.clone();
        port_from_config.default_port = port;
        if let Some(ref mut engine_config) = port_from_config.engine_specific_config {
            engine_config.storage.port = Some(port);
            engine_config.storage.path = Some(PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", from_engine.to_string().to_lowercase(), port)));
        }

        let mut port_to_config = to_config.clone();
        port_to_config.default_port = port;
        if let Some(ref mut engine_config) = port_to_config.engine_specific_config {
            engine_config.storage.port = Some(port);
            engine_config.storage.path = Some(PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", to_engine.to_string().to_lowercase(), port)));
        }

        // Initialize StorageEngineManager
        let async_manager = if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_none() {
            debug!("StorageEngineManager not initialized, creating new instance for port {}", port);
            println!("===> Creating new instance of StorageEngineManager for port {}...", port);
            let manager = StorageEngineManager::new(
                to_engine.clone(),
                &absolute_config_path,
                false,
                Some(port),
            )
            .await
            .context(format!("Failed to initialize StorageEngineManager for port {}", port))?;
            let async_manager = AsyncStorageEngineManager::from_manager(manager);
            GLOBAL_STORAGE_ENGINE_MANAGER
                .set(Arc::new(async_manager))
                .context("Failed to set GLOBAL_STORAGE_ENGINE_MANAGER")?;
            GLOBAL_STORAGE_ENGINE_MANAGER
                .get()
                .ok_or_else(|| GraphError::ConfigurationError("StorageEngineManager not accessible".to_string()))?
                .clone()
        } else {
            println!("===> Using existing StorageEngineManager for port {}...", port);
            GLOBAL_STORAGE_ENGINE_MANAGER
                .get()
                .ok_or_else(|| GraphError::ConfigurationError("StorageEngineManager not accessible".to_string()))?
                .clone()
        };

        // Perform migration
        println!("===> MIGRATE HANDLER - STEP 6: Performing migration for port {}...", port);
        if let Err(e) = async_manager.migrate_storage(port_from_config.clone(), port_to_config.clone()).await {
            warn!("Migration failed for port {}: {}", port, e);
            println!("===> ERROR: Migration failed for port {}: {}", port, e);
            failed_migrations.push((port, format!("Migration failed: {}", e)));
            continue;
        }
        successful_migrations.push(port);
        println!("===> Migration completed for port {}", port);
    }

    // Stop all running storage daemons
    println!("===> MIGRATE HANDLER - STEP 7: Stopping existing storage daemons...");
    let mut successfully_stopped_ports: Vec<u16> = Vec::new();
    let mut failed_to_stop_ports: Vec<u16> = Vec::new();

    for daemon in storage_daemons.iter() {
        let current_engine_str = daemon.engine_type.as_deref().unwrap_or("unknown");
        let expected_engine_str = daemon_api_storage_engine_type_to_string(&to_engine);

        if current_engine_str == expected_engine_str && successful_migrations.contains(&daemon.port) {
            info!("Existing daemon on port {} already uses engine type {}, reusing it.", daemon.port, expected_engine_str);
            successfully_stopped_ports.push(daemon.port);
            println!("Skipping shutdown of Storage Daemon on port {} (already using {}).", daemon.port, expected_engine_str);
            continue;
        }

        if let Some(pd_port) = tikv_pd_port {
            if daemon.port == pd_port && current_engine_str == TIKV_DAEMON_ENGINE_TYPE_NAME {
                info!("Skipping shutdown of TiKV PD daemon on port {}. Considered handled.", daemon.port);
                successfully_stopped_ports.push(daemon.port);
                continue;
            }
        }

        let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
        if is_socket_used_by_cli(&socket_path).await.unwrap_or(false) {
            warn!("Port {} is used by CLI process. Skipping shutdown to avoid self-termination.", daemon.port);
            println!("===> WARNING: Port {} is used by CLI. Skipping shutdown.", daemon.port);
            failed_to_stop_ports.push(daemon.port);
            continue;
        }

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

        let mut is_daemon_stopped = false;
        for attempt in 0..MAX_SHUTDOWN_RETRIES {
            debug!("Attempting to stop storage daemon on port {} (Attempt {} of {})", daemon.port, attempt + 1, MAX_SHUTDOWN_RETRIES);

            if let Err(e) = stop_specific_storage_daemon(daemon.port, true).await {
                warn!("Failed to stop storage daemon on port {}: {}", daemon.port, e);
                println!("===> WARNING: Failed to stop daemon on port {}: {}", daemon.port, e);
            }

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
                is_daemon_stopped = true;
                if pid_file_path.exists() {
                    tokio_fs::remove_file(&pid_file_path).await.ok();
                }
                break;
            }

            info!("Storage daemon still running on port {}, retrying stop in {}ms...", daemon.port, SHUTDOWN_RETRY_DELAY_MS);
            sleep(TokioDuration::from_millis(SHUTDOWN_RETRY_DELAY_MS)).await;
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

    // Restart storage daemons with to_engine
    println!("===> MIGRATE HANDLER - STEP 8: Restarting storage daemons...");
    let mut daemon_ports_to_restart = successful_migrations.clone();
    if !daemon_ports_to_restart.contains(&to_port) {
        daemon_ports_to_restart.push(to_port);
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
        info!("No storage daemons to restart, starting single daemon on port {}", to_port);
        println!("===> No existing storage cluster found, starting single daemon...");

        let mut port_specific_config = to_config.clone();
        if let Some(ref mut engine_config) = port_specific_config.engine_specific_config {
            engine_config.storage.port = Some(to_port);
            engine_config.storage.path = Some(PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", to_engine.to_string().to_lowercase(), to_port)));
        }

        let socket_path = format!("/tmp/graphdb-{}.ipc", to_port);
        if tokio_fs::metadata(&socket_path).await.is_ok() {
            info!("Removing stale ZeroMQ socket file: {}", socket_path);
            tokio_fs::remove_file(&socket_path)
                .await
                .context(format!("Failed to remove stale ZeroMQ socket file {}", socket_path))?;
        }

        start_storage_interactive(
            Some(to_port),
            Some(absolute_config_path.clone()),
            Some(port_specific_config),
            Some("skip_manager_set".to_string()),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
            Arc::new(TokioMutex::new(None)),
        ).await.context("Failed to start storage daemon")?;

        successful_restarts.push(to_port);
        println!("===> ✓ Storage daemon started successfully with {} engine on port {}", 
                 daemon_api_storage_engine_type_to_string(&to_engine), to_port);
    } else {
        info!("Restarting {} storage daemons on ports: {:?}", daemon_ports_to_restart.len(), daemon_ports_to_restart);
        println!("===> Restarting {} storage daemons with {} engine...", daemon_ports_to_restart.len(), 
                 daemon_api_storage_engine_type_to_string(&to_engine));

        for (index, port) in daemon_ports_to_restart.iter().enumerate() {
            println!("===> Restarting storage daemon {} of {} on port {}...", 
                     index + 1, daemon_ports_to_restart.len(), port);

            let mut port_specific_config = to_config.clone();
            if let Some(ref mut engine_config) = port_specific_config.engine_specific_config {
                engine_config.storage.port = Some(*port);
                engine_config.storage.path = Some(PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", to_engine.to_string().to_lowercase(), port)));
            }

            let socket_path = format!("/tmp/graphdb-{}.ipc", port);
            if tokio_fs::metadata(&socket_path).await.is_ok() {
                info!("Removing stale ZeroMQ socket file: {}", socket_path);
                tokio_fs::remove_file(&socket_path)
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
                sleep(TokioDuration::from_millis(1000)).await;
            }
        }

        if !successful_restarts.is_empty() {
            info!("Successfully restarted {} storage daemons on ports: {:?}", 
                 successful_restarts.len(), successful_restarts);
            println!("===> ✓ Successfully restarted {} storage daemons with {} engine", 
                     successful_restarts.len(), daemon_api_storage_engine_type_to_string(&to_engine));
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
    println!("===> MIGRATE HANDLER - STEP 9: Verifying storage daemon cluster status...");
    sleep(TokioDuration::from_millis(2000)).await;

    let current_storage_daemons = GLOBAL_DAEMON_REGISTRY
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|daemon| daemon.service_type == "storage")
        .collect::<Vec<DaemonMetadata>>();

    let running_ports: Vec<u16> = current_storage_daemons.iter().map(|d| d.port).collect();
    let expected_engine_str = daemon_api_storage_engine_type_to_string(&to_engine);

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

    if !failed_migrations.is_empty() {
        warn!("Migration failed for ports: {:?}", failed_migrations);
        println!("===> ✗ Failed to migrate {} storage daemons", failed_migrations.len());
        for (port, error) in failed_migrations {
            println!("     Port {}: {}", port, error);
        }
        if successful_migrations.is_empty() {
            return Err(anyhow!("Failed to migrate all storage daemons"));
        }
    } else {
        println!("===> ✓ Successfully migrated all storage daemons to {}", expected_engine_str);
    }

    info!("Successfully migrated from engine {:?} to {:?}", from_engine, to_engine);
    println!("Migration completed in {}ms", start_time.elapsed().as_millis());
    Ok(())
}

/// Handles the interactive 'migrate' command.
pub async fn handle_migrate_interactive(
    from_engine: StorageEngineType,
    to_engine: StorageEngineType,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    handle_migrate_command(from_engine, to_engine).await?;
    reload_storage_interactive(
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc,
    ).await?;
    Ok(())
}

/// Handles the `use storage <engine> [--permanent] [--migrate]` command.
/// The command **only** changes the *default* engine and the *default* port.
/// All existing storage daemons stay alive and stay registered.
/// **Never stops any daemon** – only changes the *default* engine/port.
pub async fn handle_use_storage_command(
    engine: StorageEngineType,
    permanent: bool,
    migrate: bool,
) -> Result<(), anyhow::Error> {
    let start_time = Instant::now();
    info!("=== handle_use_storage_command engine:{:?} permanent:{} migrate:{} ===", engine, permanent, migrate);

    // -----------------------------------------------------------------
    // 1. Load config
    // -----------------------------------------------------------------
    let cwd = std::env::current_dir()?;
    let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let absolute_config_path = cwd.join(&config_path);
    let mut current_config: StorageConfig = load_storage_config_from_yaml(Some(absolute_config_path.clone()))
        .await
        .unwrap_or_default();

    let config_root_directory = current_config.config_root_directory.as_ref()
        .ok_or_else(|| anyhow!("'config_root_directory' is missing"))?;

    // -----------------------------------------------------------------
    // 2. Load TiKV PD port
    // -----------------------------------------------------------------
    let tikv_pd_port = {
        let tikv_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if tikv_config_path.exists() {
            match std::fs::read_to_string(&tikv_config_path) {
                Ok(content) => match serde_yaml::from_str::<TiKVConfigWrapper>(&content) {
                    Ok(wrapper) => wrapper.storage.pd_endpoints.and_then(|pd| pd.split(':').last().and_then(|p| p.parse::<u16>().ok())),
                    Err(e) => { warn!("Failed to parse TiKV config: {}", e); None }
                },
                Err(e) => { warn!("Failed to read TiKV config: {}", e); None }
            }
        } else {
            None
        }
    };

    // -----------------------------------------------------------------
    // 3. Load engine-specific config → HashMap
    // -----------------------------------------------------------------
    let engine_specific_config = if engine == StorageEngineType::TiKV {
        let tikv_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV);
        if tikv_config_path.exists() {
            let content = std::fs::read_to_string(&tikv_config_path)?;
            match serde_yaml::from_str::<TiKVConfigWrapper>(&content) {
                Ok(wrapper) => {
                    let mut map = HashMap::new();
                    map.insert("storage_engine_type".to_string(), Value::String("tikv".to_string()));
                    if let Some(path) = wrapper.storage.path { map.insert("path".to_string(), Value::String(path.to_string_lossy().to_string())); }
                    if let Some(host) = wrapper.storage.host { map.insert("host".to_string(), Value::String(host)); }
                    if let Some(port) = wrapper.storage.port { map.insert("port".to_string(), Value::Number(port.into())); }
                    if let Some(pd) = wrapper.storage.pd_endpoints { map.insert("pd_endpoints".to_string(), Value::String(pd)); }
                    if let Some(user) = wrapper.storage.username { map.insert("username".to_string(), Value::String(user)); }
                    if let Some(pass) = wrapper.storage.password { map.insert("password".to_string(), Value::String(pass)); }
                    map
                },
                Err(e) => {
                    warn!("TiKV config parse error: {}. Using defaults.", e);
                    let mut map = HashMap::new();
                    map.insert("storage_engine_type".to_string(), Value::String("tikv".to_string()));
                    map.insert("path".to_string(), Value::String("/opt/graphdb/storage_data/tikv".to_string()));
                    map.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                    map.insert("port".to_string(), Value::Number(2380.into()));
                    map.insert("pd_endpoints".to_string(), Value::String("127.0.0.1:2379".to_string()));
                    map
                }
            }
        } else {
            let mut map = HashMap::new();
            map.insert("storage_engine_type".to_string(), Value::String("tikv".to_string()));
            map.insert("path".to_string(), Value::String("/opt/graphdb/storage_data/tikv".to_string()));
            map.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
            map.insert("port".to_string(), Value::Number(2380.into()));
            map.insert("pd_endpoints".to_string(), Value::String("127.0.0.1:2379".to_string()));
            map
        }
    } else {
        let config = lib::config::load_engine_specific_config(engine.clone(), config_root_directory)?;
        let mut map = selected_storage_config_to_hashmap(&config);
        map.insert("storage_engine_type".to_string(), Value::String(engine.to_string().to_lowercase()));
        map
    };

    // -----------------------------------------------------------------
    // 4. Extract port
    // -----------------------------------------------------------------
    let new_port = engine_specific_config.get("port")
        .and_then(|v| match v {
            Value::Number(n) => n.as_u64().map(|p| p as u16),
            Value::String(s) => s.parse::<u16>().ok(),
            _ => None,
        })
        .unwrap_or_else(|| {
            if engine == StorageEngineType::TiKV {
                2380
            } else if engine == StorageEngineType::RocksDB {
                current_config.default_port  // ← EXISTS
            } else {
                8052
            }
        });

    if engine != StorageEngineType::TiKV {
        if let Some(pd_port) = tikv_pd_port {
            if new_port == pd_port {
                return Err(anyhow!("Port {} is reserved for TiKV PD", new_port));
            }
        }
    }

    // -----------------------------------------------------------------
    // 5. Build path
    // -----------------------------------------------------------------
    let path = engine_specific_config.get("path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("/opt/graphdb/storage_data/{}/{}", engine.to_string().to_lowercase(), new_port)));

    // -----------------------------------------------------------------
    // 6. Build SelectedStorageConfig
    // -----------------------------------------------------------------
    let engine_config = SelectedStorageConfig {
        storage_engine_type: engine.clone(),
        storage: StorageConfigInner {
            path: Some(path.clone()),
            host: engine_specific_config.get("host").and_then(|v| v.as_str()).map(String::from),
            port: Some(new_port),
            database: engine_specific_config.get("database").and_then(|v| v.as_str()).map(String::from),
            username: engine_specific_config.get("username").and_then(|v| v.as_str()).map(String::from),
            password: engine_specific_config.get("password").and_then(|v| v.as_str()).map(String::from),
            pd_endpoints: if engine == StorageEngineType::TiKV {
                engine_specific_config.get("pd_endpoints").and_then(|v| v.as_str()).map(String::from)
            } else { None },
            cache_capacity: Some(1024 * 1024 * 1024),
            use_compression: false,
            temporary: false,
            use_raft_for_scale: false,
        },
    };

    // -----------------------------------------------------------------
    // 7. Update config
    // -----------------------------------------------------------------
    let mut new_config = current_config.clone();
    new_config.storage_engine_type = engine.clone();
    new_config.default_port = new_port;  // ← EXISTS, REQUIRED
    new_config.engine_specific_config = Some(engine_config.clone());

    // -----------------------------------------------------------------
    // 8. Save if permanent
    // -----------------------------------------------------------------
    if permanent {
        new_config.save().await?;
        println!("Configuration saved successfully.");
    }

    // -----------------------------------------------------------------
    // 9. STOP ONLY DAEMONS ON new_port
    // -----------------------------------------------------------------
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await?;
    for daemon in all_daemons {
        if daemon.service_type == "storage" && daemon.port == new_port {
            println!("Stopping storage daemon on port {}...", daemon.port);
            stop_storage_interactive(
                Some(daemon.port),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await.ok();
        }
    }

    // -----------------------------------------------------------------
    // 10. Start daemon → singleton reused
    // -----------------------------------------------------------------
    let mut port_specific_config = new_config.clone();
    if let Some(ref mut ec) = port_specific_config.engine_specific_config {
        ec.storage.port = Some(new_port);
        ec.storage.path = Some(path);
    }

    let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", new_port);
    if tokio_fs::metadata(&socket_path).await.is_ok() {
        tokio_fs::remove_file(&socket_path).await.ok();
    }

    start_storage_interactive(
        Some(new_port),
        Some(absolute_config_path.clone()),
        Some(port_specific_config),
        Some("skip_manager_set".to_string()),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
    ).await?;

    // -----------------------------------------------------------------
    // 11. Update manager
    // -----------------------------------------------------------------
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_none() {
        let manager = StorageEngineManager::new(engine.clone(), &absolute_config_path, permanent, Some(new_port)).await?;
        GLOBAL_STORAGE_ENGINE_MANAGER.set(Arc::new(AsyncStorageEngineManager::from_manager(manager)))?;
    } else {
        let async_manager = GLOBAL_STORAGE_ENGINE_MANAGER.get().unwrap();
        async_manager.use_storage(new_config.clone(), permanent, migrate).await?;
    }

    println!("Switched to {} on port {} (persisted: {})", engine, new_port, permanent);
    info!("=== Completed in {}ms ===", start_time.elapsed().as_millis());
    Ok(())
}

/// Handles the interactive 'use storage' command.
pub async fn handle_use_storage_interactive(
    engine: StorageEngineType,
    permanent: bool,
    migrate: bool,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    handle_use_storage_command(engine.clone(), permanent, migrate).await?;
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
        sleep(TokioDuration::from_millis(500)).await;
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

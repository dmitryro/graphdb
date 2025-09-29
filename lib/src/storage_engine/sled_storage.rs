use anyhow::{Result, Context, anyhow};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use once_cell::sync::Lazy;
use tokio::fs;
use tokio::sync::{OnceCell, Mutex as TokioMutex};
use log::{info, debug, warn, error, trace};
pub use crate::config::{
    SledDbWithPath, SledConfig, SledStorage, SledDaemon, SledDaemonPool, load_storage_config_from_yaml, 
    DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, StorageConfig, StorageEngineType,
    QueryResult, QueryPlan, SledClientMode, 
};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
use uuid::Uuid;
use async_trait::async_trait;
use crate::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::daemon_utils::{is_storage_daemon_running, parse_cluster_range};
use crate::daemon::daemon_management::{ check_pid_validity, find_pid_by_port, stop_process_by_pid };
use serde_json::Value;
use std::any::Any;
use futures::future::join_all;
use tokio::time::{timeout, Duration};
use crate::storage_engine::sled_client::{ SledClient, ZmqSocketWrapper };
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};

pub static SLED_DB: LazyLock<OnceCell<TokioMutex<SledDbWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static SLED_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<SledDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());

// Static variable to track active Sled database instances
static SLED_ACTIVE_DATABASES: Lazy<TokioMutex<HashSet<PathBuf>>> = Lazy::new(|| TokioMutex::new(HashSet::new()));

impl SledStorage {
    // Ensure only one instance can access the database at a given path
    pub async fn ensure_single_instance(path: &Path) -> GraphResult<()> {
        let mut active_dbs = SLED_ACTIVE_DATABASES.lock().await;
        if active_dbs.contains(path) {
            error!("Database at {:?} is already in use by another instance", path);
            println!("===> ERROR: DATABASE AT {:?} ALREADY IN USE", path);
            return Err(GraphError::StorageError(
                format!("Database at {:?} is already in use by another instance", path)
            ));
        }
        active_dbs.insert(path.to_path_buf());
        info!("Registered database instance at {:?}", path);
        println!("===> REGISTERED DATABASE INSTANCE AT {:?}", path);
        Ok(())
    }

    // Release a database instance from the active set
    pub async fn release_instance(path: &Path) {
        let mut active_dbs = SLED_ACTIVE_DATABASES.lock().await;
        if active_dbs.remove(path) {
            info!("Released database instance at {:?}", path);
            println!("===> RELEASED DATABASE INSTANCE AT {:?}", path);
        }
    }

    pub async fn check_and_cleanup_stale_daemon(port: u16, db_path: &PathBuf) -> GraphResult<()> {
        info!("Checking for stale SledDaemon on port {} with db_path {:?}", port, db_path);
        println!("===> CHECKING FOR STALE SLED DAEMON ON PORT {} WITH DB_PATH {:?}", port, db_path);

        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
        let lock_path = db_path.join("db.lck");

        // Check and clean up IPC socket
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            info!("Found IPC socket file at {}", socket_path);
            println!("===> FOUND IPC SOCKET FILE AT {}", socket_path);

            // Attempt to ping the daemon to verify if it's responsive
            match SledClient::ping_daemon(port, &socket_path).await {
                Ok(_) => {
                    info!("SledDaemon on port {} is responsive, attempting graceful shutdown", port);
                    println!("===> SLED DAEMON ON PORT {} IS RESPONSIVE, ATTEMPTING GRACEFUL SHUTDOWN", port);

                    // Find and stop the process
                    if let Some(pid) = find_pid_by_port(port).await {
                        if check_pid_validity(pid).await {
                            if let Err(e) = stop_process_by_pid("Storage Daemon", pid).await {
                                warn!("Failed to stop process {} for port {}: {}", pid, port, e);
                                println!("===> ERROR: FAILED TO STOP PROCESS {} FOR PORT {}", pid, port);
                            } else {
                                info!("Successfully stopped process {} for port {}", pid, port);
                                println!("===> SUCCESSFULLY STOPPED PROCESS {} FOR PORT {}", pid, port);
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("SledDaemon on port {} is unresponsive: {}. Cleaning up.", port, e);
                    println!("===> SLED DAEMON ON PORT {} IS UNRESPONSIVE, CLEANING UP", port);
                }
            }

            // Remove stale socket file
            if let Err(e) = tokio::fs::remove_file(&socket_path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    error!("Failed to remove stale IPC socket {}: {}", socket_path, e);
                    println!("===> ERROR: FAILED TO REMOVE STALE IPC SOCKET {}: {}", socket_path, e);
                    return Err(GraphError::StorageError(format!("Failed to remove stale IPC socket {}: {}", socket_path, e)));
                }
            }
            info!("Cleaned up IPC socket for port {}", port);
            println!("===> CLEANED UP IPC SOCKET FOR PORT {}", port);
        } else {
            info!("No IPC socket found for port {}", port);
            println!("===> NO IPC SOCKET FOUND FOR PORT {}", port);
        }

        // Check for stale lock file (only log, don’t remove since we assume it’s not locked)
        if tokio::fs::metadata(&lock_path).await.is_ok() {
            warn!("Found lock file at {}. Logging for diagnostics but not removing.", lock_path.display());
            println!("===> FOUND LOCK FILE AT {}. LOGGING BUT NOT REMOVING", lock_path.display());
        }

        // Check for running processes using sysinfo
        let mut system = System::new_with_specifics(RefreshKind::everything().with_processes(ProcessRefreshKind::everything()));
        system.refresh_processes_specifics(ProcessesToUpdate::All, true, ProcessRefreshKind::everything());
        let mut found_process = false;
        for (pid, process) in system.processes() {
            if process.cmd().iter().any(|arg| arg.to_string_lossy().contains(&format!("graphdb-{}", port))) {
                warn!("Found process {} running for port {}. Attempting to terminate.", pid, port);
                println!("===> FOUND PROCESS {} RUNNING FOR PORT {}. TERMINATING", pid, port);
                if process.kill() {
                    info!("Successfully terminated process {} for port {}", pid, port);
                    println!("===> SUCCESSFULLY TERMINATED PROCESS {} FOR PORT {}", pid, port);
                    found_process = true;
                } else {
                    error!("Failed to terminate process {} for port {}", pid, port);
                    println!("===> ERROR: FAILED TO TERMINATE PROCESS {} FOR PORT {}", pid, port);
                    return Err(GraphError::StorageError(format!("Failed to terminate process {} for port {}", pid, port)));
                }
            }
        }

        if found_process {
            // Wait briefly to ensure process termination completes
            tokio::time::sleep(Duration::from_millis(500)).await;
        } else {
            info!("No running processes found for port {}", port);
            println!("===> NO RUNNING PROCESSES FOUND FOR PORT {}", port);
        }

        // Clean up any temporary Sled files (e.g., snapshots or logs)
        let temp_files = ["snap", "log"];
        for file in temp_files.iter() {
            let temp_path = db_path.join(file);
            if tokio::fs::metadata(&temp_path).await.is_ok() {
                if let Err(e) = tokio::fs::remove_file(&temp_path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!("Failed to remove temporary file {}: {}", temp_path.display(), e);
                        println!("===> WARNING: FAILED TO REMOVE TEMPORARY FILE {}: {}", temp_path.display(), e);
                    }
                } else {
                    info!("Removed temporary file {}", temp_path.display());
                    println!("===> REMOVED TEMPORARY FILE {}", temp_path.display());
                }
            }
        }

        info!("Stale daemon check and cleanup complete for port {}", port);
        println!("===> STALE DAEMON CHECK AND CLEANUP COMPLETE FOR PORT {}", port);
        Ok(())
    }

    pub async fn new(config: &SledConfig, storage_config: &StorageConfig) -> Result<Self, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with config: {:?}", config);
        println!("===> INITIALIZING SledStorage with config: {:?}", config);

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());
        info!("Using Sled path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        // Create directory if it doesn't exist
        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO CREATE SLED DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
            })?;

        // Check if directory is writable
        let metadata = fs::metadata(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to access directory metadata at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
            })?;
        if metadata.permissions().readonly() {
            error!("Directory at {:?} is not writable", db_path);
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }
        info!("Directory at {:?}", db_path);
        println!("===> Directory at {:?} is writable", db_path);

        // Check if compression is requested but not supported
        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            error!("Compression is enabled in config but Sled was compiled without compression feature");
            println!("===> ERROR: COMPRESSION ENABLED BUT SLED COMPILED WITHOUT COMPRESSION FEATURE");
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build. Please disable use_compression in config or recompile with compression feature.".to_string()
            ));
        }

        // Clean up stale directories for other engines
        for engine in &["rocksdb", "tikv"] {
            let other_engine_path = base_data_dir.join(engine).join(port.to_string());
            if tokio::fs::metadata(&other_engine_path).await.is_ok() {
                warn!("Found stale {} directory at {:?}", engine, other_engine_path);
                println!("===> WARNING: FOUND STALE {} DIRECTORY AT {:?}", engine.to_uppercase(), other_engine_path);
                if let Err(e) = tokio::fs::remove_dir_all(&other_engine_path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!("Failed to remove stale {} directory at {:?}: {}", engine, other_engine_path, e);
                        println!("===> WARNING: FAILED TO REMOVE STALE {} DIRECTORY AT {:?}", engine.to_uppercase(), other_engine_path);
                    }
                } else {
                    info!("Removed stale {} directory at {:?}", engine, other_engine_path);
                    println!("===> REMOVED STALE {} DIRECTORY AT {:?}", engine.to_uppercase(), other_engine_path);
                }
            }
        }

        // Check for existing SLED_DB instance
        if let Some(sled_db) = SLED_DB.get() {
            let sled_db_guard = sled_db.lock().await;
            if sled_db_guard.path == db_path && sled_db_guard.db.open_tree("vertices").is_ok() && sled_db_guard.db.open_tree("edges").is_ok() {
                info!("Reusing existing SLED_DB singleton for path {:?}", db_path);
                println!("===> REUSING EXISTING SLED_DB SINGLETON FOR PATH {:?}", db_path);
                let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| {
                        error!("Failed to acquire pool map lock for port {}", port);
                        println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                        GraphError::StorageError("Failed to acquire pool map lock".to_string())
                    })?;
                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing SledDaemonPool for port {}", port);
                    println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                    return Ok(Self { pool: existing_pool.clone() });
                }
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                return Ok(Self { pool: new_pool });
            }
        }

        // Check for existing daemon
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let daemon_metadata_opt = daemon_registry.get_daemon_metadata(port).await.ok().flatten();
        if let Some(daemon_metadata) = daemon_metadata_opt {
            info!("Found existing daemon on port {} with PID {}", port, daemon_metadata.pid);
            println!("===> FOUND EXISTING DAEMON ON PORT {} WITH PID {}", port, daemon_metadata.pid);

            if is_storage_daemon_running(port).await && check_pid_validity(daemon_metadata.pid).await {
                info!("Reusing existing daemon on port {}", port);
                println!("===> REUSING EXISTING DAEMON ON PORT {} WITH PID {}", port, daemon_metadata.pid);

                let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| {
                        error!("Failed to acquire pool map lock for port {}", port);
                        println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                        GraphError::StorageError("Failed to acquire pool map lock".to_string())
                    })?;

                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing SledDaemonPool for port {}", port);
                    println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                    return Ok(Self { pool: existing_pool.clone() });
                }

                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());

                let db_path_clone = db_path.clone();
                let sled_db_instance = SLED_DB.get_or_try_init(|| async {
                    info!("Opening new Sled database at {:?}", db_path_clone);
                    println!("===> ATTEMPTING TO OPEN SLED DB AT {:?}", db_path_clone);
                    Self::check_and_cleanup_stale_daemon(port, &db_path_clone).await?;
                    let mut sled_config = sled::Config::new()
                        .path(&db_path_clone)
                        .cache_capacity(config.cache_capacity.unwrap_or(1024 * 1024 * 1024));
                    #[cfg(feature = "compression")]
                    {
                        sled_config = sled_config.use_compression(config.use_compression);
                    }
                    let db = sled_config
                        .open()
                        .map_err(|e| {
                            error!("Failed to open Sled database at {:?}: {}", db_path_clone, e);
                            println!("===> ERROR: FAILED TO OPEN SLED DB AT {:?}", db_path_clone);
                            GraphError::StorageError(format!("Failed to open Sled database at {:?}: {}", db_path_clone, e))
                        })?;
                    let vertices = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
                    let edges = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
                    let kv_count = db.iter().count();
                    info!("Opened database at {:?} with {} kv_pairs, {} vertices, {} edges", db_path_clone, kv_count, vertices.iter().count(), edges.iter().count());
                    println!("===> OPENED DATABASE AT {:?} WITH {} KV_PAIRS, {} VERTICES, {} EDGES", db_path_clone, kv_count, vertices.iter().count(), edges.iter().count());
                    Ok::<TokioMutex<SledDbWithPath>, GraphError>(TokioMutex::new(SledDbWithPath {
                        db: Arc::new(db),
                        path: db_path_clone.clone(),
                        client: None,
                    }))
                }).await?;

                {
                    let mut pool_guard = timeout(Duration::from_secs(10), new_pool.lock())
                        .await
                        .map_err(|_| {
                            error!("Failed to acquire pool lock for initialization on port {}", port);
                            println!("===> ERROR: FAILED TO ACQUIRE POOL LOCK FOR INITIALIZATION ON PORT {}", port);
                            GraphError::StorageError("Failed to acquire pool lock for initialization".to_string())
                        })?;
                    info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
                    println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
                    let sled_db_guard = sled_db_instance.lock().await;
                    timeout(Duration::from_secs(10), pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), sled_db_guard.db.clone()))
                        .await
                        .map_err(|_| {
                            error!("Timeout initializing SledDaemonPool for port {}", port);
                            println!("===> ERROR: TIMEOUT INITIALIZING SLED DAEMON POOL FOR PORT {}", port);
                            GraphError::StorageError("Timeout initializing SledDaemonPool".to_string())
                        })??;
                    info!("Initialized cluster on port {} with existing DB", port);
                    println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
                }

                let (client, socket) = SledClient::new_with_port(port).await
                    .map_err(|e| {
                        error!("Failed to initialize ZMQ client for port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO INITIALIZE ZMQ CLIENT FOR PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to initialize ZMQ client for port {}: {}", port, e))
                    })?;

                let mut sled_db_guard = sled_db_instance.lock().await;
                sled_db_guard.client = Some((client, socket));

                let daemon_metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                    data_dir: Some(db_path.clone()),
                    config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                    engine_type: Some("sled".to_string()),
                    last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
                    pid: std::process::id(),
                    port,
                };
                daemon_registry.register_daemon(daemon_metadata).await
                    .map_err(|e| {
                        error!("Failed to register daemon for port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
                    })?;
                info!("Registered daemon for port {}", port);
                println!("===> REGISTERED DAEMON FOR PORT {}", port);

                return Ok(Self { pool: new_pool });
            } else {
                warn!("Stale daemon found on port {}. Cleaning up...", port);
                println!("===> STALE DAEMON FOUND ON PORT {}. CLEANING UP", port);
                daemon_registry.remove_daemon_by_type("sled", port).await
                    .map_err(|e| {
                        error!("Failed to remove daemon registry entry for port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO REMOVE DAEMON REGISTRY ENTRY FOR PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to remove daemon registry entry: {}", e))
                    })?;
            }
        }

        info!("No existing daemon found for port {}. Opening new database...", port);
        println!("===> NO EXISTING DAEMON FOUND FOR PORT {}. CREATING NEW POOL", port);

        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        Self::ensure_single_instance(&db_path).await
            .map_err(|e| {
                error!("Cannot initialize Sled at {:?}: {}", db_path, e);
                println!("===> ERROR: CANNOT INITIALIZE SLED AT {:?}: {}", db_path, e);
                e
            })?;

        let pool = {
            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                .await
                .map_err(|_| {
                    error!("Failed to acquire pool map lock for port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool map lock".to_string())
                })?;
            let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
            pool_map_guard.insert(port, new_pool.clone());
            new_pool
        };

        let db_path_clone = db_path.clone();
        let sled_db_instance = match SLED_DB.get_or_try_init(|| async {
            info!("Opening new Sled database at {:?}", db_path_clone);
            println!("===> ATTEMPTING TO OPEN SLED DB AT {:?}", db_path_clone);
            let mut sled_config = sled::Config::new()
                .path(&db_path_clone)
                .cache_capacity(config.cache_capacity.unwrap_or(1024 * 1024 * 1024));
            #[cfg(feature = "compression")]
            {
                sled_config = sled_config.use_compression(config.use_compression);
            }
            let db = sled_config
                .open()
                .map_err(|e| {
                    error!("Failed to open Sled database at {:?}: {}", db_path_clone, e);
                    println!("===> ERROR: FAILED TO OPEN SLED DB AT {:?}", db_path_clone);
                    GraphError::StorageError(format!("Failed to open Sled database at {:?}: {}", db_path_clone, e))
                })?;
            let vertices = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edges = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
            let kv_count = db.iter().count();
            info!("Opened database at {:?} with {} kv_pairs, {} vertices, {} edges", db_path_clone, kv_count, vertices.iter().count(), edges.iter().count());
            println!("===> OPENED DATABASE AT {:?} WITH {} KV_PAIRS, {} VERTICES, {} EDGES", db_path_clone, kv_count, vertices.iter().count(), edges.iter().count());
            Ok::<TokioMutex<SledDbWithPath>, GraphError>(TokioMutex::new(SledDbWithPath {
                db: Arc::new(db),
                path: db_path_clone.clone(),
                client: None,
            }))
        }).await {
            Ok(instance) => instance,
            Err(e) => {
                Self::release_instance(&db_path).await;
                return Err(e);
            }
        };

        {
            let mut pool_guard = timeout(Duration::from_secs(10), pool.lock())
                .await
                .map_err(|_| {
                    error!("Failed to acquire pool lock for initialization on port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL LOCK FOR INITIALIZATION ON PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool lock for initialization".to_string())
                })?;
            info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            let sled_db_guard = sled_db_instance.lock().await;
            timeout(Duration::from_secs(10), pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), sled_db_guard.db.clone()))
                .await
                .map_err(|_| {
                    error!("Timeout initializing SledDaemonPool for port {}", port);
                    println!("===> ERROR: TIMEOUT INITIALIZING SLED DAEMON POOL FOR PORT {}", port);
                    GraphError::StorageError("Timeout initializing SledDaemonPool".to_string())
                })??;
            info!("Initialized cluster on port {} with existing DB", port);
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        let (client, socket) = SledClient::new_with_port(port).await
            .map_err(|e| {
                error!("Failed to initialize ZMQ client for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO INITIALIZE ZMQ CLIENT FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to initialize ZMQ client for port {}: {}", port, e))
            })?;

        let mut sled_db_guard = sled_db_instance.lock().await;
        sled_db_guard.client = Some((client, socket));

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            pid: std::process::id(),
            port,
        };
        daemon_registry.register_daemon(daemon_metadata).await
            .map_err(|e| {
                error!("Failed to register daemon for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
            })?;
        info!("Registered daemon for port {}", port);
        println!("===> REGISTERED DAEMON FOR PORT {}", port);

        info!("Successfully initialized SledStorage in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED SledStorage IN {}ms", start_time.elapsed().as_millis());
        Ok(Self { pool })
    }

    pub async fn new_with_db(config: &SledConfig, storage_config: &StorageConfig, existing_db: Arc<sled::Db>) -> Result<Self, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with existing database at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH EXISTING DB AT {:?}", config.path);

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());
        info!("Using Sled path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        // Check if compression is requested but not supported
        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            error!("Compression is enabled in config but Sled was compiled without compression feature");
            println!("===> ERROR: COMPRESSION ENABLED BUT SLED COMPILED WITHOUT COMPRESSION FEATURE");
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build. Please disable use_compression in config or recompile with compression feature.".to_string()
            ));
        }

        // Create directory if it doesn't exist
        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO CREATE SLED DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
            })?;

        // Check if directory is writable
        let metadata = fs::metadata(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to access directory metadata at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
            })?;
        if metadata.permissions().readonly() {
            error!("Directory at {:?} is not writable", db_path);
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }
        info!("Directory at {:?}", db_path);
        println!("===> Directory at {:?} is writable", db_path);

        // Clean up stale directories for other engines
        for engine in &["rocksdb", "tikv"] {
            let other_engine_path = base_data_dir.join(engine).join(port.to_string());
            if tokio::fs::metadata(&other_engine_path).await.is_ok() {
                warn!("Found stale {} directory at {:?}", engine, other_engine_path);
                println!("===> WARNING: FOUND STALE {} DIRECTORY AT {:?}", engine.to_uppercase(), other_engine_path);
                if let Err(e) = tokio::fs::remove_dir_all(&other_engine_path).await {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        warn!("Failed to remove stale {} directory at {:?}: {}", engine, other_engine_path, e);
                        println!("===> WARNING: FAILED TO REMOVE STALE {} DIRECTORY AT {:?}", engine.to_uppercase(), other_engine_path);
                    }
                } else {
                    info!("Removed stale {} directory at {:?}", engine, other_engine_path);
                    println!("===> REMOVED STALE {} DIRECTORY AT {:?}", engine.to_uppercase(), other_engine_path);
                }
            }
        }

        // Check for existing SLED_DB instance
        if let Some(sled_db) = SLED_DB.get() {
            let sled_db_guard = sled_db.lock().await;
            if sled_db_guard.path == db_path && sled_db_guard.db.open_tree("vertices").is_ok() && sled_db_guard.db.open_tree("edges").is_ok() {
                info!("Reusing existing SLED_DB singleton for path {:?}", db_path);
                println!("===> REUSING EXISTING SLED_DB SINGLETON FOR PATH {:?}", db_path);
                let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| {
                        error!("Failed to acquire pool map lock for port {}", port);
                        println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                        GraphError::StorageError("Failed to acquire pool map lock".to_string())
                    })?;
                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing SledDaemonPool for port {}", port);
                    println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                    return Ok(Self { pool: existing_pool.clone() });
                }
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                return Ok(Self { pool: new_pool });
            }
        }

        // Check for existing daemon
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let daemon_metadata_opt = daemon_registry.get_daemon_metadata(port).await.ok().flatten();
        let pool = if let Some(daemon_metadata) = daemon_metadata_opt {
            info!("Found existing daemon on port {} with PID {}", port, daemon_metadata.pid);
            println!("===> FOUND EXISTING DAEMON ON PORT {} WITH PID {}", port, daemon_metadata.pid);

            if is_storage_daemon_running(port).await && check_pid_validity(daemon_metadata.pid).await {
                info!("Reusing existing daemon on port {}", port);
                println!("===> REUSING EXISTING DAEMON ON PORT {} WITH PID {}", port, daemon_metadata.pid);

                let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| {
                        error!("Failed to acquire pool map lock for port {}", port);
                        println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                        GraphError::StorageError("Failed to acquire pool map lock".to_string())
                    })?;

                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing SledDaemonPool for port {}", port);
                    println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                    return Ok(Self { pool: existing_pool.clone() });
                }

                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());

                let db_path_clone = db_path.clone();
                let sled_db_instance = SLED_DB.get_or_try_init(|| async {
                    info!("Storing provided Sled database in singleton at {:?}", db_path_clone);
                    println!("===> STORING PROVIDED SLED DB IN SINGLETON AT {:?}", db_path_clone);
                    Self::check_and_cleanup_stale_daemon(port, &db_path_clone).await?;
                    let vertices = existing_db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
                    let edges = existing_db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
                    let kv_count = existing_db.iter().count();
                    info!("Opened database at {:?} with {} kv_pairs, {} vertices, {} edges", db_path_clone, kv_count, vertices.iter().count(), edges.iter().count());
                    println!("===> OPENED DATABASE AT {:?} WITH {} KV_PAIRS, {} VERTICES, {} EDGES", db_path_clone, kv_count, vertices.iter().count(), edges.iter().count());
                    Ok::<TokioMutex<SledDbWithPath>, GraphError>(TokioMutex::new(SledDbWithPath {
                        db: existing_db.clone(),
                        path: db_path_clone.clone(),
                        client: None,
                    }))
                }).await?;

                {
                    let mut pool_guard = timeout(Duration::from_secs(10), new_pool.lock())
                        .await
                        .map_err(|_| {
                            error!("Failed to acquire pool lock for initialization on port {}", port);
                            println!("===> ERROR: FAILED TO ACQUIRE POOL LOCK FOR INITIALIZATION ON PORT {}", port);
                            GraphError::StorageError("Failed to acquire pool lock for initialization".to_string())
                        })?;
                    info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
                    println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
                    let sled_db_guard = sled_db_instance.lock().await;
                    timeout(Duration::from_secs(10), pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), sled_db_guard.db.clone()))
                        .await
                        .map_err(|_| {
                            error!("Timeout initializing SledDaemonPool for port {}", port);
                            println!("===> ERROR: TIMEOUT INITIALIZING SLED DAEMON POOL FOR PORT {}", port);
                            GraphError::StorageError("Timeout initializing SledDaemonPool".to_string())
                        })??;
                    info!("Initialized cluster on port {} with existing DB", port);
                    println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
                }

                let (client, socket) = SledClient::new_with_port(port).await
                    .map_err(|e| {
                        error!("Failed to initialize ZMQ client for port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO INITIALIZE ZMQ CLIENT FOR PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to initialize ZMQ client for port {}: {}", port, e))
                    })?;

                let mut sled_db_guard = sled_db_instance.lock().await;
                sled_db_guard.client = Some((client, socket));

                let daemon_metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                    data_dir: Some(db_path.clone()),
                    config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                    engine_type: Some("sled".to_string()),
                    last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
                    pid: std::process::id(),
                    port,
                };
                daemon_registry.register_daemon(daemon_metadata).await
                    .map_err(|e| {
                        error!("Failed to register daemon for port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
                    })?;
                info!("Registered daemon for port {}", port);
                println!("===> REGISTERED DAEMON FOR PORT {}", port);

                return Ok(Self { pool: new_pool });
            } else {
                warn!("Stale daemon found on port {}. Cleaning up...", port);
                println!("===> STALE DAEMON FOUND ON PORT {}. CLEANING UP", port);
                daemon_registry.remove_daemon_by_type("sled", port).await
                    .map_err(|e| {
                        error!("Failed to remove daemon registry entry for port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO REMOVE DAEMON REGISTRY ENTRY FOR PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to remove daemon registry entry: {}", e))
                    })?;
            }
            Arc::new(TokioMutex::new(SledDaemonPool::new()))
        } else {
            info!("No existing daemon found for port {}. Creating new pool...", port);
            println!("===> NO EXISTING DAEMON FOUND FOR PORT {}. CREATING NEW POOL", port);

            Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

            Self::ensure_single_instance(&db_path).await
                .map_err(|e| {
                    error!("Cannot initialize Sled at {:?}: {}", db_path, e);
                    println!("===> ERROR: CANNOT INITIALIZE SLED AT {:?}: {}", db_path, e);
                    e
                })?;

            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                .await
                .map_err(|_| {
                    error!("Failed to acquire pool map lock for port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool map lock".to_string())
                })?;
            let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
            pool_map_guard.insert(port, new_pool.clone());
            new_pool
        };

        let sled_db_instance = SLED_DB.get_or_try_init(|| async {
            info!("Storing provided Sled database in singleton at {:?}", db_path);
            println!("===> STORING PROVIDED SLED DB IN SINGLETON AT {:?}", db_path);
            Self::check_and_cleanup_stale_daemon(port, &db_path).await?;
            let vertices = existing_db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edges = existing_db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
            let kv_count = existing_db.iter().count();
            info!("Opened database at {:?} with {} kv_pairs, {} vertices, {} edges", db_path, kv_count, vertices.iter().count(), edges.iter().count());
            println!("===> OPENED DATABASE AT {:?} WITH {} KV_PAIRS, {} VERTICES, {} EDGES", db_path, kv_count, vertices.iter().count(), edges.iter().count());
            Ok::<TokioMutex<SledDbWithPath>, GraphError>(TokioMutex::new(SledDbWithPath {
                db: existing_db.clone(),
                path: db_path.clone(),
                client: None,
            }))
        }).await?;

        {
            let mut pool_guard = timeout(Duration::from_secs(10), pool.lock())
                .await
                .map_err(|_| {
                    error!("Failed to acquire pool lock for initialization on port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL LOCK FOR INITIALIZATION ON PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool lock for initialization".to_string())
                })?;
            info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            let sled_db_guard = sled_db_instance.lock().await;
            timeout(Duration::from_secs(10), pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), sled_db_guard.db.clone()))
                .await
                .map_err(|_| {
                    error!("Timeout initializing SledDaemonPool for port {}", port);
                    println!("===> ERROR: TIMEOUT INITIALIZING SLED DAEMON POOL FOR PORT {}", port);
                    GraphError::StorageError("Timeout initializing SledDaemonPool".to_string())
                })??;
            info!("Initialized cluster on port {} with existing DB", port);
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        let (client, socket) = SledClient::new_with_port(port).await
            .map_err(|e| {
                error!("Failed to initialize ZMQ client for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO INITIALIZE ZMQ CLIENT FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to initialize ZMQ client for port {}: {}", port, e))
            })?;

        let mut sled_db_guard = sled_db_instance.lock().await;
        sled_db_guard.client = Some((client, socket));

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            pid: std::process::id(),
            port,
        };
        daemon_registry.register_daemon(daemon_metadata).await
            .map_err(|e| {
                error!("Failed to register daemon for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
            })?;
        info!("Registered daemon for port {}", port);
        println!("===> REGISTERED DAEMON FOR PORT {}", port);

        info!("Successfully initialized SledStorage with existing DB in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED SledStorage WITH EXISTING DB IN {}ms", start_time.elapsed().as_millis());
        Ok(Self { pool })
    }

    pub fn new_pinned(config: &SledConfig, storage_config: &StorageConfig) -> Box<dyn futures::Future<Output = Result<Self, GraphError>> + Send + 'static> {
        let config = config.clone();
        let storage_config = storage_config.clone();
        Box::new(async move {
            SledStorage::new(&config, &storage_config).await
        })
    }

    pub async fn set_key(&self, key: &str, value: &str) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        debug!("Setting key '{}' to value '{}' in Sled database at {:?}", key, value, db_lock.path);
        println!("===> SETTING KEY {} IN SLED DATABASE AT {:?}", key, db_lock.path);
        db_lock.db
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| {
                error!("Failed to set key '{}': {}", key, e);
                println!("===> ERROR: FAILED TO SET KEY {}", key);
                GraphError::StorageError(format!("Failed to set key '{}': {}", key, e))
            })?;
        let bytes_flushed = db_lock.db.flush_async().await.map_err(|e| {
            error!("Failed to flush Sled database after setting key '{}': {}", key, e);
            println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER SETTING KEY {}", key);
            GraphError::StorageError(format!("Failed to flush Sled database after setting key '{}': {}", key, e))
        })?;
        info!("Flushed {} bytes after setting key '{}'", bytes_flushed, key);
        println!("===> FLUSHED {} BYTES AFTER SETTING KEY {}", bytes_flushed, key);
        // Verify persistence
        let value_opt = db_lock.db.get(key.as_bytes()).map_err(|e| {
            error!("Failed to verify key '{}': {}", key, e);
            println!("===> ERROR: FAILED TO VERIFY KEY {}", key);
            GraphError::StorageError(format!("Failed to verify key '{}': {}", key, e))
        })?;
        if value_opt.is_none() || value_opt.unwrap().as_ref() != value.as_bytes() {
            error!("Persistence verification failed for key '{}'", key);
            println!("===> ERROR: PERSISTENCE VERIFICATION FAILED FOR KEY {}", key);
            return Err(GraphError::StorageError(format!("Persistence verification failed for key '{}'", key)));
        }
        debug!("Successfully set and verified key '{}'", key);
        println!("===> SUCCESSFULLY SET AND VERIFIED KEY {}", key);
        Ok(())
    }

    pub async fn get_key(&self, key: &str) -> GraphResult<Option<String>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        debug!("Retrieving key '{}' from Sled database at {:?}", key, db_lock.path);
        println!("===> RETRIEVING KEY {} FROM SLED DATABASE AT {:?}", key, db_lock.path);
        let value = db_lock.db
            .get(key.as_bytes())
            .map_err(|e| {
                error!("Failed to get key '{}': {}", key, e);
                println!("===> ERROR: FAILED TO GET KEY {}", key);
                GraphError::StorageError(format!("Failed to get key '{}': {}", key, e))
            })?
            .map(|v| String::from_utf8_lossy(&*v).to_string());
        debug!("Retrieved value for key '{}': {:?}", key, value);
        println!("===> RETRIEVED VALUE FOR KEY {}: {:?}", key, value);
        Ok(value)
    }

    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        let lock_path = path.join("db.lck");
        info!("Checking for lock file at {:?}", lock_path);
        println!("===> CHECKING FOR LOCK FILE AT {:?}", lock_path);

        if lock_path.exists() {
            warn!("Found lock file at {:?}", lock_path);
            println!("===> FOUND LOCK FILE AT {:?}", lock_path);
            const MAX_RETRIES: u32 = 5;
            const BASE_DELAY_MS: u64 = 1000;
            let mut attempt = 0;

            while attempt < MAX_RETRIES {
                match timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                    Ok(Ok(_)) => {
                        info!("Successfully removed lock file at {:?}", lock_path);
                        println!("===> SUCCESSFULLY REMOVED LOCK FILE AT {:?}", lock_path);
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        break;
                    }
                    Ok(Err(e)) => {
                        warn!("Failed to remove lock file at {:?} on attempt {}: {:?}", lock_path, attempt + 1, e);
                        println!("===> ERROR: FAILED TO REMOVE LOCK FILE AT {:?}", lock_path);
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                    }
                    Err(_) => {
                        warn!("Timeout removing lock file at {:?} on attempt {}", lock_path, attempt + 1);
                        println!("===> ERROR: TIMEOUT REMOVING LOCK FILE AT {:?}", lock_path);
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                    }
                }
            }

            if attempt >= MAX_RETRIES {
                error!("Failed to unlock {:?} after {} attempts", lock_path, MAX_RETRIES);
                println!("===> ERROR: FAILED TO UNLOCK SLED DATABASE AT {:?}", lock_path);
                return Err(GraphError::StorageError(format!("Failed to unlock {:?} after {} attempts", lock_path, MAX_RETRIES)));
            }
        } else {
            info!("No lock file found at {:?}", lock_path);
            println!("===> NO LOCK FILE FOUND AT {:?}", lock_path);
        }

        // Attempt to open and close the database to clear internal locks and verify state
        match sled::Config::new().path(path).open() {
            Ok(db) => {
                let vertices = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
                let edges = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
                let kv_count = db.iter().count();
                info!("Verified database at {:?} with {} kv_pairs, {} vertices, {} edges", path, kv_count, vertices.iter().count(), edges.iter().count());
                println!("===> VERIFIED DATABASE AT {:?} WITH {} KV_PAIRS, {} VERTICES, {} EDGES", path, kv_count, vertices.iter().count(), edges.iter().count());
                drop(db);
                info!("Successfully cleared internal Sled locks at {:?}", path);
                println!("===> SUCCESSFULLY CLEARED INTERNAL SLED LOCKS AT {:?}", path);
            }
            Err(e) => {
                warn!("Could not open database for lock cleanup at {:?}: {}", path, e);
                println!("===> WARNING: COULD NOT OPEN DATABASE FOR LOCK CLEANUP AT {:?}", path);
                // Proceed without failing, as the lock file is already removed
            }
        }

        Ok(())
    }

    pub async fn force_reset(config: &SledConfig, storage_config: &StorageConfig) -> GraphResult<Self> {
        warn!("FORCE RESET: Completely destroying and recreating database at {:?}", config.path);
        println!("===> FORCE RESET: DESTROYING DATABASE AT {:?}", config.path);

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        // Clean up stale daemon
        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        // Remove the database directory
        if db_path.exists() {
            info!("Destroying existing Sled database at {:?}", db_path);
            println!("===> DESTROYING EXISTING SLED DATABASE AT {:?}", db_path);
            timeout(Duration::from_secs(5), fs::remove_dir_all(&db_path))
                .await
                .map_err(|_| {
                    error!("Timeout removing Sled directory at {:?}", db_path);
                    println!("===> ERROR: TIMEOUT REMOVING SLED DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Timeout removing Sled directory at {:?}", db_path))
                })?
                .map_err(|e| {
                    error!("Failed to remove Sled directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO REMOVE SLED DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to remove Sled directory at {:?}: {}", db_path, e))
                })?;
            info!("Successfully removed Sled database directory at {:?}", db_path);
            println!("===> SUCCESSFULLY REMOVED SLED DATABASE DIRECTORY AT {:?}", db_path);
        }

        // Recreate the directory
        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to recreate Sled database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO RECREATE SLED DATABASE DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to recreate Sled database directory at {:?}: {}", db_path, e))
            })?;

        // Initialize a new SledStorage instance
        Self::new(config, storage_config).await
            .map_err(|e| {
                error!("Failed to initialize SledStorage after reset: {}", e);
                println!("===> ERROR: FAILED TO INITIALIZE SLED STORAGE AFTER RESET");
                GraphError::StorageError(format!("Failed to initialize SledStorage after reset: {}", e))
            })
    }

    pub async fn diagnose_persistence(&self) -> GraphResult<serde_json::Value> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Diagnosing persistence for SledStorage at {:?}", db_path);
        println!("===> DIAGNOSING PERSISTENCE FOR SLED STORAGE AT {:?}", db_path);

        let kv_count = db_lock.db.iter().count();
        let vertex_count = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?.iter().count();
        let edge_count = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?.iter().count();

        let disk_usage = fs::metadata(db_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let diagnostics = serde_json::json!({
            "path": db_path.to_string_lossy(),
            "kv_pairs_count": kv_count,
            "vertices_count": vertex_count,
            "edges_count": edge_count,
            "disk_usage_bytes": disk_usage,
            "is_running": self.is_running().await,
        });

        info!("Persistence diagnostics: {:?}", diagnostics);
        println!("===> PERSISTENCE DIAGNOSTICS: {:?}", diagnostics);
        Ok(diagnostics)
    }
}

impl Drop for SledStorage {
    fn drop(&mut self) {
        if let Ok(pool) = self.pool.try_lock() {
            for daemon in pool.daemons.values() {
                if let Err(e) = daemon.db.flush() {
                    eprintln!("Failed to flush sled daemon at {:?}: {}", daemon.db_path, e);
                }
            }
        } else {
            eprintln!("Failed to acquire lock on SledDaemonPool during drop");
        }
    }
}

#[async_trait]
impl StorageEngine for SledStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to SledStorage");
        println!("===> CONNECTING TO SLED STORAGE");
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::insert - inserting key {:?} into kv_pairs at {:?}", key, db_path);
        debug!("Key (utf8 if possible): {:?}", String::from_utf8_lossy(&key));
        println!("===> INSERTING KEY INTO SLED DATABASE AT {:?}", db_path);

        db_lock.db
            .insert(&key, &*value)
            .map_err(|e| {
                error!("Failed to insert key: {}", e);
                println!("===> ERROR: FAILED TO INSERT KEY INTO SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after insert: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER INSERT");
                GraphError::StorageError(e.to_string())
            })?;
        info!("SledStorage::insert - flushed {} bytes after insert at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER INSERT AT {:?}", bytes_flushed, db_path);

        let keys: Vec<_> = db_lock.db
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("SledStorage::insert - current kv_pairs keys at {:?}: {:?}", db_path, keys);
        println!("===> CURRENT KV_PAIRS KEYS AT {:?}: {:?}", db_path, keys);

        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::retrieve - retrieving key {:?} from kv_pairs at {:?}", key, db_path);
        debug!("Key (utf8 if possible): {:?}", String::from_utf8_lossy(&key));
        println!("===> RETRIEVING KEY FROM SLED DATABASE AT {:?}", db_path);

        let value_opt = db_lock.db
            .get(key)
            .map_err(|e| {
                error!("Failed to retrieve key: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE KEY FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let keys: Vec<_> = db_lock.db
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("SledStorage::retrieve - current kv_pairs keys at {:?}: {:?}", db_path, keys);
        println!("===> CURRENT KV_PAIRS KEYS AT {:?}: {:?}", db_path, keys);

        Ok(value_opt.map(|v| v.to_vec()))
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::delete - deleting key {:?} from kv_pairs at {:?}", key, db_path);
        println!("===> DELETING KEY FROM SLED DATABASE AT {:?}", db_path);

        db_lock.db
            .remove(key)
            .map_err(|e| {
                error!("Failed to delete key: {}", e);
                println!("===> ERROR: FAILED TO DELETE KEY FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after delete: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER DELETE");
                GraphError::StorageError(e.to_string())
            })?;
        info!("SledStorage::delete - flushed {} bytes after delete at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER DELETE AT {:?}", bytes_flushed, db_path);

        let keys: Vec<_> = db_lock.db
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("SledStorage::delete - current kv_pairs keys at {:?}: {:?}", db_path, keys);
        println!("===> CURRENT KV_PAIRS KEYS AT {:?}: {:?}", db_path, keys);

        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::flush - flushing database at {:?}", db_path);
        println!("===> FLUSHING SLED DATABASE AT {:?}", db_path);

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush Sled database: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;
        info!("SledStorage::flush - flushed {} bytes to disk at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES TO DISK AT {:?}", bytes_flushed, db_path);

        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SledStorage {
    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Creating vertex at path {:?}", db_path);
        println!("===> CREATING VERTEX IN SLED DATABASE AT {:?}", db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.insert(vertex.id.0.as_bytes(), serialize_vertex(&vertex)?)
            .map_err(|e| {
                error!("Failed to create vertex: {}", e);
                println!("===> ERROR: FAILED TO CREATE VERTEX IN SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after creating vertex: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER CREATING VERTEX");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after creating vertex at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER CREATING VERTEX AT {:?}", bytes_flushed, db_path);

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        println!("===> CURRENT VERTEX KEYS AT {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Retrieving vertex with id {} from path {:?}", id, db_path);
        println!("===> RETRIEVING VERTEX WITH ID {} FROM SLED DATABASE AT {:?}", id, db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let vertex = vertices
            .get(SerializableUuid(*id).0.as_bytes())
            .map_err(|e| {
                error!("Failed to retrieve vertex: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE VERTEX FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?
            .map(|v| deserialize_vertex(&*v))
            .transpose()?;

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        println!("===> CURRENT VERTEX KEYS AT {:?}: {:?}", db_path, vertex_keys);
        Ok(vertex)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Updating vertex at path {:?}", db_path);
        println!("===> UPDATING VERTEX IN SLED DATABASE AT {:?}", db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.insert(vertex.id.0.as_bytes(), serialize_vertex(&vertex)?)
            .map_err(|e| {
                error!("Failed to update vertex: {}", e);
                println!("===> ERROR: FAILED TO UPDATE VERTEX IN SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after updating vertex: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER UPDATING VERTEX");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after updating vertex at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER UPDATING VERTEX AT {:?}", bytes_flushed, db_path);

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        println!("===> CURRENT VERTEX KEYS AT {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Deleting vertex with id {} from path {:?}", id, db_path);
        println!("===> DELETING VERTEX WITH ID {} FROM SLED DATABASE AT {:?}", id, db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.remove(SerializableUuid(*id).0.as_bytes())
            .map_err(|e| {
                error!("Failed to delete vertex: {}", e);
                println!("===> ERROR: FAILED TO DELETE VERTEX FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after deleting vertex: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER DELETING VERTEX");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after deleting vertex at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER DELETING VERTEX AT {:?}", bytes_flushed, db_path);

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        println!("===> CURRENT VERTEX KEYS AT {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }
    
    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Creating edge at path {:?}", db_path);
        println!("===> CREATING EDGE IN SLED DATABASE AT {:?}", db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(edge.outbound_id.0), &edge.t, &SerializableUuid(edge.inbound_id.0))?;
        edges.insert(&edge_key, serialize_edge(&edge)?)
            .map_err(|e| {
                error!("Failed to create edge: {}", e);
                println!("===> ERROR: FAILED TO CREATE EDGE IN SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after creating edge: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER CREATING EDGE");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after creating edge at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER CREATING EDGE AT {:?}", bytes_flushed, db_path);

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        println!("===> CURRENT EDGE KEYS AT {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Retrieving edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);
        println!("===> RETRIEVING EDGE ({}, {}, {}) FROM SLED DATABASE AT {:?}", outbound_id, edge_type, inbound_id, db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        let edge = edges
            .get(&edge_key)
            .map_err(|e| {
                error!("Failed to retrieve edge: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE EDGE FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?
            .map(|v| deserialize_edge(&*v))
            .transpose()?;

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        println!("===> CURRENT EDGE KEYS AT {:?}: {:?}", db_path, edge_keys);
        Ok(edge)
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Updating edge at path {:?}", db_path);
        println!("===> UPDATING EDGE IN SLED DATABASE AT {:?}", db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(edge.outbound_id.0), &edge.t, &SerializableUuid(edge.inbound_id.0))?;
        edges.insert(&edge_key, serialize_edge(&edge)?)
            .map_err(|e| {
                error!("Failed to update edge: {}", e);
                println!("===> ERROR: FAILED TO UPDATE EDGE IN SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after updating edge: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER UPDATING EDGE");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after updating edge at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER UPDATING EDGE AT {:?}", bytes_flushed, db_path);

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        println!("===> CURRENT EDGE KEYS AT {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Deleting edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);
        println!("===> DELETING EDGE ({}, {}, {}) FROM SLED DATABASE AT {:?}", outbound_id, edge_type, inbound_id, db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        edges.remove(&edge_key)
            .map_err(|e| {
                error!("Failed to delete edge: {}", e);
                println!("===> ERROR: FAILED TO DELETE EDGE FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after deleting edge: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER DELETING EDGE");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after deleting edge at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER DELETING EDGE AT {:?}", bytes_flushed, db_path);

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        println!("===> CURRENT EDGE KEYS AT {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn close(&self) -> GraphResult<()> {
        let pool = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled pool lock".to_string()))?;
        info!("Closing SledStorage pool");
        println!("===> CLOSING SLED STORAGE POOL");

        match timeout(Duration::from_secs(10), pool.close(None)).await {
            Ok(Ok(_)) => {
                info!("Successfully closed SledStorage pool");
                println!("===> SUCCESSFULLY CLOSED SLED STORAGE POOL");
                Ok(())
            }
            Ok(Err(e)) => {
                error!("Failed to close SledStorage pool: {}", e);
                println!("===> ERROR: FAILED TO CLOSE SLED STORAGE POOL: {}", e);
                Err(e)
            }
            Err(_) => {
                error!("Timeout closing SledStorage pool");
                println!("===> ERROR: TIMEOUT CLOSING SLED STORAGE POOL");
                Err(GraphError::StorageError("Timeout closing SledStorage pool".to_string()))
            }
        }
    }

    async fn start(&self) -> Result<(), GraphError> {
        info!("Starting SledStorage");
        println!("===> STARTING SLED STORAGE");
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        info!("Stopping SledStorage");
        println!("===> STOPPING SLED STORAGE");
        self.close().await
    }

    fn get_type(&self) -> &'static str {
        "sled"
    }

    async fn is_running(&self) -> bool {
        let pool = match timeout(Duration::from_secs(5), self.pool.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                error!("Timeout acquiring Sled pool lock for is_running check");
                println!("===> ERROR: TIMEOUT ACQUIRING SLED POOL LOCK FOR IS_RUNNING CHECK");
                return false;
            }
        };
        let daemon_count = pool.daemons.len();
        info!("Checking running status for {} daemons", daemon_count);
        println!("===> CHECKING RUNNING STATUS FOR {} DAEMONS", daemon_count);
        let futures = pool.daemons.values().map(|daemon| async {
            timeout(Duration::from_secs(2), daemon.is_running()).await
                .map_err(|_| {
                    println!("===> TIMEOUT CHECKING DAEMON STATUS");
                    false
                })
                .unwrap_or(false)
        });
        let results = join_all(futures).await;
        let is_running = results.iter().any(|&r| r);
        info!("SledStorage running status: {}, daemon states: {:?}", is_running, results);
        println!("===> SLED STORAGE RUNNING STATUS: {}, DAEMON STATES: {:?}", is_running, results);
        is_running
    }

    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON SLED STORAGE (NOT IMPLEMENTED)");
        Ok(Value::Null)
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON SLED STORAGE (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Retrieving all vertices from path {:?}", db_path);
        println!("===> RETRIEVING ALL VERTICES FROM SLED DATABASE AT {:?}", db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let mut vertex_vec = Vec::new();
        for result in vertices.iter() {
            let (_k, v) = result.map_err(|e| {
                error!("Failed to retrieve vertices: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE VERTICES FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;
            vertex_vec.push(deserialize_vertex(&*v)?);
        }
        info!("Retrieved {} vertices from path {:?}", vertex_vec.len(), db_path);
        println!("===> RETRIEVED {} VERTICES FROM SLED DATABASE AT {:?}", vertex_vec.len(), db_path);
        Ok(vertex_vec)
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Retrieving all edges from path {:?}", db_path);
        println!("===> RETRIEVING ALL EDGES FROM SLED DATABASE AT {:?}", db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let mut edge_vec = Vec::new();
        for result in edges.iter() {
            let (_k, v) = result.map_err(|e| {
                error!("Failed to retrieve edges: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE EDGES FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;
            edge_vec.push(deserialize_edge(&*v)?);
        }
        info!("Retrieved {} edges from path {:?}", edge_vec.len(), db_path);
        println!("===> RETRIEVED {} EDGES FROM SLED DATABASE AT {:?}", edge_vec.len(), db_path);
        Ok(edge_vec)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(Duration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Clearing all data from path {:?}", db_path);
        println!("===> CLEARING ALL DATA FROM SLED DATABASE AT {:?}", db_path);

        db_lock.db.clear().map_err(|e| {
            error!("Failed to clear Sled database: {}", e);
            println!("===> ERROR: FAILED TO CLEAR SLED DATABASE");
            GraphError::StorageError(e.to_string())
        })?;
        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after clearing data: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER CLEARING DATA");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after clearing data at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER CLEARING DATA AT {:?}", bytes_flushed, db_path);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub async fn select_available_port(storage_config: &StorageConfig, preferred_port: u16) -> GraphResult<u16> {
    let cluster_ports = parse_cluster_range(&storage_config.cluster_range)?;

    if !is_storage_daemon_running(preferred_port).await {
        debug!("Preferred port {} is available.", preferred_port);
        println!("===> PREFERRED PORT {} IS AVAILABLE", preferred_port);
        return Ok(preferred_port);
    }

    for port in cluster_ports {
        if port == preferred_port {
            continue;
        }
        if !is_storage_daemon_running(port).await {
            debug!("Selected available port {} from cluster range", port);
            println!("===> SELECTED AVAILABLE PORT {} FROM CLUSTER RANGE", port);
            return Ok(port);
        }
    }

    error!("No available ports in cluster range {:?}", storage_config.cluster_range);
    println!("===> ERROR: NO AVAILABLE PORTS IN CLUSTER RANGE {:?}", storage_config.cluster_range);
    Err(GraphError::StorageError(format!(
        "No available ports in cluster range {:?}", storage_config.cluster_range
    )))
}
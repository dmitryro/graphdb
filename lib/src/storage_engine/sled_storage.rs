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
use crate::daemon::db_daemon_registry::{ GLOBAL_DB_DAEMON_REGISTRY, DBDaemonMetadata };
use crate::daemon::daemon_utils::{is_storage_daemon_running, parse_cluster_range};
use crate::daemon::daemon_management::{ check_pid_validity, find_pid_by_port, stop_process_by_pid, get_ipc_endpoint, extract_port };
use serde_json::Value;
use std::any::Any;
use futures::future::join_all;
use tokio::time::{timeout, sleep, Duration as TokioDuration};
use crate::storage_engine::sled_client::{ SledClient, ZmqSocketWrapper };
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use sled::Config;

pub static SLED_DB: LazyLock<OnceCell<TokioMutex<SledDbWithPath>>> = LazyLock::new(|| OnceCell::new());

//pub static SLED_DB: LazyLock<OnceCell<TokioMutex<SledDbWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static SLED_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<SledDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());

// Static variable to track active Sled database instances
pub static SLED_ACTIVE_DATABASES: LazyLock<OnceCell<TokioMutex<HashSet<PathBuf>>>> = LazyLock::new(|| OnceCell::new());
// Global ZMQ Connection Pool Map, keyed by "host:port".
static ZMQ_CLIENT_POOL_MAP: OnceCell<TokioMutex<HashMap<String, Arc<TokioMutex<SledDaemonPool>>>>> = OnceCell::const_new();

impl SledStorage {
    /// Ensures that only one instance of the database at the given path is active.
    pub async fn ensure_single_instance(db_path: &PathBuf) -> Result<(), GraphError> {
        let active_dbs = SLED_ACTIVE_DATABASES.get_or_init(|| async {
            TokioMutex::new(std::collections::HashSet::new())
        }).await;
        let mut active_dbs_guard = active_dbs.lock().await;
        if active_dbs_guard.contains(db_path) {
            error!("Database at {:?} is already in use by another instance", db_path);
            println!("===> ERROR: DATABASE AT {:?} ALREADY IN USE", db_path);
            return Err(GraphError::StorageError(format!("Database at {:?} is already in use", db_path)));
        }
        active_dbs_guard.insert(db_path.clone());
        Ok(())
    }

    /// Releases the database instance from the active databases map.
    pub async fn release_instance(db_path: &PathBuf) {
        let active_dbs = SLED_ACTIVE_DATABASES.get_or_init(|| async {
            TokioMutex::new(std::collections::HashSet::new())
        }).await;
        let mut active_dbs_guard = active_dbs.lock().await;
        active_dbs_guard.remove(db_path);
    }

    /// Checks and cleans up stale daemon for the given port and path.
    pub async fn check_and_cleanup_stale_daemon(port: u16, db_path: &PathBuf) -> Result<(), GraphError> {
        info!("Checking for stale Sled daemon on port {} with db_path {:?}", port, db_path);
        println!("===> CHECKING FOR STALE SLED DAEMON ON PORT {} WITH DB_PATH {:?}", port, db_path);

        // 1. Check and clean up stale IPC socket file
        let ipc_path = get_ipc_endpoint(port);
        let socket_path = &ipc_path[6..]; // Remove "ipc://" prefix
        if std::path::Path::new(socket_path).exists() { 
            warn!("Stale IPC socket found at {}. Attempting cleanup.", ipc_path);
            println!("===> WARNING: STALE IPC SOCKET FOUND AT {}. ATTEMPTING CLEANUP.", ipc_path);
            if let Err(e) = tokio::fs::remove_file(socket_path).await {
                warn!("Failed to remove stale IPC socket at {}: {}", ipc_path, e);
                println!("===> WARNING: FAILED TO REMOVE STALE IPC SOCKET AT {}: {}", ipc_path, e);
            } else {
                info!("Successfully removed stale IPC socket at {}", ipc_path);
                println!("===> SUCCESSFULLY REMOVED STALE IPC SOCKET AT {}", ipc_path);
            }
        }

        // 2. Check the daemon registry for a registered PID and verify if the process is running
        let daemon_registry = GLOBAL_DB_DAEMON_REGISTRY.get().await;
        if let Some(meta) = daemon_registry.get_daemon_metadata(port).await? {
            // Use the updated sysinfo API methods
            let mut system = System::new_with_specifics(
                RefreshKind::nothing() // Start with no refresh settings
                    .with_processes(ProcessRefreshKind::everything()) // Specify refreshing all process info
            );

            // Explicitly refresh only the metadata for the specific PID
            system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[Pid::from(meta.pid as usize)]), // Which processes to refresh
                false, // Do not clear existing processes before refresh
                ProcessRefreshKind::everything() // What to refresh
            );

            // Check if the process still exists in the refreshed system state
            let is_running = system.process(Pid::from(meta.pid as usize)).is_some();

            if !is_running {
                // Process is dead, clean up the registry entry
                info!("Found stale daemon on port {}. Cleaning up.", port);
                println!("===> FOUND STALE DAEMON ON PORT {}. CLEANING UP.", port);
                daemon_registry.unregister_daemon(port).await?;
            }
        }

        Ok(())
    }

    pub async fn new(config: &SledConfig, storage_config: &StorageConfig) -> Result<SledStorage, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with config at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH PORT {:?}", config.port);

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let ipc_endpoint = get_ipc_endpoint(port);

        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());
        info!("Using Sled path (for identification only) {:?}", db_path);
        println!("===> USING SLED PATH (FOR IDENTIFICATION ONLY) {:?}", db_path);
        info!("Calculated ZMQ IPC endpoint: {}", ipc_endpoint);
        println!("===> CALCULATED ZMQ IPC ENDPOINT: {}", ipc_endpoint);

        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            error!("Compression is enabled in config but Sled was compiled without compression feature");
            println!("===> ERROR: COMPRESSION ENABLED BUT SLED COMPILED WITHOUT COMPRESSION FEATURE");
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build.".to_string()
            ));
        }

        // 1. Check both registries for existing daemon
        let db_daemon_registry = GLOBAL_DB_DAEMON_REGISTRY.get().await;
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let mut existing_meta = None;

        if let Some(meta) = db_daemon_registry.get_daemon_metadata(port).await? {
            if meta.data_dir == Some(db_path.clone()) && meta.engine_type == Some("sled".to_string()) {
                existing_meta = Some(meta.clone());
            }
        }

        if existing_meta.is_none() {
            if let Some(meta) = daemon_registry.get_daemon_metadata(port).await? {
                if meta.data_dir == Some(db_path.clone()) && meta.engine_type == Some("sled".to_string()) {
                    existing_meta = Some(DBDaemonMetadata {
                        ip_address: meta.ip_address,
                        data_dir: meta.data_dir,
                        config_path: meta.config_path,
                        engine_type: meta.engine_type,
                        last_seen_nanos: meta.last_seen_nanos,
                        pid: meta.pid,
                        port: meta.port,
                        sled_db_instance: None,
                        rocksdb_db_instance: None,
                        sled_vertices: None,
                        sled_edges: None,
                        sled_kv_pairs: None,
                        rocksdb_vertices: None,
                        rocksdb_edges: None,
                        rocksdb_kv_pairs: None,
                    });
                }
            }
        }

        if let Some(_meta) = existing_meta {
            info!("Found existing Sled daemon on port {} with path {:?}", port, db_path);
            println!("===> FOUND EXISTING SLED DAEMON ON PORT {} WITH PATH {:?}", port, db_path);
            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await
                .map_err(|_| {
                    error!("Timeout acquiring pool map lock for port {}", port);
                    println!("===> ERROR: TIMEOUT ACQUIRING POOL MAP LOCK FOR PORT {}", port);
                    GraphError::StorageError("Timeout acquiring pool map lock".to_string())
                })?;
            if let Some(existing_pool) = pool_map_guard.get(&port) {
                info!("Reusing existing SledDaemonPool for port {}", port);
                println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                return Ok(SledStorage { pool: existing_pool.clone() });
            }
        }

        // 2. Clean up stale daemon
        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        // 3. Acquire or create the SledDaemonPool instance
        let pool = {
            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await
                .map_err(|_| {
                    error!("Failed to acquire pool map lock for port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool map lock".to_string())
                })?;
            if let Some(existing_pool) = pool_map_guard.get(&port) {
                info!("Reusing existing SledDaemonPool for port {}", port);
                println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                existing_pool.clone()
            } else {
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                info!("Created new SledDaemonPool for port {}", port);
                println!("===> CREATED NEW SLED DAEMON POOL FOR PORT {}", port);
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        // 4. Initialize SledDaemonPool Cluster
        {
            let mut pool_guard = timeout(TokioDuration::from_secs(10), pool.lock()).await
                .map_err(|_| {
                    error!("Failed to acquire pool lock for initialization on port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL LOCK FOR INITIALIZATION ON PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool lock for initialization".to_string())
                })?;
            info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            pool_guard.initialize_cluster(storage_config, config, Some(port)).await?;
            info!("Initialized cluster on port {}", port);
            println!("===> INITIALIZED CLUSTER ON PORT {}", port);
        }

        // 5. Initialize SLED_DB singleton with a temporary database
        let sled_db_instance = SLED_DB.get_or_try_init(|| async {
            info!("Initializing SLED_DB singleton for ZMQ client at port {}", port);
            println!("===> INITIALIZING SLED_DB SINGLETON FOR ZMQ CLIENT AT PORT {}", port);
            let temp_db = sled::Config::new()
                .temporary(true)
                .open()
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            Ok::<TokioMutex<SledDbWithPath>, GraphError>(TokioMutex::new(SledDbWithPath {
                db: Arc::new(temp_db),
                path: db_path.clone(),
                client: None,
            }))
        }).await?;

        // 6. Initialize ZMQ client
        let client_tuple = {
            const MAX_RETRIES: usize = 20;
            const INITIAL_RETRY_DELAY_MS: u64 = 100;
            let mut client_tuple_opt: Option<(SledClient, Arc<TokioMutex<ZmqSocketWrapper>>)> = None;
            info!("Waiting for ZMQ IPC readiness at {}...", ipc_endpoint);
            println!("===> WAITING FOR ZMQ IPC READINESS AT {}...", ipc_endpoint);

            for i in 0..MAX_RETRIES {
                match SledClient::connect_zmq_client_with_readiness_check(port).await {
                    Ok(c_tuple) => {
                        info!("ZMQ client connected successfully after {} retries.", i);
                        println!("===> ZMQ CLIENT CONNECTED SUCCESSFULLY AFTER {} RETRIES", i);
                        client_tuple_opt = Some(c_tuple);
                        break;
                    }
                    Err(e) => {
                        warn!("Attempt {}/{} to connect ZMQ client failed for endpoint {}: {}", i + 1, MAX_RETRIES, ipc_endpoint, e);
                        println!("===> WARNING: ATTEMPT {}/{} TO CONNECT ZMQ CLIENT FAILED FOR ENDPOINT {}: {}", i + 1, MAX_RETRIES, ipc_endpoint, e);
                        if i < MAX_RETRIES - 1 {
                            sleep(TokioDuration::from_millis(INITIAL_RETRY_DELAY_MS * (1 << i))).await;
                        } else {
                            error!("Failed to connect ZMQ client after {} retries at {}: {}", MAX_RETRIES, ipc_endpoint, e);
                            println!("===> ERROR: FAILED TO CONNECT ZMQ CLIENT AFTER {} RETRIES AT {}: {}", MAX_RETRIES, ipc_endpoint, e);
                            return Err(GraphError::StorageError(format!("Failed to connect ZMQ client after {} retries at {}: {}", MAX_RETRIES, ipc_endpoint, e)));
                        }
                    }
                }
            }
            client_tuple_opt.ok_or_else(|| {
                error!("Failed to initialize ZMQ client after successful poll");
                println!("===> ERROR: FAILED TO INITIALIZE ZMQ CLIENT AFTER SUCCESSFUL POLL");
                GraphError::StorageError("Failed to initialize ZMQ client after successful poll".to_string())
            })?
        };

        // 7. Store client tuple in SLED_DB singleton
        let mut sled_db_guard = sled_db_instance.lock().await;
        sled_db_guard.client = Some(client_tuple);
        drop(sled_db_guard);

        // 8. Register daemon in both registries
        let daemon_metadata = DBDaemonMetadata {
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            pid: std::process::id(),
            port,
            sled_db_instance: None,
            rocksdb_db_instance: None,
            sled_vertices: None,
            sled_edges: None,
            sled_kv_pairs: None,
            rocksdb_vertices: None,
            rocksdb_edges: None,
            rocksdb_kv_pairs: None,
        };

        if let Err(e) = db_daemon_registry.register_daemon(daemon_metadata).await {
            error!("Failed to register daemon in GLOBAL_DB_DAEMON_REGISTRY for port {}: {}", port, e);
            println!("===> ERROR: FAILED TO REGISTER DAEMON IN GLOBAL_DB_DAEMON_REGISTRY FOR PORT {}: {}", port, e);
            return Err(GraphError::StorageError(format!("Failed to register daemon in GLOBAL_DB_DAEMON_REGISTRY: {}", e)));
        }

        let general_daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            zmq_ready: false,
        };

        if let Err(e) = daemon_registry.register_daemon(general_daemon_metadata).await {
            error!("Failed to register daemon in GLOBAL_DAEMON_REGISTRY for port {}: {}", port, e);
            println!("===> ERROR: FAILED TO REGISTER DAEMON IN GLOBAL_DAEMON_REGISTRY FOR PORT {}: {}", port, e);
            return Err(GraphError::StorageError(format!("Failed to register daemon in GLOBAL_DAEMON_REGISTRY: {}", e)));
        }

        info!("Registered daemon for port {} in both registries", port);
        println!("===> REGISTERED DAEMON FOR PORT {} IN BOTH REGISTRIES", port);
        info!("Successfully initialized SledStorage in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED SledStorage IN {}ms", start_time.elapsed().as_millis());

        Ok(SledStorage { pool })
    }

    pub async fn new_with_db(config: &SledConfig, storage_config: &StorageConfig, existing_db: Arc<sled::Db>) -> Result<SledStorage, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with existing database at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH EXISTING DB WITH PORT {:?}", config.port);

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let ipc_endpoint = get_ipc_endpoint(port);

        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());
        info!("Using Sled path (for identification only) {:?}", db_path);
        println!("===> USING SLED PATH (FOR IDENTIFICATION ONLY) {:?}", db_path);
        info!("Calculated ZMQ IPC endpoint: {}", ipc_endpoint);
        println!("===> CALCULATED ZMQ IPC ENDPOINT: {}", ipc_endpoint);

        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            error!("Compression is enabled in config but Sled was compiled without compression feature");
            println!("===> ERROR: COMPRESSION ENABLED BUT SLED COMPILED WITHOUT COMPRESSION FEATURE");
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build.".to_string()
            ));
        }

        // 1. Check both registries for existing daemon
        let db_daemon_registry = GLOBAL_DB_DAEMON_REGISTRY.get().await;
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let mut existing_meta = None;

        if let Some(meta) = db_daemon_registry.get_daemon_metadata(port).await? {
            if meta.data_dir == Some(db_path.clone()) && meta.engine_type == Some("sled".to_string()) {
                existing_meta = Some(meta.clone());
            }
        }

        if existing_meta.is_none() {
            if let Some(meta) = daemon_registry.get_daemon_metadata(port).await? {
                if meta.data_dir == Some(db_path.clone()) && meta.engine_type == Some("sled".to_string()) {
                    existing_meta = Some(DBDaemonMetadata {
                        ip_address: meta.ip_address,
                        data_dir: meta.data_dir,
                        config_path: meta.config_path,
                        engine_type: meta.engine_type,
                        last_seen_nanos: meta.last_seen_nanos,
                        pid: meta.pid,
                        port: meta.port,
                        sled_db_instance: None,
                        rocksdb_db_instance: None,
                        sled_vertices: None,
                        sled_edges: None,
                        sled_kv_pairs: None,
                        rocksdb_vertices: None,
                        rocksdb_edges: None,
                        rocksdb_kv_pairs: None,
                    });
                }
            }
        }

        if let Some(_meta) = existing_meta {
            info!("Found existing Sled daemon on port {} with path {:?}", port, db_path);
            println!("===> FOUND EXISTING SLED DAEMON ON PORT {} WITH PATH {:?}", port, db_path);
            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await
                .map_err(|_| {
                    error!("Timeout acquiring pool map lock for port {}", port);
                    println!("===> ERROR: TIMEOUT ACQUIRING POOL MAP LOCK FOR PORT {}", port);
                    GraphError::StorageError("Timeout acquiring pool map lock".to_string())
                })?;
            if let Some(existing_pool) = pool_map_guard.get(&port) {
                info!("Reusing existing SledDaemonPool for port {}", port);
                println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                return Ok(SledStorage { pool: existing_pool.clone() });
            }
        }

        // 2. Clean up stale daemon
        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        // 3. Acquire or create the SledDaemonPool instance
        let pool = {
            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await
                .map_err(|_| {
                    error!("Failed to acquire pool map lock for port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool map lock".to_string())
                })?;
            if let Some(existing_pool) = pool_map_guard.get(&port) {
                info!("Reusing existing SledDaemonPool for port {}", port);
                println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                existing_pool.clone()
            } else {
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                info!("Created new SledDaemonPool for port {}", port);
                println!("===> CREATED NEW SLED DAEMON POOL FOR PORT {}", port);
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        // 4. Initialize SledDaemonPool Cluster
        {
            let mut pool_guard = timeout(TokioDuration::from_secs(10), pool.lock()).await
                .map_err(|_| {
                    error!("Failed to acquire pool lock for initialization on port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL LOCK FOR INITIALIZATION ON PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool lock for initialization".to_string())
                })?;
            info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), existing_db.clone()).await?;
            info!("Initialized cluster on port {} with existing DB", port);
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        // 5. Initialize SLED_DB singleton
        let sled_db_instance = SLED_DB.get_or_try_init(|| async {
            info!("Storing provided Sled database in singleton at {:?}", db_path);
            println!("===> STORING PROVIDED SLED DB IN SINGLETON AT {:?}", db_path);
            let vertices = existing_db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edges = existing_db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
            let kv_pairs = existing_db.open_tree("kv_pairs").map_err(|e| GraphError::StorageError(e.to_string()))?;
            let kv_count = existing_db.iter().count();
            info!("Opened database at {:?} with {} kv_pairs, {} vertices, {} edges", db_path, kv_count, vertices.iter().count(), edges.iter().count());
            println!("===> OPENED DATABASE AT {:?} WITH {} KV_PAIRS, {} VERTICES, {} EDGES", db_path, kv_count, vertices.iter().count(), edges.iter().count());
            Ok::<TokioMutex<SledDbWithPath>, GraphError>(TokioMutex::new(SledDbWithPath {
                db: existing_db.clone(),
                path: db_path.clone(),
                client: None,
            }))
        }).await?;

        // 6. Initialize ZMQ client
        let client_tuple = {
            const MAX_RETRIES: usize = 20;
            const INITIAL_RETRY_DELAY_MS: u64 = 100;
            let mut client_tuple_opt: Option<(SledClient, Arc<TokioMutex<ZmqSocketWrapper>>)> = None;
            info!("Waiting for ZMQ IPC readiness at {}...", ipc_endpoint);
            println!("===> WAITING FOR ZMQ IPC READINESS AT {}...", ipc_endpoint);

            for i in 0..MAX_RETRIES {
                match SledClient::connect_zmq_client_with_readiness_check(port).await {
                    Ok(c_tuple) => {
                        info!("ZMQ client connected successfully after {} retries.", i);
                        println!("===> ZMQ CLIENT CONNECTED SUCCESSFULLY AFTER {} RETRIES", i);
                        client_tuple_opt = Some(c_tuple);
                        break;
                    }
                    Err(e) => {
                        warn!("Attempt {}/{} to connect ZMQ client failed for endpoint {}: {}", i + 1, MAX_RETRIES, ipc_endpoint, e);
                        println!("===> WARNING: ATTEMPT {}/{} TO CONNECT ZMQ CLIENT FAILED FOR ENDPOINT {}: {}", i + 1, MAX_RETRIES, ipc_endpoint, e);
                        if i < MAX_RETRIES - 1 {
                            sleep(TokioDuration::from_millis(INITIAL_RETRY_DELAY_MS * (1 << i))).await;
                        } else {
                            error!("Failed to connect ZMQ client after {} retries at {}: {}", MAX_RETRIES, ipc_endpoint, e);
                            println!("===> ERROR: FAILED TO CONNECT ZMQ CLIENT AFTER {} RETRIES AT {}: {}", MAX_RETRIES, ipc_endpoint, e);
                            return Err(GraphError::StorageError(format!("Failed to connect ZMQ client after {} retries at {}: {}", MAX_RETRIES, ipc_endpoint, e)));
                        }
                    }
                }
            }
            client_tuple_opt.ok_or_else(|| {
                error!("Failed to initialize ZMQ client after successful poll");
                println!("===> ERROR: FAILED TO INITIALIZE ZMQ CLIENT AFTER SUCCESSFUL POLL");
                GraphError::StorageError("Failed to initialize ZMQ client after successful poll".to_string())
            })?
        };

        // 7. Store client tuple in SLED_DB singleton
        let mut sled_db_guard = sled_db_instance.lock().await;
        sled_db_guard.client = Some(client_tuple);
        drop(sled_db_guard);

        // 8. Register daemon in both registries
        let daemon_metadata = DBDaemonMetadata {
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            pid: std::process::id(),
            port,
            sled_db_instance: Some(existing_db.clone()),
            sled_vertices: Some(existing_db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?),
            sled_edges: Some(existing_db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?),
            sled_kv_pairs: Some(existing_db.open_tree("kv_pairs").map_err(|e| GraphError::StorageError(e.to_string()))?),
            rocksdb_db_instance: None,
            rocksdb_vertices: None,
            rocksdb_edges: None,
            rocksdb_kv_pairs: None,
        };

        if let Err(e) = db_daemon_registry.register_daemon(daemon_metadata).await {
            error!("Failed to register daemon in GLOBAL_DB_DAEMON_REGISTRY for port {}: {}", port, e);
            println!("===> ERROR: FAILED TO REGISTER DAEMON IN GLOBAL_DB_DAEMON_REGISTRY FOR PORT {}: {}", port, e);
            return Err(GraphError::StorageError(format!("Failed to register daemon in GLOBAL_DB_DAEMON_REGISTRY: {}", e)));
        }

        let general_daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            zmq_ready: false,
        };

        if let Err(e) = daemon_registry.register_daemon(general_daemon_metadata).await {
            error!("Failed to register daemon in GLOBAL_DAEMON_REGISTRY for port {}: {}", port, e);
            println!("===> ERROR: FAILED TO REGISTER DAEMON IN GLOBAL_DAEMON_REGISTRY FOR PORT {}: {}", port, e);
            return Err(GraphError::StorageError(format!("Failed to register daemon in GLOBAL_DAEMON_REGISTRY: {}", e)));
        }

        info!("Registered daemon for port {} in both registries", port);
        println!("===> REGISTERED DAEMON FOR PORT {} IN BOTH REGISTRIES", port);
        info!("Successfully initialized SledStorage with existing DB in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED SledStorage WITH EXISTING DB IN {}ms", start_time.elapsed().as_millis());

        Ok(SledStorage { pool })
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
            
            // Just remove the lock file, don't open the database
            match fs::remove_file(&lock_path).await {
                Ok(_) => {
                    info!("Successfully removed lock file at {:?}", lock_path);
                    println!("===> SUCCESSFULLY REMOVED LOCK FILE AT {:?}", lock_path);
                    tokio::time::sleep(TokioDuration::from_millis(500)).await;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    info!("Lock file already removed at {:?}", lock_path);
                    println!("===> LOCK FILE ALREADY REMOVED AT {:?}", lock_path);
                }
                Err(e) => {
                    warn!("Failed to remove lock file at {:?}: {}", lock_path, e);
                    println!("===> WARNING: FAILED TO REMOVE LOCK FILE AT {:?}", lock_path);
                    // Continue anyway - the daemon will handle it
                }
            }
        } else {
            info!("No lock file found at {:?}", lock_path);
            println!("===> NO LOCK FILE FOUND AT {:?}", lock_path);
        }
        
        info!("Successfully checked lock status at {:?}", path);
        println!("===> SUCCESSFULLY UNLOCKED SLED DATABASE AT {:?}", path);
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
            timeout(TokioDuration::from_secs(5), fs::remove_dir_all(&db_path))
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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

#[async_trait]
impl StorageEngine for SledStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to SledStorage");
        println!("===> CONNECTING TO SLED STORAGE");
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let pool = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled pool lock".to_string()))?;
        info!("Closing SledStorage pool");
        println!("===> CLOSING SLED STORAGE POOL");

        match timeout(TokioDuration::from_secs(10), pool.close(None)).await {
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
        let pool = match timeout(TokioDuration::from_secs(5), self.pool.lock()).await {
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
            timeout(TokioDuration::from_secs(2), daemon.is_running()).await
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
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

impl Drop for SledStorage {
    fn drop(&mut self) {
        // Use async runtime for cleanup to avoid blocking
        let pool = self.pool.clone();
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                error!("Failed to create runtime for Drop: {}", e);
                println!("===> ERROR: FAILED TO CREATE RUNTIME FOR DROP: {}", e);
                return;
            }
        };
        runtime.block_on(async {
            let start_time = Instant::now();
            info!("Dropping SledStorage instance...");
            println!("===> DROPPING SledStorage INSTANCE...");

            // Acquire pool lock with timeout
            let pool_guard = match timeout(TokioDuration::from_secs(5), pool.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    error!("Timeout acquiring pool lock during Drop");
                    println!("===> ERROR: TIMEOUT ACQUIRING POOL LOCK DURING DROP");
                    return;
                }
            };

            // Use get_active_ports and await it
            let ports = pool_guard.get_active_ports().await;
            if let Some(port) = ports.first() {
                let port_u16 = *port;
                info!("Cleaning up SledStorage for port {}", port_u16);
                println!("===> CLEANING UP SledStorage FOR PORT {}", port_u16);

                // *** SLED_DB SECTION LEFT COMPLETELY UNTOUCHED ***
                if let Some(sled_db) = SLED_DB.get() {
                    let mut sled_db_guard = match timeout(TokioDuration::from_secs(5), sled_db.lock()).await {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!("Timeout acquiring SLED_DB lock during Drop for port {}", port_u16);
                            println!("===> ERROR: TIMEOUT ACQUIRING SLED_DB LOCK DURING DROP FOR PORT {}", port_u16);
                            return;
                        }
                    };
                    // Clone db_path to avoid holding an immutable borrow
                    let db_path = sled_db_guard.path.clone();
                    info!("Flushing Sled database at {:?}", db_path);
                    println!("===> FLUSHING SLED DATABASE AT {:?}", db_path);
                    if let Err(e) = timeout(TokioDuration::from_secs(10), sled_db_guard.db.flush_async()).await {
                        error!("Failed to flush Sled database at {:?}: {:?}", db_path, e);
                        println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AT {:?}: {:?}", db_path, e);
                    }
                    // Now safe to mutate sled_db_guard
                    if let Some((_, zmq_socket)) = sled_db_guard.client.take() {
                        info!("Closing ZMQ client for path {:?}", db_path);
                        println!("===> CLOSING ZMQ CLIENT FOR PATH {:?}", db_path);
                        drop(zmq_socket);
                    }
                    // Release instance from SLED_ACTIVE_DATABASES
                    Self::release_instance(&db_path).await;
                }
                // *** END SLED_DB SECTION ***

                // Remove from pool map
                if let Some(pool_map) = SLED_POOL_MAP.get() {
                    let mut pool_map_guard = match timeout(TokioDuration::from_secs(5), pool_map.lock()).await {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!("Timeout acquiring pool map lock during Drop for port {}", port_u16);
                            println!("===> ERROR: TIMEOUT ACQUIRING POOL MAP LOCK DURING DROP FOR PORT {}", port_u16);
                            return;
                        }
                    };
                    pool_map_guard.remove(&port_u16);
                    info!("Removed SledDaemonPool for port {} from pool map", port_u16);
                    println!("===> REMOVED SledDaemonPool FOR PORT {} FROM POOL MAP", port_u16);
                }

                // Clean up daemon metadata from GLOBAL_DB_DAEMON_REGISTRY
                let daemon_registry = GLOBAL_DB_DAEMON_REGISTRY.get().await;
                if let Err(e) = daemon_registry.unregister_daemon(port_u16).await {
                    error!("Failed to unregister daemon for port {}: {}", port_u16, e);
                    println!("===> ERROR: FAILED TO UNREGISTER DAEMON FOR PORT {}: {}", port_u16, e);
                } else {
                    info!("Unregistered daemon for port {}", port_u16);
                    println!("===> UNREGISTERED DAEMON FOR PORT {}", port_u16);
                }

                // *** FIXED: Now correctly uses the ACTUAL db_path from SLED_DB ***
                let db_path = if let Some(sled_db) = SLED_DB.get() {
                    sled_db.lock().await.path.clone()
                } else {
                    PathBuf::from(DEFAULT_DATA_DIRECTORY).join("sled").join(port_u16.to_string())
                };
                
                if let Err(e) = Self::check_and_cleanup_stale_daemon(port_u16, &db_path).await {
                    error!("Failed to clean up stale daemon for port {}: {}", port_u16, e);
                    println!("===> ERROR: FAILED TO CLEAN UP STALE DAEMON FOR PORT {}: {}", port_u16, e);
                } else {
                    info!("Successfully cleaned up stale daemon for port {}", port_u16);
                    println!("===> SUCCESSFULLY CLEANED UP STALE DAEMON FOR PORT {}", port_u16);
                }
            }

            info!("Completed SledStorage cleanup in {}ms", start_time.elapsed().as_millis());
            println!("===> COMPLETED SledStorage CLEANUP IN {}ms", start_time.elapsed().as_millis());
        });
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
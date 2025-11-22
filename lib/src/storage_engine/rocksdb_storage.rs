use std::collections::{HashSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use once_cell::sync::Lazy;
use tokio::fs;
use tokio::runtime::{Handle, Runtime};
use tokio::sync::{OnceCell, Mutex as TokioMutex, RwLock};
use tokio::time::{timeout, sleep, Duration as TokioDuration};
use log::{info, debug, warn, error, trace};
use rocksdb::{ColumnFamilyDescriptor, DB, Options, DBCompactionStyle, WriteOptions, BoundColumnFamily, IteratorMode};
use crate::storage_engine::rocksdb_storage_daemon_pool::{ FULLTEXT_INDEX };
use serde_json::{json, Value};
use futures::future::join_all;
use uuid::Uuid;
use async_trait::async_trait;
use std::any::Any;

use crate::config::{
    RocksDBConfig, RocksDBStorage, RocksDBDaemonPool, StorageConfig, StorageEngineType,
    QueryResult, QueryPlan, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, RocksDBWithPath,
};
use crate::storage_engine::{StorageEngine, GraphStorageEngine, get_global_storage_registry};
use crate::storage_engine::storage_utils::{
    serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key
};
use crate::daemon::daemon_management::{find_pid_by_port, check_pid_validity, stop_process_by_pid,
                                       force_cleanup_engine_lock, is_storage_daemon_running,
                                       is_port_free, stop_process_by_port, get_ipc_endpoint};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, NonBlockingDaemonRegistry, DaemonMetadata};
use crate::daemon::daemon_utils::{parse_cluster_range};
use models::{Vertex, Edge, Identifier};
use models::errors::{GraphResult, GraphError};
use models::identifiers::SerializableUuid;
use super::rocksdb_client::{ RocksDBClient, ZmqSocketWrapper };

#[cfg(feature = "with-openraft-rocksdb")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, BasicNode},
    super::rocksdb_raft_storage::RocksDBRaftStorage,
};

pub static ROCKSDB_DB: LazyLock<OnceCell<TokioMutex<RocksDBWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static ROCKSDB_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<RocksDBDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());

// Singleton protection for RocksDB database instances
static ROCKSDB_ACTIVE_DATABASES: Lazy<RwLock<HashMap<PathBuf, u32>>> = Lazy::new(|| RwLock::new(HashMap::new()));

impl RocksDBStorage {
    // Enhanced singleton protection that allows reuse within the same process
    pub async fn ensure_single_instance(path: &Path) -> GraphResult<()> {
        let path_buf = path.to_path_buf();
        let current_pid = std::process::id();
        
        let active_dbs = ROCKSDB_ACTIVE_DATABASES.read().await;
        if let Some(&existing_pid) = active_dbs.get(&path_buf) {
            if existing_pid == current_pid {
                info!(
                    "Reusing database instance at {:?} within same process (PID: {})",
                    path, current_pid
                );
                println!(
                    "===> REUSING DATABASE INSTANCE AT {:?} WITHIN SAME PROCESS (PID: {})",
                    path, current_pid
                );
                return Ok(());
            } else {
                if check_pid_validity(existing_pid).await {
                    error!(
                        "Database at {:?} is already in use by another process (PID: {})",
                        path, existing_pid
                    );
                    println!(
                        "===> ERROR: DATABASE AT {:?} ALREADY IN USE BY PROCESS (PID: {})",
                        path, existing_pid
                    );
                    return Err(GraphError::StorageError(format!(
                        "Database at {:?} is already in use by process (PID: {}). Stop the process or use `force_reset` to clean up.",
                        path, existing_pid
                    )));
                } else {
                    info!(
                        "Stale process entry found for database at {:?} (PID: {}), cleaning up",
                        path, existing_pid
                    );
                    println!(
                        "===> STALE PROCESS ENTRY FOUND FOR DATABASE AT {:?} (PID: {})",
                        path, existing_pid
                    );
                }
            }
        }
        drop(active_dbs);

        let mut active_dbs = ROCKSDB_ACTIVE_DATABASES.write().await;
        active_dbs.insert(path_buf.clone(), current_pid);
        info!(
            "Registered database instance at {:?} for process (PID: {})",
            path_buf, current_pid
        );
        println!(
            "===> REGISTERED DATABASE INSTANCE AT {:?} FOR PROCESS (PID: {})",
            path_buf, current_pid
        );
        Ok(())
    }

    pub async fn release_instance(path: &Path) {
        let mut active_dbs = ROCKSDB_ACTIVE_DATABASES.write().await;
        let current_pid = std::process::id();
        
        if let Some(&existing_pid) = active_dbs.get(path) {
            if existing_pid == current_pid {
                active_dbs.remove(path);
                info!(
                    "Released database instance at {:?} for process (PID: {})",
                    path, current_pid
                );
                println!(
                    "===> RELEASED DATABASE INSTANCE AT {:?} FOR PROCESS (PID: {})",
                    path, current_pid
                );
            } else {
                warn!(
                    "Attempted to release database at {:?} but it's owned by PID: {} (current: {})",
                    path, existing_pid, current_pid
                );
                println!(
                    "===> WARNING: ATTEMPTED TO RELEASE DATABASE AT {:?} BUT IT'S OWNED BY PID: {} (CURRENT: {})",
                    path, existing_pid, current_pid
                );
            }
        }
    }

    pub async fn is_already_open(path: &Path) -> bool {
        let active_dbs = ROCKSDB_ACTIVE_DATABASES.read().await;
        let current_pid = std::process::id();
        
        if let Some(&existing_pid) = active_dbs.get(path) {
            existing_pid == current_pid
        } else {
            false
        }
    }

    pub async fn new(config: &RocksDBConfig, storage_config: &StorageConfig) -> GraphResult<Self> {
        let start = Instant::now();
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);

        // ------------------------------------------------------------------
        //  0.  make sure the config we pass around has the FINAL port
        // ------------------------------------------------------------------
        let mut final_config = config.clone();
        final_config.port = Some(port);

        let base = storage_config.data_directory
                          .clone()
                          .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
        let db_path = base.join("rocksdb").join(port.to_string());

        // === 1. Check global registry first ===
        {
            let registry = get_global_storage_registry().await;
            let registry_guard = registry.read().await;
            if registry_guard.contains_key(&port) {
                info!("RocksDBStorage already initialized for port {} — reusing from global registry", port);
                println!("===> REUSING RocksDBStorage FROM GLOBAL REGISTRY FOR PORT {}", port);
                let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                if let Some(pool) = pool_map_guard.get(&port) {
                    let pool_guard = pool.lock().await;
                    let daemon = pool_guard.daemons.values().next()
                        .ok_or(GraphError::StorageError("No daemon in pool".into()))?;
                    let db = daemon.db.clone();
                    return Ok(Self {
                        pool: pool.clone(),
                        use_raft_for_scale: final_config.use_raft_for_scale,
                        db,
                        #[cfg(feature = "with-openraft-rocksdb")]
                        raft: None,
                    });
                }
            }
        }

        // 2. directories / lock
        fs::create_dir_all(&db_path).await?;
        Self::ensure_single_instance(&db_path).await?;
        Self::force_unlock(&db_path).await?;

        // 3. pool singleton
        let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
            TokioMutex::new(HashMap::new())
        }).await;
        let pool = {
            let mut g = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
            g.entry(port)
             .or_insert_with(|| Arc::new(TokioMutex::new(RocksDBDaemonPool::new())))
             .clone()
        };

        // 4. open the real DB
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = Arc::new(DB::open_cf(&opts, &db_path,
                                      &["default","vertices","edges","kv_pairs","indexes"])?);

        // 5. create storage instance
        let storage = Self {
            pool: pool.clone(),
            use_raft_for_scale: final_config.use_raft_for_scale,
            db: db.clone(),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
        };

        // 6. register storage **before** starting daemon
        {
            let reg = get_global_storage_registry().await;
            reg.write().await.insert(port, Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>);
        }

        // 7. start daemon cluster  (use the SAME config)
        {
            let mut g = timeout(TokioDuration::from_secs(10), pool.lock()).await?;
            timeout(TokioDuration::from_secs(30),
                    g.initialize_cluster(storage_config, &final_config, Some(port))).await??;
        }

        // 8. wait for IPC
        let ipc = format!("/tmp/graphdb-{}.ipc", port);
        for _ in 0..20 {
            if Path::new(&ipc).exists() { break; }
            tokio::time::sleep(TokioDuration::from_millis(250)).await;
        }
        if !Path::new(&ipc).exists() {
            return Err(GraphError::StorageError("IPC not created".into()));
        }

        // 9. ZMQ client for singleton
        let (client, socket) = RocksDBClient::connect_zmq_client(port).await?;

        // 10. singleton
        let single = ROCKSDB_DB.get_or_init(|| async {
            let tmp = tempfile::tempdir().unwrap();
            let mut o = Options::default();
            o.create_if_missing(true);
            let db = DB::open_cf(&o, tmp.path(), &["default"]).unwrap();
            TokioMutex::new(RocksDBWithPath { db: Arc::new(db), path: db_path.clone(), client: None })
        }).await;
        single.lock().await.client = Some((client, socket));

        // 11. daemon metadata
        let daemon_reg = GLOBAL_DAEMON_REGISTRY.get().await;
        let meta = DaemonMetadata {
            service_type: "storage".into(),
            ip_address: final_config.host.clone().unwrap_or_else(|| "127.0.0.1".into()),
            data_dir: Some(db_path),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("rocksdb".into()),
            zmq_ready: true,
            engine_synced: true,
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            pid: std::process::id(),
            port,
        };
        daemon_reg.register_daemon(meta).await?;

        info!("RocksDBStorage initialised in {} ms", start.elapsed().as_millis());
        Ok(storage)
    }

    pub async fn new_with_db(
        config: &RocksDBConfig,
        storage_config: &StorageConfig,
        existing_db: Arc<DB>,
    ) -> GraphResult<Self> {
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);

        // ------------------------------------------------------------------
        //  0.  pin the port once and reuse the SAME config
        // ------------------------------------------------------------------
        let mut final_config = config.clone();
        final_config.port = Some(port);

        let base = storage_config.data_directory
                          .clone()
                          .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
        let db_path = base.join("rocksdb").join(port.to_string());

        // === 1. Check global registry first ===
        {
            let registry = get_global_storage_registry().await;
            let registry_guard = registry.read().await;
            if registry_guard.contains_key(&port) {
                info!("RocksDBStorage already initialized for port {} — reusing from global registry", port);
                println!("===> REUSING RocksDBStorage FROM GLOBAL REGISTRY FOR PORT {}", port);
                let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::new())
                }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                if let Some(pool) = pool_map_guard.get(&port) {
                    return Ok(Self {
                        pool: pool.clone(),
                        use_raft_for_scale: final_config.use_raft_for_scale,
                        db: existing_db,
                        #[cfg(feature = "with-openraft-rocksdb")]
                        raft: None,
                    });
                }
            }
        }

        // 2. pool singleton
        let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
            TokioMutex::new(HashMap::new())
        }).await;
        let pool = {
            let mut g = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
            g.entry(port)
             .or_insert_with(|| Arc::new(TokioMutex::new(RocksDBDaemonPool::new())))
             .clone()
        };

        // 3. create storage instance
        let storage = Self {
            pool: pool.clone(),
            use_raft_for_scale: final_config.use_raft_for_scale,
            db: existing_db.clone(),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
        };

        // 4. register storage **before** daemon
        {
            let reg = get_global_storage_registry().await;
            reg.write().await.insert(port, Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>);
        }

        // 5. start daemon  (use the SAME config)
        let db_for_daemon = existing_db.clone();   // clone for the closure
        {
            let mut g = timeout(TokioDuration::from_secs(10), pool.lock()).await?;
            timeout(
                TokioDuration::from_secs(10),
                g.initialize_cluster_with_db(storage_config, &final_config, Some(port), existing_db),
            )
            .await??;
        }

        // 6. wait for IPC
        let ipc = format!("/tmp/graphdb-{}.ipc", port);
        for _ in 0..20 {
            if Path::new(&ipc).exists() { break; }
            tokio::time::sleep(TokioDuration::from_millis(250)).await;
        }
        if !Path::new(&ipc).exists() {
            return Err(GraphError::StorageError("IPC not created".into()));
        }

        // 7. ZMQ client
        let (client, socket) = RocksDBClient::connect_zmq_client(port).await?;

        // 8. singleton
        let single = ROCKSDB_DB.get_or_try_init(|| async move {
            Ok::<_, GraphError>(TokioMutex::new(RocksDBWithPath {
                db: db_for_daemon,
                path: db_path,
                client: None,
            }))
        }).await?;
        single.lock().await.client = Some((client, socket));

        info!("RocksDBStorage (with existing DB) initialised");
        Ok(storage)
    }

    // Assuming other necessary imports like fs, PathBuf, log, GraphError, DaemonMetadata, etc. are defined earlier

    // NOTE: The function signature has been updated to accept the client tuple
    /// Creates a new RocksDBStorage instance using pre-connected ZMQ clients
    /// This is used when the daemon is already running and we have direct client access
    pub async fn new_with_client(
        config: &RocksDBConfig,
        storage_config: &StorageConfig,
        client: (RocksDBClient, RocksDBClient, RocksDBClient), // (storage, raft_log, raft_state)
    ) -> GraphResult<Self> {
        let start_time = Instant::now();
        info!("Initializing RocksDBStorage with client for port {:?}", config.port);
        println!("===> INITIALIZING RocksDBStorage WITH CLIENT FOR PORT {:?}", config.port);

        let (storage_client, raft_log_client, raft_state_client) = client;
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("rocksdb").join(port.to_string());

        info!("Using RocksDB path {:?}", db_path);
        println!("===> USING ROCKSDB PATH {:?}", db_path);

        // Create directory with retries
        let max_retries = 3;
        let mut attempt = 0;
        while attempt < max_retries {
            match fs::create_dir_all(&db_path).await {
                Ok(_) => {
                    info!("Successfully created/verified directory at {:?}", db_path);
                    println!("===> SUCCESSFULLY CREATED/VERIFIED DIRECTORY AT {:?}", db_path);
                    break;
                }
                Err(e) => {
                    error!("Attempt {}/{}: Failed to create database directory at {:?}: {}", attempt + 1, max_retries, db_path, e);
                    println!("===> ERROR: ATTEMPT {}/{}: FAILED TO CREATE ROCKSDB DIRECTORY AT {:?}", attempt + 1, max_retries, db_path);
                    attempt += 1;
                    if attempt == max_retries {
                        return Err(GraphError::StorageError(format!("Failed to create database directory at {:?} after {} attempts: {}", db_path, max_retries, e)));
                    }
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                }
            }
        }

        // Check writability
        let metadata = fs::metadata(&db_path).await.map_err(|e| {
            error!("Failed to access directory metadata at {:?}: {}", db_path, e);
            println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}", db_path);
            GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
        })?;
        if metadata.permissions().readonly() {
            error!("Directory at {:?} is not writable", db_path);
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        // Reuse existing singleton if path matches
        if let Some(rocks_db) = ROCKSDB_DB.get() {
            let rocks_db_guard = rocks_db.lock().await;
            if rocks_db_guard.path == db_path && rocks_db_guard.db.cf_handle("vertices").is_some() {
                info!("Reusing existing ROCKSDB_DB singleton for path {:?}", db_path);
                println!("===> REUSING EXISTING ROCKSDB_DB SINGLETON FOR PATH {:?}", db_path);

                let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
                let daemon_metadata_opt = daemon_registry.get_daemon_metadata(port).await.ok().flatten();
                if let Some(daemon_metadata) = daemon_metadata_opt {
                    if daemon_metadata.data_dir.as_ref() != Some(&db_path) {
                        warn!(
                            "PATH MISMATCH FOR ROCKSDB: REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}. Updating registry...",
                            daemon_metadata.data_dir, db_path
                        );
                        println!(
                            "===> PATH MISMATCH FOR ROCKSDB: REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}. UPDATING REGISTRY...",
                            daemon_metadata.data_dir, db_path
                        );
                        daemon_registry.remove_daemon_by_type("storage", port).await?;
                    }

                    if is_storage_daemon_running(port).await && check_pid_validity(daemon_metadata.pid).await {
                        let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                        let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                        if let Some(existing_pool) = pool_map_guard.get(&port) {
                            info!("Reusing existing RocksDBDaemonPool for port {}", port);
                            println!("===> REUSING EXISTING ROCKSDB DAEMON POOL FOR PORT {}", port);

                            let mut raft_handle: Option<crate::config::RocksDBRaftStorage> = None;
                            #[cfg(feature = "with-openraft-rocksdb")]
                            if config.use_raft_for_scale {
                                let handle = Self::initialize_cluster_with_client(
                                    config,
                                    storage_config,
                                    (raft_log_client, raft_state_client, storage_client.clone()),
                                    port,
                                ).await?;
                                raft_handle = Some(handle);
                            }

                            return Ok(Self {
                                pool: existing_pool.clone(),
                                use_raft_for_scale: config.use_raft_for_scale,
                                db: rocks_db_guard.db.clone(), // <-- Use the existing DB
                                #[cfg(feature = "with-openraft-rocksdb")]
                                raft: raft_handle,
                            });
                        }
                    }
                }
            }
        }

        Self::ensure_single_instance(&db_path).await.map_err(|e| {
            error!("Cannot initialize RocksDB at {:?}: {}", db_path, e);
            println!("===> ERROR: CANNOT INITIALIZE ROCKSDB AT {:?}: {}", db_path, e);
            e
        })?;

        Self::force_unlock(&db_path).await.map_err(|e| {
            error!("Failed to unlock database at {:?}: {}", db_path, e);
            println!("===> ERROR: FAILED TO UNLOCK DATABASE AT {:?}", db_path);
            e
        })?;

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let daemon_metadata_opt = daemon_registry.get_daemon_metadata(port).await.ok().flatten();

        let pool = if let Some(daemon_metadata) = daemon_metadata_opt {
            info!("Found existing daemon on port {} with PID {}", port, daemon_metadata.pid);
            println!("===> FOUND EXISTING DAEMON ON PORT {} WITH PID {}", port, daemon_metadata.pid);

            if !is_storage_daemon_running(port).await || !check_pid_validity(daemon_metadata.pid).await {
                warn!("Stale daemon found on port {}. Cleaning up...", port);
                println!("===> STALE DAEMON FOUND ON PORT {}. CLEANING UP", port);
                Self::force_unlock(&db_path).await?;
                daemon_registry.remove_daemon_by_type("storage", port).await?;
            } else {
                let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing RocksDBDaemonPool for port {}", port);
                    println!("===> REUSING EXISTING ROCKSDB DAEMON POOL FOR PORT {}", port);

                    let mut raft_handle: Option<crate::config::RocksDBRaftStorage> = None;
                    #[cfg(feature = "with-openraft-rocksdb")]
                    if config.use_raft_for_scale {
                        let handle = Self::initialize_cluster_with_client(
                            config,
                            storage_config,
                            (raft_log_client, raft_state_client, storage_client.clone()),
                            port,
                        ).await?;
                        raft_handle = Some(handle);
                    }

                    return Ok(Self {
                        pool: existing_pool.clone(),
                        use_raft_for_scale: config.use_raft_for_scale,
                        db: Arc::new(rocksdb::DB::open_default(&db_path)?), // Open DB if not in singleton
                        #[cfg(feature = "with-openraft-rocksdb")]
                        raft: raft_handle,
                    });
                }
            }

            let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new_with_client(storage_client.clone(), &db_path, port).await?));
            let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
            pool_map_guard.insert(port, new_pool.clone());
            new_pool
        } else {
            info!("No existing daemon found for port {}. Creating new pool...", port);
            println!("===> NO EXISTING DAEMON FOUND FOR PORT {}. CREATING NEW POOL", port);
            Self::force_unlock(&db_path).await?;

            let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new_with_client(storage_client.clone(), &db_path, port).await?));
            let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
            pool_map_guard.insert(port, new_pool.clone());
            new_pool
        };

        let config_dir = PathBuf::from(storage_config.config_root_directory.clone().unwrap_or_default());
        if !config_dir.exists() {
            fs::create_dir_all(&config_dir).await.map_err(|e| {
                error!("Failed to create config directory at {:?}: {}", config_dir, e);
                println!("===> ERROR: FAILED TO CREATE CONFIG DIRECTORY AT {:?}", config_dir);
                GraphError::StorageError(format!("Failed to create config directory at {:?}: {}", config_dir, e))
            })?;
        }

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: Some(config_dir),
            engine_type: Some("rocksdb".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            zmq_ready: false,
            engine_synced: false,
            pid: std::process::id() as u32,
            port,
        };

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        daemon_registry.register_daemon(daemon_metadata).await.map_err(|e| {
            error!("Failed to register daemon for port {}: {}", port, e);
            println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
            GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
        })?;

        info!("Registered daemon for port {}", port);
        println!("===> REGISTERED DAEMON FOR PORT {}", port);

        let mut raft_handle: Option<crate::config::RocksDBRaftStorage> = None;
        #[cfg(feature = "with-openraft-rocksdb")]
        if config.use_raft_for_scale {
            info!("Initializing OpenRaft cluster with client for port {}", port);
            println!("===> INITIALIZING OPENRAFT CLUSTER WITH CLIENT FOR PORT {}", port);
            let handle = Self::initialize_cluster_with_client(
                config,
                storage_config,
                (raft_log_client, raft_state_client, storage_client),
                port,
            ).await?;
            raft_handle = Some(handle);
        }

        // Open the actual RocksDB instance
        let db = Arc::new(rocksdb::DB::open_default(&db_path)?);

        let storage = Self {
            pool,
            use_raft_for_scale: config.use_raft_for_scale,
            db, // <-- Now included
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: raft_handle,
        };

        info!("Successfully initialized RocksDBStorage with client in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED RocksDBStorage WITH CLIENT IN {}ms", start_time.elapsed().as_millis());
        Ok(storage)
    }

    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        let lock_path = path.join("LOCK");
        info!("Checking for lock file at {:?}", lock_path);
        println!("===> CHECKING FOR LOCK FILE AT {:?}", lock_path);

        if lock_path.exists() {
            warn!("Found lock file at {:?}", lock_path);
            println!("===> FOUND LOCK FILE AT {:?}", lock_path);
            timeout(TokioDuration::from_secs(2), fs::remove_file(&lock_path))
                .await
                .map_err(|_| {
                    error!("Timeout removing lock file at {:?}", lock_path);
                    println!("===> ERROR: TIMEOUT REMOVING LOCK FILE AT {:?}", lock_path);
                    GraphError::StorageError(format!("Timeout removing lock file at {:?}", lock_path))
                })?
                .map_err(|e| {
                    error!("Failed to remove lock file at {:?}: {}", lock_path, e);
                    println!("===> ERROR: FAILED TO REMOVE LOCK FILE AT {:?}", lock_path);
                    GraphError::StorageError(format!("Failed to remove lock file at {:?}: {}", lock_path, e))
                })?;
            info!("Successfully removed lock file at {:?}", lock_path);
            println!("===> SUCCESSFULLY REMOVED LOCK FILE AT {:?}", lock_path);
        } else {
            info!("No lock file found at {:?}", lock_path);
            println!("===> NO LOCK FILE FOUND AT {:?}", lock_path);
        }
        Ok(())
    }

    pub async fn force_reset(config: &RocksDBConfig) -> GraphResult<Self> {
        warn!("FORCE RESET: Initiating database reset at {:?}", config.path);
        println!("===> FORCE RESET: INITIATING DATABASE RESET AT {:?}", config.path);

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let db_path = default_data_dir.join("rocksdb").join(port.to_string());

        if let Some(pid) = find_pid_by_port(port).await {
            if check_pid_validity(pid).await {
                error!("Cannot reset database: Active process (PID: {}) on port {}", pid, port);
                println!("===> ERROR: CANNOT RESET DATABASE: ACTIVE PROCESS (PID: {}) ON PORT {}", pid, port);
                return Err(GraphError::StorageError(format!(
                    "Cannot reset database: Active process (PID: {}) on port {}", pid, port
                )));
            }
        }

        Self::force_unlock(&db_path).await?;

        if db_path.exists() {
            info!("Destroying existing RocksDB database at {:?}", db_path);
            println!("===> DESTROYING EXISTING ROCKSDB DATABASE AT {:?}", db_path);
            let mut opts = Options::default();
            opts.set_paranoid_checks(false);
            if let Err(e) = DB::destroy(&opts, &db_path) {
                warn!("Failed to destroy database at {:?}: {}", db_path, e);
                println!("===> WARNING: FAILED TO DESTROY DATABASE AT {:?}", db_path);
            }
            timeout(TokioDuration::from_secs(5), fs::remove_dir_all(&db_path))
                .await
                .map_err(|_| {
                    error!("Timeout removing directory at {:?}", db_path);
                    println!("===> ERROR: TIMEOUT REMOVING DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Timeout removing directory at {:?}", db_path))
                })?
                .map_err(|e| {
                    error!("Failed to remove directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO REMOVE DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to remove directory at {:?}: {}", db_path, e))
                })?;
            info!("Successfully removed database directory at {:?}", db_path);
            println!("===> SUCCESSFULLY REMOVED DATABASE DIRECTORY AT {:?}", db_path);
        }

        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to recreate database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO RECREATE DATABASE DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to recreate database directory at {:?}: {}", db_path, e))
            })?;

        Self::new(config, &StorageConfig::default()).await
    }

    pub fn new_pinned(config: &RocksDBConfig, storage_config: &StorageConfig) -> Box<dyn futures::Future<Output = GraphResult<Self>> + Send + 'static> {
        let config = config.clone();
        let storage_config = storage_config.clone();
        Box::new(async move {
            RocksDBStorage::new(&config, &storage_config).await
        })
    }

    pub async fn initialize_database_instance(db_path: &Path, port: u16) -> GraphResult<TokioMutex<RocksDBWithPath>> {
        info!("Opening new RocksDB database at {:?}", db_path);
        println!("===> ATTEMPTING TO OPEN ROCKSDB AT {:?}", db_path);

        let mut should_start_daemon = false;
        if !is_port_free(port).await {
            if let Some(pid) = find_pid_by_port(port).await {
                if NonBlockingDaemonRegistry::is_pid_running(pid).await.unwrap_or(false) {
                    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
                    if let Ok(Some(metadata)) = daemon_registry.get_daemon_metadata(port).await {
                        if metadata.service_type == "storage" && metadata.pid == pid {
                            info!("Valid storage daemon (PID: {}) already running on port {}. Reusing...", pid, port);
                            println!("===> VALID STORAGE DAEMON (PID: {}) ALREADY RUNNING ON PORT {}. REUSING...", pid, port);
                        } else {
                            info!("Found process (PID: {}) on port {} but not a valid storage daemon. Stopping it...", pid, port);
                            println!("===> FOUND PROCESS (PID: {}) ON PORT {} BUT NOT A VALID STORAGE DAEMON. STOPPING IT...", pid, port);
                            stop_process_by_port("Storage Daemon", port)
                                .await
                                .map_err(|e| {
                                    error!("Failed to stop process on port {}: {}", port, e);
                                    println!("===> ERROR: FAILED TO STOP PROCESS ON PORT {}: {}", port, e);
                                    GraphError::StorageError(format!("Failed to stop process on port {}: {}", port, e))
                                })?;
                            should_start_daemon = true;
                        }
                    } else {
                        info!("Found process (PID: {}) on port {} but no daemon metadata. Stopping it...", pid, port);
                        println!("===> FOUND PROCESS (PID: {}) ON PORT {} BUT NO DAEMON METADATA. STOPPING IT...", pid, port);
                        stop_process_by_port("Storage Daemon", port)
                            .await
                            .map_err(|e| {
                                error!("Failed to stop process on port {}: {}", port, e);
                                println!("===> ERROR: FAILED TO STOP PROCESS ON PORT {}: {}", port, e);
                                GraphError::StorageError(format!("Failed to stop process on port {}: {}", port, e))
                            })?;
                        should_start_daemon = true;
                    }

                    if should_start_daemon {
                        let max_attempts = 5;
                        let mut attempts = 0;
                        while !is_port_free(port).await && attempts < max_attempts {
                            debug!("Port {} still in use after stopping PID {}. Waiting... (attempt {}/{})", port, pid, attempts + 1, max_attempts);
                            println!("===> PORT {} STILL IN USE AFTER STOPPING PID {}. WAITING... (ATTEMPT {}/{})", port, pid, attempts + 1, max_attempts);
                            sleep(TokioDuration::from_millis(500)).await;
                            attempts += 1;
                        }
                        if attempts >= max_attempts {
                            error!("Port {} still in use after {} attempts to stop PID {}", port, max_attempts, pid);
                            println!("===> ERROR: PORT {} STILL IN USE AFTER {} ATTEMPTS TO STOP PID {}", port, max_attempts, pid);
                            return Err(GraphError::StorageError(
                                format!("Port {} still in use after {} attempts to stop PID {}", port, max_attempts, pid)
                            ));
                        }
                    }
                } else {
                    info!("Stale PID {} found on port {}. Cleaning up...", pid, port);
                    println!("===> STALE PID {} FOUND ON PORT {}. CLEANING UP...", pid, port);
                    should_start_daemon = true;
                }
            }
        } else {
            should_start_daemon = true;
        }

        let lock_path = db_path.join("LOCK");
        if lock_path.exists() {
            warn!("Found lock file at {:?}", lock_path);
            println!("===> FOUND LOCK FILE AT {:?}", lock_path);
            force_cleanup_engine_lock(StorageEngineType::RocksDB, &Some(db_path.to_path_buf()))
                .await
                .map_err(|e| {
                    error!("Failed to clean up lock file at {:?}: {}", lock_path, e);
                    println!("===> ERROR: FAILED TO CLEAN UP LOCK FILE AT {:?}", lock_path);
                    GraphError::StorageError(format!("Failed to clean up lock file at {:?}: {}", lock_path, e))
                })?;
            info!("Successfully cleaned up lock file at {:?}", lock_path);
            println!("===> SUCCESSFULLY CLEANED UP LOCK FILE AT {:?}", lock_path);
        }

        let storage_config = crate::config::load_storage_config_from_yaml(None)
            .await
            .unwrap_or_else(|_| StorageConfig::default());

        let standard_cf_names = vec!["vertices", "edges", "kv_pairs"];
        let raft_cf_names = vec!["raft_vote", "raft_membership", "raft_snapshot", "raft_log", "data"];

        let existing_cfs = if db_path.exists() {
            match DB::list_cf(&Options::default(), db_path) {
                Ok(cfs) => cfs,
                Err(e) => {
                    warn!("Failed to list existing column families at {:?}: {}. Attempting to create new database.", db_path, e);
                    println!("===> WARNING: FAILED TO LIST COLUMN FAMILIES AT {:?}", db_path);
                    vec![]
                }
            }
        } else {
            vec![]
        };

        let mut cf_names = standard_cf_names.clone();
        if storage_config.use_raft_for_scale {
            cf_names.extend(raft_cf_names.clone());
        }
        for cf in &existing_cfs {
            if !cf_names.contains(&cf.as_str()) {
                warn!("Including existing column family '{}' at {:?} for compatibility.", cf, db_path);
                println!("===> INCLUDING EXISTING COLUMN FAMILY '{}' AT {:?}", cf, db_path);
                cf_names.push(cf);
            }
        }

        let cfs: Vec<ColumnFamilyDescriptor> = cf_names.iter().map(|name| {
            let mut cf_opts = Options::default();
            cf_opts.set_max_write_buffer_number(2);
            ColumnFamilyDescriptor::new(name.to_string(), cf_opts)
        }).collect();

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compaction_style(DBCompactionStyle::Level);
        opts.set_paranoid_checks(false);
        opts.set_error_if_exists(false);
        opts.set_max_open_files(storage_config.max_open_files as i32);

        let max_init_attempts = 3;
        let mut attempts = 0;
        let db = loop {
            let cfs: Vec<ColumnFamilyDescriptor> = cf_names.iter().map(|name| {
                let mut cf_opts = Options::default();
                cf_opts.set_max_write_buffer_number(2);
                ColumnFamilyDescriptor::new(*name, cf_opts)
            }).collect();

            match timeout(TokioDuration::from_secs(5), async {
                DB::open_cf_descriptors(&opts, db_path, cfs).map_err(|e| {
                    error!("Failed to open RocksDB at {:?}: {}. Run `graphdb-cli force-reset --port {}` to reset.", db_path, e, port);
                    println!("===> ERROR: FAILED TO OPEN ROCKSDB AT {:?}: {}. RUN `graphdb-cli force-reset --port {}` TO RESET.", db_path, e, port);
                    GraphError::StorageError(format!("Failed to open RocksDB database at {:?}: {}. Run `graphdb-cli force-reset --port {}` to reset.", db_path, e, port))
                })
            }).await {
                Ok(Ok(db)) => break db,
                Ok(Err(e)) => {
                    attempts += 1;
                    if attempts >= max_init_attempts {
                        error!("Failed to open RocksDB at {:?} after {} attempts", db_path, max_init_attempts);
                        println!("===> ERROR: FAILED TO OPEN ROCKSDB AT {:?} AFTER {} ATTEMPTS", db_path, max_init_attempts);
                        return Err(e);
                    }
                    error!("Failed to open RocksDB at {:?} (attempt {}/{}): {}. Retrying...", db_path, attempts, max_init_attempts, e);
                    println!("===> ERROR: FAILED TO OPEN ROCKSDB AT {:?} (ATTEMPT {}/{}): {}. RETRYING...", db_path, attempts, max_init_attempts, e);
                    sleep(TokioDuration::from_millis(1000)).await;

                    if lock_path.exists() {
                        if let Some(pid) = find_pid_by_port(port).await {
                            warn!("New process (PID: {}) created lock file at {}. Stopping it...", pid, lock_path.display());
                            println!("===> NEW PROCESS (PID: {}) CREATED LOCK FILE AT {}. STOPPING IT...", pid, lock_path.display());
                            stop_process_by_port("Storage Daemon", port)
                                .await
                                .map_err(|e| {
                                    error!("Failed to stop new process on port {}: {}", port, e);
                                    println!("===> ERROR: FAILED TO STOP NEW PROCESS ON PORT {}: {}", port, e);
                                    GraphError::StorageError(format!("Failed to stop new process on port {}: {}", port, e))
                                })?;
                            force_cleanup_engine_lock(StorageEngineType::RocksDB, &Some(db_path.to_path_buf()))
                                .await
                                .map_err(|e| {
                                    error!("Failed to clean up new lock file at {:?}: {}", lock_path, e);
                                    println!("===> ERROR: FAILED TO CLEAN UP NEW LOCK FILE AT {:?}", lock_path);
                                    GraphError::StorageError(format!("Failed to clean up new lock file at {:?}: {}", lock_path, e))
                                })?;
                            should_start_daemon = true;
                        }
                    }
                }
                Err(_) => {
                    attempts += 1;
                    if attempts >= max_init_attempts {
                        error!("Timeout opening RocksDB at {:?} after {} attempts", db_path, max_init_attempts);
                        println!("===> ERROR: TIMEOUT OPENING ROCKSDB AT {:?} AFTER {} ATTEMPTS", db_path, max_init_attempts);
                        return Err(GraphError::StorageError(format!("Timeout opening RocksDB at {:?} after {} attempts", db_path, max_init_attempts)));
                    }
                    error!("Timeout opening RocksDB at {:?} (attempt {}/{}). Retrying...", db_path, attempts, max_init_attempts);
                    println!("===> ERROR: TIMEOUT OPENING ROCKSDB AT {:?} (ATTEMPT {}/{}). RETRYING...", db_path, attempts, max_init_attempts);
                    sleep(TokioDuration::from_millis(1000)).await;
                }
            }
        };

        for cf_name in &standard_cf_names {
            if db.cf_handle(cf_name).is_none() {
                error!("Column family {} not opened in database at {:?}", cf_name, db_path);
                println!("===> ERROR: COLUMN FAMILY {} NOT OPENED IN DATABASE AT {:?}", cf_name, db_path);
                return Err(GraphError::StorageError(format!("Column family {} not opened in database at {:?}", cf_name, db_path)));
            }
        }

        if should_start_daemon {
            let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
            let daemon_metadata = DaemonMetadata {
                service_type: "storage".to_string(),
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(db_path.to_path_buf()),
                config_path: Some(PathBuf::from("./storage_daemon_server")),
                engine_type: Some("rocksdb".to_string()),
                zmq_ready: false,
                engine_synced: false,
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64,
                pid: std::process::id() as u32,
                port,
            };
            daemon_registry.register_daemon(daemon_metadata).await
                .map_err(|e| {
                    error!("Failed to register daemon for port {}: {}", port, e);
                    println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
                    GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
                })?;
            info!("Registered new storage daemon for port {}", port);
            println!("===> REGISTERED NEW STORAGE DAEMON FOR PORT {}", port);

            let pid_file = format!("/tmp/graphdb-storage-{}.pid", port);
            fs::write(&pid_file, std::process::id().to_string())
                .await
                .map_err(|e| {
                    error!("Failed to write PID file {}: {}", pid_file, e);
                    println!("===> ERROR: FAILED TO WRITE PID FILE {}: {}", pid_file, e);
                    GraphError::StorageError(format!("Failed to write PID file {}: {}", pid_file, e))
                })?;
            info!("Wrote PID file for storage daemon at {}", pid_file);
            println!("===> WROTE PID FILE FOR STORAGE DAEMON AT {}", pid_file);
        }

        info!("Successfully opened RocksDB database at {:?}", db_path);
        println!("===> SUCCESSFULLY OPENED ROCKSDB DATABASE AT {:?}", db_path);
        Ok(TokioMutex::new(RocksDBWithPath {
            db: Arc::new(db),
            path: db_path.to_path_buf(),
            client: None,
        }))
    }
    
    async fn initialize_database(
        config: &RocksDBConfig,
        storage_config: &StorageConfig,
        db_path: &Path,
        port: u16,
    ) -> GraphResult<Self> {
        let start_time = Instant::now();
        info!("Starting initialize_database for port {}", port);
        println!("===> STARTING INITIALIZE_DATABASE FOR PORT {}", port);

        // Ensure parent directory exists
        let parent_dir = db_path.parent().ok_or_else(|| {
            error!("Invalid database path: no parent directory for {:?}", db_path);
            println!("===> ERROR: INVALID DATABASE PATH: NO PARENT DIRECTORY FOR {:?}", db_path);
            GraphError::StorageError(format!("Invalid database path: no parent directory for {:?}", db_path))
        })?;
        fs::create_dir_all(&parent_dir)
            .await
            .map_err(|e| {
                error!("Failed to create parent directory at {:?}: {}", parent_dir, e);
                println!("===> ERROR: FAILED TO CREATE PARENT DIRECTORY AT {:?}", parent_dir);
                GraphError::StorageError(format!("Failed to create parent directory at {:?}: {}", parent_dir, e))
            })?;

        // Create database directory
        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO CREATE ROCKSDB DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
            })?;

        let path_buf = db_path.to_path_buf();
        Self::ensure_single_instance(&path_buf).await.map_err(|e| {
            error!("Cannot initialize RocksDB at {:?}: {}", db_path, e);
            println!("===> ERROR: CANNOT INITIALIZE ROCKSDB AT {:?}: {}", db_path, e);
            e
        })?;

        let rocks_db_instance = ROCKSDB_DB.get_or_try_init(|| async {
            info!("Opening new RocksDB database at {:?}", db_path);
            println!("===> ATTEMPTING TO OPEN ROCKSDB AT {:?}", db_path);

            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            opts.set_compaction_style(DBCompactionStyle::Level);
            opts.set_paranoid_checks(false);
            opts.set_error_if_exists(false);

            let cf_names = vec!["vertices", "edges", "kv_pairs"];
            let cfs: Vec<ColumnFamilyDescriptor> = cf_names
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
                .collect();

            let db_result = timeout(TokioDuration::from_secs(5), async {
                let db = DB::open_cf_descriptors(&opts, &db_path, cfs)
                    .map_err(|e| {
                        error!("Failed to open RocksDB at {:?}: {}", db_path, e);
                        println!("===> ERROR: FAILED TO OPEN ROCKSDB AT {:?}: {}", db_path, e);
                        GraphError::StorageError(format!("Failed to open RocksDB database at {:?}: {}", db_path, e))
                    })?;

                // Verify column families are accessible
                for cf_name in &cf_names {
                    if db.cf_handle(cf_name).is_none() {
                        error!("Column family {} not opened in database at {:?}", cf_name, db_path);
                        println!("===> ERROR: COLUMN FAMILY {} NOT OPENED IN DATABASE AT {:?}", cf_name, db_path);
                        return Err(GraphError::StorageError(format!("Column family {} not opened in database at {:?}", cf_name, db_path)));
                    }
                }

                Ok::<_, GraphError>(Arc::new(db))
            })
            .await;

            let db = db_result.map_err(|_| {
                error!("Timeout opening RocksDB at {:?}", db_path);
                println!("===> ERROR: TIMEOUT OPENING ROCKSDB AT {:?}", db_path);
                GraphError::StorageError(format!("Timeout opening RocksDB at {:?}", db_path))
            })?;

            let db = db?;

            info!("Successfully opened RocksDB database at {:?}", db_path);
            println!("===> SUCCESSFULLY OPENED ROCKSDB DATABASE AT {:?}", db_path);
            Ok::<_, GraphError>(TokioMutex::new(RocksDBWithPath {
                db,
                path: db_path.to_path_buf(),
                client: None,
            }))
        }).await.map_err(|e| {
            error!("Failed to initialize RocksDB singleton at {:?}: {}", db_path, e);
            println!("===> ERROR: FAILED TO INITIALIZE ROCKSDB SINGLETON AT {:?}", db_path);
            e
        })?;

        let pool = {
            let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock())
                .await
                .map_err(|_| {
                    error!("Failed to acquire pool map lock for port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL MAP LOCK FOR PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool map lock".to_string())
                })?;
            let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new()));
            pool_map_guard.insert(port, new_pool.clone());
            new_pool
        };

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.to_path_buf()),
            config_path: Some(PathBuf::from(storage_config.config_root_directory.clone().unwrap_or_default())),
            engine_type: Some("rocksdb".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            zmq_ready: false,
            engine_synced: false,
            pid: std::process::id() as u32,
            port,
        };
        GLOBAL_DAEMON_REGISTRY.get().await.register_daemon(daemon_metadata).await
            .map_err(|e| {
                error!("Failed to register daemon for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
            })?;
        info!("Registered daemon for port {}", port);
        println!("===> REGISTERED DAEMON FOR PORT {}", port);

        {
            let mut pool_guard = timeout(TokioDuration::from_secs(5), pool.lock())
                .await
                .map_err(|_| {
                    error!("Failed to acquire pool lock for initialization on port {}", port);
                    println!("===> ERROR: FAILED TO ACQUIRE POOL LOCK FOR INITIALIZATION ON PORT {}", port);
                    GraphError::StorageError("Failed to acquire pool lock for initialization".to_string())
                })?;
            info!("Initializing cluster with use_raft: {}", config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT: {}", config.use_raft_for_scale);
            let rocks_db_guard = rocks_db_instance.lock().await;
            timeout(TokioDuration::from_secs(5), pool_guard.initialize_with_db(config, rocks_db_guard.db.clone()))
                .await
                .map_err(|_| {
                    error!("Timeout initializing RocksDBDaemonPool for port {}", port);
                    println!("===> ERROR: TIMEOUT INITIALIZING ROCKSDB DAEMON POOL FOR PORT {}", port);
                    GraphError::StorageError("Timeout initializing RocksDBDaemonPool".to_string())
                })??;
            info!("Initialized cluster on port {} with existing DB", port);
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        let storage = Self {
            pool,
            use_raft_for_scale: config.use_raft_for_scale,
            db: Arc::new(rocksdb::DB::open_default(&db_path)?), // Open DB if not in singleton
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
        };

        #[cfg(feature = "with-openraft-rocksdb")]
        if storage.use_raft_for_scale {
            let raft_config = RaftConfig {
                cluster_name: storage_config.cluster_name.clone().unwrap_or_default(),
                ..Default::default()
            };
            let raft_storage = RocksDBRaftStorage::new_from_config(config, storage_config)?;
            let raft = Raft::new(
                storage_config.node_id.unwrap_or(1) as NodeId,
                Arc::new(raft_config),
                Arc::new(raft_storage),
                Arc::new(RocksDBClient::new()),
            );
            storage.raft = Some(raft);
        }

        info!("Successfully initialized RocksDBStorage in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED RocksDBStorage IN {}ms", start_time.elapsed().as_millis());
        Ok(storage)
    }

    // Helper method to get database path from pool
    // Helper method to get database path from pool
    async fn get_database_path(&self) -> Option<PathBuf> {
        if let Ok(pool_guard) = timeout(TokioDuration::from_secs(1), self.pool.lock()).await {
            // Access the daemons HashMap directly (no RwLock anymore)
            pool_guard.daemons.values().next().map(|daemon| daemon.db_path.clone())
        } else {
            warn!("Timeout while trying to acquire lock on the RocksDBDaemonPool.");
            None
        }
    }
    

    pub async fn set_key(&self, key: &str, value: &str) -> GraphResult<()> {
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for set_key");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR SET_KEY");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "set_key",
                    "key": key,
                    "value": value,
                    "cf": "kv_pairs",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft write failed for key '{}': {}", key, e);
                        println!("===> ERROR: RAFT WRITE FAILED FOR KEY {}", key);
                        GraphError::StorageError(format!("Raft write failed: {}", e))
                    })?;
                info!("Successfully set key '{}' via Raft", key);
                println!("===> SUCCESSFULLY SET KEY {} VIA RAFT", key);
                return Ok(());
            }
        }

        pool_guard.insert_replicated(key.as_bytes(), value.as_bytes(), self.use_raft_for_scale).await
            .map_err(|e| {
                error!("Failed to set key '{}': {}", key, e);
                println!("===> ERROR: FAILED TO SET KEY {}", key);
                GraphError::StorageError(format!("Failed to set key '{}': {}", key, e))
            })?;
        info!("Successfully set key '{}'", key);
        println!("===> SUCCESSFULLY SET KEY {}", key);
        Ok(())
    }

    pub async fn get_key(&self, key: &str) -> GraphResult<Option<String>> {
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for get_key");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR GET_KEY");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "get_key",
                    "key": key,
                    "cf": "kv_pairs"
                });
                let response = raft.client_read(request).await
                    .map_err(|e| {
                        error!("Raft read failed for key '{}': {}", key, e);
                        println!("===> ERROR: RAFT READ FAILED FOR KEY {}", key);
                        GraphError::StorageError(format!("Raft read failed: {}", e))
                    })?;
                let value = response["value"].as_str().map(|v| v.to_string());
                info!("Retrieved value for key '{}': {:?}", key, value);
                println!("===> RETRIEVED VALUE FOR KEY {}: {:?}", key, value);
                return Ok(value);
            }
        }

        let value = pool_guard.retrieve_with_failover(key.as_bytes()).await
            .map_err(|e| {
                error!("Failed to retrieve key '{}': {}", key, e);
                println!("===> ERROR: FAILED TO RETRIEVE KEY {}", key);
                GraphError::StorageError(format!("Failed to retrieve key '{}': {}", key, e))
            })?
            .map(|v| String::from_utf8_lossy(&v).to_string());
        info!("Retrieved value for key '{}': {:?}", key, value);
        println!("===> RETRIEVED VALUE FOR KEY {}: {:?}", key, value);
        Ok(value)
    }

    pub async fn delete_key(&self, key: &str) -> GraphResult<()> {
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for delete_key");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR DELETE_KEY");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "delete_key",
                    "key": key,
                    "cf": "kv_pairs",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft delete failed for key '{}': {}", key, e);
                        println!("===> ERROR: RAFT DELETE FAILED FOR KEY {}", key);
                        GraphError::StorageError(format!("Raft delete failed: {}", e))
                    })?;
                info!("Successfully deleted key '{}' via Raft", key);
                println!("===> SUCCESSFULLY DELETED KEY {} VIA RAFT", key);
                return Ok(());
            }
        }

        pool_guard.delete_replicated(key.as_bytes(), self.use_raft_for_scale).await
            .map_err(|e| {
                error!("Failed to delete key '{}': {}", key, e);
                println!("===> ERROR: FAILED TO DELETE KEY {}", key);
                GraphError::StorageError(format!("Failed to delete key '{}': {}", key, e))
            })?;
        info!("Successfully deleted key '{}'", key);
        println!("===> SUCCESSFULLY DELETED KEY {}", key);
        Ok(())
    }

    pub async fn add_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        // Get the singleton with the ZMQ client
        let singleton = ROCKSDB_DB.get()
            .ok_or_else(|| GraphError::StorageError("RocksDB singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        // Check if we have a ZMQ client
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO ADD VERTEX");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.create_vertex(vertex).await;
        }
        
        // Fallback to direct access
        println!("========================== USING DIRECT DB ACCESS TO ADD VERTEX ========================");
        drop(guard);
        
        let key = vertex.id.to_string();
        let value = serialize_vertex(&vertex).map_err(|e| {
            error!("Failed to serialize vertex '{}': {}", key, e);
            println!("===> ERROR: FAILED TO SERIALIZE VERTEX {}", key);
            GraphError::StorageError(format!("Failed to serialize vertex '{}': {}", key, e))
        })?;
        
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for add_vertex");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR ADD_VERTEX");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "add_vertex",
                    "key": key,
                    "value": value,
                    "cf": "vertices",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft write failed for vertex '{}': {}", key, e);
                        println!("===> ERROR: RAFT WRITE FAILED FOR VERTEX {}", key);
                        GraphError::StorageError(format!("Raft write failed: {}", e))
                    })?;
                info!("Successfully added vertex '{}' via Raft", key);
                println!("===> SUCCESSFULLY ADDED VERTEX {} VIA RAFT", key);
                return Ok(());
            }
        }
        
        pool_guard.insert_replicated(key.as_bytes(), &value, self.use_raft_for_scale).await
            .map_err(|e| {
                error!("Failed to add vertex '{}': {}", key, e);
                println!("===> ERROR: FAILED TO ADD VERTEX {}", key);
                GraphError::StorageError(format!("Failed to add vertex '{}': {}", key, e))
            })?;
        
        info!("Successfully added vertex '{}'", key);
        println!("===> SUCCESSFULLY ADDED VERTEX {}", key);
        Ok(())
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        // Get the singleton with the ZMQ client
        let singleton = ROCKSDB_DB.get()
            .ok_or_else(|| GraphError::StorageError("RocksDB singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        // Check if we have a ZMQ client
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO GET VERTEX");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.get_vertex(id).await;
        }
        
        // Fallback to direct access
        println!("========================== USING DIRECT DB ACCESS TO GET VERTEX ========================");
        drop(guard);
        
        let key = id.to_string();
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for get_vertex");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR GET_VERTEX");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "get_vertex",
                    "key": key,
                    "cf": "vertices"
                });
                let response = raft.client_read(request).await
                    .map_err(|e| {
                        error!("Raft read failed for vertex '{}': {}", key, e);
                        println!("===> ERROR: RAFT READ FAILED FOR VERTEX {}", key);
                        GraphError::StorageError(format!("Raft read failed: {}", e))
                    })?;
                let value = response["value"].as_str().map(|v| v.as_bytes().to_vec());
                let vertex = value.and_then(|v| deserialize_vertex(&v).ok());
                info!("Retrieved vertex '{}': {:?}", key, vertex);
                println!("===> RETRIEVED VERTEX {}: {:?}", key, vertex);
                return Ok(vertex);
            }
        }

        let value = pool_guard.retrieve_with_failover(key.as_bytes()).await
            .map_err(|e| {
                error!("Failed to retrieve vertex '{}': {}", key, e);
                println!("===> ERROR: FAILED TO RETRIEVE VERTEX {}", key);
                GraphError::StorageError(format!("Failed to retrieve vertex '{}': {}", key, e))
            })?;
        let vertex = value.and_then(|v| deserialize_vertex(&v).ok());
        info!("Retrieved vertex '{}': {:?}", key, vertex);
        println!("===> RETRIEVED VERTEX {}: {:?}", key, vertex);
        Ok(vertex)
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        // Get the singleton with the ZMQ client
        let singleton = ROCKSDB_DB.get()
            .ok_or_else(|| GraphError::StorageError("RocksDB singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        // Check if we have a ZMQ client
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO DELETE VERTEX");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.delete_vertex(id).await;
        }
        
        // Fallback to direct access
        println!("========================== USING DIRECT DB ACCESS TO DELETE VERTEX ========================");
        drop(guard);
        
        let key = id.to_string();
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for delete_vertex");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR DELETE_VERTEX");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "delete_vertex",
                    "key": key,
                    "cf": "vertices",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft delete failed for vertex '{}': {}", key, e);
                        println!("===> ERROR: RAFT DELETE FAILED FOR VERTEX {}", key);
                        GraphError::StorageError(format!("Raft delete failed: {}", e))
                    })?;
                info!("Successfully deleted vertex '{}' via Raft", key);
                println!("===> SUCCESSFULLY DELETED VERTEX {} VIA RAFT", key);
                return Ok(());
            }
        }

        pool_guard.delete_replicated(key.as_bytes(), self.use_raft_for_scale).await
            .map_err(|e| {
                error!("Failed to delete vertex '{}': {}", key, e);
                println!("===> ERROR: FAILED TO DELETE VERTEX {}", key);
                GraphError::StorageError(format!("Failed to delete vertex '{}': {}", key, e))
            })?;
        info!("Successfully deleted vertex '{}'", key);
        println!("===> SUCCESSFULLY DELETED VERTEX {}", key);
        Ok(())
    }

    pub async fn add_edge(&self, edge: Edge) -> GraphResult<()> {
        // Get the singleton with the ZMQ client
        let singleton = ROCKSDB_DB.get()
            .ok_or_else(|| GraphError::StorageError("RocksDB singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        // Check if we have a ZMQ client
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO ADD EDGE");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.create_edge(edge).await;
        }
        
        // Fallback to direct access
        println!("========================== USING DIRECT DB ACCESS TO ADD EDGE ========================");
        drop(guard);
        
        let edge_key = create_edge_key(&edge.outbound_id, &edge.edge_type, &edge.inbound_id)
            .map_err(|e| {
                error!("Failed to create edge key: {}", e);
                println!("===> ERROR: FAILED TO CREATE EDGE KEY");
                GraphError::StorageError(format!("Failed to create edge key: {}", e))
            })?;
        let value = serialize_edge(&edge).map_err(|e| {
            error!("Failed to serialize edge: {}", e);
            println!("===> ERROR: FAILED TO SERIALIZE EDGE");
            GraphError::StorageError(format!("Failed to serialize edge: {}", e))
        })?;
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for add_edge");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR ADD_EDGE");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "add_edge",
                    "key": String::from_utf8_lossy(&edge_key),
                    "value": String::from_utf8_lossy(&value),
                    "cf": "edges",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft write failed for edge: {}", e);
                        println!("===> ERROR: RAFT WRITE FAILED FOR EDGE");
                        GraphError::StorageError(format!("Raft write failed: {}", e))
                    })?;
                info!("Successfully added edge via Raft");
                println!("===> SUCCESSFULLY ADDED EDGE VIA RAFT");
                return Ok(());
            }
        }

        pool_guard.insert_replicated(&edge_key, &value, self.use_raft_for_scale).await
            .map_err(|e| {
                error!("Failed to add edge: {}", e);
                println!("===> ERROR: FAILED TO ADD EDGE");
                GraphError::StorageError(format!("Failed to add edge: {}", e))
            })?;
        info!("Successfully added edge");
        println!("===> SUCCESSFULLY ADDED EDGE");
        Ok(())
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        // Get the singleton with the ZMQ client
        let singleton = ROCKSDB_DB.get()
            .ok_or_else(|| GraphError::StorageError("RocksDB singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        // Check if we have a ZMQ client
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO GET EDGE");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.get_edge(outbound_id, edge_type, inbound_id).await;
        }
        
        // Fallback to direct access
        println!("========================== USING DIRECT DB ACCESS TO GET EDGE ========================");
        drop(guard);
        
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))
            .map_err(|e| {
                error!("Failed to create edge key: {}", e);
                println!("===> ERROR: FAILED TO CREATE EDGE KEY");
                GraphError::StorageError(format!("Failed to create edge key: {}", e))
            })?;
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for get_edge");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR GET_EDGE");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "get_edge",
                    "key": String::from_utf8_lossy(&edge_key),
                    "cf": "edges"
                });
                let response = raft.client_read(request).await
                    .map_err(|e| {
                        error!("Raft read failed for edge: {}", e);
                        println!("===> ERROR: RAFT READ FAILED FOR EDGE");
                        GraphError::StorageError(format!("Raft read failed: {}", e))
                    })?;
                let value = response["value"].as_str().map(|v| v.as_bytes().to_vec());
                let edge = value.and_then(|v| deserialize_edge(&v).ok());
                info!("Retrieved edge: {:?}", edge);
                println!("===> RETRIEVED EDGE: {:?}", edge);
                return Ok(edge);
            }
        }

        let value = pool_guard.retrieve_with_failover(&edge_key).await
            .map_err(|e| {
                error!("Failed to retrieve edge: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE EDGE");
                GraphError::StorageError(format!("Failed to retrieve edge: {}", e))
            })?;
        let edge = value.and_then(|v| deserialize_edge(&v).ok());
        info!("Retrieved edge: {:?}", edge);
        println!("===> RETRIEVED EDGE: {:?}", edge);
        Ok(edge)
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        // Get the singleton with the ZMQ client
        let singleton = ROCKSDB_DB.get()
            .ok_or_else(|| GraphError::StorageError("RocksDB singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        // Check if we have a ZMQ client
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO DELETE EDGE");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.delete_edge(outbound_id, edge_type, inbound_id).await;
        }
        
        // Fallback to direct access
        println!("========================== USING DIRECT DB ACCESS TO DELETE EDGE ========================");
        drop(guard);
        
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))
            .map_err(|e| {
                error!("Failed to create edge key: {}", e);
                println!("===> ERROR: FAILED TO CREATE EDGE KEY");
                GraphError::StorageError(format!("Failed to create edge key: {}", e))
            })?;
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for delete_edge");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR DELETE_EDGE");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "delete_edge",
                    "key": String::from_utf8_lossy(&edge_key),
                    "cf": "edges",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft delete failed for edge: {}", e);
                        println!("===> ERROR: RAFT DELETE FAILED FOR EDGE");
                        GraphError::StorageError(format!("Raft delete failed: {}", e))
                    })?;
                info!("Successfully deleted edge via Raft");
                println!("===> SUCCESSFULLY DELETED EDGE VIA RAFT");
                return Ok(());
            }
        }

        pool_guard.delete_replicated(&edge_key, self.use_raft_for_scale).await
            .map_err(|e| {
                error!("Failed to delete edge: {}", e);
                println!("===> ERROR: FAILED TO DELETE EDGE");
                GraphError::StorageError(format!("Failed to delete edge: {}", e))
            })?;
        info!("Successfully deleted edge");
        println!("===> SUCCESSFULLY DELETED EDGE");
        Ok(())
    }

    /// Retrieves all vertices by iterating through all daemons in the pool.
    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for get_all_vertices");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR GET_ALL_VERTICES");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        let mut vertices = Vec::new();

        // Access the daemons HashMap directly
        let daemons_map = &pool_guard.daemons;

        for daemon in daemons_map.values() {
            let db = &daemon.db;
            let cf = db.cf_handle("vertices")
                .ok_or_else(|| {
                    error!("vertices column family not found");
                    println!("===> ERROR: VERTICES COLUMN FAMILY NOT FOUND");
                    GraphError::StorageError("vertices column family not found".to_string())
                })?;
            for result in db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
                let (_key, value) = result.map_err(|e| {
                    error!("Error iterating vertices: {}", e);
                    println!("===> ERROR: ERROR ITERATING VERTICES");
                    GraphError::StorageError(e.to_string())
                })?;
                if let Ok(vertex) = deserialize_vertex(&value) {
                    vertices.push(vertex);
                }
            }
        }
        info!("Retrieved {} vertices", vertices.len());
        println!("===> RETRIEVED {} VERTICES", vertices.len());
        Ok(vertices)
    }

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        // Get the singleton with the ZMQ client
        let singleton = ROCKSDB_DB.get()
            .ok_or_else(|| GraphError::StorageError("RocksDB singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        // Check if we have a ZMQ client
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO FETCH ALL EDGES");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.get_all_edges().await;
        }
        
        // Fallback to direct access
        println!("========================== USING DIRECT DB ACCESS TO GET ALL EDGES ========================");
        drop(guard);
        
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for get_all_edges");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR GET_ALL_EDGES");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        let mut edges = Vec::new();
        let daemons_map = &pool_guard.daemons;

        for daemon in daemons_map.values() {
            let db = &daemon.db;
            let cf = db.cf_handle("edges")
                .ok_or_else(|| {
                    error!("edges column family not found");
                    println!("===> ERROR: EDGES COLUMN FAMILY NOT FOUND");
                    GraphError::StorageError("edges column family not found".to_string())
                })?;
            for result in db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
                let (_key, value) = result.map_err(|e| {
                    error!("Error iterating edges: {}", e);
                    println!("===> ERROR: ERROR ITERATING EDGES");
                    GraphError::StorageError(e.to_string())
                })?;
                if let Ok(edge) = deserialize_edge(&value) {
                    edges.push(edge);
                }
            }
        }
        info!("Retrieved {} edges", edges.len());
        println!("===> RETRIEVED {} EDGES", edges.len());
        Ok(edges)
    }

    /// Clears all data (vertices, edges, kv_pairs) from all daemons in the pool.
    pub async fn clear_data(&self) -> GraphResult<()> {
        let cf_names = vec!["vertices", "edges", "kv_pairs"];
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for clear_data");
                println!("===> ERROR: TIMEOUT ACQUIRING ROCKSDB POOL LOCK FOR CLEAR_DATA");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        
        // Access the daemons HashMap directly
        let daemons_map = &pool_guard.daemons;

        for daemon in daemons_map.values() {
            let db = &daemon.db;
            for cf_name in &cf_names {
                let cf = db.cf_handle(cf_name)
                    .ok_or_else(|| {
                        error!("{} column family not found", cf_name);
                        println!("===> ERROR: {} COLUMN FAMILY NOT FOUND", cf_name);
                        GraphError::StorageError(format!("{} column family not found", cf_name))
                    })?;
                let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                for result in iter {
                    let (key, _) = result.map_err(|e| {
                        error!("Error iterating {}: {}", cf_name, e);
                        println!("===> ERROR: ERROR ITERATING {}", cf_name);
                        GraphError::StorageError(e.to_string())
                    })?;
                    db.delete_cf(&cf, &key)
                        .map_err(|e| {
                            error!("Failed to delete key in {}: {}", cf_name, e);
                            println!("===> ERROR: FAILED TO DELETE KEY IN {}", cf_name);
                            GraphError::StorageError(e.to_string())
                        })?;
                }
            }
        }
        info!("Successfully cleared all data");
        println!("===> SUCCESSFULLY CLEARED ALL DATA");
        Ok(())
    }

    /// Diagnoses the persistence state of the storage engine.
    pub async fn diagnose_persistence(&self) -> GraphResult<serde_json::Value> {
        // Assume `self.pool` is Mutex<RocksDBDaemonPool>
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock()).await?; 
        
        // Access the daemons HashMap directly
        let daemons_map = &pool_guard.daemons; 
        
        let db_path = daemons_map.values().next()
            .map(|daemon| daemon.db_path.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
        
        let db = daemons_map.values().next()
            .map(|daemon| daemon.db.clone())
            .ok_or_else(|| GraphError::StorageError("No daemons available for diagnostics".to_string()))?;

        let cf_vertices = db.cf_handle("vertices").ok_or_else(|| GraphError::StorageError("vertices column family not found".to_string()))?;
        let cf_edges = db.cf_handle("edges").ok_or_else(|| GraphError::StorageError("edges column family not found".to_string()))?;
        let cf_kv_pairs = db.cf_handle("kv_pairs").ok_or_else(|| GraphError::StorageError("kv_pairs column family not found".to_string()))?;

        // Note: Iterator::count() calls on RocksDB are typically synchronous operations.
        let kv_count = db.iterator_cf(&cf_kv_pairs, rocksdb::IteratorMode::Start).count();
        let vertex_count = db.iterator_cf(&cf_vertices, rocksdb::IteratorMode::Start).count();
        let edge_count = db.iterator_cf(&cf_edges, rocksdb::IteratorMode::Start).count();
        
        let disk_usage = fs::metadata(&db_path).await
            .map(|m| m.len())
            // Convert fs::metadata error into a default value to prevent failing diagnostics
            .unwrap_or(0); 

        // FIX: Correctly iterate over tokio::fs::ReadDir asynchronously.
        let mut sst_files = 0;
        match fs::read_dir(&db_path).await {
            Ok(mut entries) => {
                while let Ok(Some(entry)) = entries.next_entry().await {
                    if entry.path().extension().map_or(false, |ext| ext == "sst") {
                        sst_files += 1;
                    }
                }
            }
            Err(e) => {
                // Log the error but continue diagnostics with 0 sst files
                error!("Failed to read directory for SST file count: {}: {}", db_path.display(), e);
                sst_files = 0;
            }
        }


        let diagnostics = serde_json::json!({
            "path": db_path.to_string_lossy(),
            "kv_pairs_count": kv_count,
            "vertices_count": vertex_count,
            "edges_count": edge_count,
            "disk_usage_bytes": disk_usage,
            "sst_files_count": sst_files,
            "is_running": self.is_running().await,
        });

        info!("Persistence diagnostics: {:?}", diagnostics);
        println!("===> PERSISTENCE DIAGNOSTICS: {:?}", diagnostics);
        Ok(diagnostics)
    }

    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down RocksDBStorage");
        
        // Flush all daemons
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring pool lock".to_string()))?;
        
        // Access the daemons HashMap directly
        let daemons_map = &pool_guard.daemons;
        for daemon in daemons_map.values() {
            if let Err(e) = daemon.db.flush() {
                error!("Failed to flush RocksDB daemon at {:?}: {}", daemon.db_path, e);
            }
            daemon.shutdown().await?;
        }
        
        // Release singleton
        if let Some(db_path) = self.get_database_path().await {
            Self::release_instance(&db_path).await;
        }
        
        Ok(())
    }
}

// --- Corrected Drop Implementation ---
impl Drop for RocksDBStorage {
    fn drop(&mut self) {
        info!("Dropping RocksDBStorage instance");
        // Try to get the database path for cleanup
        let db_path_opt = if let Ok(pool) = self.pool.try_lock() {
            pool.daemons.values().next().map(|daemon| daemon.db_path.clone())
        } else {
            eprintln!("Failed to acquire lock on outer pool during path retrieval");
            None
        };
        
        // Flush all databases before dropping
        if let Ok(pool) = self.pool.try_lock() {
            for daemon in pool.daemons.values() {
                if let Err(e) = daemon.db.flush() {
                    eprintln!("Failed to flush RocksDB daemon at {:?}: {}", daemon.db_path, e);
                }
            }
        } else {
            eprintln!("Failed to acquire lock on outer pool during drop");
        }
        
        // Release singleton instance asynchronously
        if let Some(db_path) = db_path_opt {
            info!("Attempting to release instance at path: {:?}", db_path);
            match Handle::try_current() {
                Ok(handle) => {
                    info!("Using existing Tokio runtime for cleanup");
                    let path_to_move = db_path.clone();
                    handle.spawn(async move {
                        info!("Releasing instance at {:?}", path_to_move);
                        RocksDBStorage::release_instance(&path_to_move).await;
                    });
                }
                Err(_) => {
                    info!("No existing Tokio runtime; creating new runtime for cleanup");
                    match tokio::runtime::Runtime::new() {
                        Ok(rt) => {
                            rt.block_on(async {
                                info!("Releasing instance at {:?}", db_path);
                                RocksDBStorage::release_instance(&db_path).await;
                            });
                        }
                        Err(e) => {
                            eprintln!("Failed to create runtime for cleanup: {}", e);
                        }
                    }
                }
            }
        } else {
            info!("No database path found for cleanup");
        }
    }
}

#[async_trait]
impl StorageEngine for RocksDBStorage {
    async fn connect(&self) -> GraphResult<()> {
        info!("Connecting to RocksDBStorage");
        println!("===> CONNECTING TO ROCKSDB STORAGE");
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "set_key",
                    "key": String::from_utf8_lossy(&key),
                    "value": String::from_utf8_lossy(&value),
                    "cf": "kv_pairs",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft write failed for key: {}", e);
                        println!("===> ERROR: RAFT WRITE FAILED FOR KEY");
                        GraphError::StorageError(format!("Raft write failed: {}", e))
                    })?;
                info!("Successfully inserted key via Raft");
                println!("===> SUCCESSFULLY INSERTED KEY VIA RAFT");
                return Ok(());
            }
        }

        pool_guard.insert_replicated(&key, &value, self.use_raft_for_scale).await?;
        info!("Successfully inserted key");
        println!("===> SUCCESSFULLY INSERTED KEY");
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "get_key",
                    "key": String::from_utf8_lossy(key),
                    "cf": "kv_pairs"
                });
                let response = raft.client_read(request).await
                    .map_err(|e| {
                        error!("Raft read failed for key: {}", e);
                        println!("===> ERROR: RAFT READ FAILED FOR KEY");
                        GraphError::StorageError(format!("Raft read failed: {}", e))
                    })?;
                let value = response["value"].as_str().map(|v| v.as_bytes().to_vec());
                info!("Retrieved value for key: {:?}", value);
                println!("===> RETRIEVED VALUE FOR KEY: {:?}", value);
                return Ok(value);
            }
        }

        let value = pool_guard.retrieve_with_failover(key).await?;
        info!("Retrieved value for key: {:?}", value);
        println!("===> RETRIEVED VALUE FOR KEY: {:?}", value);
        Ok(value)
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
        if self.use_raft_for_scale {
            #[cfg(feature = "with-openraft-rocksdb")]
            if let Some(raft) = &self.raft {
                let request = json!({
                    "command": "delete_key",
                    "key": String::from_utf8_lossy(key),
                    "cf": "kv_pairs",
                    "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
                });
                raft.client_write(request).await
                    .map_err(|e| {
                        error!("Raft delete failed for key: {}", e);
                        println!("===> ERROR: RAFT DELETE FAILED FOR KEY");
                        GraphError::StorageError(format!("Raft delete failed: {}", e))
                    })?;
                info!("Successfully deleted key via Raft");
                println!("===> SUCCESSFULLY DELETED KEY VIA RAFT");
                return Ok(());
            }
        }

        pool_guard.delete_replicated(key, self.use_raft_for_scale).await?;
        info!("Successfully deleted key");
        println!("===> SUCCESSFULLY DELETED KEY");
        Ok(())
    }

    /// Forces a flush (writes all outstanding memtables to disk) for all RocksDB instances.
    async fn flush(&self) -> GraphResult<()> {
        // 1. Acquire lock on the outer Mutex for the pool structure.
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| {
                error!("Timeout acquiring RocksDB pool lock for flush");
                GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string())
            })?;
        // 2. Access the daemons HashMap directly (no RwLock anymore)
        let daemons_map = &pool_guard.daemons;
        // Use the HashMap to determine the path (for logging/debugging)
        let db_path = daemons_map.values().next()
            .map(|daemon| daemon.db_path.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
            
        info!("Flushing RocksDB database at {:?}", db_path);
        println!("===> FLUSHING ROCKSDB DATABASE AT {:?}", db_path);
        // Iterate over the daemons using the HashMap
        for daemon in daemons_map.values() {
            daemon.db.flush().map_err(|e| {
                error!("Failed to flush RocksDB daemon at {:?}: {}", daemon.db_path, e);
                println!("===> ERROR: FAILED TO FLUSH ROCKSDB DAEMON AT {:?}", daemon.db_path);
                GraphError::StorageError(format!("Failed to flush RocksDB daemon at {:?}: {}", daemon.db_path, e))
            })?;
        }
        info!("Flushed RocksDB database at {:?}", db_path);
        println!("===> FLUSHED ROCKSDB DATABASE AT {:?}", db_path);
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for RocksDBStorage {
    async fn start(&self) -> Result<(), GraphError> {
        info!("Starting RocksDBStorage");
        println!("===> STARTING ROCKSDB STORAGE");
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        info!("Stopping RocksDBStorage");
        println!("===> STOPPING ROCKSDB STORAGE");
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "rocksdb"
    }

    /// Checks if at least one RocksDB daemon in the pool is currently running.
    async fn is_running(&self) -> bool {
        // Try to acquire the outer Mutex lock with a timeout
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await;
        
        if let Ok(pool_guard) = pool_guard {
            // Access the daemons HashMap directly (no RwLock anymore)
            let daemons_map = &pool_guard.daemons;
            // Check the result of the inner daemon lock access.
            for daemon in daemons_map.values() {
                // Try to lock the inner running state with a short timeout.
                match timeout(TokioDuration::from_millis(100), daemon.running.lock()).await {
                    Ok(running_guard) => {
                        if *running_guard {
                            return true;
                        }
                    },
                    Err(_) => {
                        // Log an error if the inner lock is held too long, but continue checking others.
                        error!("Timeout acquiring inner daemon 'running' lock.");
                    }
                }
            }
        } else {
            error!("Timeout acquiring RocksDB pool lock during is_running check.");
        }
        
        false
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        Err(GraphError::StorageError("Query not implemented for RocksDBStorage".to_string()))
    }
    
    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        RocksDBStorage::add_vertex(self, vertex).await
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        RocksDBStorage::get_vertex(self, id).await
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        RocksDBStorage::add_vertex(self, vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        RocksDBStorage::delete_vertex(self, id).await
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        RocksDBStorage::add_edge(self, edge).await
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        RocksDBStorage::get_edge(self, outbound_id, edge_type, inbound_id).await
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        RocksDBStorage::add_edge(self, edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        RocksDBStorage::delete_edge(self, outbound_id, edge_type, inbound_id).await
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        RocksDBStorage::get_all_vertices(self).await
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        RocksDBStorage::get_all_edges(self).await
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        Err(GraphError::StorageError("Execute query not implemented for RocksDBStorage".to_string()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        // Release the singleton instance before closing
        if let Some(db_path) = self.get_database_path().await {
            Self::release_instance(&db_path).await;
        }
        
        info!("Closing RocksDBStorage");
        println!("===> CLOSING ROCKSDB STORAGE");
        Ok(())
    }
    /// Clears all data in the "vertices", "edges", and "kv_pairs" column families
    /// across all daemons in the pool.
    async fn clear_data(&self) -> Result<(), GraphError> {
        let cf_names = vec!["vertices", "edges", "kv_pairs"];
        
        // 1. Acquire the outer Mutex lock on the pool with a timeout
        let pool_guard = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
        // 2. Access the daemons HashMap directly (no RwLock anymore)
        let daemons_map = &pool_guard.daemons;
        
        // 3. Iterate over the daemons
        for daemon in daemons_map.values() {
            let db = &daemon.db;
            for cf_name in &cf_names {
                // Get column family handle - it returns Arc<BoundColumnFamily>
                let cf = db.cf_handle(cf_name)
                    .ok_or_else(|| GraphError::StorageError(format!("{} column family not found", cf_name)))?;
                
                // Use the Arc directly with iterator_cf and delete_cf
                let iter = db.iterator_cf(&cf, IteratorMode::Start);
                
                for result in iter {
                    // Extract the key (ignoring the value)
                    let (key, _) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
                    
                    // Use the Arc when calling delete_cf
                    db.delete_cf(&cf, &key)
                        .map_err(|e| GraphError::StorageError(e.to_string()))?;
                }
            }
        }
        
        info!("Successfully cleared all data");
        println!("===> SUCCESSFULLY CLEARED ALL DATA");
        Ok(())
    }

    async fn create_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let cf = self.db.cf_handle("indexes")
            .ok_or_else(|| GraphError::StorageError("indexes column family missing".into()))?;
        let key = format!("{label}:{property}");
        let meta = json!({
            "created_at": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            "type": "btree"
        });
        self.db.put_cf(&cf, key.as_bytes(), serde_json::to_vec(&meta)?)?;
        self.db.flush()?;
        info!("Created BTree index on {label}.{property}");
        Ok(())
    }

    async fn drop_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let cf = self.db.cf_handle("indexes")
            .ok_or_else(|| GraphError::StorageError("indexes column family missing".into()))?;
        let key = format!("{label}:{property}");
        self.db.delete_cf(&cf, key.as_bytes())?;
        self.db.flush()?;
        info!("Dropped index on {label}.{property}");
        Ok(())
    }

    async fn list_indexes(&self) -> GraphResult<Vec<(String, String)>> {
        let cf = self.db.cf_handle("indexes")
            .ok_or_else(|| GraphError::StorageError("indexes column family missing".into()))?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut indexes = Vec::new();
        for item in iter {
            let (key, _) = item?;
            let key_str = String::from_utf8_lossy(&key);
            let parts: Vec<&str> = key_str.splitn(2, ':').collect();
            if parts.len() == 2 {
                indexes.push((parts[0].to_string(), parts[1].to_string()));
            }
        }
        Ok(indexes)
    }

    async fn fulltext_search(&self, query: &str, limit: usize) -> GraphResult<Vec<(String, String)>> {
        FULLTEXT_INDEX.search(query, limit)
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn fulltext_rebuild(&self) -> GraphResult<()> {
        tokio::spawn({
            let index = FULLTEXT_INDEX.clone();
            async move {
                let _ = index.commit().await;
            }
        });
        Ok(())
    }

    /// Executes an index-related command by routing it to the RocksDBDaemon via ZMQ.
    ///
    /// This method calls the internal `RocksDBClient` which handles the ZMQ request/response.
    /// Executes an index-related command by routing it to a load-balanced daemon via ZMQ.
    ///
    /// This method leverages the internal daemon pool to select an available port, 
    /// ensuring no persistent port state is stored on the struct.
    async fn execute_index_command(&self, command: &str, params: Value) -> GraphResult<QueryResult> {
        info!("RocksDBStorage received index command: {}", command);

        // 1. Construct the complete JSON payload for the daemon.
        let request = json!({
            "command": command,
            "params": params, 
        });

        // 2. Use the pool (the correct component for load balancing) to select a port.
        let pool_guard = self.pool.lock().await;
        
        // Await the async function 'select_daemon()' and use ok_or_else 
        // to handle the resulting Option<u16>.
        let port_option = pool_guard.select_daemon().await;

        let port_number = port_option
            .ok_or_else(|| {
                error!("Failed to select daemon port from pool: Daemon pool is empty or unavailable.");
                GraphError::StorageError("Daemon pool selection error: No available port.".to_string())
            })?;
        
        drop(pool_guard); // Release the lock immediately after selection

        // 3. Delegate the ZMQ communication using the selected port number 
        //    via the static one-shot SledClient function.
        let response_value = RocksDBClient::execute_one_shot_zmq_request(port_number, request).await
            .map_err(|e| {
                error!("ZMQ index command execution failed for {}: {:?}", command, e);
                e
            })?;

        // 4. Deserialize the JSON response (Value) into the expected QueryResult struct.
        match response_value.get("result") {
            Some(result_val) => {
                let query_result: QueryResult = serde_json::from_value(result_val.clone())
                    .map_err(|e| {
                        error!("Failed to deserialize query result: {}", e);
                        GraphError::DeserializationError(format!("Failed to parse QueryResult: {}", e))
                    })?;
                
                info!("Successfully executed index command: {}", command);
                Ok(query_result)
            },
            None => {
                // If the response doesn't contain a 'result' field, check for an 'error' field
                if let Some(error_msg) = response_value.get("error").and_then(|e| e.as_str()) {
                     error!("Daemon execution error for {}: {}", command, error_msg);
                     Err(GraphError::StorageError(format!("Daemon error: {}", error_msg)))
                } else {
                    error!("Unexpected response format from daemon for {}: {:?}", command, response_value);
                    Err(GraphError::StorageError(format!("Unexpected daemon response for {}: {:?}", command, response_value)))
                }
            }
        }
    }
}

#[tokio::test]
async fn test_directory_creation_retry() {
    let config = RocksDBConfig {
        port: Some(8051),
        ..Default::default()
    };
    let storage_config = StorageConfig {
        data_directory: Some(PathBuf::from("/tmp/test_graphdb")),
        config_root_directory: Some("/tmp/test_graphdb/config".to_string()),
        ..Default::default()
    };
    // Simulate a directory that can't be created initially
    let db_path = PathBuf::from("/tmp/test_graphdb/rocksdb/8051");
    fs::create_dir_all(&db_path).await.unwrap();
    fs::set_permissions(&db_path, fs::Permissions::from_mode(0o444)).await.unwrap(); // Make read-only
    let result = RocksDBStorage::new(&config, &storage_config).await;
    assert!(result.is_err());
    fs::set_permissions(&db_path, fs::Permissions::from_mode(0o755)).await.unwrap(); // Restore permissions
    fs::remove_dir_all(&db_path).await.unwrap();
    let result = RocksDBStorage::new(&config, &storage_config).await;
    assert!(result.is_ok());
}

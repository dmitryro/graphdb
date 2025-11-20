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
use crate::storage_engine::sled_storage_daemon_pool::{ FULLTEXT_INDEX };
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
use uuid::Uuid;
use async_trait::async_trait;
use crate::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine, get_global_storage_registry};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::db_daemon_registry::{ GLOBAL_DB_DAEMON_REGISTRY, DBDaemonMetadata };
use crate::daemon::daemon_utils::{is_storage_daemon_running, parse_cluster_range};
use crate::daemon::daemon_management::{ check_pid_validity, find_pid_by_port, stop_process_by_pid, get_ipc_endpoint, extract_port };
use serde_json::{ Value, json };
use std::any::Any;
use futures::future::join_all;
use tokio::time::{timeout, sleep, Duration as TokioDuration};
use crate::storage_engine::sled_client::{ SledClient, ZmqSocketWrapper };
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use sled::Config;

pub static SLED_DB: LazyLock<OnceCell<TokioMutex<SledDbWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static SLED_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<SledDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());
pub static SLED_ACTIVE_DATABASES: LazyLock<OnceCell<TokioMutex<HashSet<PathBuf>>>> = LazyLock::new(|| OnceCell::new());

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

        let ipc_path = get_ipc_endpoint(port);
        let socket_path = &ipc_path[6..];
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

        Ok(())
    }

     pub async fn new(
        config: &SledConfig,
        storage_config: &StorageConfig,
    ) -> Result<SledStorage, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with config at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH PORT {:?}", config.port);

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let mut active_port: Option<u16> = None;
        if let Ok(daemons) = daemon_registry.list_storage_daemons().await {
            for meta in daemons {
                if meta.service_type == "storage"
                    && meta.engine_type.as_deref() == Some("sled")
                    && meta.pid > 0
                    && check_pid_validity(meta.pid).await
                    && meta.zmq_ready
                    && Path::new(&format!("/tmp/graphdb-{}.ipc", meta.port)).exists()
                {
                    info!("FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    println!("===> FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    active_port = Some(meta.port);
                    break;
                }
            }
        }

        let port = active_port.unwrap_or_else(|| config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Using Sled path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build.".to_string(),
            ));
        }

        // === 1. Check global registry first ===
        {
            let registry = get_global_storage_registry().await;
            let registry_guard = registry.read().await;
            if registry_guard.contains_key(&port) {
                info!("SledStorage already initialized for port {} — reusing from global registry", port);
                println!("===> REUSING SledStorage FROM GLOBAL REGISTRY FOR PORT {}", port);
                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                if let Some(pool) = pool_map_guard.get(&port) {
                    // We need to get the actual db from the daemon pool
                    let pool_guard = pool.lock().await;
                    let daemon = pool_guard.daemons.values().next()
                        .ok_or(GraphError::StorageError("No daemon in pool".into()))?;
                    let db = daemon.db.clone();
                    return Ok(SledStorage { pool: pool.clone(), db });
                }
            }
        }

        if let Some(meta) = daemon_registry.get_daemon_metadata(port).await? {
            if meta.data_dir == Some(db_path.clone())
                && meta.engine_type == Some("sled".to_string())
                && meta.pid > 0
                && check_pid_validity(meta.pid).await
                && meta.zmq_ready
            {
                info!("Reusing healthy Sled daemon from registry: PID {} on port {}", meta.pid, port);
                println!("===> REUSING HEALTHY SLED DAEMON FROM REGISTRY: PID {} ON PORT {}", meta.pid, port);

                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    let mut pool_guard = timeout(TokioDuration::from_secs(5), existing_pool.lock()).await?;
                    pool_guard.load_balancer.update_node_health(port, true, 0).await;
                    println!("===> [LB] NODE {} MARKED HEALTHY IN SLED LOAD BALANCER (REUSING)", port);

                    let daemon = pool_guard.daemons.values().next()
                        .ok_or(GraphError::StorageError("No daemon in pool".into()))?;
                    let db = daemon.db.clone();

                    let storage = SledStorage { pool: existing_pool.clone(), db };
                    let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;

                    let registry = get_global_storage_registry().await;
                    let mut registry_guard = registry.write().await;
                    registry_guard.insert(port, storage_arc);

                    return Ok(storage);
                }
            }
        }

        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        let pool = {
            let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
            if let Some(existing_pool) = pool_map_guard.get(&port) {
                existing_pool.clone()
            } else {
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        {
            let mut pool_guard = timeout(TokioDuration::from_secs(10), pool.lock()).await?;
            info!("Initializing cluster");
            println!("===> INITIALIZING CLUSTER");
            pool_guard.initialize_cluster(storage_config, config, Some(port)).await?;
            info!("Initialized cluster on port {}", port);
            println!("===> INITIALIZED CLUSTER ON PORT {}", port);
        }

        // Get the actual db from the daemon pool
        let pool_guard = pool.lock().await;
        let daemon = pool_guard.daemons.values().next()
            .ok_or(GraphError::StorageError("No daemon in pool after init".into()))?;
        let db = daemon.db.clone();

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

        let client_tuple = {
            const MAX_RETRIES: usize = 20;
            let mut client_tuple_opt: Option<(SledClient, Arc<TokioMutex<ZmqSocketWrapper>>)> = None;
            info!("Waiting for ZMQ IPC readiness at {}...", get_ipc_endpoint(port));
            println!("===> WAITING FOR ZMQ IPC READINESS AT {}...", get_ipc_endpoint(port));
            for i in 0..MAX_RETRIES {
                match SledClient::connect_zmq_client_with_readiness_check(port).await {
                    Ok(c_tuple) => {
                        info!("ZMQ client connected successfully after {} retries.", i);
                        println!("===> ZMQ CLIENT CONNECTED SUCCESSFULLY AFTER {} RETRIES", i);
                        client_tuple_opt = Some(c_tuple);
                        break;
                    }
                    Err(e) => {
                        warn!("Attempt {}/{} to connect ZMQ client failed: {}", i + 1, MAX_RETRIES, e);
                        println!("===> WARNING: ATTEMPT {}/{} TO CONNECT ZMQ CLIENT FAILED: {}", i + 1, MAX_RETRIES, e);
                        if i < MAX_RETRIES - 1 {
                            sleep(TokioDuration::from_millis(100 * (i + 1) as u64)).await;
                        }
                    }
                }
            }
            client_tuple_opt.ok_or_else(|| {
                error!("Failed to connect ZMQ client after {} retries", MAX_RETRIES);
                GraphError::StorageError("Failed to connect ZMQ client".to_string())
            })?
        };

        {
            let mut sled_db_guard = sled_db_instance.lock().await;
            sled_db_guard.client = Some(client_tuple);
        }

        let storage = SledStorage { pool: pool.clone(), db };
        let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;

        let registry = get_global_storage_registry().await;
        let mut registry_guard = registry.write().await;
        registry_guard.insert(port, storage_arc);

        info!("Successfully initialized and registered SledStorage in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED AND REGISTERED SledStorage IN {}ms", start_time.elapsed().as_millis());
        Ok(storage)
    }

    pub async fn new_with_db(
        config: &SledConfig,
        storage_config: &StorageConfig,
        existing_db: Arc<sled::Db>,
    ) -> Result<SledStorage, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with existing database at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH EXISTING DB WITH PORT {:?}", config.port);

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let mut active_port: Option<u16> = None;
        if let Ok(daemons) = daemon_registry.list_storage_daemons().await {
            for meta in daemons {
                if meta.service_type == "storage"
                    && meta.engine_type.as_deref() == Some("sled")
                    && meta.pid > 0
                    && check_pid_validity(meta.pid).await
                    && meta.zmq_ready
                    && Path::new(&format!("/tmp/graphdb-{}.ipc", meta.port)).exists()
                {
                    info!("FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    println!("===> FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    active_port = Some(meta.port);
                    break;
                }
            }
        }

        let port = active_port.unwrap_or_else(|| config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Using Sled path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build.".to_string(),
            ));
        }

        // === 1. Check global registry first ===
        {
            let registry = get_global_storage_registry().await;
            let registry_guard = registry.read().await;
            if registry_guard.contains_key(&port) {
                info!("SledStorage already initialized for port {} — reusing from global registry", port);
                println!("===> REUSING SledStorage FROM GLOBAL REGISTRY FOR PORT {}", port);
                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                if let Some(pool) = pool_map_guard.get(&port) {
                    return Ok(SledStorage { pool: pool.clone(), db: existing_db });
                }
            }
        }

        if let Some(meta) = daemon_registry.get_daemon_metadata(port).await? {
            if meta.data_dir == Some(db_path.clone())
                && meta.engine_type == Some("sled".to_string())
                && meta.pid > 0
                && check_pid_validity(meta.pid).await
                && meta.zmq_ready
            {
                info!("Reusing healthy Sled daemon from registry: PID {} on port {}", meta.pid, port);
                println!("===> REUSING HEALTHY SLED DAEMON FROM REGISTRY: PID {} ON PORT {}", meta.pid, port);

                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    let mut pool_guard = timeout(TokioDuration::from_secs(5), existing_pool.lock()).await?;
                    pool_guard.load_balancer.update_node_health(port, true, 0).await;
                    println!("===> [LB] NODE {} MARKED HEALTHY IN SLED LOAD BALANCER (REUSING WITH EXISTING DB)", port);

                    let storage = SledStorage { pool: existing_pool.clone(), db: existing_db };
                    let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;

                    let registry = get_global_storage_registry().await;
                    let mut registry_guard = registry.write().await;
                    registry_guard.insert(port, storage_arc);

                    return Ok(storage);
                }
            }
        }

        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        let pool = {
            let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
            if let Some(existing_pool) = pool_map_guard.get(&port) {
                existing_pool.clone()
            } else {
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        {
            let mut pool_guard = timeout(TokioDuration::from_secs(10), pool.lock()).await?;
            pool_guard
                .initialize_cluster_with_db(storage_config, config, Some(port), existing_db.clone())
                .await?;
        }

        let storage = SledStorage { pool: pool.clone(), db: existing_db };
        let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;

        let registry = get_global_storage_registry().await;
        let mut registry_guard = registry.write().await;
        registry_guard.insert(port, storage_arc);

        info!("Successfully initialized and registered SledStorage with existing DB in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED AND REGISTERED SledStorage WITH EXISTING DB IN {}ms", start_time.elapsed().as_millis());
        Ok(storage)
    }

    pub fn new_pinned(config: &SledConfig, storage_config: &StorageConfig) -> Box<dyn futures::Future<Output = Result<Self, GraphError>> + Send + 'static> {
        let config = config.clone();
        let storage_config = storage_config.clone();
        Box::new(async move {
            SledStorage::new(&config, &storage_config).await
        })
    }

    // Internal implementation methods (same pattern as RocksDBStorage)
    pub async fn add_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO ADD VERTEX");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.create_vertex(vertex).await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO ADD VERTEX ========================");
        drop(guard);
        
        // Fallback to direct access
        self.create_vertex_direct(vertex).await
    }

    pub async fn get_vertex_internal(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO GET VERTEX");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.get_vertex(id).await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO GET VERTEX ========================");
        drop(guard);
        
        self.get_vertex_direct(id).await
    }

    pub async fn delete_vertex_internal(&self, id: &Uuid) -> GraphResult<()> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO DELETE VERTEX");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.delete_vertex(id).await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO DELETE VERTEX ========================");
        drop(guard);
        
        self.delete_vertex_direct(id).await
    }

    pub async fn add_edge(&self, edge: Edge) -> GraphResult<()> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO ADD EDGE");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.create_edge(edge).await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO ADD EDGE ========================");
        drop(guard);
        
        self.create_edge_direct(edge).await
    }

    pub async fn get_edge_internal(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO GET EDGE");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.get_edge(outbound_id, edge_type, inbound_id).await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO GET EDGE ========================");
        drop(guard);
        
        self.get_edge_direct(outbound_id, edge_type, inbound_id).await
    }

    pub async fn delete_edge_internal(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO DELETE EDGE");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.delete_edge(outbound_id, edge_type, inbound_id).await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO DELETE EDGE ========================");
        drop(guard);
        
        self.delete_edge_direct(outbound_id, edge_type, inbound_id).await
    }

    pub async fn get_all_vertices_internal(&self) -> GraphResult<Vec<Vertex>> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO FETCH ALL VERTICES");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.get_all_vertices().await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO GET ALL VERTICES ========================");
        drop(guard);
        
        self.get_all_vertices_direct().await
    }

    pub async fn get_all_edges_internal(&self) -> GraphResult<Vec<Edge>> {
        let singleton = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
        
        let guard = singleton.lock().await;
        
        if let Some((client, _socket)) = &guard.client {
            println!("===> USING ZMQ CLIENT TO FETCH ALL EDGES");
            let client_clone = client.clone();
            drop(guard);
            return client_clone.get_all_edges().await;
        }
        
        println!("========================== USING DIRECT DB ACCESS TO GET ALL EDGES ========================");
        drop(guard);
        
        self.get_all_edges_direct().await
    }

    // Direct database access methods (fallback)
    async fn create_vertex_direct(&self, vertex: Vertex) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Creating vertex at path {:?}", db_path);
        println!("===> CREATING VERTEX IN SLED DATABASE AT {:?}", db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.insert(vertex.id.0.as_bytes(), serialize_vertex(&vertex)?)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        db_lock.db.flush_async().await.map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex_direct(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let vertex = vertices
            .get(SerializableUuid(*id).0.as_bytes())
            .map_err(|e| GraphError::StorageError(e.to_string()))?
            .map(|v| deserialize_vertex(&*v))
            .transpose()?;
        Ok(vertex)
    }

    async fn delete_vertex_direct(&self, id: &Uuid) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.remove(SerializableUuid(*id).0.as_bytes())
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        db_lock.db.flush_async().await.map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn create_edge_direct(&self, edge: Edge) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(edge.outbound_id.0), &edge.edge_type, &SerializableUuid(edge.inbound_id.0))?;
        edges.insert(&edge_key, serialize_edge(&edge)?)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        db_lock.db.flush_async().await.map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge_direct(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        let edge = edges
            .get(&edge_key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?
            .map(|v| deserialize_edge(&*v))
            .transpose()?;
        Ok(edge)
    }

    async fn delete_edge_direct(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        edges.remove(&edge_key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        db_lock.db.flush_async().await.map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices_direct(&self) -> GraphResult<Vec<Vertex>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let mut vertex_vec = Vec::new();
        for result in vertices.iter() {
            let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
            vertex_vec.push(deserialize_vertex(&*v)?);
        }
        Ok(vertex_vec)
    }

    async fn get_all_edges_direct(&self) -> GraphResult<Vec<Edge>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let mut edge_vec = Vec::new();
        for result in edges.iter() {
            let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
            edge_vec.push(deserialize_edge(&*v)?);
        }
        Ok(edge_vec)
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

        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

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

        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to recreate Sled database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO RECREATE SLED DATABASE DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to recreate Sled database directory at {:?}: {}", db_path, e))
            })?;

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
        info!("SledStorage::insert - inserting key into kv_pairs at {:?}", db_path);
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

        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::retrieve - retrieving key from kv_pairs at {:?}", db_path);
        println!("===> RETRIEVING KEY FROM SLED DATABASE AT {:?}", db_path);

        let value_opt = db_lock.db
            .get(key)
            .map_err(|e| {
                error!("Failed to retrieve key: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE KEY FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        Ok(value_opt.map(|v| v.to_vec()))
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::delete - deleting key from kv_pairs at {:?}", db_path);
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
        SledStorage::add_vertex(self, vertex).await
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        SledStorage::get_vertex_internal(self, id).await
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        SledStorage::add_vertex(self, vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        SledStorage::delete_vertex_internal(self, id).await
    }
    
    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        SledStorage::add_edge(self, edge).await
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        SledStorage::get_edge_internal(self, outbound_id, edge_type, inbound_id).await
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        SledStorage::add_edge(self, edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        SledStorage::delete_edge_internal(self, outbound_id, edge_type, inbound_id).await
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        SledStorage::get_all_vertices_internal(self).await
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        SledStorage::get_all_edges_internal(self).await
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

    async fn execute_query(&self, _query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON SLED STORAGE (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
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

    async fn create_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let indexes_tree = self.db.open_tree("indexes")?;
        let key = format!("{label}:{property}");
        let meta = json!({
            "created_at": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            "type": "btree"
        });
        indexes_tree.insert(key.as_bytes(), serde_json::to_vec(&meta)?)?;
        indexes_tree.flush_async().await?;
        info!("Created BTree index on {label}.{property}");
        Ok(())
    }

    async fn drop_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let indexes_tree = self.db.open_tree("indexes")?;
        let key = format!("{label}:{property}");
        indexes_tree.remove(key.as_bytes())?;
        indexes_tree.flush_async().await?;
        info!("Dropped index on {label}.{property}");
        Ok(())
    }

    async fn list_indexes(&self) -> GraphResult<Vec<(String, String)>> {
        let indexes_tree = self.db.open_tree("indexes")?;
        let mut indexes = Vec::new();
        for result in indexes_tree.iter() {
            let (key, _) = result?;
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

                // *** SLED_DB SECTION: FLUSH, CLOSE ZMQ, RELEASE ACTIVE DB TRACKER ***
                if let Some(sled_db) = SLED_DB.get() {
                    let mut sled_db_guard = match timeout(TokioDuration::from_secs(5), sled_db.lock()).await {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!("Timeout acquiring SLED_DB lock during Drop for port {}", port_u16);
                            println!("===> ERROR: TIMEOUT ACQUIRING SLED_DB LOCK DURING DROP FOR PORT {}", port_u16);
                            return;
                        }
                    };
                    let db_path = sled_db_guard.path.clone();
                    info!("Flushing Sled database at {:?}", db_path);
                    println!("===> FLUSHING SLED DATABASE AT {:?}", db_path);
                    if let Err(e) = timeout(TokioDuration::from_secs(10), sled_db_guard.db.flush_async()).await {
                        error!("Failed to flush Sled database at {:?}: {:?}", db_path, e);
                        println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AT {:?}: {:?}", db_path, e);
                    }
                    if let Some((_, zmq_socket)) = sled_db_guard.client.take() {
                        info!("Closing ZMQ client for path {:?}", db_path);
                        println!("===> CLOSING ZMQ CLIENT FOR PATH {:?}", db_path);
                        drop(zmq_socket);
                    }
                    // Release from active databases tracker
                    Self::release_instance(&db_path).await;
                }
                // *** END SLED_DB SECTION ***

                // Remove from SLED_POOL_MAP
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

                // *** REMOVED: DO NOT CALL check_and_cleanup_stale_daemon HERE ***
                // This function removes entries from GLOBAL_DAEMON_REGISTRY even during normal shutdown.
                // The daemon is already stopped via `pool.close(None)` earlier.
                // Registry cleanup must be done by the daemon itself or via explicit command.

                // Optional: Log that daemon should have cleaned up registry
                info!("SledStorage drop complete for port {}. Daemon should have removed itself from GLOBAL_DAEMON_REGISTRY.", port_u16);
                println!("===> SLED STORAGE DROP COMPLETE FOR PORT {}. DAEMON SHOULD SELF-REMOVE FROM REGISTRY.", port_u16);
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

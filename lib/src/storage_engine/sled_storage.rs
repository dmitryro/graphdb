use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use tokio::fs;
use tokio::sync::{OnceCell, Mutex as TokioMutex};
use log::{info, debug, warn, error};
pub use crate::config::{
    SledDbWithPath, SledConfig, SledStorage, SledDaemonPool, load_storage_config_from_yaml, 
    DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, StorageConfig, StorageEngineType
};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
use uuid::Uuid;
use async_trait::async_trait;
use crate::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::daemon_utils::{is_storage_daemon_running, parse_cluster_range};
use serde_json::Value;
use std::any::Any;
use futures::future::join_all;
use tokio::time::{timeout, Duration};
use std::fs as std_fs;

// Static OnceCell to hold the single Sled database instance with its path
pub static SLED_DB: LazyLock<OnceCell<TokioMutex<SledDbWithPath>>> = LazyLock::new(|| OnceCell::new());

// Static map to hold SledDaemonPool instances by port
pub static SLED_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<SledDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());

impl SledStorage {
    /// Initializes a new Sled storage instance.
    /// This function constructs a unique path for the database based on the provided port number.
    pub async fn new(config: &SledConfig, storage_config: &StorageConfig) -> Result<Self, GraphError> {
        info!("Initializing SledStorage with config: {:?}", config);
        println!("===> INITIALIZING SLED DAEMON WITH PATH {:?}", config.path);

        // Get the base data directory from storage_config
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&default_data_dir);
        
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        
        // Construct proper path: base_data_dir/sled/port
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Using database path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        // Ensure base directory exists and is writable
        if !db_path.exists() {
            info!("Creating database directory at {:?}", db_path);
            println!("===> CREATING SLED DIRECTORY AT {:?}", db_path);
            fs::create_dir_all(&db_path)
                .await
                .map_err(|e| {
                    error!("Failed to create database directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO CREATE SLED DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
                })?;
        } else if !db_path.is_dir() {
            error!("Path {:?} exists but is not a directory", db_path);
            println!("===> ERROR: PATH {:?} EXISTS BUT IS NOT A DIRECTORY", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        // Check permissions - Fixed logic
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
        info!("Directory at {:?} is writable", db_path);

        let lock_path = db_path.join("db.lck");
        if lock_path.exists() {
            warn!("Found stale lock file at {:?}", lock_path);
            println!("===> FOUND STALE LOCK FILE AT {:?}", lock_path);
            if let Err(e) = fs::remove_file(&lock_path).await {
                error!("Failed to remove stale lock file at {:?}: {}", lock_path, e);
                println!("===> ERROR: FAILED TO REMOVE STALE LOCK FILE AT {:?}", lock_path);
                return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}: {}", lock_path, e)));
            }
            info!("Successfully removed stale lock file at {:?}", lock_path);
            println!("===> SUCCESSFULLY REMOVED STALE LOCK FILE AT {:?}", lock_path);
        }

        let sled_db = SLED_DB.get_or_try_init(|| async {
            info!("Opening Sled database at {:?}", db_path);
            println!("===> ATTEMPTING TO OPEN SLED DB AT {:?}", db_path);
            let db = sled::Config::new()
                .path(&db_path)
                .use_compression(config.use_compression)
                .cache_capacity(config.cache_capacity.unwrap_or(1024 * 1024 * 1024))
                .open()
                .map_err(|e| {
                    error!("Failed to open Sled database at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO OPEN SLED DB AT {:?}", db_path);
                    GraphError::StorageError(format!(
                        "Failed to open Sled database at {:?}: {}. Ensure the directory is accessible.",
                        db_path, e
                    ))
                })?;
            info!("Successfully opened Sled database at {:?}", db_path);
            println!("===> SUCCESSFULLY OPENED SLED DATABASE AT {:?}", db_path);
            Ok::<_, GraphError>(TokioMutex::new(SledDbWithPath { db: Arc::new(db), path: db_path.clone() }))
        }).await?;
        info!("Successfully initialized or accessed Sled database at {:?}", db_path);
        println!("===> SUCCESSFULLY INITIALIZED SLED DATABASE AT {:?}", db_path);

        let pool_map = SLED_POOL_MAP.get_or_init(|| async {
            TokioMutex::new(std::collections::HashMap::<u16, Arc<TokioMutex<SledDaemonPool>>>::new())
        }).await;

        if let Some(metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
            if let Some(registered_path) = &metadata.data_dir {
                if registered_path != &db_path {
                    warn!("Path mismatch: daemon registry shows {:?}, but config specifies {:?}", registered_path, db_path);
                    println!("in SledStorage new ===> PATH MISMATCH: DAEMON REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", registered_path, db_path);
                }
                
                let pool_map_guard = pool_map.lock().await;
                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing SledDaemonPool for port {}", port);
                    println!("===> REUSING EXISTING DAEMON ON PORT {} WITH MATCHING PATH {:?}", port, db_path);
                    let mut pool_guard = existing_pool.lock().await;
                    if pool_guard.daemons.is_empty() {
                        info!("Existing pool for port {} is uninitialized, initializing cluster with existing DB", port);
                        println!("===> INITIALIZING CLUSTER ON PORT {} WITH EXISTING DB", port);
                        // FIX: Use the already-opened database instead of trying to open again
                        let sled_db_guard = sled_db.lock().await;
                        (*pool_guard).initialize_cluster_with_db(storage_config, config, Some(port), sled_db_guard.db.clone()).await?;
                    }
                    return Ok(Self {
                        pool: existing_pool.clone(),
                    });
                }
            }
        }

        let pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
        {
            let mut pool_guard = pool.lock().await;
            info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            
            // FIX: Use the already-opened database instead of trying to open again
            let sled_db_guard = sled_db.lock().await;
            (*pool_guard).initialize_cluster_with_db(storage_config, config, Some(port), sled_db_guard.db.clone()).await?;
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        let mut pool_map_guard = pool_map.lock().await;
        pool_map_guard.insert(port, pool.clone());

        Ok(Self { pool })
    }
    
    pub async fn new_with_db(config: &SledConfig, storage_config: &StorageConfig, existing_db: Arc<sled::Db>) -> Result<Self, GraphError> {
        info!("Initializing SledStorage with existing database at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH EXISTING DB AT {:?}", config.path);

        // Get the base data directory from storage_config
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&default_data_dir);
        
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        
        // Construct proper path: base_data_dir/sled/port
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Using database path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        // Ensure base directory exists and is writable
        if !db_path.exists() {
            info!("Creating database directory at {:?}", db_path);
            println!("===> CREATING SLED DIRECTORY AT {:?}", db_path);
            fs::create_dir_all(&db_path)
                .await
                .map_err(|e| {
                    error!("Failed to create database directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO CREATE SLED DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
                })?;
        } else if !db_path.is_dir() {
            error!("Path {:?} exists but is not a directory", db_path);
            println!("===> ERROR: PATH {:?} EXISTS BUT IS NOT A DIRECTORY", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        // Check permissions - Fixed logic
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
        info!("Directory at {:?} is writable", db_path);

        // Update SLED_DB singleton with the provided database
        let sled_db = SLED_DB.get_or_try_init(|| async {
            info!("Storing provided Sled database in singleton at {:?}", db_path);
            println!("===> STORING PROVIDED SLED DB IN SINGLETON AT {:?}", db_path);
            Ok::<_, GraphError>(TokioMutex::new(SledDbWithPath { db: existing_db.clone(), path: db_path.clone() }))
        }).await?;
        {
            let mut sled_db_guard = sled_db.lock().await;
            sled_db_guard.db = existing_db.clone();
            sled_db_guard.path = db_path.clone();
        }
        info!("Successfully updated Sled database singleton at {:?}", db_path);
        println!("===> SUCCESSFULLY UPDATED SLED DATABASE SINGLETON AT {:?}", db_path);

        let pool_map = SLED_POOL_MAP.get_or_init(|| async {
            TokioMutex::new(HashMap::<u16, Arc<TokioMutex<SledDaemonPool>>>::new())
        }).await;

        if let Some(metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
            if let Some(registered_path) = &metadata.data_dir {
                if registered_path != &db_path {
                    warn!("Path mismatch: daemon registry shows {:?}, but config specifies {:?}", registered_path, db_path);
                    println!("in SledStorage new_with_db ===> PATH MISMATCH: DAEMON REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", registered_path, db_path);
                }
                
                let pool_map_guard = pool_map.lock().await;
                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Reusing existing SledDaemonPool for port {}", port);
                    println!("===> REUSING EXISTING DAEMON ON PORT {} WITH MATCHING PATH {:?}", port, db_path);
                    let mut pool_guard = existing_pool.lock().await;
                    if pool_guard.daemons.is_empty() {
                        info!("Existing pool for port {} is uninitialized, initializing cluster with existing DB", port);
                        println!("===> INITIALIZING CLUSTER ON PORT {} WITH EXISTING DB", port);
                        pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), existing_db.clone()).await?;
                    }
                    return Ok(Self {
                        pool: existing_pool.clone(),
                    });
                }
            }
        }

        // FIX: Create pool with existing database instead of trying to open new one
        let pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
        {
            let mut pool_guard = pool.lock().await;
            info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), existing_db).await?;
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }
        let mut pool_map_guard = pool_map.lock().await;
        pool_map_guard.insert(port, pool.clone());

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
        let db_lock = db.lock().await;
        debug!("Setting key '{}' to value '{}' in Sled database at {:?}", key, value, db_lock.path);
        db_lock.db
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| {
                error!("Failed to set key '{}': {}", key, e);
                GraphError::StorageError(format!("Failed to set key '{}': {}", key, e))
            })?;
        db_lock.db.flush_async().await.map_err(|e| {
            error!("Failed to flush Sled database after setting key '{}': {}", key, e);
            GraphError::StorageError(format!("Failed to flush Sled database after setting key '{}': {}", key, e))
        })?;
        debug!("Successfully set key '{}' and flushed database", key);
        Ok(())
    }

    pub async fn get_key(&self, key: &str) -> GraphResult<Option<String>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        debug!("Retrieving key '{}' from Sled database at {:?}", key, db_lock.path);
        let value = db_lock.db
            .get(key.as_bytes())
            .map_err(|e| {
                error!("Failed to get key '{}': {}", key, e);
                GraphError::StorageError(format!("Failed to get key '{}': {}", key, e))
            })?
            .map(|v| String::from_utf8_lossy(&*v).to_string());
        debug!("Retrieved value for key '{}': {:?}", key, value);
        Ok(value)
    }

    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        info!("Attempting to force unlock Sled database at {:?}", path);
        let lock_path = path.join("db.lck");

        debug!("Checking for lock file at {:?}", lock_path);
        if lock_path.exists() {
            warn!("Found potential lock file at {:?}", lock_path);
            println!("===> FOUND LOCK FILE AT {:?}", lock_path);
            #[cfg(unix)]
            {
                const MAX_RETRIES: u32 = 3;
                const BASE_DELAY_MS: u64 = 1000;
                let mut attempt = 0;

                while attempt < MAX_RETRIES {
                    if let Err(e) = timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                        warn!("Failed to remove lock file at {:?} on attempt {}: {:?}", lock_path, attempt + 1, e);
                        println!("===> ERROR: FAILED TO REMOVE LOCK FILE AT {:?}", lock_path);
                        attempt += 1;
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                        continue;
                    }
                    info!("Successfully removed lock file at {:?}", lock_path);
                    println!("===> SUCCESSFULLY REMOVED LOCK FILE AT {:?}", lock_path);
                    break;
                }

                if attempt >= MAX_RETRIES {
                    error!("Failed to unlock {:?} after {} attempts", lock_path, MAX_RETRIES);
                    println!("===> ERROR: FAILED TO UNLOCK SLED DATABASE AT {:?}", lock_path);
                    return Err(GraphError::StorageError(format!("Failed to unlock {:?} after {} attempts", lock_path, MAX_RETRIES)));
                }
            }
            #[cfg(not(unix))]
            {
                if let Err(e) = timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                    error!("Failed to remove lock file at {:?}: {:?}", lock_path, e);
                    println!("===> ERROR: FAILED TO REMOVE LOCK FILE AT {:?}", lock_path);
                    return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}", lock_path)));
                }
                info!("Successfully removed lock file at {:?}", lock_path);
                println!("===> SUCCESSFULLY REMOVED LOCK FILE AT {:?}", lock_path);
            }
        } else {
            info!("No lock file found at {:?}", lock_path);
            println!("===> NO LOCK FILE FOUND AT {:?}", lock_path);
        }
        Ok(())
    }

    pub async fn force_reset(config: &SledConfig) -> GraphResult<Self> {
        warn!("FORCE RESET: Completely destroying and recreating database at {:?}", config.path);
        println!("===> FORCE RESET: DESTROYING DATABASE AT {:?}", config.path);
        let db_path = config.path.clone();
        if db_path.exists() {
            if let Err(e) = timeout(Duration::from_secs(10), fs::remove_dir_all(&db_path)).await {
                error!("Timeout or error removing database directory at {:?}: {:?}", db_path, e);
                println!("===> ERROR: FAILED TO REMOVE DATABASE DIRECTORY AT {:?}", db_path);
                return Err(GraphError::StorageError(format!("Failed to remove database directory: {:?}", e)));
            }
            info!("Successfully removed database directory at {:?}", db_path);
            println!("===> SUCCESSFULLY REMOVED DATABASE DIRECTORY AT {:?}", db_path);
        }
        SledStorage::new(config, &StorageConfig::default()).await
            .map_err(|e| {
                error!("Failed to initialize SledStorage after reset: {}", e);
                println!("===> ERROR: FAILED TO INITIALIZE SLED STORAGE AFTER RESET");
                GraphError::StorageError(format!("Failed to initialize SledStorage after reset: {}", e))
            })
    }

    pub async fn diagnose_persistence(&self) -> GraphResult<serde_json::Value> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Diagnosing persistence for SledStorage at {:?}", db_path);

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
            "is_running": true,
        });

        info!("Persistence diagnostics: {:?}", diagnostics);
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
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("SledStorage::insert - inserting key {:?} into kv_pairs at {:?}", key, db_path);
        debug!("Key (utf8 if possible): {:?}", String::from_utf8_lossy(&key));

        db_lock.db
            .insert(&key, &*value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("SledStorage::insert - flushed {} bytes after insert at {:?}", bytes_flushed, db_path);

        let keys: Vec<_> = db_lock.db
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("SledStorage::insert - current kv_pairs keys at {:?}: {:?}", db_path, keys);

        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("SledStorage::retrieve - retrieving key {:?} from kv_pairs at {:?}", key, db_path);
        debug!("Key (utf8 if possible): {:?}", String::from_utf8_lossy(&key));

        let value_opt = db_lock.db
            .get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let keys: Vec<_> = db_lock.db
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("SledStorage::retrieve - current kv_pairs keys at {:?}: {:?}", db_path, keys);

        Ok(value_opt.map(|v| v.to_vec()))
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("SledStorage::delete - deleting key {:?} from kv_pairs at {:?}", key, db_path);

        db_lock.db
            .remove(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("SledStorage::delete - flushed {} bytes after delete at {:?}", bytes_flushed, db_path);

        let keys: Vec<_> = db_lock.db
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("SledStorage::delete - current kv_pairs keys at {:?}: {:?}", db_path, keys);

        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("SledStorage::flush - flushing database at {:?}", db_path);

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("SledStorage::flush - flushed {} bytes to disk at {:?}", bytes_flushed, db_path);

        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SledStorage {
    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Creating vertex at path {:?}", db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.insert(vertex.id.0.as_bytes(), serialize_vertex(&vertex)?)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after creating vertex at {:?}", bytes_flushed, db_path);

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Retrieving vertex with id {} from path {:?}", id, db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let vertex = vertices
            .get(SerializableUuid(*id).0.as_bytes())
            .map_err(|e| GraphError::StorageError(e.to_string()))?
            .map(|v| deserialize_vertex(&*v))
            .transpose()?;

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(vertex)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Updating vertex at path {:?}", db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.insert(vertex.id.0.as_bytes(), serialize_vertex(&vertex)?)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after updating vertex at {:?}", bytes_flushed, db_path);

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Deleting vertex with id {} from path {:?}", id, db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        vertices.remove(SerializableUuid(*id).0.as_bytes())
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after deleting vertex at {:?}", bytes_flushed, db_path);

        let vertex_keys: Vec<_> = vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current vertex keys at {:?}: {:?}", db_path, vertex_keys);
        Ok(())
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Creating edge at path {:?}", db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(edge.outbound_id.0), &edge.t, &SerializableUuid(edge.inbound_id.0))?;
        edges.insert(&edge_key, serialize_edge(&edge)?)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after creating edge at {:?}", bytes_flushed, db_path);

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Retrieving edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        let edge = edges
            .get(&edge_key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?
            .map(|v| deserialize_edge(&*v))
            .transpose()?;

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(edge)
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Updating edge at path {:?}", db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(edge.outbound_id.0), &edge.t, &SerializableUuid(edge.inbound_id.0))?;
        edges.insert(&edge_key, serialize_edge(&edge)?)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after updating edge at {:?}", bytes_flushed, db_path);

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Deleting edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edge_key = create_edge_key(&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id))?;
        edges.remove(&edge_key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after deleting edge at {:?}", bytes_flushed, db_path);

        let edge_keys: Vec<_> = edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .map(|k| String::from_utf8_lossy(&*k).to_string())
            .collect();
        info!("Current edge keys at {:?}: {:?}", db_path, edge_keys);
        Ok(())
    }

    async fn close(&self) -> GraphResult<()> {
        let pool = self.pool.lock().await;
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
                println!("===> ERROR: FAILED TO CLOSE SLED STORAGE POOL");
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
        let pool = self.pool.lock().await;
        let daemon_count = pool.daemons.len();
        info!("Checking running status for {} daemons", daemon_count);
        let futures = pool.daemons.values().map(|daemon| async {
            timeout(Duration::from_secs(2), daemon.is_running()).await
                .map_err(|_| false)
                .unwrap_or(false)
        });
        let results = join_all(futures).await;
        let is_running = results.iter().any(|&r| r);
        info!("SledStorage running status: {}, daemon states: {:?}", is_running, results);
        is_running
    }

    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        Ok(Value::Null)
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Retrieving all vertices from path {:?}", db_path);

        let vertices = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let mut vertex_vec = Vec::new();
        for result in vertices.iter() {
            let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
            vertex_vec.push(deserialize_vertex(&*v)?);
        }
        info!("Retrieved {} vertices from path {:?}", vertex_vec.len(), db_path);
        Ok(vertex_vec)
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Retrieving all edges from path {:?}", db_path);

        let edges = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let mut edge_vec = Vec::new();
        for result in edges.iter() {
            let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
            edge_vec.push(deserialize_edge(&*v)?);
        }
        info!("Retrieved {} edges from path {:?}", edge_vec.len(), db_path);
        Ok(edge_vec)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = db.lock().await;
        let db_path = &db_lock.path;
        info!("Clearing all data from path {:?}", db_path);

        db_lock.db.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Flushed {} bytes after clearing data at {:?}", bytes_flushed, db_path);
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
        return Ok(preferred_port);
    }

    for port in cluster_ports {
        if port == preferred_port {
            continue;
        }
        if !is_storage_daemon_running(port).await {
            debug!("Selected available port {} from cluster range", port);
            return Ok(port);
        }
    }

    error!("No available ports in cluster range {:?}", storage_config.cluster_range);
    println!("===> ERROR: NO AVAILABLE PORTS IN CLUSTER RANGE {:?}", storage_config.cluster_range);
    Err(GraphError::StorageError(format!(
        "No available ports in cluster range {:?}", storage_config.cluster_range
    )))
}

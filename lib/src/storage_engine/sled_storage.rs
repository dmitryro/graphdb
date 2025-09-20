use anyhow::{Result, Context, anyhow};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use tokio::fs;
use tokio::sync::{OnceCell, Mutex as TokioMutex};
use log::{info, debug, warn, error, trace};
pub use crate::config::{
    SledDbWithPath, SledConfig, SledStorage, SledDaemon, SledDaemonPool, load_storage_config_from_yaml, 
    DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, StorageConfig, StorageEngineType,
    QueryResult, QueryPlan,
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
use crate::storage_engine::sled_client::{ SledClient };

pub static SLED_DB: LazyLock<OnceCell<TokioMutex<SledDbWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static SLED_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<SledDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());

impl SledStorage {
    pub async fn new(config: &SledConfig, storage_config: &StorageConfig) -> Result<Self, GraphError> {
        let start_time = Instant::now();
        println!("===> INITIALIZING SledStorage with config: {:?}", config);
        
        let default_data_dir = PathBuf::from("./storage_daemon_server");
        let base_data_dir = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&default_data_dir);
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        println!("===> USING SLED PATH {:?}", db_path);
        
        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                println!("===> ERROR: FAILED TO CREATE SLED DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
            })?;
        
        if !db_path.is_dir() {
            println!("===> ERROR: PATH {:?} EXISTS BUT IS NOT A DIRECTORY", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        let metadata = fs::metadata(&db_path)
            .await
            .map_err(|e| {
                println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
            })?;
        
        if metadata.permissions().readonly() {
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }
        
        println!("===> Directory at {:?} is writable", db_path);

        // This check is now here, but its logic has been simplified and moved.
        // The main daemon start logic will handle checking the registry before calling this method.
        let daemon_metadata_opt = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await.ok().flatten();

        let pool = if let Some(_daemon_metadata) = daemon_metadata_opt {
            println!("===> FOUND EXISTING DAEMON ON PORT {}, ATTEMPTING TO REUSE POOL", port);
            
            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(std::collections::HashMap::new())
            }).await;
            
            let mut pool_map_guard = pool_map.lock().await;

            if let Some(existing_pool) = pool_map_guard.get(&port) {
                println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                return Ok(Self { pool: existing_pool.clone() });
            } else {
                println!("===> STALE DAEMON REGISTRY ENTRY FOUND, CREATING NEW POOL");
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        } else {
            println!("===> NO EXISTING DAEMON FOUND, CREATING A NEW SLED DAEMON POOL");
            Self::force_unlock(&db_path).await?;
            let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(std::collections::HashMap::new())
            }).await;
            
            let mut pool_map_guard = pool_map.lock().await;
            
            let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
            pool_map_guard.insert(port, new_pool.clone());
            new_pool
        };

        let sled_db_instance = timeout(Duration::from_secs(10), async {
            println!("===> ATTEMPTING TO OPEN SLED DB AT {:?}", db_path);
            let db = sled::Config::new()
                .path(&db_path)
                .use_compression(config.use_compression)
                .cache_capacity(config.cache_capacity.unwrap_or(1024 * 1024 * 1024))
                .open()
                .map_err(|e| {
                    println!("===> ERROR: FAILED TO OPEN SLED DB AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to open Sled database at {:?}: {}. Ensure no other process is using the database.", db_path, e))
                })?;
            println!("===> SUCCESSFULLY OPENED SLED DATABASE AT {:?}", db_path);
            Ok::<_, GraphError>(TokioMutex::new(SledDbWithPath { db: Arc::new(db), path: db_path.clone() }))
        }).await
            .map_err(|_| GraphError::StorageError("Timeout opening Sled database".to_string()))?;

        SLED_DB.set(sled_db_instance?)
            .map_err(|_| GraphError::StorageError("Failed to set SLED_DB singleton".to_string()))?;

        {
            let mut pool_guard = timeout(Duration::from_secs(10), pool.lock())
                .await
                .map_err(|_| GraphError::StorageError("Failed to acquire pool lock for initialization".to_string()))?;
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            let sled_db_guard = SLED_DB.get().unwrap().lock().await;
            timeout(Duration::from_secs(10), pool_guard.initialize_cluster_with_db(storage_config, config, Some(port), sled_db_guard.db.clone()))
                .await
                .map_err(|_| GraphError::StorageError("Timeout initializing SledDaemonPool".to_string()))??;
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        println!("===> SUCCESSFULLY INITIALIZED SledStorage in {}ms", start_time.elapsed().as_millis());
        Ok(Self { pool })
    }

    pub async fn new_with_db(config: &SledConfig, storage_config: &StorageConfig, existing_db: Arc<sled::Db>) -> Result<Self, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with existing database at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH EXISTING DB AT {:?}", config.path);

        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&default_data_dir);
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Using database path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO CREATE SLED DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
            })?;
        if !db_path.is_dir() {
            error!("Path {:?} exists but is not a directory", db_path);
            println!("===> ERROR: PATH {:?} EXISTS BUT IS NOT A DIRECTORY", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

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

        // Enhanced lock cleanup
        Self::force_unlock(&db_path).await?;

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

        let pool = {
            let metadata = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?;
            if let Some(metadata) = metadata {
                if let Some(registered_path) = &metadata.data_dir {
                    if registered_path != &db_path {
                        warn!("Path mismatch: daemon registry shows {:?}, but config specifies {:?}", registered_path, db_path);
                        println!("===> PATH MISMATCH: DAEMON REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", registered_path, db_path);
                    }
                }

                let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::<u16, Arc<TokioMutex<SledDaemonPool>>>::new())
                }).await;
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Failed to acquire pool map lock".to_string()))?;

                if let Some(existing_pool) = pool_map_guard.get(&port) {
                    info!("Found existing daemon on port {}. Checking pool state...", port);
                    println!("===> FOUND EXISTING DAEMON ON PORT {}", port);

                    let pool_guard = timeout(Duration::from_secs(5), existing_pool.lock())
                        .await
                        .map_err(|_| GraphError::StorageError("Failed to acquire existing pool lock".to_string()))?;

                    if !pool_guard.daemons.is_empty() {
                        info!("Reusing existing SledDaemonPool for port {}", port);
                        println!("===> REUSING EXISTING SLED DAEMON POOL FOR PORT {}", port);
                        return Ok(Self { pool: existing_pool.clone() });
                    }

                    // Clean up stale metadata
                    if let pid = metadata.pid {
                        if let Err(e) = std::process::Command::new("kill")
                            .arg("-9")
                            .arg(pid.to_string())
                            .output()
                        {
                            warn!("Failed to kill daemon with PID {}: {}", pid, e);
                        }
                    }
                    Self::force_unlock(&db_path).await?;
                    GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("sled", port).await?;
                }

                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            } else {
                let pool_map = SLED_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::<u16, Arc<TokioMutex<SledDaemonPool>>>::new())
                }).await;
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Failed to acquire pool map lock".to_string()))?;
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        {
            let mut pool_guard = timeout(Duration::from_secs(10), pool.lock())
                .await
                .map_err(|_| GraphError::StorageError("Failed to acquire pool lock for initialization".to_string()))?;
            info!("Initializing cluster with use_raft_for_scale: {}", storage_config.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}", storage_config.use_raft_for_scale);
            timeout(Duration::from_secs(10), pool_guard.initialize_cluster(storage_config, config, Some(port)))
                .await
                .map_err(|_| GraphError::StorageError("Timeout initializing SledDaemonPool".to_string()))??;
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        info!("Successfully initialized SledStorage with existing database in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED SLED STORAGE WITH EXISTING DB IN {}ms", start_time.elapsed().as_millis());
        Ok(Self { pool })
    }

    /// Creates a new `SledStorage` instance using an existing, connected `SledClient`.
    /// This method bypasses the complex setup of `SledStorage::new` and is
    /// intended for cases where the client connection is managed externally.

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
        db_lock.db.flush_async().await.map_err(|e| {
            error!("Failed to flush Sled database after setting key '{}': {}", key, e);
            println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER SETTING KEY {}", key);
            GraphError::StorageError(format!("Failed to flush Sled database after setting key '{}': {}", key, e))
        })?;
        debug!("Successfully set key '{}' and flushed database", key);
        println!("===> SUCCESSFULLY SET KEY {} AND FLUSHED DATABASE", key);
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
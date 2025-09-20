use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::sync::{OnceCell, Mutex as TokioMutex, RwLock};
use tokio::time::{timeout, Duration};
use log::{info, debug, warn, error, trace};
use rocksdb::{DB, ColumnFamilyDescriptor, Options, WriteOptions};
use serde_json::{json, Value};
use futures::future::join_all;
use uuid::Uuid;
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;

use crate::config::{
    RocksDBConfig, RocksDBStorage, RocksDBDaemonPool, StorageConfig, StorageEngineType,
    QueryResult, QueryPlan, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, RocksDbWithPath,
};
use crate::storage_engine::{StorageEngine, GraphStorageEngine};
use crate::storage_engine::storage_utils::{
    serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key
};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::daemon_utils::{is_storage_daemon_running, parse_cluster_range};
use models::{Vertex, Edge, Identifier};
use models::errors::{GraphResult, GraphError};
use models::identifiers::SerializableUuid;
use super::rocksdb_client::RocksDBClient;

#[cfg(feature = "with-openraft-rocksdb")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, BasicNode},
    super::rocksdb_raft_storage::RocksDBRaftStorage,
};

pub static ROCKSDB_DB: LazyLock<OnceCell<TokioMutex<RocksDbWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static ROCKSDB_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<RocksDBDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());

impl RocksDBStorage {
    pub async fn new(config: &RocksDBConfig, storage_config: &StorageConfig) -> GraphResult<Self> {
        let start_time = Instant::now();
        info!("Initializing RocksDBStorage with config: {:?}", config);
        println!("===> INITIALIZING RocksDBStorage with config: {:?}", config);

        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&default_data_dir);
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let db_path = base_data_dir.join("rocksdb").join(port.to_string());

        info!("Using RocksDB path {:?}", db_path);
        println!("===> USING ROCKSDB PATH {:?}", db_path);

        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO CREATE ROCKSDB DIRECTORY AT {:?}", db_path);
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
        println!("===> Directory at {:?} is writable", db_path);

        let daemon_metadata_opt = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await.ok().flatten();

        let pool = if let Some(_daemon_metadata) = daemon_metadata_opt {
            info!("Found existing daemon on port {}, attempting to reuse pool", port);
            println!("===> FOUND EXISTING DAEMON ON PORT {}, ATTEMPTING TO REUSE POOL", port);
            
            let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            
            let mut pool_map_guard = pool_map.lock().await;

            if let Some(existing_pool) = pool_map_guard.get(&port) {
                info!("Reusing existing RocksDBDaemonPool for port {}", port);
                println!("===> REUSING EXISTING ROCKSDB DAEMON POOL FOR PORT {}", port);
                return Ok(Self {
                    pool: existing_pool.clone(),
                    use_raft_for_scale: config.use_raft_for_scale,
                    #[cfg(feature = "with-openraft-rocksdb")]
                    raft: None,
                });
            } else {
                info!("Stale daemon registry entry found, creating new pool");
                println!("===> STALE DAEMON REGISTRY ENTRY FOUND, CREATING NEW POOL");
                Self::force_unlock(&db_path).await?;
                let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        } else {
            info!("No existing daemon found, creating a new RocksDBDaemonPool");
            println!("===> NO EXISTING DAEMON FOUND, CREATING A NEW ROCKSDB DAEMON POOL");
            Self::force_unlock(&db_path).await?;
            let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                TokioMutex::new(HashMap::new())
            }).await;
            
            let mut pool_map_guard = pool_map.lock().await;
            
            let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new()));
            pool_map_guard.insert(port, new_pool.clone());
            new_pool
        };

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);

        let cf_names = vec!["vertices", "edges", "kv_pairs"];
        let cfs: Vec<ColumnFamilyDescriptor> = cf_names
            .into_iter()
            .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
            .collect();

        let rocks_db_instance = timeout(Duration::from_secs(10), async {
            info!("Attempting to open RocksDB at {:?}", db_path);
            println!("===> ATTEMPTING TO OPEN ROCKSDB AT {:?}", db_path);
            let db = DB::open_cf_descriptors(&opts, &db_path, cfs)
                .map_err(|e| {
                    error!("Failed to open RocksDB at {:?}", db_path);
                    println!("===> ERROR: FAILED TO OPEN ROCKSDB AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to open RocksDB database at {:?}: {}. Ensure no other process is using the database.", db_path, e))
                })?;
            info!("Successfully opened RocksDB database at {:?}", db_path);
            println!("===> SUCCESSFULLY OPENED ROCKSDB DATABASE AT {:?}", db_path);
            Ok::<_, GraphError>(TokioMutex::new(RocksDbWithPath { db: Arc::new(db), path: db_path.clone() }))
        }).await
            .map_err(|_| GraphError::StorageError("Timeout opening RocksDB database".to_string()))?;

        ROCKSDB_DB.set(rocks_db_instance?)
            .map_err(|_| GraphError::StorageError("Failed to set ROCKSDB_DB singleton".to_string()))?;

        {
            let mut pool_guard = timeout(Duration::from_secs(10), pool.lock())
                .await
                .map_err(|_| GraphError::StorageError("Failed to acquire pool lock for initialization".to_string()))?;
            info!("Initializing cluster with use_raft: {}", pool_guard.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT: {}", pool_guard.use_raft_for_scale);
            let rocks_db_guard = ROCKSDB_DB.get().unwrap().lock().await;
            timeout(Duration::from_secs(10), pool_guard.initialize_with_db(config, rocks_db_guard.db.clone()))
                .await
                .map_err(|_| GraphError::StorageError("Timeout initializing RocksDBDaemonPool".to_string()))??;
            info!("Initialized cluster on port {} with existing DB", port);
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        let storage = Self {
            pool,
            use_raft_for_scale: config.use_raft_for_scale,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
        };

        #[cfg(feature = "with-openraft-rocksdb")]
        if storage.use_raft_for_scale {
            let raft_config = RaftConfig {
                cluster_name: storage_config.cluster_name.clone().unwrap_or_default(),
                ..Default::default()
            };
            let rocks_db_guard = ROCKSDB_DB.get().unwrap().lock().await;
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
        println!("===> SUCCESSFULLY INITIALIZED RocksDBStorage in {}ms", start_time.elapsed().as_millis());
        Ok(storage)
    }

    pub async fn new_with_db(config: &RocksDBConfig, storage_config: &StorageConfig, existing_db: Arc<DB>) -> GraphResult<Self> {
        let start_time = Instant::now();
        info!("Initializing RocksDBStorage with existing database at {:?}", config.path);
        println!("===> INITIALIZING ROCKSDB STORAGE WITH EXISTING DB AT {:?}", config.path);

        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&default_data_dir);
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let db_path = base_data_dir.join("rocksdb").join(port.to_string());

        info!("Using database path {:?}", db_path);
        println!("===> USING ROCKSDB PATH {:?}", db_path);

        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO CREATE ROCKSDB DIRECTORY AT {:?}", db_path);
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
            error!("Directory at {:?}", db_path);
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        info!("Directory at {:?}", db_path);
        println!("===> Directory at {:?} is writable", db_path);

        Self::force_unlock(&db_path).await?;

        let rocks_db = ROCKSDB_DB.get_or_try_init(|| async {
            info!("Storing provided RocksDB database in singleton at {:?}", db_path);
            println!("===> STORING PROVIDED ROCKSDB IN SINGLETON AT {:?}", db_path);
            Ok::<_, GraphError>(TokioMutex::new(RocksDbWithPath { db: existing_db.clone(), path: db_path.clone() }))
        }).await?;

        {
            let mut rocks_db_guard = rocks_db.lock().await;
            rocks_db_guard.db = existing_db.clone();
            rocks_db_guard.path = db_path.clone();
        }
        info!("Successfully updated RocksDB database singleton at {:?}", db_path);
        println!("===> SUCCESSFULLY UPDATED ROCKSDB DATABASE SINGLETON AT {:?}", db_path);

        let pool = {
            let metadata = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?;
            if let Some(metadata) = metadata {
                if let Some(registered_path) = &metadata.data_dir {
                    if registered_path != &db_path {
                        warn!("Path mismatch: daemon registry shows {:?}, but config specifies {:?}", registered_path, db_path);
                        println!("===> PATH MISMATCH: DAEMON REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", registered_path, db_path);
                    }
                }

                let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::<u16, Arc<TokioMutex<RocksDBDaemonPool>>>::new())
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
                        info!("Reusing existing RocksDBDaemonPool for port {}", port);
                        println!("===> REUSING EXISTING ROCKSDB DAEMON POOL FOR PORT {}", port);
                        return Ok(Self {
                            pool: existing_pool.clone(),
                            use_raft_for_scale: config.use_raft_for_scale,
                            #[cfg(feature = "with-openraft-rocksdb")]
                            raft: None,
                        });
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
                    GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("rocksdb", port).await?;
                }

                let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            } else {
                let pool_map = ROCKSDB_POOL_MAP.get_or_init(|| async {
                    TokioMutex::new(HashMap::<u16, Arc<TokioMutex<RocksDBDaemonPool>>>::new())
                }).await;
                let mut pool_map_guard = timeout(Duration::from_secs(5), pool_map.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Failed to acquire pool map lock".to_string()))?;
                let new_pool = Arc::new(TokioMutex::new(RocksDBDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        {
            let mut pool_guard = timeout(Duration::from_secs(10), pool.lock())
                .await
                .map_err(|_| GraphError::StorageError("Failed to acquire pool lock for initialization".to_string()))?;
            info!("Initializing cluster with use_raft: {}", pool_guard.use_raft_for_scale);
            println!("===> INITIALIZING CLUSTER WITH USE_RAFT: {}", pool_guard.use_raft_for_scale);
            timeout(Duration::from_secs(10), pool_guard.initialize_with_db(config, existing_db.clone()))
                .await
                .map_err(|_| GraphError::StorageError("Timeout initializing RocksDBDaemonPool".to_string()))??;
            info!("Initialized cluster on port {} with existing DB", port);
            println!("===> INITIALIZED CLUSTER ON PORT {} WITH EXISTING DB", port);
        }

        let storage = Self {
            pool,
            use_raft_for_scale: config.use_raft_for_scale,
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

        info!("Successfully initialized RocksDBStorage with existing database in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED RocksDBStorage with existing database in {}ms", start_time.elapsed().as_millis());
        Ok(storage)
    }

    pub fn new_pinned(config: &RocksDBConfig, storage_config: &StorageConfig) -> Box<dyn futures::Future<Output = GraphResult<Self>> + Send + 'static> {
        let config = config.clone();
        let storage_config = storage_config.clone();
        Box::new(async move {
            RocksDBStorage::new(&config, &storage_config).await
        })
    }

    pub async fn set_key(&self, key: &str, value: &str) -> GraphResult<()> {
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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

        pool_guard.insert_replicated(key.as_bytes(), value.as_bytes(), self.use_raft_for_scale).await?;
        info!("Successfully set key '{}'", key);
        println!("===> SUCCESSFULLY SET KEY {}", key);
        Ok(())
    }

    pub async fn get_key(&self, key: &str) -> GraphResult<Option<String>> {
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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

        let value = pool_guard.retrieve_with_failover(key.as_bytes()).await?
            .map(|v| String::from_utf8_lossy(&v).to_string());
        info!("Retrieved value for key '{}': {:?}", key, value);
        println!("===> RETRIEVED VALUE FOR KEY {}: {:?}", key, value);
        Ok(value)
    }

    pub async fn delete_key(&self, key: &str) -> GraphResult<()> {
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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

        pool_guard.delete_replicated(key.as_bytes(), self.use_raft_for_scale).await?;
        info!("Successfully deleted key '{}'", key);
        println!("===> SUCCESSFULLY DELETED KEY {}", key);
        Ok(())
    }

    pub async fn add_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let key = vertex.id.to_string();
        let value = serialize_vertex(&vertex)?;
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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

        pool_guard.insert_replicated(key.as_bytes(), &value, self.use_raft_for_scale).await?;
        info!("Successfully added vertex '{}'", key);
        println!("===> SUCCESSFULLY ADDED VERTEX {}", key);
        Ok(())
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.to_string();
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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
                return Ok(value.and_then(|v| deserialize_vertex(&v).ok()));
            }
        }

        let value = pool_guard.retrieve_with_failover(key.as_bytes()).await?;
        Ok(value.and_then(|v| deserialize_vertex(&v).ok()))
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key = id.to_string();
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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

        pool_guard.delete_replicated(key.as_bytes(), self.use_raft_for_scale).await?;
        info!("Successfully deleted vertex '{}'", key);
        println!("===> SUCCESSFULLY DELETED VERTEX {}", key);
        Ok(())
    }

    pub async fn add_edge(&self, edge: Edge) -> GraphResult<()> {
        let edge_key = create_edge_key(
            &edge.outbound_id,
            &edge.t,
            &edge.inbound_id,
        )?;
        let value = serialize_edge(&edge)?;
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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

        pool_guard.insert_replicated(&edge_key, &value, self.use_raft_for_scale).await?;
        info!("Successfully added edge");
        println!("===> SUCCESSFULLY ADDED EDGE");
        Ok(())
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let edge_key = create_edge_key(
            &SerializableUuid(*outbound_id),
            edge_type,
            &SerializableUuid(*inbound_id),
        )?;
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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
                return Ok(value.and_then(|v| deserialize_edge(&v).ok()));
            }
        }

        let value = pool_guard.retrieve_with_failover(&edge_key).await?;
        Ok(value.and_then(|v| deserialize_edge(&v).ok()))
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let edge_key = create_edge_key(
            &SerializableUuid(*outbound_id),
            edge_type,
            &SerializableUuid(*inbound_id),
        )?;
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
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

        pool_guard.delete_replicated(&edge_key, self.use_raft_for_scale).await?;
        info!("Successfully deleted edge");
        println!("===> SUCCESSFULLY DELETED EDGE");
        Ok(())
    }

    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
        let mut vertices = Vec::new();
        for daemon in pool_guard.daemons.values() {
            let db = &daemon.db;
            let cf = db.cf_handle("vertices")
                .ok_or_else(|| GraphError::StorageError("vertices column family not found".to_string()))?;
            for result in db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
                let (_key, value) = result.map_err(|e| {
                    error!("Error iterating vertices: {}", e);
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
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
        let mut edges = Vec::new();
        for daemon in pool_guard.daemons.values() {
            let db = &daemon.db;
            let cf = db.cf_handle("edges")
                .ok_or_else(|| GraphError::StorageError("edges column family not found".to_string()))?;
            for result in db.iterator_cf(&cf, rocksdb::IteratorMode::Start) {
                let (_key, value) = result.map_err(|e| {
                    error!("Error iterating edges: {}", e);
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

    pub async fn clear_data(&self) -> GraphResult<()> {
        let cf_names = vec!["vertices", "edges", "kv_pairs"];
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        
        for daemon in pool_guard.daemons.values() {
            let db = &daemon.db;
            for cf_name in &cf_names {
                let cf = db.cf_handle(cf_name)
                    .ok_or_else(|| GraphError::StorageError(format!("{} column family not found", cf_name)))?;
                let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
                for result in iter {
                    let (key, _) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
                    db.delete_cf(&cf, &key)
                        .map_err(|e| GraphError::StorageError(e.to_string()))?;
                }
            }
        }
        info!("Successfully cleared all data");
        println!("===> SUCCESSFULLY CLEARED ALL DATA");
        Ok(())
    }

    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        info!("Attempting to force unlock RocksDB database at {:?}", path);
        println!("===> ATTEMPTING TO FORCE UNLOCK ROCKSDB DATABASE AT {:?}", path);
        let lock_path = path.join("LOCK");

        debug!("Checking for lock file at {:?}", lock_path);
        println!("===> CHECKING FOR LOCK FILE AT {:?}", lock_path);
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
                    return Ok(());
                }
                return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}", lock_path)));
            }
            #[cfg(not(unix))]
            {
                if let Err(e) = timeout(Duration::from_secs(2), fs::remove_file(&lock_path)).await {
                    warn!("Failed to remove lock file at {:?}: {:?}", lock_path, e);
                    println!("===> ERROR: FAILED TO REMOVE LOCK FILE AT {:?}", lock_path);
                    return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}", lock_path)));
                }
                info!("Successfully removed lock file at {:?}", lock_path);
                println!("===> SUCCESSFULLY REMOVED LOCK FILE AT {:?}", lock_path);
            }
        }
        Ok(())
    }

    pub async fn force_reset(config: &RocksDBConfig) -> GraphResult<Self> {
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
        RocksDBStorage::new(config, &StorageConfig::default()).await
            .map_err(|e| {
                error!("Failed to initialize RocksDBStorage after reset: {}", e);
                println!("===> ERROR: FAILED TO INITIALIZE ROCKSDB STORAGE AFTER RESET");
                GraphError::StorageError(format!("Failed to initialize RocksDBStorage after reset: {}", e))
            })
    }

    pub async fn diagnose_persistence(&self) -> GraphResult<serde_json::Value> {
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        let db_path = pool_guard.daemons.values().next()
            .map(|daemon| daemon.db_path.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
        info!("Diagnosing persistence for RocksDBStorage at {:?}", db_path);
        println!("===> DIAGNOSING PERSISTENCE FOR ROCKSDB STORAGE AT {:?}", db_path);

        let db = pool_guard.daemons.values().next()
            .map(|daemon| daemon.db.clone())
            .ok_or_else(|| GraphError::StorageError("No daemons available for diagnostics".to_string()))?;

        let cf_vertices = db.cf_handle("vertices").ok_or_else(|| GraphError::StorageError("vertices column family not found".to_string()))?;
        let cf_edges = db.cf_handle("edges").ok_or_else(|| GraphError::StorageError("edges column family not found".to_string()))?;
        let cf_kv_pairs = db.cf_handle("kv_pairs").ok_or_else(|| GraphError::StorageError("kv_pairs column family not found".to_string()))?;

        let kv_count = db.iterator_cf(&cf_kv_pairs, rocksdb::IteratorMode::Start).count();
        let vertex_count = db.iterator_cf(&cf_vertices, rocksdb::IteratorMode::Start).count();
        let edge_count = db.iterator_cf(&cf_edges, rocksdb::IteratorMode::Start).count();

        let disk_usage = fs::metadata(&db_path)
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

impl Drop for RocksDBStorage {
    fn drop(&mut self) {
        if let Ok(pool) = self.pool.try_lock() {
            for daemon in pool.daemons.values() {
                if let Err(e) = daemon.db.flush() {
                    eprintln!("Failed to flush RocksDB daemon at {:?}: {}", daemon.db_path, e);
                }
            }
        } else {
            eprintln!("Failed to acquire lock on RocksDBDaemonPool during drop");
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
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
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
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
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
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
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

    async fn flush(&self) -> GraphResult<()> {
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring RocksDB pool lock".to_string()))?;
        let db_path = pool_guard.daemons.values().next()
            .map(|daemon| daemon.db_path.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
        info!("Flushing RocksDB database at {:?}", db_path);
        println!("===> FLUSHING ROCKSDB DATABASE AT {:?}", db_path);

        for daemon in pool_guard.daemons.values() {
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

    async fn is_running(&self) -> bool {
        let pool_guard = timeout(Duration::from_secs(5), self.pool.lock())
            .await;
        if let Ok(pool_guard) = pool_guard {
            for daemon in pool_guard.daemons.values() {
                if *daemon.running.lock().await {
                    return true;
                }
            }
        }
        false
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        Err(GraphError::StorageError("Query not implemented for RocksDBStorage".to_string()))
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.add_vertex(vertex).await
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        self.get_vertex(id).await
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.add_vertex(vertex).await // For simplicity, reuse add_vertex; update logic may need expansion
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.delete_vertex(id).await
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.add_edge(edge).await
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        self.get_edge(outbound_id, edge_type, inbound_id).await
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.add_edge(edge).await // For simplicity, reuse add_edge; update logic may need expansion
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.delete_edge(outbound_id, edge_type, inbound_id).await
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        self.get_all_vertices().await
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        self.get_all_edges().await
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        Err(GraphError::StorageError("Execute query not implemented for RocksDBStorage".to_string()))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        info!("Closing RocksDBStorage");
        println!("===> CLOSING ROCKSDB STORAGE");
        Ok(())
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.clear_data().await
    }
}
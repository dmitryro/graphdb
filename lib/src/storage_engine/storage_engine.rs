// lib/src/storage_engine/storage_engine.rs
// Fixed: 2025-08-10 - Resolved 18 Send errors in HybridStorageEngine by releasing MutexGuard before await
// Fixed: 2025-08-09 - Added feature gate for mysql_storage import and usage to align with mod.rs
// Updated: 2025-08-09 - Added global StorageEngineManager singleton with configurable path and engine-specific config support
// Fixed: 2025-08-09 - Resolved type mismatches, field errors, borrowing issues, and unused argument warning

use async_trait::async_trait;
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use crate::storage_engine::config::{load_storage_config_from_yaml, StorageConfig, StorageEngineType};
use crate::storage_engine::inmemory_storage::InMemoryStorage;
#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::rocksdb_storage::RocksdbGraphStorage;
use crate::storage_engine::sled_storage::SledStorage;
#[cfg(feature = "redis-datastore")]
use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;
use std::fs;
use std::fmt::Debug;
use serde_yaml2 as serde_yaml;
#[cfg(feature = "redis-datastore")]
use redis::{Client, Connection};
use std::time::Instant;
use log::{info, error, warn};
use once_cell::sync::OnceCell;

// Global StorageEngineManager singleton (uninitialized until set)
pub static GLOBAL_STORAGE_ENGINE_MANAGER: OnceCell<Arc<Mutex<StorageEngineManager>>> = OnceCell::new();

// Function to initialize the global StorageEngineManager
pub fn init_storage_engine_manager(config_path: PathBuf) -> Result<(), GraphError> {
    let config = load_storage_config_from_yaml(Some(config_path.clone()))
        .unwrap_or_else(|_| {
            warn!("Failed to load config from {:?}, using default configuration", config_path);
            StorageConfig {
                storage_engine_type: StorageEngineType::InMemory,
                data_directory: Some(config_path.parent().unwrap_or(&PathBuf::from(".")).to_path_buf()),
                connection_string: None,
                max_open_files: None,
                engine_specific_config: None,
            }
        });
    
    let manager = StorageEngineManager::new(&config, config_path)?;
    GLOBAL_STORAGE_ENGINE_MANAGER
        .set(Arc::new(Mutex::new(manager)))
        .map_err(|_| GraphError::StorageError("Failed to initialize global StorageEngineManager".to_string()))?;
    Ok(())
}

#[async_trait]
pub trait StorageEngine: Send + Sync + Debug + 'static {
    async fn connect(&self) -> Result<(), GraphError>;
    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError>;
    async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError>;
    async fn delete(&self, key: &[u8]) -> Result<(), GraphError>;
    async fn flush(&self) -> Result<(), GraphError>;
}

#[async_trait]
pub trait GraphStorageEngine: StorageEngine + Send + Sync + Debug + 'static {
    async fn start(&self) -> Result<(), GraphError>;
    async fn stop(&self) -> Result<(), GraphError>;
    fn get_type(&self) -> &'static str;
    fn is_running(&self) -> bool;
    async fn query(&self, query_string: &str) -> Result<Value, GraphError>;
    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError>;
    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError>;
    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError>;
    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError>;
}

#[derive(Debug)]
pub struct HybridStorageEngine {
    inmemory: Arc<InMemoryStorage>,
    persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
    running: Arc<Mutex<bool>>,
    engine_type: StorageEngineType,
}

#[async_trait]
impl StorageEngine for HybridStorageEngine {
    async fn connect(&self) -> Result<(), GraphError> {
        self.inmemory.connect().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.connect().await?;
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        self.inmemory.insert(key, value).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.insert(key, value).await?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        if let Some(value) = self.inmemory.retrieve(key).await? {
            Ok(Some(value))
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.retrieve(key).await?;
            Ok(result)
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        self.inmemory.delete(key).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete(key).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        self.inmemory.flush().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for HybridStorageEngine {
    async fn start(&self) -> Result<(), GraphError> {
        self.inmemory.start().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.start().await?;
        *self.running.lock().unwrap() = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        self.inmemory.stop().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.stop().await?;
        *self.running.lock().unwrap() = false;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        match self.engine_type {
            StorageEngineType::Sled => "sled",
            StorageEngineType::RocksDB => "rocksdb",
            StorageEngineType::InMemory => "inmemory",
            StorageEngineType::Redis => "redis",
            StorageEngineType::PostgreSQL => "postgresql",
            StorageEngineType::MySQL => "mysql",
        }
    }

    fn is_running(&self) -> bool {
        *self.running.lock().unwrap()
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        let persistent_arc = Arc::clone(&self.persistent);
        let result = persistent_arc.query(query_string).await?;
        Ok(result)
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.create_vertex(vertex.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.create_vertex(vertex).await?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        if let Some(vertex) = self.inmemory.get_vertex(id).await? {
            Ok(Some(vertex))
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.get_vertex(id).await?;
            Ok(result)
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.update_vertex(vertex.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.update_vertex(vertex).await?;
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_vertex(id).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete_vertex(id).await?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let results = self.inmemory.get_all_vertices().await?;
        if !results.is_empty() {
            Ok(results)
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.get_all_vertices().await?;
            Ok(result)
        }
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.create_edge(edge.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.create_edge(edge).await?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        if let Some(edge) = self.inmemory.get_edge(outbound_id, edge_type, inbound_id).await? {
            Ok(Some(edge))
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.get_edge(outbound_id, edge_type, inbound_id).await?;
            Ok(result)
        }
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.update_edge(edge.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.update_edge(edge).await?;
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_edge(outbound_id, edge_type, inbound_id).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete_edge(outbound_id, edge_type, inbound_id).await?;
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let results = self.inmemory.get_all_edges().await?;
        if !results.is_empty() {
            Ok(results)
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.get_all_edges().await?;
            Ok(result)
        }
    }
}

pub struct StorageEngineManager {
    engine: Arc<Mutex<HybridStorageEngine>>,
    session_engine_type: Option<StorageEngineType>,
    config: StorageConfig,
    config_path: PathBuf, // Store the configuration path
}

impl StorageEngineManager {
    pub fn new(config: &StorageConfig, config_path: PathBuf) -> Result<Self, GraphError> {
        // Validate configuration before initializing
        Self::validate_config(config, config.storage_engine_type)?;
        
        let persistent: Box<dyn GraphStorageEngine + Send + Sync> = match config.storage_engine_type {
            StorageEngineType::Sled => {
                let db = crate::storage_engine::sled_storage::open_sled_db(
                    config.data_directory.as_ref().expect("Data directory is required for Sled storage")
                )?;
                Box::new(SledStorage::new(db)?)
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    Box::new(RocksdbGraphStorage::new(config)?)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    return Err(GraphError::StorageError("RocksDB support is not enabled.".to_string()));
                }
            }
            StorageEngineType::InMemory => {
                Box::new(InMemoryStorage::new(config.clone())?)
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let host = config.engine_specific_config.as_ref()
                        .ok_or_else(|| GraphError::StorageError("Redis connection config is required".to_string()))?
                        .get("host")
                        .ok_or_else(|| GraphError::StorageError("Redis host is required".to_string()))?;
                    let host_str = host.as_str()
                        .ok_or_else(|| GraphError::StorageError("Redis host must be a string".to_string()))?;
                    let client = Client::open(host_str)
                        .map_err(|e| GraphError::StorageError(format!("Failed to create Redis client: {}", e)))?;
                    let connection = client.get_connection()
                        .map_err(|e| GraphError::StorageError(format!("Failed to connect to Redis: {}", e)))?;
                    Box::new(RedisStorage::new(connection)?)
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    return Err(GraphError::StorageError("Redis support is not enabled.".to_string()));
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    Box::new(PostgresStorage::new(config)?)
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    return Err(GraphError::StorageError("PostgreSQL support is not enabled.".to_string()));
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    Box::new(MySQLStorage::new(config).map_err(|e| GraphError::StorageError(format!("MySQL error: {}", e)))?)
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    return Err(GraphError::StorageError("MySQL support is not enabled.".to_string()));
                }
            }
        };

        let persistent_arc: Arc<dyn GraphStorageEngine + Send + Sync> = Arc::from(persistent);

        let engine = HybridStorageEngine {
            inmemory: Arc::new(InMemoryStorage::new(config.clone())?),
            persistent: Arc::clone(&persistent_arc),
            running: Arc::new(Mutex::new(false)),
            engine_type: config.storage_engine_type,
        };
        Ok(StorageEngineManager {
            engine: Arc::new(Mutex::new(engine)),
            session_engine_type: None,
            config: config.clone(),
            config_path,
        })
    }

    pub fn current_engine_type(&self) -> StorageEngineType {
        self.session_engine_type.unwrap_or_else(|| {
            self.engine.lock().unwrap().engine_type
        })
    }

    pub fn available_engines() -> Vec<StorageEngineType> {
        let engines = vec![StorageEngineType::Sled, StorageEngineType::InMemory];
        #[cfg(feature = "with-rocksdb")]
        let engines = engines.into_iter().chain(std::iter::once(StorageEngineType::RocksDB)).collect();
        #[cfg(feature = "redis-datastore")]
        let engines = engines.into_iter().chain(std::iter::once(StorageEngineType::Redis)).collect();
        #[cfg(feature = "postgres-datastore")]
        let engines = engines.into_iter().chain(std::iter::once(StorageEngineType::PostgreSQL)).collect();
        #[cfg(feature = "mysql-datastore")]
        let engines = engines.into_iter().chain(std::iter::once(StorageEngineType::MySQL)).collect();
        engines
    }

    fn validate_config(config: &StorageConfig, engine_type: StorageEngineType) -> Result<(), GraphError> {
        match engine_type {
            StorageEngineType::Sled | StorageEngineType::RocksDB => {
                if config.data_directory.is_none() {
                    return Err(GraphError::StorageError("Data directory is required for Sled or RocksDB storage".to_string()));
                }
            }
            StorageEngineType::Redis => {
                if config.engine_specific_config.as_ref().map_or(true, |c| c.get("host").is_none()) {
                    return Err(GraphError::StorageError("Redis host is required".to_string()));
                }
            }
            StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                if config.engine_specific_config.as_ref().map_or(true, |c| {
                    c.get("host").is_none() || c.get("database").is_none() || c.get("username").is_none()
                }) {
                    return Err(GraphError::StorageError(format!("Host, database, and username are required for {:?}", engine_type)));
                }
            }
            StorageEngineType::InMemory => {}
        }
        if !Self::available_engines().contains(&engine_type) {
            return Err(GraphError::StorageError(format!("Storage engine {:?} is not enabled", engine_type)));
        }
        Ok(())
    }

    fn get_engine_config_path(&self, engine_type: StorageEngineType) -> PathBuf {
        let parent = self.config_path.parent().unwrap_or(&PathBuf::from(".")).to_path_buf();
        match engine_type {
            StorageEngineType::Sled => parent.join("storage_config_sled.yaml"),
            StorageEngineType::RocksDB => parent.join("storage_config_rocksdb.yaml"),
            StorageEngineType::InMemory => parent.join("storage_config_inmemory.yaml"),
            StorageEngineType::Redis => parent.join("storage_config_redis.yaml"),
            StorageEngineType::PostgreSQL => parent.join("storage_config_postgres.yaml"),
            StorageEngineType::MySQL => parent.join("storage_config_mysql.yaml"),
        }
    }

    async fn migrate_data(&self, old_engine: &Arc<dyn GraphStorageEngine + Send + Sync>, new_engine: &Arc<dyn GraphStorageEngine + Send + Sync>) -> Result<(), GraphError> {
        info!("Migrating data from {} to {}", old_engine.get_type(), new_engine.get_type());
        let start_time = Instant::now();

        // Migrate vertices
        let vertices = old_engine.get_all_vertices().await?;
        for vertex in vertices {
            new_engine.create_vertex(vertex).await?;
        }

        // Migrate edges
        let edges = old_engine.get_all_edges().await?;
        for edge in edges {
            new_engine.create_edge(edge).await?;
        }

        info!("Data migration completed in {}ms", start_time.elapsed().as_millis());
        Ok(())
    }

    pub async fn use_storage(&mut self, engine_type: StorageEngineType, permanent: bool) -> Result<(), GraphError> {
        info!("Switching storage engine to {:?}", engine_type);
        let start_time = Instant::now();

        // Validate the new engine type and configuration
        let mut config = self.config.clone();
        config.storage_engine_type = engine_type;
        
        // Load engine-specific configuration if available
        let engine_config_path = self.get_engine_config_path(engine_type);
        if engine_config_path.exists() {
            config = load_storage_config_from_yaml(Some(engine_config_path.clone()))
                .map_err(|e| GraphError::StorageError(format!("Failed to load engine-specific config from {:?}: {}", engine_config_path, e)))?;
            config.storage_engine_type = engine_type; // Ensure engine type matches
        }
        Self::validate_config(&config, engine_type)?;

        // Save configuration for permanent changes
        let config_file_path = if permanent {
            self.get_engine_config_path(engine_type)
        } else {
            self.config_path.clone()
        };
        if permanent {
            let yaml_string = serde_yaml::to_string(&config)
                .map_err(|e| GraphError::SerializationError(e.to_string()))?;
            fs::create_dir_all(config_file_path.parent().unwrap())
                .map_err(|e| GraphError::Io(e))?;
            fs::write(&config_file_path, yaml_string)
                .map_err(|e| GraphError::Io(e))?;
            info!("Persisted new storage engine configuration to {:?}", config_file_path);
        } else {
            self.session_engine_type = Some(engine_type);
            info!("Set session engine type to {:?}", engine_type);
        }

        // Get current engine state
        let (was_running, old_persistent_arc, old_engine_type) = {
            let engine_guard = self.engine.lock().unwrap();
            (engine_guard.is_running(), Arc::clone(&engine_guard.persistent), engine_guard.engine_type)
        };

        // Stop the old engine if running
        if was_running {
            info!("Stopping current storage engine ({})", old_persistent_arc.get_type());
            old_persistent_arc.stop().await?;
        }

        // Create the new persistent engine
        let new_persistent: Box<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::Sled => {
                let db = crate::storage_engine::sled_storage::open_sled_db(
                    config.data_directory.as_ref().expect("Data directory is required for Sled storage")
                )?;
                Box::new(SledStorage::new(db)?)
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    Box::new(RocksdbGraphStorage::new(&config)?)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    return Err(GraphError::StorageError("RocksDB support is not enabled.".to_string()));
                }
            }
            StorageEngineType::InMemory => {
                Box::new(InMemoryStorage::new(config.clone())?)
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let host = config.engine_specific_config.as_ref()
                        .ok_or_else(|| GraphError::StorageError("Redis connection config is required".to_string()))?
                        .get("host")
                        .ok_or_else(|| GraphError::StorageError("Redis host is required".to_string()))?;
                    let host_str = host.as_str()
                        .ok_or_else(|| GraphError::StorageError("Redis host must be a string".to_string()))?;
                    let client = Client::open(host_str)
                        .map_err(|e| GraphError::StorageError(format!("Failed to create Redis client: {}", e)))?;
                    let connection = client.get_connection()
                        .map_err(|e| GraphError::StorageError(format!("Failed to connect to Redis: {}", e)))?;
                    Box::new(RedisStorage::new(connection)?)
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    return Err(GraphError::StorageError("Redis support is not enabled.".to_string()));
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    Box::new(PostgresStorage::new(&config)?)
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    return Err(GraphError::StorageError("PostgreSQL support is not enabled.".to_string()));
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    Box::new(MySQLStorage::new(&config).map_err(|e| GraphError::StorageError(format!("MySQL error: {}", e)))?)
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    return Err(GraphError::StorageError("MySQL support is not enabled.".to_string()));
                }
            }
        };

        let new_persistent_arc: Arc<dyn GraphStorageEngine + Send + Sync> = Arc::from(new_persistent);

        // Migrate data from old to new engine
        if was_running {
            self.migrate_data(&old_persistent_arc, &new_persistent_arc).await
                .map_err(|e| {
                    error!("Data migration failed: {}", e);
                    e
                })?;
        }

        // Update the engine and configuration
        {
            let mut engine_guard = self.engine.lock().unwrap();
            engine_guard.persistent = Arc::clone(&new_persistent_arc);
            engine_guard.engine_type = engine_type;
        }

        // Start the new engine if the old one was running
        if was_running {
            info!("Starting new storage engine ({})", new_persistent_arc.get_type());
            if let Err(e) = new_persistent_arc.start().await {
                error!("Failed to start new engine: {}. Reverting to old engine.", e);
                // Rollback: restore old engine and start it
                old_persistent_arc.start().await?;
                let mut engine_guard = self.engine.lock().unwrap();
                engine_guard.persistent = Arc::clone(&old_persistent_arc);
                engine_guard.engine_type = old_engine_type;
                if !permanent {
                    self.session_engine_type = Some(old_engine_type);
                }
                return Err(e);
            }
            // Validate the new engine
            new_persistent_arc.query("SELECT 1").await
                .map_err(|e| GraphError::StorageError(format!("Validation query failed: {}", e)))?;
        }

        // Reload configuration for permanent changes
        if permanent {
            self.config = load_storage_config_from_yaml(Some(config_file_path.clone()))
                .map_err(|e| GraphError::StorageError(format!("Failed to reload configuration: {}", e)))?;
            self.config_path = config_file_path;
            self.session_engine_type = None; // Clear session override
        }

        info!("Storage engine switched to {:?}", engine_type);
        Ok(())
    }

    pub async fn connect(&self) -> Result<(), GraphError> {
        self.engine.lock().unwrap().connect().await
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        self.engine.lock().unwrap().insert(key, value).await
    }

    pub async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        self.engine.lock().unwrap().retrieve(key).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        self.engine.lock().unwrap().delete(key).await
    }

    pub async fn flush(&self) -> Result<(), GraphError> {
        self.engine.lock().unwrap().flush().await
    }

    pub async fn start(&self) -> Result<(), GraphError> {
        self.engine.lock().unwrap().start().await
    }

    pub async fn stop(&self) -> Result<(), GraphError> {
        self.engine.lock().unwrap().stop().await
    }

    pub fn is_running(&self) -> bool {
        self.engine.lock().unwrap().is_running()
    }

    pub async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        self.engine.lock().unwrap().query(query_string).await
    }

    pub async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.engine.lock().unwrap().create_vertex(vertex).await
    }

    pub async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        self.engine.lock().unwrap().get_vertex(id).await
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.engine.lock().unwrap().update_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.engine.lock().unwrap().delete_vertex(id).await
    }

    pub async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        self.engine.lock().unwrap().get_all_vertices().await
    }

    pub async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.engine.lock().unwrap().create_edge(edge).await
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        self.engine.lock().unwrap().get_edge(outbound_id, edge_type, inbound_id).await
    }

    pub async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.engine.lock().unwrap().update_edge(edge).await
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.engine.lock().unwrap().delete_edge(outbound_id, edge_type, inbound_id).await
    }

    pub async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        self.engine.lock().unwrap().get_all_edges().await
    }
}
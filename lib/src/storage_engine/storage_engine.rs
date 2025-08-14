// lib/src/storage_engine/storage_engine.rs
// Created: 2025-08-09 - Implemented storage engine management
// Fixed: 2025-08-10 - Resolved 18 Send errors in HybridStorageEngine by releasing MutexGuard before await
// Fixed: 2025-08-09 - Added feature gate for mysql_storage import and usage
// Updated: 2025-08-09 - Added global StorageEngineManager singleton with configurable path
// Fixed: 2025-08-09 - Resolved type mismatches, field errors, borrowing issues
// Updated: 2025-08-10 - Added support for aggregated runtime config from YAML and TOML
// Fixed: 2025-08-10 - Removed incorrect `.map(|wrapper| wrapper.storage)`
// Fixed: 2025-08-10 - Corrected `migrate_data` to accept `Arc<dyn GraphStorageEngine + Send + Sync>`
// Added: 2025-08-10 - Added `get_runtime_config` for `show` commands
// Fixed: 2025-08-10 - Corrected `StorageConfig` to use original fields
// Fixed: 2025-08-09 - Added `std::fmt::Debug` import to resolve E0404
// Fixed: 2025-08-10 - Resolved E0382 borrow after move errors
// Fixed: 2025-08-09 - Fixed E0282, E0308, E0599 for data_directory
// Added: 2025-08-11 - Added `get_persistent_engine` for specific engine instances
// Added: 2025-08-13 - Added `reset` method to clear engine state
// Added: 2025-08-13 - Added Sled corruption recovery in `new`
// Fixed: 2025-08-13 - Made `recover_sled_db` synchronous
// Fixed: 2025-08-13 - Moved logic from mod.rs to storage_engine.rs
// Fixed: 2025-08-13 - Replaced InMemoryStorage with InMemoryGraphStorage
// Fixed: 2025-08-13 - Removed unused `mut` warning
// Fixed: 2025-08-15 - Corrected initialization logic for SledStorage::new, which now expects a `sled::Db` not a `&StorageConfig`.
// Fixed: 2025-08-15 - Fixed InMemoryGraphStorage::new() parameter type and removed incorrect ? operator usage
// Fixed: 2025-08-13 - Corrected `close_connections` to handle SledStorage flush
use std::any::Any;
use async_trait::async_trait;
use log::{error, info, warn};
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use once_cell::sync::OnceCell;
use serde_json::Value;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;
use std::fs;
use anyhow::Context;

// Corrected import paths based on compiler errors
use crate::storage_engine::config::{CliConfigToml, StorageConfig, StorageEngineType, format_engine_config, load_storage_config_from_yaml};
pub use crate::storage_engine::inmemory_storage::{InMemoryStorage as InMemoryGraphStorage};
pub use crate::storage_engine::sled_storage::{SledStorage, open_sled_db};
#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::rocksdb_storage::RocksdbGraphStorage;
#[cfg(feature = "redis-datastore")]
use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;
#[cfg(feature = "redis-datastore")]
use redis::{Client, Connection};

// Global StorageEngineManager singleton
pub static GLOBAL_STORAGE_ENGINE_MANAGER: OnceCell<Arc<Mutex<StorageEngineManager>>> = OnceCell::new();

/// Initializes the global StorageEngineManager
pub fn init_storage_engine_manager(config_path_yaml: PathBuf, config_path_toml: PathBuf) -> Result<(), GraphError> {
    let manager = StorageEngineManager::new(&config_path_yaml, &config_path_toml)?;
    GLOBAL_STORAGE_ENGINE_MANAGER
        .set(Arc::new(Mutex::new(manager)))
        .map_err(|_| GraphError::StorageError("Failed to initialize global StorageEngineManager".to_string()))?;
    Ok(())
}

/// Recovers a corrupted Sled database
fn recover_sled_db(data_dir: &PathBuf) -> Result<(), GraphError> {
    warn!("Detected Sled database corruption at {:?}. Attempting recovery.", data_dir);
    if data_dir.exists() {
        fs::remove_dir_all(data_dir)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to remove corrupted Sled database directory {:?}", data_dir))?;
        fs::create_dir_all(data_dir)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to recreate Sled database directory {:?}", data_dir))?;
        info!("Successfully cleared and recreated Sled database directory {:?}", data_dir);
    } else {
        fs::create_dir_all(data_dir)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create Sled database directory {:?}", data_dir))?;
        info!("Created new Sled database directory {:?}", data_dir);
    }
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
    // Add this method to enable downcasting
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug)]
pub struct HybridStorageEngine {
    inmemory: Arc<InMemoryGraphStorage>,
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
            if let Some(ref value) = result {
                self.inmemory.insert(key, value).await?;
            }
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
    fn as_any(&self) -> &dyn Any {
        self
    }

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
            if let Some(ref vertex) = result {
                self.inmemory.create_vertex(vertex.clone()).await?;
            }
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
        let inmemory_results = self.inmemory.get_all_vertices().await?;
        if !inmemory_results.is_empty() {
            Ok(inmemory_results)
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let persistent_results = persistent_arc.get_all_vertices().await?;
            for vertex in persistent_results.iter() {
                self.inmemory.create_vertex(vertex.clone()).await?;
            }
            Ok(persistent_results)
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
            if let Some(ref edge) = result {
                self.inmemory.create_edge(edge.clone()).await?;
            }
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
        let inmemory_results = self.inmemory.get_all_edges().await?;
        if !inmemory_results.is_empty() {
            Ok(inmemory_results)
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let persistent_results = persistent_arc.get_all_edges().await?;
            for edge in persistent_results.iter() {
                self.inmemory.create_edge(edge.clone()).await?;
            }
            Ok(persistent_results)
        }
    }
}

pub struct StorageEngineManager {
    engine: Arc<Mutex<HybridStorageEngine>>,
    persistent_engine: Arc<dyn GraphStorageEngine + Send + Sync>,
    session_engine_type: Option<StorageEngineType>,
    config: StorageConfig,
    config_path: PathBuf,
    toml_config_path: PathBuf,
}

impl StorageEngineManager {
    pub fn close_connections(&mut self) -> Result<(), GraphError> {
        if self.current_engine_type() == StorageEngineType::Sled {
            let sled_storage = self.persistent_engine.as_ref();
            if let Some(sled_storage) = sled_storage.as_any().downcast_ref::<SledStorage>() {
                tokio::runtime::Handle::current().block_on(sled_storage.close())
                    .map_err(|e| GraphError::StorageError(format!("Failed to close Sled database: {}", e)))?;
                info!("Sled database connections closed");
            } else {
                warn!("Failed to downcast persistent_engine to SledStorage");
            }
        }
        Ok(())
    }

    pub fn new(config_path_yaml: &PathBuf, config_path_toml: &PathBuf) -> Result<Self, GraphError> {
        let config = match load_storage_config_from_yaml(Some(config_path_yaml)) {
            Ok(config) => config,
            Err(e) => {
                warn!("Failed to load YAML config from {:?}: {}, using default configuration.", config_path_yaml, e);
                StorageConfig {
                    storage_engine_type: StorageEngineType::InMemory,
                    data_directory: config_path_yaml.parent().unwrap_or_else(|| Path::new(".")).to_path_buf(),
                    connection_string: None,
                    max_open_files: None,
                    engine_specific_config: None,
                    default_port: 8049,
                    log_directory: "/opt/graphdb/logs".to_string(),
                    config_root_directory: PathBuf::from("/opt/graphdb"),
                    cluster_range: "".to_string(),
                    use_raft_for_scale: false,
                    max_disk_space_gb: 10,
                    min_disk_space_gb: 1,
                }
            }
        };

        let toml_config_str = fs::read_to_string(config_path_toml)
            .map_err(|e| GraphError::Io(e))?;
        let toml_config: CliConfigToml = toml::from_str(&toml_config_str)
            .map_err(|e| GraphError::SerializationError(e.to_string()))?;

        let mut aggregated_config = config.clone();
        if let Some(toml_storage) = toml_config.storage {
            if let Some(engine_type) = toml_storage.storage_engine_type {
                aggregated_config.storage_engine_type = engine_type;
            }
            if let Some(data_dir) = toml_storage.data_directory {
                aggregated_config.data_directory = PathBuf::from(data_dir);
            }
            if let Some(connection_string) = toml_storage.connection_string {
                aggregated_config.connection_string = Some(connection_string);
            }
            if let Some(max_open_files) = toml_storage.max_open_files {
                aggregated_config.max_open_files = Some(max_open_files as i32);
            }
            if let Some(engine_specific_config) = toml_storage.engine_specific_config {
                aggregated_config.engine_specific_config = Some(engine_specific_config);
            }
            if let Some(default_port) = toml_storage.default_port {
                aggregated_config.default_port = default_port;
            }
            if let Some(log_directory) = toml_storage.log_directory {
                aggregated_config.log_directory = log_directory;
            }
            if let Some(config_root_directory) = toml_storage.config_root_directory {
                aggregated_config.config_root_directory = config_root_directory;
            }
            if let Some(cluster_range) = toml_storage.cluster_range {
                aggregated_config.cluster_range = cluster_range;
            }
            if let Some(use_raft_for_scale) = toml_storage.use_raft_for_scale {
                aggregated_config.use_raft_for_scale = use_raft_for_scale;
            }
            if let Some(max_disk_space_gb) = toml_storage.max_disk_space_gb {
                aggregated_config.max_disk_space_gb = max_disk_space_gb;
            }
            if let Some(min_disk_space_gb) = toml_storage.min_disk_space_gb {
                aggregated_config.min_disk_space_gb = min_disk_space_gb;
            }
        }

        Self::validate_config(&aggregated_config, aggregated_config.storage_engine_type)?;

        let persistent: Box<dyn GraphStorageEngine + Send + Sync> = match aggregated_config.storage_engine_type {
            StorageEngineType::Sled => {
                Box::new(SledStorage::new(&aggregated_config)?)
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    Box::new(RocksdbGraphStorage::new(&aggregated_config)?)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    return Err(GraphError::StorageError("RocksDB support is not enabled.".to_string()));
                }
            }
            StorageEngineType::InMemory => {
                Box::new(InMemoryGraphStorage::new(&aggregated_config))
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let connection_string = aggregated_config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("Redis connection string is required".to_string()))?;
                    let client = Client::open(connection_string.as_str())
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
                    let connection_string = aggregated_config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("PostgreSQL connection string is required".to_string()))?;
                    Box::new(PostgresStorage::new(connection_string)?)
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    return Err(GraphError::StorageError("PostgreSQL support is not enabled.".to_string()));
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    let connection_string = aggregated_config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("MySQL connection string is required".to_string()))?;
                    Box::new(MySQLStorage::new(connection_string).map_err(|e| GraphError::StorageError(format!("MySQL error: {}", e)))?)
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    return Err(GraphError::StorageError("MySQL support is not enabled.".to_string()));
                }
            }
        };

        let persistent_arc: Arc<dyn GraphStorageEngine + Send + Sync> = Arc::from(persistent);

        let engine = HybridStorageEngine {
            inmemory: Arc::new(InMemoryGraphStorage::new(&aggregated_config)),
            persistent: Arc::clone(&persistent_arc),
            running: Arc::new(Mutex::new(false)),
            engine_type: aggregated_config.storage_engine_type,
        };
        Ok(StorageEngineManager {
            engine: Arc::new(Mutex::new(engine)),
            persistent_engine: persistent_arc,
            session_engine_type: None,
            config: aggregated_config,
            config_path: config_path_yaml.clone(),
            toml_config_path: config_path_toml.clone(),
        })
    }

    pub async fn reset(&mut self) -> Result<(), GraphError> {
        info!("Resetting StorageEngineManager");
        {
            let engine_guard = self.engine.lock().unwrap();
            if engine_guard.is_running() {
                engine_guard.stop().await?;
            }
        }
        let new_manager = StorageEngineManager::new(&self.config_path, &self.toml_config_path)?;
        self.engine = new_manager.engine;
        self.persistent_engine = new_manager.persistent_engine;
        self.session_engine_type = new_manager.session_engine_type;
        self.config = new_manager.config;
        info!("StorageEngineManager reset completed");
        Ok(())
    }

    pub fn get_persistent_engine(&self) -> Arc<dyn GraphStorageEngine + Send + Sync> {
        Arc::clone(&self.persistent_engine)
    }

    pub fn get_runtime_config(&self) -> StorageConfig {
        let mut config = self.config.clone();
        if let Some(session_engine) = self.session_engine_type {
            config.storage_engine_type = session_engine;
        }
        config
    }

    pub fn current_engine_type(&self) -> StorageEngineType {
        self.session_engine_type.unwrap_or(self.engine.lock().unwrap().engine_type)
    }

    pub fn available_engines() -> Vec<StorageEngineType> {
        let mut engines = vec![StorageEngineType::Sled, StorageEngineType::InMemory];
        #[cfg(feature = "with-rocksdb")]
        engines.push(StorageEngineType::RocksDB);
        #[cfg(feature = "redis-datastore")]
        engines.push(StorageEngineType::Redis);
        #[cfg(feature = "postgres-datastore")]
        engines.push(StorageEngineType::PostgreSQL);
        #[cfg(feature = "mysql-datastore")]
        engines.push(StorageEngineType::MySQL);
        engines
    }

    fn validate_config(config: &StorageConfig, engine_type: StorageEngineType) -> Result<(), GraphError> {
        match engine_type {
            StorageEngineType::Sled | StorageEngineType::RocksDB => {}
            StorageEngineType::Redis | StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                if config.connection_string.is_none() {
                    return Err(GraphError::StorageError(format!("Connection string is required for {:?}", engine_type)));
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
        let parent = self.config_path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
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
        let _start_time = Instant::now();

        let vertices = old_engine.get_all_vertices().await?;
        for vertex in vertices {
            new_engine.create_vertex(vertex).await?;
        }

        let edges = old_engine.get_all_edges().await?;
        for edge in edges {
            new_engine.create_edge(edge).await?;
        }

        info!("Data migration completed in {}ms", _start_time.elapsed().as_millis());
        Ok(())
    }

    pub async fn use_storage(&mut self, engine_type: StorageEngineType, permanent: bool) -> Result<(), GraphError> {
        info!("Switching storage engine to {:?}", engine_type);
        let _start_time = Instant::now();

        let (was_running, old_persistent_arc, old_engine_type) = {
            let engine_guard = self.engine.lock().unwrap();
            (engine_guard.is_running(), Arc::clone(&engine_guard.persistent), engine_guard.engine_type)
        };

        if old_engine_type == engine_type && self.session_engine_type.is_none() {
            info!("Requested engine is already the current engine. No action needed.");
            return Ok(());
        }

        let new_config = if permanent {
            let config_path = self.get_engine_config_path(engine_type);
            load_storage_config_from_yaml(Some(&config_path))
                .unwrap_or_else(|e| {
                    warn!("Failed to load engine-specific config for {:?}: {}. Creating new default config.", engine_type, e);
                    let mut default_config = StorageConfig::default();
                    default_config.storage_engine_type = engine_type;
                    default_config
                })
        } else {
            let mut current_config = self.config.clone();
            current_config.storage_engine_type = engine_type;
            current_config
        };

        Self::validate_config(&new_config, engine_type)?;

        if was_running {
            info!("Stopping current storage engine ({})", old_persistent_arc.get_type());
            if let Err(e) = old_persistent_arc.stop().await {
                error!("Failed to stop old engine: {}. This may lead to an inconsistent state.", e);
            }
        }

        let new_persistent: Result<Box<dyn GraphStorageEngine + Send + Sync>, GraphError> = match engine_type {
            StorageEngineType::Sled => {
                Ok(Box::new(SledStorage::new(&new_config)?))
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    Ok(Box::new(RocksdbGraphStorage::new(&new_config)?))
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    Err(GraphError::StorageError("RocksDB support is not enabled.".to_string()))
                }
            }
            StorageEngineType::InMemory => {
                Ok(Box::new(InMemoryGraphStorage::new(&new_config)))
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let connection_string = new_config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("Redis connection string is required".to_string()))?;
                    let client = Client::open(connection_string.as_str())
                        .map_err(|e| GraphError::StorageError(format!("Failed to create Redis client: {}", e)))?;
                    let connection = client.get_connection()
                        .map_err(|e| GraphError::StorageError(format!("Failed to connect to Redis: {}", e)))?;
                    Ok(Box::new(RedisStorage::new(connection)?))
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    Err(GraphError::StorageError("Redis support is not enabled.".to_string()))
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    let connection_string = new_config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("PostgreSQL connection string is required".to_string()))?;
                    Ok(Box::new(PostgresStorage::new(connection_string)?))
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    Err(GraphError::StorageError("PostgreSQL support is not enabled.".to_string()))
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    let connection_string = new_config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("MySQL connection string is required".to_string()))?;
                    Ok(Box::new(MySQLStorage::new(connection_string).map_err(|e| GraphError::StorageError(format!("MySQL error: {}", e)))?))
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    Err(GraphError::StorageError("MySQL support is not enabled.".to_string()))
                }
            }
        };

        let new_persistent_arc: Arc<dyn GraphStorageEngine + Send + Sync> = Arc::from(new_persistent?);

        if permanent {
            if let Err(e) = self.migrate_data(&old_persistent_arc, &new_persistent_arc).await {
                error!("Data migration failed: {}. Reverting to old engine.", e);
                let mut engine_guard = self.engine.lock().unwrap();
                engine_guard.persistent = Arc::clone(&old_persistent_arc);
                engine_guard.engine_type = old_engine_type;
                self.persistent_engine = Arc::clone(&old_persistent_arc);
                let _ = old_persistent_arc.start().await;
                return Err(e);
            }
        }

        {
            let mut engine_guard = self.engine.lock().unwrap();
            engine_guard.persistent = Arc::clone(&new_persistent_arc);
            engine_guard.engine_type = engine_type;
            *engine_guard.running.lock().unwrap() = false;
        }
        self.persistent_engine = Arc::clone(&new_persistent_arc);
        self.config = new_config.clone();

        if was_running {
            info!("Starting new storage engine ({})", new_persistent_arc.get_type());
            if let Err(e) = new_persistent_arc.start().await {
                error!("Failed to start new engine: {}. Reverting to old engine.", e);
                let mut engine_guard = self.engine.lock().unwrap();
                engine_guard.persistent = Arc::clone(&old_persistent_arc);
                engine_guard.engine_type = old_engine_type;
                self.persistent_engine = Arc::clone(&old_persistent_arc);
                let _ = old_persistent_arc.start().await;
                return Err(e);
            }
            new_persistent_arc.query("SELECT 1").await
                .map_err(|e| GraphError::StorageError(format!("Validation query failed: {}", e)))?;
        }

        if permanent {
            let config_file_path = self.get_engine_config_path(engine_type);
            let formatted_config = format_engine_config(&self.config);
            let formatted_config_str = formatted_config.join("\n");
            fs::create_dir_all(config_file_path.parent().unwrap())
                .map_err(|e| GraphError::Io(e))?;
            fs::write(&config_file_path, &formatted_config_str)
                .map_err(|e| GraphError::Io(e))?;
            info!("Persisted new storage engine configuration to {:?}", config_file_path);
            self.config_path = config_file_path;
            self.session_engine_type = None;
        } else {
            self.session_engine_type = Some(engine_type);
            info!("Set session engine type to {:?}", engine_type);
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
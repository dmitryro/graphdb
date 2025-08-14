// lib/src/storage_engine/storage_engine.rs
// Created: 2025-08-09 - Implemented storage engine management
// Fixed: 2025-08-10 - Resolved 18 Send errors in HybridStorageEngine by releasing MutexGuard before await
// Fixed: 2025-08-09 - Added feature gate for mysql_storage import and usage
// Updated: 2025-08-09 - Added global StorageEngineManager singleton with configurable path
// Fixed: 2025-08-09 - Resolved type mismatches, field errors, borrowing issues
// Updated: 2025-08-10 - Added support for runtime config from YAML
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
// Fixed: 2025-08-13 - Corrected initialization logic for SledStorage
// Fixed: 2025-08-13 - Updated `close_connections` to support all storage engines and handle async `close` methods
// Fixed: 2025-08-13 - Refactored to use tokio::sync::Mutex instead of std::sync::Mutex for consistency
// Fixed: 2025-08-13 - Fixed E0369 in `HybridStorageEngine::is_running` by dereferencing MutexGuard
// Added: 2025-08-13 - Added async `close` method to `GraphStorageEngine` trait
// Updated: 2025-08-13 - Made `close_connections` async to align with async storage engine methods
// Fixed: 2025-08-13 - Updated Redis initialization to use async connections
// Updated: 2025-08-13 - Removed TOML dependency, rely solely on YAML from storage_daemon_server
// Added: 2025-08-13 - Support for engine-specific YAML configs (storage_config_<engine>.yaml)
// Added: 2025-08-13 - Directory validation and creation for data_directory, log_directory, and engine-specific paths
// Fixed: 2025-08-13 - Corrected StorageConfig field access to use engine_specific_config
// Fixed: 2025-08-13 - Fixed constructor arguments for InMemoryGraphStorage and SledStorage
// Fixed: 2025-08-13 - Corrected type mismatches for PathBuf and String
// Fixed: 2025-08-13 - Improved error handling with proper GraphError propagation

use std::any::Any;
use std::collections::HashMap;
use async_trait::async_trait;
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use once_cell::sync::OnceCell;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use std::time::Instant;
use uuid::Uuid;
use std::fs;
use anyhow::Context;
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use log::{info, error, warn, debug};
//∂use serde_yaml2::Mapping;

// Corrected import paths based on compiler errors
use crate::storage_engine::config::{DEFAULT_DATA_DIRECTORY, StorageConfig, StorageEngineType, StorageConfigWrapper, load_storage_config_from_yaml};
pub use crate::storage_engine::inmemory_storage::InMemoryStorage as InMemoryGraphStorage;
pub use crate::storage_engine::sled_storage::{SledStorage, open_sled_db};
#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::rocksdb_storage::RocksdbGraphStorage;
#[cfg(feature = "redis-datastore")]
use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;

pub static GLOBAL_STORAGE_ENGINE_MANAGER: OnceCell<Arc<TokioMutex<StorageEngineManager>>> = OnceCell::new();

/// Initializes the global StorageEngineManager
pub async fn init_storage_engine_manager(config_path_yaml: PathBuf) -> Result<(), GraphError> {
    info!("Initializing StorageEngineManager with YAML: {:?}", config_path_yaml);
    // Ensure directory for YAML config exists
    if let Some(parent) = config_path_yaml.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create directory for YAML config: {:?}", parent))?;
    }
    let manager = StorageEngineManager::new(&config_path_yaml).await
        .map_err(|e| {
            error!("Failed to create StorageEngineManager: {}", e);
            e
        })?;
    GLOBAL_STORAGE_ENGINE_MANAGER
        .set(Arc::new(TokioMutex::new(manager)))
        .map_err(|_| {
            let err = GraphError::StorageError("Failed to set global StorageEngineManager".to_string());
            error!("{}", err);
            err
        })?;
    info!("StorageEngineManager initialized successfully");
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


/// Creates a default YAML configuration file
fn create_default_yaml_config(yaml_path: &PathBuf, engine_type: StorageEngineType) -> Result<(), GraphError> {
    info!("Creating default YAML config at {:?}", yaml_path);
    
    // For now, we'll create the config without engine-specific settings
    // You can manually edit the YAML file to add engine-specific config later
    // Create the config using the correct field types based on the error messages
    let default_yaml = serde_yaml2::to_string(&StorageConfigWrapper {
        storage: StorageConfig {
            storage_engine_type: engine_type,
            config_root_directory: PathBuf::from("./storage_daemon_server"), // Remove Some()
            data_directory: PathBuf::from("./storage_daemon_server/data"), // Remove Some()
            log_directory: "./storage_daemon_server/logs".to_string(), // String, not PathBuf
            default_port: 8083,
            cluster_range: "8083".to_string(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            max_open_files: Some(100), // Wrap in Some()
            connection_string: None, // Add missing field
            engine_specific_config: None,
        },
    })
    .map_err(|e| GraphError::SerializationError(e.to_string()))?;
    
    let parent_dir = yaml_path.parent()
        .ok_or_else(|| GraphError::StorageError("Invalid YAML config path".to_string()))?;
    fs::create_dir_all(parent_dir)
        .map_err(|e| GraphError::Io(e))
        .with_context(|| format!("Failed to create directory for YAML config at {:?}", parent_dir))?;
    fs::write(yaml_path, default_yaml)
        .map_err(|e| GraphError::Io(e))
        .with_context(|| format!("Failed to write default YAML config to {:?}", yaml_path))?;
    info!("Default YAML config created at {:?}", yaml_path);
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
    fn as_any(&self) -> &dyn Any;
    async fn close(&self) -> Result<(), GraphError>;
}

#[derive(Debug)]
pub struct HybridStorageEngine {
    inmemory: Arc<InMemoryGraphStorage>,
    persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
    running: Arc<TokioMutex<bool>>,
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
        *self.running.lock().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        self.inmemory.stop().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.stop().await?;
        *self.running.lock().await = false;
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
        let rt = tokio::runtime::Handle::current();
        *rt.block_on(self.running.lock()) == true
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.query(query_string).await
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.create_vertex(vertex.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.create_vertex(vertex).await
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
        persistent_arc.update_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_vertex(id).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete_vertex(id).await
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
        persistent_arc.create_edge(edge).await
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
        persistent_arc.update_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_edge(outbound_id, edge_type, inbound_id).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete_edge(outbound_id, edge_type, inbound_id).await
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

    async fn close(&self) -> Result<(), GraphError> {
        self.inmemory.close().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.close().await?;
        info!("HybridStorageEngine closed");
        Ok(())
    }
}

#[derive(Debug)]
pub struct StorageEngineManager {
    engine: Arc<TokioMutex<HybridStorageEngine>>,
    persistent_engine: Arc<dyn GraphStorageEngine + Send + Sync>,
    session_engine_type: Option<StorageEngineType>,
    config: StorageConfig,
    config_path: PathBuf,
}

impl StorageEngineManager {
    pub async fn new(config_path_yaml: &Path) -> Result<Self, GraphError> {
        info!("Creating StorageEngineManager with YAML config: {:?}", config_path_yaml);
        debug!("Loading configuration from {:?}", config_path_yaml);
        
        // Load YAML configuration
        let mut config = if config_path_yaml.exists() {
            load_storage_config_from_yaml(Some(&config_path_yaml.to_path_buf()))
                .map_err(|e| {
                    error!("Failed to load YAML config from {:?}: {}", config_path_yaml, e);
                    GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
                })?
        } else {
            warn!("YAML config file not found at {:?}, creating default", config_path_yaml);
            create_default_yaml_config(&config_path_yaml.to_path_buf(), StorageEngineType::Sled)?;
            load_storage_config_from_yaml(Some(&config_path_yaml.to_path_buf()))
                .map_err(|e| {
                    error!("Failed to load default YAML config from {:?}: {}", config_path_yaml, e);
                    GraphError::ConfigurationError(format!("Failed to load default YAML config: {}", e))
                })?
        };
        debug!("Loaded config: {:?}", config);
        println!("STEP 1 IN ENGINE");
        // Validate and create directories
        let data_dir = config.data_directory.clone();
        let log_dir = PathBuf::from(config.log_directory.clone());
        let engine_path = match config.storage_engine_type {
            StorageEngineType::Sled => {
                config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("Sled engine_specific_config: {:?}", map);
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or_else(|| {
                        let path = PathBuf::from("./storage_daemon_server/data/sled");
                        warn!("No path specified for Sled, using default: {:?}", path);
                        path
                    })
            }
            StorageEngineType::RocksDB => {
                config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("RocksDB engine_specific_config: {:?}", map);
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or_else(|| {
                        let path = PathBuf::from("./storage_daemon_server/data/rocksdb");
                        warn!("No path specified for RocksDB, using default: {:?}", path);
                        path
                    })
            }
            _ => {
                let path = data_dir.clone();
                info!("Using data_directory for non-Sled/RocksDB engine: {:?}", path);
                path
            }
        };
        println!("STEP 2 IN ENGINE");
        info!("Selected engine path: {:?}", engine_path);

        for dir in [&data_dir, &log_dir, &engine_path] {
            if !dir.exists() {
                info!("Creating directory: {:?}", dir);
                fs::create_dir_all(dir)
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("Failed to create directory: {:?}", dir))?;
            }
            // Check write permissions
            let test_file = dir.join(".write_test");
            fs::write(&test_file, "")
                .map_err(|e| GraphError::Io(e))
                .with_context(|| format!("No write permissions for directory: {:?}", dir))?;
            fs::remove_file(&test_file)
                .map_err(|e| GraphError::Io(e))
                .with_context(|| format!("Failed to remove test file in {:?}", dir))?;
        }
        
        // Initialize storage engine based on config
        let engine_type = config.storage_engine_type;
        info!("Validating config for engine type: {:?}", engine_type);

        println!("STEP 3 IN ENGINE");
        Self::validate_config(&config, engine_type)?;
        println!("STEP 4 IN ENGINE");
        let persistent: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::InMemory => {
                info!("Initializing InMemory engine");
                Arc::new(InMemoryGraphStorage::new(&config))
            }
            
            StorageEngineType::Sled => {
                println!("STEP 5.1 IN ENGINE - SLED");
                info!("Initializing Sled engine with path: {:?}", engine_path);
                if !engine_path.exists() {
                    recover_sled_db(&engine_path)?;
                }
                let sled_db = open_sled_db(&engine_path)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open Sled database: {}", e)))?;
                Arc::new(SledStorage::new(&config)?)
            }
            
            #[cfg(feature = "with-rocksdb")]
            StorageEngineType::RocksDB => {
                println!("STEP 5.2 IN ENGINE - let me see, RocksDB");
                info!("Initializing RocksDB engine with path: {:?}", engine_path);
                Arc::new(RocksdbGraphStorage::new(&engine_path, &config)?)
            }
            
            #[cfg(feature = "redis-datastore")]
            StorageEngineType::Redis => {
                let connection_string = config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("Redis engine_specific_config: {:?}", map);
                        format!(
                            "redis://{}:{}/{}",
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(6379),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("0")
                        )
                    })
                    .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());
                info!("Initializing Redis engine with connection: {}", connection_string);
                let redis_storage = RedisStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to initialize Redis: {}", e)))?;
                Arc::new(redis_storage)
            }
            #[cfg(feature = "postgres-datastore")]
            StorageEngineType::PostgreSQL => {
                let connection_string = config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("PostgreSQL engine_specific_config: {:?}", map);
                        format!(
                            "postgresql://{}:{}@{}:{}/{}",
                            map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                            map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(5432),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                        )
                    })
                    .unwrap_or_else(|| "postgresql://graphdb_user:secure_password@127.0.0.1:5432/graphdb".to_string());
                info!("Initializing PostgreSQL engine with connection: {}", connection_string);
                let postgres_storage = PostgresStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to initialize PostgreSQL: {}", e)))?;
                Arc::new(postgres_storage)
            }
            #[cfg(feature = "mysql-datastore")]
            StorageEngineType::MySQL => {
                let connection_string = config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("MySQL engine_specific_config: {:?}", map);
                        format!(
                            "mysql://{}:{}@{}:{}/{}",
                            map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                            map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(3306),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                        )
                    })
                    .unwrap_or_else(|| "mysql://graphdb_user:secure_password@127.0.0.1:3306/graphdb".to_string());
                info!("Initializing MySQL engine with connection: {}", connection_string);
                let mysql_storage = MySQLStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to initialize MySQL: {}", e)))?;
                Arc::new(mysql_storage)
            }
            _ => {
                warn!("Unsupported storage engine type: {:?}, falling back to InMemory", engine_type);
                Arc::new(InMemoryGraphStorage::new(&config))
            }
        };
        println!("STEP 6 IN ENGINE");
        let engine = Arc::new(TokioMutex::new(HybridStorageEngine {
            inmemory: Arc::new(InMemoryGraphStorage::new(&config)),
            persistent: Arc::clone(&persistent),
            running: Arc::new(TokioMutex::new(false)),
            engine_type,
        }));

        info!("StorageEngineManager initialized with engine: {:?}", engine_type);
        Ok(StorageEngineManager {
            engine,
            persistent_engine: persistent,
            session_engine_type: None,
            config,
            config_path: config_path_yaml.to_path_buf(),
        })
    }

    pub async fn close_connections(&mut self) -> Result<(), GraphError> {
        let engine_type = self.current_engine_type();
        info!("Closing connections for engine type: {:?}", engine_type);

        match engine_type {
            StorageEngineType::Sled => {
                if let Some(sled_storage) = self.persistent_engine.as_any().downcast_ref::<SledStorage>() {
                    sled_storage.close().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to close Sled database: {}", e)))?;
                    info!("Sled database connections closed");
                } else {
                    warn!("Failed to downcast persistent_engine to SledStorage");
                }
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    if let Some(rocksdb_storage) = self.persistent_engine.as_any().downcast_ref::<RocksdbGraphStorage>() {
                        rocksdb_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB database: {}", e)))?;
                        info!("RocksDB database connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to RocksdbGraphStorage");
                    }
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    warn!("RocksDB support is not enabled, skipping close");
                }
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    if let Some(redis_storage) = self.persistent_engine.as_any().downcast_ref::<RedisStorage>() {
                        redis_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close Redis connection: {}", e)))?;
                        info!("Redis connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to RedisStorage");
                    }
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    warn!("Redis support is not enabled, skipping close");
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    if let Some(postgres_storage) = self.persistent_engine.as_any().downcast_ref::<PostgresStorage>() {
                        postgres_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close PostgreSQL connection: {}", e)))?;
                        info!("PostgreSQL connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to PostgresStorage");
                    }
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    warn!("PostgreSQL support is not enabled, skipping close");
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    if let Some(mysql_storage) = self.persistent_engine.as_any().downcast_ref::<MySQLStorage>() {
                        mysql_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close MySQL connection: {}", e)))?;
                        info!("MySQL connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to MySQLStorage");
                    }
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    warn!("MySQL support is not enabled, skipping close");
                }
            }
            StorageEngineType::InMemory => {
                if let Some(inmemory_storage) = self.persistent_engine.as_any().downcast_ref::<InMemoryGraphStorage>() {
                    inmemory_storage.close().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to close InMemory storage: {}", e)))?;
                    info!("InMemory storage flushed");
                } else {
                    warn!("Failed to downcast persistent_engine to InMemoryGraphStorage");
                }
            }
        }

        let engine = self.engine.lock().await;
        engine.close().await
            .map_err(|e| GraphError::StorageError(format!("Failed to close HybridStorageEngine: {}", e)))?;
        Ok(())
    }

    pub async fn reset(&mut self) -> Result<(), GraphError> {
        info!("Resetting StorageEngineManager");
        let engine = self.engine.lock().await;
        if engine.is_running() {
            engine.stop().await?;
        }
        drop(engine); // Release the lock
        let new_manager = StorageEngineManager::new(&self.config_path).await?;
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
        self.session_engine_type.unwrap_or(self.engine.blocking_lock().engine_type)
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
        info!("Validating config for engine type: {:?}", engine_type);
        debug!("Full config: {:?}", config);

        match engine_type {
            StorageEngineType::Sled | StorageEngineType::RocksDB => {
                let path = config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("engine_specific_config for {:?}: {:?}", engine_type, map);
                        map.get("path").and_then(|v| {
                            let path_str = v.as_str();
                            debug!("Extracted path: {:?}", path_str);
                            path_str.map(PathBuf::from)
                        })
                    })
                    .unwrap_or_else(|| {
                        let default_path = match engine_type {
                            StorageEngineType::Sled => PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY)),
                            StorageEngineType::RocksDB => PathBuf::from(&format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY)),
                            _ => config.data_directory.clone(),
                        };
                        warn!("No path specified in engine_specific_config, using default: {:?}", default_path);
                        default_path
                    });
                println!("IN VALIDATOR - STEP 3.1");
                info!("Validating path for {:?}: {:?}", engine_type, path);
                if !path.exists() {
                    info!("Creating path: {:?}", path);
                    fs::create_dir_all(&path)
                        .map_err(|e| GraphError::Io(e))
                        .with_context(|| format!("Failed to create engine-specific path: {:?}", path))?;
                }
                if !path.is_dir() {
                    return Err(GraphError::ConfigurationError(format!("Path for {:?} is not a directory: {:?}", engine_type, path)));
                }
                let test_file = path.join(".write_test");
                fs::write(&test_file, "")
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("No write permissions for engine-specific path: {:?}", path))?;
                fs::remove_file(&test_file)
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("Failed to remove test file in {:?}", path))?;
            }
            StorageEngineType::Redis | StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                let map = config.engine_specific_config
                    .as_ref()
                    .ok_or_else(|| {
                        error!("engine_specific_config is missing for {:?}", engine_type);
                        GraphError::ConfigurationError(format!("Engine-specific config required for {:?}", engine_type))
                    })?;
                debug!("engine_specific_config for {:?}: {:?}", engine_type, map);
                if !map.contains_key("host") || !map.contains_key("port") {
                    return Err(GraphError::ConfigurationError(format!("Host and port are required for {:?}", engine_type)));
                }
                if matches!(engine_type, StorageEngineType::PostgreSQL | StorageEngineType::MySQL) {
                    if !map.contains_key("username") || !map.contains_key("password") || !map.contains_key("database") {
                        return Err(GraphError::ConfigurationError(format!("Username, password, and database are required for {:?}", engine_type)));
                    }
                }
            }
            StorageEngineType::InMemory => {
                info!("No specific validation required for InMemory engine");
            }
        }
        // The previous check "if !Self::available_engines().contains(&engine_type)" was removed
        // because it prevented supported engines from being configured.
        // The `validate_config` function should only validate the configuration, not
        // whether the engine is enabled, which should be handled at a higher level.
        info!("Config validation successful for {:?}", engine_type);
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
        let start_time = Instant::now();

        let vertices = old_engine.get_all_vertices().await?;
        for vertex in vertices {
            new_engine.create_vertex(vertex).await?;
        }

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

        let (was_running, old_persistent_arc, old_engine_type) = {
            let engine_guard = self.engine.lock().await;
            (engine_guard.is_running(), Arc::clone(&engine_guard.persistent), engine_guard.engine_type)
        };

        if old_engine_type == engine_type && self.session_engine_type.is_none() {
            info!("Requested engine is already the current engine. No action needed.");
            return Ok(());
        }

        let config_path = self.get_engine_config_path(engine_type);
        let new_config = if config_path.exists() {
            debug!("Loading engine-specific config from {:?}", config_path);
            load_storage_config_from_yaml(Some(&config_path))
                .map_err(|e| {
                    error!("Failed to load engine-specific config for {:?}: {}", engine_type, e);
                    GraphError::ConfigurationError(format!("Failed to load config from {:?}", config_path))
                })?
        } else if permanent {
            warn!("Engine-specific config file not found at {:?}, creating default", config_path);
            create_default_yaml_config(&config_path, engine_type)?;
            load_storage_config_from_yaml(Some(&config_path))
                .map_err(|e| {
                    error!("Failed to load default config for {:?}: {}", engine_type, e);
                    GraphError::ConfigurationError(format!("Failed to load default config from {:?}", config_path))
                })?
        } else {
            let mut current_config = self.config.clone();
            current_config.storage_engine_type = engine_type;
            let mut engine_config = HashMap::new();
            match engine_type {
                StorageEngineType::Sled => {
                    engine_config.insert(
                        "path".to_string(),
                        Value::String("./storage_daemon_server/data/sled".to_string()),
                    );
                }
                StorageEngineType::RocksDB => {
                    engine_config.insert(
                        "path".to_string(),
                        Value::String("./storage_daemon_server/data/rocksdb".to_string()),
                    );
                }
                StorageEngineType::Redis => {
                    engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                    engine_config.insert("port".to_string(), Value::Number(6379.into()));
                    engine_config.insert("database".to_string(), Value::String("0".to_string()));
                }
                StorageEngineType::PostgreSQL => {
                    engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                    engine_config.insert("port".to_string(), Value::Number(5432.into()));
                    engine_config.insert("username".to_string(), Value::String("graphdb_user".to_string()));
                    engine_config.insert("password".to_string(), Value::String("secure_password".to_string()));
                    engine_config.insert("database".to_string(), Value::String("graphdb".to_string()));
                }
                StorageEngineType::MySQL => {
                    engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                    engine_config.insert("port".to_string(), Value::Number(3306.into()));
                    engine_config.insert("username".to_string(), Value::String("graphdb_user".to_string()));
                    engine_config.insert("password".to_string(), Value::String("secure_password".to_string()));
                    engine_config.insert("database".to_string(), Value::String("graphdb".to_string()));
                }
                StorageEngineType::InMemory => {}
            }
            current_config.engine_specific_config = Some(engine_config);
            debug!("Using temporary config for {:?}: {:?}", engine_type, current_config);
            current_config
        };

        info!("Validating new config for {:?}", engine_type);
        Self::validate_config(&new_config, engine_type)?;

        let engine_path = match engine_type {
            StorageEngineType::Sled => {
                new_config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("Sled engine_specific_config: {:?}", map);
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or(PathBuf::from("./storage_daemon_server/data/sled"))
            }
            StorageEngineType::RocksDB => {
                new_config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("RocksDB engine_specific_config: {:?}", map);
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or(PathBuf::from("./storage_daemon_server/data/rocksdb"))
            }
            _ => {
                let path = new_config.data_directory.clone();
                if path.as_os_str().is_empty() {
                    PathBuf::from("./storage_daemon_server/data")
                } else {
                    path
                }
            }
        };
        info!("Selected engine path for new engine: {:?}", engine_path);

        if !engine_path.exists() {
            info!("Creating engine path: {:?}", engine_path);
            fs::create_dir_all(&engine_path)
                .map_err(|e| GraphError::Io(e))
                .with_context(|| format!("Failed to create engine-specific path: {:?}", engine_path))?;
        }

        let new_persistent: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::InMemory => {
                info!("Initializing InMemory engine");
                Arc::new(InMemoryGraphStorage::new(&new_config))
            }
            StorageEngineType::Sled => {
                info!("Initializing Sled engine with path: {:?}", engine_path);
                if !engine_path.exists() {
                    recover_sled_db(&engine_path)?;
                }
                let sled_db = open_sled_db(&engine_path)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open Sled database: {}", e)))?;
                Arc::new(SledStorage::new(&new_config)?)
            }
            #[cfg(feature = "with-rocksdb")]
            StorageEngineType::RocksDB => {
                info!("Initializing RocksDB engine with path: {:?}", engine_path);
                Arc::new(RocksdbGraphStorage::new(&engine_path, &new_config)?)
            }
            #[cfg(not(feature = "with-rocksdb"))]
            StorageEngineType::RocksDB => {
                return Err(GraphError::StorageError("RocksDB support is not enabled".to_string()));
            }
            #[cfg(feature = "redis-datastore")]
            StorageEngineType::Redis => {
                let connection_string = new_config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("Redis engine_specific_config: {:?}", map);
                        format!(
                            "redis://{}:{}/{}",
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(6379),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("0")
                        )
                    })
                    .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());
                info!("Initializing Redis engine with connection: {}", connection_string);
                let redis_storage = RedisStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to create RedisStorage: {}", e)))?;
                Arc::new(redis_storage)
            }
            #[cfg(feature = "postgres-datastore")]
            StorageEngineType::PostgreSQL => {
                let connection_string = new_config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("PostgreSQL engine_specific_config: {:?}", map);
                        format!(
                            "postgresql://{}:{}@{}:{}/{}",
                            map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                            map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(5432),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                        )
                    })
                    .unwrap_or_else(|| "postgresql://graphdb_user:secure_password@127.0.0.1:5432/graphdb".to_string());
                info!("Initializing PostgreSQL engine with connection: {}", connection_string);
                let postgres_storage = PostgresStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to create PostgresStorage: {}", e)))?;
                Arc::new(postgres_storage)
            }
            #[cfg(feature = "mysql-datastore")]
            StorageEngineType::MySQL => {
                let connection_string = new_config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("MySQL engine_specific_config: {:?}", map);
                        format!(
                            "mysql://{}:{}@{}:{}/{}",
                            map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                            map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(3306),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                        )
                    })
                    .unwrap_or_else(|| "mysql://graphdb_user:secure_password@127.0.0.1:3306/graphdb".to_string());
                info!("Initializing MySQL engine with connection: {}", connection_string);
                let mysql_storage = MySQLStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to create MySQLStorage: {}", e)))?;
                Arc::new(mysql_storage)
            }
            _ => {
                warn!("Unsupported storage engine type: {:?}, falling back to InMemory", engine_type);
                Arc::new(InMemoryGraphStorage::new(&new_config))
            }
        };

        if permanent {
            if let Err(e) = self.migrate_data(&old_persistent_arc, &new_persistent).await {
                error!("Data migration failed: {}. Reverting to old engine.", e);
                let mut engine_guard = self.engine.lock().await;
                engine_guard.persistent = Arc::clone(&old_persistent_arc);
                engine_guard.engine_type = old_engine_type;
                self.persistent_engine = Arc::clone(&old_persistent_arc);
                let _ = old_persistent_arc.start().await;
                return Err(e);
            }
        }

        {
            let mut engine_guard = self.engine.lock().await;
            engine_guard.persistent = Arc::clone(&new_persistent);
            engine_guard.engine_type = engine_type;
            *engine_guard.running.lock().await = false;
        }
        self.persistent_engine = Arc::clone(&new_persistent);
        self.config = new_config.clone();

        if was_running {
            info!("Starting new storage engine ({})", new_persistent.get_type());
            if let Err(e) = new_persistent.start().await {
                error!("Failed to start new engine: {}. Reverting to old engine.", e);
                let mut engine_guard = self.engine.lock().await;
                engine_guard.persistent = Arc::clone(&old_persistent_arc);
                engine_guard.engine_type = old_engine_type;
                self.persistent_engine = Arc::clone(&old_persistent_arc);
                let _ = old_persistent_arc.start().await;
                return Err(e);
            }
            new_persistent.query("SELECT 1").await
                .map_err(|e| GraphError::StorageError(format!("Validation query failed: {}", e)))?;
        }

        if permanent {
            let config_yaml = serde_yaml::to_string(&StorageConfigWrapper { storage: self.config.clone() })
                .map_err(|e| GraphError::SerializationError(format!("Failed to serialize config: {}", e)))?;
            fs::create_dir_all(self.config_path.parent().unwrap())
                .map_err(|e| GraphError::Io(e))
                .with_context(|| format!("Failed to create directory for config: {:?}", self.config_path))?;
            fs::write(&self.config_path, config_yaml)
                .map_err(|e| GraphError::Io(e))
                .with_context(|| format!("Failed to write config to {:?}", self.config_path))?;
            info!("Persisted new storage engine configuration to {:?}", self.config_path);
            self.session_engine_type = None;
        } else {
            self.session_engine_type = Some(engine_type);
            info!("Set session engine type to {:?}", engine_type);
        }

        info!("Storage engine switched to {:?} in {}ms", engine_type, start_time.elapsed().as_millis());
        Ok(())
    }

    pub async fn connect(&self) -> Result<(), GraphError> {
        self.engine.lock().await.connect().await
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        self.engine.lock().await.insert(key, value).await
    }

    pub async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        self.engine.lock().await.retrieve(key).await
    }

    pub async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        self.engine.lock().await.delete(key).await
    }

    pub async fn flush(&self) -> Result<(), GraphError> {
        self.engine.lock().await.flush().await
    }

    pub async fn start(&self) -> Result<(), GraphError> {
        self.engine.lock().await.start().await
    }

    pub async fn stop(&self) -> Result<(), GraphError> {
        self.engine.lock().await.stop().await
    }

    pub fn is_running(&self) -> bool {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.engine.lock()).is_running()
    }

    pub async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        self.engine.lock().await.query(query_string).await
    }

    pub async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.engine.lock().await.create_vertex(vertex).await
    }

    pub async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        self.engine.lock().await.get_vertex(id).await
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.engine.lock().await.update_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.engine.lock().await.delete_vertex(id).await
    }

    pub async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        self.engine.lock().await.get_all_vertices().await
    }

    pub async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.engine.lock().await.create_edge(edge).await
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        self.engine.lock().await.get_edge(outbound_id, edge_type, inbound_id).await
    }

    pub async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.engine.lock().await.update_edge(edge).await
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.engine.lock().await.delete_edge(outbound_id, edge_type, inbound_id).await
    }

    pub async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        self.engine.lock().await.get_all_edges().await
    }
}
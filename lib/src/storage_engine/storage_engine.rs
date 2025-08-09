// lib/src/storage_engine/storage_engine.rs
// Fixed: 2025-08-10 - Resolved 18 Send errors in HybridStorageEngine by releasing MutexGuard before await
// Fixed: 2025-08-09 - Added feature gate for mysql_storage import and usage to align with mod.rs

use async_trait::async_trait;
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use crate::storage_engine::config::{load_storage_config_from_yaml, StorageConfig, StorageConfigWrapper, StorageEngineType};
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
    // Changed from Arc<Mutex<Box<dyn ...>>> to Arc<dyn ...> so we can clone Arc and call async methods
    // without holding a MutexGuard across await points.
    persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
    running: Arc<Mutex<bool>>,
    engine_type: StorageEngineType,
}

#[async_trait]
impl StorageEngine for HybridStorageEngine {
    async fn connect(&self) -> Result<(), GraphError> {
        self.inmemory.connect().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        // call on the Arc directly; the future will own a reference to the Arc, so no guard is involved
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
}

impl StorageEngineManager {
    pub fn new(config: &StorageConfig) -> Result<Self, GraphError> {
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
                        .host.as_ref()
                        .ok_or_else(|| GraphError::StorageError("Redis host is required".to_string()))?;
                    let client = Client::open(host.as_str())
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

        // Convert Box<dyn ...> into Arc<dyn ...> so it can be cloned and used without MutexGuards
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
        })
    }

    pub fn current_engine_type(&self) -> StorageEngineType {
        self.session_engine_type.unwrap_or_else(|| {
            self.engine.lock().unwrap().engine_type
        })
    }

    pub async fn use_storage(&mut self, engine_type: StorageEngineType, permanent: bool) -> Result<(), GraphError> {
        let mut config = self.config.clone();
        config.storage_engine_type = engine_type;

        // Perform synchronous file I/O for permanent changes before touching the engine state.
        if permanent {
            let wrapper = StorageConfigWrapper { storage: config.clone() };
            let yaml_string = serde_yaml::to_string(&wrapper)
                .map_err(|e| GraphError::SerializationError(e.to_string()))?;
            let config_file_path = config.data_directory.as_ref()
                .expect("Data directory is required for writing config")
                .join("storage_config.yaml");
            fs::write(&config_file_path, yaml_string)
                .map_err(|e| GraphError::Io(e))?;
        } else {
            // Update session type synchronously.
            self.session_engine_type = Some(engine_type);
        }

        // Step 1: Check if the current engine is running and get a handle to the old persistent engine.
        let (was_running, old_persistent_arc) = {
            let engine_guard = self.engine.lock().unwrap();
            (engine_guard.is_running(), Arc::clone(&engine_guard.persistent))
        };

        // Step 2: If the old engine was running, stop it.
        if was_running {
            old_persistent_arc.stop().await?;
        }

        // Step 3: Create the new persistent engine.
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
                        .host.as_ref()
                        .ok_or_else(|| GraphError::StorageError("Redis host is required".to_string()))?;
                    let client = Client::open(host.as_str())
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

        // Step 4: Replace the persistent engine and update the type.
        {
            let mut engine_guard = self.engine.lock().unwrap();
            engine_guard.persistent = Arc::clone(&new_persistent_arc);
            engine_guard.engine_type = engine_type;
        }

        // Step 5: If the old engine was running, start the new one.
        if was_running {
            new_persistent_arc.start().await?;
        }
        
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

    pub async fn start(&mut self) -> Result<(), GraphError> {
        self.engine.lock().unwrap().start().await
    }

    pub async fn stop(&mut self) -> Result<(), GraphError> {
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

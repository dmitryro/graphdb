use anyhow::{anyhow, Result, Context};
use async_trait::async_trait;
use log::{error, info, warn};
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use serde_json::{Value, Map};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;
use std::collections::HashMap;

use crate::storage_engine::config::{
    StorageConfigWrapper, StorageEngineType, StorageConfig,
};
use crate::storage_engine::{
    SledStorage,
    TikvStorage,
};
pub use crate::storage_engine::{GraphStorageEngine, StorageEngine};
pub use crate::storage_engine::config::{CliConfigToml, format_engine_config, load_storage_config_from_yaml};
pub use crate::storage_engine::inmemory_storage::{InMemoryStorage as InMemoryGraphStorage};
#[cfg(feature = "with-rocksdb")]
pub use crate::storage_engine::rocksdb_storage::RocksdbStorage;
#[cfg(feature = "redis-datastore")]
pub use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
pub use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
pub use crate::storage_engine::mysql_storage::MySQLStorage;
#[cfg(feature = "redis-datastore")]
use redis::{Client, Connection};

// We need these config types to create the storage engines.
use crate::storage_engine::config::{SledConfig, RocksdbConfig, TikvConfig};

pub struct Database {
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
}

impl Database {
    pub async fn new(config: StorageConfig) -> Result<Self, GraphError> {
        let storage: Arc<dyn GraphStorageEngine + Send + Sync> = match config.storage_engine_type {
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    let sled_config = if let Some(ref engine_config) = config.engine_specific_config {
                        let config_value = serde_json::to_value(engine_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?;
                        serde_json::from_value(config_value)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to parse SledConfig: {}", e)))?
                    } else {
                        SledConfig {
                            storage_engine_type: config.storage_engine_type.clone(),
                            path: config.data_directory.clone(),
                            host: None, // No host or port on the top-level StorageConfig
                            port: None,
                        }
                    };
                    Arc::new(SledStorage::new(&sled_config).await?)
                }
                #[cfg(not(feature = "with-sled"))]
                {
                    return Err(GraphError::StorageError("Sled support is not enabled.".to_string()));
                }
            }
            StorageEngineType::InMemory => {
                Arc::new(InMemoryGraphStorage::new(&config))
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    let rocksdb_config = if let Some(ref engine_config) = config.engine_specific_config {
                        let config_value = serde_json::to_value(engine_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?;
                        serde_json::from_value(config_value)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to parse RocksdbConfig: {}", e)))?
                    } else {
                        RocksdbConfig {
                            storage_engine_type: config.storage_engine_type.clone(),
                            path: config.data_directory.clone(),
                            host: None,
                            port: None,
                        }
                    };
                    Arc::new(RocksdbStorage::new(&rocksdb_config)?)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    return Err(GraphError::StorageError("RocksDB support is not enabled.".to_string()));
                }
            }
            StorageEngineType::TiKV => {
                #[cfg(feature = "with-tikv")]
                {
                    let tikv_config = if let Some(ref engine_config) = config.engine_specific_config {
                        let config_value = serde_json::to_value(engine_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?;
                        serde_json::from_value(config_value)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to parse TikvConfig: {}", e)))?
                    } else {
                        TikvConfig {
                            storage_engine_type: config.storage_engine_type.clone(),
                            path: config.data_directory.clone(),
                            host: None,
                            port: None,
                            pd_endpoints: None,
                            username: None,
                            password: None,
                        }
                    };
                    Arc::new(TikvStorage::new(&tikv_config).await?)
                }
                #[cfg(not(feature = "with-tikv"))]
                {
                    return Err(GraphError::StorageError("TiKV support is not enabled.".to_string()));
                }
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let connection_string = config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("Redis connection string is required".to_string()))?;
                    let client = Client::open(connection_string.as_str())
                        .map_err(|e| GraphError::StorageError(format!("Failed to create Redis client: {}", e)))?;
                    let connection = client.get_connection()
                        .map_err(|e| GraphError::StorageError(format!("Failed to connect to Redis: {}", e)))?;
                    Arc::new(RedisStorage::new(connection)?)
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    return Err(GraphError::StorageError("Redis support is not enabled.".to_string()));
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    let connection_string = config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("PostgreSQL connection string is required".to_string()))?;
                    Arc::new(PostgresStorage::new(connection_string)?)
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    return Err(GraphError::StorageError("PostgreSQL support is not enabled.".to_string()));
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    let connection_string = config.connection_string.as_ref()
                        .ok_or_else(|| GraphError::StorageError("MySQL connection string is required".to_string()))?;
                    Arc::new(MySQLStorage::new(connection_string).map_err(|e| GraphError::StorageError(format!("MySQL error: {}", e)))?)
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    return Err(GraphError::StorageError("MySQL support is not enabled.".to_string()));
                }
            }
        };
        Ok(Database { storage })
    }

    async fn load_engine(&self, config_wrapper: StorageConfigWrapper) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>> {
        let engine_type = config_wrapper.storage.storage_engine_type;
        let persistent_engine: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::Sled => {
                let sled_config = if let Some(ref engine_config) = config_wrapper.storage.engine_specific_config {
                    let config_value = serde_json::to_value(engine_config).context("Failed to serialize sled config")?;
                    serde_json::from_value(config_value).context("Failed to deserialize sled config")?
                } else {
                    SledConfig {
                        storage_engine_type: config_wrapper.storage.storage_engine_type.clone(),
                        path: config_wrapper.storage.data_directory.clone(),
                        host: None,
                        port: None,
                    }
                };
                Arc::new(SledStorage::new(&sled_config).await?)
            }
            StorageEngineType::RocksDB => {
                let rocksdb_config = if let Some(ref engine_config) = config_wrapper.storage.engine_specific_config {
                    let config_value = serde_json::to_value(engine_config).context("Failed to serialize RocksDB config")?;
                    serde_json::from_value(config_value).context("Failed to deserialize RocksDB config")?
                } else {
                    RocksdbConfig {
                        storage_engine_type: config_wrapper.storage.storage_engine_type.clone(),
                        path: config_wrapper.storage.data_directory.clone(),
                        host: None,
                        port: None,
                    }
                };
                Arc::new(RocksdbStorage::new(&rocksdb_config)?)
            }
            StorageEngineType::TiKV => {
                let tikv_config = if let Some(ref engine_config) = config_wrapper.storage.engine_specific_config {
                    let config_value = serde_json::to_value(engine_config).context("Failed to serialize TiKV config")?;
                    serde_json::from_value(config_value).context("Failed to deserialize TiKV config")?
                } else {
                    TikvConfig {
                        storage_engine_type: config_wrapper.storage.storage_engine_type.clone(),
                        path: config_wrapper.storage.data_directory.clone(),
                        host: None,
                        port: None,
                        pd_endpoints: None,
                        username: None,
                        password: None,
                    }
                };
                Arc::new(TikvStorage::new(&tikv_config).await?)
            }
            _ => return Err(anyhow!("Unsupported storage engine type")),
        };
        Ok(persistent_engine)
    }

    pub async fn start(&self) -> Result<(), GraphError> {
        self.storage.start().await
    }

    pub async fn stop(&self) -> Result<(), GraphError> {
        self.storage.stop().await
    }

    pub async fn is_running(&self) -> bool {
        self.storage.is_running().await
    }

    pub async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        self.storage.query(query_string).await
    }

    pub async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.storage.create_vertex(vertex).await
    }

    pub async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        self.storage.get_vertex(id).await
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.storage.update_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.storage.delete_vertex(id).await
    }

    pub async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        self.storage.get_all_vertices().await
    }

    pub async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.storage.create_edge(edge).await
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        self.storage.get_edge(outbound_id, edge_type, inbound_id).await
    }

    pub async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.storage.update_edge(edge).await
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.storage.delete_edge(outbound_id, edge_type, inbound_id).await
    }

    pub async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        self.storage.get_all_edges().await
    }
}

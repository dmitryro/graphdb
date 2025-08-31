// lib/src/database.rs
// Created: 2025-08-09 - Implemented database management
// Fixed: 2025-08-13 - Replaced InMemoryStorage with InMemoryGraphStorage
// Fixed: 2025-08-13 - Updated imports to align with storage_engine/mod.rs
// Fixed: 2025-08-14 - Removed unresolved import `open_sled_db` and refactored its usage.
// Fixed: 2025-08-14 - Cleaned up unused imports.
// Fixed: 2025-08-16 - Resolved type mismatch by properly deserializing Sled and RocksDB configs.
// Fixed: 2025-08-17 - Added host and port to SledConfig and RocksdbConfig initializers.
// Fixed: 2025-08-17 - Corrected mismatched types for Option<String> and Option<u16> when building SledConfig and RocksdbConfig.
// Fixed: 2025-08-19 - Corrected field access on `StorageConfigWrapper` and a variant name typo.
// Fixed: 2025-08-19 - Changed `SledStorage` to `TikvStorage` in the TiKV arm of `load_engine`.
// Fixed: 2025-08-19 - Corrected the `serde_json::from_value` calls in `load_engine` to properly handle `HashMap` to `Value` conversion.
// Fixed: 2025-08-19 - Fixed HashMap to Map conversion using `serde_json::Map::from_iter()`.
// Fixed: 2025-08-30 - Removed incorrect `SelectedStorageConfig` handling and used flat `HashMap` for `engine_specific_config`.
// Fixed: 2025-08-30 - Fixed ownership errors by cloning `engine_specific_config` before mapping.

use anyhow::Context;
use async_trait::async_trait;
use log::{error, info, warn};
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use serde_json::{Value, Map};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;
use anyhow::{anyhow, Result};
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
        // Clone engine_specific_config to avoid partial move
        let engine_specific_config = config.engine_specific_config.clone().map(|map| {
            HashMap::from_iter(map.into_iter())
        });

        let mut new_config = config.clone();
        new_config.engine_specific_config = engine_specific_config;

        let storage: Arc<dyn GraphStorageEngine + Send + Sync> = match new_config.storage_engine_type {
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    // Deserialize the specific SledConfig from the flattened map
                    #[derive(serde::Deserialize)]
                    struct SledConfigMap {
                        path: PathBuf,
                        host: Option<String>,
                        port: Option<u16>,
                    }
                    let sled_config_map: SledConfigMap = serde_json::from_value(
                        serde_json::to_value(&new_config.engine_specific_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?
                    ).map_err(|e| GraphError::ConfigurationError(format!("Failed to parse SledConfigMap: {}", e)))?;

                    let sled_config = SledConfig {
                        storage_engine_type: new_config.storage_engine_type.clone(),
                        path: sled_config_map.path,
                        host: sled_config_map.host,
                        port: sled_config_map.port,
                    };
                    Arc::new(SledStorage::new(&sled_config).await?)
                }
                #[cfg(not(feature = "with-sled"))]
                {
                    return Err(GraphError::StorageError("Sled support is not enabled.".to_string()));
                }
            }
            StorageEngineType::InMemory => {
                Arc::new(InMemoryGraphStorage::new(&new_config))
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    // Deserialize the specific RocksdbConfig from the flattened map
                    #[derive(serde::Deserialize)]
                    struct RocksdbConfigMap {
                        path: PathBuf,
                        host: Option<String>,
                        port: Option<u16>,
                    }
                    let rocksdb_config_map: RocksdbConfigMap = serde_json::from_value(
                        serde_json::to_value(&new_config.engine_specific_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?
                    ).map_err(|e| GraphError::ConfigurationError(format!("Failed to parse RocksdbConfigMap: {}", e)))?;

                    let rocksdb_config = RocksdbConfig {
                        storage_engine_type: new_config.storage_engine_type.clone(),
                        path: rocksdb_config_map.path,
                        host: rocksdb_config_map.host,
                        port: rocksdb_config_map.port,
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
                    // Deserialize the specific TikvConfig from the flattened map
                    #[derive(serde::Deserialize)]
                    struct TikvConfigMap {
                        path: PathBuf,
                        host: Option<String>,
                        port: Option<u16>,
                        pd_endpoints: Option<String>,
                        password: Option<String>,
                        username: Option<String>,
                    }
                    let tikv_config_map: TikvConfigMap = serde_json::from_value(
                        serde_json::to_value(&new_config.engine_specific_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?
                    ).map_err(|e| GraphError::ConfigurationError(format!("Failed to parse TikvConfigMap: {}", e)))?;

                    let tikv_config = TikvConfig {
                        storage_engine_type: new_config.storage_engine_type.clone(),
                        path: tikv_config_map.path,
                        host: tikv_config_map.host,
                        port: tikv_config_map.port,
                        pd_endpoints: tikv_config_map.pd_endpoints,
                        username: tikv_config_map.username,
                        password: tikv_config_map.password,
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
                    let connection_string = new_config.connection_string.as_ref()
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
                    let connection_string = new_config.connection_string.as_ref()
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
                    let connection_string = new_config.connection_string.as_ref()
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
        // Clone engine_specific_config to avoid partial move
        let engine_specific_config = config_wrapper.storage.engine_specific_config.clone().map(|map| {
            HashMap::from_iter(map.into_iter())
        });

        let mut new_config = config_wrapper.storage.clone();
        new_config.engine_specific_config = engine_specific_config;

        let persistent_engine: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::Sled => {
                let sled_config: SledConfig = serde_json::from_value(
                    serde_json::to_value(&new_config.engine_specific_config)
                        .map_err(|e| anyhow!("Failed to serialize sled config: {}", e))?
                ).context("Failed to deserialize sled config")?;
                Arc::new(SledStorage::new(&sled_config).await?)
            }
            StorageEngineType::RocksDB => {
                let rocksdb_config: RocksdbConfig = serde_json::from_value(
                    serde_json::to_value(&new_config.engine_specific_config)
                        .map_err(|e| anyhow!("Failed to serialize rocksdb config: {}", e))?
                ).context("Failed to deserialize Rocksdb config")?;
                Arc::new(RocksdbStorage::new(&rocksdb_config)?)
            }
            StorageEngineType::TiKV => {
                let tikv_config: TikvConfig = serde_json::from_value(
                    serde_json::to_value(&new_config.engine_specific_config)
                        .map_err(|e| anyhow!("Failed to serialize tikv config: {}", e))?
                ).context("Failed to deserialize TiKV config")?;
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

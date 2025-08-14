// lib/src/database.rs
// Created: 2025-08-09 - Implemented database management
// Fixed: 2025-08-13 - Replaced InMemoryStorage with InMemoryGraphStorage
// Fixed: 2025-08-13 - Updated imports to align with storage_engine/mod.rs
// Fixed: 2025-08-14 - Removed unresolved import `open_sled_db` and refactored its usage.
// Fixed: 2025-08-14 - Cleaned up unused imports.
// Fixed: 2025-08-14 - Corrected import name from InMemoryGraphStorage to InMemoryStorage.
// Fixed: 2025-08-14 - Corrected SledStorage instantiation to pass &StorageConfig.

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
use sled::Db;

pub use crate::storage_engine::{GraphStorageEngine, StorageEngine};
pub use crate::storage_engine::config::{CliConfigToml, StorageConfig, StorageEngineType, format_engine_config, load_storage_config_from_yaml};
pub use crate::storage_engine::inmemory_storage::InMemoryStorage;
pub use crate::storage_engine::sled_storage::{SledStorage};
#[cfg(feature = "with-rocksdb")]
pub use crate::storage_engine::rocksdb_storage::RocksdbGraphStorage;
#[cfg(feature = "redis-datastore")]
pub use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
pub use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
pub use crate::storage_engine::mysql_storage::MySQLStorage;
#[cfg(feature = "redis-datastore")]
use redis::{Client, Connection};

pub struct Database {
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
}

impl Database {
    pub fn new(config: StorageConfig) -> Result<Self, GraphError> {
        let storage: Arc<dyn GraphStorageEngine + Send + Sync> = match config.storage_engine_type {
            StorageEngineType::Sled => {
                Arc::new(SledStorage::new(&config)?)
            }
            StorageEngineType::InMemory => {
                Arc::new(InMemoryStorage::new(&config))
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    Arc::new(RocksdbGraphStorage::new(&config)?)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    return Err(GraphError::StorageError("RocksDB support is not enabled.".to_string()));
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

    pub async fn start(&self) -> Result<(), GraphError> {
        self.storage.start().await
    }

    pub async fn stop(&self) -> Result<(), GraphError> {
        self.storage.stop().await
    }

    pub fn is_running(&self) -> bool {
        self.storage.is_running()
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

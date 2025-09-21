use anyhow::{anyhow, Result, Context};
use async_trait::async_trait;
use log::{error, info, warn};
use models::errors::GraphError;
use models::{Edge, Identifier, Vertex};
use serde_json::{Value, Map};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use uuid::Uuid;
use std::collections::HashMap;

use crate::config::{
    StorageConfig, SledConfig, RocksDBConfig, TikvConfig, StorageEngineType, StorageConfigWrapper, DEFAULT_DATA_DIRECTORY
};
use crate::storage_engine::{
    SledStorage,
    RocksDBStorage,
    TikvStorage,
    HybridStorage,
    InMemoryStorage,
};
use crate::daemon::daemon_management::is_storage_daemon_running;
#[cfg(feature = "redis-datastore")]
use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};

pub struct Database {
    pub storage: Arc<dyn GraphStorageEngine + Send + Sync>,
    pub config: StorageConfig,
}

impl Database {
    pub async fn new(config: StorageConfig) -> Result<Self, GraphError> {
        let storage: Arc<dyn GraphStorageEngine + Send + Sync> = match config.storage_engine_type {
            StorageEngineType::InMemory => {
                Arc::new(InMemoryStorage::new(&config))
            }
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    let sled_config = if let Some(ref engine_config) = config.engine_specific_config {
                        let config_value = serde_json::to_value(engine_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?;
                        serde_json::from_value(config_value)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to parse SledConfig: {}", e)))?
                    } else {
                        let port = config.default_port;
                        let path = config.data_directory.clone()
                            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                            .join("sled")
                            .join(port.to_string());
                        SledConfig {
                            storage_engine_type: config.storage_engine_type.clone(),
                            path,
                            host: Some("127.0.0.1".to_string()),
                            port: Some(port),
                            cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                            temporary: false,
                            use_compression: true,
                        }
                    };
                    // Check for existing daemon
                    let port = sled_config.port.unwrap_or(config.default_port);
                    if is_storage_daemon_running(port).await {
                        info!("Reusing existing Sled daemon on port {}", port);
                    }
                    Arc::new(SledStorage::new(&sled_config, &config).await?)
                }
                #[cfg(not(feature = "with-sled"))]
                return Err(GraphError::ConfigurationError("Sled feature not enabled".to_string()));
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    let rocksdb_config = if let Some(ref engine_config) = config.engine_specific_config {
                        let config_value = serde_json::to_value(engine_config)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize map: {}", e)))?;
                        serde_json::from_value(config_value)
                            .map_err(|e| GraphError::ConfigurationError(format!("Failed to parse RocksDBConfig: {}", e)))?
                    } else {
                        let port = config.default_port;
                        let path = config.data_directory.clone()
                            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                            .join("rocksdb")
                            .join(port.to_string());
                        RocksDBConfig {
                            storage_engine_type: config.storage_engine_type.clone(),
                            path,
                            host: Some("127.0.0.1".to_string()),
                            port: Some(port),
                            cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                            temporary: false,
                            use_compression: true,
                            use_raft_for_scale: false,
                            max_background_jobs: Some(1000),
                        }
                    };
                    // Check for existing daemon
                    let port = rocksdb_config.port.unwrap_or(config.default_port);
                    if is_storage_daemon_running(port).await {
                        info!("Reusing existing RocksDB daemon on port {}", port);
                    }
                    Arc::new(RocksDBStorage::new(&rocksdb_config, &config).await?)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                return Err(GraphError::ConfigurationError("RocksDB feature not enabled".to_string()));
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
                            path: config.data_directory.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
                            host: None,
                            port: None,
                            pd_endpoints: config.engine_specific_config.as_ref()
                                .and_then(|config| config.storage.pd_endpoints.clone()),
                            username: None,
                            password: None,
                        }
                    };
                    Arc::new(TikvStorage::new(&tikv_config).await?)
                }
                #[cfg(not(feature = "with-tikv"))]
                return Err(GraphError::ConfigurationError("TiKV feature not enabled".to_string()));
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let connection_string = config.engine_specific_config.as_ref()
                        .and_then(|config| config.storage.connection_string.as_ref())
                        .ok_or_else(|| GraphError::StorageError("Redis connection string is required".to_string()))?;
                    Arc::new(RedisStorage::new(connection_string)?)
                }
                #[cfg(not(feature = "redis-datastore"))]
                return Err(GraphError::ConfigurationError("Redis feature not enabled".to_string()));
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    let connection_string = config.engine_specific_config.as_ref()
                        .and_then(|config| config.storage.connection_string.as_ref())
                        .ok_or_else(|| GraphError::StorageError("PostgreSQL connection string is required".to_string()))?;
                    Arc::new(PostgresStorage::new(connection_string)?)
                }
                #[cfg(not(feature = "postgres-datastore"))]
                return Err(GraphError::ConfigurationError("PostgreSQL feature not enabled".to_string()));
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    let connection_string = config.engine_specific_config.as_ref()
                        .and_then(|config| config.storage.connection_string.as_ref())
                        .ok_or_else(|| GraphError::StorageError("MySQL connection string is required".to_string()))?;
                    Arc::new(MySQLStorage::new(connection_string)?)
                }
                #[cfg(not(feature = "mysql-datastore"))]
                return Err(GraphError::ConfigurationError("MySQL feature not enabled".to_string()));
            }
            StorageEngineType::Hybrid => {
                #[cfg(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv"))]
                {
                    let persistent = match config.engine_specific_config.as_ref()
                        .map(|config| config.storage_engine_type)
                    {
                        Some(StorageEngineType::Sled) => {
                            #[cfg(feature = "with-sled")]
                            {
                                let port = config.default_port;
                                let path = config.data_directory.clone()
                                    .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                                    .join("sled")
                                    .join(port.to_string());
                                let sled_config = SledConfig {
                                    storage_engine_type: StorageEngineType::Sled,
                                    path,
                                    host: Some("127.0.0.1".to_string()),
                                    port: Some(port),
                                    cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                                    temporary: false,
                                    use_compression: true,
                                };
                                // Check for existing daemon
                                if is_storage_daemon_running(port).await {
                                    info!("Reusing existing Sled daemon on port {}", port);
                                }
                                Arc::new(SledStorage::new(&sled_config, &config).await?)
                                    as Arc<dyn GraphStorageEngine + Send + Sync>
                            }
                            #[cfg(not(feature = "with-sled"))]
                            return Err(GraphError::ConfigurationError("Sled feature not enabled".to_string()));
                        }
                        Some(StorageEngineType::RocksDB) => {
                            #[cfg(feature = "with-rocksdb")]
                            {
                                let port = config.default_port;
                                let path = config.data_directory.clone()
                                    .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                                    .join("rocksdb")
                                    .join(port.to_string());
                                let rocksdb_config = RocksDBConfig {
                                    storage_engine_type: StorageEngineType::RocksDB,
                                    path,
                                    host: Some("127.0.0.1".to_string()),
                                    port: Some(port),
                                    cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                                    temporary: false,
                                    use_compression: true,
                                    use_raft_for_scale: false,
                                    max_background_jobs: Some(1000),
                                };
                                // Check for existing daemon
                                if is_storage_daemon_running(port).await {
                                    info!("Reusing existing RocksDB daemon on port {}", port);
                                }
                                Arc::new(RocksDBStorage::new(&rocksdb_config, &config).await?)
                                    as Arc<dyn GraphStorageEngine + Send + Sync>
                            }
                            #[cfg(not(feature = "with-rocksdb"))]
                            return Err(GraphError::ConfigurationError("RocksDB feature not enabled".to_string()));
                        }
                        Some(StorageEngineType::TiKV) => {
                            #[cfg(feature = "with-tikv")]
                            {
                                let tikv_config = TikvConfig {
                                    storage_engine_type: StorageEngineType::TiKV,
                                    path: config.data_directory.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
                                    host: None,
                                    port: None,
                                    pd_endpoints: config.engine_specific_config.as_ref()
                                        .and_then(|config| config.storage.pd_endpoints.clone()),
                                    username: None,
                                    password: None,
                                };
                                Arc::new(TikvStorage::new(&tikv_config).await?)
                                    as Arc<dyn GraphStorageEngine + Send + Sync>
                            }
                            #[cfg(not(feature = "with-tikv"))]
                            return Err(GraphError::ConfigurationError("TiKV feature not enabled".to_string()));
                        }
                        _ => return Err(GraphError::ConfigurationError("Unsupported persistent engine for Hybrid".to_string())),
                    };
                    Arc::new(HybridStorage::new(persistent))
                }
                #[cfg(not(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")))]
                return Err(GraphError::ConfigurationError("No persistent storage engines enabled for Hybrid".to_string()));
            }
        };
        Ok(Database { storage, config })
    }

    async fn load_engine(&self, config_wrapper: StorageConfigWrapper) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, anyhow::Error> {
        let engine_type = config_wrapper.storage.storage_engine_type.clone();
        let storage: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::InMemory => {
                Arc::new(InMemoryStorage::new(&config_wrapper.storage))
            }
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    let port = config_wrapper.storage.default_port;
                    let path = config_wrapper.storage.data_directory.clone()
                        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                        .join("sled")
                        .join(port.to_string());
                    let sled_config = SledConfig {
                        storage_engine_type: StorageEngineType::Sled,
                        path,
                        host: Some("127.0.0.1".to_string()),
                        port: Some(port),
                        cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                        temporary: false,
                        use_compression: true,
                    };
                    // Check for existing daemon
                    if is_storage_daemon_running(port).await {
                        info!("Reusing existing Sled daemon on port {}", port);
                    }
                    Arc::new(SledStorage::new(&sled_config, &config_wrapper.storage).await?)
                        as Arc<dyn GraphStorageEngine + Send + Sync>
                }
                #[cfg(not(feature = "with-sled"))]
                return Err(anyhow!("Sled support is not enabled."));
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    let port = config_wrapper.storage.default_port;
                    let path = config_wrapper.storage.data_directory.clone()
                        .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                        .join("rocksdb")
                        .join(port.to_string());
                    let rocksdb_config = RocksDBConfig {
                        storage_engine_type: StorageEngineType::RocksDB,
                        path,
                        host: Some("127.0.0.1".to_string()),
                        port: Some(port),
                        cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                        temporary: false,
                        use_compression: true,
                        use_raft_for_scale: false,
                        max_background_jobs: Some(1000),
                    };
                    // Check for existing daemon
                    if is_storage_daemon_running(port).await {
                        info!("Reusing existing RocksDB daemon on port {}", port);
                    }
                    Arc::new(RocksDBStorage::new(&rocksdb_config, &config_wrapper.storage).await?)
                        as Arc<dyn GraphStorageEngine + Send + Sync>
                }
                #[cfg(not(feature = "with-rocksdb"))]
                return Err(anyhow!("RocksDB support is not enabled."));
            }
            StorageEngineType::TiKV => {
                #[cfg(feature = "with-tikv")]
                {
                    let tikv_config = TikvConfig {
                        storage_engine_type: StorageEngineType::TiKV,
                        path: config_wrapper.storage.data_directory.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
                        host: None,
                        port: None,
                        pd_endpoints: config_wrapper.storage.engine_specific_config.as_ref()
                            .and_then(|config| config.storage.pd_endpoints.clone()),
                        username: None,
                        password: None,
                    };
                    Arc::new(TikvStorage::new(&tikv_config).await?)
                        as Arc<dyn GraphStorageEngine + Send + Sync>
                }
                #[cfg(not(feature = "with-tikv"))]
                return Err(anyhow!("TiKV support is not enabled."));
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let connection_string = config_wrapper.storage.engine_specific_config.as_ref()
                        .and_then(|config| config.storage.connection_string.as_ref())
                        .ok_or_else(|| anyhow!("Redis connection string is required"))?;
                    Arc::new(RedisStorage::new(connection_string)?)
                        as Arc<dyn GraphStorageEngine + Send + Sync>
                }
                #[cfg(not(feature = "redis-datastore"))]
                return Err(anyhow!("Redis support is not enabled."));
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    let connection_string = config_wrapper.storage.engine_specific_config.as_ref()
                        .and_then(|config| config.storage.connection_string.as_ref())
                        .ok_or_else(|| anyhow!("PostgreSQL connection string is required"))?;
                    Arc::new(PostgresStorage::new(connection_string)?)
                        as Arc<dyn GraphStorageEngine + Send + Sync>
                }
                #[cfg(not(feature = "postgres-datastore"))]
                return Err(anyhow!("PostgreSQL support is not enabled."));
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    let connection_string = config_wrapper.storage.engine_specific_config.as_ref()
                        .and_then(|config| config.storage.connection_string.as_ref())
                        .ok_or_else(|| anyhow!("MySQL connection string is required"))?;
                    Arc::new(MySQLStorage::new(connection_string)?)
                        as Arc<dyn GraphStorageEngine + Send + Sync>
                }
                #[cfg(not(feature = "mysql-datastore"))]
                return Err(anyhow!("MySQL support is not enabled."));
            }
            StorageEngineType::Hybrid => {
                #[cfg(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv"))]
                {
                    let persistent = match config_wrapper.storage.engine_specific_config.as_ref()
                        .map(|config| config.storage_engine_type)
                    {
                        Some(StorageEngineType::Sled) => {
                            #[cfg(feature = "with-sled")]
                            {
                                let port = config_wrapper.storage.default_port;
                                let path = config_wrapper.storage.data_directory.clone()
                                    .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                                    .join("sled")
                                    .join(port.to_string());
                                let sled_config = SledConfig {
                                    storage_engine_type: StorageEngineType::Sled,
                                    path,
                                    host: Some("127.0.0.1".to_string()),
                                    port: Some(port),
                                    cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                                    temporary: false,
                                    use_compression: true,
                                };
                                // Check for existing daemon
                                if is_storage_daemon_running(port).await {
                                    info!("Reusing existing Sled daemon on port {}", port);
                                }
                                Arc::new(SledStorage::new(&sled_config, &config_wrapper.storage).await?)
                                    as Arc<dyn GraphStorageEngine + Send + Sync>
                            }
                            #[cfg(not(feature = "with-sled"))]
                            return Err(anyhow!("Sled support is not enabled."));
                        }
                        Some(StorageEngineType::RocksDB) => {
                            #[cfg(feature = "with-rocksdb")]
                            {
                                let port = config_wrapper.storage.default_port;
                                let path = config_wrapper.storage.data_directory.clone()
                                    .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                                    .join("rocksdb")
                                    .join(port.to_string());
                                let rocksdb_config = RocksDBConfig {
                                    storage_engine_type: StorageEngineType::RocksDB,
                                    path,
                                    host: Some("127.0.0.1".to_string()),
                                    port: Some(port),
                                    cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                                    temporary: false,
                                    use_compression: true,
                                    use_raft_for_scale: false,
                                    max_background_jobs: Some(1000),
                                };
                                // Check for existing daemon
                                if is_storage_daemon_running(port).await {
                                    info!("Reusing existing RocksDB daemon on port {}", port);
                                }
                                Arc::new(RocksDBStorage::new(&rocksdb_config, &config_wrapper.storage).await?)
                                    as Arc<dyn GraphStorageEngine + Send + Sync>
                            }
                            #[cfg(not(feature = "with-rocksdb"))]
                            return Err(anyhow!("RocksDB support is not enabled."));
                        }
                        Some(StorageEngineType::TiKV) => {
                            #[cfg(feature = "with-tikv")]
                            {
                                let tikv_config = TikvConfig {
                                    storage_engine_type: StorageEngineType::TiKV,
                                    path: config_wrapper.storage.data_directory.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
                                    host: None,
                                    port: None,
                                    pd_endpoints: config_wrapper.storage.engine_specific_config.as_ref()
                                        .and_then(|config| config.storage.pd_endpoints.clone()),
                                    username: None,
                                    password: None,
                                };
                                Arc::new(TikvStorage::new(&tikv_config).await?)
                                    as Arc<dyn GraphStorageEngine + Send + Sync>
                            }
                            #[cfg(not(feature = "with-tikv"))]
                            return Err(anyhow!("TiKV support is not enabled."));
                        }
                        _ => return Err(anyhow!("Unsupported persistent engine for Hybrid")),
                    };
                    Arc::new(HybridStorage::new(persistent))
                }
                #[cfg(not(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")))]
                return Err(anyhow!("No persistent storage engines enabled for Hybrid"));
            }
        };
        Ok(storage)
    }

    pub fn get_storage_engine(&self) -> Arc<dyn StorageEngine + Send + Sync> {
        Arc::clone(&self.storage) as Arc<dyn StorageEngine + Send + Sync>
    }

    pub fn get_storage_engine_type(&self) -> StorageEngineType {
        self.config.storage_engine_type.clone()
    }

    pub fn get_persistent_engine_type(&self) -> Option<String> {
        if self.config.storage_engine_type == StorageEngineType::Hybrid {
            self.config.engine_specific_config.as_ref()
                .map(|config| config.storage_engine_type.to_string())
        } else {
            Some(self.config.storage_engine_type.to_string())
        }
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

    pub async fn close(&self) -> Result<(), GraphError> {
        self.storage.close().await
    }
}
// lib/src/storage_engine/mod.rs
// Created: 2025-08-09 - Declared storage engine submodules
// Updated: 2025-08-13 - Moved logic to storage_engine.rs, kept only module declarations and re-exports
// Added: 2025-08-13 - Added storage_utils module
// Fixed: 2025-08-15 - Corrected re-export of InMemoryStorage and removed non-existent open_sled_db function
// Updated: 2025-08-14 - Added detailed debugging for create_storage to trace RocksDB failures
// Fixed: 2025-08-14 - Fixed type mismatch for RocksDBStorage::new error handling
// Updated: 2025-09-01 - Implemented Hybrid match arm, fixed TiKV branch to use TikvConfig
// Updated: 2025-09-01 - Fixed TiKV deserialization, restored InMemoryGraphStorage alias, added start calls, improved Hybrid config and error messages
// Fixed: 2025-09-09 - Replaced incorrect enum variant matching on SelectedStorageConfig with struct field access,
//                     fixed Hybrid storage to default to Sled as persistent engine, and handled data_directory type mismatch
// Fixed: 2025-09-17 - Added missing storage_config argument to RocksDBStorage::new and awaited the async result
// Fixed: 2025-09-18 - Added missing fields to RocksDBConfig initializers to resolve compilation errors

use log::{info, error, warn, debug};
use std::sync::Arc;
use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};
use serde_json::Value;

// Declare submodules
pub mod config;
pub mod errors;
pub mod inmemory_storage;
pub mod raft_storage;
pub mod storage_utils;
pub mod storage_engine;
pub mod load_balancer;
pub mod sled_client;
pub mod zmq_client;
pub mod edge;
pub mod node;
pub mod types;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_raft_storage;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage;
#[cfg(feature = "with-tikv")]
pub mod tikv_storage;
#[cfg(feature = "redis-datastore")]
pub mod redis_storage;
#[cfg(feature = "postgres-datastore")]
pub mod postgres_storage;
#[cfg(feature = "mysql-datastore")]
pub mod mysql_storage;
#[cfg(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv"))]
pub mod hybrid_storage;
#[cfg(feature = "with-sled")]
pub mod sled_storage_daemon_pool;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage_daemon_pool;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_client;
// Re-export key items
pub use crate::config::{
    CliConfigToml, StorageConfig, StorageEngineType, RocksDBConfig, SledConfig, TikvConfig, HybridConfig,
    format_engine_config, load_storage_config_from_yaml, SelectedStorageConfig,
};
pub use inmemory_storage::{InMemoryStorage as InMemoryGraphStorage};
#[cfg(feature = "with-sled")]
pub mod sled_storage;
#[cfg(feature = "with-sled")]
pub use crate::config::{ SledStorage, SledDaemonPool };
#[cfg(feature = "with-sled")]
pub use storage_engine::{
    AsyncStorageEngineManager, GraphStorageEngine, StorageEngine, 
    SurrealdbGraphStorage,
    StorageEngineManager, emergency_cleanup_storage_engine_manager, init_storage_engine_manager, 
    GLOBAL_STORAGE_ENGINE_MANAGER, recover_sled, log_lock_file_diagnostics, lock_file_exists
};
pub use storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};

// Correctly re-export storage engines under feature flags
#[cfg(feature = "with-rocksdb")]
pub use config::RocksDBStorage;
#[cfg(feature = "with-tikv")]
pub use tikv_storage::TikvStorage;
#[cfg(feature = "redis-datastore")]
pub use redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
pub use postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
pub use mysql_storage::MySQLStorage;
#[cfg(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv"))]
pub use hybrid_storage::HybridStorage;
pub use inmemory_storage::InMemoryStorage;
pub use zmq_client::*;
pub use node::*;
pub use edge::*;
pub use types::*;
pub use errors::*;
pub use rocksdb_client::*;

/// Creates a storage engine instance based on the provided configuration.
///
/// Uses Sled as the default storage engine (as per StorageConfig::default).
/// Supports RocksDB (if "with-rocksdb" feature is enabled), InMemory, Redis (if "redis-datastore" feature is enabled),
/// PostgreSQL (if "postgres-datastore" feature is enabled), MySQL (if "mysql-datastore" feature is enabled),
/// and Hybrid (if any of "with-sled", "with-rocksdb", or "with-tikv" features are enabled).
pub async fn create_storage(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine>> {
    debug!("Creating storage with config: {:?}", config);
    debug!("Storage engine type: {:?}", config.storage_engine_type);
    debug!("Engine specific config: {:?}", config.engine_specific_config);
    debug!("Data directory: {:?}", config.data_directory);

    let storage: Arc<dyn GraphStorageEngine> = match config.storage_engine_type {
        StorageEngineType::RocksDB => {
            debug!("Attempting to create RocksDB storage");
            #[cfg(feature = "with-rocksdb")]
            {
                let rocksdb_config: RocksDBConfig = match config.engine_specific_config.as_ref() {
                    Some(selected) if selected.storage_engine_type == StorageEngineType::RocksDB => {
                        serde_json::from_value(serde_json::to_value(&selected.storage)?)
                            .map_err(|e| anyhow!("Failed to deserialize RocksDB config: {}", e))?
                    }
                    _ => RocksDBConfig {
                        storage_engine_type: StorageEngineType::RocksDB,
                        path: config.data_directory.clone().unwrap_or(PathBuf::from(config::DEFAULT_DATA_DIRECTORY)),
                        host: None,
                        port: None,
                        cache_capacity: None,
                        temporary: false, // Added missing field
                        use_compression: true, // Added missing field
                        use_raft_for_scale: false, // Added missing field
                        max_background_jobs: Some(1000),
                    },
                };

                match RocksDBStorage::new(&rocksdb_config, &config).await {
                    Ok(storage) => {
                        info!("Created RocksDB storage");
                        Arc::new(storage)
                    },
                    Err(e) => {
                        error!("Failed to create RocksDB storage: {}", e);
                        return Err(anyhow::Error::from(e));
                    }
                }
            }
            #[cfg(not(feature = "with-rocksdb"))]
            {
                error!("RocksDB support is not enabled in this build");
                return Err(anyhow!("RocksDB support is not enabled. Use Sled (default), InMemory, Redis, PostgreSQL, or MySQL.{}", if cfg!(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")) { " or Hybrid" } else { "" }));
            }
        }
        StorageEngineType::Hybrid => {
            debug!("Attempting to create Hybrid storage");
            #[cfg(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv"))]
            {
                // Default to Sled as the persistent engine if available, else RocksDB, then TiKV
                let persistent_engine = match (
                    cfg!(feature = "with-sled"),
                    cfg!(feature = "with-rocksdb"),
                    cfg!(feature = "with-tikv"),
                    config.engine_specific_config.as_ref(),
                ) {
                    (true, _, _, Some(selected)) if selected.storage_engine_type == StorageEngineType::Hybrid => "sled",
                    (true, _, _, None) => "sled",
                    (false, true, _, _) => "rocksdb",
                    (false, false, true, _) => "tikv",
                    _ => return Err(anyhow!("No supported persistent engine available for Hybrid storage. Enable 'with-sled', 'with-rocksdb', or 'with-tikv'.")),
                };

                let persistent: Arc<dyn GraphStorageEngine + Send + Sync> = match persistent_engine {
                    "sled" => {
                        #[cfg(feature = "with-sled")]
                        {
                            let sled_config: SledConfig = match config.engine_specific_config.as_ref() {
                                Some(selected) if selected.storage_engine_type == StorageEngineType::Hybrid => {
                                    serde_json::from_value(serde_json::to_value(&selected.storage)?)
                                        .map_err(|e| anyhow!("Failed to deserialize Sled config for Hybrid: {}", e))?
                                }
                                _ => SledConfig {
                                    storage_engine_type: StorageEngineType::Sled,
                                    path: config.data_directory.clone().unwrap_or(PathBuf::from(config::DEFAULT_DATA_DIRECTORY)).join("sled"),
                                    host: None,
                                    port: None,
                                    cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                                    temporary: false,
                                    use_compression: true,
                                },
                            };

                            match SledStorage::new(&sled_config, &config).await {
                                Ok(storage) => {
                                    info!("Created Sled storage for Hybrid");
                                    Arc::new(storage)
                                },
                                Err(e) => {
                                    error!("Failed to create Sled storage for Hybrid: {}", e);
                                    return Err(anyhow::Error::from(e));
                                }
                            }
                        }
                        #[cfg(not(feature = "with-sled"))]
                        return Err(anyhow!("Sled support is not enabled for Hybrid."));
                    }
                    "rocksdb" => {
                        #[cfg(feature = "with-rocksdb")]
                        {
                            let rocksdb_config: RocksDBConfig = match config.engine_specific_config.as_ref() {
                                Some(selected) if selected.storage_engine_type == StorageEngineType::Hybrid => {
                                    serde_json::from_value(serde_json::to_value(&selected.storage)?)
                                        .map_err(|e| anyhow!("Failed to deserialize RocksDB config for Hybrid: {}", e))?
                                }
                                _ => RocksDBConfig {
                                    storage_engine_type: StorageEngineType::RocksDB,
                                    path: config.data_directory.clone().unwrap_or(PathBuf::from(config::DEFAULT_DATA_DIRECTORY)).join("rocksdb"),
                                    host: None,
                                    port: None,
                                    cache_capacity: None,
                                    temporary: false, // Added missing field
                                    use_compression: true, // Added missing field
                                    use_raft_for_scale: false, // Added missing field
                                    max_background_jobs: Some(1000),
                                },
                            };

                            match RocksDBStorage::new(&rocksdb_config, &config).await {
                                Ok(storage) => {
                                    info!("Created RocksDB storage for Hybrid");
                                    Arc::new(storage)
                                },
                                Err(e) => {
                                    error!("Failed to create RocksDB storage for Hybrid: {}", e);
                                    return Err(anyhow::Error::from(e));
                                }
                            }
                        }
                        #[cfg(not(feature = "with-rocksdb"))]
                        return Err(anyhow!("RocksDB support is not enabled for Hybrid."));
                    }
                    "tikv" => {
                        #[cfg(feature = "with-tikv")]
                        {
                            let tikv_config: TikvConfig = match config.engine_specific_config.as_ref() {
                                Some(selected) if selected.storage_engine_type == StorageEngineType::Hybrid => {
                                    serde_json::from_value(serde_json::to_value(&selected.storage)?)
                                        .map_err(|e| anyhow!("Failed to deserialize TiKV config for Hybrid: {}", e))?
                                }
                                _ => TikvConfig {
                                    storage_engine_type: StorageEngineType::TiKV,
                                    path: config.data_directory.clone().unwrap_or(PathBuf::from(config::DEFAULT_DATA_DIRECTORY)).join("tikv"),
                                    host: None,
                                    port: None,
                                    pd_endpoints: None,
                                    username: None,
                                    password: None,
                                },
                            };

                            match TikvStorage::new(&tikv_config).await {
                                Ok(storage) => {
                                    info!("Created TiKV storage for Hybrid");
                                    Arc::new(storage)
                                },
                                Err(e) => {
                                    error!("Failed to create TiKV storage for Hybrid: {}", e);
                                    return Err(anyhow::Error::from(e));
                                }
                            }
                        }
                        #[cfg(not(feature = "with-tikv"))]
                        return Err(anyhow!("TiKV support is not enabled for Hybrid."));
                    }
                    _ => {
                        error!("Unsupported persistent engine for Hybrid: {}", persistent_engine);
                        return Err(anyhow!("Unsupported persistent engine for Hybrid: {}", persistent_engine));
                    }
                };
                info!("Created Hybrid storage with persistent engine: {}", persistent_engine);
                Arc::new(HybridStorage::new(persistent))
            }
            #[cfg(not(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")))]
            {
                error!("No persistent storage engines enabled for Hybrid");
                return Err(anyhow!("No persistent storage engines enabled for Hybrid. Enable 'with-sled', 'with-rocksdb', or 'with-tikv'."));
            }
        }
        StorageEngineType::Sled => {
            debug!("Attempting to create Sled storage");
            #[cfg(feature = "with-sled")]
            {
                let sled_config: SledConfig = match config.engine_specific_config.as_ref() {
                    Some(selected) if selected.storage_engine_type == StorageEngineType::Sled => {
                        serde_json::from_value(serde_json::to_value(&selected.storage)?)
                            .map_err(|e| anyhow!("Failed to deserialize Sled config: {}", e))?
                    }
                    _ => SledConfig {
                        storage_engine_type: StorageEngineType::Sled,
                        path: config.data_directory.clone().unwrap_or(PathBuf::from(config::DEFAULT_DATA_DIRECTORY)),
                        host: None,
                        port: None,
                        cache_capacity: Some(1024 * 1024 * 1024), // 1GB default
                        temporary: false,
                        use_compression: true,
                    },
                };

                match SledStorage::new(&sled_config, &config).await {
                    Ok(storage) => {
                        info!("Created Sled storage");
                        Arc::new(storage)
                    },
                    Err(e) => {
                        error!("Failed to create Sled storage: {}", e);
                        return Err(anyhow::Error::from(e));
                    }
                }
            }
            #[cfg(not(feature = "with-sled"))]
            {
                error!("Sled support is not enabled in this build");
                return Err(anyhow!("Sled support is not enabled. Use InMemory, Redis, PostgreSQL, or MySQL.{}", if cfg!(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")) { " or Hybrid" } else { "" }));
            }
        }
        StorageEngineType::TiKV => {
            debug!("Attempting to create TiKV storage");
            #[cfg(feature = "with-tikv")]
            {
                let tikv_config: TikvConfig = match config.engine_specific_config.as_ref() {
                    Some(selected) if selected.storage_engine_type == StorageEngineType::TiKV => {
                        serde_json::from_value(serde_json::to_value(&selected.storage)?)
                            .map_err(|e| anyhow!("Failed to deserialize TiKV config: {}", e))?
                    }
                    _ => return Err(anyhow!("TiKV configuration is missing.")),
                };

                match TikvStorage::new(&tikv_config).await {
                    Ok(storage) => {
                        info!("Created TiKV storage");
                        Arc::new(storage)
                    },
                    Err(e) => {
                        error!("Failed to create TiKV storage: {}", e);
                        return Err(anyhow::Error::from(e));
                    }
                }
            }
            #[cfg(not(feature = "with-tikv"))]
            {
                error!("TiKV support is not enabled in this build");
                return Err(anyhow!("TiKV support is not enabled. Use Sled (default), InMemory, Redis, PostgreSQL, or MySQL.{}", if cfg!(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")) { " or Hybrid" } else { "" }));
            }
        }
        StorageEngineType::InMemory => {
            debug!("Attempting to create InMemory storage");
            let storage = Arc::new(InMemoryGraphStorage::new(config));
            info!("Created InMemory storage");
            storage
        }
        StorageEngineType::Redis => {
            debug!("Attempting to create Redis storage");
            #[cfg(feature = "redis-datastore")]
            {
                let client = redis::Client::open(config.connection_string.as_ref()
                    .ok_or_else(|| {
                        error!("Redis connection string is missing");
                        anyhow!("Redis connection string is required")
                    })?)?;
                let connection = client.get_connection()
                    .map_err(|e| {
                        error!("Failed to connect to Redis: {}", e);
                        anyhow!("Failed to connect to Redis: {}", e)
                    })?;
                match RedisStorage::new(connection) {
                    Ok(storage) => {
                        info!("Created Redis storage");
                        Arc::new(storage)
                    },
                    Err(e) => {
                        error!("Failed to create Redis storage: {}", e);
                        return Err(anyhow::Error::from(e));
                    }
                }
            }
            #[cfg(not(feature = "redis-datastore"))]
            {
                error!("Redis support is not enabled in this build");
                return Err(anyhow!("Redis support is not enabled. Use Sled (default), InMemory, PostgreSQL, or MySQL.{}", if cfg!(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")) { " or Hybrid" } else { "" }));
            }
        }
        StorageEngineType::PostgreSQL => {
            debug!("Attempting to create PostgreSQL storage");
            #[cfg(feature = "postgres-datastore")]
            {
                match PostgresStorage::new(config) {
                    Ok(storage) => {
                        info!("Created PostgreSQL storage");
                        Arc::new(storage)
                    },
                    Err(e) => {
                        error!("Failed to create PostgreSQL storage: {}", e);
                        return Err(anyhow::Error::from(e));
                    }
                }
            }
            #[cfg(not(feature = "postgres-datastore"))]
            {
                error!("PostgreSQL support is not enabled in this build");
                return Err(anyhow!("PostgreSQL support is not enabled. Use Sled (default), InMemory, Redis, or MySQL.{}", if cfg!(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")) { " or Hybrid" } else { "" }));
            }
        }
        StorageEngineType::MySQL => {
            debug!("Attempting to create MySQL storage");
            #[cfg(feature = "mysql-datastore")]
            {
                match MySQLStorage::new(config) {
                    Ok(storage) => {
                        info!("Created MySQL storage");
                        Arc::new(storage)
                    },
                    Err(e) => {
                        error!("Failed to create MySQL storage: {}", e);
                        return Err(anyhow::Error::from(e));
                    }
                }
            }
            #[cfg(not(feature = "mysql-datastore"))]
            {
                error!("MySQL support is not enabled in this build");
                return Err(anyhow!("MySQL support is not enabled. Use Sled (default), InMemory, Redis, or PostgreSQL.{}", if cfg!(any(feature = "with-sled", feature = "with-rocksdb", feature = "with-tikv")) { " or Hybrid" } else { "" }));
            }
        }
    };

    // Start the storage engine to ensure it's initialized
    debug!("Starting storage engine: {:?}", config.storage_engine_type);
    storage.start().await.map_err(|e| {
        error!("Failed to start storage engine {}: {}", config.storage_engine_type, e);
        anyhow!("Failed to start storage engine {}: {}", config.storage_engine_type, e)
    })?;

    Ok(storage)
}

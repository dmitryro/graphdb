// lib/src/storage_engine/mod.rs

// Created: 2025-08-09 - Declared storage engine submodules
// Updated: 2025-08-13 - Moved logic to storage_engine.rs, kept only module declarations and re-exports
// Added: 2025-08-13 - Added storage_utils module
// Fixed: 2025-08-15 - Corrected re-export of InMemoryStorage and removed non-existent open_sled_db function.
// Updated: 2025-08-14 - Added detailed debugging for create_storage to trace RocksDB failures
// Fixed: 2025-08-14 - Fixed type mismatch for RocksdbStorage::new error handling

use log::{info, error, warn, debug};
use std::sync::Arc;
use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};

// Declare submodules
pub mod config;
pub mod inmemory_storage;
pub mod storage_utils;
pub mod storage_engine;
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

// Re-export key items
pub use config::{CliConfigToml, StorageConfig, StorageEngineType, RocksdbConfig, SledConfig, TikvConfig,
                 format_engine_config, load_storage_config_from_yaml};
pub use inmemory_storage::{InMemoryStorage as InMemoryGraphStorage};
#[cfg(feature = "with-sled")]
pub mod sled_storage;
#[cfg(feature = "with-sled")]
pub use self::sled_storage::SledStorage;
#[cfg(feature = "with-sled")]
pub use storage_engine::{ AsyncStorageEngineManager, GraphStorageEngine, HybridStorageEngine, StorageEngine, 
                          SurrealdbGraphStorage,
                          StorageEngineManager, emergency_cleanup_storage_engine_manager, init_storage_engine_manager, 
                          GLOBAL_STORAGE_ENGINE_MANAGER, recover_sled, log_lock_file_diagnostics, lock_file_exists };
pub use storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};

// Correctly re-export RocksdbStorage from its module under the feature flag
#[cfg(feature = "with-rocksdb")]
pub use rocksdb_storage::RocksdbStorage;
#[cfg(feature = "with-tikv")]
pub use tikv_storage::TikvStorage;
#[cfg(feature = "redis-datastore")]
pub use redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
pub use postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
pub use mysql_storage::MySQLStorage;

/// Creates a storage engine instance based on the provided configuration.
///
/// Uses Sled as the default storage engine (as per StorageConfig::default).
/// Supports RocksDB (if "with-rocksdb" feature is enabled), InMemory, Redis (if "redis-datastore" feature is enabled),
/// PostgreSQL (if "postgres-datastore" feature is enabled), and MySQL (if "mysql-datastore" feature is enabled) storage.
pub async fn create_storage(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine>> {
    debug!("Creating storage with config: {:?}", config);
    debug!("Storage engine type: {:?}", config.storage_engine_type);
    debug!("Engine specific config: {:?}", config.engine_specific_config);
    debug!("Data directory: {:?}", config.data_directory);

    match config.storage_engine_type {
        StorageEngineType::RocksDB => {
            debug!("Attempting to create RocksDB storage");
            #[cfg(feature = "with-rocksdb")]
            {
                // Correctly deserialize the specific RocksdbConfig from the generic HashMap
                let rocksdb_config: RocksdbConfig = serde_json::from_value(
                    config.engine_specific_config
                        .as_ref()
                        .ok_or_else(|| anyhow!("RocksDB configuration is missing."))?
                        .get("rocksdb")
                        .ok_or_else(|| anyhow!("RocksDB configuration not found in engine_specific_config."))?
                        .clone()
                ).map_err(|e| anyhow!("Failed to deserialize RocksDB config: {}", e))?;

                match RocksdbStorage::new(&rocksdb_config) {
                    Ok(storage) => {
                        info!("Created RocksDB storage");
                        Ok(Arc::new(storage))
                    },
                    Err(e) => {
                        error!("Failed to create RocksDB storage: {}", e);
                        Err(anyhow::Error::from(e))
                    }
                }
            }
            #[cfg(not(feature = "with-rocksdb"))]
            {
                error!("RocksDB support is not enabled in this build");
                Err(anyhow!("RocksDB support is not enabled. Use Sled (default), InMemory, Redis, PostgreSQL, or MySQL."))
            }
        }
        StorageEngineType::Sled => {
            debug!("Attempting to create Sled storage");
            #[cfg(feature = "with-sled")]
            {
                // Correctly deserialize the specific SledConfig from the generic HashMap
                let sled_config: SledConfig = serde_json::from_value(
                    config.engine_specific_config
                        .as_ref()
                        .ok_or_else(|| anyhow!("Sled configuration is missing."))?
                        .get("sled")
                        .ok_or_else(|| anyhow!("Sled configuration not found in engine_specific_config."))?
                        .clone()
                ).map_err(|e| anyhow!("Failed to deserialize Sled config: {}", e))?;

                match SledStorage::new(&sled_config).await {
                    Ok(storage) => {
                        info!("Created Sled storage");
                        Ok(Arc::new(storage) as Arc<dyn GraphStorageEngine>)
                    },
                    Err(e) => {
                        error!("Failed to create Sled storage: {}", e);
                        Err(anyhow::Error::from(e))
                    }
                }
            }
            #[cfg(not(feature = "with-sled"))]
            {
                error!("Sled support is not enabled in this build");
                Err(anyhow!("Sled support is not enabled. Please enable the 'with-sled' feature."))
            }
        }

        StorageEngineType::TiKV => {
            debug!("Attempting to create TiKV storage");
            #[cfg(feature = "with-tikv")]
            {
                // Correctly deserialize the specific SledConfig from the generic HashMap
                let tikv_config: SledConfig = serde_json::from_value(
                    config.engine_specific_config
                        .as_ref()
                        .ok_or_else(|| anyhow!("TiKV configuration is missing."))?
                        .get("sled")
                        .ok_or_else(|| anyhow!("TiKV configuration not found in engine_specific_config."))?
                        .clone()
                ).map_err(|e| anyhow!("Failed to deserialize Sled config: {}", e))?;

                match SledStorage::new(&tikv_config).await {
                    Ok(storage) => {
                        info!("Created Sled storage");
                        Ok(Arc::new(storage) as Arc<dyn GraphStorageEngine>)
                    },
                    Err(e) => {
                        error!("Failed to create Sled storage: {}", e);
                        Err(anyhow::Error::from(e))
                    }
                }
            }
            #[cfg(not(feature = "with-sled"))]
            {
                error!("TiKV support is not enabled in this build");
                Err(anyhow!("TiKV support is not enabled. Please enable the 'with-sled' feature."))
            }
        }

        StorageEngineType::InMemory => {
            debug!("Attempting to create InMemory storage");
            info!("Created InMemory storage");
            Ok(Arc::new(InMemoryGraphStorage::new(config)) as Arc<dyn GraphStorageEngine>)
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
                        Ok(Arc::new(storage) as Arc<dyn GraphStorageEngine>)
                    },
                    Err(e) => {
                        error!("Failed to create Redis storage: {}", e);
                        Err(anyhow::Error::from(e)) // Convert GraphError to anyhow::Error
                    }
                }
            }
            #[cfg(not(feature = "redis-datastore"))]
            {
                error!("Redis support is not enabled in this build");
                Err(anyhow!("Redis support is not enabled. Use Sled (default), InMemory, PostgreSQL, or MySQL."))
            }
        }
        StorageEngineType::PostgreSQL => {
            debug!("Attempting to create PostgreSQL storage");
            #[cfg(feature = "postgres-datastore")]
            {
                match PostgresStorage::new(config) {
                    Ok(storage) => {
                        info!("Created PostgreSQL storage");
                        Ok(Arc::new(storage) as Arc<dyn GraphStorageEngine>)
                    },
                    Err(e) => {
                        error!("Failed to create PostgreSQL storage: {}", e);
                        Err(anyhow::Error::from(e)) // Convert GraphError to anyhow::Error
                    }
                }
            }
            #[cfg(not(feature = "postgres-datastore"))]
            {
                error!("PostgreSQL support is not enabled in this build");
                Err(anyhow!("PostgreSQL support is not enabled. Use Sled (default), InMemory, Redis, or MySQL."))
            }
        }
        StorageEngineType::MySQL => {
            debug!("Attempting to create MySQL storage");
            #[cfg(feature = "mysql-datastore")]
            {
                match MySQLStorage::new(config) {
                    Ok(storage) => {
                        info!("Created MySQL storage");
                        Ok(Arc::new(storage) as Arc<dyn GraphStorageEngine>)
                    },
                    Err(e) => {
                        error!("Failed to create MySQL storage: {}", e);
                        Err(anyhow::Error::from(e)) // Convert GraphError to anyhow::Error
                    }
                }
            }
            #[cfg(not(feature = "mysql-datastore"))]
            {
                error!("MySQL support is not enabled in this build");
                Err(anyhow!("MySQL support is not enabled. Use Sled (default), InMemory, Redis, or PostgreSQL."))
            }
        }
    }
}

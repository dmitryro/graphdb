// lib/src/storage_engine/mod.rs
// Created: 2025-08-09 - Declared storage engine submodules
// Updated: 2025-08-13 - Moved logic to storage_engine.rs, kept only module declarations and re-exports
// Added: 2025-08-13 - Added storage_utils module
// Fixed: 2025-08-15 - Corrected re-export of InMemoryStorage and removed non-existent open_sled_db function.

// Declare submodules
pub mod config;
pub mod inmemory_storage;
pub mod sled_storage;
pub mod storage_utils;
pub mod storage_engine;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage;
#[cfg(feature = "redis-datastore")]
pub mod redis_storage;
#[cfg(feature = "postgres-datastore")]
pub mod postgres_storage;
#[cfg(feature = "mysql-datastore")]
pub mod mysql_storage;

// Re-export key items
pub use config::{CliConfigToml, StorageConfig, StorageEngineType, format_engine_config, load_storage_config_from_yaml};
pub use crate::storage_engine::inmemory_storage::{InMemoryStorage as InMemoryGraphStorage};
pub use sled_storage::SledStorage;
pub use storage_engine::{GraphStorageEngine, HybridStorageEngine, StorageEngine, StorageEngineManager, init_storage_engine_manager, GLOBAL_STORAGE_ENGINE_MANAGER};
pub use storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
#[cfg(feature = "with-rocksdb")]
pub use rocksdb_storage::RocksdbGraphStorage;
#[cfg(feature = "redis-datastore")]
pub use redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
pub use postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
pub use mysql_storage::MySQLStorage;

// Import necessary items for the create_storage function
use std::sync::Arc;
use anyhow::{Result, anyhow};
use std::path::{Path, PathBuf};

/// Creates a storage engine instance based on the provided configuration.
///
/// Uses Sled as the default storage engine (as per StorageConfig::default).
/// Supports RocksDB (if "with-rocksdb" feature is enabled), InMemory, Redis (if "redis-datastore" feature is enabled),
/// PostgreSQL (if "postgres-datastore" feature is enabled), and MySQL (if "mysql-datastore" feature is enabled) storage.
pub fn create_storage(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine>> {
    match config.storage_engine_type {
        StorageEngineType::Sled => {
            // SledStorage::new now expects a reference to the config and returns a Result
            Ok(Arc::new(SledStorage::new(config)?) as Arc<dyn GraphStorageEngine>)
        }
        StorageEngineType::RocksDB => {
            #[cfg(feature = "with-rocksdb")]
            {
                // RocksDBStorage::new now expects a reference to the config
                Ok(Arc::new(RocksdbGraphStorage::new(config)?) as Arc<dyn GraphStorageEngine>)
            }
            #[cfg(not(feature = "with-rocksdb"))]
            {
                Err(anyhow!("RocksDB support is not enabled. Use Sled (default), InMemory, Redis, PostgreSQL, or MySQL."))
            }
        }
        StorageEngineType::InMemory => {
            // InMemoryGraphStorage::new now expects a reference to the config
            // and does not return a Result, so the '?' is removed
            Ok(Arc::new(InMemoryGraphStorage::new(config)) as Arc<dyn GraphStorageEngine>)
        }
        StorageEngineType::Redis => {
            #[cfg(feature = "redis-datastore")]
            {
                let client = redis::Client::open(config.connection_string.as_ref()
                    .ok_or_else(|| anyhow!("Redis connection string is required"))?)?;
                let connection = client.get_connection()
                    .map_err(|e| anyhow!("Failed to connect to Redis: {}", e))?;
                Ok(Arc::new(RedisStorage::new(connection)?) as Arc<dyn GraphStorageEngine>)
            }
            #[cfg(not(feature = "redis-datastore"))]
            {
                Err(anyhow!("Redis support is not enabled. Use Sled (default), InMemory, PostgreSQL, or MySQL."))
            }
        }
        StorageEngineType::PostgreSQL => {
            #[cfg(feature = "postgres-datastore")]
            {
                Ok(Arc::new(PostgresStorage::new(config)?) as Arc<dyn GraphStorageEngine>)
            }
            #[cfg(not(feature = "postgres-datastore"))]
            {
                Err(anyhow!("PostgreSQL support is not enabled. Use Sled (default), InMemory, Redis, or MySQL."))
            }
        }
        StorageEngineType::MySQL => {
            #[cfg(feature = "mysql-datastore")]
            {
                Ok(Arc::new(MySQLStorage::new(config)?) as Arc<dyn GraphStorageEngine>)
            }
            #[cfg(not(feature = "mysql-datastore"))]
            {
                Err(anyhow!("MySQL support is not enabled. Use Sled (default), InMemory, Redis, or PostgreSQL."))
            }
        }
    }
}

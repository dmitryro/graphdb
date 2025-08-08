// lib/src/storage_engine/mod.rs
// Fixed: 2025-08-07 - Resolved import conflicts and missing dependencies
// Fixed: 2025-08-07 - Coerced GraphError to anyhow::Error in create_storage
// Updated: 2025-08-08 - Added Redis, PostgreSQL, and MySQL storage engines to create_storage
// Updated: 2025-08-08 - Added module declarations and re-exports for redis_storage, postgres_storage, and mysql_storage
// Fixed: 2025-08-08 - Added feature gates for Redis and PostgreSQL to align with Cargo.toml

// Module declarations
pub mod storage_engine;
pub mod sled_storage;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage;
pub mod inmemory_storage;
#[cfg(feature = "redis-datastore")]
pub mod redis_storage;
#[cfg(feature = "postgres-datastore")]
pub mod postgres_storage;
pub mod mysql_storage;
pub mod config;
pub mod storage_utils;

// Re-export key types and traits for external use
pub use storage_engine::{GraphStorageEngine, StorageEngine};
pub use sled_storage::{SledStorage, open_sled_db};
#[cfg(feature = "with-rocksdb")]
pub use rocksdb_storage::RocksDBStorage;
pub use inmemory_storage::InMemoryStorage;
#[cfg(feature = "redis-datastore")]
pub use redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
pub use postgres_storage::PostgresStorage;
pub use mysql_storage::MySQLStorage;
pub use config::{StorageConfig, StorageEngineType};

// Required imports for the create_storage function
use anyhow::Result;
use std::sync::Arc;
#[cfg(feature = "redis-datastore")]
use redis::Client as RedisClient;

/// Creates a storage engine instance based on the provided configuration.
/// 
/// Uses Sled as the default storage engine (as per StorageConfig::default).
/// Supports RocksDB (if "with-rocksdb" feature is enabled), InMemory, Redis (if "redis-datastore" feature is enabled),
/// PostgreSQL (if "postgres-datastore" feature is enabled), and MySQL storage.
pub fn create_storage(config: StorageConfig) -> Result<Arc<dyn GraphStorageEngine>> {
    match config.engine_type {
        StorageEngineType::Sled => {
            let db = open_sled_db(&config.data_path)?;
            Ok(SledStorage::new(db).map(|storage| Arc::new(storage) as Arc<dyn GraphStorageEngine>)?)
        }
        StorageEngineType::RocksDB => {
            #[cfg(feature = "with-rocksdb")]
            {
                Ok(RocksDBStorage::new(config).map(|storage| Arc::new(storage) as Arc<dyn GraphStorageEngine>)?)
            }
            #[cfg(not(feature = "with-rocksdb"))]
            {
                Err(anyhow::anyhow!("RocksDB support is not enabled. Use Sled (default), InMemory, Redis, PostgreSQL, or MySQL."))
            }
        }
        StorageEngineType::InMemory => {
            Ok(InMemoryStorage::new(config).map(|storage| Arc::new(storage) as Arc<dyn GraphStorageEngine>)?)
        }
        #[cfg(feature = "redis-datastore")]
        StorageEngineType::Redis => {
            let client = RedisClient::open(config.connection_string.as_ref()
                .ok_or_else(|| anyhow::anyhow!("Redis connection string is required"))?)?;
            let connection = client.get_connection()
                .map_err(|e| anyhow::anyhow!("Failed to connect to Redis: {}", e))?;
            Ok(RedisStorage::new(connection).map(|storage| Arc::new(storage) as Arc<dyn GraphStorageEngine>)?)
        }
        #[cfg(not(feature = "redis-datastore"))]
        StorageEngineType::Redis => {
            Err(anyhow::anyhow!("Redis support is not enabled. Use Sled (default), InMemory, PostgreSQL, or MySQL."))
        }
        #[cfg(feature = "postgres-datastore")]
        StorageEngineType::PostgreSQL => {
            Ok(PostgresStorage::new(&config).map(|storage| Arc::new(storage) as Arc<dyn GraphStorageEngine>)?)
        }
        #[cfg(not(feature = "postgres-datastore"))]
        StorageEngineType::PostgreSQL => {
            Err(anyhow::anyhow!("PostgreSQL support is not enabled. Use Sled (default), InMemory, Redis, or MySQL."))
        }
        StorageEngineType::MySQL => {
            Ok(MySQLStorage::new(&config).map(|storage| Arc::new(storage) as Arc<dyn GraphStorageEngine>)?)
        }
    }
}
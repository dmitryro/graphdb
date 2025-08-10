// lib/src/storage_engine/mod.rs
// Fixed: 2025-08-07 - Resolved import conflicts and missing dependencies
// Fixed: 2025-08-07 - Coerced GraphError to anyhow::Error in create_storage
// Updated: 2025-08-08 - Added Redis, PostgreSQL, and MySQL storage engines to create_storage
// Updated: 2025-08-08 - Added module declarations and re-exports for redis_storage, postgres_storage, and mysql_storage
// Fixed: 2025-08-08 - Added feature gates for Redis and PostgreSQL to align with Cargo.toml
// Fixed: 2025-08-08 - Removed circular module declaration for `storage_engine`.
// Fixed: 2025-08-08 - Added feature gates for MySQL to align with Cargo.toml.
// Fixed: 2025-08-08 - Refactored `create_storage` to use idiomatic `Result` handling.
// Fixed: 2025-08-09 - Removed incorrect import of SerializableInternString and handled Option<PathBuf> in create_storage.
// Fixed: 2025-08-09 - Fixed E0282 for data_directory by using explicit AsRef<Path>
// Module declarations
// Note: This file is mod.rs, so it defines the `storage_engine` module itself.
// A declaration for `storage_engine` would be a circular reference.
pub mod storage_engine; // Assuming this refers to a sibling file, not this mod.rs.
pub mod sled_storage;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage;
pub mod inmemory_storage;
#[cfg(feature = "redis-datastore")]
pub mod redis_storage;
#[cfg(feature = "postgres-datastore")]
pub mod postgres_storage;
#[cfg(feature = "mysql-datastore")]
pub mod mysql_storage;
pub mod config;
pub mod storage_utils;

// Re-export key types and traits for external use
pub use storage_engine::{GraphStorageEngine, 
                         StorageEngine,
                         StorageEngineManager,
                         GLOBAL_STORAGE_ENGINE_MANAGER};
pub use sled_storage::{SledStorage, open_sled_db};
#[cfg(feature = "with-rocksdb")]
pub use rocksdb_storage::RocksDBStorage;
pub use inmemory_storage::InMemoryStorage;
#[cfg(feature = "redis-datastore")]
pub use redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
pub use postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
pub use mysql_storage::MySQLStorage;
pub use config::{StorageConfig, StorageEngineType};

// Required imports for the create_storage function
use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(feature = "redis-datastore")]
use redis::Client as RedisClient;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
use models::identifiers::Identifier;

/// Creates a storage engine instance based on the provided configuration.
///
/// Uses Sled as the default storage engine (as per StorageConfig::default).
/// Supports RocksDB (if "with-rocksdb" feature is enabled), InMemory, Redis (if "redis-datastore" feature is enabled),
/// PostgreSQL (if "postgres-datastore" feature is enabled), and MySQL (if "mysql-datastore" feature is enabled) storage.
pub fn create_storage(config: StorageConfig) -> Result<Arc<dyn GraphStorageEngine>> {
    match config.storage_engine_type {
        StorageEngineType::Sled => {
            let db = open_sled_db(
                <PathBuf as AsRef<Path>>::as_ref(&config.data_directory)
            )?;
            Ok(Arc::new(SledStorage::new(db)?) as Arc<dyn GraphStorageEngine>)
        }
        StorageEngineType::RocksDB => {
            #[cfg(feature = "with-rocksdb")]
            {
                Ok(Arc::new(RocksDBStorage::new(config)?) as Arc<dyn GraphStorageEngine>)
            }
            #[cfg(not(feature = "with-rocksdb"))]
            {
                Err(anyhow!("RocksDB support is not enabled. Use Sled (default), InMemory, Redis, PostgreSQL, or MySQL."))
            }
        }
        StorageEngineType::InMemory => {
            Ok(Arc::new(InMemoryStorage::new(config)?) as Arc<dyn GraphStorageEngine>)
        }
        StorageEngineType::Redis => {
            #[cfg(feature = "redis-datastore")]
            {
                let client = RedisClient::open(config.connection_string.as_ref()
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
                Ok(Arc::new(PostgresStorage::new(&config)?) as Arc<dyn GraphStorageEngine>)
            }
            #[cfg(not(feature = "postgres-datastore"))]
            {
                Err(anyhow!("PostgreSQL support is not enabled. Use Sled (default), InMemory, Redis, or MySQL."))
            }
        }
        StorageEngineType::MySQL => {
            #[cfg(feature = "mysql-datastore")]
            {
                Ok(Arc::new(MySQLStorage::new(&config)?) as Arc<dyn GraphStorageEngine>)
            }
            #[cfg(not(feature = "mysql-datastore"))]
            {
                Err(anyhow!("MySQL support is not enabled. Use Sled (default), InMemory, Redis, or PostgreSQL."))
            }
        }
    }
}
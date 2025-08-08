// lib/src/storage_engine/mod.rs
// Fixed: 2025-08-07 - Resolved import conflicts and missing dependencies
// Fixed: 2025-08-07 - Coerced GraphError to anyhow::Error in create_storage

// Module declarations
pub mod storage_engine;
pub mod sled_storage;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage;
pub mod inmemory_storage;
pub mod config;
pub mod storage_utils;

// Re-export key types and traits for external use
pub use storage_engine::{GraphStorageEngine, StorageEngine};
pub use sled_storage::{SledStorage, open_sled_db};
#[cfg(feature = "with-rocksdb")]
pub use rocksdb_storage::RocksDBStorage;
pub use inmemory_storage::InMemoryStorage;
pub use config::{StorageConfig, StorageEngineType};

// Required imports for the create_storage function
use anyhow::Result;
use std::sync::Arc;

/// Creates a storage engine instance based on the provided configuration.
/// 
/// Uses Sled as the default storage engine (as per StorageConfig::default).
/// Supports RocksDB (if the "with-rocksdb" feature is enabled) and InMemory storage.
pub fn create_storage(config: StorageConfig) -> Result<Arc<dyn GraphStorageEngine>> {
    match config.engine_type {
        StorageEngineType::Sled => {
            // Sled is the default storage engine, as defined in StorageConfig::default
            // Use the open_sled_db function and then create SledStorage
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
                Err(anyhow::anyhow!("RocksDB support is not enabled. Use Sled (default) or InMemory."))
            }
        }
        StorageEngineType::InMemory => {
            // InMemory storage for lightweight or testing scenarios
            Ok(InMemoryStorage::new(config).map(|storage| Arc::new(storage) as Arc<dyn GraphStorageEngine>)?)
        }
    }
}
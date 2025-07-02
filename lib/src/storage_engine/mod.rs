// lib/src/storage_engine/mod.rs
// Corrected: 2025-07-02 - Fixed module re-exports to resolve import errors.

pub mod storage_engine; // Declares the module defined in storage_engine.rs
pub mod sled_storage;
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage;
pub mod config;

// Re-export key traits and structs for easier access from `crate::storage_engine::*`
pub use self::storage_engine::{StorageEngine, GraphStorageEngine};
pub use self::sled_storage::{SledGraphStorage, open_sled_db}; // SledGraphStorage is the actual implementation struct
#[cfg(feature = "with-rocksdb")]
pub use self::rocksdb_storage::RocksDBStorage;
pub use self::config::{StorageConfig, StorageEngineType};


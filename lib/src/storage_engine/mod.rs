// lib/src/storage_engine/mod.rs
// Corrected: 2025-07-02 - Fixed module re-exports to resolve import errors.
// Refactored: 2025-07-02 - Added storage_utils module for common helper functions.

pub mod storage_engine; // Declares the module defined in storage_engine.rs (now trait-only)
pub mod sled_storage; // Declares the module for Sled implementation
#[cfg(feature = "with-rocksdb")]
pub mod rocksdb_storage; // Declares the module for RocksDB implementation
pub mod config;
pub mod storage_utils; // Declare the new utility module

// Re-export key traits and structs for easier access from `crate::storage_engine::*`
pub use self::storage_engine::{StorageEngine, GraphStorageEngine};
// Corrected: Re-export SledStorage (not SledGraphStorage)
pub use self::sled_storage::{SledStorage, open_sled_db};
#[cfg(feature = "with-rocksdb")]
pub use self::rocksdb_storage::RocksDBStorage;
pub use self::config::{StorageConfig, StorageEngineType};
// No need to re-export individual functions from storage_utils here unless they are meant for direct top-level use.
// They will be imported directly by sled_storage and rocksdb_storage.


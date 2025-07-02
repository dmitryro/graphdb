// lib/src/storage_engine/config.rs

use serde::{Serialize, Deserialize};

/// Enum to specify the type of storage engine to use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageEngineType {
    Sled,
    RocksDB,
    // Add other storage types like PostgreSQL, Redis, etc., as needed
}

/// Configuration for the storage engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The type of storage engine to use (e.g., Sled, RocksDB).
    pub engine_type: StorageEngineType,
    /// The path to the directory where the database files will be stored.
    pub data_path: String,
    /// Optional: Any specific configuration parameters for the chosen engine.
    /// This could be a JSON string or a more structured enum/struct later.
    pub engine_specific_config: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            engine_type: StorageEngineType::Sled, // Default to Sled
            data_path: "./data/graphdb_storage".to_string(), // Default data path
            engine_specific_config: None,
        }
    }
}


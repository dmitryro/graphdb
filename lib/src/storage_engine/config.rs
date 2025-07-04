// lib/src/storage_engine/config.rs

use serde::{Serialize, Deserialize};
use clap::ValueEnum; // <--- Add this line

/// Enum to specify the type of storage engine to use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ValueEnum)] // <--- Add ValueEnum here
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
    /// Optional: Maximum number of open files for RocksDB.
    /// This field is specifically used by the RocksDB storage engine.
    #[serde(default)] // Allows this field to be optional in config files, defaulting to None
    pub max_open_files: Option<i32>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            engine_type: StorageEngineType::Sled, // Default to Sled
            data_path: "./data/graphdb_storage".to_string(), // Default data path
            engine_specific_config: None,
            max_open_files: None, // Default to None for max_open_files
        }
    }
}

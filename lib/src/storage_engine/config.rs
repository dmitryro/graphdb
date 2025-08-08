// lib/src/storage_engine/config.rs
// Updated: 2025-07-04 - Added InMemory to StorageEngineType.
// Fixed: 2025-07-30 - Added #[serde(rename_all = "lowercase")] to StorageEngineType.

use anyhow::{anyhow, Context, Result};
use serde::{Serialize, Deserialize};
use clap::ValueEnum;
use std::str::FromStr;

/// Enum to specify the type of storage engine to use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")] // <--- THIS IS THE CRITICAL ADDITION
pub enum StorageEngineType {
    Sled,
    RocksDB,
    InMemory, // Added InMemory option
    Redis,
    PostgreSQL,
    MySQL,
    // Add other storage types like PostgreSQL, Redis, etc., as needed
}

impl FromStr for StorageEngineType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" => Ok(StorageEngineType::RocksDB),
            "inmemory" => Ok(StorageEngineType::InMemory),
            "postgresql" => Ok(StorageEngineType::PostgreSQL),
            "mysql" => Ok(StorageEngineType::MySQL),
            "redis" => Ok(StorageEngineType::Redis),
            _ => Err(anyhow!("Unknown storage engine type: {}", s)),
        }
    }
}

impl ToString for StorageEngineType {
    fn to_string(&self) -> String {
        match self {
            StorageEngineType::Sled => "sled".to_string(),
            StorageEngineType::RocksDB => "rocksdb".to_string(),
            StorageEngineType::InMemory => "inmemory".to_string(),
            StorageEngineType::PostgreSQL => "postgresql".to_string(),
            StorageEngineType::MySQL => "mysql".to_string(),
            StorageEngineType::Redis => "redis".to_string(),
        }
    }
}

/// Configuration for the storage engine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    /// The type of storage engine to use (e.g., Sled, RocksDB, InMemory).
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

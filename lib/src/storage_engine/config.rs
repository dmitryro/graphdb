// lib/src/storage_engine/config.rs
// Updated: 2025-08-08 - Added StorageEngineType enum and serialization/deserialization logic
// Fixed: 2025-08-09 - Replaced serde_yaml with serde_yaml2
// Fixed: 2025-08-09 - Imported GraphError directly from models::errors
// ADDED: 2025-08-10 - Implemented the `std::fmt::Display` trait for `StorageEngineType` to allow it to be formatted with `{}` in `println!` macros.
// FIXED: 2025-08-10 - Removed the conflicting `ToString` implementation, as `Display` provides the same functionality.
// FIXED: 2025-08-10 - Removed `fmt::Display` from the derive macro as it is not a standard attribute.

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use models::errors::GraphError;
use serde_yaml2 as serde_yaml;
use serde_json::Value; // Import Value from serde_json
use std::fs;
use std::fmt; // New import for the Display trait

/// An enum representing the supported storage engine types.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum StorageEngineType {
    Sled,
    RocksDB,
    InMemory,
    Redis,
    PostgreSQL,
    MySQL,
}

impl FromStr for StorageEngineType {
    type Err = GraphError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" => Ok(StorageEngineType::RocksDB),
            "inmemory" => Ok(StorageEngineType::InMemory),
            "redis" => Ok(StorageEngineType::Redis),
            "postgresql" => Ok(StorageEngineType::PostgreSQL),
            "mysql" => Ok(StorageEngineType::MySQL),
            _ => Err(GraphError::InvalidData(format!("Unknown storage engine type: {}", s))),
        }
    }
}

// Implemented the `Display` trait to allow formatting with `{}`
impl fmt::Display for StorageEngineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageEngineType::Sled => write!(f, "Sled"),
            StorageEngineType::RocksDB => write!(f, "RocksDB"),
            StorageEngineType::InMemory => write!(f, "InMemory"),
            StorageEngineType::Redis => write!(f, "Redis"),
            StorageEngineType::PostgreSQL => write!(f, "PostgreSQL"),
            StorageEngineType::MySQL => write!(f, "MySQL"),
        }
    }
}

/// A specific configuration for the Sled storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledStorageConfig {
    pub max_open_files: Option<usize>,
}

/// Configuration for the storage engine.
/// This struct is a composite of all possible configuration fields,
/// with many being optional depending on the chosen engine.
/// Configuration for the graph storage system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    /// The type of storage engine to use (e.g., "inmemory", "sled").
    /// This was previously named `engine_type`.
    pub storage_engine_type: StorageEngineType,
    /// The directory where persistent data will be stored.
    /// This was previously named `data_path`.
    pub data_directory: Option<PathBuf>,
    /// A connection string for remote storage engines like MySQL.
    pub connection_string: Option<String>,
    /// The maximum number of open files for the storage engine.
    pub max_open_files: Option<usize>,
    /// A map for engine-specific configuration options.
    pub engine_specific_config: Option<HashMap<String, serde_json::Value>>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedStorageConfig {
    pub storage: StorageConfigInner,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfigInner {
    pub path: Option<PathBuf>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
}

impl Default for SelectedStorageConfig {
    fn default() -> Self {
        SelectedStorageConfig {
            storage: StorageConfigInner {
                path: Some(PathBuf::from("/opt/graphdb/storage_data")),
                host: Some("127.0.0.1".to_string()),
                port: Some(8049),
                username: None,
                password: None,
                database: None,
            },
        }
    }
}

/// Loads the storage configuration from a YAML file or provides a default.
pub fn load_storage_config_from_yaml(config_file_path: Option<PathBuf>) -> Result<StorageConfig, GraphError> {
    let default_config_path = PathBuf::from("/opt/graphdb/storage_config.yaml");
    let path_to_use = config_file_path.unwrap_or(default_config_path);

    let config = if path_to_use.exists() {
        let content = fs::read_to_string(&path_to_use)
            .map_err(|e| GraphError::Io(e))?;
        let wrapper: StorageConfigWrapper = serde_yaml::from_str(&content)
            .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
        wrapper.storage
    } else {
        // This default configuration has been updated to match the corrected StorageConfig struct.
        StorageConfig {
            storage_engine_type: StorageEngineType::RocksDB,
            data_directory: Some(PathBuf::from("/opt/graphdb/storage_data")),
            connection_string: None,
            max_open_files: None,
            engine_specific_config: None,
        }
    };
    Ok(config)
}

/// A wrapper struct for deserializing the nested YAML configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}
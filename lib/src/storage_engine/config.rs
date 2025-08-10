// lib/src/storage_engine/config.rs
// Updated: 2025-08-08 - Added StorageEngineType enum and serialization/deserialization logic
// Fixed: 2025-08-09 - Replaced serde_yaml with serde_yaml2
// Fixed: 2025-08-09 - Imported GraphError directly from models::errors
// ADDED: 2025-08-10 - Implemented the `std::fmt::Display` trait for `StorageEngineType`
// FIXED: 2025-08-10 - Removed conflicting `ToString` implementation
// FIXED: 2025-08-10 - Removed `fmt::Display` from derive macro
// ADDED: 2025-08-10 - Added `CliConfig` struct for TOML parsing
// ADDED: 2025-08-10 - Added `format_engine_config` function for `show config`
// FIXED: 2025-08-09 - Reverted `StorageConfig` to original fields
// Updated: 2025-08-09 - Made `data_directory` non-optional and added CLI fields

use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::str::FromStr;
use models::errors::GraphError;
use serde_yaml2 as serde_yaml;
use serde_json::Value;
use std::fs;
use std::fmt;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    /// The type of storage engine to use (e.g., "inmemory", "sled").
    pub storage_engine_type: StorageEngineType,
    /// The directory where persistent data will be stored.
    pub data_directory: PathBuf,
    /// A connection string for remote storage engines like MySQL.
    pub connection_string: Option<String>,
    /// The maximum number of open files for the storage engine.
    pub max_open_files: Option<usize>,
    /// A map for engine-specific configuration options.
    pub engine_specific_config: Option<HashMap<String, serde_json::Value>>,
    /// Default port for storage daemon.
    pub default_port: u16,
    /// Directory for log files.
    pub log_directory: String,
    /// Root directory for configuration files.
    pub config_root_directory: PathBuf,
    /// Cluster range for distributed setup.
    pub cluster_range: String,
    /// Whether to use Raft for scaling.
    pub use_raft_for_scale: bool,
}

/// CLI configuration loaded from TOML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CliConfig {
    /// Storage-specific configuration.
    pub storage: Option<StorageConfig>,
}

/// Formats the engine-specific configuration for display in `show config`.
pub fn format_engine_config(config: &StorageConfig) -> Vec<String> {
    let mut lines = vec![format!("- storage_engine_type: {}", config.storage_engine_type)];
    lines.push(format!("- data_directory: {}", config.data_directory.display()));
    if let Some(conn_str) = &config.connection_string {
        lines.push(format!("- connection_string: {}", conn_str));
    }
    if let Some(max_files) = config.max_open_files {
        lines.push(format!("- max_open_files: {}", max_files));
    }
    if let Some(specific_config) = &config.engine_specific_config {
        for (key, value) in specific_config {
            lines.push(format!("- {}: {}", key, value));
        }
    }
    lines.push(format!("- default_port: {}", config.default_port));
    lines.push(format!("- log_directory: {}", config.log_directory));
    lines.push(format!("- config_root_directory: {}", config.config_root_directory.display()));
    lines.push(format!("- cluster_range: {}", config.cluster_range));
    lines.push(format!("- use_raft_for_scale: {}", config.use_raft_for_scale));
    lines
}

/// Loads the storage configuration from a YAML file or provides a default.
pub fn load_storage_config_from_yaml(config_file_path: Option<PathBuf>) -> Result<StorageConfig, GraphError> {
    let default_config_path = PathBuf::from("/opt/graphdb/storage_config.yaml");
    let path_to_use = config_file_path.unwrap_or(default_config_path);

    let config = if path_to_use.exists() {
        let content = fs::read_to_string(&path_to_use)
            .map_err(|e| GraphError::Io(e))?;
        serde_yaml::from_str(&content)
            .map_err(|e| GraphError::DeserializationError(e.to_string()))?
    } else {
        StorageConfig {
            storage_engine_type: StorageEngineType::RocksDB,
            data_directory: PathBuf::from("/opt/graphdb/storage_data"),
            connection_string: None,
            max_open_files: None,
            engine_specific_config: None,
            default_port: 8049,
            log_directory: "/opt/graphdb/logs".to_string(),
            config_root_directory: PathBuf::from("/opt/graphdb"),
            cluster_range: "".to_string(),
            use_raft_for_scale: false,
        }
    };
    Ok(config)
}

/// A wrapper struct for deserializing the nested YAML configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
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
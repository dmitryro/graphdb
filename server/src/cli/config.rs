// server/src/cli/config.rs

// This file handles loading CLI and Storage daemon configurations.

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use anyhow::{Context, Result};
use std::str::FromStr; // Required for FromStr trait implementation
use serde_yaml2; // <--- Add this import
use daemon_api::StorageEngineType as DaemonApiStorageEngineType; // Added: Import daemon_api's StorageEngineType


// CLI's assumed default storage port. This is used for consistency in stop/status commands.
// The actual daemon port is determined by the daemon itself based on CLI arguments or its own config file.
pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;

/// Represents the top-level structure of the CLI configuration file (e.g., config.toml).
#[derive(Debug, Deserialize)]
pub struct CliConfig {
    pub server: ServerConfig,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

/// Loads the CLI configuration from `server/src/cli/config.toml`.
pub fn load_cli_config() -> Result<CliConfig> {
    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("cli")
        .join("config.toml");

    let config_content = fs::read_to_string(&config_path)
        .context(format!("Failed to read CLI config file: {}", config_path.display()))?;

    let config: CliConfig = toml::from_str(&config_content)
        .context("Failed to parse CLI config file")?;

    Ok(config)
}

/// Define the StorageConfig struct to mirror the content under 'storage:' in storage_config.yaml.
#[derive(Debug, Deserialize, Serialize, Clone)] // Added Clone for passing to daemonized process
pub struct StorageConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_disk_space_gb: u64,
    pub min_disk_space_gb: u64,
    pub use_raft_for_scale: bool,
    pub storage_engine_type: String,
}

// Added: Default implementation for StorageConfig
impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            data_directory: "/tmp/graphdb_data".to_string(),
            log_directory: "/tmp/graphdb_logs".to_string(),
            default_port: 8090, // A common default port for storage
            cluster_range: "8090-8100".to_string(), // Example default cluster range
            max_disk_space_gb: 100, // 100 GB default
            min_disk_space_gb: 10,  // 10 GB default
            use_raft_for_scale: false, // Default to not using Raft
            storage_engine_type: "sled".to_string(), // Default to Sled
        }
    }
}

// Define a wrapper struct to match the 'storage:' key in the YAML config.
#[derive(Debug, Deserialize)]
struct StorageConfigWrapper {
    storage: StorageConfig,
}

/// Loads the Storage daemon configuration from `storage_daemon_server/storage_config.yaml`.
pub fn load_storage_config(config_file_path: Option<PathBuf>) -> Result<StorageConfig, anyhow::Error> {
    let default_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // Go up to the workspace root of the server crate
        .ok_or_else(|| anyhow::anyhow!("Failed to get parent directory of server crate"))?
        .join("storage_daemon_server")
        .join("storage_config.yaml");

    let path_to_use = config_file_path.unwrap_or(default_config_path);

    let config_content = fs::read_to_string(&path_to_use)
        .map_err(|e| anyhow::anyhow!("Failed to read storage config file {}: {}", path_to_use.display(), e))?;

    // Change from serde_yaml::from_str to serde_yaml2::from_str
    let wrapper: StorageConfigWrapper = serde_yaml2::from_str(&config_content) // <--- Changed here
        .map_err(|e| anyhow::anyhow!("Failed to parse storage config file {}: {}", path_to_use.display(), e))?;

    Ok(wrapper.storage)
}

/// Function to get default REST API port from config
pub fn get_default_rest_port_from_config() -> u16 {
    8082 // Default REST API port
}

/// Enum for different storage engine types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEngineType {
    Sled,
    RocksDB,
    InMemory, // Added InMemory option to align with lib and daemon_api
    // Add other storage engine types here
}

impl FromStr for StorageEngineType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" => Ok(StorageEngineType::RocksDB),
            "inmemory" => Ok(StorageEngineType::InMemory), // Added InMemory
            _ => Err(anyhow::anyhow!("Unknown storage engine type: {}", s)),
        }
    }
}

impl ToString for StorageEngineType {
    fn to_string(&self) -> String {
        match self {
            StorageEngineType::Sled => "sled".to_string(),
            StorageEngineType::RocksDB => "rocksdb".to_string(),
            StorageEngineType::InMemory => "inmemory".to_string(), // Added InMemory
        }
    }
}

// Added: From implementation to convert from cli::config::StorageEngineType
// to daemon_api::StorageEngineType
impl From<StorageEngineType> for DaemonApiStorageEngineType {
    fn from(cli_type: StorageEngineType) -> Self {
        match cli_type {
            StorageEngineType::Sled => DaemonApiStorageEngineType::Sled,
            StorageEngineType::RocksDB => DaemonApiStorageEngineType::RocksDB,
            StorageEngineType::InMemory => DaemonApiStorageEngineType::InMemory,
        }
    }
}


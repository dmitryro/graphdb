// rest_api/src/config.rs

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use anyhow::{Context, Result};
use std::str::FromStr; // Required for FromStr trait
use serde_yaml2; // FIX: Use serde_yaml2

// CLI's assumed default storage port. This is used for consistency in stop/status commands.
// The actual daemon port is determined by the daemon itself based on CLI arguments or its own config file.
pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;

/// Represents the configuration for the REST API server itself.
#[derive(Debug, Deserialize)]
pub struct RestApiConfig {
    pub port: u16,
    pub host: String,
}

/// Loads the REST API configuration from `rest_api_config.toml` (or similar).
/// For now, hardcode defaults as there's no explicit file.
pub fn load_rest_api_config() -> Result<RestApiConfig> {
    // In a real scenario, you'd load from a file like this:
    // let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("rest_api_config.toml");
    // let config_content = fs::read_to_string(&config_path)
    //     .context(format!("Failed to read REST API config file: {}", config_path.display()))?;
    // let config: RestApiConfig = toml::from_str(&config_content)
    //     .context("Failed to parse REST API config file")?;
    // Ok(config)

    // For now, return a default configuration if no file is specified or found.
    Ok(RestApiConfig {
        port: 8082, // Default REST API port
        host: "127.0.0.1".to_string(),
    })
}

/// Define the StorageConfig struct to mirror the content under 'storage:' in storage_config.yaml.
#[derive(Debug, Deserialize, Serialize, Clone)]
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

    let wrapper: StorageConfigWrapper = serde_yaml2::from_str(&config_content) // FIX: Use serde_yaml2::from_str
        .map_err(|e| anyhow::anyhow!("Failed to parse storage config file {}: {}", path_to_use.display(), e))?;

    Ok(wrapper.storage)
}

/// Enum for different storage engine types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEngineType {
    Sled,
    RocksDB,
    // Add other storage engine types here
}

impl FromStr for StorageEngineType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" => Ok(StorageEngineType::RocksDB),
            _ => Err(anyhow::anyhow!("Unknown storage engine type: {}", s)),
        }
    }
}

impl ToString for StorageEngineType {
    fn to_string(&self) -> String {
        match self {
            StorageEngineType::Sled => "sled".to_string(),
            StorageEngineType::RocksDB => "rocksdb".to_string(),
        }
    }
}


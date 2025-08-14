// lib/src/storage_engine/config.rs
// Corrected: 2025-08-11 - Removed duplicate imports and structs.
// Corrected: 2025-08-11 - Reverted `CliConfig` back to `CliConfigToml` and fixed paths.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use models::errors::{GraphError, GraphError::StorageError as StorageError};
use serde_yaml2 as serde_yaml;
use serde_json::Value;
use std::fs;
use std::fmt::{self, Display, Formatter, Result as FmtResult};
use std::collections::HashMap;

// --- Custom PathBuf Serialization Module ---
mod path_buf_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(path: &PathBuf, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&path.to_string_lossy())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(PathBuf::from(s))
    }
}

// --- Custom StringOrU16 Serialization Module ---
mod string_or_u16 {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(s) => serializer.serialize_str(s),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(s) => Ok(Some(s)),
            serde_json::Value::Number(num) => Ok(Some(num.to_string())),
            _ => Ok(None),
        }
    }
}

// --- Custom OptionStorageEngineType Serialization Module ---
mod option_storage_engine_type_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &Option<StorageEngineType>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(engine_type) => serializer.serialize_str(&engine_type.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<StorageEngineType>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        match s {
            Some(engine_str) => {
                let engine_type = StorageEngineType::from_str(&engine_str)
                    .map_err(serde::de::Error::custom)?;
                Ok(Some(engine_type))
            },
            None => Ok(None),
        }
    }
}

/// An enum representing the supported storage engine types.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
            "rocksdb" | "rocks-db" => Ok(StorageEngineType::RocksDB),
            "inmemory" | "in-memory" => Ok(StorageEngineType::InMemory),
            "redis" => Ok(StorageEngineType::Redis),
            "postgresql" | "postgres" | "postgre-sql" => Ok(StorageEngineType::PostgreSQL),
            "mysql" | "my-sql" => Ok(StorageEngineType::MySQL),
            _ => Err(GraphError::InvalidData(format!("Unknown storage engine type: {}", s))),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EngineTypeOnly {
    storage_engine_type: StorageEngineType,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RocksdbConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SledConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
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

/// Configuration for the storage engine from YAML config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    /// The root directory for configuration files.
    #[serde(default = "default_config_root_directory")]
    #[serde(with = "path_buf_serde")]
    pub config_root_directory: PathBuf,
    /// The directory where persistent data will be stored.
    #[serde(with = "path_buf_serde")]
    pub data_directory: PathBuf,
    /// Directory for log files.
    #[serde(default = "default_log_directory")]
    pub log_directory: String,
    /// Default port for storage daemon.
    #[serde(default = "default_default_port")]
    pub default_port: u16,
    /// Cluster range for distributed setup.
    #[serde(default = "default_cluster_range")]
    pub cluster_range: String,
    /// Maximum disk space allowed (in GB).
    #[serde(default = "default_max_disk_space_gb")]
    pub max_disk_space_gb: u64,
    /// Minimum disk space required (in GB).
    #[serde(default = "default_min_disk_space_gb")]
    pub min_disk_space_gb: u64,
    /// Whether to use Raft for scaling.
    #[serde(default = "default_use_raft_for_scale")]
    pub use_raft_for_scale: bool,
    /// The type of storage engine to use.
    #[serde(default = "default_storage_engine_type")]
    pub storage_engine_type: StorageEngineType,
    /// Connection string for database engines.
    #[serde(default)]
    pub connection_string: Option<String>,
    /// Engine-specific configuration.
    #[serde(default)]
    pub engine_specific_config: Option<HashMap<String, Value>>,
    /// Maximum number of open files for the storage engine.
    #[serde(default = "default_max_open_files")]
    pub max_open_files: Option<i32>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            config_root_directory: default_config_root_directory(),
            data_directory: PathBuf::from("/opt/graphdb/storage_data"),
            log_directory: default_log_directory(),
            default_port: default_default_port(),
            cluster_range: default_cluster_range(),
            max_disk_space_gb: default_max_disk_space_gb(),
            min_disk_space_gb: default_min_disk_space_gb(),
            use_raft_for_scale: default_use_raft_for_scale(),
            storage_engine_type: default_storage_engine_type(),
            connection_string: None,
            engine_specific_config: None,
            max_open_files: default_max_open_files(),
        }
    }
}

fn default_config_root_directory() -> PathBuf { PathBuf::from("/opt/graphdb") }
fn default_log_directory() -> String { "/var/log/graphdb".to_string() }
fn default_default_port() -> u16 { 8049 }
fn default_cluster_range() -> String { "8049".to_string() }
fn default_max_disk_space_gb() -> u64 { 1000 }
fn default_min_disk_space_gb() -> u64 { 10 }
fn default_use_raft_for_scale() -> bool { true }
fn default_storage_engine_type() -> StorageEngineType { StorageEngineType::RocksDB }
fn default_max_open_files() -> Option<i32> { Some(1024) }

/// A wrapper struct for deserializing the nested YAML configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

// ---- START OF NEW STRUCTS REQUIRED BY LIB CRATE ----
/// Parallel struct for AppConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct AppConfig {
    pub version: Option<String>,
}

/// Parallel struct for ServerConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

/// Parallel struct for RestConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct RestConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

/// Parallel struct for DaemonConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DaemonConfig {
    pub process_name: Option<String>,
    pub user: Option<String>,
    pub group: Option<String>,
}

/// Parallel struct for LogConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LogConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

/// Parallel struct for PathsConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct PathsConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

/// Parallel struct for SecurityConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct SecurityConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

/// Parallel struct for DeploymentConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DeploymentConfig {
    #[serde(rename = "config-root-directory")]
    pub config_root_directory: Option<PathBuf>,
}

/// Parallel struct for CliTomlStorageConfig
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub struct CliTomlStorageConfig {
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub default_port: Option<u16>,
    #[serde(with = "string_or_u16", default)]
    pub cluster_range: Option<String>,
    #[serde(default)]
    pub data_directory: Option<String>,
    #[serde(default)]
    pub config_root_directory: Option<PathBuf>,
    #[serde(default)]
    pub log_directory: Option<String>,
    #[serde(default)]
    pub max_disk_space_gb: Option<u64>,
    #[serde(default)]
    pub min_disk_space_gb: Option<u64>,
    #[serde(default)]
    pub use_raft_for_scale: Option<bool>,
    #[serde(with = "option_storage_engine_type_serde", default)]
    pub storage_engine_type: Option<StorageEngineType>,
    #[serde(default)]
    pub max_open_files: Option<u64>,
    #[serde(default)]
    pub connection_string: Option<String>,
    #[serde(default)]
    pub engine_specific_config: Option<HashMap<String, Value>>,
}

/// A struct representing the full TOML configuration, used to successfully
/// deserialize the entire config file in the lib crate.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub struct CliConfigToml {
    pub app: AppConfig,
    pub server: ServerConfig,
    pub rest: RestConfig,
    pub daemon: DaemonConfig,
    pub storage: Option<CliTomlStorageConfig>,
    pub log: Option<LogConfig>,
    pub paths: Option<PathsConfig>,
    pub security: Option<SecurityConfig>,
    pub deployment: DeploymentConfig,
    #[serde(default)]
    pub enable_plugins: bool,
}

// Function to load storage config from a YAML file.
// This is now the `load_engine_specific_config` function from your previous attempt.
pub fn load_storage_config_from_yaml(config_path: Option<&PathBuf>) -> Result<StorageConfig, GraphError> {
    let path = match config_path {
        Some(p) => p.to_owned(),
        None => PathBuf::from("/opt/graphdb/storage.yaml"),
    };

    let content = fs::read_to_string(&path)
        .map_err(|e| GraphError::Io(e))?;
    
    let wrapper: StorageConfigWrapper = serde_yaml::from_str(&content)
        .map_err(|e| GraphError::SerializationError(e.to_string()))?;

    Ok(wrapper.storage)
}

/// Helper function to format engine-specific configuration details
pub fn format_engine_config(storage_config: &StorageConfig) -> Vec<String> {
    let mut config_lines = Vec::new();
    
    // Display the storage engine type prominently.
    // We can use `to_string()` on the enum directly because it implements `Display`.
    config_lines.push(format!("Engine: {}", storage_config.storage_engine_type));
    
    // Display engine-specific configuration if available.
    // The `engine_specific_config` is a HashMap, not a struct, so we iterate over it.
    if let Some(ref engine_config_map) = storage_config.engine_specific_config {
        config_lines.push("  Engine-Specific Configuration:".to_string());
        for (key, value) in engine_config_map.iter() {
            // Use the JSON Value's `to_string` method for formatting.
            config_lines.push(format!("    {}: {}", key, value));
        }
    } else {
        config_lines.push("  Config: Using default configuration".to_string());
    }
    
    // Add general storage configuration.
    // We must handle the `Option<i32>` type for max_open_files.
    if let Some(files) = storage_config.max_open_files {
        config_lines.push(format!("  Max Open Files: {}", files));
    } else {
        config_lines.push("  Max Open Files: Not specified".to_string());
    }
    
    config_lines.push(format!("  Max Disk Space: {} GB", storage_config.max_disk_space_gb));
    config_lines.push(format!("  Min Disk Space: {} GB", storage_config.min_disk_space_gb));
    config_lines.push(format!("  Use Raft: {}", storage_config.use_raft_for_scale));
    
    config_lines
}

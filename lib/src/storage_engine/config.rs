// lib/src/storage_engine/config.rs
// Corrected: 2025-08-11 - Removed duplicate imports and structs.
// Corrected: 2025-08-11 - Reverted `CliConfig` back to `CliConfigToml` and fixed paths.
// Updated: 2025-08-13 - Added custom deserializer to handle both snake_case and kebab-case fields.
// Updated: 2025-08-13 - Enhanced `load_storage_config_from_yaml` with directory creation and detailed error logging.
// Updated: 2025-08-14 - Fixed serde_yaml2 compatibility issues

use serde::{Deserialize, Serialize, Deserializer};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use models::errors::{GraphError, GraphError::StorageError as StorageError};
use serde_yaml2 as serde_yaml;
use serde_json::Value; // Use serde_json::Value instead of serde_yaml::Value
use std::fs;
use std::fmt::{self, Display, Formatter, Result as FmtResult};
use std::collections::HashMap;
use log::{info, debug, warn, error};

// --- Constants ---
pub const DAEMON_REGISTRY_DB_PATH: &str = "./daemon_registry_db";
pub const DEFAULT_DAEMON_PORT: u16 = 9001;
pub const DEFAULT_REST_API_PORT: u16 = 8081;
pub const DEFAULT_STORAGE_PORT: u16 = 8049;
pub const MAX_CLUSTER_SIZE: usize = 100;
pub const DEFAULT_MAIN_PORT: u16 = 9001;
pub const DEFAULT_CLUSTER_RANGE: &str = "9001-9005";
pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;
pub const DEFAULT_DATA_DIRECTORY: &str = "/opt/graphdb/storage_data";
pub const DEFAULT_LOG_DIRECTORY: &str = "/opt/graphdb/logs";
pub const DEFAULT_STORAGE_CONFIG_PATH: &str = "./storage_daemon_server/storage_config.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/storage_config.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB: &str = "./storage_daemon_server/storage_config_rocksdb.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_SLED: &str = "./storage_daemon_server/storage_config_sled.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_POSTGRES: &str = "./storage_daemon_server/storage_config_postgres.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_MYSQL: &str = "./storage_daemon_server/storage_config_mysql.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_REDIS: &str = "./storage_daemon_server/storage_config_redis.yaml";
pub const DEFAULT_REST_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/rest_api_config.yaml";
pub const DEFAULT_DAEMON_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/daemon_config.yaml";
pub const DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/main_app_config.yaml";
pub const PID_FILE_DIR: &str = "./storage_daemon_server/pids";
pub const DAEMON_PID_FILE_NAME_PREFIX: &str = "graphdb-daemon-";
pub const REST_PID_FILE_NAME_PREFIX: &str = "graphdb-rest-";
pub const STORAGE_PID_FILE_NAME_PREFIX: &str = "graphdb-storage-";
pub const EXECUTABLE_NAME: &str = "graphdb-server";
pub const DEFAULT_CONFIG_ROOT_DIRECTORY_STR: &str = "./storage_daemon_server";
pub const DEFAULT_HTTP_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_HTTP_RETRIES: u32 = 3;
pub const DEFAULT_GRPC_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_GRPC_RETRIES: u32 = 5;

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
    use super::*;
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

#[derive(Debug, Deserialize, Serialize)]
pub struct EngineTypeOnly {
    pub storage_engine_type: StorageEngineType,
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

/// Configuration for the storage engine from YAML config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    /// The root directory for configuration files.
    #[serde(default = "default_config_root_directory")]
    #[serde(with = "path_buf_serde")]
    pub config_root_directory: PathBuf,
    /// The directory where persistent data will be stored.
    #[serde(default = "default_data_directory")]
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
            data_directory: default_data_directory(),
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
fn default_data_directory() -> PathBuf { PathBuf::from("./storage_daemon_server/data") }
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
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    pub version: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig { version: Some("0.1.0".to_string()) }
    }
}

/// Parallel struct for ServerConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            port: Some(8049),
            host: Some("127.0.0.1".to_string()),
        }
    }
}

/// Parallel struct for RestConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

impl Default for RestConfig {
    fn default() -> Self {
        RestConfig {
            port: Some(8080),
            host: Some("127.0.0.1".to_string()),
        }
    }
}

/// Parallel struct for DaemonConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaemonConfig {
    pub process_name: Option<String>,
    pub user: Option<String>,
    pub group: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        DaemonConfig {
            process_name: Some("graphdb".to_string()),
            user: Some("graphdb".to_string()),
            group: Some("graphdb".to_string()),
        }
    }
}

/// Parallel struct for LogConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LogConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {}
    }
}

/// Parallel struct for PathsConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PathsConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

impl Default for PathsConfig {
    fn default() -> Self {
        PathsConfig {}
    }
}

/// Parallel struct for SecurityConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SecurityConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

impl Default for SecurityConfig {
    fn default() -> Self {
        SecurityConfig {}
    }
}

/// Parallel struct for DeploymentConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeploymentConfig {
    #[serde(rename = "config-root-directory")]
    pub config_root_directory: Option<PathBuf>,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        DeploymentConfig {
            config_root_directory: Some(PathBuf::from("/opt/graphdb")),
        }
    }
}

/// Parallel struct for CliTomlStorageConfig
#[derive(Debug, Deserialize, Serialize, Clone)]
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

impl Default for CliTomlStorageConfig {
    fn default() -> Self {
        CliTomlStorageConfig {
            port: Some(8049),
            default_port: Some(8049),
            cluster_range: Some("8049".to_string()),
            data_directory: Some("/opt/graphdb/storage_data".to_string()),
            config_root_directory: Some(PathBuf::from("/opt/graphdb")),
            log_directory: Some("/var/log/graphdb".to_string()),
            max_disk_space_gb: Some(1000),
            min_disk_space_gb: Some(10),
            use_raft_for_scale: Some(true),
            storage_engine_type: Some(StorageEngineType::RocksDB),
            max_open_files: Some(1024),
            connection_string: None,
            engine_specific_config: None,
        }
    }
}

/// A struct representing the full TOML configuration, used to successfully
/// deserialize the entire config file in the lib crate.
#[derive(Debug, Deserialize, Serialize, Clone)]
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

impl Default for CliConfigToml {
    fn default() -> Self {
        CliConfigToml {
            app: AppConfig::default(),
            server: ServerConfig::default(),
            rest: RestConfig::default(),
            daemon: DaemonConfig::default(),
            storage: Some(CliTomlStorageConfig::default()),
            log: Some(LogConfig::default()),
            paths: Some(PathsConfig::default()),
            security: Some(SecurityConfig::default()),
            deployment: DeploymentConfig::default(),
            enable_plugins: false,
        }
    }
}

// Function to load storage config from a YAML file.
pub fn load_storage_config_from_yaml(config_path: Option<&PathBuf>) -> Result<StorageConfig, GraphError> {
    let path = match config_path {
        Some(p) => {
            info!("Loading storage config from provided path: {:?}", p);
            p.to_owned()
        }
        None => {
            let default_path = PathBuf::from("./storage_daemon_server/storage_config.yaml");
            info!("No config path provided, using default: {:?}", default_path);
            default_path
        }
    };

    // Read and log raw YAML content
    let content = match fs::read_to_string(&path) {
        Ok(content) => {
            debug!("Raw YAML content from {:?}:\n{}", path, content);
            content
        }
        Err(e) => {
            warn!("Failed to read YAML file at {:?}: {}. Using default config.", path, e);
            let config = StorageConfig::default();
            // Ensure directories for default config
            fs::create_dir_all(&config.data_directory)
                .map_err(|e| GraphError::Io(e))?;
            info!("Ensured default data directory exists: {:?}", config.data_directory);
            fs::create_dir_all(&config.log_directory)
                .map_err(|e| GraphError::Io(e))?;
            info!("Ensured default log directory exists: {:?}", config.log_directory);
            return Ok(config);
        }
    };

    // Attempt to deserialize YAML
    let config = match serde_yaml2::from_str::<StorageConfigWrapper>(&content) {
        Ok(wrapper) => {
            info!("Successfully parsed YAML config with 'storage' key from {:?}", path);
            wrapper.storage
        }
        Err(e1) => {
            warn!("Failed to parse as StorageConfigWrapper: {}. Trying direct StorageConfig.", e1);
            match serde_yaml2::from_str::<StorageConfig>(&content) {
                Ok(config) => {
                    info!("Successfully parsed YAML config directly as StorageConfig from {:?}", path);
                    config
                }
                Err(e2) => {
                    error!("Failed to parse YAML config at {:?}: {}", path, e2);
                    // Log partial parse for debugging
                    if let Ok(partial) = serde_yaml2::from_str::<serde_json::Value>(&content) {
                        debug!("Partial YAML parse result: {:?}", partial);
                    } else {
                        debug!("Failed to parse YAML content as JSON for partial logging");
                    }
                    return Err(GraphError::SerializationError(e2.to_string()));
                }
            }
        }
    };

    // Create directories
    fs::create_dir_all(&config.data_directory)
        .map_err(|e| GraphError::Io(e))?;
    info!("Ensured data directory exists: {:?}", config.data_directory);
    fs::create_dir_all(&config.log_directory)
        .map_err(|e| GraphError::Io(e))?;
    info!("Ensured log directory exists: {:?}", config.log_directory);

    // Validate cluster range
    if let Ok(range) = parse_cluster_range(&config.cluster_range) {
        if !range.contains(&config.default_port) {
            warn!(
                "Default port {} is outside cluster range {}",
                config.default_port, config.cluster_range
            );
            return Err(GraphError::InvalidData(format!(
                "Default port {} is not within cluster range {}",
                config.default_port, config.cluster_range
            )));
        }
        debug!("Validated default port {} is within cluster range {}", config.default_port, config.cluster_range);
    } else {
        error!("Invalid cluster range format: {}", config.cluster_range);
        return Err(GraphError::InvalidData(format!(
            "Invalid cluster range format: {}",
            config.cluster_range
        )));
    }

    info!("Successfully loaded and validated storage config: {:?}", config);
    Ok(config)
}

// Helper function to parse cluster range (e.g., "8000-9000" or single port "8049")
fn parse_cluster_range(range: &str) -> Result<std::ops::RangeInclusive<u16>, GraphError> {
    if range.contains('-') {
        let parts: Vec<&str> = range.split('-').collect();
        if parts.len() != 2 {
            return Err(GraphError::InvalidData(format!("Invalid cluster range format: {}", range)));
        }
        let start = parts[0].parse::<u16>().map_err(|e| {
            GraphError::InvalidData(format!("Invalid cluster range start: {}", e))
        })?;
        let end = parts[1].parse::<u16>().map_err(|e| {
            GraphError::InvalidData(format!("Invalid cluster range end: {}", e))
        })?;
        Ok(start..=end)
    } else {
        let port = range.parse::<u16>().map_err(|e| {
            GraphError::InvalidData(format!("Invalid cluster range port: {}", e))
        })?;
        Ok(port..=port)
    }
}

/// Helper function to format engine-specific configuration details
pub fn format_engine_config(storage_config: &StorageConfig) -> Vec<String> {
    let mut config_lines = Vec::new();
    
    // Display the storage engine type prominently.
    config_lines.push(format!("Engine: {}", storage_config.storage_engine_type));
    
    // Display engine-specific configuration if available.
    if let Some(ref engine_config_map) = storage_config.engine_specific_config {
        config_lines.push("  Engine-Specific Configuration:".to_string());
        for (key, value) in engine_config_map.iter() {
            config_lines.push(format!("    {}: {}", key, value));
        }
    } else {
        config_lines.push("  Config: Using default configuration".to_string());
    }
    
    // Add general storage configuration.
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

// Helper function to create a default YAML config file for a specific engine type
// --- Updated `create_default_yaml_config` function ---
// Helper function to create a default YAML config file for a specific engine type
// --- Updated `create_default_yaml_config` function ---
pub fn create_default_yaml_config(config_path: &PathBuf, engine_type: StorageEngineType) -> Result<(), GraphError> {
    let mut config = StorageConfig::default();
    config.storage_engine_type = engine_type;

    // Determine the engine-specific config file path using constants
    let engine_config_path = match engine_type {
        StorageEngineType::Sled => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED),
        StorageEngineType::RocksDB => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB),
        StorageEngineType::Redis => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS),
        StorageEngineType::PostgreSQL => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES),
        StorageEngineType::MySQL => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL),
        StorageEngineType::InMemory => PathBuf::from("./storage_daemon_server/storage_config_inmemory.yaml"), // No constant provided for InMemory
    };

    // Set default values for fields
    config.config_root_directory = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR);
    config.log_directory = PathBuf::from(DEFAULT_LOG_DIRECTORY).to_string_lossy().to_string();
    config.default_port = DEFAULT_STORAGE_PORT;
    config.cluster_range = DEFAULT_STORAGE_PORT.to_string();

    // Load engine-specific configuration to set data_directory
    if engine_config_path.exists() {
        let content = fs::read_to_string(&engine_config_path)
            .map_err(|e| GraphError::Io(e))?;
        debug!("Raw YAML content from {:?}:\n{}", engine_config_path, content);

        // Parse the engine-specific config as a serde_json::Value
        match serde_yaml2::from_str::<serde_json::Value>(&content) { // FIX: Changed serde_yaml to serde_yaml2
            Ok(yaml_value) => {
                info!("Successfully parsed engine-specific config from {:?}", engine_config_path);
                // Extract the `storage` mapping
                if let Some(storage_map) = yaml_value.get("storage").and_then(|v| v.as_object()) {
                    // Set data_directory from the `path` field if present
                    if let Some(path_str) = storage_map.get("path").and_then(|v| v.as_str()) {
                        config.data_directory = PathBuf::from(path_str);
                    }
                } else {
                    warn!("No valid `storage` mapping found in {:?}", engine_config_path);
                }
            }
            Err(e) => {
                warn!("Failed to parse engine-specific config at {:?}: {}. Using default settings.", engine_config_path, e);
                // Log partial parse for debugging
                if let Ok(partial) = serde_yaml2::from_str::<serde_json::Value>(&content) { // FIX: Changed serde_yaml to serde_yaml2
                    debug!("Partial YAML parse result: {:?}", partial);
                }
                // Fall back to default path
                let default_path = match engine_type {
                    StorageEngineType::Sled => format!("{}/sled", DEFAULT_DATA_DIRECTORY),
                    StorageEngineType::RocksDB => format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY),
                    _ => PathBuf::from(DEFAULT_DATA_DIRECTORY).to_string_lossy().to_string(),
                };
                config.data_directory = PathBuf::from(&default_path);
            }
        }
    } else {
        warn!("Engine-specific config file not found at {:?}. Using default settings.", engine_config_path);
        // Fall back to default path
        let default_path = match engine_type {
            StorageEngineType::Sled => format!("{}/sled", DEFAULT_DATA_DIRECTORY),
            StorageEngineType::RocksDB => format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY),
            _ => PathBuf::from(DEFAULT_DATA_DIRECTORY).to_string_lossy().to_string(),
        };
        config.data_directory = PathBuf::from(&default_path);
    }

    // Explicitly set engine_specific_config to None to defer to engine-specific YAML file
    config.engine_specific_config = None;

    let wrapper = StorageConfigWrapper { storage: config };
    let yaml_content = serde_yaml2::to_string(&wrapper) // FIX: Changed serde_yaml to serde_yaml2
        .map_err(|e| GraphError::SerializationError(format!("Failed to serialize default config: {}", e)))?;

    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| GraphError::Io(e))?;
    }

    fs::write(config_path, yaml_content)
        .map_err(|e| GraphError::Io(e))?;

    info!("Created default config file at {:?}", config_path);
    Ok(())
}
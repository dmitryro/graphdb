// lib/src/storage_engine/config.rs
// Corrected: 2025-08-11 - Removed duplicate imports and structs.
// Corrected: 2025-08-11 - Reverted `CliConfig` back to `CliConfigToml` and fixed paths.
// Updated: 2025-08-13 - Added custom deserializer to handle both snake_case and kebab-case fields.
// Updated: 2025-08-13 - Enhanced `load_storage_config_from_yaml` with directory creation and detailed error logging.
// Updated: 2025-08-14 - Fixed serde_yaml2 compatibility issues
// Updated: 2025-08-14 - Added extensive debugging and tracing for configuration loading and validation
// Updated: 2025-08-14 - Fixed parse_cluster_range to handle single ports and quoted strings as valid clusters
// Updated: 2025-08-14 - Ensured engine_specific_config port is applied to default_port
// Updated: 2025-08-14 - Enhanced RocksDB path validation with detailed logging
// Updated: 2025-08-14 - Added engine validation and consistent RocksDB defaults

use serde::{Deserialize, Serialize, Deserializer};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use models::errors::{GraphError, GraphError::StorageError as StorageError};
use serde_yaml2 as serde_yaml;
use serde_json::Value;
use std::fs;
use std::fmt::{self, Display, Formatter, Result as FmtResult};
use std::collections::HashMap;
use log::{info, debug, warn, error, trace};

// --- Constants ---
pub const DAEMON_REGISTRY_DB_PATH: &str = "./daemon_registry_db";
pub const DEFAULT_DAEMON_PORT: u16 = 9001;
pub const DEFAULT_REST_API_PORT: u16 = 8081;
pub const DEFAULT_STORAGE_PORT: u16 = 8049;
pub const MAX_CLUSTER_SIZE: usize = 100;
pub const DEFAULT_MAIN_PORT: u16 = 9001;
pub const DEFAULT_CLUSTER_RANGE: &str = "8049";
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
pub mod path_buf_serde {
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
        trace!("Parsing storage engine type: {}", s);
        match s.to_lowercase().as_str() {
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" | "rocks-db" => Ok(StorageEngineType::RocksDB),
            "inmemory" | "in-memory" => Ok(StorageEngineType::InMemory),
            "redis" => Ok(StorageEngineType::Redis),
            "postgresql" | "postgres" | "postgre-sql" => Ok(StorageEngineType::PostgreSQL),
            "mysql" | "my-sql" => Ok(StorageEngineType::MySQL),
            _ => {
                error!("Unknown storage engine type: {}", s);
                Err(GraphError::InvalidData(format!("Unknown storage engine type: {}", s)))
            }
        }
    }
}

impl fmt::Display for StorageEngineType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageEngineType::Sled => write!(f, "sled"),
            StorageEngineType::RocksDB => write!(f, "rocksdb"),
            StorageEngineType::InMemory => write!(f, "inmemory"),
            StorageEngineType::Redis => write!(f, "redis"),
            StorageEngineType::PostgreSQL => write!(f, "postgresql"),
            StorageEngineType::MySQL => write!(f, "mysql"),
        }
    }
}

/// Returns the list of available storage engines based on feature flags.
pub fn available_engines() -> Vec<StorageEngineType> {
    let mut engines = vec![StorageEngineType::Sled]; // Sled is always available
    #[cfg(feature = "rocksdb")]
    engines.push(StorageEngineType::RocksDB);
    #[cfg(feature = "redis")]
    engines.push(StorageEngineType::Redis);
    #[cfg(feature = "postgresql")]
    engines.push(StorageEngineType::PostgreSQL);
    #[cfg(feature = "mysql")]
    engines.push(StorageEngineType::MySQL);
    #[cfg(feature = "inmemory")]
    engines.push(StorageEngineType::InMemory);
    debug!("Available storage engines: {:?}", engines);
    engines
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EngineTypeOnly {
    pub storage_engine_type: StorageEngineType,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RocksdbConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
        // Added host and port to match the YAML configuration
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SledConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
        // Added host and port to match the YAML configuration
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
        trace!("Creating default StorageConfig");
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
            engine_specific_config: Some(HashMap::from_iter(vec![
                ("path".to_string(), Value::String(format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY))),
                ("host".to_string(), Value::String("127.0.0.1".to_string())),
                ("port".to_string(), Value::Number(DEFAULT_STORAGE_PORT.into())),
            ])),
            max_open_files: default_max_open_files(),
        }
    }
}

fn default_config_root_directory() -> PathBuf {
    trace!("Default config_root_directory: {}", DEFAULT_CONFIG_ROOT_DIRECTORY_STR);
    PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
}

fn default_data_directory() -> PathBuf {
    trace!("Default data_directory: {}", DEFAULT_DATA_DIRECTORY);
    PathBuf::from(DEFAULT_DATA_DIRECTORY)
}

fn default_log_directory() -> String {
    trace!("Default log_directory: {}", DEFAULT_LOG_DIRECTORY);
    DEFAULT_LOG_DIRECTORY.to_string()
}

fn default_default_port() -> u16 {
    trace!("Default default_port: {}", DEFAULT_STORAGE_PORT);
    DEFAULT_STORAGE_PORT
}

fn default_cluster_range() -> String {
    trace!("Default cluster_range: {}", DEFAULT_STORAGE_PORT);
    DEFAULT_STORAGE_PORT.to_string()
}

fn default_max_disk_space_gb() -> u64 {
    trace!("Default max_disk_space_gb: 1000");
    1000
}

fn default_min_disk_space_gb() -> u64 {
    trace!("Default min_disk_space_gb: 10");
    10
}

fn default_use_raft_for_scale() -> bool {
    trace!("Default use_raft_for_scale: true");
    true
}

fn default_storage_engine_type() -> StorageEngineType {
    trace!("Default storage_engine_type: RocksDB");
    StorageEngineType::RocksDB
}

fn default_max_open_files() -> Option<i32> {
    trace!("Default max_open_files: 1024");
    Some(1024)
}

/// A wrapper struct for deserializing the nested YAML configuration.
#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

// ---- START OF NEW STRUCTS REQUIRED BY LIB CRATE ----
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    pub version: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        trace!("Creating default AppConfig");
        AppConfig { version: Some("0.1.0".to_string()) }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        trace!("Creating default ServerConfig");
        ServerConfig {
            port: Some(DEFAULT_STORAGE_PORT),
            host: Some("127.0.0.1".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

impl Default for RestConfig {
    fn default() -> Self {
        trace!("Creating default RestConfig");
        RestConfig {
            port: Some(DEFAULT_REST_API_PORT),
            host: Some("127.0.0.1".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaemonConfig {
    pub process_name: Option<String>,
    pub user: Option<String>,
    pub group: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        trace!("Creating default DaemonConfig");
        DaemonConfig {
            process_name: Some("graphdb".to_string()),
            user: Some("graphdb".to_string()),
            group: Some("graphdb".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct LogConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

impl Default for LogConfig {
    fn default() -> Self {
        trace!("Creating default LogConfig");
        LogConfig {}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PathsConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

impl Default for PathsConfig {
    fn default() -> Self {
        trace!("Creating default PathsConfig");
        PathsConfig {}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SecurityConfig {
    // This is an empty struct in the provided TOML, so we just define it.
}

impl Default for SecurityConfig {
    fn default() -> Self {
        trace!("Creating default SecurityConfig");
        SecurityConfig {}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeploymentConfig {
    #[serde(rename = "config-root-directory")]
    pub config_root_directory: Option<PathBuf>,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        trace!("Creating default DeploymentConfig");
        DeploymentConfig {
            config_root_directory: Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
        }
    }
}

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
        trace!("Creating default CliTomlStorageConfig");
        CliTomlStorageConfig {
            port: Some(DEFAULT_STORAGE_PORT),
            default_port: Some(DEFAULT_STORAGE_PORT),
            cluster_range: Some(DEFAULT_STORAGE_PORT.to_string()),
            data_directory: Some(DEFAULT_DATA_DIRECTORY.to_string()),
            config_root_directory: Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            log_directory: Some(DEFAULT_LOG_DIRECTORY.to_string()),
            max_disk_space_gb: Some(1000),
            min_disk_space_gb: Some(10),
            use_raft_for_scale: Some(true),
            storage_engine_type: Some(StorageEngineType::RocksDB),
            max_open_files: Some(1024),
            connection_string: None,
            engine_specific_config: Some(HashMap::from_iter(vec![
                ("path".to_string(), Value::String(format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY))),
                ("host".to_string(), Value::String("127.0.0.1".to_string())),
                ("port".to_string(), Value::Number(DEFAULT_STORAGE_PORT.into())),
            ])),
        }
    }
}

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
        trace!("Creating default CliConfigToml");
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

// --- Updated `load_storage_config_from_yaml` function ---
pub fn load_storage_config_from_yaml(config_path: Option<&PathBuf>) -> Result<StorageConfig, GraphError> {
    trace!("Starting load_storage_config_from_yaml with config_path: {:?}", config_path);
    let path = match config_path {
        Some(p) => {
            info!("Loading storage config from provided path: {:?}", p);
            trace!("Using provided path: {:?}", p);
            p.to_owned()
        }
        None => {
            let default_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
            info!("No config path provided, using default: {:?}", default_path);
            trace!("Using default path: {:?}", default_path);
            default_path
        }
    };
    debug!("In Storage Engine - Loading config from {:?}", path);

    // Read and log raw YAML content
    let content = match fs::read_to_string(&path) {
        Ok(content) => {
            debug!("Raw YAML content from {:?}:\n{}", path, content);
            trace!("Successfully read YAML content, length: {} bytes", content.len());
            content
        }
        Err(e) => {
            warn!("Failed to read YAML file at {:?}: {}. Using default config.", path, e);
            trace!("Error reading YAML: {:?}", e);
            let config = StorageConfig::default();
            trace!("Default config created: {:?}", config);
            // Ensure directories for default config
            fs::create_dir_all(&config.data_directory)
                .map_err(|e| {
                    error!("Failed to create data directory {:?}: {}", config.data_directory, e);
                    GraphError::Io(e)
                })?;
            info!("Ensured default data directory exists: {:?}", config.data_directory);
            fs::create_dir_all(&config.log_directory)
                .map_err(|e| {
                    error!("Failed to create log directory {:?}: {}", config.log_directory, e);
                    GraphError::Io(e)
                })?;
            info!("Ensured default log directory exists: {:?}", config.log_directory);
            return Ok(config);
        }
    };

    // Attempt to deserialize YAML
    let mut config = match serde_yaml::from_str::<StorageConfigWrapper>(&content) {
        Ok(wrapper) => {
            info!("Successfully parsed YAML config with 'storage' key from {:?}", path);
            trace!("Parsed StorageConfigWrapper: {:?}", wrapper);
            wrapper.storage
        }
        Err(e1) => {
            warn!("Failed to parse as StorageConfigWrapper: {}. Trying direct StorageConfig.", e1);
            trace!("StorageConfigWrapper parse error: {:?}", e1);
            match serde_yaml::from_str::<StorageConfig>(&content) {
                Ok(config) => {
                    info!("Successfully parsed YAML config directly as StorageConfig from {:?}", path);
                    trace!("Parsed StorageConfig directly: {:?}", config);
                    config
                }
                Err(e2) => {
                    error!("Failed to parse YAML config at {:?}: {}", path, e2);
                    trace!("StorageConfig parse error: {:?}", e2);
                    // Log partial parse for debugging
                    if let Ok(partial) = serde_yaml::from_str::<serde_json::Value>(&content) {
                        debug!("Partial YAML parse result: {:?}", partial);
                        trace!("Partial parse successful, content: {:?}", partial);
                    } else {
                        debug!("Failed to parse YAML content as JSON for partial logging");
                        trace!("Partial parse failed");
                    }
                    return Err(GraphError::SerializationError(e2.to_string()));
                }
            }
        }
    };
    debug!("In Storage Engine - Parsed config: {:?}", config);
    trace!("Config details - storage_engine_type: {:?}, default_port: {}, cluster_range: {}", 
        config.storage_engine_type, config.default_port, config.cluster_range);

    // Validate storage engine
    if !available_engines().contains(&config.storage_engine_type) {
        error!("Storage engine type {:?} is not available. Available engines: {:?}", 
            config.storage_engine_type, available_engines());
        return Err(GraphError::InvalidData(format!(
            "Storage engine type {:?} is not available. Available engines: {:?}", 
            config.storage_engine_type, available_engines()
        )));
    }

    // Normalize storage_engine_type
    let engine_type_str = config.storage_engine_type.to_string();
    trace!("Raw storage_engine_type: {}", engine_type_str);
    let normalized_engine_type = engine_type_str.to_lowercase();
    trace!("Normalized storage_engine_type: {}", normalized_engine_type);
    config.storage_engine_type = match normalized_engine_type.as_str() {
        "rocksdb" => StorageEngineType::RocksDB,
        "sled" => StorageEngineType::Sled,
        "inmemory" => StorageEngineType::InMemory,
        "redis" => StorageEngineType::Redis,
        "postgresql" => StorageEngineType::PostgreSQL,
        "mysql" => StorageEngineType::MySQL,
        other => {
            error!("Invalid storage_engine_type '{}'", other);
            return Err(GraphError::InvalidData(format!("Invalid storage_engine_type '{}'", other)));
        }
    };
    trace!("Set storage_engine_type to: {:?}", config.storage_engine_type);

    // Validate and create RocksDB path if engine is RocksDB
    if config.storage_engine_type == StorageEngineType::RocksDB {
        trace!("Validating RocksDB configuration");
        let rocksdb_path = config
            .engine_specific_config
            .as_ref()
            .and_then(|cfg| {
                trace!("Extracting path from engine_specific_config: {:?}", cfg);
                cfg.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
            })
            .unwrap_or_else(|| {
                let fallback_path = config.data_directory.join("rocksdb");
                warn!("No path in engine_specific_config, using fallback: {:?}", fallback_path);
                trace!("Fallback RocksDB path: {:?}", fallback_path);
                fallback_path
            });
        debug!("RocksDB path: {:?}", rocksdb_path);
        if !rocksdb_path.exists() {
            warn!("RocksDB path does not exist: {:?}", rocksdb_path);
            trace!("Creating RocksDB directory: {:?}", rocksdb_path);
            fs::create_dir_all(&rocksdb_path)
                .map_err(|e| {
                    error!("Failed to create RocksDB directory {:?}: {}", rocksdb_path, e);
                    GraphError::Io(e)
                })?;
            info!("Created RocksDB directory: {:?}", rocksdb_path);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let permissions = fs::Permissions::from_mode(0o755);
                trace!("Setting permissions for RocksDB path: {:?}", rocksdb_path);
                fs::set_permissions(&rocksdb_path, permissions)
                    .map_err(|e| {
                        error!("Failed to set permissions for {:?}: {}", rocksdb_path, e);
                        GraphError::Io(e)
                    })?;
                debug!("Set permissions for RocksDB path: {:?}", rocksdb_path);
            }
        }
        if !rocksdb_path.is_dir() {
            error!("RocksDB path is not a directory: {:?}", rocksdb_path);
            trace!("Path validation failed: not a directory");
            return Err(GraphError::InvalidData(format!("RocksDB path is not a directory: {:?}", rocksdb_path)));
        }
        if let Ok(metadata) = fs::metadata(&rocksdb_path) {
            debug!("RocksDB path permissions: {:?}", metadata.permissions());
            trace!("RocksDB path metadata: {:?}", metadata);
        } else {
            warn!("Failed to retrieve metadata for RocksDB path: {:?}", rocksdb_path);
            trace!("Metadata retrieval failed");
        }
        debug!("Verified RocksDB path: {:?}", rocksdb_path);

        // Update default_port with engine-specific port if available
        if let Some(engine_config) = &config.engine_specific_config {
            trace!("Checking engine_specific_config for port: {:?}", engine_config);
            if let Some(port_value) = engine_config.get("port") {
                trace!("Found port value: {:?}", port_value);
                if let Some(port) = port_value.as_u64().and_then(|p| u16::try_from(p).ok()) {
                    debug!("Updating default_port to engine-specific port: {}", port);
                    trace!("Setting default_port to {}", port);
                    config.default_port = port;
                } else {
                    warn!("Invalid port value in engine_specific_config: {:?}", port_value);
                    trace!("Port value parsing failed: {:?}", port_value);
                }
            } else {
                warn!("No port found in engine_specific_config");
                trace!("engine_specific_config keys: {:?}", engine_config.keys());
            }
        } else {
            warn!("No engine_specific_config provided for RocksDB");
            trace!("engine_specific_config is None");
        }
    } else {
        trace!("Non-RocksDB engine, skipping path validation");
    }

    // Create directories
    trace!("Creating data directory: {:?}", config.data_directory);
    fs::create_dir_all(&config.data_directory)
        .map_err(|e| {
            error!("Failed to create data directory {:?}: {}", config.data_directory, e);
            GraphError::Io(e)
        })?;
    info!("Ensured data directory exists: {:?}", config.data_directory);
    trace!("Creating log directory: {:?}", config.log_directory);
    fs::create_dir_all(&config.log_directory)
        .map_err(|e| {
            error!("Failed to create log directory {:?}: {}", config.log_directory, e);
            GraphError::Io(e)
        })?;
    info!("Ensured log directory exists: {:?}", config.log_directory);

    // Validate cluster range
    trace!("Validating cluster range: {}", config.cluster_range);
    if let Ok(range) = parse_cluster_range(&config.cluster_range) {
        trace!("Parsed cluster range: {:?}", range);
        if !range.contains(&config.default_port) {
            warn!(
                "Default port {} is outside cluster range {}",
                config.default_port, config.cluster_range
            );
            trace!("Port validation failed: {} not in {:?}", config.default_port, range);
            return Err(GraphError::InvalidData(format!(
                "Default port {} is not within cluster range {}",
                config.default_port, config.cluster_range
            )));
        }
        debug!("Validated default port {} is within cluster range {}", config.default_port, config.cluster_range);
    } else {
        error!("Invalid cluster range format: {}", config.cluster_range);
        trace!("Cluster range parsing failed");
        return Err(GraphError::InvalidData(format!(
            "Invalid cluster range format: {}",
            config.cluster_range
        )));
    }

    info!("Successfully loaded and validated storage config: {:?}", config);
    trace!("Final config - storage_engine_type: {:?}, default_port: {}, cluster_range: {}", 
        config.storage_engine_type, config.default_port, config.cluster_range);
    Ok(config)
}

// --- Updated `parse_cluster_range` function ---
fn parse_cluster_range(range: &str) -> Result<std::ops::RangeInclusive<u16>, GraphError> {
    trace!("Parsing cluster range: {}", range);
    let cleaned_range = range.trim_matches(|c| c == '"' || c == '\'');
    trace!("Cleaned range (quotes removed): {}", cleaned_range);
    if cleaned_range.contains('-') {
        trace!("Detected range format");
        let parts: Vec<&str> = cleaned_range.split('-').collect();
        trace!("Split parts: {:?}", parts);
        if parts.len() != 2 {
            error!("Invalid cluster range format: {}", cleaned_range);
            trace!("Invalid number of parts: {}", parts.len());
            return Err(GraphError::InvalidData(format!("Invalid cluster range format: {}", cleaned_range)));
        }
        let start = parts[0].parse::<u16>().map_err(|e| {
            error!("Invalid cluster range start: {}", e);
            trace!("Start parse error: {}", e);
            GraphError::InvalidData(format!("Invalid cluster range start: {}", e))
        })?;
        let end = parts[1].parse::<u16>().map_err(|e| {
            error!("Invalid cluster range end: {}", e);
            trace!("End parse error: {}", e);
            GraphError::InvalidData(format!("Invalid cluster range end: {}", e))
        })?;
        debug!("Parsed range: {}-{}", start, end);
        trace!("Returning range: {}..={}", start, end);
        Ok(start..=end)
    } else {
        trace!("Detected single port format");
        let port = cleaned_range.parse::<u16>().map_err(|e| {
            error!("Invalid cluster range port: {}", e);
            trace!("Single port parse error: {}", e);
            GraphError::InvalidData(format!("Invalid cluster range port: {}", e))
        })?;
        debug!("Parsed single port: {}", port);
        trace!("Returning single port range: {}..={}", port, port);
        Ok(port..=port)
    }
}

// --- Updated `format_engine_config` function ---
pub fn format_engine_config(storage_config: &StorageConfig) -> Vec<String> {
    trace!("Formatting engine config for {:?}", storage_config);
    let mut config_lines = Vec::new();
    
    // Display the storage engine type prominently.
    config_lines.push(format!("Engine: {}", storage_config.storage_engine_type));
    trace!("Added engine type: {}", storage_config.storage_engine_type);
    
    // Display engine-specific configuration if available.
    if let Some(ref engine_config_map) = storage_config.engine_specific_config {
        config_lines.push("  Engine-Specific Configuration:".to_string());
        trace!("Processing engine_specific_config: {:?}", engine_config_map);
        for (key, value) in engine_config_map.iter() {
            config_lines.push(format!("    {}: {}", key, value));
            trace!("Added config line: {}: {}", key, value);
        }
    } else {
        config_lines.push("  Config: Using default configuration".to_string());
        trace!("No engine_specific_config, using default configuration");
    }
    
    // Add general storage configuration.
    if let Some(files) = storage_config.max_open_files {
        config_lines.push(format!("  Max Open Files: {}", files));
        trace!("Added max_open_files: {}", files);
    } else {
        config_lines.push("  Max Open Files: Not specified".to_string());
        trace!("No max_open_files specified");
    }
    
    config_lines.push(format!("  Max Disk Space: {} GB", storage_config.max_disk_space_gb));
    trace!("Added max_disk_space_gb: {}", storage_config.max_disk_space_gb);
    config_lines.push(format!("  Min Disk Space: {} GB", storage_config.min_disk_space_gb));
    trace!("Added min_disk_space_gb: {}", storage_config.min_disk_space_gb);
    config_lines.push(format!("  Use Raft: {}", storage_config.use_raft_for_scale));
    trace!("Added use_raft_for_scale: {}", storage_config.use_raft_for_scale);
    
    trace!("Returning config lines: {:?}", config_lines);
    config_lines
}

// Option 2: Remove the validation if it's not critical
fn create_default_yaml_config(yaml_path: &PathBuf, engine_type: StorageEngineType) -> Result<(), GraphError> {
    info!("Creating default YAML config at {:?}", yaml_path);
    trace!("Creating default config for engine_type: {:?}", engine_type);
    
    let mut config = StorageConfig::default();
    config.storage_engine_type = engine_type;
    trace!("Set storage_engine_type to: {:?}", engine_type);
    
    // Set engine-specific configuration
    let engine_path = match engine_type {
        StorageEngineType::Sled => format!("{}/sled", DEFAULT_DATA_DIRECTORY),
        StorageEngineType::RocksDB => format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY),
        StorageEngineType::Redis => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::PostgreSQL => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::MySQL => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::InMemory => DEFAULT_DATA_DIRECTORY.to_string(),
    };
    
    let engine_port = match engine_type {
        StorageEngineType::Redis => 6379,
        StorageEngineType::PostgreSQL => 5432,
        StorageEngineType::MySQL => 3306,
        _ => DEFAULT_STORAGE_PORT, // Fixed the wildcard pattern
    };
    
    trace!("Setting engine_path: {}, engine_port: {}", engine_path, engine_port);
    
    config.engine_specific_config = Some(HashMap::from_iter(vec![
        ("path".to_string(), Value::String(engine_path)),
        ("host".to_string(), Value::String("127.0.0.1".to_string())),
        ("port".to_string(), Value::Number(engine_port.into())),
    ]));
    
    trace!("Set engine_specific_config: {:?}", config.engine_specific_config);
    config.data_directory = PathBuf::from(DEFAULT_DATA_DIRECTORY);
    trace!("Set data_directory: {:?}", config.data_directory);
    config.default_port = engine_port;
    trace!("Set default_port: {}", config.default_port);
    config.cluster_range = engine_port.to_string();
    trace!("Set cluster_range: {}", config.cluster_range);
    
    let wrapper = StorageConfigWrapper { storage: config };
    trace!("Created StorageConfigWrapper: {:?}", wrapper);
    
    let yaml_content = serde_yaml::to_string(&wrapper)
        .map_err(|e| {
            error!("Failed to serialize default config: {}", e);
            trace!("Serialization error: {:?}", e);
            GraphError::SerializationError(format!("Failed to serialize default config: {}", e))
        })?;
    
    debug!("Generated YAML content:\n{}", yaml_content);
    
    if let Some(parent) = yaml_path.parent() {
        trace!("Creating parent directories: {:?}", parent);
        fs::create_dir_all(parent)
            .map_err(|e| {
                error!("Failed to create parent directories for {:?}: {}", yaml_path, e);
                trace!("Directory creation error: {:?}", e);
                GraphError::Io(e)
            })?;
    }
    
    fs::write(yaml_path, yaml_content)
        .map_err(|e| {
            error!("Failed to write YAML config to {:?}: {}", yaml_path, e);
            trace!("Write error: {:?}", e);
            GraphError::Io(e)
        })?;
    
    info!("Created default config file at {:?}", yaml_path);
    trace!("Default config file written successfully");
    Ok(())
}

/// Loads engine-specific configuration from a YAML file (e.g., storage_config_rocksdb.yaml).
/// Returns a HashMap containing engine-specific settings, ensuring a 'path' key for RocksDB/Sled.
/// Host and port are optional and only included if present in the YAML or needed as defaults.
pub fn load_engine_specific_config(
    engine_type: StorageEngineType,
    base_path: &Path,
) -> Result<HashMap<String, Value>, GraphError> {
    // Construct the path to the specific engine config file (e.g., storage_config_sled.yaml)
    let engine_config_path = base_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("storage_config_{}.yaml", engine_type.to_string().to_lowercase()));
    info!("Attempting to load engine-specific config for {:?} from {:?}", engine_type, engine_config_path);
    trace!("Constructed engine config path: {:?}", engine_config_path);

    let mut engine_specific_config = HashMap::new();

    if engine_config_path.exists() {
        // Read and log the raw YAML content from the engine-specific file
        let content = fs::read_to_string(&engine_config_path)
            .map_err(|e| {
                error!("Failed to read engine-specific YAML file at {:?}: {}", engine_config_path, e);
                GraphError::Io(e)
            })?;
        debug!("Raw engine-specific YAML content from {:?}:\n{}", engine_config_path, content);
        trace!("Successfully read content from engine config file.");

        // Deserialize the content into the `StorageConfigWrapper` to extract the `engine_specific_config`
        let wrapper: StorageConfigWrapper = serde_yaml::from_str(&content)
            .map_err(|e| {
                error!("Failed to deserialize engine-specific YAML from {:?}: {}", engine_config_path, e);
                trace!("Deserialization error details: {:?}", e);
                GraphError::SerializationError(format!("Failed to deserialize engine-specific YAML: {}", e))
            })?;

        // Extract the `engine_specific_config` map, using a default empty map if it's null
        engine_specific_config = wrapper.storage.engine_specific_config.unwrap_or_default();
        debug!("Deserialized engine-specific config: {:?}", engine_specific_config);
        trace!("Successfully deserialized engine-specific config.");
    } else {
        warn!("Engine-specific config file not found at {:?}. Using defaults.", engine_config_path);
        trace!("Engine config file does not exist. Proceeding with default values.");
    }

    // Ensure a 'path' key is present for RocksDB and Sled, which require a file path.
    // Use the DEFAULT_DATA_DIRECTORY constant as the base for the default path.
    if matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) &&
        (!engine_specific_config.contains_key("path") ||
         engine_specific_config.get("path").and_then(|v| v.as_str()).unwrap_or("").is_empty()) {
        let default_path = format!("{}/{}", DEFAULT_DATA_DIRECTORY, engine_type.to_string().to_lowercase());
        warn!("No valid 'path' in engine-specific config for {:?}, using default: {:?}", engine_type, default_path);
        engine_specific_config.insert("path".to_string(), Value::String(default_path));
        trace!("Inserted default path into config.");
    }

    // Ensure 'host' and 'port' keys are present for remote-capable engines.
    if !engine_specific_config.contains_key("host") &&
        matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) {
        debug!("No 'host' in engine-specific config for {:?}, using default: 127.0.0.1", engine_type);
        engine_specific_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
        trace!("Inserted default host into config.");
    }
    if !engine_specific_config.contains_key("port") &&
        matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) {
        let default_port = match engine_type {
            // These engines have a default port for local operations.
            StorageEngineType::RocksDB | StorageEngineType::Sled => 8049,
            // Other engines may not have a specific port defined here
            _ => 0,
        };
        debug!("No 'port' in engine-specific config for {:?}, using default: {}", engine_type, default_port);
        engine_specific_config.insert("port".to_string(), Value::Number(default_port.into()));
        trace!("Inserted default port into config.");
    }

    // A crucial step to ensure the engine_specific_config deserializes correctly:
    // It must contain the `storage_engine_type` field.
    let storage_type_value = serde_json::to_value(engine_type.clone())
        .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize StorageEngineType: {}", e)))?;
    engine_specific_config.insert("storage_engine_type".to_string(), storage_type_value);
    trace!("Inserted storage_engine_type into config.");

    trace!("Final engine-specific config for {:?}: {:?}", engine_type, engine_specific_config);
    Ok(engine_specific_config)
}
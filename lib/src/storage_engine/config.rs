use anyhow::{Result, Context, anyhow, Error};
use serde::{Deserialize, Serialize, Deserializer};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use models::errors::{GraphError, GraphError::StorageError as StorageError};
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
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
pub const DEFAULT_STORAGE_CONFIG_PATH_TIKV: &str = "./storage_daemon_server/storage_config_tikv.yaml";
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
pub const LOCK_FILE_PATH: &str = "/tmp/graphdb_storage_lock";
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

// Default values for the StorageConfigInner fields to ensure proper deserialization
fn default_path() -> Option<PathBuf> {
    Some(PathBuf::from("/opt/graphdb/storage_data"))
}

fn default_host() -> Option<String> {
    Some("127.0.0.1".to_string())
}

fn default_port() -> Option<u16> {
    Some(8049)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfigInner {
    // Shared fields for path-based engines (Sled, RocksDB)
    #[serde(default = "default_path")]
    pub path: Option<PathBuf>,
    
    // Shared fields for URI-based engines (PostgreSQL, MySQL, Redis)
    #[serde(default = "default_host")]
    pub host: Option<String>,
    
    // Port field for all engines that require it.
    #[serde(default = "default_port")]
    pub port: Option<u16>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub pd_endpoints: Option<String>,
}

// The outer struct that holds the engine type and the configuration details.
// This structure correctly represents your desired model.
// The outer struct that holds the engine type and the configuration details.
// This structure correctly represents your desired model.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelectedStorageConfig {
    pub storage_engine_type: StorageEngineType,
    #[serde(flatten)] // This flattens the inner struct into the outer one during serialization/deserialization
    pub storage: StorageConfigInner,
}

// This `impl` block provides a default instance of `SelectedStorageConfig`.
// It's useful for scenarios where no configuration is provided, ensuring
// the application can start with a sensible default.
impl Default for SelectedStorageConfig {
    fn default() -> Self {
        // RocksDB is a more robust default.
        // We now correctly instantiate the struct, setting the engine type and
        // the default values for its corresponding fields in the inner struct.
        Self {
            storage_engine_type: StorageEngineType::RocksDB,
            storage: StorageConfigInner {
                path: default_path(),
                port: default_port(),
                host: None, // Set non-relevant fields to None
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            },
        }
    }

}

impl SelectedStorageConfig {
    pub fn load_from_yaml<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let message = format!("AND THIS WILL FAIL - SEE {}", path_str);
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file {:?}", path.as_ref()))?;
        serde_yaml2::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", path.as_ref()))
    }
}


/// An enum representing the supported storage engine types.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngineType {
    Sled,
    RocksDB,
    InMemory,
    TiKV,
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
            "tikv" => Ok(StorageEngineType::TiKV),
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
            StorageEngineType::TiKV => write!(f, "tikv"),
            StorageEngineType::InMemory => write!(f, "inmemory"),
            StorageEngineType::Redis => write!(f, "redis"),
            StorageEngineType::PostgreSQL => write!(f, "postgresql"),
            StorageEngineType::MySQL => write!(f, "mysql"),
        }
    }
}

impl<'de> Deserialize<'de> for StorageEngineType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "inmemory" => Ok(StorageEngineType::InMemory),
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" => Ok(StorageEngineType::RocksDB),
            "tikv" => Ok(StorageEngineType::TiKV),
            "redis" => Ok(StorageEngineType::Redis),
            "postgresql" => Ok(StorageEngineType::PostgreSQL),
            "mysql" => Ok(StorageEngineType::MySQL),
            _ => Err(serde::de::Error::custom(format!("Unknown storage engine type: {}", s))),
        }
    }
}

/// Returns the list of available storage engines based on feature flags.
pub fn available_engines() -> Vec<StorageEngineType> {
    let mut engines = vec![StorageEngineType::Sled]; // Sled is always available
    #[cfg(feature = "rocksdb")]
    engines.push(StorageEngineType::RocksDB);
    #[cfg(feature = "tikv")]
    engines.push(StorageEngineType::TiKV);
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

// Helper function to convert StorageEngineType to String
pub fn daemon_api_storage_engine_type_to_string(engine_type: &StorageEngineType) -> String {
    match engine_type {
        StorageEngineType::Sled => "sled".to_string(),
        StorageEngineType::RocksDB => "rocksdb".to_string(),
        StorageEngineType::InMemory => "inmemory".to_string(),
        StorageEngineType::TiKV => "tikv".to_string(),
        StorageEngineType::Redis => "redis".to_string(),
        StorageEngineType::PostgreSQL => "postgresql".to_string(),
        StorageEngineType::MySQL => "mysql".to_string(),
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EngineTypeOnly {
    pub storage_engine_type: StorageEngineType,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RocksdbConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SledConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TikvConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pd_endpoints: Option<String>, // Comma-separated PD endpoints (e.g., "127.0.0.1:2382")
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RedisConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MySQLConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PostgreSQLConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

/// Configuration for the storage engine from YAML config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    #[serde(default = "default_config_root_directory")]
    #[serde(with = "path_buf_serde")]
    pub config_root_directory: PathBuf,
    #[serde(default = "default_data_directory")]
    #[serde(with = "path_buf_serde")]
    pub data_directory: PathBuf,
    #[serde(default = "default_log_directory")]
    pub log_directory: String,
    #[serde(default = "default_default_port")]
    pub default_port: u16,
    #[serde(default = "default_cluster_range")]
    pub cluster_range: String,
    #[serde(default = "default_max_disk_space_gb")]
    pub max_disk_space_gb: u64,
    #[serde(default = "default_min_disk_space_gb")]
    pub min_disk_space_gb: u64,
    #[serde(default = "default_use_raft_for_scale")]
    pub use_raft_for_scale: bool,
    #[serde(default = "default_storage_engine_type")]
    pub storage_engine_type: StorageEngineType,
    #[serde(default)]
    pub connection_string: Option<String>,
    #[serde(default)]
    pub engine_specific_config: Option<HashMap<String, Value>>,
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
                ("path".to_string(), Value::String(format!("{}/sled", DEFAULT_DATA_DIRECTORY))),
                ("host".to_string(), Value::String("127.0.0.1".to_string())),
                ("port".to_string(), Value::Number(DEFAULT_STORAGE_PORT.into())),
            ])),
            max_open_files: default_max_open_files(),
        }
    }
}

impl StorageConfig {
    pub fn save(&self) -> Result<()> {
        let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
        let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        let config_path = if project_config_path.exists() {
            project_config_path
        } else {
            default_config_path
        };
        
        let (engine_path, engine_host, engine_port) = match self.engine_specific_config.as_ref() {
            Some(es) => {
                let p = es.get("path")
                    .and_then(|v| v.as_str())
                    .map(PathBuf::from)
                    .unwrap_or_else(|| {
                        PathBuf::from(format!("{}/{}", DEFAULT_DATA_DIRECTORY, self.storage_engine_type.to_string().to_lowercase()))
                    });
                let h = es.get("host")
                    .and_then(|v| v.as_str())
                    .unwrap_or("127.0.0.1")
                    .to_string();
                let pt = es.get("port")
                    .and_then(|v| v.as_u64())
                    .map(|p| p as u16)
                    .unwrap_or(self.default_port);
                (p, h, pt)
            },
            None => {
                let default_path = PathBuf::from(format!("{}/{}", DEFAULT_DATA_DIRECTORY, self.storage_engine_type.to_string().to_lowercase()));
                let default_host = "127.0.0.1".to_string();
                let default_port = self.default_port;
                (default_path, default_host, default_port)
            }
        };
        
        let yaml_string = format!(
r#"storage:
  config_root_directory: "{}"
  data_directory: "{}"
  log_directory: "{}"
  default_port: {}
  cluster_range: "{}"
  max_disk_space_gb: {}
  min_disk_space_gb: {}
  use_raft_for_scale: {}
  storage_engine_type: "{}"
  engine_specific_config:
    path: "{}"
    host: "{}"
    port: {}
  max_open_files: {}
"#,
            self.config_root_directory.display(),
            self.data_directory.display(),
            self.log_directory,
            self.default_port,
            self.cluster_range,
            self.max_disk_space_gb,
            self.min_disk_space_gb,
            self.use_raft_for_scale,
            self.storage_engine_type.to_string(),
            engine_path.display(),
            engine_host,
            engine_port,
            self.max_open_files.unwrap_or(-1)
        );
        
        fs::create_dir_all(config_path.parent().unwrap())
            .context(format!("Failed to create parent directories for {}", config_path.display()))?;
        fs::write(&config_path, yaml_string)
            .context(format!("Failed to write StorageConfig to file: {}", config_path.display()))?;
        info!("Saved storage configuration to {:?}", config_path);
        
        let written_content = fs::read_to_string(&config_path)
            .context(format!("Failed to read back storage config file: {}", config_path.display()))?;
        match serde_yaml::from_str::<StorageConfigWrapper>(&written_content) {
            Ok(_) => info!("Successfully verified written storage config at {:?}", config_path),
            Err(e) => warn!("Failed to verify written storage config at {:?}: {}", config_path, e),
        };
        
        Ok(())
    }
}

fn default_config_root_directory() -> PathBuf {
    trace!("Default config_root_directory: {}", DEFAULT_CONFIG_ROOT_DIRECTORY_STR);
    PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
}

pub fn default_data_directory() -> PathBuf {
    trace!("Default data_directory: {}", DEFAULT_DATA_DIRECTORY);
    PathBuf::from(DEFAULT_DATA_DIRECTORY)
}

pub fn default_log_directory() -> String {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

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
}

impl Default for LogConfig {
    fn default() -> Self {
        trace!("Creating default LogConfig");
        LogConfig {}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PathsConfig {
}

impl Default for PathsConfig {
    fn default() -> Self {
        trace!("Creating default PathsConfig");
        PathsConfig {}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SecurityConfig {
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

/*
impl ToString for StorageEngineType {
    fn to_string(&self) -> String {
        match self {
            StorageEngineType::Sled => "sled".to_string(),
            StorageEngineType::RocksDB => "rocksdb".to_string(),
            StorageEngineType::TiKV => "tikv".to_string(),
            StorageEngineType::Redis => "redis".to_string(),
            StorageEngineType::PostgreSQL => "postgresql".to_string(),
            StorageEngineType::MySQL => "mysql".to_string(),
            StorageEngineType::InMemory => "inmemory".to_string(),
        }
    }
}*/

pub fn is_port_in_cluster_range(port: u16, range_str: &str) -> bool {
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() == 1 {
        if let Ok(single_port) = parts[0].trim().parse::<u16>() {
            return port == single_port;
        }
    } else if parts.len() == 2 {
        if let (Ok(start), Ok(end)) = (parts[0].trim().parse::<u16>(), parts[1].trim().parse::<u16>()) {
            return port >= start && port <= end;
        }
    }
    false
}   

pub fn load_storage_config_from_yaml(config_file_path: Option<&PathBuf>) -> Result<StorageConfig, GraphError> {
    let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
    let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let path_to_use = config_file_path
        .unwrap_or_else(|| {
            if project_config_path.exists() {
                debug!("Using project config path: {:?}", project_config_path);
                &project_config_path
            } else {
                debug!("Using default config path: {:?}", default_config_path);
                &default_config_path
            }
        });
    info!("Loading storage config from {:?}", path_to_use);
    debug!("Current working directory: {:?}", std::env::current_dir().unwrap_or_default());

    let mut config = if path_to_use.exists() {
        let config_content = fs::read_to_string(&path_to_use)
            .context(format!("Failed to read storage config file: {}", path_to_use.display()))?;
        debug!("Raw YAML content:\n{}", config_content);

        // Replace underscores with hyphens for all relevant fields
        let modified_content = config_content
            .replace("storage_engine_type", "storage-engine-type")
            .replace("engine_specific_config", "engine-specific-config");
        debug!("Modified YAML content:\n{}", modified_content);

        // Try parsing as StorageConfigWrapper
        match serde_yaml::from_str::<StorageConfigWrapper>(&modified_content) {
            Ok(wrapper) => {
                info!("Successfully parsed config with 'storage' key.");
                debug!("Parsed StorageConfigWrapper: {:?}", wrapper);
                let mut config = wrapper.storage;
                // Ensure engine_specific_config is populated
                if config.engine_specific_config.is_none() {
                    warn!("engine_specific_config is None after parsing StorageConfigWrapper. Populating from JSON.");
                    if let Ok(json_value) = serde_yaml::from_str::<serde_json::Value>(&modified_content) {
                        if let Some(storage) = json_value.get("storage").and_then(|v| v.get("engine-specific-config")) {
                            config.engine_specific_config = Some(serde_json::from_value(storage.clone()).map_err(|e| {
                                error!("Failed to deserialize engine_specific_config from JSON: {}", e);
                                GraphError::SerializationError(e.to_string())
                            })?);
                            debug!("Populated engine_specific_config: {:?}", config.engine_specific_config);
                        }
                    }
                }
                config
            }
            Err(e1) => {
                warn!("Failed to parse as StorageConfigWrapper: {}. Trying direct StorageConfig.", e1);
                // Try direct StorageConfig
                match serde_yaml::from_str::<StorageConfig>(&modified_content) {
                    Ok(config) => {
                        info!("Successfully parsed config directly as StorageConfig.");
                        debug!("Parsed StorageConfig: {:?}", config);
                        config
                    }
                    Err(e2) => {
                        error!("Failed to parse storage config YAML at {}: {}", path_to_use.display(), e2);
                        if let Ok(json_value) = serde_yaml::from_str::<serde_json::Value>(&modified_content) {
                            error!("Partial YAML parse result: {:?}", json_value);
                        } else {
                            error!("Failed to parse YAML content as JSON for partial logging");
                        }
                        return Err(anyhow!("Serialization error: {}", e2).into());
                    }
                }
            }
        }
    } else {
        warn!("Config file not found at {}. Using default storage config.", path_to_use.display());
        StorageConfig::default()
    };

    // Set data_directory to default if empty
    if config.data_directory.as_os_str().is_empty() {
        warn!("No data_directory specified in config, applying default: {:?}", default_data_directory());
        config.data_directory = default_data_directory();
    }

    // Validate cluster range
    let range = parse_cluster_range(&config.cluster_range)?;
    if !range.contains(&config.default_port) {
        warn!("Port {} is outside cluster range {}", config.default_port, config.cluster_range);
        return Err(anyhow!("Port {} is outside cluster range {}", config.default_port, config.cluster_range).into());
    }

    // Ensure directories exist
    fs::create_dir_all(&config.data_directory)
        .context(format!("Failed to create data directory {:?}", config.data_directory))?;
    info!("Ensured data directory exists: {:?}", config.data_directory);

    if config.log_directory.is_empty() {
        warn!("No log_directory specified in config, using default: {:?}", default_log_directory());
        config.log_directory = default_log_directory();
    }
    fs::create_dir_all(&config.log_directory)
        .context(format!("Failed to create log directory {:?}", config.log_directory))?;
    info!("Ensured log directory exists: {:?}", config.log_directory);

    // Ensure engine_specific_config has storage_engine_type
    if let Some(ref mut engine_config) = config.engine_specific_config {
        let storage_type_value = serde_json::to_value(config.storage_engine_type.clone())
            .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize StorageEngineType: {}", e)))?;
        engine_config.insert("storage_engine_type".to_string(), storage_type_value);
    }

    info!(
        "Loaded storage config: default_port={}, cluster_range={}, data_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, log_directory={:?}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        config.default_port,
        config.cluster_range,
        config.data_directory,
        config.storage_engine_type,
        config.engine_specific_config,
        config.log_directory,
        config.max_disk_space_gb,
        config.min_disk_space_gb,
        config.use_raft_for_scale
    );
    println!(
        "[DEBUG] => Loaded config: default_port={}, cluster_range={}, data_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, log_directory={:?}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        config.default_port,
        config.cluster_range,
        config.data_directory,
        config.storage_engine_type,
        config.engine_specific_config,
        config.log_directory,
        config.max_disk_space_gb,
        config.min_disk_space_gb,
        config.use_raft_for_scale
    );
    Ok(config)
}

fn replace_underscore_with_hyphen(v: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = v {
        let mut new_map = Map::new();
        let keys: Vec<String> = map.keys().cloned().collect();
        for k in keys {
            let new_k = k.replace('_', "-");
            if let Some(mut v) = map.remove(&k) {
                replace_underscore_with_hyphen(&mut v);
                new_map.insert(new_k, v);
            }
        }
        *map = new_map;
    } else if let serde_json::Value::Array(seq) = v {
        for item in seq {
            replace_underscore_with_hyphen(item);
        }
    }
}

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

pub fn format_engine_config(storage_config: &StorageConfig) -> Vec<String> {
    trace!("Formatting engine config for {:?}", storage_config);
    let mut config_lines = Vec::new();
    
    config_lines.push(format!("Engine: {}", storage_config.storage_engine_type));
    trace!("Added engine type: {}", storage_config.storage_engine_type);
    
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

fn create_default_yaml_config(yaml_path: &PathBuf, engine_type: StorageEngineType) -> Result<(), GraphError> {
    info!("Creating default YAML config at {:?}", yaml_path);
    trace!("Creating default config for engine_type: {:?}", engine_type);
    
    let mut config = StorageConfig::default();
    config.storage_engine_type = engine_type;
    trace!("Set storage_engine_type to: {:?}", engine_type);
    
    let engine_path = match engine_type {
        StorageEngineType::Sled => format!("{}/sled", DEFAULT_DATA_DIRECTORY),
        StorageEngineType::RocksDB => format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY),
        StorageEngineType::Redis => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::PostgreSQL => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::MySQL => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::InMemory => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::TiKV => format!("{}/tikv", DEFAULT_DATA_DIRECTORY),
    };
    
    let engine_port = match engine_type {
        StorageEngineType::Redis => 6379,
        StorageEngineType::PostgreSQL => 5432,
        StorageEngineType::MySQL => 3306,
        _ => DEFAULT_STORAGE_PORT,
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

pub fn load_engine_specific_config(
    engine_type: StorageEngineType,
    base_path: &Path,
) -> Result<HashMap<String, Value>, GraphError> {
    let engine_config_path = base_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("storage_config_{}.yaml", engine_type.to_string().to_lowercase()));
    info!("Attempting to load engine-specific config for {:?} from {:?}", engine_type, engine_config_path);
    trace!("Constructed engine config path: {:?}", engine_config_path);

    let mut engine_specific_config = HashMap::new();

    if engine_config_path.exists() {
        let content = fs::read_to_string(&engine_config_path)
            .map_err(|e| {
                error!("Failed to read engine-specific YAML file at {:?}: {}", engine_config_path, e);
                GraphError::Io(e)
            })?;
        debug!("Raw engine-specific YAML content from {:?}:\n{}", engine_config_path, content);
        trace!("Successfully read content from engine config file.");

        let wrapper: StorageConfigWrapper = serde_yaml::from_str(&content)
            .map_err(|e| {
                error!("Failed to deserialize engine-specific YAML from {:?}: {}", engine_config_path, e);
                trace!("Deserialization error details: {:?}", e);
                GraphError::SerializationError(format!("Failed to deserialize engine-specific YAML: {}", e))
            })?;

        engine_specific_config = wrapper.storage.engine_specific_config.unwrap_or_default();
        debug!("Deserialized engine-specific config: {:?}", engine_specific_config);
        trace!("Successfully deserialized engine-specific config.");
    } else {
        warn!("Engine-specific config file not found at {:?}. Using defaults.", engine_config_path);
        trace!("Engine config file does not exist. Proceeding with default values.");
    }

    if matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) &&
        (!engine_specific_config.contains_key("path") ||
         engine_specific_config.get("path").and_then(|v| v.as_str()).unwrap_or("").is_empty()) {
        let default_path = format!("{}/{}", DEFAULT_DATA_DIRECTORY, engine_type.to_string().to_lowercase());
        warn!("No valid 'path' in engine-specific config for {:?}, using default: {:?}", engine_type, default_path);
        engine_specific_config.insert("path".to_string(), Value::String(default_path));
        trace!("Inserted default path into config.");
    }

    if !engine_specific_config.contains_key("host") &&
        matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) {
        debug!("No 'host' in engine-specific config for {:?}, using default: 127.0.0.1", engine_type);
        engine_specific_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
        trace!("Inserted default host into config.");
    }
    if !engine_specific_config.contains_key("port") &&
        matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) {
        let default_port = match engine_type {
            StorageEngineType::RocksDB | StorageEngineType::Sled => 8049,
            _ => 0,
        };
        debug!("No 'port' in engine-specific config for {:?}, using default: {}", engine_type, default_port);
        engine_specific_config.insert("port".to_string(), Value::Number(default_port.into()));
        trace!("Inserted default port into config.");
    }

    let storage_type_value = serde_json::to_value(engine_type.clone())
        .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize StorageEngineType: {}", e)))?;
    engine_specific_config.insert("storage_engine_type".to_string(), storage_type_value);
    trace!("Inserted storage_engine_type into config.");

    trace!("Final engine-specific config for {:?}: {:?}", engine_type, engine_specific_config);
    Ok(engine_specific_config)
}

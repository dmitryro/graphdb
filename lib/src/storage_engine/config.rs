use std::sync::Arc;
use anyhow::{Result, Context, anyhow};
use serde::{Deserialize, Serialize, Deserializer, Serializer};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use models::errors::{GraphError, GraphError::StorageError};
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
use std::fs;
use std::io::Write;
use std::fmt::{self, Display, Formatter};
use std::collections::HashMap;
use log::{info, debug, warn, error, trace};
use openraft_memstore::MemStore;
#[cfg(feature = "with-openraft-sled")]
use openraft_sled::SledRaftStorage;
use sled::{Config, Db, Tree};
use crate::daemon_registry::DaemonMetadata;

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
pub const DEFAULT_STORAGE_CONFIG_PATH_INMEMORY: &str = "./storage_daemon_server/storage_config_inmemory.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_HYBRID: &str = "./storage_daemon_server/storage_config_hybrid.yaml";
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
    pub fn serialize<S>(path: &PathBuf, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
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
    pub fn serialize<S>(value: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
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
    pub fn serialize<S>(value: &Option<StorageEngineType>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
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

// Define a struct to mirror the YAML configuration
#[derive(Debug, Deserialize)]
struct EngineConfig {
    storage: HashMap<String, Value>,
}


// --- Struct Definitions ---
#[derive(Debug, Clone)]
pub struct SledDaemon {
    pub port: u16,
    pub db_path: PathBuf,
    pub db: Arc<Db>,
    pub vertices: Tree,
    pub edges: Tree,
    pub kv_pairs: Tree,
    pub running: Arc<TokioMutex<bool>>,
    #[cfg(feature = "with-openraft-sled")]
    pub raft_storage: Arc<openraft_sled::SledRaftStorage>,
    #[cfg(feature = "with-openraft-sled")]
    pub node_id: u64,
}

#[derive(Debug, Default, Clone)]
pub struct SledDaemonPool {
    pub daemons: HashMap<u16, Arc<SledDaemon>>,
    pub registry: Arc<RwLock<HashMap<u16, DaemonMetadata>>>,
    pub initialized: Arc<RwLock<bool>>, 
}

#[derive(Debug, Clone)]
pub struct SledStorage {
    pub pool: Arc<TokioMutex<SledDaemonPool>>,
}

fn default_path() -> Option<PathBuf> {
    Some(PathBuf::from("/opt/graphdb/storage_data"))
}

fn default_host() -> Option<String> {
    Some("127.0.0.1".to_string())
}

fn default_port() -> Option<u16> {
    Some(DEFAULT_STORAGE_PORT)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfigInner {
    #[serde(default = "default_path")]
    pub path: Option<PathBuf>,
    #[serde(default = "default_host")]
    pub host: Option<String>,
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
  
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct EngineSpecificConfig {
    pub storage_engine_type: StorageEngineType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pd_endpoints: Option<String>,
}   

#[derive(Debug, Deserialize, Serialize)]
pub struct EngineSpecificConfigWrapper {
    storage: EngineSpecificConfig,
}   

#[derive(Clone, Debug, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig;

pub struct MemStoreForTypeConfig {
    pub inner: Arc<MemStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelectedStorageConfig {
    pub storage_engine_type: StorageEngineType,
    #[serde(flatten)]
    pub storage: StorageConfigInner,
}

impl Default for SelectedStorageConfig {
    fn default() -> Self {
        Self {
            storage_engine_type: StorageEngineType::Sled,
            storage: StorageConfigInner {
                path: default_path(),
                port: default_port(),
                host: default_host(),
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
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file {:?}", path))?;
        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", path))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngineType {
    Hybrid,
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
            "hybrid" => Ok(StorageEngineType::Hybrid),
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
            StorageEngineType::Hybrid => write!(f, "hybrid"),
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
            "hybrid" => Ok(StorageEngineType::Hybrid),
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

pub fn is_engine_specific_config_complete(config: &Option<HashMap<String, Value>>) -> bool {
    if let Some(engine_config) = config {
        engine_config.contains_key("path") || engine_config.contains_key("host")
    } else {
        false
    }
}

pub fn create_default_selected_storage_config(engine_type: &StorageEngineType) -> HashMap<String, Value> {
    let engine_path_name = engine_type.to_string().to_lowercase();
    let engine_data_path = PathBuf::from(DEFAULT_DATA_DIRECTORY).join(&engine_path_name);
    let engine_port = match engine_type {
        StorageEngineType::Redis => 6379,
        StorageEngineType::PostgreSQL => 5432,
        StorageEngineType::MySQL => 3306,
        _ => DEFAULT_STORAGE_PORT,
    };
    HashMap::from_iter(vec![
        ("storage_engine_type".to_string(), Value::String(engine_path_name)),
        ("path".to_string(), Value::String(engine_data_path.to_string_lossy().into())),
        ("host".to_string(), Value::String("127.0.0.1".to_string())),
        ("port".to_string(), Value::Number(engine_port.into())),
    ])
}

pub fn available_engines() -> Vec<StorageEngineType> {
    let mut engines = vec![StorageEngineType::Sled];
    #[cfg(feature = "rocksdb")]
    engines.push(StorageEngineType::RocksDB);
    #[cfg(feature = "tikv")]
    engines.push(StorageEngineType::TiKV);
    #[cfg(any(feature = "sled", feature = "rocksdb", feature = "tikv"))]
    engines.push(StorageEngineType::Hybrid);
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

pub fn daemon_api_storage_engine_type_to_string(engine_type: &StorageEngineType) -> String {
    match engine_type {
        StorageEngineType::Hybrid => "hybrid".to_string(),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksdbConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SledConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
    #[serde(default)]
    pub temporary: bool,
    #[serde(default)]
    pub use_compression: bool,
    #[serde(default)]
    pub cache_capacity: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TikvConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pd_endpoints: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySQLConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgreSQLConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

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
            engine_specific_config: Some(create_default_selected_storage_config(&StorageEngineType::Sled)),
            max_open_files: default_max_open_files(),
        }
    }
}

impl StorageConfig {
    pub async fn load(path: &Path) -> Result<StorageConfig> {
        if !path.exists() {
            info!("Config file not found at {:?}", path);
            let default_config = StorageConfig::default();
            default_config.save().await.context("Failed to save default config.")?;
            return Ok(default_config);
        }

        let config_content = fs::read_to_string(path)
            .context(format!("Failed to read storage config file: {}", path.display()))?;
        debug!("Raw YAML content from {:?}:\n{}", path, config_content);

        let wrapper: StorageConfigWrapper = serde_yaml::from_str(&config_content)
            .map_err(|e| anyhow!("Failed to parse YAML as StorageConfigWrapper: {}", e))?;
        let mut config = wrapper.storage;

        // Prioritize engine_specific_config["port"] if present
        if let Some(engine_config) = &config.engine_specific_config {
            if let Some(port) = engine_config.get("port").and_then(|v| v.as_u64()).map(|p| p as u16) {
                config.default_port = port;
                debug!("Overriding default_port with engine_specific_config port: {}", port);
            }
        }

        if config.engine_specific_config.is_none() || !is_engine_specific_config_complete(&config.engine_specific_config) {
            let engine_specific_config_path = match config.storage_engine_type.to_string().to_lowercase().as_str() {
                "sled" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED)),
                "rocksdb" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB)),
                "tikv" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV)),
                "mysql" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL)),
                "postgres" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES)),
                "redis" => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS)),
                _ => None,
            };

            if let Some(engine_config_path) = engine_specific_config_path {
                if engine_config_path.exists() {
                    info!("engine_specific_config is null or incomplete, loading from {:?}", engine_config_path);
                    let engine_config_content = fs::read_to_string(&engine_config_path)
                        .context(format!("Failed to read engine-specific config file: {}", engine_config_path.display()))?;
                    debug!("Raw YAML content from {:?}:\n{}", engine_config_path, engine_config_content);

                    let engine_wrapper: StorageConfigWrapper = serde_yaml::from_str(&engine_config_content)
                        .map_err(|e| anyhow!("Failed to parse YAML as StorageConfigWrapper from {:?}: {}", engine_config_path, e))?;
                    config.engine_specific_config = engine_wrapper.storage.engine_specific_config.or_else(|| {
                        info!("No valid engine_specific_config in {:?}, using default for {:?}", 
                            engine_config_path, config.storage_engine_type);
                        Some(create_default_selected_storage_config(&config.storage_engine_type))
                    });
                } else {
                    info!("Engine-specific config file not found at {:?}, using default for {:?}", 
                        engine_config_path, config.storage_engine_type);
                    config.engine_specific_config = Some(create_default_selected_storage_config(&config.storage_engine_type));
                }
            } else {
                info!("No engine-specific config path defined for engine type {:?}, using default", config.storage_engine_type);
                config.engine_specific_config = Some(create_default_selected_storage_config(&config.storage_engine_type));
            }
        }

        if let Some(engine_config) = config.engine_specific_config.clone() {
            if let Some(engine_type_value) = engine_config.get("storage_engine_type") {
                let engine_type_str = engine_type_value.as_str().ok_or_else(|| {
                    error!("Invalid storage_engine_type in engine_specific_config: {:?}", engine_type_value);
                    GraphError::ConfigurationError("Invalid storage_engine_type in engine_specific_config".to_string())
                })?;
                let engine_type = StorageEngineType::from_str(engine_type_str).map_err(|e| {
                    error!("Failed to parse storage_engine_type from engine_specific_config: {}", e);
                    e
                })?;
                if config.storage_engine_type != engine_type {
                    info!(
                        "Top-level storage_engine_type ({:?}) does not match engine_specific_config ({:?}). Synchronizing to the engine-specific type.",
                        config.storage_engine_type, engine_type
                    );
                    config.storage_engine_type = engine_type;
                }
            }

            let engine_path_name = config.storage_engine_type.to_string().to_lowercase();
            let data_dir_path = PathBuf::from(DEFAULT_DATA_DIRECTORY);
            let engine_data_path = data_dir_path.join(&engine_path_name);

            let config_path = engine_config.get("path").and_then(|v| v.as_str()).map(PathBuf::from);
            if config_path.is_none() || config_path != Some(engine_data_path.clone()) {
                info!(
                    "Engine-specific path was not set or mismatched. Setting path to: {:?}", engine_data_path
                );
                let mut updated_engine_config = engine_config.clone();
                updated_engine_config.insert("path".to_string(), Value::String(engine_data_path.to_string_lossy().into()));
                updated_engine_config.insert("port".to_string(), Value::Number(config.default_port.into()));
                config.engine_specific_config = Some(updated_engine_config);
            }
        } else {
            info!("'engine_specific_config' was missing, setting to default for engine: {:?}", config.storage_engine_type);
            config.engine_specific_config = Some(create_default_selected_storage_config(&config.storage_engine_type));
        }

        fs::create_dir_all(&config.data_directory)
            .context(format!("Failed to create data directory {:?}", config.data_directory))?;
        info!("Ensured data directory exists: {:?}", config.data_directory);

        fs::create_dir_all(&config.log_directory)
            .context(format!("Failed to create log directory {:?}", config.log_directory))?;
        info!("Ensured log directory exists: {:?}", config.log_directory);

        let validated_config = config.validate().context("Failed to validate storage configuration")?;
        info!("Successfully loaded and validated storage configuration from {:?}", path);

        debug!(
            "Final validated config: default_port={}, cluster_range={}, data_directory={:?}, log_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, use_raft_for_scale={}",
            validated_config.default_port,
            validated_config.cluster_range,
            validated_config.data_directory,
            validated_config.log_directory,
            validated_config.storage_engine_type,
            validated_config.engine_specific_config,
            validated_config.use_raft_for_scale,
        );
        Ok(validated_config)
    }

    pub async fn save(&self) -> Result<(), anyhow::Error> {
        let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        debug!("Saving configuration to {:?}", config_path);

        let (engine_path, engine_host, engine_port, engine_username, engine_password, engine_pd_endpoints) = match self.engine_specific_config.as_ref() {
            Some(es) => (
                es.get("path").and_then(|v| v.as_str()).map(PathBuf::from).unwrap_or_else(|| PathBuf::from(format!(
                    "{}/{}",
                    DEFAULT_DATA_DIRECTORY,
                    self.storage_engine_type.to_string().to_lowercase()
                ))),
                es.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1").to_string(),
                es.get("port").and_then(|v| v.as_u64()).map(|p| p as u16).unwrap_or(self.default_port),
                es.get("username").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                es.get("password").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
                es.get("pd_endpoints").and_then(|v| v.as_str()).unwrap_or_default().to_string(),
            ),
            None => (
                PathBuf::from(format!(
                    "{}/{}",
                    DEFAULT_DATA_DIRECTORY,
                    self.storage_engine_type.to_string().to_lowercase()
                )),
                "127.0.0.1".to_string(),
                self.default_port,
                String::new(),
                String::new(),
                String::new(),
            ),
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
        storage_engine_type: "{}"
        username: "{}"
        password: "{}"
        pd_endpoints: "{}"
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
            self.storage_engine_type.to_string().to_lowercase(),
            engine_path.display(),
            engine_host,
            engine_port,
            self.storage_engine_type.to_string().to_lowercase(),
            engine_username,
            engine_password,
            engine_pd_endpoints,
            self.max_open_files.unwrap_or(1024)
        );

        fs::create_dir_all(config_path.parent().unwrap())
            .context(format!("Failed to create parent directories for {}", config_path.display()))?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&config_path)
            .context(format!("Failed to open config file for writing: {}", config_path.display()))?;
        file.write_all(yaml_string.as_bytes())
            .context(format!("Failed to write StorageConfig to file: {}", config_path.display()))?;
        file.flush()
            .context(format!("Failed to flush config file: {}", config_path.display()))?;
        info!("Saved storage configuration to {:?}", config_path);

        let written_content = fs::read_to_string(&config_path)
            .context(format!("Failed to read back storage config file: {}", config_path.display()))?;
        if written_content != yaml_string {
            error!("Written config does not match expected content at {:?}", config_path);
            return Err(anyhow!("Written config verification failed"));
        }
        debug!("Verified written content:\n{}", written_content);

        Ok(())
    }

    pub fn validate(self) -> Result<Self, GraphError> {
        let available_engines = available_engines();
        if !available_engines.contains(&self.storage_engine_type) {
            return Err(GraphError::InvalidStorageEngine(format!(
                "Storage engine {:?} is not enabled. Available engines: {:?}", 
                self.storage_engine_type, available_engines
            )));
        }
        if let Some(engine_config) = &self.engine_specific_config {
            if let Some(engine_type_value) = engine_config.get("storage_engine_type") {
                let engine_type = StorageEngineType::from_str(
                    engine_type_value.as_str().ok_or_else(|| {
                        GraphError::ConfigurationError("Invalid storage_engine_type in engine_specific_config".to_string())
                    })?
                )?;
                if engine_type != self.storage_engine_type {
                    return Err(GraphError::ConfigurationError(
                        "engine_specific_config.storage_engine_type must match storage_engine_type".to_string()
                    ));
                }
            }
            if let Some(port) = engine_config.get("port").and_then(|v| v.as_u64()).map(|p| p as u16) {
                if !is_port_in_cluster_range(port, &self.cluster_range) {
                    return Err(GraphError::ConfigurationError(format!(
                        "Port {} is outside cluster range {}", port, self.cluster_range
                    )));
                }
            }
        }
        if self.default_port == 0 {
            return Err(GraphError::ConfigurationError("default_port must be non-zero".to_string()));
        }
        if !is_port_in_cluster_range(self.default_port, &self.cluster_range) {
            return Err(GraphError::ConfigurationError(format!(
                "default_port {} is outside cluster range {}", self.default_port, self.cluster_range
            )));
        }
        if self.cluster_range.is_empty() {
            return Err(GraphError::ConfigurationError("cluster_range must be non-empty".to_string()));
        }
        Ok(self)
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
    trace!("Default cluster_range: {}", DEFAULT_CLUSTER_RANGE);
    DEFAULT_CLUSTER_RANGE.to_string()
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
    trace!("Default use_raft_for_scale: false");
    false
}

fn default_storage_engine_type() -> StorageEngineType {
    trace!("Default storage_engine_type: Sled");
    StorageEngineType::Sled
}

fn default_max_open_files() -> Option<i32> {
    trace!("Default max_open_files: 1024");
    Some(1024)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub version: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        trace!("Creating default AppConfig");
        AppConfig { version: Some("0.1.0".to_string()) }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogConfig {
}

impl Default for LogConfig {
    fn default() -> Self {
        trace!("Creating default LogConfig");
        LogConfig {}
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PathsConfig {
}

impl Default for PathsConfig {
    fn default() -> Self {
        trace!("Creating default PathsConfig");
        PathsConfig {}
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SecurityConfig {
}

impl Default for SecurityConfig {
    fn default() -> Self {
        trace!("Creating default SecurityConfig");
        SecurityConfig {}
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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
            cluster_range: Some(DEFAULT_CLUSTER_RANGE.to_string()),
            data_directory: Some(DEFAULT_DATA_DIRECTORY.to_string()),
            config_root_directory: Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            log_directory: Some(DEFAULT_LOG_DIRECTORY.to_string()),
            max_disk_space_gb: Some(1000),
            min_disk_space_gb: Some(10),
            use_raft_for_scale: Some(false),
            storage_engine_type: Some(StorageEngineType::Sled),
            max_open_files: Some(1024),
            connection_string: None,
            engine_specific_config: Some(create_default_selected_storage_config(&StorageEngineType::Sled)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

pub async fn load_storage_config_from_yaml(config_file_path: Option<&Path>) -> Result<StorageConfig, GraphError> {
    // Determine the path to use for loading the main configuration file
    let main_config_path = config_file_path.map(|p| p.to_path_buf()).unwrap_or_else(|| {
        let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        if project_config_path.exists() {
            debug!("Using project config path: {:?}", project_config_path);
            project_config_path
        } else {
            let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
            debug!("Using default config path: {:?}", default_config_path);
            default_config_path
        }
    });

    // Log the file being read
    debug!("Reading configuration from file: {:?}", main_config_path);

    // Use StorageConfig::load to handle the loading and validation
    let config = StorageConfig::load(&main_config_path).await
        .context(format!("Failed to load storage config from {:?}", main_config_path))?;

    // Log the final validated configuration
    debug!("Final validated configuration: {:?}", config);
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

pub fn parse_cluster_range(range: &str) -> Result<std::ops::RangeInclusive<u16>, GraphError> {
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
        StorageEngineType::Hybrid => format!("{}/hybrid", DEFAULT_DATA_DIRECTORY),
    };
    
    let engine_port = match engine_type {
        StorageEngineType::Redis => 6379,
        StorageEngineType::PostgreSQL => 5432,
        StorageEngineType::MySQL => 3306,
        _ => config.default_port,
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


// Loads engine-specific configuration
pub fn load_engine_specific_config(
    engine_type: StorageEngineType,
    base_path: &Path,
) -> Result<HashMap<String, Value>, GraphError> {
    let engine_config_path = base_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("storage_config_{}.yaml", engine_type.to_string().to_lowercase()));
    info!(
        "Attempting to load engine-specific config for {:?} from {:?}",
        engine_type, engine_config_path
    );
    trace!("Constructed engine config path: {:?}", engine_config_path);

    // Initialize the default engine-specific config map
    let mut engine_specific_config: HashMap<String, Value> = HashMap::new();

    if engine_config_path.exists() {
        let content = fs::read_to_string(&engine_config_path).map_err(|e| {
            error!(
                "Failed to read engine-specific YAML file at {:?}: {}",
                engine_config_path, e
            );
            GraphError::Io(e)
        })?;
        debug!(
            "Raw engine-specific YAML content from {:?}:\n{}",
            engine_config_path, content
        );
        trace!("Successfully read content from engine config file.");

        // Deserialize the YAML content directly into our custom struct
        let config: EngineConfig = serde_yaml::from_str(&content).map_err(|e| {
            error!(
                "Failed to deserialize engine-specific YAML from {:?}: {}",
                engine_config_path, e
            );
            GraphError::SerializationError(format!(
                "Failed to deserialize engine-specific YAML: {}",
                e
            ))
        })?;

        // Validate and insert values from the deserialized struct
        let mut storage_map = config.storage;

        if let Some(storage_engine_type_value) = storage_map.remove("storage_engine_type") {
            let storage_engine_type_str =
                storage_engine_type_value
                    .as_str()
                    .ok_or_else(|| GraphError::SerializationError(
                        "storage_engine_type value is not a string".to_string()
                    ))?;
            let storage_engine_type = StorageEngineType::from_str(storage_engine_type_str)
                .map_err(|e| GraphError::SerializationError(format!("Invalid storage_engine_type: {}", e)))?;
            engine_specific_config.insert("storage_engine_type".to_string(), Value::String(storage_engine_type.to_string().to_lowercase()));
        }

        // Insert remaining fields from the storage map
        for (key, value) in storage_map {
            engine_specific_config.insert(key, value);
        }

        debug!(
            "Deserialized engine-specific config: {:?}",
            engine_specific_config
        );
        trace!("Successfully deserialized engine-specific config.");
    } else {
        warn!(
            "Engine-specific config file not found at {:?}. Using defaults.",
            engine_config_path
        );
        trace!("Engine config file does not exist. Proceeding with default values.");
    }

    // Ensure required fields for Sled and RocksDB
    if matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) {
        if !engine_specific_config.contains_key("path") || engine_specific_config.get("path").and_then(|v| v.as_str()).unwrap_or("").is_empty() {
            let default_path = format!("{}/{}", "/opt/graphdb/storage_data", engine_type.to_string().to_lowercase());
            warn!("No valid 'path' in engine-specific config for {:?}, using default: {:?}", engine_type, default_path);
            engine_specific_config.insert("path".to_string(), Value::String(default_path));
        }
        if !engine_specific_config.contains_key("host") {
            debug!("No 'host' in engine-specific config for {:?}, using default: 127.0.0.1", engine_type);
            engine_specific_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
        }
        if !engine_specific_config.contains_key("port") {
            let default_port = 8049;
            debug!("No 'port' in engine-specific config for {:?}, using default: {}", engine_type, default_port);
            engine_specific_config.insert("port".to_string(), Value::Number(default_port.into()));
        }
    }
    // Ensure required fields for TiKV
    if matches!(engine_type, StorageEngineType::TiKV) {
        if !engine_specific_config.contains_key("pd_endpoints") || engine_specific_config.get("pd_endpoints").and_then(|v| v.as_str()).unwrap_or("").is_empty() {
            let default_pd_endpoints = "127.0.0.1:2379";
            warn!("No valid 'pd_endpoints' in engine-specific config for TiKV, using default: {:?}", default_pd_endpoints);
            engine_specific_config.insert("pd_endpoints".to_string(), Value::String(default_pd_endpoints.to_string()));
        }
        if !engine_specific_config.contains_key("host") {
            debug!("No 'host' in engine-specific config for TiKV, using default: 127.0.0.1");
            engine_specific_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
        }
        if !engine_specific_config.contains_key("port") {
            let default_port = 2379;
            debug!("No 'port' in engine-specific config for TiKV, using default: {}", default_port);
            engine_specific_config.insert("port".to_string(), Value::Number(default_port.into()));
        }
    }

    // Ensure storage_engine_type is set
    let storage_type_value = serde_json::to_value(engine_type.to_string().to_lowercase())
        .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize StorageEngineType: {}", e)))?;
    engine_specific_config.insert("storage_engine_type".to_string(), storage_type_value);
    trace!(
        "Final engine-specific config for {:?}: {:?}",
        engine_type,
        engine_specific_config
    );
    Ok(engine_specific_config)
}

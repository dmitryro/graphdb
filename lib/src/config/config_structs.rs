use std::sync::Arc;
use anyhow::{Result, Context, anyhow};
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::time::Duration;
use clap::{Args, Parser, Subcommand};
use log::{debug, error, info, warn, trace};
use std::path::{Path, PathBuf};
use std::collections::{HashMap};
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer, Deserializer};
use serde::de::{self, MapAccess, Visitor};
use serde_yaml2 as serde_yaml;
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::{Map, Value};
use crate::config::config_defaults::*;
use crate::config::config_constants::*;
use crate::config::config_serializers::*;
use crate::query_exec_engine::query_exec_engine::{QueryExecEngine};
use crate::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction,
                    ReloadAction, RestCliCommand, StatusAction, StorageAction, ShowAction, ShowArgs,
                    StartAction, StopAction, StopArgs, DaemonCliCommand, UseAction, SaveAction};
use crate::daemon_utils::{is_port_in_cluster_range, is_valid_cluster_range, parse_cluster_range};
pub use crate::storage_engine::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
pub use models::errors::GraphError;
use openraft_memstore::MemStore;
#[cfg(feature = "with-openraft-sled")]
use openraft_sled::SledRaftStorage;
use sled::{Config, Db, Tree};
use crate::daemon_registry::DaemonMetadata;

/// Replication strategy for data operations
/// Represents the strategy for replicating data writes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    /// Replicate to all available nodes
    AllNodes,
    /// Replicate to N nodes (including primary)
    NNodes(usize),
    /// Use Raft consensus
    Raft,
}

/// Health status of a daemon node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub port: u16,
    pub is_healthy: bool,
    pub last_check: SystemTime,
    pub response_time_ms: u64,
    pub error_count: u32,
}

/// Load balancer for routing requests across healthy nodes
/// The configuration for the load balancer in a distributed setup.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct LoadBalancer {
    #[serde(skip)]
    pub nodes: Arc<RwLock<HashMap<u16, NodeHealth>>>,
    #[serde(skip)]
    pub current_index: Arc<TokioMutex<usize>>,
    pub replication_factor: usize,
}

#[derive(Clone)]
pub struct HealthCheckConfig {
    pub interval: Duration,
    pub connect_timeout: Duration,
    pub response_buffer_size: usize,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
            response_buffer_size: 1024,
        }
    }
}

// StorageEngineType
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngineType {
    Hybrid,
    Sled,
    RocksDB,
    TiKV,
    InMemory,
    Redis,
    PostgreSQL,
    MySQL,
}

#[derive(Clone, Debug, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct TypeConfig;

pub struct MemStoreForTypeConfig {
    pub inner: Arc<MemStore>,
}

// Define a struct to mirror the YAML configuration
#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    pub storage: HashMap<String, Value>,
}

// Struct to hold sled::Db and its path
#[derive(Debug)]
pub struct SledDbWithPath {
    pub db: Arc<sled::Db>,
    pub path: PathBuf,
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
    pub load_balancer: Arc<LoadBalancer>,
    pub use_raft: bool,
}

#[derive(Debug, Clone)]
pub struct SledStorage {
    pub pool: Arc<TokioMutex<SledDaemonPool>>,
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

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SledConfig {
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub temporary: bool,
    pub use_compression: bool,
    pub cache_capacity: Option<u64>,
    pub storage_engine_type: StorageEngineType,
}

impl Default for SledConfig {
    fn default() -> Self {
        SledConfig {
            host: Some(String::from("127.0.0.1")),
            port: Some(8049),
            path: PathBuf::from("/opt/graphdb/storage_data/sled"),
            temporary: false,
            use_compression: true,
            cache_capacity: None,
            storage_engine_type: StorageEngineType::Sled,
        }
    }
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

// A temporary struct to deserialize the raw YAML content, which is flexible.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RawStorageConfig {
    pub config_root_directory: Option<PathBuf>,
    pub data_directory: Option<PathBuf>,
    pub log_directory: Option<PathBuf>,
    pub default_port: Option<u16>,
    pub cluster_range: Option<String>,
    pub max_disk_space_gb: Option<u64>,
    pub min_disk_space_gb: Option<u64>,
    pub use_raft_for_scale: Option<bool>,
    pub storage_engine_type: Option<StorageEngineType>,
    pub engine_specific_config: Option<HashMap<String, serde_json::Value>>,
    pub max_open_files: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfigInner {
    #[serde(with = "option_path_buf_serde", default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub pd_endpoints: Option<String>,
    #[serde(default)]
    pub use_compression: bool,
    #[serde(default)]
    pub cache_capacity: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelectedStorageConfig {
    #[serde(with = "storage_engine_type_serde")]
    pub storage_engine_type: StorageEngineType,
    #[serde(flatten)]
    pub storage: StorageConfigInner,
}

// THIS IS THE MISSING STRUCT DEFINITION
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SelectedStorageConfigWrapper {
    pub storage: SelectedStorageConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MainDaemonConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

#[derive(Debug, Deserialize)]
pub struct MainConfigWrapper {
    pub main_daemon: MainDaemonConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestApiConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RestApiConfigWrapper {
   pub config_root_directory: String,
   pub rest_api: RestApiConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaemonYamlConfig {
    pub config_root_directory: String,
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_connections: u32,
    pub max_open_files: u64,
    pub use_raft_for_scale: bool,
    pub log_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(with = "option_path_buf_serde", default, alias = "config-root-directory")]
    pub config_root_directory: Option<PathBuf>,
    #[serde(with = "option_path_buf_serde", default = "default_data_directory", alias = "data-directory")]
    pub data_directory: Option<PathBuf>,
    #[serde(with = "option_path_buf_serde", default = "default_log_directory", alias = "log-directory")]
    pub log_directory: Option<PathBuf>,
    #[serde(default = "default_default_port", alias = "default-port")]
    pub default_port: u16,
    #[serde(with = "string_or_u16_non_option", default = "default_cluster_range", alias = "cluster-range")]
    pub cluster_range: String,
    #[serde(default = "default_max_disk_space_gb", alias = "max-disk-space-gb")]
    pub max_disk_space_gb: u64,
    #[serde(default = "default_min_disk_space_gb", alias = "min-disk-space-gb")]
    pub min_disk_space_gb: u64,
    #[serde(default = "default_use_raft_for_scale", alias = "use-raft-for-scale")]
    pub use_raft_for_scale: bool,
    #[serde(with = "storage_engine_type_serde", default = "default_storage_engine_type", alias = "storage-engine-type")]
    pub storage_engine_type: StorageEngineType,
    #[serde(default = "default_engine_specific_config")]
    pub engine_specific_config: Option<SelectedStorageConfig>,
    #[serde(default = "default_max_open_files", alias = "max-open-files")]
    pub max_open_files: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct TiKVConfigWrapper {
    pub storage: SpecificEngineFileConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SpecificEngineFileConfig {
    #[serde(with = "storage_engine_type_serde", alias = "storage-engine-type")]
    pub storage_engine_type: StorageEngineType,
    #[serde(with = "option_path_buf_serde", default, alias = "path")]
    pub path: Option<PathBuf>,
    #[serde(default, alias = "host")]
    pub host: Option<String>,
    #[serde(default, alias = "port")]
    pub port: Option<u16>,
    #[serde(default, alias = "database")]
    pub database: Option<String>,
    #[serde(default, alias = "username")]
    pub username: Option<String>,
    #[serde(default, alias = "password")]
    pub password: Option<String>,
    #[serde(default, alias = "pd_endpoints")]
    pub pd_endpoints: Option<String>,
}

#[derive(Debug, Parser, Clone)]
#[clap(name = "GraphDB CLI", about = "Command line interface for GraphDB")]
pub struct CliConfig {
    #[clap(subcommand)]
    pub command: Commands,
    #[clap(
        long,
        env = "GRAPHDB_HTTP_TIMEOUT_SECONDS",
        default_value_t = DEFAULT_HTTP_TIMEOUT_SECONDS,
        help = "HTTP request timeout in seconds"
    )]
    pub http_timeout_seconds: u64,
    #[clap(
        long,
        env = "GRAPHDB_HTTP_RETRIES",
        default_value_t = DEFAULT_HTTP_RETRIES,
        help = "Number of HTTP request retries"
    )]
    pub http_retries: u32,
    #[clap(
        long,
        env = "GRAPHDB_GRPC_TIMEOUT_SECONDS",
        default_value_t = DEFAULT_GRPC_TIMEOUT_SECONDS,
        help = "gRPC request timeout in seconds"
    )]
    pub grpc_timeout_seconds: u64,
    #[clap(
        long,
        env = "GRAPHDB_GRPC_RETRIES",
        default_value_t = DEFAULT_GRPC_RETRIES,
        help = "Number of gRPC request retries"
    )]
    pub grpc_retries: u32,
    #[clap(long, help = "Port for the main GraphDB Daemon")]
    pub daemon_port: Option<u16>,
    #[clap(long, help = "Cluster range for the main GraphDB Daemon (e.g., '9001-9005')")]
    pub daemon_cluster: Option<String>,
    #[clap(long, help = "Port for the REST API")]
    pub rest_port: Option<u16>,
    #[clap(long, help = "Cluster range for the REST API")]
    pub rest_cluster: Option<String>,
    #[clap(long, help = "Port for the Storage Daemon (synonym for --port in storage commands)")]
    pub storage_port: Option<u16>,
    #[clap(long, help = "Cluster range for the Storage Daemon (synonym for --cluster in storage commands)")]
    pub storage_cluster: Option<String>,
    #[clap(long, help = "Path to the storage daemon configuration YAML")]
    pub storage_config_path: Option<PathBuf>,
    #[clap(long, help = "Path to the REST API configuration YAML")]
    pub rest_api_config_path: Option<PathBuf>,
    #[clap(long, help = "Path to the main daemon configuration YAML")]
    pub main_daemon_config_path: Option<PathBuf>,
    #[clap(long, help = "Root directory for configurations, if not using default paths")]
    pub config_root_directory: Option<PathBuf>,
    #[clap(long, short = 'c', help = "Run CLI in interactive mode")]
    pub cli: bool,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GlobalYamlConfig {
    pub storage: Option<StorageYamlConfig>,
    pub rest_api: Option<RestApiYamlConfig>,
    pub main_daemon: Option<MainDaemonYamlConfig>,
    pub config_root_directory: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageYamlConfig {
    pub default_port: u16,
    pub cluster_range: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RestApiYamlConfig {
    pub default_port: u16,
    pub cluster_range: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MainDaemonYamlConfig {
    pub default_port: u16,
    pub cluster_range: String,
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

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LogConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PathsConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct SecurityConfig {}

#[derive(Debug, Serialize, Deserialize, Clone)] // Fixed: Added Serialize and Deserialize derives
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
    pub max_connections: u32, // Fixed: Corrected syntax from `<u32>` to `u32`
}

impl Default for ServerConfig {
    fn default() -> Self {
        trace!("Creating default ServerConfig");
        ServerConfig {
            port: Some(DEFAULT_STORAGE_PORT),
            host: Some("127.0.0.1".to_string()),
            max_connections: 100, // Added: Provide a default value for max_connections
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
    pub port: Option<u16>,
    pub process_name: String,
    pub user: String,
    pub group: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_connections: u32,
    pub max_open_files: u64,
    pub use_raft_for_scale: bool,
    pub log_level: String,
    #[serde(default)]
    pub log_directory: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        trace!("Creating default DaemonConfig");
        DaemonConfig {
            port: None,
            process_name: "graphdb".to_string(), // Fixed: Use String directly
            user: "graphdb".to_string(),          // Fixed: Use String directly
            group: "graphdb".to_string(),         // Fixed: Use String directly
            default_port: 8049,
            cluster_range: "8000-9000".to_string(), // Fixed: Use String directly
            max_connections: 300,
            max_open_files: 100,
            use_raft_for_scale: false,
            log_level: "DEBUG".to_string(),
            log_directory: None,
        }
    }
}

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
    pub config_file: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CliConfigToml {
    pub app: AppConfig,
    pub server: ServerConfig,
    pub rest: RestConfig,
    pub daemon: DaemonConfig,
    pub storage: CliTomlStorageConfig,
    pub deployment: DeploymentConfig,
    pub log: LogConfig,
    pub paths: PathsConfig,
    pub security: SecurityConfig,
    #[serde(default)]
    pub enable_plugins: bool,
}

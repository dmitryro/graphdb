use clap::{Args, Parser, Subcommand};
use log::{debug, error, info, warn, trace};
use std::path::{Path, PathBuf};
use std::collections::{HashMap};
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer, Deserializer};
use serde::de::{self, MapAccess, Visitor};
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
use crate::cli::config_constants::*;
use crate::cli::serializers::*;
use lib::storage_engine::config::StorageConfig as EngineStorageConfig;
use lib::query_exec_engine::query_exec_engine::{QueryExecEngine};
use crate::cli::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction, 
                           ReloadAction, RestCliCommand, StatusAction, StorageAction, ShowAction, ShowArgs,
                           StartAction, StopAction, StopArgs, DaemonCliCommand, UseAction, SaveAction};
use super::daemon_management::{is_port_in_cluster_range, is_valid_cluster_range, parse_cluster_range}; // Import the correct helper function
pub use lib::storage_engine::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
pub use lib::storage_engine::config::{EngineTypeOnly, RocksdbConfig, SledConfig, TikvConfig, MySQLConfig, 
                                      RedisConfig, PostgreSQLConfig};
pub use models::errors::GraphError;
pub use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig, 
                                      SelectedStorageConfig,
                                      StorageConfigInner};
pub use crate::cli::config_defaults::*;
pub use crate::cli::serializers::*;


// Define a struct to mirror the YAML configuration
#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    pub storage: HashMap<String, Value>,
}

// A temporary struct to deserialize the raw YAML content, which is flexible.
#[derive(Debug, Deserialize, Serialize, Clone)]
//#[serde(rename_all = "kebab-case")]
#[serde(rename_all = "snake_case")]
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
    pub pd_endpoints: Option<String>, // Added for TiKV
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


// --- YAML Configuration Structs ---
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

/// The top-level `[storage]` configuration struct, which can be loaded from TOML.
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
    #[serde(default = "default_engine_specific_config", alias = "engine-specific-config")]
    pub engine_specific_config: Option<SelectedStorageConfig>,
    #[serde(default = "default_max_open_files", alias = "max-open-files")]
    pub max_open_files: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}


#[derive(Debug, Deserialize, Serialize)]
pub struct EngineSpecificConfigWrapper {
    storage: EngineSpecificConfig,
}

// Helper struct for deserializing storage_config_tikv.yaml with storage wrapper
#[derive(Debug, Serialize, Deserialize)]
pub struct TiKVConfigWrapper {
    pub storage: SpecificEngineFileConfig,
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
}

// New struct to handle the format of engine-specific files
// Update SpecificEngineFileConfig to handle both snake_case and kebab-case
// Custom deserializer for SpecificEngineFileConfig to handle both snake_case and kebab-case
// New struct to handle the format of engine-specific files
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

// --- CLI Configuration ---
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

// --- Configuration Structs ---
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

// --- TOML CLI Config ---
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    pub version: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeploymentConfig {
    pub config_root_directory: PathBuf,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LogConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PathsConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct SecurityConfig {}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestConfig {
    pub port: u16,
    pub host: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaemonConfig {
    pub port: Option<u16>,
    pub process_name: String,
    pub user: String,
    pub group: String,
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



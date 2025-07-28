// server/src/cli/config.rs

// This file handles loading CLI and Storage daemon configurations.

use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_yaml2;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use toml;

// Added necessary import for StorageEngineType conversion
use daemon_api::StorageEngineType as DaemonApiStorageEngineType;

// Constants for default values and paths
pub const DEFAULT_DAEMON_PORT: u16 = 9001;
pub const DEFAULT_REST_API_PORT: u16 = 8081;
pub const DEFAULT_STORAGE_PORT: u16 = 8049;
pub const MAX_CLUSTER_SIZE: usize = 100;
pub const DEFAULT_MAIN_PORT: u16 = 9001;
pub const DEFAULT_CLUSTER_RANGE: &str = "9001-9005";

pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;
pub const DEFAULT_STORAGE_CONFIG_PATH_RELATIVE: &str = "storage_daemon_server/storage_config.yaml";
pub const DEFAULT_REST_CONFIG_PATH_RELATIVE: &str = "rest_api/rest_api_config.yaml";
pub const DEFAULT_DAEMON_CONFIG_PATH_RELATIVE: &str = "daemon_api/daemon_config.yaml";
pub const DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE: &str = "server/main_app_config.yaml";


// --- Constants for PID file locations and executable names ---
pub const PID_FILE_DIR: &str = "/tmp/graphdb/pids";
pub const DAEMON_PID_FILE_NAME_PREFIX: &str = "graphdb-daemon-";
pub const REST_PID_FILE_NAME_PREFIX: &str = "graphdb-rest-";
pub const STORAGE_PID_FILE_NAME_PREFIX: &str = "graphdb-storage-";
pub const EXECUTABLE_NAME: &str = "graphdb-server"; // Name of the main executable


pub const DEFAULT_CONFIG_ROOT_DIRECTORY_STR: &str = if cfg!(target_family = "unix") {
    "/opt/graphdb"
} else if cfg!(target_family = "windows") {
    "C:\\Program Files\\GraphDB"
} else {
    "/opt/graphdb"
};

fn default_config_root_directory_for_yaml() -> PathBuf {
    #[cfg(target_family = "unix")]
    {
        PathBuf::from("/opt/graphdb")
    }
    #[cfg(target_family = "windows")]
    {
        PathBuf::from("C:\\Program Files\\GraphDB")
    }
    #[cfg(not(any(target_family = "unix", target_family = "windows")))]
    {
        PathBuf::from("/opt/graphdb")
    }
}



// Main Daemon Configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MainDaemonConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

impl Default for MainDaemonConfig {
    fn default() -> Self {
        MainDaemonConfig {
            data_directory: "/opt/graphdb/daemon_data".to_string(),
            log_directory: "/var/log/graphdb/daemon".to_string(),
            default_port: DEFAULT_MAIN_PORT,
            cluster_range: DEFAULT_CLUSTER_RANGE.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct MainConfigWrapper {
    main_daemon: MainDaemonConfig,
}

pub fn load_main_daemon_config(config_file_path: Option<&str>) -> Result<MainDaemonConfig> {
    let default_config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join(DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load Main Daemon config from {:?}", path_to_use);

    if path_to_use.exists() {
        let config_content = fs::read_to_string(&path_to_use)
            .context(format!("Failed to read main daemon config file: {}", path_to_use.display()))?;
        let wrapper: MainConfigWrapper = serde_yaml2::from_str(&config_content)
            .context(format!("Failed to parse main daemon config YAML: {}", path_to_use.display()))?;
        info!("Successfully loaded Main Daemon config: {:?}", wrapper.main_daemon);
        Ok(wrapper.main_daemon)
    } else {
        warn!("Config file not found at {}. Using default Main Daemon config.", path_to_use.display());
        Ok(MainDaemonConfig::default())
    }
}

// REST API Configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestApiConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

impl Default for RestApiConfig {
    fn default() -> Self {
        RestApiConfig {
            data_directory: "/opt/graphdb/storage_data".to_string(),
            log_directory: "/var/log/graphdb".to_string(),
            default_port: DEFAULT_REST_API_PORT,
            cluster_range: "8082-8086".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RestApiConfigWrapper {
    config_root_directory: String,
    rest_api: RestApiConfig,
}

pub fn load_rest_config(config_file_path: Option<&str>) -> Result<RestApiConfig> {
    let default_config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join(DEFAULT_REST_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load REST API config from {:?}", path_to_use);

    if path_to_use.exists() {
        let config_content = fs::read_to_string(&path_to_use)
            .context(format!("Failed to read REST API config file: {}", path_to_use.display()))?;
        info!("Successfully read config file: {}", path_to_use.display());
        let wrapper: RestApiConfigWrapper = serde_yaml2::from_str(&config_content)
            .context(format!("Failed to parse REST API config YAML: {}", path_to_use.display()))?;
        info!("Loaded REST API config: {:?}", wrapper.rest_api);
        Ok(wrapper.rest_api)
    } else {
        warn!("Config file not found at {}. Using default REST API config.", path_to_use.display());
        Ok(RestApiConfig::default())
    }
}

pub fn save_rest_config(config: &RestApiConfig) -> Result<()> {
    let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join(DEFAULT_REST_CONFIG_PATH_RELATIVE);

    let wrapper = RestApiConfigWrapper {
        config_root_directory: DEFAULT_CONFIG_ROOT_DIRECTORY_STR.to_string(),
        rest_api: config.clone(),
    };

    let yaml_string = serde_yaml2::to_string(&wrapper)
        .context("Failed to serialize RestApiConfig to YAML")?;

    fs::create_dir_all(config_path.parent().unwrap())
        .context(format!("Failed to create parent directories for {}", config_path.display()))?;

    fs::write(&config_path, yaml_string)
        .context(format!("Failed to write RestApiConfig to file: {}", config_path.display()))?;

    Ok(())
}

pub fn get_default_rest_port() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
}

pub fn get_rest_cluster_range() -> String {
    load_rest_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| RestApiConfig::default().cluster_range)
}

// CLI Configuration (TOML-based)
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct AppConfig {
    pub version: String,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct DeploymentConfig {
    pub config_root_directory: PathBuf,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LogConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PathsConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct SecurityConfig {}

/// Represents the top-level structure of the CLI configuration file (e.g., config.toml).
#[derive(Debug, Deserialize)]
pub struct CliConfig {
    pub app: AppConfig,
    pub server: ServerConfig,
    // Reverted the field name to `rest` to match the TOML file structure
    pub rest: RestConfig,
    pub daemon: DaemonConfig,
    pub storage: Option<CliTomlStorageConfig>,
    pub log: Option<LogConfig>,
    pub paths: Option<PathsConfig>,
    pub security: Option<SecurityConfig>,
    pub deployment: DeploymentConfig,
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

// Storage Configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_config_root_directory_for_yaml")]
    pub config_root_directory: PathBuf,
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_disk_space_gb: u64,
    pub min_disk_space_gb: u64,
    pub use_raft_for_scale: bool,
    pub max_open_files: u64,
    pub storage_engine_type: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            config_root_directory: default_config_root_directory_for_yaml(),
            data_directory: "/opt/graphdb/storage_data".to_string(),
            log_directory: "/var/log/graphdb".to_string(),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: "9050-9054".to_string(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".to_string(),
            max_open_files: 1024,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

pub fn load_storage_config(config_file_path: Option<PathBuf>) -> Result<StorageConfig> {
    let default_config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path.unwrap_or(default_config_path);

    info!("Attempting to load Storage config from {:?}", path_to_use);

    if path_to_use.exists() {
        let config_content = fs::read_to_string(&path_to_use)
            .context(format!("Failed to read storage config file: {}", path_to_use.display()))?;
        let wrapper: StorageConfigWrapper = serde_yaml2::from_str(&config_content)
            .context(format!("Failed to parse storage config YAML: {}", path_to_use.display()))?;
        info!("Loaded Storage config: {:?}", wrapper.storage);
        Ok(wrapper.storage)
    } else {
        warn!("Config file not found at {}. Using default Storage config.", path_to_use.display());
        Ok(StorageConfig::default())
    }
}

pub fn load_storage_config_str(config_file_path: Option<&str>) -> Result<StorageConfig> {
    let path = config_file_path.map(PathBuf::from);
    load_storage_config(path)
}

pub fn get_default_storage_port_from_config_or_cli_default() -> u16 {
    load_storage_config(None)
        .map(|config| config.default_port)
        .unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
}

pub fn get_storage_cluster_range() -> String {
    load_storage_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| StorageConfig::default().cluster_range)
}

/// Function to get default REST API port from config
pub fn get_default_rest_port_from_config() -> u16 {
    8082 // Default REST API port
}

/// Enum for different storage engine types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct DaemonConfig {
    pub port: Option<u16>,
    pub process_name: String,
    pub user: String,
    pub group: String,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct CliTomlStorageConfig {
    pub port: Option<u16>,
    pub default_port: Option<u16>,
    pub cluster_range: Option<String>,
    pub data_directory: Option<String>,
    pub log_directory: Option<String>,
    pub max_disk_space_gb: Option<u64>,
    pub min_disk_space_gb: Option<u64>,
    pub use_raft_for_scale: Option<bool>,
    pub storage_engine_type: Option<StorageEngineType>,
    pub max_open_files: Option<u64>,
    pub config_file: Option<String>,
}

// Reverted to `RestConfig` for the CLI TOML parsing
#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct RestConfig {
    pub port: u16,
    pub host: String,
}

// CLI Command Structures
#[derive(Debug, Args)]
pub struct DaemonCommandWrapper {
    #[clap(subcommand)]
    pub command: DaemonCliCommand,
}

#[derive(Debug, Args)]
pub struct RestCommandWrapper {
    #[clap(subcommand)]
    pub command: RestCliCommand,
}

#[derive(Debug, Args)]
pub struct StorageActionWrapper {
    #[clap(subcommand)]
    pub command: StorageAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum DaemonCliCommand {
    Start {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
    },
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Status {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Restart {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
    },
    Reload {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
    },
    List,
    ClearAll,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum RestCliCommand {
    Start {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Status {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Version,
    Health,
    Query {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'q')]
        query: String,
    },
    RegisterUser {
        #[clap(long)]
        username: String,
        #[clap(long)]
        password: String,
    },
    Authenticate {
        #[clap(long)]
        username: String,
        #[clap(long)]
        password: String,
    },
    GraphQuery {
        #[clap(long, short = 'q')]
        query_string: String,
        #[clap(long)]
        persist: Option<bool>,
    },
    StorageQuery,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum StorageAction {
    Start {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
    },
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Status {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    List,
}

#[derive(Debug, Args, PartialEq)]
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: StatusAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum StatusAction {
    All,
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: StopAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum StopAction {
    All,
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct ReloadArgs {
    #[clap(subcommand)]
    pub action: ReloadAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum ReloadAction {
    All {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct RestartArgs {
    #[clap(subcommand)]
    pub action: Option<RestartAction>,
    #[clap(long, short = 'p')]
    pub port: Option<u16>,
    #[clap(long, short = 'c', alias = "join-cluster")]
    pub cluster: Option<String>,
    #[clap(long)]
    pub config_file: Option<PathBuf>,
    #[clap(long)]
    pub listen_port: Option<u16>,
    #[clap(long)]
    pub storage_port: Option<u16>,
    #[clap(long, value_hint = clap::ValueHint::FilePath)]
    pub storage_config_file: Option<PathBuf>,
    #[clap(long, value_hint = clap::ValueHint::DirPath)]
    pub data_directory: Option<String>,
    #[clap(long, value_hint = clap::ValueHint::DirPath)]
    pub log_directory: Option<String>,
    #[clap(long)]
    pub max_disk_space_gb: Option<u64>,
    #[clap(long)]
    pub min_disk_space_gb: Option<u64>,
    #[clap(long)]
    pub use_raft_for_scale: Option<bool>,
    #[clap(long)]
    pub storage_engine_type: Option<String>,
    #[clap(long)]
    pub daemon: Option<bool>,
    #[clap(long)]
    pub rest: Option<bool>,
    #[clap(long)]
    pub storage: Option<bool>,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum RestartAction {
    All {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        config_file: Option<PathBuf>,
        #[clap(long)]
        listen_port: Option<u16>,
        #[clap(long)]
        storage_port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        storage_config_file: Option<PathBuf>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct ClearDataArgs {
    #[clap(subcommand)]
    pub action: ClearDataAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum ClearDataAction {
    All,
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
}

#[derive(Debug, Args, PartialEq)]
pub struct RegisterUserArgs {
    #[clap(long)]
    pub username: String,
    #[clap(long)]
    pub password: String,
}

#[derive(Debug, Args, PartialEq)]
pub struct AuthArgs {
    #[clap(long)]
    pub username: String,
    #[clap(long)]
    pub password: String,
}

// Daemon (daemon_api) Configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaemonYamlConfig {
    pub config_root_directory: PathBuf,
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_connections: u32,
    pub max_open_files: u64,
    pub use_raft_for_scale: bool,
    pub log_level: String,
}

impl Default for DaemonYamlConfig {
    fn default() -> Self {
        DaemonYamlConfig {
            config_root_directory: PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            data_directory: "/opt/graphdb/daemon_data".to_string(),
            log_directory: "/var/log/graphdb/daemon".to_string(),
            default_port: DEFAULT_DAEMON_PORT,
            cluster_range: "9001-9004".to_string(),
            max_connections: 1000,
            max_open_files: 1024,
            use_raft_for_scale: true,
            log_level: "info".to_string(),
        }
    }
}

pub fn load_daemon_config(config_file_path: Option<&str>) -> Result<DaemonYamlConfig> {
    let default_config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join(DEFAULT_DAEMON_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load Daemon config from {:?}", path_to_use);

    if path_to_use.exists() {
        let config_content = fs::read_to_string(&path_to_use)
            .context(format!("Failed to read daemon config file: {}", path_to_use.display()))?;
        let config: DaemonYamlConfig = serde_yaml2::from_str(&config_content)
            .context(format!("Failed to parse daemon config YAML: {}", path_to_use.display()))?;
        info!("Loaded Daemon config: {:?}", config);
        Ok(config)
    } else {
        warn!("Config file not found at {}. Using default Daemon config.", path_to_use.display());
        Ok(DaemonYamlConfig::default())
    }
}

pub fn save_daemon_config(config: &DaemonYamlConfig) -> Result<()> {
    let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join(DEFAULT_DAEMON_CONFIG_PATH_RELATIVE);

    let yaml_string = serde_yaml2::to_string(config)
        .context("Failed to serialize DaemonYamlConfig to YAML")?;

    fs::create_dir_all(config_path.parent().unwrap())
        .context(format!("Failed to create parent directories for {}", config_path.display()))?;

    fs::write(&config_path, yaml_string)
        .context(format!("Failed to write DaemonYamlConfig to file: {}", config_path.display()))?;

    Ok(())
}

pub fn get_default_daemon_port() -> u16 {
    load_daemon_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_DAEMON_PORT)
}

pub fn get_daemon_cluster_range() -> String {
    load_daemon_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| DaemonYamlConfig::default().cluster_range)
}


pub fn get_cli_storage_config(config_file_path: Option<PathBuf>) -> CliTomlStorageConfig {
    let _ = config_file_path;

    load_cli_config()
        .ok()
        .and_then(|cli_config| cli_config.storage)
        .unwrap_or_default()
}

/// Helper function to get the default storage port from CLI config, with fallbacks.
pub fn get_cli_storage_default_port() -> u16 {
    load_cli_config()
        .ok()
        .and_then(|cli_config| {
            cli_config.storage.and_then(|storage_config| {
                storage_config.port
                    .or(storage_config.default_port)
            })
        })
        .unwrap_or(DEFAULT_STORAGE_PORT)
}

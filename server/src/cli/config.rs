// server/src/cli/config.rs

// This file handles loading CLI and Storage daemon configurations,
// and defines the command-line arguments and subcommands for the GraphDB CLI using the `clap` crate.

use clap::{Args, Subcommand};
use std::path::{PathBuf, Path}; // Import Path
use serde::{Deserialize, Serialize};
use std::fs;
use anyhow::{Context, Result};
use std::str::FromStr;
use serde_yaml2;
use daemon_api::StorageEngineType as DaemonApiStorageEngineType;
use toml;

// --- Configuration Structs for loading CLI defaults from config.toml ---

// Corrected: Define the constant using `if cfg!(...)` for conditional compilation
pub const DEFAULT_CONFIG_ROOT_DIRECTORY_STR: &str = if cfg!(target_family = "unix") {
    "/opt/graphdb"
} else if cfg!(target_family = "windows") {
    "C:\\Program Files\\GraphDB"
} else {
    // Fallback for other target families (e.g., macOS, which is also "unix")
    "/opt/graphdb"
};


// New: AppConfig struct to match the [app] section in config.toml
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct AppConfig {
    pub version: String,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

// New: RestConfig struct to match the [rest] section in config.toml
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct RestConfig {
    pub port: u16,
    pub host: String,
}

// New: DaemonConfig struct to match the [daemon] section in config.toml
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DaemonConfig {
    pub process_name: String,
    pub user: String,
    pub group: String,
}

// New: CliTomlStorageConfig for parsing the [storage] section in config.toml
// This is separate from the more comprehensive StorageConfig used for storage_config.yaml.
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct CliTomlStorageConfig {
    pub default_port: Option<u16>, // Changed to Option<u16> to match handlers.rs usage
    pub cluster_range: Option<String>, // Changed to Option<String>
    pub data_directory: Option<String>,
    pub log_directory: Option<String>,
    pub max_disk_space_gb: Option<u64>,
    pub min_disk_space_gb: Option<u64>,
    pub use_raft_for_scale: Option<bool>,
    pub storage_engine_type: Option<StorageEngineType>, // Use the enum directly
    pub max_open_files: Option<u64>,
    pub config_file: Option<String>, // Added config_file field
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct DeploymentConfig {
    // This is the absolute path to the root of the deployed GraphDB application.
    // Configuration files (like storage_config.yaml) and executables (like storage_daemon_server)
    // are expected to be found relative to this directory.
    pub config_root_directory: PathBuf,
}

// Updated: CliConfig now includes all top-level sections from config.toml
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct CliConfig {
    pub app: AppConfig,
    pub server: ServerConfig,
    pub rest: RestConfig,
    pub daemon: DaemonConfig,
    pub storage: Option<CliTomlStorageConfig>, // Make storage optional
    pub log: Option<LogConfig>, // Added LogConfig
    pub paths: Option<PathsConfig>, // Added PathsConfig
    pub security: Option<SecurityConfig>, // Added SecurityConfig
    pub deployment: DeploymentConfig, // NEW FIELD for deployment paths
}

// Dummy structs to satisfy CliConfig for now
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct LogConfig {}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct PathsConfig {}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct SecurityConfig {}


// Corrected: `default_config_root_directory` is now a const, so remove the function.
// This `StorageConfig` is for the YAML file (`storage_daemon_server/storage_config.yaml`).
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_config_root_directory_for_yaml")] // Use a distinct default function for YAML
    pub config_root_directory: PathBuf, // NEW FIELD for the daemon's own config root
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

// Default function for config_root_directory if it's not specified in the YAML
// This is a function, not a const, to match the #[serde(default = "...")] attribute.
fn default_config_root_directory_for_yaml() -> PathBuf { // Changed return type to PathBuf
    // Provide a reasonable default for typical deployments
    #[cfg(target_family = "unix")]
    { PathBuf::from("/opt/graphdb") }
    #[cfg(target_family = "windows")]
    { PathBuf::from("C:\\Program Files\\GraphDB") }
    #[cfg(not(any(target_family = "unix", target_family = "windows")))]
    { PathBuf::from("/opt/graphdb") }
}


// Default implementation for StorageConfig (unchanged, but now includes config_root_directory)
impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            config_root_directory: default_config_root_directory_for_yaml(), // Initialize this field
            default_port: 8090,
            data_directory: "/tmp/graphdb_data".to_string(),
            log_directory: "/tmp/graphdb_logs".to_string(),
            cluster_range: "8090-8100".to_string(),
            max_disk_space_gb: 100,
            min_disk_space_gb: 10,
            use_raft_for_scale: false,
            storage_engine_type: "sled".to_string(),
            max_open_files: 1024,
        }
    }
}

// CLI's assumed default storage port. This is used for consistency in stop/status commands.
pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;
pub const DEFAULT_STORAGE_CONFIG_PATH: &str = "storage_daemon_server/storage_config.yaml";

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

/// Represents a wrapper for the storage config in the YAML file.
#[derive(Debug, Deserialize)]
pub struct ConfigWrapper {
    pub storage: StorageConfig, // This still uses the original StorageConfig
}

/// Loads the Storage daemon configuration from `storage_daemon_server/storage_config.yaml`.
pub fn load_storage_config(config_file_path: Option<PathBuf>) -> Result<StorageConfig, anyhow::Error> {
    let default_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Failed to get parent directory of server crate"))?
        .join("storage_daemon_server")
        .join("storage_config.yaml");

    let path_to_use = config_file_path.unwrap_or(default_config_path);

    let config_content = fs::read_to_string(&path_to_use)
        .map_err(|e| anyhow::anyhow!("Failed to read storage config file {}: {}", path_to_use.display(), e))?;

    let wrapper: ConfigWrapper = serde_yaml2::from_str(&config_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse storage config file {}: {}", path_to_use.display(), e))?;

    Ok(wrapper.storage)
}

/// Function to get default REST API port from config
pub fn get_default_rest_port_from_config() -> u16 {
    8082
}

/// Function to get default storage port from config or CLI default
pub fn get_default_storage_port_from_config_or_cli_default() -> u16 {
    load_storage_config(None)
        .map(|config| config.default_port)
        .unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
}

/// Function to get CLI storage config, potentially from a file
pub fn get_cli_storage_config(config_file_path: Option<PathBuf>) -> CliTomlStorageConfig {
    load_cli_config()
        .ok()
        .and_then(|cli_config| cli_config.storage)
        .unwrap_or_default()
}


/// Enum for different storage engine types.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum StorageEngineType {
    Sled,
    RocksDB,
    InMemory,
}

impl FromStr for StorageEngineType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" => Ok(StorageEngineType::RocksDB),
            "inmemory" => Ok(StorageEngineType::InMemory),
            _ => Err(anyhow::anyhow!("Unknown storage engine type: {}", s)),
        }
    }
}

impl ToString for StorageEngineType {
    fn to_string(&self) -> String {
        match self {
            StorageEngineType::Sled => "sled".to_string(),
            StorageEngineType::RocksDB => "rocksdb".to_string(),
            StorageEngineType::InMemory => "inmemory".to_string(),
        }
    }
}

impl From<StorageEngineType> for DaemonApiStorageEngineType {
    fn from(cli_type: StorageEngineType) -> Self {
        match cli_type {
            StorageEngineType::Sled => DaemonApiStorageEngineType::Sled,
            StorageEngineType::RocksDB => DaemonApiStorageEngineType::RocksDB,
            StorageEngineType::InMemory => DaemonApiStorageEngineType::InMemory,
        }
    }
}

// --- CLI Argument Structs (from user's current config.rs) ---

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


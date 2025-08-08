// server/src/cli/config.rs
// Existing changelog entries from cli.rs and interactive.rs are not repeated here as they pertain to different files.
// ADDED: 2025-08-08 - Added `enable_plugins` field to `CliConfigToml`. Implemented `save` method for `CliConfigToml` to serialize to TOML file. Replaced `std::fs` with `fs` and ensured consistent use of `serde_yaml2` to align with cli.rs.
// UPDATED: 2025-08-08 - Changed `load_cli_config` to use `/opt/graphdb/config.toml` instead of `config.toml` in `CARGO_MANIFEST_DIR` to align with `save` method and system config paths.
// UPDATED: 2025-08-08 - Updated `load_cli_config` to load from `server/src/cli/config.toml` using `CARGO_MANIFEST_DIR` and save to `/opt/graphdb/config.toml` as per user clarification.

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize, Serializer, Deserializer};
use serde_json::Value;
use serde_yaml2;
use std::env;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use dirs;
use toml;
use serde_yaml2 as serde_yaml;
// Import types from commands.rs
use crate::cli::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction, 
                           ReloadAction, RestCliCommand, StatusAction, StorageAction, 
                           StartAction, StopAction, 
                           StopArgs, DaemonCliCommand};
pub use lib::storage_engine::config::StorageEngineType;

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

// --- Custom Option<PathBuf> Serialization Module ---
mod option_path_buf_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(opt_path: &Option<PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match opt_path {
            Some(path) => path_buf_serde::serialize(path, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_s = Option::<String>::deserialize(deserializer)?;
        Ok(opt_s.map(PathBuf::from))
    }
}

// --- Custom StorageEngineType Serialization Module ---
mod storage_engine_type_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(engine_type: &StorageEngineType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&engine_type.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StorageEngineType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        StorageEngineType::from_str(&s).map_err(serde::de::Error::custom)
    }

    #[derive(Deserialize)]
    struct StorageEngineTypeWrapper {
        name: String,
    }

    pub mod mapping {
        use super::*;
        pub fn deserialize<'de, D>(deserializer: D) -> Result<StorageEngineType, D::Error>
        where
            D: Deserializer<'de>,
        {
            let wrapper = StorageEngineTypeWrapper::deserialize(deserializer)?;
            StorageEngineType::from_str(&wrapper.name).map_err(serde::de::Error::custom)
        }
    }
}

// --- Constants ---
pub const DAEMON_REGISTRY_DB_PATH: &str = "./daemon_registry_db";
pub const DEFAULT_DAEMON_PORT: u16 = 9001;
pub const DEFAULT_REST_API_PORT: u16 = 8081;
pub const DEFAULT_STORAGE_PORT: u16 = 8049;
pub const MAX_CLUSTER_SIZE: usize = 100;
pub const DEFAULT_MAIN_PORT: u16 = 9001;
pub const DEFAULT_CLUSTER_RANGE: &str = "9001-9005";
pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;
pub const DEFAULT_STORAGE_CONFIG_PATH: &str = "/opt/graphdb/storage_config.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/storage_config.yaml";
pub const DEFAULT_REST_CONFIG_PATH_RELATIVE: &str = "/opt/graphdb/rest_api_config.yaml";
pub const DEFAULT_DAEMON_CONFIG_PATH_RELATIVE: &str = "/opt/graphdb/daemon_config.yaml";
pub const DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE: &str = "/opt/graphdb/main_app_config.yaml";
pub const PID_FILE_DIR: &str = "/tmp/graphdb/pids";
pub const DAEMON_PID_FILE_NAME_PREFIX: &str = "graphdb-daemon-";
pub const REST_PID_FILE_NAME_PREFIX: &str = "graphdb-rest-";
pub const STORAGE_PID_FILE_NAME_PREFIX: &str = "graphdb-storage-";
pub const EXECUTABLE_NAME: &str = "graphdb-server";
pub const DEFAULT_CONFIG_ROOT_DIRECTORY_STR: &str = "/opt/graphdb";
pub const DEFAULT_HTTP_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_HTTP_RETRIES: u32 = 3;
pub const DEFAULT_GRPC_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_GRPC_RETRIES: u32 = 5;

pub fn default_config_root_directory() -> PathBuf {
    PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
}

fn default_config_root_directory_option() -> Option<PathBuf> {
    Some(default_config_root_directory())
}

// --- YAML Configuration Structs ---
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
            data_directory: format!("{}/daemon_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
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
            data_directory: format!("{}/rest_api_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            log_directory: "/var/log/graphdb".to_string(),
            default_port: DEFAULT_REST_API_PORT,
            cluster_range: format!("{}", DEFAULT_REST_API_PORT),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RestApiConfigWrapper {
    config_root_directory: String,
    rest_api: RestApiConfig,
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

impl Default for DaemonYamlConfig {
    fn default() -> Self {
        DaemonYamlConfig {
            config_root_directory: default_config_root_directory().to_string_lossy().into_owned(),
            data_directory: format!("{}/daemon_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            log_directory: "/var/log/graphdb/daemon".to_string(),
            default_port: DEFAULT_DAEMON_PORT,
            cluster_range: format!("{}", DEFAULT_DAEMON_PORT),
            max_connections: 1000,
            max_open_files: 1024,
            use_raft_for_scale: true,
            log_level: "info".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    #[serde(default = "default_config_root_directory")]
    #[serde(with = "path_buf_serde")]
    pub config_root_directory: PathBuf,
    #[serde(with = "path_buf_serde")]
    pub data_directory: PathBuf,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_disk_space_gb: u64,
    pub min_disk_space_gb: u64,
    pub use_raft_for_scale: bool,
    #[serde(with = "storage_engine_type_serde")]
    pub storage_engine_type: StorageEngineType,
    pub engine_specific_config: Option<String>,
    pub max_open_files: u64,
}

impl StorageConfig {
    pub fn parse_cluster_range(&self) -> Result<Vec<u16>> {
        if self.cluster_range.contains('-') {
            let parts: Vec<&str> = self.cluster_range.split('-').collect();
            if parts.len() != 2 {
                return Err(anyhow!("Invalid cluster_range format: {}", self.cluster_range));
            }
            let start: u16 = parts[0].parse().map_err(|e| anyhow!("Invalid start port: {}", e))?;
            let end: u16 = parts[1].parse().map_err(|e| anyhow!("Invalid end port: {}", e))?;
            Ok((start..=end).collect())
        } else {
            let port: u16 = self.cluster_range.parse().map_err(|e| anyhow!("Invalid port: {}", e))?;
            Ok(vec![port])
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            config_root_directory: default_config_root_directory(),
            data_directory: PathBuf::from(format!("{}/storage_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            log_directory: "/var/log/graphdb".to_string(),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: DEFAULT_STORAGE_PORT.to_string(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: StorageEngineType::Sled,
            engine_specific_config: None,
            max_open_files: 1024,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

// Helper function to convert StorageEngineType to String
pub fn daemon_api_storage_engine_type_to_string(engine_type: &StorageEngineType) -> String {
    engine_type.to_string()
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
}

// --- CLI Config Loading ---
impl CliConfig {
    pub fn load(interactive_command: Option<CommandType>) -> Result<Self> {
        let mut args = if let Some(cmd) = interactive_command {
            // Construct CliConfig from CommandType in interactive mode
            let mut config = CliConfig {
                command: Commands::Exit, // Placeholder, will be overridden
                http_timeout_seconds: DEFAULT_HTTP_TIMEOUT_SECONDS,
                http_retries: DEFAULT_HTTP_RETRIES,
                grpc_timeout_seconds: DEFAULT_GRPC_TIMEOUT_SECONDS,
                grpc_retries: DEFAULT_GRPC_RETRIES,
                daemon_port: None,
                daemon_cluster: None,
                rest_port: None,
                rest_cluster: None,
                storage_port: None,
                storage_cluster: None,
                storage_config_path: None,
                rest_api_config_path: None,
                main_daemon_config_path: None,
                config_root_directory: None,
            };
            match cmd {
                CommandType::StartStorage { port, config_file, cluster, storage_port, storage_cluster } => {
                    config.command = Commands::Storage(StorageAction::Start {
                        port: storage_port.or(port),
                        config_file: config_file.clone(),
                        cluster: storage_cluster.clone().or(cluster.clone()),
                        storage_port,
                        storage_cluster: storage_cluster.clone(),
                    });
                    config.storage_port = storage_port.or(port);
                    config.storage_cluster = storage_cluster.or(cluster);
                    config.storage_config_path = config_file;
                }
                CommandType::Exit => {
                    config.command = Commands::Exit;
                }
                _ => {
                    return Err(anyhow!("Unsupported interactive command: {:?}", cmd));
                }
            }
            config
        } else {
            match <Self as clap::Parser>::try_parse() {
                Ok(args) => args,
                Err(e) => {
                    error!("Failed to parse CLI arguments: {:?}", e);
                    return Err(anyhow!("CLI parsing error: {}", e));
                }
            }
        };
        let config_root = args
            .config_root_directory
            .clone()
            .unwrap_or_else(|| default_config_root_directory());

        debug!("Resolved config root directory: {:?}", config_root);
        println!("Loading Config ==> STEP 2");
        let global_config_path = config_root.join("graphdb_config.yaml");
        let mut global_yaml_config: Option<GlobalYamlConfig> = None;
        if global_config_path.exists() {
            debug!("Attempting to load global config from: {:?}", global_config_path);
            match fs::read_to_string(&global_config_path) {
                Ok(content) => match serde_yaml::from_str(&content) {
                    Ok(config) => {
                        global_yaml_config = Some(config);
                        debug!("Successfully loaded global config.");
                    }
                    Err(e) => warn!("Failed to parse global config YAML {:?}: {}", global_config_path, e),
                },
                Err(e) => warn!("Failed to read global config file {:?}: {}", global_config_path, e),
            }
        }
        let storage_yaml_config = load_storage_config_from_yaml(args.storage_config_path.clone())
            .map_err(|e| anyhow!("Failed to load storage config: {}", e))
            .unwrap_or_else(|e| {
                warn!("Failed to load storage config, using default: {}", e);
                StorageConfig::default()
            });

        let rest_api_yaml_config = Self::load_specific_yaml(
            args.rest_api_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.rest_api.clone()),
            &PathBuf::from(DEFAULT_REST_CONFIG_PATH_RELATIVE),
            "rest_api_config.yaml",
        );

        let main_daemon_yaml_config = Self::load_specific_yaml(
            args.main_daemon_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.main_daemon.clone()),
            &PathBuf::from(DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE),
            "main_app_config.yaml",
        );

        // Store storage config path for use in getters
        args.storage_config_path = Some(
            args.storage_config_path
                .unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE))
        );

        // Resolve Daemon Port
        if args.daemon_port.is_none() {
            args.daemon_port = match &args.command {
                Commands::Start { action: Some(StartAction::Daemon { port, daemon_port, .. }), .. } => daemon_port.or(*port),
                Commands::Start { action: Some(StartAction::All { daemon_port, port, .. }), .. } => daemon_port.or(*port),
                Commands::Daemon(DaemonCliCommand::Start { port, daemon_port, .. }) => daemon_port.or(*port),
                Commands::Stop(StopArgs { action: Some(StopAction::Daemon { port }), .. }) => *port,
                Commands::Daemon(DaemonCliCommand::Stop { port, .. }) => *port,
                Commands::Status(StatusArgs { action: Some(StatusAction::Daemon { port, .. }), .. }) => *port,
                Commands::Daemon(DaemonCliCommand::Status { port, .. }) => *port,
                Commands::Reload(ReloadArgs { action: Some(ReloadAction::Daemon { port }), .. }) => *port,
                Commands::Restart(RestartArgs { action: RestartAction::Daemon { port, daemon_port, .. } }) => daemon_port.or(*port),
                Commands::Restart(RestartArgs { action: RestartAction::All { port, daemon_port, .. } }) => daemon_port.or(*port),
                _ => main_daemon_yaml_config.as_ref().map(|c| c.default_port),
            };
        }

        // Resolve Daemon Cluster
        if args.daemon_cluster.is_none() {
            args.daemon_cluster = match &args.command {
                Commands::Start { action: Some(StartAction::Daemon { cluster, daemon_cluster, .. }), .. } => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Start { action: Some(StartAction::All { daemon_cluster, cluster, .. }), .. } => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Daemon(DaemonCliCommand::Start { cluster, daemon_cluster, .. }) => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Status(StatusArgs { action: Some(StatusAction::Daemon { cluster, .. }), .. }) => cluster.clone(),
                Commands::Daemon(DaemonCliCommand::Status { cluster, .. }) => cluster.clone(),
                Commands::Restart(RestartArgs { action: RestartAction::Daemon { cluster, daemon_cluster, .. } }) => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Restart(RestartArgs { action: RestartAction::All { cluster, daemon_cluster, .. } }) => daemon_cluster.clone().or_else(|| cluster.clone()),
                _ => main_daemon_yaml_config.as_ref().map(|c| c.cluster_range.clone()),
            };
        }

        // Resolve REST Port
        if args.rest_port.is_none() {
            args.rest_port = match &args.command {
                Commands::Start { action: Some(StartAction::Rest { port, rest_port, .. }), .. } => rest_port.or(*port),
                Commands::Start { action: Some(StartAction::All { rest_port, listen_port, .. }), .. } => rest_port.or(*listen_port),
                Commands::Rest(RestCliCommand::Start { port, rest_port, .. }) => rest_port.or(*port),
                Commands::Stop(StopArgs { action: Some(StopAction::Rest { port, .. }), .. }) => *port,
                Commands::Rest(RestCliCommand::Stop { port, .. }) => *port,
                Commands::Status(StatusArgs { action: Some(StatusAction::Rest { port, .. }), .. }) => *port,
                Commands::Rest(RestCliCommand::Status { port, .. }) => *port,
                Commands::Restart(RestartArgs { action: RestartAction::Rest { port, rest_port, .. } }) => rest_port.or(*port),
                Commands::Restart(RestartArgs { action: RestartAction::All { listen_port, rest_port, .. } }) => rest_port.or(*listen_port),
                _ => rest_api_yaml_config.as_ref().map(|c| c.default_port),
            };
        }

        // Resolve REST Cluster
        if args.rest_cluster.is_none() {
            args.rest_cluster = match &args.command {
                Commands::Start { action: Some(StartAction::Rest { cluster, rest_cluster, .. }), .. } => rest_cluster.clone().or_else(|| cluster.clone()),
                Commands::Start { action: Some(StartAction::All { rest_cluster, .. }), .. } => rest_cluster.clone(),
                Commands::Rest(RestCliCommand::Start { cluster, rest_cluster, .. }) => rest_cluster.clone().or_else(|| cluster.clone()),
                Commands::Status(StatusArgs { action: Some(StatusAction::Rest { cluster, .. }), .. }) => cluster.clone(),
                Commands::Rest(RestCliCommand::Status { cluster, .. }) => cluster.clone(),
                Commands::Restart(RestartArgs { action: RestartAction::Rest { cluster, rest_cluster, .. } }) => rest_cluster.clone().or_else(|| cluster.clone()),
                Commands::Restart(RestartArgs { action: RestartAction::All { rest_cluster, .. } }) => rest_cluster.clone(),
                _ => Some(args.rest_port.unwrap_or(DEFAULT_REST_API_PORT).to_string()),
            };
        }

        // Resolve Storage Port
        if args.storage_port.is_none() {
            args.storage_port = match &args.command {
                Commands::Start { action: Some(StartAction::Storage { port, storage_port, .. }), .. } => storage_port.or(*port),
                Commands::Start { action: Some(StartAction::All { storage_port, .. }), .. } => *storage_port,
                Commands::Storage(StorageAction::Start { port, storage_port, .. }) => storage_port.or(*port),
                Commands::Stop(StopArgs { action: Some(StopAction::Storage { port }), .. }) => *port,
                Commands::Storage(StorageAction::Stop { port, .. }) => *port,
                Commands::Status(StatusArgs { action: Some(StatusAction::Storage { port, .. }), .. }) => *port,
                Commands::Storage(StorageAction::Status { port, .. }) => *port,
                Commands::Restart(RestartArgs { action: RestartAction::Storage { port, storage_port, .. } }) => storage_port.or(*port),
                Commands::Restart(RestartArgs { action: RestartAction::All { storage_port, .. } }) => *storage_port,
                _ => Some(storage_yaml_config.default_port),
            };
        }

        // Resolve Storage Cluster
        if args.storage_cluster.is_none() {
            args.storage_cluster = match &args.command {
                Commands::Start { action: Some(StartAction::Storage { cluster, storage_cluster, .. }), .. } => storage_cluster.clone().or_else(|| cluster.clone()),
                Commands::Start { action: Some(StartAction::All { storage_cluster, .. }), .. } => storage_cluster.clone(),
                Commands::Storage(StorageAction::Start { cluster, storage_cluster, .. }) => storage_cluster.clone().or_else(|| cluster.clone()),
                Commands::Status(StatusArgs { action: Some(StatusAction::Storage { cluster, .. }), .. }) => cluster.clone(),
                Commands::Storage(StorageAction::Status { cluster, .. }) => cluster.clone(),
                Commands::Restart(RestartArgs { action: RestartAction::Storage { cluster, storage_cluster, .. } }) => storage_cluster.clone().or_else(|| cluster.clone()),
                Commands::Restart(RestartArgs { action: RestartAction::All { storage_cluster, .. } }) => storage_cluster.clone(),
                _ => Some(storage_yaml_config.cluster_range.clone()),
            };
        }

        println!("SUCCESS: CLI config loaded.");
        info!("SUCCESS: Determined config path: {:?}", args.storage_config_path);
        info!("SUCCESS: Storage config loaded: {:?}", storage_yaml_config);
        Ok(args)
    }

    pub fn load_specific_yaml<T>(
        cli_path: Option<&PathBuf>,
        global_embed: Option<T>,
        default_path: &Path,
        config_name_for_log: &str,
    ) -> Option<T>
    where
        T: for<'de> Deserialize<'de> + Clone + std::fmt::Debug,
    {
        if let Some(path) = cli_path {
            debug!("Attempting to load {} from CLI specified path: {:?}", config_name_for_log, path);
            match fs::canonicalize(path) {
                Ok(canonical_path) => match fs::read_to_string(&canonical_path) {
                    Ok(content) => match serde_yaml::from_str(&content) {
                        Ok(config) => {
                            info!("Successfully loaded {} from CLI path: {:?}", config_name_for_log, canonical_path);
                            return Some(config);
                        }
                        Err(e) => {
                            error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, canonical_path, e);
                            warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, canonical_path, e);
                        }
                    },
                    Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, canonical_path, e),
                },
                Err(e) => warn!("Failed to canonicalize {} path {:?}: {}", config_name_for_log, path, e),
            }
        }

        if let Some(embed) = global_embed {
            debug!("Using {} config embedded in global config.", config_name_for_log);
            return Some(embed);
        }

        if default_path.exists() {
            debug!("Attempting to load {} from default path: {:?}", config_name_for_log, default_path);
            match fs::canonicalize(default_path) {
                Ok(canonical_path) => match fs::read_to_string(&canonical_path) {
                    Ok(content) => match serde_yaml::from_str(&content) {
                        Ok(config) => {
                            info!("Successfully loaded {} from default path: {:?}", config_name_for_log, canonical_path);
                            return Some(config);
                        }
                        Err(e) => {
                            error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, canonical_path, e);
                            warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, canonical_path, e);
                        }
                    },
                    Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, canonical_path, e),
                },
                Err(e) => warn!("Failed to canonicalize {} path {:?}: {}", config_name_for_log, default_path, e),
            }
        }

        debug!("No {} config found via CLI, global, or default paths.", config_name_for_log);
        None
    }

    pub fn get_daemon_address(&self) -> Result<SocketAddr> {
        let ip_str = "127.0.0.1";
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_daemon_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_daemon_port(&self) -> Result<u16> {
        self.daemon_port
            .with_context(|| "Daemon port not specified and no default found.")
    }

    pub fn get_daemon_cluster_range(&self) -> Result<String> {
        self.daemon_cluster
            .clone()
            .with_context(|| "Daemon cluster range not specified and no default found.")
    }

    pub fn get_rest_api_port(&self) -> Result<u16> {
        self.rest_port
            .with_context(|| "REST API port not specified and no default found.")
    }

    pub fn get_rest_api_cluster_range(&self) -> Result<String> {
        self.rest_cluster
            .clone()
            .with_context(|| "REST API cluster range not specified and no default found.")
    }

    pub fn get_storage_daemon_address(&self) -> Result<SocketAddr> {
        let ip_str = "127.0.0.1";
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_storage_daemon_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_storage_daemon_port(&self) -> Result<u16> {
        self.storage_port
            .with_context(|| "Storage Daemon port not specified and no default found.")
    }

    pub fn get_storage_daemon_cluster_range(&self) -> Result<String> {
        self.storage_cluster
            .clone()
            .with_context(|| "Storage Daemon cluster range not specified and no default found.")
    }

    pub fn get_rest_api_address(&self) -> Result<SocketAddr> {
        let ip_str = "127.0.0.1";
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_rest_api_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_data_directory(&self) -> Result<String> {
        match &self.command {
            Commands::Start { action: Some(start_action), .. } => match start_action {
                StartAction::Daemon { .. } => Ok(MainDaemonConfig::default().data_directory),
                StartAction::Rest { .. } => Ok(RestApiConfig::default().data_directory),
                StartAction::Storage { .. } => Ok(StorageConfig::default().data_directory.to_string_lossy().into_owned()),
                _ => Err(anyhow!("Not in a start context to retrieve data directory.")),
            },
            Commands::Rest(RestCliCommand::Start { .. }) => {
                Ok(RestApiConfig::default().data_directory)
            }
            Commands::Storage(StorageAction::Start { .. }) => {
                Ok(StorageConfig::default().data_directory.to_string_lossy().into_owned())
            }
            Commands::Daemon(DaemonCliCommand::Start { .. }) => {
                Ok(MainDaemonConfig::default().data_directory)
            }
            _ => Err(anyhow!("Not in a start/cli context to retrieve data directory.")),
        }
    }

    pub fn get_log_directory(&self) -> Result<String> {
        match &self.command {
            Commands::Start { action: Some(start_action), .. } => match start_action {
                StartAction::Daemon { .. } => Ok(MainDaemonConfig::default().log_directory),
                StartAction::Rest { .. } => Ok(RestApiConfig::default().log_directory),
                StartAction::Storage { .. } => Ok(StorageConfig::default().log_directory),
                _ => Err(anyhow!("Not in a start context to retrieve log directory.")),
            },
            Commands::Rest(RestCliCommand::Start { .. }) => {
                Ok(RestApiConfig::default().log_directory)
            }
            Commands::Storage(StorageAction::Start { .. }) => {
                Ok(StorageConfig::default().log_directory)
            }
            Commands::Daemon(DaemonCliCommand::Start { .. }) => {
                Ok(MainDaemonConfig::default().log_directory)
            }
            _ => Err(anyhow!("Not in a start/cli context to retrieve log directory.")),
        }
    }

    pub fn get_storage_max_disk_space_gb(&self) -> Result<u64> {
        Ok(StorageConfig::default().max_disk_space_gb)
    }

    pub fn get_storage_min_disk_space_gb(&self) -> Result<u64> {
        Ok(StorageConfig::default().min_disk_space_gb)
    }

    pub fn get_storage_use_raft_for_scale(&self) -> Result<bool> {
        Ok(StorageConfig::default().use_raft_for_scale)
    }

    pub fn get_storage_engine_type(&self) -> Result<String> {
        Ok(daemon_api_storage_engine_type_to_string(&StorageConfig::default().storage_engine_type))
    }

    pub fn get_http_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.http_timeout_seconds)
    }

    pub fn get_http_retries(&self) -> u32 {
        self.http_retries
    }

    pub fn get_grpc_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.grpc_timeout_seconds)
    }

    pub fn get_grpc_retries(&self) -> u32 {
        self.grpc_retries
    }

    pub fn get_cli_internal_storage_engine(&self) -> Result<StorageEngineType> {
        Ok(StorageConfig::default().storage_engine_type)
    }

    pub fn get_cli_daemon_storage_engine(&self) -> Result<StorageEngineType> {
        Ok(StorageConfig::default().storage_engine_type)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct GlobalYamlConfig {
    #[serde(default = "default_config_root_directory_option")]
    #[serde(with = "option_path_buf_serde")]
    config_root_directory: Option<PathBuf>,
    storage: Option<StorageConfig>,
    rest_api: Option<RestApiConfig>,
    main_daemon: Option<MainDaemonConfig>,
}

// --- Config Loading Functions ---
pub fn load_main_daemon_config(config_file_path: Option<&str>) -> Result<MainDaemonConfig> {
    let default_config_path = PathBuf::from(DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load Main Daemon config from {:?}", path_to_use);

    if path_to_use.exists() {
        match fs::canonicalize(&path_to_use) {
            Ok(canonical_path) => {
                let config_content = fs::read_to_string(&canonical_path)
                    .context(format!("Failed to read main daemon config file: {}", canonical_path.display()))?;
                debug!("Main Daemon config content: {}", config_content);
                let wrapper: MainConfigWrapper = serde_yaml2::from_str(&config_content)
                    .map_err(|e| {
                        error!("YAML parsing error for Main Daemon at {:?}: {:?}", canonical_path, e);
                        if let Ok(partial) = serde_yaml2::from_str::<Value>(&config_content) {
                            error!("Partial YAML parse: {:?}", partial);
                        }
                        anyhow!("Failed to parse main daemon config YAML: {}", canonical_path.display())
                    })?;
                info!("Successfully loaded Main Daemon config: {:?}", wrapper.main_daemon);
                Ok(wrapper.main_daemon)
            }
            Err(e) => {
                warn!("Failed to canonicalize Main Daemon config path {:?}", path_to_use);
                warn!("Config file not found at {}. Using default Main Daemon config.", path_to_use.display());
                Ok(MainDaemonConfig::default())
            }
        }
    } else {
        warn!("Config file not found at {}. Using default Main Daemon config.", path_to_use.display());
        Ok(MainDaemonConfig::default())
    }
}

pub fn load_rest_config(config_file_path: Option<&str>) -> Result<RestApiConfig> {
    let default_config_path = PathBuf::from(DEFAULT_REST_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load REST API config from {:?}", path_to_use);

    if path_to_use.exists() {
        match fs::canonicalize(&path_to_use) {
            Ok(canonical_path) => {
                let config_content = fs::read_to_string(&canonical_path)
                    .context(format!("Failed to read REST API config file: {}", canonical_path.display()))?;
                debug!("REST API config content: {}", config_content);
                let wrapper: RestApiConfigWrapper = serde_yaml2::from_str(&config_content)
                    .map_err(|e| {
                        error!("YAML parsing error for REST API at {:?}: {:?}", canonical_path, e);
                        if let Ok(partial) = serde_yaml2::from_str::<Value>(&config_content) {
                            error!("Partial YAML parse: {:?}", partial);
                        }
                        anyhow!("Failed to parse REST API config YAML: {}", canonical_path.display())
                    })?;
                info!("Loaded REST API config: {:?}", wrapper.rest_api);
                Ok(wrapper.rest_api)
            }
            Err(e) => {
                warn!("Failed to canonicalize REST API config path {:?}", path_to_use);
                warn!("Config file not found at {}. Using default REST API config.", path_to_use.display());
                Ok(RestApiConfig::default())
            }
        }
    } else {
        warn!("Config file not found at {}. Using default REST API config.", path_to_use.display());
        Ok(RestApiConfig::default())
    }
}

pub fn save_rest_config(config: &RestApiConfig) -> Result<()> {
    let config_path = PathBuf::from(DEFAULT_REST_CONFIG_PATH_RELATIVE);

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

pub fn get_default_rest_port_from_config() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
}

pub fn get_rest_cluster_range() -> String {
    load_rest_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| RestApiConfig::default().cluster_range)
}

/// Loads the storage configuration from a YAML file or returns a default configuration.
/// Prints the loaded configuration to the terminal.
pub fn load_storage_config_from_yaml(config_file_path: Option<PathBuf>) -> Result<StorageConfig> {
    let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
    let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let path_to_use = config_file_path
        .or_else(|| project_config_path.exists().then(|| project_config_path))
        .unwrap_or(default_config_path);

    info!("Loading storage config from {:?}", path_to_use);
    let config = if path_to_use.exists() {
        let config_content = fs::read_to_string(&path_to_use)
            .context(format!("Failed to read storage config file: {}", path_to_use.display()))?;
        debug!("Raw YAML content for storage_config.yaml: {}", config_content);
        let wrapper: StorageConfigWrapper = serde_yaml::from_str(&config_content)
            .map_err(|e| {
                error!("Failed to parse storage config YAML at {}: {}", path_to_use.display(), e);
                if let Ok(partial) = serde_yaml::from_str::<Value>(&config_content) {
                    error!("Partial YAML parse: {:?}", partial);
                }
                anyhow!("Failed to parse storage config YAML: {}", e)
            })?;
        wrapper.storage
    } else {
        info!("Config file not found at {}. Using default storage config.", path_to_use.display());
        StorageConfig::default()
    };

    // Log the loaded configuration
    info!(
        "Loaded storage config: default_port={}, cluster_range={}, data_directory={:?}, storage_engine_type={:?}, log_directory={}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        config.default_port,
        config.cluster_range,
        config.data_directory,
        config.storage_engine_type,
        config.log_directory,
        config.max_disk_space_gb,
        config.min_disk_space_gb,
        config.use_raft_for_scale
    );

    Ok(config)
}

pub fn load_storage_config_str(config_file_path: Option<&str>) -> Result<StorageConfig> {
    let path = config_file_path.map(PathBuf::from);
    load_storage_config_from_yaml(path)
}

pub fn get_default_storage_port_from_config_or_cli_default() -> u16 {
    load_storage_config_from_yaml(None)
        .map(|config| config.default_port)
        .unwrap_or(DEFAULT_STORAGE_PORT)
}

pub fn get_storage_cluster_range() -> String {
    load_storage_config_from_yaml(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| StorageConfig::default().cluster_range)
}

pub fn get_default_rest_port() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
}

pub fn load_daemon_config(config_file_path: Option<&str>) -> Result<DaemonYamlConfig> {
    let default_config_path = PathBuf::from(DEFAULT_DAEMON_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load Daemon config from {:?}", path_to_use);

    if path_to_use.exists() {
        match fs::canonicalize(&path_to_use) {
            Ok(canonical_path) => {
                let config_content = fs::read_to_string(&canonical_path)
                    .context(format!("Failed to read daemon config file: {}", canonical_path.display()))?;
                debug!("Daemon config content: {}", config_content);
                let config: DaemonYamlConfig = serde_yaml2::from_str(&config_content)
                    .map_err(|e| {
                        error!("YAML parsing error for Daemon at {:?}: {:?}", canonical_path, e);
                        if let Ok(partial) = serde_yaml2::from_str::<Value>(&config_content) {
                            error!("Partial YAML parse: {:?}", partial);
                        }
                        anyhow!("Failed to parse daemon config YAML: {}", canonical_path.display())
                    })?;
                info!("Loaded Daemon config: {:?}", config);
                Ok(config)
            }
            Err(e) => {
                warn!("Failed to canonicalize Daemon config path {:?}", path_to_use);
                warn!("Config file not found at {}. Using default Daemon config.", path_to_use.display());
                Ok(DaemonYamlConfig::default())
            }
        }
    } else {
        warn!("Config file not found at {}. Using default Daemon config.", path_to_use.display());
        Ok(DaemonYamlConfig::default())
    }
}

pub fn save_daemon_config(config: &DaemonYamlConfig) -> Result<()> {
    let config_path = PathBuf::from(DEFAULT_DAEMON_CONFIG_PATH_RELATIVE);

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

// --- TOML CLI Config ---
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

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct RestConfig {
    pub port: u16,
    pub host: String,
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

#[derive(Debug, Deserialize, Serialize)]
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

impl CliConfigToml {
    pub fn save(&self) -> Result<()> {
        let config_path = PathBuf::from("/opt/graphdb/config.toml");

        let toml_string = toml::to_string(self)
            .context("Failed to serialize CliConfigToml to TOML")?;

        fs::create_dir_all(config_path.parent().unwrap())
            .context(format!("Failed to create parent directories for {}", config_path.display()))?;

        fs::write(&config_path, toml_string)
            .context(format!("Failed to write CliConfigToml to file: {}", config_path.display()))?;

        Ok(())
    }
}

pub fn load_cli_config() -> Result<CliConfigToml> {
    let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("cli")
        .join("config.toml");

    let config_content = fs::read_to_string(&config_path)
        .context(format!("Failed to read CLI config file: {}", config_path.display()))?;

    let config: CliConfigToml = toml::from_str(&config_content)
        .context("Failed to parse CLI config file")?;

    Ok(config)
}

pub fn get_cli_storage_config(config_file_path: Option<PathBuf>) -> CliTomlStorageConfig {
    let _ = config_file_path;
    load_cli_config()
        .ok()
        .and_then(|cli_config| cli_config.storage)
        .unwrap_or_default()
}

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
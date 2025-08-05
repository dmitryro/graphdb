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
pub const DEFAULT_STORAGE_CONFIG_PATH_RELATIVE: &str = "storage_daemon_server/storage_config.yaml";
pub const DEFAULT_REST_CONFIG_PATH_RELATIVE: &str = "rest_api/rest_api_config.yaml";
pub const DEFAULT_DAEMON_CONFIG_PATH_RELATIVE: &str = "daemon_api/daemon_config.yaml";
pub const DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE: &str = "server/main_app_config.yaml";
pub const PID_FILE_DIR: &str = "/tmp/graphdb/pids";
pub const DAEMON_PID_FILE_NAME_PREFIX: &str = "graphdb-daemon-";
pub const REST_PID_FILE_NAME_PREFIX: &str = "graphdb-rest-";
pub const STORAGE_PID_FILE_NAME_PREFIX: &str = "graphdb-storage-";
pub const EXECUTABLE_NAME: &str = "graphdb-server";
pub const DEFAULT_CONFIG_ROOT_DIRECTORY_STR: &str = if cfg!(target_family = "unix") {
    "/opt/graphdb"
} else if cfg!(target_family = "windows") {
    "C:\\Program Files\\GraphDB"
} else {
    "/opt/graphdb"
};
pub const DEFAULT_HTTP_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_HTTP_RETRIES: u32 = 3;
pub const DEFAULT_GRPC_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_GRPC_RETRIES: u32 = 5;

fn default_config_root_directory_for_yaml() -> PathBuf {
    dirs::home_dir()
        .map(|home| home.join(DEFAULT_CONFIG_ROOT_DIRECTORY_STR))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR))
}

fn default_config_root_directory_for_yaml_option() -> Option<PathBuf> {
    Some(default_config_root_directory_for_yaml())
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
            config_root_directory: default_config_root_directory_for_yaml().to_string_lossy().into_owned(),
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
    #[serde(default = "default_config_root_directory_for_yaml")]
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

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            config_root_directory: default_config_root_directory_for_yaml(),
            data_directory: PathBuf::from(format!("{}/storage_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            log_directory: "/var/log/graphdb".to_string(),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: format!("{}", DEFAULT_STORAGE_PORT),
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
    pub command: Command,

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

    #[clap(long, help = "Port for the Storage Daemon")]
    pub storage_port: Option<u16>,

    #[clap(long, help = "Cluster range for the Storage Daemon")]
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

#[derive(Debug, Subcommand, Clone)]
pub enum Command {
    Start {
        #[clap(
            long,
            default_value = "127.0.0.1",
            help = "IP address to bind the daemon to"
        )]
        ip: String,
        #[clap(long, help = "Port to bind the daemon to")]
        port: Option<u16>,
        #[clap(
            long,
            help = "Cluster range for the daemon (e.g., '9001-9005' or '9001')"
        )]
        cluster_range: Option<String>,
        #[clap(long, help = "Path to the data directory")]
        data_directory: Option<String>,
        #[clap(long, help = "Path to the log directory")]
        log_directory: Option<String>,
    },
    Stop {
        #[clap(long, help = "Port of the daemon to stop")]
        port: Option<u16>,
    },
    RestApi {
        #[clap(subcommand)]
        command: RestCliCommand,
    },
    Storage {
        #[clap(subcommand)]
        command: StorageAction,
    },
    Status {
        #[clap(subcommand)]
        command: Option<StatusAction>,
    },
    Cli {
        #[clap(
            long,
            default_value = "127.0.0.1",
            help = "IP address to bind the daemon to"
        )]
        ip: String,
        #[clap(long, help = "Port to bind the daemon to")]
        port: Option<u16>,
        #[clap(
            long,
            help = "Path to the data directory, defaults to ./data/graphdb_data"
        )]
        data_directory: Option<String>,
        #[clap(
            long,
            help = "Path to the log directory, defaults to ./log/graphdb_log"
        )]
        log_directory: Option<String>,
        #[clap(
            long,
            help = "Sets the internal storage engine for the CLI client (e.g., 'sled', 'rocksdb')"
        )]
        internal_storage_engine: Option<StorageEngineType>,
        #[clap(
            long,
            help = "Sets the storage engine type to use for daemon storage"
        )]
        daemon_storage_engine: Option<StorageEngineType>,
    },
}

#[derive(Debug, Subcommand, Clone)]
pub enum RestCliCommand {
    Start {
        #[clap(
            long,
            default_value = "127.0.0.1",
            help = "IP address to bind the REST API to"
        )]
        ip: String,
        #[clap(long, help = "Port to bind the REST API to")]
        port: Option<u16>,
        #[clap(
            long,
            help = "Cluster range for the REST API (e.g., '8082-8086' or '8082')"
        )]
        cluster_range: Option<String>,
        #[clap(long, help = "Path to the data directory")]
        data_directory: Option<String>,
        #[clap(long, help = "Path to the log directory")]
        log_directory: Option<String>,
    },
    Stop {
        #[clap(long, help = "Port of the REST API to stop")]
        port: Option<u16>,
    },
    Status {
        #[clap(long, help = "Port of the REST API to check status")]
        port: Option<u16>,
    },
    Version,
    Health,
    Query {
        #[clap(long, help = "Port for the query")]
        port: Option<u16>,
        #[clap(long, help = "GraphQL query string")]
        query: String,
    },
    RegisterUser {
        #[clap(long, help = "Username for registration")]
        username: String,
        #[clap(long, help = "Password for registration")]
        password: String,
    },
    Authenticate {
        #[clap(long, help = "Username for authentication")]
        username: String,
        #[clap(long, help = "Password for authentication")]
        password: String,
    },
    GraphQuery {
        #[clap(long, help = "GraphQL query string")]
        query_string: String,
        #[clap(long, help = "Persist the query results")]
        persist: Option<bool>,
    },
    StorageQuery,
}

#[derive(Debug, Subcommand, Clone)]
pub enum StorageAction {
    Start {
        #[clap(
            long,
            default_value = "127.0.0.1",
            help = "IP address to bind the Storage Daemon to"
        )]
        ip: String,
        #[clap(long, help = "Port to bind the Storage Daemon to")]
        port: Option<u16>,
        #[clap(
            long,
            help = "Cluster range for the Storage Daemon (e.g., '9000-9002' or '9000')"
        )]
        cluster_range: Option<String>,
        #[clap(long, help = "Path to the data directory")]
        data_directory: Option<String>,
        #[clap(long, help = "Path to the log directory")]
        log_directory: Option<String>,
        #[clap(
            long,
            help = "Max disk space in GB for the storage daemon (e.g., 1000)"
        )]
        max_disk_space_gb: Option<u64>,
        #[clap(
            long,
            help = "Min free disk space threshold in GB for the storage daemon"
        )]
        min_disk_space_gb: Option<u64>,
        #[clap(
            long,
            help = "Enable Raft for scaling and consistency in storage daemon"
        )]
        use_raft_for_scale: Option<bool>,
        #[clap(long, help = "Storage engine type ('sled' or 'rocksdb')")]
        storage_engine_type: Option<String>,
        #[clap(long, help = "Run as daemon")]
        daemon: Option<bool>,
        #[clap(long, help = "Run with REST API")]
        rest: Option<bool>,
    },
    Stop {
        #[clap(long, help = "Port of the Storage Daemon to stop")]
        port: Option<u16>,
    },
    Status {
        #[clap(long, help = "Port of the Storage Daemon to check status")]
        port: Option<u16>,
    },
    List,
}

#[derive(Debug, Subcommand, Clone)]
pub enum StatusAction {
    All,
    Daemon {
        #[clap(long, help = "Port of the daemon to check status")]
        port: Option<u16>,
    },
    Rest {
        #[clap(long, help = "Port of the REST API to check status")]
        port: Option<u16>,
    },
    Storage {
        #[clap(long, help = "Port of the Storage Daemon to check status")]
        port: Option<u16>,
    },
    Cluster,
}

// --- CLI Config Loading ---
impl CliConfig {
    pub fn load() -> Result<Self> {
        let mut args = <Self as clap::Parser>::parse();
        let config_root = args
            .config_root_directory
            .clone()
            .unwrap_or_else(|| default_config_root_directory_for_yaml());

        debug!("Resolved config root directory: {:?}", config_root);

        let global_config_path = config_root.join("graphdb_config.yaml");
        let mut global_yaml_config: Option<GlobalYamlConfig> = None;
        if global_config_path.exists() {
            debug!("Attempting to load global config from: {:?}", global_config_path);
            match fs::read_to_string(&global_config_path) {
                Ok(content) => match serde_yaml2::from_str(&content) {
                    Ok(config) => {
                        global_yaml_config = Some(config);
                        debug!("Successfully loaded global config.");
                    }
                    Err(e) => warn!("Failed to parse global config YAML {:?}: {}", global_config_path, e),
                },
                Err(e) => warn!("Failed to read global config file {:?}: {}", global_config_path, e),
            }
        }

        let storage_yaml_config = Self::load_specific_yaml(
            args.storage_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.storage.clone()),
            &config_root.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE),
            "storage_daemon_server/storage_config.yaml",
        );

        let rest_api_yaml_config = Self::load_specific_yaml(
            args.rest_api_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.rest_api.clone()),
            &config_root.join(DEFAULT_REST_CONFIG_PATH_RELATIVE),
            "rest_api/rest_api_config.yaml",
        );

        let main_daemon_yaml_config = Self::load_specific_yaml(
            args.main_daemon_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.main_daemon.clone()),
            &config_root.join(DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE),
            "server/main_app_config.yaml",
        );

        // Prioritize CLI arguments over config defaults
        if args.daemon_port.is_none() {
            args.daemon_port = match &args.command {
                Command::Start { port, .. } => *port,
                Command::Stop { port } => *port,
                Command::Cli { port, .. } => *port,
                _ => main_daemon_yaml_config.as_ref().map(|c| c.default_port),
            };
        }
        if args.daemon_cluster.is_none() {
            args.daemon_cluster = match &args.command {
                Command::Start { cluster_range, .. } => cluster_range.clone(),
                _ => main_daemon_yaml_config.as_ref().map(|c| c.cluster_range.clone()),
            };
        }

        if args.rest_port.is_none() {
            args.rest_port = match &args.command {
                Command::RestApi {
                    command: RestCliCommand::Start { port, .. },
                } => *port,
                Command::RestApi {
                    command: RestCliCommand::Stop { port },
                } => *port,
                _ => rest_api_yaml_config.as_ref().map(|c| c.default_port),
            };
        }
        // Ensure rest_cluster is set to the single REST port to avoid cluster range issues
        if args.rest_cluster.is_none() {
            args.rest_cluster = Some(args.rest_port.unwrap_or(DEFAULT_REST_API_PORT).to_string());
        }

        if args.storage_port.is_none() {
            args.storage_port = match &args.command {
                Command::Storage {
                    command: StorageAction::Start { port, .. },
                } => *port,
                Command::Storage {
                    command: StorageAction::Stop { port, .. },
                } => *port,
                _ => storage_yaml_config
                    .as_ref()
                    .map(|c| c.default_port)
                    .or(Some(get_default_storage_port_from_config_or_cli_default())),
            };
        }
        if args.storage_cluster.is_none() {
            args.storage_cluster = match &args.command {
                Command::Storage {
                    command: StorageAction::Start { cluster_range, .. },
                } => cluster_range.clone(),
                _ => storage_yaml_config.as_ref().map(|c| c.cluster_range.clone()),
            };
        }

        match &mut args.command {
            Command::Start {
                data_directory,
                log_directory,
                ..
            } => {
                if data_directory.is_none() {
                    *data_directory = main_daemon_yaml_config
                        .as_ref()
                        .map(|c| c.data_directory.clone());
                }
                if log_directory.is_none() {
                    *log_directory = main_daemon_yaml_config
                        .as_ref()
                        .map(|c| c.log_directory.clone());
                }
            }
            Command::RestApi {
                command: RestCliCommand::Start {
                    data_directory,
                    log_directory,
                    ..
                },
            } => {
                if data_directory.is_none() {
                    *data_directory = rest_api_yaml_config
                        .as_ref()
                        .map(|c| c.data_directory.clone());
                }
                if log_directory.is_none() {
                    *log_directory = rest_api_yaml_config
                        .as_ref()
                        .map(|c| c.log_directory.clone());
                }
            }
            Command::Storage {
                command: StorageAction::Start {
                    data_directory,
                    log_directory,
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type,
                    ..
                },
            } => {
                if data_directory.is_none() {
                    *data_directory = storage_yaml_config
                        .as_ref()
                        .map(|c| c.data_directory.to_string_lossy().into_owned());
                }
                if log_directory.is_none() {
                    *log_directory = storage_yaml_config
                        .as_ref()
                        .map(|c| c.log_directory.clone());
                }
                if max_disk_space_gb.is_none() {
                    *max_disk_space_gb = storage_yaml_config
                        .as_ref()
                        .map(|c| c.max_disk_space_gb);
                }
                if min_disk_space_gb.is_none() {
                    *min_disk_space_gb = storage_yaml_config
                        .as_ref()
                        .map(|c| c.min_disk_space_gb);
                }
                if use_raft_for_scale.is_none() {
                    *use_raft_for_scale = storage_yaml_config
                        .as_ref()
                        .map(|c| c.use_raft_for_scale);
                }
                if storage_engine_type.is_none() {
                    *storage_engine_type = storage_yaml_config
                        .as_ref()
                        .map(|c| daemon_api_storage_engine_type_to_string(&c.storage_engine_type));
                }
            }
            _ => {}
        }

        Ok(args)
    }

    pub fn load_cli_config() -> Result<Self> {
        Self::load()
    }

    fn load_specific_yaml<T>(
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
            match fs::read_to_string(path) {
                Ok(content) => match serde_yaml2::from_str(&content) {
                    Ok(config) => {
                        info!("Successfully loaded {} from CLI path: {:?}", config_name_for_log, path);
                        return Some(config);
                    }
                    Err(e) => {
                        error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, path, e);
                        warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, path, e);
                    }
                },
                Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, path, e),
            }
        }

        if let Some(embed) = global_embed {
            debug!("Using {} config embedded in global config.", config_name_for_log);
            return Some(embed);
        }

        if default_path.exists() {
            debug!("Attempting to load {} from default path: {:?}", config_name_for_log, default_path);
            match fs::read_to_string(default_path) {
                Ok(content) => match serde_yaml2::from_str(&content) {
                    Ok(config) => {
                        info!("Successfully loaded {} from default path: {:?}", config_name_for_log, default_path);
                        return Some(config);
                    }
                    Err(e) => {
                        error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, default_path, e);
                        warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, default_path, e);
                    }
                },
                Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, default_path, e),
            }
        }

        debug!("No {} config found via CLI, global, or default paths.", config_name_for_log);
        None
    }

    pub fn get_daemon_address(&self) -> Result<SocketAddr> {
        let ip_str = match &self.command {
            Command::Start { ip, .. } => ip,
            Command::Cli { ip, .. } => ip,
            _ => return Err(anyhow!("Not in a daemon start/cli context to retrieve IP address.")),
        };
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_daemon_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_daemon_port(&self) -> Result<u16> {
        match &self.command {
            Command::Start { port, .. } | Command::Stop { port } | Command::Cli { port, .. } => {
                port.or(self.daemon_port)
                    .with_context(|| "Daemon port not specified and no default found.")
            }
            _ => self.daemon_port
                .with_context(|| "Daemon port not specified and no default found for this command."),
        }
    }

    pub fn get_daemon_cluster_range(&self) -> Result<String> {
        match &self.command {
            Command::Start { cluster_range, .. } => cluster_range
                .clone()
                .or(self.daemon_cluster.clone())
                .with_context(|| "Daemon cluster range not specified and no default found."),
            _ => self.daemon_cluster.clone().with_context(|| {
                "Daemon cluster range not specified and no default found for this command."
            }),
        }
    }

    pub fn get_rest_api_address(&self) -> Result<SocketAddr> {
        let ip_str = match &self.command {
            Command::RestApi {
                command: RestCliCommand::Start { ip, .. },
            } => ip,
            _ => return Err(anyhow!("Not in a REST API start context to retrieve IP address.")),
        };
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_rest_api_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_rest_api_port(&self) -> Result<u16> {
        match &self.command {
            Command::RestApi {
                command: RestCliCommand::Start { port, .. },
            }
            | Command::RestApi {
                command: RestCliCommand::Stop { port },
            } => port
                .or(self.rest_port)
                .with_context(|| "REST API port not specified and no default found."),
            _ => self.rest_port
                .with_context(|| "REST API port not specified and no default found for this command."),
        }
    }

    pub fn get_rest_api_cluster_range(&self) -> Result<String> {
        match &self.command {
            Command::RestApi {
                command: RestCliCommand::Start { cluster_range, .. },
            } => cluster_range
                .clone()
                .or(self.rest_cluster.clone())
                .with_context(|| "REST API cluster range not specified and no default found."),
            _ => self.rest_cluster.clone().with_context(|| {
                "REST API cluster range not specified and no default found for this command."
            }),
        }
    }

    pub fn get_storage_daemon_address(&self) -> Result<SocketAddr> {
        let ip_str = match &self.command {
            Command::Storage {
                command: StorageAction::Start { ip, .. },
            } => ip,
            _ => return Err(anyhow!("Not in a Storage Daemon start context to retrieve IP address.")),
        };
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_storage_daemon_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_storage_daemon_port(&self) -> Result<u16> {
        match &self.command {
            Command::Storage {
                command: StorageAction::Start { port, .. },
            }
            | Command::Storage {
                command: StorageAction::Stop { port, .. },
            } => port
                .or(self.storage_port)
                .with_context(|| "Storage Daemon port not specified and no default found."),
            _ => self.storage_port
                .with_context(|| "Storage Daemon port not specified and no default found for this command."),
        }
    }

    pub fn get_storage_daemon_cluster_range(&self) -> Result<String> {
        match &self.command {
            Command::Storage {
                command: StorageAction::Start { cluster_range, .. },
            } => cluster_range
                .clone()
                .or(self.storage_cluster.clone())
                .with_context(|| "Storage Daemon cluster range not specified and no default found."),
            _ => self.storage_cluster.clone().with_context(|| {
                "Storage Daemon cluster range not specified and no default found for this command."
            }),
        }
    }

    pub fn get_data_directory(&self) -> Result<String> {
        match &self.command {
            Command::Start { data_directory, .. } => data_directory
                .clone()
                .with_context(|| "Data directory not specified for Daemon."),
            Command::RestApi {
                command: RestCliCommand::Start { data_directory, .. },
            } => data_directory
                .clone()
                .with_context(|| "Data directory not specified for REST API."),
            Command::Storage {
                command: StorageAction::Start { data_directory, .. },
            } => data_directory
                .clone()
                .with_context(|| "Data directory not specified for Storage Daemon."),
            Command::Cli { data_directory, .. } => data_directory
                .clone()
                .with_context(|| "Data directory not specified for CLI."),
            _ => Err(anyhow!("Not in a start/cli context to retrieve data directory.")),
        }
    }

    pub fn get_log_directory(&self) -> Result<String> {
        match &self.command {
            Command::Start { log_directory, .. } => log_directory
                .clone()
                .with_context(|| "Log directory not specified for Daemon."),
            Command::RestApi {
                command: RestCliCommand::Start { log_directory, .. },
            } => log_directory
                .clone()
                .with_context(|| "Log directory not specified for REST API."),
            Command::Storage {
                command: StorageAction::Start { log_directory, .. },
            } => log_directory
                .clone()
                .with_context(|| "Log directory not specified for Storage Daemon."),
            Command::Cli { log_directory, .. } => log_directory
                .clone()
                .with_context(|| "Log directory not specified for CLI."),
            _ => Err(anyhow!("Not in a start/cli context to retrieve log directory.")),
        }
    }

    pub fn get_storage_max_disk_space_gb(&self) -> Result<u64> {
        match &self.command {
            Command::Storage {
                command: StorageAction::Start { max_disk_space_gb, .. },
            } => max_disk_space_gb
                .with_context(|| "Max disk space not specified for Storage Daemon."),
            _ => Err(anyhow!("Not in a Storage Daemon start context to retrieve max disk space.")),
        }
    }

    pub fn get_storage_min_disk_space_gb(&self) -> Result<u64> {
        match &self.command {
            Command::Storage {
                command: StorageAction::Start { min_disk_space_gb, .. },
            } => min_disk_space_gb
                .with_context(|| "Min disk space not specified for Storage Daemon."),
            _ => Err(anyhow!("Not in a Storage Daemon start context to retrieve min disk space.")),
        }
    }

    pub fn get_storage_use_raft_for_scale(&self) -> Result<bool> {
        match &self.command {
            Command::Storage {
                command: StorageAction::Start { use_raft_for_scale, .. },
            } => use_raft_for_scale
                .with_context(|| "Raft for scale not specified for Storage Daemon."),
            _ => Err(anyhow!("Not in a Storage Daemon start context to retrieve use_raft_for_scale.")),
        }
    }

    pub fn get_storage_engine_type(&self) -> Result<String> {
        match &self.command {
            Command::Storage {
                command: StorageAction::Start { storage_engine_type, .. },
            } => storage_engine_type
                .clone()
                .with_context(|| "Storage engine type not specified for Storage Daemon."),
            _ => Err(anyhow!("Not in a Storage Daemon start context to retrieve storage engine type.")),
        }
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
        match &self.command {
            Command::Cli {
                internal_storage_engine,
                ..
            } => internal_storage_engine
                .clone()
                .with_context(|| "Internal storage engine not specified for CLI."),
            _ => Err(anyhow!("Not in a CLI context to retrieve internal storage engine.")),
        }
    }

    pub fn get_cli_daemon_storage_engine(&self) -> Result<StorageEngineType> {
        match &self.command {
            Command::Cli {
                daemon_storage_engine,
                ..
            } => daemon_storage_engine
                .clone()
                .with_context(|| "Daemon storage engine not specified for CLI."),
            _ => Err(anyhow!("Not in a CLI context to retrieve daemon storage engine.")),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct GlobalYamlConfig {
    #[serde(default = "default_config_root_directory_for_yaml_option")]
    #[serde(with = "option_path_buf_serde")]
    config_root_directory: Option<PathBuf>,
    storage: Option<StorageConfig>,
    rest_api: Option<RestApiConfig>,
    main_daemon: Option<MainDaemonConfig>,
}

// --- Config Loading Functions ---
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
        debug!("Main Daemon config content: {}", config_content);
        let wrapper: MainConfigWrapper = serde_yaml2::from_str(&config_content)
            .map_err(|e| {
                error!("YAML parsing error for Main Daemon at {:?}: {:?}", path_to_use, e);
                if let Ok(partial) = serde_yaml2::from_str::<Value>(&config_content) {
                    error!("Partial YAML parse: {:?}", partial);
                }
                anyhow!("Failed to parse main daemon config YAML: {}", path_to_use.display())
            })?;
        info!("Successfully loaded Main Daemon config: {:?}", wrapper.main_daemon);
        Ok(wrapper.main_daemon)
    } else {
        warn!("Config file not found at {}. Using default Main Daemon config.", path_to_use.display());
        Ok(MainDaemonConfig::default())
    }
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
        debug!("REST API config content: {}", config_content);
        let wrapper: RestApiConfigWrapper = serde_yaml2::from_str(&config_content)
            .map_err(|e| {
                error!("YAML parsing error for REST API at {:?}: {:?}", path_to_use, e);
                if let Ok(partial) = serde_yaml2::from_str::<Value>(&config_content) {      
                    error!("Partial YAML parse: {:?}", partial);
                }
                anyhow!("Failed to parse REST API config YAML: {}", path_to_use.display())
            })?;
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

pub fn load_storage_config_from_yaml(config_file_path: Option<PathBuf>) -> Result<StorageConfig> {
    let default_config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let path_to_use = config_file_path.unwrap_or(default_config_path);
    info!("Loading Storage config from {:?}", path_to_use);

    if !path_to_use.exists() {
        warn!("Config file not found at {}. Using default Storage config.", path_to_use.display());
        return Ok(StorageConfig::default());
    }

    let config_content = fs::read_to_string(&path_to_use)
        .context(format!("Failed to read storage config file: {}", path_to_use.display()))?;
    debug!("Storage config content: {}", config_content);

    let wrapper: StorageConfigWrapper = serde_yaml2::from_str(&config_content)
        .map_err(|e| {
            error!("YAML parsing error for Storage at {:?}: {:?}", path_to_use, e);
            if let Ok(partial) = serde_yaml2::from_str::<Value>(&config_content) {
                error!("Partial YAML parse: {:?}", partial);
            }
            anyhow!("Failed to parse storage config YAML at {}: {}", path_to_use.display(), e)
        })?;

    let config = wrapper.storage;
    if config.data_directory.as_os_str().is_empty() {
        warn!("data_directory is empty in {}. Using default: {:?}", path_to_use.display(), StorageConfig::default().data_directory);
        return Ok(StorageConfig {
            data_directory: StorageConfig::default().data_directory,
            ..config
        });
    }

    if config.default_port == 0 {
        warn!("default_port is invalid (0) in {}. Using default: {}", path_to_use.display(), DEFAULT_STORAGE_PORT);
        return Ok(StorageConfig {
            default_port: DEFAULT_STORAGE_PORT,
            ..config
        });
    }

    info!("Loaded Storage config: {:?}", config);
    Ok(config)
}

pub fn load_storage_config_str(config_file_path: Option<&str>) -> Result<StorageConfig> {
    let path = config_file_path.map(PathBuf::from);
    load_storage_config_from_yaml(path)
}

pub fn get_default_storage_port_from_config_or_cli_default() -> u16 {
    load_storage_config_from_yaml(None)
        .map(|config| config.default_port)
        .unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
}

pub fn get_storage_cluster_range() -> String {
    load_storage_config_from_yaml(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| StorageConfig::default().cluster_range)
}

pub fn get_default_rest_port_from_config() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
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
        debug!("Daemon config content: {}", config_content);
        let config: DaemonYamlConfig = serde_yaml2::from_str(&config_content)
            .map_err(|e| {
                error!("YAML parsing error for Daemon at {:?}: {:?}", path_to_use, e);
                if let Ok(partial) = serde_yaml2::from_str::<Value>(&config_content) {
                    error!("Partial YAML parse: {:?}", partial);
                }
                anyhow!("Failed to parse daemon config YAML: {}", path_to_use.display())
            })?;
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

#[derive(Debug, Deserialize)]
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
use std::fmt;
use std::str::FromStr;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn, trace};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_yaml2 as serde_yaml;
use futures::FutureExt;
use crate::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction, 
                     ReloadAction, RestCliCommand, StatusAction, StorageAction, ShowAction, ShowArgs,
                     StartAction, StopAction, StopArgs, DaemonCliCommand, UseAction, SaveAction};
pub use crate::config::StorageConfig as EngineStorageConfig;
pub use crate::config::{StorageEngineType, 
                       SelectedStorageConfig,
                       StorageConfigInner};
use crate::query_exec_engine::query_exec_engine::{QueryExecEngine};
pub use crate::storage_engine::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
pub use crate::config::{EngineTypeOnly, RocksDBConfig, SledConfig, TikvConfig, MySQLConfig, 
                       RedisConfig, PostgreSQLConfig};
pub use models::errors::GraphError;
pub use crate::config::config_structs::*;
pub use crate::config::config_constants::*;
pub use crate::config::config_defaults::*;
pub use crate::config::config_helpers::*;
pub use crate::config::config_serializers::*;

impl FromStr for StorageEngineType {
    type Err = GraphError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        trace!("Parsing storage engine type: {}", s);
        match s.to_lowercase().as_str() {
            "hybrid" => Ok(StorageEngineType::Hybrid),
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" | "rocks_db" => Ok(StorageEngineType::RocksDB),
            "tikv" | "ti_kv" => Ok(StorageEngineType::TiKV),
            "inmemory" | "in_memory" => Ok(StorageEngineType::InMemory),
            "redis" => Ok(StorageEngineType::Redis),
            "postgresql" | "postgres" | "postgres_sql" => Ok(StorageEngineType::PostgreSQL),
            "mysql" | "my_sql" => Ok(StorageEngineType::MySQL),
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

impl CliConfig {
    pub async fn execute(&self, manager: &mut StorageEngineManager) -> Result<(), GraphError> {
        match &self.command {
            Commands::Use(UseAction::Storage { engine, permanent }) => {
                let is_permanent = *permanent;
                info!("Switching to storage engine: {:?}, permanent: {}", engine, is_permanent);
                let available_engines = StorageEngineManager::available_engines();
                if !available_engines.contains(engine) {
                    return Err(GraphError::InvalidStorageEngine(format!(
                        "Storage engine {:?} is not enabled. Available engines: {:?}", engine, available_engines
                    )));
                }

                // Load or create storage configuration
                let mut storage_config = match load_storage_config_from_yaml(None).await {
                    Ok(config) => {
                        debug!("Loaded existing storage config: {:?}", config);
                        config
                    }
                    Err(e) => {
                        warn!("Failed to load storage config: {}. Using default config.", e);
                        StorageConfig::default()
                    }
                };

                // Update storage_config with engine-specific settings
                storage_config.storage_engine_type = *engine;
                let default_port = match engine {
                    StorageEngineType::TiKV => 2380,
                    _ => 8052, // Default for Sled, RocksDB, etc.
                };
                storage_config.default_port = default_port;
                storage_config.cluster_range = default_port.to_string();
                storage_config.engine_specific_config = Some(SelectedStorageConfig {
                    storage_engine_type: *engine,
                    storage: StorageConfigInner {
                        path: Some(PathBuf::from(format!(
                            "{}/{}",
                            storage_config.data_directory.as_ref().map_or(DEFAULT_DATA_DIRECTORY.to_string(), |p| p.to_string_lossy().to_string()),
                            engine.to_string().to_lowercase()
                        ))),
                        host: Some("127.0.0.1".to_string()),
                        port: Some(default_port),
                        username: None,
                        password: None,
                        database: None,
                        pd_endpoints: if *engine == StorageEngineType::TiKV {
                            Some("127.0.0.1:2379".to_string())
                        } else {
                            None
                        },
                        cache_capacity: Some(1024 * 1024 * 1024),
                        use_compression: true,
                        temporary: false,
                        use_raft_for_scale: false,
                    },
                });

                // Load engine-specific config file if it exists
                let engine_config_path = match engine.to_string().to_lowercase().as_str() {
                    "sled" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED),
                    "rocksdb" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB),
                    "postgres" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES),
                    "mysql" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL),
                    "redis" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS),
                    "tikv" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV),
                    _ => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH),
                };
                if engine_config_path.exists() {
                    match SelectedStorageConfig::load_from_yaml(&engine_config_path) {
                        Ok(engine_specific) => {
                            storage_config.engine_specific_config = Some(engine_specific);
                            if let Some(port) = storage_config.engine_specific_config.as_ref().and_then(|c| c.storage.port) {
                                storage_config.default_port = port;
                                storage_config.cluster_range = port.to_string();
                            }
                            debug!("Loaded engine-specific config from {:?}: {:?}", engine_config_path, storage_config.engine_specific_config);
                        }
                        Err(e) => {
                            warn!("Failed to load engine-specific config from {:?}: {}. Using constructed config.", engine_config_path, e);
                        }
                    }
                }

                // Call use_storage with the storage_config
                manager.use_storage(storage_config.clone(), is_permanent).await?;

                // Save config if permanent
                if is_permanent {
                    storage_config.save().await
                        .map_err(|e| GraphError::ConfigurationError(format!("Failed to save storage config: {}", e)))?;
                    let reloaded_config = load_storage_config_from_yaml(None).await
                        .map_err(|e| GraphError::ConfigurationError(format!("Failed to reload storage config: {}", e)))?;
                    info!("Reloaded storage config: {:?}", reloaded_config);
                    if reloaded_config.storage_engine_type != *engine {
                        error!("Reloaded config has incorrect storage_engine_type: expected {:?}, got {:?}", engine, reloaded_config.storage_engine_type);
                        return Err(GraphError::ConfigurationError("Failed to reload correct storage engine type".to_string()));
                    }
                }

                println!("Switched to storage engine {} (persisted: {})", daemon_api_storage_engine_type_to_string(engine), is_permanent);
                Ok(())
            }
            Commands::Show(ShowArgs { action: ShowAction::Storage }) => {
                let storage_config = load_storage_config_from_yaml(None).await
                    .map_err(|e| GraphError::ConfigurationError(format!("Failed to load storage config: {}", e)))?;
                debug!("Loaded storage config for show: {:?}", storage_config);
                println!("Current Storage Configuration (from {:?}):", PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
                println!("- storage_engine_type: {}", storage_config.storage_engine_type.to_string().to_lowercase());
                println!("- config_root_directory: {:?}", storage_config.config_root_directory.unwrap_or_else(|| PathBuf::from("N/A")));
                println!("- data_directory: {:?}", storage_config.data_directory.unwrap_or_else(|| PathBuf::from("N/A")));
                println!("- log_directory: {:?}", storage_config.log_directory.unwrap_or_else(|| PathBuf::from("N/A")));
                println!("- default_port: {}", storage_config.default_port);
                println!("- cluster_range: {}", storage_config.cluster_range);
                println!("- max_disk_space_gb: {}", storage_config.max_disk_space_gb);
                println!("- min_disk_space_gb: {}", storage_config.min_disk_space_gb);
                println!("- use_raft_for_scale: {}", storage_config.use_raft_for_scale);
                println!("- max_open_files: {}", storage_config.max_open_files);
                if let Some(engine_config) = &storage_config.engine_specific_config {
                    println!("- engine_specific_config:");
                    println!("  - storage_engine_type: {}", engine_config.storage_engine_type.to_string().to_lowercase());
                    println!("  - path: {:?}", engine_config.storage.path.as_ref().map(|p| p.as_path()).unwrap_or_else(|| Path::new("None")));
                    println!("  - host: {}", engine_config.storage.host.clone().unwrap_or("None".to_string()));
                    println!("  - port: {}", engine_config.storage.port.unwrap_or(0));
                    println!("  - username: {}", engine_config.storage.username.clone().unwrap_or("None".to_string()));
                    println!("  - password: {}", engine_config.storage.password.clone().unwrap_or("None".to_string()));
                    println!("  - database: {}", engine_config.storage.database.clone().unwrap_or("None".to_string()));
                } else {
                    println!("- engine_specific_config: None");
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    
    /// Loads configuration from CLI arguments, then YAML files, and finally built-in defaults.
    pub fn load(interactive_command: Option<CommandType>) -> Result<Self> {
        // Parse CLI arguments first to get the base configuration
        let mut args = if let Some(cmd) = interactive_command {
            let mut config = CliConfig {
                command: Commands::Exit,
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
                cli: true,
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
                }
                CommandType::UseStorage { engine, permanent } => {
                    config.command = Commands::Use(UseAction::Storage { engine, permanent });
                }
                CommandType::SaveStorage => {
                    config.command = Commands::Save(SaveAction::Storage);
                }
                CommandType::SaveConfig => {
                    config.command = Commands::Save(SaveAction::Configuration);
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

        // Determine the root configuration directory
        let default_config_root_directory = || PathBuf::from("/etc/graphdb");
        let config_root = args.config_root_directory.clone().unwrap_or_else(|| default_config_root_directory());
        debug!("Resolved config root directory: {:?}", config_root);

        // Load global YAML config
        let global_config_path = config_root.join("graphdb_config.yaml");
        let global_yaml_config: Option<GlobalYamlConfig> = Self::load_specific_yaml(
            None,
            None::<GlobalYamlConfig>,
            &global_config_path,
            "global",
        );

        // Load all other configs, prioritizing CLI args first, then embedded global configs, then defaults.
        let storage_config_path = args.storage_config_path.clone().unwrap_or_else(|| config_root.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
        let storage_yaml_config: Option<StorageYamlConfig> = Self::load_specific_yaml(
            args.storage_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.storage.clone()),
            &storage_config_path,
            "storage",
        );

        let rest_api_config_path = args.rest_api_config_path.clone().unwrap_or_else(|| config_root.join(DEFAULT_REST_CONFIG_PATH_RELATIVE));
        let rest_api_yaml_config: Option<RestApiYamlConfig> = Self::load_specific_yaml(
            args.rest_api_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.rest_api.clone()),
            &rest_api_config_path,
            "rest_api",
        );

        let main_daemon_config_path = args.main_daemon_config_path.clone().unwrap_or_else(|| config_root.join(DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE));
        let main_daemon_yaml_config: Option<MainDaemonYamlConfig> = Self::load_specific_yaml(
            args.main_daemon_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.main_daemon.clone()),
            &main_daemon_config_path,
            "main_daemon",
        );

        // Apply loaded configurations, ensuring CLI args have precedence.
        if args.daemon_port.is_none() {
            args.daemon_port = main_daemon_yaml_config.as_ref().and_then(|c| Some(c.default_port));
        }
        if args.daemon_cluster.is_none() {
            args.daemon_cluster = main_daemon_yaml_config.as_ref().and_then(|c| Some(c.cluster_range.clone()));
        }
        if args.rest_port.is_none() {
            args.rest_port = rest_api_yaml_config.as_ref().and_then(|c| Some(c.default_port));
        }
        if args.rest_cluster.is_none() {
            args.rest_cluster = rest_api_yaml_config.as_ref().and_then(|c| Some(c.cluster_range.clone()));
        }
        if args.storage_port.is_none() {
            args.storage_port = storage_yaml_config.as_ref().and_then(|c| Some(c.default_port));
        }
        if args.storage_cluster.is_none() {
            args.storage_cluster = storage_yaml_config.as_ref().and_then(|c| Some(c.cluster_range.clone()));
        }
        if args.storage_config_path.is_none() {
            args.storage_config_path = Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
        }

        info!("SUCCESS: CLI config loaded.");
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
            match fs::canonicalize(path) {
                Ok(canonical_path) => {
                    match fs::read_to_string(&canonical_path) {
                        Ok(content) => {
                            match serde_yaml2::from_str(&content) {
                                Ok(config) => {
                                    info!("Loaded {} config from CLI path: {:?}", config_name_for_log, canonical_path);
                                    return Some(config);
                                }
                                Err(e) => {
                                    error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, canonical_path, e);
                                    warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, canonical_path, e);
                                }
                            }
                        }
                        Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, canonical_path, e),
                    }
                }
                Err(e) => warn!("Failed to canonicalize {} path {:?}: {}", config_name_for_log, path, e),
            }
        }

        if let Some(embed) = global_embed {
            debug!("Using {} config embedded in global config.", config_name_for_log);
            return Some(embed);
        }

        if default_path.exists() {
            match fs::canonicalize(default_path) {
                Ok(canonical_path) => {
                    match fs::read_to_string(&canonical_path) {
                        Ok(content) => {
                            match serde_yaml2::from_str(&content) {
                                Ok(config) => {
                                    info!("Loaded {} config from default path: {:?}", config_name_for_log, canonical_path);
                                    return Some(config);
                                }
                                Err(e) => {
                                    error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, canonical_path, e);
                                    warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, canonical_path, e);
                                }
                            }
                        }
                        Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, canonical_path, e),
                    }
                }
                Err(e) => warn!("Failed to canonicalize {} path {:?}: {}", config_name_for_log, default_path, e),
            }
        }

        debug!("No {} config found via CLI, global, or default paths. Using default.", config_name_for_log);
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
                StartAction::Daemon { .. } => Ok(DEFAULT_DATA_DIRECTORY.to_string()),
                StartAction::Rest { .. } => Ok(DEFAULT_DATA_DIRECTORY.to_string()),
                StartAction::Storage { .. } => Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned()),
                _ => Err(anyhow!("Not in a start context to retrieve data directory.")),
            },
            Commands::Rest(RestCliCommand::Start { .. }) => {
                Ok(DEFAULT_DATA_DIRECTORY.to_string())
            }
            Commands::Storage(StorageAction::Start { .. }) => {
                Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned())
            }
            Commands::Daemon(DaemonCliCommand::Start { .. }) => {
                Ok(DEFAULT_DATA_DIRECTORY.to_string())
            }
            Commands::Use(UseAction::Storage { .. }) => {
                Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned())
            }
            Commands::Save(SaveAction::Storage) => {
                Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned())
            }
            Commands::Save(SaveAction::Configuration) => {
                Ok(default_config_root_directory().to_string_lossy().into_owned())
            }
            _ => Err(anyhow!("Not in a start/cli context to retrieve data directory.")),
        }
    }

    pub fn get_log_directory(&self) -> Result<String> {
        match &self.command {
            Commands::Start { action: Some(start_action), .. } => match start_action {
                StartAction::Daemon { .. } => Ok(DEFAULT_LOG_DIRECTORY.to_string()),
                StartAction::Rest { .. } => Ok(DEFAULT_LOG_DIRECTORY.to_string()),
                StartAction::Storage { .. } => {
                    StorageConfig::default().log_directory
                        .map(|path| path.to_string_lossy().to_string())
                        .ok_or_else(|| anyhow!("No log directory configured for storage"))
                },
                _ => Err(anyhow!("Not in a start context to retrieve log directory.")),
            },
            Commands::Rest(RestCliCommand::Start { .. }) => {
                Ok(DEFAULT_LOG_DIRECTORY.to_string())
            }
            Commands::Storage(StorageAction::Start { .. }) => {
                StorageConfig::default().log_directory
                    .map(|path| path.to_string_lossy().to_string())
                    .ok_or_else(|| anyhow!("No log directory configured for storage"))
            }
            Commands::Daemon(DaemonCliCommand::Start { .. }) => {
                Ok(DEFAULT_LOG_DIRECTORY.to_string())
            }
            Commands::Use(UseAction::Storage { .. }) => {
                StorageConfig::default().log_directory
                    .map(|path| path.to_string_lossy().to_string())
                    .ok_or_else(|| anyhow!("No log directory configured for storage"))
            }
            Commands::Save(SaveAction::Storage) => {
                StorageConfig::default().log_directory
                    .map(|path| path.to_string_lossy().to_string())
                    .ok_or_else(|| anyhow!("No log directory configured for storage"))
            }
            Commands::Save(SaveAction::Configuration) => {
                Ok(DEFAULT_LOG_DIRECTORY.to_string())
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

impl Default for MainDaemonConfig {
    fn default() -> Self {
        MainDaemonConfig {
            data_directory: DEFAULT_DATA_DIRECTORY.to_string(),
            log_directory: DEFAULT_LOG_DIRECTORY.to_string(),
            default_port: DEFAULT_DAEMON_PORT,
            cluster_range: "8080-8082".to_string(),
        }
    }
}

impl Default for RestApiConfig {
    fn default() -> Self {
        RestApiConfig {
            data_directory: DEFAULT_DATA_DIRECTORY.to_string(),
            log_directory: DEFAULT_LOG_DIRECTORY.to_string(),
            default_port: DEFAULT_REST_PORT,
            cluster_range: "8083-8085".to_string(),
        }
    }
}

impl Default for CliConfigToml {
    fn default() -> Self {
        CliConfigToml {
            app: AppConfig::default(),
            server: ServerConfig::default(),
            rest: RestConfig::default(),
            daemon: DaemonConfig::default(),
            storage: CliTomlStorageConfig::default(),
            deployment: DeploymentConfig::default(),
            log: LogConfig::default(),
            paths: PathsConfig::default(),
            security: SecurityConfig::default(),
            enable_plugins: false,
        }
    }
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

        info!("Saved CLI configuration to {:?}", config_path);
        Ok(())
    }
}

impl From<&CliTomlStorageConfig> for EngineStorageConfig {
    fn from(cli: &CliTomlStorageConfig) -> Self {
        EngineStorageConfig {
            storage_engine_type: cli.storage_engine_type.unwrap_or(StorageEngineType::Sled),
            data_directory: Some(cli.data_directory
                .clone()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))),
            max_open_files: cli.max_open_files.unwrap_or(1024),
            engine_specific_config: None,
            default_port: cli.default_port.unwrap_or(DEFAULT_STORAGE_PORT),
            log_directory: Some(cli.log_directory
                .clone()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_DIRECTORY))),
            config_root_directory: Some(cli.config_root_directory
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR))),
            cluster_range: cli.cluster_range
                .clone()
                .unwrap_or("8083-8087".to_string()),
            use_raft_for_scale: cli.use_raft_for_scale.unwrap_or(true),
            max_disk_space_gb: cli.max_disk_space_gb.unwrap_or(1000),
            min_disk_space_gb: cli.min_disk_space_gb.unwrap_or(10),
        }
    }
}

impl From<CliTomlStorageConfig> for StorageConfig {
    fn from(cli: CliTomlStorageConfig) -> Self {
        StorageConfig {
            config_root_directory: cli.config_root_directory,
            data_directory: cli.data_directory.map(PathBuf::from),
            log_directory: cli.log_directory.map(PathBuf::from),
            default_port: cli.default_port.unwrap_or(9042),
            cluster_range: cli.cluster_range.unwrap_or_else(|| "9042".to_string()),
            max_disk_space_gb: cli.max_disk_space_gb.unwrap_or(1000),
            min_disk_space_gb: cli.min_disk_space_gb.unwrap_or(10),
            use_raft_for_scale: cli.use_raft_for_scale.unwrap_or(true),
            storage_engine_type: cli.storage_engine_type.unwrap_or(StorageEngineType::RocksDB),
            engine_specific_config: None,
            max_open_files: cli.max_open_files.unwrap_or(1024),
        }
    }
}

impl StorageConfig {
    /// Creates a new StorageConfig instance configured for an in-memory storage engine.
    /// This is a common pattern for setting up test environments or temporary storage.
    pub fn new_in_memory() -> Self {
        Self {
            storage_engine_type: StorageEngineType::RocksDB,
            data_directory: Some(PathBuf::from("")), 
            config_root_directory: None,
            log_directory: None,
            default_port: 0,
            cluster_range: "".to_string(),
            max_disk_space_gb: 1,
            min_disk_space_gb: 0,
            use_raft_for_scale: false,
            engine_specific_config: None,
            max_open_files: 1024,
        }
    }
}

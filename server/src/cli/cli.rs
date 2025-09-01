use clap::{Parser, Subcommand, CommandFactory};
use anyhow::{Result, Context, anyhow};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, OnceCell, oneshot};
use tokio::task::JoinHandle;
use std::process;
use std::env;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use serde_yaml2 as serde_yaml;
use std::fs;
use storage_daemon_server::{StorageSettings, StorageSettingsWrapper};
use log::{info, debug, warn};
use models::errors::GraphError;

// Import modules
use crate::cli::commands::{
    parse_kv_operation, ConfigAction, DaemonCliCommand, HelpArgs, ReloadAction, RestartAction,
    RestCliCommand, SaveAction, ShowAction, StartAction, StatusAction, StopAction, StorageAction,
    StatusArgs, StopArgs, ReloadArgs, RestartArgs, UseAction
};
use crate::cli::config::{
    self, load_storage_config_from_yaml, SelectedStorageConfig, StorageConfig as CliStorageConfig,
    StorageConfigInner, StorageEngineType, DEFAULT_STORAGE_CONFIG_PATH_MYSQL,
    DEFAULT_STORAGE_CONFIG_PATH_POSTGRES, DEFAULT_STORAGE_CONFIG_PATH_REDIS,
    DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB, DEFAULT_STORAGE_CONFIG_PATH_SLED,
    DEFAULT_STORAGE_CONFIG_PATH_TIKV, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    load_cli_config
};
use crate::cli::config as config_mod;
use crate::cli::daemon_management;
use crate::cli::handlers as handlers_mod;
use crate::cli::handlers_storage::{ start_storage_interactive, stop_storage_interactive, };
use crate::cli::handlers_utils::parse_storage_engine;
use crate::cli::help_display as help_display_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::handlers_queries::initialize_storage_for_query;
use lib::database::Database;
use lib::query_parser::config::KeyValueStore;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::query_exec_engine::QueryExecEngine;
use lib::storage_engine::config::{StorageConfig as LibStorageConfig, StorageEngineType as LibStorageEngineType};
use lib::storage_engine::storage_engine::{StorageEngineManager, AsyncStorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};

/// GraphDB Command Line Interface
#[derive(Parser, Debug)]
#[clap(author, version, about = "GraphDB Command Line Interface", long_about = None)]
#[clap(propagate_version = true)]
pub struct CliArgs {
    #[clap(subcommand)]
    pub command: Option<Commands>,
    #[clap(long, short = 'c')]
    pub cli: bool,
    #[clap(long)]
    pub enable_plugins: bool,
    #[clap(long, short = 'q')]
    pub query: Option<String>,
    #[clap(long, hide = true)]
    pub internal_rest_api_run: bool,
    #[clap(long, hide = true)]
    pub internal_storage_daemon_run: bool,
    #[clap(long, hide = true)]
    pub internal_daemon_run: bool,
    #[clap(long, hide = true)]
    pub internal_port: Option<u16>,
    #[clap(long, hide = true)]
    pub internal_storage_config_path: Option<PathBuf>,
    #[clap(long, hide = true)]
    pub internal_storage_engine: Option<StorageEngineType>,
    #[clap(long, hide = true)]
    pub internal_data_directory: Option<PathBuf>,
    #[clap(long, hide = true)]
    pub internal_cluster_range: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Start {
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the daemon. Conflicts with --daemon-port if both specified.")]
        port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the daemon. Conflicts with --daemon-cluster if both specified.")]
        cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the daemon (synonym for --port).")]
        daemon_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the daemon (synonym for --cluster).")]
        daemon_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Listen port for the REST API.")]
        listen_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the REST API. Conflicts with --listen-port if both specified.")]
        rest_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the REST API.")]
        rest_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the Storage Daemon. Synonym for --port in `start storage`.")]
        storage_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the Storage Daemon. Synonym for --cluster in `start storage`.")]
        storage_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(PathBuf), help = "Path to the Storage Daemon configuration file.")]
        storage_config: Option<PathBuf>,
        #[clap(subcommand)]
        action: Option<StartAction>,
    },
    Stop(StopArgs),
    Status(StatusArgs),
    #[clap(subcommand)]
    Daemon(DaemonCliCommand),
    #[clap(subcommand)]
    Rest(RestCliCommand),
    #[clap(subcommand)]
    Storage(StorageAction),
    #[clap(subcommand)]
    Use(UseAction),
    #[clap(subcommand)]
    Save(SaveAction),
    Reload(ReloadArgs),
    Restart(RestartArgs),
    Interactive,
    Auth { username: String, password: String },
    Authenticate { username: String, password: String },
    Register { username: String, password: String },
    Version,
    Health,
    Help(HelpArgs),
    Clear,
    Exit,
    Quit,
    Show {
        #[clap(subcommand)]
        action: ShowAction,
    },
    Exec {
        #[arg(long, help = "Command to execute on the storage engine")]
        command: String,
    },
    Query {
        #[arg(long, help = "Query to execute on the storage engine")]
        query: String,
    },
    Kv {
        #[arg(value_parser = parse_kv_operation, help = "Key-value operation (e.g., get, set, delete)")]
        operation: String,
        #[arg(name = "KEY", help = "Key for the key-value operation")]
        key: Option<String>,
        #[arg(name = "VALUE", help = "Value for the key-value operation (required for set)", required_if_eq("operation", "set"))]
        value: Option<String>,
    },
}

// Use a OnceCell to manage the singleton instance of the QueryExecEngine.
static QUERY_ENGINE_SINGLETON: OnceCell<Arc<QueryExecEngine>> = OnceCell::const_new();



// Correct Solution: Remove Sync requirement and fix parameter types

fn start_wrapper(
    port: Option<u16>,
    config_path: Option<PathBuf>,
    storage_config: Option<CliStorageConfig>, // This is the same as CliStorageConfig (alias)
    engine_name: Option<String>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
    // No conversion needed since CliStorageConfig is an alias to StorageConfig
    Box::pin(start_storage_interactive(
        port,
        config_path,
        storage_config, // Pass directly, no conversion needed
        engine_name,
        shutdown_tx,
        handle,
        port_arc,
    ))
}

fn stop_wrapper(
    port: Option<u16>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
    Box::pin(stop_storage_interactive(
        port,
        shutdown_tx,
        handle,
        port_arc,
    ))
    // Removed the cast to Send + Sync - just return Send
}

pub async fn get_query_engine_singleton() -> Result<&'static Arc<QueryExecEngine>> {
    initialize_storage_for_query(start_wrapper, stop_wrapper).await?;
    
    QUERY_ENGINE_SINGLETON.get_or_try_init(|| async {
        let storage_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        let cli_storage_config = if storage_config_path.exists() {
            println!("Loading storage config from {}", storage_config_path.display());
            load_storage_config_from_yaml(Some(storage_config_path.clone()))
                .with_context(|| format!("Failed to load storage config from {}", storage_config_path.display()))?
        } else {
            println!("No storage configuration file found at {}. Defaulting to InMemory storage. Use 'use storage <engine_name>' and 'save storage' to persist your configuration.", storage_config_path.display());
            CliStorageConfig::new_in_memory()
        };
        
        let lib_storage_config = map_cli_to_lib_storage_config(cli_storage_config);
        let database = Arc::new(
            Database::new(lib_storage_config)
                .await
                .map_err(|e| anyhow!("Failed to create Database: {}", e))?,
        );
        let kv_store = Arc::new(KeyValueStore::new());
        Ok(Arc::new(QueryExecEngine::new(database, kv_store)))
    }).await
}

// Maps CliStorageConfig to LibStorageConfig
pub fn map_cli_to_lib_storage_config(cli_config: CliStorageConfig) -> LibStorageConfig {
    let engine_specific_config = cli_config.engine_specific_config.as_ref().map(|selected| {
        let mut map = HashMap::new();
        
        // Insert storage_engine_type (snake_case only)
        map.insert(
            "storage_engine_type".to_string(),
            Value::String(selected.storage_engine_type.to_string().to_lowercase())
        );
        
        // Normalize path to exclude port number
        let engine_path_name = selected.storage_engine_type.to_string().to_lowercase();
        let data_dir_path = cli_config.data_directory.as_ref().map_or(
            PathBuf::from("/opt/graphdb/storage_data"),
            |p| p.clone()
        );
        let engine_data_path = data_dir_path.join(&engine_path_name);
        map.insert(
            "path".to_string(),
            Value::String(engine_data_path.to_string_lossy().to_string())
        );
        
        // Insert other fields
        if let Some(host) = &selected.storage.host {
            map.insert("host".to_string(), Value::String(host.clone()));
        }
        if let Some(port) = selected.storage.port {
            map.insert("port".to_string(), Value::Number(port.into()));
        }
        if let Some(username) = &selected.storage.username {
            map.insert("username".to_string(), Value::String(username.clone()));
        }
        if let Some(password) = &selected.storage.password {
            map.insert("password".to_string(), Value::String(password.clone()));
        }
        if let Some(database) = &selected.storage.database {
            map.insert("database".to_string(), Value::String(database.clone()));
        }
        if let Some(pd_endpoints) = &selected.storage.pd_endpoints {
            map.insert("pd_endpoints".to_string(), Value::String(pd_endpoints.clone()));
        }
        
        map
    });

    LibStorageConfig {
        config_root_directory: cli_config
            .config_root_directory
            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/config")),
        data_directory: cli_config
            .data_directory
            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data")),
        log_directory: cli_config
            .log_directory
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_else(|| "/opt/graphdb/logs".to_string()),
        default_port: cli_config.default_port,
        cluster_range: cli_config.cluster_range,
        max_disk_space_gb: cli_config.max_disk_space_gb,
        min_disk_space_gb: cli_config.min_disk_space_gb,
        use_raft_for_scale: cli_config.use_raft_for_scale,
        storage_engine_type: match cli_config.storage_engine_type {
            StorageEngineType::RocksDB => LibStorageEngineType::RocksDB,
            StorageEngineType::TiKV => LibStorageEngineType::TiKV,
            StorageEngineType::Sled => LibStorageEngineType::Sled,
            StorageEngineType::PostgreSQL => LibStorageEngineType::PostgreSQL,
            StorageEngineType::MySQL => LibStorageEngineType::MySQL,
            StorageEngineType::Redis => LibStorageEngineType::Redis,
            StorageEngineType::InMemory => LibStorageEngineType::InMemory,
        },
        engine_specific_config,
        max_open_files: Some(cli_config.max_open_files as i32),
        connection_string: match cli_config.storage_engine_type {
            StorageEngineType::PostgreSQL => Some("postgres://localhost:5432/graphdb".to_string()),
            StorageEngineType::MySQL => Some("mysql://localhost:3306/graphdb".to_string()),
            StorageEngineType::Redis => Some("redis://localhost:6379".to_string()),
            _ => Some("".to_string()),
        },
    }
}

// Re-usable function to handle all commands.
pub async fn run_single_command(
    command: Commands,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    // Initialize query engine for commands that need it
    let query_engine = match command {
        Commands::Exec { .. } | Commands::Query { .. } | Commands::Kv { .. } => {
            Some(get_query_engine_singleton().await?)
        }
        _ => None,
    };

    match command {
        Commands::Start {
            port: top_port,
            cluster: top_cluster,
            daemon_port: top_daemon_port,
            daemon_cluster: top_daemon_cluster,
            listen_port: top_listen_port,
            rest_port: top_rest_port,
            rest_cluster: top_rest_cluster,
            storage_port: top_storage_port,
            storage_cluster: top_storage_cluster,
            storage_config: top_storage_config,
            action,
        } => {
            let effective_action = match action {
                Some(StartAction::All {
                    port,
                    cluster,
                    daemon_port,
                    daemon_cluster,
                    listen_port,
                    rest_port,
                    rest_cluster,
                    storage_port,
                    storage_cluster,
                    storage_config,
                }) => StartAction::All {
                    port,
                    cluster,
                    daemon_port,
                    daemon_cluster,
                    listen_port,
                    rest_port,
                    rest_cluster,
                    storage_port,
                    storage_cluster,
                    storage_config,
                },
                Some(other_action) => other_action,
                None => {
                    if top_port.is_some()
                        || top_cluster.is_some()
                        || top_daemon_port.is_some()
                        || top_daemon_cluster.is_some()
                        || top_listen_port.is_some()
                        || top_rest_port.is_some()
                        || top_rest_cluster.is_some()
                    {
                        StartAction::All {
                            port: top_port,
                            cluster: top_cluster.clone(),
                            daemon_port: top_daemon_port,
                            daemon_cluster: top_daemon_cluster,
                            listen_port: top_listen_port,
                            rest_port: top_rest_port,
                            rest_cluster: top_rest_cluster,
                            storage_port: top_storage_port,
                            storage_cluster: top_storage_cluster,
                            storage_config: top_storage_config.clone(),
                        }
                    } else if top_storage_port.is_some()
                        || top_storage_cluster.is_some()
                        || top_storage_config.is_some()
                    {
                        StartAction::Storage {
                            port: top_storage_port,
                            cluster: top_storage_cluster.clone(),
                            config_file: top_storage_config.clone(),
                            storage_port: top_storage_port,
                            storage_cluster: top_storage_cluster,
                        }
                    } else {
                        StartAction::All {
                            port: None,
                            cluster: None,
                            daemon_port: None,
                            daemon_cluster: None,
                            listen_port: None,
                            rest_port: None,
                            rest_cluster: None,
                            storage_port: None,
                            storage_cluster: None,
                            storage_config: None,
                        }
                    }
                }
            };

            match effective_action {
                StartAction::All {
                    port,
                    cluster,
                    daemon_port,
                    daemon_cluster,
                    listen_port,
                    rest_port,
                    rest_cluster,
                    storage_port,
                    storage_cluster,
                    storage_config,
                } => {
                    handlers_mod::handle_start_all_interactive(
                        port.or(daemon_port),
                        cluster.or(daemon_cluster),
                        listen_port.or(rest_port),
                        rest_cluster,
                        storage_port,
                        storage_cluster,
                        storage_config,
                        daemon_handles.clone(),
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_port_arc.clone(),
                        rest_api_handle.clone(),
                        storage_daemon_shutdown_tx_opt.clone(),
                        storage_daemon_handle.clone(),
                        storage_daemon_port_arc.clone(),
                    )
                    .await?;
                }
                StartAction::Daemon {
                    port,
                    cluster,
                    daemon_port,
                    daemon_cluster,
                } => {
                    handlers_mod::handle_daemon_command_interactive(
                        DaemonCliCommand::Start {
                            port,
                            cluster,
                            daemon_port,
                            daemon_cluster,
                        },
                        daemon_handles.clone(),
                    )
                    .await?;
                }
                StartAction::Rest { port: rest_start_port, cluster: rest_start_cluster, rest_port, rest_cluster } => {
                    handlers_mod::handle_rest_command_interactive(
                        RestCliCommand::Start { port: rest_start_port, cluster: rest_start_cluster, rest_port, rest_cluster },
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_handle.clone(),
                        rest_api_port_arc.clone(),
                    ).await?;
                }
                StartAction::Storage {
                    port,
                    config_file,
                    cluster,
                    storage_port,
                    storage_cluster,
                } => {
                    handlers_mod::handle_storage_command_interactive(
                        StorageAction::Start {
                            port,
                            config_file,
                            cluster,
                            storage_port,
                            storage_cluster,
                        },
                        storage_daemon_shutdown_tx_opt.clone(),
                        storage_daemon_handle.clone(),
                        storage_daemon_port_arc.clone(),
                    )
                    .await?;
                }
            }
        }
        Commands::Stop(stop_args) => {
            handlers_mod::handle_stop_command(stop_args).await?;
        }
        Commands::Status(status_args) => {
            handlers_mod::handle_status_command(
                status_args,
                rest_api_port_arc.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
        }
        Commands::Use(action) => {
            match action {
                UseAction::Storage { engine, permanent } => {
                    handlers_mod::handle_use_storage_command(engine, permanent).await?;
                }
                UseAction::Plugin { enable } => {
                    let mut config = load_cli_config()?;
                    config.enable_plugins = enable;
                    config.save()?;
                    println!("Plugins {}", if enable { "enabled" } else { "disabled" });
                    handlers_mod::handle_show_plugins_command().await?;
                }
            }
        }
        Commands::Save(action) => {
            let mut config = load_cli_config()?;
            match action {
                SaveAction::Configuration => {
                    config.save()?;
                    println!("CLI configuration saved persistently");
                }
                SaveAction::Storage => {
                    if let Some(engine) = config.storage.storage_engine_type.clone() {
                        let engine_config_file = match engine {
                            StorageEngineType::RocksDB => DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB,
                            StorageEngineType::Sled => DEFAULT_STORAGE_CONFIG_PATH_SLED,
                            StorageEngineType::TiKV => DEFAULT_STORAGE_CONFIG_PATH_TIKV,
                            StorageEngineType::PostgreSQL => DEFAULT_STORAGE_CONFIG_PATH_POSTGRES,
                            StorageEngineType::MySQL => DEFAULT_STORAGE_CONFIG_PATH_MYSQL,
                            StorageEngineType::Redis => DEFAULT_STORAGE_CONFIG_PATH_REDIS,
                            StorageEngineType::InMemory => {
                                println!(
                                    "Storage configuration not saved: InMemory (no persistent config required)"
                                );
                                return Ok(());
                            }
                        };
                        let storage_config_path = PathBuf::from("/opt/graphdb/storage_data/config.yaml");
                        let storage_settings = if storage_config_path.exists() {
                            StorageSettings::load_from_yaml(&storage_config_path).with_context(|| {
                                format!(
                                    "Failed to load core config from {:?}",
                                    storage_config_path
                                )
                            })?
                        } else {
                            StorageSettings::default()
                        };

                        let selected_config = if PathBuf::from(engine_config_file).exists() {
                            SelectedStorageConfig::load_from_yaml(&PathBuf::from(
                                engine_config_file,
                            ))
                            .with_context(|| {
                                format!(
                                    "Failed to load config from {:?}",
                                    engine_config_file
                                )
                            })?
                        } else {
                            println!(
                                "Config file {:?} not found; using default storage-specific settings",
                                engine_config_file
                            );
                            SelectedStorageConfig::default()
                        };

                        let mut merged_settings = storage_settings;
                        merged_settings.storage_engine_type = engine.to_string();
                        if let Some(port) = selected_config.storage.port {
                            merged_settings.default_port = port;
                        }

                        let storage_settings_wrapper =
                            StorageSettingsWrapper { storage: merged_settings };
                        let content = serde_yaml::to_string(&storage_settings_wrapper)
                            .with_context(|| "Failed to serialize storage settings")?;
                        if let Some(parent) = storage_config_path.parent() {
                            fs::create_dir_all(parent).with_context(|| {
                                format!("Failed to create config directory {:?}", parent)
                            })?;
                        }
                        fs::write(&storage_config_path, content).with_context(|| {
                            format!(
                                "Failed to write storage config to {:?}",
                                storage_config_path
                            )
                        })?;
                        println!("Storage configuration saved persistently");
                    } else {
                        println!("No storage engine configured; nothing to save");
                    }
                }
            }
        }
        Commands::Reload(reload_args) => {
            handlers_mod::handle_reload_command_interactive(reload_args).await?;
        }
        Commands::Restart(restart_args) => {
            handlers_mod::handle_restart_command_interactive(
                restart_args,
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
        }
        Commands::Storage(storage_action) => {
            handlers_mod::handle_storage_command(storage_action).await?;
        }
        Commands::Daemon(daemon_cmd) => {
            handlers_mod::handle_daemon_command_interactive(daemon_cmd, daemon_handles.clone())
                .await?;
        }
        Commands::Rest(rest_cmd) => {
            handlers_mod::handle_rest_command_interactive(
                rest_cmd,
                rest_api_shutdown_tx_opt.clone(),
                rest_api_handle.clone(),
                rest_api_port_arc.clone(),
            )
            .await?;
        }
        Commands::Interactive => {
            // Handled by the main flow of start_cli()
        }
        Commands::Help(help_args) => {
            let mut cmd = CliArgs::command();
            if let Some(command_filter) = help_args.filter_command {
                help_display_mod::print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else if !help_args.command_path.is_empty() {
                let command_filter = help_args.command_path.join(" ");
                help_display_mod::print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else {
                help_display_mod::print_help_clap_generated();
            }
        }
        Commands::Auth { username, password } | Commands::Authenticate { username, password } => {
            handlers_mod::authenticate_user(username, password).await;
        }
        Commands::Register { username, password } => {
            handlers_mod::register_user(username, password).await;
        }
        Commands::Version => {
            handlers_mod::display_rest_api_version().await;
        }
        Commands::Health => {
            handlers_mod::display_rest_api_health().await;
        }
        Commands::Clear => {
            handlers_mod::clear_terminal_screen().await?;
            handlers_mod::print_welcome_screen();
        }
        Commands::Exit | Commands::Quit => {
            println!("Exiting CLI. Goodbye!");
            process::exit(0);
        }
        Commands::Show { action } => {
            match action {
                ShowAction::Storage => {
                    handlers_mod::handle_show_storage_command().await?;
                }
                ShowAction::Plugins => {
                    handlers_mod::handle_show_plugins_command().await?;
                }
                ShowAction::Config { config_type } => {
                    match config_type {
                        ConfigAction::All => {
                            handlers_mod::handle_show_all_config_command().await?;
                        }
                        ConfigAction::Rest => {
                            handlers_mod::handle_show_rest_config_command().await?;
                        }
                        ConfigAction::Storage => {
                            handlers_mod::handle_show_storage_config_command().await?;
                        }
                        ConfigAction::Main => {
                            handlers_mod::handle_show_main_config_command().await?;
                        }
                    }
                }
            }
        }
        Commands::Exec { command } => {
            handlers_mod::handle_exec_command(query_engine.unwrap().clone(), command).await?;
        }
        Commands::Query { query } => {
            handlers_mod::handle_query_command(query_engine.unwrap().clone(), query).await?;
        }
        Commands::Kv { operation, key, value } => {
            match parse_kv_operation(&operation) {
                Ok(op) => {
                    match op.as_str() {
                        "get" => {
                            if let Some(key) = key {
                                handlers_mod::handle_kv_command(query_engine.unwrap().clone(), op, key, None)
                                    .await?;
                            } else {
                                return Err(anyhow!("Missing key for 'kv get' command. Usage: kv get <key> or kv get --key <key>"));
                            }
                        }
                        "set" => {
                            match (key, value) {
                                (Some(key), Some(value)) => {
                                    handlers_mod::handle_kv_command(query_engine.unwrap().clone(), op, key, Some(value))
                                        .await?;
                                }
                                (Some(_), None) => {
                                    return Err(anyhow!("Missing value for 'kv set' command. Usage: kv set <key> <value> or kv set --key <key> --value <value>"));
                                }
                                _ => {
                                    return Err(anyhow!("Missing key for 'kv set' command. Usage: kv set <key> <value> or kv set --key <key> --value <value>"));
                                }
                            }
                        }
                        "delete" => {
                            if let Some(key) = key {
                                handlers_mod::handle_kv_command(query_engine.unwrap().clone(), op, key, None)
                                    .await?;
                            } else {
                                return Err(anyhow!("Missing key for 'kv delete' command. Usage: kv delete <key> or kv delete --key <key>"));
                            }
                        }
                        _ => {
                            return Err(anyhow!("Invalid KV operation: '{}'. Supported operations: get, set, delete", operation));
                        }
                    }
                }
                Err(e) => {
                    return Err(anyhow!("{}", e));
                }
            }
        }
    }
    Ok(())
}

/// Main entry point for CLI command handling.
///
/// This function now correctly prioritizes command-line arguments over
/// configuration file settings, and refactors the logic for better readability.
pub async fn start_cli() -> Result<()> {
    let args_vec: Vec<String> = env::args().collect();
    if args_vec.len() > 1 && args_vec[1].to_lowercase() == "help" {
        let help_command_args: Vec<String> = args_vec.into_iter().skip(2).collect();
        let command_filter = if help_command_args.is_empty() {
            "".to_string()
        } else {
            help_command_args.join(" ")
        };
        let mut cmd = CliArgs::command();
        help_display_mod::print_filtered_help_clap_generated(&mut cmd, &command_filter);
        process::exit(0);
    }

    // Parse command-line arguments first.
    let mut args = CliArgs::parse();

    // Handle internal daemon runs. This logic should be executed before
    // attempting to load the configuration, as it's a special internal mode.
    if args.internal_rest_api_run || args.internal_storage_daemon_run || args.internal_daemon_run {
        let converted_storage_engine = args.internal_storage_engine.map(|se_cli| se_cli.into());
        return daemon_management::handle_internal_daemon_run(
            args.internal_rest_api_run,
            args.internal_storage_daemon_run,
            args.internal_port,
            args.internal_storage_config_path,
            converted_storage_engine,
        ).await;
    }

    // Note: The CLI configuration is no longer loaded eagerly.
    // It will be loaded lazily by the individual command handlers as needed.

    // Handle direct query execution.
    if let Some(query_string) = args.query {
        println!("Executing direct query: {}", query_string);
        match parse_query_from_string(&query_string) {
            Ok(parsed_query) => match parsed_query {
                QueryType::Cypher => println!("  -> Identified as Cypher query."),
                QueryType::SQL => println!("  -> Identified as SQL query."),
                QueryType::GraphQL => println!("  -> Identified as GraphQL query."),
            },
            Err(e) => eprintln!("Error parsing query: {}", e),
        }
        return Ok(());
    }

    let should_enter_interactive_mode = args.cli || args.command.is_none();

    let daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(TokioMutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>> = Arc::new(TokioMutex::new(None));
    let rest_api_port_arc: Arc<TokioMutex<Option<u16>>> = Arc::new(TokioMutex::new(None));
    let rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>> = Arc::new(TokioMutex::new(None));
    let storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>> = Arc::new(TokioMutex::new(None));
    let storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>> = Arc::new(TokioMutex::new(None));
    let storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>> = Arc::new(TokioMutex::new(None));

    // Execute a single command if one was provided.
    if let Some(command) = args.command {
        run_single_command(
            command,
            daemon_handles.clone(),
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
            storage_daemon_shutdown_tx_opt.clone(),
            storage_daemon_handle.clone(),
            storage_daemon_port_arc.clone(),
        ).await?;

        // If not in interactive mode, exit after running the command.
        if !should_enter_interactive_mode {
            return Ok(());
        }
    }

    // Enter interactive mode if the conditions are met.
    if should_enter_interactive_mode {
        interactive_mod::run_cli_interactive(
            daemon_handles.clone(),
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
            storage_daemon_shutdown_tx_opt.clone(),
            storage_daemon_handle.clone(),
            storage_daemon_port_arc.clone(),
            get_query_engine_singleton().await?.clone(),
        ).await?;
    }

    Ok(())
}

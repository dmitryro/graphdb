use clap::{Parser, CommandFactory};
use anyhow::{Result, Context, anyhow};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{Mutex as TokioMutex, oneshot};
use tokio::task::JoinHandle;
use std::process;
use std::env;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use serde_yaml2 as serde_yaml;
use std::fs;
use lib::daemon::storage_daemon_server::{StorageSettings, StorageSettingsWrapper};
use log::{info, debug, warn, error};
use models::errors::GraphError;
use tokio::time::{timeout, Duration as TokioDuration};
use std::future::Future;
// Import modules
use lib::commands::{
    parse_kv_operation, ConfigAction, DaemonCliCommand, HelpArgs, ReloadAction, RestartAction,
    RestCliCommand, SaveAction, ShowAction, StartAction, StatusAction, StopAction, StorageAction,
    StatusArgs, StopArgs, ReloadArgs, RestartArgs, UseAction, MigrateAction, GraphAction, IndexAction,
};
use lib::config::{
    self, load_storage_config_from_yaml, SelectedStorageConfig, StorageConfig,
    StorageConfigInner, StorageEngineType, DEFAULT_STORAGE_CONFIG_PATH_MYSQL,
    DEFAULT_STORAGE_CONFIG_PATH_POSTGRES, DEFAULT_STORAGE_CONFIG_PATH_REDIS,
    DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB, DEFAULT_STORAGE_CONFIG_PATH_SLED,
    DEFAULT_STORAGE_CONFIG_PATH_TIKV, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_STORAGE_CONFIG_PATH_HYBRID,
    load_cli_config
};
use lib::config as config_mod;
use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::cli::daemon_management;
use crate::cli::handlers as handlers_mod;
use crate::cli::handlers_storage::{ start_storage_interactive, stop_storage_interactive };
use crate::cli::handlers_utils::{
    START_STORAGE_FN_SINGLETON,
    STOP_STORAGE_FN_SINGLETON,
    parse_storage_engine, 
    handle_internal_daemon_run,
    StartStorageFn,
    StopStorageFn,
    adapt_start_storage,
    adapt_stop_storage,
};

use crate::cli::help_display as help_display_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::handlers_queries::{
    initialize_storage_for_query,
    handle_cypher_query,
    handle_sql_query,
    handle_graphql_query,
};

pub use crate::cli::handlers_graph::handle_graph_command;
pub use crate::cli::handlers_index::{ self, handle_index_command, initialize_storage_for_index };
use lib::database::Database;
use lib::query_parser::config::KeyValueStore;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::query_exec_engine::QueryExecEngine;
use lib::storage_engine::storage_engine::{StorageEngineManager, AsyncStorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};

/// Detect query language from query content (auto-detection)
fn detect_query_language(query: &str) -> &'static str {
    let trimmed = query.trim_start();
    if trimmed.is_empty() {
        return "unknown";
    }

    let prefix: String = trimmed.chars().take(15).collect::<String>().to_uppercase();

    // SQL
    if prefix.starts_with("SELECT")
        || prefix.starts_with("INSERT")
        || prefix.starts_with("UPDATE")
        || prefix.starts_with("DELETE")
        || prefix.starts_with("WITH")
        || prefix.starts_with("CREATE TABLE")
        || prefix.starts_with("ALTER TABLE")
    {
        return "sql";
    }

    // Cypher
    if prefix.starts_with("MATCH")
        || prefix.starts_with("CREATE")
        || prefix.starts_with("MERGE")
        || prefix.starts_with("RETURN")
        || prefix.starts_with("OPTIONAL MATCH")
        || prefix.starts_with("UNWIND")
        || prefix.starts_with("CALL")
    {
        return "cypher";
    }

    // GraphQL
    if trimmed.starts_with('{') 
        || trimmed.contains("\"query\"") 
        || trimmed.contains("\"mutation\"")
        || trimmed.contains("query ") 
        || trimmed.contains("mutation ")
    {
        return "graphql";
    }

    "unknown"
}

/// Sanitize only Cypher queries — preserve SQL and GraphQL
fn sanitize_cypher_query(input: &str) -> String {
    let trimmed = input.trim_start();
    let upper = trimmed.to_uppercase();

    // Only sanitize if it looks like Cypher
    if !(upper.starts_with("MATCH")
        || upper.starts_with("CREATE")
        || upper.starts_with("MERGE")
        || upper.starts_with("RETURN")
        || upper.starts_with("OPTIONAL MATCH")) {
        return input.to_string();
    }

    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_pattern = false;
    let mut paren_depth = 0;

    while let Some(c) = chars.next() {
        match c {
            '(' => {
                paren_depth += 1;
                in_pattern = true;
                result.push(c);
            }
            ')' => {
                paren_depth -= 1;
                result.push(c);
                if paren_depth == 0 {
                    in_pattern = false;
                }
            }
            _ if c.is_whitespace() && !in_pattern => {
                while chars.peek().map(|ch| ch.is_whitespace()).unwrap_or(false) {
                    chars.next();
                }
                if let Some(&next) = chars.peek() {
                    if next.is_ascii_alphabetic() && next.is_uppercase() {
                        result.push(' ');
                        continue;
                    }
                }
                result.push(' ');
            }
            _ => result.push(c),
        }
    }
    result.trim().to_string()
}

#[derive(Parser, Debug)]
#[clap(author, version, about = "GraphDB Command Line Interface", long_about = None)]
#[clap(propagate_version = true)]
pub struct CliArgs {
    /// Cypher/SQL/GraphQL query (positional or via -q) — language auto-detected if not specified
    #[clap(value_name = "QUERY", trailing_var_arg = true)]
    pub query: Vec<String>,
    /// Execute query (explicit)
    #[clap(long, short = 'q', value_name = "QUERY")]
    pub explicit_query: Option<String>,
    /// Run in interactive mode
    #[clap(long, short = 'c', action = clap::ArgAction::SetTrue)]
    pub cli: bool,
    /// Enable experimental plugins
    #[clap(long)]
    pub enable_plugins: bool,
    // Internal flags
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
    #[clap(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Debug, Clone, clap::Subcommand)]
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
    #[clap(name = "query", alias = "q", alias = "e", alias = "exec", alias = "unified")]
    Query {
        #[arg(value_name = "QUERY")]
        query: String,
        #[arg(long, short = 'l', value_name = "LANG", help = "Query language: sql, cypher, graphql (auto-detected if omitted)")]
        language: Option<String>,
    },
    Kv {
        #[arg(value_parser = parse_kv_operation, help = "Key-value operation (e.g., get, set, delete)")]
        operation: String,
        #[arg(name = "KEY", help = "Key for the key-value operation")]
        key: Option<String>,
        #[arg(name = "VALUE", help = "Value for the key-value operation (required for set)", required_if_eq("operation", "set"))]
        value: Option<String>,
    },
    Set {
        #[arg(name = "KEY", help = "Key to set")]
        key: String,
        #[arg(name = "VALUE", help = "Value to set")]
        value: String,
    },
    Get {
        #[arg(name = "KEY", help = "Key to retrieve")]
        key: String,
    },
    Delete {
        #[arg(name = "KEY", help = "Key to delete")]
        key: String,
    },
    Migrate(MigrateAction),
    
    /// Graph domain actions: insert person, medical records, delete, load
    #[clap(subcommand)]
    Graph(GraphAction),

    /// Full-text search and index management
    #[clap(subcommand)]
    Index(IndexAction),

}

// Use a TokioMutex to manage the singleton instance of the QueryExecEngine.
static QUERY_ENGINE_SINGLETON: TokioMutex<Option<Arc<QueryExecEngine>>> = TokioMutex::const_new(None);

fn start_wrapper(
    port: Option<u16>,
    config_path: Option<PathBuf>,
    storage_config: Option<StorageConfig>,
    engine_name: Option<String>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
    Box::pin(async move {
        let result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("Failed to create Tokio runtime for start_storage_interactive")?;
            rt.block_on(start_storage_interactive(
                port,
                config_path,
                storage_config,
                engine_name,
                shutdown_tx,
                handle,
                port_arc,
            ))
        })
        .await
        .context("Spawned task for start_storage_interactive panicked")??;
        Ok(result)
    })
}

fn stop_wrapper(
    port: Option<u16>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
    Box::pin(async move {
        let result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("Failed to create Tokio runtime for stop_storage_interactive")?;
            rt.block_on(stop_storage_interactive(
                port,
                shutdown_tx,
                handle,
                port_arc,
            ))
        })
        .await
        .context("Spawned task for stop_storage_interactive panicked")??;
        Ok(result)
    })
}

/// This function implements the singleton pattern for the QueryExecEngine.
/// It ensures that the QueryExecEngine is initialized only once, even with concurrent access.
pub async fn get_query_engine_singleton() -> Result<Arc<QueryExecEngine>> {
    let mutex_timeout = TokioDuration::from_secs(5);
    let mut singleton_guard = timeout(mutex_timeout, QUERY_ENGINE_SINGLETON.lock())
        .await
        .map_err(|_| anyhow!("Failed to acquire query engine singleton mutex after {} seconds", mutex_timeout.as_secs()))?;
    if let Some(engine) = singleton_guard.as_ref() {
        info!("Query engine singleton already initialized, returning existing instance");
        println!("===> Query engine singleton already initialized, returning existing instance");
        return Ok(Arc::clone(engine));
    }
    drop(singleton_guard);
    let query_engine = async move {
        info!("Starting query engine singleton initialization...");
        println!("===> Starting query engine singleton initialization...");
        info!("Calling initialize_storage_for_query with a timeout...");
        println!("===> Calling initialize_storage_for_query with a timeout...");
        timeout(TokioDuration::from_secs(60), initialize_storage_for_query(start_wrapper, stop_wrapper)).await
            .context("Storage initialization timed out after 60 seconds")?
            .context("Failed to initialize storage for query execution")?;
        let storage_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        info!("Loading storage config from {}", storage_config_path.display());
        println!("===> Loading storage config from {}", storage_config_path.display());
        let storage_config = if storage_config_path.exists() {
            timeout(TokioDuration::from_secs(10), load_storage_config_from_yaml(Some(storage_config_path.clone()))).await
                .context("Loading storage config timed out after 10 seconds")?
                .with_context(|| format!("Failed to load storage config from {}", storage_config_path.display()))?
        } else {
            info!("No storage configuration file found at {}. Defaulting to InMemory storage.", storage_config_path.display());
            println!("===> No storage configuration file found at {}. Defaulting to InMemory storage. Use 'use storage <engine_name>' and 'save storage' to persist your configuration.", storage_config_path.display());
            StorageConfig::new_in_memory()
        };
        info!("Creating new Database instance...");
        println!("===> Creating new Database instance...");
        let database = timeout(TokioDuration::from_secs(60), Database::new(storage_config)).await
            .context("Database creation timed out after 60 seconds")?
            .map_err(|e| anyhow!("Failed to create Database: {}", e))?;
        info!("Creating new QueryExecEngine instance...");
        println!("===> Creating new QueryExecEngine instance...");
        let query_engine = Arc::new(QueryExecEngine::new(Arc::new(database)));
        Ok::<Arc<QueryExecEngine>, anyhow::Error>(query_engine)
    }.await?;
    let mut singleton_guard = QUERY_ENGINE_SINGLETON.lock().await;
    info!("Storing query engine in singleton...");
    println!("===> Storing query engine in singleton...");
    *singleton_guard = Some(Arc::clone(&query_engine));
    info!("Query engine singleton initialization completed.");
    println!("===> Query engine singleton initialization completed.");
    Ok(query_engine)
}

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
    let is_interactive = matches!(command, Commands::Interactive) || env::var("GRAPHDB_CLI_INTERACTIVE").as_deref() == Ok("1");
    let mut env_var_set = false;
    if is_interactive && env::var("GRAPHDB_CLI_INTERACTIVE").is_err() {
        debug!("Setting GRAPHDB_CLI_INTERACTIVE=1 for command: {:?}", command);
        println!("===> Set GRAPHDB_CLI_INTERACTIVE=1 for command: {:?}", command);
        unsafe { env::set_var("GRAPHDB_CLI_INTERACTIVE", "1"); }
        env_var_set = true;
    }
    info!("Running command: {:?}", command);
    println!("===> Running command: {:?}", command);
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
                UseAction::Storage { engine, permanent, migrate } => {
                    handlers_mod::handle_use_storage_command(engine, permanent, migrate).await?;
                }
                UseAction::Plugin { enable } => {
                    let mut config = load_cli_config().await?;
                    config.enable_plugins = enable;
                    config.save()?;
                    println!("Plugins {}", if enable { "enabled" } else { "disabled" });
                    handlers_mod::handle_show_plugins_command().await?;
                }
            }
        }
        Commands::Save(action) => {
            let mut config = load_cli_config().await?;
            match action {
                SaveAction::Configuration => {
                    config.save()?;
                    println!("CLI configuration saved persistently");
                }
                SaveAction::Storage => {
                    if let Some(engine) = config.storage.storage_engine_type.clone() {
                        let engine_config_file = match engine {
                            StorageEngineType::Hybrid => DEFAULT_STORAGE_CONFIG_PATH_HYBRID,
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
        Commands::Interactive => {}
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
        Commands::Query { query, language } => {
            let raw_query = query.trim().to_string();
            let detected_lang = if language.is_none() {
                detect_query_language(&raw_query)
            } else {
                ""
            };

            let effective_lang = language
                .as_deref()
                .unwrap_or(detected_lang)
                .trim()
                .to_lowercase();

            let query_to_execute = if effective_lang == "cypher" {
                sanitize_cypher_query(&raw_query)
            } else {
                raw_query.clone()
            };

            info!(
                "Executing query: '{}', language: {} (detected: {}, explicit: {:?})",
                query_to_execute, effective_lang, detected_lang, language
            );
            println!(
                "===> Executing query: '{}', language: {}",
                query_to_execute, effective_lang
            );

            let query_engine = get_query_engine_singleton().await?;

            match effective_lang.as_str() {
                "cypher" => handle_cypher_query(query_engine, query_to_execute).await?,
                "sql" => handle_sql_query(query_engine, query_to_execute).await?,
                "graphql" => handle_graphql_query(query_engine, query_to_execute).await?,
                "unknown" | "" => {
                    return Err(anyhow!(
                        "Could not detect query language. Use --language sql|cypher|graphql"
                    ));
                }
                _ => {
                    return Err(anyhow!(
                        "Unsupported query language: '{}'. Use sql, cypher, or graphql",
                        effective_lang
                    ));
                }
            }
        }
        Commands::Kv { operation, key, value } => {
            info!("Executing KV command: operation={}, key={:?}, value={:?}", operation, key, value);
            println!("===> Executing KV command: operation={}, key={:?}, value={:?}", operation, key, value);
            let query_engine = get_query_engine_singleton().await?;
            match parse_kv_operation(&operation) {
                Ok(op) => {
                    match op.as_str() {
                        "get" => {
                            if let Some(key) = key {
                                handlers_mod::handle_kv_command(query_engine, op, key, None).await?;
                            } else {
                                return Err(anyhow!("Missing key for 'kv get' command. Usage: kv get <key> or kv get --key <key>"));
                            }
                        }
                        "set" => {
                            match (key, value) {
                                (Some(key), Some(value)) => {
                                    handlers_mod::handle_kv_command(query_engine, op, key, Some(value)).await?;
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
                                handlers_mod::handle_kv_command(query_engine, op, key, None).await?;
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
        Commands::Set { key, value } => {
            info!("Executing Set command: key={}, value={}", key, value);
            println!("===> Executing Set command: key={}, value={}", key, value);
            let query_engine = get_query_engine_singleton().await?;
            handlers_mod::handle_kv_command(query_engine, "set".to_string(), key, Some(value)).await?;
        }
        Commands::Get { key } => {
            info!("Executing Get command: key={}", key);
            println!("===> Executing Get command: key={}", key);
            let query_engine = get_query_engine_singleton().await?;
            handlers_mod::handle_kv_command(query_engine, "get".to_string(), key, None).await?;
        }
        Commands::Delete { key } => {
            info!("Executing Delete command: key={}", key);
            println!("===> Executing Delete command: key={}", key);
            let query_engine = get_query_engine_singleton().await?;
            handlers_mod::handle_kv_command(query_engine, "delete".to_string(), key, None).await?;
        }
        Commands::Migrate(action) => {
            let from_engine = action.from
                .or(action.source)
                .or(action.from_engine_pos)
                .ok_or_else(|| anyhow!("No source engine specified for 'migrate' command. Usage: migrate --from <engine> --to <engine> or migrate <from_engine> <to_engine>"))?;
            let to_engine = action.to
                .or(action.dest)
                .or(action.to_engine_pos)
                .ok_or_else(|| anyhow!("No destination engine specified for 'migrate' command. Usage: migrate --from <engine> --to <engine> or migrate <from_engine> <to_engine>"))?;
            info!("Executing Migrate command: from_engine={:?}, to_engine={:?}", from_engine, to_engine);
            println!("===> Executing Migrate command: from_engine={:?}, to_engine={:?}", from_engine, to_engine);
            handlers_mod::handle_migrate_interactive(
                from_engine,
                to_engine,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;
        }
        // === NEW: Graph Domain Commands ===
        Commands::Graph(action) => {
            info!("Executing graph domain command: {:?}", action);
            println!("===> Executing graph domain command: {:?}", action);
            let engine = get_query_engine_singleton().await?;
            handle_graph_command(engine, action).await?;
        }

        // === NEW: Index & Full-Text Search Commands ===
        // === NEW: Index & Full-Text Search Commands ===
        // === Index & Full-Text Search Commands ===
        // === Index & Full-Text Search Commands ===
        Commands::Index(action) => {
            info!("Executing index command: {:?}", action);
            println!("===> Executing index command: {:?}", action);

            // 1. Ensure a Storage Daemon is running, initializing the client internally.
            // NOTE: The function is called without arguments, keeping the cli.rs contract intact.
            let daemon_port = handlers_index::initialize_storage_for_index()
                .await
                .context("Indexing command requires a running and healthy storage daemon.")?;
            
            // 2. Execute the command handler.
            handlers_index::handle_index_command(action).await?;

            info!("Index command executed successfully on port {}", daemon_port);
            println!("===> Index command completed successfully.");
        }
    }
    if env_var_set {
        info!("Removing GRAPHDB_CLI_INTERACTIVE after command execution");
        println!("===> Removed GRAPHDB_CLI_INTERACTIVE after command execution");
        unsafe { env::remove_var("GRAPHDB_CLI_INTERACTIVE"); }
    }
    info!("Command execution completed successfully");
    println!("===> Command execution completed successfully");
    Ok(())
}

/// Main entry point for CLI command handling.
pub async fn start_cli() -> Result<()> {
    // --- REQUIRED INITIALIZATION STEP ---
    // Set the global function pointers before running any command.
    START_STORAGE_FN_SINGLETON
        // FIX: Use the adapter function which returns the correct Pin<Box<dyn Future>> type
        .set(adapt_start_storage as StartStorageFn)
        .expect("Failed to set StartStorageFn singleton.");
        
    STOP_STORAGE_FN_SINGLETON
        // FIX: Use the adapter function which returns the correct Pin<Box<dyn Future>> type
        .set(adapt_stop_storage as StopStorageFn)
        .expect("Failed to set StopStorageFn singleton.");

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
    let args = CliArgs::parse();
    if args.internal_rest_api_run || args.internal_storage_daemon_run || args.internal_daemon_run {
        let converted_storage_engine = args.internal_storage_engine.map(|se_cli| se_cli.into());
        return handle_internal_daemon_run(
            args.internal_rest_api_run,
            args.internal_storage_daemon_run,
            args.internal_port,
            args.internal_storage_config_path,
            converted_storage_engine,
        ).await;
    }
    let daemon_handles = Arc::new(TokioMutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt = Arc::new(TokioMutex::new(None));
    let rest_api_port_arc = Arc::new(TokioMutex::new(None));
    let rest_api_handle = Arc::new(TokioMutex::new(None));
    let storage_daemon_shutdown_tx_opt = Arc::new(TokioMutex::new(None));
    let storage_daemon_handle = Arc::new(TokioMutex::new(None));
    let storage_daemon_port_arc = Arc::new(TokioMutex::new(None));
    // Resolve query from any source
    let raw_query = if let Some(q) = args.explicit_query {
        Some(q)
    } else if !args.query.is_empty() {
        Some(args.query.join(" "))
    } else {
        None
    };
    let should_enter_interactive = args.cli || (args.command.is_none() && raw_query.is_none());
    if let Some(query) = raw_query {
        let cmd = Commands::Query { query, language: None };
        run_single_command(
            cmd,
            daemon_handles.clone(),
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
            storage_daemon_shutdown_tx_opt.clone(),
            storage_daemon_handle.clone(),
            storage_daemon_port_arc.clone(),
        ).await?;
        if !should_enter_interactive {
            return Ok(());
        }
    }
    if let Some(cmd) = args.command {
        run_single_command(
            cmd,
            daemon_handles.clone(),
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
            storage_daemon_shutdown_tx_opt.clone(),
            storage_daemon_handle.clone(),
            storage_daemon_port_arc.clone(),
        ).await?;
        if !should_enter_interactive {
            return Ok(());
        }
    }
    if should_enter_interactive {
        interactive_mod::run_cli_interactive(
            daemon_handles.clone(),
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
            storage_daemon_shutdown_tx_opt.clone(),
            storage_daemon_handle.clone(),
            storage_daemon_port_arc.clone(),
        ).await?;
    }
    Ok(())
}

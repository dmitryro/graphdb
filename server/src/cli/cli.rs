// server/src/cli/cli.rs
// Refactored: 2025-07-04 - Updated to use new storage engine names and removed obsolete imports.
// Fixed: 2025-07-04 - Corrected argument types for handle_restart_command_interactive.
// Fixed: 2025-07-04 - Corrected function name for run_cli_interactive.
// Fixed: 2025-07-04 - Resolved Mutex type mismatches and argument wrapping for handlers.
// Reverted: 2025-07-04 - Reverted to user-provided base code structure, re-applying necessary fixes.
// Fixed: 2025-07-04 - Corrected argument count for run_cli_interactive calls.
// FIXED: 2025-07-27 - Removed unused imports in interactive.rs.
// FIXED: 2025-07-27 - Corrected type mismatch for storage_config in StartAction::All.
// FIXED: 2025-07-27 - Corrected handle_reload_command call to use handle_reload_command_interactive.
// FIXED: 2025-07-27 - Corrected type mismatch for storage_config by converting to PathBuf for StartAction::All.
// FIXED: 2025-07-27 - Corrected argument passing for handle_start_all_interactive and handle_rest_command_interactive.
// FIXED: 2025-07-27 - Corrected handle_reload_command_interactive argument count and types.
// FIXED: 2025-07-27 - Corrected handle_internal_daemon_run import and call path.
// UPDATED: 2025-07-28 - Synchronized with latest commands.rs and interactive.rs changes.
// FIXED: 2025-07-28 - Removed unused FromArgMatches import from clap.
// FIXED: 2025-07-29 - Added missing internal arguments to CliArgs struct and fixed handle_internal_daemon_run call.
// FIXED: 2025-07-30 - Adjusted StartAction::Storage to use `--port` instead of `--storage-port` to resolve argument conflict with top-level 'start' command.
// FIXED: 2025-07-30 - Removed redundant top-level storage_port, storage_cluster, and storage_config from Commands::Start.
// RESTORED: 2025-07-30 - Restored missing internal flags in CliArgs and corrected handle_internal_daemon_run arguments.
// RESTORED: 2025-07-30 - Restored top-level arguments for Commands::Start and refined implicit action logic.
// REVERTED: 2025-07-30 - Reverted `handle_internal_daemon_run` call to 5 arguments and removed corresponding `CliArgs` fields.
// FIXED: 2025-07-31 - Added missing fields `storage_port`, `storage_cluster` to StartAction::Storage initializer (line 271).
// FIXED: 2025-07-31 - Added missing fields `daemon_port`, `daemon_cluster`, `rest_port`, `rest_cluster`, `storage_port`, `storage_cluster` to patterns and initializers for StartAction::Daemon, StartAction::Rest, StartAction::Storage (lines 325, 331, 339, 327, 333, 341).
// FIXED: 2025-07-31 - Corrected `filter_command` to `command_filter` in Commands::Help block (line 404) to resolve E0425.
// FIXED: 2025-08-05 - Modified effective_action logic to prioritize StartAction::All when non-storage-specific arguments (e.g., --cluster, --listen-port) are present alongside --storage-port, fixing incorrect `Cannot specify both --port and --storage-port` error.
// FIXED: 2025-08-06 - Refactored to execute commands before entering interactive mode, addressing the request to not exit after `start` command.
// FIXED: 2025-08-08 - Used `serde_yaml2` and `fs` as per user request. Updated to access `storage_engine_type` via `config.storage` and added `enable_plugins` to `CliConfigToml` in `config.rs`. Implemented `save` method for `CliConfigToml`.
// UPDATED: 2025-08-08 - Modified `UseAction::Storage` to use custom `parse_storage_engine` for `engine` argument, supporting `sled`, `rocksdb`, `rocks-db`, `inmemory`, `in-memory`, `redis`, `postgres`, `postgresql`, `postgre-sql`, `mysql`, `my-sql`.
// FIXED: 2025-08-08 - Corrected `Commands::Use` match in `run_single_command` to use struct variant syntax `Commands::Use { action }` to resolve E0164.
// FIXED: 2025-08-08 - Corrected `rank_port` to `rest_port` in `StartAction::All` pattern to resolve E0026.
// FIXED: 2025-08-08 - Removed mutable `storage_settings` in `UseAction::Storage` to fix warning.
// ADDED: 2025-08-08 - Introduced `SelectedStorageConfig` for storage-specific YAMLs, overriding core `StorageSettings` fields, and updated logging to handle both with masked password.
// FIXED: 2025-08-08 - Removed incorrect `cluster_range` reference in `SelectedStorageConfig` to resolve E0609.
// FIXED: 2025-08-08 - Corrected `StorageSettings` field types to handle `Option<T>` for `config_root_directory`, `data_directory`, `log_directory`, `cluster_range` to resolve E0308.
// FIXED: 2025-08-08 - Used `serde_yaml2` with `from_str` for YAML deserialization to resolve E0425 and E0432.
// FIXED: 2025-08-08 - Ensured `StorageSettings` fields are accessed as `Option<T>` in logging to resolve E0308.
// FIXED: 2025-08-08 - Updated `SelectedStorageConfig` to handle nested `storage` key in YAML files to fix RocksDB parsing error.
// FIXED: 2025-08-08 - Ensured `host` and `username` are logged for all storage engines when available.
// FIXED: 2025-08-08 - Corrected filename typo (`sorage` → `storage`) in config.rs constants.
// ADDED: 2025-08-09 - Added `Save` subcommand with `Configuration` and `Storage` variants to support `save configuration` and `save storage` commands.
// FIXED: 2025-08-09 - Removed `permanent` field from `Commands::Save` and `UseAction::Plugin` to fix `E0027` at `cli.rs:459`. Kept `config`/`configuration` aliases in `parse_storage_engine`.

use clap::{Parser, Subcommand, CommandFactory};
use anyhow::{Result, Context};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use std::process;
use std::env;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_yaml2;
use std::fs;
use toml;
use storage_daemon_server::StorageSettings;

// Import modules
use crate::cli::commands::{
    DaemonCliCommand, RestCliCommand, StorageAction, UseAction, SaveAction,
    StatusArgs, StopArgs, ReloadArgs, RestartArgs,
    ReloadAction, RestartAction, StartAction, StopAction, StatusAction,
    HelpArgs, ShowAction, ConfigAction,
};
use crate::cli::config::{
    self, DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB, DEFAULT_STORAGE_CONFIG_PATH_SLED,
    DEFAULT_STORAGE_CONFIG_PATH_POSTGRES, DEFAULT_STORAGE_CONFIG_PATH_MYSQL,
    DEFAULT_STORAGE_CONFIG_PATH_REDIS, SelectedStorageConfig, StorageConfigInner
};
use crate::cli::config as config_mod;
use crate::cli::handlers as handlers_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::help_display as help_display_mod;
use crate::cli::daemon_management;
use crate::cli::handlers_utils::{parse_storage_engine};
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::storage_engine::config::StorageEngineType;
use storage_daemon_server::{StorageSettingsWrapper};
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
}

// Re-usable function to handle all commands. This is called from both interactive and non-interactive modes.
// Re-usable function to handle all commands. This is called from both interactive and non-interactive modes.
pub async fn run_single_command(
    command: Commands,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
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
                    port, cluster, daemon_port, daemon_cluster,
                    listen_port, rest_port, rest_cluster,
                    storage_port, storage_cluster, storage_config
                }) => {
                    StartAction::All {
                        port, cluster, daemon_port, daemon_cluster,
                        listen_port, rest_port, rest_cluster,
                        storage_port, storage_cluster,
                        storage_config,
                    }
                },
                Some(other_action) => other_action,
                None => {
                    if top_port.is_some() || top_cluster.is_some() || top_daemon_port.is_some() || top_daemon_cluster.is_some() ||
                        top_listen_port.is_some() || top_rest_port.is_some() || top_rest_cluster.is_some() {
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
                    } else if top_storage_port.is_some() || top_storage_cluster.is_some() || top_storage_config.is_some() {
                        StartAction::Storage {
                            port: top_storage_port,
                            cluster: top_storage_cluster.clone(),
                            config_file: top_storage_config.clone(),
                            storage_port: top_storage_port,
                            storage_cluster: top_storage_cluster,
                        }
                    } else {
                        StartAction::All {
                            port: None, cluster: None, daemon_port: None, daemon_cluster: None,
                            listen_port: None, rest_port: None, rest_cluster: None,
                            storage_port: None, storage_cluster: None, storage_config: None,
                        }
                    }
                }
            };

            match effective_action {
                StartAction::All {
                    port, cluster, daemon_port, daemon_cluster,
                    listen_port, rest_port, rest_cluster,
                    storage_port, storage_cluster, storage_config,
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
                    ).await?;
                }
                StartAction::Daemon { port, cluster, daemon_port, daemon_cluster } => {
                    handlers_mod::handle_daemon_command_interactive(
                        DaemonCliCommand::Start { port, cluster, daemon_port, daemon_cluster },
                        daemon_handles.clone(),
                    ).await?;
                }
                StartAction::Rest { port: rest_start_port, cluster: rest_start_cluster, rest_port, rest_cluster } => {
                    handlers_mod::handle_rest_command_interactive(
                        RestCliCommand::Start { port: rest_start_port, cluster: rest_start_cluster, rest_port, rest_cluster },
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_handle.clone(),
                        rest_api_port_arc.clone(),
                    ).await?;
                }
                StartAction::Storage { port, config_file, cluster, storage_port, storage_cluster } => {
                    handlers_mod::handle_storage_command_interactive(
                        StorageAction::Start { port, config_file, cluster, storage_port, storage_cluster },
                        storage_daemon_shutdown_tx_opt.clone(),
                        storage_daemon_handle.clone(),
                        storage_daemon_port_arc.clone(),
                    ).await?;
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
            ).await?;
        }
        Commands::Use(action) => {
            match action {
                UseAction::Storage { engine, permanent } => {
                    // Delegate all logic to the new handler function.
                    // This replaces the entire block of code that was here before.
                    handlers_mod::handle_use_storage_command(engine, permanent).await?;
                }
                UseAction::Plugin { enable } => {
                    let mut config = config_mod::load_cli_config()?;
                    config.enable_plugins = enable;
                    config.save()?;
                    println!("Plugins {}", if enable { "enabled" } else { "disabled" });
                    handlers_mod::handle_show_plugins_command().await?;
                }
            }
        }
        Commands::Save(action) => {
            let mut config = config_mod::load_cli_config()?;
            match action {
                SaveAction::Configuration => {
                    config.save()?;
                    println!("CLI configuration saved persistently");
                }
                SaveAction::Storage => {
                    if let Some(engine) = config.storage.as_ref().and_then(|s| s.storage_engine_type.clone()) {
                        let engine_config_file = match engine {
                            StorageEngineType::RocksDB => DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB,
                            StorageEngineType::Sled => DEFAULT_STORAGE_CONFIG_PATH_SLED,
                            StorageEngineType::PostgreSQL => DEFAULT_STORAGE_CONFIG_PATH_POSTGRES,
                            StorageEngineType::MySQL => DEFAULT_STORAGE_CONFIG_PATH_MYSQL,
                            StorageEngineType::Redis => DEFAULT_STORAGE_CONFIG_PATH_REDIS,
                            StorageEngineType::InMemory => {
                                println!("Storage configuration not saved: InMemory (no persistent config required)");
                                return Ok(());
                            }
                        };

                        let storage_config_path = PathBuf::from("/opt/graphdb/storage_data/config.yaml");
                        let storage_settings = if storage_config_path.exists() {
                            StorageSettings::load_from_yaml(&storage_config_path)
                                .with_context(|| format!("Failed to load core config from {:?}", storage_config_path))?
                        } else {
                            StorageSettings::default()
                        };

                        let selected_config = if PathBuf::from(engine_config_file).exists() {
                            SelectedStorageConfig::load_from_yaml(&PathBuf::from(engine_config_file))
                                .with_context(|| format!("Failed to load config from {:?}", engine_config_file))?
                        } else {
                            println!("Config file {:?} not found; using default storage-specific settings", engine_config_file);
                            SelectedStorageConfig::default()
                        };

                        let mut merged_settings = storage_settings;
                        merged_settings.storage_engine_type = engine.to_string();
                        if let Some(port) = selected_config.storage.port {
                            merged_settings.default_port = port;
                        }

                        let storage_settings_wrapper = StorageSettingsWrapper { storage: merged_settings };
                        let content = serde_yaml2::to_string(&storage_settings_wrapper)
                            .with_context(|| "Failed to serialize storage settings")?;
                        if let Some(parent) = storage_config_path.parent() {
                            fs::create_dir_all(parent)
                                .with_context(|| format!("Failed to create config directory {:?}", parent))?;
                        }
                        fs::write(&storage_config_path, content)
                            .with_context(|| format!("Failed to write storage config to {:?}", storage_config_path))?;
                        println!("Storage configuration saved persistently");
                    } else {
                        println!("No storage engine configured; nothing to save");
                    }
                }
            }
        }
        Commands::Reload(reload_args) => {
            handlers_mod::handle_reload_command_interactive(
                reload_args,
            ).await?;
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
            ).await?;
        }
        Commands::Storage(storage_action) => {
            handlers_mod::handle_storage_command(storage_action).await?;
        }
        Commands::Daemon(daemon_cmd) => {
            handlers_mod::handle_daemon_command_interactive(daemon_cmd, daemon_handles.clone()).await?;
        }
        Commands::Rest(rest_cmd) => {
            handlers_mod::handle_rest_command_interactive(
                rest_cmd,
                rest_api_shutdown_tx_opt.clone(),
                rest_api_handle.clone(),
                rest_api_port_arc.clone(),
            ).await?;
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
                   //  let config = config_mod::load_cli_config()?;
                   // let storage_config = config.storage.unwrap_or_default();

                    // println!("Current Storage Engine: {:?}", storage_config.storage_engine_type);
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
                            //let config = config_mod::load_cli_config()?;
                            //let storage_config = config.storage.unwrap_or_default();
                            handlers_mod::handle_show_storage_config_command().await?;
                        }
                        ConfigAction::Main => {
                            handlers_mod::handle_show_main_config_command().await?;
                        }
                    }
                }
            }
        }
        _ => {
            // Placeholder for new or unknown commands
            println!("Unknown command or not yet implemented");
        }
    }
    Ok(())
}

/// Main entry point for CLI command handling.
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

    let args = CliArgs::parse();

    if args.internal_rest_api_run || args.internal_storage_daemon_run || args.internal_daemon_run {
        let converted_storage_engine = args.internal_storage_engine.map(|se_cli| {
            se_cli.into()
        });
        return daemon_management::handle_internal_daemon_run(
            args.internal_rest_api_run,
            args.internal_storage_daemon_run,
            args.internal_port,
            args.internal_storage_config_path,
            converted_storage_engine,
        ).await;
    }
    let mut config = match config_mod::load_cli_config() {
        Ok(config) => config,
        Err(e) => return Err(e),
    };

    if let Some(engine) = config.storage.as_ref().and_then(|s| s.storage_engine_type.clone()) {
        let storage_config_path = PathBuf::from("/opt/graphdb/storage_data/config.yaml");
        if storage_config_path.exists() {
            let storage_settings = StorageSettings::load_from_yaml(&storage_config_path)?;
            let storage_settings_wrapper = StorageSettingsWrapper { storage: storage_settings };
            let content = serde_yaml2::to_string(&storage_settings_wrapper)?;
            if let Some(parent) = storage_config_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&storage_config_path, content)?;
        }
    }
    if config.enable_plugins {
        println!("Experimental plugins enabled from config.");
    }

    let args = CliArgs {
        enable_plugins: config.enable_plugins,
        internal_storage_engine: config.storage.as_ref().and_then(|s| s.storage_engine_type.clone()),
        ..args
    };

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

    let daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let rest_api_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None));
    let rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));
    let storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));
    let storage_daemon_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None));

    let should_enter_interactive_mode = args.cli || args.command.is_none();

    if let Some(command) = args.command {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
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

        if !should_enter_interactive_mode {
            return Ok(());
        }
    }

    if should_enter_interactive_mode {
        if args.enable_plugins {
            println!("Experimental plugins is enabled.");
        }
        interactive_mod::run_cli_interactive(
            daemon_handles,
            rest_api_shutdown_tx_opt,
            rest_api_port_arc,
            rest_api_handle,
            storage_daemon_shutdown_tx_opt,
            storage_daemon_handle,
            storage_daemon_port_arc,
        ).await?;
    } else if args.enable_plugins {
        println!("Experimental plugins is enabled.");
    }

    Ok(())
}
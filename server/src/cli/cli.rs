// server/src/cli/cli.rs

use anyhow::Result;
use clap::{Parser, Subcommand, CommandFactory};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use std::process;
use std::env;

// Import modules
use crate::cli::commands::{
    DaemonCliCommand, RestCliCommand, StorageAction,
    StatusArgs, StopArgs, ReloadArgs, RestartArgs,
    ReloadAction, RestartAction, StartAction, StopAction, StatusAction
};
use crate::cli::config as config_mod; // Alias for config module
use crate::cli::handlers as handlers_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::help_display as help_display_mod; // Alias the module directly
use crate::cli::daemon_management::handle_internal_daemon_run; // Corrected: Imported directly

use lib::query_parser::{parse_query_from_string, QueryType};
// Removed unused import: use lib::storage_engine::config::StorageEngineType as LibStorageEngineType;

/// GraphDB Command Line Interface
#[derive(Parser, Debug)]
#[clap(author, version, about = "GraphDB Command Line Interface", long_about = None)]
#[clap(propagate_version = true)]
pub struct CliArgs {
    #[clap(subcommand)]
    pub command: Option<Commands>,

    /// Run CLI in interactive mode
    #[clap(long, short = 'c')]
    pub cli: bool,
    /// Enable experimental plugins
    #[clap(long)]
    pub enable_plugins: bool,
    /// Execute a direct query string
    #[clap(long, short = 'q')]
    pub query: Option<String>,

    // Internal flags for daemonized processes (hidden from help)
    #[clap(long, hide = true)]
    pub internal_rest_api_run: bool,
    #[clap(long, hide = true)]
    pub internal_storage_daemon_run: bool,
    #[clap(long, hide = true)]
    pub internal_port: Option<u16>,
    #[clap(long, hide = true)]
    pub internal_storage_config_path: Option<PathBuf>,
    #[clap(long, hide = true)]
    pub internal_storage_engine: Option<config_mod::StorageEngineType>, // Use config_mod alias
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start GraphDB components (daemon, rest, storage, or all)
    Start {
        #[clap(subcommand)]
        action: Option<StartAction>,
        // Removed top-level port, cluster, listen_port, storage_port, storage_config_file
        // These are now only available within StartAction::All
    },
    /// Stop GraphDB components (daemon, rest, storage, or all)
    Stop(StopArgs), // Corrected: Changed to directly use StopArgs
    /// Get status of GraphDB components (daemon, rest, storage, cluster, or all)
    Status(StatusArgs),
    /// Manage GraphDB daemon instances
    #[clap(subcommand)]
    Daemon(DaemonCliCommand),
    /// Manage REST API server
    #[clap(subcommand)]
    Rest(RestCliCommand),
    /// Manage standalone Storage daemon
    #[clap(subcommand)]
    Storage(StorageAction),
    /// Reload GraphDB components (all, rest, storage, daemon, or cluster)
    Reload(ReloadArgs),
    /// Restart GraphDB components (all, rest, storage, daemon, or cluster)
    Restart(RestartArgs),
    /// Run CLI in interactive mode
    Interactive,
    /// Authenticate a user and get a token
    Auth {
        username: String,
        password: String,
    },
    /// Authenticate a user and get a token (alias for 'auth')
    Authenticate {
        username: String,
        password: String,
    },
    /// Register a new user
    Register {
        username: String,
        password: String,
    },
    /// Get the version of the REST API server
    Version,
    /// Perform a health check on the REST API server
    Health,
    /// Display help message
    Help(HelpArgs),
    // Added missing top-level commands to the Commands enum definition
    // These were missing from the enum definition but present in the match arm
    // No explicit variants needed for Clear/Clean as they are not top-level subcommands
}

#[derive(Parser, Debug)]
pub struct HelpArgs {
    /// Filter help for a specific command (e.g., "start daemon")
    #[clap(long, short = 'c')]
    pub filter_command: Option<String>,
    /// Positional arguments for the command path (e.g., "start", "daemon")
    pub command_path: Vec<String>,
}

/// Main entry point for CLI command handling.
///
/// This function dispatches commands based on `CliArgs` and manages the lifecycle
/// of daemon and REST API processes.
#[tokio::main]
pub async fn start_cli() -> Result<()> {
    // --- Custom Help Command Handling ---
    let args_vec: Vec<String> = env::args().collect();
    if args_vec.len() > 1 && args_vec[1].to_lowercase() == "help" {
        let help_command_args: Vec<String> = args_vec.into_iter().skip(2).collect();
        let filter_command = if help_command_args.is_empty() {
            "".to_string()
        } else {
            help_command_args.join(" ")
        };
        let mut cmd = CliArgs::command();
        help_display_mod::print_filtered_help_clap_generated(&mut cmd, &filter_command);
        process::exit(0);
    }
    // --- End Custom Help Command Handling ---

    let args = CliArgs::parse();

    if args.internal_rest_api_run || args.internal_storage_daemon_run {
        let converted_storage_engine = args.internal_storage_engine.map(|se_cli| {
            se_cli.into()
        });

        return handle_internal_daemon_run(
            args.internal_rest_api_run,
            args.internal_storage_daemon_run,
            args.internal_port, // Corrected: Pass Option<u16> directly
            args.internal_storage_config_path,
            converted_storage_engine,
        ).await;
    }

    let config = match config_mod::load_cli_config() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading configuration: {}", e);
            eprintln!("Attempted to load from: {}", std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join("cli")
                .join("config.toml")
                .display());
            process::exit(1);
        }
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

    // Shared state for interactive mode to manage daemon processes
    let daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let rest_api_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None));
    let rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));
    let storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));

    let should_enter_interactive_mode = args.cli || args.command.is_none();

    if let Some(command) = args.command {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        match command {
            // Updated Commands::Start to only have 'action'
            Commands::Start { action } => { // Removed port, cluster, listen_port, storage_port, storage_config_file
                match action {
                    Some(StartAction::All { port, cluster, listen_port, storage_port, storage_config_file }) => {
                        handlers_mod::handle_start_all_interactive(
                            port, cluster, listen_port, storage_port, storage_config_file,
                            daemon_handles.clone(), rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone(),
                            storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(),
                        ).await?;
                    }
                    Some(StartAction::Daemon { port, cluster }) => {
                        handlers_mod::handle_daemon_command_interactive(
                            DaemonCliCommand::Start { port, cluster },
                            daemon_handles.clone(),
                        ).await?;
                    }
                    // FIX: Updated StartAction::Rest destructuring and call to match commands.rs
                    Some(StartAction::Rest { port: rest_start_port }) => { // 'port' is now the listen-port
                        handlers_mod::handle_rest_command_interactive(
                            // Pass the 'port' argument as the single port for RestCliCommand::Start
                            RestCliCommand::Start { port: rest_start_port },
                            rest_api_shutdown_tx_opt.clone(),
                            rest_api_port_arc.clone(),
                            rest_api_handle.clone(),
                        ).await?;
                    }
                    // FIX: Updated StartAction::Storage destructuring to use `port`
                    Some(StartAction::Storage { port, config_file: storage_start_config_file }) => { // Corrected: Destructure `port`
                        handlers_mod::handle_storage_command_interactive(
                            StorageAction::Start { port, config_file: storage_start_config_file }, // FIX: Pass `port`
                            storage_daemon_shutdown_tx_opt.clone(),
                            storage_daemon_handle.clone(),
                        ).await?;
                    }
                    None => {
                        // Default `start` command now implies `start all` without specific args
                        handlers_mod::handle_start_all_interactive(
                            None, None, None, None, None, // Pass None for all options
                            daemon_handles.clone(), rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone(),
                            storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(),
                        ).await?;
                    }
                }
            }
            Commands::Stop(stop_args) => {
                handlers_mod::handle_stop_command(stop_args).await?;
            }
            Commands::Status(status_args) => {
                handlers_mod::handle_status_command(status_args, rest_api_port_arc.clone()).await?;
            }
            Commands::Reload(reload_args) => {
                handlers_mod::handle_reload_command(reload_args).await?;
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
                ).await?;
            }
            Commands::Storage(storage_action) => {
                handlers_mod::handle_storage_command_interactive(
                    storage_action,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                ).await?;
            }
            Commands::Daemon(daemon_cmd) => {
                handlers_mod::handle_daemon_command_interactive(daemon_cmd, daemon_handles.clone()).await?;
            }
            Commands::Rest(rest_cmd) => {
                handlers_mod::handle_rest_command_interactive(
                    rest_cmd,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                ).await?;
            }
            Commands::Interactive => {
                interactive_mod::run_cli_interactive(
                    daemon_handles,
                    rest_api_shutdown_tx_opt,
                    rest_api_port_arc,
                    rest_api_handle,
                    storage_daemon_shutdown_tx_opt,
                    storage_daemon_handle,
                ).await?;
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
            Commands::Auth { username, password } => {
                handlers_mod::authenticate_user(username, password).await;
            }
            Commands::Authenticate { username, password } => {
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
        }
        return Ok(());
    }

    if should_enter_interactive_mode {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        interactive_mod::run_cli_interactive(
            daemon_handles,
            rest_api_shutdown_tx_opt,
            rest_api_port_arc,
            rest_api_handle,
            storage_daemon_shutdown_tx_opt,
            storage_daemon_handle,
        ).await?;
    } else if args.enable_plugins {
        println!("Experimental plugins is enabled.");
    }

    Ok(())
}


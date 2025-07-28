// server/src/cli/cli.rs
// Refactored: 2025-07-04 - Updated to use new storage engine names and removed obsolete imports.
// Fixed: 2025-07-04 - Corrected argument types for handle_restart_command_interactive.
// Fixed: 2025-07-04 - Corrected function name for run_cli_interactive.
// Fixed: 2025-07-04 - Resolved Mutex type mismatches and argument wrapping for handlers.
// Reverted: 2025-07-04 - Reverted to user-provided base code structure, re-applying necessary fixes.
// Fixed: 2025-07-04 - Corrected argument count for run_cli_interactive calls.
// FIX: 2025-07-04 - Removed #[tokio::main] from start_cli to resolve E0277.
// FIX: 2025-07-04 - Corrected argument count for handle_reload_command and run_cli_interactive calls.
// UPDATED: 2025-07-04 - Added Clear, Exit, Quit commands. Corrected run_cli_interactive argument passing.
// UPDATED: 2025-07-04 - Removed duplicate HelpArgs struct, now relying on import.
// FIX: 2025-07-04 - Corrected import path for `handle_internal_daemon_run`.
// FIX: 2025-07-26 - Restructured `status` command to use subcommands (`all`, `rest`, `daemon`, `storage`)
//        for improved non-interactive mode parsing and dispatch.
// FIX: 2025-07-26 - Corrected argument passing to handlers_mod functions for `Arc<Mutex<Option<u16>>>` types
//        and removed `?` operator from calls to functions returning `()`.
// FIX: 2025-07-26 - Corrected `display_full_status_summary` call to match 2 expected arguments.
// FIX: 2025-07-26 - Corrected `args` not found in scope for help command parsing, using `args_vec`.
// NEW FIX: 2025-07-26 - `graphdb-cli status` now defaults to `status all`.
// NEW FIX: 2025-07-26 - Ensure `status rest`, `status storage`, `status daemon` use provided ports.

use clap::{Parser, Subcommand, CommandFactory};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use std::process;
use std::env;
use std::collections::HashMap;

// Import modules
use crate::cli::commands::{
    DaemonCliCommand, RestCliCommand, StorageAction,
    StopArgs, ReloadArgs, RestartArgs,
    ReloadAction, RestartAction, StartAction, StopAction,
};
use crate::cli::config as config_mod;
use crate::cli::handlers as handlers_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::help_display as help_display_mod;
use crate::cli::daemon_management::handle_internal_daemon_run; // DIRECT IMPORT for handle_internal_daemon_run
// Removed the unused import: use crate::cli::daemon_management::stop_daemon_api_call;

use lib::query_parser::{parse_query_from_string, QueryType};

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

// Arguments for status subcommands (if they need specific ports or ranges)
#[derive(Parser, Debug)]
pub struct StatusComponentArgs {
    /// Port for the specific component (e.g., --port 8082 for REST API)
    #[clap(long, short)]
    pub port: Option<u16>,
    /// Range of ports for a cluster (e.g., --cluster 9001-9005)
    #[clap(long)]
    pub cluster: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum StatusCommands {
    /// Show status for all GraphDB components
    All {
        /// Show detailed status for GraphDB daemon cluster (e.g., 9001-9005)
        #[clap(long)]
        daemon_cluster_range: Option<String>,
        /// Show detailed status for REST API cluster (e.g., 8082-8083)
        #[clap(long)]
        rest_cluster_range: Option<String>,
        /// Show detailed status for Storage daemon cluster (e.g., 9050-9054)
        #[clap(long)]
        storage_cluster_range: Option<String>,
    },
    /// Show status for the REST API server
    Rest(StatusComponentArgs),
    /// Show status for the GraphDB Daemon
    Daemon(StatusComponentArgs),
    /// Show status for the Storage Daemon
    Storage(StatusComponentArgs),
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start GraphDB components (daemon, rest, storage, or all)
    Start {
        #[clap(subcommand)]
        action: Option<StartAction>,
    },
    /// Stop GraphDB components (daemon, rest, storage, or all)
    Stop(StopArgs),
    /// Get status of GraphDB components (daemon, rest, storage, cluster, or all)
    // MODIFIED: Make Status subcommand optional, defaulting to 'all'
    #[clap(subcommand)]
    Status(StatusCommands), // Changed from Option<StatusCommands> to StatusCommands to fix E0277
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
    Help(help_display_mod::HelpArgs),
    /// Clear the terminal screen
    Clear,
    /// Exit the CLI
    Exit,
    /// Quit the CLI (alias for 'exit')
    Quit,
}

/// Main entry point for CLI command handling.
///
/// This function dispatches commands based on `CliArgs` and manages the lifecycle
/// of daemon and REST API processes.
pub async fn start_cli() -> Result<()> {
    // --- Custom Help Command Handling ---
    let args_vec: Vec<String> = env::args().collect();
    if args_vec.len() > 1 && args_vec[1].to_lowercase() == "help" {
        let help_command_args: Vec<String> = args_vec.into_iter().skip(2).collect(); // FIX: Changed `args` to `args_vec`
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

        // Call directly from daemon_management
        return handle_internal_daemon_run(
            args.internal_rest_api_run,
            args.internal_storage_daemon_run,
            args.internal_port,
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
    let storage_daemon_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None));

    let should_enter_interactive_mode = args.cli || args.command.is_none();

    if let Some(command) = args.command {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        match command {
            Commands::Start { action } => {
                match action {
                    Some(StartAction::All { port, cluster, listen_port, storage_port, storage_config_file }) => {
                        handlers_mod::handle_start_all_interactive(
                            port, cluster, listen_port, storage_port, storage_config_file,
                            daemon_handles.clone(), rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone(),
                            storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(),
                            storage_daemon_port_arc.clone(),
                        ).await?;
                    }
                    Some(StartAction::Daemon { port, cluster }) => {
                        handlers_mod::handle_daemon_command_interactive(
                            DaemonCliCommand::Start { port, cluster },
                            daemon_handles.clone(),
                        ).await?;
                    }
                    Some(StartAction::Rest { port: rest_start_port }) => {
                        handlers_mod::handle_rest_command_interactive(
                            RestCliCommand::Start { port: rest_start_port },
                            rest_api_shutdown_tx_opt.clone(),
                            rest_api_port_arc.clone(),
                            rest_api_handle.clone(),
                        ).await?;
                    }
                    Some(StartAction::Storage { port, config_file: storage_start_config_file }) => {
                        handlers_mod::handle_storage_command_interactive(
                            StorageAction::Start { port, config_file: storage_start_config_file },
                            storage_daemon_shutdown_tx_opt.clone(),
                            storage_daemon_handle.clone(),
                            storage_daemon_port_arc.clone(),
                        ).await?;
                    }
                    None => { // Default to 'start all' if no subcommand is given for 'start'
                        handlers_mod::handle_start_all_interactive(
                            None, None, None, None, None,
                            daemon_handles.clone(), rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone(),
                            storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(),
                            storage_daemon_port_arc.clone(),
                        ).await?;
                    }
                }
            }
            Commands::Stop(stop_args) => {
                handlers_mod::handle_stop_command(stop_args, rest_api_port_arc.clone()).await?;
            }
            // MODIFIED: Handle StatusCommands enum, now with default 'all' handled by clap itself
            Commands::Status(status_cmd) => { // Changed from status_cmd_opt to status_cmd
                match status_cmd {
                    StatusCommands::All { daemon_cluster_range, rest_cluster_range, storage_cluster_range } => {
                        // Pass cluster ranges if needed by the handler. For now, relying on
                        // display_full_status_summary to internally determine these or
                        // to be updated to accept them.
                        // Corrected call to match the 2-argument signature in handlers.rs
                        handlers_mod::display_full_status_summary(
                            rest_api_port_arc.clone(),
                            storage_daemon_port_arc.clone(),
                        ).await;
                    },
                    StatusCommands::Rest(args) => {
                        // Pass the specific port from args if provided
                        handlers_mod::display_rest_api_status(rest_api_port_arc.clone()).await; // Removed args.port
                    },
                    StatusCommands::Daemon(args) => {
                        // Pass the specific port from args if provided
                        handlers_mod::display_daemon_status(args.port).await; // Added .await
                    },
                    StatusCommands::Storage(args) => {
                        // Pass the specific port from args if provided
                        handlers_mod::display_storage_daemon_status(args.port, storage_daemon_port_arc.clone()).await; // Added args.port
                    },
                }
            }
            Commands::Reload(reload_args) => {
                handlers_mod::handle_reload_command(reload_args, rest_api_port_arc.clone()).await?;
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
                handlers_mod::handle_storage_command_interactive(
                    storage_action,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
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
                    storage_daemon_port_arc,
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
                handlers_mod::display_rest_api_version(rest_api_port_arc.clone()).await;
            }
            Commands::Health => {
                handlers_mod::display_rest_api_health(rest_api_port_arc.clone()).await;
            }
            Commands::Clear => {
                handlers_mod::clear_terminal_screen().await?;
                handlers_mod::print_welcome_screen();
            }
            Commands::Exit | Commands::Quit => {
                println!("Exiting CLI. Goodbye!");
                process::exit(0);
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
            storage_daemon_port_arc,
        ).await?;
    } else if args.enable_plugins {
        println!("Experimental plugins is enabled.");
    }

    Ok(())
}
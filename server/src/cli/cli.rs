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

use clap::{Parser, Subcommand, CommandFactory}; // Removed FromArgMatches
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
    StatusArgs, StopArgs, ReloadArgs, RestartArgs,
    ReloadAction, RestartAction, StartAction, StopAction, StatusAction,
    HelpArgs // Import HelpArgs from commands.rs for consistency
};
use crate::cli::config as config_mod;
use crate::cli::handlers as handlers_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::help_display as help_display_mod; // Still needed for print functions
use crate::cli::daemon_management; // Import daemon_management directly


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

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start GraphDB components (daemon, rest, storage, or all)
    Start {
        // These arguments capture top-level args for 'start' that implicitly mean 'start all'.
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
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the Storage Daemon.")]
        storage_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the Storage Daemon.")]
        storage_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Path to the Storage Daemon configuration file.")]
        storage_config: Option<String>, // Keep as String here

        #[clap(subcommand)]
        action: Option<StartAction>, // This captures explicit subcommands like `start rest` or `start all`
    },
    /// Stop GraphDB components (daemon, rest, storage, or all)
    Stop(StopArgs),
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
    Help(HelpArgs), // Use the HelpArgs struct from commands.rs
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

        // Call directly from daemon_management
        return daemon_management::handle_internal_daemon_run( // Corrected import path
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
            eprintln!("Error loading configuration: {}", e); // Changed to eprintln for consistency
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
        match lib::query_parser::parse_query_from_string(&query_string) { // Fully qualified path
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
                storage_config: top_storage_config_str, // Renamed to avoid conflict
                action,
            } => {
                let effective_action = match action {
                    Some(StartAction::All {
                        port, cluster, daemon_port, daemon_cluster,
                        listen_port, rest_port, rest_cluster,
                        storage_port, storage_cluster, storage_config
                    }) => {
                        // Explicit 'all' subcommand takes precedence for its own arguments.
                        StartAction::All {
                            port, cluster, daemon_port, daemon_cluster,
                            listen_port, rest_port, rest_cluster,
                            storage_port, storage_cluster,
                            storage_config, // This is already PathBuf from commands.rs
                        }
                    },
                    Some(other_action) => other_action, // Other explicit subcommands
                    None => {
                        // No explicit subcommand; check if top-level arguments for 'all' were provided.
                        if top_port.is_some() || top_cluster.is_some() || top_daemon_port.is_some() || top_daemon_cluster.is_some() ||
                            top_listen_port.is_some() || top_rest_port.is_some() || top_rest_cluster.is_some() ||
                            top_storage_port.is_some() || top_storage_cluster.is_some() || top_storage_config_str.is_some() {
                            StartAction::All {
                                port: top_port,
                                cluster: top_cluster,
                                daemon_port: top_daemon_port,
                                daemon_cluster: top_daemon_cluster,
                                listen_port: top_listen_port,
                                rest_port: top_rest_port,
                                rest_cluster: top_rest_cluster,
                                storage_port: top_storage_port,
                                storage_cluster: top_storage_cluster,
                                storage_config: top_storage_config_str.map(PathBuf::from), // Convert String to PathBuf
                            }
                        } else {
                            // No explicit subcommand and no top-level 'all' arguments, default to 'start all' without arguments.
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
                        storage_port, storage_cluster, storage_config, // Now PathBuf
                    } => {
                        // Call handle_start_all_interactive with the correct arguments
                        handlers_mod::handle_start_all_interactive(
                            port.or(daemon_port), // Consolidate daemon port
                            cluster.or(daemon_cluster), // Consolidate daemon cluster
                            listen_port.or(rest_port), // Consolidate rest port
                            rest_cluster, // Pass rest_cluster
                            storage_port,
                            storage_cluster, // Pass storage_cluster
                            storage_config, // This is already Option<PathBuf>
                            daemon_handles.clone(),
                            rest_api_shutdown_tx_opt.clone(),
                            rest_api_port_arc.clone(),
                            rest_api_handle.clone(),
                            storage_daemon_shutdown_tx_opt.clone(),
                            storage_daemon_handle.clone(),
                            storage_daemon_port_arc.clone(),
                        ).await?;
                    }
                    StartAction::Daemon { port, cluster } => {
                        handlers_mod::handle_daemon_command_interactive(
                            DaemonCliCommand::Start { port, cluster },
                            daemon_handles.clone(),
                        ).await?;
                    }
                    StartAction::Rest { port: rest_start_port, cluster: rest_start_cluster } => {
                        handlers_mod::handle_rest_command_interactive(
                            RestCliCommand::Start { port: rest_start_port, cluster: rest_start_cluster },
                            rest_api_shutdown_tx_opt.clone(),
                            rest_api_handle.clone(), // Corrected order
                            rest_api_port_arc.clone(), // Corrected order
                        ).await?;
                    }
                    StartAction::Storage { port, config_file: storage_start_config_file, cluster: storage_start_cluster } => {
                        // `config_file` here is PathBuf because that's what `StorageAction::Start` expects from `commands.rs`.
                        handlers_mod::handle_storage_command_interactive(
                            StorageAction::Start { port, config_file: storage_start_config_file, cluster: storage_start_cluster },
                            storage_daemon_shutdown_tx_opt.clone(),
                            storage_daemon_handle.clone(),
                            storage_daemon_port_arc.clone(),
                        ).await?;
                    }
                }
            }
            Commands::Stop(stop_args) => {
                // FIX: Make `stop` synonymous with `stop all` if no subcommand is provided.
                let updated_stop_args = StopArgs {
                    action: Some(stop_args.action.unwrap_or(StopAction::All))
                };

                handlers_mod::handle_stop_command(updated_stop_args).await?;
            }
            Commands::Status(status_args) => {
                handlers_mod::handle_status_command(
                    status_args,
                    rest_api_port_arc.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
            }
            Commands::Reload(reload_args) => {
                handlers_mod::handle_reload_command_interactive(
                    reload_args,
                    // Remove the extra arguments here, as handlers.rs::handle_reload_command_interactive only takes one.
                    // The interactive state (daemon_handles, rest_api_shutdown_tx_opt, etc.) should be managed within
                    // handle_reload_command (which handle_reload_command_interactive calls) or passed differently
                    // if they are truly needed by the interactive reload logic itself.
                    // Based on the provided signature, only reload_args is expected.
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
                // This now calls the non-interactive version, which spawns a new process.
                handlers_mod::handle_storage_command(storage_action).await?;
            }
            Commands::Daemon(daemon_cmd) => {
                handlers_mod::handle_daemon_command_interactive(daemon_cmd, daemon_handles.clone()).await?;
            }
            Commands::Rest(rest_cmd) => {
                handlers_mod::handle_rest_command_interactive(
                    rest_cmd,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_handle.clone(), // Corrected order
                    rest_api_port_arc.clone(), // Corrected order
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
        }
        return Ok(());
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

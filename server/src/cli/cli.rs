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
// FIX: 2025-07-06 - Updated Start command handling to support direct flags without subcommand.
// FIX: 2025-07-06 - Ensured rest_api_port_arc is updated for non-interactive REST API start.
// FIX: 2025-07-06 - Reintroduced rest_api_handle declaration to resolve E0425 errors.
// FIX: 2025-07-06 - Added missing arguments to handle_start_all_interactive calls and completed StorageAction::Start fields.
// FIX: 2025-07-06 - Removed invalid Start and StartAction imports and updated Commands enum.
// FIX: 2025-07-12 - Added 'Start' as a subcommand to resolve 'unrecognized subcommand' error.
// FIX: 2025-07-12 - Corrected StartArgs handling to match StartAction::All fields for handle_start_all_interactive.
// FIX: 2025-07-12 - Added top-level flags to CliArgs for start command and logic to infer StartAction based on flags.
// FIX: 2025-07-12 - Fixed E0533 by using matches! for StartAction::All comparison in ambiguous port check.
// FIX: 2025-07-12 - Updated StartArgs handling to use top-level flags when action is None, ensuring --cluster and --daemon are recognized.

use clap::{Parser, Subcommand, CommandFactory, Args};
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
    ReloadAction, RestartAction, StopAction, StatusAction,
    StartArgs, StartAction
};
use crate::cli::config as config_mod;
use crate::cli::handlers as handlers_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::help_display as help_display_mod;
use crate::cli::daemon_management::handle_internal_daemon_run;

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
    pub internal_storage_engine: Option<config_mod::StorageEngineType>,

    // Start-related flags (available at top level for 'start' command)
    #[clap(long, short = 'p', help = "Port for the daemon, REST API, or storage (requires subcommand or flags if ambiguous)")]
    pub port: Option<u16>,
    #[clap(long, short = 'c', alias = "join-cluster", help = "Cluster range for the daemon, REST, or storage (e.g., '9001-9004')")]
    pub cluster: Option<String>,
    #[clap(long, help = "Listen port for the REST API")]
    pub listen_port: Option<u16>,
    #[clap(long, help = "Storage port for the storage daemon")]
    pub storage_port: Option<u16>,
    #[clap(long, help = "Path to storage configuration file")]
    pub storage_config_file: Option<PathBuf>,
    #[clap(long, help = "Data directory for storage daemon")]
    pub data_directory: Option<String>,
    #[clap(long, help = "Log directory for storage daemon")]
    pub log_directory: Option<String>,
    #[clap(long, help = "Maximum disk space in GB for storage daemon")]
    pub max_disk_space_gb: Option<u64>,
    #[clap(long, help = "Minimum disk space in GB for storage daemon")]
    pub min_disk_space_gb: Option<u64>,
    #[clap(long, help = "Use Raft for scale in storage daemon")]
    pub use_raft_for_scale: Option<bool>,
    #[clap(long, help = "Storage engine type (e.g., 'sled', 'rocksdb')")]
    pub storage_engine_type: Option<String>,
    #[clap(long, help = "Start the daemon component")]
    pub daemon: Option<bool>,
    #[clap(long, help = "Start the REST API component")]
    pub rest: Option<bool>,
    #[clap(long, help = "Start the storage component")]
    pub storage: Option<bool>,
}

/// Subcommands for the CLI
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start GraphDB components (daemon, rest, storage, or all)
    Start(StartArgs),
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
            Commands::Start(start_args) => {
                // Check top-level flags to infer StartAction if no subcommand is provided
                match start_args.action {
                    Some(action) => {
                        // Handle explicit subcommand
                        match action {
                            StartAction::All {
                                port,
                                cluster,
                                config_file,
                                listen_port,
                                storage_port,
                                storage_config_file,
                                data_directory,
                                log_directory,
                                max_disk_space_gb,
                                min_disk_space_gb,
                                use_raft_for_scale,
                                storage_engine_type,
                                .. // Added to ignore daemon, rest, storage fields
                            } => {
                                // Update rest_api_port_arc and storage_daemon_port_arc before starting
                                if let Some(listen_port) = listen_port.or(start_args.listen_port).or(args.listen_port) {
                                    let mut port_guard = rest_api_port_arc.lock().await;
                                    *port_guard = Some(listen_port);
                                }
                                if let Some(storage_port) = storage_port.or(start_args.storage_port).or(args.storage_port) {
                                    let mut port_guard = storage_daemon_port_arc.lock().await;
                                    *port_guard = Some(storage_port);
                                }
                                handlers_mod::handle_start_all_interactive(
                                    port.or(start_args.port).or(args.port),
                                    cluster.or(start_args.cluster).or(args.cluster),
                                    listen_port.or(start_args.listen_port).or(args.listen_port),
                                    storage_port.or(start_args.storage_port).or(args.storage_port),
                                    storage_config_file.or(start_args.storage_config_file).or(args.storage_config_file),
                                    data_directory.or(start_args.data_directory).or(args.data_directory),
                                    log_directory.or(start_args.log_directory).or(args.log_directory),
                                    max_disk_space_gb.or(start_args.max_disk_space_gb).or(args.max_disk_space_gb),
                                    min_disk_space_gb.or(start_args.min_disk_space_gb).or(args.min_disk_space_gb),
                                    use_raft_for_scale.or(start_args.use_raft_for_scale).or(args.use_raft_for_scale),
                                    storage_engine_type.or(start_args.storage_engine_type).or(args.storage_engine_type),
                                    daemon_handles.clone(),
                                    rest_api_shutdown_tx_opt.clone(),
                                    rest_api_port_arc.clone(),
                                    rest_api_handle.clone(),
                                    storage_daemon_shutdown_tx_opt.clone(),
                                    storage_daemon_handle.clone(),
                                    storage_daemon_port_arc.clone(),
                                ).await?;
                            }
                            StartAction::Daemon { port, cluster, .. } => { // Added ..
                                handlers_mod::handle_daemon_command_interactive(
                                    DaemonCliCommand::Start {
                                        port: port.or(start_args.port).or(args.port),
                                        cluster: cluster.or(start_args.cluster).or(args.cluster),
                                    },
                                    daemon_handles.clone(),
                                ).await?;
                            }
                            StartAction::Rest { port, .. } => { // Simplified and added ..
                                handlers_mod::handle_rest_command_interactive(
                                    RestCliCommand::Start {
                                        port: port.or(start_args.listen_port).or(args.listen_port).or(start_args.port).or(args.port),
                                    },
                                    rest_api_shutdown_tx_opt.clone(),
                                    rest_api_port_arc.clone(),
                                    rest_api_handle.clone(),
                                ).await?;
                            }
                            StartAction::Storage {
                                port,
                                config_file,
                                cluster,
                                data_directory,
                                log_directory,
                                max_disk_space_gb,
                                min_disk_space_gb,
                                use_raft_for_scale,
                                storage_engine_type,
                                .. // Added to ignore daemon, rest fields
                            } => {
                                handlers_mod::handle_storage_command_interactive(
                                    StorageAction::Start {
                                        port: port.or(start_args.storage_port).or(args.storage_port).or(start_args.port).or(args.port),
                                        config_file: config_file.or(start_args.storage_config_file).or(args.storage_config_file),
                                        cluster: cluster.or(start_args.cluster).or(args.cluster),
                                        data_directory: data_directory.or(start_args.data_directory).or(args.data_directory),
                                        log_directory: log_directory.or(start_args.log_directory).or(args.log_directory),
                                        max_disk_space_gb: max_disk_space_gb.or(start_args.max_disk_space_gb).or(args.max_disk_space_gb),
                                        min_disk_space_gb: min_disk_space_gb.or(start_args.min_disk_space_gb).or(args.min_disk_space_gb),
                                        use_raft_for_scale: use_raft_for_scale.or(start_args.use_raft_for_scale).or(args.use_raft_for_scale),
                                        storage_engine_type: storage_engine_type.or(start_args.storage_engine_type).or(args.storage_engine_type),
                                    },
                                    storage_daemon_shutdown_tx_opt.clone(),
                                    storage_daemon_handle.clone(),
                                    storage_daemon_port_arc.clone(),
                                ).await?;
                            }
                        }
                    }
                    None => {
                        // Infer StartAction from top-level flags
                        if start_args.listen_port.is_some() || start_args.rest.unwrap_or(false) || args.listen_port.is_some() || args.rest.unwrap_or(false) {
                            // Imply StartAction::Rest
                            let port = start_args.listen_port.or(args.listen_port).or(start_args.port).or(args.port);
                            handlers_mod::handle_rest_command_interactive(
                                RestCliCommand::Start { port },
                                rest_api_shutdown_tx_opt.clone(),
                                rest_api_port_arc.clone(),
                                rest_api_handle.clone(),
                            ).await?;
                        } else if start_args.storage_port.is_some() || start_args.storage.unwrap_or(false) || args.storage_port.is_some() || args.storage.unwrap_or(false) ||
                                  start_args.storage_config_file.is_some() || args.storage_config_file.is_some() ||
                                  start_args.data_directory.is_some() || args.data_directory.is_some() ||
                                  start_args.log_directory.is_some() || args.log_directory.is_some() ||
                                  start_args.max_disk_space_gb.is_some() || args.max_disk_space_gb.is_some() ||
                                  start_args.min_disk_space_gb.is_some() || args.min_disk_space_gb.is_some() ||
                                  start_args.use_raft_for_scale.is_some() || args.use_raft_for_scale.is_some() ||
                                  start_args.storage_engine_type.is_some() || args.storage_engine_type.is_some() {
                            // Imply StartAction::Storage
                            let port = start_args.storage_port.or(args.storage_port).or(start_args.port).or(args.port);
                            handlers_mod::handle_storage_command_interactive(
                                StorageAction::Start {
                                    port,
                                    config_file: start_args.storage_config_file.or(args.storage_config_file),
                                    cluster: start_args.cluster.or(args.cluster),
                                    data_directory: start_args.data_directory.or(args.data_directory),
                                    log_directory: start_args.log_directory.or(args.log_directory),
                                    max_disk_space_gb: start_args.max_disk_space_gb.or(args.max_disk_space_gb),
                                    min_disk_space_gb: start_args.min_disk_space_gb.or(args.min_disk_space_gb),
                                    use_raft_for_scale: start_args.use_raft_for_scale.or(args.use_raft_for_scale),
                                    storage_engine_type: start_args.storage_engine_type.or(args.storage_engine_type),
                                },
                                storage_daemon_shutdown_tx_opt.clone(),
                                storage_daemon_handle.clone(),
                                storage_daemon_port_arc.clone(),
                            ).await?;
                        } else if start_args.daemon.unwrap_or(false) || args.daemon.unwrap_or(false) {
                            // Imply StartAction::Daemon
                            handlers_mod::handle_daemon_command_interactive(
                                DaemonCliCommand::Start {
                                    port: start_args.port.or(args.port),
                                    cluster: start_args.cluster.or(args.cluster),
                                },
                                daemon_handles.clone(),
                            ).await?;
                        } else if start_args.port.is_some() || args.port.is_some() {
                            // Ambiguous --port without specific flags or subcommand
                            eprintln!("Error: --port is ambiguous without specifying --daemon, --rest, or --storage. Please specify the component to start.");
                            process::exit(1);
                        } else {
                            // Default to StartAction::All if no specific flags or subcommand
                            handlers_mod::handle_start_all_interactive(
                                start_args.port.or(args.port),
                                start_args.cluster.or(args.cluster),
                                start_args.listen_port.or(args.listen_port),
                                start_args.storage_port.or(args.storage_port),
                                start_args.storage_config_file.or(args.storage_config_file),
                                start_args.data_directory.or(args.data_directory),
                                start_args.log_directory.or(args.log_directory),
                                start_args.max_disk_space_gb.or(args.max_disk_space_gb),
                                start_args.min_disk_space_gb.or(args.min_disk_space_gb),
                                start_args.use_raft_for_scale.or(args.use_raft_for_scale),
                                start_args.storage_engine_type.or(args.storage_engine_type),
                                daemon_handles.clone(),
                                rest_api_shutdown_tx_opt.clone(),
                                rest_api_port_arc.clone(),
                                rest_api_handle.clone(),
                                storage_daemon_shutdown_tx_opt.clone(),
                                storage_daemon_handle.clone(),
                                storage_daemon_port_arc.clone(),
                            ).await?;
                        }
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
    } else if should_enter_interactive_mode {
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

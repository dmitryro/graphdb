use clap::{Parser, Subcommand, CommandFactory};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use tokio::task::JoinHandle;
use std::process;
use std::env;
use std::collections::HashMap;

// Import modules
use crate::cli::commands::{
    DaemonCliCommand, RestCliCommand, StorageAction,
    StatusArgs, StopArgs, ReloadArgs, RestartArgs,
    StartArgs, QueryArgs, GraphQueryArgs, HelpArgs,
    StartAction, RestartAction, ReloadAction, ClearDataArgs,
    StatusAction, StopAction,
};
use crate::cli::config as config_mod;
use crate::cli::config::{DEFAULT_STORAGE_PORT, DEFAULT_REST_API_PORT, DEFAULT_DAEMON_PORT};
use crate::cli::handlers as handlers_mod;
use crate::cli::interactive as interactive_mod;
use crate::cli::help_display::{print_interactive_filtered_help, print_help_clap_generated};
use crate::cli::daemon_management;
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
    /// Execute a direct query string (for top-level query)
    #[clap(long, short = 'q')]
    pub query: Option<String>,
    /// Persist the query result (for top-level query)
    #[clap(long)]
    pub persist: bool,

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
    #[clap(long, hide = true)]
    pub internal_cluster: Option<String>,

    // Start-related flags (available at top level for 'start' command or implicit start)
    #[clap(long, short = 'p', help = "Port for the daemon, REST API, or storage (requires subcommand or flags if ambiguous)")]
    pub port: Option<u16>,
    #[clap(long, short = 'C', alias = "join-cluster", help = "Cluster for the daemon, REST, or storage (e.g., '9001-9004')")]
    pub cluster: Option<String>,
    #[clap(long, help = "Listen port for the REST API")]
    pub listen_port: Option<u16>,
    #[clap(long, help = "REST API port")]
    pub rest_port: Option<u16>,
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
    #[clap(long, help = "Join cluster for daemon")]
    pub join_cluster: Option<bool>,
    #[clap(long, help = "Join cluster for REST API")]
    pub join_rest_cluster: Option<bool>,
    #[clap(long, help = "Join cluster for storage")]
    pub join_storage_cluster: Option<bool>,
    #[clap(long, help = "Daemon port")]
    pub daemon_port: Option<u16>,
    #[clap(long, help = "Daemon cluster")]
    pub daemon_cluster: Option<String>,
    #[clap(long, help = "Config file for daemon")]
    pub config_file: Option<PathBuf>,
    #[clap(long, help = "REST cluster")]
    pub rest_cluster: Option<String>,
    #[clap(long, help = "Storage cluster")]
    pub storage_cluster: Option<String>,
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
    Daemon {
        #[clap(subcommand)]
        command: DaemonCliCommand,
    },
    /// Manage REST API server
    Rest {
        #[clap(subcommand)]
        command: RestCliCommand,
    },
    /// Manage standalone Storage daemon
    Storage {
        #[clap(subcommand)]
        command: StorageAction,
    },
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
    /// Execute a direct query string
    Query(QueryArgs),
    /// Execute a graph query
    GraphQuery(GraphQueryArgs),
    /// Display help message
    Help(HelpArgs),
    /// Clear data for GraphDB components
    ClearData(ClearDataArgs),
    /// Clear the terminal screen
    Clear,
    /// Exit the CLI
    Exit,
    /// Quit the CLI (alias for 'exit')
    Quit,
}

/// Type aliases for daemon management handles
pub type DaemonHandles = Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>;
pub type RestApiShutdownTx = Arc<Mutex<Option<oneshot::Sender<()>>>>;
pub type RestApiPort = Arc<Mutex<Option<u16>>>;
pub type RestApiHandle = Arc<Mutex<Option<JoinHandle<()>>>>;
pub type StorageDaemonShutdownTx = Arc<Mutex<Option<oneshot::Sender<()>>>>;
pub type StorageDaemonHandle = Arc<Mutex<Option<JoinHandle<()>>>>;
pub type StorageDaemonPort = Arc<Mutex<Option<u16>>>;

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
        print_interactive_filtered_help(&mut cmd, &filter_command);
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

    // Load default storage config
    let storage_config = match config_mod::load_storage_config(None) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Warning: Could not load storage config: {}. Using defaults.", e);
            config_mod::StorageConfig::default()
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
    let daemon_handles: DaemonHandles = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: RestApiShutdownTx = Arc::new(Mutex::new(None));
    let rest_api_port_arc: RestApiPort = Arc::new(Mutex::new(None));
    let rest_api_handle: RestApiHandle = Arc::new(Mutex::new(None));
    let storage_daemon_shutdown_tx_opt: StorageDaemonShutdownTx = Arc::new(Mutex::new(None));
    let storage_daemon_handle: StorageDaemonHandle = Arc::new(Mutex::new(None));
    let storage_daemon_port_arc: StorageDaemonPort = Arc::new(Mutex::new(None));

    // Initialize shared port state by discovering currently running services
    if let Some(port) = daemon_management::find_running_rest_api_port().await {
        *rest_api_port_arc.lock().await = Some(port);
    }
    if let Some(port) = daemon_management::find_running_storage_daemon_port().await {
        *storage_daemon_port_arc.lock().await = Some(port);
    }
    // No direct discovery for main daemon as it's not daemonized in the same way,
    // and its port is typically managed by `daemon_handles`.

    let should_enter_interactive_mode = args.cli || args.command.is_none();

    // Check if any top-level start-related flags are present
    let has_start_flags = args.port.is_some() || args.cluster.is_some() ||
                          args.listen_port.is_some() || args.rest_port.is_some() ||
                          args.storage_port.is_some() || args.storage_config_file.is_some() ||
                          args.data_directory.is_some() || args.log_directory.is_some() ||
                          args.max_disk_space_gb.is_some() || args.min_disk_space_gb.is_some() ||
                          args.use_raft_for_scale.is_some() || args.storage_engine_type.is_some() ||
                          args.daemon.is_some() || args.rest.is_some() || args.storage.is_some() ||
                          args.join_cluster.is_some() || args.join_rest_cluster.is_some() ||
                          args.join_storage_cluster.is_some() || args.daemon_port.is_some() ||
                          args.daemon_cluster.is_some() || args.config_file.is_some() ||
                          args.rest_cluster.is_some() || args.storage_cluster.is_some();

    if let Some(command) = args.command {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        match command {
            Commands::Start(start_args_subcommand) => {
                // Determine the action based on the subcommand's action field
                let effective_start_action = start_args_subcommand.action.unwrap_or_else(|| {
                    StartAction::All {
                        port: start_args_subcommand.port.or(args.port),
                        cluster: start_args_subcommand.cluster.clone().or(args.cluster.clone()),
                        config_file: start_args_subcommand.config_file.clone().or(args.config_file.clone()),
                        listen_port: start_args_subcommand.listen_port.or(args.listen_port),
                        rest_port: start_args_subcommand.rest_port.or(args.rest_port),
                        storage_port: start_args_subcommand.storage_port.or(args.storage_port),
                        storage_config_file: start_args_subcommand.storage_config_file.clone().or(args.storage_config_file.clone()),
                        data_directory: start_args_subcommand.data_directory.clone().or(args.data_directory.clone()),
                        log_directory: start_args_subcommand.log_directory.clone().or(args.log_directory.clone()),
                        max_disk_space_gb: start_args_subcommand.max_disk_space_gb.or(args.max_disk_space_gb),
                        min_disk_space_gb: start_args_subcommand.min_disk_space_gb.or(args.min_disk_space_gb),
                        use_raft_for_scale: start_args_subcommand.use_raft_for_scale.or(args.use_raft_for_scale),
                        storage_engine_type: start_args_subcommand.storage_engine_type.clone().or(args.storage_engine_type.clone()),
                        daemon: start_args_subcommand.daemon.or(args.daemon),
                        rest: start_args_subcommand.rest.or(args.rest),
                        storage: start_args_subcommand.storage.or(args.storage),
                        daemon_port: start_args_subcommand.daemon_port.or(args.daemon_port),
                        daemon_cluster: start_args_subcommand.daemon_cluster.clone().or(args.daemon_cluster.clone()),
                        rest_cluster: start_args_subcommand.rest_cluster.clone().or(args.rest_cluster.clone()),
                        join_cluster: start_args_subcommand.join_cluster.or(args.join_cluster),
                        join_rest_cluster: start_args_subcommand.join_rest_cluster.or(args.join_rest_cluster),
                        join_storage_cluster: start_args_subcommand.join_storage_cluster.or(args.join_storage_cluster),
                        storage_cluster: start_args_subcommand.storage_cluster.clone().or(args.storage_cluster.clone()),
                    }
                });

                // Construct a new StartArgs to pass to the handler, ensuring all fields are populated
                let final_start_args = StartArgs {
                    action: Some(effective_start_action),
                    port: None,
                    rest_port: None,
                    storage_port: None,
                    cluster: None,
                    rest_cluster: None,
                    storage_cluster: None,
                    join_cluster: None,
                    join_rest_cluster: None,
                    join_storage_cluster: None,
                    storage_config_file: None,
                    data_directory: None,
                    log_directory: None,
                    max_disk_space_gb: None,
                    min_disk_space_gb: None,
                    use_raft_for_scale: None,
                    storage_engine_type: None,
                    config_file: None,
                    daemon: None,
                    rest: None,
                    storage: None,
                    daemon_port: None,
                    listen_port: None,
                    daemon_cluster: None,
                };

                // Update rest_api_port_arc, storage_daemon_port_arc, and daemon_handles based on the final start args
                match &final_start_args.action {
                    Some(StartAction::All { listen_port, rest_port, storage_port, daemon_port, .. }) => {
                        if let Some(port_val) = listen_port.or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(Some(DEFAULT_REST_API_PORT)) {
                            let mut port_guard = rest_api_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                        if let Some(storage_port_val) = storage_port.as_ref().copied().or(args.storage_port).or(Some(DEFAULT_STORAGE_PORT)) {
                            let mut port_guard = storage_daemon_port_arc.lock().await;
                            *port_guard = Some(storage_port_val);
                        }
                        if let Some(daemon_port_val) = daemon_port.as_ref().copied().or(args.daemon_port).or(Some(DEFAULT_DAEMON_PORT)) {
                            let mut handles_guard = daemon_handles.lock().await;
                            handles_guard.insert(daemon_port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                        }
                    },
                    Some(StartAction::Rest { port, listen_port, rest_port, .. }) => {
                        if let Some(port_val) = port.or(listen_port.as_ref().copied()).or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(args.port).or(Some(DEFAULT_REST_API_PORT)) {
                            let mut port_guard = rest_api_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                    },
                    Some(StartAction::Storage { port, storage_port, .. }) => {
                        if let Some(port_val) = port.or(storage_port.as_ref().copied()).or(args.storage_port).or(args.port).or(Some(DEFAULT_STORAGE_PORT)) {
                            let mut port_guard = storage_daemon_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                    },
                    Some(StartAction::Daemon { port, daemon_port, .. }) => {
                        if let Some(port_val) = port.or(daemon_port.as_ref().copied()).or(args.daemon_port).or(args.port).or(Some(DEFAULT_DAEMON_PORT)) {
                            let mut handles_guard = daemon_handles.lock().await;
                            handles_guard.insert(port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                        }
                    },
                    _ => {}
                }

                handlers_mod::handle_start_command(
                    final_start_args,
                    daemon_handles.clone(),
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
            },
            Commands::Stop(stop_args) => {
                handlers_mod::handle_stop_command(
                    stop_args,
                    daemon_handles.clone(),
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
            },
            Commands::Status(status_args) => {
                handlers_mod::handle_status_command(
                    status_args,
                    rest_api_port_arc.clone(),
                    storage_daemon_port_arc.clone(),
                    args.rest_cluster.clone(),
                    args.daemon_cluster.clone(),
                    args.storage_cluster.clone(),
                ).await?;
            },
            Commands::Reload(reload_args) => {
                // Update ports in shared state for reload
                match &reload_args.action {
                    Some(ReloadAction::All { listen_port, rest_port, storage_port, daemon_port, .. }) => {
                        if let Some(port_val) = listen_port.or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(Some(DEFAULT_REST_API_PORT)) {
                            let mut port_guard = rest_api_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                        if let Some(storage_port_val) = storage_port.as_ref().copied().or(args.storage_port).or(Some(DEFAULT_STORAGE_PORT)) {
                            let mut port_guard = storage_daemon_port_arc.lock().await;
                            *port_guard = Some(storage_port_val);
                        }
                        if let Some(daemon_port_val) = daemon_port.as_ref().copied().or(args.daemon_port).or(Some(DEFAULT_DAEMON_PORT)) {
                            let mut handles_guard = daemon_handles.lock().await;
                            handles_guard.insert(daemon_port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                        }
                    },
                    Some(ReloadAction::Rest { port, listen_port, rest_port, .. }) => {
                        if let Some(port_val) = port.or(listen_port.as_ref().copied()).or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(args.port).or(Some(DEFAULT_REST_API_PORT)) {
                            let mut port_guard = rest_api_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                    },
                    Some(ReloadAction::Storage { port, storage_port, .. }) => {
                        if let Some(port_val) = port.or(storage_port.as_ref().copied()).or(args.storage_port).or(args.port).or(Some(DEFAULT_STORAGE_PORT)) {
                            let mut port_guard = storage_daemon_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                    },
                    Some(ReloadAction::Daemon { port, daemon_port, .. }) => {
                        if let Some(port_val) = port.or(daemon_port.as_ref().copied()).or(args.daemon_port).or(args.port).or(Some(DEFAULT_DAEMON_PORT)) {
                            let mut handles_guard = daemon_handles.lock().await;
                            handles_guard.insert(port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                        }
                    },
                    _ => {}
                }

                handlers_mod::handle_reload_command(
                    reload_args,
                    daemon_handles.clone(),
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
            },
            Commands::Restart(restart_args) => {
                // Validate critical fields
                if let Some(action) = &restart_args.action {
                    match action {
                        RestartAction::Rest { port, listen_port, rest_port, .. } => {
                            if port.is_none() && listen_port.is_none() && rest_port.is_none() && args.port.is_none() && args.listen_port.is_none() && args.rest_port.is_none() {
                                eprintln!("Error: --port or --listen-port or --rest-port is required for 'restart rest'.");
                                process::exit(1);
                            }
                        }
                        RestartAction::Storage { port, storage_port, .. } => {
                            if port.is_none() && storage_port.is_none() && args.port.is_none() && args.storage_port.is_none() {
                                eprintln!("Error: --port or --storage-port is required for 'restart storage'.");
                                process::exit(1);
                            }
                        }
                        RestartAction::Daemon { port, daemon_port, .. } => {
                            if port.is_none() && daemon_port.is_none() && args.port.is_none() && args.daemon_port.is_none() {
                                eprintln!("Error: --port or --daemon-port is required for 'restart daemon'.");
                                process::exit(1);
                            }
                        }
                        _ => {}
                    }
                }

                // Update ports in shared state
                match &restart_args.action {
                    Some(RestartAction::All { listen_port, rest_port, storage_port, daemon_port, .. }) => {
                        if let Some(port_val) = listen_port.or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(Some(DEFAULT_REST_API_PORT)) {
                            let mut port_guard = rest_api_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                        if let Some(storage_port_val) = storage_port.as_ref().copied().or(args.storage_port).or(Some(DEFAULT_STORAGE_PORT)) {
                            let mut port_guard = storage_daemon_port_arc.lock().await;
                            *port_guard = Some(storage_port_val);
                        }
                        if let Some(daemon_port_val) = daemon_port.as_ref().copied().or(args.daemon_port).or(Some(DEFAULT_DAEMON_PORT)) {
                            let mut handles_guard = daemon_handles.lock().await;
                            handles_guard.insert(daemon_port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                        }
                    },
                    Some(RestartAction::Rest { port, listen_port, rest_port, .. }) => {
                        if let Some(port_val) = port.or(listen_port.as_ref().copied()).or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(args.port).or(Some(DEFAULT_REST_API_PORT)) {
                            let mut port_guard = rest_api_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                    },
                    Some(RestartAction::Storage { port, storage_port, .. }) => {
                        if let Some(port_val) = port.or(storage_port.as_ref().copied()).or(args.storage_port).or(args.port).or(Some(DEFAULT_STORAGE_PORT)) {
                            let mut port_guard = storage_daemon_port_arc.lock().await;
                            *port_guard = Some(port_val);
                        }
                    },
                    Some(RestartAction::Daemon { port, daemon_port, .. }) => {
                        if let Some(port_val) = port.or(daemon_port.as_ref().copied()).or(args.daemon_port).or(args.port).or(Some(DEFAULT_DAEMON_PORT)) {
                            let mut handles_guard = daemon_handles.lock().await;
                            handles_guard.insert(port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                        }
                    },
                    _ => {}
                }

                handlers_mod::handle_restart_command(
                    restart_args,
                    daemon_handles.clone(),
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
            },
            Commands::Daemon { command: daemon_cmd } => {
                handlers_mod::handle_daemon_command_interactive(
                    daemon_cmd,
                    daemon_handles.clone(),
                    args.daemon_cluster.clone(),
                ).await?;
            },
            Commands::Rest { command: rest_cmd } => {
                handlers_mod::handle_rest_command_interactive(
                    rest_cmd,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                    args.rest_cluster.clone(),
                ).await?;
            },
            Commands::Storage { command: storage_action } => {
                handlers_mod::handle_storage_command_interactive(
                    storage_action,
                    daemon_handles.clone(),
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                    args.storage_cluster.clone(),
                ).await?;
            },
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
            },
            Commands::Auth { username, password } | Commands::Authenticate { username, password } => {
                handlers_mod::authenticate_user(username, password).await;
            },
            Commands::Register { username, password } => {
                handlers_mod::register_user(username, password).await;
            },
            Commands::Version => {
                handlers_mod::display_rest_api_version().await;
            },
            Commands::Health => {
                handlers_mod::display_rest_api_health().await;
            },
            Commands::Query(query_args) => {
                handlers_mod::execute_graph_query(query_args.query, query_args.persist).await;
            },
            Commands::GraphQuery(graph_query_args) => {
                handlers_mod::execute_graph_query(graph_query_args.query_string, graph_query_args.persist).await;
            },
            Commands::Help(help_args) => {
                let mut cmd = CliArgs::command();
                if let Some(command_filter) = help_args.filter_command.as_ref() {
                    print_interactive_filtered_help(&mut cmd, command_filter);
                } else {
                    print_help_clap_generated();
                }
            },
            Commands::ClearData(clear_data_args) => {
                handlers_mod::handle_clear_data_command(clear_data_args).await?;
            },
            Commands::Clear => {
                handlers_mod::clear_terminal_screen().await?;
                handlers_mod::print_welcome_screen();
            },
            Commands::Exit | Commands::Quit => {
                println!("Exiting CLI. Goodbye!");
                process::exit(0);
            },
        }
    } else if has_start_flags {
        println!("No explicit command given, but start-related flags detected. Assuming 'start all'.");

        let inferred_start_action = if args.daemon.unwrap_or(false) {
            StartAction::Daemon {
                port: args.port,
                cluster: args.cluster.clone(),
                join_cluster: args.join_cluster,
                config_file: args.config_file.clone(),
                daemon_port: args.daemon_port,
                daemon_cluster: args.daemon_cluster.clone(),
                join_daemon_cluster: args.join_cluster,
                daemon: args.daemon,
                rest: args.rest,
                storage: args.storage,
            }
        } else if args.rest.unwrap_or(false) {
            StartAction::Rest {
                port: args.port,
                cluster: args.cluster.clone(),
                join_cluster: args.join_cluster,
                listen_port: args.listen_port,
                rest_port: args.rest_port,
                rest_cluster: args.rest_cluster.clone(),
                join_rest_cluster: args.join_rest_cluster,
                daemon: args.daemon,
                storage: args.storage,
                config_file: args.config_file.clone(),
                rest: args.rest,
            }
        } else if args.storage.unwrap_or(false) {
            StartAction::Storage {
                port: args.port,
                config_file: args.config_file.clone(),
                cluster: args.cluster.clone(),
                join_cluster: args.join_cluster,
                data_directory: args.data_directory.clone(),
                log_directory: args.log_directory.clone(),
                max_disk_space_gb: args.max_disk_space_gb,
                min_disk_space_gb: args.min_disk_space_gb,
                use_raft_for_scale: args.use_raft_for_scale,
                storage_engine_type: args.storage_engine_type.clone(),
                daemon: args.daemon,
                rest: args.rest,
                storage: args.storage,
                storage_port: args.storage_port,
                storage_cluster: args.storage_cluster.clone(),
                join_storage_cluster: args.join_storage_cluster,
            }
        } else {
            StartAction::All {
                port: args.port,
                cluster: args.cluster.clone(),
                config_file: args.config_file.clone(),
                listen_port: args.listen_port,
                rest_port: args.rest_port,
                storage_port: args.storage_port,
                storage_config_file: args.storage_config_file.clone(),
                data_directory: args.data_directory.clone(),
                log_directory: args.log_directory.clone(),
                max_disk_space_gb: args.max_disk_space_gb,
                min_disk_space_gb: args.min_disk_space_gb,
                use_raft_for_scale: args.use_raft_for_scale,
                storage_engine_type: args.storage_engine_type.clone(),
                daemon: args.daemon,
                rest: args.rest,
                storage: args.storage,
                daemon_port: args.daemon_port,
                daemon_cluster: args.daemon_cluster.clone(),
                rest_cluster: args.rest_cluster.clone(),
                join_cluster: args.join_cluster,
                join_rest_cluster: args.join_rest_cluster,
                join_storage_cluster: args.join_storage_cluster,
                storage_cluster: args.storage_cluster.clone(),
            }
        };

        // Update rest_api_port_arc, storage_daemon_port_arc, and daemon_handles based on the inferred action
        match &inferred_start_action {
            StartAction::All { listen_port, rest_port, storage_port, daemon_port, .. } => {
                if let Some(port_val) = listen_port.or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(Some(DEFAULT_REST_API_PORT)) {
                    let mut port_guard = rest_api_port_arc.lock().await;
                    *port_guard = Some(port_val);
                }
                if let Some(storage_port_val) = storage_port.as_ref().copied().or(args.storage_port).or(Some(DEFAULT_STORAGE_PORT)) {
                    let mut port_guard = storage_daemon_port_arc.lock().await;
                    *port_guard = Some(storage_port_val);
                }
                if let Some(daemon_port_val) = daemon_port.as_ref().copied().or(args.daemon_port).or(Some(DEFAULT_DAEMON_PORT)) {
                    let mut handles_guard = daemon_handles.lock().await;
                    handles_guard.insert(daemon_port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                }
            },
            StartAction::Rest { port, listen_port, rest_port, .. } => {
                if let Some(port_val) = port.or(listen_port.as_ref().copied()).or(rest_port.as_ref().copied()).or(args.listen_port).or(args.rest_port).or(args.port).or(Some(DEFAULT_REST_API_PORT)) {
                    let mut port_guard = rest_api_port_arc.lock().await;
                    *port_guard = Some(port_val);
                }
            },
            StartAction::Storage { port, storage_port, .. } => {
                if let Some(port_val) = port.or(storage_port.as_ref().copied()).or(args.storage_port).or(args.port).or(Some(DEFAULT_STORAGE_PORT)) {
                    let mut port_guard = storage_daemon_port_arc.lock().await;
                    *port_guard = Some(port_val);
                }
            },
            StartAction::Daemon { port, daemon_port, .. } => {
                if let Some(port_val) = port.or(daemon_port.as_ref().copied()).or(args.daemon_port).or(args.port).or(Some(DEFAULT_DAEMON_PORT)) {
                    let mut handles_guard = daemon_handles.lock().await;
                    handles_guard.insert(port_val, (tokio::task::spawn(async {}), oneshot::channel().0));
                }
            },
            _ => {}
        }

        handlers_mod::handle_start_command(
            StartArgs {
                action: Some(inferred_start_action),
                port: None,
                rest_port: None,
                storage_port: None,
                cluster: None,
                rest_cluster: None,
                storage_cluster: None,
                join_cluster: None,
                join_rest_cluster: None,
                join_storage_cluster: None,
                storage_config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                config_file: None,
                daemon: None,
                rest: None,
                storage: None,
                daemon_port: None,
                listen_port: None,
                daemon_cluster: None,
            },
            daemon_handles.clone(),
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
            storage_daemon_shutdown_tx_opt.clone(),
            storage_daemon_handle.clone(),
            storage_daemon_port_arc.clone(),
        ).await?;
    } else if should_enter_interactive_mode {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        println!("No command specified. Entering interactive mode. Type 'help' for commands.");
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
        println!("Experimental plugins are enabled.");
    }

    Ok(())
}
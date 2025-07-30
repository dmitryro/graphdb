// server/src/cli/commands.rs

// This file defines the command-line arguments and subcommands
// for the GraphDB CLI using the `clap` crate.

use clap::{Parser, Subcommand, Arg, Args, ValueEnum}; // Added ValueEnum for StorageEngineType
use std::path::PathBuf;
use uuid::Uuid; // Corrected: Ensure Uuid is imported

// Re-defining HelpArgs here to ensure consistency with interactive.rs parsing
// If HelpArgs is truly meant to be in a separate help_display.rs, it should be imported from there.
// For this update, I'll define it here for clarity based on the interactive.rs context.
#[derive(Debug, PartialEq, Clone, Args)] // Added #[derive(Args)]
pub struct HelpArgs {
    pub filter_command: Option<String>,
    pub command_path: Vec<String>,
}

/// Enum representing the parsed command type in interactive mode.
#[derive(Debug, PartialEq, Clone)]
pub enum CommandType {
    // Daemon Commands
    Daemon(DaemonCliCommand),

    // Rest Commands
    Rest(RestCliCommand),

    // Storage Commands
    Storage(StorageAction),

    // Top-level Start command variants (can also be subcommands of 'start')
    StartRest { port: Option<u16>, cluster: Option<String> },
    StartStorage { port: Option<u16>, config_file: Option<PathBuf>, cluster: Option<String> },
    StartDaemon { port: Option<u16>, cluster: Option<String> },
    StartAll {
        port: Option<u16>,
        cluster: Option<String>,
        daemon_port: Option<u16>,
        daemon_cluster: Option<String>,
        listen_port: Option<u16>,
        rest_port: Option<u16>,
        rest_cluster: Option<String>,
        storage_port: Option<u16>,
        storage_cluster: Option<String>,
        storage_config_file: Option<PathBuf>,
    },

    // Top-level Stop commands (can also be subcommands of 'stop')
    StopAll,
    StopRest,
    StopDaemon(Option<u16>), // Takes an optional port
    StopStorage(Option<u16>), // Takes an optional port

    // Top-level Status commands (can also be subcommands of 'status')
    StatusSummary, // 'status' or 'status all' or 'status summary'
    //StatusRest(Option<u16>),
    StatusDaemon(Option<u16>),
    StatusStorage(Option<u16>),
    StatusCluster,

    // Authentication and User Management
    Auth { username: String, password: String }, // Alias for Authenticate
    Authenticate { username: String, password: String },
    RegisterUser { username: String, password: String }, // Renamed from Register for clarity

    // General Information
    Version,
    Health,

    // Reload Commands
    ReloadAll,
    ReloadRest,
    ReloadStorage,
    ReloadDaemon(Option<u16>),
    ReloadCluster,

    // Restart Commands
    RestartAll {
        port: Option<u16>,
        cluster: Option<String>,
        listen_port: Option<u16>,
        storage_port: Option<u16>,
        storage_config_file: Option<PathBuf>,
        daemon_cluster: Option<String>,
        daemon_port: Option<u16>,
        rest_cluster: Option<String>,
        rest_port: Option<u16>,
        storage_cluster: Option<String>,
    },
    RestartRest { port: Option<u16>, cluster: Option<String> },
    RestartStorage { port: Option<u16>, config_file: Option<PathBuf>, cluster: Option<String> },
    RestartDaemon { port: Option<u16>, cluster: Option<String> },
    RestartCluster,

    // Utility Commands
    Clear,
    Help(HelpArgs), // Uses the HelpArgs struct
    Exit, // 'exit', 'quit', 'q'
    Unknown, // For unparsable commands
}


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
    pub internal_storage_engine: Option<crate::cli::config::StorageEngineType>, // Use config_mod alias
}

#[derive(Subcommand, Debug, PartialEq, Clone)] // Added PartialEq and Clone
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
        storage_config: Option<String>, // Keep as String here, convert to PathBuf in handler

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
    Help(HelpArgs), // Use the HelpArgs struct
    /// Clear the terminal screen
    Clear,
    /// Exit the CLI
    Exit,
    /// Quit the CLI (alias for 'exit')
    Quit,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StartAction {
    /// Start all components (daemon, rest, storage)
    All {
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the daemon.")]
        port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the daemon.")]
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
        #[arg(long, value_parser = clap::value_parser!(PathBuf), help = "Path to the Storage Daemon configuration file.")]
        storage_config: Option<PathBuf>, // Changed to PathBuf
    },
    /// Start the GraphDB daemon.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "9001-9005").
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Start the REST API server.
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Start the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
    },
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: Option<StopAction>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StopAction {
    /// Stop all running components.
    All,
    /// Stop the REST API server.
    Rest {
        /// Port of the daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Stop a specific GraphDB daemon instance by port.
    Daemon {
        /// Port of the daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Stop the standalone Storage daemon by port.
    Storage {
        /// Port of the storage daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: Option<StatusAction>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StatusAction {
    /// Get a summary status of all running components.
    All,
    Summary,
    /// Get the status of the REST API server.
    Rest {
        /// Port of the daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of a specific GraphDB daemon instance by port.
    Daemon {
        /// Port of the daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon by port.
    Storage {
        /// Port of the storage daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of the entire cluster.
    Cluster,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DaemonCliCommand {
    /// Start the GraphDB daemon.
    Start {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "9001-9005").
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Stop a specific GraphDB daemon instance.
    Stop {
        /// Port of the daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of a specific GraphDB daemon instance.
    Status {
        /// Port of the daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    /// List all running GraphDB daemon instances.
    List,
    /// Clear all daemon-related processes and state.
    ClearAll,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum RestCliCommand {
    /// Start the REST API server.
    Start {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Stop the REST API server.
    Stop,
    /// Get the status of the REST API server.
    Status {
       /// Port for the Storage daemon.
        #[clap(long)] 
        cluster: Option<String>,  
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Perform a health check on the REST API server.
    Health,
    /// Get the version of the REST API server.
    Version,
    /// Register a new user with the REST API.
    RegisterUser {
        username: String,
        password: String,
    },
    /// Authenticate a user with the REST API.
    Authenticate {
        username: String,
        password: String,
    },
    /// Execute a graph query via the REST API.
    GraphQuery {
        query_string: String,
        #[clap(long)]
        persist: Option<bool>,
    },
    /// Execute a storage query via the REST API.
    StorageQuery,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StorageAction {
    /// Start the standalone Storage daemon.
    Start {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Stop the standalone Storage daemon.
    Stop {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon.
    Status {
        /// Port for the Storage daemon.
        #[clap(long)]
        cluster: Option<String>,
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    StorageQuery,
    /// Perform a health check on the REST API server.
    Health,
    /// Get the version of the REST API server.
    Version,
    List,
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct ReloadArgs {
    #[clap(subcommand)]
    pub action: Option<ReloadAction>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ReloadAction {
    /// Reload all components.
    All,
    /// Reload the REST API server.
    Rest,
    /// Reload the standalone Storage daemon.
    Storage,
    /// Reload a specific GraphDB daemon.
    Daemon {
        /// Port for the daemon to reload.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Reload the cluster configuration.
    Cluster,
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct RestartArgs {
    #[clap(subcommand)]
    pub action: RestartAction,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum RestartAction {
    /// Restart all components.
    All {
        #[arg(long, value_parser = clap::value_parser!(u16))]
        port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        listen_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        storage_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(PathBuf))]
        storage_config_file: Option<PathBuf>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        daemon_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        daemon_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        rest_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        rest_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        storage_cluster: Option<String>,
    },
    /// Restart the REST API server.
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for REST.
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Restart the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage.
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Restart a specific GraphDB daemon.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon.
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Restart the cluster configuration.
    Cluster,
}

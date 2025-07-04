// server/src/cli/commands.rs

// This file defines all the CLI argument structures and enums using `clap`.

use clap::{Parser, Subcommand, Args};
use std::path::PathBuf;
use crate::cli::config::StorageEngineType; // Import StorageEngineType from config module

/// Arguments for the `start` command's sub-actions.
#[derive(Subcommand, Debug, PartialEq, Clone)] // Added Clone for easier handling in interactive mode
pub enum StartAction {
    /// Start the REST API server.
    Rest {
        /// Port for the REST API server to listen on. Defaults to value in config.
        #[arg(long, short)]
        port: Option<u16>,
        /// Alias for --port, for consistency with other commands.
        #[arg(long)]
        listen_port: Option<u16>,
    },
    /// Start the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon to listen on. Defaults to value in config.
        #[arg(long, short)]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[arg(long, short = 'f', value_name = "FILE")]
        config_file: Option<PathBuf>,
    },
    /// Start a GraphDB daemon instance.
    Daemon {
        /// Port for the daemon to listen on.
        #[arg(long, short)]
        port: Option<u16>,
        /// Range of ports for a cluster (e.g., "9000-9002").
        #[arg(long, short)]
        cluster: Option<String>,
    },
    /// Start all core GraphDB components (daemon, REST API, storage).
    All {
        /// Port for the main Graph Daemon to listen on. Ignored if --cluster is used.
        #[arg(short = 'p', long = "port", value_name = "PORT")]
        port: Option<u16>,
        /// Range of ports for a cluster (e.g., "9001-9005"). Max 10 ports.
        #[arg(long = "cluster", value_name = "START-END")]
        cluster: Option<String>,
        /// Expose REST API on this port.
        #[arg(long = "listen-port", value_name = "LISTEN_PORT")]
        listen_port: Option<u16>,
        /// Port for the standalone Storage daemon.
        #[arg(long = "storage-port", value_name = "STORAGE_PORT")]
        storage_port: Option<u16>,
        /// Path to the storage daemon's configuration file.
        #[arg(long = "storage-config", value_name = "STORAGE_CONFIG_FILE")]
        storage_config_file: Option<PathBuf>,
    },
}

/// Arguments for the top-level `start` command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct StartArgs {
    #[command(subcommand)]
    pub action: StartAction,
}

/// Arguments for the `daemon` subcommand.
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DaemonCliCommand {
    /// Start a GraphDB daemon instance.
    Start {
        /// Port for the daemon to listen on.
        #[arg(long, short)]
        port: Option<u16>,
        /// Range of ports for a cluster (e.g., "9000-9002").
        #[arg(long, short)]
        cluster: Option<String>,
    },
    /// Stop a running GraphDB daemon instance.
    Stop {
        /// Port of the daemon to stop.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// Get the status of a GraphDB daemon instance.
    Status {
        /// Port of the daemon to check status for.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// List all currently managed daemon instances.
    List,
    /// Stop all managed daemon instances and attempt to kill any external ones.
    ClearAll,
}

/// Arguments for the `rest` subcommand.
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum RestCliCommand {
    /// Start the REST API server.
    Start {
        /// Port for the REST API server to listen on. Defaults to value in config.
        #[arg(long, short)]
        port: Option<u16>,
        /// Alias for --port, for consistency with other commands.
        #[arg(long)]
        listen_port: Option<u16>,
    },
    /// Stop the REST API server.
    Stop,
    /// Get the status of the REST API server.
    Status,
    /// Perform a health check on the REST API server.
    Health,
    /// Get the version of the REST API server.
    Version,
    /// Register a new user with the GraphDB REST API.
    RegisterUser {
        /// Username for the new user.
        username: String,
        /// Password for the new user.
        password: String,
    },
    /// Authenticate a user and get an authentication token.
    Authenticate {
        /// Username to authenticate.
        username: String,
        /// Password for authentication.
        password: String,
    },
    /// Execute a graph query via the REST API.
    GraphQuery {
        /// The graph query string.
        query_string: String,
        /// Whether to persist the results of the query.
        #[arg(long)]
        persist: Option<bool>,
    },
    /// Execute a storage-level query via the REST API (placeholder).
    StorageQuery,
}

/// Arguments for the `storage` subcommand.
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StorageAction {
    /// Start the standalone Storage daemon.
    Start {
        /// Port for the Storage daemon to listen on. Defaults to value in config.
        #[arg(long, short)]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[arg(long, short = 'f', value_name = "FILE")]
        config_file: Option<PathBuf>,
    },
    /// Stop the standalone Storage daemon.
    Stop {
        /// Port of the Storage daemon to stop.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon.
    Status {
        /// Port of the Storage daemon to check status for.
        #[arg(long, short)]
        port: Option<u16>,
    },
}

/// Arguments for the top-level `stop` command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct StopArgs {
    /// Specify which component to stop (rest, daemon, storage). If omitted, all components will be stopped.
    #[command(subcommand)]
    pub action: Option<StopAction>,
}

/// Actions for the top-level `stop` command.
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StopAction {
    /// Stop the REST API server.
    Rest,
    /// Stop a GraphDB daemon instance.
    Daemon {
        /// Port of the daemon to stop.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// Stop the standalone Storage daemon.
    Storage {
        /// Port of the Storage daemon to stop.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// Stop all core GraphDB components.
    All,
}

/// Arguments for the top-level `status` command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct StatusArgs {
    /// Specify which component to get status for (rest, daemon, storage). If omitted, a summary will be displayed.
    #[command(subcommand)]
    pub action: Option<StatusAction>,
}

/// Actions for the top-level `status` command.
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StatusAction {
    /// Get the status of the REST API server.
    Rest,
    /// Get the status of a GraphDB daemon instance.
    Daemon {
        /// Port of the daemon to check status for.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon.
    Storage {
        /// Port of the Storage daemon to check status for.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// Get the status of the entire cluster.
    Cluster, // Added for 'status cluster'
    /// Get a comprehensive status summary of all GraphDB components.
    All,
}

/// Arguments for the top-level `auth` command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct AuthArgs {
    /// Username for authentication.
    pub username: String,
    /// Password for authentication.
    pub password: String,
}

/// Arguments for the top-level `authenticate` command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct AuthenticateArgs {
    /// Username to authenticate.
    pub username: String,
    /// Password for authentication.
    pub password: String,
}

/// Arguments for the top-level `register` command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct RegisterArgs {
    /// Username for the new user.
    pub username: String,
    /// Password for the new user.
    pub password: String,
}

/// Arguments for the top-level `reload` command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct ReloadArgs {
    /// Specify which component to reload (all, rest, storage, daemon, cluster).
    #[command(subcommand)]
    pub action: ReloadAction,
}

/// Actions for the top-level `reload` command.
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ReloadAction {
    /// Reload all GraphDB components.
    All,
    /// Reload the REST API server.
    Rest,
    /// Reload the standalone Storage daemon.
    Storage,
    /// Reload a specific GraphDB daemon instance.
    Daemon {
        /// Port of the daemon to reload.
        #[arg(long, short)]
        port: Option<u16>,
    },
    /// Reload the cluster configuration.
    Cluster,
}


/// Main CLI arguments structure
#[derive(Parser, Debug)]
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI")]
pub struct CliArgs {
    #[arg(long, help = "Execute a direct query string.")]
    pub query: Option<String>,

    #[arg(long, help = "Force entry into the interactive CLI mode.")]
    pub cli: bool,

    #[arg(long, help = "Enable experimental plugins (feature flag).")]
    pub enable_plugins: bool,

    // Internal flags for daemonized processes. Hidden from user help.
    #[arg(long, hide = true)]
    pub internal_rest_api_run: bool,
    #[arg(long, hide = true)]
    pub internal_storage_daemon_run: bool,
    #[arg(long, hide = true)]
    pub internal_port: Option<u16>,
    #[arg(long, hide = true)]
    pub internal_storage_config_path: Option<PathBuf>,
    #[arg(long, hide = true)]
    pub internal_storage_engine: Option<StorageEngineType>,

    #[command(subcommand)]
    pub command: Option<GraphDbCommands>,
}

/// Enum representing the top-level commands.
#[derive(Subcommand, Debug)]
pub enum GraphDbCommands {
    /// View a graph by ID.
    ViewGraph {
        #[arg(long = "graph-id", value_name = "GRAPH_ID", help = "ID of the graph to view.")]
        graph_id: Option<u32>,
    },
    /// View history for a graph.
    ViewGraphHistory {
        #[arg(long = "graph-id", value_name = "GRAPH_ID", help = "ID of the graph to view history for.")]
        graph_id: Option<u32>,
        #[arg(long = "start-date", value_name = "START_DATE", help = "Start date for history (YYYY-MM-DD).")]
        start_date: Option<String>,
        #[arg(long = "end-date", value_name = "END_DATE", help = "End date for history (YYYY-MM-DD).")]
        end_date: Option<String>,
    },
    /// Index a node by ID.
    IndexNode {
        #[arg(long = "node-id", value_name = "NODE_ID", help = "ID of the node to index.")]
        node_id: Option<u32>,
    },
    /// Cache node state by ID.
    CacheNodeState {
        #[arg(long = "node-id", value_name = "NODE_ID", help = "ID of the node to cache state for.")]
        node_id: Option<u32>,
    },
    /// Start various GraphDB components (REST API, Storage Daemon, optional Graph Daemon).
    Start(StartArgs), // Now uses StartArgs for subcommands like 'start rest' or 'start all'
    /// Stop various GraphDB components (all by default, or specific with subcommands).
    Stop(StopArgs),
    /// Get a comprehensive status summary of all GraphDB components or specific component status.
    Status(StatusArgs),
    
    /// Commands related to the standalone Storage daemon
    #[clap(subcommand)]
    Storage(StorageAction),

    /// Commands related to the GraphDB daemon itself.
    #[clap(subcommand)]
    Daemon(DaemonCliCommand),

    /// Commands related to the REST API server.
    #[clap(subcommand)]
    Rest(RestCliCommand),

    /// Authenticate a user and get an authentication token.
    Auth(AuthArgs), // Added top-level Auth command
    /// Authenticate a user and get an authentication token.
    Authenticate(AuthenticateArgs), // Added top-level Authenticate command
    /// Register a new user.
    Register(RegisterArgs), // Added top-level Register command
    /// Get the version of the REST API server.
    Version, // Added top-level Version command
    /// Perform a health check on the REST API server.
    Health, // Added top-level Health command
    /// Reload GraphDB components.
    Reload(ReloadArgs), // Added top-level Reload command

    /// Clear the terminal screen.
    Clear,
    /// Alias for 'clear' to clear the terminal screen.
    Clean,

    // Help command is handled interactively, not via clap subcommand here.
}

// Data structures for internal use or communication (if needed)
// These might be moved to a `models.rs` if they grow in number and complexity.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct DaemonData {
    pub port: u16,
    pub host: String,
    pub pid: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct KVPair {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct PidStore {
    pub pid: u32,
}


// server/src/cli/commands.rs

// This file defines all the CLI argument structures and enums using `clap`.

use clap::{Parser, Subcommand, Args};
use std::path::PathBuf;
use crate::cli::config::StorageEngineType; // Import StorageEngineType from config module

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

/// Top-level GraphDB CLI commands
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
    Start {
        #[arg(short = 'p', long = "port", value_name = "PORT", help = "Port for the main Graph Daemon to listen on. Ignored if --cluster is used.")]
        port: Option<u16>,
        #[arg(long = "cluster", value_name = "START-END", help = "Start a cluster of daemons on a range of ports (e.g., '9001-9005'). Max 10 ports.")]
        cluster: Option<String>,
        #[arg(long = "listen-port", value_name = "LISTEN_PORT", help = "Expose REST API on this port")]
        listen_port: Option<u16>,
        #[arg(long = "storage-port", value_name = "STORAGE_PORT", help = "Port for the standalone Storage daemon.")]
        storage_port: Option<u16>,
        #[arg(long = "storage-config", value_name = "STORAGE_CONFIG_FILE", help = "Path to the storage daemon's configuration file.")]
        storage_config_file: Option<PathBuf>,
    },
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

    // Removed the Help subcommand from here.
    // The 'help' command will now be handled manually in cli.rs
}

/// Arguments for the top-level `stop` command
#[derive(Args, Debug)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: Option<StopAction>,
}

/// Subcommands for the top-level `stop` command
#[derive(Subcommand, Debug)]
pub enum StopAction {
    /// Stop the REST API server.
    Rest,
    /// Stop a specific GraphDB daemon.
    Daemon {
        /// The port of the daemon to stop.
        #[arg(long)]
        port: Option<u16>,
    },
    /// Stop the standalone Storage daemon.
    Storage {
        /// The port of the Storage daemon to stop. If not provided, it attempts to stop the commonly assumed default port (8085).
        #[arg(long)]
        port: Option<u16>,
    },
}

/// Arguments for the top-level `status` command
#[derive(Args, Debug)]
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: Option<StatusAction>,
}

/// Subcommands for the top-level `status` command
#[derive(Subcommand, Debug)]
pub enum StatusAction {
    /// Check the status of the REST API server.
    Rest,
    /// Check the status of a specific GraphDB daemon.
    Daemon {
        /// The port of the daemon to check.
        #[arg(long)]
        port: Option<u16>,
    },
    /// Check the status of the standalone Storage daemon.
    Storage {
        /// The port of the Storage daemon to check. If not provided, it checks the commonly assumed default port (8085).
        #[arg(long)]
        port: Option<u16>,
    },
}

/// Commands for interacting with the GraphDB daemon
#[derive(Subcommand, Debug, PartialEq)]
pub enum DaemonCliCommand {
    /// Start the GraphDB daemon
    Start {
        #[arg(short = 'p', long = "port", value_name = "PORT", help = "Port for the daemon to listen on.")]
        port: Option<u16>,
        #[arg(long = "cluster", value_name = "START-END", help = "Start a cluster of daemons on a range of ports (e.g., '9001-9005'). Max 10 ports.")]
        cluster: Option<String>,
    },
    /// Stop the GraphDB daemon
    Stop {
        #[arg(short = 'p', long = "port", value_name = "PORT", help = "Port of the daemon to stop.")]
        port: Option<u16>,
    },
    /// Get status of the GraphDB daemon
    Status {
        #[arg(short = 'p', long = "port", value_name = "PORT", help = "Port of the daemon to check status for.")]
        port: Option<u16>,
    },
    /// List all daemons managed by this CLI instance.
    List,
    /// Stop all daemons managed by this CLI instance and attempt to kill external ones.
    ClearAll,
}

/// Commands for interacting with the REST API server
#[derive(Subcommand, Debug, PartialEq)]
pub enum RestCliCommand {
    /// Start the REST API server
    Start {
        #[arg(long)]
        port: Option<u16>,
        #[arg(long = "listen-port", value_name = "LISTEN_PORT", help = "Expose REST API on this port")]
        listen_port: Option<u16>, // Added listen_port
    },
    /// Stop the REST API server
    Stop,
    /// Check the status of the REST API server
    Status,
    /// Perform a health check on the REST API server
    Health,
    /// Get the version of the REST API server
    Version,
    /// Register a new user via REST API
    RegisterUser {
        username: String,
        password: String,
    },
    /// Authenticate a user and get a token via REST API
    Authenticate {
        username: String,
        password: String,
    },
    /// Execute a graph query via REST API
    GraphQuery {
        query_string: String,
        #[arg(long)]
        persist: Option<bool>,
    },
    /// Execute a storage query via REST API (placeholder)
    StorageQuery,
}

/// Commands for interacting with the standalone Storage daemon
#[derive(Subcommand, Debug, PartialEq)]
pub enum StorageAction {
    /// Start the standalone Storage daemon
    Start {
        /// The port for the standalone Storage daemon. If not provided, the storage daemon will use its own configured default.
        #[clap(long)]
        port: Option<u16>,
        /// Path to the storage daemon's configuration file (default: storage_config.yaml in daemon's CWD).
        #[clap(long, default_value = "storage_config.yaml")]
        config_file: PathBuf,
    },
    /// Stop the standalone Storage daemon
    Stop {
        /// The port of the standalone Storage daemon to stop. If not provided, it attempts to stop the daemon on its common default port (8085).
        #[clap(long)]
        port: Option<u16>,
    },
    /// Check the status of the standalone Storage daemon
    Status {
        /// The port of the standalone Storage daemon to check. If not provided, it checks the daemon on the commonly assumed default port (8085).
        #[arg(long)]
        port: Option<u16>,
    },
}

// Removed HelpArgs struct as it's no longer needed for clap parsing.

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


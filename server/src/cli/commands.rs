// server/src/cli/commands.rs

// This file defines the command-line arguments and subcommands
// for the GraphDB CLI using the `clap` crate.

use clap::{Parser, Subcommand, Arg}; // Added Arg for better attribute usage
use std::path::PathBuf;
use uuid::Uuid; // Corrected: Ensure Uuid is imported
use crate::cli::help_display::HelpArgs;

/// Enum representing the parsed command type in interactive mode.
#[derive(Debug, PartialEq)]
pub enum CommandType {
    Daemon(DaemonCliCommand),
    Rest(RestCliCommand),
    Storage(StorageAction),
    // Top-level Start command variants
    StartRest { port: Option<u16>, cluster: Option<String> }, // Added cluster
    StartStorage { port: Option<u16>, config_file: Option<PathBuf>, cluster: Option<String> }, // Added cluster
    StartDaemon { port: Option<u16>, cluster: Option<String> }, // Added for top-level 'start daemon'
    StartAll {
        port: Option<u16>,
        cluster: Option<String>,
        daemon_port: Option<u16>,    // Added to match `handle_start_all_interactive`
        daemon_cluster: Option<String>, // Added to match `handle_start_all_interactive`
        listen_port: Option<u16>,
        rest_port: Option<u16>,      // Added to match `handle_start_all_interactive`
        rest_cluster: Option<String>, // Added to match `handle_start_all_interactive`
        storage_port: Option<u16>,
        storage_cluster: Option<String>, // Added to match `handle_start_all_interactive`
        storage_config_file: Option<PathBuf>, // Changed back to PathBuf for consistency with StorageAction
    }, // Added for top-level 'start all' or generic 'start'
    StopAll,
    StopRest,
    StopDaemon(Option<u16>),
    StopStorage(Option<u16>),
    StatusSummary, // Represents 'status' or 'status all'
    StatusRest,
    StatusDaemon(Option<u16>),
    StatusStorage(Option<u16>),
    StatusCluster, // Added for 'status cluster'
    Auth { username: String, password: String }, // Added for 'auth <username> <password>'
    Authenticate { username: String, password: String }, // Added for 'authenticate <username> <password>'
    RegisterUser { username: String, password: String }, // Added for 'register <username> <password>'
    Version, // Added for 'version'
    Health, // Added for 'health'
    ReloadAll, // Added for 'reload all'
    ReloadRest, // Added for 'reload rest'
    ReloadStorage, // Added for 'reload storage'
    ReloadDaemon(Option<u16>), // Added for 'reload daemon [--port <port>]'
    ReloadCluster, // Added for 'reload cluster'
    RestartAll { // Added for 'restart all'
        port: Option<u16>,
        cluster: Option<String>,
        daemon_port: Option<u16>,    // Added to match RestartAction::All
        daemon_cluster: Option<String>, // Added to match RestartAction::All
        listen_port: Option<u16>,
        rest_port: Option<u16>,      // Added to match RestartAction::All
        rest_cluster: Option<String>, // Added to match RestartAction::All
        storage_port: Option<u16>,
        storage_cluster: Option<String>, // Added to match RestartAction::All
        storage_config_file: Option<PathBuf>, // Changed back to PathBuf for consistency with StorageAction
    },
    RestartRest { port: Option<u16>, cluster: Option<String>, }, // ADDED: Added cluster field
    RestartStorage { port: Option<u16>, cluster: Option<String>, config_file: Option<PathBuf> }, // ADDED: Added cluster field
    RestartDaemon { port: Option<u16>, cluster: Option<String> }, // ADDED: Added cluster field
    RestartCluster, // ADDED
    Help(HelpArgs),
    Clear, // Added for 'clear' and 'clean' commands
    Exit,
    Unknown,
}

/// Commands for managing GraphDB daemon instances.
#[derive(Debug, Subcommand, PartialEq)] // Removed Args, Added PartialEq
pub enum DaemonCliCommand {
    /// Start a GraphDB daemon instance.
    Start {
        /// Port for the daemon to listen on.
        #[clap(long, short = 'p', aliases = ["daemon-port"])] // Added daemon-port as a synonym
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "8080-8085").
        #[clap(long, aliases = ["daemon-cluster"])] // Added daemon-cluster as a synonym
        cluster: Option<String>,
    },
    /// Stop a GraphDB daemon instance.
    Stop {
        /// Port of the daemon to stop. If not specified, attempts to stop the default or all managed daemons.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of a GraphDB daemon instance.
    Status {
        /// Port of the daemon to check status for.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// List all daemon instances managed by this CLI.
    List,
    /// Stop all managed daemons and attempt to kill any external daemon processes.
    ClearAll,
}

/// Commands for managing the REST API server.
#[derive(Debug, Subcommand, PartialEq)] // Removed Args, Added PartialEq
pub enum RestCliCommand {
    /// Start the REST API server.
    Start {
        /// Port for the REST API to listen on.
        #[clap(long, short = 'p', name = "listen-port", aliases = ["rest-port"])] // Use name to map --listen-port to 'port', added rest-port as a synonym
        port: Option<u16>,
        // Removed listen_port field for consistency
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long, aliases = ["rest-cluster"])] // Added rest-cluster as a synonym
        cluster: Option<String>,
    },
    /// Stop the REST API server.
    Stop,
    /// Get the status of the REST API server.
    Status,
    /// Perform a health check on the REST API server.
    Health,
    /// Get the version of the REST API server.
    Version,
    /// Register a new user via the REST API.
    RegisterUser {
        /// Username for the new user.
        username: String,
        /// Password for the new user.
        password: String,
    },
    /// Authenticate a user and get an authentication token via the REST API.
    Authenticate {
        /// Username to authenticate.
        username: String,
        /// Password for authentication.
        password: String,
    },
    /// Execute a graph query via the REST API.
    GraphQuery {
        /// The query string to execute.
        query_string: String,
        /// Whether to persist the query results.
        #[clap(long)]
        persist: Option<bool>,
    },
    /// Execute a storage query (placeholder).
    StorageQuery,
}

/// Actions for managing the standalone Storage daemon.
#[derive(Debug, Subcommand, PartialEq)] // Removed Args, Added PartialEq
pub enum StorageAction {
    /// Start the standalone Storage daemon.
    Start {
        /// Port for the Storage daemon to listen on.
        #[clap(long, short = 'p', name = "storage-port", aliases = ["storage-port"])] // Consistent: Use name to map --storage-port to 'port'
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage (e.g., "8080-8085").
        #[clap(long, aliases = ["storage-cluster"])] // Added storage-cluster as a synonym
        cluster: Option<String>,
    },
    /// Stop the standalone Storage daemon.
    Stop {
        /// Port of the storage daemon to stop. If not specified, attempts to stop the default or managed storage daemon.
        #[clap(long, short = 'p', name = "storage-port")] // Consistent: Use name to map --storage-port to 'port'
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon.
    Status {
        /// Port of the storage daemon to check status for.
        #[clap(long, short = 'p', name = "storage-port")] // Consistent: Use name to map --storage-port to 'port'
        port: Option<u16>,
    },
}

/// Arguments for the top-level `status` command.
#[derive(Debug, Parser)]
pub struct StatusArgs {
    /// Get a comprehensive status summary of all GraphDB components.
    #[clap(long, short = 'a')]
    pub all: bool,
    /// Cluster range for daemon status (e.g., "8080-8085").
    #[clap(long)]
    pub daemon_cluster_range: Option<String>,
    /// Cluster range for REST API status (e.g., "8080-8085").
    #[clap(long)]
    pub rest_cluster_range: Option<String>,
    #[clap(subcommand)]
    pub action: Option<StatusAction>,
}

/// Actions for the `status` command.
#[derive(Debug, Subcommand, PartialEq)] // Added PartialEq
pub enum StatusAction {
    All,
    /// Get detailed status of the REST API component.
    Rest,
    /// Get detailed status of a specific daemon or list common ones.
    Daemon {
        /// Port of the daemon to check status for.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get detailed status of the Storage component.
    Storage {
        /// Port of the storage daemon to check status for.
        #[clap(long, short = 'p', name = "storage-port")] // Consistent: Use name to map --storage-port to 'port'
        port: Option<u16>,
    },
    /// Get status of the cluster (placeholder).
    Cluster,
    // The `All` variant is removed here because the `all` field is now directly in `StatusArgs`.
    // /// Get a comprehensive status summary of all GraphDB components.
    // All,
}

/// Arguments for the top-level `stop` command.
#[derive(Debug, Parser)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: Option<StopAction>,
    // Removed: pub port: Option<u16>, // Removed this field to align with observed compiler error
}

/// Actions for the `stop` command.
#[derive(Debug, Subcommand, PartialEq)] // Added PartialEq
pub enum StopAction {
    /// Stop the REST API server.
    Rest,
    /// Stop a GraphDB daemon.
    Daemon {
        /// Port of the daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Stop the standalone Storage daemon.
    Storage {
        /// Port of the storage daemon to stop.
        #[clap(long, short = 'p', name = "storage-port")] // Consistent: Use name to map --storage-port to 'port'
        port: Option<u16>,
    },
    /// Stop all GraphDB components.
    All,
}

/// Arguments for the top-level `start` command actions.
#[derive(Debug, Subcommand, PartialEq)] // Added PartialEq
pub enum StartAction {
    /// Start the REST API server.
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port", aliases = ["rest-port"])] // Consistent: Use name to map --listen-port to 'port', added rest-port as a synonym
        port: Option<u16>,
        // Removed `listen_port` field as it's redundant with `port` and `name` attribute
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long, aliases = ["rest-cluster"])] // Added rest-cluster as a synonym
        cluster: Option<String>,
    },
    /// Start the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port", aliases = ["storage-port"])] // Consistent: Use name to map --storage-port to 'port'
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage (e.g., "8080-8085").
        #[clap(long, aliases = ["storage-cluster"])] // Added storage-cluster as a synonym
        cluster: Option<String>,
    },
    /// Start a GraphDB daemon instance.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p', aliases = ["daemon-port"])] // Added daemon-port as a synonym
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "8080-8085").
        #[clap(long, aliases = ["daemon-cluster"])] // Added daemon-cluster as a synonym
        cluster: Option<String>,
    },
    /// Start all core GraphDB components.
    All {
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
        #[arg(long, value_parser = clap::value_parser!(PathBuf), help = "Path to the Storage Daemon configuration file.")] // Changed to PathBuf
        storage_config: Option<PathBuf>,
    },
}

/// Arguments for the top-level `reload` command.
#[derive(Debug, Parser)]
pub struct ReloadArgs {
    #[clap(subcommand)]
    pub action: ReloadAction,
}

/// Actions for the `reload` command.
#[derive(Debug, Subcommand, PartialEq)] // Added PartialEq
pub enum ReloadAction {
    /// Reload all GraphDB components (stop and restart).
    All,
    /// Reload the REST API server.
    Rest,
    /// Reload the standalone Storage daemon.
    Storage,
    /// Reload a specific GraphDB daemon.
    Daemon {
        /// Port of the daemon to reload.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Reload cluster configuration (placeholder).
    Cluster,
}

/// Arguments for the top-level `restart` command.
#[derive(Debug, Parser, PartialEq)] // Added PartialEq
pub struct RestartArgs {
    #[clap(subcommand)]
    pub action: RestartAction, // FIX: Changed to mandatory RestartAction
}

/// Actions for the `restart` command.
#[derive(Debug, Subcommand, PartialEq)] // Added PartialEq
pub enum RestartAction {
    /// Restart all core GraphDB components.
    All {
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the daemon (if restarting daemon). Conflicts with --daemon-port if both specified.")]
        port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for daemon (e.g., \"8080-8085\"). Conflicts with --daemon-cluster if both specified.")]
        cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the daemon (synonym for --port).")]
        daemon_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the daemon (synonym for --cluster).")]
        daemon_cluster: Option<String>,
        /// Listen port for the REST API.
        #[clap(long)]
        listen_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the REST API. Conflicts with --listen-port if both specified.")]
        rest_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the REST API.")]
        rest_cluster: Option<String>,
        /// Storage port for the Storage daemon.
        #[clap(long)]
        storage_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the Storage Daemon.")]
        storage_cluster: Option<String>,
        /// Path to the storage configuration file.
        #[clap(long)]
        storage_config_file: Option<PathBuf>, // Changed to PathBuf
    },
    /// Restart the REST API server.
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")] // Consistent: Use name to map --listen-port to 'port'
        port: Option<u16>,
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long, aliases = ["rest-cluster"])] // Added rest-cluster as a synonym
        cluster: Option<String>, // ADDED
    },
    /// Restart the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port")] // Consistent: Use name to map --storage-port to 'port'
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage (e.g., "8080-8085").
        #[clap(long, aliases = ["storage-cluster"])] // Added storage-cluster as a synonym
        cluster: Option<String>, // ADDED
    },
    /// Restart a specific GraphDB daemon.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Restart cluster configuration (placeholder).
    Cluster,
}

// server/src/cli/commands.rs

// This file defines the command-line arguments and subcommands
// for the GraphDB CLI using the `clap` crate.

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use uuid::Uuid; // Ensure Uuid is imported

/// Commands for managing GraphDB daemon instances.
#[derive(Debug, Subcommand, PartialEq)]
pub enum DaemonCliCommand {
    /// Start a GraphDB daemon instance.
    Start {
        /// Port for the daemon to listen on.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "8080-8085").
        #[clap(long)]
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
#[derive(Debug, Subcommand, PartialEq)]
pub enum RestCliCommand {
    /// Start the REST API server.
    Start {
        /// Port for the REST API to listen on.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
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
#[derive(Debug, Subcommand, PartialEq)]
pub enum StorageAction {
    /// Start the standalone Storage daemon.
    Start {
        /// Port for the Storage daemon to listen on.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
    },
    /// Stop the standalone Storage daemon.
    Stop {
        /// Port of the storage daemon to stop. If not specified, attempts to stop the default or managed storage daemon.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon.
    Status {
        /// Port of the storage daemon to check status for.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
    },
}

/// Arguments for the top-level `status` command.
#[derive(Debug, Parser)]
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: Option<StatusAction>,
}

/// Actions for the `status` command.
#[derive(Debug, Subcommand, PartialEq)]
pub enum StatusAction {
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
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
    },
    /// Get status of the cluster (placeholder).
    Cluster,
    /// Get a comprehensive status summary of all GraphDB components.
    All,
}

/// Arguments for the top-level `stop` command.
#[derive(Debug, Parser)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: Option<StopAction>,
}

/// Actions for the `stop` command.
#[derive(Debug, Subcommand, PartialEq)]
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
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
    },
    /// Stop all GraphDB components.
    All,
}

/// Arguments for the top-level `start` command.
#[derive(Debug, Parser)]
pub struct Start {
    #[clap(subcommand)]
    pub action: Option<StartAction>,
    /// Port for the daemon (if starting daemon).
    #[clap(long, short = 'p')]
    pub port: Option<u16>,
    /// Cluster range for daemon (e.g., "8080-8085").
    #[clap(long)]
    pub cluster: Option<String>,
    /// Listen port for the REST API.
    #[clap(long)]
    pub listen_port: Option<u16>,
    /// Storage port for the Storage daemon.
    #[clap(long)]
    pub storage_port: Option<u16>,
    /// Path to the storage configuration file.
    #[clap(long)]
    pub storage_config_file: Option<PathBuf>,
}

/// Actions for the top-level `start` command actions.
#[derive(Debug, Subcommand, PartialEq)]
pub enum StartAction {
    /// Start the REST API server.
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
    },
    /// Start the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
    },
    /// Start a GraphDB daemon instance.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Start all core GraphDB components.
    All {
        /// Port for the daemon (if starting daemon).
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
        /// Listen port for the REST API.
        #[clap(long)]
        listen_port: Option<u16>,
        /// Storage port for the Storage daemon.
        #[clap(long)]
        storage_port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        storage_config_file: Option<PathBuf>,
    },
}

/// Arguments for the top-level `reload` command.
#[derive(Debug, Parser)]
pub struct ReloadArgs {
    #[clap(subcommand)]
    pub action: ReloadAction,
}

/// Actions for the `reload` command.
#[derive(Debug, Subcommand, PartialEq)]
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
#[derive(Debug, Parser)]
pub struct RestartArgs {
    #[clap(subcommand)]
    pub action: RestartAction,
}

/// Actions for the `restart` command.
#[derive(Debug, Subcommand, PartialEq)]
pub enum RestartAction {
    /// Restart all core GraphDB components.
    All {
        /// Port for the daemon (if restarting daemon).
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
        /// Listen port for the REST API.
        #[clap(long)]
        listen_port: Option<u16>,
        /// Storage port for the Storage daemon.
        #[clap(long)]
        storage_port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        storage_config_file: Option<PathBuf>,
    },
    /// Restart the REST API server.
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
    },
    /// Restart the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
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

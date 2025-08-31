// server/src/cli/commands.rs
// ADDED: 2025-08-08 - Added `pub use crate::cli::config::StorageEngineType` to re-export `StorageEngineType` publicly, resolving `error[E0603]` in `interactive.rs`.
// UPDATED: 2025-08-08 - Changed import from `lib::storage_engine::config::StorageEngineType` to `crate::cli::config::StorageEngineType` to align with project structure.
// UPDATED: 2025-08-08 - Ensured `StorageEngineType` supports `Sled`, `RocksDB`, `InMemory`, `Redis`, `PostgreSQL`, `MySQL` as per updated enum definition.
// NOTE: Kept `std::path::PathBuf` as it's standard for CLI args and compatible with `fs2` in `config.rs`.
// FIXED: 2025-08-08 - Added `permanent` boolean field to `UseAction::Storage` and `CommandType::UseStorage` to resolve `E0026` error in `config.rs`.
// ADDED: 2025-08-09 - Added `Save` command with `Storage` and `Config` subcommands to save storage engine and configuration changes.
// FIXED: 2025-08-09 - Added `ShowArgs` wrapper struct to fix `E0277` trait bound error for `ShowAction`.

use clap::{Parser, Subcommand, Arg, Args};
use std::path::PathBuf;
use uuid::Uuid;
// Re-export StorageEngineType to make it accessible to `interactive.rs`
pub use crate::cli::config::StorageEngineType;

/// Custom parser for storage engine to accept many aliases (rocksdb, rocks-db, postgres, postgresql, postgre-sql, mysql, my-sql, inmemory, in-memory).
pub fn parse_storage_engine(engine: &str) -> Result<StorageEngineType, String> {
    match engine.to_lowercase().as_str() {
        "sled" => Ok(StorageEngineType::Sled),
        "tikv" => Ok(StorageEngineType::TiKV),
        "rocksdb" | "rocks-db" => Ok(StorageEngineType::RocksDB),
        "inmemory" | "in-memory" | "in_memory" => Ok(StorageEngineType::InMemory),
        "redis" => Ok(StorageEngineType::Redis),
        "postgres" | "postgresql" | "postgre-sql" | "postgres-sql" => Ok(StorageEngineType::PostgreSQL),
        "mysql" | "my-sql" | "my_sql" => Ok(StorageEngineType::MySQL),
        other => Err(format!(
            "Invalid storage engine: '{}'. Supported values (examples): sled, rocksdb, rocks-db, tikv, inmemory, in-memory, redis, postgres, postgresql, postgre-sql, mysql, my-sql",
            other
        )),
    }
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

    // Use Commands
    UseStorage { engine: StorageEngineType, permanent: bool },
    UsePlugin { enable: bool },

    // Save Commands
    SaveStorage,
    SaveConfig,

    // Top-level Start command variants (can also be subcommands of 'start')
    StartRest { port: Option<u16>, cluster: Option<String>, rest_port: Option<u16>, rest_cluster: Option<String> },
    StartStorage { port: Option<u16>, config_file: Option<PathBuf>, cluster: Option<String>, storage_port: Option<u16>, storage_cluster: Option<String> },
    StartDaemon { port: Option<u16>, cluster: Option<String>, daemon_port: Option<u16>, daemon_cluster: Option<String> },
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
    StopRest(Option<u16>),
    StopDaemon(Option<u16>),
    StopStorage(Option<u16>),

    // Top-level Status commands (can also be subcommands of 'status')
    StatusSummary,
    StatusDaemon(Option<u16>),
    StatusStorage(Option<u16>),
    StatusCluster,
    StatusRaft(Option<u16>),
    //ShowStorage,
    // Authentication and User Management
    Auth { username: String, password: String },
    Authenticate { username: String, password: String },
    RegisterUser { username: String, password: String },

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
    RestartRest { port: Option<u16>, cluster: Option<String>, rest_port: Option<u16>, rest_cluster: Option<String> },
    RestartStorage { port: Option<u16>, config_file: Option<PathBuf>, cluster: Option<String>, storage_port: Option<u16>, storage_cluster: Option<String> },
    RestartDaemon { port: Option<u16>, cluster: Option<String>, daemon_port: Option<u16>, daemon_cluster: Option<String> },
    RestartCluster,
    Show(ShowAction),
    // Utility Commands
    Clear,
    Help(HelpArgs),
    Exit,
    Unknown,
}

#[derive(Debug, PartialEq, Clone, Args)]
pub struct HelpArgs {
    pub filter_command: Option<String>,
    pub command_path: Vec<String>,
}

/// Arguments for a unified query command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct QueryArgs {
    /// The query string to execute.
    pub query_string: String,
    /// Optional flag to explicitly specify the query language (e.g., cypher, sql, graphql, kv).
    #[clap(long, short)]
    pub language: Option<String>,
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
    pub internal_daemon_run: bool,
    #[clap(long, hide = true)]
    pub internal_port: Option<u16>,
    #[clap(long, hide = true)]
    pub internal_storage_config_path: Option<PathBuf>,
    #[clap(long, hide = true)]
    pub internal_storage_engine: Option<StorageEngineType>,
    #[clap(long, hide = true)]
    pub internal_data_directory: Option<PathBuf>,
    #[clap(long, hide = true)]
    pub internal_cluster_range: Option<String>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum Commands {
    /// Start GraphDB components (daemon, rest, storage, or all)
    Start {
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
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the Storage Daemon. Synonym for --port in `start storage`.")]
        storage_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the Storage Daemon. Synonym for --cluster in `start storage`.")]
        storage_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(PathBuf), help = "Path to the Storage Daemon configuration file.")]
        storage_config: Option<PathBuf>,
        #[clap(subcommand)]
        action: Option<StartAction>,
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
    /// Configure components (storage engine or plugins)
    #[clap(subcommand)]
    Use(UseAction),
    /// Save changes to storage or configuration
    #[clap(subcommand)]
    Save(SaveAction),
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
    /// Clear the terminal screen
    Clear,
    /// Exit the CLI
    Exit,
    /// Quit the CLI (alias for 'exit')
    Quit,
    /// Show information about system components
    Show(ShowArgs),
    /// Execute a query against the running GraphDB REST API.
    #[clap(alias = "q")]
    Query(QueryArgs),
    /// An alias for the `query` command.
    #[clap(alias = "e")]
    Exec(QueryArgs),
     /// Interact with the key-value store.
    #[clap(alias = "k")]
    Kv {
        #[clap(subcommand)]
        action: KvAction,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum KvAction {
    /// Get a value by its key from the storage engine.
    Get {
        /// The key to retrieve.
        #[clap(name = "KEY")]
        key: String,
    },
    /// Set a key-value pair in the storage engine.
    Set {
        /// The key to set.
        #[clap(name = "KEY")]
        key: String,
        /// The value to associate with the key.
        #[clap(name = "VALUE")]
        value: String,
    },
}


#[derive(Args, Debug, PartialEq, Clone)]
pub struct ShowArgs {
    #[clap(subcommand)]
    pub action: ShowAction,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum SaveAction {
    /// Save changes to the storage engine configuration
    Storage,
    /// Save changes to the general configuration (alias: save config)
    #[clap(name = "config")]
    Configuration,
}


// A new enum for the Show command's subcommands:
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ShowAction {
    /// Show the current storage engine.
    Storage,
    /// Show configuration information.
    Config {
        #[clap(subcommand)]
        config_type: ConfigAction,
    },
    /// Show the status of experimental plugins.
    Plugins,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ConfigAction {
    /// Show all configurations.
    All,
    /// Show REST API configuration.
    Rest,
    /// Show Storage configuration.
    Storage,
    /// Show main daemon configuration.
    Main,
}


#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StartAction {
    /// Start all components (daemon, rest, storage)
    All {
        /// Port for the main daemon
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the main daemon.")]
        port: Option<u16>,
        /// Cluster range for the main daemon (e.g., "9001-9005")
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the main daemon.")]
        cluster: Option<String>,
        /// Port for the main daemon (synonym for --port)
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the main daemon (synonym for --port).", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for the main daemon (synonym for --cluster)
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the main daemon (synonym for --cluster).", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
        /// Listen port for the REST API
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Listen port for the REST API.")]
        listen_port: Option<u16>,
        /// Port for the REST API (synonym for --listen-port)
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the REST API (synonym for --listen-port).", conflicts_with = "listen_port")]
        rest_port: Option<u16>,
        /// Cluster range for the REST API (e.g., "8080-8085")
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the REST API.")]
        rest_cluster: Option<String>,
        /// Port for the Storage Daemon
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the Storage Daemon.")]
        storage_port: Option<u16>,
        /// Cluster range for the Storage Daemon (e.g., "8080-8085")
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the Storage Daemon.")]
        storage_cluster: Option<String>,
        /// Path to the Storage Daemon configuration file
        #[arg(long, value_parser = clap::value_parser!(PathBuf), help = "Path to the Storage Daemon configuration file.")]
        storage_config: Option<PathBuf>,
    },
    /// Start the GraphDB daemon.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "9001-9005").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the daemon (synonym for --port).
        #[clap(long = "daemon-port", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for daemon (synonym for --cluster).
        #[clap(long = "daemon-cluster", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
    },
    /// Start the REST API server.
    #[clap(name = "rest")]
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the REST API (synonym for --listen-port).
        #[clap(long = "rest-port", conflicts_with = "port")]
        rest_port: Option<u16>,
        /// Cluster range for REST (synonym for --cluster).
        #[clap(long = "rest-cluster", conflicts_with = "cluster")]
        rest_cluster: Option<String>,
    },
    /// Start the standalone Storage daemon.
    #[clap(name = "storage")]
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
        /// Port for the Storage daemon (synonym for --storage-port).
        #[clap(long = "storage-port")]
        storage_port: Option<u16>,
        /// Cluster range for Storage (synonym for --cluster).
        #[clap(long = "storage-cluster", conflicts_with = "cluster")]
        storage_cluster: Option<String>,
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
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Get the status of a specific GraphDB daemon instance by port.
    Daemon {
        /// Port of the daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Get the status of the standalone Storage daemon by port.
    Storage {
        /// Port of the storage daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Get the status of the entire cluster.
    Cluster,
    /// Get the status of Raft nodes (Storage daemons).
    Raft {
        /// Port of the storage daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
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
        /// Port for the daemon (synonym for --port).
        #[clap(long = "daemon-port", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for daemon (synonym for --cluster).
        #[clap(long = "daemon-cluster", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
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
        /// Port for the REST API (synonym for --listen-port).
        #[clap(long = "rest-port", conflicts_with = "port")]
        rest_port: Option<u16>,
        /// Cluster range for REST (synonym for --cluster).
        #[clap(long = "rest-cluster", conflicts_with = "cluster")]
        rest_cluster: Option<String>,
    },
    /// Stop the REST API server.
    Stop {
        #[clap(long)]
        port: Option<u16>,
    },
    /// Get the status of the REST API server.
    Status {
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
        /// Port for the Storage daemon (synonym for --storage-port).
        #[clap(long = "storage-port")]
        storage_port: Option<u16>,
        /// Cluster range for Storage (synonym for --cluster).
        #[clap(long = "storage-cluster", conflicts_with = "cluster")]
        storage_cluster: Option<String>,
    },
    /// Stop the standalone Storage daemon.
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon.
    Status {
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
    Show, // New variant for 'show storage'
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
        /// Port for the REST API (synonym for --port).
        #[clap(long = "rest-port", conflicts_with = "port")]
        rest_port: Option<u16>,
        /// Cluster range for REST (synonym for --cluster).
        #[clap(long = "rest-cluster", conflicts_with = "cluster")]
        rest_cluster: Option<String>,
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
        /// Port for the Storage daemon (synonym for --port).
        #[clap(long = "storage-port", conflicts_with = "port")]
        storage_port: Option<u16>,
        /// Cluster range for Storage (synonym for --cluster).
        #[clap(long = "storage-cluster", conflicts_with = "cluster")]
        storage_cluster: Option<String>,
    },
    /// Restart a specific GraphDB daemon.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon.
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the daemon (synonym for --port).
        #[clap(long = "daemon-port", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for daemon (synonym for --cluster).
        #[clap(long = "daemon-cluster", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
    },
    /// Restart the cluster configuration.
    Cluster,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum UseAction {
    /// Configure the storage engine (e.g., sled, rocksdb, tikv, inmemory, redis, postgresql, mysql).
    Storage {
        /// Storage engine to use.
        #[arg(value_parser = parse_storage_engine)]
        engine: StorageEngineType,
        /// Persist the storage engine choice across sessions.
        #[clap(long, default_value = "false")]
        permanent: bool,
    },
    /// Enable or disable experimental plugins.
    Plugin {
        /// Enable or disable plugins (true to enable, false to disable).
        #[clap(long, default_value = "true")]
        enable: bool,
    },
}

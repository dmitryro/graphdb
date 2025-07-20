// This file defines the command-line arguments and subcommands
// for the GraphDB CLI using the `clap` crate.
// FIX: 2025-07-12 - Added cluster field to StartAction::Rest to support --cluster and --join-cluster flags, aligning with RestartAction::Rest and interactive.rs parsing.
// FIX: 2025-07-12 - Added alias "join-cluster" to cluster fields in StartAction and RestartAction to explicitly support --join-cluster flag.
// FIX: 2025-07-12 - Verified RestartAction::Storage includes cluster field to resolve E0026 and E0559 errors in interactive.rs.
// FIX: 2025-07-12 - Ensured daemon, rest, and storage fields in RestartAction variants are Option<bool> to align with interactive.rs fixes for E0063 and E0308.
// FIX: 2025-07-12 - Made StartArgs.action optional and added top-level flags to StartArgs to support direct flag usage without subcommand.
// NEW: 2025-07-12 - Added daemon and storage fields to StartAction::Rest and RestartAction::Rest to resolve E0063 errors in interactive.rs.
// NEW: 2025-07-12 - Added daemon and rest fields to StorageAction::Start to align with StartAction::Storage and RestartAction::Storage for consistency and reuse.
// FIX: 2025-07-15 - Added `is_any_set` method to `StartArgs` to help `cli.rs` determine if start-related flags are present.
// FIX: 2025-07-15 - Added `daemon_port`, `listen_port`, `daemon_cluster`, `rest_cluster`, `storage_cluster`, `join_rest_cluster`, `join_storage_cluster` to StartArgs and StartAction::All/Daemon/Rest/Storage for comprehensive flag parsing.
// FIX: 2025-07-15 - Ensured all `ReloadAction` and `RestartAction` variants have comprehensive fields for consistency and proper parsing.
// FIX: 2025-07-15 - Added missing fields to `StopArgs` and `StatusArgs` top-level and subcommand variants.
// FIX: 2025-07-15 - Ensured `Clone` derive is present on all necessary structs and enums.
// FIX: 2025-07-19 - Moved `CommandType` from `interactive.rs` to `commands.rs` and added it as a new enum, preserving all existing definitions.
// FIX: 2025-07-19 - Added Parser to clap imports and fixed missing cluster fields in CommandType status variants.
// FIX: 2025-07-19 - Corrected clap attributes from #[clap(...)] to #[command(...)] on CliArgs.
// FIX: 2025-07-19 - Corrected `StopArgs` and `StatusArgs` top-level fields to match interactive.rs usage.
// FIX: 2025-07-19 - Added `config_file` to `StartAction::Rest` and `RestartAction::Rest`.
// FIX: 2025-07-19 - Added `cluster` to `RestartAction::Cluster`.
// FIX: 2025-07-19 - Removed `storage_port` from `ClearDataAction::Storage` as per comment.
// FIX: 2025-07-19 - Removed `email` from `RegisterUserArgs`.
// FIX: 2025-07-19 - Added missing `config_file` to `CommandType::StartRest` and `CommandType::RestartRest`.
// FIX: 2025-07-19 - Added missing `daemon`, `rest`, `storage` to `CommandType::RestartStorage`.

use clap::{Args, Parser, Subcommand, ValueHint};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum CliCommand {
    /// Start a GraphDB component (daemon, REST API, or storage daemon)
    Start(StartArgs),
    /// Stop a GraphDB component (daemon, REST API, or storage daemon)
    Stop(StopArgs),
    /// Restart a GraphDB component (daemon, REST API, or storage daemon)
    Restart(RestartArgs),
    /// Reload configuration for a GraphDB component (daemon or REST API)
    Reload(ReloadArgs),
    /// Get the status of GraphDB components
    Status(StatusArgs),
    /// Clear data for a GraphDB component
    ClearData(ClearDataArgs),
    /// Register a new user
    RegisterUser(RegisterUserArgs),
    /// Authenticate a user
    Auth(AuthArgs),
    /// Execute a query against the GraphDB REST API
    Query(QueryArgs),
    /// Execute a graph query against the GraphDB REST API
    GraphQuery(GraphQueryArgs),
    /// Display help information
    Help(HelpArgs),
    /// Print version information
    Version,
    /// Check health of GraphDB components
    Health,
    /// Enter interactive CLI mode
    Interactive,
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct QueryArgs {
    #[arg(long)]
    pub query: String,
    #[arg(long)]
    pub persist: Option<bool>,
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct GraphQueryArgs {
    #[arg(long)]
    pub query_string: String,
    #[arg(long)]
    pub persist: Option<bool>,
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct HelpArgs {
    #[arg(long)]
    pub filter_command: Option<String>,
    #[arg(skip)] // This field is not directly parsed from CLI args but set programmatically
    pub command_path: Vec<String>,
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct DaemonCommandWrapper {
    #[command(subcommand)]
    pub command: DaemonCliCommand,
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct RestCommandWrapper {
    #[command(subcommand)]
    pub command: RestCliCommand,
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct StorageActionWrapper {
    #[command(subcommand)]
    pub command: StorageAction,
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum DaemonCliCommand {
    Start {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        join_daemon_cluster: Option<bool>,
    },
    Stop {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        daemon_port: Option<u16>,
    },
    Status {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        daemon_port: Option<u16>,
    },
    Restart {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        join_daemon_cluster: Option<bool>,
    },
    Reload {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        join_daemon_cluster: Option<bool>,
    },
    List,
    ClearAll,
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum RestCliCommand {
    Start {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long, short = 'c')]
        cluster: Option<String>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] // Added missing field
        config_file: Option<PathBuf>,
    },
    Stop {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
    },
    Status {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
    },
    Version,
    Health,
    Query {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'q')]
        query: String,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
    },
    RegisterUser {
        #[arg(long)]
        username: String,
        #[arg(long)]
        password: String,
    },
    Authenticate {
        #[arg(long)]
        username: String,
        #[arg(long)]
        password: String,
    },
    GraphQuery {
        #[arg(long, short = 'q')]
        query_string: String,
        #[arg(long)]
        persist: Option<bool>,
    },
    StorageQuery,
    Restart {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] // Added missing field
        config_file: Option<PathBuf>,
    },
    Reload {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] // Added missing field
        config_file: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum StorageAction {
    Start {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
    },
    Stop {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        storage_port: Option<u16>,
    },
    Status {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        storage_port: Option<u16>,
    },
    List,
    Restart {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
    },
    Reload {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
    },
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct StatusArgs {
    #[command(subcommand)]
    pub action: Option<StatusAction>,
    // Top-level flags for status
    #[arg(long, short = 'p')]
    pub port: Option<u16>,
    #[arg(long)]
    pub rest_port: Option<u16>,
    #[arg(long)]
    pub storage_port: Option<u16>,
    #[arg(long, short = 'c')]
    pub cluster: Option<String>,
    #[arg(long)]
    pub rest_cluster: Option<String>,
    #[arg(long)]
    pub daemon_cluster: Option<String>,
    #[arg(long)]
    pub storage_cluster: Option<String>,
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum StatusAction {
    All {
        #[arg(long, short = 'c')]
        cluster: Option<String>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        storage_cluster: Option<String>,
    },
    Daemon {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long, short = 'c')]
        cluster: Option<String>,
    },
    Rest {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long, short = 'c')]
        cluster: Option<String>,
    },
    Storage {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long, short = 'c')]
        cluster: Option<String>,
    },
    Cluster {
        #[arg(long, short = 'c')]
        cluster: Option<String>,
    },
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct StopArgs {
    #[command(subcommand)]
    pub action: Option<StopAction>, // Made action optional
    // Top-level flags for stop
    #[arg(long, short = 'p')]
    pub port: Option<u16>,
    #[arg(long)]
    pub rest_port: Option<u16>,
    #[arg(long)]
    pub storage_port: Option<u16>,
    #[arg(long)]
    pub daemon_port: Option<u16>,
    #[arg(long)]
    pub cluster: Option<String>, // Added top-level cluster
    #[arg(long)]
    pub rest_cluster: Option<String>, // Added top-level rest_cluster
    #[arg(long)]
    pub daemon_cluster: Option<String>, // Added top-level daemon_cluster
    #[arg(long)]
    pub storage_cluster: Option<String>, // Added top-level storage_cluster
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum StopAction {
    All {
        #[arg(long, short = 'p')]
        daemon_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        storage_cluster: Option<String>,
    },
    Daemon {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        cluster: Option<String>, // Added cluster field
    },
    Rest {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        cluster: Option<String>, // Added cluster field
    },
    Storage {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        cluster: Option<String>, // Added cluster field
    },
    Cluster {
        #[arg(long)]
        cluster: Option<String>,
    },
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct ReloadArgs {
    #[command(subcommand)]
    pub action: Option<ReloadAction>, // Made action optional
    // Top-level flags that apply if no subcommand is given (implies ReloadAction::All)
    #[arg(long, short = 'p')]
    pub port: Option<u16>,
    #[arg(long)]
    pub rest_port: Option<u16>,
    #[arg(long)]
    pub storage_port: Option<u16>,
    #[arg(long, short = 'c', alias = "join-cluster")]
    pub cluster: Option<String>,
    #[arg(long)]
    pub rest_cluster: Option<String>,
    #[arg(long)]
    pub storage_cluster: Option<String>,
    #[arg(long)]
    pub join_cluster: Option<bool>,
    #[arg(long)]
    pub join_rest_cluster: Option<bool>,
    #[arg(long)]
    pub join_storage_cluster: Option<bool>,
    #[arg(long)]
    pub config_file: Option<PathBuf>,
    #[arg(long)]
    pub storage_config_file: Option<PathBuf>,
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    pub data_directory: Option<String>,
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    pub log_directory: Option<String>,
    #[arg(long)]
    pub max_disk_space_gb: Option<u64>,
    #[arg(long)]
    pub min_disk_space_gb: Option<u64>,
    #[arg(long)]
    pub use_raft_for_scale: Option<bool>,
    #[arg(long)]
    pub storage_engine_type: Option<String>,
    #[arg(long)]
    pub daemon: Option<bool>,
    #[arg(long)]
    pub rest: Option<bool>,
    #[arg(long)]
    pub storage: Option<bool>,
    #[arg(long)]
    pub daemon_port: Option<u16>,
    #[arg(long)]
    pub listen_port: Option<u16>,
    #[arg(long)]
    pub daemon_cluster: Option<String>,
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum ReloadAction {
    All {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        storage_config_file: Option<PathBuf>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
    },
    Daemon {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        join_daemon_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
    },
    Rest {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
    },
    Storage {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct RestartArgs {
    #[command(subcommand)]
    pub action: Option<RestartAction>,
    // Top-level flags for `restart` command
    #[arg(long, short = 'p')]
    pub port: Option<u16>,
    #[arg(long)]
    pub rest_port: Option<u16>,
    #[arg(long)]
    pub storage_port: Option<u16>,
    #[arg(long, short = 'c', alias = "join-cluster")]
    pub cluster: Option<String>,
    #[arg(long)]
    pub rest_cluster: Option<String>,
    #[arg(long)]
    pub storage_cluster: Option<String>,
    #[arg(long)]
    pub join_cluster: Option<bool>,
    #[arg(long)]
    pub join_rest_cluster: Option<bool>,
    #[arg(long)]
    pub join_storage_cluster: Option<bool>,
    #[arg(long)]
    pub config_file: Option<PathBuf>,
    #[arg(long)]
    pub storage_config_file: Option<PathBuf>,
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    pub data_directory: Option<String>,
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    pub log_directory: Option<String>,
    #[arg(long)]
    pub max_disk_space_gb: Option<u64>,
    #[arg(long)]
    pub min_disk_space_gb: Option<u64>,
    #[arg(long)]
    pub use_raft_for_scale: Option<bool>,
    #[arg(long)]
    pub storage_engine_type: Option<String>,
    #[arg(long)]
    pub daemon: Option<bool>,
    #[arg(long)]
    pub rest: Option<bool>,
    #[arg(long)]
    pub storage: Option<bool>,
    #[arg(long)]
    pub daemon_port: Option<u16>,
    #[arg(long)]
    pub listen_port: Option<u16>,
    #[arg(long)]
    pub daemon_cluster: Option<String>,
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum RestartAction {
    All {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
        #[arg(long)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        storage_config_file: Option<PathBuf>,
        #[arg(long, value_hint = ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
    },
    Daemon {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long, value_hint = ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        join_daemon_cluster: Option<bool>,
    },
    Rest {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] // Added missing field
        config_file: Option<PathBuf>,
    },
    Storage {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, value_hint = ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
    },
    Cluster {
        #[arg(long, short = 'c')] // Added missing cluster field
        cluster: Option<String>,
    },
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct StartArgs {
    #[command(subcommand)]
    pub action: Option<StartAction>,
    #[arg(long, short = 'p')]
    pub port: Option<u16>,
    #[arg(long)]
    pub rest_port: Option<u16>,
    #[arg(long)]
    pub storage_port: Option<u16>,
    #[arg(long, short = 'c', alias = "join-cluster")]
    pub cluster: Option<String>,
    #[arg(long)]
    pub rest_cluster: Option<String>,
    #[arg(long)]
    pub storage_cluster: Option<String>,
    #[arg(long)]
    pub join_cluster: Option<bool>,
    #[arg(long)]
    pub join_rest_cluster: Option<bool>,
    #[arg(long)]
    pub join_storage_cluster: Option<bool>,
    #[arg(long, value_hint = clap::ValueHint::FilePath)]
    pub storage_config_file: Option<PathBuf>,
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    pub data_directory: Option<String>,
    #[arg(long, value_hint = clap::ValueHint::DirPath)]
    pub log_directory: Option<String>,
    #[arg(long)]
    pub max_disk_space_gb: Option<u64>,
    #[arg(long)]
    pub min_disk_space_gb: Option<u64>,
    #[arg(long)]
    pub use_raft_for_scale: Option<bool>,
    #[arg(long)]
    pub storage_engine_type: Option<String>,
    #[arg(long)]
    pub config_file: Option<PathBuf>,
    #[arg(long)]
    pub daemon: Option<bool>,
    #[arg(long)]
    pub rest: Option<bool>,
    #[arg(long)]
    pub storage: Option<bool>,
    #[arg(long)]
    pub daemon_port: Option<u16>,
    #[arg(long)]
    pub listen_port: Option<u16>,
    #[arg(long)]
    pub daemon_cluster: Option<String>,
}

impl StartArgs {
    /// Checks if any of the start-related flags are explicitly set.
    /// This helps differentiate between `graphdb-cli` (no args) and `graphdb-cli start` (no subcommand, but maybe flags).
    pub fn is_any_set(&self) -> bool {
        self.port.is_some()
            || self.rest_port.is_some()
            || self.storage_port.is_some()
            || self.cluster.is_some()
            || self.rest_cluster.is_some()
            || self.storage_cluster.is_some()
            || self.join_cluster.is_some()
            || self.join_rest_cluster.is_some()
            || self.join_storage_cluster.is_some()
            || self.storage_config_file.is_some()
            || self.data_directory.is_some()
            || self.log_directory.is_some()
            || self.max_disk_space_gb.is_some()
            || self.min_disk_space_gb.is_some()
            || self.use_raft_for_scale.is_some()
            || self.storage_engine_type.is_some()
            || self.config_file.is_some()
            || self.daemon.is_some()
            || self.rest.is_some()
            || self.storage.is_some()
            || self.daemon_port.is_some()
            || self.listen_port.is_some()
            || self.daemon_cluster.is_some()
    }
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum StartAction {
    All {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
        #[arg(long)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        storage_config_file: Option<PathBuf>,
        #[arg(long, value_hint = ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
    },
    Daemon {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long)]
        daemon_port: Option<u16>,
        #[arg(long)]
        daemon_cluster: Option<String>,
        #[arg(long)]
        join_daemon_cluster: Option<bool>,
    },
    Rest {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        listen_port: Option<u16>,
        #[arg(long)]
        rest_port: Option<u16>,
        #[arg(long)]
        rest_cluster: Option<String>,
        #[arg(long)]
        join_rest_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] // Added missing field
        config_file: Option<PathBuf>,
    },
    Storage {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[arg(long)]
        max_disk_space_gb: Option<u64>,
        #[arg(long)]
        min_disk_space_gb: Option<u64>,
        #[arg(long)]
        use_raft_for_scale: Option<bool>,
        #[arg(long)]
        storage_engine_type: Option<String>,
        #[arg(long)]
        daemon: Option<bool>,
        #[arg(long)]
        rest: Option<bool>,
        #[arg(long)]
        storage: Option<bool>,
        #[arg(long)]
        storage_port: Option<u16>,
        #[arg(long)]
        storage_cluster: Option<String>,
        #[arg(long)]
        join_storage_cluster: Option<bool>,
    },
    Cluster {
        #[arg(long)]
        join_cluster: Option<bool>,
        #[arg(long)]
        cluster: Option<String>,
    },
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct ClearDataArgs {
    #[command(subcommand)]
    pub action: ClearDataAction,
}

#[derive(Debug, Subcommand, PartialEq, Clone)]
pub enum ClearDataAction {
    All,
    Storage {
        #[arg(long, short = 'p')]
        port: Option<u16>,
        // Removed storage_port: Option<u16>, as per previous comment/error
    },
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct RegisterUserArgs {
    #[arg(long)]
    pub username: String,
    #[arg(long)]
    pub password: String,
    // Removed email: Option<String>, as per error log
}

#[derive(Debug, Args, PartialEq, Clone)]
pub struct AuthArgs {
    #[arg(long)]
    pub username: String,
    #[arg(long)]
    pub password: String,
}

// Define the top-level CommandType enum for interactive mode
// This enum is intended to be used by interactive.rs
#[derive(Debug, PartialEq, Clone, Subcommand)]
pub enum CommandType {
    // Subcommands that delegate to more complex enums
    #[command(subcommand)]
    Daemon(DaemonCliCommand),
    #[command(subcommand)]
    Rest(RestCliCommand),
    #[command(subcommand)]
    Storage(StorageAction),

    // Start Commands (flattened from StartArgs/StartAction)
    StartRest {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, short = 'c')] cluster: Option<String>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_rest_cluster: Option<bool>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] 
        config_file: Option<PathBuf>, // Added missing field
    },
    StartStorage {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] config_file: Option<PathBuf>,
        #[arg(long, short = 'c')] cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] log_directory: Option<String>,
        #[arg(long)] max_disk_space_gb: Option<u64>,
        #[arg(long)] min_disk_space_gb: Option<u64>,
        #[arg(long)] use_raft_for_scale: Option<bool>,
        #[arg(long)] storage_engine_type: Option<String>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_storage_cluster: Option<bool>,
        #[arg(long)] storage_cluster: Option<String>,
        #[arg(long)] storage_port: Option<u16>,
    },
    StartDaemon {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, short = 'c')] cluster: Option<String>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] daemon_cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] config_file: Option<PathBuf>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_daemon_cluster: Option<bool>,
    },
    StartAll {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long)] storage_cluster: Option<String>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_rest_cluster: Option<bool>,
        #[arg(long)] join_storage_cluster: Option<bool>,
        #[arg(long)] config_file: Option<PathBuf>,
        #[arg(long)] storage_config_file: Option<PathBuf>,
        #[arg(long, value_hint = ValueHint::DirPath)] data_directory: Option<String>,
        #[arg(long, value_hint = ValueHint::DirPath)] log_directory: Option<String>,
        #[arg(long)] max_disk_space_gb: Option<u64>,
        #[arg(long)] min_disk_space_gb: Option<u64>,
        #[arg(long)] use_raft_for_scale: Option<bool>,
        #[arg(long)] storage_engine_type: Option<String>,
        #[arg(long)] daemon: Option<bool>,
        #[arg(long)] rest: Option<bool>,
        #[arg(long)] storage: Option<bool>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] daemon_cluster: Option<String>,
    },
    StartCluster {
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] cluster: Option<String>,
    },

    // Stop Commands (flattened from StopArgs/StopAction)
    StopAll {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long)] daemon_cluster: Option<String>,
        #[arg(long)] storage_cluster: Option<String>,
    },
    StopRest {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] rest_cluster: Option<String>,
    },
    StopDaemon {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] daemon_cluster: Option<String>,
    },
    StopStorage {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long)] storage_cluster: Option<String>,
    },
    StopCluster {
        #[arg(long)] cluster: Option<String>,
    },

    // Status Commands (flattened from StatusArgs/StatusAction)
    StatusSummary { // Equivalent to StatusAction::All
        #[arg(long, short = 'c')] cluster: Option<String>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long)] daemon_cluster: Option<String>,
        #[arg(long)] storage_cluster: Option<String>,
    },
    StatusRest {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long, short = 'c')] cluster: Option<String>,
        #[arg(long)] rest_cluster: Option<String>,
    },
    StatusDaemon {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long, short = 'c')] cluster: Option<String>,
        #[arg(long)] daemon_cluster: Option<String>,
    },
    StatusStorage {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long, short = 'c')] cluster: Option<String>,
        #[arg(long)] storage_cluster: Option<String>,
    },
    StatusCluster {
        #[arg(long, short = 'c')] cluster: Option<String>,
    },

    // Reload Commands (flattened from ReloadArgs/ReloadAction)
    ReloadAll {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long)] storage_cluster: Option<String>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_rest_cluster: Option<bool>,
        #[arg(long)] join_storage_cluster: Option<bool>,
        #[arg(long)] daemon: Option<bool>,
        #[arg(long)] rest: Option<bool>,
        #[arg(long)] storage: Option<bool>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] daemon_cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] config_file: Option<PathBuf>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] storage_config_file: Option<PathBuf>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] log_directory: Option<String>,
        #[arg(long)] max_disk_space_gb: Option<u64>,
        #[arg(long)] min_disk_space_gb: Option<u64>,
        #[arg(long)] use_raft_for_scale: Option<bool>,
        #[arg(long)] storage_engine_type: Option<String>,
    },
    ReloadDaemon {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] daemon: Option<bool>,
        #[arg(long)] rest: Option<bool>,
        #[arg(long)] storage: Option<bool>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] daemon_cluster: Option<String>,
        #[arg(long)] join_daemon_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] config_file: Option<PathBuf>,
    },
    ReloadRest {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] daemon: Option<bool>,
        #[arg(long)] rest: Option<bool>,
        #[arg(long)] storage: Option<bool>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long)] join_rest_cluster: Option<bool>,
    },
    ReloadStorage {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] log_directory: Option<String>,
        #[arg(long)] max_disk_space_gb: Option<u64>,
        #[arg(long)] min_disk_space_gb: Option<u64>,
        #[arg(long)] use_raft_for_scale: Option<bool>,
        #[arg(long)] storage_engine_type: Option<String>,
        #[arg(long)] daemon: Option<bool>,
        #[arg(long)] rest: Option<bool>,
        #[arg(long)] storage: Option<bool>,
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long)] storage_cluster: Option<String>,
        #[arg(long)] join_storage_cluster: Option<bool>,
    },
    ReloadCluster {
        #[arg(long, short = 'c')] cluster: Option<String>,
    },

    // Restart Commands (flattened from RestartArgs/RestartAction)
    RestartAll {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long)] storage_cluster: Option<String>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_rest_cluster: Option<bool>,
        #[arg(long)] join_storage_cluster: Option<bool>,
        #[arg(long)] config_file: Option<PathBuf>,
        #[arg(long)] storage_config_file: Option<PathBuf>,
        #[arg(long, value_hint = ValueHint::DirPath)] data_directory: Option<String>,
        #[arg(long, value_hint = ValueHint::DirPath)] log_directory: Option<String>,
        #[arg(long)] max_disk_space_gb: Option<u64>,
        #[arg(long)] min_disk_space_gb: Option<u64>,
        #[arg(long)] use_raft_for_scale: Option<bool>,
        #[arg(long)] storage_engine_type: Option<String>,
        #[arg(long)] daemon: Option<bool>,
        #[arg(long)] rest: Option<bool>,
        #[arg(long)] storage: Option<bool>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] daemon_cluster: Option<String>,
    },
    RestartDaemon {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] daemon_port: Option<u16>,
        #[arg(long)] daemon_cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] config_file: Option<PathBuf>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_daemon_cluster: Option<bool>,
    },
    RestartRest {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long)] listen_port: Option<u16>,
        #[arg(long)] rest_port: Option<u16>,
        #[arg(long)] join_cluster: Option<bool>,
        #[arg(long)] join_rest_cluster: Option<bool>,
        #[arg(long)] rest_cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] // Added missing field
        config_file: Option<PathBuf>,
    },
    RestartStorage {
        #[arg(long, short = 'p')] port: Option<u16>,
        #[arg(long, value_hint = clap::ValueHint::FilePath)] config_file: Option<PathBuf>,
        #[arg(long, short = 'c', alias = "join-cluster")] cluster: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] data_directory: Option<String>,
        #[arg(long, value_hint = clap::ValueHint::DirPath)] log_directory: Option<String>,
        #[arg(long)] max_disk_space_gb: Option<u64>,
        #[arg(long)] min_disk_space_gb: Option<u64>,
        #[arg(long)] use_raft_for_scale: Option<bool>,
        #[arg(long)] storage_engine_type: Option<String>,
        #[arg(long)] daemon: Option<bool>, // Added missing field
        #[arg(long)] rest: Option<bool>, // Added missing field
        #[arg(long)] storage: Option<bool>, // Added missing field
        #[arg(long)] storage_port: Option<u16>,
        #[arg(long)] storage_cluster: Option<String>,
        #[arg(long)] join_storage_cluster: Option<bool>,
        #[arg(long)] join_cluster: Option<bool>,
    },
    RestartCluster {
        #[arg(long, short = 'c')] cluster: Option<String>, // Added missing cluster field
    },

    // Clear Data Command (flattened from ClearDataArgs/ClearDataAction)
    ClearAllData {
        
    }, // Equivalent to ClearDataAction::All
    ClearStorageData {
        #[arg(long, short = 'p')] port: Option<u16>,
        // Remove storage_port: Option<u16>,
    },

    // User Management Commands
    RegisterUser {
        #[arg(long)] username: String,
        #[arg(long)] password: String,
    },
    Authenticate {
        #[arg(long)] username: String,
        #[arg(long)] password: String,
    },

    // Query Commands
    Query(QueryArgs),
    GraphQuery(GraphQueryArgs),
    StorageQuery {

    },

    // Information Commands
    Version,
    Health,

    // Utility Commands
    Clear, // For clearing terminal screen
    Help(HelpArgs),
    Exit,
    Unknown,
}

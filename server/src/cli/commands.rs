// This file defines the command-line arguments and subcommands
// for the GraphDB CLI using the `clap` crate.
// FIX: 2025-07-12 - Added cluster field to StartAction::Rest to support --cluster and --join-cluster flags, aligning with RestartAction::Rest and interactive.rs parsing.
// FIX: 2025-07-12 - Added alias "join-cluster" to cluster fields in StartAction and RestartAction to explicitly support --join-cluster flag.
// FIX: 2025-07-12 - Verified RestartAction::Storage includes cluster field to resolve E0026 and E0559 errors in interactive.rs.
// FIX: 2025-07-12 - Ensured daemon, rest, and storage fields in RestartAction variants are Option<bool> to align with interactive.rs fixes for E0063 and E0308.
// FIX: 2025-07-12 - Made StartArgs.action optional and added top-level flags to StartArgs to support direct flag usage without subcommand.
// NEW: 2025-07-12 - Added daemon and storage fields to StartAction::Rest and RestartAction::Rest to resolve E0063 errors in interactive.rs.
// NEW: 2025-07-12 - Added daemon and rest fields to StorageAction::Start to align with StartAction::Storage and RestartAction::Storage for consistency and reuse.

use clap::{Args, Subcommand};
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct DaemonCommandWrapper {
    #[clap(subcommand)]
    pub command: DaemonCliCommand,
}

#[derive(Debug, Args)]
pub struct RestCommandWrapper {
    #[clap(subcommand)]
    pub command: RestCliCommand,
}

#[derive(Debug, Args)]
pub struct StorageActionWrapper {
    #[clap(subcommand)]
    pub command: StorageAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum DaemonCliCommand {
    Start {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
    },
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Status {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    // These variants for Restart/Reload directly under DaemonCliCommand
    // are only for `graphdb-cli daemon restart` and `graphdb-cli daemon reload`.
    // The main `graphdb-cli restart` command uses RestartArgs/RestartAction.
    Restart {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
    },
    Reload {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
    },
    List,
    ClearAll,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum RestCliCommand {
    Start {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Status {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Version,
    Health,
    Query {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'q')]
        query: String,
    },
    RegisterUser {
        #[clap(long)]
        username: String,
        #[clap(long)]
        password: String,
    },
    Authenticate {
        #[clap(long)]
        username: String,
        #[clap(long)]
        password: String,
    },
    GraphQuery {
        #[clap(long, short = 'q')]
        query_string: String,
        #[clap(long)]
        persist: Option<bool>,
    },
    StorageQuery,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum StorageAction {
    Start {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
    },
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Status {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    List,
}

#[derive(Debug, Args, PartialEq)]
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: StatusAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum StatusAction {
    All,
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: StopAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum StopAction {
    All,
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct ReloadArgs {
    #[clap(subcommand)]
    pub action: ReloadAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum ReloadAction {
    All {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        rest: Option<bool>, // Should these apply to daemon? Usually not
        #[clap(long)]
        storage: Option<bool>, // Should these apply to daemon? Usually not
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>, // Cluster generally applies to storage/daemon, less to rest directly
        #[clap(long)]
        daemon: Option<bool>, // Should these apply to rest? Usually not
        #[clap(long)]
        storage: Option<bool>, // Should these apply to rest? Usually not
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[clap(long)]
        daemon: Option<bool>, // Should these apply to storage? Usually not
        #[clap(long)]
        rest: Option<bool>, // Should these apply to storage? Usually not
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct RestartArgs {
    #[clap(subcommand)]
    pub action: Option<RestartAction>, // Make action optional to allow top-level flags
    // Top-level flags that apply if no subcommand is given (implies RestartAction::All)
    #[clap(long, short = 'p')]
    pub port: Option<u16>,
    #[clap(long, short = 'c', alias = "join-cluster")]
    pub cluster: Option<String>,
    #[clap(long)]
    pub config_file: Option<PathBuf>, // General config for all or specific?
    #[clap(long)]
    pub listen_port: Option<u16>, // For daemon/rest
    #[clap(long)]
    pub storage_port: Option<u16>, // For storage
    #[clap(long, value_hint = clap::ValueHint::FilePath)]
    pub storage_config_file: Option<PathBuf>, // Specific for storage
    #[clap(long, value_hint = clap::ValueHint::DirPath)]
    pub data_directory: Option<String>, // Specific for storage
    #[clap(long, value_hint = clap::ValueHint::DirPath)]
    pub log_directory: Option<String>, // Specific for storage/daemon/rest
    #[clap(long)]
    pub max_disk_space_gb: Option<u64>, // Specific for storage
    #[clap(long)]
    pub min_disk_space_gb: Option<u64>, // Specific for storage
    #[clap(long)]
    pub use_raft_for_scale: Option<bool>, // Specific for storage
    #[clap(long)]
    pub storage_engine_type: Option<String>, // Specific for storage
    #[clap(long)]
    pub daemon: Option<bool>, // Used with `restart all` or top-level to explicitly target daemon
    #[clap(long)]
    pub rest: Option<bool>,   // Used with `restart all` or top-level to explicitly target rest
    #[clap(long)]
    pub storage: Option<bool>, // Used with `restart all` or top-level to explicitly target storage
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum RestartAction {
    All {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        config_file: Option<PathBuf>,
        #[clap(long)]
        listen_port: Option<u16>,
        #[clap(long)]
        storage_port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        storage_config_file: Option<PathBuf>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        // These fields (rest, storage) might be redundant/confusing under Daemon
        // as they imply restarting other services *from* the daemon command.
        // If the intent is `restart daemon --rest --storage`, it's better handled by `restart all --daemon --rest --storage`.
        // I'll leave them for now as they were in your previous version, but note the ambiguity.
        #[clap(long)]
        daemon: Option<bool>, // Redundant, implied by Daemon subcommand
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>, // Cluster usually not directly relevant for REST but kept for consistency
        #[clap(long)]
        daemon: Option<bool>, // Redundant here
        #[clap(long)]
        storage: Option<bool>, // Redundant here
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>, // Redundant here
        #[clap(long)]
        rest: Option<bool>,   // Redundant here
    },
    Cluster, // A restart for the entire cluster (could imply specific actions on all nodes)
}

#[derive(Debug, Args, PartialEq)]
pub struct StartArgs {
    #[clap(subcommand)]
    pub action: Option<StartAction>,
    #[clap(long, short = 'p')]
    pub port: Option<u16>,
    #[clap(long, short = 'c', alias = "join-cluster")]
    pub cluster: Option<String>,
    #[clap(long)]
    pub listen_port: Option<u16>,
    #[clap(long)]
    pub storage_port: Option<u16>,
    #[clap(long, value_hint = clap::ValueHint::FilePath)]
    pub storage_config_file: Option<PathBuf>,
    #[clap(long, value_hint = clap::ValueHint::DirPath)]
    pub data_directory: Option<String>,
    #[clap(long, value_hint = clap::ValueHint::DirPath)]
    pub log_directory: Option<String>,
    #[clap(long)]
    pub max_disk_space_gb: Option<u64>,
    #[clap(long)]
    pub min_disk_space_gb: Option<u64>,
    #[clap(long)]
    pub use_raft_for_scale: Option<bool>,
    #[clap(long)]
    pub storage_engine_type: Option<String>,
    #[clap(long)]
    pub daemon: Option<bool>,
    #[clap(long)]
    pub rest: Option<bool>,
    #[clap(long)]
    pub storage: Option<bool>,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum StartAction {
    All {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        config_file: Option<PathBuf>,
        #[clap(long)]
        listen_port: Option<u16>,
        #[clap(long)]
        storage_port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        storage_config_file: Option<PathBuf>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Daemon {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Rest {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        storage: Option<bool>,
    },
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long, value_hint = clap::ValueHint::FilePath)]
        config_file: Option<PathBuf>,
        #[clap(long, short = 'c', alias = "join-cluster")]
        cluster: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        data_directory: Option<String>,
        #[clap(long, value_hint = clap::ValueHint::DirPath)]
        log_directory: Option<String>,
        #[clap(long)]
        max_disk_space_gb: Option<u64>,
        #[clap(long)]
        min_disk_space_gb: Option<u64>,
        #[clap(long)]
        use_raft_for_scale: Option<bool>,
        #[clap(long)]
        storage_engine_type: Option<String>,
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
    },
}

#[derive(Debug, Args, PartialEq)]
pub struct ClearDataArgs {
    #[clap(subcommand)]
    pub action: ClearDataAction,
}

#[derive(Debug, Subcommand, PartialEq)]
pub enum ClearDataAction {
    All,
    Storage {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
}

#[derive(Debug, Args, PartialEq)]
pub struct RegisterUserArgs {
    #[clap(long)]
    pub username: String,
    #[clap(long)]
    pub password: String,
}

#[derive(Debug, Args, PartialEq)]
pub struct AuthArgs {
    #[clap(long)]
    pub username: String,
    #[clap(long)]
    pub password: String,
}

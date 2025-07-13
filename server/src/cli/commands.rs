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
        #[clap(long)]
        daemon: Option<bool>,
        #[clap(long)]
        rest: Option<bool>,
    },
    Cluster,
}

#[derive(Debug, Args, PartialEq)]
pub struct RestartArgs {
    #[clap(subcommand)]
    pub action: Option<RestartAction>,
    #[clap(long, short = 'p')]
    pub port: Option<u16>,
    #[clap(long, short = 'c', alias = "join-cluster")]
    pub cluster: Option<String>,
    #[clap(long)]
    pub config_file: Option<PathBuf>,
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
    Cluster,
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

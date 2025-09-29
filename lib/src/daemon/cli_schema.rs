// daemon_api/src/cli_schema.rs

use clap::{Parser, Subcommand, Args};
use std::path::PathBuf;
use crate::storage_engine::config::StorageEngineType;

#[derive(Parser, Debug)]
#[clap(author, version, about = "GraphDB CLI for managing graph data and daemons", long_about = None)]
pub struct CliArgs {
    #[clap(long)]
    pub cli: bool,
    #[clap(long, conflicts_with = "cli")]
    pub query: Option<String>,
    #[clap(subcommand)]
    pub command: Option<GraphDbCommands>,
}

#[derive(Subcommand, Debug)]
pub enum GraphDbCommands {
    Start(StartArgs),
    Stop(StopArgs),
    Status(StatusArgs),
    Daemon(DaemonCommandWrapper),
    Rest(RestCommandWrapper),
    Storage(StorageActionWrapper),
    #[clap(arg_required_else_help = true)]
    ViewGraph {
        #[clap(long)]
        graph_id: Option<String>,
    },
    #[clap(arg_required_else_help = true)]
    ViewGraphHistory {
        #[clap(long)]
        graph_id: Option<String>,
        #[clap(long)]
        start_date: Option<String>,
        #[clap(long)]
        end_date: Option<String>,
    },
    #[clap(arg_required_else_help = true)]
    IndexNode {
        #[clap(long)]
        node_id: Option<String>,
    },
    #[clap(arg_required_else_help = true)]
    CacheNodeState {
        #[clap(long)]
        node_id: Option<String>,
    },
    Help(HelpArgs),
}

#[derive(Args, Debug)]
pub struct StartArgs {
    #[clap(long, conflicts_with = "cluster")]
    pub port: Option<u16>,
    #[clap(long)]
    pub cluster: Option<String>,
    #[clap(long)]
    pub listen_port: Option<u16>,
    #[clap(long)]
    pub storage_port: Option<u16>,
    #[clap(long)]
    pub storage_config_file: Option<PathBuf>,
    #[clap(subcommand)]
    pub component: Option<StartComponent>,
}

#[derive(Subcommand, Debug)]
pub enum StartComponent {
    Rest(StartRestArgs),
    Storage(StartStorageArgs),
}

#[derive(Args, Debug)]
pub struct StartRestArgs {
    #[clap(long)]
    pub port: Option<u16>,
}

#[derive(Args, Debug)]
pub struct StartStorageArgs {
    #[clap(long)]
    pub port: Option<u16>,
    #[clap(long)]
    pub config_file: Option<PathBuf>,
    #[clap(long, value_enum)]
    pub engine_type: Option<StorageEngineType>,
}

#[derive(Args, Debug)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: Option<StopAction>,
}

#[derive(Subcommand, Debug)]
pub enum StopAction {
    Rest,
    Daemon {
        #[clap(long)]
        port: Option<u16>,
    },
    Storage {
        #[clap(long)]
        port: Option<u16>,
    },
}

#[derive(Args, Debug)]
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: Option<StatusAction>,
}

#[derive(Subcommand, Debug)]
pub enum StatusAction {
    Rest,
    Daemon {
        #[clap(long)]
        port: Option<u16>,
    },
    Storage {
        #[clap(long)]
        port: Option<u16>,
    },
}

#[derive(Args, Debug)]
pub struct DaemonCommandWrapper {
    #[clap(subcommand)]
    pub command: DaemonCliCommand,
}

#[derive(Subcommand, Debug)]
pub enum DaemonCliCommand {
    Start {
        #[clap(long)]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    Stop {
        #[clap(long)]
        port: Option<u16>,
    },
    Status {
        #[clap(long)]
        port: Option<u16>,
    },
    List,
    ClearAll,
}

#[derive(Args, Debug)]
pub struct RestCommandWrapper {
    #[clap(subcommand)]
    pub command: RestCliCommand,
}

#[derive(Subcommand, Debug)]
pub enum RestCliCommand {
    Start {
        #[clap(long)]
        port: Option<u16>,
    },
    Stop,
    Status,
    Health,
    Version,
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
        query_string: String,
        #[clap(long)]
        persist: Option<bool>,
    },
    StorageQuery,
}

#[derive(Args, Debug)]
pub struct StorageActionWrapper {
    #[clap(subcommand)]
    pub action: StorageAction,
}

#[derive(Subcommand, Debug)]
pub enum StorageAction {
    Start {
        #[clap(long)]
        port: Option<u16>,
        #[clap(long)]
        config_file: Option<PathBuf>,
        #[clap(long, value_enum)]
        engine_type: Option<StorageEngineType>,
    },
    Stop {
        #[clap(long)]
        port: Option<u16>,
    },
    Status {
        #[clap(long)]
        port: Option<u16>,
    },
}

#[derive(Args, Debug)]
pub struct HelpArgs {
    pub command_path: Vec<String>,
}

#[derive(Debug)]
pub struct KVPair {
    pub key: String,
    pub value: String,
}

#[derive(Debug)]
pub struct PidStore {
    pub pid: u32,
    pub port: u16,
    pub component: String,
}

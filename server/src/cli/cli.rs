// server/src/cli/cli.rs

use clap::Parser;
use std::process;
use std::env;
use std::collections::HashMap;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use std::sync::Arc;

use crate::cli::{
    commands::{CliArgs, GraphDbCommands, StartAction, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs},
    handlers::{self as handlers_mod, print_welcome_screen},
    interactive::{self as interactive_mod, run_cli_interactive},
    daemon_management,
    config::{self as config_mod},
    help_display::{self as help_display_mod},
};

use anyhow::Result;
use clap::CommandFactory;
use lib::storage_engine::config::StorageEngineType as LibStorageEngineType;

/// The main entry point for the CLI logic, called by server/src/main.rs
/// This function parses command-line arguments and dispatches to appropriate handlers.
#[tokio::main]
pub async fn start_cli() -> Result<()> {
    // --- Custom Help Command Handling ---
    let args_vec: Vec<String> = env::args().collect();
    if args_vec.len() > 1 && args_vec[1].to_lowercase() == "help" {
        let help_command_args: Vec<String> = args_vec.into_iter().skip(2).collect();
        let filter_command = if help_command_args.is_empty() {
            "".to_string()
        } else {
            help_command_args.join(" ")
        };
        let mut cmd = CliArgs::command();
        help_display_mod::print_filtered_help_clap_generated(&mut cmd, &filter_command);
        process::exit(0);
    }
    // --- End Custom Help Command Handling ---

    let args = CliArgs::parse();

    if args.internal_rest_api_run || args.internal_storage_daemon_run {
        let converted_storage_engine = args.internal_storage_engine.map(|se_cli| {
            match se_cli {
                config_mod::StorageEngineType::Sled => LibStorageEngineType::Sled,
                config_mod::StorageEngineType::RocksDB => LibStorageEngineType::RocksDB,
                config_mod::StorageEngineType::InMemory => LibStorageEngineType::InMemory,
            }
        });

        return daemon_management::handle_internal_daemon_run(
            args.internal_rest_api_run,
            args.internal_storage_daemon_run,
            args.internal_port,
            args.internal_storage_config_path,
            converted_storage_engine,
        ).await;
    }

    let config = match config_mod::load_cli_config() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading configuration: {}", e);
            eprintln!("Attempted to load from: {}", std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join("cli")
                .join("config.toml")
                .display());
            process::exit(1);
        }
    };

    if let Some(query_string) = args.query {
        println!("Executing direct query: {}", query_string);
        use lib::query_parser::{parse_query_from_string, QueryType};
        match parse_query_from_string(&query_string) {
            Ok(parsed_query) => match parsed_query {
                QueryType::Cypher => println!("  -> Identified as Cypher query."),
                QueryType::SQL => println!("  -> Identified as SQL query."),
                QueryType::GraphQL => println!("  -> Identified as GraphQL query."),
            },
            Err(e) => eprintln!("Error parsing query: {}", e),
        }
        return Ok(());
    }

    // Shared state for interactive mode to manage daemon processes
    let daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let rest_api_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None));
    let rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));

    let should_enter_interactive_mode = args.cli || args.command.is_none();

    if let Some(command) = args.command {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        match command {
            GraphDbCommands::ViewGraph { graph_id } => {
                if let Some(id) = graph_id {
                    println!("Executing view-graph for graph ID: {}", id);
                } else {
                    println!("Executing view-graph without specifying a graph ID");
                }
            }
            GraphDbCommands::ViewGraphHistory { graph_id, start_date, end_date } => {
                if let Some(graph_id) = graph_id {
                    println!("Executing view-graph-history for graph ID: {}", graph_id);
                } else {
                    println!("Executing view-graph-history with no graph ID specified");
                }
                if let Some(start) = start_date {
                    println!("Start Date: {}", start);
                } else {
                    println!("Start Date: not specified");
                }
                if let Some(end_val) = end_date {
                    println!("End Date: {}", end_val);
                } else {
                    println!("End Date: not specified");
                }
            }
            GraphDbCommands::IndexNode { node_id } => {
                if let Some(id) = node_id {
                    println!("Executing index-node for node ID: {}", id);
                } else {
                    println!("Executing index-node with no node ID specified");
                }
            }
            GraphDbCommands::CacheNodeState { node_id } => {
                if let Some(id) = node_id {
                    println!("Executing cache-node-state for node ID: {}", id);
                } else {
                    println!("Executing cache-node-state with no node ID specified");
                }
            }
            GraphDbCommands::Start(start_args) => {
                match start_args.action {
                    StartAction::All { port, cluster, listen_port, storage_port, storage_config_file } => {
                        handlers_mod::handle_start_all_interactive(
                            port,
                            cluster,
                            listen_port,
                            storage_port,
                            storage_config_file,
                            daemon_handles.clone(),
                            rest_api_shutdown_tx_opt.clone(),
                            rest_api_port_arc.clone(),
                            rest_api_handle.clone(),
                        ).await?;
                    }
                    StartAction::Daemon { port, cluster } => {
                        handlers_mod::handle_daemon_command_interactive(
                            DaemonCliCommand::Start { port, cluster },
                            daemon_handles.clone(),
                        ).await?;
                    }
                    StartAction::Rest { port, listen_port } => {
                        handlers_mod::handle_rest_command_interactive(
                            RestCliCommand::Start { port, listen_port },
                            rest_api_shutdown_tx_opt.clone(),
                            rest_api_port_arc.clone(),
                            rest_api_handle.clone(),
                        ).await?;
                    }
                    StartAction::Storage { port, config_file } => {
                        handlers_mod::handle_storage_command_interactive(
                            StorageAction::Start { port, config_file },
                        ).await?;
                    }
                }
            }
            GraphDbCommands::Stop(stop_args) => {
                handlers_mod::handle_stop_command(stop_args).await?;
            }
            GraphDbCommands::Status(status_args) => {
                handlers_mod::handle_status_command(status_args).await?;
            }
            GraphDbCommands::Reload(reload_args) => {
                handlers_mod::handle_reload_command(
                    reload_args,
                    daemon_handles.clone(),
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                ).await?;
            }
            GraphDbCommands::Storage(storage_action) => {
                handlers_mod::handle_storage_command_interactive(storage_action).await?;
            }
            GraphDbCommands::Daemon(daemon_cmd) => {
                handlers_mod::handle_daemon_command_interactive(daemon_cmd, daemon_handles.clone()).await?;
            }
            GraphDbCommands::Rest(rest_cmd) => {
                handlers_mod::handle_rest_command_interactive(
                    rest_cmd,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                ).await?;
            }
            GraphDbCommands::Auth(auth_args) => {
                println!("Auth command received: {:?}", auth_args);
            }
            GraphDbCommands::Authenticate(auth_args) => {
                println!("Authenticate command received: {:?}", auth_args);
            }
            GraphDbCommands::Register(register_args) => {
                println!("Register command received: {:?}", register_args);
            }
            GraphDbCommands::Version => {
                println!("Version command received.");
            }
            GraphDbCommands::Health => {
                println!("Health command received.");
            }
            GraphDbCommands::Clear | GraphDbCommands::Clean => {
                handlers_mod::clear_terminal_screen().await?;
            }
        }
        return Ok(());
    }

    if should_enter_interactive_mode {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        // print_welcome_screen(); // Removed: This is now handled by interactive_mod::run_cli_interactive
        interactive_mod::run_cli_interactive(
            daemon_handles,
            rest_api_shutdown_tx_opt,
            rest_api_port_arc,
            rest_api_handle,
        ).await?;
    } else if args.enable_plugins {
        println!("Experimental plugins is enabled.");
    }

    Ok(())
}


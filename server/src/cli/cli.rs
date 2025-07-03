// server/src/cli/cli.rs

// This file serves as the main entry point for the GraphDB CLI executable.
// It parses command-line arguments and dispatches them to appropriate handlers
// in other modules.

use anyhow::Result;
use clap::{CommandFactory, Parser}; // Added Parser import
use std::process; // For process::exit
// Removed unused import: use std::path::PathBuf;

// Import necessary items from the cli module (which re-exports from its sub-modules)
use crate::cli::{
    commands::{CliArgs, GraphDbCommands},
    handlers,
    interactive,
    daemon_management,
    config,
    help_display,
};

/// The main entry point for the CLI logic, called by server/src/main.rs
/// This function parses command-line arguments and dispatches to appropriate handlers.
#[tokio::main]
pub async fn start_cli() -> Result<()> {
    let args = CliArgs::parse();

    // Handle internal daemon runs first. These are special invocations
    // where the CLI executable is run as a background daemon.
    if args.internal_rest_api_run || args.internal_storage_daemon_run {
        return daemon_management::handle_internal_daemon_run(
            args.internal_rest_api_run,
            args.internal_storage_daemon_run,
            args.internal_port,
            args.internal_storage_config_path,
            args.internal_storage_engine,
        ).await;
    }

    // Load CLI configuration. If it fails, print an error and exit.
    let config = match config::load_cli_config() {
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

    // Handle direct query execution if the `--query` flag is present.
    if let Some(query_string) = args.query {
        println!("Executing direct query: {}", query_string);
        // Assuming lib::query_parser is accessible and QueryType is defined
        use lib::query_parser::{parse_query_from_string, QueryType};
        match parse_query_from_string(&query_string) {
            Ok(parsed_query) => match parsed_query {
                QueryType::Cypher => println!("  -> Identified as Cypher query."),
                QueryType::SQL => println!("  -> Identified as SQL query."),
                QueryType::GraphQL => println!("  -> Identified as GraphQL query."),
            },
            Err(e) => eprintln!("Error parsing query: {}", e),
        }
        return Ok(()); // Exit after processing a direct query
    }

    // Handle explicit command execution if a subcommand is provided.
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
            GraphDbCommands::Start { port, cluster, listen_port, storage_port, storage_config_file } => {
                // Delegate to the handler module for 'start' command logic
                handlers::handle_start_command(port, cluster, listen_port, storage_port, storage_config_file, &config).await?;
            }
            GraphDbCommands::Stop(stop_args) => {
                // Delegate to the handler module for 'stop' command logic
                handlers::handle_stop_command(stop_args).await?;
            }
            GraphDbCommands::Status(status_args) => {
                // Delegate to the handler module for 'status' command logic
                handlers::handle_status_command(status_args).await?;
            }
            GraphDbCommands::Storage(storage_action) => {
                // Delegate to the handler module for 'storage' command logic
                handlers::handle_storage_command(storage_action).await?;
            }
            GraphDbCommands::Daemon(daemon_cmd) => {
                // Delegate to the handler module for 'daemon' command logic
                handlers::handle_daemon_command(daemon_cmd).await?;
            }
            GraphDbCommands::Rest(rest_cmd) => {
                // Delegate to the handler module for 'rest' command logic
                handlers::handle_rest_command(rest_cmd).await?;
            }
            GraphDbCommands::Help(help_args) => {
                // Delegate to the help_display module for 'help' display
                let mut cmd = CliArgs::command(); // Get the top-level Command object for clap's help generation
                if let Some(command_filter) = help_args.filter_command {
                    help_display::print_filtered_help_clap_generated(&mut cmd, &command_filter);
                } else if !help_args.command_path.is_empty() {
                    let command_filter = help_args.command_path.join(" ");
                    help_display::print_filtered_help_clap_generated(&mut cmd, &command_filter);
                } else {
                    help_display::print_help_clap_generated();
                }
            }
        }
        return Ok(()); // Exit after processing a direct command
    }

    // If no specific command or query is given, and `--cli` flag is present,
    // or if no arguments are given at all, enter interactive CLI mode.
    if args.cli {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        interactive::run_cli_interactive().await?;
        return Ok(());
    }

    // If only `--enable-plugins` is given without other commands.
    if args.enable_plugins {
        println!("Experimental plugins is enabled.");
        return Ok(());
    }

    // Default to interactive CLI if no other commands/args are given
    interactive::run_cli_interactive().await?;
    Ok(())
}


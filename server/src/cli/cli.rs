use clap::{Parser, Subcommand};
use crossterm::{
    cursor,
    style::{self, Color},
    terminal::{Clear, ClearType},
    ExecutableCommand,
};
use std::collections::HashSet;
use std::io::{self, Write};
use graphdb_lib::query_parser::{parse_query_from_string, QueryType};

/// CLI entry point for GraphDB
#[derive(Parser, Debug)]
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI")]
struct CliArgs {
    #[command(subcommand)]
    command: Option<GraphDbCommands>,
}

/// Subcommands for GraphDB CLI
#[derive(Subcommand, Debug)]
enum GraphDbCommands {
    /// View the graph
    ViewGraph {
        /// ID of the graph to view
        #[arg(value_name = "GRAPH_ID")]
        graph_id: Option<u32>,
    },
    /// View the graph history
    ViewGraphHistory {
        /// Optional ID of the graph
        #[arg(value_name = "GRAPH_ID")]
        graph_id: Option<u32>,
        /// Optional start date for history (format: YYYY-MM-DD)
        #[arg(value_name = "START_DATE")]
        start_date: Option<String>,
        /// Optional end date for history (format: YYYY-MM-DD)
        #[arg(value_name = "END_DATE")]
        end_date: Option<String>,
    },
    /// Index a node
    IndexNode {
        /// Optional ID of the node to index
        #[arg(value_name = "NODE_ID")]
        node_id: Option<u32>,
    },
    /// Cache the node state
    CacheNodeState {
        /// ID of the node
        #[arg(value_name = "NODE_ID")]
        node_id: Option<u32>,
    },
}

pub fn start_cli() {
    let args = CliArgs::parse();

    match args.command {
        Some(command) => {
            // Output the command and its parameters to the terminal
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

                    if let Some(end) = end_date {
                        println!("End Date: {}", end);
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
            }
        }
        None => {
            // No subcommand provided; enter the interactive CLI
            interactive_cli();
        }
    }
}

fn interactive_cli() {
    let valid_commands: HashSet<&str> = [
        "help", "status", "list", "connect", "clear", "view-graph", "view-graph-history",
        "index-node", "cache-node-state", "exit", "quit", "q",
    ]
    .iter()
    .cloned()
    .collect();

    let mut stdout = io::stdout();
    stdout.execute(Clear(ClearType::All)).expect("Failed to clear screen");
    stdout.execute(cursor::MoveTo(0, 0)).expect("Failed to move cursor");

    stdout
        .execute(style::SetForegroundColor(Color::Cyan))
        .expect("Failed to set color");
    writeln!(
        stdout,
        "\nWelcome to GraphDB CLI\nType a command and press Enter. Type 'exit', 'quit', or 'q' to quit.\n"
    )
    .expect("Failed to write greeting");
    stdout.execute(style::ResetColor).expect("Failed to reset color");

    stdout.flush().expect("Failed to flush stdout");

    loop {
        stdout
            .execute(style::SetForegroundColor(Color::Cyan))
            .expect("Failed to set color");
        print!("=> ");
        stdout.execute(style::ResetColor).expect("Failed to reset color");
        io::stdout().flush().expect("Failed to flush stdout");

        let mut input = String::new();
        if let Err(e) = io::stdin().read_line(&mut input) {
            println!("Error reading input: {}", e);
            continue;
        }

        let command = input.trim();

        if valid_commands.contains(command) {
            if command == "exit" || command == "quit" || command == "q" {
                println!("\nExiting GraphDB CLI... Goodbye!\n");
                break;
            }
            println!("Executing command: {}", command);

            // Handle commands entered in the interactive CLI
            match command {
                "view-graph" => {
                    println!("Executing view-graph command");
                    // Add your logic here
                }
                "view-graph-history" => {
                    println!("Executing view-graph-history command");
                    // Add your logic here
                }
                "index-node" => {
                    println!("Executing index-node command");
                    // Add your logic here
                }
                "cache-node-state" => {
                    println!("Executing cache-node-state command");
                    // Add your logic here
                }
                _ => {
                    println!("Unknown command: {}", command);
                }
            }
        } else if !command.is_empty() {
            // Attempt to parse the command as a query
            match parse_query_from_string(command) {
                Ok(parsed_query) => match parsed_query {
                    QueryType::Cypher => {
                        println!("Cypher query detected: {}", command);
                    }
                    QueryType::SQL => {
                        println!("SQL query detected: {}", command);
                    }
                    QueryType::GraphQL => {
                        println!("GraphQL query detected: {}", command);
                    }
                },
                Err(_) => {
                    stdout
                        .execute(style::SetForegroundColor(Color::Yellow))
                        .expect("Failed to set color");
                    println!("Unknown command: {}", command);
                    stdout.execute(style::ResetColor).expect("Failed to reset color");
                }
            }
        }
    }
}


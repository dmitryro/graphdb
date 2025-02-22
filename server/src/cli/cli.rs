use graphdb_daemon::daemonize::{Daemonize, DaemonizeBuilder};
use clap::{Parser, Subcommand};
use crossterm::{
    cursor,
    style::{self, Color},
    terminal::{Clear, ClearType},
    ExecutableCommand,
};
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, Write};
use std::process::{Command, exit};
use graphdb_lib::query_parser::{parse_query_from_string, QueryType};
use config::Config;
use std::path::Path;
use std::ffi::CString;
use proctitle::set_title;

// CLI entry point for GraphDB
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
        #[arg(value_name = "GRAPH_ID")]
        graph_id: Option<u32>,
    },
    /// View the graph history
    ViewGraphHistory {
        #[arg(value_name = "GRAPH_ID")]
        graph_id: Option<u32>,
        #[arg(value_name = "START_DATE")]
        start_date: Option<String>,
        #[arg(value_name = "END_DATE")]
        end_date: Option<String>,
    },
    /// Index a node
    IndexNode {
        #[arg(value_name = "NODE_ID")]
        node_id: Option<u32>,
    },
    /// Cache the node state
    CacheNodeState {
        #[arg(value_name = "NODE_ID")]
        node_id: Option<u32>,
    },
    /// Start the server as a daemon
    Start {
        #[arg(short = 'p', long = "port", value_name = "PORT")]
        port: Option<u16>,
    },
    /// Stop the server daemon
    Stop,
}

pub fn start_cli() {
    let args = CliArgs::parse();

    match args.command {
        Some(command) => match command {
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
            GraphDbCommands::Start { port } => {
                start_daemon(port);
            }
            GraphDbCommands::Stop => {
                stop_daemon();
            }
        },
        None => {
            interactive_cli();
        }
    }
}

fn set_daemon_process_name(name: &str) {
      set_title(name);
}

fn start_daemon(port: Option<u16>) {
    // Define the path to the configuration file
    let config_path = "server/src/cli/config.toml";

    // Initialize default host and port
    let mut host_to_use = "127.0.0.1".to_string();
    let mut port_to_use = 8080;
    let mut process_name_to_use: String  = "graphdb-cli".to_string();

    // Read from config file if it exists
    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(config::File::with_name(config_path))
            .build()
            .unwrap();

        if let Ok(host) = config.get_string("server.host") {
            host_to_use = host;
        }

        if let Ok(process_name) = config.get_string("daemon.process_name") {
            process_name_to_use = process_name;
        }

        if let Ok(port) = config.get_int("server.port") {
            port_to_use = port as u16;
        }
    }

    // Override port if provided as a command-line argument
    if let Some(p) = port {
        port_to_use = p;
    }

    // Set up the output and error files for the daemon
    let stdout = File::create("/tmp/daemon.out").unwrap();
    let stderr = File::create("/tmp/daemon.err").unwrap();

    // Set the daemon process name
    set_daemon_process_name("graphdb-daemon");

    // Configure the daemon
    let daemonize = DaemonizeBuilder::default()
        .working_directory("/tmp")
        .umask(0o777)
        .stdout(stdout)
        .stderr(stderr)
        .process_name(process_name_to_use.as_str())
        .host(&*host_to_use)
        .port(port_to_use)
        .build()
        .expect("Failed to build Daemonize object");

    // Start the daemon
    match daemonize.start() {
        Ok(_) => {
            println!("Daemon started with PID: {}", std::process::id());
            Command::new("server")
                .arg(format!("--host={}", host_to_use))
                .arg(format!("--port={}", port_to_use))
                .spawn()
                .expect("Failed to start server");
        }
        Err(e) => {
            eprintln!("Failed to start server: {:?}", e);
            exit(1);
        }
    }
}

fn stop_daemon() {
    let process_name = "graphdb-daemon";
    match Command::new("pgrep")
        .arg(process_name)
        .output()
    {
        Ok(output) => {
            if output.status.success() {
                let pid = String::from_utf8_lossy(&output.stdout);
                match Command::new("kill")
                    .arg("-SIGTERM")
                    .arg(pid.trim())
                    .status()
                {
                    Ok(_) => println!("The daemon service was successfully stopped."),
                    Err(e) => eprintln!("Failed to stop daemon: {}", e),
                }
            } else {
                println!("No daemon process found.");
            }
        }
        Err(e) => eprintln!("Failed to search for daemon process: {}", e),
    }
}

fn interactive_cli() {
    let valid_commands: HashSet<&str> = [
        "help", "status", "list", "connect", "clear", "view-graph", "view-graph-history",
        "index-node", "cache-node-state", "exit", "quit", "q", "start", "stop", "--port", "--host",
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
                "start" => {
                    start_daemon(None);
                }
                "stop" => {
                    stop_daemon();
                }
                _ => {
                    println!("Unknown command: {}", command);
                }
            }
        } else if !command.is_empty() {
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

use graphdb_daemon::daemonize::DaemonizeBuilder;
use graphdb_daemon::DaemonizeError;
use clap::{Parser, Subcommand};
use crossterm::{
    cursor,
    style::{self, Color},
    terminal::{Clear, ClearType},
    ExecutableCommand,
};
use std::fs::{self, File};
use std::io::{self, Write};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::vec::Vec;
use graphdb_lib::query_parser::{parse_query_from_string, QueryType};
use config::{Config, File as ConfigFile};
use std::path::Path;
use serde::{Serialize, Deserialize};
use shared_memory::Shmem;
use std::collections::HashSet;
use lazy_static::lazy_static;
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};
use daemon_api::{start_daemon, stop_daemon};

lazy_static! {
    static ref SHARED_MEMORY_KEYS: Mutex<HashSet<i32>> = Mutex::new(HashSet::new());
}

#[derive(Parser, Debug)]
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI")]
struct CliArgs {
    #[arg(long, help = "Execute a direct query string.")]
    query: Option<String>,

    #[arg(long, help = "Force entry into the interactive CLI mode.")]
    cli: bool,

    #[arg(long, help = "Enable experimental plugins (feature flag).")]
    enable_plugins: bool,

    #[command(subcommand)]
    command: Option<GraphDbCommands>,
}

#[derive(Serialize, Deserialize, Debug)]
struct DaemonData {
    port: u16,
    host: String,
    pid: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct KVPair {
    key: String,
    value: Vec<u8>,
}

#[derive(Subcommand, Debug)]
enum GraphDbCommands {
    ViewGraph {
        #[arg(long = "graph-id", value_name = "GRAPH_ID", help = "ID of the graph to view.")]
        graph_id: Option<u32>,
    },
    ViewGraphHistory {
        #[arg(long = "graph-id", value_name = "GRAPH_ID", help = "ID of the graph to view history for.")]
        graph_id: Option<u32>,
        #[arg(long = "start-date", value_name = "START_DATE", help = "Start date for history (YYYY-MM-DD).")]
        start_date: Option<String>,
        #[arg(long = "end-date", value_name = "END_DATE", help = "End date for history (YYYY-MM-DD).")]
        end_date: Option<String>,
    },
    IndexNode {
        #[arg(long = "node-id", value_name = "NODE_ID", help = "ID of the node to index.")]
        node_id: Option<u32>,
    },
    CacheNodeState {
        #[arg(long = "node-id", value_name = "NODE_ID", help = "ID of the node to cache state for.")]
        node_id: Option<u32>,
    },
    Start {
        #[arg(short = 'p', long = "port", value_name = "PORT", help = "Port for the daemon to listen on. Ignored if --cluster is used.")]
        port: Option<u16>,
        #[arg(long = "cluster", value_name = "START-END", help = "Start a cluster of daemons on a range of ports (e.g., '9001-9005'). Max 10 ports.")]
        cluster: Option<String>,
        #[arg(long = "listen-port", value_name = "LISTEN_PORT", help = "Expose REST API on this port")]
        listen_port: Option<u16>,
    },
    Stop,
}

#[derive(Serialize, Deserialize, Debug)]
struct PidStore {
    pid: u32,
}

pub fn start_cli() {
    let args = CliArgs::parse();

    if let Some(query_string) = args.query {
        println!("Executing direct query: {}", query_string);
        match parse_query_from_string(&query_string) {
            Ok(parsed_query) => match parsed_query {
                QueryType::Cypher => println!("  -> Identified as Cypher query."),
                QueryType::SQL => println!("  -> Identified as SQL query."),
                QueryType::GraphQL => println!("  -> Identified as GraphQL query."),
            },
            Err(e) => eprintln!("Error parsing query: {}", e),
        }
        return;
    }

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
            GraphDbCommands::Start { port, cluster, listen_port } => {
                let rest_api_port = listen_port.unwrap_or(8082);
                let skip_ports = vec![rest_api_port];

                // Start core daemons (single or cluster), always skipping the REST API port!
                let daemon_result = start_daemon(port, cluster.clone(), skip_ports.clone());
                match daemon_result {
                    Ok(()) => {
                        println!("Daemon(s) started successfully.");
                    }
                    Err(e) => {
                        eprintln!("Failed to start daemon(s): {:?}", e);
                        std::process::exit(1);
                    }
                }

                // REST API daemonization logic (only if --listen-port given)
                if let Some(rest_port) = listen_port {
                    if rest_port < 1024 || rest_port > 65535 {
                        eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port);
                        std::process::exit(1);
                    }

                    // Kill any process on rest_port before daemonizing REST API server
                    let output = std::process::Command::new("lsof")
                        .arg("-i")
                        .arg(format!(":{}", rest_port))
                        .arg("-t")
                        .output();
                    if let Ok(output) = output {
                        if !output.stdout.is_empty() {
                            let pids = String::from_utf8_lossy(&output.stdout);
                            for pid in pids.trim().lines() {
                                if let Ok(pid) = pid.parse::<i32>() {
                                    println!("Killing process {} on port {}", pid, rest_port);
                                    let _ = std::process::Command::new("kill")
                                        .arg("-9")
                                        .arg(pid.to_string())
                                        .output();
                                }
                            }
                        }
                    }

                    // Wait for port to be released
                    let addr = format!("127.0.0.1:{}", rest_port);
                    let start_time = std::time::Instant::now();
                    let wait_timeout = std::time::Duration::from_secs(3);
                    let poll_interval = std::time::Duration::from_millis(100);
                    let mut port_freed = false;
                    while start_time.elapsed() < wait_timeout {
                        match std::net::TcpListener::bind(&addr) {
                            Ok(_) => {
                                port_freed = true;
                                break;
                            }
                            Err(_) => {
                                std::thread::sleep(poll_interval);
                            }
                        }
                    }
                    if !port_freed {
                        eprintln!("Failed to free up port {} after killing processes. Try again.", rest_port);
                        std::process::exit(1);
                    }

                    // Daemonize REST API server, but run REST API logic in the child!
                    let mut daemonize_builder = DaemonizeBuilder::new()
                        .working_directory("/tmp")
                        .umask(0o027)
                        .process_name(&format!("graphdb-rest-api-{}", rest_port))
                        .host("127.0.0.1")
                        .port(rest_port)
                        .skip_ports(vec![]); // REST API server should bind this port

                    match daemonize_builder.fork_only() {
                        Ok(child_pid) => {
                            if child_pid == 0 {
                                // In REST API child daemon: RUN THE REST API SERVER!
                                let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
                                let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                                let result = rt.block_on(graphdb_rest_api::start_server(rest_port, shutdown_rx));
                                if let Err(e) = result {
                                    eprintln!("REST API server failed: {:?}", e);
                                    std::process::exit(1);
                                }
                                std::process::exit(0);
                            } else {
                                println!("REST API server daemonized with PID {}", child_pid);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to daemonize REST API server: {:?}", e);
                            std::process::exit(1);
                        }
                    }
                }
            }
            GraphDbCommands::Stop => {
                let stop_result = stop_daemon();
                match stop_result {
                    Ok(()) => {
                        println!("Daemon(s) stopped successfully.");
                    }
                    Err(e) => {
                        eprintln!("Failed to stop daemon(s): {:?}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
        return;
    }

    if args.cli {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        interactive_cli();
        return;
    }

    if args.enable_plugins {
        println!("Experimental plugins are enabled.");
        return;
    }

    interactive_cli();
}

fn interactive_cli() {
    let valid_commands: HashSet<&str> = [
        "help", "status", "list", "connect", "clear", "view-graph", "view-graph-history",
        "index-node", "cache-node-state", "exit", "quit", "q", "start", "stop",
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
                }
                "view-graph-history" => {
                    println!("Executing view-graph-history command");
                }
                "index-node" => {
                    println!("Executing index-node command");
                }
                "cache-node-state" => {
                    println!("Executing cache-node-state command");
                }
                "start" => {
                    // Always skip the REST API port in interactive CLI mode as well
                    let rest_api_port = 8082; // You may want to make this configurable
                    let skip_ports = vec![rest_api_port];
                    let result = start_daemon(None, None, skip_ports);
                    match result {
                        Ok(()) => println!("Daemon(s) started successfully."),
                        Err(e) => eprintln!("Failed to start daemon(s): {:?}", e),
                    }
                }
                "stop" => {
                    let result = stop_daemon();
                    match result {
                        Ok(()) => println!("Daemon(s) stopped successfully."),
                        Err(e) => eprintln!("Failed to stop daemon(s): {:?}", e),
                    }
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

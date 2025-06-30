// server/src/cli/cli.rs
// Corrected: 2025-06-30 - Removed all file system interactions, including logging and config loading.

use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::oneshot;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::io::Write; // For flushing stdout - This is acceptable as it's stdout, not a file.

use daemon_api::{start_daemon, stop_daemon, DaemonError}; // Ensure DaemonError is used if needed
use graphdb_rest_api::start_server as start_rest_server; // Alias to avoid name collision

// Imports for CLI argument parsing and daemonization logic
use clap::{Parser, Subcommand};
use crossterm::{
    cursor,
    style::{self, Color},
    terminal::{Clear, ClearType},
    ExecutableCommand,
};
// Removed: use std::fs::{self, File}; // No longer using file system
use std::process::{Command, Stdio};
use graphdb_lib::query_parser::{parse_query_from_string, QueryType};
// Removed: use config::{Config, File as ConfigFile}; // No longer loading config from files
use std::path::Path; // Keep if path manipulation is needed for other non-file purposes.
use serde::{Serialize, Deserialize};
use shared_memory::Shmem; // If used for daemon communication, otherwise remove.
use std::collections::HashSet;
use lazy_static::lazy_static; // If SHARED_MEMORY_KEYS or similar is used.
use std::net::ToSocketAddrs; // Required for TcpListener::bind in port check
use std::time::{Duration, Instant};
use graphdb_daemon::daemonize::DaemonizeBuilder; // Correct import for DaemonizeBuilder

// Re-declare lazy_static for SHARED_MEMORY_KEYS if it's used within daemon/cli interactions
lazy_static! {
    static ref SHARED_MEMORY_KEYS: Mutex<HashSet<i32>> = Mutex::new(HashSet::new());
}

// Re-declare CLI argument structures
#[derive(Parser, Debug)]
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI")]
pub struct CliArgs { // Made public
    #[arg(long, help = "Execute a direct query string.")]
    pub query: Option<String>,

    #[arg(long, help = "Force entry into the interactive CLI mode.")]
    pub cli: bool,

    #[arg(long, help = "Enable experimental plugins (feature flag).")]
    pub enable_plugins: bool,

    #[command(subcommand)]
    pub command: Option<GraphDbCommands>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonData { // Made public
    pub port: u16,
    pub host: String,
    pub pid: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KVPair { // Made public
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Subcommand, Debug)]
pub enum GraphDbCommands { // Made public
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
pub struct PidStore { // Made public
    pid: u32,
}


#[derive(Debug, PartialEq)]
enum CommandType {
    DaemonStart,
    DaemonStop,
    DaemonStatus,
    DaemonHealth,
    DaemonVersion,
    DaemonStartCluster,
    DaemonStopCluster,
    DaemonClusterStatus,
    ListDaemons,
    ClearAllDaemons,
    RestApiStart,
    RestApiStop,
    RestApiStatus,
    RestApiHealth,
    RestApiVersion,
    RestApiRegisterUser,
    RestApiAuthenticate,
    RestApiGraphQuery,
    RestApiStorageQuery,
    Help,
    Exit,
    Unknown,
}

// Function to parse commands from the CLI (used by interactive mode)
fn parse_command(input: &str) -> (CommandType, Vec<String>) {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    let command_str = parts[0].to_lowercase();
    let args: Vec<String> = parts[1..].iter().map(|&s| s.to_string()).collect();

    let cmd_type = match command_str.as_str() {
        "daemon" => match args.first().map(|s| s.to_lowercase()).as_deref() {
            Some("start") => CommandType::DaemonStart,
            Some("stop") => CommandType::DaemonStop,
            Some("status") => CommandType::DaemonStatus,
            Some("health") => CommandType::DaemonHealth,
            Some("version") => CommandType::DaemonVersion,
            Some("start-cluster") => CommandType::DaemonStartCluster,
            Some("stop-cluster") => CommandType::DaemonStopCluster,
            Some("cluster-status") => CommandType::DaemonClusterStatus,
            _ => CommandType::Unknown,
        },
        "list-daemons" => CommandType::ListDaemons,
        "clear-daemons" => CommandType::ClearAllDaemons,
        "rest" => match args.first().map(|s| s.to_lowercase()).as_deref() {
            Some("start") => CommandType::RestApiStart,
            Some("stop") => CommandType::RestApiStop,
            Some("status") => CommandType::RestApiStatus,
            Some("health") => CommandType::RestApiHealth,
            Some("version") => CommandType::RestApiVersion,
            Some("register-user") => CommandType::RestApiRegisterUser,
            Some("authenticate") => CommandType::RestApiAuthenticate,
            Some("graph-query") => CommandType::RestApiGraphQuery,
            Some("storage-query") => CommandType::RestApiStorageQuery,
            _ => CommandType::Unknown,
        },
        "help" => CommandType::Help,
        "exit" | "quit" | "q" => CommandType::Exit, // Added 'q' for exit
        _ => CommandType::Unknown,
    };

    (cmd_type, args)
}

// Handler for CLI commands (interactive mode)
async fn handle_command(
    command: CommandType,
    args: Vec<String>,
    daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>, // Now Option<u16> to reflect if it's running
    rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
) {
    match command {
        CommandType::DaemonStart => {
            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let port = port_arg.unwrap_or(8080); // Default port for `daemon start`

            let mut handles = daemon_handles.lock().await;

            if handles.contains_key(&port) {
                println!("Daemon on port {} is already running (managed by this CLI).", port);
                return;
            }

            println!("Attempting to start daemon on port {}...", port);
            // Removed: log_cli_event(&format!("CLI: Attempting to start daemon on port {}", port));

            let (tx, rx) = oneshot::channel();
            // Get the current REST API port to skip it for daemon binding
            let current_rest_port = *rest_api_port_arc.lock().await;
            let skip_ports = if let Some(p) = current_rest_port { vec![p] } else { vec![] };

            let daemon_join_handle = tokio::spawn(async move {
                let result = start_daemon(Some(port), None, skip_ports).await;
                match result {
                    Ok(_) => println!("Daemon on port {} started successfully.", port),
                    Err(e) => eprintln!("Failed to start daemon on port {}: {:?}", port, e),
                }
                let _ = rx.await; // Wait for shutdown signal
                println!("Daemon on port {} is shutting down...", port);
            });

            handles.insert(port, (daemon_join_handle, tx));
            println!("Daemon started (initiation successful, check logs for full status).");
            // Removed: log_cli_event(&format!("CLI: Daemon on port {} initiation successful.", port));
        }
        CommandType::DaemonStop => {
            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let mut handles = daemon_handles.lock().await;

            if let Some(port) = port_arg {
                if let Some((_, tx)) = handles.remove(&port) {
                    println!("Sending stop signal to daemon on port {}...", port);
                    // Removed: log_cli_event(&format!("CLI: Sending stop signal to daemon on port {}", port));
                    if tx.send(()).is_err() {
                        eprintln!("Failed to send shutdown signal to daemon on port {}. It might have already stopped.", port);
                        // Removed: log_cli_event(&format!("CLI: Failed to send shutdown signal to daemon on port {}. It might have already stopped.", port));
                    } else {
                        println!("Daemon on port {} stopping...", port);
                    }
                } else {
                    println!("No daemon found running on port {} (managed by this CLI).", port);
                    // Removed: log_cli_event(&format!("CLI: No daemon found running on port {}.", port));
                }
            } else {
                println!("Usage: daemon stop <port>");
                // Removed: log_cli_event("CLI: Usage: daemon stop <port>");
            }
        }
        CommandType::DaemonStatus => {
            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let handles = daemon_handles.lock().await;
            if handles.contains_key(&port_arg.unwrap_or(0)) { // Use a default for check, if needed
                println!("Daemon on port {} is running (managed by this CLI).", port_arg.unwrap_or(0));
            } else {
                println!("Daemon on port {} is not running (managed by this CLI).", port_arg.unwrap_or(0));
            }
        }
        CommandType::DaemonHealth => {
            println!("Not implemented: Daemon health check.");
            // Removed: log_cli_event("CLI: Not implemented: Daemon health check.");
        }
        CommandType::DaemonVersion => {
            println!("Not implemented: Daemon version check.");
            // Removed: log_cli_event("CLI: Not implemented: Daemon version check.");
        }
        CommandType::DaemonStartCluster => {
            let range_arg = args.get(1);
            if let Some(range_str) = range_arg {
                let parts: Vec<&str> = range_str.split('-').collect();
                if parts.len() == 2 {
                    let start_port_parse = parts[0].parse::<u16>();
                    let end_port_parse = parts[1].parse::<u16>();

                    if let (Ok(start_port), Ok(end_port)) = (start_port_parse, end_port_parse) {
                        if start_port <= end_port && end_port - start_port <= 10 {
                            let mut handles = daemon_handles.lock().await;
                            let current_rest_port = *rest_api_port_arc.lock().await;
                            let base_skip_ports = if let Some(p) = current_rest_port { vec![p] } else { vec![] };
                            let mut started_any = false;

                            for port in start_port..=end_port {
                                if base_skip_ports.contains(&port) {
                                    println!("Skipping port {} as it's reserved for the REST API.", port);
                                    continue;
                                }
                                if handles.contains_key(&port) {
                                    println!("Daemon on port {} is already running, skipping.", port);
                                    continue;
                                }
                                println!("Attempting to start daemon on port {} as part of cluster...", port);
                                // Removed: log_cli_event(&format!("CLI: Attempting to start daemon on port {} as part of cluster.", port));

                                let (tx, rx) = oneshot::channel();
                                let current_port = port; // Capture port for the spawned task
                                let cluster_range = format!("{}-{}", start_port, end_port);
                                let task_skip_ports = base_skip_ports.clone(); // Clone for each task

                                let daemon_join_handle = tokio::spawn(async move {
                                    let result = start_daemon(Some(current_port), Some(cluster_range), task_skip_ports).await;
                                    match result {
                                        Ok(_) => println!("Daemon on port {} started successfully in cluster.", current_port),
                                        Err(e) => eprintln!("Failed to start daemon on port {} in cluster: {:?}", current_port, e),
                                    }
                                    let _ = rx.await;
                                    println!("Daemon on port {} is shutting down from cluster...", current_port);
                                });
                                handles.insert(port, (daemon_join_handle, tx));
                                started_any = true;
                            }
                            if started_any {
                                println!("Cluster initiation successful (check logs for full status).");
                                // Removed: log_cli_event(&format!("CLI: Daemon cluster {}-{} initiation successful.", start_port, end_port));
                            } else {
                                println!("No new daemons were started in the cluster range {}-{}.", start_port, end_port);
                            }
                        } else {
                            println!("Invalid port range or range exceeds 10 ports. Use: daemon start-cluster <start_port>-<end_port> (max 10 ports)");
                            // Removed: log_cli_event("CLI: Invalid port range or range exceeds 10 ports for cluster.");
                        }
                    } else {
                        println!("Invalid port numbers in range. Use: daemon start-cluster <start_port>-<end_port>");
                        // Removed: log_cli_event("CLI: Invalid port numbers in range for cluster.");
                    }
                } else {
                    println!("Invalid range format. Use: daemon start-cluster <start_port>-<end_port>");
                    // Removed: log_cli_event("CLI: Invalid range format for cluster.");
                }
            } else {
                println!("Usage: daemon start-cluster <start_port>-<end_port>");
                // Removed: log_cli_event("CLI: Usage: daemon start-cluster <start_port>-<end_port>");
            }
        }
        CommandType::DaemonStopCluster => {
            println!("Not implemented: Stop a specific cluster. Use `list-daemons` and `daemon stop <port>` for now, or `clear-daemons`.");
            // Removed: log_cli_event("CLI: Not implemented: Stop a specific cluster.");
        }
        CommandType::DaemonClusterStatus => {
            println!("Not implemented: Daemon cluster status check.");
            // Removed: log_cli_event("CLI: Not implemented: Daemon cluster status check.");
        }
        CommandType::ListDaemons => {
            let handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No daemons currently managed by this CLI instance.");
            } else {
                println!("Currently running daemons (managed by this CLI):");
                for port in handles.keys() {
                    println!("- Daemon on port {}", port);
                }
            }
            // Removed: log_cli_event("CLI: Listed managed daemons.");
        }
        CommandType::ClearAllDaemons => {
            let mut handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No daemons to clear managed by this CLI.");
                // Removed: log_cli_event("CLI: No daemons to clear.");
                return;
            }
            println!("Stopping all {} managed daemons...", handles.len());
            // Removed: log_cli_event(&format!("CLI: Stopping all {} managed daemons.", handles.len()));

            let mut stopped_count = 0;
            let mut failed_count = 0;
            let ports: Vec<u16> = handles.keys().cloned().collect();

            for port in ports {
                if let Some((_, tx)) = handles.remove(&port) {
                    println!("Stopping daemon on port {}...", port);
                    if tx.send(()).is_err() {
                        eprintln!("Failed to send shutdown signal to daemon on port {}. It might have already stopped.", port);
                        failed_count += 1;
                    } else {
                        stopped_count += 1;
                    }
                }
            }
            // Trigger the global stop_daemon which will attempt to kill external processes too
            println!("Sending global stop signal to all external daemon processes...");
            // Corrected: Removed `.await` as `stop_daemon` is synchronous
            let stop_result = stop_daemon();
            match stop_result {
                Ok(()) => println!("Global daemon stop signal sent successfully."),
                Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
            }
            // Removed: log_cli_event(&format!("CLI: Stopped {} daemons. Failed to signal {} daemons. Global stop result: {:?}", stopped_count, failed_count, stop_result));


            println!("Stopped {} daemons. Failed to signal {} daemons (managed by this CLI).", stopped_count, failed_count);
            // Removed: log_cli_event(&format!("CLI: Stopped {} daemons. Failed to signal {} daemons.", stopped_count, failed_count));
        }
        CommandType::RestApiStart => {
            let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
            let mut rest_handle_guard = rest_api_handle.lock().await;
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;

            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let rest_port = port_arg.unwrap_or(8082); // Default REST API port for interactive mode

            if rest_api_port_guard.is_some() {
                println!("REST API server is already running on port {}.", rest_api_port_guard.unwrap());
                // Removed: log_cli_event(&format!("CLI: REST API server is already running on port {}.", rest_api_port_guard.unwrap()));
                return;
            }

            if rest_port < 1024 || rest_port > 65535 {
                eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port);
                // Removed: log_cli_event(&format!("CLI: Invalid REST API port specified: {}", rest_port));
                return;
            }

            // Check if any daemon is running on this port
            let daemon_handles_locked = daemon_handles.lock().await;
            if daemon_handles_locked.contains_key(&rest_port) {
                eprintln!("Cannot start REST API on port {} because a daemon is already running there (managed by this CLI).", rest_port);
                // Removed: log_cli_event(&format!("CLI: Cannot start REST API on port {} as daemon is present.", rest_port));
                return;
            }
            drop(daemon_handles_locked); // Release lock before trying to kill processes

            // Kill any process on rest_port before daemonizing REST API server
            let output = Command::new("lsof")
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
                            let _ = Command::new("kill")
                                .arg("-9")
                                .arg(pid.to_string())
                                .output();
                        }
                    }
                }
            }

            // Wait for port to be released
            let addr = format!("127.0.0.1:{}", rest_port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
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
                // Removed: log_cli_event(&format!("CLI: Failed to free up REST API port {}.", rest_port));
                return;
            }

            println!("Starting REST API server on port {}...", rest_port);
            // Removed: log_cli_event(&format!("CLI: Starting REST API server on port {}", rest_port));

            // Daemonize REST API server
            let mut daemonize_builder = DaemonizeBuilder::new()
                .working_directory("/tmp") // This is acceptable as a temporary working directory for the daemon process, not direct file access for data storage
                .umask(0o027)
                .process_name(&format!("graphdb-rest-api-{}", rest_port))
                .host("127.0.0.1")
                .port(rest_port)
                .skip_ports(vec![]); // REST API server should bind this port

            match daemonize_builder.fork_only() {
                Ok(child_pid) => {
                    if child_pid == 0 {
                        // In REST API child daemon process
                        let child_rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for REST API daemon child");
                        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel(); // Dummy channel for daemon child
                        let result = child_rt.block_on(start_rest_server(rest_port, shutdown_rx)); // Use child_rt.block_on
                        if let Err(e) = result {
                            eprintln!("REST API server failed: {:?}", e);
                            std::process::exit(1);
                        }
                        std::process::exit(0);
                    } else {
                        // In parent CLI process
                        let (tx, rx) = oneshot::channel();
                        *rest_tx_guard = Some(tx);
                        *rest_api_port_guard = Some(rest_port); // Store the actual port it's running on

                        let handle = tokio::spawn(async move {
                            let _ = rx.await; // This will just wait indefinitely unless tx is dropped
                        });
                        *rest_handle_guard = Some(handle);

                        println!("REST API server daemonized with PID {}", child_pid);
                        // Removed: log_cli_event(&format!("CLI: REST API server daemonized with PID {}.", child_pid));
                    }
                }
                Err(e) => {
                    eprintln!("Failed to daemonize REST API server: {:?}", e);
                    // Removed: log_cli_event(&format!("CLI: Failed to daemonize REST API server: {:?}", e));
                }
            }
        }
        CommandType::RestApiStop => {
            let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
            let mut rest_handle_guard = rest_api_handle.lock().await;
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;

            if let Some(port) = rest_api_port_guard.take() { // Take the port, assuming it's stopping
                println!("Attempting to stop REST API server on port {}...", port);
                // Removed: log_cli_event(&format!("CLI: Attempting to stop REST API server on port {}.", port));

                // Send a signal to the oneshot channel (if it exists, though it might not be effective for forked daemon)
                if let Some(tx) = rest_tx_guard.take() {
                    let _ = tx.send(()); // Signal the handle if it's waiting
                }

                // Attempt to kill the process directly using lsof/kill
                let output = Command::new("lsof")
                    .arg("-i")
                    .arg(format!(":{}", port))
                    .arg("-t")
                    .output();
                if let Ok(output) = output {
                    if !output.stdout.is_empty() {
                        let pids = String::from_utf8_lossy(&output.stdout);
                        for pid in pids.trim().lines() {
                            if let Ok(pid) = pid.parse::<i32>() {
                                println!("Killing REST API process {} on port {}", pid, port);
                                let _ = Command::new("kill")
                                    .arg("-9")
                                    .arg(pid.to_string())
                                    .output();
                            }
                        }
                    } else {
                        println!("No process found listening on port {} for REST API.", port);
                    }
                } else {
                    eprintln!("Failed to run lsof to find REST API process.");
                }

                // Join the handle to clean up the task, even if the process was killed externally
                if let Some(handle) = rest_handle_guard.take() {
                    let _ = handle.await; // Wait for the task to finish (e.g., if it received a signal or exited)
                }

                println!("REST API server on port {} stopped (or no longer running).", port);
                // Removed: log_cli_event(&format!("CLI: REST API server on port {} stopped.", port));

            } else {
                println!("REST API server is not running (managed by this CLI).");
                // Removed: log_cli_event("CLI: REST API server is not running.");
            }
        }
        CommandType::RestApiStatus => {
            let rest_api_port_guard = rest_api_port_arc.lock().await;
            if let Some(port) = *rest_api_port_guard {
                println!("REST API server is expected to be running on port {}.", port);
                // You could add a simple HTTP GET /health check here if you want real status
            } else {
                println!("REST API server is not running (managed by this CLI).");
            }
        }
        CommandType::RestApiHealth | CommandType::RestApiVersion |
        CommandType::RestApiRegisterUser | CommandType::RestApiAuthenticate |
        CommandType::RestApiGraphQuery | CommandType::RestApiStorageQuery => {
            let rest_api_port_guard = rest_api_port_arc.lock().await;
            if let Some(port) = *rest_api_port_guard {
                println!("This command requires interaction with the running REST API. Please use an HTTP client (e.g., curl, Postman) to interact with http://127.0.0.1:{}/api/v1/{}.",
                    port,
                    match command {
                        CommandType::RestApiHealth => "health",
                        CommandType::RestApiVersion => "version",
                        CommandType::RestApiRegisterUser => "register",
                        CommandType::RestApiAuthenticate => "auth",
                        CommandType::RestApiGraphQuery => "query",
                        CommandType::RestApiStorageQuery => "storage_query",
                        _ => "unknown" // Should not happen
                    }
                );
            } else {
                println!("REST API server is not running. Please start it first using 'rest start [port]'.");
            }
            // Removed: log_cli_event(&format!("CLI: Instructed user to use HTTP client for REST API command: {:?}", command));
        }
        CommandType::Help => {
            print_help();
            // Removed: log_cli_event("CLI: Displayed help.");
        }
        CommandType::Exit => {
            println!("Exiting CLI.");
            // Removed: log_cli_event("CLI: Exiting CLI.");
            // Shutdown logic for daemons and REST API will be handled by the main loop
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
            // Removed: log_cli_event(&format!("CLI: Unknown command: {}", args.join(" ")));
        }
    }
}

fn print_help() {
    println!("\nGraphDB CLI Commands:");
    println!("  daemon start [port]        - Start a new daemon instance on a specified port (default 8080).");
    println!("  daemon stop <port>         - Stop a daemon instance running on the specified port (managed by this CLI).");
    println!("  daemon status <port>       - Check if a daemon on a specific port is running (managed by this CLI).");
    println!("  daemon start-cluster <start_port>-<end_port> - Start a cluster of daemons (max 10 ports).");
    println!("  list-daemons               - List all daemon instances managed by this CLI.");
    println!("  clear-daemons              - Stop all managed daemons and send global stop to all daemon processes.");
    println!("");
    println!("  rest start [port]          - Start the GraphDB REST API server on a specified port (default 8082).");
    println!("  rest stop                  - Stop the GraphDB REST API server.");
    println!("  rest status                - Check if the REST API server is running.");
    println!("  rest health                - (Use HTTP client) Check REST API health.");
    println!("  rest version               - (Use HTTP client) Get REST API version.");
    println!("  rest register-user         - (Use HTTP client) Register a new user via REST API.");
    println!("  rest authenticate          - (Use HTTP client) Authenticate a user via REST API.");
    println!("  rest graph-query           - (Use HTTP client) Send a graph query to the REST API.");
    println!("  rest storage-query         - (Use HTTP client) Send a storage query to the REST API.");
    println!("");
    println!("  help                       - Display this help message.");
    println!("  exit / quit / q            - Exit the CLI application.");
}

// Removed: log_cli_event function

/// Main asynchronous loop for the CLI interactive mode.
/// This function is called by `start_cli` when interactive mode is detected.
async fn main_loop() {
    let daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let rest_api_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None)); // Tracks the port if REST API is running
    let rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(None));


    println!("GraphDB CLI. Type 'help' for commands.");
    // Removed: log_cli_event("CLI started.");

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut input = String::new();

    loop {
        print!("graphdb> ");
        let _ = std::io::stdout().flush(); // Ensure prompt is displayed

        input.clear();
        if let Err(e) = reader.read_line(&mut input).await {
            eprintln!("Failed to read line: {}", e);
            // Removed: log_cli_event(&format!("CLI: Failed to read input line: {}", e));
            break;
        }

        let (command, args) = parse_command(&input);

        if command == CommandType::Exit {
            break; // Exit the loop
        }

        let daemon_handles_clone = Arc::clone(&daemon_handles);
        let rest_api_shutdown_tx_opt_clone = Arc::clone(&rest_api_shutdown_tx_opt);
        let rest_api_port_arc_clone = Arc::clone(&rest_api_port_arc);
        let rest_api_handle_clone = Arc::clone(&rest_api_handle);

        handle_command(
            command,
            args,
            daemon_handles_clone,
            rest_api_shutdown_tx_opt_clone,
            rest_api_port_arc_clone,
            rest_api_handle_clone,
        ).await;
    }

    // Graceful shutdown logic when exiting the loop
    println!("Shutting down GraphDB CLI components...");
    // Removed: log_cli_event("CLI: Initiating graceful shutdown of components.");

    // Stop REST API server if it was started by this CLI
    let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
    let mut rest_handle_guard = rest_api_handle.lock().await;
    let mut rest_api_port_guard = rest_api_port_arc.lock().await;

    if let Some(port) = rest_api_port_guard.take() {
        println!("Attempting to stop REST API server on port {} during exit...", port);
        // Removed: log_cli_event(&format!("CLI: Attempting to stop REST API server on port {} during exit.", port));

        // Signal the spawned task (if it's still waiting)
        if let Some(tx) = rest_tx_guard.take() {
            let _ = tx.send(());
        }

        // Attempt to kill the process directly using lsof/kill
        let output = Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output();
        if let Ok(output) = output {
            if !output.stdout.is_empty() {
                let pids = String::from_utf8_lossy(&output.stdout);
                for pid in pids.trim().lines() {
                    if let Ok(pid) = pid.parse::<i32>() {
                        println!("Killing REST API process {} on port {}", pid, port);
                        let _ = Command::new("kill")
                            .arg("-9")
                            .arg(pid.to_string())
                            .output();
                    }
                }
            }
        }

        if let Some(handle) = rest_handle_guard.take() {
            let _ = handle.await; // Wait for the task to finish
        }
        println!("REST API server on port {} stopped.", port);
        // Removed: log_cli_event(&format!("CLI: REST API server on port {} stopped.", port));
    }


    // Stop all managed daemons
    let mut handles = daemon_handles.lock().await;
    if !handles.is_empty() {
        println!("Stopping all managed daemon instances...");
        // Removed: log_cli_event("CLI: Stopping all managed daemon instances.");
        let mut join_handles = Vec::new();
        for (port, (handle, tx)) in handles.drain() { // Corrected: `drain()` takes no arguments.
            println!("Signaling daemon on port {} to stop.", port);
            // Removed: log_cli_event(&format!("CLI: Signaling daemon on port {} to stop.", port));
            if tx.send(()).is_err() {
                eprintln!("Warning: Daemon on port {} already stopped or signal failed.", port);
                // Removed: log_cli_event(&format!("CLI: Warning: Daemon on port {} already stopped or signal failed.", port));
            }
            join_handles.push(handle);
        }
        for handle in join_handles {
            let _ = handle.await; // Wait for each daemon task to complete
        }
        println!("All managed daemon instances stopped.");
        // Removed: log_cli_event("CLI: All managed daemon instances stopped.");
    }

    // Send a global stop signal to ensure any external daemon processes are terminated
    println!("Sending global stop signal to all daemon processes...");
    // Corrected: Removed `.await` as `stop_daemon` is synchronous
    let stop_result = stop_daemon();
    match stop_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }
    // Removed: log_cli_event(&format!("CLI: Global daemon stop signal sent. Result: {:?}", stop_result));


    println!("GraphDB CLI shutdown complete. Goodbye!");
    // Removed: log_cli_event("CLI shutdown complete.");
}

// The main entry point for the CLI logic, called by server/src/main.rs
pub fn start_cli() {
    let args = CliArgs::parse();

    // Create a new Tokio runtime for handling async operations
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for CLI");

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
                // Determine the REST API port if provided
                let explicit_rest_api_port = listen_port;

                // Prepare skip_ports for daemons, including the REST API port if explicitly set.
                let skip_ports = if let Some(rest_p) = explicit_rest_api_port {
                    vec![rest_p]
                } else {
                    vec![]
                };

                // Start core daemons (single or cluster), always skipping the REST API port!
                // `start_daemon` is async, so `block_on` is appropriate here.
                let daemon_result = rt.block_on(start_daemon(port, cluster.clone(), skip_ports.clone()));
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
                if let Some(rest_port) = explicit_rest_api_port {
                    if rest_port < 1024 || rest_port > 65535 {
                        eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port);
                        std::process::exit(1);
                    }

                    // Kill any process on rest_port before daemonizing REST API server
                    let output = Command::new("lsof")
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
                                    let _ = Command::new("kill")
                                        .arg("-9")
                                        .arg(pid.to_string())
                                        .output();
                                }
                            }
                        }
                    }

                    // Wait for port to be released
                    let addr = format!("127.0.0.1:{}", rest_port);
                    let start_time = Instant::now();
                    let wait_timeout = Duration::from_secs(3);
                    let poll_interval = Duration::from_millis(100);
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
                        .working_directory("/tmp") // This is acceptable as a temporary working directory for the daemon process, not direct file access for data storage
                        .umask(0o027)
                        .process_name(&format!("graphdb-rest-api-{}", rest_port))
                        .host("127.0.0.1")
                        .port(rest_port)
                        .skip_ports(vec![]); // REST API server should bind this port

                    match daemonize_builder.fork_only() {
                        Ok(child_pid) => {
                            if child_pid == 0 {
                                // In REST API child daemon: RUN THE REST API SERVER!
                                // Here, we create a new Tokio runtime for the child process.
                                let child_rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for REST API daemon child");
                                let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                                let result = child_rt.block_on(start_rest_server(rest_port, shutdown_rx)); // Use child_rt.block_on
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
                // Corrected: Removed `block_on` as `stop_daemon` is synchronous.
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
        return; // Exit after processing a direct command
    }

    if args.cli {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        // Call the interactive CLI main loop, blocking on the runtime
        rt.block_on(main_loop());
        return;
    }

    if args.enable_plugins {
        println!("Experimental plugins are enabled.");
        return;
    }

    // Default to interactive CLI if no other commands/args are given
    rt.block_on(main_loop());
}

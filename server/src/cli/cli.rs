// server/src/cli/cli.rs
// Corrected: 2025-06-30 - Removed all file system interactions, including logging and config loading.

use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::oneshot;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::io::Write; // For flushing stdout - This is acceptable as it's stdout, not a file.
use std::time::{Duration, Instant}; // Added: Import Duration and Instant from std::time
use std::path::PathBuf; // Added: Import PathBuf
use anyhow::Context; // Added: Import Context trait for .context() method
use anyhow::Result; // Added: Import Result for anyhow::Error
use serde_yaml; // Added: For parsing YAML config
use std::fs; // Added: For reading files

use daemon_api::{start_daemon, stop_daemon};
use rest_api::start_server as start_rest_server; // Alias to avoid name collision, removed graphdb_ prefix
use storage_daemon_server::run_storage_daemon as start_storage_server; // Corrected: Use run_storage_daemon directly from lib.rs

// Imports for CLI argument parsing and daemonization logic
use clap::{Parser, Subcommand};
use std::process::Command;
use lib::query_parser::{parse_query_from_string, QueryType}; // Removed graphdb_ prefix
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use lazy_static::lazy_static;
// Removed: use std::net::ToSocketAddrs; // Required for TcpListener::bind in port check
use daemon::daemonize::DaemonizeBuilder; // Removed graphdb_ prefix


// Re-declare lazy_static for SHARED_MEMORY_KEYS if it's used within daemon/cli interactions
lazy_static! {
    static ref SHARED_MEMORY_KEYS: Mutex<HashSet<i32>> = Mutex::new(HashSet::new());
}

// CLI's assumed default storage port. This is used for consistency in stop/status commands.
// The actual daemon port is determined by the daemon itself based on CLI arguments or its own config file.
const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085; // Re-added constant

/// Function to get default REST API port from config
fn get_default_rest_port_from_config() -> u16 {
    8082 // Default REST API port
}

/// Helper to find and kill a process by port. This is used for all daemon processes.
fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> { // Re-added function
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    let output = Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t") // Only print PIDs
        .output()
        .context(format!("Failed to run lsof to find {} process on port {}", process_name, port))?;

    let pids = String::from_utf8_lossy(&output.stdout);
    let pids: Vec<i32> = pids.trim().lines().filter_map(|s| s.parse::<i32>().ok()).collect();

    if pids.is_empty() {
        println!("No {} process found running on port {}.", process_name, port);
        return Ok(());
    }

    for pid in pids {
        println!("Killing process {} (for {} on port {})...", pid, process_name, port);
        match Command::new("kill").arg("-9").arg(pid.to_string()).status() {
            Ok(status) if status.success() => println!("Process {} killed successfully.", pid),
            Ok(_) => eprintln!("Failed to kill process {}.", pid),
            Err(e) => eprintln!("Error killing process {}: {}", pid, e),
        }
    }
    Ok(())
}

/// Helper to check if a process is running on a given port. This is used for all daemon processes.
fn check_process_status_by_port(process_name: &str, port: u16) -> bool { // Re-added function
    let output = Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
        .output();

    if let Ok(output) = output {
        let pids = String::from_utf8_lossy(&output.stdout);
        if !pids.trim().is_empty() {
            println!("{} on port {} is running with PID(s): {}.", process_name, port, pids.trim().replace("\n", ", "));
            return true;
        }
    }
    println!("{} on port {} is NOT running.", process_name, port);
    false
}

// Define the StorageConfig struct to mirror storage_config.yaml
#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_disk_space_gb: u64,
    pub min_disk_space_gb: u64,
    pub use_raft_for_scale: bool,
}

/// Loads the Storage daemon configuration from `storage_daemon_server/storage_config.yaml`.
pub fn load_storage_config(config_file_path: Option<PathBuf>) -> Result<StorageConfig, anyhow::Error> {
    let default_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // Go up to the workspace root of the server crate
        .ok_or_else(|| anyhow::anyhow!("Failed to get parent directory of server crate"))?
        .join("storage_daemon_server")
        .join("storage_config.yaml");

    let path_to_use = config_file_path.unwrap_or(default_config_path);

    let config_content = fs::read_to_string(&path_to_use)
        .map_err(|e| anyhow::anyhow!("Failed to read storage config file {}: {}", path_to_use.display(), e))?;

    let config: StorageConfig = serde_yaml::from_str(&config_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse storage config file {}: {}", path_to_use.display(), e))?;

    Ok(config)
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
        #[arg(long = "storage-port", value_name = "STORAGE_PORT", help = "Port for the standalone Storage daemon.")]
        storage_port: Option<u16>,
        #[arg(long = "storage-config", value_name = "STORAGE_CONFIG_FILE", help = "Path to the storage daemon's configuration file.")]
        storage_config_file: Option<PathBuf>,
    },
    Stop,
    /// Commands related to the standalone Storage daemon
    #[clap(subcommand)]
    StorageCommand(StorageAction), // Re-added StorageCommand
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PidStore { // Made public
    pid: u32,
}

#[derive(Subcommand, Debug)] // Re-added StorageAction enum
pub enum StorageAction {
    /// Start the standalone Storage daemon
    Start {
        /// The port for the standalone Storage daemon. If not provided, the storage daemon will use its own configured default.
        #[clap(long)]
        port: Option<u16>,
        /// Path to the storage daemon's configuration file (default: storage_config.yaml in daemon's CWD).
        #[clap(long, default_value = "storage_config.yaml")]
        config_file: PathBuf,
    },
    /// Stop the standalone Storage daemon
    Stop {
        /// The port of the standalone Storage daemon to stop. If not provided, it attempts to stop the daemon on its common default port (8085).
        #[clap(long)]
        port: Option<u16>,
    },
    /// Check the status of the standalone Storage daemon
    Status {
        /// The port of the standalone Storage daemon to check. If not provided, it checks the daemon on the commonly assumed default port (8085).
        #[clap(long)]
        port: Option<u16>,
    },
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
    StorageStart, // Re-added
    StorageStop,  // Re-added
    StorageStatus,// Re-added
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
        "storage" => { // Re-added interactive storage commands parsing
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "start" => CommandType::StorageStart,
                    "stop" => CommandType::StorageStop,
                    "status" => CommandType::StorageStatus,
                    _ => CommandType::Unknown,
                }
            } else {
                CommandType::Unknown
            }
        }
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
        }
        CommandType::DaemonStop => {
            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let mut handles = daemon_handles.lock().await;

            if let Some(port) = port_arg {
                if let Some((_, tx)) = handles.remove(&port) {
                    println!("Sending stop signal to daemon on port {}...", port);
                    if tx.send(()).is_err() {
                        eprintln!("Failed to send shutdown signal to daemon on port {}. It might have already stopped.", port);
                    } else {
                        println!("Daemon on port {} stopping...", port);
                    }
                } else {
                    println!("No daemon found running on port {} (managed by this CLI).", port);
                }
            } else {
                println!("Usage: daemon stop <port>");
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
        }
        CommandType::DaemonVersion => {
            println!("Not implemented: Daemon version check.");
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

                                let (tx, rx) = oneshot::channel();
                                let current_port = port; // Capture port for the spawned task
                                let cluster_range = format!("{}-{}", start_port, end_port);
                                let task_skip_ports = base_skip_ports.clone(); // Clone for each task

                                let daemon_join_handle = tokio::spawn(async move {
                                    let result = start_daemon(Some(current_port), Some(cluster_range), task_skip_ports).await;
                                    match result {
                                        Ok(_) => println!("Daemon on port {} started successfully in cluster.", current_port),
                                        Err(e) => eprintln!("Failed to start daemon on port {}: {:?}", current_port, e),
                                    }
                                    let _ = rx.await; // Wait for shutdown signal
                                    println!("Daemon on port {} is shutting down...", current_port);
                                });
                                handles.insert(port, (daemon_join_handle, tx));
                                started_any = true;
                            }
                            if started_any {
                                println!("Cluster initiation successful (check logs for full status).");
                            } else {
                                println!("No new daemons were started in the cluster range {}-{}.", start_port, end_port);
                            }
                        } else {
                            println!("Invalid port range or range exceeds 10 ports. Use: daemon start-cluster <start_port>-<end_port> (max 10 ports)");
                        }
                    } else {
                        println!("Invalid port numbers in range. Use: daemon start-cluster <start_port>-<end_port>");
                    }
                } else {
                    println!("Invalid range format. Use: daemon start-cluster <start_port>-<end_port>");
                }
            } else {
                println!("Usage: daemon start-cluster <start_port>-<end_port>");
            }
        }
        CommandType::DaemonStopCluster => {
            println!("Not implemented: Stop a specific cluster. Use `list-daemons` and `daemon stop <port>` for now, or `clear-daemons`.");
        }
        CommandType::DaemonClusterStatus => {
            println!("Not implemented: Daemon cluster status check.");
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
        }
        CommandType::ClearAllDaemons => {
            let mut handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No daemons to clear managed by this CLI.");
                return;
            }
            println!("Stopping all {} managed daemons...", handles.len());

            let mut stopped_count = 0;
            let mut failed_count = 0;
            let ports: Vec<u16> = handles.keys().cloned().collect();

            for port in ports {
                if let Some((_, tx)) = handles.remove(&port) {
                    println!("Sending stop signal to daemon on port {}...", port);
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
            let stop_result = stop_daemon();
            match stop_result {
                Ok(()) => println!("Global daemon stop signal sent successfully."),
                Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
            }

            println!("Stopped {} daemons. Failed to signal {} daemons (managed by this CLI).", stopped_count, failed_count);
        }
        CommandType::RestApiStart => {
            let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
            let mut rest_handle_guard = rest_api_handle.lock().await;
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;

            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let rest_port = port_arg.unwrap_or(8082); // Default REST API port for interactive mode

            if rest_api_port_guard.is_some() {
                println!("REST API server is already running on port {}.", rest_api_port_guard.unwrap());
                return;
            }

            if rest_port < 1024 || rest_port > 65535 {
                eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port);
                return;
            }

            // Check if any daemon is running on this port
            let daemon_handles_locked = daemon_handles.lock().await;
            if daemon_handles_locked.contains_key(&rest_port) {
                eprintln!("Cannot start REST API on port {} because a daemon is already running there (managed by this CLI).", rest_port);
                return;
            }
            drop(daemon_handles_locked); // Release lock before trying to kill processes

            // Attempt to kill the process directly using lsof/kill
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
                return;
            }

            println!("Starting REST API server on port {}...", rest_port);

            // Daemonize REST API server
            let mut daemonize_builder = DaemonizeBuilder::new()
                .working_directory("/tmp") // This is acceptable as a temporary working directory for the daemon process, not direct file access for data storage
                .umask(0o027)
                .process_name(&format!("rest-api-{}", rest_port))
                .host("127.0.0.1")
                .port(rest_port)
                .skip_ports(vec![]); // REST API server should bind this port

            match daemonize_builder.fork_only() {
                Ok(child_pid) => {
                    if child_pid == 0 {
                        // In REST API child daemon process
                        let child_rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for REST API daemon child");
                        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel(); // Dummy channel for daemon child
                        let result = child_rt.block_on(start_rest_server(rest_port, shutdown_rx, "".to_string()));
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
                    }
                }
                Err(e) => {
                    eprintln!("Failed to daemonize REST API server: {:?}", e);
                }
            }
        }
        CommandType::RestApiStop => {
            let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
            let mut rest_handle_guard = rest_api_handle.lock().await;
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;

            if let Some(port) = rest_api_port_guard.take() { // Take the port, assuming it's stopping
                println!("Attempting to stop REST API server on port {}...", port);

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

            } else {
                println!("REST API server is not running (managed by this CLI).");
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
        }
        CommandType::StorageStart => {
            eprintln!("Interactive 'storage start' command is deprecated. Please use 'graphdb-cli start --storage-port <port>' for detached daemonization.");
        }
        CommandType::StorageStop => {
            let port_to_stop = args.get(0).and_then(|s| s.parse::<u16>().ok()).unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);

            if let Err(e) = stop_process_by_port("storage-daemon", port_to_stop) {
                eprintln!("Error stopping Standalone Storage daemon on port {}: {}", port_to_stop, e);
            } else {
                println!("Standalone Storage daemon stop command processed for port {}.", port_to_stop);
            }
        }
        CommandType::StorageStatus => {
            let port_to_check = args.get(0).and_then(|s| s.parse::<u16>().ok()).unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            check_process_status_by_port("storage-daemon", port_to_check);
        }
        CommandType::Help => {
            print_help();
        }
        CommandType::Exit => {
            println!("Exiting CLI.");
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
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
    println!("  clear-daemons               - Stop all managed daemons and send global stop to all daemon processes.");
    println!("");
    println!("  rest start [port]          - Start the REST API server on a specified port (default 8082).");
    println!("  rest stop                  - Stop the REST API server.");
    println!("  rest status                - Check if the REST API server is running.");
    println!("  rest health                - (Use HTTP client) Check REST API health.");
    println!("  rest version               - (Use HTTP client) Get REST API version.");
    println!("  rest register-user         - (Use HTTP client) Register a new user via REST API.");
    println!("  rest authenticate          - (Use HTTP client) Authenticate a user via REST API.");
    println!("  rest graph-query           - (Use HTTP client) Send a graph query to the REST API.");
    println!("  rest storage-query         - (Use HTTP client) Send a storage query to the REST API.");
    println!("");
    println!("  storage start [port]       - Start the standalone Storage daemon on a specified port. If no port, it lets the storage daemon use its own configured default (from storage_config.yaml, usually 8085).");
    println!("  storage stop [port]        - Stop the standalone Storage daemon (attempts to kill by port). If no port, it attempts to stop the daemon on the commonly assumed default port (8085).");
    println!("  storage status [port]      - Check if the standalone Storage daemon is running (by port). If no port, it checks the daemon on the commonly assumed default port (8085).");
    println!("");
    println!("  help                       - Display this help message.");
    println!("  exit / quit / q            - Exit the CLI application.");
}

/// Main asynchronous loop for the CLI interactive mode.
/// This function is called by `start_cli` when interactive mode is detected.
async fn main_loop() {
    let daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let rest_api_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::new(None)); // Tracks the port if REST API is running
    let rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(None));


    println!("GraphDB CLI. Type 'help' for commands.");

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut input = String::new();

    loop {
        print!("graphdb> ");
        let _ = std::io::stdout().flush(); // Ensure prompt is displayed

        input.clear();
        if let Err(e) = reader.read_line(&mut input).await {
            eprintln!("Failed to read line: {}", e);
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

    // Stop REST API server if it was started by this CLI
    let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
    let mut rest_handle_guard = rest_api_handle.lock().await;
    let mut rest_api_port_guard = rest_api_port_arc.lock().await;

    if let Some(port) = rest_api_port_guard.take() {
        println!("Attempting to stop REST API server on port {} during exit...", port);

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
    }


    // Stop all managed daemons
    let mut handles = daemon_handles.lock().await;
    if !handles.is_empty() {
        println!("Stopping all managed daemon instances...");
        let mut join_handles = Vec::new();
        for (port, (handle, tx)) in handles.drain() { // Corrected: `drain()` takes no arguments.
            println!("Signaling daemon on port {} to stop.", port);
            if tx.send(()).is_err() {
                eprintln!("Warning: Daemon on port {} already stopped or signal failed.", port);
            }
            join_handles.push(handle);
        }
        for handle in join_handles {
            let _ = handle.await; // Wait for each daemon task to complete
        }
        println!("All managed daemon instances stopped.");
    }

    // Send a global stop signal to ensure any external daemon processes are terminated
    println!("Sending global stop signal to all daemon processes...");
    let stop_result = stop_daemon();
    match stop_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }

    println!("GraphDB CLI shutdown complete. Goodbye!");
}

// The main entry point for the CLI logic, called by server/src/main.rs
pub fn start_cli() {
    // Load configuration at the very beginning of the CLI start.
    // This part remains as it's about CLI's own config, not daemon's runtime behavior.
    let config = match crate::cli::config::load_cli_config() { // Corrected path to config module
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Error loading configuration: {}", e);
            eprintln!("Attempted to load from: {}", std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("src")
                .join("cli")
                .join("config.toml")
                .display());
            std::process::exit(1);
        }
    };

    let args = CliArgs::parse();

    // Create a new Tokio runtime for handling async operations
    let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for CLI");

    if let Some(query_string) = args.query {
        println!("Executing direct query: {}", query_string);
        // Corrected: Use the imported parse_query_from_string and QueryType
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
            GraphDbCommands::Start { port, cluster, listen_port, storage_port, storage_config_file } => {
                // Variables to store startup summary
                let mut daemon_status_msg = "Not launched".to_string();
                let mut rest_api_status_msg = "Not launched".to_string();
                let mut storage_status_msg = "Not launched".to_string();

                // Determine the REST API port if provided
                let explicit_rest_api_port = listen_port;
                let explicit_storage_port = storage_port;

                // Prepare skip_ports for daemons, including the REST API and Storage ports if explicitly set.
                let mut skip_ports = Vec::new();
                if let Some(rest_p) = explicit_rest_api_port {
                    skip_ports.push(rest_p);
                }
                if let Some(storage_p) = explicit_storage_port {
                    skip_ports.push(storage_p);
                }


                // Start core daemons (single or cluster), always skipping the REST API and Storage ports!
                let daemon_result = rt.block_on(start_daemon(port, cluster.clone(), skip_ports.clone()));
                match daemon_result {
                    Ok(()) => {
                        if let Some(cluster_range) = cluster {
                            daemon_status_msg = format!("Running on cluster ports: {}", cluster_range);
                        } else if let Some(p) = port {
                            daemon_status_msg = format!("Running on port: {}", p);
                        } else {
                            // Default daemon port if neither cluster nor explicit port is given
                            daemon_status_msg = format!("Running on default port: {}", config.server.port.unwrap_or(8080));
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to start daemon(s): {:?}", e);
                        daemon_status_msg = format!("Failed to start ({:?})", e);
                        // Do not exit here, allow other components to try starting
                    }
                }

                // REST API daemonization logic (only if --listen-port given)
                if let Some(rest_port) = explicit_rest_api_port {
                    if rest_port < 1024 || rest_port > 65535 {
                        eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port);
                        rest_api_status_msg = format!("Invalid port: {}", rest_port);
                    } else {
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
                            rest_api_status_msg = format!("Failed to free up port {}.", rest_port);
                        } else {
                            println!("Starting REST API server on port {}...", rest_port);

                            // Daemonize REST API server
                            let mut daemonize_builder = DaemonizeBuilder::new()
                                .working_directory("/tmp") // This is acceptable as a temporary working directory for the daemon process, not direct file access for data storage
                                .umask(0o027)
                                .process_name(&format!("rest-api-{}", rest_port))
                                .host("127.0.0.1")
                                .port(rest_port)
                                .skip_ports(vec![]); // REST API server should bind this port

                            match daemonize_builder.fork_only() {
                                Ok(child_pid) => {
                                    if child_pid == 0 {
                                        // In REST API child daemon: RUN THE REST API SERVER!
                                        let child_rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for REST API daemon child");
                                        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
                                        let result = child_rt.block_on(start_rest_server(rest_port, shutdown_rx, "".to_string()));
                                        if let Err(e) = result {
                                            eprintln!("REST API server failed: {:?}", e);
                                            std::process::exit(1); // Exit child process on failure
                                        }
                                        std::process::exit(0); // Exit child process on success
                                    } else {
                                        println!("REST API server daemonized with PID {}", child_pid);
                                        rest_api_status_msg = format!("Running on port: {}", rest_port);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to daemonize REST API server: {:?}", e);
                                    rest_api_status_msg = format!("Failed to daemonize ({:?})", e);
                                }
                            }
                        }
                    }
                }

                // Storage daemonization logic (only if --storage-port given)
                if let Some(s_port) = explicit_storage_port {
                    if s_port < 1024 || s_port > 65535 {
                        eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", s_port);
                        storage_status_msg = format!("Invalid port: {}", s_port);
                    } else {
                        // Kill any process on storage_port before daemonizing Storage daemon
                        let output = Command::new("lsof")
                            .arg("-i")
                            .arg(format!(":{}", s_port))
                            .arg("-t")
                            .output();
                        if let Ok(output) = output {
                            if !output.stdout.is_empty() {
                                let pids = String::from_utf8_lossy(&output.stdout);
                                for pid in pids.trim().lines() {
                                    if let Ok(pid) = pid.parse::<i32>() {
                                        println!("Killing process {} on storage port {}", pid, s_port);
                                        let _ = Command::new("kill")
                                            .arg("-9")
                                            .arg(pid.to_string())
                                            .output();
                                    }
                                }
                            }
                        }

                        // Wait for storage port to be released
                        let addr = format!("127.0.0.1:{}", s_port);
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
                            eprintln!("Failed to free up storage port {} after killing processes. Try again.", s_port);
                            storage_status_msg = format!("Failed to free up port {}.", s_port);
                        } else {
                            println!("Starting Storage daemon on port {}...", s_port);
                            let loaded_storage_config = load_storage_config(storage_config_file.clone());
                            match loaded_storage_config {
                                Ok(cfg) => {
                                    println!("  Using config file: {}", storage_config_file.as_ref().map_or("default".to_string(), |p| p.display().to_string()));
                                    println!("  Storage Metrics:");
                                    println!("    Data Directory: {}", cfg.data_directory);
                                    println!("    Log Directory: {}", cfg.log_directory);
                                    println!("    Default Port (from config): {}", cfg.default_port);
                                    println!("    Cluster Range (from config): {}", cfg.cluster_range);
                                    println!("    Max Disk Space: {} GB", cfg.max_disk_space_gb);
                                    println!("    Min Disk Space: {} GB", cfg.min_disk_space_gb);
                                    println!("    Use Raft for Scale: {}", cfg.use_raft_for_scale);
                                }
                                Err(e) => {
                                    eprintln!("Error loading storage config: {:?}", e);
                                }
                            }

                            // Determine the path to the storage config file for the daemon process
                            let actual_storage_config_path = storage_config_file.unwrap_or_else(|| {
                                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                                    .parent()
                                    .expect("Failed to get parent directory of server crate")
                                    .join("storage_daemon_server")
                                    .join("storage_config.yaml")
                            });

                            // Daemonize Storage daemon
                            let mut daemonize_builder = DaemonizeBuilder::new()
                                .working_directory("/tmp") // Temporary working directory for the daemon process
                                .umask(0o027)
                                .process_name(&format!("storage-daemon-{}", s_port))
                                .host("127.0.0.1")
                                .port(s_port)
                                .skip_ports(vec![]); // Storage daemon should bind this port

                            match daemonize_builder.fork_only() {
                                Ok(child_pid) => {
                                    if child_pid == 0 {
                                        // In Storage daemon child process: RUN THE STORAGE SERVER!
                                        let child_rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for Storage daemon child");
                                        // The run_storage_daemon function handles its own Ctrl+C listener and shutdown channel.
                                        let result = child_rt.block_on(start_storage_server(Some(s_port), actual_storage_config_path)); // Corrected arguments
                                        if let Err(e) = result {
                                            eprintln!("Storage daemon failed: {:?}", e);
                                            std::process::exit(1); // Exit child process on failure
                                        }
                                        std::process::exit(0); // Exit child process on success
                                    } else {
                                        println!("Storage daemon daemonized with PID {}", child_pid);
                                        storage_status_msg = format!("Running on port: {}", s_port);
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Failed to daemonize Storage daemon: {:?}", e);
                                    storage_status_msg = format!("Failed to daemonize ({:?})", e);
                                }
                            }
                        }
                    }
                } else {
                    // If no explicit storage port, assume default if the daemon starts with its own default
                    // This is a heuristic for reporting; the daemon itself truly determines its port.
                    let loaded_storage_config = load_storage_config(None); // Load default config
                    match loaded_storage_config {
                        Ok(cfg) => {
                            storage_status_msg = format!("Running on default port: {}", cfg.default_port);
                            println!("  Storage Metrics (from default config):");
                            println!("    Data Directory: {}", cfg.data_directory);
                            println!("    Log Directory: {}", cfg.log_directory);
                            println!("    Default Port (from config): {}", cfg.default_port);
                            println!("    Cluster Range (from config): {}", cfg.cluster_range);
                            println!("    Max Disk Space: {} GB", cfg.max_disk_space_gb);
                            println!("    Min Disk Space: {} GB", cfg.min_disk_space_gb);
                            println!("    Use Raft for Scale: {}", cfg.use_raft_for_scale);
                        }
                        Err(e) => {
                            eprintln!("Error loading default storage config: {:?}", e);
                            storage_status_msg = format!("Failed to load default config ({:?})", e);
                        }
                    }
                }

                // Final summary output as a formatted table
                println!("\n--- Component Startup Summary ---");
                println!("{:<15} {:<50}", "Component", "Status");
                println!("{:-<15} {:-<50}", "", "");
                println!("{:<15} {:<50}", "GraphDB", daemon_status_msg);
                println!("{:<15} {:<50}", "REST API", rest_api_status_msg);
                println!("{:<15} {:<50}", "Storage", storage_status_msg);
                println!("---------------------------------\n");

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
            // Re-added StorageCommand handling
            GraphDbCommands::StorageCommand(action) => {
                // These interactive commands are deprecated, but the logic remains for direct calls.
                let rt_local = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for CLI storage command"); // Use a distinct name
                match action {
                    StorageAction::Start { port, config_file } => {
                        eprintln!("Interactive 'storage start' command is deprecated. Please use 'graphdb-cli start --storage-port <port>' for detached daemonization.");
                    }
                    StorageAction::Stop { port } => {
                        let port_to_stop = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
                        rt_local.block_on(async { // Use rt_local.block_on for async call in sync context
                            if let Err(e) = stop_process_by_port("storage-daemon", port_to_stop) {
                                eprintln!("Error stopping Standalone Storage daemon on port {}: {}", port_to_stop, e);
                            } else {
                                println!("Standalone Storage daemon stop command processed for port {}.", port_to_stop);
                            }
                        });
                    }
                    StorageAction::Status { port } => {
                        let port_to_check = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
                        check_process_status_by_port("storage-daemon", port_to_check);
                    }
                }
                return;
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
        println!("Experimental plugins is enabled.");
        return;
    }

    // Default to interactive CLI if no other commands/args are given
    rt.block_on(main_loop());
}


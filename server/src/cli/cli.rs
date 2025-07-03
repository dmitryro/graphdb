// server/src/cli/cli.rs
// Complete file: 2025-07-02 - Final integration of all parts,
// including consistent daemonization logic for REST API and Storage components.
// Enhanced status command for comprehensive system overview.
// FIX: Corrected daemonization argument parsing for storage daemon.
// FIX: Implemented dynamic port discovery for Storage Daemon status reporting.
// FIX: Finalized status command structure to allow `status rest`, `status daemon`, `status storage`
//      as subcommands of `status`, while `status` alone provides a summary.
// FIX: Corrected 'anyihow' typo to 'anyhow'.
// FIX: Corrected 'value' attribute to 'value_name' for clap arg.

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
use std::process; // Added for process::exit
use tokio::process::Command; // Used for spawning daemonized processes
use std::str::FromStr; // Added: Required for FromStr trait implementation for StorageEngineType

use daemon_api::{start_daemon, stop_daemon};
use rest_api::start_server as start_rest_server; // Alias to avoid name collision, removed graphdb_ prefix
use storage_daemon_server::run_storage_daemon as start_storage_server; // Corrected: Use run_storage_daemon directly from lib.rs

// Imports for CLI argument parsing and daemonization logic
use clap::{Parser, Subcommand, Args}; // Added Args for the new StatusArgs struct
use lib::query_parser::{parse_query_from_string, QueryType}; // Removed graphdb_ prefix
use serde::{Serialize, Deserialize};
use std::collections::HashSet;
use lazy_static::lazy_static;
use reqwest; // Added for REST API calls (ensure reqwest = "0.11" or compatible is in Cargo.toml)
use serde_json; // Added for JSON serialization/deserialization

// Import the configuration loading and types (corrected path)
use crate::cli::config::{load_cli_config, StorageEngineType};


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
fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> {
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    let output = std::process::Command::new("lsof") // Use std::process::Command for lsof/kill
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
        match std::process::Command::new("kill").arg("-9").arg(pid.to_string()).status() {
            Ok(status) if status.success() => println!("Process {} killed successfully.", pid),
            Ok(_) => eprintln!("Failed to kill process {}.", pid),
            Err(e) => eprintln!("Error killing process {}: {}", pid, e),
        }
    }
    Ok(())
}

/// Helper to check if a process is running on a given port. This is used for all daemon processes.
fn check_process_status_by_port(process_name: &str, port: u16) -> bool {
    let output = std::process::Command::new("lsof") // Use std::process::Command for lsof
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
        .output();

    if let Ok(output) = output {
        let pids = String::from_utf8_lossy(&output.stdout);
        if !pids.trim().is_empty() {
            // println!("{} on port {} is running with PID(s): {}.", process_name, port, pids.trim().replace("\n", ", "));
            return true;
        }
    }
    // println!("{} on port {} is NOT running.", process_name, port);
    false
}

// Define the StorageConfig struct to mirror the content under 'storage:' in storage_config.yaml.
// This struct is now the inner representation of the storage configuration.
#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_disk_space_gb: u64,
    pub min_disk_space_gb: u64,
    pub use_raft_for_scale: bool,
    pub storage_engine_type: String, // Added this field for engine selection
}

// Define a wrapper struct to match the 'storage:' key in the YAML config.
// This will be used by the CLI to correctly parse the YAML.
#[derive(Debug, Deserialize)]
struct StorageConfigWrapper {
    storage: StorageConfig,
}

/// Loads the Storage daemon configuration from `storage_daemon_server/storage_config.yaml`.
/// This function now correctly handles the top-level `storage:` key.
pub fn load_storage_config(config_file_path: Option<PathBuf>) -> Result<StorageConfig, anyhow::Error> {
    let default_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent() // Go up to the workspace root of the server crate
        .ok_or_else(|| anyhow::anyhow!("Failed to get parent directory of server crate"))?
        .join("storage_daemon_server")
        .join("storage_config.yaml");

    let path_to_use = config_file_path.unwrap_or(default_config_path);

    let config_content = fs::read_to_string(&path_to_use)
        .map_err(|e| anyhow::anyhow!("Failed to read storage config file {}: {}", path_to_use.display(), e))?;

    // Parse into the wrapper struct which correctly handles the 'storage:' key
    let wrapper: StorageConfigWrapper = serde_yaml::from_str(&config_content)
        .map_err(|e| anyhow::anyhow!("Failed to parse storage config file {}: {}", path_to_use.display(), e))?; // Corrected typo here

    Ok(wrapper.storage) // Return the inner StorageConfig
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

    // Internal flags for daemonized processes. Hidden from user help.
    #[arg(long, hide = true)]
    pub internal_rest_api_run: bool,
    #[arg(long, hide = true)]
    pub internal_storage_daemon_run: bool,
    #[arg(long, hide = true)]
    pub internal_port: Option<u16>,
    #[arg(long, hide = true)]
    pub internal_storage_config_path: Option<PathBuf>,
    #[arg(long, hide = true)]
    pub internal_storage_engine: Option<StorageEngineType>,


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
    /// Get a comprehensive status summary of all GraphDB components or specific component status.
    Status(StatusArgs), // Now takes a StatusArgs struct
    
    /// Commands related to the standalone Storage daemon
    #[clap(subcommand)]
    StorageCommand(StorageAction), // Re-added StorageCommand
}

#[derive(Args, Debug)] // Use Args for a struct that holds subcommands
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: Option<StatusAction>, // This can now be Option<StatusAction>!
}

#[derive(Subcommand, Debug)]
pub enum StatusAction {
    /// Check the status of the REST API server.
    Rest,
    /// Check the status of a specific GraphDB daemon.
    Daemon {
        /// The port of the daemon to check.
        #[arg(long)]
        port: Option<u16>,
    },
    /// Check the status of the standalone Storage daemon.
    Storage {
        /// The port of the Storage daemon to check. If not provided, it checks the commonly assumed default port (8085).
        #[arg(long)]
        port: Option<u16>,
    },
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
    DaemonStatus, // For interactive `daemon status <port>`
    DaemonHealth,
    DaemonVersion,
    DaemonStartCluster,
    DaemonStopCluster,
    DaemonClusterStatus,
    ListDaemons,
    ClearAllDaemons,
    RestApiStart,
    RestApiStop,
    RestApiStatus, // For interactive `rest status`
    RestApiHealth,
    RestApiVersion,
    RestApiRegisterUser,
    RestApiAuthenticate,
    RestApiGraphQuery,
    RestApiStorageQuery,
    StorageStart, // For interactive `storage start`
    StorageStop,  // For interactive `storage stop`
    StorageStatus,// For interactive `storage status`
    StatusSummary, // For interactive `status` (overall)
    StatusRest, // For interactive `status rest`
    StatusDaemon, // For interactive `status daemon`
    StatusStorage, // For interactive `status storage`
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
        "status" => { // Handle `status` and its subcommands for interactive mode
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => CommandType::StatusRest,
                    "daemon" => CommandType::StatusDaemon,
                    "storage" => CommandType::StatusStorage,
                    _ => CommandType::StatusSummary, // Fallback to summary if unknown subcommand
                }
            } else {
                CommandType::StatusSummary // Default to summary if no subcommand given
            }
        }
        "help" => CommandType::Help,
        "exit" | "quit" | "q" => CommandType::Exit, // Added 'q' for exit
        _ => CommandType::Unknown,
    };

    (cmd_type, args)
}

/// Helper function to find a running storage daemon's port.
/// Scans a range of common ports using `lsof`.
async fn find_running_storage_daemon_port() -> Option<u16> {
    // Check a range of ports around the default
    let common_storage_ports_to_check = (CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS..=CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS + 10).collect::<Vec<u16>>();
    for port in common_storage_ports_to_check {
        let output = tokio::process::Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output()
            .await;

        if let Ok(output) = output {
            let pids = String::from_utf8_lossy(&output.stdout);
            if !pids.trim().is_empty() {
                // Found a process listening on this port. Assume it's our storage daemon.
                // This is a heuristic; a more robust solution would involve PID files or health checks.
                return Some(port);
            }
        }
    }
    None
}

/// Displays detailed status for the REST API server.
async fn display_rest_api_status() {
    let rest_port = get_default_rest_port_from_config();

    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let rest_health_url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
    let rest_version_url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();

    match client.get(&rest_health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            rest_api_status = "Running".to_string();
            let version_info = client.get(&rest_version_url).send().await;
            match version_info {
                Ok(v_resp) if v_resp.status().is_success() => {
                    let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    rest_api_details = format!("Version: {}", version);
                },
                _ => rest_api_details = "Version: N/A".to_string(),
            }
        },
        _ => { /* Status remains "Down" */ },
    }
    println!("{:<15} {:<10} {:<40}", rest_api_status, rest_port, rest_api_details);
    println!("--------------------------------------------------");
}

/// Displays detailed status for a specific GraphDB daemon or lists common ones.
async fn display_daemon_status(port_arg: Option<u16>) {
    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    if let Some(port) = port_arg {
        let status_message = if check_process_status_by_port("GraphDB Daemon", port) {
            "Running".to_string()
        } else {
            "Down".to_string()
        };
        println!("{:<15} {:<10} {:<40}", status_message, port, "Core Graph Processing");
    } else {
        // If no port specified, check common daemon ports
        let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
        let mut found_any = false;
        for &port in &common_daemon_ports {
            if check_process_status_by_port("GraphDB Daemon", port) {
                println!("{:<15} {:<10} {:<40}", "Running", port, "Core Graph Processing");
                found_any = true;
            }
        }
        if !found_any {
            println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No daemons found on common ports.");
        }
        println!("\nTo check a specific daemon, use 'status daemon --port <port>'.");
    }
    println!("--------------------------------------------------");
}

/// Displays detailed status for the standalone Storage daemon.
async fn display_storage_daemon_status(port_arg: Option<u16>) {
    let port_to_check = if let Some(p) = port_arg {
        p
    } else {
        find_running_storage_daemon_port().await.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
    };

    let storage_config = load_storage_config(None)
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config for status check: {}. Using defaults.", e);
            StorageConfig {
                data_directory: "/tmp/graphdb_data".to_string(),
                log_directory: "/var/log/graphdb".to_string(),
                default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                cluster_range: "9000-9002".to_string(),
                max_disk_space_gb: 1000,
                min_disk_space_gb: 10,
                use_raft_for_scale: true,
                storage_engine_type: "sled".to_string(),
            }
        });

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let storage_engine_type_str = storage_config.storage_engine_type;
    let status_message = if check_process_status_by_port("Storage Daemon", port_to_check) {
        "Running".to_string()
    } else {
        "Down".to_string()
    };
    println!("{:<15} {:<10} {:<40}", status_message, port_to_check, format!("Type: {}", storage_engine_type_str));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Data Dir: {}", storage_config.data_directory));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Log Dir: {}", storage_config.log_directory));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Default Port (Config): {}", storage_config.default_port));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Cluster Range: {}", storage_config.cluster_range));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Max Disk: {} GB", storage_config.max_disk_space_gb));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Min Disk: {} GB", storage_config.min_disk_space_gb));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Raft Enabled: {}", storage_config.use_raft_for_scale));
    println!("--------------------------------------------------");
}


/// Displays a comprehensive status summary of all GraphDB components.
async fn display_full_status_summary() {
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    // --- 1. GraphDB Daemon Status ---
    let mut daemon_status_msg = "Down".to_string();
    let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005]; // Common ports for daemons/cluster
    let mut running_daemon_ports = Vec::new();

    for &port in &common_daemon_ports {
        let output = std::process::Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output();
        if let Ok(output) = output {
            if !output.stdout.is_empty() {
                running_daemon_ports.push(port.to_string());
            }
        }
    }
    if !running_daemon_ports.is_empty() {
        daemon_status_msg = format!("Running on: {}", running_daemon_ports.join(", "));
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status_msg, "N/A", "Core Graph Processing");

    // --- 2. REST API Status ---
    let rest_port = get_default_rest_port_from_config(); // Use default for external check
    let rest_health_url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
    let rest_version_url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();

    match client.get(&rest_health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            rest_api_status = "Running".to_string();
            let version_info = client.get(&rest_version_url).send().await;
            match version_info {
                Ok(v_resp) if v_resp.status().is_success() => {
                    let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    rest_api_details = format!("Version: {}", version);
                },
                _ => rest_api_details = "Version: N/A".to_string(),
            }
        },
        _ => { /* Status remains "Down" */ },
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, rest_port, rest_api_details);

    // --- 3. Storage Daemon Status ---
    let storage_config = load_storage_config(None)
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config for status check: {}. Using defaults.", e);
            StorageConfig {
                data_directory: "/tmp/graphdb_data".to_string(),
                log_directory: "/var/log/graphdb".to_string(),
                default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                cluster_range: "9000-9002".to_string(),
                max_disk_space_gb: 1000,
                min_disk_space_gb: 10,
                use_raft_for_scale: true,
                storage_engine_type: "sled".to_string(),
            }
        });

    let mut storage_daemon_status = "Down".to_string();
    let mut actual_storage_port_reported = storage_config.default_port; // Start with default from config

    if let Some(found_port) = find_running_storage_daemon_port().await {
        storage_daemon_status = "Running".to_string();
        actual_storage_port_reported = found_port;
    }

    println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, actual_storage_port_reported, format!("Type: {}", storage_config.storage_engine_type));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_directory));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Log Dir: {}", storage_config.log_directory));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Range: {}", storage_config.cluster_range));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Disk: {} GB", storage_config.max_disk_space_gb));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Min Disk: {} GB", storage_config.min_disk_space_gb));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Raft Enabled: {}", storage_config.use_raft_for_scale));

    println!("----------------------------------------------------------------------------------");
    println!("Note: 'Running' status indicates port responsiveness. For full health, check logs.");
}


// Handler for CLI commands (interactive mode)
async fn handle_command(
    command: CommandType,
    args: Vec<String>,
    daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>, // Now Option<u16> to reflect if it's running
    rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
) -> Result<()> { // Changed return type to Result<()>
    match command {
        CommandType::DaemonStart => {
            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let port = port_arg.unwrap_or(8080); // Default port for `daemon start`

            let mut handles = daemon_handles.lock().await;

            if handles.contains_key(&port) {
                println!("Daemon on port {} is already running (managed by this CLI).", port);
                return Ok(());
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
            display_daemon_status(port_arg).await;
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
                return Ok(());
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
            let rest_port = port_arg.unwrap_or(get_default_rest_port_from_config()); // Default REST API port for interactive mode

            if rest_api_port_guard.is_some() {
                println!("REST API server is already running on port {}.", rest_api_port_guard.unwrap());
                return Ok(());
            }

            if rest_port < 1024 || rest_port > 65535 {
                eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port);
                return Ok(());
            }

            // Check if any daemon is running on this port
            let daemon_handles_locked = daemon_handles.lock().await;
            if daemon_handles_locked.contains_key(&rest_port) {
                eprintln!("Cannot start REST API on port {} because a daemon is already running there (managed by this CLI).", rest_port);
                return Ok(());
            }
            drop(daemon_handles_locked); // Release lock before trying to kill processes

            // Attempt to kill the process directly using lsof/kill
            stop_process_by_port("REST API", rest_port)?; // Use the helper function

            // Wait for port to be released
            let addr = format!("127.0.0.1:{}", rest_port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;

            while start_time.elapsed() < wait_timeout {
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(_) => {
                        port_freed = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if !port_freed {
                eprintln!("Failed to free up port {} after killing processes. Try again.", rest_port);
                return Ok(());
            }

            println!("Starting REST API server on port {}...", rest_port);

            // Load storage config for the REST API server to pass to daemonized process
            let current_storage_config = load_storage_config(None)
                .unwrap_or_else(|e| {
                    eprintln!("Warning: Could not load storage config for REST API daemonization: {}. Using defaults.", e);
                    // Create a default StorageConfig for fallback
                    StorageConfig {
                        data_directory: "/tmp/graphdb_data".to_string(),
                        log_directory: "/var/log/graphdb".to_string(),
                        default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                        cluster_range: "9000-9002".to_string(),
                        max_disk_space_gb: 1000,
                        min_disk_space_gb: 10,
                        use_raft_for_scale: true,
                        storage_engine_type: "sled".to_string(), // Default to sled
                    }
                });

            let storage_engine_type = StorageEngineType::from_str(&current_storage_config.storage_engine_type)
                .unwrap_or(StorageEngineType::Sled); // Default to Sled if parsing fails

            // Daemonize REST API server by spawning a new process
            let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
            let child = Command::new(&current_exe)
                .arg("--internal-rest-api-run") // Signal the new process to run as REST daemon
                .arg("--internal-port")
                .arg(rest_port.to_string())
                .arg("--internal-storage-config-path")
                .arg(current_storage_config.data_directory.clone()) // Pass data_directory as config_file for mock
                .arg("--internal-storage-engine")
                .arg(storage_engine_type.to_string()) // Pass the determined storage engine type
                .spawn()
                .context("Failed to spawn REST API daemon process")?;

            println!("REST API server daemonized with PID {}", child.id().unwrap_or(0));
            let (tx, rx) = oneshot::channel(); // Create a channel for the CLI to signal shutdown
            *rest_tx_guard = Some(tx);
            *rest_api_port_guard = Some(rest_port); // Store the actual port it's running on

            let handle = tokio::spawn(async move {
                let _ = rx.await; // This task will wait for the CLI to send a shutdown signal
                // In a real daemon, you might also have a signal handler here to kill the child process
            });
            *rest_handle_guard = Some(handle);
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
                stop_process_by_port("REST API", port)?; // Use the helper function

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
            display_rest_api_status().await;
        }
        CommandType::RestApiHealth => {
            let rest_port = get_default_rest_port_from_config();
            let url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
            let client = reqwest::Client::new();
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("REST API Health on port {}: Status: {}, Body: {}", rest_port, status, body);
                }
                Err(e) => {
                    eprintln!("Failed to connect to REST API on port {} for health check: {}", rest_port, e);
                }
            }
        }
        CommandType::RestApiVersion => {
            let rest_port = get_default_rest_port_from_config();
            let url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
            let client = reqwest::Client::new();
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("REST API Version on port {}: Status: {}, Body: {}", rest_port, status, body);
                }
                Err(e) => {
                    eprintln!("Failed to connect to REST API on port {} for version check: {}", rest_port, e);
                }
            }
        }
        CommandType::RestApiRegisterUser => {
            if args.len() < 3 {
                println!("Usage: rest register-user <username> <password>");
                return Ok(());
            }
            let username = &args[1];
            let password = &args[2];
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/register", rest_port);
            let request_body = serde_json::json!({
                "username": username,
                "password": password,
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Registration Response Status: {}", status);
                    println!("Registration Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send registration request: {}", e);
                }
            }
        }
        CommandType::RestApiAuthenticate => {
            if args.len() < 3 {
                println!("Usage: rest authenticate <username> <password>");
                return Ok(());
            }
            let username = &args[1];
            let password = &args[2];
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/auth", rest_port);
            let request_body = serde_json::json!({
                "username": username,
                "password": password,
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Authentication Response Status: {}", status);
                    println!("Authentication Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send authentication request: {}", e);
                }
            }
        }
        CommandType::RestApiGraphQuery => {
            if args.len() < 2 {
                println!("Usage: rest graph-query \"<query_string>\" [persist]");
                return Ok(());
            }
            let query_string = &args[1];
            let persist = args.get(2).map(|s| s.parse::<bool>().unwrap_or(false)).unwrap_or(false);

            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/query", rest_port);
            let request_body = serde_json::json!({
                "query": query_string,
                "persist": persist,
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Graph Query Response Status: {}", status);
                    println!("Graph Query Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send graph query: {}", e);
                }
            }
        }
        CommandType::RestApiStorageQuery => {
            println!("Not implemented: REST API Storage Query. Use `rest graph-query` for now.");
        }
        CommandType::StorageStart => {
            eprintln!("Interactive 'storage start' command is deprecated. Please use 'graphdb-cli start --storage-port <port>' for detached daemonization.");
        }
        CommandType::StorageStop => {
            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            let storage_port = port_arg.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            println!("Attempting to stop Storage Daemon on port {}...", storage_port);
            if let Err(e) = stop_process_by_port("Storage Daemon", storage_port) {
                eprintln!("Error stopping Storage Daemon: {}", e);
            }
        }
        CommandType::StorageStatus => {
            let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
            display_storage_daemon_status(port_arg).await;
        }
        CommandType::StatusSummary => {
            display_full_status_summary().await; // Call without arguments
        }
        CommandType::StatusRest => { // Handle `status rest`
            display_rest_api_status().await;
        }
        CommandType::StatusDaemon => { // Handle `status daemon`
            // For interactive mode, the port argument would be the second argument after "status daemon"
            // e.g., `status daemon 8080` or `status daemon --port 8080`
            let port_arg_index = args.iter().position(|s| s == "--port").map(|i| i + 1)
                                .or_else(|| args.get(1).map(|_| 1)); // If no --port, assume it's the first arg
            let port_arg = port_arg_index.and_then(|idx| args.get(idx)).and_then(|s| s.parse::<u16>().ok());
            display_daemon_status(port_arg).await;
        }
        CommandType::StatusStorage => { // Handle `status storage`
            // For interactive mode, the port argument would be the second argument after "status storage"
            let port_arg_index = args.iter().position(|s| s == "--port").map(|i| i + 1)
                                .or_else(|| args.get(1).map(|_| 1));
            let port_arg = port_arg_index.and_then(|idx| args.get(idx)).and_then(|s| s.parse::<u16>().ok());
            display_storage_daemon_status(port_arg).await;
        }
        CommandType::Help => {
            print_help();
        }
        CommandType::Exit => {
            println!("Exiting CLI. Goodbye!");
            // Perform any necessary cleanup before exiting
            let mut handles = daemon_handles.lock().await;
            for (_, (_, tx)) in handles.drain() {
                let _ = tx.send(()); // Send shutdown signal
            }
            let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
            if let Some(tx) = rest_tx_guard.take() {
                let _ = tx.send(());
            }
            process::exit(0);
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
        }
        _ => {
            println!("Command {:?} not yet implemented.", command);
        }
    }
    Ok(())
}

fn print_help() {
    println!("\nGraphDB CLI Commands:");
    println!("  daemon start [port]                  - Start a daemon on a specific port (default 8080)");
    println!("  daemon stop <port>                   - Stop the daemon on the specified port");
    println!("  daemon status <port>                 - Check if a daemon on the specified port is running (managed by this CLI)");
    println!("  daemon start-cluster <start>-<end>   - Start a cluster of daemons (max 10 ports)");
    println!("  list-daemons                         - List daemons currently managed by this CLI instance");
    println!("  clear-daemons                        - Stop all daemons managed by this CLI instance and attempt to kill external ones");
    println!("  rest start [port]                    - Start the REST API server on a specific port (default 8082)");
    println!("  rest stop                            - Stop the REST API server");
    println!("  rest status                          - Check the status of the REST API server");
    println!("  rest health                          - Perform a health check on the REST API server");
    println!("  rest version                         - Get the version of the REST API server");
    println!("  rest register-user <username> <password> - Register a new user via REST API");
    println!("  rest authenticate <username> <password>  - Authenticate a user and get a token via REST API");
    println!("  rest graph-query \"<query_string>\" [persist] - Execute a graph query via REST API");
    println!("  storage start [--port <port>] [--config-file <path>] - Start the standalone Storage daemon");
    println!("  storage stop [--port <port>]         - Stop the standalone Storage daemon");
    println!("  storage status [--port <port>]       - Check the status of the standalone Storage daemon");
    println!("  status                               - Get a comprehensive status summary of all GraphDB components");
    println!("  status rest                          - Get detailed status of the REST API component");
    println!("  status daemon [--port <port>]        - Get detailed status of a specific daemon or list common ones");
    println!("  status storage [--port <port>]       - Get detailed status of the Storage component");
    println!("  help                                 - Display this help message");
    println!("\nNote: Commands like 'view-graph', 'index-node', etc., are placeholders.");
}

/// Main asynchronous loop for the CLI interactive mode.
/// This function is called when interactive mode is detected.
async fn run_cli_interactive() -> Result<()> {
    let daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let rest_api_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::<Option<u16>>::new(None)); // Tracks the port if REST API is running
    let rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(None));

    println!("GraphDB CLI. Type 'help' for commands.");

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut input = String::new();

    loop {
        print!("graphdb-cli> ");
        let _ = std::io::stdout().flush(); // Ensure prompt is displayed

        input.clear();
        if let Err(e) = reader.read_line(&mut input).await {
            eprintln!("Failed to read line: {}", e);
            break;
        }

        let (command, args) = parse_command(&input);

        if command == CommandType::Exit {
            // Handle exit command directly to ensure cleanup
            handle_command(
                command,
                args,
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
            ).await?;
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
        ).await?;
    }

    // Graceful shutdown logic when exiting the loop
    println!("Shutting down GraphDB CLI components...");

    // Stop REST API server if it was started by this CLI
    let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
    let mut rest_handle_guard = rest_api_handle.lock().await;
    let mut rest_api_port_guard = rest_api_port_arc.lock().await;

    if let Some(port) = rest_api_port_guard.take() {
        println!("Attempting to stop REST API server on port {} during exit...", port);

        // Send a signal to the oneshot channel (if it's still waiting)
        if let Some(tx) = rest_tx_guard.take() {
            let _ = tx.send(()); // Signal the handle if it's waiting
        }

        // Attempt to kill the process directly using lsof/kill
        stop_process_by_port("REST API", port)?; // Use the helper function

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
        for (port, (handle, tx)) in handles.drain() {
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
    Ok(())
}

/// The main entry point for the CLI logic, called by server/src/main.rs
/// This function parses command-line arguments and dispatches to appropriate handlers.
#[tokio::main] // Add #[tokio::main] here
pub async fn start_cli() -> Result<()> {
    let args = CliArgs::parse(); // Parse all args, including hidden ones

    if args.internal_rest_api_run {
        // This is the daemonized REST API server process
        let daemon_listen_port = args.internal_port.unwrap_or_else(|| {
            get_default_rest_port_from_config()
        });
        let storage_config_path = args.internal_storage_config_path.unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("storage_daemon_server")
                .join("storage_config.yaml")
        });
        let storage_config = load_storage_config(Some(storage_config_path.clone()))
            .unwrap_or_else(|e| {
                eprintln!("[DAEMON PROCESS] Warning: Could not load storage config for REST API: {}. Using defaults.", e);
                StorageConfig {
                    data_directory: "/tmp/graphdb_data".to_string(),
                    log_directory: "/var/log/graphdb".to_string(),
                    default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                    cluster_range: "9000-9002".to_string(),
                    max_disk_space_gb: 1000,
                    min_disk_space_gb: 10,
                    use_raft_for_scale: true,
                    storage_engine_type: "sled".to_string(),
                }
            });

        println!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let (_tx, rx) = oneshot::channel(); // Dummy channel for shutdown
        let result = start_rest_server(daemon_listen_port, rx, storage_config.data_directory.clone()).await;
        if let Err(e) = result {
            eprintln!("[DAEMON PROCESS] REST API server failed: {:?}", e);
            std::process::exit(1);
        }
        println!("[DAEMON PROCESS] REST API server (daemonized) stopped.");
        std::process::exit(0);
    } else if args.internal_storage_daemon_run {
        // This is the daemonized Storage daemon process
        let daemon_listen_port = args.internal_port.unwrap_or_else(|| {
            // Fallback to storage's default if not provided
            load_storage_config(None).map(|c| c.default_port).unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
        });
        let storage_config_path = args.internal_storage_config_path.unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("storage_daemon_server")
                .join("storage_config.yaml")
        });

        println!("[DAEMON PROCESS] Starting Storage daemon (daemonized) on port {}...", daemon_listen_port);
        let result = start_storage_server(Some(daemon_listen_port), storage_config_path).await;
        if let Err(e) = result {
            eprintln!("[DAEMON PROCESS] Storage daemon failed: {:?}", e);
            std::process::exit(1);
        }
        println!("[DAEMON PROCESS] Storage daemon (daemonized) stopped.");
        std::process::exit(0);
    } else {
        // This is the primary CLI process, not a daemonized internal run.
        let config = match load_cli_config() {
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
            return Ok(()); // Exit after processing a direct query
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
                    let daemon_result = start_daemon(port, cluster.clone(), skip_ports.clone()).await;
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
                            stop_process_by_port("REST API", rest_port)?;

                            // Wait for port to be released
                            let addr = format!("127.0.0.1:{}", rest_port);
                            let start_time = Instant::now();
                            let wait_timeout = Duration::from_secs(3);
                            let poll_interval = Duration::from_millis(100);
                            let mut port_freed = false;

                            while start_time.elapsed() < wait_timeout {
                                match tokio::net::TcpListener::bind(&addr).await {
                                    Ok(_) => {
                                        port_freed = true;
                                        break;
                                    }
                                    Err(_) => {
                                        tokio::time::sleep(poll_interval).await;
                                    }
                                }
                            }

                            if !port_freed {
                                eprintln!("Failed to free up port {} after killing processes. Try again.", rest_port);
                                rest_api_status_msg = format!("Failed to free up port {}.", rest_port);
                            } else {
                                println!("Starting REST API server on port {}...", rest_port);

                                // Load storage config for the REST API server to pass to daemonized process
                                let current_storage_config = load_storage_config(storage_config_file.clone())
                                    .unwrap_or_else(|e| {
                                        eprintln!("Warning: Could not load storage config for REST API daemonization: {}. Using defaults.", e);
                                        // Create a default StorageConfig for fallback
                                        StorageConfig {
                                            data_directory: "/tmp/graphdb_data".to_string(),
                                            log_directory: "/var/log/graphdb".to_string(),
                                            default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                                            cluster_range: "9000-9002".to_string(),
                                            max_disk_space_gb: 1000,
                                            min_disk_space_gb: 10,
                                            use_raft_for_scale: true,
                                            storage_engine_type: "sled".to_string(), // Default to sled
                                        }
                                    });

                                let storage_engine_type = StorageEngineType::from_str(&current_storage_config.storage_engine_type)
                                    .unwrap_or(StorageEngineType::Sled); // Default to Sled if parsing fails

                                // Daemonize REST API server by spawning a new process
                                let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
                                let child = Command::new(&current_exe)
                                    .arg("--internal-rest-api-run") // Signal the new process to run as REST daemon
                                    .arg("--internal-port")
                                    .arg(rest_port.to_string())
                                    .arg("--internal-storage-config-path")
                                    .arg(current_storage_config.data_directory.clone()) // Pass data_directory as config_file for mock
                                    .arg("--internal-storage-engine")
                                    .arg(storage_engine_type.to_string()) // Pass the determined storage engine type
                                    .spawn()
                                    .context("Failed to spawn REST API daemon process")?;

                                println!("REST API server daemonized with PID {}", child.id().unwrap_or(0));
                                rest_api_status_msg = format!("Running on port: {}", rest_port);
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
                            stop_process_by_port("Storage Daemon", s_port)?;

                            // Wait for storage port to be released
                            let addr = format!("127.0.0.1:{}", s_port);
                            let start_time = Instant::now();
                            let wait_timeout = Duration::from_secs(3);
                            let poll_interval = Duration::from_millis(100);
                            let mut port_freed = false;

                            while start_time.elapsed() < wait_timeout {
                                match tokio::net::TcpListener::bind(&addr).await {
                                    Ok(_) => {
                                        port_freed = true;
                                        break;
                                    }
                                    Err(_) => {
                                        tokio::time::sleep(poll_interval).await;
                                    }
                                }
                            }

                            if !port_freed {
                                eprintln!("Failed to free up storage port {} after killing processes. Try again.", s_port);
                                storage_status_msg = format!("Failed to free up port {}.", s_port);
                            } else {
                                println!("Starting Storage daemon on port {}...", s_port);
                                // The CLI's load_storage_config is for displaying metrics, not for the daemon's actual startup.
                                // We attempt to load it here to provide immediate feedback on the config file's format.
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
                                        println!("    Storage Engine Type: {}", cfg.storage_engine_type); // Display the engine type
                                    }
                                    Err(e) => {
                                        eprintln!("Error loading storage config for CLI display: {:?}", e);
                                        // Do not exit, allow daemonization to proceed, as the daemon will load its own config.
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

                                // Daemonize Storage daemon by spawning a new process
                                let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
                                let child = Command::new(&current_exe)
                                    .arg("--internal-storage-daemon-run") // Signal the new process to run as Storage daemon
                                    .arg("--internal-port")
                                    .arg(s_port.to_string())
                                    .arg("--internal-storage-config-path")
                                    .arg(actual_storage_config_path.clone())
                                    .spawn()
                                    .context("Failed to spawn Storage daemon process")?;

                                println!("Storage daemon daemonized with PID {}", child.id().unwrap_or(0));
                                // --- ADD HEALTH CHECK HERE IN PARENT PROCESS ---
                                let addr_check = format!("127.0.0.1:{}", s_port);
                                let health_check_timeout = Duration::from_secs(5); // Give it a few seconds
                                let poll_interval = Duration::from_millis(200);
                                let mut started_ok = false;
                                let start_time = Instant::now();

                                while start_time.elapsed() < health_check_timeout {
                                    match tokio::net::TcpStream::connect(&addr_check).await {
                                        Ok(_) => {
                                            println!("Storage daemon on port {} responded to health check.", s_port);
                                            started_ok = true;
                                            break;
                                        }
                                        Err(_) => {
                                            tokio::time::sleep(poll_interval).await;
                                        }
                                    }
                                }

                                if started_ok {
                                    storage_status_msg = format!("Running on port: {}", s_port);
                                } else {
                                    eprintln!("Warning: Storage daemon daemonized with PID {} but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
                                        child.id().unwrap_or(0), s_port, health_check_timeout);
                                    storage_status_msg = format!("Daemonized but failed to become reachable on port {}", s_port);
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
                                println!("    Storage Engine Type: {}", cfg.storage_engine_type); // Display the engine type
                            }
                            Err(e) => {
                                eprintln!("Error loading default storage config for CLI display: {:?}", e);
                                storage_status_msg = format!("Failed to load default config for display ({:?})", e);
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
                // Handle the `status` command with its optional subcommand
                GraphDbCommands::Status(status_args) => {
                    match status_args.action {
                        Some(StatusAction::Rest) => {
                            display_rest_api_status().await;
                        }
                        Some(StatusAction::Daemon { port }) => {
                            display_daemon_status(port).await;
                        }
                        Some(StatusAction::Storage { port }) => {
                            display_storage_daemon_status(port).await;
                        }
                        None => { // This is for `graphdb-cli status` (no subcommand)
                            display_full_status_summary().await;
                        }
                    }
                }
                // Re-added StorageCommand handling
                GraphDbCommands::StorageCommand(action) => {
                    // These interactive commands are deprecated, but the logic remains for direct calls.
                    match action {
                        StorageAction::Start { port, config_file } => {
                            eprintln!("Interactive 'storage start' command is deprecated. Please use 'graphdb-cli start --storage-port <port>' for detached daemonization.");
                            // For a daemonized start via `graphdb-cli start --storage-port`, the logic is handled above in `GraphDbCommands::Start`
                            // If we absolutely needed to start it directly from this interactive path, it would look similar to the `GraphDbCommands::Start` block.
                            // For now, we'll just print the deprecation message.
                        }
                        StorageAction::Stop { port } => {
                            let port_to_stop = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
                            if let Err(e) = stop_process_by_port("storage-daemon", port_to_stop) {
                                eprintln!("Error stopping Standalone Storage daemon on port {}: {}", port_to_stop, e);
                            } else {
                                println!("Standalone Storage daemon stop command processed for port {}.", port_to_stop);
                            }
                        }
                        StorageAction::Status { port } => {
                            display_storage_daemon_status(port).await;
                        }
                    }
                }
            }
            return Ok(()); // Exit after processing a direct command
        }

        if args.cli {
            if args.enable_plugins {
                println!("Experimental plugins are enabled.");
            }
            // Call the interactive CLI main loop
            run_cli_interactive().await?;
            return Ok(());
        }

        if args.enable_plugins {
            println!("Experimental plugins is enabled.");
            return Ok(());
        }

        // Default to interactive CLI if no other commands/args are given
        run_cli_interactive().await?;
        Ok(())
    }
}


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
use regex::Regex;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use std::net::ToSocketAddrs; // Required for TcpStream::connect_timeout parsing


// SHARED_MEMORY_KEYS definition (kept only one instance, for KVPair logic)
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
    },
    Stop,
}

#[derive(Serialize, Deserialize, Debug)]
struct PidStore {
    pid: u32,
}

pub fn start_cli() {
    let args = CliArgs::parse();

    // 1. Handle direct query execution (highest priority, exits after)
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
        return; // Exit after executing direct query
    }

    // 2. Handle subcommands (next priority)
    if let Some(command) = args.command {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
            // Add actual plugin activation logic here if needed
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
            GraphDbCommands::Start { port, cluster } => {
                start_daemon(port, cluster);
            }
            GraphDbCommands::Stop => {
                stop_daemon();
            }
        }
        return; // Exit after executing a subcommand
    }

    // 3. Handle explicit interactive CLI request
    if args.cli {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        interactive_cli();
        return; // Exit after interactive CLI
    }

    // 4. Handle --enable-plugins alone (if no other command or --cli)
    if args.enable_plugins {
        println!("Experimental plugins are enabled.");
        return; // Exit after reporting plugin status
    }

    // 5. Default: No arguments or flags provided, enter interactive CLI
    interactive_cli();
}

// Helper function to clean up existing daemon instances (BROADER SEARCH)
fn cleanup_existing_daemons(base_process_name: &str) {
    println!("Proactively checking for and terminating existing 'graphdb' related daemon instances...");

    // Broaden pgrep search to anything containing "graphdb" in the command line
    let pgrep_result = Command::new("pgrep")
        .arg("-f")
        .arg("graphdb") // Catch "graphdb-cli", "graphdb-c", etc.
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    match pgrep_result {
        Ok(output) => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if pids.is_empty() {
                if !output.stderr.is_empty() {
                    eprintln!("Error running `pgrep`: {}", String::from_utf8_lossy(&output.stderr));
                } else {
                    println!("No 'graphdb' related daemon instances found by pgrep.");
                }
                return;
            }

            println!("Found {} 'graphdb' related instances to check: {:?}", pids.len(), pids);

            let mut terminated_count = 0;
            for pid in pids {
                // Verify the command name using ps -o comm= before killing
                let comm_output = Command::new("ps")
                    .arg("-o")
                    .arg("comm=") // Get just the command name
                    .arg(format!("{}", pid))
                    .stdout(Stdio::piped())
                    .spawn()
                    .and_then(|child| child.wait_with_output());

                if let Ok(comm_out) = comm_output {
                    let command_name = String::from_utf8_lossy(&comm_out.stdout).trim().to_lowercase();
                    // Check for our specific daemon names
                    if command_name.contains("graphdb-c") || command_name.contains("graphdb-cli") || command_name.contains(&base_process_name.to_lowercase()) {
                        println!("Attempting to send SIGTERM to PID {} (Command: {})...", pid, command_name);
                        if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                            eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                        } else {
                            println!("Sent SIGTERM to PID {}.", pid);
                            terminated_count += 1;
                        }
                    } else {
                        println!("Skipping PID {} (Command: {}): Not a recognized 'graphdb' daemon process.", pid, command_name);
                    }
                } else {
                    eprintln!("Could not verify command for PID {}. Attempting to terminate as a precaution.", pid);
                    if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                        eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                    } else {
                        println!("Sent SIGTERM to PID {}.", pid);
                        terminated_count += 1;
                    }
                }
            }
            if terminated_count > 0 {
                // Give processes time to terminate
                std::thread::sleep(std::time::Duration::from_millis(1000));
                println!("Proactive pgrep-based cleanup complete. Terminated {} processes.", terminated_count);
            } else {
                println!("No 'graphdb' daemon processes required termination by pgrep.");
            }
        }
        Err(e) => {
            eprintln!("Failed to execute `pgrep` command: {}. Please ensure `pgrep` is installed and in your PATH.", e);
        }
    }

    // Remove any base PID file (e.g., /tmp/graphdb-cli.pid)
    let base_pid_file_path = format!("/tmp/{}.pid", base_process_name);
    if Path::new(&base_pid_file_path).exists() {
         if let Err(e) = fs::remove_file(&base_pid_file_path) {
            eprintln!("Warning: Failed to remove stale base PID file {}: {}", base_pid_file_path, e);
        } else {
            println!("Removed stale base PID file: {}", base_pid_file_path);
        }
    }
}


fn start_daemon(port: Option<u16>, cluster_range: Option<String>) {
    let config_path = "server/src/cli/config.toml";

    let mut host_to_use = "127.0.0.1".to_string();
    let mut default_port = 8080;
    let mut base_process_name = "graphdb-cli".to_string(); // Base name from config/default

    // Load configuration for daemon (host, port, process_name)
    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()
            .unwrap();

        if let Ok(host) = config.get_string("server.host") {
            host_to_use = host;
        }
        if let Ok(cfg_port) = config.get_int("server.port") {
            default_port = cfg_port as u16;
        }
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            base_process_name = process_name;
        }
    }

    let mut ports_to_start: Vec<u16> = Vec::new();

    if let Some(range_str) = cluster_range {
        // Parse range: e.g., "9001-9005"
        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            eprintln!("Error: Invalid cluster port range format. Expected 'START-END', got '{}'.", range_str);
            std::process::exit(1);
        }
        let start_port = parts[0].parse::<u16>().unwrap_or(0);
        let end_port = parts[1].parse::<u16>().unwrap_or(0);

        if start_port == 0 || end_port == 0 || start_port > end_port {
            eprintln!("Error: Invalid cluster port range '{}'. Ports must be positive integers and start must be less than or equal to end.", range_str);
            std::process::exit(1);
        }

        let num_ports = end_port - start_port + 1;
        if num_ports == 0 { // Handle case like 9001-9000
             eprintln!("Error: Cluster port range is empty.");
             std::process::exit(1);
        }
        if num_ports > 10 {
            eprintln!("Error: Cluster port range size ({}) exceeds maximum allowed (10).", num_ports);
            std::process::exit(1);
        }

        for p in start_port..=end_port {
            ports_to_start.push(p);
        }
        println!("Attempting to start cluster on ports: {:?}", ports_to_start);
    } else {
        // Single port mode
        ports_to_start.push(port.unwrap_or(default_port));
        println!("Attempting to start single daemon on port: {}", ports_to_start[0]);
    }

    // --- Phase 1: Aggressive Pre-launch Port Cleanup ---
    // This cleanup is crucial to prevent AddressInUse errors and should happen BEFORE daemon starts.
    println!("\n--- Phase 1: Aggressively cleaning up specified ports ---");
    let lsof_regex = Regex::new(r"^\s*(\S+)\s+(\d+)\s+").expect("Invalid lsof regex");
    let mut ports_to_skip_due_to_external_app: HashSet<u16> = HashSet::new();

    for &current_port in &ports_to_start {
        println!("Checking port {}:{} for existing processes...", host_to_use, current_port);

        let lsof_output = Command::new("lsof")
            .arg("-iTCP")
            .arg("-sTCP:LISTEN")
            .arg("-P")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()) // Capture stderr for lsof errors
            .spawn()
            .and_then(|child| child.wait_with_output());

        match lsof_output {
            Ok(output) => {
                if !output.stderr.is_empty() {
                    eprintln!("Warning: lsof reported errors for port {}: {}", current_port, String::from_utf8_lossy(&output.stderr));
                }
                let output_str = String::from_utf8_lossy(&output.stdout);
                let mut found_process_info = None;

                for line in output_str.lines() {
                    if line.contains("LISTEN") && line.contains(&format!(":{}", current_port)) {
                        if let Some(captures) = lsof_regex.captures(line) {
                            if let (Some(cmd_match), Some(pid_match)) = (captures.get(1), captures.get(2)) {
                                if let Ok(pid) = pid_match.as_str().parse::<u32>() {
                                    found_process_info = Some((cmd_match.as_str().to_string(), pid));
                                    break;
                                }
                            }
                        }
                    }
                }

                if let Some((cmd_on_port, pid_on_port)) = found_process_info {
                    // Check if the process occupying the port is related to 'graphdb'
                    // Using a broader check to catch 'graphdb-c', 'graphdb-cli', etc.
                    let is_our_app = cmd_on_port.to_lowercase().contains("graphdb");

                    if is_our_app {
                        println!("  Port {}:{} is held by a 'graphdb' process: {} (PID {}). Terminating...",
                                 current_port, host_to_use, cmd_on_port, pid_on_port);
                        if let Err(e) = kill(Pid::from_raw(pid_on_port as i32), Signal::SIGTERM) {
                            eprintln!("  Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid_on_port, e);
                        } else {
                            println!("  Sent SIGTERM to PID {}. Waiting for port to free...", pid_on_port);
                            // Wait for the port to truly be released
                            let max_wait_attempts = 20; // 20 * 100ms = 2 seconds
                            let mut attempts = 0;
                            let socket_addr_to_check = format!("{}:{}", host_to_use, current_port)
                                .to_socket_addrs().unwrap().next().unwrap();
                            while attempts < max_wait_attempts {
                                std::thread::sleep(std::time::Duration::from_millis(100));
                                if std::net::TcpStream::connect_timeout(&socket_addr_to_check, std::time::Duration::from_millis(50)).is_err() {
                                    // Connection refused or other error means port is likely free
                                    // However, a connection *succeeding* means it's still in use.
                                    // A robust check for "free" is that it explicitly fails with ConnectionRefused, not success.
                                    // The `is_err()` check is generally enough here, as it implies it's not LISTENing.
                                    break; // Port is no longer connectable, assumed free.
                                }
                                attempts += 1;
                            }
                            if attempts == max_wait_attempts {
                                eprintln!("  Warning: Port {}:{} still appears in use after termination attempt. Manual intervention might be required.", current_port, host_to_use);
                                // This port might still fail startup, but we did our best to clear it.
                            } else {
                                println!("  Port {}:{} is now free.", current_port, host_to_use);
                            }
                        }
                    } else {
                        eprintln!("  Port {}:{} is occupied by another non-'graphdb' application: {} (PID {}). Skipping startup on this port.",
                                  current_port, host_to_use, cmd_on_port, pid_on_port);
                        ports_to_skip_due_to_external_app.insert(current_port);
                    }
                } else {
                    println!("  Port {}:{} is free.", current_port, host_to_use);
                }
            }
            Err(e) => {
                eprintln!("Failed to execute `lsof` command for port {}: {}. Please ensure `lsof` is installed and in your PATH. Cannot guarantee port cleanliness.", current_port, e);
                // If lsof fails, we can't properly clean. Mark for potential failure.
            }
        }
    }
    println!("--- Phase 1: Port cleanup complete ---");

    // --- Phase 2: Attempt to start daemons ---
    let max_port_check_attempts = 10; // How many times to check the port after daemon forks
    let port_check_interval_ms = 200; // Interval in milliseconds between port checks

    let mut successful_starts = 0;
    let mut final_failed_ports = Vec::new(); // Will contain ports that genuinely failed after all attempts

    for current_port in ports_to_start {
        if ports_to_skip_due_to_external_app.contains(&current_port) {
            final_failed_ports.push(current_port);
            continue; // Skip starting on this port
        }

        println!("\n--- Attempting to start daemon on port {} ---", current_port);

        // Unique process name and log files for each daemon in a cluster
        let specific_process_name = format!("{}-{}", base_process_name, current_port);
        let specific_stdout_file_path = format!("/tmp/daemon-{}.out", current_port);
        let specific_stderr_file_path = format!("/tmp/daemon-{}.err", current_port);

        // Always recreate stdout/stderr files for each daemon attempt
        let stdout = File::create(&specific_stdout_file_path).expect(&format!("Failed to create stdout file for port {}", current_port));
        let stderr = File::create(&specific_stderr_file_path).expect(&format!("Failed to create stderr file for port {}", current_port));

        let mut daemonize = DaemonizeBuilder::new()
            .working_directory("/tmp")
            .umask(0o027)
            .stdout(stdout)
            .stderr(stderr)
            .process_name(&specific_process_name) // Use port-specific name for daemon
            .host(&host_to_use)
            .port(current_port)
            .build()
            .expect("Failed to build Daemonize object");

        match daemonize.start() {
            Ok(child_pid_from_fork) => {
                println!("Original process exited, daemonizing (initial PID: {}).", child_pid_from_fork);

                let socket_addr = format!("{}:{}", host_to_use, current_port)
                    .to_socket_addrs()
                    .expect("Invalid host:port address")
                    .next()
                    .expect("No socket address found");

                let mut port_confirmed_listening = false;
                for i in 0..max_port_check_attempts {
                    std::thread::sleep(std::time::Duration::from_millis(port_check_interval_ms));
                    print!("\r  Checking port {}:{} (Attempt {}/{}) for daemon '{}'...", host_to_use, current_port, i + 1, max_port_check_attempts, specific_process_name);
                    io::stdout().flush().unwrap();

                    if std::net::TcpStream::connect_timeout(
                        &socket_addr,
                        std::time::Duration::from_millis(100)
                    ).is_ok() {
                        println!("\rDaemon '{}' started and is listening on {}:{}!                ", specific_process_name, host_to_use, current_port);
                        port_confirmed_listening = true;
                        successful_starts += 1;
                        break; // Port confirmed, move to next daemon in cluster
                    }
                }
                println!("\r                                                                "); // Clear last line

                if !port_confirmed_listening {
                    eprintln!("Daemon '{}' did not appear to start successfully on port {}:{} within the timeout (PID {}).",
                              specific_process_name, host_to_use, current_port, child_pid_from_fork);
                    final_failed_ports.push(current_port);
                }
            }
            Err(DaemonizeError::AddrInUse(h, p)) => {
                // This case should be rare now due to proactive cleanup.
                eprintln!("Daemonization failed immediately for port {}: {}:{} already in use (after proactive cleanup!).", current_port, h, p);
                final_failed_ports.push(current_port);
            }
            Err(e) => { // Other errors from daemonize.start()
                eprintln!("Initial daemonization failed for port {}: {}", current_port, e);
                final_failed_ports.push(current_port);
            }
        }
    } // End of loop for ports

    // Final result reporting and exit
    if !final_failed_ports.is_empty() {
        eprintln!("\nCluster startup completed with failures. Failed to start on ports: {:?}", final_failed_ports);
        eprintln!("Please check /tmp/daemon-{{port}}.err logs for details.");
        std::process::exit(1); // Exit with error if any daemon failed to start
    } else if successful_starts > 0 {
        println!("\nSuccessfully started {} daemon instances.", successful_starts);
        std::process::exit(0); // Exit successfully after all daemons started
    } else {
        eprintln!("\nNo daemon instances were started.");
        std::process::exit(1); // Exit with error if nothing started
    }
}

// Helper function to read PID from the daemon's PID file (not directly used for multi-instance management but kept for reference)
fn get_daemon_pid_from_file(process_name: &str) -> Option<u32> {
    let pid_file_path = format!("/tmp/{}.pid", process_name);
    let path = Path::new(&pid_file_path);
    if path.exists() {
        match std::fs::read_to_string(path) {
            Ok(content) => content.trim().parse::<u32>().ok(),
            Err(e) => {
                eprintln!("Warning: Could not read PID file {}: {}", pid_file_path, e);
                None
            }
        }
    } else {
        None
    }
}

// UPDATED: stop_daemon function for robustness with clusters
fn stop_daemon() {
    let config_path = "server/src/cli/config.toml";
    let mut base_process_name = "graphdb-cli".to_string(); // Default process name

    // Re-read config to ensure consistency with how the daemon was started
    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()
            .unwrap();
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            base_process_name = process_name;
        }
    }

    println!("Attempting to stop all 'graphdb' related daemon instances...");

    // Use `pgrep -f` to find processes whose command line *contains* "graphdb"
    let pgrep_result = Command::new("pgrep")
        .arg("-f") // Search full command line
        .arg("graphdb") // Broaden search to anything containing "graphdb"
        .stdout(Stdio::piped())
        .stderr(Stdio::piped()) // Capture stderr for better error reporting
        .spawn()
        .and_then(|child| child.wait_with_output());

    let mut stopped_any = false;
    match pgrep_result {
        Ok(output) => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if pids.is_empty() {
                if !output.stderr.is_empty() {
                    eprintln!("Error running `pgrep`: {}", String::from_utf8_lossy(&output.stderr));
                    eprintln!("Manual intervention may be required.");
                } else {
                    println!("No 'graphdb' related daemon instances found running.");
                }
            } else {
                println!("Found {} 'graphdb' related instances to stop: {:?}", pids.len(), pids);
                for pid in pids {
                    // Double check with `ps` that this PID is likely ours before killing
                    let comm_output = Command::new("ps")
                        .arg("-o")
                        .arg("comm=") // Get just the command name
                        .arg(format!("{}", pid))
                        .stdout(Stdio::piped())
                        .spawn()
                        .and_then(|child| child.wait_with_output());

                    if let Ok(comm_out) = comm_output {
                        let command_name = String::from_utf8_lossy(&comm_out.stdout).trim().to_lowercase();
                        // Check for our specific daemon names, including the truncated 'graphdb-c'
                        if command_name.contains("graphdb-c") || command_name.contains("graphdb-cli") || command_name.contains(&base_process_name.to_lowercase()) {
                            println!("Sending SIGTERM to PID {} (Command: {})...", pid, command_name);
                            if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                                eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                            } else {
                                println!("Sent SIGTERM to PID {}.", pid);
                                stopped_any = true;
                            }
                        } else {
                            println!("Skipping PID {} (Command: {}): Not a recognized 'graphdb' daemon process.", pid, command_name);
                        }
                    } else {
                        eprintln!("Could not verify command for PID {}. Attempting to terminate as a precaution.", pid);
                        if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                            eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                        } else {
                            println!("Sent SIGTERM to PID {}.", pid);
                            stopped_any = true;
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(1000)); // Give time to terminate
            }
        }
        Err(e) => {
            eprintln!("Failed to execute `pgrep` command: {}. Manual intervention may be required.", e);
            eprintln!("Please ensure `pgrep` is installed and in your PATH.");
        }
    }

    // Clean up any potential PID files (base and port-specific ones)
    let base_pid_file_path = format!("/tmp/{}.pid", base_process_name);
    if Path::new(&base_pid_file_path).exists() {
        if let Err(e) = fs::remove_file(&base_pid_file_path) {
            eprintln!("Warning: Failed to remove base PID file {}: {}", base_pid_file_path, e);
        } else {
            println!("Removed base PID file: {}", base_pid_file_path);
        }
    }
    // Also remove any specific PID files that might be lingering for common daemon ports
    // This assumes daemonize creates /tmp/daemon-PORT.pid for each instance.
    for i in 8000..=9010 { // Cover a common range of daemon ports
        let specific_pid_file_path = format!("/tmp/daemon-{}.pid", i);
        if Path::new(&specific_pid_file_path).exists() {
            if let Err(e) = fs::remove_file(&specific_pid_file_path) {
                eprintln!("Warning: Failed to remove port-specific PID file {}: {}", specific_pid_file_path, e);
            } else {
                println!("Removed port-specific PID file: {}", specific_pid_file_path);
            }
        }
    }


    if stopped_any {
        println!("Successfully stopped daemon instances.");
    } else {
        println!("No daemon instances were found or stopped.");
    }
}

// These functions are related to the old shared memory approach for KVPairs,
// which seems unrelated to the daemon control. Keeping them as is.
// Note: The `SHARED_MEMORY_KEYS` is for this KVPair logic only, not daemon PIDs.

fn store_kv_pair(shmem: &Shmem, offset: usize, key: String, value: Vec<u8>) {
    let kv_store_ptr = unsafe { shmem.as_ptr().add(offset) as *mut KVPair };
    const NUM_ENTRIES: usize = 10;

    let key_as_i32 = match key.parse::<i32>() {
        Ok(parsed_key) => parsed_key,
        Err(_) => {
            eprintln!("Failed to parse key: {}", key);
            return;
        }
    };

    let mut keys = SHARED_MEMORY_KEYS.lock().unwrap();
    if keys.contains(&key_as_i32) {
        eprintln!("Shared memory key {} is already in use!", key);
        return;
    }

    for i in 0..NUM_ENTRIES {
        let entry_ptr = unsafe { kv_store_ptr.add(i) };
        let entry = unsafe { entry_ptr.as_mut().unwrap() };

        if entry.key.is_empty() {
            entry.key = key.clone();
            entry.value = value;
            keys.insert(key_as_i32);
            return;
        }
    }
    eprintln!("Key-value store is full!");
}

fn retrieve_kv_pair(shmem: &Shmem, offset: usize, key: &String) -> Option<Vec<u8>> {
    let kv_store_ptr = unsafe { shmem.as_ptr().add(offset) as *const KVPair };
    const NUM_ENTRIES: usize = 10;

    let keys = SHARED_MEMORY_KEYS.lock().unwrap();

    let key_as_i32 = key.parse::<i32>();

    if let Ok(key_i32) = key_as_i32 {
        if !keys.contains(&key_i32) {
            eprintln!("Key {} not found in shared memory!", key);
            return None;
        }
    } else {
        eprintln!("Failed to parse key: {}", key);
        return None;
    }

    for i in 0..NUM_ENTRIES {
        let entry_ptr = unsafe { kv_store_ptr.add(i) };
        let entry = unsafe { entry_ptr.as_ref().unwrap() };

        if entry.key == *key {
            return Some(entry.value.clone());
        }
    }
    None
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
                    // For interactive CLI, default to single daemon start
                    start_daemon(None, None);
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
use regex::Regex;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use std::net::ToSocketAddrs; // Required for TcpStream::connect_timeout parsing


// SHARED_MEMORY_KEYS definition (kept only one instance, for KVPair logic)
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
    },
    Stop,
}

#[derive(Serialize, Deserialize, Debug)]
struct PidStore {
    pid: u32,
}

pub fn start_cli() {
    let args = CliArgs::parse();

    // 1. Handle direct query execution (highest priority, exits after)
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
        return; // Exit after executing direct query
    }

    // 2. Handle subcommands (next priority)
    if let Some(command) = args.command {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
            // Add actual plugin activation logic here if needed
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
            GraphDbCommands::Start { port, cluster } => {
                start_daemon(port, cluster);
            }
            GraphDbCommands::Stop => {
                stop_daemon();
            }
        }
        return; // Exit after executing a subcommand
    }

    // 3. Handle explicit interactive CLI request
    if args.cli {
        if args.enable_plugins {
            println!("Experimental plugins are enabled.");
        }
        interactive_cli();
        return; // Exit after interactive CLI
    }

    // 4. Handle --enable-plugins alone (if no other command or --cli)
    if args.enable_plugins {
        println!("Experimental plugins are enabled.");
        return; // Exit after reporting plugin status
    }

    // 5. Default: No arguments or flags provided, enter interactive CLI
    interactive_cli();
}

// Helper function to clean up existing daemon instances
fn cleanup_existing_daemons(base_process_name: &str) {
    println!("Proactively checking for and terminating existing '{}' daemon instances...", base_process_name);

    let pgrep_result = Command::new("pgrep")
        .arg("-f") // Search full command line
        .arg(format!("^{}", regex::escape(base_process_name))) // Regex: starts with `base_process_name`
        .stdout(Stdio::piped())
        .stderr(Stdio::piped()) // Capture stderr for better error reporting
        .spawn()
        .and_then(|child| child.wait_with_output());

    match pgrep_result {
        Ok(output) => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if pids.is_empty() {
                // If stdout is empty, pgrep found no matches. Check stderr for actual errors.
                if !output.stderr.is_empty() {
                    eprintln!("Error running `pgrep`: {}", String::from_utf8_lossy(&output.stderr));
                    eprintln!("Please ensure `pgrep` is installed and in your PATH.");
                } else {
                    println!("No existing '{}' daemon instances found by pgrep.", base_process_name);
                }
                return;
            }

            println!("Found {} existing '{}' daemon instances: {:?}", pids.len(), base_process_name, pids);

            for pid in pids {
                println!("Attempting to send SIGTERM to existing daemon PID {}...", pid);
                if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                    eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                } else {
                    println!("Sent SIGTERM to PID {}.", pid);
                }
            }
            // Give processes time to terminate
            std::thread::sleep(std::time::Duration::from_millis(1000));
            println!("Proactive cleanup complete.");

            // Remove any PID files associated with the base process name (e.g., /tmp/graphdb-cli.pid)
            let base_pid_file_path = format!("/tmp/{}.pid", base_process_name);
            if Path::new(&base_pid_file_path).exists() {
                 if let Err(e) = fs::remove_file(&base_pid_file_path) {
                    eprintln!("Warning: Failed to remove stale base PID file {}: {}", base_pid_file_path, e);
                } else {
                    println!("Removed stale base PID file: {}", base_pid_file_path);
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to execute `pgrep` command: {}. Please ensure `pgrep` is installed and in your PATH.", e);
        }
    }
}


fn start_daemon(port: Option<u16>, cluster_range: Option<String>) {
    let config_path = "server/src/cli/config.toml";

    let mut host_to_use = "127.0.0.1".to_string();
    let mut default_port = 8080;
    let mut base_process_name = "graphdb-cli".to_string(); // Base name from config/default

    // Load configuration for daemon (host, port, process_name)
    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()
            .unwrap();

        if let Ok(host) = config.get_string("server.host") {
            host_to_use = host;
        }
        if let Ok(cfg_port) = config.get_int("server.port") {
            default_port = cfg_port as u16;
        }
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            base_process_name = process_name;
        }
    }

    // --- Proactive cleanup before attempting to start (now uses base_process_name) ---
    cleanup_existing_daemons(&base_process_name);
    // --- END Proactive cleanup ---

    let mut ports_to_start: Vec<u16> = Vec::new();

    if let Some(range_str) = cluster_range {
        // Parse range: e.g., "9001-9005"
        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            eprintln!("Error: Invalid cluster port range format. Expected 'START-END', got '{}'.", range_str);
            std::process::exit(1);
        }
        let start_port = parts[0].parse::<u16>().unwrap_or(0);
        let end_port = parts[1].parse::<u16>().unwrap_or(0);

        if start_port == 0 || end_port == 0 || start_port > end_port {
            eprintln!("Error: Invalid cluster port range '{}'. Ports must be positive integers and start must be less than or equal to end.", range_str);
            std::process::exit(1);
        }

        let num_ports = end_port - start_port + 1;
        if num_ports == 0 { // Handle case like 9001-9000
             eprintln!("Error: Cluster port range is empty.");
             std::process::exit(1);
        }
        if num_ports > 10 {
            eprintln!("Error: Cluster port range size ({}) exceeds maximum allowed (10).", num_ports);
            std::process::exit(1);
        }

        for p in start_port..=end_port {
            ports_to_start.push(p);
        }
        println!("Attempting to start cluster on ports: {:?}", ports_to_start);
    } else {
        // Single port mode
        ports_to_start.push(port.unwrap_or(default_port));
        println!("Attempting to start single daemon on port: {}", ports_to_start[0]);
    }

    let max_port_check_attempts = 10; // How many times to check the port after daemon forks
    let port_check_interval_ms = 200; // Interval in milliseconds between port checks

    // Regex to parse lsof output like: "COMMAND    PID USER" from the start of the line
    let lsof_regex = Regex::new(r"^\s*(\S+)\s+(\d+)\s+").expect("Invalid lsof regex");

    let mut successful_starts = 0;
    let mut failed_ports = Vec::new();

    for current_port in ports_to_start {
        println!("\n--- Attempting to start daemon on port {} ---", current_port);

        // Unique process name and log files for each daemon in a cluster
        let specific_process_name = format!("{}-{}", base_process_name, current_port);
        let specific_stdout_file_path = format!("/tmp/daemon-{}.out", current_port);
        let specific_stderr_file_path = format!("/tmp/daemon-{}.err", current_port);

        // Always recreate stdout/stderr files for each daemon attempt
        let stdout = File::create(&specific_stdout_file_path).expect(&format!("Failed to create stdout file for port {}", current_port));
        let stderr = File::create(&specific_stderr_file_path).expect(&format!("Failed to create stderr file for port {}", current_port));

        let mut daemonize = DaemonizeBuilder::new()
            .working_directory("/tmp")
            .umask(0o027)
            .stdout(stdout)
            .stderr(stderr)
            .process_name(&specific_process_name) // Use port-specific name for daemon
            .host(&host_to_use)
            .port(current_port)
            .build()
            .expect("Failed to build Daemonize object");

        match daemonize.start() {
            Ok(child_pid_from_fork) => {
                println!("Original process exited, daemonizing (initial PID: {}).", child_pid_from_fork);

                let socket_addr = format!("{}:{}", host_to_use, current_port)
                    .to_socket_addrs()
                    .expect("Invalid host:port address")
                    .next()
                    .expect("No socket address found");

                let mut port_confirmed_listening = false;
                for i in 0..max_port_check_attempts {
                    std::thread::sleep(std::time::Duration::from_millis(port_check_interval_ms));
                    print!("\r  Checking port {}:{} (Attempt {}/{}) for daemon '{}'...", host_to_use, current_port, i + 1, max_port_check_attempts, specific_process_name);
                    io::stdout().flush().unwrap();

                    match std::net::TcpStream::connect_timeout(
                        &socket_addr,
                        std::time::Duration::from_millis(100)
                    ) {
                        Ok(_) => {
                            println!("\rDaemon '{}' started and is listening on {}:{}!                ", specific_process_name, host_to_use, current_port);
                            port_confirmed_listening = true;
                            successful_starts += 1;
                            break; // Port confirmed, move to next daemon in cluster
                        }
                        Err(e) => {
                            if e.kind() != io::ErrorKind::ConnectionRefused {
                                eprintln!("\r  Port check error for {}:{}: {} (kind: {:?})                     ", host_to_use, current_port, e, e.kind());
                            }
                        }
                    }
                }
                println!("\r                                                                "); // Clear last line

                if !port_confirmed_listening {
                    eprintln!("Daemon '{}' did not appear to start successfully on port {}:{} within the timeout (PID {}).",
                              specific_process_name, host_to_use, current_port, child_pid_from_fork);
                    failed_ports.push(current_port);
                }
            }
            Err(DaemonizeError::AddrInUse(h, p)) => {
                eprintln!("Daemonization failed immediately for port {}: {}:{} already in use.", current_port, h, p);
                failed_ports.push(current_port);
            }
            Err(e) => { // Other errors from daemonize.start()
                eprintln!("Initial daemonization failed for port {}: {}", current_port, e);
                failed_ports.push(current_port);
            }
        }

        // This lsof check remains relevant if a specific port failed due to Address In Use
        // and we want to provide immediate feedback on what caused the failure for *that* port.
        if failed_ports.contains(&current_port) {
            println!("Checking for processes listening on {}:{}...", host_to_use, current_port);

            let lsof_output = Command::new("lsof")
                .arg("-iTCP") // Only TCP sockets
                .arg("-sTCP:LISTEN") // Only listening sockets
                .arg("-P") // Numeric port numbers, no service names
                .stdout(Stdio::piped())
                .spawn()
                .and_then(|child| child.wait_with_output())
                .expect("Failed to run lsof. Is it installed and in your PATH?");

            let output_str = String::from_utf8_lossy(&lsof_output.stdout);
            let mut found_process_info = None;

            for line in output_str.lines() {
                if line.contains("LISTEN") && line.contains(&format!(":{}", current_port)) {
                    if let Some(captures) = lsof_regex.captures(line) {
                        if let (Some(cmd_match), Some(pid_match)) = (captures.get(1), captures.get(2)) {
                            if let Ok(pid) = pid_match.as_str().parse::<u32>() {
                                found_process_info = Some((cmd_match.as_str().to_string(), pid));
                                break;
                            }
                        }
                    }
                }
            }

            match found_process_info {
                Some((cmd_on_port, pid_on_port)) => {
                    // Check if the process occupying the port contains our base process name (case-insensitive)
                    let is_our_app = cmd_on_port.to_lowercase().contains(&base_process_name.to_lowercase());
                    if is_our_app {
                        eprintln!("Port {}:{} is held by our own daemon instance (PID {}). It might be a residual process. Attempting to terminate it...", current_port, host_to_use, pid_on_port);
                        // --- KILLING OUR OWN STALE PROCESS ---
                        if let Err(e) = kill(Pid::from_raw(pid_on_port as i32), Signal::SIGTERM) {
                            eprintln!("Failed to send SIGTERM to our own stale PID {}: {}. It might already be gone or permissions issue. Manual intervention may be required.", pid_on_port, e);
                        } else {
                            println!("Sent SIGTERM to our own stale PID {}.", pid_on_port);
                            // Give a small moment for process to die and release the port
                            std::thread::sleep(std::time::Duration::from_millis(500));
                        }
                    } else { // Not our app, but still occupying the port
                        eprintln!("Port {}:{} is occupied by another application: {} (PID {}). Attempting to terminate it...",
                                  current_port, host_to_use, cmd_on_port, pid_on_port);
                        // --- KILLING OTHER APPLICATION PROCESS ---
                        if let Err(e) = kill(Pid::from_raw(pid_on_port as i32), Signal::SIGTERM) {
                            eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue. Manual intervention may be required.", pid_on_port, e);
                        } else {
                            println!("Sent SIGTERM to PID {}.", pid_on_port);
                            // Give a small moment for process to die and release the port
                            std::thread::sleep(std::time::Duration::from_millis(500));
                        }
                    }
                }
                None => {
                    eprintln!("Could not identify any process holding port {}:{}.", host_to_use, current_port);
                    eprintln!("This is unexpected, as the daemon itself reported 'Address in use' or port polling failed. The port might be fleetingly held or requires manual intervention.");
                }
            }
        }
    } // End of loop for ports

    // Final result reporting and exit
    if !failed_ports.is_empty() {
        eprintln!("\nCluster startup completed with failures. Failed to start on ports: {:?}", failed_ports);
        eprintln!("Please check /tmp/daemon-{{port}}.err logs for details.");
        std::process::exit(1); // Exit with error if any daemon failed to start
    } else if successful_starts > 0 {
        println!("\nSuccessfully started {} daemon instances.", successful_starts);
        std::process::exit(0); // Exit successfully after all daemons started
    } else {
        eprintln!("\nNo daemon instances were started.");
        std::process::exit(1); // Exit with error if nothing started
    }
}

// Helper function to read PID from the daemon's PID file (not directly used for multi-instance management but kept for reference)
fn get_daemon_pid_from_file(process_name: &str) -> Option<u32> {
    let pid_file_path = format!("/tmp/{}.pid", process_name);
    let path = Path::new(&pid_file_path);
    if path.exists() {
        match std::fs::read_to_string(path) {
            Ok(content) => content.trim().parse::<u32>().ok(),
            Err(e) => {
                eprintln!("Warning: Could not read PID file {}: {}", pid_file_path, e);
                None
            }
        }
    } else {
        None
    }
}

// UPDATED: stop_daemon function for robustness with clusters
fn stop_daemon() {
    let config_path = "server/src/cli/config.toml";
    let mut base_process_name = "graphdb-cli".to_string(); // Default process name

    // Re-read config to ensure consistency with how the daemon was started
    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()
            .unwrap();
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            base_process_name = process_name;
        }
    }

    println!("Attempting to stop all '{}' daemon instances...", base_process_name);

    // Use `pgrep -f` to find processes whose command line *starts with* the base name
    let pgrep_result = Command::new("pgrep")
        .arg("-f") // Search full command line
        .arg(format!("^{}", regex::escape(&base_process_name))) // Regex: starts with `base_process_name`
        .stdout(Stdio::piped())
        .stderr(Stdio::piped()) // Capture stderr for better error reporting
        .spawn()
        .and_then(|child| child.wait_with_output());

    let mut stopped_any = false;
    match pgrep_result {
        Ok(output) => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if pids.is_empty() {
                // If stdout is empty, pgrep found no matches. Check stderr for actual errors.
                if !output.stderr.is_empty() {
                    eprintln!("Error running `pgrep`: {}", String::from_utf8_lossy(&output.stderr));
                    eprintln!("Manual intervention may be required.");
                } else {
                    println!("No '{}' daemon instances found running.", base_process_name);
                }
            } else {
                println!("Found {} instances to stop: {:?}", pids.len(), pids);
                for pid in pids {
                    println!("Sending SIGTERM to PID {}...", pid);
                    if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                        eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                    } else {
                        println!("Sent SIGTERM to PID {}.", pid);
                        stopped_any = true;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(1000)); // Give time to terminate
            }
        }
        Err(e) => {
            eprintln!("Failed to execute `pgrep` command: {}. Manual intervention may be required.", e);
            eprintln!("Please ensure `pgrep` is installed and in your PATH.");
        }
    }

    // Clean up any potential PID files (base and port-specific ones)
    let base_pid_file_path = format!("/tmp/{}.pid", base_process_name);
    if Path::new(&base_pid_file_path).exists() {
        if let Err(e) = fs::remove_file(&base_pid_file_path) {
            eprintln!("Warning: Failed to remove base PID file {}: {}", base_pid_file_path, e);
        } else {
            println!("Removed base PID file: {}", base_pid_file_path);
        }
    }
    // Note: The `pgrep -f` approach for stopping makes explicit PID file deletion for each port less critical,
    // as it targets the running processes directly.

    if stopped_any {
        println!("Successfully stopped daemon instances.");
    } else {
        println!("No daemon instances were found or stopped.");
    }
}

// These functions are related to the old shared memory approach for KVPairs,
// which seems unrelated to the daemon control. Keeping them as is.
// Note: The `SHARED_MEMORY_KEYS` is for this KVPair logic only, not daemon PIDs.

fn store_kv_pair(shmem: &Shmem, offset: usize, key: String, value: Vec<u8>) {
    let kv_store_ptr = unsafe { shmem.as_ptr().add(offset) as *mut KVPair };
    const NUM_ENTRIES: usize = 10;

    let key_as_i32 = match key.parse::<i32>() {
        Ok(parsed_key) => parsed_key,
        Err(_) => {
            eprintln!("Failed to parse key: {}", key);
            return;
        }
    };

    let mut keys = SHARED_MEMORY_KEYS.lock().unwrap();
    if keys.contains(&key_as_i32) {
        eprintln!("Shared memory key {} is already in use!", key);
        return;
    }

    for i in 0..NUM_ENTRIES {
        let entry_ptr = unsafe { kv_store_ptr.add(i) };
        let entry = unsafe { entry_ptr.as_mut().unwrap() };

        if entry.key.is_empty() {
            entry.key = key.clone();
            entry.value = value;
            keys.insert(key_as_i32);
            return;
        }
    }
    eprintln!("Key-value store is full!");
}

fn retrieve_kv_pair(shmem: &Shmem, offset: usize, key: &String) -> Option<Vec<u8>> {
    let kv_store_ptr = unsafe { shmem.as_ptr().add(offset) as *const KVPair };
    const NUM_ENTRIES: usize = 10;

    let keys = SHARED_MEMORY_KEYS.lock().unwrap();

    let key_as_i32 = key.parse::<i32>();

    if let Ok(key_i32) = key_as_i32 {
        if !keys.contains(&key_i32) {
            eprintln!("Key {} not found in shared memory!", key);
            return None;
        }
    } else {
        eprintln!("Failed to parse key: {}", key);
        return None;
    }

    for i in 0..NUM_ENTRIES {
        let entry_ptr = unsafe { kv_store_ptr.add(i) };
        let entry = unsafe { entry_ptr.as_ref().unwrap() };

        if entry.key == *key {
            return Some(entry.value.clone());
        }
    }
    None
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
                    // For interactive CLI, default to single daemon start
                    start_daemon(None, None);
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
}

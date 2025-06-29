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
        #[arg(short = 'p', long = "port", value_name = "PORT", help = "Port for the daemon to listen on.")]
        port: Option<u16>,
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
            GraphDbCommands::Start { port } => {
                start_daemon(port);
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

// NEW: Helper function to clean up existing daemon instances
fn cleanup_existing_daemons(process_name: &str) {
    println!("Proactively checking for and terminating existing '{}' daemon instances...", process_name);

    // Try to find PIDs using `pgrep` (exact match for process name)
    let pgrep_output = Command::new("pgrep")
        .arg("-x") // Exact match for process name
        .arg(process_name)
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    match pgrep_output {
        Ok(output) if output.status.success() => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if pids.is_empty() {
                println!("No existing '{}' daemon instances found by pgrep.", process_name);
                return;
            }

            println!("Found {} existing '{}' daemon instances: {:?}", pids.len(), process_name, pids);

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

            // Also, remove the PID file if it exists, as it could be stale.
            // The PID file stores the PID of the last started daemon.
            let pid_file_path = format!("/tmp/{}.pid", process_name);
            if Path::new(&pid_file_path).exists() {
                 if let Err(e) = fs::remove_file(&pid_file_path) {
                    eprintln!("Warning: Failed to remove stale PID file {}: {}", pid_file_path, e);
                } else {
                    println!("Removed stale PID file: {}", pid_file_path);
                }
            }
        }
        _ => {
            eprintln!("Could not use `pgrep` to find existing daemon instances. Please ensure `pgrep` is installed and in your PATH.");
            eprintln!("If daemon startup fails, `lsof` will still be used for port-specific cleanup.");
        }
    }
}


fn start_daemon(port: Option<u16>) {
    let config_path = "server/src/cli/config.toml";

    let mut host_to_use = "127.0.0.1".to_string();
    let mut port_to_use = port.unwrap_or(8080);
    let mut process_name_to_use = "graphdb-cli".to_string(); // Name used for PID file

    // Load configuration for daemon (host, port, process_name)
    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()
            .unwrap();

        if let Ok(host) = config.get_string("server.host") {
            host_to_use = host;
        }
        // If port was not specified via CLI, try to get it from config
        if port.is_none() {
            if let Ok(cfg_port) = config.get_int("server.port") {
                port_to_use = cfg_port as u16;
            }
        }
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            process_name_to_use = process_name;
        }
    }

    // --- NEW: Proactive cleanup before attempting to start ---
    cleanup_existing_daemons(&process_name_to_use);
    // --- END NEW ---

    let stdout_file_path = "/tmp/daemon.out";
    let stderr_file_path = "/tmp/daemon.err";
    let max_daemon_start_retries = 2; // Number of times to retry the full daemon startup process
    let max_port_check_attempts = 10; // How many times to check the port after daemon forks
    let port_check_interval_ms = 200; // Interval in milliseconds between port checks

    // Regex to parse lsof output like: "COMMAND    PID USER" from the start of the line
    let lsof_regex = Regex::new(r"^\s*(\S+)\s+(\d+)\s+").expect("Invalid lsof regex");

    for attempt in 0..max_daemon_start_retries {
        println!("Attempting to start daemon (Full attempt {})...", attempt + 1);

        // Recreate stdout/stderr files for each attempt to clear previous logs if daemon fails
        let stdout = File::create(stdout_file_path).expect("Failed to create stdout file");
        let stderr = File::create(stderr_file_path).expect("Failed to create stderr file");

        let mut daemonize = DaemonizeBuilder::new()
            .working_directory("/tmp")
            .umask(0o027)
            .stdout(stdout)
            .stderr(stderr)
            .process_name(&process_name_to_use)
            .host(&host_to_use)
            .port(port_to_use)
            .build()
            .expect("Failed to build Daemonize object");

        match daemonize.start() {
            Ok(child_pid_from_fork) => {
                println!("Original process exited, daemonizing (initial PID: {}).", child_pid_from_fork);

                // --- Poll for port availability to confirm daemon actually started ---
                let mut daemon_is_listening = false;
                let socket_addr = format!("{}:{}", host_to_use, port_to_use)
                    .to_socket_addrs()
                    .expect("Invalid host:port address")
                    .next()
                    .expect("No socket address found");

                for i in 0..max_port_check_attempts {
                    std::thread::sleep(std::time::Duration::from_millis(port_check_interval_ms));
                    print!("\r  Checking port {}:{} (Attempt {}/{})...", host_to_use, port_to_use, i + 1, max_port_check_attempts);
                    io::stdout().flush().unwrap(); // Flush print immediately

                    match std::net::TcpStream::connect_timeout(
                        &socket_addr,
                        std::time::Duration::from_millis(100) // Small timeout for connect attempt
                    ) {
                        Ok(_) => {
                            println!("\rDaemon started and is listening on {}:{}!                ", host_to_use, port_to_use);
                            daemon_is_listening = true;
                            // **CRITICAL FIX:** Forceful exit to return control to console
                            std::process::exit(0);
                        }
                        Err(e) => {
                            // If the error is not 'Connection refused' (which is expected while waiting),
                            // it might indicate another problem.
                            if e.kind() != io::ErrorKind::ConnectionRefused {
                                eprintln!("\r  Port check error: {} (kind: {:?})                     ", e, e.kind());
                            }
                        }
                    }
                }
                println!("\r                                                                "); // Clear last line
                eprintln!("Daemon did not appear to start successfully on port {}:{} within the timeout (PID {}).",
                          host_to_use, port_to_use, child_pid_from_fork);
                // No return here, falls through to lsof check and retry if polling failed
            }
            Err(DaemonizeError::AddrInUse(h, p)) => {
                eprintln!("Daemonization failed immediately: Address {}:{} already in use.", h, p);
                // Fall through to lsof check and retry
            }
            Err(e) => { // Other errors from daemonize.start()
                eprintln!("Initial daemonization failed: {}", e);
                return; // Unrecoverable error, exit function
            }
        }

        // If we reach here, it means either:
        // 1. The initial daemonize.start() failed (e.g., first fork error, or directly AddrInUse).
        // 2. The daemon successfully forked, but failed to bind/listen (e.g., AddrInUse in grandchild),
        //    and our port polling confirmed this.
        // In both cases, we need to try to clean up the specific problematic port if it's still held.

        println!("Checking for processes listening on {}:{}...", host_to_use, port_to_use);

        // --- Get PID and Command from lsof ---
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
            // Check for the specific port and LISTEN state
            if line.contains("LISTEN") && line.contains(&format!(":{}", port_to_use)) {
                if let Some(captures) = lsof_regex.captures(line) {
                    if let (Some(cmd_match), Some(pid_match)) = (captures.get(1), captures.get(2)) {
                        if let Ok(pid) = pid_match.as_str().parse::<u32>() {
                            found_process_info = Some((cmd_match.as_str().to_string(), pid));
                            break; // Found the listening process, break loop
                        }
                    }
                }
            }
        }

        match found_process_info {
            Some((cmd_on_port, pid_on_port)) => {
                let daemon_pid_from_file = get_daemon_pid_from_file(&process_name_to_use);

                // Check if the process occupying the port contains "graphdb" or our specific process name (case-insensitive)
                let is_our_app = cmd_on_port.to_lowercase().contains("graphdb") ||
                                 cmd_on_port.to_lowercase().contains(&process_name_to_use.to_lowercase());

                if is_our_app {
                    if let Some(expected_pid) = daemon_pid_from_file {
                        // If the PID on the port matches our PID file, it's our own daemon already running.
                        // This indicates an issue where we tried to start it again, but it was actually fine.
                        // However, the current logic is to try killing it if we are in a "failed to start" path.
                        if expected_pid == pid_on_port {
                            println!("Found a running instance of our daemon (PID {}) on port {}:{}. This likely explains the binding failure.",
                                     pid_on_port, host_to_use, port_to_use);
                            println!("Attempting to terminate it to clear the port for a fresh start...",);
                        } else {
                            // PID from file doesn't match PID on port, but command name matches.
                            // This means a stale daemon process is there, but PID file might be old/corrupted.
                            println!("Found a stale/different instance of graphdb-cli/daemon (PID {}) on port {}:{}. Terminating...",
                                      pid_on_port, host_to_use, port_to_use);
                        }
                    } else {
                        // PID file not found, but a graphdb process is on the port. Kill it.
                        println!("Found an unnamed/stale graphdb process (PID {}) on port {}:{}. Terminating...",
                                  pid_on_port, host_to_use, port_to_use);
                    }

                    // Attempt to kill the found PID
                    if let Err(e) = kill(Pid::from_raw(pid_on_port as i32), Signal::SIGTERM) {
                        eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue. Manual intervention may be required.", pid_on_port, e);
                    } else {
                        println!("Sent SIGTERM to PID {}.", pid_on_port);
                        // Also remove the PID file if it exists and matches
                        let pid_file_path = format!("/tmp/{}.pid", process_name_to_use);
                        if Path::new(&pid_file_path).exists() {
                            if let Ok(file_pid_str) = fs::read_to_string(&pid_file_path) {
                                if let Ok(file_pid) = file_pid_str.trim().parse::<u32>() {
                                    if file_pid == pid_on_port {
                                        fs::remove_file(&pid_file_path).ok(); // Ignore error if it fails to remove
                                        println!("Removed stale PID file: {}", pid_file_path);
                                    }
                                }
                            }
                        }
                    }
                } else { // Not our app
                    eprintln!("Port {}:{} is occupied by another application: {} (PID {}). Attempting to terminate it...",
                              host_to_use, port_to_use, cmd_on_port, pid_on_port);
                    // Attempt to kill the other application
                    if let Err(e) = kill(Pid::from_raw(pid_on_port as i32), Signal::SIGTERM) {
                        eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue. Manual intervention may be required.", pid_on_port, e);
                    } else {
                        println!("Sent SIGTERM to PID {}.", pid_on_port);
                    }
                }
                // Always sleep before retry to give processes time to terminate and release the port
                std::thread::sleep(std::time::Duration::from_millis(1500)); // Slightly longer sleep
                // Continue to the next attempt in the outer loop
            }
            None => {
                eprintln!("Could not identify any process holding port {}:{}.", host_to_use, port_to_use);
                eprintln!("This is unexpected, as the daemon itself reported 'Address in use'. The port might be fleetingly held or requires manual intervention.");
                if attempt < max_daemon_start_retries - 1 {
                    eprintln!("Retrying startup anyway after a brief pause...");
                    std::thread::sleep(std::time::Duration::from_millis(1000));
                } else {
                    eprintln!("Daemon startup failed after all retries and could not clear the port.");
                    return;
                }
            }
        }
    }
    eprintln!("Daemon startup failed after all retries."); // Only reached if loop completes without success
}

// Helper function to read PID from the daemon's PID file
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

// UPDATED: stop_daemon function for robustness
fn stop_daemon() {
    let config_path = "server/src/cli/config.toml";
    let mut process_name_to_use = "graphdb-cli".to_string(); // Default process name

    // Re-read config to ensure consistency with how the daemon was started
    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()
            .unwrap();
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            process_name_to_use = process_name;
        }
    }

    println!("Attempting to stop all '{}' daemon instances...", process_name_to_use);

    let pgrep_output = Command::new("pgrep")
        .arg("-x") // Exact match
        .arg(&process_name_to_use)
        .stdout(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    let mut stopped_any = false;
    match pgrep_output {
        Ok(output) if output.status.success() => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if pids.is_empty() {
                println!("No '{}' daemon instances found running.", process_name_to_use);
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
        _ => {
            eprintln!("Could not use `pgrep` to find existing daemon instances. Manual intervention may be required.");
        }
    }

    let pid_file_path = format!("/tmp/{}.pid", process_name_to_use);
    if Path::new(&pid_file_path).exists() {
        if let Err(e) = fs::remove_file(&pid_file_path) {
            eprintln!("Warning: Failed to remove PID file {}: {}", pid_file_path, e);
        } else {
            println!("Removed PID file: {}", pid_file_path);
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
                    start_daemon(None); // Call start_daemon without explicit port from interactive CLI
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

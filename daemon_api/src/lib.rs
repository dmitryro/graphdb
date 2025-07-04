// daemon_api/src/lib.rs
use serde::{Serialize, Deserialize};
// Changed to tokio::process::Command where .await is used
use tokio::process::Command; 
use std::sync::{Arc, Mutex};
use std::fs::{self, File};
use std::path::Path;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use config::{Config, File as ConfigFile};
use daemon::{DaemonizeBuilder, DaemonizeError};
use lazy_static::lazy_static;
use regex::Regex;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream; // Use tokio's TcpStream for async connect
use tokio::time::{sleep, Duration}; // Use tokio's sleep for async delays
use anyhow::Result; // Use anyhow::Result for consistent error handling
use std::process::Stdio; // FIX: Added to bring Stdio into scope

// Public modules for shared CLI schema and help generation
pub mod cli_schema;
pub mod help_generator;

// Re-export common types and functions for easier access
pub use cli_schema::{CliArgs, GraphDbCommands, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, StopAction, StatusAction, HelpArgs};
pub use lib::storage_engine::config::StorageEngineType;
pub use help_generator::{generate_full_help, generate_help_for_path};

// Shared constant for the assumed default storage daemon port used by CLI status and REST API
pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonData {
    pub port: u16,
    pub pid: u32,
}

lazy_static! {
    // SHUTDOWN_FLAG uses Arc<Mutex<bool>> to be shared across threads and tasks
    pub static ref SHUTDOWN_FLAG: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

fn remove_pid_file(pid_file_path: &str) {
    if Path::new(pid_file_path).exists() {
        let _ = fs::remove_file(pid_file_path);
    }
}

fn cleanup_existing_daemons(base_process_name: &str) {
    let _ = std::process::Command::new("pkill") // Use std::process::Command here as it's not awaited
        .arg("-f")
        .arg(base_process_name)
        .status();
    std::thread::sleep(std::time::Duration::from_millis(1000)); // This is blocking, but for cleanup it might be acceptable.
}

#[derive(Debug, thiserror::Error)] // Use thiserror for better error handling integration
pub enum DaemonError {
    #[error("Daemonize error: {0}")]
    Daemonize(String), // FIX: Changed to String as DaemonizeError doesn't implement std::error::Error
    #[error("Invalid port range: {0}")]
    InvalidPortRange(String),
    #[error("Invalid cluster format: {0}")]
    InvalidClusterFormat(String),
    #[error("No daemons started")]
    NoDaemonsStarted,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Config error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("Process error: {0}")]
    ProcessError(String),
    #[error("General error: {0}")]
    GeneralError(String),
    #[error("Reqwest error: {0}")] // Added for potential HTTP client calls, although not used here yet
    Reqwest(#[from] reqwest::Error), // Ensure reqwest is in Cargo.toml if this is enabled
}

// FIX: Manual From implementation for DaemonizeError
impl From<DaemonizeError> for DaemonError {
    fn from(err: DaemonizeError) -> Self {
        DaemonError::Daemonize(format!("{}", err))
    }
}

/// Starts one or more GraphDB daemon processes.
/// Returns Result<(), DaemonError> for compatibility with code that matches on Result.
/// Accepts skip_ports: Vec<u16> to avoid binding reserved ports like the REST API port.
pub async fn start_daemon(port: Option<u16>, cluster_range: Option<String>, skip_ports: Vec<u16>) -> Result<(), DaemonError> {
    let config_path = "server/src/cli/config.toml";

    let mut host_to_use = "127.0.0.1".to_string();
    let mut default_port = 8080;
    let mut base_process_name = "graphdb-cli".to_string();

    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()?;

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

    cleanup_existing_daemons(&base_process_name);

    let mut ports_to_start: Vec<u16> = Vec::new();

    if let Some(range_str) = cluster_range {
        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            return Err(DaemonError::InvalidClusterFormat(range_str));
        }
        let start_port = parts[0].parse::<u16>().unwrap_or(0);
        let end_port = parts[1].parse::<u16>().unwrap_or(0);

        if start_port == 0 || end_port == 0 || start_port > end_port {
            return Err(DaemonError::InvalidPortRange(range_str));
        }

        let num_ports = end_port - start_port + 1;
        if num_ports == 0 {
            return Err(DaemonError::InvalidPortRange(range_str));
        }
        if num_ports > 10 {
            return Err(DaemonError::InvalidPortRange(format!(
                "Cluster port range size ({}) exceeds maximum allowed (10).",
                num_ports
            )));
        }

        for p in start_port..=end_port {
            ports_to_start.push(p);
        }
    } else {
        ports_to_start.push(port.unwrap_or(default_port));
    }

    let max_port_check_attempts = 10;
    let port_check_interval_ms = 200;
    let _lsof_regex = Regex::new(r"^\s*(\S+)\s+(\d+)\s+").expect("Invalid lsof regex");

    let mut any_started = false;
    for current_port in ports_to_start {
        // Skip ports in skip_ports vec (e.g., REST API port)
        if skip_ports.contains(&current_port) {
            println!("[INFO] Skipping reserved port {}: reserved for another service.", current_port);
            continue;
        }

        let specific_process_name = format!("{}-{}", base_process_name, current_port);
        let specific_stdout_file_path = format!("/tmp/daemon-{}.out", current_port);
        let specific_stderr_file_path = format!("/tmp/daemon-{}.err", current_port);

        let stdout = File::create(&specific_stdout_file_path)?;
        let stderr = File::create(&specific_stderr_file_path)?;

        let mut daemonize = DaemonizeBuilder::new()
            .working_directory("/tmp")
            .umask(0o027)
            .stdout(stdout)
            .stderr(stderr)
            .process_name(&specific_process_name)
            .host(&host_to_use)
            .port(current_port)
            .skip_ports(skip_ports.clone())
            .build()?;

        match daemonize.start() {
            Ok(_child_pid_from_fork) => {
                let socket_addr = format!("{}:{}", host_to_use, current_port)
                    .to_socket_addrs()
                    .map_err(DaemonError::Io)?
                    .next()
                    .ok_or_else(|| DaemonError::InvalidPortRange(format!("No socket for port {}", current_port)))?;

                for _ in 0..max_port_check_attempts {
                    sleep(Duration::from_millis(port_check_interval_ms)).await; // Use tokio sleep
                    if TcpStream::connect(
                        &socket_addr
                    ).await.is_ok() { // Use tokio TcpStream and await
                        any_started = true;
                        break;
                    }
                }
            }
            Err(e) => {
                return Err(e.into()); // FIX: Use .into() to leverage the From implementation
            }
        }
    }

    if !any_started {
        return Err(DaemonError::NoDaemonsStarted);
    }
    Ok(())
}

/// Stops a specific daemon instance running on the given port.
pub fn stop_port_daemon(port: u16) -> Result<(), DaemonError> {
    println!("Attempting to stop daemon instance on port {}...", port);

    let pid_file_path = format!("/tmp/daemon-{}.pid", port);

    // 1. Try to read PID from a PID file
    if Path::new(&pid_file_path).exists() {
        match std::fs::read_to_string(&pid_file_path) {
            Ok(pid_str) => {
                if let Ok(pid_val) = pid_str.trim().parse::<u32>() {
                    println!("Found PID {} in file {}. Attempting to kill...", pid_val, pid_file_path);
                    if let Err(e) = kill(Pid::from_raw(pid_val as i32), Signal::SIGTERM) {
                        eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid_val, e);
                        remove_pid_file(&pid_file_path); // Clean up stale PID file
                        return Err(DaemonError::ProcessError(format!("Failed to kill process with PID {}: {}", pid_val, e)));
                    } else {
                        println!("Successfully sent SIGTERM to daemon on port {}.", port);
                        remove_pid_file(&pid_file_path); // Clean up PID file on success
                        return Ok(());
                    }
                } else {
                    eprintln!("Invalid PID found in file {}: {}", pid_file_path, pid_str);
                    remove_pid_file(&pid_file_path); // Remove corrupted PID file
                }
            }
            Err(e) => {
                eprintln!("Failed to read PID file {}: {}", pid_file_path, e);
            }
        }
    }

    // 2. If PID file doesn't exist or failed, try to find by process name and port
    // This assumes your daemon's command line arguments or `ps` output would contain the port.
    // Example: `graphdb --port 8080`
    let pgrep_arg = format!("graphdb --port {}", port); // Adjust this regex if your daemon uses a different argument structure

    let pgrep_result = std::process::Command::new("pgrep") // Use std::process::Command here
        .arg("-f")
        .arg(&pgrep_arg)
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
                println!("No daemon found running with command line matching '{}'.", pgrep_arg);
                return Err(DaemonError::GeneralError(format!("No daemon found on port {}", port)));
            } else {
                for pid in pids {
                    // Double check to ensure we're killing the right process if needed,
                    // but `pgrep -f` with a specific argument is usually quite precise.
                    println!("Found PID {} matching '{}'. Attempting to kill...", pid, pgrep_arg);
                    if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                        eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                        return Err(DaemonError::ProcessError(format!("Failed to kill process with PID {}: {}", pid, e)));
                    } else {
                        println!("Successfully sent SIGTERM to daemon on port {}.", port);
                        remove_pid_file(&pid_file_path); // Clean up PID file even if found by pgrep
                        return Ok(());
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to execute `pgrep` command for port {}: {}. Please ensure `pgrep` is installed and in your PATH.", port, e);
            return Err(DaemonError::ProcessError(format!("Failed to execute `pgrep`: {}", e)));
        }
    }

    // If we reach here, no daemon was found or successfully stopped.
    Err(DaemonError::GeneralError(format!("Could not find or stop daemon on port {}", port)))
}

/// Stops all daemons, returns Result<(), DaemonError> for compatibility.
pub fn stop_daemon() -> Result<(), DaemonError> {
    let config_path = "server/src/cli/config.toml";
    let mut base_process_name = "graphdb-cli".to_string();

    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()?;
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            base_process_name = process_name;
        }
    }

    println!("Attempting to stop all 'graphdb' related daemon instances...");

    let pgrep_result = std::process::Command::new("pgrep") // Use std::process::Command here
        .arg("-f")
        .arg("graphdb")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    let mut stopped_any = false;
    match pgrep_result {
        Ok(output) => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if !pids.is_empty() {
                for pid in pids {
                    let comm_output = std::process::Command::new("ps") // Use std::process::Command here
                        .arg("-o")
                        .arg("comm=")
                        .arg(format!("{}", pid))
                        .stdout(Stdio::piped())
                        .spawn()
                        .and_then(|child| child.wait_with_output());

                    if let Ok(comm_out) = comm_output {
                        let command_name = String::from_utf8_lossy(&comm_out.stdout).trim().to_lowercase();
                        if command_name.contains("graphdb") || command_name.contains(&base_process_name.to_lowercase()) {
                            if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                                eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                            } else {
                                stopped_any = true;
                            }
                        } else {
                            // not a daemon, skip
                        }
                    } else {
                        if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                            eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                        } else {
                            stopped_any = true;
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(1000));
            }
        }
        Err(e) => {
            eprintln!("Failed to execute `pgrep` command: {}. Manual intervention may be required.", e);
            eprintln!("Please ensure `pgrep` is installed and in your PATH.");
        }
    }

    // Remove known PID files (base and per-port)
    let base_pid_file_path = "/tmp/graphdb-cli.pid";
    remove_pid_file(base_pid_file_path);

    for port in 8000..=9010 { // Iterate over a common range of daemon ports to cleanup PID files
        let port_pid_file = format!("/tmp/daemon-{}.pid", port);
        remove_pid_file(&port_pid_file);
    }

    *SHUTDOWN_FLAG.lock().unwrap() = true;
    if stopped_any {
        Ok(())
    } else {
        Ok(()) // Not an error if nothing stopped
    }
}

/// Finds the running storage daemon port by checking common PID file locations
/// or by listing processes.
pub async fn find_running_storage_daemon_port() -> Option<u16> {
    // 1. Check for the default storage daemon PID file
    let default_pid_file = format!("/tmp/daemon-{}.pid", CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
    if Path::new(&default_pid_file).exists() {
        if let Ok(pid_str) = fs::read_to_string(&default_pid_file) {
            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                // Check if the process with this PID is actually running
                // SIGCONT just checks if the process exists without killing it
                if kill(Pid::from_raw(pid), Signal::SIGCONT).is_ok() {
                    println!("[daemon_api] Found storage daemon PID {} in {}. Assumed running on port {}", pid, default_pid_file, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
                    return Some(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
                } else {
                    println!("[daemon_api] PID {} in {} is stale. Removing file.", pid, default_pid_file);
                    remove_pid_file(&default_pid_file);
                }
            }
        }
    }

    // 2. Fallback: Search for "storage_daemon_server" process using lsof
    // This is more precise than pgrep -f for finding processes by port.
    let common_storage_ports = [CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, 8086, 8087]; // Add other common storage daemon ports if any
    for &port in &common_storage_ports {
        let output = Command::new("lsof") // Use tokio::process::Command here
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t") // Only print PIDs
            .output()
            .await; // .await is now valid

        if let Ok(output) = output {
            let pids = String::from_utf8_lossy(&output.stdout);
            if !pids.trim().is_empty() {
                // Found a PID listening on this port. Assume it's the storage daemon.
                // This is a heuristic; a more robust solution would involve PID files or health checks.
                return Some(port);
            }
        }
    }
    
    // As a last resort, check for the process name directly (less reliable for port determination)
    let pgrep_result = std::process::Command::new("pgrep") // Use std::process::Command here
        .arg("-f")
        .arg("storage_daemon_server") // Assuming the storage daemon executable is named this
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    if let Ok(output) = pgrep_result {
        let pids_str = String::from_utf8_lossy(&output.stdout);
        let pids: Vec<u32> = pids_str.lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .collect();
        if !pids.is_empty() {
            println!("[daemon_api] Found storage daemon by process name. Cannot determine port reliably, returning default.");
            // If we found it by name but not by port, we can't reliably say which port.
            // For now, return the default.
            return Some(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
        }
    }

    None
}

/// A wrapper function that simply calls the existing `stop_daemon` logic.
/// This matches the previous mock's signature.
pub fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    // The existing stop_daemon returns Result<(), DaemonError>, convert to anyhow::Result
    stop_daemon().map_err(|e| anyhow::anyhow!("Daemon stop failed: {}", e))
}


// server/src/cli/daemon_management.rs

// This file contains logic for daemonizing and managing background processes
// (REST API, Storage, and GraphDB daemons).

use anyhow::{Context, Result, Error};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use std::process;
use std::str::FromStr;
use clap::ValueEnum;

// For TCP connection checks and async sleeps
use tokio::net::TcpStream;
use tokio::time::{self, Duration};

// For system process information
// FIX: Removed SystemExt, ProcessExt, and Signal from sysinfo imports, as they are not needed.
// SystemExt and ProcessExt are not required for System::new, refresh_all, or Process::name.
// Signal is not used; nix::sys::signal::Signal is used for Unix signal handling.
use sysinfo::{System, Pid, RefreshKind, Process};

use std::ffi::OsStr;
#[cfg(target_family = "unix")]
use std::os::unix::ffi::OsStrExt;

// Import necessary items from sibling modules
use crate::cli::config::{get_default_rest_port_from_config, load_storage_config, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, StorageConfig as CliStorageConfig};
use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig};

// External crates
use daemon_api::{stop_daemon, DaemonError}; // Assuming daemon_api provides a way to gracefully stop
use rest_api::start_server as start_rest_server;
use storage_daemon_server::run_storage_daemon as start_storage_server;

// NIX imports for signal handling (Conditional for Unix-like systems)
#[cfg(target_family = "unix")]
use nix::sys::signal::{self, Signal}; // Explicitly import Signal enum
#[cfg(target_family = "unix")]
use nix::unistd::Pid as NixPid;

// Logging imports
use log::{info, warn, error, debug};

use tokio::fs;
use std::process::Stdio;

// --- PID File Management ---

// Define a consistent location for PID files.
// For a production application, you might want a more specific,
// non-temporary directory (e.g., ~/.graphdb/run/ or /var/run/graphdb/).
// Using temp_dir for ease of testing/demonstration.
fn get_pid_file_path(port: u16) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("graphdb_daemon_{}.pid", port)); // Specific for the main daemon
    path
}

fn get_rest_pid_file_path(port: u16) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("graphdb_rest_{}.pid", port));
    path
}

fn get_storage_pid_file_path(port: u16) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("graphdb_storage_{}.pid", port));
    path
}

/// Writes the current process's PID to a file.
/// This function should be called by the daemon itself when it starts.
pub async fn write_pid_file(service_port: u16, pid: u32, service_type: &str) -> Result<()> {
    let pid_file = match service_type {
        "main" => get_pid_file_path(service_port),
        "rest" => get_rest_pid_file_path(service_port),
        "storage" => get_storage_pid_file_path(service_port),
        _ => return Err(anyhow::anyhow!("Invalid service type provided for PID file writing: {}", service_type)),
    };

    fs::write(&pid_file, pid.to_string())
        .await
        .with_context(|| format!("Failed to write PID {} for {} service to file: {:?}", pid, service_type, pid_file))?;
    info!("Successfully wrote PID {} for {} service to {:?}", pid, service_type, pid_file);
    Ok(())
}

/// Reads the PID from a daemon's PID file.
async fn read_pid_file(service_port: u16, service_type: &str) -> Result<u32> {
    let pid_file = match service_type {
        "main" => get_pid_file_path(service_port),
        "rest" => get_rest_pid_file_path(service_port),
        "storage" => get_storage_pid_file_path(service_port),
        _ => return Err(anyhow::anyhow!("Invalid service type provided for PID file reading: {}", service_type)),
    };

    let pid_str = fs::read_to_string(&pid_file)
        .await
        .with_context(|| format!("Failed to read PID from {} file: {:?}", service_type, pid_file))?;
    let pid = pid_str
        .trim()
        .parse::<u32>()
        .with_context(|| format!("Failed to parse PID from {} file content: '{}'", service_type, pid_str))?;
    Ok(pid)
}

/// Deletes a daemon's PID file.
async fn delete_pid_file(service_port: u16, service_type: &str) -> Result<()> {
    let pid_file = match service_type {
        "main" => get_pid_file_path(service_port),
        "rest" => get_rest_pid_file_path(service_port),
        "storage" => get_storage_pid_file_path(service_port),
        _ => return Err(anyhow::anyhow!("Invalid service type provided for PID file deletion: {}", service_type)),
    };

    if pid_file.exists() {
        fs::remove_file(&pid_file)
            .await
            .with_context(|| format!("Failed to delete {} PID file: {:?}", service_type, pid_file))?;
        info!("Successfully deleted {} PID file: {:?}", service_type, pid_file);
    }
    Ok(())
}

/// Checks if a process with the given PID is currently running.
pub async fn is_process_running(pid: u32) -> bool {
    let mut system = System::new();
    system.refresh_all();
    system.process(Pid::from_u32(pid)).is_some()
}

// Unix-specific signal sending using `nix`
#[cfg(target_family = "unix")]
async fn send_unix_signal(pid: u32, signal: Signal) -> Result<()> {
    let pid_nix = NixPid::from_raw(pid as i32);
    signal::kill(pid_nix, signal)
        .with_context(|| format!("Failed to send signal {:?} to PID {}", signal, pid))?;
    debug!("Sent signal {:?} to PID {}", signal, pid);
    Ok(())
}

// Windows-specific termination using `taskkill` command
#[cfg(target_family = "windows")]
async fn terminate_windows_process(pid: u32, force: bool) -> Result<()> {
    let mut cmd = tokio::process::Command::new("taskkill");
    cmd.arg("/PID").arg(pid.to_string());
    if force {
        cmd.arg("/F"); // Forcefully terminate
    }
    cmd.arg("/T"); // Terminate child processes created by the target process

    let status = cmd.status()
        .await
        .with_context(|| format!("Failed to execute taskkill for PID {}", pid))?;

    if status.success() {
        debug!("Sent termination command (force: {}) to PID {}", force, pid);
        Ok(())
    } else {
        Err(anyhow::anyhow!("taskkill failed for PID {} (force: {}) with status: {:?}", pid, force, status))
    }
}

/// Attempts to stop a daemon process using PID file for direct lookup,
/// falling back to port/name scan if no PID file or if it's outdated.
async fn stop_daemon_by_pid_or_scan(
    daemon_name: &str,
    port: u16,
    service_type: &str, // "main", "rest", "storage"
) -> Result<()> {
    info!("Attempting to stop {} service on port {}", daemon_name, port);

    let mut pid_from_file: Option<u32> = None;
    if let Ok(pid) = read_pid_file(port, service_type).await {
        if is_process_running(pid).await {
            info!("Found {} running with PID {} from PID file.", daemon_name, pid);
            pid_from_file = Some(pid);
        } else {
            warn!("PID file for {} on port {} exists (PID {}), but process is not running. Cleaning up stale PID file.", daemon_name, port, pid);
            let _ = delete_pid_file(port, service_type).await;
        }
    }

    let target_pid = if pid_from_file.is_some() {
        pid_from_file
    } else {
        // Fallback to searching by port/name if PID file is absent or stale
        warn!("PID file not found or stale for {} on port {}. Attempting to find by port scan.", daemon_name, port);
        find_daemon_process_on_port(port, Some(daemon_name)).await
    };

    let Some(pid) = target_pid else {
        info!("No {} process found running on port {} to stop.", daemon_name, port);
        let _ = delete_pid_file(port, service_type).await; // Ensure no PID file left behind
        return Ok(());
    };

    info!("Stopping {} (PID: {}). Attempting graceful shutdown...", daemon_name, pid);

    // 1. Attempt graceful shutdown
    #[cfg(target_family = "unix")]
    {
        if let Err(e) = send_unix_signal(pid, Signal::SIGTERM).await {
            warn!("Failed to send SIGTERM to {} (PID {}): {}", daemon_name, pid, e);
        }
    }
    #[cfg(target_family = "windows")]
    {
        if let Err(e) = terminate_windows_process(pid, false).await { // Try non-forceful first
            debug!("Non-forceful termination failed for {} (PID {}): {}. Will try forceful if needed.", daemon_name, pid, e);
        }
    }

    // Wait for a short duration for the process to exit
    let mut attempts = 0;
    while is_process_running(pid).await && attempts < 5 { // Try for 5 seconds
        info!("Waiting for {} (PID {}) on port {} to stop (attempt {}/5)...", daemon_name, pid, port, attempts + 1);
        time::sleep(Duration::from_secs(1)).await;
        attempts += 1;
    }

    if is_process_running(pid).await {
        error!("{} (PID {}) on port {} did not stop gracefully after {} seconds. Attempting forceful termination.", daemon_name, pid, port, attempts);
        // 2. Attempt forceful shutdown if still running
        #[cfg(target_family = "unix")]
        {
            if let Err(e) = send_unix_signal(pid, Signal::SIGKILL).await {
                return Err(anyhow::anyhow!("Failed to send SIGKILL to {} (PID {}): {}", daemon_name, pid, e));
            }
        }
        #[cfg(target_family = "windows")]
        {
            if let Err(e) = terminate_windows_process(pid, true).await { // taskkill /F is forceful
                return Err(anyhow::anyhow!("Failed to forcefully terminate {} (PID {}): {}", daemon_name, pid, e));
            }
        }
        // Give it a final moment after forceful kill
        time::sleep(Duration::from_millis(500)).await;
    }

    if is_process_running(pid).await {
        error!("{} (PID {}) on port {} is still running after forceful termination. Manual intervention may be required.", daemon_name, pid, port);
        Err(anyhow::anyhow!("{} on port {} (PID {}) could not be stopped.", daemon_name, port, pid))
    } else {
        info!("{} (PID {}) on port {} successfully stopped.", daemon_name, pid, port);
        // Clean up PID file after successful stop
        let _ = delete_pid_file(port, service_type).await;
        Ok(())
    }
}

/// Handles the internal execution of daemonized processes (REST API or Storage).
/// This function is called when the CLI is invoked with `--internal-rest-api-run` or `--internal-storage-daemon-run`.
pub async fn handle_internal_daemon_run(
    is_rest_api_run: bool,
    is_storage_daemon_run: bool,
    internal_port: Option<u16>,
    internal_storage_config_path: Option<PathBuf>,
    _internal_storage_engine: Option<StorageEngineType>, // Marked as unused
) -> Result<()> {
    if is_rest_api_run {
        let daemon_listen_port = internal_port.unwrap_or_else(get_default_rest_port_from_config);

        // Write PID file for REST service
        let current_pid = process::id();
        write_pid_file(daemon_listen_port, current_pid, "rest").await?;

        let cli_storage_config = load_storage_config(internal_storage_config_path.clone())
            .unwrap_or_else(|e| {
                eprintln!("[DAEMON PROCESS] Warning: Could not load storage config for REST API: {}. Using defaults for CLI config.", e);
                CliStorageConfig::default()
            });

        println!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let (_tx, rx) = tokio::sync::oneshot::channel(); // Dummy channel for shutdown
        let result = start_rest_server(daemon_listen_port, rx, cli_storage_config.data_directory.clone()).await;
        if let Err(e) = result {
            eprintln!("[DAEMON PROCESS] REST API server failed: {:?}", e);
            let _ = delete_pid_file(daemon_listen_port, "rest").await; // Clean up PID file on failure
            process::exit(1);
        }
        println!("[DAEMON PROCESS] REST API server (daemonized) stopped.");
        let _ = delete_pid_file(daemon_listen_port, "rest").await; // Clean up PID file on graceful exit
        process::exit(0);
    } else if is_storage_daemon_run {
        let daemon_listen_port = internal_port.unwrap_or_else(|| {
            load_storage_config(None).map(|c| c.default_port).unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
        });
        let storage_config_path = internal_storage_config_path.unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("Failed to get parent directory of server crate")
                .join("storage_daemon_server")
                .join("storage_config.yaml")
        });

        // Write PID file for Storage service
        let current_pid = process::id();
        write_pid_file(daemon_listen_port, current_pid, "storage").await?;

        println!("[DAEMON PROCESS] Starting Storage daemon (daemonized) on port {}...", daemon_listen_port);
        let result = start_storage_server(Some(daemon_listen_port), storage_config_path).await;
        if let Err(e) = result {
            eprintln!("[DAEMON PROCESS] Storage daemon failed: {:?}", e);
            let _ = delete_pid_file(daemon_listen_port, "storage").await; // Clean up PID file on failure
            process::exit(1);
        }
        println!("[DAEMON PROCESS] Storage daemon (daemonized) stopped.");
        let _ = delete_pid_file(daemon_listen_port, "storage").await; // Clean up PID file on graceful exit
        process::exit(0);
    }
    Ok(())
}

/// Spawns a new process to run a daemon (REST API or Storage) in the background.
pub async fn start_daemon_process(
    is_rest: bool,
    is_storage: bool,
    port: Option<u16>,
    config_path: Option<PathBuf>,
    engine_type: Option<StorageEngineType>,
) -> Result<()> {
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
    let mut command = Command::new(current_exe);

    if is_rest {
        command.arg("--internal-rest-api-run");
    } else if is_storage {
        command.arg("--internal-storage-daemon-run");
    }

    if let Some(p) = port {
        command.arg("--internal-port").arg(p.to_string());
    }
    if let Some(path) = config_path {
        command.arg("--internal-storage-config-path").arg(path);
    }
    if let Some(engine) = engine_type {
        command.arg("--internal-storage-engine").arg(engine.to_possible_value().unwrap().get_name());
    }

    // Set stdio to null to detach from parent's stdout/stderr
    #[cfg(not(target_family = "windows"))] // On Unix, detach
    {
        use std::os::unix::process::CommandExt;
        command.stdout(Stdio::null()).stderr(Stdio::null());
        unsafe {
            command.pre_exec(|| {
                // Detach from controlling terminal
                nix::unistd::setsid().map(|_| ());
                Ok(())
            });
        }
    }
    #[cfg(target_family = "windows")] // On Windows, still detach effectively
    {
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
        command.stdout(Stdio::null()).stderr(Stdio::null());
    }

    let child = command.spawn().context("Failed to spawn daemon process")?;
    println!("Daemonized process spawned with PID {}", child.id().unwrap_or(0));
    // The PID file writing now happens in `handle_internal_daemon_run` within the spawned process.
    Ok(())
}

/// Checks if a daemon (general) is running by first checking PID file, then network port.
pub async fn is_daemon_running(port: u16, service_type: &str) -> bool {
    // 1. Check PID file first for direct identification
    if let Ok(pid) = read_pid_file(port, service_type).await {
        if is_process_running(pid).await {
            debug!("{} daemon on port {} detected via PID file (PID: {}).", service_type, port, pid);
            return true;
        } else {
            warn!("Stale PID file for {} daemon on port {} (PID: {}). Cleaning up.", service_type, port, pid);
            let _ = delete_pid_file(port, service_type).await; // Clean up stale PID file
        }
    }

    // 2. Fallback to TCP connection check if PID file is absent or stale
    debug!("PID file check failed for {} on port {}. Falling back to TCP stream check.", service_type, port);
    TcpStream::connect(format!("127.0.0.1:{}", port)).await.is_ok()
}

/// Checks if the REST API daemon is running on the given port.
pub async fn is_rest_api_running(port: u16) -> bool {
    is_daemon_running(port, "rest").await
}

/// Checks if the Storage daemon is running on the given port.
pub async fn is_storage_daemon_running(port: u16) -> bool {
    is_daemon_running(port, "storage").await
}

/// NEW: Sends a stop signal to the main daemon process identified by its port.
/// This assumes the "main" daemon writes its PID to `graphdb_daemon_<port>.pid`.
pub async fn send_stop_signal_to_daemon_main(port: u16) -> Result<()> {
    // This now calls the more general stop_daemon_by_pid_or_scan.
    // The "main" daemon should ideally write its PID file.
    stop_daemon_by_pid_or_scan("main daemon", port, "main").await
}

/// Sends a stop signal to the REST API daemon process.
/// This is a specific wrapper for backward compatibility or explicit API intent.
pub async fn send_stop_signal_to_rest_api(port: u16) -> Result<()> {
    send_stop_signal_to_daemon("REST API daemon", port).await
}

/// Sends a stop signal to a daemon process.
/// This function now primarily dispatches to `stop_daemon_by_pid_or_scan`
/// but also includes a call to `daemon_api::stop_daemon()` if it's generally applicable.
pub async fn send_stop_signal_to_daemon(daemon_name: &str, port: u16) -> Result<()> {
    // Try to stop via general daemon API call first (if applicable to all daemons)
    let api_stop_result = stop_daemon();
    match api_stop_result {
        Ok(_) => info!("Successfully sent general API stop signal."),
        Err(e) => warn!("Warning: Failed to send general API stop signal: {:?}", e),
    }

    let service_type = if daemon_name.to_lowercase().contains("main") {
        "main"
    } else if daemon_name.to_lowercase().contains("rest api") {
        "rest"
    } else if daemon_name.to_lowercase().contains("storage") {
        "storage"
    } else {
        // Fallback for unknown daemon names, might not use PID files
        warn!("Unknown daemon name '{}'. Will attempt stop by process name/port only.", daemon_name);
        return stop_daemon_by_process_name_and_port(daemon_name, port).await;
    };

    stop_daemon_by_pid_or_scan(daemon_name, port, service_type).await
}

// Fallback for stopping daemons not necessarily using PID files or for generic names.
async fn stop_daemon_by_process_name_and_port(daemon_name: &str, port: u16) -> Result<()> {
    println!("Attempting to stop {} by process name and port {}. (No specific PID file found or type not recognized)", daemon_name, port);

    if let Some(pid) = find_daemon_process_on_port(port, Some(daemon_name)).await {
        println!("Attempting to kill {} process (PID: {}).", daemon_name, pid);
        #[cfg(target_family = "unix")]
        {
            match signal::kill(NixPid::from_raw(pid as i32), Signal::SIGTERM) {
                Ok(_) => println!("Successfully sent SIGTERM to {} (PID: {}).", daemon_name, pid),
                Err(e) => return Err(anyhow::anyhow!("Failed to send SIGTERM to {} (PID: {}): {}", daemon_name, pid, e)),
            }
        }
        #[cfg(not(target_family = "unix"))]
        {
            let mut sys = System::new_all();
            if let Some(process) = sys.process(Pid::from_u32(pid)) {
                // Windows: Use taskkill for termination, as sysinfo signal handling is not reliable
                if let Err(e) = terminate_windows_process(pid, false).await {
                    warn!("Non-forceful termination failed for {} (PID {}): {}. Trying forceful.", daemon_name, pid, e);
                    if let Err(e) = terminate_windows_process(pid, true).await {
                        return Err(anyhow::anyhow!("Failed to forcefully terminate {} (PID {}): {}", daemon_name, pid, e));
                    }
                }
                let start_time = tokio::time::Instant::now();
                while sys.process(Pid::from_u32(pid)).is_some() && start_time.elapsed() < Duration::from_secs(10) {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    sys.refresh_all();
                }
                if sys.process(Pid::from_u32(pid)).is_some() {
                    eprintln!("Warning: {} (PID: {}) did not terminate after 10 seconds.", daemon_name, pid);
                    return Err(anyhow::anyhow!("Daemon did not stop gracefully."));
                }
            } else {
                eprintln!("Warning: Process with PID {} not found after initial check.", pid);
            }
        }
    } else {
        println!("No {} process found on port {} to kill.", daemon_name, port);
    }
    Ok(())
}

/// Retrieves the storage configuration from the default path, or uses defaults if not found.
pub fn get_cli_storage_config() -> CliStorageConfig {
    load_storage_config(None)
        .unwrap_or_else(|e| {
            eprintln!(
                "Warning: Failed to load storage config: {}. Using default values.",
                e
            );
            CliStorageConfig::default()
        })
}

/// Gets the default storage port from the config file, or a hardcoded CLI default if config fails.
pub fn get_default_storage_port_from_config_or_cli_default() -> u16 {
    load_storage_config(None)
        .map(|c| c.default_port)
        .unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
}

/// Helper function to find a running daemon's PID on a specific port.
/// Returns the PID as u32 if found, otherwise None.
/// `expected_name` can be used to filter processes (e.g., "graphdb-cli", "rest_api_server").
pub async fn find_daemon_process_on_port(port: u16, expected_name: Option<&str>) -> Option<u32> {
    let mut sys = System::new();
    sys.refresh_all();

    let mut target_names: Vec<&str> = vec!["graphdb-cli", "rest_api_server", "storage_daemon_server"];
    if let Some(name) = expected_name {
        target_names.push(name); // Add the specific name to the list
    }

    // Try lsof first on Unix-like systems for quick lookup
    #[cfg(target_family = "unix")]
    {
        let output = tokio::process::Command::new("lsof")
            .arg("-ti")
            .arg(port.to_string())
            .output()
            .await;

        if let Ok(output) = output {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for pid_str in stdout.lines() {
                    if let Ok(pid) = pid_str.trim().parse::<u32>() {
                        sys.refresh_all(); // Refresh processes data
                        if let Some(process) = sys.process(Pid::from_u32(pid)) {
                            let process_name = process.name().to_string_lossy();
                            if target_names.iter().any(|&n| process_name.contains(n)) {
                                debug!("lsof found PID {} for {} on port {}", pid, process_name, port);
                                return Some(pid);
                            }
                        }
                    }
                }
            }
        } else {
            debug!("lsof command failed or not found, falling back to sysinfo scan.");
        }
    }

    // Fallback for non-Unix systems or if lsof fails/doesn't identify correctly
    debug!("Scanning processes with sysinfo for daemon on port {}", port);
    for (pid, process) in sys.processes() {
        let process_name = process.name().to_string_lossy();
        if target_names.iter().any(|&n| process_name.contains(n)) {
            // Further check by trying to connect to the port
            // This is a more reliable way to confirm if *this* process is listening on the port
            if TcpStream::connect(format!("127.0.0.1:{}", port)).await.is_ok() {
                debug!("sysinfo confirmed PID {} for {} listening on port {}", pid.as_u32(), process_name, port);
                return Some(pid.as_u32());
            }
        }
    }
    None
}

/// Helper function to find a running storage daemon's port.
/// Scans a range of common ports using `lsof` (Unix) or attempts TCP connect.
pub async fn find_running_storage_daemon_port() -> Option<u16> {
    let common_storage_ports_to_check = (CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS..=CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS + 10).collect::<Vec<u16>>();
    for port in common_storage_ports_to_check {
        // First, check if a PID file exists for this port's storage daemon
        if let Ok(pid) = read_pid_file(port, "storage").await {
            if is_process_running(pid).await {
                info!("Storage daemon found via PID file on port {} (PID: {})", port, pid);
                return Some(port);
            } else {
                let _ = delete_pid_file(port, "storage").await; // Clean up stale PID file
            }
        }

        // Fallback to lsof (Unix) or TCP connect (cross-platform)
        #[cfg(target_family = "unix")]
        {
            let output = tokio::process::Command::new("lsof")
                .arg("-i")
                .arg(format!(":{}", port))
                .arg("-t")
                .output()
                .await;

            if let Ok(output) = output {
                let pids = String::from_utf8_lossy(&output.stdout);
                if !pids.trim().is_empty() {
                    // It found a process listening, now verify if it's our storage daemon
                    if let Some(pid) = pids.lines().next().and_then(|s| s.trim().parse::<u32>().ok()) {
                        let mut sys = System::new();
                        sys.refresh_all();
                        if let Some(process) = sys.process(Pid::from_u32(pid)) {
                            if process.name().to_string_lossy().contains("storage_daemon_server") {
                                info!("Storage daemon found via lsof on port {} (PID: {})", port, pid);
                                return Some(port);
                            }
                        }
                    }
                }
            }
        }

        // Cross-platform TCP connect check
        if TcpStream::connect(format!("127.0.0.1:{}", port)).await.is_ok() {
            // Found something listening. Now try to confirm it's our storage daemon
            // by checking process names via sysinfo. This is less direct than PID file/lsof for ownership.
            if let Some(pid) = find_daemon_process_on_port(port, Some("storage_daemon_server")).await {
                info!("Storage daemon found via TCP connect and sysinfo on port {} (PID: {})", port, pid);
                return Some(port);
            }
        }
    }
    None
}

/// Calls the `daemon_api::stop_daemon` function.
/// This is a general API call, might not stop specific daemons.
pub fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    stop_daemon().map_err(|e| anyhow::anyhow!("Daemon API error: {:?}", e))
}

/// Attempts to clear all daemon processes by sending a global stop signal
/// and then trying to kill processes on common daemon ports and by generic names.
pub async fn clear_all_daemon_processes() -> Result<(), anyhow::Error> {
    println!("Attempting to clear all GraphDB daemon processes...");

    let stop_result = stop_daemon_api_call();
    match stop_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }

    let common_daemon_ports = [
        get_default_rest_port_from_config(),
        get_default_storage_port_from_config_or_cli_default(),
        8000, 8080, 8081, 9001, 9002, 9003, 9004, 9005 // Other potential default ports
    ];

    let mut stopped_pids = std::collections::HashSet::new();

    // Iterate through common ports and try to stop services
    for &port in &common_daemon_ports {
        // Try stopping main, rest, storage by PID file first
        for service_type in ["main", "rest", "storage"] {
            if let Ok(pid) = read_pid_file(port, service_type).await {
                if !stopped_pids.contains(&pid) {
                    if let Ok(_) = stop_daemon_by_pid_or_scan("specific daemon", port, service_type).await {
                        stopped_pids.insert(pid);
                    }
                }
            }
        }

        // Then, try to find and kill any remaining processes listening on these ports
        let output_result = tokio::process::Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output()
            .await;

        if let Ok(output) = output_result {
            if output.status.success() { // Check if lsof command itself succeeded
                let pids_str = String::from_utf8_lossy(&output.stdout);
                for pid_str in pids_str.trim().lines() {
                    if let Ok(pid) = pid_str.parse::<u32>() {
                        if !stopped_pids.contains(&pid) {
                            println!("Killing remaining process {} on port {}...", pid, port);
                            let mut sys = System::new();
                            sys.refresh_all();
                            if let Some(process) = sys.process(Pid::from_u32(pid)) {
                                // Try graceful then forceful
                                #[cfg(target_family = "unix")]
                                {
                                    if let Err(e) = send_unix_signal(pid, Signal::SIGTERM).await {
                                        warn!("Failed to send SIGTERM to PID {}: {}", pid, e);
                                    }
                                    time::sleep(Duration::from_millis(500)).await;
                                    if is_process_running(pid).await {
                                        if let Err(e) = send_unix_signal(pid, Signal::SIGKILL).await {
                                            warn!("Failed to send SIGKILL to PID {}: {}", pid, e);
                                        }
                                    }
                                }
                                #[cfg(target_family = "windows")]
                                {
                                    if let Err(e) = terminate_windows_process(pid, false).await {
                                        warn!("Non-forceful termination failed for PID {}: {}. Trying forceful.", pid, e);
                                        if let Err(e) = terminate_windows_process(pid, true).await {
                                            warn!("Forceful termination failed for PID {}: {}", pid, e);
                                        }
                                    }
                                }
                                stopped_pids.insert(pid);
                            }
                        }
                    }
                }
            } else {
                debug!("lsof command failed for port {}: {:?}", port, output.status);
            }
        } else {
            // Fallback for Windows or if lsof isn't available/fails
            debug!("lsof command not found or failed for port {}. Skipping lsof check for this port.", port);
        }
    }

    // Finally, iterate through generic process names and kill any remaining processes
    let generic_process_names = ["graphdb-cli", "rest_api_server", "storage_daemon_server"];
    for name in &generic_process_names {
        #[cfg(target_family = "unix")]
        {
            let pgrep_spawn_result = tokio::process::Command::new("pgrep")
                .arg("-f")
                .arg(name)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn();

            if let Ok(mut child) = pgrep_spawn_result {
                let pgrep_output_result = child.wait_with_output().await;
                if let Ok(output) = pgrep_output_result {
                    let pids_str = String::from_utf8_lossy(&output.stdout);
                    for pid_str in pids_str.lines() {
                        if let Ok(pid) = pid_str.trim().parse::<u32>() {
                            if !stopped_pids.contains(&pid) {
                                println!("Killing process {} ({})...", pid, name);
                                let mut sys = System::new();
                                sys.refresh_all();
                                if let Some(process) = sys.process(Pid::from_u32(pid)) {
                                    if let Err(e) = send_unix_signal(pid, Signal::SIGTERM).await {
                                        warn!("Failed to send SIGTERM to {} (PID {}): {}", name, pid, e);
                                    }
                                    time::sleep(Duration::from_millis(500)).await;
                                    if is_process_running(pid).await {
                                        if let Err(e) = send_unix_signal(pid, Signal::SIGKILL).await {
                                            warn!("Failed to send SIGKILL to {} (PID {}): {}", name, pid, e);
                                        }
                                    }
                                    stopped_pids.insert(pid);
                                }
                            }
                        }
                    }
                } else {
                    eprintln!("Error getting pgrep output for '{}': {:?}", name, pgrep_output_result.unwrap_err());
                }
            } else {
                eprintln!("Error spawning pgrep for '{}': {:?}", name, pgrep_spawn_result.unwrap_err());
            }
        }
        #[cfg(not(target_family = "unix"))]
        {
            // For Windows, sysinfo is a better cross-platform way to find processes by name
            let mut sys = System::new_all();
            for (pid, process) in sys.processes() {
                if process.name().to_string_lossy().contains(name) {
                    if !stopped_pids.contains(&pid.as_u32()) {
                        println!("Killing process {} ({})...", pid.as_u32(), name);
                        if let Err(e) = terminate_windows_process(pid.as_u32(), false).await {
                            warn!("Non-forceful termination failed for {} (PID {}): {}. Trying forceful.", name, pid.as_u32(), e);
                            if let Err(e) = terminate_windows_process(pid.as_u32(), true).await {
                                warn!("Forceful termination failed for {} (PID {}): {}", name, pid.as_u32(), e);
                            }
                        }
                        stopped_pids.insert(pid.as_u32());
                    }
                }
            }
        }
    }

    Ok(())
}

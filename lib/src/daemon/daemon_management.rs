use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use std::path::{PathBuf, Path};
use std::process;
use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};
use std::sync::{Arc, LazyLock};
use std::collections::{HashSet, HashMap};
use std::fmt;
use serde_yaml2 as serde_yaml;
use regex::Regex;
use sysinfo::{System, Pid, Process, ProcessStatus, ProcessesToUpdate, RefreshKind, ProcessRefreshKind};
use tokio::task::JoinHandle;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::net::TcpStream;
use tokio::process::{Command as TokioCommand, Child};
use tokio::time::{self, Duration as TokioDuration, sleep, timeout};
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader, ErrorKind};
use log::{info, error, warn, debug};
use chrono::Utc;
use nix::unistd::Pid as NixPid;
use nix::sys::signal::{self, kill, Signal};
use futures::future;
use std::process::Stdio;
use tokio::process::Command;
use std::os::unix::process::ExitStatusExt;
use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileTypeExt;
use tokio::fs as tokio_fs;
use rocksdb::{DBCompactionStyle, Options, WriteBatch, DB};
use zmq::{ self, REQ, Context as ZmqContext, Message, SocketType};
use serde_json::{json, Value};
use models::errors::{GraphError, GraphResult};
use crate::config::{
    get_default_rest_port_from_config,
    load_storage_config_str as load_storage_config,
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
    StorageConfig,
    DEFAULT_DAEMON_PORT,
    DEFAULT_REST_API_PORT,
    DEFAULT_STORAGE_PORT,
    MAX_CLUSTER_SIZE,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
    EXECUTABLE_NAME,
    DAEMON_REGISTRY_DB_PATH,
    load_main_daemon_config,
    load_rest_config,
    StorageEngineType,
    daemon_api_storage_engine_type_to_string,
    RocksDBStorage, TikvStorage, SledStorage, SledClient
};
use crate::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::daemon_api::{start_daemon, stop_daemon, stop_port_daemon, DaemonError};
use crate::daemon::storage_daemon_server::run_storage_daemon as start_storage_server;
/// Helper to run an external command with a timeout.
pub async fn run_command_with_timeout(
    command_name: &str,
    args: &[&str],
    timeout_duration: Duration,
) -> Result<std::process::Output, anyhow::Error> {
    let mut command = TokioCommand::new(command_name);
    command.args(args);
    
    tokio::time::timeout(timeout_duration, command.output())
        .await
        .map_err(|_| anyhow!("Command '{} {}' timed out after {:?}", command_name, args.join(" "), timeout_duration))?
        .context(format!("Failed to run command '{} {}'", command_name, args.join(" ")))
}

/// Helper to check if a process is running by PID.
pub async fn check_process_status_by_pid(pid: Pid) -> bool {
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).is_some()
}

// Helper to get process ID by port provided
pub async fn get_pid_for_port(port: u16) -> Result<u32, anyhow::Error> {
    let output = Command::new("lsof")
        .arg("-i")
        .arg("-P")
        .arg(format!(":{}", port))
        .output()
        .await
        .context("Failed to execute lsof")?;  

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines().skip(1) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        debug!("lsof output for port {}: {:?}", port, line);
        if parts.len() > 1 {
            if let Ok(pid) = parts[1].parse::<u32>() {
                return Ok(pid);
            }
        }
    }

    Err(anyhow!("No process found listening on port {}", port))
}

// Helper function to check if a string is a valid cluster range (e.g., "8081-8084")
pub fn is_valid_cluster_range(range: &str) -> bool {
    range.contains('-') && range.split('-').all(|s| s.parse::<u16>().is_ok())
}

/// Checks if a process is running on the specified port.
pub async fn check_process_status_by_port(
    process_name: &str,
    port: u16,
) -> bool {
    // Early return if port is not listening to avoid unnecessary registry checks
    if !is_port_listening(port).await {
        debug!("Port {} is not listening for {}.", port, process_name);
        return false;
    }
    let metadata = match tokio::time::timeout(
        Duration::from_secs(2),
        GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port),
    ).await {
        Ok(Ok(Some(meta))) => Some(meta),
        Ok(Ok(None)) => None,
        Ok(Err(e)) => {
            error!("Failed to get daemon metadata for port {}: {}", port, e);
            None
        }
        Err(_) => {
            error!("Timeout accessing daemon metadata for port {}", port);
            None
        }
    };
    if let Some(meta) = metadata {
        if check_process_status_by_pid(Pid::from_u32(meta.pid)).await {
            let updated_metadata = DaemonMetadata {
                last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                ..meta
            };
            if let Err(e) = tokio::time::timeout(
                Duration::from_secs(2),
                GLOBAL_DAEMON_REGISTRY.register_daemon(updated_metadata),
            ).await {
                error!("Failed to update daemon metadata for port {}: {:?}", port, e);
            }
            return true;
        }
        // Unregister stale entry
        if let Err(e) = tokio::time::timeout(
            Duration::from_secs(2),
            GLOBAL_DAEMON_REGISTRY.unregister_daemon(port),
        ).await {
            error!("Failed to unregister daemon for port {}: {:?}", port, e);
        }
    }
    // Fallback to system scan if no registry entry
    match find_pid_by_port(port).await {
        Some(pid) => {
            let service_type = if process_name.contains("Storage") {
                "storage"
            } else if process_name.contains("REST") {
                "rest"
            } else {
                "main"
            };
            let metadata = DaemonMetadata {
                service_type: service_type.to_string(),
                port,
                pid, // pid is already u32
                ip_address: "127.0.0.1".to_string(),
                data_dir: None,
                config_path: None,
                engine_type: None,
                last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            };
            if let Err(e) = tokio::time::timeout(
                Duration::from_secs(2),
                GLOBAL_DAEMON_REGISTRY.register_daemon(metadata),
            ).await {
                error!("Failed to register daemon for port {}: {:?}", port, e);
            }
            true
        }
        None => false,
    }
}

/// Stops a process running on the specified port.
/// This version is robust against race conditions where the process
/// is killed but a subsequent check fails, and avoids thread joins.
pub async fn stop_process_by_port(
    process_name: &str,
    port: u16
) -> Result<(), anyhow::Error> {
    println!("Attempting to stop {} on port {}...", process_name, port);
    info!("Attempting to stop {} on port {}", process_name, port);

    // Determine service type
    let service_type_str = if process_name.contains("Storage") { "storage" }
        else if process_name.contains("REST") { "rest" }
        else { "main" };
    let service_type = ServiceType::from_str(service_type_str, true)
        .map_err(|e| anyhow!("Invalid service type: {}", e))?;

    // Check daemon registry for metadata
    let metadata = match timeout(
        Duration::from_secs(2),
        GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port),
    ).await {
        Ok(Ok(Some(meta))) => Some(meta),
        Ok(Ok(None)) => None,
        Ok(Err(e)) => {
            error!("Failed to get daemon metadata for port {}: {}", port, e);
            None
        }
        Err(_) => {
            error!("Timeout accessing daemon metadata for port {}", port);
            None
        }
    };

    // Get PID from metadata or find_pid_by_port
    let pid_to_stop = match metadata.map(|meta| meta.pid) {
        Some(pid) => Some(pid),
        None => find_pid_by_port(port).await, // This is already async, no closure needed
    };

    // Skip termination for TiKV PD port (2379) unless explicitly required
    if port == 2379 && !process_name.contains("TiKV") {
        info!("Skipping termination for TiKV PD port {} (process: {})", port, process_name);
        println!("Skipping termination for TiKV PD port {} (process: {}).", port, process_name);
        return Ok(());
    }

    if let Some(pid) = pid_to_stop {
        let nix_pid = NixPid::from_raw(pid as i32);
        info!("Found PID {} for {} on port {}", pid, process_name, port);

        // Send SIGTERM
        match signal::kill(nix_pid, Signal::SIGTERM) {
            Ok(_) => {
                println!("Sent SIGTERM to PID {} for {} on port {}.", pid, process_name, port);
                info!("Sent SIGTERM to PID {} for {} on port {}", pid, process_name, port);
            }
            Err(nix::Error::ESRCH) => {
                info!("Process with PID {} on port {} already terminated", pid, port);
                println!("Process with PID {} on port {} already terminated.", pid, port);
                // Proceed to cleanup
            }
            Err(e) => {
                error!("Failed to send SIGTERM to PID {}: {}", pid, e);
                return Err(anyhow!("Failed to send SIGTERM to PID {}: {}", pid, e));
            }
        }

        // Poll for port to become free
        let wait_timeout = Duration::from_secs(5);
        let poll_interval = Duration::from_millis(200);
        let start_time = Instant::now();
        while start_time.elapsed() < wait_timeout {
            if is_port_free(port).await {
                info!("Port {} is now free after SIGTERM for PID {}", port, pid);
                println!("Port {} is now free.", port);
                break;
            }
            debug!("Port {} still in use for PID {} after {}ms", port, pid, start_time.elapsed().as_millis());
            sleep(poll_interval).await;
        }

        // If port is still in use, send SIGKILL
        if !is_port_free(port).await {
            warn!("Process {} (PID {}) on port {} still running after SIGTERM. Sending SIGKILL.", process_name, pid, port);
            match signal::kill(nix_pid, Signal::SIGKILL) {
                Ok(_) => {
                    info!("Sent SIGKILL to PID {} for {} on port {}", pid, process_name, port);
                    // Poll again after SIGKILL
                    let sigkill_timeout = Duration::from_secs(2);
                    let sigkill_start = Instant::now();
                    while sigkill_start.elapsed() < sigkill_timeout {
                        if is_port_free(port).await {
                            info!("Port {} is now free after SIGKILL for PID {}", port, pid);
                            println!("Port {} is now free.", port);
                            break;
                        }
                        debug!("Port {} still in use after SIGKILL for PID {} after {}ms", port, pid, sigkill_start.elapsed().as_millis());
                        sleep(poll_interval).await;
                    }
                }
                Err(nix::Error::ESRCH) => {
                    info!("Process with PID {} on port {} already terminated during SIGKILL attempt", pid, port);
                    println!("Process with PID {} on port {} already terminated.", pid, port);
                }
                Err(e) => {
                    error!("Failed to send SIGKILL to PID {}: {}", pid, e);
                    return Err(anyhow!("Failed to send SIGKILL to PID {}: {}", pid, e));
                }
            }
        }

        // Final check
        if !is_port_free(port).await {
            return Err(anyhow!("Failed to stop {} (PID {}) on port {} after SIGTERM and SIGKILL.", process_name, pid, port));
        }
    } else {
        info!("No process found for {} on port {}.", process_name, port);
        println!("No process found for {} on port {}.", process_name, port);
    }

    // Unregister daemon and remove PID file
    match timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.unregister_daemon(port)).await {
        Ok(Ok(_)) => info!("Successfully unregistered daemon for port {}", port),
        Ok(Err(e)) => error!("Failed to unregister daemon for port {}: {}", port, e),
        Err(_) => error!("Timeout unregistering daemon for port {}", port),
    };

    if let Err(e) = remove_pid_file(port, &service_type).await {
        warn!("Failed to remove PID file for port {}: {}", port, e);
    }

    if is_port_free(port).await {
        info!("{} on port {} stopped successfully.", process_name, port);
        println!("{} on port {} stopped.", process_name, port);
        Ok(())
    } else {
        error!("Port {} remained in use after termination attempts.", port);
        Err(anyhow!("Port {} remained in use after termination attempts.", port))
    }
}

/// Signal-based reload for a daemon process.
pub async fn reload_daemon_process(
    process_name: &str,
    port: u16,
) -> Result<(), anyhow::Error> {
    let metadata = match tokio::time::timeout(
        Duration::from_secs(2),
        GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port),
    ).await {
        Ok(Ok(Some(meta))) => Some(meta),
        Ok(Ok(None)) => None,
        Ok(Err(e)) => {
            error!("Failed to get daemon metadata for port {}: {}", port, e);
            None
        }
        Err(_) => {
            error!("Timeout accessing daemon metadata for port {}", port);
            None
        }
    };

    if let Some(meta) = metadata {
        if let Err(e) = kill(NixPid::from_raw(meta.pid as i32), Signal::SIGHUP) {
            return Err(anyhow!("Failed to send SIGHUP to PID {} for {} on port {}: {}", meta.pid, process_name, port, e));
        }
        println!("Sent SIGHUP to PID {} for {} on port {}.", meta.pid, process_name, port);
        
        let updated_metadata = DaemonMetadata {
            last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            ..meta
        };
        if let Err(e) = tokio::time::timeout(
            Duration::from_secs(2),
            GLOBAL_DAEMON_REGISTRY.register_daemon(updated_metadata),
        ).await {
            error!("Failed to update daemon metadata for port {}: {:?}", port, e);
        }
        Ok(())
    } else {
        Err(anyhow!("No {} process found in registry for port {}.", process_name, port))
    }
}

/// Cleans up stale entries in the daemon registry by checking if their PIDs are still running.
pub async fn cleanup_daemon_registry_stale_entries() -> Result<(), anyhow::Error> {
    let all_metadata = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await?;
    let mut cleaned_up_count = 0;

    for metadata in all_metadata {
        if !is_pid_running(metadata.pid).await {
            warn!("Found stale registry entry for PID {} on port {}. Unregistering.", metadata.pid, metadata.port);
            if let Err(e) = tokio::time::timeout(
                Duration::from_secs(2),
                GLOBAL_DAEMON_REGISTRY.unregister_daemon(metadata.port),
            ).await {
                error!("Failed to unregister daemon for port {}: {:?}", metadata.port, e);
            } else {
                cleaned_up_count += 1;
            }
        }
    }

    if cleaned_up_count > 0 {
        info!("Successfully cleaned up {} stale daemon registry entries.", cleaned_up_count);
    } else {
        debug!("No stale daemon registry entries found.");
    }

    Ok(())
}

/// Finds all ports for a given service type.
pub async fn find_all_ports_with_service_type(service_type: &str) -> Vec<u16> {
    let all_daemons = match tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata()).await {
        Ok(Ok(daemons)) => daemons,
        Ok(Err(e)) => {
            error!("Failed to get all daemon metadata from registry: {}", e);
            return Vec::new();
        }
        Err(_) => {
            error!("Timeout accessing daemon metadata");
            return Vec::new();
        }
    };

    let tasks: Vec<_> = all_daemons
        .into_iter()
        .filter(|d| d.service_type == service_type)
        .map(|d| {
            tokio::spawn(async move {
                if is_process_running(d.pid).await && is_port_listening(d.port).await {
                    Some(d.port)
                } else {
                    None
                }
            })
        })
        .collect();

    let mut ports = future::join_all(tasks)
        .await
        .into_iter()
        .filter_map(|res| res.unwrap_or(None))
        .collect::<Vec<u16>>();
    ports.sort_unstable();
    ports
}

/// Lists and reports the status of all registered daemons.
pub async fn list_and_report_running_daemons() -> Result<(), anyhow::Error> {
    let mut running_daemons = match tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata()).await {
        Ok(Ok(daemons)) => daemons,
        Ok(Err(e)) => {
            warn!("Failed to get daemon metadata: {}", e);
            Vec::new()
        }
        Err(_) => {
            warn!("Timeout accessing daemon metadata");
            Vec::new()
        }
    };

    if running_daemons.is_empty() {
        let scanned_daemons = get_all_daemon_processes_with_ports().await
            .context("Failed to fetch daemon processes with ports")?;
        for (port, (pid, service_type)) in scanned_daemons {
            let metadata = DaemonMetadata {
                service_type,
                port,
                pid,
                ip_address: "127.0.0.1".to_string(),
                data_dir: None,
                config_path: None,
                engine_type: None,
                last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            };
            if let Err(e) = tokio::time::timeout(
                Duration::from_secs(2),
                GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()),
            ).await {
                error!("Failed to register daemon for port {}: {:?}", port, e);
            }
            running_daemons.push(metadata);
        }
    }

    if running_daemons.is_empty() {
        println!("No daemons are currently registered.");
        return Ok(());
    }

    println!("Registered Daemons:");
    println!("{:<15} {:<10} {:<10} {:<20}", "Service Type", "Port", "PID", "Status");

    let tasks: Vec<_> = running_daemons.into_iter().map(|daemon| {
        tokio::spawn(async move {
            let status = check_process_status_by_pid(Pid::from_u32(daemon.pid)).await;
            (daemon, status)
        })
    }).collect();

    let results = future::join_all(tasks).await;
    for res in results {
        let (daemon, status) = res.unwrap_or((DaemonMetadata {
            service_type: "unknown".to_string(),
            port: 0,
            pid: 0,
            ip_address: "".to_string(),
            data_dir: None,
            config_path: None,
            engine_type: None,
            last_seen_nanos: 0,
        }, false));
        let status_str = if status { "Running" } else { "Stopped" };
        println!("{:<15} {:<10} {:<10} {:<20}", daemon.service_type, daemon.port, daemon.pid, status_str);
    }
    Ok(())
}

pub async fn start_graphdb_daemon_core(port: u16) -> Result<(), anyhow::Error> {
    info!("[DAEMON PROCESS] Attempting to start ACTUAL GraphDB core daemon on port {}...", port);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await
        .with_context(|| format!("Failed to bind GraphDB Daemon to port {}", port))?;
    info!("[DAEMON PROCESS] GraphDB Daemon listener active on port {}", port);

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("[DAEMON PROCESS] GraphDB Daemon received Ctrl+C, exiting.");
            Ok(())
        }
        result = async {
            loop {
                match listener.accept().await {
                    Ok((_socket, _addr)) => {
                        // In a real daemon, you'd handle the connection here
                    },
                    Err(e) => {
                        error!("[DAEMON PROCESS] GraphDB Daemon listener error: {:?}", e);
                        return Err(e.into());
                    }
                }
            }
        } => {
            info!("[DAEMON PROCESS] GraphDB Daemon listener stopped.");
            result
        }
    }
}

/// Calls the `daemon_api::stop_daemon` function.
pub async fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    stop_daemon().await.map_err(|e| anyhow!("Daemon stop failed: {}", e))
}

/// A wrapper to stop a specific daemon instance using `stop_port_daemon` from daemon_api.
pub async fn stop_port_daemon_call(port: u16, daemon_type: &str) -> Result<(), anyhow::Error> {
    stop_port_daemon(port, daemon_type).await.map_err(|e| anyhow::anyhow!("Daemon stop failed: {}", e))
}

/// Helper function to find a running storage daemon's port.
pub async fn find_running_storage_daemon_port() -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    let mut running_ports: Vec<u16> = all_daemons
        .into_iter()
        .filter(|(_port, (_pid, service_type))| service_type == "storage")
        .map(|(port, _)| port)
        .collect();
    running_ports.sort_unstable();
    running_ports
}

/// Clears all running daemon processes.
pub async fn clear_all_daemon_processes() -> Result<(), anyhow::Error> {
    println!("Attempting to clear all GraphDB daemon processes...");

    // Call stop_daemon_api_call asynchronously
    if let Err(e) = stop_daemon_api_call().await {
        error!("Failed to send global stop signal: {:?}", e);
    } else {
        println!("Global daemon stop signal sent successfully.");
    }

    let registered_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await?;
    let mut pids_to_kill: HashMap<u16, (u32, String)> = HashMap::new();
    let mut ports_to_unregister: Vec<u16> = Vec::new();

    for metadata in registered_daemons {
        let is_running = is_process_running(metadata.pid).await;
        let age = Utc::now().signed_duration_since(chrono::DateTime::<Utc>::from_timestamp_nanos(metadata.last_seen_nanos));
        if !is_running && age > chrono::Duration::seconds(300) {
            warn!("Removing stale registry entry for {} daemon on port {} (PID {}).", metadata.service_type, metadata.port, metadata.pid);
            ports_to_unregister.push(metadata.port);
        } else if is_running {
            pids_to_kill.insert(metadata.port, (metadata.pid, metadata.service_type.clone()));
        }
    }

    // Unregister stale daemons after collecting them, to avoid holding a lock across an await
    for port in ports_to_unregister {
        if let Err(e) = tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.unregister_daemon(port)).await {
            error!("Failed to unregister daemon for port {}: {:?}", port, e);
        }
    }

    let output = run_command_with_timeout(
        "lsof",
        &["-i", "TCP", "-s", "TCP:LISTEN", "-a", "-c", "graphdb"],
        Duration::from_secs(3),
    ).await.unwrap_or_else(|e| {
        warn!("Failed to run lsof command for process discovery: {}", e);
        std::process::Output {
            status: std::process::ExitStatus::from_raw(1),
            stdout: Vec::new(),
            stderr: format!("lsof error: {}", e).into_bytes(),
        }
    });

    let stdout = String::from_utf8_lossy(&output.stdout);
    let re = Regex::new(r":(\d+)\s+\(LISTEN\)").expect("Failed to create regex for port parsing");
    
    for line in stdout.lines() {
        if let Some(captures) = re.captures(line) {
            if let Some(port_match) = captures.get(1) {
                if let Ok(port) = port_match.as_str().parse::<u16>() {
                    if let Some(pid) = find_pid_by_port(port).await {
                        if !pids_to_kill.contains_key(&port) {
                            let service_type = if is_storage_daemon_running(port).await {
                                "storage".to_string()
                            } else if is_rest_api_running(port).await {
                                "rest".to_string()
                            } else {
                                "main".to_string()
                            };
                            pids_to_kill.insert(port, (pid, service_type));
                        }
                    }
                }
            }
        }
    }

    let mut errors = Vec::new();
    if !pids_to_kill.is_empty() {
        println!("Found {} daemon processes to terminate.", pids_to_kill.len());
        let tasks: Vec<_> = pids_to_kill.into_iter().map(|(port, (pid, service_type))| {
            println!("Attempting to stop {} process {} (port {})...", service_type, pid, port);
            tokio::spawn(async move {
                stop_process_by_port(&service_type, port).await.map_err(|e| (port, service_type, e))
            })
        }).collect();

        let results = future::join_all(tasks).await;
        for res in results {
            if let Ok(Err((port, service_type, e))) = res {
                errors.push(format!("Failed to stop {} on port {}: {}", service_type, port, e));
            }
        }
    } else {
        println!("No GraphDB daemon processes found to kill.");
    }

    if let Err(e) = GLOBAL_DAEMON_REGISTRY.clear_all_daemons().await {
        errors.push(format!("Failed to clear daemon registry: {}", e));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Errors occurred during daemon cleanup:\n{}", errors.join("\n")))
    }
}

/// Attempts to find a daemon process by checking its internal arguments and performing a TCP health check.
pub async fn find_daemon_process_on_port_with_args(
    port: u16,
    expected_name_for_logging: &str,
    expected_internal_arg: &str,
) -> Option<u32> {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);

    let cli_exe_name_full = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_owned()))
        .unwrap_or_else(|| "graphdb-cli".to_string());
    let cli_name_pattern = Regex::new(&format!(r"(?i){}|graphdb-c", regex::escape(&cli_exe_name_full)))
        .expect("Failed to create cli_name_pattern regex");

    debug!("Searching for process: name_hint='{}', internal_arg='{}', port={}",
           expected_name_for_logging, expected_internal_arg, port);

    for (pid, process) in sys.processes() {
        let process_name_lower = process.name().to_string_lossy().to_lowercase();
        let cmd_args: Vec<String> = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        let first_cmd_arg_lower = cmd_args.first().map_or("".to_string(), |s| s.to_lowercase());

        debug!("  - Checking PID {}: Name='{:?}', Cmd='{:?}'", pid, process.name(), cmd_args);

        let is_our_cli_process = cli_name_pattern.is_match(&process_name_lower) ||
                                 cli_name_pattern.is_match(&first_cmd_arg_lower);

        if is_our_cli_process {
            debug!("    -> PID {} is a potential graphdb-cli process (regex match).", pid);

            let has_expected_internal_arg = cmd_args.contains(&expected_internal_arg.to_string());
            debug!("    -> PID {} has_expected_internal_arg ('{}'): {}", pid, expected_internal_arg, has_expected_internal_arg);

            let mut matched_port = false;
            if let Some(pos) = cmd_args.iter().position(|arg| arg == "--internal-port") {
                if let Some(port_str) = cmd_args.get(pos + 1) {
                    if let Ok(p) = port_str.parse::<u16>() {
                        if p == port {
                            matched_port = true;
                        }
                    }
                }
            }
            debug!("    -> PID {} matched_port ({}): {}", pid, port, matched_port);

            if has_expected_internal_arg && matched_port {
                info!("    -> PID {} is a candidate for {} on port {}. Performing TCP check...", pid, expected_name_for_logging, port);
                if tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(format!("127.0.0.1:{}", port))).await.is_ok_and(|res| res.is_ok()) {
                    info!("    -> TCP check successful for PID {} (port {}). CONFIRMED.", pid.as_u32(), port);
                    return Some(pid.as_u32());
                } else {
                    warn!("    -> TCP check failed for PID {} (port {}). Process not fully listening yet or connection timed out.", pid.as_u32(), port);
                }
            }
        }
    }
    debug!("No process found matching criteria for {} on port {}", expected_name_for_logging, port);
    None
}

/// Helper to get the listening TCP port for a given PID using lsof.
pub async fn get_listening_port_for_pid(pid: u32) -> Option<u16> {
    let output_result = run_command_with_timeout(
        "lsof",
        &["-i", "-P", "-n", &format!("-a -p {}", pid), "-sTCP:LISTEN"],
        Duration::from_secs(1),
    ).await;

    if let Ok(output) = output_result {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let re = Regex::new(r":(\d+)\s+\(LISTEN\)").expect("Failed to create regex for port parsing");
        for line in stdout.lines() {
            if let Some(captures) = re.captures(line) {
                if let Some(port_match) = captures.get(1) {
                    if let Ok(port) = port_match.as_str().parse::<u16>() {
                        debug!("  -> Parsed listening port {} from lsof output: {}", port, line);
                        return Some(port);
                    }
                }
            }
            let re_ipv6 = Regex::new(r":\[[0-9a-fA-F:]+\]:(\d+)\s+\(LISTEN\)").expect("Failed to create regex for IPv6 port parsing");
            if let Some(captures) = re_ipv6.captures(line) {
                if let Some(port_match) = captures.get(1) {
                    if let Ok(port) = port_match.as_str().parse::<u16>() {
                        debug!("  -> Parsed listening port {} from lsof IPv6 output: {}", port, line);
                        return Some(port);
                    }
                }
            }
        }
    }
    debug!("No listening port found for PID {} from lsof output.", pid);
    None
}

/// Helper to get all running daemon processes (main, REST, storage) and their associated ports.
pub async fn get_all_daemon_processes_with_ports() -> Result<HashMap<u16, (u32, String)>, anyhow::Error> {
    let mut found_daemons: HashMap<u16, (u32, String)> = HashMap::new();
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);

    let cli_exe_name_full = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_owned()))
        .unwrap_or_else(|| "graphdb-cli".to_string());
    
    let graphdb_component_names = vec![
        cli_exe_name_full.clone(),
        String::from("graphdb-c"),
        String::from("rest_api_server"),
        String::from("storage_daemon_server"),
    ];

    let registered_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await?;

    for metadata in registered_daemons {
        let is_running = is_process_running(metadata.pid).await;
        let is_listening = is_port_listening(metadata.port).await;
        let age = Utc::now().signed_duration_since(chrono::DateTime::<Utc>::from_timestamp_nanos(metadata.last_seen_nanos));

        if is_running && is_listening {
            info!("Verified registered {} daemon (PID {}) on port {}.", metadata.service_type, metadata.pid, metadata.port);
            found_daemons.insert(metadata.port, (metadata.pid, metadata.service_type));
        } else if age > chrono::Duration::seconds(300) {
            warn!("Registered {} daemon (PID {}) on port {} is stale (running: {}, listening: {}). Unregistering.",
                  metadata.service_type, metadata.pid, metadata.port, is_running, is_listening);
            tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.unregister_daemon(metadata.port)).await??;
        }
    }

    let storage_cli_config = load_storage_config(None).await
        .unwrap_or_else(|e| {
            warn!("Could not load storage config for daemon discovery (secondary scan): {}. Using defaults.", e);
            StorageConfig::default()
        });
    
    let storage_cluster_ports = if storage_cli_config.cluster_range.is_empty() {
        Vec::new()
    } else {
        parse_cluster_range(&storage_cli_config.cluster_range).unwrap_or_else(|e| {
            warn!("Failed to parse storage cluster range '{}': {}. Ignoring.", storage_cli_config.cluster_range, e);
            Vec::new()
        })
    };

    // Fallback to ps and grep for additional process discovery
    let ps_output = run_command_with_timeout(
        "ps",
        &["-eo", "pid,cmd"],
        Duration::from_secs(3),
    ).await.unwrap_or_else(|e| {
        warn!("Failed to run ps command for process discovery: {}", e);
        std::process::Output {
            status: std::process::ExitStatus::from_raw(1),
            stdout: Vec::new(),
            stderr: format!("ps error: {}", e).into_bytes(),
        }
    });
    let ps_lines = String::from_utf8_lossy(&ps_output.stdout);
    let ps_regex = Regex::new(r"^\s*(\d+)\s+.*graphdb.*").expect("Failed to create ps regex");

    let mut ps_pids = Vec::new();
    for line in ps_lines.lines() {
        if let Some(captures) = ps_regex.captures(line) {
            if let Some(pid_str) = captures.get(1) {
                if let Ok(pid) = pid_str.as_str().parse::<u32>() {
                    ps_pids.push(pid);
                }
            }
        }
    }

    let tasks: Vec<_> = sys.processes().iter().filter_map(|(pid, process)| {
        let pid_u32 = pid.as_u32();
        let graphdb_component_names = graphdb_component_names.clone();
        let storage_cluster_ports = storage_cluster_ports.clone();
        if ps_pids.contains(&pid_u32) || graphdb_component_names.iter().any(|name| process.name().to_string_lossy().contains(name)) {
            let cmd_args: Vec<String> = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect();
            Some(tokio::spawn(async move {
                let l_port_opt = get_listening_port_for_pid(pid_u32).await;
                if let Some(l_port) = l_port_opt {
                    let full_cmd_line = cmd_args.join(" ");
                    let mut service_type_found: Option<&str> = None;
                    let mut internal_arg_port: Option<u16> = None;

                    if let Some(pos) = cmd_args.iter().position(|arg| arg == "--internal-port") {
                        if let Some(port_str) = cmd_args.get(pos + 1) {
                            if let Ok(p) = port_str.parse::<u16>() {
                                internal_arg_port = Some(p);
                            }
                        }
                    }

                    if full_cmd_line.contains("--internal-daemon-run") {
                        service_type_found = Some("main");
                    } else if full_cmd_line.contains("--internal-rest-api-run") {
                        service_type_found = Some("rest");
                    } else if full_cmd_line.contains("--internal-storage-daemon-run") {
                        service_type_found = Some("storage");
                    } else {
                        if graphdb_component_names.iter().any(|name| full_cmd_line.contains(name)) {
                            if full_cmd_line.contains("rest_api_server") {
                                service_type_found = Some("rest");
                            } else if full_cmd_line.contains("storage_daemon_server") {
                                service_type_found = Some("storage");
                            }
                            if service_type_found.is_none() {
                                if l_port == DEFAULT_DAEMON_PORT { service_type_found = Some("main"); }
                                else if l_port == DEFAULT_REST_API_PORT { service_type_found = Some("rest"); }
                                else if l_port == DEFAULT_STORAGE_PORT { service_type_found = Some("storage"); }
                                else if storage_cluster_ports.contains(&l_port) {
                                    service_type_found = Some("storage");
                                }
                            }
                        }
                    }

                    let final_service_type = service_type_found.unwrap_or("unknown");
                    let port_matches_arg_or_not_specified = internal_arg_port.map_or(true, |arg_p| arg_p == l_port);

                    if final_service_type != "unknown" && port_matches_arg_or_not_specified {
                        info!("Found {} daemon (PID {}) on port {} via secondary sysinfo/lsof/ps scan.", final_service_type, pid_u32, l_port);
                        let new_metadata = DaemonMetadata {
                            service_type: final_service_type.to_string(),
                            port: l_port,
                            pid: pid_u32,
                            ip_address: "127.0.0.1".to_string(),
                            data_dir: None,
                            config_path: None,
                            engine_type: None,
                            last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                        };
                        tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.register_daemon(new_metadata)).await.ok();
                        Some((l_port, (pid_u32, final_service_type.to_string())))
                    } else {
                        debug!("Secondary scan: Process PID {} listening on port {} not classified as known GraphDB daemon type ('{}') or port mismatch (arg: {:?}, actual: {}).", pid_u32, l_port, final_service_type, internal_arg_port, l_port);
                        None
                    }
                } else {
                    debug!("Process PID {} is not listening on any TCP port.", pid_u32);
                    None
                }
            }))
        } else {
            None
        }
    }).collect();

    let results = future::join_all(tasks).await;
    for res in results {
        if let Ok(Some((port, daemon_info))) = res {
            found_daemons.insert(port, daemon_info);
        }
    }

    Ok(found_daemons)
}

pub async fn find_all_running_rest_ports(
    _rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    _rest_cluster: Option<String>,
) -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    let mut running_ports: Vec<u16> = all_daemons
        .into_iter()
        .filter(|(_port, (_pid, service_type))| service_type == "rest")
        .map(|(port, _)| port)
        .collect();
    running_ports.sort_unstable();
    running_ports
}

pub async fn find_all_running_daemon_ports(
    cluster: Option<String>,
) -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    let cluster_ports = if let Some(cluster_range) = cluster {
        parse_cluster_range(&cluster_range).unwrap_or_default()
    } else {
        Vec::new()
    };

    let mut running_ports: Vec<u16> = all_daemons
        .into_iter()
        .filter(|(port, (_pid, service_type))| service_type == "main" && (cluster_ports.is_empty() || cluster_ports.contains(port)))
        .map(|(port, _)| port)
        .collect();

    running_ports.sort_unstable();
    running_ports
}

pub async fn find_all_running_storage_daemon_ports(cluster: Option<String>) -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    let cluster_ports = if let Some(cluster_range) = cluster {
        parse_cluster_range(&cluster_range).unwrap_or_default()
    } else {
        Vec::new()
    };

    let mut running_ports: Vec<u16> = all_daemons
        .into_iter()
        .filter(|(port, (_pid, service_type))| service_type == "storage" && (cluster_ports.is_empty() || cluster_ports.contains(port)))
        .map(|(port, _)| port)
        .collect();

    running_ports.sort_unstable();
    running_ports
}

pub fn launch_daemon_process(
    port: u16,
    _cluster: Option<String>,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<JoinHandle<()>, anyhow::Error> {
    let handle = tokio::spawn(async move {
        info!("GraphDB daemon spawned on port {}.", port);
        tokio::select! {
            _ = start_graphdb_daemon_core(port) => {
                info!("GraphDB daemon on port {} core exited.", port);
            },
            _ = shutdown_rx => {
                info!("GraphDB daemon on port {} received shutdown signal.", port);
            }
        }
    });
    Ok(handle)
}

pub async fn stop_managed_daemon(
    port: u16,
    daemon_handles: Arc<TokioMutex<std::collections::HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<bool, anyhow::Error> {
    let mut handles = daemon_handles.lock().await;
    if let Some((handle, tx)) = handles.remove(&port) {
        if tx.send(()).is_ok() {
            info!("Sent shutdown signal to daemon on port {}.", port);
            handle.await.map_err(|e| anyhow!(e.to_string()))?;
            Ok(true)
        } else {
            warn!("Daemon on port {} was already shutting down or stopped.", port);
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

pub async fn list_daemon_processes() -> Result<Vec<u32>, anyhow::Error> {
    let all_daemons = get_all_daemon_processes_with_ports().await?;
    Ok(all_daemons.values().map(|(pid, _)| *pid).collect())
}

pub async fn kill_daemon_process(pid: u32) -> Result<(), anyhow::Error> {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);
    if let Some(process) = sys.process(Pid::from_u32(pid)) {
        if process.kill() {
            info!("Killed process with PID {}", pid);
            Ok(())
        } else {
            Err(anyhow!("Failed to kill process with PID {}", pid))
        }
    } else {
        warn!("Process with PID {} not found, might have already exited.", pid);
        Ok(())
    }
}

pub async fn is_main_daemon_running(port: u16) -> bool {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    all_daemons.get(&port).map_or(false, |(_pid, service_type)| service_type == "main")
}

pub async fn is_rest_api_running(port: u16) -> bool {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    all_daemons.get(&port).map_or(false, |(_pid, service_type)| service_type == "rest")
}


pub async fn get_running_storage_daemons() -> GraphResult<Vec<u16>> {
    let all_daemons = get_all_daemon_processes_with_ports()
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to get daemon processes: {}", e)))?;

    let mut active_ports = Vec::new();

    for (port, (pid, service_type)) in all_daemons {
        if service_type != "storage" {
            debug!("Skipping non-storage daemon on port {} with service_type {}", port, service_type);
            continue;
        }

        // Verify process existence
        let mut system = System::new_with_specifics(
            RefreshKind::nothing().with_processes(ProcessRefreshKind::everything())
        );
        if system.process(Pid::from(pid as usize)).is_none() {
            warn!("Daemon on port {} with PID {} is registered but not running", port, pid);
            if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", port).await {
                warn!("Failed to remove stale daemon entry for port {}: {}", port, e);
            }
            continue;
        }

        // Check daemon responsiveness
        match timeout(TokioDuration::from_millis(1000), ping_daemon(port)).await {
            Ok(Ok(response)) if response["status"] == "success" => {
                info!("Storage daemon on port {} (PID {}) is running and responsive", port, pid);
                active_ports.push(port);
                if let Ok(Some(mut metadata)) = GLOBAL_DAEMON_REGISTRY.find_daemon_by_port(port).await {
                    metadata.last_seen_nanos = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64;
                    if let Err(e) = GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await {
                        warn!("Failed to update registry for port {}: {}", port, e);
                    }
                }
            }
            Ok(Ok(response)) => {
                warn!("Storage daemon on port {} (PID {}) responded with invalid status: {:?}", port, pid, response);
                if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", port).await {
                    warn!("Failed to remove stale daemon entry for port {}: {}", port, e);
                }
            }
            Ok(Err(e)) => {
                warn!("Failed to ping storage daemon on port {} (PID {}): {}", port, pid, e);
                if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", port).await {
                    warn!("Failed to remove stale daemon entry for port {}: {}", port, e);
                }
            }
            Err(_) => {
                warn!("Timeout pinging storage daemon on port {} (PID {})", port, pid);
                if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", port).await {
                    warn!("Failed to remove stale daemon entry for port {}: {}", port, e);
                }
            }
        }
    }

    Ok(active_ports)
}

async fn ping_daemon(port: u16) -> GraphResult<Value> {
    let context = ZmqContext::new();
    let socket = context.socket(zmq::REQ)
        .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;

    socket.set_rcvtimeo(1000)
        .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
    socket.set_sndtimeo(1000)
        .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;
    socket.set_connect_timeout(1000)
        .map_err(|e| GraphError::StorageError(format!("Failed to set connect timeout: {}", e)))?;

    let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
    debug!("Attempting to connect to ZeroMQ endpoint {} for port {}", endpoint, port);
    socket.connect(&endpoint)
        .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", endpoint, e)))?;

    let request = json!({ "command": "ping" });
    socket.send(serde_json::to_vec(&request)?, 0)
        .map_err(|e| GraphError::StorageError(format!("Failed to send ping request to port {}: {}", port, e)))?;

    let reply = socket.recv_bytes(0)
        .map_err(|e| GraphError::StorageError(format!("Failed to receive ping response from port {}: {}", port, e)))?;

    serde_json::from_slice(&reply)
        .map_err(|e| GraphError::StorageError(format!("Failed to parse ping response from port {}: {}", port, e)))
}

pub async fn is_storage_daemon_running(port: u16) -> bool {
    // Check if a daemon is registered for this port.
    let daemon_metadata = match GLOBAL_DAEMON_REGISTRY.find_daemon_by_port(port).await {
        Ok(Some(metadata)) if metadata.service_type == "storage" => {
            debug!("Found daemon in registry for port {}: {:?}", port, metadata);
            Some(metadata)
        }
        Ok(Some(metadata)) => {
            debug!("Found daemon on port {}, but service_type is {} (not storage), removing entry", port, metadata.service_type);
            if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", port).await {
                warn!("Failed to remove stale daemon entry for port {}: {}", port, e);
            }
            None
        }
        Ok(None) => {
            debug!("No daemon found in registry for port {}", port);
            None
        }
        Err(e) => {
            warn!("Failed to query registry for port {}: {}", port, e);
            None
        }
    };

    let Some(metadata) = daemon_metadata else {
        // If no entry in registry, check for any process listening on the port.
        if let Some(pid) = find_pid_by_port(port).await {
            info!("Found process with PID {} listening on port {} but no registry entry. Assuming it's a valid daemon.", pid, port);
            return true;
        }
        debug!("No daemon found for port {} in registry or port scan", port);
        return false;
    };

    // If a registry entry exists, verify the process is still running.
    let pid = Pid::from(metadata.pid as usize);
    let mut system = System::new();
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
    if system.process(pid).is_none() {
        warn!("Daemon on port {} with PID {} is registered but not running, removing stale entry", port, metadata.pid);
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", port).await {
            warn!("Failed to remove stale daemon entry for port {}: {}", port, e);
        }
        false
    } else {
        info!("Daemon on port {} (PID {}) is running and responsive", port, metadata.pid);
        true
    }
}

pub async fn stop_specific_main_daemon(port: u16, should_reclaim: bool) -> Result<(), anyhow::Error> {
    println!("Attempting to stop GraphDB Daemon on port {}...", port);
    stop_daemon_by_pid_or_scan("GraphDB Daemon", port, "main", should_reclaim).await
}

pub async fn stop_specific_rest_api_daemon(port: u16, should_reclaim: bool) -> Result<(), anyhow::Error> {
    println!("Attempting to stop REST API on port {}...", port);
    stop_daemon_by_pid_or_scan("REST API daemon", port, "rest", should_reclaim).await
}

pub async fn stop_specific_storage_daemon(port: u16, should_reclaim: bool) -> Result<(), anyhow::Error> {
    println!("Attempting to stop Storage daemon on port {}...", port);
    stop_daemon_by_pid_or_scan("Storage daemon", port, "storage", should_reclaim).await
}

async fn read_pid_from_file(path: &PathBuf) -> Result<u32, anyhow::Error> {
    let pid_str = tokio::fs::read_to_string(path).await
        .with_context(|| format!("Failed to read PID from file: {:?}", path))?;
    let pid = pid_str.trim().parse::<u32>()
        .with_context(|| format!("Failed to parse PID from file: '{}'", pid_str))?;
    Ok(pid)
}

#[derive(Clone, Copy, Debug)]
pub enum ServiceType {
    Daemon,
    RestAPI,
    StorageDaemon,
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ValueEnum for ServiceType {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Daemon, Self::RestAPI, Self::StorageDaemon]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        Some(match self {
            Self::Daemon => clap::builder::PossibleValue::new("main"),
            Self::RestAPI => clap::builder::PossibleValue::new("rest"),
            Self::StorageDaemon => clap::builder::PossibleValue::new("storage"),
        })
    }

    fn from_str(input: &str, ignore_case: bool) -> Result<Self, String> {
        let input = if ignore_case { input.to_lowercase() } else { input.to_string() };
        match input.as_str() {
            "main" => Ok(Self::Daemon),
            "rest" => Ok(Self::RestAPI),
            "storage" => Ok(Self::StorageDaemon),
            _ => Err(format!("Invalid service type: {}", input)),
        }
    }
}

pub async fn read_pid_file(port: u16, service_type: &ServiceType) -> Result<u32, anyhow::Error> {
    let path = match service_type {
        ServiceType::Daemon => std::env::temp_dir().join(format!("graphdb_daemon_{}.pid", port)),
        ServiceType::RestAPI => std::env::temp_dir().join(format!("graphdb_rest_{}.pid", port)),
        ServiceType::StorageDaemon => std::env::temp_dir().join(format!("graphdb_storage_{}.pid", port)),
    };
    read_pid_from_file(&path).await
}

pub async fn remove_pid_file(port: u16, service_type: &ServiceType) -> Result<(), anyhow::Error> {
    let path = match service_type {
        ServiceType::Daemon => std::env::temp_dir().join(format!("graphdb_daemon_{}.pid", port)),
        ServiceType::RestAPI => std::env::temp_dir().join(format!("graphdb_rest_{}.pid", port)),
        ServiceType::StorageDaemon => std::env::temp_dir().join(format!("graphdb_storage_{}.pid", port)),
    };
    if path.exists() {
        tokio::fs::remove_file(&path).await
            .with_context(|| format!("Failed to delete {:?} PID file: {:?}", service_type, path))?;
        info!("Deleted {:?} PID file: {:?}", service_type, path);
    }
    Ok(())
}

pub async fn stop_daemon_by_pid_or_scan(
    daemon_name: &str,
    port: u16,
    service_type_str: &str,
    should_reclaim: bool,
) -> Result<(), anyhow::Error> {
    let service_type = ServiceType::from_str(service_type_str, true)
        .map_err(|e| anyhow!("Invalid service type: {}", e))?;

    let mut target_pid: Option<u32> = None;
    if let Ok(Some(metadata)) = tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port)).await? {
        let is_running = is_process_running(metadata.pid).await;
        let age = Utc::now().signed_duration_since(chrono::DateTime::<Utc>::from_timestamp_nanos(metadata.last_seen_nanos));
        if is_running {
            target_pid = Some(metadata.pid);
            info!("Found {} (PID {}) on port {} in registry.", daemon_name, metadata.pid, port);
        } else if age > chrono::Duration::seconds(300) {
            warn!("Registry entry for {} on port {} (PID {}) is stale. Unregistering.", daemon_name, port, metadata.pid);
            tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.unregister_daemon(port)).await??;
        }
    }

    if target_pid.is_none() {
        let expected_arg = match service_type {
            ServiceType::Daemon => "--internal-daemon-run",
            ServiceType::RestAPI => "--internal-rest-api-run",
            ServiceType::StorageDaemon => "--internal-storage-daemon-run",
        };
        target_pid = find_daemon_process_on_port_with_args(port, daemon_name, expected_arg).await;
        if let Some(pid) = target_pid {
            info!("Found {} (PID {}) on port {} via direct scan. Registering in registry.", daemon_name, pid, port);
            let new_metadata = DaemonMetadata {
                service_type: service_type_str.to_string(),
                port,
                pid,
                ip_address: "127.0.0.1".to_string(),
                data_dir: None,
                config_path: None,
                engine_type: None,
                last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            };
            tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.register_daemon(new_metadata)).await??;
        }
    }

    if let Some(pid) = target_pid {
        if !should_reclaim && !is_port_free(port).await {
            return Err(anyhow!("Port {} is in use by PID {}. Use --join-cluster or a different port.", port, pid));
        }
        info!("Stopping {} (PID {}) on port {}...", daemon_name, pid, port);
        
        let mut sys = System::new();
        sys.refresh_processes(ProcessesToUpdate::All, true);

        if let Some(process) = sys.process(Pid::from_u32(pid)) {
            if process.kill_with(sysinfo::Signal::Term).unwrap_or(false) {
                let start_time = Instant::now();
                let wait_timeout = Duration::from_secs(5);
                while start_time.elapsed() < wait_timeout {
                    if !is_process_running(pid).await {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                if is_process_running(pid).await {
                    warn!("Failed to stop {} (PID {}) on port {}: Process still running after SIGTERM. Sending SIGKILL.", daemon_name, pid, port);
                    if process.kill_with(sysinfo::Signal::Kill).unwrap_or(false) {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        if is_process_running(pid).await {
                            return Err(anyhow!("Failed to force stop {} (PID {}) on port {}.", daemon_name, pid, port));
                        }
                    } else {
                        return Err(anyhow!("Failed to send SIGKILL to {} (PID {}) on port {}.", daemon_name, pid, port));
                    }
                }
                tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.unregister_daemon(port)).await??;
                remove_pid_file(port, &service_type).await?;
                info!("Successfully stopped {} (PID {}) on port {}.", daemon_name, pid, port);
            } else {
                error!("Failed to send termination signal to {} (PID {}) on port {}.", daemon_name, pid, port);
                return Err(anyhow!("Failed to terminate {} (PID {}) on port {}.", daemon_name, pid, port));
            }
        } else {
            warn!("Service {} (PID {}) on port {} not found after initial scan.", daemon_name, pid, port);
            tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.unregister_daemon(port)).await??;
            remove_pid_file(port, &service_type).await?;
        }
    } else {
        info!("No {} found on port {}.", daemon_name, port);
        tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.unregister_daemon(port)).await??;
        remove_pid_file(port, &service_type).await?;
    }
    Ok(())
}

pub async fn load_storage_config_path_or_default(path: Option<PathBuf>) -> Result<StorageConfig, anyhow::Error> {
    let path_owned: Option<String> = path.map(|p| p.to_string_lossy().into_owned());
    let path_str: Option<&str> = path_owned.as_deref();
    load_storage_config(path_str).await
}

pub fn parse_cluster_range(range_str: &str) -> Result<Vec<u16>, anyhow::Error> {
    if range_str.is_empty() || range_str == "null" {
        return Err(anyhow!("Cluster range is empty or null"));
    }

    // Validate input format
    if !range_str.chars().all(|c| c.is_digit(10) || c == '-') {
        return Err(anyhow!("Invalid characters in cluster range: {}", range_str));
    }

    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() == 2 {
        let start_port: u16 = parts[0]
            .trim()
            .parse()
            .context(format!("Invalid start port: {}", parts[0]))?;
        let end_port: u16 = parts[1]
            .trim()
            .parse()
            .context(format!("Invalid end port: {}", parts[1]))?;
        if start_port < 1024 || end_port < 1024 {
            return Err(anyhow!("Ports must be >= 1024: start={}, end={}", start_port, end_port));
        }
        if start_port > end_port {
            return Err(anyhow!("Start port cannot be greater than end port."));
        }
        let ports: Vec<u16> = (start_port..=end_port).collect();
        if ports.len() > MAX_CLUSTER_SIZE {
            return Err(anyhow!(
                "Cluster port range size ({}) exceeds maximum allowed ({})",
                ports.len(),
                MAX_CLUSTER_SIZE
            ));
        }
        Ok(ports)
    } else if parts.len() == 1 {
        let port: u16 = range_str
            .trim()
            .parse()
            .context(format!("Invalid port: {}", range_str))?;
        if port < 1024 {
            return Err(anyhow!("Port must be >= 1024: {}", port));
        }
        Ok(vec![port])
    } else {
        Err(anyhow!(
            "Invalid cluster range format. Expected 'start-end' or a single port: {}",
            range_str
        ))
    }
}

/// Checks if a given port is within a specified cluster range.
/// If no range is provided, it returns true, indicating the port should be displayed.
pub fn is_port_in_range(port: u16, range_str_opt: Option<&String>) -> Result<bool> {
    if let Some(range_str) = range_str_opt {
        let ports = parse_cluster_range(range_str)?;
        Ok(ports.contains(&port))
    } else {
        // If no range is specified, assume we want to show all daemons.
        Ok(true)
    }
}

pub fn is_port_in_cluster_range(port: u16, range_str: &str) -> bool {
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() == 1 {
        // Single port range
        if let Ok(single_port) = parts[0].trim().parse::<u16>() {
            return port == single_port;
        }
    } else if parts.len() == 2 {
        // Ranged ports
        if let (Ok(start), Ok(end)) = (parts[0].trim().parse::<u16>(), parts[1].trim().parse::<u16>()) {
            return port >= start && port <= end;
        }
    }
    false
}

pub fn is_port_within_range(port: u16, cluster_ports: &[u16]) -> bool {
    cluster_ports.contains(&port)
}

//pub async fn is_port_free(port: u16) -> bool {
//   !is_port_listening(port).await
//}

pub async fn is_port_listening(port: u16) -> bool {
    tokio::time::timeout(Duration::from_secs(1), async {
        TcpStream::connect(format!("127.0.0.1:{}", port)).await.is_ok()
    }).await.unwrap_or(false)
}

pub async fn is_daemon_running(port: u16) -> bool {
    !is_port_free(port).await
}

pub async fn is_process_running(pid: u32) -> bool {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);
    sys.process(Pid::from_u32(pid)).is_some()
}

pub async fn find_pid_by_port(port: u16) -> Option<u32> {
    for attempt in 0..5 {
        let output = TokioCommand::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-sTCP:LISTEN")
            .arg("-t")
            .output()
            .await
            .ok()?;

        if output.status.success() {
            let pid_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !pid_str.is_empty() {
                if let Ok(pid) = pid_str.parse::<u32>() {
                    if is_process_running(pid).await {
                        println!("[DEBUG] Found PID {} for port {} on attempt {}", pid, port, attempt);
                        return Some(pid);
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!("[DEBUG] No valid PID found for port {} after 5 attempts", port);
    None
}

pub async fn find_all_running_rest_api_ports() -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    let mut running_ports: Vec<u16> = all_daemons
        .into_iter()
        .filter(|(_port, (_pid, service_type))| service_type == "rest")
        .map(|(port, _)| port)
        .collect();
    running_ports.sort_unstable();
    running_ports
}

/// Helper function to check if a process ID is valid by sending a signal 0.
/// This works on most Unix-like systems.
pub async fn check_pid_validity(pid: u32) -> bool {
    // We send signal 0, which is a way to check if a process exists and we have permissions.
    // It will return a zero exit code if the process exists.
    let output = tokio::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .output()
        .await
        .unwrap_or_else(|_| {
            debug!("Failed to run 'kill -0' command to check PID {}", pid);
            std::process::Output {
                status: std::process::ExitStatus::default(),
                stdout: b"".to_vec(),
                stderr: b"".to_vec(),
            }
        });

    output.status.success()
}

/// Helper function to check if a port is free.
pub async fn is_port_free(port: u16) -> bool {
    tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .is_ok()
}

pub async fn stop_process_by_pid(service_name: &str, pid: u32) -> Result<(), GraphError> {
    info!("Sending SIGTERM to {} (PID: {})", service_name, pid);
    println!("===> SENDING SIGTERM TO {} (PID: {})", service_name, pid);

    // Prevent sending SIGTERM to the CLI process
    if pid == std::process::id() as u32 {
        warn!("Attempted to send SIGTERM to CLI process (PID: {}). Ignoring.", pid);
        println!("===> WARNING: ATTEMPTED TO SEND SIGTERM TO CLI PROCESS (PID: {}). IGNORING", pid);
        return Ok(());
    }

    // Send SIGTERM to the daemon process
    let pid_nix = NixPid::from_raw(pid as i32);
    kill(pid_nix, Signal::SIGTERM).map_err(|e| {
        error!("Failed to send SIGTERM to {} (PID: {}): {}", service_name, pid, e);
        println!("===> ERROR: FAILED TO SEND SIGTERM TO {} (PID: {})", service_name, pid);
        GraphError::StorageError(format!("Failed to send SIGTERM to {} (PID: {}): {}", service_name, pid, e))
    })?;

    // Wait for process to terminate
    for _ in 0..10 {
        if !check_pid_validity(pid).await {
            info!("Successfully stopped {} (PID: {})", service_name, pid);
            println!("===> SUCCESSFULLY STOPPED {} (PID: {})", service_name, pid);
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    error!("Failed to stop {} (PID: {}) after 5 seconds", service_name, pid);
    println!("===> ERROR: FAILED TO STOP {} (PID: {}) AFTER 5 SECONDS", service_name, pid);
    Err(GraphError::StorageError(format!("Failed to stop {} (PID: {}) after 5 seconds", service_name, pid)))
}

// Helper function to parse cluster range
pub fn parse_port_cluster_range(range: &str) -> Result<Vec<u16>, anyhow::Error> {
    if range.is_empty() {
        return Ok(vec![]);
    }
    if range.contains('-') {
        let parts: Vec<&str> = range.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid cluster range format: {}", range));
        }                        
        let start: u16 = parts[0].parse().context("Failed to parse cluster range start")?;
        let end: u16 = parts[1].parse().context("Failed to parse cluster range end")?;
        Ok((start..=end).collect())
    } else {
        let port: u16 = range.parse().context("Failed to parse single port")?;
        Ok(vec![port])            
    }
}        

pub async fn is_pid_running(pid: u32) -> bool {
    let mut sys = sysinfo::System::new();
    let sysinfo_pid = sysinfo::Pid::from_u32(pid);
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sysinfo_pid]), true);
    sys.process(sysinfo_pid).is_some()
}

pub async fn find_port_by_pid(pid: u32) -> Option<u16> {
    let output = TokioCommand::new("lsof")
        .arg("-P")
        .arg("-iTCP")
        .arg("-sTCP:LISTEN")
        .arg("-p")
        .arg(pid.to_string())
        .output().await.ok()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let re = Regex::new(r"TCP.*:(\d+)\s+\(LISTEN\)").unwrap();
        if let Some(captures) = re.captures(&stdout) {
            if let Some(port_match) = captures.get(1) {
                return port_match.as_str().parse::<u16>().ok();
            }
        }
    }
    None
}

pub async fn check_daemon_health(addr: &str) -> Result<bool> {
    const HEALTH_TIMEOUT_SECS: u64 = 10;
    const MAX_RETRIES: u32 = 5;
    const BASE_RETRY_DELAY_MS: u64 = 500;

    let request = json!({ "command": "ping" });
    let request_data = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("Failed to serialize health check request: {}", e))?;

    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_RETRIES {
        debug!("Health check attempt {}/{} for {}", attempt, MAX_RETRIES, addr);

        let response_result = tokio::time::timeout(
            TokioDuration::from_secs(HEALTH_TIMEOUT_SECS),
            tokio::task::spawn_blocking({
                let addr = addr.to_string();
                let request_data = request_data.clone();
                move || {
                    let zmq_context = zmq::Context::new();
                    let client = zmq_context.socket(zmq::REQ)
                        .map_err(|e| anyhow!("Failed to create ZMQ socket: {}", e))?;

                    client.set_rcvtimeo((HEALTH_TIMEOUT_SECS * 1000) as i32)
                        .map_err(|e| anyhow!("Failed to set receive timeout: {}", e))?;
                    client.set_sndtimeo((HEALTH_TIMEOUT_SECS * 1000) as i32)
                        .map_err(|e| anyhow!("Failed to set send timeout: {}", e))?;
                    client.set_linger(0)
                        .map_err(|e| anyhow!("Failed to set linger: {}", e))?;

                    client.connect(&addr)
                        .map_err(|e| anyhow!("Failed to connect to {}: {}", addr, e))?;

                    client.send(&request_data, 0)
                        .map_err(|e| anyhow!("Failed to send ping request to {}: {}", addr, e))?;

                    let mut msg = zmq::Message::new();
                    client.recv(&mut msg, 0)
                        .map_err(|e| anyhow!("Failed to receive ping response from {}: {}", addr, e))?;

                    let response: Value = serde_json::from_slice(msg.as_ref())
                        .map_err(|e| anyhow!("Failed to deserialize ping response from {}: {}", addr, e))?;

                    Ok::<bool, anyhow::Error>(response.get("status").and_then(|s| s.as_str()) == Some("success"))
                }
            })
        )
        .await;

        match response_result {
            Ok(Ok(Ok(true))) => {
                info!("Health check succeeded for {}", addr);
                return Ok(true);
            }
            Ok(Ok(Ok(false))) => {
                last_error = Some(anyhow!("Health check failed: Invalid response from {}", addr));
            }
            Ok(Ok(Err(e))) => {
                last_error = Some(e);
            }
            Ok(Err(e)) => {
                last_error = Some(e.into());
            }
            Err(_) => {
                last_error = Some(anyhow!("Health check timed out after {} seconds", HEALTH_TIMEOUT_SECS));
            }
        }

        if attempt < MAX_RETRIES {
            let delay = BASE_RETRY_DELAY_MS * 2u64.pow(attempt - 1);
            debug!("Retrying health check after {}ms", delay);
            tokio::time::sleep(TokioDuration::from_millis(delay)).await;
        }
    }

    error!("Health check failed after {} attempts: {:?}", MAX_RETRIES, last_error);
    Err(last_error.unwrap_or_else(|| anyhow!("Health check failed after {} attempts", MAX_RETRIES)))
}

pub fn spawn_storage_daemon(
    port: u16,
    config_file: Option<PathBuf>,
    _shutdown_rx: oneshot::Receiver<()>,
) -> Result<JoinHandle<()>, anyhow::Error> {
    let storage_config_path = config_file.unwrap_or_else(|| {
        PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
            .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)
    });

    let handle = tokio::spawn(async move {
        info!("Storage daemon spawned on port {}.", port);
        match start_storage_server(Some(port), storage_config_path).await {
            Ok(_) => info!("Storage daemon on port {} stopped successfully.", port),
            Err(e) => error!("Storage daemon on port {} exited with error: {:?}", port, e),
        }
    });
    Ok(handle)
}


/// Starts a daemon process based on the service type and returns its PID.
pub async fn spawn_daemon_process(
    port: Option<u16>,
    cluster_range: Option<String>,
    is_rest: bool,
    is_storage: bool,
) -> Result<u32> {
    let actual_port = port.unwrap_or(if is_rest {
        DEFAULT_REST_API_PORT
    } else if is_storage {
        DEFAULT_STORAGE_PORT
    } else {
        DEFAULT_DAEMON_PORT
    });

    // Define skip_ports based on service type
    let skip_ports = if is_rest {
        vec![DEFAULT_STORAGE_PORT] // Skip storage port for REST API
    } else if is_storage {
        vec![DEFAULT_REST_API_PORT] // Skip REST API port for storage
    } else {
        vec![DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT] // Skip both for main daemon
    };

    // Determine service type
    let service_type = if is_rest {
        "rest"
    } else if is_storage {
        "storage"
    } else {
        "main"
    };

    // Use cluster_range only for main daemon, pass None for REST and storage
    let effective_cluster = if is_rest || is_storage {
        None
    } else {
        cluster_range
    };

    debug!("Starting daemon: port={}, cluster_range={:?}, skip_ports={:?}, service_type={}", actual_port, effective_cluster, skip_ports, service_type);
    start_daemon(Some(actual_port), effective_cluster, skip_ports, service_type, None)
        .await
        .map_err(|e| {
            error!("Failed to start daemon: port={}, error={}", actual_port, e);
            anyhow!("Failed to start daemon process: {}", e)
        })?;

    // Read PID from file
    let pid_file = format!("/tmp/graphdb-{}-{}.pid", service_type, actual_port);
    let pid = fs::read_to_string(&pid_file)
        .map_err(|e| anyhow!("Failed to read PID file {}: {}", pid_file, e))?
        .trim()
        .parse::<u32>()
        .map_err(|e| anyhow!("Invalid PID in {}: {}", pid_file, e))?;

    info!("Spawned daemon process with PID {} on port {}", pid, actual_port);
    Ok(pid)
}

pub async fn start_daemon_process(
    is_rest: bool,
    is_storage: bool,
    port: Option<u16>,
    config_path: Option<PathBuf>,
    engine_type: Option<String>,
) -> Result<u32> {
    let config_path_str = config_path.as_ref().map(|p| p.to_string_lossy().into_owned());
    
    // Move async config loading outside the closure
    let default_port = if is_rest {
        load_rest_config(config_path_str.as_deref())
            .map(|c| c.default_port)
            .unwrap_or(DEFAULT_REST_API_PORT)
    } else if is_storage {
        // Handle async storage config loading
        match load_storage_config(config_path_str.as_deref()).await {
            Ok(c) => c.default_port,
            Err(_) => DEFAULT_STORAGE_PORT,
        }
    } else {
        load_main_daemon_config(config_path_str.as_deref())
            .map(|c| c.default_port)
            .unwrap_or(DEFAULT_DAEMON_PORT)
    };
    
    let actual_port = port.unwrap_or(default_port);

    if !is_port_free(actual_port).await {
        error!("Port {} is already in use", actual_port);
        return Err(anyhow!("Port {} is already in use", actual_port));
    }

    let process_name = if is_rest {
        "REST API"
    } else if is_storage {
        "Storage Daemon"
    } else {
        "GraphDB Daemon"
    };
    info!("Starting {} process on port {}", process_name, actual_port);

    let _config_content = if let Some(ref path) = config_path {
        if !path.exists() {
            warn!("Config file not found at {}. Using defaults.", path.display());
            None
        } else {
            let content = fs::read_to_string(path)
                .context(format!("Failed to read config file: {}", path.display()))?;
            debug!("Config content for {}: {}", process_name, content);
            Some(content)
        }
    } else {
        None
    };

    let data_dir = if is_storage {
        load_storage_config(config_path_str.as_deref()).await
            .ok()
            .and_then(|c| c.data_directory) // c.data_directory is Option<PathBuf>
            .unwrap_or_else(|| PathBuf::from(format!("{}/storage_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)))
    } else if is_rest {
        load_rest_config(config_path_str.as_deref())
            .ok()
            .and_then(|c| Some(PathBuf::from(c.data_directory))) // c.data_directory is String, convert to PathBuf
            .unwrap_or_else(|| PathBuf::from(format!("{}/rest_api_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)))
    } else {
        load_main_daemon_config(config_path_str.as_deref())
            .ok()
            .and_then(|c| Some(PathBuf::from(c.data_directory))) // c.data_directory is String, convert to PathBuf
            .unwrap_or_else(|| PathBuf::from(format!("{}/daemon_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)))
    };

    // Spawn as TokioCommand so we can pipe stdout/stderr
    let mut child = TokioCommand::new(std::env::current_exe()?)
        .arg("--daemon")
        .arg("--port").arg(actual_port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("Failed to spawn {} process", process_name))?;

    let child_pid = child.id().ok_or_else(|| anyhow!("Failed to get PID of spawned {}", process_name))?;

    // Capture stdout logs
    if let Some(stdout) = child.stdout.take() {
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                info!("[{} stdout] {}", process_name, line);
            }
        });
    }

    // Capture stderr logs
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = reader.next_line().await {
                error!("[{} stderr] {}", process_name, line);
            }
        });
    }

    let addr = format!("127.0.0.1:{}", actual_port);
    let health_check_timeout = Duration::from_secs(5);
    let poll_interval = Duration::from_millis(200);
    let start_time = Instant::now();

    while start_time.elapsed() < health_check_timeout {
        if TcpStream::connect(&addr).await.is_ok() {
            info!("{} process started successfully with PID {} on port {}", process_name, child_pid, actual_port);

            let metadata = DaemonMetadata {
                service_type: if is_rest { "rest" } else if is_storage { "storage" } else { "main" }.to_string(),
                port: actual_port,
                pid: child_pid,
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(data_dir),
                config_path,
                engine_type,
                last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            };

            GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await
                .with_context(|| format!("Failed to register {} daemon on port {}", process_name, actual_port))?;

            return Ok(child_pid);
        }
        time::sleep(poll_interval).await;
    }

    error!("{} process on port {} failed to become reachable after {} seconds", process_name, actual_port, health_check_timeout.as_secs());
    Err(anyhow!("{} process on port {} failed to start", process_name, actual_port))
}

/// Starts a main daemon process and registers it.
pub async fn start_daemon_with_port(p: u16, service_type: &str) -> Result<(), anyhow::Error> {
    if !is_port_free(p).await {
        return Err(anyhow!("Port {} is already in use.", p));
    }

    if GLOBAL_DAEMON_REGISTRY.find_daemon_by_port(p).await?.is_some() {
        warn!("Daemon already registered on port {}.", p);
        return Ok(());
    }

    start_daemon(Some(p), None, Vec::new(), service_type, None)
        .await
        .map_err(|e| anyhow!("Failed to start daemon via daemon_api: {}", e))?;

    let pid = find_pid_by_port(p).await.ok_or_else(|| anyhow!("Failed to find PID for newly started daemon on port {}. Registration failed.", p))?;

    if pid == 0 {
        return Err(anyhow!("Invalid PID for newly started daemon on port {}. Registration failed.", p));
    }

    let details = DaemonMetadata {
        service_type: service_type.to_string(),
        port: p,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: None,
        config_path: None,
        engine_type: None,
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.register_daemon(details))
        .await
        .map_err(|_| anyhow!("Timeout registering daemon"))?
        .context("Failed to register daemon in registry")?;

    Ok(())
}

/// Wraps `start_daemon` from daemon_api to start a daemon process and return its PID.
pub async fn start_daemon_with_pid(
    port: Option<u16>,
    cluster: Option<String>,
    args: Vec<String>,
    service_type: &str,
) -> Result<u32, anyhow::Error> {
    let args_u16: Result<Vec<u16>> = args
        .into_iter()
        .map(|arg| {
            arg.parse::<u16>()
                .map_err(|e| anyhow!("Failed to parse argument '{}' as u16: {}", arg, e))
        })
        .collect();
    let args_u16 = args_u16.context("Failed to convert arguments to u16")?;

    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
    if !is_port_free(actual_port).await {
        return Err(anyhow!("Port {} is already in use.", actual_port));
    }

    let mut command = Command::new(std::env::current_exe().context("Failed to get current executable path")?);
    
    if let Some(port) = port {
        command.arg("--port").arg(port.to_string());
    }
    
    if let Some(cluster) = &cluster {
        command.arg("--cluster").arg(cluster);
    }
    
    for arg in &args_u16 {
        command.arg(arg.to_string());
    }
    
    command.stdout(Stdio::null()).stderr(Stdio::null());
    
    let child = command
        .spawn()
        .context("Failed to spawn daemon process")?;
    
    let pid = child.id().context("Failed to get PID of spawned daemon process")?;
    
    start_daemon(port, cluster.clone(), args_u16, service_type, None)
        .await
        .context("Failed to initialize daemon via daemon_api")?;

    // Handle cluster ports
    let cluster_ports = if let Some(cluster_range) = cluster {
        parse_cluster_range(&cluster_range).context("Failed to parse cluster range")?
    } else {
        Vec::new()
    };

    let metadata = DaemonMetadata {
        service_type: service_type.to_string(),
        port: actual_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: None,
        config_path: None,
        engine_type: None,
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.register_daemon(metadata))
        .await
        .map_err(|_| anyhow!("Timeout registering daemon"))?
        .context("Failed to register daemon in registry")?;

    // Register additional cluster ports if specified
    for cluster_port in cluster_ports {
        if cluster_port != actual_port {
            let cluster_metadata = DaemonMetadata {
                service_type: service_type.to_string(),
                port: cluster_port,
                pid,
                ip_address: "127.0.0.1".to_string(),
                data_dir: None,
                config_path: None,
                engine_type: None,
                last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            };
            tokio::time::timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.register_daemon(cluster_metadata))
                .await
                .map_err(|_| anyhow!("Timeout registering cluster daemon"))?
                .context("Failed to register cluster daemon in registry")?;
        }
    }

    Ok(pid)
}


/// Signal-based restart (stop and start) for a daemon process.
pub async fn restart_daemon_process(
    process_name: &str,
    port: u16,
    is_rest_api_server: bool,
    is_storage_daemon: bool,
    config_path: Option<PathBuf>,
    engine_type: Option<StorageEngineType>,
) -> Result<(), anyhow::Error> {
    let engine_type_str = engine_type.map(|et| et.to_string());
    
    // Stop the process first
    stop_process_by_port(process_name, port).await?;
    
    // Start the daemon process
    start_daemon_process(
        is_rest_api_server,
        is_storage_daemon,
        Some(port),
        config_path,
        engine_type_str,
    )
    .await?;
    
    Ok(())
}

/// Restarts a storage daemon on the specified port without health checks.
pub async fn restart_storage_daemon(port: u16) -> GraphResult<()> {
    info!("Restarting storage daemon on port {}", port);
    println!("===> RESTARTING STORAGE DAEMON ON PORT {}", port);

    // Load storage configuration
    let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join("storage_config.yaml");
    let storage_config = load_storage_config_path_or_default(Some(config_path.clone()))
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to load storage config: {}", e)))?;

    // Resolve data directory
    let data_dir = storage_config
        .data_directory
        .as_ref()
        .map(|d| d.clone())
        .unwrap_or_else(|| {
            warn!("Storage config data_directory is None, using default: /opt/graphdb/storage_data");
            PathBuf::from("/opt/graphdb/storage_data")
        });

    let engine_type = storage_config.storage_engine_type.clone();
    let engine_path_name = daemon_api_storage_engine_type_to_string(&engine_type).to_lowercase();
    let instance_path = data_dir.join(&engine_path_name).join(port.to_string());

    // Check for existing valid daemon
    if let Some(daemon) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
        if daemon.pid > 0
            && check_pid_validity(daemon.pid).await
            && daemon.engine_type.as_ref() == Some(&engine_type.to_string())
        {
            let socket_addr = format!("ipc:///tmp/graphdb-{}.ipc", port);
            if SledClient::ping_daemon(port, &socket_addr).await.is_ok() {
                info!("Valid daemon already running on port {} with PID {}. Skipping restart.", port, daemon.pid);
                println!("===> REUSING EXISTING DAEMON ON PORT {} WITH PID {}", port, daemon.pid);
                return Ok(());
            }
        }
        // Stop stale or mismatched daemon
        if let Err(e) = stop_process_by_pid("Storage Daemon", daemon.pid).await {
            warn!("Failed to stop existing daemon on port {} (PID {}): {}. Proceeding.", port, daemon.pid, e);
            println!("===> WARNING: FAILED TO STOP DAEMON ON PORT {} (PID {})", port, daemon.pid);
        }
        GLOBAL_DAEMON_REGISTRY
            .remove_daemon_by_type("storage", port)
            .await?;
        println!("===> REMOVED STALE DAEMON ON PORT {} FROM REGISTRY", port);
    }

    // Clean up stale IPC socket file
    let socket_path = format!("/tmp/graphdb-{}.ipc", port);
    if tokio_fs::metadata(&socket_path).await.is_ok() {
        if let Err(e) = tokio_fs::remove_file(&socket_path).await {
            warn!("Failed to remove stale IPC socket file {}: {}. Continuing.", socket_path, e);
            println!("===> WARNING: FAILED TO REMOVE STALE IPC SOCKET FILE {}", socket_path);
        } else {
            info!("Removed stale IPC socket file {}", socket_path);
            println!("===> REMOVED STALE IPC SOCKET FILE {}", socket_path);
        }
    }

    // Clean up stale PID file
    let pid_file_path = PathBuf::from("/tmp").join(format!("storage_daemon_{}.pid", port));
    if tokio_fs::metadata(&pid_file_path).await.is_ok() {
        if let Err(e) = tokio_fs::remove_file(&pid_file_path).await {
            warn!("Failed to remove stale PID file {}: {}. Continuing.", pid_file_path.display(), e);
            println!("===> WARNING: FAILED TO REMOVE STALE PID FILE {}", pid_file_path.display());
        } else {
            info!("Removed stale PID file {}", pid_file_path.display());
            println!("===> REMOVED STALE PID FILE {}", pid_file_path.display());
        }
    }

    // Verify port is free
    let max_port_attempts = 3;
    let port_free = {
        let mut free = false;
        for attempt in 1..=max_port_attempts {
            if is_port_free(port).await {
                free = true;
                break;
            }
            warn!("Port {} is still in use on attempt {}/{}", port, attempt, max_port_attempts);
            println!("===> WARNING: PORT {} STILL IN USE ON ATTEMPT {}", port, attempt);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        free
    };
    if !port_free {
        return Err(GraphError::StorageError(format!(
            "Port {} is not free after {} attempts",
            port, max_port_attempts
        )));
    }
    println!("===> PORT {} IS FREE", port);

    // Spawn new daemon
    let daemon_config_string = serde_yaml::to_string(&storage_config)
        .map_err(|e| GraphError::StorageError(format!("Failed to serialize daemon config: {}", e)))?;
    let storage_config_for_spawn = storage_config.clone();
    let handle: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = start_daemon(
            Some(port),
            Some(daemon_config_string),
            vec![],
            "storage",
            Some(storage_config_for_spawn),
        )
        .await
        {
            error!("Failed to spawn storage daemon on port {}: {}", port, e);
            println!("===> ERROR: FAILED TO SPAWN STORAGE DAEMON ON PORT {}: {}", port, e);
        }
    });

    // Wait for daemon to be discoverable
    let max_startup_attempts = 10;
    let mut daemon_responsive = false;
    let mut pid = None;

    for attempt in 1..=max_startup_attempts {
        if let Some(p) = timeout(Duration::from_secs(2), find_pid_by_port(port)).await.ok().flatten() {
            if p > 0 && check_pid_validity(p).await {
                pid = Some(p);
                info!("Found daemon PID {} for port {} on attempt {}", p, port, attempt);
                println!("===> FOUND DAEMON PID {} FOR PORT {} ON ATTEMPT {}", p, port, attempt);

                // Register daemon metadata
                let metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: p,
                    ip_address: "127.0.0.1".to_string(),
                    data_dir: Some(instance_path.clone()),
                    config_path: Some(config_path.clone()),
                    engine_type: Some(daemon_api_storage_engine_type_to_string(&engine_type)),
                    last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                };

                if let Err(e) = timeout(Duration::from_secs(2), GLOBAL_DAEMON_REGISTRY.register_daemon(metadata)).await {
                    warn!("Timeout registering daemon metadata on attempt {}: {:?}", attempt, e);
                    println!("===> WARNING: TIMEOUT REGISTERING DAEMON ON PORT {} ON ATTEMPT {}", port, attempt);
                } else {
                    info!("Registered daemon on port {} with path {:?}", port, instance_path);
                    println!("===> REGISTERED DAEMON ON PORT {} WITH PATH {:?}", port, instance_path);
                }

                // Check responsiveness
                let socket_addr = format!("ipc:///tmp/graphdb-{}.ipc", port);
                if SledClient::ping_daemon(port, &socket_addr).await.is_ok() {
                    info!("Storage daemon on port {} is responsive after {} attempts", port, attempt);
                    println!("===> STORAGE DAEMON ON PORT {} IS RESPONSIVE AFTER {} ATTEMPTS", port, attempt);
                    daemon_responsive = true;
                    break;
                } else {
                    info!("Daemon found but not yet responsive on attempt {}, waiting...", attempt);
                    println!("===> DAEMON ON PORT {} NOT YET RESPONSIVE ON ATTEMPT {}", port, attempt);
                }
            }
        }
        if attempt < max_startup_attempts {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    if !daemon_responsive {
        handle.abort();
        return Err(GraphError::StorageError(format!(
            "Storage daemon on port {} failed to become responsive after {} attempts",
            port, max_startup_attempts
        )));
    }

    // Keep handle alive
    tokio::spawn(async move {
        let _ = handle.await;
        info!("Storage daemon task on port {} completed", port);
    });

    info!("Storage daemon on port {} successfully restarted and is responsive", port);
    println!("===> STORAGE DAEMON ON PORT {} SUCCESSFULLY RESTARTED", port);
    Ok(())
}

/// Checks if the specified Unix domain socket path is used by the current CLI process.
/// The socket path is expected to be in the format `ipc:///tmp/graphdb-<port>.ipc`.
pub async fn is_socket_used_by_cli(socket_path: &str) -> Result<bool> {
    // Extract port from socket path (e.g., ipc:///tmp/graphdb-8080.ipc -> 8080)
    let socket_path_clean = socket_path.trim_start_matches("ipc://");
    let path = Path::new(socket_path_clean);
    let file_name = path
        .file_name()
        .and_then(|f| f.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid socket path: {}", socket_path))?;

    let port_str = file_name
        .strip_prefix("graphdb-")
        .and_then(|s| s.strip_suffix(".ipc"))
        .ok_or_else(|| anyhow::anyhow!("Socket path does not match expected format: {}", socket_path))?;

    let port: u16 = port_str
        .parse()
        .context(format!("Failed to parse port from socket path: {}", port_str))?;

    // Check if the socket file exists
    if !path.exists() {
        debug!("Socket path {} does not exist", socket_path);
        return Ok(false);
    }

    let metadata = tokio_fs::metadata(path)
        .await
        .context(format!("Failed to get metadata for socket path {}", socket_path))?;
    
    if !metadata.file_type().is_socket() {
        debug!("Path {} exists but is not a socket", socket_path);
        return Ok(false);
    }

    // Get the current process ID and inspect command-line arguments
    let pid = process::id();
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);

    let has_expected_port = if let Some(process) = sys.process(Pid::from_u32(pid)) {
        let cmd_args: Vec<String> = process
            .cmd()
            .iter()
            .map(|s| s.to_string_lossy().into_owned())
            .collect();
        if let Some(pos) = cmd_args.iter().position(|arg| arg == "--internal-port") {
            if let Some(port_arg) = cmd_args.get(pos + 1) {
                port_arg.parse::<u16>().ok() == Some(port)
            } else {
                false
            }
        } else {
            false
        }
    } else {
        debug!("Current process (PID {}) not found in sysinfo", pid);
        return Ok(false);
    };

    if !has_expected_port {
        debug!("CLI process (PID {}) does not have --internal-port {} in command args", pid, port);
        return Ok(false);
    }

    // Attempt to connect to the socket using ZeroMQ to verify usage
    let zmq_context = ZmqContext::new();
    let socket = zmq_context
        .socket(SocketType::REQ)
        .context("Failed to create ZeroMQ socket")?;

    socket
        .set_connect_timeout(1000)
        .context("Failed to set ZeroMQ connect timeout")?;
    socket
        .set_rcvtimeo(1000)
        .context("Failed to set ZeroMQ receive timeout")?;
    socket
        .set_sndtimeo(1000)
        .context("Failed to set ZeroMQ send timeout")?;

    let connect_result = socket.connect(socket_path); // Removed .await
    if connect_result.is_ok() {
        debug!("Successfully connected to socket {} from CLI process (PID {})", socket_path, pid);
        Ok(true)
    } else {
        debug!("Failed to connect to socket {} from CLI process (PID {}), assuming not used by this CLI", socket_path, pid);
        Ok(false)
    }
}



pub async fn stop_storage_interactive(
    port: Option<u16>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    daemon_port: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;

    let port = port.ok_or_else(|| anyhow::anyhow!("No port specified for stopping storage daemon"))?;
    info!("Attempting to stop Storage Daemon on port {}...", port);
    println!("===> Attempting to stop Storage Daemon on port {}...", port);

    // Find the daemon for the specified port
    let daemon = daemon_registry
        .get_all_daemon_metadata()
        .await
        .context("Failed to access daemon registry")?
        .into_iter()
        .find(|d| d.service_type == "storage" && d.port == port);

    match daemon {
        Some(d) if d.pid != 0 && d.pid != std::process::id() && check_pid_validity(d.pid).await => {
            info!("Found valid daemon on port {} with PID {}. Stopping...", port, d.pid);
            println!("===> Found valid daemon on port {} with PID {}. Stopping...", port, d.pid);

            // Clean up engine-specific lock files (e.g., RocksDB LOCK file)
            if let Some(engine_type_str) = &d.engine_type {
                let daemon_engine = match engine_type_str.as_str() {
                    "sled" => StorageEngineType::Sled,
                    "rocksdb" => StorageEngineType::RocksDB,
                    "tikv" => StorageEngineType::TiKV,
                    _ => StorageEngineType::Sled, // Default to Sled if unknown
                };
                if let Some(data_dir) = &d.data_dir {
                    if let Err(e) = force_cleanup_engine_lock(daemon_engine, &Some(data_dir.clone())).await {
                        warn!("Failed to clean up lock files for engine {} at {:?}: {}", engine_type_str, data_dir, e);
                        println!("===> WARNING: Failed to clean up lock files for engine {} at {:?}", engine_type_str, data_dir);
                    } else {
                        info!("Successfully cleaned up lock files for engine {} at {:?}", engine_type_str, data_dir);
                        println!("===> Successfully cleaned up lock files for engine {} at {:?}", engine_type_str, data_dir);
                    }
                }
            }

            // Send SIGTERM with retries
            let max_attempts = 3;
            let retry_interval = Duration::from_millis(500);
            let mut process_stopped = false;

            for attempt in 1..=max_attempts {
                if let Err(e) = kill(NixPid::from_raw(d.pid as i32), Signal::SIGTERM) {
                    warn!("Failed to send SIGTERM to PID {} for port {} on attempt {}: {}", d.pid, port, attempt, e);
                    println!("===> WARNING: Failed to send SIGTERM to PID {} for port {} on attempt {}", d.pid, port, attempt);
                } else {
                    info!("Sent SIGTERM to PID {} for Storage Daemon on port {} (attempt {}).", d.pid, port, attempt);
                    println!("===> Sent SIGTERM to PID {} for Storage Daemon on port {}.", d.pid, port);
                }

                // Wait to ensure the process exits
                if let Ok(_) = timeout(retry_interval, async {
                    while check_pid_validity(d.pid).await {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }).await {
                    process_stopped = true;
                    break;
                }

                warn!("Port {} is still in use after SIGTERM (attempt {}). Retrying...", port, attempt);
                println!("===> WARNING: Port {} is still in use after SIGTERM (attempt {}).", port, attempt);
            }

            if !process_stopped {
                warn!("Failed to stop daemon on port {} (PID {}) after {} attempts.", port, d.pid, max_attempts);
                println!("===> WARNING: Failed to stop daemon on port {} (PID {}) after {} attempts.", port, d.pid, max_attempts);
            }

            // Verify port is free
            if is_port_free(port).await {
                info!("Port {} is now free.", port);
                println!("===> Port {} is now free.", port);
            } else {
                warn!("Port {} is still in use after termination attempts.", port);
                println!("===> WARNING: Port {} is still in use after termination attempts.", port);
            }

            // Remove daemon from registry
            daemon_registry
                .unregister_daemon(port)
                .await
                .context(format!("Failed to remove daemon on port {} from registry", port))?;
            info!("Storage daemon on port {} removed from registry.", port);
            println!("===> Storage daemon on port {} removed from registry.", port);
        }
        Some(d) => {
            info!("No valid PID found for Storage Daemon on port {} (PID: {}). Cleaning up registry.", port, d.pid);
            println!("===> No valid PID found for Storage Daemon on port {} (PID: {}). Cleaning up registry.", port, d.pid);

            // Clean up engine-specific lock files
            if let Some(engine_type_str) = &d.engine_type {
                let daemon_engine = match engine_type_str.as_str() {
                    "sled" => StorageEngineType::Sled,
                    "rocksdb" => StorageEngineType::RocksDB,
                    "tikv" => StorageEngineType::TiKV,
                    _ => StorageEngineType::Sled,
                };
                if let Some(data_dir) = &d.data_dir {
                    if let Err(e) = force_cleanup_engine_lock(daemon_engine, &Some(data_dir.clone())).await {
                        warn!("Failed to clean up lock files for engine {} at {:?}: {}", engine_type_str, data_dir, e);
                        println!("===> WARNING: Failed to clean up lock files for engine {} at {:?}", engine_type_str, data_dir);
                    } else {
                        info!("Successfully cleaned up lock files for engine {} at {:?}", engine_type_str, data_dir);
                        println!("===> Successfully cleaned up lock files for engine {} at {:?}", engine_type_str, data_dir);
                    }
                }
            }

            // Remove from registry
            daemon_registry
                .unregister_daemon(port)
                .await
                .context(format!("Failed to remove daemon on port {} from registry", port))?;
            info!("Storage daemon on port {} removed from registry.", port);
            println!("===> Storage daemon on port {} removed from registry.", port);
        }
        None => {
            info!("No Storage Daemon found on port {} in registry.", port);
            println!("===> No Storage Daemon found on port {} in registry.", port);

            // Check for stray lock files
            let data_dir = PathBuf::from("/opt/graphdb/storage_data").join("rocksdb").join(port.to_string());
            if let Err(e) = force_cleanup_engine_lock(StorageEngineType::RocksDB, &Some(data_dir)).await {
                warn!("Failed to clean up lock files for port {}: {}", port, e);
                println!("===> WARNING: Failed to clean up lock files for port {}", port);
            } else {
                info!("Successfully cleaned up lock files for port {}", port);
                println!("===> Successfully cleaned up lock files for port {}", port);
            }
        }
    }

    // Clear shutdown_tx
    let mut shutdown_tx_guard = shutdown_tx.lock().await;
    if let Some(tx) = shutdown_tx_guard.take() {
        let _ = tx.send(());
        info!("Sent shutdown signal for port {}.", port);
        println!("===> Sent shutdown signal for port {}.", port);
    }
    drop(shutdown_tx_guard);

    // Await daemon handle
    let mut daemon_handle_guard = daemon_handle.lock().await;
    if let Some(handle) = daemon_handle_guard.take() {
        if let Err(e) = timeout(Duration::from_secs(5), handle).await {
            warn!("Failed to await daemon handle for port {}: {}", port, e);
            println!("===> WARNING: Failed to await daemon handle for port {}", port);
        } else {
            info!("Daemon handle for port {} completed.", port);
            println!("===> Daemon handle for port {} completed.", port);
        }
    }
    drop(daemon_handle_guard);

    // Clear daemon port
    let mut daemon_port_guard = daemon_port.lock().await;
    *daemon_port_guard = None;
    drop(daemon_port_guard);

    // Final port check
    if is_port_free(port).await {
        info!("Storage daemon on port {} stopped successfully.", port);
        println!("===> Storage daemon on port {} stopped successfully.", port);
    } else {
        warn!("Storage daemon on port {} may not have stopped completely; port is still in use.", port);
        println!("===> WARNING: Storage daemon on port {} may not have stopped completely; port is still in use.", port);
    }

    Ok(())
}

pub async fn force_cleanup_engine_lock(engine: StorageEngineType, data_directory: &Option<PathBuf>) -> Result<()> {
    info!("Attempting to force cleanup lock files for engine: {:?}", engine);

    let engine_path = match data_directory.as_ref() {
        Some(p) => p.join(daemon_api_storage_engine_type_to_string(&engine)),
        None => {
            warn!("Data directory not specified, skipping cleanup for engine: {:?}", engine);
            return Ok(());
        }
    };

    // Determine the lock file path based on the engine type.
    let lock_file_path = match engine {
        StorageEngineType::RocksDB => engine_path.join("LOCK"),
        StorageEngineType::Sled => engine_path.join("db.lck"),
        _ => {
            warn!("No specific cleanup logic for engine: {:?}", engine);
            return Ok(());
        }
    };

    // Check if the directory is writable
    if let Some(parent) = lock_file_path.parent() {
        if !parent.exists() {
            debug!("Creating directory for engine: {:?}", parent);
            fs::create_dir_all(parent)
                .context(format!("Failed to create directory for engine at {:?}", parent))?;
        }
        // Test write access by attempting to create a temporary file
        let test_file = parent.join(".test_write_access");
        match OpenOptions::new().write(true).create(true).open(&test_file) {
            Ok(_) => {
                debug!("Write access confirmed for directory: {:?}", parent);
                let _ = fs::remove_file(&test_file); // Clean up test file
            }
            Err(e) => {
                warn!("No write access to directory {:?}: {}", parent, e);
                return Err(anyhow!("Insufficient permissions to write to directory {:?}", parent));
            }
        }
    }

    // Retry logic for removing the lock file
    let max_attempts = 10;
    let mut attempt = 1;
    let mut delay = Duration::from_millis(200);

    while lock_file_path.exists() && attempt <= max_attempts {
        info!(
            "Attempt {} of {}: Found lingering lock file for {:?} at {:?}. Attempting to remove it.",
            attempt, max_attempts, engine, lock_file_path
        );
        match fs::remove_file(&lock_file_path) {
            Ok(_) => {
                info!("Successfully removed lock file for {:?} at {:?}", engine, lock_file_path);
                return Ok(());
            }
            Err(e) => {
                warn!(
                    "Attempt {} failed to remove lock file for {:?} at {:?}: {}",
                    attempt, engine, lock_file_path, e
                );
                if attempt == max_attempts && engine == StorageEngineType::RocksDB {
                    info!("Attempting RocksDB repair to release stale lock at {:?}", engine_path);
                    let mut opts = Options::default();
                    opts.create_if_missing(true);
                    match DB::repair(&opts, &engine_path) {
                        Ok(_) => {
                            info!("Successfully repaired RocksDB at {:?}", engine_path);
                            return Ok(());
                        }
                        Err(e) => {
                            warn!("Failed to repair RocksDB at {:?}: {}", engine_path, e);
                            return Err(anyhow!("Failed to repair RocksDB or remove lock file for {:?} at {:?} after {} attempts: {}", engine, lock_file_path, max_attempts, e));
                        }
                    }
                }
                // Wait before retrying, with exponential backoff
                tokio::time::sleep(delay).await;
                delay = delay.checked_mul(2).unwrap_or(Duration::from_millis(5000)); // Cap the delay
                attempt += 1;
            }
        }
    }

    if !lock_file_path.exists() {
        info!("No lock file found for {:?} at {:?}", engine, lock_file_path);
    } else {
        return Err(anyhow!("Lock file for {:?} at {:?} still exists after cleanup attempts.", engine, lock_file_path));
    }

    Ok(())
}

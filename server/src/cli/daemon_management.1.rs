// This file contains logic for daemonizing and managing background processes
// (REST API, Storage, and GraphDB daemons).

use anyhow::{anyhow, Context, Result, Error};
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
use std::time::{Instant};
use clap::ValueEnum;

use tokio::task::JoinHandle;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::net::TcpStream;
use tokio::process::Command as TokioCommand;
use tokio::time::{self, Duration};
use tokio::io::{AsyncWriteExt, ErrorKind};
use log::{info, error, warn, debug};
use sysinfo::{System, Pid}; 
use std::sync::Arc;
use regex::Regex; 
use std::collections::{HashSet, HashMap};
use std::ffi::OsStr; // Add this for OsStr
use std::borrow::Cow; // Add this for Cow

use crate::cli::config::{
    get_default_rest_port_from_config,
    load_storage_config,
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
    StorageConfig as CliStorageConfig,
    DEFAULT_DAEMON_PORT,
    DEFAULT_REST_API_PORT,
    DEFAULT_STORAGE_PORT,
    MAX_CLUSTER_SIZE,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
    PID_FILE_DIR,
    DAEMON_PID_FILE_NAME_PREFIX,
    REST_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_NAME_PREFIX,
    EXECUTABLE_NAME,
    load_main_daemon_config, 
};

use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig};

use daemon_api::{stop_daemon, DaemonError};
use rest_api::start_server as start_rest_server;
use storage_daemon_server::run_storage_daemon as start_storage_server;

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
        .map_err(|_| anyhow::anyhow!("Command '{} {}' timed out after {:?}", command_name, args.join(" "), timeout_duration))?
        .context(format!("Failed to run command '{} {}'", command_name, args.join(" ")))
}  

async fn start_graphdb_daemon_core(port: u16) -> Result<()> {
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

pub async fn handle_internal_daemon_run(
    is_rest_api_run: bool,
    is_storage_daemon_run: bool,
    internal_port: Option<u16>,
    internal_storage_config_path: Option<PathBuf>,
    _internal_storage_engine: Option<StorageEngineType>,
) -> Result<()> {
    if is_rest_api_run {
        let daemon_listen_port = internal_port.unwrap_or_else(get_default_rest_port_from_config);

        let cli_storage_config = load_storage_config(internal_storage_config_path.clone())
            .unwrap_or_else(|e| {
                eprintln!("[DAEMON PROCESS] Warning: Could not load storage config for REST API: {}. Using defaults for CLI config.", e);
                CliStorageConfig::default()
            });

        info!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let (_tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel::<()>();
        let result = start_rest_server(daemon_listen_port, rx_shutdown, cli_storage_config.data_directory.clone()).await;
        if let Err(e) = result {
            error!("[DAEMON PROCESS] REST API server failed: {:?}", e);
        }
        info!("[DAEMON PROCESS] REST API server (daemonized) stopped.");
        Ok(()) 
    } else if is_storage_daemon_run {
        let daemon_listen_port = internal_port.unwrap_or_else(|| {
            load_storage_config(None).map(|c| c.default_port).unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
        });
        let storage_config_path = internal_storage_config_path.unwrap_or_else(|| {
            PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
                .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)
        });

        info!("[DAEMON PROCESS] Starting Storage daemon (daemonized) on port {}...", daemon_listen_port);
        let result = start_storage_server(Some(daemon_listen_port), storage_config_path).await;
        if let Err(e) = result {
            error!("[DAEMON PROCESS] Storage daemon failed: {:?}", e);
        }
        info!("[DAEMON PROCESS] Storage daemon (daemonized) stopped.");
        Ok(()) 
    } else {
        let main_app_config = load_main_daemon_config(None)
            .unwrap_or_else(|e| {
                error!("[DAEMON PROCESS] Could not load main app config: {}. Using default daemon port.", e);
                Default::default()
            });

        let daemon_listen_port = internal_port.unwrap_or(main_app_config.default_port);

        info!("[DAEMON PROCESS] Starting GraphDB Daemon (daemonized) on port {}...", daemon_listen_port);

        let result = start_graphdb_daemon_core(daemon_listen_port).await;
        if let Err(e) = result {
            error!("[DAEMON PROCESS] GraphDB Daemon failed: {:?}", e);
        }
        info!("[DAEMON PROCESS] GraphDB Daemon (daemonized) stopped.");
        Ok(()) 
    }
}

pub async fn start_daemon_process(
    is_rest: bool,
    is_storage: bool,
    port: Option<u16>,
    config_path: Option<PathBuf>,
    engine_type: Option<StorageEngineType>,
) -> Result<()> {
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
    let mut command = TokioCommand::new(&current_exe);

    if is_rest {
        command.arg("--internal-rest-api-run");
    } else if is_storage {
        command.arg("--internal-storage-daemon-run");
    } else {
        command.arg("--internal-daemon-run");
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

    command.stdin(std::process::Stdio::null())
           .stdout(std::process::Stdio::null())
           .stderr(std::process::Stdio::null());

    let child = command.spawn().context("Failed to spawn daemon process")?;
    info!("Daemonized process spawned with PID {}", child.id().unwrap_or(0));

    Ok(())
}

/// Attempts to find a daemon process by checking its internal arguments and performing a TCP health check.
pub async fn find_daemon_process_on_port_with_args(
    port: u16,
    expected_name_for_logging: &str,  
    expected_internal_arg: &str, 
) -> Option<u32> {
    let mut sys = System::new();
    sys.refresh_all();

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
                if time::timeout(Duration::from_secs(1), TcpStream::connect(format!("127.0.0.1:{}", port))).await.is_ok_and(|res| res.is_ok()) {
                    info!("    -> TCP check successful for PID {} (port {}). CONFIRMED.", pid.as_u32(), port);
                    return Some(pid.as_u32());
                } else {
                    warn!("    -> TCP check failed for PID {} (port {}). Process likely not fully listening yet or connection timed out.", pid.as_u32(), port);
                }
            }
        }
    }
    debug!("No process found matching criteria for {} on port {}", expected_name_for_logging, port);
    None
}

/// Helper to get all running daemon processes (main, REST, storage) and their associated ports.
/// This is more efficient for `status` command as it avoids N scans.
pub async fn get_all_daemon_processes_with_ports() -> HashMap<u16, (u32, String)> {
    let mut found_daemons: HashMap<u16, (u32, String)> = HashMap::new();

    let default_ports = vec![
        (DEFAULT_DAEMON_PORT, "main"),
        (DEFAULT_REST_API_PORT, "rest"),
        (DEFAULT_STORAGE_PORT, "storage"),
    ];

    for (port, service_type_str) in default_ports {
        let service_type = match service_type_str {
            "main" => ServiceType::Daemon,
            "rest" => ServiceType::RestAPI,
            "storage" => ServiceType::StorageDaemon,
            _ => continue, 
        };
        
        let pid_file_path = get_pid_file_path_for_service(port, &service_type).await;

        if let Ok(pid) = read_pid_from_file(&pid_file_path).await {
            let mut sys = System::new();
            sys.refresh_all(); 

            if let Some(process) = sys.process(Pid::from_u32(pid)) {
                let process_name_lower = process.name().to_string_lossy().to_lowercase();
                let cli_exe_name_full = std::env::current_exe()
                    .ok()
                    .and_then(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_owned()))
                    .unwrap_or_else(|| "graphdb-cli".to_string());
                let cli_name_pattern = Regex::new(&format!(r"(?i){}|graphdb-c", regex::escape(&cli_exe_name_full)))
                    .expect("Failed to create cli_name_pattern regex");

                let first_cmd_arg_lower = process.cmd().first().map_or("".to_string(), |s| s.to_string_lossy().to_lowercase()); 
                let is_our_cli_process = cli_name_pattern.is_match(&process_name_lower) ||
                                             cli_name_pattern.is_match(&first_cmd_arg_lower);

                if is_our_cli_process {
                    if tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(format!("127.0.0.1:{}", port))).await.is_ok_and(|res| res.is_ok()) {
                        found_daemons.insert(port, (pid, service_type_str.to_string()));
                        debug!("Found {} daemon (PID {}) on port {} via PID file and TCP check.", service_type_str, pid, port);
                    } else {
                        warn!("PID file for {} on port {} exists (PID {}), but TCP check failed. Stale PID file?", service_type_str, port, pid);
                        let _ = delete_pid_file(port, service_type_str).await;
                    }
                } else {
                    warn!("PID file for {} on port {} exists (PID {}), but process name '{}' does not match expected GraphDB pattern. Stale PID file?", service_type_str, port, pid, process_name_lower);
                    let _ = delete_pid_file(port, service_type_str).await;
                }
            } else {
                warn!("PID file for {} on port {} exists (PID {}), but process is not running. Deleting stale PID file.", service_type_str, port, pid);
                let _ = delete_pid_file(port, service_type_str).await;
            }
        } else if let Err(e) = read_pid_from_file(&pid_file_path).await {
            if e.downcast_ref::<std::io::Error>().map_or(false, |io_err| io_err.kind() == ErrorKind::NotFound) {
                debug!("PID file for {} on port {} not found.", service_type_str, port);
            } else {
                error!("Error reading PID file for {} on port {}: {:?}", service_type_str, port, e);
            }
        }
    }

    let mut sys = System::new();
    sys.refresh_all(); 

    let cli_exe_name_full = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_owned()))
        .unwrap_or_else(|| "graphdb-cli".to_string());
    let cli_name_pattern = Regex::new(&format!(r"(?i){}|graphdb-c", regex::escape(&cli_exe_name_full)))
        .expect("Failed to create cli_name_pattern regex");

    for (pid, process) in sys.processes() {
        if found_daemons.values().any(|(p, _)| *p == pid.as_u32()) {
            continue;
        }

        let process_name_lower = process.name().to_string_lossy().to_lowercase();
        let cmd_args: Vec<String> = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        let first_cmd_arg_lower = cmd_args.first().map_or("".to_string(), |s| s.to_lowercase()); 

        let is_our_cli_process = cli_name_pattern.is_match(&process_name_lower) ||
                                     cli_name_pattern.is_match(&first_cmd_arg_lower);

        if is_our_cli_process {
            let mut service_type_found: Option<&str> = None;
            let mut port_found: Option<u16> = None;

            if cmd_args.contains(&"--internal-daemon-run".to_string()) {
                service_type_found = Some("main");
            } else if cmd_args.contains(&"--internal-rest-api-run".to_string()) {
                service_type_found = Some("rest");
            } else if cmd_args.contains(&"--internal-storage-daemon-run".to_string()) {
                service_type_found = Some("storage");
            }

            if let Some(pos) = cmd_args.iter().position(|arg| arg == "--internal-port") {
                if let Some(port_str) = cmd_args.get(pos + 1) {
                    if let Ok(p) = port_str.parse::<u16>() {
                        port_found = Some(p);
                    }
                }
            }

            if let (Some(port), Some(service_type)) = (port_found, service_type_found) {
                if tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(format!("127.0.0.1:{}", port))).await.is_ok_and(|res| res.is_ok()) {
                    found_daemons.insert(port, (pid.as_u32(), service_type.to_string()));
                    debug!("Found {} daemon (PID {}) on port {} via sysinfo scan and TCP check.", service_type, pid, port);
                } else {
                    debug!("TCP check failed for PID {} (port {}) and service_type {} during sysinfo scan. Likely not fully listening yet or timed out.", pid, port, service_type);
                }
            }
        }
    }
    found_daemons
}


pub async fn find_running_storage_daemon_port() -> Option<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await;
    for (port, (_pid, service_type)) in all_daemons {
        if service_type == "storage" {
            return Some(port);
        }
    }
    None
}

pub async fn find_all_running_rest_api_ports(
    _rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, 
    _rest_cluster: Option<String>, 
) -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await;
    let mut running_ports = Vec::new();
    for (port, (_pid, service_type)) in all_daemons {
        if service_type == "rest" {
            running_ports.push(port);
        }
    }
    running_ports.sort_unstable();
    running_ports
}

pub async fn find_all_running_daemon_ports(
    _cluster: Option<String>, 
) -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await;
    let mut running_ports = Vec::new();
    for (port, (_pid, service_type)) in all_daemons {
        if service_type == "main" {
            running_ports.push(port);
        }
    }
    running_ports.sort_unstable();
    running_ports
}

pub async fn find_all_running_storage_daemon_ports(_cluster: Option<String>) -> Vec<u16> {
    let all_daemons = get_all_daemon_processes_with_ports().await;
    let mut running_ports = Vec::new();
    for (port, (_pid, service_type)) in all_daemons {
        if service_type == "storage" {
            running_ports.push(port);
        }
    }
    running_ports.sort_unstable();
    running_ports
}

/// Launches a main GraphDB daemon process. This is for the `daemon start` command.
/// Note: Cluster handling for the main daemon should be managed by the calling CLI logic
/// (e.g., iterating through cluster ports and calling this function for each).
pub fn launch_daemon_process(
    port: u16,
    _cluster: Option<String>, 
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<JoinHandle<()>> {
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

/// Spawns the REST API server in a separate Tokio task.
pub fn spawn_rest_api_server(
    port: u16,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<JoinHandle<()>> {
    let cli_storage_config = match load_storage_config(None) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("[CLI] Warning: Could not load storage config for REST API during spawn: {}. Using defaults.", e);
            CliStorageConfig::default()
        }
    };
    let data_directory = cli_storage_config.data_directory.clone();

    let handle = tokio::spawn(async move {
        info!("REST API server spawned on port {}.", port);
        match start_rest_server(port, shutdown_rx, data_directory).await {
            Ok(_) => info!("REST API server on port {} stopped successfully.", port),
            Err(e) => error!("REST API server on port {} exited with error: {:?}", port, e),
        }
    });
    Ok(handle)
}


/// Spawns the Storage daemon in a separate Tokio task.
pub fn spawn_storage_daemon(
    port: u16,
    config_file: Option<PathBuf>,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<JoinHandle<()>> {
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

/// Stops a managed daemon process by sending a shutdown signal.
pub async fn stop_managed_daemon(
    port: u16,
    daemon_handles: Arc<TokioMutex<std::collections::HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<bool> {
    let mut handles = daemon_handles.lock().await;
    if let Some((handle, tx)) = handles.remove(&port) {
        if tx.send(()).is_ok() {
            info!("Sent shutdown signal to daemon on port {}.", port);
            handle.await.map_err(|e| anyhow::anyhow!(e.to_string()))?; 
            Ok(true)
        } else {
            warn!("Daemon on port {} was already shutting down or stopped.", port);
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

/// Lists all daemon processes found by `sysinfo`.
pub async fn list_daemon_processes() -> Result<Vec<u32>> {
    let mut sys = System::new();
    sys.refresh_all();
    let current_exe_name_full = std::env::current_exe()
        .map(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_owned()))
        .unwrap_or(None)
        .unwrap_or_else(|| "graphdb-cli".to_string());
    let cli_name_pattern = Regex::new(&format!(r"(?i){}|graphdb-c", regex::escape(&current_exe_name_full)))
        .expect("Failed to create cli_name_pattern regex for list_daemon_processes");


    let mut pids = Vec::new();
    for (pid, process) in sys.processes() {
        let process_name_lower = process.name().to_string_lossy().to_lowercase();
        let cmd_args: Vec<String> = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect();
        let first_cmd_arg_lower = cmd_args.first().map_or("".to_string(), |s| s.to_lowercase()); 

        let is_graphdb_process = cli_name_pattern.is_match(&process_name_lower) ||
                                     cli_name_pattern.is_match(&first_cmd_arg_lower) ||
                                     process_name_lower.contains("rest_api_server") || 
                                     process_name_lower.contains("storage_daemon_server"); 

        let is_daemonized = cmd_args.contains(&"--internal-daemon-run".to_string()) ||
                                 cmd_args.contains(&"--internal-rest-api-run".to_string()) ||
                                 cmd_args.contains(&"--internal-storage-daemon-run".to_string());

        if is_graphdb_process && is_daemonized {
            pids.push(pid.as_u32());
        }
    }
    Ok(pids)
}

/// Kills a daemon process by PID.
pub fn kill_daemon_process(pid: u32) -> Result<()> {
    let mut sys = System::new();
    sys.refresh_all(); 
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
    let all_daemons = get_all_daemon_processes_with_ports().await;
    all_daemons.get(&port).map_or(false, |(_pid, service_type)| service_type == "main")
}

pub async fn is_rest_api_running(port: u16) -> bool {
    let all_daemons = get_all_daemon_processes_with_ports().await;
    all_daemons.get(&port).map_or(false, |(_pid, service_type)| service_type == "rest")
}

pub async fn is_storage_daemon_running(port: u16) -> bool {
    let all_daemons = get_all_daemon_processes_with_ports().await;
    all_daemons.get(&port).map_or(false, |(_pid, service_type)| service_type == "storage")
}

pub async fn is_port_free(port: u16) -> bool {
    tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await.is_ok()
}

pub fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    stop_daemon().map_err(|e| anyhow::anyhow!("Daemon API error: {:?}", e))
}

pub async fn clear_all_daemon_processes() -> Result<(), anyhow::Error> {
    println!("Attempting to clear all GraphDB daemon processes...");

    let stop_result = stop_daemon_api_call();
    match stop_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }

    let mut sys = System::new();
    sys.refresh_all();

    let current_exe_name_full = std::env::current_exe()
        .map(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_owned()))
        .unwrap_or(None)
        .unwrap_or_else(|| "graphdb-cli".to_string());
    let cli_name_pattern = Regex::new(&format!(r"(?i){}|graphdb-c", regex::escape(&current_exe_name_full)))
        .expect("Failed to create cli_name_pattern regex for clear_all_daemon_processes");


    let processes_to_kill: Vec<(u32, String)> = sys.processes().iter()
        .filter_map(|(pid, process)| {
            let process_name_lower = process.name().to_string_lossy().to_lowercase();
            let cmd_args: Vec<String> = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect();
            let first_cmd_arg_lower = cmd_args.first().map_or("".to_string(), |s| s.to_lowercase()); 

            let is_graphdb_process = cli_name_pattern.is_match(&process_name_lower) ||
                                         cli_name_pattern.is_match(&first_cmd_arg_lower) ||
                                         process_name_lower.contains("rest_api_server") ||
                                         process_name_lower.contains("storage_daemon_server");

            let is_daemonized = cmd_args.contains(&"--internal-daemon-run".to_string()) ||
                                 cmd_args.contains(&"--internal-rest-api-run".to_string()) ||
                                 cmd_args.contains(&"--internal-storage-daemon-run".to_string());

            if is_graphdb_process && is_daemonized {
                Some((pid.as_u32(), process.name().to_string_lossy().into_owned()))
            } else {
                None
            }
        })
        .collect();

    if processes_to_kill.is_empty() {
        println!("No GraphDB daemon processes found to kill.");
    } else {
        for (pid, name) in processes_to_kill {
            println!("Killing process {} ({}) with SIGTERM...", pid, name);
            match nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), nix::sys::signal::Signal::SIGTERM) {
                Ok(_) => println!("Successfully sent SIGTERM to PID {}.", pid),
                Err(e) => eprintln!("Failed to send SIGTERM to PID {}: {}", pid, e),
            }
        }
    }

    Ok(())
}

// New public wrapper for stopping a specific main daemon instance
pub async fn stop_specific_main_daemon(port: u16, should_reclaim: bool) -> Result<()> {
    println!("Attempting to stop GraphDB Daemon on port {}...", port);
    stop_daemon_by_pid_or_scan("GraphDB Daemon", port, "main", should_reclaim).await
}

// New public wrapper for stopping a specific REST API daemon instance
pub async fn stop_specific_rest_api_daemon(port: u16, should_reclaim: bool) -> Result<()> {
    println!("Attempting to stop REST API on port {}...", port);
    stop_daemon_by_pid_or_scan("REST API daemon", port, "rest", should_reclaim).await
}

// New public wrapper for stopping a specific Storage daemon instance
pub async fn stop_specific_storage_daemon(port: u16, should_reclaim: bool) -> Result<()> {
    println!("Attempting to stop Storage daemon on port {}...", port);
    stop_daemon_by_pid_or_scan("Storage daemon", port, "storage", should_reclaim).await
}

async fn write_pid_file(service_port: u16, pid: u32, service_type: &str) -> Result<()> {
    let _pid_file = match service_type {
        "main" => get_pid_file_path(service_port),
        "rest" => get_rest_pid_file_path(service_port),
        "storage" => get_storage_pid_file_path(service_port), 
        _ => return Err(anyhow!("Invalid service type: {}", service_type)),
    };
    tokio::fs::write(&_pid_file, pid.to_string()).await
        .with_context(|| format!("Failed to write PID {} for {} service to {:?}", pid, service_type, _pid_file))?;
    info!("Wrote PID {} for {} service to {:?}", pid, service_type, _pid_file);
    Ok(())
}

async fn read_pid_from_file(path: &PathBuf) -> Result<u32, anyhow::Error> {
    let pid_str = tokio::fs::read_to_string(path).await
        .with_context(|| format!("Failed to read PID from file: {:?}", path))?;
    let pid = pid_str.trim().parse::<u32>()
        .with_context(|| format!("Failed to parse PID from file: '{}'", pid_str))?;
    Ok(pid)
}

async fn delete_pid_file(service_port: u16, service_type: &str) -> Result<()> {
    let _pid_file = match service_type {
        "main" => get_pid_file_path(service_port),
        "rest" => get_rest_pid_file_path(service_port),
        "storage" => get_storage_pid_file_path(service_port), 
        _ => return Err(anyhow!("Invalid service type: {}", service_type)),
    };
    if _pid_file.exists() {
        tokio::fs::remove_file(&_pid_file).await
            .with_context(|| format!("Failed to delete {} PID file: {:?}", service_type, _pid_file))?;
        info!("Deleted {} PID file: {:?}", service_type, _pid_file);
    }
    Ok(())
}

fn get_pid_file_path(port: u16) -> PathBuf {
    std::env::temp_dir().join(format!("graphdb_daemon_{}.pid", port))
}

fn get_rest_pid_file_path(port: u16) -> PathBuf {
    std::env::temp_dir().join(format!("graphdb_rest_{}.pid", port))
}

fn get_storage_pid_file_path(port: u16) -> PathBuf {
    std::env::temp_dir().join(format!("graphdb_storage_{}.pid", port))
}

async fn is_process_running(pid: u32) -> bool {
    let mut sys = System::new();
    sys.refresh_all();
    sys.process(Pid::from_u32(pid)).is_some()
}

#[derive(Clone, Copy)] 
enum ServiceType {
    Daemon,
    RestAPI,
    StorageDaemon,
}

// Helper to get PID file path based on ServiceType
async fn get_pid_file_path_for_service(port: u16, service_type: &ServiceType) -> PathBuf {
    match service_type {
        ServiceType::Daemon => get_pid_file_path(port),
        ServiceType::RestAPI => get_rest_pid_file_path(port),
        ServiceType::StorageDaemon => get_storage_pid_file_path(port),
    }
}

// Helper to read PID from file for a given ServiceType
async fn read_pid_file(port: u16, service_type: &ServiceType) -> Result<u32, anyhow::Error> {
    let path = get_pid_file_path_for_service(port, service_type).await;
    read_pid_from_file(&path).await
}

// Helper to remove PID file for a given ServiceType
async fn remove_pid_file(port: u16, service_type: &ServiceType) -> Result<()> {
    let path = get_pid_file_path_for_service(port, service_type).await;
    if path.exists() {
        tokio::fs::remove_file(&path).await
            .with_context(|| format!("Failed to delete PID file: {:?}", path))?;
        info!("Deleted PID file: {:?}", path);
    }
    Ok(())
}

pub async fn stop_daemon_by_pid_or_scan(
    daemon_name: &str,
    port: u16,
    service_type_str: &str, 
    should_reclaim: bool,
) -> Result<()> {
    let service_type = match service_type_str {
        "main" => ServiceType::Daemon,
        "rest" => ServiceType::RestAPI,
        "storage" => ServiceType::StorageDaemon,
        _ => return Err(anyhow!("Invalid service type: {}", service_type_str)),
    };

    let mut pid_from_file: Option<u32> = None;
    if let Ok(pid_read) = read_pid_file(port, &service_type).await {
        if is_process_running(pid_read).await {
            pid_from_file = Some(pid_read);
        } else {
            warn!("PID file for {} on port {} exists (PID {}), but process is not running.", daemon_name, port, pid_read);
            remove_pid_file(port, &service_type).await?;
        }
    }

    let target_pid = if let Some(pid) = pid_from_file {
        Some(pid)
    } else {
        let expected_arg = match service_type {
            ServiceType::Daemon => "--internal-daemon-run",
            ServiceType::RestAPI => "--internal-rest-api-run",
            ServiceType::StorageDaemon => "--internal-storage-daemon-run",
        };
        find_daemon_process_on_port_with_args(port, daemon_name, expected_arg).await
    };

    if let Some(pid) = target_pid {
        if !should_reclaim && !is_port_free(port).await {
            return Err(anyhow!("Port {} is in use by PID {}. Use --join-cluster or a different port.", port, pid));
        }
        info!("Stopping {} (PID {}) on port {}...", daemon_name, pid, port);
        
        // Create a new System instance for the current state, avoiding borrow conflict
        let mut sys = System::new_all(); 
        sys.refresh_all(); 

        if let Some(process) = sys.process(Pid::from_u32(pid)) {
            if process.kill_with(sysinfo::Signal::Term).unwrap_or(false) {
                let mut attempts = 0;
                while is_process_running(pid).await && attempts < 5 {
                    time::sleep(Duration::from_secs(1)).await;
                    attempts += 1;
                    // is_process_running already refreshes its own sysinfo, so no need to refresh here
                }
                if is_process_running(pid).await {
                    error!("Failed to stop {} (PID {}) on port {}: Process still running after SIGTERM.", daemon_name, pid, port);
                    // Re-fetch process after potential refresh in is_process_running if needed for kill_with
                    let mut sys_force_kill = System::new_all();
                    sys_force_kill.refresh_all();
                    if let Some(process_force_kill) = sys_force_kill.process(Pid::from_u32(pid)) {
                        if process_force_kill.kill_with(sysinfo::Signal::Kill).unwrap_or(false) {
                            time::sleep(Duration::from_secs(1)).await; 
                            if is_process_running(pid).await {
                                return Err(anyhow!("Failed to force stop {} (PID {}) on port {}.", daemon_name, pid, port));
                            }
                        } else {
                            return Err(anyhow!("Failed to send SIGKILL to {} (PID {}) on port {}.", daemon_name, pid, port));
                        }
                    } else {
                        warn!("Process {} (PID {}) not found for force kill attempt.", daemon_name, pid);
                    }
                }
                remove_pid_file(port, &service_type).await?;
                info!("Successfully stopped {} (PID {}) on port {}.", daemon_name, pid, port);
            } else {
                error!("Failed to send termination signal to {} (PID {}) on port {}.", daemon_name, pid, port);
                return Err(anyhow!("Failed to terminate {} (PID {}) on port {}.", daemon_name, pid, port));
            }
        } else {
            warn!("Service {} (PID {}) on port {} not found after initial scan.", daemon_name, pid, port);
            remove_pid_file(port, &service_type).await?; 
        }
    } else {
        info!("No {} found on port {}.", daemon_name, port);
        if let Ok(_) = read_pid_file(port, &service_type).await {
             remove_pid_file(port, &service_type).await?; 
        }
    }
    Ok(())
}

pub fn load_storage_config_path_or_default(path: Option<PathBuf>) -> Result<CliStorageConfig> {
    load_storage_config(path)
}

/// Helper to check if a process is running and listening on a given TCP port using lsof.
pub async fn check_process_status_by_port(_process_name: &str, port: u16) -> bool {
    let output_result = run_command_with_timeout(
        "lsof",
        &["-i", &format!("TCP:{}", port), "-s", "TCP:LISTEN", "-t"],
        Duration::from_secs(3), 
    ).await;

    if let Ok(output) = output_result {
        let pids = String::from_utf8_lossy(&output.stdout);
        if !pids.trim().is_empty() {
            return true;
        }
    }
    false
}

pub fn parse_cluster_range(range_str: &str) -> Result<Vec<u16>> {
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
    } else {
        match range_str.parse::<u16>() {
            Ok(port) => Ok(vec![port]),
            Err(_) => Err(anyhow!(
                "Invalid cluster range size. Expected 'start-end' or a single port: {}",
                range_str
            )),
        }
    }
}

pub async fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> {
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    
    let output = run_command_with_timeout(
        "lsof",
        &["-i", &format!(":{}", port), "-t"],
        Duration::from_secs(3), 
    ).await?;

    let pids = String::from_utf8_lossy(&output.stdout);
    let pids: Vec<i32> = pids.trim().lines().filter_map(|s| s.parse::<i32>().ok()).collect();

    if pids.is_empty() {
        println!("No {} process found running on port {}.", process_name, port);
        return Ok(());
    }

    for pid in pids {
        println!("Killing process {} (for {} on port {})...", pid, process_name, port);
        match TokioCommand::new("kill").arg("-9").arg(pid.to_string()).status().await {
            Ok(status) if status.success() => println!("Process {} killed successfully.", pid),
            Ok(_) => eprintln!("Failed to kill process {}.", pid),
            Err(e) => eprintln!("Error killing process {}: {}", pid, e),
        }
    }

    let start_time = Instant::now();
    let wait_timeout = Duration::from_secs(5); 
    let poll_interval = Duration::from_millis(200);

    while start_time.elapsed() < wait_timeout {
        if is_port_free(port).await {
            println!("Port {} is now free.", port);
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    Err(anyhow::anyhow!("Port {} remained in use after killing processes within {:?}.", port, wait_timeout))
}

/// Checks if a given port on localhost is actively listening.
/// This is done by attempting to establish a TCP connection.
pub async fn is_port_listening(port: u16) -> bool {
    tokio::time::timeout(Duration::from_secs(1), async {
        TcpStream::connect(format!("127.0.0.1:{}", port)).await.is_ok()
    }).await.unwrap_or(false)
}

/// Helper function to find a process by a given port and a keyword in its command line.
/// This is a basic approach and might need refinement for more robust PID finding.
pub fn find_process_by_port(port: u16, keyword_in_cmd: &str, system: &System) -> Option<Pid> {
    for (pid, process) in system.processes() {
        // Convert OsStr slices to Cow<str> for string operations that require UTF-8
        let cmd_line_parts: Vec<Cow<str>> = process.cmd().iter().map(|s| s.to_string_lossy()).collect();
        let cmd_line = cmd_line_parts.join(" ");

        if cmd_line.contains(keyword_in_cmd) && cmd_line.contains(&format!("--port {}", port)) {
            return Some(*pid);
        }
    }
    None
}

pub async fn is_daemon_running(port: u16) -> bool {
    !is_port_free(port).await 
}

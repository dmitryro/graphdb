// server/src/cli/daemon_management.rs
// This file contains logic for daemonizing and managing background processes
// (REST API, Storage, and GraphDB daemons).

use anyhow::{anyhow, Context, Result, Error};
use clap::ValueEnum;
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
use std::time::{Instant};
use std::sync::Arc;
use std::collections::{HashSet, HashMap};
use std::ffi::OsStr;
use std::borrow::Cow;
use std::fmt;
use regex::Regex;
use sysinfo::{System, Pid, Process, ProcessStatus, ProcessesToUpdate};
use tokio::task::JoinHandle;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::net::TcpStream;
use tokio::process::Command as TokioCommand;
use tokio::time::{self, Duration};
use tokio::io::{AsyncWriteExt, ErrorKind};
use log::{info, error, warn, debug};
use chrono::Utc;
use serde::{Serialize, Deserialize};
use nix::unistd::Pid as NixPid;

use crate::cli::config::{
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
};
use crate::cli::daemon_registry::{DaemonRegistry, DaemonMetadata};
pub use crate::cli::daemon_registry::{GLOBAL_DAEMON_REGISTRY};
use lib::storage_engine::config::{StorageEngineType};
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
        .map_err(|_| anyhow!("Command '{} {}' timed out after {:?}", command_name, args.join(" "), timeout_duration))?
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

/// Handles the internal execution of daemonized processes (REST API or Storage).
/// This function is called when the CLI is invoked with `--internal-rest-api-run` or `--internal-storage-daemon-run`.
pub async fn handle_internal_daemon_run(
    is_rest_api_run: bool,
    is_storage_daemon_run: bool,
    internal_port: Option<u16>,
    internal_storage_config_path: Option<PathBuf>,
    _internal_storage_engine: Option<StorageEngineType>,
) -> Result<()> {
    if is_rest_api_run {
        let daemon_listen_port = internal_port.unwrap_or_else(get_default_rest_port_from_config);

        let cli_storage_config = load_storage_config(
            internal_storage_config_path
                .as_ref()
                .and_then(|path| path.to_str())
        )
        .unwrap_or_else(|e| {
            eprintln!("[DAEMON PROCESS] Warning: Could not load storage config for REST API: {}. Using defaults for CLI config.", e);
            StorageConfig::default()
        });

        info!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let (_tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel::<()>();
        
        let result = start_rest_server(
            daemon_listen_port,
            rx_shutdown,
            cli_storage_config.data_directory.to_string_lossy().into_owned(),
        ).await;

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

/// Spawns a daemon process (REST, Storage, or Main) and writes its PID to the DaemonRegistry.
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

    if let Some(path) = config_path.clone() {
        command.arg("--internal-storage-config-path").arg(path);
    }

    if let Some(ref engine) = engine_type {
        command.arg("--internal-storage-engine")
               .arg(engine.to_possible_value().unwrap().get_name());
    }

    command.stdin(std::process::Stdio::null())
           .stdout(std::process::Stdio::null())
           .stderr(std::process::Stdio::null());

    let child = command.spawn().context("Failed to spawn daemon process")?;
    let spawned_pid = child.id().unwrap_or(0);
    info!("Daemonized process spawned with PID {}", spawned_pid);

    let service_type_str = if is_rest {
        "rest"
    } else if is_storage {
        "storage"
    } else {
        "main"
    };

    let actual_port = port.unwrap_or_else(|| {
        if is_rest {
            get_default_rest_port_from_config()
        } else if is_storage {
            load_storage_config(None)
                .map(|c| c.default_port)
                .unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
        } else {
            load_main_daemon_config(None)
                .map(|c| c.default_port)
                .unwrap_or(DEFAULT_DAEMON_PORT)
        }
    });

    let metadata = DaemonMetadata {
        service_type: service_type_str.to_string(),
        port: actual_port,
        pid: spawned_pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: None,
        config_path,
        engine_type: engine_type
            .as_ref()
            .map(|e| e.to_possible_value().unwrap().get_name().to_string()),
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await?;
    Ok(())
}

/// Helper function to find a running storage daemon's port.
/// Scans a range of common ports using `lsof`.
pub async fn find_running_storage_daemon_port() -> Option<u16> {
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    all_daemons.iter()
        .filter(|metadata| metadata.service_type == "storage")
        .map(|metadata| metadata.port)
        .next()
}

/// Calls the `daemon_api::stop_daemon` function.
/// Renamed to avoid name collision with `commands::DaemonCliCommand::Stop`.
pub fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    stop_daemon().map_err(|e| anyhow!("Daemon API error: {:?}", e))
}

/// Attempts to find a daemon process by checking its internal arguments and performing a TCP health check.
/// This function is primarily used by `stop_daemon_by_pid_or_scan` as a fallback.
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
/// This version prioritizes DaemonRegistry and falls back to sysinfo/lsof scan for verification.
pub async fn get_all_daemon_processes_with_ports() -> HashMap<u16, (u32, String)> {
    let mut found_daemons: HashMap<u16, (u32, String)> = HashMap::new();
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);

    let cli_exe_name_full = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().and_then(|s| s.to_str()).map(|s| s.to_owned()))
        .unwrap_or_else(|| "graphdb-cli".to_string());
    
    let cli_name_pattern = Regex::new(&format!(r"(?i){}|graphdb-c", regex::escape(&cli_exe_name_full)))
        .expect("Failed to create cli_name_pattern regex");

    let registered_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();

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
            GLOBAL_DAEMON_REGISTRY.unregister_daemon(metadata.port).await.ok();
        }
    }

    let storage_cli_config = load_storage_config(None)
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

    let graphdb_component_names = vec![
        cli_exe_name_full.as_str(),
        "graphdb-c",
        "rest_api_server",
        "storage_daemon_server",
    ];

    for (pid, process) in sys.processes() {
        let l_port_opt = get_listening_port_for_pid(pid.as_u32()).await;

        if let Some(l_port) = l_port_opt {
            if found_daemons.contains_key(&l_port) {
                debug!("Secondary scan: Process PID {} on port {} already found via registry. Skipping.", pid, l_port);
                continue;
            }

            let process_name_lower = process.name().to_string_lossy().to_lowercase();
            let cmd_args: Vec<String> = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect();
            let full_cmd_line = cmd_args.join(" ");

            debug!("  -> Secondary scan: Process PID {} is listening on port {}. Name='{:?}'", pid, l_port, process.name());
            debug!("     Full command line args for PID {}: '{}'", pid, full_cmd_line);

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
                if graphdb_component_names.iter().any(|name| process_name_lower.contains(name)) {
                    if process_name_lower.contains("rest_api_server") {
                        service_type_found = Some("rest");
                    } else if process_name_lower.contains("storage_daemon_server") {
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
                if found_daemons.insert(l_port, (pid.as_u32(), final_service_type.to_string())).is_none() {
                    info!("Found {} daemon (PID {}) on port {} via secondary sysinfo/lsof scan.", final_service_type, pid, l_port);
                    let new_metadata = DaemonMetadata {
                        service_type: final_service_type.to_string(),
                        port: l_port,
                        pid: pid.as_u32(),
                        ip_address: "127.0.0.1".to_string(),
                        data_dir: None,
                        config_path: None,
                        engine_type: None,
                        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                    };
                    GLOBAL_DAEMON_REGISTRY.register_daemon(new_metadata).await.ok();
                }
            } else {
                debug!("Secondary scan: Process PID {} listening on port {} not classified as known GraphDB daemon type ('{}') or port mismatch (arg: {:?}, actual: {}).", pid, l_port, final_service_type, internal_arg_port, l_port);
            }
        } else {
            debug!("Process PID {} is not listening on any TCP port.", pid);
        }
    }
    found_daemons
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
            StorageConfig::default()
        }
    };
    let data_directory = cli_storage_config.data_directory.clone();

    let handle = tokio::spawn(async move {
        info!("REST API server spawned on port {}.", port);
 
        match start_rest_server(
            port,
            shutdown_rx,
            data_directory.to_string_lossy().into_owned(), // <--- fix here
        ).await {
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
    _shutdown_rx: oneshot::Receiver<()>,
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

/// Lists all daemon processes found by `sysinfo`.
pub async fn list_daemon_processes() -> Result<Vec<u32>> {
    let all_daemons = get_all_daemon_processes_with_ports().await;
    Ok(all_daemons.values().map(|(pid, _)| *pid).collect())
}

/// Kills a daemon process by PID.
pub fn kill_daemon_process(pid: u32) -> Result<()> {
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

pub async fn clear_all_daemon_processes() -> Result<(), anyhow::Error> {
    println!("Attempting to clear all GraphDB daemon processes...");

    let stop_result = stop_daemon_api_call();
    match stop_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }

    let registered_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let mut pids_to_kill = Vec::new();
    for metadata in registered_daemons {
        let is_running = is_process_running(metadata.pid).await;
        let age = Utc::now().signed_duration_since(chrono::DateTime::<Utc>::from_timestamp_nanos(metadata.last_seen_nanos));
        if !is_running && age > chrono::Duration::seconds(300) {
            warn!("Removing stale registry entry for {} daemon on port {} (PID {}).", metadata.service_type, metadata.port, metadata.pid);
            GLOBAL_DAEMON_REGISTRY.unregister_daemon(metadata.port).await.ok();
            continue;
        }
        if is_running {
            pids_to_kill.push((metadata.pid, metadata.port.to_string()));
        }
    }

    if pids_to_kill.is_empty() {
        println!("No GraphDB daemon processes found to kill.");
    } else {
        for (pid, port_str) in pids_to_kill {
            println!("Killing process {} (port {})...", pid, port_str);
            match nix::sys::signal::kill(NixPid::from_raw(pid as i32), nix::sys::signal::Signal::SIGTERM) {
                Ok(_) => println!("Successfully sent SIGTERM to PID {}.", pid),
                Err(e) => eprintln!("Failed to send SIGTERM to PID {}: {}", pid, e),
            }
            if let Ok(port) = port_str.parse::<u16>() {
                GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await.ok();
            }
        }
    }

    GLOBAL_DAEMON_REGISTRY.clear_all_daemons().await?;
    Ok(())
}

pub async fn stop_specific_main_daemon(port: u16, should_reclaim: bool) -> Result<()> {
    println!("Attempting to stop GraphDB Daemon on port {}...", port);
    stop_daemon_by_pid_or_scan("GraphDB Daemon", port, "main", should_reclaim).await
}

pub async fn stop_specific_rest_api_daemon(port: u16, should_reclaim: bool) -> Result<()> {
    println!("Attempting to stop REST API on port {}...", port);
    stop_daemon_by_pid_or_scan("REST API daemon", port, "rest", should_reclaim).await
}

pub async fn stop_specific_storage_daemon(port: u16, should_reclaim: bool) -> Result<()> {
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
enum ServiceType {
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

async fn read_pid_file(port: u16, service_type: &ServiceType) -> Result<u32, anyhow::Error> {
    let path = match service_type {
        ServiceType::Daemon => std::env::temp_dir().join(format!("graphdb_daemon_{}.pid", port)),
        ServiceType::RestAPI => std::env::temp_dir().join(format!("graphdb_rest_{}.pid", port)),
        ServiceType::StorageDaemon => std::env::temp_dir().join(format!("graphdb_storage_{}.pid", port)),
    };
    read_pid_from_file(&path).await
}

async fn remove_pid_file(port: u16, service_type: &ServiceType) -> Result<()> {
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
) -> Result<()> {
    let service_type = match service_type_str {
        "main" => ServiceType::Daemon,
        "rest" => ServiceType::RestAPI,
        "storage" => ServiceType::StorageDaemon,
        _ => return Err(anyhow!("Invalid service type: {}", service_type_str)),
    };

    let metadata_from_registry = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?;
    let mut target_pid: Option<u32> = None;

    if let Some(metadata) = metadata_from_registry {
        let is_running = is_process_running(metadata.pid).await;
        let age = Utc::now().signed_duration_since(chrono::DateTime::<Utc>::from_timestamp_nanos(metadata.last_seen_nanos));
        if is_running {
            target_pid = Some(metadata.pid);
            info!("Found {} (PID {}) on port {} in registry.", daemon_name, metadata.pid, port);
        } else if age > chrono::Duration::seconds(300) {
            warn!("Registry entry for {} on port {} (PID {}) is stale. Unregistering.", daemon_name, port, metadata.pid);
            GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await.ok();
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
            GLOBAL_DAEMON_REGISTRY.register_daemon(new_metadata).await.ok();
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
                let _wait_timeout = Duration::from_secs(5);
                let mut attempts = 0;
                while is_process_running(pid).await && attempts < 5 {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    attempts += 1;
                }
                if is_process_running(pid).await {
                    error!("Failed to stop {} (PID {}) on port {}: Process still running after SIGTERM.", daemon_name, pid, port);
                    let mut sys_force_kill = System::new();
                    sys_force_kill.refresh_processes(ProcessesToUpdate::All, true);
                    if let Some(process_force_kill) = sys_force_kill.process(Pid::from_u32(pid)) {
                        if process_force_kill.kill_with(sysinfo::Signal::Kill).unwrap_or(false) {
                            tokio::time::sleep(Duration::from_secs(1)).await;
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
                GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await?;
                remove_pid_file(port, &service_type).await?;
                info!("Successfully stopped {} (PID {}) on port {}.", daemon_name, pid, port);
            } else {
                error!("Failed to send termination signal to {} (PID {}) on port {}.", daemon_name, pid, port);
                return Err(anyhow!("Failed to terminate {} (PID {}) on port {}.", daemon_name, pid, port));
            }
        } else {
            warn!("Service {} (PID {}) on port {} not found after initial scan.", daemon_name, pid, port);
            GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await?;
            remove_pid_file(port, &service_type).await?;
        }
    } else {
        info!("No {} found on port {}.", daemon_name, port);
        GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await.ok();
        remove_pid_file(port, &service_type).await?;
    }
    Ok(())
}

pub fn load_storage_config_path_or_default(path: Option<PathBuf>) -> Result<StorageConfig> {
    let path_owned: Option<String> = path.map(|p| p.to_string_lossy().into_owned());
    let path_str: Option<&str> = path_owned.as_deref();
    load_storage_config(path_str)
}

pub async fn check_process_status_by_port(_process_name: &str, port: u16) -> bool {
    let metadata = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await.ok().flatten();
    if let Some(m) = metadata {
        is_process_running(m.pid).await && is_port_listening(m.port).await
    } else {
        false
    }
}

pub fn parse_cluster_range(range_str: &str) -> Result<Vec<u16>> {
    // Handle null or empty input
    if range_str.is_empty() || range_str == "null" {
        return Err(anyhow!("Cluster range is empty or null"));
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
        Ok(vec![port])
    } else {
        Err(anyhow!(
            "Invalid cluster range format. Expected 'start-end' or a single port: {}",
            range_str
        ))
    }
}

pub async fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> {
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    
    let service_type_str = if process_name.contains("Storage") { "storage" }
                           else if process_name.contains("REST") { "rest" }
                           else { "main" };

    let stop_result = stop_daemon_by_pid_or_scan(process_name, port, service_type_str, true).await;

    if stop_result.is_ok() {
        println!("Process for {} on port {} stopped successfully via registry/PID based method.", process_name, port);
        return Ok(());
    } else {
        eprintln!("Registry/PID based stop failed for {} on port {}: {:?}", process_name, port, stop_result.err());
        println!("Falling back to direct lsof/kill -9 for {} on port {}...", process_name, port);
    }

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
    let _wait_timeout = Duration::from_secs(5);
    let poll_interval = Duration::from_millis(200);

    while start_time.elapsed() < _wait_timeout {
        if is_port_free(port).await {
            println!("Port {} is now free.", port);
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    Err(anyhow!("Port {} remained in use after killing processes within {:?}", port, _wait_timeout))
}

pub async fn is_port_listening(port: u16) -> bool {
    tokio::time::timeout(Duration::from_secs(1), async {
        TcpStream::connect(format!("127.0.0.1:{}", port)).await.is_ok()
    }).await.unwrap_or(false)
}

pub fn find_process_by_port(port: u16, keyword_in_cmd: &str, system: &System) -> Option<Pid> {
    for (pid, process) in system.processes() {
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

pub async fn is_process_running(pid: u32) -> bool {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All, true);
    sys.process(Pid::from_u32(pid)).is_some()
}

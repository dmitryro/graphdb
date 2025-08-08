use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use std::path::PathBuf;
use std::process;
use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};
use std::sync::{Arc, LazyLock};
use std::collections::{HashSet, HashMap};
use std::fmt;
use regex::Regex;
use sysinfo::{System, Pid, Process, ProcessStatus, ProcessesToUpdate};
use tokio::task::JoinHandle;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::net::TcpStream;
use tokio::process::{Command as TokioCommand, Child};
use tokio::time::{self, Duration as TokioDuration};
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader, ErrorKind};
use log::{info, error, warn, debug};
use chrono::Utc;
use nix::unistd::Pid as NixPid;
use nix::sys::signal::{kill, Signal};
use daemon_api::{start_daemon, stop_daemon, stop_port_daemon, DaemonError};
use rest_api::start_server as start_rest_server;
use storage_daemon_server::run_storage_daemon as start_storage_server;
use futures::future;
use std::process::Stdio;
use tokio::process::Command;
use std::os::unix::process::ExitStatusExt;
use std::fs;

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
    load_rest_config,
};
use daemon_api::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::storage_engine::config::StorageEngineType;

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

// Helper function to check if a string is a valid cluster range (e.g., "8081-8084")
fn is_valid_cluster_range(range: &str) -> bool {
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
/// This version is now more robust against race conditions where the process
/// is killed but a subsequent check fails.
pub async fn stop_process_by_port(
    process_name: &str,
    port: u16
) -> Result<(), anyhow::Error> {
    println!("Attempting to stop {} on port {}...", process_name, port);
    let service_type_str = if process_name.contains("Storage") { "storage" }
        else if process_name.contains("REST") { "rest" }
        else { "main" };
    let service_type = ServiceType::from_str(service_type_str, true)
        .map_err(|e| anyhow!("Invalid service type: {}", e))?;
    
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
    
    let mut pid_to_stop: Option<u32> = metadata.map(|meta| meta.pid);
    if pid_to_stop.is_none() {
        pid_to_stop = find_pid_by_port(port).await;
    }

    if let Some(pid) = pid_to_stop {
        let nix_pid = NixPid::from_raw(pid as i32);
        
        if let Err(e) = kill(nix_pid, Signal::SIGTERM) {
            error!("Failed to send SIGTERM to PID {}: {}", pid, e);
        } else {
            println!("Sent SIGTERM to PID {} for {} on port {}.", pid, process_name, port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(5);
            let poll_interval = Duration::from_millis(200);
            while start_time.elapsed() < wait_timeout {
                if is_port_free(port).await {
                    break;
                }
                tokio::time::sleep(poll_interval).await;
            }

            if is_process_running(pid).await {
                warn!("Process {} (PID {}) on port {} still running after SIGTERM. Sending SIGKILL.", process_name, pid, port);
                if let Err(e) = kill(nix_pid, Signal::SIGKILL) {
                    error!("Failed to send SIGKILL to PID {}: {}", pid, e);
                } else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if is_process_running(pid).await {
                        return Err(anyhow!("Failed to stop {} (PID {}) on port {}.", process_name, pid, port));
                    }
                }
            }
        }
    } else {
        println!("No process found for {} on port {}.", process_name, port);
    }

    if let Err(e) = tokio::time::timeout(
        Duration::from_secs(2),
        GLOBAL_DAEMON_REGISTRY.unregister_daemon(port),
    ).await {
        error!("Failed to unregister daemon for port {}: {:?}", port, e);
    }
    remove_pid_file(port, &service_type).await?;

    if is_port_free(port).await {
        println!("Port {} is now free.", port);
        Ok(())
    } else {
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

async fn start_graphdb_daemon_core(port: u16) -> Result<(), anyhow::Error> {
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

pub async fn handle_internal_daemon_run(
    is_rest_api_run: bool,
    is_storage_daemon_run: bool,
    internal_port: Option<u16>,
    internal_storage_config_path: Option<PathBuf>,
    _internal_storage_engine: Option<StorageEngineType>,
) -> Result<(), anyhow::Error> {
    if is_rest_api_run {
        let daemon_listen_port = internal_port.unwrap_or_else(get_default_rest_port_from_config);
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel::<()>();
        let rest_api_shutdown_tx_opt = Arc::new(TokioMutex::new(Some(tx_shutdown)));
        let rest_api_port_arc = Arc::new(TokioMutex::new(None));
        let rest_api_handle = Arc::new(TokioMutex::new(None));

        info!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let result = crate::cli::handlers::start_rest_api_interactive(
            Some(daemon_listen_port),
            None,
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
        ).await;

        if let Err(e) = result {
            error!("[DAEMON PROCESS] REST API server failed: {:?}", e);
            return Err(e);
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
        let storage_daemon_shutdown_tx_opt = Arc::new(TokioMutex::new(None));
        let storage_daemon_port_arc = Arc::new(TokioMutex::new(None));
        let storage_daemon_handle = Arc::new(TokioMutex::new(None));

        info!("[DAEMON PROCESS] Starting Storage daemon (daemonized) on port {}...", daemon_listen_port);
        let result = crate::cli::handlers::start_storage_interactive(
            Some(daemon_listen_port),
            Some(storage_config_path),
            None,
            storage_daemon_shutdown_tx_opt,
            storage_daemon_handle,
            storage_daemon_port_arc,
        ).await;
        if let Err(e) = result {
            error!("[DAEMON PROCESS] Storage daemon failed: {:?}", e);
            return Err(e);
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
            return Err(e);
        }
        info!("[DAEMON PROCESS] GraphDB Daemon (daemonized) stopped.");
        Ok(())
    }
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

    start_daemon(Some(p), None, Vec::new(), service_type)
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
    start_daemon(Some(actual_port), effective_cluster, skip_ports, service_type)
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
    let actual_port = port.unwrap_or_else(|| {
        if is_rest {
            load_rest_config(config_path_str.as_deref())
                .map(|c| c.default_port)
                .unwrap_or(DEFAULT_REST_API_PORT)
        } else if is_storage {
            load_storage_config(config_path_str.as_deref())
                .map(|c| c.default_port)
                .unwrap_or(DEFAULT_STORAGE_PORT)
        } else {
            load_main_daemon_config(config_path_str.as_deref())
                .map(|c| c.default_port)
                .unwrap_or(DEFAULT_DAEMON_PORT)
        }
    });

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
        load_storage_config(config_path_str.as_deref())
            .map(|c| PathBuf::from(c.data_directory))
            .unwrap_or_else(|_| PathBuf::from(format!("{}/storage_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)))
    } else if is_rest {
        load_rest_config(config_path_str.as_deref())
            .map(|c| PathBuf::from(c.data_directory))
            .unwrap_or_else(|_| PathBuf::from(format!("{}/rest_api_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)))
    } else {
        load_main_daemon_config(config_path_str.as_deref())
            .map(|c| PathBuf::from(c.data_directory))
            .unwrap_or_else(|_| PathBuf::from(format!("{}/daemon_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)))
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
    
    start_daemon(port, cluster.clone(), args_u16, service_type)
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

/// Calls the `daemon_api::stop_daemon` function.
pub async fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    stop_daemon().await.map_err(|e| anyhow!("Daemon stop failed: {}", e))
}

/// A wrapper to stop a specific daemon instance using `stop_port_daemon` from daemon_api.
pub async fn stop_port_daemon_call(port: u16, daemon_type: &str) -> Result<(), anyhow::Error> {
    stop_port_daemon(port, daemon_type).await.map_err(|e| anyhow::anyhow!("Daemon stop failed: {}", e))
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

pub fn spawn_rest_api_server(
    port: u16,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<JoinHandle<()>, anyhow::Error> {
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
            data_directory.to_string_lossy().into_owned(),
        ).await {
            Ok(_) => info!("REST API server on port {} stopped successfully.", port),
            Err(e) => error!("REST API server on port {} exited with error: {:?}", port, e),
        }
    });
    Ok(handle)
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

pub async fn is_storage_daemon_running(port: u16) -> bool {
    let all_daemons = get_all_daemon_processes_with_ports().await.unwrap_or_default();
    all_daemons.get(&port).map_or(false, |(_pid, service_type)| service_type == "storage")
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

pub fn load_storage_config_path_or_default(path: Option<PathBuf>) -> Result<StorageConfig, anyhow::Error> {
    let path_owned: Option<String> = path.map(|p| p.to_string_lossy().into_owned());
    let path_str: Option<&str> = path_owned.as_deref();
    load_storage_config(path_str)
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

pub async fn is_port_free(port: u16) -> bool {
    !is_port_listening(port).await
}

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

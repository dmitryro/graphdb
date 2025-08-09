
use anyhow::{Result, Context, anyhow};
use std::collections::HashMap;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use futures::stream::StreamExt;
use log::{info, error, warn, debug};
use std::fs;
use chrono::Utc;

// Placeholder struct for daemon args
#[derive(Debug, Clone)]
pub struct DaemonArgs {
    pub port: Option<u16>,
    pub cluster: Option<String>,
}

// Import command structs from commands.rs
use crate::cli::commands::DaemonCliCommand;

// Import configuration-related items
use crate::cli::config::{
    DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR, MainDaemonConfig, load_daemon_config, load_main_daemon_config
};

// Import daemon management utilities
use crate::cli::daemon_management::{
    check_process_status_by_port, is_port_free, parse_cluster_range, stop_process_by_port, find_pid_by_port
};

// Import daemon API and registry
use daemon_api::{start_daemon};
use daemon_api::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};

/// Displays detailed status for a specific GraphDB daemon or lists all running ones.
/// Enhanced version of display_daemon_status with better formatting and deduplication
pub async fn display_daemon_status(port_arg: Option<u16>) {
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry contents for daemon status: {:?}", all_daemons);
    
    let running_daemon_ports: Vec<u16> = futures::stream::iter(all_daemons.iter())
        .filter_map(|d| async move {
            if d.service_type == "main" && check_process_status_by_port("GraphDB Daemon", d.port).await {
                Some(d.port)
            } else {
                None
            }
        })
        .collect::<Vec<u16>>()
        .await;
    
    let ports_to_display = match port_arg {
        Some(p) if running_daemon_ports.contains(&p) => vec![p],
        Some(p) => {
            println!("No GraphDB Daemon found on port {}.", p);
            vec![]
        }
        None => running_daemon_ports,
    };

    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<20} {:<15} {:<10} {:<50}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<50}", "", "", "", "");
    
    if ports_to_display.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<50}", "GraphDB Daemon", "Down", "N/A", "No running daemons found in registry.");
    } else {
        for (i, &port) in ports_to_display.iter().enumerate() {
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "main");
            
            // Show "GraphDB Daemon" only for the first daemon, empty for others
            let component_name = if i == 0 { "GraphDB Daemon" } else { "" };
            
            if let Some(meta) = metadata {
                println!("{:<20} {:<15} {:<10} {:<50}", component_name, "Running", port, format!("PID: {}", meta.pid));
                println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Data Directory: {:?}", meta.data_dir));
                println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", "Service Type: Core Graph Processing");
                
                // Add configuration details from daemon config if available
                if let Ok(daemon_config) = load_daemon_config(None) {
                    println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Max Connections: {}", daemon_config.max_connections));
                    println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Max Open Files: {}", daemon_config.max_open_files));
                    println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Use Raft: {}", daemon_config.use_raft_for_scale));
                    println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Log Level: {}", daemon_config.log_level));
                    println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Log Directory: {}", daemon_config.log_directory));
                }
            } else {
                println!("{:<20} {:<15} {:<10} {:<50}", component_name, "Running", port, "Core Graph Processing (No metadata)");
            }
            
            // Add separator between multiple daemons
            if ports_to_display.len() > 1 && i < ports_to_display.len() - 1 {
                println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", "");
            }
        }
    }
    
    if port_arg.is_none() && !ports_to_display.is_empty() {
        println!("\nTo check a specific main daemon, use 'status daemon --port <port>'.");
    }
    println!("--------------------------------------------------");
}

/// Handles `daemon` subcommand for direct CLI execution.
pub async fn handle_daemon_command(
    daemon_cmd: DaemonCliCommand,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match daemon_cmd {
        DaemonCliCommand::Start { port, cluster, daemon_port, daemon_cluster } => {
            if port.is_some() && daemon_port.is_some() {
                return Err(anyhow!("Cannot specify both --port and --daemon-port"));
            }
            if cluster.is_some() && daemon_cluster.is_some() {
                return Err(anyhow!("Cannot specify both --cluster and --daemon-cluster"));
            }
            let actual_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let actual_cluster = daemon_cluster.or(cluster);
            if let Some(cluster) = actual_cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    start_daemon_instance_interactive(Some(p), None, daemon_handles.clone()).await?;
                }
            } else {
                start_daemon_instance_interactive(Some(actual_port), None, daemon_handles).await?;
            }
            Ok(())
        }
        DaemonCliCommand::Stop { port } => {
            stop_daemon_instance_interactive(port, daemon_handles).await
        }
        DaemonCliCommand::Status { port, cluster } => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_daemon_status(Some(p)).await;
                }
            } else {
                display_daemon_status(port).await;
            }
            Ok(())
        }
        DaemonCliCommand::List => {
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let daemon_ports: Vec<u16> = all_daemons.iter()
                .filter(|d| d.service_type == "main")
                .map(|d| d.port)
                .collect();
            if daemon_ports.is_empty() {
                println!("No daemons are currently registered.");
            } else {
                println!("Registered daemons:");
                for port in daemon_ports {
                    println!("- Daemon on port {}", port);
                }
            }
            Ok(())
        }
        DaemonCliCommand::ClearAll => {
            stop_daemon_instance_interactive(None, daemon_handles).await?;
            Ok(())
        }
    }
}

/// Starts a daemon instance managed by the interactive CLI.
/// This version includes a workaround to handle a race condition/bug
/// in the `daemon_api` crate where it fails to register the daemon.
pub async fn start_daemon_instance_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
    if check_process_status_by_port("GraphDB Daemon", actual_port).await {
        info!("Daemon already running on port {}", actual_port);
        return Ok(());
    }
    
    info!("Starting GraphDB Daemon on port {}", actual_port);
    // Use the existing, more robust stop function to free the port first.
    let _ = stop_process_by_port("Main Daemon", actual_port).await;

    let cluster_clone = cluster.clone();

    let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
        .join("daemon_api/daemon_config.yaml");
    let config = match load_main_daemon_config(Some(&config_path.to_string_lossy())) {
        Ok(config) => {
            info!("Successfully loaded Daemon config: {:?}", config);
            config
        }
        Err(e) => {
            error!("Failed to load config file {:?}: {}", config_path, e);
            if config_path.exists() {
                let content = fs::read_to_string(&config_path)
                    .unwrap_or_else(|e| format!("Failed to read file: {}", e));
                error!("Config file content: {}", content);
            } else {
                error!("Config file does not exist at {:?}", config_path);
            }
            warn!("Using default Daemon configuration");
            MainDaemonConfig {
                data_directory: format!("{}/daemon_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
                log_directory: "/var/log/graphdb/daemon".to_string(),
                default_port: actual_port,
                cluster_range: cluster_clone.unwrap_or_else(|| format!("{}", actual_port)),
            }
        }
    };

    let data_dir = PathBuf::from(&config.data_directory);

    if !is_port_free(actual_port).await {
        return Err(anyhow!("Port {} is already in use.", actual_port));
    }

    // This is the key change: We call `start_daemon` to spawn the process,
    // but we use `if let Err(...)` to handle the result gracefully without
    // causing a fatal error in our function. The `daemon_api`'s internal
    // error is non-fatal to the process itself.
    if let Err(e) = start_daemon(
        Some(actual_port),
        cluster,
        vec![DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT],
        "main",
    )
    .await
    {
        error!("daemon_api::start_daemon returned an error, but we'll proceed as the process may have still started: {}", e);
    }
    let mut pid = 0;
    let start_time = Instant::now();
    let timeout_duration = Duration::from_secs(10);
    while pid == 0 && start_time.elapsed() < timeout_duration {
        pid = find_pid_by_port(actual_port).await.unwrap_or(0);
        if pid == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    if pid == 0 {
        return Err(anyhow!(
            "Failed to find a valid PID for the daemon on port {} after waiting.",
            actual_port
        ));
    }

    let metadata = DaemonMetadata {
        service_type: "main".to_string(),
        port: actual_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(data_dir.clone()),
        config_path: Some(config_path.clone()),
        engine_type: None,
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    let mut attempts = 0;
    let max_attempts = 3;
    while attempts < max_attempts {
        match GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await {
            Ok(_) => {
                info!(
                    "Successfully registered daemon with PID {} on port {}",
                    pid, actual_port
                );
                break;
            }
            Err(e) => {
                attempts += 1;
                error!(
                    "Failed to register daemon (attempt {}/{}): {}",
                    attempts, max_attempts, e
                );
                if attempts >= max_attempts {
                    return Err(e).context("Failed to register GraphDB Daemon after multiple attempts");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    let addr_check = format!("127.0.0.1:{}", actual_port);
    let health_check_timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(500);
    let start_time = Instant::now();

    while start_time.elapsed() < health_check_timeout {
        if tokio::net::TcpStream::connect(&addr_check).await.is_ok() {
            let pid = find_pid_by_port(actual_port).await.unwrap_or(0);
            info!(
                "GraphDB Daemon started on port {} (PID {})",
                actual_port, pid
            );
            let (tx, rx) = oneshot::channel();
            let handle = tokio::spawn(async move {
                rx.await.ok();
                info!("GraphDB Daemon on port {} shutting down", actual_port);
            });
            let mut handles = daemon_handles.lock().await;
            handles.insert(actual_port, (handle, tx));
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    error!("Daemon on port {} failed to become reachable", actual_port);
    Err(anyhow!("Daemon failed to start on port {}", actual_port))
}

/// Stops the main daemon on the specified port or all main daemons if no port is specified.
pub async fn stop_main_interactive(
    port: Option<u16>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
) -> Result<(), anyhow::Error> {
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        GLOBAL_DAEMON_REGISTRY
            .get_all_daemon_metadata()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|d| d.service_type == "main")
            .map(|d| d.port)
            .collect()
    };

    let mut errors = Vec::new();
    for actual_port in ports_to_stop {
        log::info!("Attempting to stop main daemon on port {}", actual_port);

        if !check_process_status_by_port("Main Daemon", actual_port).await {
            log::info!("No main daemon running on port {}", actual_port);
            if let Ok(Some(_)) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("main", actual_port).await {
                log::info!("Removed stale main daemon registry entry for port {}", actual_port);
            }
            println!("Main daemon on port {} stopped.", actual_port);
            continue;
        }

        // Stop the process
        if let Err(e) = stop_process_by_port("Main Daemon", actual_port).await {
            log::error!("Failed to stop main daemon process on port {}: {}", actual_port, e);
            errors.push(anyhow!("Failed to stop main daemon on port {}: {}", actual_port, e));
        }

        // Attempt to deregister the daemon
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("main", actual_port).await {
            log::warn!("Failed to remove main daemon (port: {}) from registry: {}", actual_port, e);
        } else {
            log::info!("Successfully removed main daemon on port {} from registry", actual_port);
        }

        // Verify port is free with retries
        let mut attempts = 0;
        let max_attempts = 7;
        let mut port_free = false;
        while attempts < max_attempts {
            if is_port_free(actual_port).await {
                port_free = true;
                log::info!("Main daemon on port {} stopped successfully", actual_port);
                println!("Main daemon on port {} stopped.", actual_port);
                break;
            }
            debug!("Port {} still in use after attempt {}", actual_port, attempts + 1);
            tokio::time::sleep(Duration::from_millis(500)).await;
            attempts += 1;
        }

        if !port_free {
            errors.push(anyhow!("Port {} is still in use after attempting to stop main daemon", actual_port));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to stop main daemon: {:?}", errors))
    }
}

pub async fn handle_daemon_command_interactive(
    command: DaemonCliCommand,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match command {
        DaemonCliCommand::Start { port, cluster, daemon_port, daemon_cluster } => {
            if port.is_some() && daemon_port.is_some() {
                return Err(anyhow!("Cannot specify both --port and --daemon-port"));
            }
            if cluster.is_some() && daemon_cluster.is_some() {
                return Err(anyhow!("Cannot specify both --cluster and --daemon-cluster"));
            }
            let actual_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let actual_cluster = daemon_cluster.or(cluster);
            start_daemon_instance_interactive(Some(actual_port), actual_cluster, daemon_handles).await
        }
        DaemonCliCommand::Stop { port } => {
            stop_daemon_instance_interactive(port, daemon_handles).await
        }
        DaemonCliCommand::Status { port, .. } => {
            display_daemon_status(port).await;
            Ok(())
        }
        DaemonCliCommand::List => {
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let daemon_ports: Vec<u16> = all_daemons.iter()
                .filter(|d| d.service_type == "main")
                .map(|d| d.port)
                .collect();
            if daemon_ports.is_empty() {
                println!("No daemons are currently managed by this CLI instance.");
            } else {
                println!("Daemons managed by this CLI instance:");
                for port in daemon_ports {
                    println!("- Daemon on port {}", port);
                }
            }
            Ok(())
        }
        DaemonCliCommand::ClearAll => {
            stop_daemon_instance_interactive(None, daemon_handles).await?;
            Ok(())
        }
    }
}

/// Handles the interactive 'reload daemon' command.
pub async fn reload_daemon_interactive(port: Option<u16>) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
    println!("Reloading GraphDB daemon on port {}...", actual_port);
    
    stop_process_by_port("GraphDB Daemon", actual_port).await?;
    
    let daemon_result = start_daemon(Some(actual_port), None, vec![], "main").await;
    match daemon_result {
        Ok(()) => println!("Daemon on port {} reloaded (stopped and restarted).", actual_port),
        Err(e) => eprintln!("Failed to restart daemon on port {}: {:?}", actual_port, e),
    }

    Ok(())
}

/// Stops a daemon instance managed by the interactive CLI.
/// This version correctly uses the robust `stop_process_by_port` function
/// and avoids redundant unregistration calls.
pub async fn stop_daemon_instance_interactive(
    port: Option<u16>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let mut handles = daemon_handles.lock().await;
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default()
            .iter()
            .filter(|d| d.service_type == "main")
            .map(|d| d.port)
            .collect()
    };

    for actual_port in ports_to_stop {
        // First, attempt to stop the in-memory handle. This is the cleaner shutdown.
        if let Some((handle, shutdown_tx)) = handles.remove(&actual_port) {
            info!("Sending shutdown signal to in-memory handle for daemon on port {}", actual_port);
            shutdown_tx.send(()).ok();
            let _ = handle.await; // Await the handle to ensure it's finished.
        }

        // Now, attempt to stop the actual process on the OS using the robust function.
        // This function now handles unregistering the daemon and removing the PID file.
        let _ = stop_process_by_port("GraphDB Daemon", actual_port).await;
        
        println!("GraphDB Daemon on port {} stopped.", actual_port);
    }

    Ok(())
}
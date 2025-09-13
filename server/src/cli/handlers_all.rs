use anyhow::{Result, Context, anyhow};
use std::collections::HashMap;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use std::time::Duration;
use futures::stream::StreamExt;
use log::{info, error, warn, debug};
use config::Config;
use reqwest::Client;
use chrono::Utc;
use nix::sys::signal;
use futures::future;

// Import configuration-related items
use lib::config::{
    DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, StorageConfig, load_storage_config_from_yaml, 
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR, StorageEngineType, daemon_api_storage_engine_type_to_string,
};

// Import daemon management utilities
use crate::cli::daemon_management::{
    check_process_status_by_port, is_port_free, parse_cluster_range, stop_process_by_port, is_port_in_cluster_range
};

// Import handler functions for individual components
use crate::cli::handlers_rest::{start_rest_api_interactive, stop_rest_api_interactive, handle_show_rest_config_command};
use crate::cli::handlers_storage::{start_storage_interactive, stop_storage_interactive, handle_show_storage_config_command};
use crate::cli::handlers_utils::{format_engine_config, load_tikv_pd_port};
use crate::cli::handlers_main::{start_daemon_instance_interactive, stop_main_interactive, handle_show_main_config_command};
use lib::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER,
                          emergency_cleanup_storage_engine_manager};
// Import daemon registry
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};

// Static lock to prevent concurrent storage daemon startups
lazy_static::lazy_static! {
    static ref STORAGE_START_LOCK: Arc<TokioMutex<()>> = Arc::new(TokioMutex::new(()));
}

/// Stops all components managed by the interactive CLI, then attempts to stop any others.
pub async fn stop_all_interactive(
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<(), anyhow::Error> {
    println!("==> STOP - INTERACTIVE ALL");
    println!("Stopping all GraphDB components...");
    log::info!("Starting shutdown of all GraphDB components");

    let mut stopped_count = 0;
    let mut errors = Vec::new();

    // Load TiKV PD port from configuration - keep as Option<u16>
    let tikv_pd_port = load_tikv_pd_port().await;

    // Log initial registry state
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Initial daemon registry state: {:?}", all_daemons);

    // Stop REST API instances
    log::info!("Stopping all REST API instances...");
    match stop_rest_api_interactive(
        None,
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await {
        Ok(()) => {
            log::info!("REST API instances stopped successfully");
            println!("REST API instances stopped.");
            stopped_count += 1;
        }
        Err(e) => {
            println!("Failed to stop REST API instances: {}", e);
            log::error!("Failed to stop REST API instances: {}", e);
            errors.push(format!("REST API instances: {}", e));
        }
    }

    // Stop main daemons using daemon_handles
    log::info!("Stopping all main daemon instances from handles...");
    {
        let mut handles = daemon_handles.lock().await;
        let ports: Vec<u16> = handles.keys().copied().collect();
        for port in ports {
            if let Some((handle, tx)) = handles.remove(&port) {
                log::info!("Sending stop signal to main daemon on port {}", port);
                println!("Stopping Main Daemon on port {}...", port);
                if let Err(_) = tx.send(()) {
                    println!("Failed to send stop signal to main daemon on port {}: channel closed or receiver dropped", port);
                    log::error!("Failed to send stop signal to main daemon on port {}: channel closed or receiver dropped", port);
                    errors.push(format!("Main daemon on port {}: channel closed or receiver dropped", port));
                } else {
                    match tokio::time::timeout(Duration::from_secs(5), handle).await {
                        Ok(Ok(_)) => {
                            log::info!("Main daemon on port {} stopped successfully", port);
                            println!("Main Daemon on port {} stopped.", port);
                            stopped_count += 1;
                        }
                        Ok(Err(e)) => {
                            println!("Main daemon on port {} failed to stop: {}", port, e);
                            log::error!("Main daemon on port {} failed to stop: {}", port, e);
                            errors.push(format!("Main daemon on port {}: {}", port, e));
                        }
                        Err(_) => {
                            println!("Main daemon on port {} timed out after 5 seconds", port);
                            log::error!("Main daemon on port {} timed out after 5 seconds", port);
                            errors.push(format!("Main daemon on port {}: timed out after 5 seconds", port));
                        }
                    }
                }
            }
        }
    }

    // Fallback to stop_main_interactive for any remaining daemons
    log::info!("Checking for remaining main daemon instances...");
    match stop_main_interactive(
        None,
        Arc::new(TokioMutex::new(None)),
    ).await {
        Ok(()) => {
            log::info!("Remaining main daemon instances stopped successfully");
            println!("Remaining main daemon instances stopped.");
            stopped_count += 1;
        }
        Err(e) => {
            println!("Failed to stop remaining main daemon instances: {}", e);
            log::error!("Failed to stop remaining main daemon instances: {}", e);
            errors.push(format!("Remaining main daemon instances: {}", e));
        }
    }

    // Stop storage daemons
    log::info!("Stopping all storage daemon instances...");
    match stop_storage_interactive(
        None,
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone(),
    ).await {
        Ok(()) => {
            log::info!("Storage daemon instances stopped successfully");
            println!("Storage daemon instances stopped.");
            stopped_count += 1;
        }
        Err(e) => {
            // Check if error is due to skipping TiKV PD port
            if tikv_pd_port.is_some() && e.to_string().contains("Skipping termination for TiKV PD port") {
                log::info!("Skipped termination for TiKV storage daemon on port {:?}", tikv_pd_port);
                println!("Skipped termination for TiKV storage daemon on port {:?}.", tikv_pd_port);
                stopped_count += 1;
            } else {
                println!("Failed to stop storage daemon instances: {}", e);
                log::error!("Failed to stop storage daemon instances: {}", e);
                errors.push(format!("Storage daemon instances: {}", e));
            }
        }
    }

    // Clear any remaining registry entries
    let remaining_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    if !remaining_daemons.is_empty() {
        warn!("Found stale registry entries after stopping components: {:?}", remaining_daemons);
        for daemon in remaining_daemons {
            // Skip TiKV PD port in registry cleanup
            if let Some(tikv_port) = tikv_pd_port {
                if tikv_port == daemon.port && daemon.service_type == "storage" {
                    info!("Skipping registry cleanup for TiKV storage daemon on port {}", daemon.port);
                    println!("Skipping registry cleanup for TiKV storage daemon on port {}.", daemon.port);
                    continue;
                }
            }
            
            if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type(&daemon.service_type, daemon.port).await {
                warn!("Failed to remove stale {} daemon on port {} from registry: {}", daemon.service_type, daemon.port, e);
                errors.push(format!("Remove stale {} daemon on port {}: {}", daemon.service_type, daemon.port, e));
            } else {
                info!("Removed stale {} daemon on port {} from registry", daemon.service_type, daemon.port);
                println!("Removed stale {} daemon on port {} from registry.", daemon.service_type, daemon.port);
            }
        }
    }

    // Log final registry state
    let final_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    if !final_daemons.is_empty() {
        // Only warn if non-TiKV daemons remain
        let non_tikv_daemons = final_daemons.iter()
            .filter(|d| {
                if let Some(tikv_port) = tikv_pd_port {
                    tikv_port != d.port || d.service_type != "storage"
                } else {
                    true
                }
            })
            .collect::<Vec<_>>();
        if !non_tikv_daemons.is_empty() {
            println!("Warning: Some non-TiKV daemons still registered: {:?}", non_tikv_daemons);
            log::warn!("Some non-TiKV daemons still registered after stop attempt: {:?}", non_tikv_daemons);
            errors.push(format!("Non-TiKV daemons still registered: {:?}", non_tikv_daemons));
        }
    }

    if stopped_count == 0 && errors.is_empty() {
        println!("No running components were found to stop.");
        log::info!("No components were running to stop");
    } else {
        println!("Stop all completed: {} component groups stopped, {} failed.", stopped_count, errors.len());
        log::info!("Stop all completed: {} component groups stopped, {} failed", stopped_count, errors.len());
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to stop one or more components: {:?}", errors))
    }
}

/// Handles the interactive 'reload all' command.
pub async fn reload_all_interactive(
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    println!("Reloading all GraphDB components...");

    stop_all_interactive(
        daemon_handles.clone(),
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone(),
    ).await?;

    println!("Restarting all GraphDB components after reload...");
    start_daemon_instance_interactive(None, None, daemon_handles).await?;
    start_rest_api_interactive(None, None, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?; // Added None for cluster
    start_storage_interactive(
        None, // port
        None, // config_file
        None, // cluster_opt
        None,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc
    ).await?;
    println!("All GraphDB components reloaded (stopped and restarted).");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_start_all_interactive(
    daemon_port: Option<u16>,
    daemon_cluster: Option<String>,
    rest_port: Option<u16>,
    _rest_cluster: Option<String>,
    storage_port: Option<u16>,
    _storage_cluster: Option<String>,
    storage_config: Option<PathBuf>,
    _daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    info!("Starting all GraphDB components...");
    println!("Starting all GraphDB components...");
    let mut errors = Vec::new();

    // Log invocation to debug multiple calls
    debug!("handle_start_all_interactive invoked at {:?}", Utc::now());

    // Load configuration for main daemon cluster range
    let config_path_toml = "server/src/cli/config.toml";
    let main_config_yaml = "server/main_app_config.yaml";
    let mut config_builder = Config::builder();
    if Path::new(config_path_toml).exists() {
        config_builder = config_builder.add_source(config::File::with_name(config_path_toml));
    }
    if Path::new(main_config_yaml).exists() {
        config_builder = config_builder.add_source(config::File::with_name(main_config_yaml));
    }
    let config = config_builder.build().context("Failed to load configuration")?;
    let cluster_range = config.get_string("main_daemon.cluster_range").ok();

    // Start main daemons
    let daemon_ports = if let Some(range) = cluster_range {
        parse_cluster_range(&range)?
    } else {
        vec![daemon_port.unwrap_or(DEFAULT_DAEMON_PORT)]
    };
    let reserved_ports = vec![
        rest_port.unwrap_or(DEFAULT_REST_API_PORT),
        storage_port.unwrap_or(DEFAULT_STORAGE_PORT),
    ];
    for port in daemon_ports {
        if reserved_ports.contains(&port) {
            warn!("Skipping daemon on port {}: reserved for another service.", port);
            errors.push(format!("Daemon on port {}: reserved for another service", port));
            continue;
        }
        info!("Starting GraphDB Daemon on port {}...", port);
        if !is_port_free(port).await {
            info!("Port {} is in use for GraphDB Daemon. Checking registry before stopping.", port);
            if let Ok(Some(metadata)) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await {
                if metadata.service_type == "main" && signal::kill(nix::unistd::Pid::from_raw(metadata.pid as i32), None).is_ok() {
                    warn!("Port {} is used by a valid GraphDB Daemon (PID {}). Skipping startup.", port, metadata.pid);
                    continue;
                }
            }
            info!("No valid GraphDB Daemon found on port {}. Attempting to stop existing process.", port);
            if let Err(e) = stop_process_by_port("GraphDB Daemon", port).await {
                warn!("Failed to stop existing process on port {}: {}", port, e);
            }
        }
        if let Err(e) = start_daemon_instance_interactive(Some(port), daemon_cluster.clone(), _daemon_handles.clone()).await {
            error!("Failed to start daemon on port {}: {}", port, e);
            errors.push(format!("Daemon on port {}: {}", port, e));
        } else {
            info!("GraphDB Daemon started on port {}.", port);
            println!("GraphDB Daemon started on port {}.", port);
        }
    }

    // Start REST API
    let rest_port = rest_port.unwrap_or(DEFAULT_REST_API_PORT);
    info!("Starting REST API on port {}...", rest_port);
    if !is_port_free(rest_port).await {
        info!("Port {} is in use for REST API. Checking registry before stopping.", rest_port);
        if let Ok(Some(metadata)) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(rest_port).await {
            if metadata.service_type == "rest" && signal::kill(nix::unistd::Pid::from_raw(metadata.pid as i32), None).is_ok() {
                warn!("Port {} is used by a valid REST API (PID {}). Skipping startup.", rest_port, metadata.pid);
            } else {
                info!("No valid REST API found on port {}. Attempting to stop existing process.", rest_port);
                if let Err(e) = stop_process_by_port("REST API", rest_port).await {
                    warn!("Failed to stop existing process on port {}: {}", rest_port, e);
                }
            }
        } else {
            info!("No valid REST API found on port {}. Attempting to stop existing process.", rest_port);
            if let Err(e) = stop_process_by_port("REST API", rest_port).await {
                warn!("Failed to stop existing process on port {}: {}", rest_port, e);
            }
        }
    }
    if let Err(e) = start_rest_api_interactive(
        Some(rest_port),
        None,
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await {
        error!("Failed to start REST API on port {}: {}", rest_port, e);
        errors.push(format!("REST API on port {}: {}", rest_port, e));
    } else {
        // Health check for REST API
        let addr_check = format!("127.0.0.1:{}", rest_port);
        let health_check_timeout = Duration::from_secs(20);
        let poll_interval = Duration::from_millis(500);
        let start_time = tokio::time::Instant::now();
        let mut is_running = false;

        debug!("Starting REST API health check on port {}", rest_port);
        while start_time.elapsed() < health_check_timeout {
            if tokio::net::TcpStream::connect(&addr_check).await.is_ok() {
                info!("REST API server started and reachable on port {}.", rest_port);
                println!("REST API server started on port {}.", rest_port);
                is_running = true;
                break;
            }
            debug!("REST API health check on port {}: Attempting connection (elapsed: {:?})", rest_port, start_time.elapsed());
            tokio::time::sleep(poll_interval).await;
        }

        if !is_running {
            error!("REST API on port {} failed to become reachable within {} seconds", rest_port, health_check_timeout.as_secs());
            errors.push(format!("REST API on port {}: Failed to become reachable", rest_port));
        }
    }

    // Start Storage Daemon with lock
    let _lock = STORAGE_START_LOCK.lock().await; // Acquire lock to prevent concurrent starts
    let actual_storage_config = storage_config.unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));

    // Load storage configuration to get the engine-specific port
    let storage_config_full = load_storage_config_from_yaml(Some(actual_storage_config.clone())).await
        .context("Failed to load storage configuration")?;
    let engine_specific_config = storage_config_full.engine_specific_config
        .context("No engine-specific configuration found")?;

    // Prioritize command-line storage_port, then engine-specific port, then default
    let selected_storage_port = storage_port.unwrap_or_else(|| {
        engine_specific_config.storage.port.unwrap_or(DEFAULT_STORAGE_PORT)
    });

    info!("Starting Storage Daemon on port {}...", selected_storage_port);
    debug!("Using storage port {} from configuration {:?}", selected_storage_port, engine_specific_config);

    // Check if a valid storage daemon is already running
    if !is_port_free(selected_storage_port).await {
        if let Ok(Some(metadata)) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(selected_storage_port).await {
            if metadata.service_type == "storage" && signal::kill(nix::unistd::Pid::from_raw(metadata.pid as i32), None).is_ok() {
                let addr_check = format!("127.0.0.1:{}", selected_storage_port);
                let health_check_timeout = Duration::from_secs(20);
                let poll_interval = Duration::from_millis(500);
                let start_time = tokio::time::Instant::now();
                let mut is_running = false;

                debug!("Starting Storage Daemon health check on port {}", selected_storage_port);
                while start_time.elapsed() < health_check_timeout {
                    if tokio::net::TcpStream::connect(&addr_check).await.is_ok() {
                        info!("Storage Daemon already running and reachable on port {} (PID {}). Skipping startup.", selected_storage_port, metadata.pid);
                        println!("Storage Daemon already running on port {}.", selected_storage_port);
                        is_running = true;
                        break;
                    }
                    debug!("Storage Daemon health check on port {}: Attempting connection (elapsed: {:?})", selected_storage_port, start_time.elapsed());
                    tokio::time::sleep(poll_interval).await;
                }

                if is_running {
                    if errors.is_empty() {
                        info!("All GraphDB components started successfully");
                        println!("All GraphDB components started successfully.");
                        return Ok(());
                    } else {
                        warn!("Some GraphDB components failed to start: {:?}", errors);
                        return Ok(());
                    }
                } else {
                    warn!("Storage Daemon on port {} (PID {}) is not healthy. Stopping and restarting.", selected_storage_port, metadata.pid);
                    if let Err(e) = stop_process_by_port("Storage Daemon", selected_storage_port).await {
                        warn!("Failed to stop existing process on port {}: {}", selected_storage_port, e);
                    }
                }
            }
        }
        info!("No valid Storage Daemon found on port {}. Attempting to stop existing process.", selected_storage_port);
        if let Err(e) = stop_process_by_port("Storage Daemon", selected_storage_port).await {
            warn!("Failed to stop existing process on port {}: {}", selected_storage_port, e);
        }
    }

    // Determine storage ports: use specific port if provided, else use cluster_range
    let storage_ports = if storage_port.is_some() || engine_specific_config.storage.port.is_some() {
        // Use the specific port if provided via command line or engine-specific config
        vec![selected_storage_port]
    } else if let range = storage_config_full.cluster_range {
        // Use cluster_range from storage_config.yaml if no specific port is provided
        parse_cluster_range(&range)?
    } else {
        // Fallback to DEFAULT_STORAGE_PORT
        vec![DEFAULT_STORAGE_PORT]
    };

    for port in storage_ports {
        info!("Starting Storage Daemon on port {}...", port);
        debug!("Calling start_storage_interactive for port {}", port);
        match start_storage_interactive(
            Some(port),
            Some(actual_storage_config.clone()),
            None,
            None,
            storage_daemon_shutdown_tx_opt.clone(),
            storage_daemon_handle.clone(),
            storage_daemon_port_arc.clone(),
        ).await {
            Ok(_) => {
                // Health check for Storage Daemon
                let addr_check = format!("127.0.0.1:{}", port);
                let health_check_timeout = Duration::from_secs(20);
                let poll_interval = Duration::from_millis(500);
                let start_time = tokio::time::Instant::now();
                let mut is_running = false;

                debug!("Starting Storage Daemon health check on port {}", port);
                while start_time.elapsed() < health_check_timeout {
                    if tokio::net::TcpStream::connect(&addr_check).await.is_ok() {
                        if let Ok(Some(metadata)) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await {
                            if signal::kill(nix::unistd::Pid::from_raw(metadata.pid as i32), None).is_ok() {
                                info!("Storage Daemon started and reachable on port {} (PID {}).", port, metadata.pid);
                                println!("Storage Daemon started on port {}.", port);
                                is_running = true;
                                break;
                            } else {
                                warn!("Storage Daemon on port {} started but PID {} is no longer valid.", port, metadata.pid);
                            }
                        }
                    }
                    debug!("Storage Daemon health check on port {}: Attempting connection (elapsed: {:?})", port, start_time.elapsed());
                    tokio::time::sleep(poll_interval).await;
                }

                if !is_running {
                    error!("Storage Daemon on port {} failed to become reachable within {} seconds", port, health_check_timeout.as_secs());
                    errors.push(format!("Storage Daemon on port {}: Failed to become reachable", port));
                }
            }
            Err(e) => {
                error!("Failed to start Storage Daemon on port {}: {}", port, e);
                errors.push(format!("Storage Daemon on port {}: {}", port, e));
            }
        }
    }

    if errors.is_empty() {
        info!("All GraphDB components started successfully");
        println!("All GraphDB components started successfully.");
        Ok(())
    } else {
        warn!("Some GraphDB components failed to start: {:?}", errors);
        Ok(())
    }
}

/// Displays the full status summary for all GraphDB components.
/// This function relies exclusively on the `GLOBAL_DAEMON_REGISTRY` to find
/// running components. It first retrieves all registered daemons, then checks
/// the live status of each one by its PID and port. It provides detailed
/// information for each component type, including health checks for the REST API
/// and configuration details for the Storage daemons.
pub async fn display_full_status_summary(
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    info!("Displaying full status summary");
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry contents: {:?}", all_daemons);

    // Concurrently check the status of all registered daemons
    let mut tasks = Vec::new();
    for daemon in all_daemons.iter() {
        let daemon_clone = daemon.clone();
        tasks.push(tokio::spawn(async move {
            let is_running = check_process_status_by_port(&daemon_clone.service_type, daemon_clone.port).await;
            (daemon_clone, is_running)
        }));
    }

    let results = future::join_all(tasks).await;

    let mut running_daemons: Vec<&DaemonMetadata> = Vec::new();
    let mut down_daemons: Vec<&DaemonMetadata> = Vec::new();

    for res in results {
        if let Ok((daemon, is_running)) = res {
            if is_running {
                running_daemons.push(all_daemons.iter().find(|d| d.pid == daemon.pid).unwrap());
            } else {
                down_daemons.push(all_daemons.iter().find(|d| d.pid == daemon.pid).unwrap());
            }
        }
    }

    // GraphDB Daemon status
    let main_daemons: Vec<&DaemonMetadata> = running_daemons.iter().filter(|d| d.service_type == "main").copied().collect();
    if main_daemons.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", "Down", "N/A", "No daemons found in registry.");
    } else {
        for (i, &daemon) in main_daemons.iter().enumerate() {
            let details = format!("PID: {} | Core Graph Processing", daemon.pid);
            let component_name = if i == 0 { "GraphDB Daemon" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, "Running", daemon.port, details);
        }
    }
    println!("\n");

    // REST API status
    let rest_daemons: Vec<&DaemonMetadata> = running_daemons.iter().filter(|d| d.service_type == "rest").copied().collect();
    if rest_daemons.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "REST API", "Down", "N/A", "No REST API servers found in registry.");
    } else {
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .context("Failed to build reqwest client")?;

        for (i, &daemon) in rest_daemons.iter().enumerate() {
            let mut status = "Running";
            let mut details = String::new();

            let health_url = format!("http://127.0.0.1:{}/api/v1/health", daemon.port);
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    details = "Health: OK".to_string();
                    let version_url = format!("http://127.0.0.1:{}/api/v1/version", daemon.port);
                    if let Ok(v_resp) = client.get(&version_url).send().await {
                        if v_resp.status().is_success() {
                            if let Ok(v_json) = v_resp.json::<serde_json::Value>().await {
                                let version = v_json["version"].as_str().unwrap_or("N/A");
                                details = format!("{}; Version: {}", details, version);
                            }
                        }
                    }
                },
                _ => {
                    status = "Down";
                    details = "Health check failed".to_string();
                }
            }

            details = format!("PID: {} | {}", daemon.pid, details);
            let component_name = if i == 0 { "REST API" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, status, daemon.port, details);
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Directory: {:?}", daemon.data_dir));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", "Service Type: HTTP REST API");
            println!(); // Add a blank line for readability between multiple REST APIs
        }
    }
    println!("\n");

    // Storage Daemon status
    let storage_daemons: Vec<&DaemonMetadata> = running_daemons.iter().filter(|d| d.service_type == "storage").copied().collect();
    if storage_daemons.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for (i, &daemon) in storage_daemons.iter().enumerate() {
            let config_path = daemon.config_path.clone()
                .unwrap_or_else(|| PathBuf::from("./storage_daemon_server/storage_config.yaml"));
            let storage_config = load_storage_config_from_yaml(Some(config_path.clone())).await
                .unwrap_or_else(|e| {
                    warn!("Failed to load storage config from {:?}: {}, using default", config_path, e);
                    StorageConfig::default()
                });

            let engine_type = if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                let current_engine = manager.current_engine_type().await;
                daemon_api_storage_engine_type_to_string(&current_engine)
            } else {
                daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type)
            };
            
            let details = format!("PID: {} | Engine: {}", daemon.pid, engine_type);
            let component_name = if i == 0 { "Storage Daemon" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, "Running", daemon.port, details);
            
            // Print configuration details
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Path: {}", storage_config.engine_specific_config.as_ref().and_then(|c| c.storage.path.as_ref()).map_or("N/A".to_string(), |p| p.display().to_string())));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Host: {}", storage_config.engine_specific_config.as_ref().map_or("N/A", |c| c.storage.host.as_ref().map_or("N/A", |h| h.as_str()))));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Port: {}", daemon.port));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Open Files: {}", storage_config.max_open_files));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Disk Space: {} GB", storage_config.max_disk_space_gb));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Min Disk Space: {} GB", storage_config.min_disk_space_gb));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Use Raft: {}", storage_config.use_raft_for_scale));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Directory: {}", storage_config.data_directory.as_ref().map_or("N/A".to_string(), |p| p.display().to_string())));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Log Directory: {}", storage_config.log_directory.as_ref().map_or("N/A".to_string(), |p| p.display().to_string())));
            let config_root_display = storage_config.config_root_directory.as_ref().map_or("N/A".to_string(), |p| p.display().to_string());
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Config Root: {}", if config_root_display == "N/A" { DEFAULT_CONFIG_ROOT_DIRECTORY_STR.to_string() } else { config_root_display }));
            
            let cluster_range = storage_config.cluster_range.clone();
            if is_port_in_cluster_range(daemon.port, &cluster_range) {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Range: {}", cluster_range));
            } else {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Range: {} (Port {} is outside this range!)", cluster_range, daemon.port));
            }
            
            println!(); // Add a blank line for readability between multiple storage daemons
        }
    }

    // Set `Arc`s to the first running port, if any
    *storage_daemon_port_arc.lock().await = storage_daemons.first().map(|d| d.port);
    *rest_api_port_arc.lock().await = rest_daemons.first().map(|d| d.port);
    
    println!("--------------------------------------------------");
    Ok(())
}

/// A handler for the 'show config all' command.
/// It calls the individual show config handlers to display all services configurations.
pub async fn handle_show_all_config_command() -> Result<()> {
    println!("==================================================");
    handle_show_main_config_command().await?;
    println!("==================================================");
    handle_show_rest_config_command().await?;
    println!("==================================================");
    handle_show_storage_config_command().await?;
    println!("==================================================");
    Ok(())
}
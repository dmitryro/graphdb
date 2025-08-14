
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

// Import configuration-related items
use crate::cli::config::{
    DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, StorageConfig, load_storage_config_from_yaml, 
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
};

// Import daemon management utilities
use crate::cli::daemon_management::{
    check_process_status_by_port, is_port_free, parse_cluster_range, stop_process_by_port, is_port_in_cluster_range
};

// Import handler functions for individual components
use crate::cli::handlers_rest::{start_rest_api_interactive, stop_rest_api_interactive, handle_show_rest_config_command};
use crate::cli::handlers_storage::{start_storage_interactive, stop_storage_interactive, handle_show_storage_config_command};
use crate::cli::handlers_utils::{format_engine_config};
use crate::cli::handlers_main::{start_daemon_instance_interactive, stop_main_interactive, handle_show_main_config_command};

// Import daemon registry
use daemon_api::daemon_registry::{GLOBAL_DAEMON_REGISTRY};

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
    let mut failed_count = 0;

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
            stopped_count += 1;
        }
        Err(e) => {
            println!("Failed to stop REST API instances: {}", e);
            log::error!("Failed to stop REST API instances: {}", e);
            failed_count += 1;
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
                if let Err(_) = tx.send(()) {
                    println!("Failed to send stop signal to main daemon on port {}: channel closed or receiver dropped", port);
                    log::error!("Failed to send stop signal to main daemon on port {}: channel closed or receiver dropped", port);
                    failed_count += 1;
                } else {
                    match tokio::time::timeout(Duration::from_secs(5), handle).await {
                        Ok(Ok(_)) => {
                            log::info!("Main daemon on port {} stopped successfully", port);
                            stopped_count += 1;
                        }
                        Ok(Err(e)) => {
                            println!("Main daemon on port {} failed to stop: {}", port, e);
                            log::error!("Main daemon on port {} failed to stop: {}", port, e);
                            failed_count += 1;
                        }
                        Err(_) => {
                            println!("Main daemon on port {} timed out after 5 seconds", port);
                            log::error!("Main daemon on port {} timed out after 5 seconds", port);
                            failed_count += 1;
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
            stopped_count += 1;
        }
        Err(e) => {
            println!("Failed to stop remaining main daemon instances: {}", e);
            log::error!("Failed to stop remaining main daemon instances: {}", e);
            failed_count += 1;
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
            stopped_count += 1;
        }
        Err(e) => {
            println!("Failed to stop storage daemon instances: {}", e);
            log::error!("Failed to stop storage daemon instances: {}", e);
            failed_count += 1;
        }
    }

    // Clear any remaining registry entries
    let remaining_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    if !remaining_daemons.is_empty() {
        warn!("Found stale registry entries after stopping components: {:?}", remaining_daemons);
        for daemon in remaining_daemons {
            if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type(&daemon.service_type, daemon.port).await {
                warn!("Failed to remove stale {} daemon on port {} from registry: {}", daemon.service_type, daemon.port, e);
                failed_count += 1;
            } else {
                info!("Removed stale {} daemon on port {} from registry", daemon.service_type, daemon.port);
            }
        }
    }

    // Log final registry state
    let final_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    if !final_daemons.is_empty() {
        println!("Warning: Some daemons still registered: {:?}", final_daemons);
        log::warn!("Some daemons still registered after stop attempt: {:?}", final_daemons);
        failed_count += 1;
    }

    if stopped_count == 0 && failed_count == 0 {
        println!("No running components were found to stop.");
        log::info!("No components were running to stop");
    } else {
        println!("Stop all completed: {} component groups stopped, {} failed.", stopped_count, failed_count);
        log::info!("Stop all completed: {} component groups stopped, {} failed", stopped_count, failed_count);
    }

    if failed_count > 0 {
        Err(anyhow!("Failed to stop one or more components: {} failures detected", failed_count))
    } else {
        Ok(())
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
    println!("Starting all GraphDB components...");
    let mut errors = Vec::new();

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
        println!("Starting GraphDB Daemon on port {}...", port);
        if !is_port_free(port).await {
            println!("Port {} is already in use for GraphDB Daemon. Attempting to stop existing process.", port);
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
    println!("Starting REST API on port {}...", rest_port);
    if !is_port_free(rest_port).await {
        println!("Port {} is already in use for REST API. Attempting to stop existing process.", rest_port);
        if let Err(e) = stop_process_by_port("REST API", rest_port).await {
            warn!("Failed to stop existing process on port {}: {}", rest_port, e);
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
        let health_check_timeout = Duration::from_secs(15);
        let poll_interval = Duration::from_millis(500);
        let start_time = tokio::time::Instant::now();
        let mut is_running = false;

        while start_time.elapsed() < health_check_timeout {
            if tokio::net::TcpStream::connect(&addr_check).await.is_ok() {
                info!("REST API server started and reachable on port {}.", rest_port);
                println!("REST API server started on port {}.", rest_port);
                is_running = true;
                break;
            }
            tokio::time::sleep(poll_interval).await;
        }

        if !is_running {
            error!("REST API on port {} failed to become reachable within {} seconds", rest_port, health_check_timeout.as_secs());
            errors.push(format!("REST API on port {}: Failed to become reachable", rest_port));
        }
    }

    // Start Storage Daemon
    let actual_storage_config = storage_config.unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
    if let Err(e) = start_storage_interactive(
        storage_port,
        Some(actual_storage_config),
        None,
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone(),
    ).await {
        error!("Failed to start Storage Daemon: {}", e);
        errors.push(format!("Storage Daemon: {}", e));
    }

    if errors.is_empty() {
        info!("All GraphDB components started successfully");
        println!("All GraphDB components started successfully.");
        Ok(())
    } else {
        error!("Failed to start one or more GraphDB components: {:?}", errors);
        Err(anyhow!("Failed to start one or more components: {:?}", errors))
    }
}


/// This function relies exclusively on the `GLOBAL_DAEMON_REGISTRY` to find
/// running components. It first retrieves all registered daemons, then checks
/// the live status of each one by its PID and port. It provides detailed
/// information for each component type, including health checks for the REST API
/// and configuration details for the Storage daemons.
/// Displays full status summary of all components.
/// This function relies exclusively on the `GLOBAL_DAEMON_REGISTRY` to find
/// running components. It first retrieves all registered daemons, then checks
/// the live status of each one by its PID and port. It provides detailed
/// information for each component type, including health checks for the REST API
/// and configuration details for the Storage daemons.
/// Displays full status summary of all components.
/// Enhanced version of display_full_status_summary with better storage formatting
pub async fn display_full_status_summary(
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>
) -> Result<()> {
    info!("Displaying full status summary");
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry contents: {:?}", all_daemons);

    // GraphDB Daemon status (unchanged)
    let daemon_ports: Vec<u16> = futures::stream::iter(all_daemons.iter().filter(|d| d.service_type == "main"))
        .filter_map(|d| async move {
            let mut attempts = 0;
            let max_attempts = 5;
            while attempts < max_attempts {
                if check_process_status_by_port("GraphDB Daemon", d.port).await {
                    debug!("Found running GraphDB Daemon on port {}", d.port);
                    return Some(d.port);
                }
                debug!("No process found for GraphDB Daemon on port {} (attempt {})", d.port, attempts + 1);
                tokio::time::sleep(Duration::from_millis(500)).await;
                attempts += 1;
            }
            None
        })
        .collect::<Vec<u16>>()
        .await;
    
    if daemon_ports.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", "Down", "N/A", "No daemons found in registry.");
    } else {
        for (i, &port) in daemon_ports.iter().enumerate() {
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "main");
            let details = if let Some(meta) = metadata {
                format!("PID: {} | Core Graph Processing", meta.pid)
            } else {
                "Core Graph Processing".to_string()
            };
            let component_name = if i == 0 { "GraphDB Daemon" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, "Running", port, details);
        }
    }
    println!("\n");

    // REST API status (unchanged)
    let rest_ports: Vec<u16> = futures::stream::iter(all_daemons.iter().filter(|d| d.service_type == "rest"))
        .filter_map(|d| async move {
            let mut attempts = 0;
            let max_attempts = 5;
            while attempts < max_attempts {
                if check_process_status_by_port("REST API", d.port).await {
                    debug!("Found running REST API on port {}", d.port);
                    return Some(d.port);
                }
                debug!("No process found for REST API on port {} (attempt {})", d.port, attempts + 1);
                tokio::time::sleep(Duration::from_millis(500)).await;
                attempts += 1;
            }
            None
        })
        .collect::<Vec<u16>>()
        .await;

    if rest_ports.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "REST API", "Down", "N/A", "No REST API servers found in registry.");
    } else {
        let client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .context("Failed to build reqwest client")?;
            
        for (i, &port) in rest_ports.iter().enumerate() {
            let mut rest_api_status = "Running";
            let mut rest_api_details = String::new();
            
            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    rest_api_details = "Health: OK".to_string();
                    let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
                    if let Ok(v_resp) = client.get(&version_url).send().await {
                        if v_resp.status().is_success() {
                            if let Ok(v_json) = v_resp.json::<serde_json::Value>().await {
                                let version = v_json["version"].as_str().unwrap_or("N/A");
                                rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                            }
                        }
                    }
                },
                _ => {
                    rest_api_status = "Down";
                    rest_api_details = "Health check failed".to_string();
                }
            }
            
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "rest");
            if let Some(meta) = metadata {
                rest_api_details = format!("PID: {} | {}", meta.pid, rest_api_details);
            }
            
            let component_name = if i == 0 { "REST API" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, rest_api_status, port, rest_api_details);
            
            if let Some(meta) = metadata {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Directory: {:?}", meta.data_dir));
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", "Service Type: HTTP REST API");
            }
            
            if rest_ports.len() > 1 && i < rest_ports.len() - 1 {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", "");
            }
        }
    }
    println!("\n");

    // Storage Daemon status
    let storage_ports: Vec<u16> = futures::stream::iter(all_daemons.iter().filter(|d| d.service_type == "storage"))
        .filter_map(|d| async move {
            let mut attempts = 0;
            let max_attempts = 5;
            while attempts < max_attempts {
                if check_process_status_by_port("Storage Daemon", d.port).await {
                    debug!("Found running Storage Daemon on port {}", d.port);
                    return Some(d.port);
                }
                debug!("No process found for Storage Daemon on port {} (attempt {})", d.port, attempts + 1);
                tokio::time::sleep(Duration::from_millis(500)).await;
                attempts += 1;
            }
            None
        })
        .collect::<Vec<u16>>()
        .await;

    if storage_ports.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for (i, &port) in storage_ports.iter().enumerate() {
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running"
            } else {
                "Down"
            };
            
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "storage");
            let pid_info = if let Some(meta) = metadata {
                format!("PID: {}", meta.pid)
            } else {
                "PID: Unknown".to_string()
            };
            
            let component_name = if i == 0 { "Storage Daemon" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, storage_daemon_status, port, pid_info);
            
            // Load port-specific config from metadata
            let mut storage_config = StorageConfig::default();
            let mut config_loaded = false;
            let mut data_dir_display = "N/A".to_string();
            let mut log_dir_display = "N/A".to_string();
            let mut config_root_display = "N/A".to_string();
            
            if let Some(meta) = metadata {
                if let Some(config_path) = &meta.config_path {
                    match load_storage_config_from_yaml(Some(config_path.clone())) {
                        Ok(config) => {
                            storage_config = config;
                            config_loaded = true;
                            data_dir_display = storage_config.data_directory
                                .as_ref()
                                .map_or("N/A".to_string(), |p| p.display().to_string());
                            log_dir_display = storage_config.log_directory
                                .as_ref()
                                .map_or("N/A".to_string(), |p| p.display().to_string());
                            config_root_display = storage_config.config_root_directory
                                .as_ref()
                                .map_or("N/A".to_string(), |p| p.display().to_string());
                            debug!("Loaded storage config for port {} from {:?}", port, config_path);
                        }
                        Err(e) => {
                            warn!("Failed to load storage config for port {} from {:?}: {}", port, config_path, e);
                            if e.to_string().contains("WouldBlock") {
                                debug!("WouldBlock error during config load for port {}", port);
                            }
                        }
                    }
                } else {
                    // Fallback to default config path
                    let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join("storage_config.yaml");
                    match load_storage_config_from_yaml(Some(config_path.clone())) {
                        Ok(config) => {
                            storage_config = config;
                            config_loaded = true;
                            data_dir_display = storage_config.data_directory
                                .as_ref()
                                .map_or("N/A".to_string(), |p| p.display().to_string());
                            log_dir_display = storage_config.log_directory
                                .as_ref()
                                .map_or("N/A".to_string(), |p| p.display().to_string());
                            config_root_display = storage_config.config_root_directory
                                .as_ref()
                                .map_or("N/A".to_string(), |p| p.display().to_string());
                            debug!("Loaded default storage config for port {} from {:?}", port, config_path);
                        }
                        Err(e) => {
                            warn!("Failed to load default storage config for port {}: {}", port, e);
                        }
                    }
                }
            }

            // Display config only if loaded
            if config_loaded {
                let engine_config_lines = format_engine_config(&storage_config);
                for config_line in engine_config_lines {
                    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", config_line);
                }

                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Directory: {}", data_dir_display));
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Log Directory: {}", log_dir_display));
                
                // Fixed: Ensure Config Root is displayed correctly when a config is loaded.
                // Also, check if the config_root_directory is None and use the default value.
                let final_config_root = if config_root_display == "N/A" {
                    DEFAULT_CONFIG_ROOT_DIRECTORY_STR.to_string()
                } else {
                    config_root_display.clone()
                };
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Config Root: {}", final_config_root));

                let cluster_range = storage_config.cluster_range.clone();
                if is_port_in_cluster_range(port, &cluster_range) {
                    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Range: {}", cluster_range));
                } else {
                    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Range: {} (Port {} is outside this range!)", cluster_range, port));
                }
            } else {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", "Configuration not loaded due to error");
            }
            
            if storage_ports.len() > 1 && i < storage_ports.len() - 1 {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", "");
            }
        }
    }

    *storage_daemon_port_arc.lock().await = storage_ports.first().copied();
    *rest_api_port_arc.lock().await = rest_ports.first().copied();
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



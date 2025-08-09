
// server/src/cli/handlers.rs

// This file contains the handlers for various CLI commands, encapsulating
// the logic for interacting with daemon, REST API, and storage components.

use anyhow::{Result, Context, anyhow}; // Added `anyhow` macro import
use std::collections::HashMap;
use std::path::{PathBuf, Path};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use rand;
use std::sync::Arc;
use std::net::{IpAddr, SocketAddr};
use tokio::process::Command as TokioCommand;
use std::process::Stdio;
use std::time::{Duration, Instant};
use std::ops::RangeInclusive;
use std::io::{self, Write};
use futures::future; // Added for parallel execution
use futures::stream::StreamExt;
use sysinfo::Pid;
use chrono::Utc;
use config::Config;
use log::{info, error, warn, debug};
use std::fs;
use reqwest::Client;
use serde_json::Value;
use fs2;

// Import command structs from commands.rs
use crate::cli::commands::{CommandType, Commands, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopAction, StopArgs,
                           StartAction, ReloadArgs, CliArgs, StatusAction, ReloadAction, RestartArgs, RestartAction};
use crate::cli::config::{load_storage_config_str as load_storage_config, DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, 
                         DEFAULT_STORAGE_PORT, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, DEFAULT_REST_CONFIG_PATH_RELATIVE,
                         DEFAULT_CONFIG_ROOT_DIRECTORY_STR, DEFAULT_DAEMON_CONFIG_PATH_RELATIVE, DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE,
                         CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, DAEMON_REGISTRY_DB_PATH, StorageConfig, 
                         get_default_rest_port_from_config, load_main_daemon_config, load_daemon_config, load_storage_config_from_yaml, 
                         daemon_api_storage_engine_type_to_string, RestApiConfig, MainDaemonConfig, load_rest_config, 
                         CliConfig, default_config_root_directory, CliTomlStorageConfig, load_cli_config};
use crate::cli::daemon_management::{find_running_storage_daemon_port, clear_all_daemon_processes, start_daemon_process, 
                                    stop_daemon_api_call, handle_internal_daemon_run, load_storage_config_path_or_default,
                                    run_command_with_timeout, is_port_free, find_pid_by_port, is_rest_api_running,
                                    find_all_running_daemon_ports, check_process_status_by_port, parse_cluster_range,
                                    find_all_running_rest_api_ports, start_daemon_with_pid, is_port_listening,
                                    stop_process_by_port}; // Added handle_internal_daemon_run
pub use crate::cli::handlers_utils::{get_current_exe_path, format_engine_config, print_welcome_screen, write_registry_fallback, 
                                     read_registry_fallback, clear_terminal_screen, ensure_daemon_registry_paths_exist,
                                     execute_storage_query};
pub use crate::cli::handlers_storage::{storage, start_storage_interactive, stop_storage_interactive, 
                                       display_storage_daemon_status, handle_storage_command, handle_storage_command_interactive, 
                                       stop_storage, use_storage_engine, handle_save_storage, reload_storage_interactive};
pub use crate::cli::handlers_rest::{RestArgs, rest, display_rest_api_status, handle_rest_command, handle_rest_command_interactive, 
                                    start_rest_api_interactive, stop_rest_api_interactive,  display_rest_api_health,
                                    display_rest_api_version, register_user, authenticate_user, execute_graph_query};                                     
use daemon_api::{stop_daemon, start_daemon};
use daemon_api::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::storage_engine::config::{StorageEngineType};
use serde_yaml2 as serde_yaml; // Use serde_yaml2 for YAML parsing

// Placeholder imports for daemon and rest args, assuming they exist in your project structure
#[derive(Debug, Clone)]
pub struct DaemonArgs {
    pub port: Option<u16>,
    pub cluster: Option<String>,
}

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

/// Displays the Raft status for a storage daemon running on the specified port.
pub async fn display_raft_status(port: Option<u16>) -> Result<()> {
    let port = port.unwrap_or(8081); // Default port for storage daemon
    // Placeholder: Implement actual Raft status checking logic here
    // For example, query the storage daemon's Raft node status via an API or internal call
    println!("Checking Raft status for storage daemon on port {}...", port);
    println!("Raft status: Not implemented yet (placeholder).");
    // TODO: Add actual Raft status checking logic, e.g., querying a Raft node's state
    Ok(())
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

/// Displays the status of the entire cluster. (Placeholder)
pub async fn display_cluster_status() {
    println!("\n--- Cluster Status ---");
    println!("Cluster status is a placeholder. In a real implementation, this would query all daemons in the cluster.");
    println!("--------------------------------------------------");
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
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry contents: {:?}", all_daemons);

    // Enhanced Daemon status with individual daemon display
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
            
            // Show "GraphDB Daemon" only for the first daemon, empty for others
            let component_name = if i == 0 { "GraphDB Daemon" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, "Running", port, details);
        }
    }
    println!("\n");

    // REST API status - Enhanced with individual API display like daemons
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
            
            // Health check
            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    rest_api_details = "Health: OK".to_string();
                    
                    // Version check
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
            
            // Show "REST API" only for the first instance, empty for others
            let component_name = if i == 0 { "REST API" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, rest_api_status, port, rest_api_details);
            
            // Add metadata details on separate lines like other services
            if let Some(meta) = metadata {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Directory: {:?}", meta.data_dir));
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", "Service Type: HTTP REST API");
            }
            
            // Add separator between multiple REST APIs
            if rest_ports.len() > 1 && i < rest_ports.len() - 1 {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", "");
            }
        }
    }
    println!("\n");

    // Enhanced Storage status with detailed configuration
    let storage_config = load_storage_config_from_yaml(None).unwrap_or_else(|_| StorageConfig::default());
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
            
            // Show "Storage Daemon" only for the first instance, empty for others
            let component_name = if i == 0 { "Storage Daemon" } else { "" };
            println!("{:<20} {:<15} {:<10} {:<40}", component_name, storage_daemon_status, port, pid_info);
            
            // Display detailed engine configuration
            let engine_config_lines = format_engine_config(&storage_config);
            for config_line in engine_config_lines {
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", config_line);
            }
            
            // Display data directory
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Directory: {}", storage_config.data_directory.display()));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Log Directory: {}", storage_config.log_directory));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Range: {}", storage_config.cluster_range));
            
            // Add separator between multiple storage daemons
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

// --- Command Handlers for direct CLI execution (non-interactive) ---
/// Handles the top-level `start` command.
pub async fn handle_start_command(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    _config: &crate::cli::config::CliConfig,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let mut daemon_status_msg = "Not requested".to_string();
    let mut rest_api_status_msg = "Not requested".to_string();
    let mut storage_status_msg = "Not requested".to_string();

    let start_daemon_requested = port.is_some() || cluster.is_some();
    let start_rest_requested = listen_port.is_some();
    let start_storage_requested = storage_port.is_some() || storage_config_file.is_some();

    let mut errors = Vec::new();

    if start_daemon_requested {
        let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
        let daemon_result = start_daemon_instance_interactive(Some(actual_port), cluster.clone(), daemon_handles.clone()).await;
        daemon_status_msg = match daemon_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_rest_requested {
        let actual_port = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        let rest_result = start_rest_api_interactive(Some(actual_port), None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await;
        rest_api_status_msg = match rest_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("REST API on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_storage_requested {
        let actual_port = storage_port.unwrap_or(DEFAULT_STORAGE_PORT);
        let actual_config_file = storage_config_file.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
        let storage_result = start_storage_interactive(
            Some(actual_port),
            Some(actual_config_file),
            None,
            rest_api_shutdown_tx_opt.clone(), // Reuse existing mutex to avoid registry conflicts
            rest_api_handle.clone(), // Reuse existing handle
            rest_api_port_arc.clone(), // Reuse existing port arc
        ).await;
        storage_status_msg = match storage_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Storage Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    println!("\n--- Component Startup Summary ---");
    println!("{:<15} {:<50}", "Component", "Status");
    println!("{:-<15} {:-<50}", "", "");
    println!("{:<15} {:<50}", "GraphDB", daemon_status_msg);
    println!("{:<15} {:<50}", "REST API", rest_api_status_msg);
    println!("{:<15} {:<50}", "Storage", storage_status_msg);
    println!("---------------------------------\n");

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to start one or more components: {:?}", errors))
    }
}

// --- Command Handlers for direct CLI execution (non-interactive) ---
/// Handles the top-level `start` command.
pub async fn handle_start_command_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    _config: &crate::cli::config::CliConfig,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let mut daemon_status_msg = "Not requested".to_string();
    let mut rest_api_status_msg = "Not requested".to_string();
    let mut storage_status_msg = "Not requested".to_string();

    let start_daemon_requested = port.is_some() || cluster.is_some();
    let start_rest_requested = listen_port.is_some();
    let start_storage_requested = storage_port.is_some() || storage_config_file.is_some();

    let mut errors = Vec::new();

    if start_daemon_requested {
        let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
        let daemon_result = start_daemon_instance_interactive(Some(actual_port), cluster.clone(), daemon_handles.clone()).await;
        daemon_status_msg = match daemon_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_rest_requested {
        let actual_port = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        let rest_result = start_rest_api_interactive(Some(actual_port), None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await;
        rest_api_status_msg = match rest_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("REST API on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_storage_requested {
        let actual_port = storage_port.unwrap_or(DEFAULT_STORAGE_PORT);
        let actual_config_file = storage_config_file.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
        let storage_result = start_storage_interactive(
            Some(actual_port),
            Some(actual_config_file),
            None,
            rest_api_shutdown_tx_opt.clone(), // Reuse existing mutex to avoid registry conflicts
            rest_api_handle.clone(), // Reuse existing handle
            rest_api_port_arc.clone(), // Reuse existing port arc
        ).await;
        storage_status_msg = match storage_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Storage Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    println!("\n--- Component Startup Summary ---");
    println!("{:<15} {:<50}", "Component", "Status");
    println!("{:-<15} {:-<50}", "", "");
    println!("{:<15} {:<50}", "GraphDB", daemon_status_msg);
    println!("{:<15} {:<50}", "REST API", rest_api_status_msg);
    println!("{:<15} {:<50}", "Storage", storage_status_msg);
    println!("---------------------------------\n");

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to start one or more components: {:?}", errors))
    }
}

/// Handles `stop` subcommand for direct CLI execution.
pub async fn handle_stop_command(args: StopArgs) -> Result<()> {
    info!("Processing stop command with args: {:?}", args);
    match args.action.unwrap_or(StopAction::All) {
        StopAction::All => {
            println!("Stopping all GraphDB components...");
            info!("Initiating stop for all components");

            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            debug!("Initial daemon registry state: {:?}", all_daemons);
            if all_daemons.is_empty() {
                println!("No running components found in registry.");
                info!("No components registered in GLOBAL_DAEMON_REGISTRY");
                return Ok(());
            }

            let mut stopped_count = 0;
            let mut errors = Vec::new();

            // Stop REST API daemons
            let rest_daemons: Vec<DaemonMetadata> = all_daemons
                .iter()
                .filter(|d| d.service_type == "rest")
                .cloned()
                .collect();
            for daemon in rest_daemons {
                println!("Stopping REST API on port {} (PID: {})...", daemon.port, daemon.pid);
                info!("Attempting to stop REST API on port {} (PID: {})", daemon.port, daemon.pid);
                match stop_rest_api_interactive(
                    Some(daemon.port),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                )
                .await
                {
                    Ok(()) => {
                        if is_port_free(daemon.port).await {
                            info!("REST API on port {} stopped successfully", daemon.port);
                            stopped_count += 1;
                        } else {
                            warn!("REST API on port {} stopped but port is still in use", daemon.port);
                            errors.push(format!("REST API on port {}: port still in use", daemon.port));
                        }
                    }
                    Err(e) => {
                        println!("Failed to stop REST API on port {}: {}", daemon.port, e);
                        error!("Failed to stop REST API on port {}: {}", daemon.port, e);
                        errors.push(format!("REST API on port {}: {}", daemon.port, e));
                    }
                }
            }

            // Stop main daemons
            let main_daemons: Vec<DaemonMetadata> = all_daemons
                .iter()
                .filter(|d| d.service_type == "main")
                .cloned()
                .collect();
            for daemon in main_daemons {
                println!("Stopping main daemon on port {} (PID: {})...", daemon.port, daemon.pid);
                info!("Attempting to stop main daemon on port {} (PID: {})", daemon.port, daemon.pid);
                match stop_main_interactive(Some(daemon.port), Arc::new(TokioMutex::new(None))).await {
                    Ok(()) => {
                        if is_port_free(daemon.port).await {
                            info!("Main daemon on port {} stopped successfully", daemon.port);
                            stopped_count += 1;
                        } else {
                            warn!("Main daemon on port {} stopped but port is still in use", daemon.port);
                            errors.push(format!("Main daemon on port {}: port still in use", daemon.port));
                        }
                    }
                    Err(e) => {
                        println!("Failed to stop main daemon on port {}: {}", daemon.port, e);
                        error!("Failed to stop main daemon on port {}: {}", daemon.port, e);
                        errors.push(format!("Main daemon on port {}: {}", daemon.port, e));
                    }
                }
            }

            // Stop storage daemons
            let storage_daemons: Vec<DaemonMetadata> = all_daemons
                .iter()
                .filter(|d| d.service_type == "storage")
                .cloned()
                .collect();
            for daemon in storage_daemons {
                println!("Stopping storage daemon on port {} (PID: {})...", daemon.port, daemon.pid);
                info!("Attempting to stop storage daemon on port {} (PID: {})", daemon.port, daemon.pid);
                match stop_storage_interactive(
                    Some(daemon.port),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                )
                .await
                {
                    Ok(()) => {
                        if is_port_free(daemon.port).await {
                            info!("Storage daemon on port {} stopped successfully", daemon.port);
                            stopped_count += 1;
                        } else {
                            warn!("Storage daemon on port {} stopped but port is still in use", daemon.port);
                            errors.push(format!("Storage daemon on port {}: port still in use", daemon.port));
                        }
                    }
                    Err(e) => {
                        println!("Failed to stop storage daemon on port {}: {}", daemon.port, e);
                        error!("Failed to stop storage daemon on port {}: {}", daemon.port, e);
                        errors.push(format!("Storage daemon on port {}: {}", daemon.port, e));
                    }
                }
            }

            // Clear any remaining registry entries
            let remaining_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            if !remaining_daemons.is_empty() {
                warn!("Found stale registry entries after stopping components: {:?}", remaining_daemons);
                for daemon in remaining_daemons {
                    if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type(&daemon.service_type, daemon.port).await {
                        warn!("Failed to remove stale {} daemon on port {} from registry: {}", daemon.service_type, daemon.port, e);
                        errors.push(format!("Failed to remove stale {} daemon on port {}: {}", daemon.service_type, daemon.port, e));
                    } else {
                        info!("Removed stale {} daemon on port {} from registry", daemon.service_type, daemon.port);
                    }
                }
            }

            println!(
                "Stop all completed: {} components stopped, {} failed.",
                stopped_count,
                errors.len()
            );
            info!(
                "Stop all completed: {} components stopped, {} failed",
                stopped_count,
                errors.len()
            );

            if errors.is_empty() {
                Ok(())
            } else {
                Err(anyhow!("Failed to stop one or more components: {:?}", errors))
            }
        }
        StopAction::Storage { port } => {
            info!("Stopping storage daemon on port {:?}", port);
            stop_storage_interactive(
                port,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            )
            .await
            .map_err(|e| anyhow!("Failed to stop storage daemon: {}", e))?;
            if let Some(p) = port {
                if is_port_free(p).await {
                    info!("Storage daemon on port {} stopped successfully", p);
                    println!("Storage daemon on port {} stopped.", p);
                    Ok(())
                } else {
                    Err(anyhow!("Storage daemon on port {} stopped but port is still in use", p))
                }
            } else {
                Ok(())
            }
        }
        StopAction::Rest { port } => {
            info!("Stopping REST API on port {:?}", port);
            stop_rest_api_interactive(
                port,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            )
            .await
            .map_err(|e| anyhow!("Failed to stop REST API: {}", e))?;
            if let Some(p) = port {
                if is_port_free(p).await {
                    info!("REST API on port {} stopped successfully", p);
                    println!("REST API on port {} stopped.", p);
                    Ok(())
                } else {
                    Err(anyhow!("REST API on port {} stopped but port is still in use", p))
                }
            } else {
                Ok(())
            }
        }
        StopAction::Daemon { port } => {
            info!("Stopping main daemon on port {:?}", port);
            stop_main_interactive(port, Arc::new(TokioMutex::new(None)))
                .await
                .map_err(|e| anyhow!("Failed to stop main daemon: {}", e))?;
            if let Some(p) = port {
                if is_port_free(p).await {
                    info!("Main daemon on port {} stopped successfully", p);
                    println!("Main daemon on port {} stopped.", p);
                    Ok(())
                } else {
                    Err(anyhow!("Main daemon on port {} stopped but port is still in use", p))
                }
            } else {
                Ok(())
            }
        }
    }
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

pub async fn handle_status_command(
    status_args: StatusArgs,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match status_args.action {
        Some(StatusAction::Rest { port, cluster }) => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_rest_api_status(Some(p), rest_api_port_arc.clone()).await;
                }
            } else {
                display_rest_api_status(port, rest_api_port_arc).await;
            }
        }
        Some(StatusAction::Daemon { port, cluster }) => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_daemon_status(Some(p)).await;
                }
            } else {
                display_daemon_status(port).await;
            }
        }
        Some(StatusAction::Storage { port, cluster }) => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_storage_daemon_status(Some(p), storage_daemon_port_arc.clone()).await;
                }
            } else {
                display_storage_daemon_status(port, storage_daemon_port_arc).await;
            }
        }
        Some(StatusAction::Cluster) => {
            display_cluster_status().await;
        }
        Some(StatusAction::All) | None => {
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await?;
        }
        Some(StatusAction::Summary) => {
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await?;
        }
        Some(StatusAction::Raft { port }) => {
            display_raft_status(port).await?;
        }
    }
    Ok(())
}

/*
pub async fn execute_storage_query() {
    println!("Executing storage query...");
    println!("Storage query executed (placeholder).");
}
*/
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

/// Handles the interactive 'reload rest' command.
pub async fn reload_rest_interactive(
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    println!("Reloading REST API server...");

    let running_ports = find_all_running_rest_api_ports().await;

    // Stop all found REST API instances
    stop_rest_api_interactive(
        None, // Stop all REST API instances
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await?;

    // Start new instances
    if running_ports.is_empty() {
        println!("No REST API servers found running to reload. Starting one on default port {}.", DEFAULT_REST_API_PORT);
        start_rest_api_interactive(
            Some(DEFAULT_REST_API_PORT),
            None,
            rest_api_shutdown_tx_opt,
            rest_api_port_arc,
            rest_api_handle,
        ).await?;
    } else {
        for &port in &running_ports {
            start_rest_api_interactive(
                Some(port),
                None,
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
            ).await?;
            println!("REST API server restarted on port {}.", port);
        }
    }

    println!("REST API server reloaded.");
    Ok(())
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

/// Handles the interactive 'reload cluster' command.
pub async fn reload_cluster_interactive() -> Result<()> {
    println!("Reloading cluster configuration...");
    println!("A full cluster reload involves coordinating restarts/reloads across multiple daemon instances.");
    println!("This is a placeholder for complex cluster management logic.");
    println!("You might need to stop and restart individual daemons or use a cluster-wide command.");
    
    Ok(())
}

/// Handles the interactive 'restart' command.
#[allow(clippy::too_many_arguments)]
pub async fn handle_restart_command_interactive(
    restart_args: RestartArgs,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match restart_args.action {
        RestartAction::All {
            port: _,
            cluster: _,
            daemon_port,
            daemon_cluster,
            listen_port: _,
            rest_port,
            rest_cluster,
            storage_port,
            storage_cluster,
            storage_config_file,
        } => {
            println!("Restarting all GraphDB components...");
            stop_all_interactive(
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;

            let storage_config_pathbuf = storage_config_file.map(PathBuf::from);

            handle_start_all_interactive(
                daemon_port,
                daemon_cluster,
                rest_port,
                rest_cluster,
                storage_port,
                storage_cluster,
                storage_config_pathbuf,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
            println!("All GraphDB components restarted.");
        },
        RestartAction::Rest { port, cluster, rest_port, rest_cluster } => {
            println!("Restarting REST API server...");
            let ports_to_restart = if let Some(p) = port.or(rest_port) {
                vec![p]
            } else {
                find_all_running_rest_api_ports().await
            };
            let effective_cluster = cluster.or(rest_cluster);

            if ports_to_restart.is_empty() {
                println!("No REST API servers found running to restart. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                start_rest_api_interactive(
                    Some(DEFAULT_REST_API_PORT),
                    effective_cluster.clone(),
                    rest_api_shutdown_tx_opt,
                    rest_api_port_arc,
                    rest_api_handle,
                ).await?;
            } else {
                for &port in &ports_to_restart {
                    stop_rest_api_interactive(
                        Some(port),
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_port_arc.clone(),
                        rest_api_handle.clone(),
                    ).await?;
                    start_rest_api_interactive(
                        Some(port),
                        effective_cluster.clone(),
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_port_arc.clone(),
                        rest_api_handle.clone(),
                    ).await?;
                    println!("REST API server restarted on port {}.", port);
                }
            }
        },
        RestartAction::Daemon { port, cluster, daemon_port, daemon_cluster } => {
            let actual_port = port.or(daemon_port).unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Restarting GraphDB Daemon on port {}...", actual_port);
            stop_daemon_instance_interactive(Some(actual_port), daemon_handles.clone()).await?;
            start_daemon_instance_interactive(
                Some(actual_port),
                cluster.or(daemon_cluster),
                daemon_handles,
            ).await?;
            println!("GraphDB Daemon restarted on port {}.", actual_port);
        },
        RestartAction::Storage { port, config_file, cluster, storage_port, storage_cluster } => {
            let actual_port = port.or(storage_port).unwrap_or_else(|| {
                load_storage_config(None)
                    .map(|c| c.default_port)
                    .unwrap_or(DEFAULT_STORAGE_PORT)
            });
            println!("Restarting Storage Daemon on port {}...", actual_port);
            stop_storage_interactive(
                Some(actual_port),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;
            start_storage_interactive(
                Some(actual_port),
                config_file,
                cluster.or(storage_cluster),
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
            println!("Storage Daemon restarted on port {}.", actual_port);
        },
        RestartAction::Cluster => {
            println!("Restarting cluster configuration...");
            println!("A full cluster restart involves coordinating restarts across multiple daemon instances.");
            println!("This is a placeholder for complex cluster management logic.");
        },
    }
    Ok(())
}

pub async fn handle_reload_command(reload_args: ReloadArgs) -> Result<(), anyhow::Error> {
    let action_to_perform = reload_args.action.unwrap_or(ReloadAction::All);
    let mut errors = Vec::new();

    // Load configuration for cluster range
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

    match action_to_perform {
        ReloadAction::All => {
            println!("Reloading all GraphDB components...");
            if let Err(e) = stop_daemon().await {
                error!("Failed to stop all components: {}", e);
                errors.push(format!("Stop all components: {}", e));
            }

            // Define reserved ports
            let rest_port = DEFAULT_REST_API_PORT;
            let storage_port = CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
            let reserved_ports = vec![rest_port, storage_port];

            // Reload daemons
            let daemon_ports = if let Some(range) = cluster_range {
                parse_cluster_range(&range)?
            } else {
                vec![DEFAULT_DAEMON_PORT]
            };
            for port in daemon_ports {
                if reserved_ports.contains(&port) {
                    warn!("Skipping daemon reload on port {}: reserved for another service.", port);
                    errors.push(format!("Daemon on port {}: reserved for another service", port));
                    continue;
                }
                println!("Reloading GraphDB Daemon on port {}...", port);
                if let Err(e) = stop_process_by_port("GraphDB Daemon", port).await {
                    warn!("Failed to stop daemon on port {}: {}", port, e);
                }
                match start_daemon(Some(port), None, reserved_ports.clone(), "main").await {
                    Ok(_) => {
                        info!("GraphDB Daemon restarted on port {}.", port);
                        println!("GraphDB Daemon restarted on port {}.", port);
                    }
                    Err(e) => {
                        error!("Failed to restart daemon on port {}: {}", port, e);
                        errors.push(format!("Daemon on port {}: {}", port, e));
                    }
                }
            }

            // Reload REST API
            let rest_ports_to_restart = find_all_running_rest_api_ports().await;
            for &port in &rest_ports_to_restart {
                println!("Reloading REST API on port {}...", port);
                if let Err(e) = stop_process_by_port("REST API", port).await {
                    warn!("Failed to stop REST API on port {}: {}", port, e);
                }
                match start_daemon(Some(port), None, reserved_ports.clone(), "rest").await {
                    Ok(_) => {
                        info!("REST API server restarted on port {}.", port);
                        println!("REST API server restarted on port {}.", port);
                    }
                    Err(e) => {
                        error!("Failed to restart REST API on port {}: {}", port, e);
                        errors.push(format!("REST API on port {}: {}", port, e));
                    }
                }
            }
            if rest_ports_to_restart.is_empty() {
                println!("No REST API servers found running. Starting one on port {}.", rest_port);
                if let Err(e) = stop_process_by_port("REST API", rest_port).await {
                    warn!("Failed to stop REST API on port {}: {}", rest_port, e);
                }
                match start_daemon(Some(rest_port), None, reserved_ports.clone(), "rest").await {
                    Ok(_) => {
                        info!("REST API server started on port {}.", rest_port);
                        println!("REST API server started on port {}.", rest_port);
                    }
                    Err(e) => {
                        error!("Failed to start REST API on port {}: {}", rest_port, e);
                        errors.push(format!("REST API on port {}: {}", rest_port, e));
                    }
                }
            }

            // Reload Storage Daemon
            let storage_ports = find_running_storage_daemon_port().await;
            let storage_port_to_restart = storage_ports.first().copied().unwrap_or(storage_port);
            println!("Reloading Storage Daemon on port {}...", storage_port_to_restart);
            if let Err(e) = stop_process_by_port("Storage Daemon", storage_port_to_restart).await {
                warn!("Failed to stop Storage Daemon on port {}: {}", storage_port_to_restart, e);
            }
            match start_daemon(Some(storage_port_to_restart), None, reserved_ports.clone(), "storage").await {
                Ok(_) => {
                    info!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                    println!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                }
                Err(e) => {
                    error!("Failed to restart Storage Daemon on port {}: {}", storage_port_to_restart, e);
                    errors.push(format!("Storage Daemon on port {}: {}", storage_port_to_restart, e));
                }
            }
        }
        ReloadAction::Rest => {
            println!("Reloading REST API server...");
            let reserved_ports = vec![CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
            let rest_port = DEFAULT_REST_API_PORT;
            let running_ports = find_all_running_rest_api_ports().await;
            if running_ports.is_empty() {
                println!("No REST API servers found running. Starting one on port {}.", rest_port);
                if let Err(e) = stop_process_by_port("REST API", rest_port).await {
                    warn!("Failed to stop REST API on port {}: {}", rest_port, e);
                }
                match start_daemon(Some(rest_port), None, reserved_ports.clone(), "rest").await {
                    Ok(_) => {
                        info!("REST API server started on port {}.", rest_port);
                        println!("REST API server started on port {}.", rest_port);
                    }
                    Err(e) => {
                        error!("Failed to start REST API on port {}: {}", rest_port, e);
                        errors.push(format!("REST API on port {}: {}", rest_port, e));
                    }
                }
            } else {
                for &port in &running_ports {
                    println!("Reloading REST API on port {}...", port);
                    if let Err(e) = stop_process_by_port("REST API", port).await {
                        warn!("Failed to stop REST API on port {}: {}", port, e);
                    }
                    match start_daemon(Some(port), None, reserved_ports.clone(), "rest").await {
                        Ok(_) => {
                            info!("REST API server restarted on port {}.", port);
                            println!("REST API server restarted on port {}.", port);
                        }
                        Err(e) => {
                            error!("Failed to restart REST API on port {}: {}", port, e);
                            errors.push(format!("REST API on port {}: {}", port, e));
                        }
                    }
                }
            }
        }
        ReloadAction::Storage => {
            println!("Reloading standalone Storage daemon...");
            let reserved_ports = vec![DEFAULT_REST_API_PORT];
            let storage_port = CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
            let storage_ports = find_running_storage_daemon_port().await;
            let storage_port_to_restart = storage_ports.first().copied().unwrap_or(storage_port);
            println!("Reloading Storage Daemon on port {}...", storage_port_to_restart);
            if let Err(e) = stop_process_by_port("Storage Daemon", storage_port_to_restart).await {
                warn!("Failed to stop Storage Daemon on port {}: {}", storage_port_to_restart, e);
            }
            match start_daemon(Some(storage_port_to_restart), None, reserved_ports.clone(), "storage").await {
                Ok(_) => {
                    info!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                    println!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                }
                Err(e) => {
                    error!("Failed to restart Storage Daemon on port {}: {}", storage_port_to_restart, e);
                    errors.push(format!("Storage Daemon on port {}: {}", storage_port_to_restart, e));
                }
            }
        }
        ReloadAction::Daemon { port } => {
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Reloading GraphDB daemon on port {}...", actual_port);
            let reserved_ports = vec![DEFAULT_REST_API_PORT, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
            if reserved_ports.contains(&actual_port) {
                warn!("Skipping daemon reload on port {}: reserved for another service.", actual_port);
                errors.push(format!("Daemon on port {}: reserved for another service", actual_port));
            } else {
                if let Err(e) = stop_process_by_port("GraphDB Daemon", actual_port).await {
                    warn!("Failed to stop daemon on port {}: {}", actual_port, e);
                }
                match start_daemon(Some(actual_port), None, reserved_ports.clone(), "main").await {
                    Ok(_) => {
                        info!("Daemon on port {} reloaded (stopped and restarted).", actual_port);
                        println!("Daemon on port {} reloaded (stopped and restarted).", actual_port);
                    }
                    Err(e) => {
                        error!("Failed to restart daemon on port {}: {}", actual_port, e);
                        errors.push(format!("Daemon on port {}: {}", actual_port, e));
                    }
                }
            }
        }
        ReloadAction::Cluster => {
            println!("Reloading cluster configuration...");
            let reserved_ports = vec![DEFAULT_REST_API_PORT, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
            let daemon_ports = if let Some(range) = cluster_range {
                parse_cluster_range(&range)?
            } else {
                vec![DEFAULT_DAEMON_PORT]
            };
            for port in daemon_ports {
                if reserved_ports.contains(&port) {
                    warn!("Skipping daemon reload on port {}: reserved for another service.", port);
                    errors.push(format!("Daemon on port {}: reserved for another service", port));
                    continue;
                }
                println!("Reloading GraphDB Daemon on port {}...", port);
                if let Err(e) = stop_process_by_port("GraphDB Daemon", port).await {
                    warn!("Failed to stop daemon on port {}: {}", port, e);
                }
                match start_daemon(Some(port), None, reserved_ports.clone(), "main").await {
                    Ok(_) => {
                        info!("GraphDB Daemon reloaded on port {}.", port);
                        println!("GraphDB Daemon reloaded on port {}.", port);
                    }
                    Err(e) => {
                        error!("Failed to reload daemon on port {}: {}", port, e);
                        errors.push(format!("Daemon on port {}: {}", port, e));
                    }
                }
            }
        }
    }

    if errors.is_empty() {
        info!("Reload command completed successfully");
        Ok(())
    } else {
        error!("Failed to reload one or more components: {:?}", errors);
        Err(anyhow::anyhow!("Failed to reload one or more components: {:?}", errors))
    }
}

/// Handles the top-level `reload` command for interactive use case.
/// This function serves as a wrapper for the non-interactive handle_reload_command,
/// providing the same core logic for interactive scenarios.
pub async fn handle_reload_command_interactive(
    reload_args: ReloadArgs,
) -> Result<()> {
    // For an interactive use case, you might add prompts here,
    // e.g., "Are you sure you want to reload all components? (y/N)"
    // For now, it directly calls the non-interactive logic as requested,
    // assuming the "interactive use case" means it's callable in an interactive shell.
    handle_reload_command(reload_args).await
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

pub async fn handle_save_config() -> Result<()> {
    println!("Saved configuration...");
     Ok(())
}

/// Enables or disables plugins and persists the setting to the config file.
pub async fn use_plugin(enable: bool) -> Result<()> {
    // Load the current configuration from server/src/cli/config.toml
    let config_path = PathBuf::from("server/src/cli/config.toml");
    let mut config = load_cli_config()
        .map_err(|e| anyhow!("Failed to load config from {}: {}", config_path.display(), e))?;

    // Update the plugins_enabled field
    config.enable_plugins = enable;

    // Save the updated configuration to /opt/graphdb/config.toml
    let save_path = PathBuf::from("/opt/graphdb/config.toml");
    config.save()
        .map_err(|e| anyhow!("Failed to save config to {}: {}", save_path.display(), e))?;

    println!("Plugins set to enabled: {}", enable);
    Ok(())
}
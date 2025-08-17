
// server/src/cli/handlers_rest.rs

// This file contains the handlers for REST API-related CLI commands, encapsulating
// the logic for interacting with the REST API components.

use anyhow::{Result, Context, anyhow};
use std::path::{PathBuf};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use std::time::{Duration, Instant};
use futures::stream::StreamExt;
use chrono::Utc;
use log::{info, error, warn, debug};
use std::fs;

// Import command structs from commands.rs
use crate::cli::commands::{RestCliCommand};

// Import configuration-related items
use crate::cli::config::{
    DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR, DEFAULT_DAEMON_PORT,
    RestApiConfig, load_rest_config, 
};

// Import daemon management utilities
use crate::cli::daemon_management::{
    find_all_running_rest_api_ports, 
    stop_process_by_port, find_pid_by_port,
    check_process_status_by_port, is_port_free,
};

// Import utility functions
use crate::cli::handlers_utils::{
    write_registry_fallback, read_registry_fallback, execute_storage_query,
};

use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use daemon_api::{start_daemon};

/// A temporary struct to hold the combined REST configuration for display.
#[derive(Debug)]
struct DisplayRestConfig {
    rest_api_enabled: bool,
    default_port: u16,
    cluster_range: String,
    config_root_directory: PathBuf,
}

// Placeholder struct for RestArgs
#[derive(Debug, Clone)]
pub struct RestArgs {
    pub port: Option<u16>,
    pub listen_port: Option<u16>,
    pub api_key: Option<String>,
    pub storage_port: Option<u16>,
}

// Placeholder for REST API and Storage API functions
pub mod rest {
    pub mod api {
        use anyhow::Result;
        use reqwest::Client;
        use std::time::Duration;

        // Function to check REST API status (actual health check)
        pub async fn check_rest_api_status(port: u16) -> Result<String> {
            let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
            let url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => Ok("OK".to_string()),
                Ok(resp) => Err(anyhow::anyhow!("REST API health check failed with status: {}", resp.status())),
                Err(e) => Err(anyhow::anyhow!("Failed to connect to REST API for health check: {}", e)),
            }
        }

        pub async fn perform_rest_api_health_check() -> Result<String> {
            // This function needs to know the port. It should probably be passed.
            // For now, it's a placeholder.
            Ok("Healthy".to_string())
        }
        pub async fn get_rest_api_version() -> Result<String> { Ok("0.1.0".to_string()) }
        pub async fn register_user(_username: &str, _password: &str) -> Result<()> {
            println!("Registering user {}...", _username);
            Ok(())
        }
        pub async fn authenticate_user(_username: &str, _password: &str) -> Result<String> {
            println!("Authenticating user {}...", _username);
            Ok("dummy_token".to_string())
        }
        pub async fn execute_graph_query(_query: &str, _persist: Option<bool>) -> Result<String> { Ok(format!("Query result for: {}", _query)) }
    }
}

pub async fn execute_graph_query(query_string: String, persist: Option<bool>) {
    println!("Executing graph query: '{}' (persist: {:?})", query_string, persist);
    match rest::api::execute_graph_query(&query_string, persist).await {
        Ok(result) => println!("Graph query result: {}", result),
        Err(e) => eprintln!("Failed to execute graph query: {}", e),
    }           
}    

pub async fn register_user(username: String, password: String) {
    println!("Registering user '{}'...", username);
    match rest::api::register_user(&username, &password).await {
        Ok(_) => println!("User '{}' registered successfully.", username),
        Err(e) => eprintln!("Failed to register user '{}': {}", username, e),
    }
}

pub async fn authenticate_user(username: String, password: String) {
    println!("Authenticating user '{}'...", username);
    match rest::api::authenticate_user(&username, &password).await {
        Ok(token) => println!("User '{}' authenticated successfully. Token: {}", username, token),
        Err(e) => eprintln!("Failed to authenticate user '{}': {}", username, e),
    }
}

pub async fn display_rest_api_status(port_arg: Option<u16>, rest_api_port_arc: Arc<TokioMutex<Option<u16>>>) {
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let running_rest_ports: Vec<u16> = futures::stream::iter(all_daemons.iter())
        .filter_map(|d| async move {
            if d.service_type == "rest" && check_process_status_by_port("REST API", d.port).await {
                Some(d.port)
            } else {
                None
            }
        })
        .collect::<Vec<u16>>()
        .await;
        
    let ports_to_display = match port_arg {
        Some(p) if running_rest_ports.contains(&p) => vec![p],
        Some(p) => {
            println!("No REST API server found on port {}.", p);
            vec![]
        }
        None => running_rest_ports,
    };
    
    println!("\n--- REST API Status ---");
    println!("{:<20} {:<15} {:<10} {:<50}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<50}", "", "", "", "");
    
    if ports_to_display.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<50}", "REST API", "Down", "N/A", "No REST API servers found in registry.");
    } else {
        for (i, &port) in ports_to_display.iter().enumerate() {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("Failed to build reqwest client");
                
            let mut rest_api_status = "Down";
            let mut rest_api_details = String::new();
            
            // Health check
            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    rest_api_status = "Running";
                    rest_api_details = "Health: OK".to_string();
                    
                    // Version check
                    let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
                    match client.get(&version_url).send().await {
                        Ok(v_resp) if v_resp.status().is_success() => {
                            if let Ok(v_json) = v_resp.json::<serde_json::Value>().await {
                                let version = v_json["version"].as_str().unwrap_or("N/A");
                                rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                            }
                        },
                        _ => rest_api_details = format!("{}; Version: N/A", rest_api_details),
                    }
                },
                _ => {
                    rest_api_details = "Health: Down (Failed to connect or unhealthy)".to_string();
                },
            }
            
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "rest");
            
            // Show "REST API" only for the first instance, empty for others
            let component_name = if i == 0 { "REST API" } else { "" };
            
            if let Some(meta) = metadata {
                println!("{:<20} {:<15} {:<10} {:<50}", component_name, rest_api_status, port, format!("PID: {}", meta.pid));
                println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Data Directory: {:?}", meta.data_dir));
                println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", format!("Service Type: HTTP REST API"));
                println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", rest_api_details);
            } else {
                println!("{:<20} {:<15} {:<10} {:<50}", component_name, rest_api_status, port, rest_api_details);
            }
            
            // Add separator between multiple REST APIs
            if ports_to_display.len() > 1 && i < ports_to_display.len() - 1 {
                println!("{:<20} {:<15} {:<10} {:<50}", "", "", "", "");
            }
        }
    }
    
    if port_arg.is_none() && !ports_to_display.is_empty() {
        println!("\nTo check a specific REST API, use 'status rest --port <port>'.");
    }
    
    *rest_api_port_arc.lock().await = ports_to_display.first().copied();
    println!("--------------------------------------------------");
}

pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port, cluster, rest_port, rest_cluster } => {
            // Validate mutual exclusivity
            if port.is_some() && rest_port.is_some() {
                return Err(anyhow!("Cannot specify both --port and --rest-port"));
            }
            if cluster.is_some() && rest_cluster.is_some() {
                return Err(anyhow!("Cannot specify both --cluster and --rest-cluster"));
            }

            // Select port: prefer --rest-port, then --port, then default
            let actual_port = rest_port.or(port).unwrap_or(DEFAULT_REST_API_PORT);
            // Select cluster: prefer --rest-cluster, then --cluster
            let actual_cluster = rest_cluster.or(cluster);

            // Log the selected options for debugging
            println!("Starting REST API with port: {}, cluster: {:?}", actual_port, actual_cluster);

            start_rest_api_interactive(
                Some(actual_port),
                actual_cluster,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            ).await
        }
        RestCliCommand::Stop { port } => {
            stop_rest_api_interactive(port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status { port, cluster: _cluster } => {
            display_rest_api_status(port, rest_api_port_arc).await;
            Ok(())
        }
        RestCliCommand::Health => {
            let running_ports = find_all_running_rest_api_ports().await;
            if running_ports.is_empty() {
                println!("No REST API servers found to check health.");
            } else {
                for port in running_ports {
                    match rest::api::check_rest_api_status(port).await {
                        Ok(status) => println!("REST API Health on port {}: {}", port, status),
                        Err(e) => eprintln!("Failed to check REST API health on port {}: {}", port, e),
                    }
                }
            }
            Ok(())
        }
        RestCliCommand::Version => {
            let running_ports = find_all_running_rest_api_ports().await;
            if running_ports.is_empty() {
                println!("No REST API servers found to get version from.");
            } else {
                for port in running_ports {
                    let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
                    let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
                    match client.get(&version_url).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            let v_json: serde_json::Value = resp.json().await.unwrap_or_default();
                            let version = v_json["version"].as_str().unwrap_or("N/A");
                            println!("REST API Version on port {}: {}", port, version);
                        }
                        Err(e) => eprintln!("Failed to get REST API version on port {}: {}", port, e),
                        _ => eprintln!("Failed to get REST API version on port {}.", port),
                    }
                }
            }
            Ok(())
        }
        RestCliCommand::RegisterUser { username, password } => {
            rest::api::register_user(&username, &password).await?;
            Ok(())
        }
        RestCliCommand::Authenticate { username, password } => {
            rest::api::authenticate_user(&username, &password).await?;
            Ok(())
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            rest::api::execute_graph_query(&query_string, persist).await?;
            Ok(())
        }
        RestCliCommand::StorageQuery => {
            println!("Executing storage query via REST API.");
            Ok(())
        }
    }
}

/// Handles `rest` subcommand for interactive mode.
pub async fn handle_rest_command_interactive(
    action: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match action {
        RestCliCommand::Start { port, cluster, rest_port, rest_cluster } => {
            if port.is_some() && rest_port.is_some() {
                return Err(anyhow!("Cannot specify both --port and --rest-port"));
            }
            if cluster.is_some() && rest_cluster.is_some() {
                return Err(anyhow!("Cannot specify both --cluster and --rest-cluster"));
            }
            let actual_port = rest_port.or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let actual_cluster = rest_cluster.or(cluster);
            start_rest_api_interactive(Some(actual_port), actual_cluster, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Stop { port } => {
            stop_rest_api_interactive(port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status { port, .. } => {
            display_rest_api_status(port, rest_api_port_arc).await;
            Ok(())
        }
        RestCliCommand::Health => {
            display_rest_api_health().await;
            Ok(())
        }
        RestCliCommand::Version => {
            display_rest_api_version().await;
            Ok(())
        }
        RestCliCommand::RegisterUser { username, password } => {
            register_user(username, password).await;
            Ok(())
        }
        RestCliCommand::Authenticate { username, password } => {
            authenticate_user(username, password).await;
            Ok(())
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            execute_graph_query(query_string, persist).await;
            Ok(())
        }
        RestCliCommand::StorageQuery => {
            execute_storage_query().await;
            Ok(())
        }
    }
}

/// Starts the REST API in interactive mode.
pub async fn start_rest_api_interactive(
    port: Option<u16>,
    _cluster: Option<String>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<(), anyhow::Error> {
    use fs2::FileExt;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join("rest_api/rest_api_config.yaml");
    let config_path_str = config_path.to_string_lossy().into_owned();
    let config = load_rest_config(Some(&config_path_str))
        .map_err(|e| {
            error!("Failed to load config file {:?}: {}", config_path, e);
            if config_path.exists() {
                let content = fs::read_to_string(&config_path)
                    .unwrap_or_else(|e| format!("Failed to read file: {}", e));
                error!("Config file content: {}", content);
            } else {
                error!("Config file does not exist at {:?}", config_path);
            }
            e
        })
        .ok();

    let actual_port = port.unwrap_or(DEFAULT_REST_API_PORT);
    info!("Starting REST API on port {}", actual_port);

    // Check if the port is used by a registered GraphDB component
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let is_graphdb_process = all_daemons.iter().any(|d| d.port == actual_port && (d.service_type == "main" || d.service_type == "rest" || d.service_type == "storage"));

    if check_process_status_by_port("REST API", actual_port).await {
        info!("REST API already running on port {}. Allowing multiple instances.", actual_port);
        // Skip stopping to allow multiple REST API instances
    } else if !is_port_free(actual_port).await && !is_graphdb_process {
        info!("Port {} is in use by a non-GraphDB process. Attempting to stop.", actual_port);
        stop_process_by_port("REST API", actual_port)
            .await
            .map_err(|e| anyhow!("Failed to stop existing process on port {}: {}", actual_port, e))?;
    } else if is_graphdb_process {
        info!("Port {} is in use by a GraphDB component (main, rest, or storage). Allowing multiple instances.", actual_port);
    } else {
        info!("No process found running on port {}", actual_port);
    }

    // Verify port is free only if not allowing multiple instances
    if !is_port_free(actual_port).await && !is_graphdb_process {
        warn!("Port {} is still in use by a non-GraphDB process after attempting to stop. Proceeding to start additional REST API.", actual_port);
    }

    let config = config.unwrap_or_else(|| {
        warn!("Using default REST API configuration");
        RestApiConfig {
            data_directory: PathBuf::from(format!("{}/rest_api_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR)).display().to_string(),
            log_directory: PathBuf::from("/var/log/graphdb").display().to_string(),
            default_port: actual_port,
            cluster_range: format!("{}", actual_port),
        }
    });

    let data_dir_for_metadata = config.data_directory.clone();

    // Log registry state before starting daemon
    debug!("Registry state before starting REST API: {:?}", all_daemons);

    start_daemon(Some(actual_port), None, vec![DEFAULT_DAEMON_PORT, DEFAULT_STORAGE_PORT], "rest")
        .await
        .map_err(|e| anyhow!("Failed to start REST API via daemon_api: {}", e))?;

    // Find PID with proper Option handling
    let pid = match find_pid_by_port(actual_port).await {
        Some(0) | None => {
            log::warn!("No valid PID found via lsof for REST API on port {}. Checking registry.", actual_port);
            match GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(actual_port).await {
                Ok(Some(metadata)) if metadata.service_type == "rest" => metadata.pid,
                Ok(Some(metadata)) => {
                    log::error!("Port {} is registered to a non-REST service: {}", actual_port, metadata.service_type);
                    return Err(anyhow!("Port {} is registered to a non-REST service: {}. Cannot start REST API.", actual_port, metadata.service_type));
                }
                Ok(None) => {
                    log::error!("No registry metadata found for port {}.", actual_port);
                    return Err(anyhow!("Failed to find PID for REST API on port {}. Ensure the process is binding to the port.", actual_port));
                }
                Err(e) => {
                    log::error!("Error accessing registry for port {}: {}", actual_port, e);
                    return Err(anyhow!("Failed to access registry for port {}: {}.", actual_port, e));
                }
            }
        }
        Some(p) => p,
    };

    let metadata = DaemonMetadata {
        service_type: "rest".to_string(),
        port: actual_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(PathBuf::from(data_dir_for_metadata)),
        config_path: Some(config_path),
        engine_type: None,
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    // --- FIX FOR OS ERROR 20 & ROBUST FILE HANDLING ---
    // This section has been refactored to handle file system interactions more reliably.
    let fallback_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.json");
    let fallback_dir = fallback_path.parent().ok_or_else(|| anyhow!("Invalid fallback path: {:?}", fallback_path))?;
    let lock_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.lock");

    // Retry loop for creating the parent directory
    let mut dir_attempts = 0;
    let max_dir_attempts = 3;
    while dir_attempts < max_dir_attempts {
        match tokio::fs::create_dir_all(fallback_dir).await {
            Ok(_) => {
                debug!("Successfully created directory {:?}", fallback_dir);
                break;
            }
            Err(e) => {
                dir_attempts += 1;
                let os_err_code = e.raw_os_error().unwrap_or(-1);
                error!("Failed to create directory {:?} (attempt {}/{}): {} (os error {})",
                    fallback_dir, dir_attempts, max_dir_attempts, e, os_err_code);
                if dir_attempts >= max_dir_attempts {
                    return Err(anyhow!("Failed to create directory {:?} after {} attempts: {}", fallback_dir, max_dir_attempts, e));
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
        }
    }
    
    // Now, open or create the lock file.
    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .mode(0o666) // Set permissions for the lock file
        .open(&lock_path)
        .map_err(|e| {
            let os_err_code = e.raw_os_error().unwrap_or(-1);
            anyhow!("Failed to open lock file {:?}: {} (os error {})", lock_path, e, os_err_code)
        })?;

    let mut attempts = 0;
    let max_attempts = 3;
    while attempts < max_attempts {
        match GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await {
            Ok(_) => {
                info!("Successfully registered REST API with PID {} on port {}", pid, actual_port);
                let new_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
                debug!("Registry state after registering REST API: {:?}", new_daemons);

                // Acquire exclusive lock and write to fallback file
                let mut lock_attempts = 0;
                let max_lock_attempts = 3;
                while lock_attempts < max_lock_attempts {
                    match lock_file.try_lock_exclusive() {
                        Ok(_) => {
                            debug!("Acquired exclusive lock on {:?}", lock_path);
                            if let Err(e) = write_registry_fallback(&new_daemons, &fallback_path).await {
                                warn!("Failed to write registry fallback to {:?}: {}", fallback_path, e);
                            } else {
                                debug!("Successfully wrote registry fallback to {:?}", fallback_path);
                            }
                            if let Err(e) = fs2::FileExt::unlock(&lock_file) {
                                error!("Failed to release lock on {:?}: {}", lock_path, e);
                            } else {
                                debug!("Released lock on {:?}", lock_path);
                            }
                            break;
                        }
                        Err(lock_err) => {
                            lock_attempts += 1;
                            error!("Failed to acquire lock on {:?} (attempt {}/{}): {}", lock_path, lock_attempts, max_lock_attempts, lock_err);
                            if lock_attempts >= max_lock_attempts {
                                return Err(anyhow!("Failed to acquire lock on {:?} after {} attempts: {}", lock_path, max_lock_attempts, lock_err));
                            }
                            std::thread::sleep(std::time::Duration::from_millis(500));
                        }
                    }
                }
                // Remove the lock file after the operation
                let _ = std::fs::remove_file(&lock_path);
                break;
            }
            Err(e) => {
                attempts += 1;
                error!("Failed to register REST API (attempt {}/{}): {}", attempts, max_attempts, e);
                if attempts >= max_attempts {
                    // Attempt to restore registry from fallback file
                    if let Ok(daemons) = read_registry_fallback(&fallback_path).await {
                        warn!("Restoring registry from fallback file: {:?}", daemons);
                        for daemon in daemons {
                            let _ = GLOBAL_DAEMON_REGISTRY.register_daemon(daemon).await;
                        }
                    }
                    return Err(e).context("Failed to register REST API after multiple attempts");
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    // --- END OF FIX ---

    let addr_check = format!("127.0.0.1:{}", actual_port);
    let health_check_timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(500);
    let start_time = Instant::now();

    // Lock mutexes with timeout to avoid deadlocks
    let mut shutdown_lock = tokio::time::timeout(Duration::from_secs(5), rest_api_shutdown_tx_opt.lock())
        .await
        .map_err(|_| anyhow!("Timeout acquiring shutdown mutex for REST API on port {}", actual_port))?;
    let mut port_lock = tokio::time::timeout(Duration::from_secs(5), rest_api_port_arc.lock())
        .await
        .map_err(|_| anyhow!("Timeout acquiring port mutex for REST API on port {}", actual_port))?;
    let mut handle_lock = tokio::time::timeout(Duration::from_secs(5), rest_api_handle.lock())
        .await
        .map_err(|_| anyhow!("Timeout acquiring handle mutex for REST API on port {}", actual_port))?;

    while start_time.elapsed() < health_check_timeout {
        if tokio::net::TcpStream::connect(&addr_check).await.is_ok() {
            info!("REST API started on port {} (PID {})", actual_port, pid);
            *port_lock = Some(actual_port);
            let (tx, rx) = oneshot::channel();
            *shutdown_lock = Some(tx);
            let handle = tokio::spawn(async move {
                rx.await.ok();
                info!("REST API on port {} shutting down", actual_port);
            });
            *handle_lock = Some(handle);
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    error!("REST API on port {} failed to become reachable", actual_port);
    Err(anyhow!("REST API failed to start on port {}", actual_port))
}

/// Stops a REST API instance in interactive mode.
pub async fn stop_rest_api_interactive(
    port: Option<u16>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<(), anyhow::Error> {
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        GLOBAL_DAEMON_REGISTRY
            .get_all_daemon_metadata()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|d| d.service_type == "rest")
            .map(|d| d.port)
            .collect()
    };

    for actual_port in ports_to_stop {
        let is_managed_port = rest_api_port_arc.lock().await.as_ref().map_or(false, |p| *p == actual_port);
        if is_managed_port {
            let mut handle_lock = rest_api_handle.lock().await;
            let mut shutdown_tx_lock = rest_api_shutdown_tx_opt.lock().await;
            if let Some(tx) = shutdown_tx_lock.take() {
                if tx.send(()).is_ok() {
                    log::info!("Sent shutdown signal to REST API on port {}", actual_port);
                } else {
                    log::warn!("Failed to send shutdown signal to REST API on port {}", actual_port);
                }
            }
            if let Some(handle) = handle_lock.take() {
                handle.await?;
                *rest_api_port_arc.lock().await = None;
            }
        }

        if !check_process_status_by_port("REST API", actual_port).await {
            log::info!("No REST API running on port {}", actual_port);
            if let Ok(Some(_)) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("rest", actual_port).await {
                log::info!("Removed stale REST API registry entry for port {}", actual_port);
                println!("REST API server on port {} stopped.", actual_port);
            }
            continue;
        }

        // Find PID from lsof or fallback to registry
        let pid_option: Option<u32> = {
            let lsof_pid = find_pid_by_port(actual_port).await;
            if let Some(p) = lsof_pid {
                if p != 0 {
                    Some(p)
                } else {
                    log::warn!("PID 0 found via lsof for REST API on port {}. Checking registry.", actual_port);
                    // If PID is 0, treat as not found and fall back to registry
                    match GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(actual_port).await {
                        Ok(Some(metadata)) if metadata.service_type == "rest" => Some(metadata.pid),
                        _ => {
                            log::warn!("No valid PID found for REST API on port {} in registry either.", actual_port);
                            None
                        }
                    }
                }
            } else {
                log::warn!("No PID found via lsof for REST API on port {}. Checking registry.", actual_port);
                // If lsof returned None, try registry
                match GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(actual_port).await {
                    Ok(Some(metadata)) if metadata.service_type == "rest" => Some(metadata.pid),
                    _ => {
                        log::warn!("No valid PID found for REST API on port {} in registry either.", actual_port);
                        None
                    }
                }
            }
        };

        let pid = if let Some(p) = pid_option {
            p
        } else {
            // If no PID was found after all attempts, clean up registry and continue to next port
            if let Ok(Some(_)) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("rest", actual_port).await {
                log::info!("Removed stale REST API registry entry for port {}", actual_port);
                println!("REST API server on port {} stopped.", actual_port);
            }
            continue; // Skip the rest of the loop for this port
        };

        stop_process_by_port("REST API", actual_port)
            .await
            .map_err(|e| {
                log::error!("Failed to stop REST API process on port {}: {}", actual_port, e);
                anyhow::anyhow!("Failed to stop REST API on port {}: {}", actual_port, e)
            })?;

        GLOBAL_DAEMON_REGISTRY
            .remove_daemon_by_type("rest", actual_port)
            .await
            .map_err(|e| {
                log::error!(
                    "Failed to remove REST API (port: {}, PID: {}) from registry: {}",
                    actual_port,
                    pid,
                    e
                );
                anyhow::anyhow!("Failed to remove REST API from registry: {}", e)
            })?;

        log::info!("REST API on port {} (PID: {}) stopped successfully", actual_port, pid);
        println!("REST API server on port {} stopped.", actual_port);
    }

    Ok(())
}

// --- Status Display Functions (simplified for brevity, ensure they exist elsewhere) ---

pub async fn display_rest_api_health() {
    println!("Performing REST API health check...");
    let running_ports = find_all_running_rest_api_ports().await;
    if running_ports.is_empty() {
        println!("No REST API servers found to check health.");
    } else {
        for port in running_ports {
            match rest::api::check_rest_api_status(port).await {
                Ok(health) => println!("REST API Health on port {}: {}", port, health),
                Err(e) => eprintln!("Failed to get REST API health on port {}: {}", port, e),
            }
        }
    }
}

pub async fn display_rest_api_version() {
    println!("Getting REST API version...");
    let running_ports = find_all_running_rest_api_ports().await;
    if running_ports.is_empty() {
        println!("No REST API servers found to get version from.");
    } else {
        for port in running_ports {
            let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
            let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
            match client.get(&version_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let v_json: serde_json::Value = resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    println!("REST API Version on port {}: {}", port, version);
                },
                Err(e) => eprintln!("Failed to get REST API version on port {}: {}", port, e),
                _ => eprintln!("Failed to get REST API version on port {}.", port),
            }
        }
    }
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

/// A handler for the 'show config rest' command.
/// It loads the current configuration and displays the REST API settings.
pub async fn handle_show_rest_config_command() -> Result<()> {
    // Load the main CLI configuration from the default file path.
    // We assume the load_rest_config function exists and is in scope.
    let config = load_rest_config(None)
        .map_err(|e| anyhow!("Failed to load REST API config: {}", e))?;

    println!("Current REST API Configuration:");
    println!("- default_port: {}", config.default_port);
    println!("- cluster_range: {}", config.cluster_range);
    println!("- data_directory: {}", config.data_directory);
    println!("- log_directory: {}", config.log_directory);

    Ok(())
}


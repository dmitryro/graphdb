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
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use crossterm::cursor::MoveTo;
use std::io::{self, Write};
use futures::future; // Added for parallel execution
use futures::stream::StreamExt;
use sysinfo::Pid;
use chrono::Utc;
use config::Config;
use log::{info, error, warn, debug};
use std::fs;

// Import command structs from commands.rs
use crate::cli::commands::{Commands, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopAction, StopArgs,
                           StartAction, ReloadArgs, CliArgs, StatusAction, ReloadAction, RestartArgs, RestartAction};
use crate::cli::config::{load_storage_config_str as load_storage_config, DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, 
                         DEFAULT_STORAGE_PORT, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, DEFAULT_REST_CONFIG_PATH_RELATIVE,
                         DEFAULT_CONFIG_ROOT_DIRECTORY_STR, DEFAULT_DAEMON_CONFIG_PATH_RELATIVE, DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE,
                         CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, StorageConfig, get_default_rest_port_from_config, 
                         load_main_daemon_config, load_storage_config_from_yaml, daemon_api_storage_engine_type_to_string,
                         RestApiConfig, MainDaemonConfig, load_rest_config, CliConfig, default_config_root_directory};
use crate::cli::daemon_management::{find_running_storage_daemon_port, clear_all_daemon_processes, start_daemon_process, 
                                    stop_daemon_api_call, handle_internal_daemon_run, load_storage_config_path_or_default,
                                    run_command_with_timeout, is_port_free, find_pid_by_port, is_rest_api_running,
                                    find_all_running_daemon_ports, check_process_status_by_port, parse_cluster_range,
                                    find_all_running_rest_api_ports, start_daemon_with_pid, is_port_listening}; // Added handle_internal_daemon_run
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

pub mod storage {
    pub mod api {
        use anyhow::Result;
        pub async fn check_storage_daemon_status(_port: u16) -> Result<String> { Ok("Running".to_string()) }
    }
}

/// Helper to get the path to the current executable.
pub fn get_current_exe_path() -> Result<PathBuf> {
    std::env::current_exe()
        .context("Failed to get current executable path")
}

/// Helper to find and kill a process by port. This is used for all daemon processes.
pub async fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> {
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    
    let output = run_command_with_timeout(
        "lsof",
        &["-i", &format!(":{}", port), "-t"],
        Duration::from_secs(3), // Short timeout for lsof
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

    // Add a retry loop to ensure the port is actually freed
    let start_time = Instant::now();
    let wait_timeout = Duration::from_secs(5); // Increased timeout
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
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    if ports_to_display.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No REST API servers found in registry.");
    } else {
        for &port in &ports_to_display {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build().expect("Failed to build reqwest client");

            let mut rest_api_status = "Down".to_string();
            let mut rest_api_details = String::new();

            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    rest_api_status = "Running".to_string();
                    rest_api_details = "Health: OK".to_string();

                    let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
                    match client.get(&version_url).send().await {
                        Ok(v_resp) if v_resp.status().is_success() => {
                            let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                            let version = v_json["version"].as_str().unwrap_or("N/A");
                            rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                        },
                        _ => rest_api_details = format!("{}; Version: N/A", rest_api_details),
                    }
                },
                _ => {
                    rest_api_details = "Health: Down (Failed to connect or unhealthy)".to_string();
                },
            }

            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "rest");
            if let Some(meta) = metadata {
                rest_api_details = format!("{}; PID: {}, Data Dir: {:?}", rest_api_details, meta.pid, meta.data_dir);
            }

            println!("{:<15} {:<10} {:<40}", rest_api_status, port, rest_api_details);
        }
    }

    *rest_api_port_arc.lock().await = ports_to_display.first().copied();
    println!("--------------------------------------------------");
}

/*
pub async fn display_storage_daemon_status(port_arg: Option<u16>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    use crate::cli::daemon_registry::GLOBAL_DAEMON_REGISTRY;
    use std::path::PathBuf;

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    // Storage Daemon Status
    let storage_config = StorageConfig::default();
    let storage_ports = find_running_storage_daemon_port().await;

    if storage_ports.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", "Down", "N/A", "No storage daemons found in registry.");
    } else {
   


        match port_arg {
            Some(p) =>  {
                       println!("PORT ARG WAS {}", p);
            },
            None => println!("PORT ARG WAS None"),
        }

        for &port in &storage_ports {
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running".to_string()
            } else {
                "Down".to_string()
            };
            println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, port, format!("Type: {:?}", storage_config.storage_engine_type));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_directory.display()));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
        }
    }
    println!("--------------------------------------------------");
} */

pub async fn display_storage_daemon_status(port_arg: Option<u16>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let storage_config = StorageConfig::default();
    let running_storage_ports: Vec<u16> = futures::stream::iter(all_daemons.iter())
        .filter_map(|d| async move {
            if d.service_type == "storage" && check_process_status_by_port("Storage Daemon", d.port).await {
                Some(d.port)
            } else {
                None
            }
        })
        .collect::<Vec<u16>>()
        .await;

    let ports_to_display = match port_arg {
        Some(p) if running_storage_ports.contains(&p) => vec![p],
        Some(p) => {
            println!("No Storage Daemon found on port {}.", p);
            vec![]
        }
        None => running_storage_ports,
    };

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    if ports_to_display.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for &port in &ports_to_display {
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running".to_string()
            } else {
                "Down".to_string()
            };
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "storage");
            let details = if let Some(meta) = metadata {
                format!("PID: {}, Type: {:?}", meta.pid, storage_config.storage_engine_type)
            } else {
                format!("Type: {:?}", storage_config.storage_engine_type)
            };

            println!("{:<15} {:<10} {:<40}", storage_daemon_status, port, details);
            println!("{:<15} {:<10} {:<40}", "", "", format!("Data Dir: {}", storage_config.data_directory.display()));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
        }
    }

    *storage_daemon_port_arc.lock().await = ports_to_display.first().copied();
    println!("--------------------------------------------------");
}

/*
pub async fn display_storage_daemon_status(port_arg: Option<u16>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    // 1. Find all currently running storage daemon ports.
    let all_running_ports = find_running_storage_daemon_port().await;

    // 2. Determine which ports to display based on the `port_arg`.
    let ports_to_display = match port_arg {
        Some(p) => {
            // If a specific port is provided, check if it's in the list of running ports.
            if all_running_ports.contains(&p) {
                vec![p]
            } else {
                // If the specified port is not running, print a message and exit.
                println!("\n--- Storage Daemon Status ---");
                println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
                println!("{:-<15} {:-<10} {:-<40}", "", "", "");
                println!("{:<15} {:<10} {:<40}", "Down", p, "No daemon found on this port.");
                println!("--------------------------------------------------");
                return;
            }
        },
        None => {
            // If no port is specified, display all running ports.
            all_running_ports
        }
    };

    // 3. Print the header.
    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    // 4. Iterate over the filtered or unfiltered list of ports to display the status.
    if ports_to_display.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for port in ports_to_display {
            // In a real implementation, we would load the config specific to this daemon instance,
            // but for this example, we will use the default config.
            let storage_config = StorageConfig::default();
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running"
            } else {
                "Down"
            };
            println!("{:<15} {:<10} {:<40}", storage_daemon_status, port, format!("Type: {:?}", storage_config.storage_engine_type));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Data Dir: {}", storage_config.data_directory.display()));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
        }
    }
    println!("--------------------------------------------------");
}
*/
/// Displays detailed status for a specific GraphDB daemon or lists all running ones.
pub async fn display_daemon_status(port_arg: Option<u16>) {
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
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
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    if ports_to_display.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", "Down", "N/A", "No running daemons found in registry.");
    } else {
        for &port in &ports_to_display {
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "main");
            let details = if let Some(meta) = metadata {
                format!("PID: {}", meta.pid)
            } else {
                "Core Graph Processing".to_string()
            };
            println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", "Running", port, details);
        }
    }

    if port_arg.is_none() {
        println!("\nTo check a specific daemon, use 'status daemon --port <port>'.");
    }
    println!("--------------------------------------------------");
}

/// Displays detailed status for the standalone Storage daemon.

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
    stop_process_by_port("GraphDB Daemon", actual_port).await?;

    let cluster_clone = cluster.clone(); // Prevent move error

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

    start_daemon(Some(actual_port), cluster, vec![DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT], "main")
        .await
        .map_err(|e| anyhow!("Failed to start daemon via daemon_api: {}", e))?;

    let pid = find_pid_by_port(actual_port).await.ok_or_else(|| {
        anyhow!("Failed to find PID for daemon on port {}. Ensure the daemon is binding to the port.", actual_port)
    })?;

    if pid == 0 {
        return Err(anyhow!("Invalid PID for daemon on port {}. Registration failed.", actual_port));
    }

    let metadata = DaemonMetadata {
        service_type: "main".to_string(),
        port: actual_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(data_dir),
        config_path: Some(config_path),
        engine_type: None,
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    let mut attempts = 0;
    let max_attempts = 3;
    while attempts < max_attempts {
        match GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await {
            Ok(_) => {
                info!("Successfully registered daemon with PID {} on port {}", pid, actual_port);
                break;
            }
            Err(e) => {
                attempts += 1;
                error!("Failed to register daemon (attempt {}/{}): {}", attempts, max_attempts, e);
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
            info!("GraphDB Daemon started on port {} (PID {})", actual_port, pid);
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


/*

/// Displays detailed status for the standalone Storage daemon.
pub async fn display_storage_daemon_status(port_arg: Option<u16>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    use crate::cli::daemon_registry::GLOBAL_DAEMON_REGISTRY;
    use std::path::PathBuf;

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let storage_config = StorageConfig::default();

    if let Some(p) = port_arg {
        // Handle specific port status check
        let storage_daemon_status = if check_process_status_by_port("Storage Daemon", p).await {
            "Running".to_string()
        } else {
            "Down".to_string()
        };
        println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, p, format!("Type: {:?}", storage_config.storage_engine_type));
        println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_directory.display()));
        println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
        println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
    } else {
        // The original logic for displaying all running daemons
        let storage_ports = find_running_storage_daemon_port().await;
        if storage_ports.is_empty() {
            println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", "Down", "N/A", "No storage daemons found in registry.");
        } else {
            for &port in &storage_ports {
                println!("----> PORT WAS {}", port);
                let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                    "Running".to_string()
                } else {
                    "Down".to_string()
                };
                println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, port, format!("Type: {:?}", storage_config.storage_engine_type));
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_directory.display()));
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
                println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
            }
        }
    }
    
    println!("--------------------------------------------------");
}

*/
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
pub async fn display_full_status_summary(rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) -> Result<()> {
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();

    // Daemon status
    let daemon_ports: Vec<u16> = all_daemons.iter()
        .filter(|d| d.service_type == "main")
        .map(|d| d.port)
        .collect();
    let daemon_status_msg = if daemon_ports.is_empty() {
        "Down".to_string()
    } else {
        format!("Running on: {}", daemon_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "))
    };
    let daemon_ports_display = if daemon_ports.is_empty() {
        "N/A".to_string()
    } else {
        daemon_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
    };
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status_msg, daemon_ports_display, "Core Graph Processing");

    // REST API status
    let rest_ports: Vec<u16> = all_daemons.iter()
        .filter(|d| d.service_type == "rest")
        .map(|d| d.port)
        .collect();
    let rest_api_status = if rest_ports.is_empty() {
        "Down".to_string()
    } else {
        "Running".to_string()
    };
    let rest_ports_display = if rest_ports.is_empty() {
        DEFAULT_REST_API_PORT.to_string()
    } else {
        rest_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
    };
    let mut rest_api_details = String::new();
    if let Some(&port) = rest_ports.first() {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .context("Failed to build reqwest client")?;
        let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
        let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
        if let Ok(resp) = client.get(&health_url).send().await {
            if resp.status().is_success() {
                rest_api_details = "Health: OK".to_string();
                if let Ok(v_resp) = client.get(&version_url).send().await {
                    if v_resp.status().is_success() {
                        let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                        let version = v_json["version"].as_str().unwrap_or("N/A");
                        rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                    }
                }
            }
        }
        let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "rest");
        if let Some(meta) = metadata {
            rest_api_details = format!("{}; PID: {}, Data Dir: {:?}", rest_api_details, meta.pid, meta.data_dir);
        }
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, rest_ports_display, rest_api_details);

    // Storage status
    let storage_ports: Vec<u16> = all_daemons.iter()
        .filter(|d| d.service_type == "storage")
        .map(|d| d.port)
        .collect();
    if storage_ports.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        let storage_config = load_storage_config(None)?;
        for &port in &storage_ports {
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running"
            } else {
                "Down"
            };
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "storage");
            let details = if let Some(meta) = metadata {
                format!("PID: {}, Type: {:?}", meta.pid, storage_config.storage_engine_type)
            } else {
                format!("Type: {:?}", storage_config.storage_engine_type)
            };
            println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, port, details);
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_directory.display()));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
            println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
        }
    }

    *storage_daemon_port_arc.lock().await = storage_ports.first().copied();
    *rest_api_port_arc.lock().await = rest_ports.first().copied();
    println!("--------------------------------------------------");
    Ok(())
}


/// Prints a visually appealing welcome screen for the CLI.
pub fn print_welcome_screen() {
    let (cols, rows) = terminal_size().unwrap_or((120, 40)); // Get actual terminal size, default to 120x40
    let total_width = cols as usize;
    let border_char = '#';

    let line_str = border_char.to_string().repeat(total_width);

    let title = "GraphDB Command Line Interface";
    let version = "Version 0.1.0 (Experimental)";
    let welcome_msg = "Welcome! Type 'help' for a list of commands.";
    let start_tip = "Tip: Use 'start all' to launch all components.";
    let status_tip = "Tip: Use 'status all' to check component health.";
    let clear_tip = "Use 'clear' or 'clean' to clear the terminal.";
    let exit_tip = "Type 'exit' or 'quit' to leave the CLI.";

    // Modified: print_centered_colored now takes an `is_bold` argument and adds more internal padding
    let print_centered_colored = |text: &str, text_color: style::Color, is_bold: bool| {
        let internal_padding_chars = 6; // 3 spaces on each side inside the borders
        let content_width = total_width.saturating_sub(2 + internal_padding_chars); // Account for 2 border chars and internal padding
        let padding_len = content_width.saturating_sub(text.len());
        let left_padding = padding_len / 2;
        let right_padding = padding_len - left_padding;

        print!("{}", style::SetForegroundColor(style::Color::Cyan));
        print!("{}", border_char);
        print!("{}", " ".repeat(internal_padding_chars / 2)); // Left internal padding

        print!("{}", style::ResetColor); // Reset color before text to apply text_color
        let styled_text = if is_bold {
            text.with(text_color).bold()
        } else {
            text.with(text_color)
        };

        print!("{}", " ".repeat(left_padding));
        print!("{}", styled_text);
        print!("{}", " ".repeat(right_padding));

        print!("{}", style::SetForegroundColor(style::Color::Cyan)); // Set color for right internal padding and border
        println!("{}{}", border_char, style::ResetColor);
    };

    // Calculate dynamic vertical padding
    let content_lines = 13; // Increased for more vertical spacing
    let available_rows = rows as usize;
    let top_bottom_padding = available_rows.saturating_sub(content_lines) / 2;

    for _ in 0..top_bottom_padding {
        println!();
    }

    println!("{}", line_str.clone().with(style::Color::Cyan));
    print_centered_colored("", style::Color::Blue, false); // Empty line for vertical spacing
    print_centered_colored(title, style::Color::DarkCyan, true); // Made title bold
    print_centered_colored(version, style::Color::White, true); // Made version bold
    print_centered_colored("", style::Color::Blue, false); // Empty line for vertical spacing
    print_centered_colored(welcome_msg, style::Color::Green, true); // Made welcome message bold
    print_centered_colored(start_tip, style::Color::Yellow, false);
    print_centered_colored(status_tip, style::Color::Yellow, false);
    print_centered_colored(clear_tip, style::Color::Yellow, false);
    print_centered_colored(exit_tip, style::Color::Red, false);
    print_centered_colored("", style::Color::Blue, false); // Empty line for vertical spacing
    println!("{}", line_str.with(style::Color::Cyan));
    
    for _ in 0..top_bottom_padding {
        println!();
    }
}

/// Clears the terminal screen.
pub async fn clear_terminal_screen() -> Result<()> {
    execute!(io::stdout(), Clear(ClearType::All), MoveTo(0, 0))
        .context("Failed to clear terminal screen or move cursor")?;
    io::stdout().flush()?; // Ensure the changes are immediately visible
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

/// Handles `stop` subcommand for direct CLI execution.
pub async fn handle_stop_command(args: StopArgs) -> Result<()> {
    info!("Processing stop command with args: {:?}", args);
    match args.action.unwrap_or(StopAction::All) {
        StopAction::All => {
            println!("Stopping all GraphDB components...");
            info!("Initiating stop for all components");

            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            if all_daemons.is_empty() {
                println!("No running components found in registry.");
                info!("No components registered in GLOBAL_DAEMON_REGISTRY");
                return Ok(());
            }

            let mut stopped_count = 0;
            let mut errors = Vec::new();

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

pub async fn execute_graph_query(query_string: String, persist: Option<bool>) {
    println!("Executing graph query: '{}' (persist: {:?})", query_string, persist);
    match rest::api::execute_graph_query(&query_string, persist).await {
        Ok(result) => println!("Graph query result: {}", result),
        Err(e) => eprintln!("Failed to execute graph query: {}", e),
    }
}

pub async fn execute_storage_query() {
    println!("Executing storage query...");
    println!("Storage query executed (placeholder).");
}
/*

/// Starts a daemon instance and manages its lifecycle in the interactive CLI.
pub async fn start_daemon_instance_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

    if check_process_status_by_port("GraphDB Daemon", actual_port).await {
        println!("Daemon on port {} is already running.", actual_port);
        return Ok(());
    }

    println!("Attempting to start daemon on port {}...", actual_port);

    // Call start_daemon which returns the PID (u32)
    let pid = start_daemon(port, cluster, vec![]).await?;
    
    println!("GraphDB Daemon launched with PID {} on port {}. It should be running in the background.", pid, actual_port);
    
    // Register the daemon in the registry
    let metadata = DaemonMetadata {
        service_type: "main".to_string(),
        port: actual_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: None,
        config_path: None,
        engine_type: None,
        last_seen_nanos: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };
    GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await
        .context("Failed to register daemon in registry")?;
        
    let mut handles = daemon_handles.lock().await;
    let (tx_shutdown, rx_shutdown) = oneshot::channel();
    let handle = tokio::spawn(async {
        let _ = rx_shutdown.await;
        // Logic to gracefully shutdown the daemon process can go here
        // For example, sending a signal or calling a shutdown API.
    });
    handles.insert(actual_port, (handle, tx_shutdown));

    Ok(())
}
*/


/// Stops the main daemon on the specified port.
pub async fn stop_main_interactive(
    port: Option<u16>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
) -> Result<(), anyhow::Error> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
    log::info!("Attempting to stop main daemon on port {}", actual_port);

    if !check_process_status_by_port("Main Daemon", actual_port).await {
        log::info!("No main daemon running on port {}", actual_port);
        return Ok(());
    }

    stop_process_by_port("Main Daemon", actual_port)
        .await
        .map_err(|e| {
            log::error!("Failed to stop main daemon process on port {}: {}", actual_port, e);
            anyhow::anyhow!("Failed to stop main daemon on port {}: {}", actual_port, e)
        })?;

    let pid = find_pid_by_port(actual_port)
        .await
        .ok_or_else(|| {
            log::error!("Failed to find PID for main daemon on port {}", actual_port);
            anyhow::anyhow!("Failed to find PID for main daemon on port {}", actual_port)
        })?;

    if pid == 0 {
        log::warn!("Invalid PID (0) for main daemon on port {}", actual_port);
        return Ok(());
    }

    GLOBAL_DAEMON_REGISTRY
        .remove_daemon_by_type("main", actual_port)
        .await
        .map_err(|e| {
            log::error!("Failed to remove main daemon (port: {}, PID: {}) from registry: {}", actual_port, pid, e);
            anyhow::anyhow!("Failed to remove main daemon from registry: {}", e)
        })?;

    let mut shutdown_tx_guard = shutdown_tx.lock().await;
    if let Some(tx) = shutdown_tx_guard.take() {
        if tx.send(()).is_ok() {
            log::info!("Sent shutdown signal to main daemon on port {}", actual_port);
        } else {
            log::warn!("Failed to send shutdown signal to main daemon on port {}", actual_port);
        }
    }

    log::info!("Main daemon on port {} (PID: {}) stopped successfully", actual_port, pid);
    Ok(())
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
        GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default()
            .iter()
            .filter(|d| d.service_type == "rest")
            .map(|d| d.port)
            .collect()
    };

    for actual_port in ports_to_stop {
        let is_managed_port = rest_api_port_arc.lock().await.map_or(false, |p| p == actual_port);
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
            continue;
        }

        stop_process_by_port("REST API", actual_port)
            .await
            .map_err(|e| {
                log::error!("Failed to stop REST API process on port {}: {}", actual_port, e);
                anyhow::anyhow!("Failed to stop REST API on port {}: {}", actual_port, e)
            })?;

        let pid = find_pid_by_port(actual_port)
            .await
            .ok_or_else(|| {
                log::error!("Failed to find PID for REST API on port {}", actual_port);
                anyhow::anyhow!("Failed to find PID for REST API on port {}", actual_port)
            })?;

        if pid == 0 {
            log::warn!("Invalid PID (0) for REST API on port {}", actual_port);
            continue;
        }

        GLOBAL_DAEMON_REGISTRY
            .remove_daemon_by_type("rest", actual_port)
            .await
            .map_err(|e| {
                log::error!("Failed to remove REST API (port: {}, PID: {}) from registry: {}", actual_port, pid, e);
                anyhow::anyhow!("Failed to remove REST API from registry: {}", e)
            })?;

        log::info!("REST API on port {} (PID: {}) stopped successfully", actual_port, pid);
        println!("REST API server on port {} stopped.", actual_port);
    }

    Ok(())
}

/// Starts the Storage Daemon in interactive mode.
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    _cluster_opt: Option<String>, // Ignored, as cluster_range validation is removed
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    // Load CLI config
    let cli_config = CliConfig::load().map_err(|e| anyhow!("Failed to load CLI config: {}", e))?;

    // Determine the config file path
    let config_path = match &cli_config.command {
        Commands::Start { action: Some(StartAction::All { storage_config, .. }), .. } => {
            storage_config.clone()
                .or(config_file)
                .unwrap_or_else(|| default_config_root_directory().join("storage_config.yaml"))
        }
        Commands::Start { action: Some(StartAction::Storage { config_file: action_config_file, .. }), .. } => {
            action_config_file.clone()
                .or(config_file)
                .unwrap_or_else(|| default_config_root_directory().join("storage_config.yaml"))
        }
        Commands::Storage(StorageAction::Start { config_file: action_config_file, .. }) => {
            action_config_file.clone()
                .or(config_file)
                .unwrap_or_else(|| default_config_root_directory().join("storage_config.yaml"))
        }
        _ => config_file.unwrap_or_else(|| default_config_root_directory().join("storage_config.yaml")),
    };
    info!("=======> I WILL BE TRYING TO LOAD IT FROM {:?}", config_path);

    // Load storage configuration
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
        .map_err(|e| {
            error!("Failed to load storage config from {:?}: {}. Using default.", config_path, e);
            if config_path.exists() {
                if let Ok(content) = std::fs::read_to_string(&config_path) {
                    error!("Config file content: {}", content);
                } else {
                    error!("Config file exists but cannot be read at {:?}", config_path);
                }
            } else {
                error!("Config file does not exist at {:?}", config_path);
            }
            e
        })?;
    info!("Loaded Storage Config: {:?}", storage_config);

    // Select port: CLI --storage-port > CLI --port > YAML default_port > DEFAULT_STORAGE_PORT
    let selected_port = match &cli_config.command {
        Commands::Start { action: Some(StartAction::All { storage_port, port: cmd_port, .. }), .. } => {
            if port.is_some() {
                warn!("Ignoring port parameter ({:?}) as it should be set via CLI arguments", port);
            }
            storage_port.or(*cmd_port).unwrap_or(storage_config.default_port)
        }
        Commands::Start { action: Some(StartAction::Storage { storage_port, port: cmd_port, .. }), .. } => {
            if port.is_some() {
                warn!("Ignoring port parameter ({:?}) as it should be set via CLI arguments", port);
            }
            storage_port.or(*cmd_port).unwrap_or(storage_config.default_port)
        }
        Commands::Storage(StorageAction::Start { storage_port, port: cmd_port, .. }) => {
            if port.is_some() {
                warn!("Ignoring port parameter ({:?}) as it should be set via CLI arguments", port);
            }
            storage_port.or(*cmd_port).unwrap_or(storage_config.default_port)
        }
        _ => {
            port.unwrap_or(storage_config.default_port)
        }
    };
    info!("===> SELECTED PORT {}", selected_port);
    println!("===> SELECTED PORT {}", selected_port);

    let ip = "127.0.0.1";
    let ip_addr: std::net::IpAddr = ip.parse().with_context(|| format!("Invalid IP address: {}", ip))?;
    let addr = SocketAddr::new(ip_addr, selected_port);
    info!("Starting storage daemon on {}", addr);

    // Check if the port is used by a registered GraphDB component
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let is_graphdb_process = all_daemons.iter().any(|d| d.port == selected_port && (d.service_type == "main" || d.service_type == "rest" || d.service_type == "storage"));

    if check_process_status_by_port("Storage Daemon", selected_port).await {
        info!("Storage Daemon already running on port {}. Allowing multiple instances.", selected_port);
        // Skip stopping to allow multiple Storage Daemons
    } else if !is_port_free(selected_port).await && !is_graphdb_process {
        info!("Port {} is in use by a non-GraphDB process. Attempting to stop.", selected_port);
        stop_process_by_port("Storage Daemon", selected_port)
            .await
            .map_err(|e| anyhow!("Failed to stop existing process on port {}: {}", selected_port, e))?;
    } else if is_graphdb_process {
        info!("Port {} is in use by a GraphDB component (main, rest, or storage). Allowing multiple instances.", selected_port);
    } else {
        info!("No process found running on port {}", selected_port);
    }

    // Verify port is free only if not allowing multiple instances
    if !is_port_free(selected_port).await && !is_graphdb_process {
        warn!("Port {} is still in use by a non-GraphDB process after attempting to stop. Proceeding to start additional Storage Daemon.", selected_port);
    }

    // Use config values directly
    let data_dir = storage_config.data_directory;
    let engine_type = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);

    // Start the daemon with explicit port logging
    debug!("Calling start_daemon with port: {:?}", Some(selected_port));
    start_daemon(Some(selected_port), None, vec![], "storage")
        .await
        .map_err(|e| {
            error!("Failed to start storage daemon on port {}: {}", selected_port, e);
            anyhow!("Failed to start storage daemon on port {}: {}", selected_port, e)
        })?;

    // Find PID with retry
    let mut attempts = 0;
    let max_attempts = 5;
    let pid = loop {
        match find_pid_by_port(selected_port).await {
            Some(pid) if pid != 0 => break pid,
            _ => {
                attempts += 1;
                if attempts >= max_attempts {
                    error!("Failed to find valid PID for storage daemon on port {} after {} attempts", selected_port, max_attempts);
                    return Err(anyhow!("Failed to find PID for storage daemon on port {}", selected_port));
                }
                debug!("No valid PID found for port {} on attempt {}", selected_port, attempts);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };

    // Register daemon with retry
    let metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port: selected_port,
        pid,
        ip_address: ip.to_string(),
        data_dir: Some(data_dir.clone()),
        config_path: Some(config_path),
        engine_type: Some(engine_type.clone()),
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    let mut reg_attempts = 0;
    let max_reg_attempts = 3;
    while reg_attempts < max_reg_attempts {
        match GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await {
            Ok(_) => {
                info!("Successfully registered storage daemon with PID {} on port {}", pid, selected_port);
                println!("==> Successfully registered storage daemon with PID {} on port {}", pid, selected_port);
                break;
            }
            Err(e) => {
                reg_attempts += 1;
                error!("Failed to register storage daemon on port {} (attempt {}/{}): {}", selected_port, reg_attempts, max_reg_attempts, e);
                if reg_attempts >= max_reg_attempts {
                    return Err(e).context(format!("Failed to register storage daemon on port {} after multiple attempts", selected_port));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    // Health check
    let addr_check = format!("{}:{}", ip, selected_port);
    let health_check_timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(500);
    let start_time = tokio::time::Instant::now();

    while start_time.elapsed() < health_check_timeout {
        if tokio::net::TcpStream::connect(&addr_check).await.is_ok() {
            info!("Storage daemon started on port {} (PID {}) with engine type {:?}", selected_port, pid, engine_type);
            *storage_daemon_port_arc.lock().await = Some(selected_port);
            let (tx, rx) = oneshot::channel();
            *storage_daemon_shutdown_tx_opt.lock().await = Some(tx);
            let handle = tokio::spawn(async move {
                rx.await.ok();
                info!("Storage daemon on port {} shutting down", selected_port);
            });
            *storage_daemon_handle.lock().await = Some(handle);
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    error!("Storage daemon on port {} failed to become reachable within {} seconds", selected_port, health_check_timeout.as_secs());
    Err(anyhow!("Storage daemon failed to start on port {}", selected_port))
}

pub async fn start_rest_api_interactive(
    port: Option<u16>,
    _cluster: Option<String>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
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

    if check_process_status_by_port("REST API", actual_port).await {
        info!("REST API already running on port {}", actual_port);
        return Ok(());
    }

    info!("Starting REST API on port {}", actual_port);
    stop_process_by_port("REST API", actual_port).await?;

    if !is_port_free(actual_port).await {
        return Err(anyhow!("Port {} is already in use.", actual_port));
    }

    let config = config.unwrap_or_else(|| {
        warn!("Using default REST API configuration");
        RestApiConfig {
            data_directory: format!("{}/rest_api_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            log_directory: "/var/log/graphdb".to_string(),
            default_port: actual_port,
            cluster_range: format!("{}", actual_port),
        }
    });

    let data_dir = PathBuf::from(&config.data_directory);

    start_daemon(Some(actual_port), None, vec![DEFAULT_DAEMON_PORT, DEFAULT_STORAGE_PORT], "rest")
        .await
        .map_err(|e| anyhow!("Failed to start REST API via daemon_api: {}", e))?;

    let pid = find_pid_by_port(actual_port).await.ok_or_else(|| {
        anyhow!("Failed to find PID for REST API on port {}. Ensure the process is binding to the port.", actual_port)
    })?;

    if pid == 0 {
        return Err(anyhow!("Invalid PID for REST API on port {}. Registration failed.", actual_port));
    }

    let metadata = DaemonMetadata {
        service_type: "rest".to_string(),
        port: actual_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(data_dir),
        config_path: Some(config_path),
        engine_type: None,
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    let mut attempts = 0;
    let max_attempts = 3;
    while attempts < max_attempts {
        match GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await {
            Ok(_) => {
                info!("Successfully registered REST API with PID {} on port {}", pid, actual_port);
                break;
            }
            Err(e) => {
                attempts += 1;
                error!("Failed to register REST API (attempt {}/{}): {}", attempts, max_attempts, e);
                if attempts >= max_attempts {
                    return Err(e).context("Failed to register REST API after multiple attempts");
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
            info!("REST API started on port {} (PID {})", actual_port, pid);
            *rest_api_port_arc.lock().await = Some(actual_port);
            let (tx, rx) = oneshot::channel();
            *rest_api_shutdown_tx_opt.lock().await = Some(tx);
            let handle = tokio::spawn(async move {
                rx.await.ok();
                info!("REST API on port {} shutting down", actual_port);
            });
            *rest_api_handle.lock().await = Some(handle);
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    error!("REST API on port {} failed to become reachable", actual_port);
    Err(anyhow!("REST API failed to start on port {}", actual_port))
}

pub async fn stop_storage_interactive(
    port: Option<u16>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        let config_path = default_config_root_directory().join("storage_config.yaml");
        let storage_config = load_storage_config_from_yaml(Some(config_path))
            .unwrap_or_else(|_| StorageConfig::default());
        let default_port = if storage_config.default_port != 0 {
            storage_config.default_port
        } else {
            DEFAULT_STORAGE_PORT
        };
        GLOBAL_DAEMON_REGISTRY
            .get_all_daemon_metadata()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|d| d.service_type == "storage")
            .map(|d| d.port)
            .chain(std::iter::once(default_port))
            .collect()
    };

    let mut errors = Vec::new();
    for actual_port in ports_to_stop {
        let is_managed_port = storage_daemon_port_arc.lock().await.as_ref() == Some(&actual_port);
        if is_managed_port {
            let mut handle_lock = storage_daemon_handle.lock().await;
            let mut shutdown_tx_lock = storage_daemon_shutdown_tx_opt.lock().await;
            if let Some(tx) = shutdown_tx_lock.take() {
                if tx.send(()).is_ok() {
                    info!("Sent shutdown signal to storage daemon on port {}", actual_port);
                } else {
                    warn!("Failed to send shutdown signal to storage daemon on port {}", actual_port);
                }
            }
            if let Some(handle) = handle_lock.take() {
                if let Err(e) = handle.await {
                    warn!("Failed to join storage daemon handle on port {}: {}", actual_port, e);
                }
                *storage_daemon_port_arc.lock().await = None;
            }
        }

        if !check_process_status_by_port("Storage Daemon", actual_port).await {
            info!("No storage daemon running on port {}", actual_port);
            println!("No storage daemon running on port {}", actual_port);
            if let Err(e) = GLOBAL_DAEMON_REGISTRY
                .remove_daemon_by_type("storage", actual_port)
                .await
            {
                errors.push(anyhow!("Failed to clean up storage daemon (port: {}) from registry: {}", actual_port, e));
            }
            continue;
        }

        if let Err(e) = stop_process_by_port("Storage Daemon", actual_port).await {
            errors.push(anyhow!("Failed to stop storage daemon process on port {}: {}", actual_port, e));
            continue;
        }

        // Wait briefly to ensure process termination
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Verify port is free
        if is_port_free(actual_port).await {
            info!("Storage daemon on port {} stopped successfully", actual_port);
            println!("Storage daemon on port {} stopped.", actual_port);
            if let Err(e) = GLOBAL_DAEMON_REGISTRY
                .remove_daemon_by_type("storage", actual_port)
                .await
            {
                errors.push(anyhow!("Failed to remove storage daemon (port: {}) from registry: {}", actual_port, e));
            }
        } else {
            errors.push(anyhow!("Port {} is still in use after attempting to stop storage daemon", actual_port));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to stop storage daemon: {:?}", errors))
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

/// Stops a standalone storage daemon. This is the non-interactive version.
pub async fn stop_storage(
    port: Option<u16>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);

    println!("Attempting to stop Storage daemon on port {}...", actual_port);
    stop_process_by_port("Storage Daemon", actual_port).await?;
    println!("Standalone Storage daemon on port {} stopped.", actual_port);
    Ok(())
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

    // Stop main daemons
    log::info!("Stopping all main daemon instances...");
    match stop_main_interactive(
        None,
        Arc::new(TokioMutex::new(None)),
    ).await {
        Ok(()) => {
            log::info!("Main daemon instances stopped successfully");
            stopped_count += 1;
        }
        Err(e) => {
            println!("Failed to stop main daemon instances: {}", e);
            log::error!("Failed to stop main daemon instances: {}", e);
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

    // Stop external daemon processes
    log::info!("Sending global stop signal to all external daemon processes...");
    match tokio::time::timeout(Duration::from_secs(10), stop_daemon_api_call()).await {
        Ok(Ok(_)) => {
            log::info!("Global stop signal sent successfully to all external daemon processes");
            stopped_count += 1;
        }
        Ok(Err(e)) => {
            println!("Failed to send global stop signal to daemons: {}", e);
            log::error!("Failed to send global stop signal to daemons: {}", e);
            failed_count += 1;
        }
        Err(_) => {
            println!("Global stop signal timed out after 10 seconds");
            log::error!("Global stop signal timed out after 10 seconds");
            failed_count += 1;
        }
    }

    if stopped_count == 0 && failed_count == 0 {
        println!("No running components were found to stop.");
        log::info!("No components were running to stop");
    } else {
        println!("Stop all completed: {} component groups stopped, {} failed.", stopped_count, failed_count);
        log::info!("Stop all completed: {} component groups stopped, {} failed", stopped_count, failed_count);
    }
    Ok(())
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

/// Handles `storage` subcommand for direct CLI execution.
pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file, cluster, storage_port, storage_cluster } => {
            // Only check for conflicts in `start storage` context (not `start all`)
            let is_start_storage = storage_port.is_some() || storage_cluster.is_some() || (port.is_some() && storage_port.is_none() && cluster.is_some() && storage_cluster.is_none());
            if is_start_storage {
                if port.is_some() && storage_port.is_some() {
                    log::error!("Cannot specify both --port and --storage-port in `start storage`");
                    return Err(anyhow!("Cannot specify both --port and --storage-port"));
                }
                if cluster.is_some() && storage_cluster.is_some() {
                    log::error!("Cannot specify both --cluster and --storage-cluster in `start storage`");
                    return Err(anyhow!("Cannot specify both --cluster and --storage-cluster"));
                }
            }
            let actual_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let actual_cluster = storage_cluster.or(cluster);
            let actual_config_file = config_file.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
            log::info!("Starting storage daemon with port: {:?}, cluster: {:?}, config: {:?}", actual_port, actual_cluster, actual_config_file);
            if let Some(cluster) = actual_cluster {
                let ports = parse_cluster_range(&cluster)?;
                log::info!("Parsed cluster range for storage daemon: {:?}", ports);
                for p in ports {
                    start_storage_interactive(
                        Some(p),
                        Some(actual_config_file.clone()),
                        None,
                        Arc::new(TokioMutex::new(None)),
                        Arc::new(TokioMutex::new(None)),
                        Arc::new(TokioMutex::new(None)),
                    ).await?;
                }
            } else {
                start_storage_interactive(
                    Some(actual_port),
                    Some(actual_config_file),
                    None,
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                ).await?;
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(
                port,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await
        }
        StorageAction::Status { port, cluster } => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_storage_daemon_status(Some(p), Arc::new(TokioMutex::new(None))).await;
                }
            } else {
                display_storage_daemon_status(port, Arc::new(TokioMutex::new(None))).await;
            }
            Ok(())
        }
        StorageAction::StorageQuery => {
            println!("Performing Storage Query (simulated, non-interactive mode)...");
            Ok(())
        }
        StorageAction::Health => {
            println!("Performing Storage Health Check (simulated, non-interactive mode)...");
            Ok(())
        }
        StorageAction::Version => {
            println!("Retrieving Storage Version (simulated, non-interactive mode)...");
            Ok(())
        }
        StorageAction::List => {
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();
            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}, Engine: {:?}", 
                             daemon.port, daemon.pid, daemon.data_dir, daemon.engine_type);
                }
            }
            Ok(())
        }
    }
}

/// Handles `StorageAction` variants in interactive mode.
pub async fn handle_storage_command_interactive(
    action: StorageAction,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match action {
        StorageAction::Start { port, config_file, cluster, storage_port, storage_cluster } => {
            // Only check for conflicts in `start storage` context (not `start all`)
            let is_start_storage = storage_port.is_some() || storage_cluster.is_some() || (port.is_some() && storage_port.is_none() && cluster.is_some() && storage_cluster.is_none());
            if is_start_storage {
                if port.is_some() && storage_port.is_some() {
                    log::error!("Cannot specify both --port and --storage-port in `start storage`");
                    return Err(anyhow!("Cannot specify both --port and --storage-port"));
                }
                if cluster.is_some() && storage_cluster.is_some() {
                    log::error!("Cannot specify both --cluster and --storage-cluster in `start storage`");
                    return Err(anyhow!("Cannot specify both --cluster and --storage-cluster"));
                }
            }
            let actual_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let actual_cluster = storage_cluster.or(cluster);
            log::info!("Starting storage daemon interactively with port: {:?}, cluster: {:?}", actual_port, actual_cluster);
            start_storage_interactive(
                Some(actual_port),
                config_file,
                actual_cluster,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port, .. } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::StorageQuery => {
            execute_storage_query().await;
            Ok(())
        }
        StorageAction::Health => {
            println!("Performing Storage Health Check (simulated, interactive mode)...");
            Ok(())
        }
        StorageAction::Version => {
            println!("Retrieving Storage Version (simulated, interactive mode)...");
            Ok(())
        }
        StorageAction::List => {
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();
            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}, Engine: {:?}", 
                             daemon.port, daemon.pid, daemon.data_dir, daemon.engine_type);
                }
            }
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

/// Handles the interactive 'reload storage' command.
pub async fn reload_storage_interactive(
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    println!("Reloading standalone Storage daemon...");
    
    // Pass the actual port if it's currently known, otherwise None to let it default
    let current_storage_port = storage_daemon_port_arc.lock().await.unwrap_or(DEFAULT_STORAGE_PORT);

    stop_storage_interactive(
        Some(current_storage_port), // Pass Some(port) for targeted stop
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone()
    ).await?;

    start_storage_interactive(
        Some(current_storage_port), // Pass Some(port) for targeted start
        None, // config_file, assuming default if not specified
        None, // cluster_opt
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc
    ).await?;
    
    println!("Standalone Storage daemon reloaded.");
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
        if let Some((handle, shutdown_tx)) = handles.remove(&actual_port) {
            shutdown_tx.send(()).ok();
            handle.await?;
        }
        stop_process_by_port("GraphDB Daemon", actual_port).await?;
        println!("GraphDB Daemon on port {} stopped and unregistered.", actual_port);
    }

    Ok(())
}          

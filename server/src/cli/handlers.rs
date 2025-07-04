// server/src/cli/handlers.rs
// Updated: 2025-07-04 - Corrected IP address for REST API calls.

use anyhow::{Context, Result};
use std::time::Duration;
use std::path::PathBuf;
use std::time::Instant;
use tokio::sync::{oneshot, Mutex};
use std::collections::HashMap;
use std::sync::Arc;

// Import necessary items from sibling modules
use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, StopAction, StatusAction};
use crate::cli::config::{get_default_rest_port_from_config, load_storage_config, StorageConfig, StorageEngineType, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS};
use crate::cli::daemon_management::{find_running_storage_daemon_port, start_daemon_process, stop_daemon_api_call};
use crate::cli::config::CliConfig;

// External crates
use reqwest;
use serde_json;
use std::str::FromStr;
use daemon_api::start_daemon;

/// Helper to find and kill a process by port. This is used for all daemon processes.
pub fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> {
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    let output = std::process::Command::new("lsof") // Use std::process::Command for lsof/kill
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t") // Only print PIDs
        .output()
        .context(format!("Failed to run lsof to find {} process on port {}", process_name, port))?;

    let pids = String::from_utf8_lossy(&output.stdout);
    let pids: Vec<i32> = pids.trim().lines().filter_map(|s| s.parse::<i32>().ok()).collect();

    if pids.is_empty() {
        println!("No {} process found running on port {}.", process_name, port);
        return Ok(());
    }

    for pid in pids {
        println!("Killing process {} (for {} on port {})...", pid, process_name, port);
        match std::process::Command::new("kill").arg("-9").arg(pid.to_string()).status() {
            Ok(status) if status.success() => println!("Process {} killed successfully.", pid),
            Ok(_) => eprintln!("Failed to kill process {}.", pid),
            Err(e) => eprintln!("Error killing process {}: {}", pid, e),
        }
    }
    Ok(())
}

/// Helper to check if a process is running on a given port. This is used for all daemon processes.
pub fn check_process_status_by_port(_process_name: &str, port: u16) -> bool {
    let output = std::process::Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
        .output();

    if let Ok(output) = output {
        let pids = String::from_utf8_lossy(&output.stdout);
        if !pids.trim().is_empty() {
            return true;
        }
    }
    false
}

/// Displays detailed status for the REST API server.
pub async fn display_rest_api_status() {
    let rest_port = get_default_rest_port_from_config();

    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let rest_health_url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
    let rest_version_url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();

    match client.get(&rest_health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            rest_api_status = "Running".to_string();
            let version_info = client.get(&rest_version_url).send().await;
            match version_info {
                Ok(v_resp) if v_resp.status().is_success() => {
                    let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    rest_api_details = format!("Version: {}", version);
                },
                _ => rest_api_details = "Version: N/A".to_string(),
            }
        },
        _ => { /* Status remains "Down" */ },
    }
    println!("{:<15} {:<10} {:<40}", rest_api_status, rest_port, rest_api_details);
    println!("--------------------------------------------------");
}

/// Displays detailed status for a specific GraphDB daemon or lists common ones.
pub async fn display_daemon_status(port_arg: Option<u16>) {
    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    if let Some(port) = port_arg {
        let status_message = if check_process_status_by_port("GraphDB Daemon", port) {
            "Running".to_string()
        } else {
            "Down".to_string()
        };
        println!("{:<15} {:<10} {:<40}", status_message, port, "Core Graph Processing");
    } else {
        let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
        let mut found_any = false;
        for &port in &common_daemon_ports {
            if check_process_status_by_port("GraphDB Daemon", port) {
                println!("{:<15} {:<10} {:<40}", "Running", port, "Core Graph Processing");
                found_any = true;
            }
        }
        if !found_any {
            println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No daemons found on common ports.");
        }
        println!("\nTo check a specific daemon, use 'status daemon --port <port>'.");
    }
    println!("--------------------------------------------------");
}

/// Displays detailed status for the standalone Storage daemon.
pub async fn display_storage_daemon_status(port_arg: Option<u16>) {
    let port_to_check = if let Some(p) = port_arg {
        p
    } else {
        find_running_storage_daemon_port().await.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
    };

    let storage_config = load_storage_config(None)
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config for status check: {}. Using defaults.", e);
            StorageConfig {
                data_directory: "/tmp/graphdb_data".to_string(),
                log_directory: "/var/log/graphdb".to_string(),
                default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                cluster_range: "9000-9002".to_string(),
                max_disk_space_gb: 1000,
                min_disk_space_gb: 10,
                use_raft_for_scale: true,
                storage_engine_type: "sled".to_string(),
            }
        });

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let storage_engine_type_str = storage_config.storage_engine_type;
    let status_message = if check_process_status_by_port("Storage Daemon", port_to_check) {
        "Running".to_string()
    } else {
        "Down".to_string()
    };
    println!("{:<15} {:<10} {:<40}", status_message, port_to_check, format!("Type: {}", storage_engine_type_str));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Data Dir: {}", storage_config.data_directory));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Log Dir: {}", storage_config.log_directory));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Cluster Range: {}", storage_config.cluster_range));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Max Disk: {} GB", storage_config.max_disk_space_gb));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Min Disk: {} GB", storage_config.min_disk_space_gb));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Raft Enabled: {}", storage_config.use_raft_for_scale));
    println!("--------------------------------------------------");
}

/// Displays a comprehensive status summary of all GraphDB components.
pub async fn display_full_status_summary() {
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    // --- 1. GraphDB Daemon Status ---
    let mut daemon_status_msg = "Not launched".to_string();
    let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
    let mut running_daemon_ports = Vec::new();

    for &port in &common_daemon_ports {
        let output = std::process::Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output();
        if let Ok(output) = output {
            if !output.stdout.is_empty() {
                running_daemon_ports.push(port.to_string());
            }
        }
    }
    if !running_daemon_ports.is_empty() {
        daemon_status_msg = format!("Running on: {}", running_daemon_ports.join(", "));
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status_msg, "N/A", "Core Graph Processing");

    // --- 2. REST API Status ---
    let rest_port = get_default_rest_port_from_config();
    let rest_health_url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
    let rest_version_url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();

    match client.get(&rest_health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            rest_api_status = "Running".to_string();
            let version_info = client.get(&rest_version_url).send().await;
            match version_info {
                Ok(v_resp) if v_resp.status().is_success() => {
                    let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    rest_api_details = format!("Version: {}", version);
                },
                _ => rest_api_details = "Version: N/A".to_string(),
            }
        },
        _ => { /* Status remains "Down" */ },
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, rest_port, rest_api_details);

    // --- 3. Storage Daemon Status ---
    let storage_config = load_storage_config(None)
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config for status check: {}. Using defaults.", e);
            StorageConfig {
                data_directory: "/tmp/graphdb_data".to_string(),
                log_directory: "/var/log/graphdb".to_string(),
                default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                cluster_range: "9000-9002".to_string(),
                max_disk_space_gb: 1000,
                min_disk_space_gb: 10,
                use_raft_for_scale: true,
                storage_engine_type: "sled".to_string(),
            }
        });

    let mut storage_daemon_status = "Down".to_string();
    let mut actual_storage_port_reported = storage_config.default_port;

    if let Some(found_port) = find_running_storage_daemon_port().await {
        storage_daemon_status = "Running".to_string();
        actual_storage_port_reported = found_port;
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, actual_storage_port_reported, format!("Type: {}", storage_config.storage_engine_type));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_directory));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Log Dir: {}", storage_config.log_directory));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Range: {}", storage_config.cluster_range));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Disk: {} GB", storage_config.max_disk_space_gb));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Min Disk: {} GB", storage_config.min_disk_space_gb));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Raft Enabled: {}", storage_config.use_raft_for_scale));
    println!("--------------------------------------------------");
}

/// Prints a visually appealing welcome screen for the CLI.
pub fn print_welcome_screen() {
    println!("\n{}", "#".repeat(70));
    println!("{} {:^66} {}", "#", "GraphDB Command Line Interface", "#");
    println!("{} {:^66} {}", "#", "Version 0.1.0 (Experimental)", "#");
    println!("{} {:^66} {}", "#", "", "#");
    println!("{} {:^66} {}", "#", "Welcome! Type 'help' for a list of commands.", "#");
    println!("{}", "#".repeat(70));
    println!("");
}

// --- Command Handlers for direct CLI execution (non-interactive) ---

/// Handles the top-level `start` command.
pub async fn handle_start_command(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    config: &CliConfig, // Pass CliConfig reference
) -> Result<()> {
    let mut daemon_status_msg = "Not launched".to_string();
    let mut rest_api_status_msg = "Not launched".to_string();
    let mut storage_status_msg = "Not launched".to_string();

    let explicit_rest_api_port = listen_port;
    let explicit_storage_port = storage_port;

    let mut skip_ports = Vec::new();
    if let Some(rest_p) = explicit_rest_api_port {
        skip_ports.push(rest_p);
    }
    if let Some(storage_p) = explicit_storage_port {
        skip_ports.push(storage_p);
    }

    if port.is_some() || cluster.is_some() {
        let daemon_result = start_daemon(port, cluster.clone(), skip_ports.clone()).await;
        match daemon_result {
            Ok(()) => {
                if let Some(cluster_range) = cluster {
                    daemon_status_msg = format!("Running on cluster ports: {}", cluster_range);
                } else if let Some(p) = port {
                    daemon_status_msg = format!("Running on port: {}", p);
                } else {
                    daemon_status_msg = format!("Running on default port: {}", config.server.port.unwrap_or(8080));
                }
            }
            Err(e) => {
                eprintln!("Failed to start daemon(s): {:?}", e);
                daemon_status_msg = format!("Failed to start ({:?})", e);
            }
        }
    } else {
        daemon_status_msg = "Not requested".to_string();
    }

    if let Some(rest_port) = explicit_rest_api_port {
        if rest_port < 1024 || rest_port > 65535 {
            eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port);
            rest_api_status_msg = format!("Invalid port: {}", rest_port);
        } else {
            stop_process_by_port("REST API", rest_port)?;
            let addr = format!("127.0.0.1:{}", rest_port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;

            while start_time.elapsed() < wait_timeout {
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(_) => {
                        port_freed = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if !port_freed {
                eprintln!("Failed to free up port {} after killing processes. Try again.", rest_port);
                rest_api_status_msg = format!("Failed to free up port {}.", rest_port);
            } else {
                println!("Starting REST API server on port {}...", rest_port);
                let current_storage_config = load_storage_config(storage_config_file.clone())
                    .unwrap_or_else(|e| {
                        eprintln!("Warning: Could not load storage config for REST API daemonization: {}. Using defaults.", e);
                        StorageConfig {
                            data_directory: "/tmp/graphdb_data".to_string(),
                            log_directory: "/var/log/graphdb".to_string(),
                            default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                            cluster_range: "9000-9002".to_string(),
                            max_disk_space_gb: 1000,
                            min_disk_space_gb: 10,
                            use_raft_for_scale: true,
                            storage_engine_type: "sled".to_string(),
                        }
                    });

                let storage_engine_type = StorageEngineType::from_str(&current_storage_config.storage_engine_type)
                    .unwrap_or(StorageEngineType::Sled);

                start_daemon_process(
                    true, false, Some(rest_port), Some(current_storage_config.data_directory.clone().into()), Some(storage_engine_type)
                ).await?;
                rest_api_status_msg = format!("Running on port: {}", rest_port);
            }
        }
    } else {
        rest_api_status_msg = "Not requested".to_string();
    }

    if let Some(s_port) = explicit_storage_port {
        if s_port < 1024 || s_port > 65535 {
            eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", s_port);
            storage_status_msg = format!("Invalid port: {}", s_port);
        } else {
            stop_process_by_port("Storage Daemon", s_port)?;
            let addr = format!("127.0.0.1:{}", s_port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;

            while start_time.elapsed() < wait_timeout {
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(_) => {
                        port_freed = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if !port_freed {
                eprintln!("Failed to free up storage port {} after killing processes. Try again.", s_port);
                storage_status_msg = format!("Failed to free up port {}.", s_port);
            } else {
                println!("Starting Storage daemon on port {}...", s_port);
                let loaded_storage_config = load_storage_config(storage_config_file.clone());
                match loaded_storage_config {
                    Ok(cfg) => {
                        println!("  Using config file: {}", storage_config_file.as_ref().map_or("default".to_string(), |p| p.display().to_string()));
                        println!("  Storage Metrics:");
                        println!("    Data Directory: {}", cfg.data_directory);
                        println!("    Log Directory: {}", cfg.log_directory);
                        println!("    Default Port (from config): {}", cfg.default_port);
                        println!("    Cluster Range (from config): {}", cfg.cluster_range);
                        println!("    Max Disk Space: {} GB", cfg.max_disk_space_gb);
                        println!("    Min Disk Space: {} GB", cfg.min_disk_space_gb);
                        println!("    Use Raft for Scale: {}", cfg.use_raft_for_scale);
                        println!("    Storage Engine Type: {}", cfg.storage_engine_type);
                    }
                    Err(e) => {
                        eprintln!("Error loading storage config for CLI display: {:?}", e);
                    }
                }

                let actual_storage_config_path = storage_config_file.unwrap_or_else(|| {
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .parent()
                        .expect("Failed to get parent directory of server crate")
                        .join("storage_daemon_server")
                        .join("storage_config.yaml")
                });

                start_daemon_process(
                    false, true, Some(s_port), Some(actual_storage_config_path), None
                ).await?;
                
                let addr_check = format!("127.0.0.1:{}", s_port);
                let health_check_timeout = Duration::from_secs(5);
                let poll_interval = Duration::from_millis(200);
                let mut started_ok = false;
                let start_time = Instant::now();

                while start_time.elapsed() < health_check_timeout {
                    match tokio::net::TcpStream::connect(&addr_check).await {
                        Ok(_) => {
                            println!("Storage daemon on port {} responded to health check.", s_port);
                            started_ok = true;
                            break;
                        }
                        Err(_) => {
                            tokio::time::sleep(poll_interval).await;
                        }
                    }
                }

                if started_ok {
                    storage_status_msg = format!("Running on port: {}", s_port);
                } else {
                    eprintln!("Warning: Storage daemon daemonized with PID {} but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
                        0, s_port, health_check_timeout);
                    storage_status_msg = format!("Daemonized but failed to become reachable on port {}", s_port);
                }
            }
        }
    } else {
        storage_status_msg = "Not requested".to_string();
    }

    println!("\n--- Component Startup Summary ---");
    println!("{:<15} {:<50}", "Component", "Status");
    println!("{:-<15} {:-<50}", "", "");
    println!("{:<15} {:<50}", "GraphDB", daemon_status_msg);
    println!("{:<15} {:<50}", "REST API", rest_api_status_msg);
    println!("{:<15} {:<50}", "Storage", storage_status_msg);
    println!("---------------------------------\n");
    Ok(())
}

/// Handles the top-level `stop` command.
pub async fn handle_stop_command(stop_args: StopArgs) -> Result<()> {
    match stop_args.action {
        Some(StopAction::Rest) => {
            let rest_port = get_default_rest_port_from_config();
            stop_process_by_port("REST API", rest_port)?;
            println!("REST API stop command processed for port {}.", rest_port);
        }
        Some(StopAction::Daemon { port }) => {
            let p = port.unwrap_or(8080);
            stop_process_by_port("GraphDB Daemon", p)?;
            println!("GraphDB Daemon stop command processed for port {}.", p);
        }
        Some(StopAction::Storage { port }) => {
            let p = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            stop_process_by_port("Storage Daemon", p)?;
            println!("Storage Daemon stop command processed for port {}.", p);
        }
        None => { // Stop all components
            println!("Attempting to stop all GraphDB components...");
            let rest_port = get_default_rest_port_from_config();
            stop_process_by_port("REST API", rest_port)?;
            println!("REST API stop command processed for port {}.", rest_port);

            let storage_port_to_stop = find_running_storage_daemon_port().await.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            stop_process_by_port("Storage Daemon", storage_port_to_stop)?;
            println!("Standalone Storage daemon stop command processed for port {}.", storage_port_to_stop);

            let stop_daemon_result = stop_daemon_api_call();
            match stop_daemon_result {
                Ok(()) => println!("Global daemon stop signal sent successfully."),
                Err(ref e) => eprintln!("Failed to send global stop signal to daemons: {:?}", e),
            }
        }
    }
    Ok(())
}

/// Handles `rest` subcommand for direct CLI execution.
pub async fn handle_rest_command(rest_cmd: RestCliCommand) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port, listen_port } => { // Corrected pattern match
            let target_port = listen_port.or(port).unwrap_or_else(get_default_rest_port_from_config);
            println!("Starting REST API server on port {}...", target_port);
            // Logic to start REST API server (daemonize)
            let current_storage_config = load_storage_config(None) // Assuming no specific config file for direct rest start
                .unwrap_or_else(|e| {
                    eprintln!("Warning: Could not load storage config for REST API daemonization: {}. Using defaults.", e);
                    StorageConfig {
                        data_directory: "/tmp/graphdb_data".to_string(),
                        log_directory: "/var/log/graphdb".to_string(),
                        default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                        cluster_range: "9000-9002".to_string(),
                        max_disk_space_gb: 1000,
                        min_disk_space_gb: 10,
                        use_raft_for_scale: true,
                        storage_engine_type: "sled".to_string(),
                    }
                });

            let storage_engine_type = StorageEngineType::from_str(&current_storage_config.storage_engine_type)
                .unwrap_or(StorageEngineType::Sled);

            start_daemon_process(
                true, // is_rest_api
                false, // is_storage_daemon
                Some(target_port),
                Some(current_storage_config.data_directory.clone().into()), // Pass data directory for storage
                Some(storage_engine_type),
            ).await?;
            println!("REST API server daemonized on port {}.", target_port);
        }
        RestCliCommand::Stop => {
            let rest_port = get_default_rest_port_from_config();
            stop_process_by_port("REST API", rest_port)?;
            println!("REST API server on port {} stopped.", rest_port);
        }
        RestCliCommand::Status => {
            display_rest_api_status().await;
        }
        RestCliCommand::Health => {
            let rest_port = get_default_rest_port_from_config();
            let url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
            let client = reqwest::Client::new();
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("REST API Health on port {}: Status: {}, Body: {}", rest_port, status, body);
                }
                Err(e) => {
                    eprintln!("Failed to connect to REST API on port {} for health check: {}", rest_port, e);
                }
            }
        }
        RestCliCommand::Version => {
            let rest_port = get_default_rest_port_from_config();
            let url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
            let client = reqwest::Client::new();
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("REST API Version on port {}: Status: {}, Body: {}", rest_port, status, body);
                }
                Err(e) => {
                    eprintln!("Failed to connect to REST API on port {} for version check: {}", rest_port, e);
                }
            }
        }
        RestCliCommand::RegisterUser { username, password } => {
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/register", rest_port); // Corrected IP
            let request_body = serde_json::json!({
                "username": username,
                "password": password,
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Registration Response Status: {}", status);
                    println!("Registration Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send registration request: {}", e);
                }
            }
        }
        RestCliCommand::Authenticate { username, password } => {
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/auth", rest_port); // Corrected IP
            let request_body = serde_json::json!({
                "username": username,
                "password": password,
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Authentication Response Status: {}", status);
                    println!("Authentication Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send authentication request: {}", e);
                }
            }
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/query", rest_port); // Corrected IP
            let request_body = serde_json::json!({
                "query": query_string,
                "persist": persist.unwrap_or(false),
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Graph Query Response Status: {}", status);
                    println!("Graph Query Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send graph query: {}", e);
                }
            }
        }
        RestCliCommand::StorageQuery => {
            println!("Not implemented: REST API Storage Query.");
        }
    }
    Ok(())
}

/// Handles `daemon` subcommand for direct CLI execution.
pub async fn handle_daemon_command(daemon_cmd: DaemonCliCommand) -> Result<()> {
    match daemon_cmd {
        DaemonCliCommand::Start { port, cluster } => {
            let p = port.unwrap_or(8080);
            let skip_ports = vec![];
            let daemon_result = start_daemon(Some(p), cluster, skip_ports).await;
            match daemon_result {
                Ok(()) => println!("Daemon on port {} started successfully.", p),
                Err(e) => eprintln!("Failed to start daemon on port {}: {:?}", p, e),
            }
        }
        DaemonCliCommand::Stop { port } => {
            let p = port.unwrap_or(8080); // Default daemon port
            stop_process_by_port("GraphDB Daemon", p)?;
            println!("GraphDB Daemon on port {} stopped.", p);
        }
        DaemonCliCommand::Status { port } => {
            display_daemon_status(port).await;
        }
        DaemonCliCommand::List => {
            let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
            println!("Checking for running GraphDB daemons on common ports:");
            let mut found_any = false;
            for &port in &common_daemon_ports {
                if check_process_status_by_port("GraphDB Daemon", port) {
                    println!("- GraphDB Daemon running on port {}", port);
                    found_any = true;
                }
            }
            if !found_any {
                println!("No GraphDB daemons found on common ports.");
            }
        }
        DaemonCliCommand::ClearAll => {
            println!("Attempting to stop all GraphDB daemon processes...");
            let stop_result = stop_daemon_api_call();
            match stop_result {
                Ok(()) => println!("Global daemon stop signal sent successfully."),
                Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
            }
        }
    }
    Ok(())
}

/// Handles `storage` subcommand for direct CLI execution.
pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file } => {
            let target_port = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            println!("Starting Storage daemon on port {}...", target_port);

            let loaded_storage_config = load_storage_config(Some(config_file.clone()));
            match loaded_storage_config {
                Ok(cfg) => {
                    println!("  Using config file: {}", config_file.display());
                    println!("  Storage Metrics:");
                    println!("    Data Directory: {}", cfg.data_directory);
                    println!("    Log Directory: {}", cfg.log_directory);
                    println!("    Default Port (from config): {}", cfg.default_port);
                    println!("    Cluster Range (from config): {}", cfg.cluster_range);
                    println!("    Max Disk Space: {} GB", cfg.max_disk_space_gb);
                    println!("    Min Disk Space: {} GB", cfg.min_disk_space_gb);
                    println!("    Use Raft for Scale: {}", cfg.use_raft_for_scale);
                    println!("    Storage Engine Type: {}", cfg.storage_engine_type);
                }
                Err(e) => {
                    eprintln!("Error loading storage config for CLI display: {:?}", e);
                }
            }

            // Ensure the port is free before starting
            stop_process_by_port("Storage Daemon", target_port)?;
            let addr = format!("127.0.0.1:{}", target_port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;

            while start_time.elapsed() < wait_timeout {
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(_) => {
                        port_freed = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if !port_freed {
                eprintln!("Failed to free up storage port {} after killing processes. Try again.", target_port);
                return Err(anyhow::anyhow!("Failed to free storage port {}", target_port));
            }

            start_daemon_process(
                false, // is_rest_api
                true, // is_storage_daemon
                Some(target_port),
                Some(config_file),
                None, // StorageEngineType is determined by the daemon itself from config
            ).await?;
            println!("Storage daemon daemonized on port {}.", target_port);
        }
        StorageAction::Stop { port } => {
            let p = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            stop_process_by_port("Storage Daemon", p)?;
            println!("Storage Daemon on port {} stopped.", p);
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port).await;
        }
    }
    Ok(())
}

/// Handles the top-level `status` command.
pub async fn handle_status_command(status_args: StatusArgs) -> Result<()> {
    match status_args.action {
        Some(StatusAction::Rest) => { // Corrected to use StatusAction directly
            display_rest_api_status().await;
        }
        Some(StatusAction::Daemon { port }) => { // Corrected to use StatusAction directly
            display_daemon_status(port).await;
        }
        Some(StatusAction::Storage { port }) => { // Corrected to use StatusAction directly
            display_storage_daemon_status(port).await;
        }
        None => {
            display_full_status_summary().await;
        }
    }
    Ok(())
}

// --- Command Handlers for interactive CLI execution ---
// These handlers are specifically designed to be called from the interactive loop
// and manage shared state (daemon_handles, rest_api_shutdown_tx_opt, etc.).

pub async fn handle_daemon_command_interactive(
    daemon_cmd: DaemonCliCommand,
    daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match daemon_cmd {
        DaemonCliCommand::Start { port, cluster } => {
            let p = port.unwrap_or(8080);
            let mut handles = daemon_handles.lock().await;

            if handles.contains_key(&p) {
                println!("Daemon on port {} is already running (managed by this CLI).", p);
                return Ok(());
            }

            println!("Attempting to start daemon on port {}...", p);

            let (tx, rx) = oneshot::channel();
            let skip_ports = vec![]; // For interactive, REST/Storage ports are managed separately

            let daemon_join_handle = tokio::spawn(async move {
                let result = start_daemon(Some(p), cluster, skip_ports).await;
                match result {
                    Ok(_) => println!("Daemon on port {} started successfully.", p),
                    Err(e) => eprintln!("Failed to start daemon on port {}: {:?}", p, e),
                }
                let _ = rx.await;
                println!("Daemon on port {} is shutting down...", p);
            });
            handles.insert(p, (daemon_join_handle, tx));
            println!("Daemon started (initiation successful, check logs for full status).");
        }
        DaemonCliCommand::Stop { port } => {
            let mut handles = daemon_handles.lock().await;
            if let Some(current_port) = port {
                if let Some((_, tx)) = handles.remove(&current_port) {
                    println!("Sending stop signal to daemon on port {}...", current_port);
                    if tx.send(()).is_err() {
                        eprintln!("Failed to send shutdown signal to daemon on port {}. It might have already stopped.", current_port);
                    } else {
                        println!("Daemon on port {} stopping...", current_port);
                    }
                } else {
                    println!("No daemon found running on port {} (managed by this CLI).", current_port);
                }
            } else {
                println!("Usage: daemon stop --port <port>");
            }
        }
        DaemonCliCommand::Status { port } => {
            display_daemon_status(port).await;
        }
        DaemonCliCommand::List => {
            let handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No daemons currently managed by this CLI instance.");
            } else {
                println!("Currently running daemons (managed by this CLI):");
                for port in handles.keys() {
                    println!("- Daemon on port {}", port);
                }
            }
        }
        DaemonCliCommand::ClearAll => {
            let mut handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No daemons to clear managed by this CLI.");
                return Ok(());
            }
            println!("Stopping all {} managed daemons...", handles.len());

            let ports: Vec<u16> = handles.keys().cloned().collect();
            for port in ports {
                if let Some((_, tx)) = handles.remove(&port) {
                    println!("Sending stop signal to daemon on port {}...", port);
                    if tx.send(()).is_err() {
                        eprintln!("Failed to send shutdown signal to daemon on port {}. It might have already stopped.", port);
                    } else {
                        println!("Daemon on port {} stopping...", port);
                    }
                }
            }
            println!("Sending global stop signal to all external daemon processes...");
            let stop_result = stop_daemon_api_call();
            match stop_result {
                Ok(()) => println!("Global daemon stop signal sent successfully."),
                Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
            }
            println!("All managed daemon instances and external daemon processes stopped.");
        }
    }
    Ok(())
}

pub async fn handle_rest_command_interactive(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port, listen_port } => { // Corrected pattern match
            let target_port = listen_port.or(port).unwrap_or_else(get_default_rest_port_from_config);

            if target_port < 1024 || target_port > 65535 {
                eprintln!("Invalid port: {}. Must be between 1024 and 65535.", target_port);
                return Ok(());
            }

            // Check if REST API is already running or managed by this CLI
            let mut current_port_guard = rest_api_port_arc.lock().await;
            if let Some(p) = *current_port_guard {
                if p == target_port {
                    println!("REST API server already managed by this CLI on port {}.", target_port);
                    return Ok(());
                } else {
                    println!("Warning: REST API server already managed by this CLI on a different port ({}). Stopping it first.", p);
                    // Attempt to stop the previously managed instance
                    if let Some(tx) = rest_api_shutdown_tx_opt.lock().await.take() {
                        let _ = tx.send(());
                    }
                    if let Some(handle) = rest_api_handle.lock().await.take() {
                        let _ = handle.await;
                    }
                    stop_process_by_port("REST API", p)?;
                    *current_port_guard = None; // Clear the old port
                }
            }

            // Ensure the port is free before starting
            stop_process_by_port("REST API", target_port)?;
            let addr = format!("127.0.0.1:{}", target_port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;

            while start_time.elapsed() < wait_timeout {
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(_) => {
                        port_freed = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if !port_freed {
                eprintln!("Failed to free up port {} after killing processes. Try again.", target_port);
                return Ok(());
            }

            println!("Starting REST API server on port {}...", target_port);
            let (tx, rx) = oneshot::channel();
            let rest_api_handle_clone = Arc::clone(&rest_api_handle);
            let rest_api_port_arc_clone = Arc::clone(&rest_api_port_arc);

            let handle = tokio::spawn(async move {
                // In a real scenario, this would be a call to rest_api::start_server
                // that blocks until a shutdown signal is received.
                println!("Interactive REST API on port {} started (simulated).", target_port);
                let _ = rx.await; // Wait for shutdown signal
                println!("Interactive REST API on port {} shutting down (simulated).", target_port);
                // Clean up the handle and port from the shared state
                *rest_api_handle_clone.lock().await = None;
                *rest_api_port_arc_clone.lock().await = None;
            });

            *rest_api_shutdown_tx_opt.lock().await = Some(tx);
            *rest_api_port_arc.lock().await = Some(target_port);
            *rest_api_handle.lock().await = Some(handle);

            println!("REST API server started on port {}. Managed by CLI.", target_port);
        }
        RestCliCommand::Stop => {
            let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
            let mut rest_handle_guard = rest_api_handle.lock().await;
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;

            if let Some(port) = rest_api_port_guard.take() {
                println!("Stopping managed REST API server on port {}...", port);
                if let Some(tx) = rest_tx_guard.take() {
                    let _ = tx.send(()); // Signal the task to shut down
                }
                if let Some(handle) = rest_handle_guard.take() {
                    let _ = handle.await; // Wait for the task to finish
                }
                println!("Managed REST API server on port {} stopped.", port);
            } else {
                let rest_port = get_default_rest_port_from_config();
                println!("No managed REST API server found. Attempting to stop external process on default port {}...", rest_port);
                stop_process_by_port("REST API", rest_port)?;
            }
        }
        RestCliCommand::Status => {
            display_rest_api_status().await;
        }
        RestCliCommand::Health => {
            let rest_port = get_default_rest_port_from_config();
            let url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
            let client = reqwest::Client::new();
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("REST API Health on port {}: Status: {}, Body: {}", rest_port, status, body);
                }
                Err(e) => {
                    eprintln!("Failed to connect to REST API on port {} for health check: {}", rest_port, e);
                }
            }
        }
        RestCliCommand::Version => {
            let rest_port = get_default_rest_port_from_config();
            let url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
            let client = reqwest::Client::new();
            match client.get(&url).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("REST API Version on port {}: Status: {}, Body: {}", rest_port, status, body);
                }
                Err(e) => {
                    eprintln!("Failed to connect to REST API on port {} for version check: {}", rest_port, e);
                }
            }
        }
        RestCliCommand::RegisterUser { username, password } => {
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/register", rest_port); // Corrected IP
            let request_body = serde_json::json!({
                "username": username,
                "password": password,
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Registration Response Status: {}", status);
                    println!("Registration Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send registration request: {}", e);
                }
            }
        }
        RestCliCommand::Authenticate { username, password } => {
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/auth", rest_port); // Corrected IP
            let request_body = serde_json::json!({
                "username": username,
                "password": password,
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Authentication Response Status: {}", status);
                    println!("Authentication Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send authentication request: {}", e);
                }
            }
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            let rest_port = get_default_rest_port_from_config();
            let client = reqwest::Client::new();
            let url = format!("http://127.0.0.1:{}/api/v1/query", rest_port); // Corrected IP
            let request_body = serde_json::json!({
                "query": query_string,
                "persist": persist.unwrap_or(false),
            });

            match client.post(&url).json(&request_body).send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response.text().await.unwrap_or_default();
                    println!("Graph Query Response Status: {}", status);
                    println!("Graph Query Response Body: {}", body);
                }
                Err(e) => {
                    eprintln!("Failed to send graph query: {}", e);
                }
            }
        }
        RestCliCommand::StorageQuery => {
            println!("Not implemented: REST API Storage Query.");
        }
    }
    Ok(())
}

pub async fn handle_storage_command_interactive(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file } => {
            let target_port = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            println!("Starting Storage daemon interactively on port {}...", target_port);

            let loaded_storage_config = load_storage_config(Some(config_file.clone()));
            match loaded_storage_config {
                Ok(cfg) => {
                    println!("  Using config file: {}", config_file.display());
                    println!("  Storage Metrics:");
                    println!("    Data Directory: {}", cfg.data_directory);
                    println!("    Log Directory: {}", cfg.log_directory);
                    println!("    Default Port (from config): {}", cfg.default_port);
                    println!("    Cluster Range (from config): {}", cfg.cluster_range);
                    println!("    Max Disk Space: {} GB", cfg.max_disk_space_gb);
                    println!("    Min Disk Space: {} GB", cfg.min_disk_space_gb);
                    println!("    Use Raft for Scale: {}", cfg.use_raft_for_scale);
                    println!("    Storage Engine Type: {}", cfg.storage_engine_type);
                }
                Err(e) => {
                    eprintln!("Error loading storage config for CLI display: {:?}", e);
                }
            }

            // Ensure the port is free before starting
            stop_process_by_port("Storage Daemon", target_port)?;
            let addr = format!("127.0.0.1:{}", target_port);
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;

            while start_time.elapsed() < wait_timeout {
                match tokio::net::TcpListener::bind(&addr).await {
                    Ok(_) => {
                        port_freed = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if !port_freed {
                eprintln!("Failed to free up storage port {} after killing processes. Try again.", target_port);
                return Err(anyhow::anyhow!("Failed to free storage port {}", target_port));
            }

            // In interactive mode, we start the daemon as a child process directly
            start_daemon_process(
                false, // is_rest_api
                true, // is_storage_daemon
                Some(target_port),
                Some(config_file),
                None, // StorageEngineType is determined by the daemon itself from config
            ).await?;
            println!("Storage daemon started on port {}. Daemonized.", target_port);

            let addr_check = format!("127.0.0.1:{}", target_port);
            let health_check_timeout = Duration::from_secs(5);
            let poll_interval = Duration::from_millis(200);
            let mut started_ok = false;
            let start_time = Instant::now();

            while start_time.elapsed() < health_check_timeout {
                match tokio::net::TcpStream::connect(&addr_check).await {
                    Ok(_) => {
                        println!("Storage daemon on port {} responded to health check.", target_port);
                        started_ok = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if started_ok {
                println!("Storage daemon on port {} is reachable.", target_port);
            } else {
                eprintln!("Warning: Storage daemon daemonized with PID {} but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
                    0, target_port, health_check_timeout);
            }
        }
        StorageAction::Stop { port } => {
            let target_port = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
            stop_process_by_port("Storage Daemon", target_port)?;
            println!("Storage Daemon on port {} stopped.", target_port);
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port).await;
        }
    }
    Ok(())
}

/// Handles the top-level `stop` command in interactive mode.
pub async fn handle_stop_all_interactive() -> Result<()> {
    println!("Attempting to stop all GraphDB components interactively...");
    let rest_port = get_default_rest_port_from_config();
    stop_process_by_port("REST API", rest_port)?;
    println!("REST API stop command processed for port {}.", rest_port);

    let storage_port_to_stop = find_running_storage_daemon_port().await.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
    stop_process_by_port("Storage Daemon", storage_port_to_stop)?;
    println!("Standalone Storage daemon stop command processed for port {}.", storage_port_to_stop);

    let stop_daemon_result = stop_daemon_api_call();
    match stop_daemon_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }
    println!("All GraphDB components stop commands processed.");
    Ok(())
}

/// Handles stopping the REST API in interactive mode.
pub async fn handle_stop_rest_interactive() -> Result<()> {
    let rest_port = get_default_rest_port_from_config();
    stop_process_by_port("REST API", rest_port)?;
    println!("REST API stop command processed for port {}.", rest_port);
    Ok(())
}

/// Handles stopping a specific daemon in interactive mode.
pub async fn handle_stop_daemon_interactive(port: Option<u16>) -> Result<()> {
    let p = port.unwrap_or(8080);
    stop_process_by_port("GraphDB Daemon", p)?;
    println!("GraphDB Daemon stop command processed for port {}.", p);
    Ok(())
}

/// Handles stopping the storage daemon in interactive mode.
pub async fn handle_stop_storage_interactive(port: Option<u16>) -> Result<()> {
    let p = port.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);
    stop_process_by_port("Storage Daemon", p)?;
    println!("Storage Daemon stop command processed for port {}.", p);
    Ok(())
}


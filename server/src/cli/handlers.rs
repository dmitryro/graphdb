// This file contains handler functions for CLI commands.
// FIX: 2025-07-12 - Added cluster field to StartAction patterns to resolve E0027.
// FIX: 2025-07-13 - Added proper storage config loading from storage_daemon_server/storage_config.yaml.
// FIX: 2025-07-13 - Fixed daemon status to correctly show running state for cluster ports.
// FIX: 2025-07-13 - Made `graphdb-cli status` behave like `status all`.
// FIX: 2025-07-13 - Fixed infinite loop in storage start and stop commands.
// FIX: 2025-07-13 - Implemented proper port precedence and conflict checking.
// FIX: 2025-07-12 - Added missing interactive command handlers and fixed type mismatches.

use anyhow::{Result, Context};
use std::collections::{HashMap, HashSet};
use std::path::{PathBuf, Path};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use std::sync::Arc;
use std::process::Stdio; // Make sure this is imported
use tokio::process::Command as TokioCommand;
use std::time::{Duration, Instant};
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use crossterm::cursor::MoveTo;
use std::io::{self, Write};
use std::fs; 
use std::str::FromStr; // Required for FromStr trait
use sysinfo::{System, Pid, ProcessesToUpdate}; // Add System and other sysinfo items if not already present
use futures::{future, join};
use serde_json::Value;
use reqwest::Client; 
use console::style; // Assuming for colored output

use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, RestartArgs, StatusAction, StopAction, ReloadAction, RestartAction, StartArgs, StartAction};
use crate::cli::config::{CliConfig, ServerConfig, get_default_rest_port_from_config, load_cli_config, load_storage_config, 
                         CliTomlStorageConfig as CliStorageConfig, StorageConfig, StorageEngineType as CliStorageEngineType, DEFAULT_STORAGE_CONFIG_PATH};
use lib::storage_engine::config::{StorageConfig as LibStorageConfig, StorageEngineType};
use crate::cli::daemon_management::{start_daemon_process,
                                   find_daemon_process_on_port,
                                   send_stop_signal_to_daemon_main,
                                     send_stop_signal_to_daemon,
                                     is_daemon_running,
                                     get_default_storage_port_from_config_or_cli_default,
                                     is_rest_api_running,
                                     send_stop_signal_to_rest_api,
                                     get_cli_storage_config};

use storage_daemon_server as storage_lib; 
use daemon_api::{stop_daemon, start_daemon};



const DEFAULT_DAEMON_PORT: u16 = 8000;
const DEFAULT_REST_API_PORT: u16 = 8080;
const DEFAULT_STORAGE_PORT: u16 = 8085;

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

pub mod rest {
    pub mod api {
        use anyhow::Result;
        use reqwest::Client;
        use std::time::Duration;

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
        pub async fn execute_graph_query(_query: &str, _persist: Option<bool>) -> Result<String> {
            Ok(format!("Query result for: {}", _query))
        }
    }
}

pub mod storage {
    pub mod api {
        use anyhow::Result;
        pub async fn check_storage_daemon_status(_port: u16) -> Result<String> { Ok("Running".to_string()) }
    }
}

pub fn get_current_exe_path() -> Result<PathBuf> {
    std::env::current_exe().context("Failed to get current executable path")
}

pub async fn is_port_free(port: u16) -> bool {
    tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await.is_ok()
}

async fn run_command_with_timeout(
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
        match TokioCommand::new("kill").arg("-15").arg(pid.to_string()).status().await {
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

    Err(anyhow::anyhow!("Port {} remained in use after killing processes within {:?}", port, wait_timeout))
}

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

pub async fn find_all_running_rest_api_ports() -> Vec<u16> {
    let common_rest_ports = [DEFAULT_REST_API_PORT, 8081, 8082, 8083, 8084, 8085, 8086];
    let mut running_ports = Vec::new();
    for &port in &common_rest_ports {
        if rest::api::check_rest_api_status(port).await.is_ok() {
            running_ports.push(port);
        }
    }
    running_ports
}

pub async fn find_all_running_storage_daemon_ports() -> Vec<u16> {
    let config = load_storage_config(None).unwrap_or_else(|_| StorageConfig {
        data_directory: "/opt/graphdb/storage_data".into(),
        log_directory: "/var/log/graphdb".into(),
        default_port: 8085,
        cluster_range: "9000-9002".into(),
        max_disk_space_gb: 1000,
        min_disk_space_gb: 10,
        use_raft_for_scale: true,
        storage_engine_type: "sled".into(),
        max_open_files: 1024,
        config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
            .parent()
            .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
            .to_path_buf(),
    });
    let mut ports_to_check = vec![config.default_port];
    if let Ok(cluster_ports) = parse_cluster_range(&config.cluster_range) {
        ports_to_check.extend(cluster_ports);
    }
    let mut running_ports = Vec::new();
    for &port in &ports_to_check {
        if check_process_status_by_port("Storage Daemon", port).await {
            running_ports.push(port);
        }
    }
    running_ports
}

pub async fn find_all_running_daemon_ports() -> Vec<u16> {
    let common_daemon_ports = [DEFAULT_DAEMON_PORT, 8081, 9001, 9002, 9003, 9004, 9005];
    let mut running_ports = Vec::new();
    for &port in &common_daemon_ports {
        if check_process_status_by_port("GraphDB Daemon", port).await {
            running_ports.push(port);
        }
    }
    running_ports
}

pub async fn display_rest_api_status(_rest_api_port_arc: Arc<Mutex<Option<u16>>>) {
    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let running_ports = find_all_running_rest_api_ports().await;

    if running_ports.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No REST API servers found on common ports.");
    } else {
        for &port in &running_ports {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build().expect("Failed to build reqwest client");

            let mut rest_api_details = String::new();

            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    rest_api_details = format!("Health: OK");

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
                _ => rest_api_details = "Health: Down".to_string(),
            }
            println!("{:<15} {:<10} {:<40}", "Running", port, rest_api_details);
        }
    }

    println!("--------------------------------------------------");
}

pub async fn display_daemon_status(port_arg: Option<u16>) {
    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let ports_to_check = if let Some(p) = port_arg {
        vec![p]
    } else {
        find_all_running_daemon_ports().await
    };

    let mut found_any = false;
    for &port in &ports_to_check {
        let status_message = if check_process_status_by_port("GraphDB Daemon", port).await {
            found_any = true;
            "Running".to_string()
        } else {
            "Down".to_string()
        };
        println!("{:<15} {:<10} {:<40}", status_message, port, "Core Graph Processing");
    }

    if !found_any && port_arg.is_none() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No daemons found on common ports.");
    }
    if port_arg.is_none() {
        println!("\nTo check a specific daemon, use 'status daemon --port <port>'.");
    }
    println!("--------------------------------------------------");
}

pub async fn display_storage_daemon_status(port_arg: Option<u16>, _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>) {
    let config = load_storage_config(None).unwrap_or_else(|_| StorageConfig {
        data_directory: "/opt/graphdb/storage_data".into(),
        log_directory: "/var/log/graphdb".into(),
        default_port: 8085,
        cluster_range: "9000-9002".into(),
        max_disk_space_gb: 1000,
        min_disk_space_gb: 10,
        use_raft_for_scale: true,
        storage_engine_type: "sled".into(),
        max_open_files: 1024,
        config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
            .parent()
            .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
            .to_path_buf(),
    });
    let port_to_check = port_arg.unwrap_or(config.default_port);

    let storage_config = LibStorageConfig::default();
    let cluster_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let status_message = if check_process_status_by_port("Storage Daemon", port_to_check).await {
        "Running".to_string()
    } else {
        "Down".to_string()
    };
    println!("{:<15} {:<10} {:<40}", status_message, port_to_check, format!("Type: {:?}", storage_config.engine_type));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Data Dir: {}", storage_config.data_path));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
    println!("{:<15} {:<10} {:<40}", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
    if !cluster_ports.is_empty() {
        println!("{:<15} {:<10} {:<40}", "", "", format!("Cluster Ports: {}", cluster_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")));
    }
    println!("--------------------------------------------------");
}

pub async fn display_cluster_status() {
    println!("\n--- Cluster Status ---");
    println!("Cluster status is a placeholder. In a real implementation, this would query all daemons in the cluster.");
    println!("--------------------------------------------------");
}

pub async fn display_full_status_summary(_rest_api_port_arc: Arc<Mutex<Option<u16>>>, _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>) {
    let config = load_storage_config(None).expect("Failed to load storage config");
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    let common_daemon_ports = [DEFAULT_DAEMON_PORT, 8081, 9001, 9002, 9003, 9004, 9005];
    let daemon_checks: Vec<_> = common_daemon_ports.iter().map(|&port| {
        tokio::spawn(async move {
            (port, check_process_status_by_port("GraphDB Daemon", port).await)
        })
    }).collect();

    let results = future::join_all(daemon_checks).await;
    let mut running_daemon_ports = Vec::new();
    for result in results {
        if let Ok((port, is_running)) = result {
            if is_running {
                running_daemon_ports.push(port);
            }
        } else {
            eprintln!("Error checking daemon status: {:?}", result.unwrap_err());
        }
    }

    let daemon_status_msg = if !running_daemon_ports.is_empty() {
        format!("Running on: {}", running_daemon_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "))
    } else {
        "Down".to_string()
    };
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status_msg, "N/A", "Core Graph Processing");

    let running_rest_ports = find_all_running_rest_api_ports().await;
    let rest_api_status = if running_rest_ports.is_empty() {
        "Down".to_string()
    } else {
        "Running".to_string()
    };
    let rest_ports_display = if running_rest_ports.is_empty() {
        DEFAULT_REST_API_PORT.to_string()
    } else {
        running_rest_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
    };
    let mut rest_api_details = String::new();

    if !running_rest_ports.is_empty() {
        if let Some(&port) = running_rest_ports.first() {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build().expect("Failed to build reqwest client");
            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);

            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    rest_api_details = format!("Health: OK");
                    let version_info = client.get(&version_url).send().await;
                    match version_info {
                        Ok(v_resp) if v_resp.status().is_success() => {
                            let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                            let version = v_json["version"].as_str().unwrap_or("N/A");
                            rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                        },
                        _ => rest_api_details = "Version: N/A".to_string(),
                    }
                },
                _ => rest_api_details = "Health: Down".to_string(),
            }
        }
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, rest_ports_display, rest_api_details);

    // FIX: Use the 'config' variable (loaded from storage_config.yaml) for display details
    // instead of LibStorageConfig::default().
    let cluster_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();
    let running_storage_ports = find_all_running_storage_daemon_ports().await;
    let storage_daemon_status = if running_storage_ports.is_empty() {
        "Down".to_string()
    } else {
        "Running".to_string()
    };
    let storage_ports_display = if running_storage_ports.is_empty() {
        config.default_port.to_string()
    } else {
        running_storage_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
    };

    let mut storage_daemon_details = String::new();
    use std::fmt::Write;

    write!(storage_daemon_details, "Type: {}", config.storage_engine_type).unwrap(); // Use config's engine type
    write!(storage_daemon_details, "; Data Dir: {}", config.data_directory).unwrap(); // Use config's data directory
    write!(storage_daemon_details, "; Max Open Files: {}", config.max_open_files).unwrap(); // Use config's max open files

    // Removed "Engine Config: None" as it's not present in your provided storage_config.yaml
    // and was a default from LibStorageConfig.

    if !cluster_ports.is_empty() {
        write!(storage_daemon_details, "; Configured Cluster: {}", cluster_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")).unwrap();
    }

    println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, storage_ports_display, storage_daemon_details);
    println!("--------------------------------------------------");
}

// Helper to parse cluster range, returns Vec of ports
fn parse_cluster_range(s: &str) -> Result<Vec<u16>> {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() == 2 {
        let start = parts[0].parse::<u16>().context("Invalid start port in cluster range")?;
        let end = parts[1].parse::<u16>().context("Invalid end port in cluster range")?;
        if start <= end {
            Ok((start..=end).collect())
        } else {
            Err(anyhow::anyhow!("Start port ({}) must be less than or equal to end port ({}) in cluster range '{}'.", start, end, s))
        }
    } else {
        Err(anyhow::anyhow!("Invalid cluster range format: '{}'. Expected 'start_port-end_port'.", s))
    }
}

pub fn print_welcome_screen() {
    let (cols, rows) = terminal_size().unwrap_or((120, 40));
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

    let print_centered_colored = |text: &str, text_color: style::Color, is_bold: bool| {
        let internal_padding_chars = 6;
        let content_width = total_width.saturating_sub(2 + internal_padding_chars);
        let padding_len = content_width.saturating_sub(text.len());
        let left_padding = padding_len / 2;
        let right_padding = padding_len - left_padding;

        print!("{}", style::SetForegroundColor(style::Color::Cyan));
        print!("{}", border_char);
        print!("{}", " ".repeat(internal_padding_chars / 2));

        print!("{}", style::ResetColor);
        let styled_text = if is_bold {
            text.with(text_color).bold()
        } else {
            text.with(text_color)
        };

        print!("{}", " ".repeat(left_padding));
        print!("{}", styled_text);
        print!("{}", " ".repeat(right_padding));

        print!("{}", style::SetForegroundColor(style::Color::Cyan));
        println!("{}{}", border_char, style::ResetColor);
    };

    let content_lines = 13;
    let available_rows = rows as usize;
    let top_bottom_padding = available_rows.saturating_sub(content_lines) / 2;

    for _ in 0..top_bottom_padding {
        println!();
    }

    println!("{}", line_str.clone().with(style::Color::Cyan));
    print_centered_colored("", style::Color::Blue, false);
    print_centered_colored(title, style::Color::DarkCyan, true);
    print_centered_colored(version, style::Color::White, true);
    print_centered_colored("", style::Color::Blue, false);
    print_centered_colored(welcome_msg, style::Color::Green, true);
    print_centered_colored(start_tip, style::Color::Yellow, false);
    print_centered_colored(status_tip, style::Color::Yellow, false);
    print_centered_colored(clear_tip, style::Color::Yellow, false);
    print_centered_colored(exit_tip, style::Color::Red, false);
    print_centered_colored("", style::Color::Blue, false);
    println!("{}", line_str.with(style::Color::Cyan));

    for _ in 0..top_bottom_padding {
        println!();
    }
}

pub async fn clear_terminal_screen() -> Result<()> {
    execute!(io::stdout(), Clear(ClearType::All), MoveTo(0, 0))
        .context("Failed to clear terminal screen or move cursor")?;
    io::stdout().flush()?;
    Ok(())
}

async fn check_port_conflicts(port: u16, component: &str, skip_ports: &[u16]) -> Result<()> {
    if skip_ports.contains(&port) {
        return Err(anyhow::anyhow!("Port {} is reserved for another GraphDB component", port));
    }
    // Check if the port is in use by a *different* type of GraphDB component
    // or a non-GraphDB process.
    // If the component is "GraphDB Daemon" and the port is in use by another "GraphDB Daemon" (expected in cluster)
    // then it's not a conflict for the purpose of starting *this* daemon instance.
    let is_daemon_running_on_port = check_process_status_by_port("GraphDB Daemon", port).await;
    let is_rest_api_running_on_port = check_process_status_by_port("REST API", port).await;
    let is_storage_daemon_running_on_port = check_process_status_by_port("Storage Daemon", port).await;

    if component == "GraphDB Daemon" {
        if is_rest_api_running_on_port || is_storage_daemon_running_on_port {
            return Err(anyhow::anyhow!("Port {} is already in use by a REST API or Storage Daemon component.", port));
        }
        // If it's another GraphDB Daemon, we allow it for cluster scenarios,
        // but we should still ensure it's not a *different* process trying to bind to the same port.
        // The `start_daemon` function itself will handle the actual bind error if the port is truly unavailable.
        // For CLI-managed daemons, we assume `stop_process_by_port` handles cleanup first.
        if is_daemon_running_on_port {
            // If it's already running, and it's *this* type of component, it's not a conflict for *starting*
            // but rather an indication it's already active. The caller should decide how to handle.
            // For cluster, this means one instance is already up, which is fine.
            // For single instance, it means it's already running.
            // We'll let the `start_daemon` or `start_daemon_process` handle the actual bind error if it's truly occupied.
            // This `check_port_conflicts` is more about preventing cross-component conflicts.
            println!("Port {} is already in use by a GraphDB Daemon, proceeding assuming it's part of the intended cluster or already running.", port);
            return Ok(()); // Not a conflict for *this* component type
        }
    } else if component == "REST API" {
        if is_daemon_running_on_port || is_storage_daemon_running_on_port {
            return Err(anyhow::anyhow!("Port {} is already in use by a GraphDB Daemon or Storage Daemon component.", port));
        }
    } else if component == "Storage Daemon" {
        if is_daemon_running_on_port || is_rest_api_running_on_port {
            return Err(anyhow::anyhow!("Port {} is already in use by a GraphDB Daemon or REST API component.", port));
        }
    }

    // Fallback: if any process is listening on the port, and it's not handled by the specific component logic above,
    // then it's a general conflict.
    if !is_port_free(port).await {
        return Err(anyhow::anyhow!("Port {} is already in use by an unknown process.", port));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_start_command(
    start_args: StartArgs, // Assuming StartArgs contains all necessary flags (port, cluster, listen_port, storage_port, daemon, rest, storage etc.)
    _config: &crate::cli::config::CliConfig,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    // Ensure DEFAULT_REST_API_PORT is imported, typically from the rest_api crate
    // You might also need to ensure DEFAULT_DAEMON_PORT is imported, e.g.:
    // use daemon_api::DEFAULT_DAEMON_PORT; // Assuming this constant exists in daemon_api

    // Load storage config once
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
                .parent()
                .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
                .to_path_buf(),
        });

    let mut daemon_status_msg = "Not requested".to_string();
    let mut rest_api_status_msg = "Not requested".to_string();
    let mut storage_status_msg = "Not requested".to_string();

    // --- 1. Identify all ports that need to be cleaned up ---
    let mut ports_to_clean = HashSet::new();

    // Determine which components are requested to start based on action or default behavior
    let (mut start_daemon_requested, mut start_rest_requested, mut start_storage_requested) = (false, false, false);
    match &start_args.action {
        Some(StartAction::All { daemon, rest, storage, .. }) => {
            start_daemon_requested = daemon.unwrap_or(true);
            start_rest_requested = rest.unwrap_or(true);
            start_storage_requested = storage.unwrap_or(true);
        },
        Some(StartAction::Daemon { daemon, .. }) => start_daemon_requested = daemon.unwrap_or(true),
        Some(StartAction::Rest { .. }) => start_rest_requested = true,
        Some(StartAction::Storage { .. }) => start_storage_requested = true,
        None => { // Default case if no specific action is provided (e.g., `graphdb-cli start`)
            start_daemon_requested = true;
            start_rest_requested = true;
            start_storage_requested = true;
        }
    }

    // Collect ports for Main GraphDB Daemon
    if start_daemon_requested {
        if let Some(p) = start_args.port {
            ports_to_clean.insert(p);
        }
        if let Some(cluster_str) = start_args.cluster.clone() {
            if let Ok(ports) = parse_cluster_range(&cluster_str) {
                ports_to_clean.extend(ports);
            } else {
                eprintln!("Warning: Invalid cluster range format '{}' during initial port collection. It will be re-validated during actual daemon start.", cluster_str);
            }
        } else if start_args.port.is_none() { // If no explicit port or cluster, use default for main daemon
            ports_to_clean.insert(DEFAULT_DAEMON_PORT);
        }
    }

    // Collect ports for REST API
    if start_rest_requested {
        if let Some(p) = start_args.listen_port {
            ports_to_clean.insert(p);
        } else { // If no explicit listen_port, use default
            // FIX: Use DEFAULT_REST_API_PORT directly as instructed
            ports_to_clean.insert(DEFAULT_REST_API_PORT);
        }
    }

    // Collect ports for Storage Daemon
    if start_storage_requested {
        if let Some(p) = start_args.storage_port {
            ports_to_clean.insert(p);
        } else { // If no explicit storage_port, use default from config
            ports_to_clean.insert(config.default_port);
        }
        // Also consider storage cluster range (from start_args.cluster or storage config's cluster_range)
        let storage_cluster_range_for_cleanup = start_args.cluster.clone().or(Some(config.cluster_range.clone()));
        if let Some(cluster_str) = storage_cluster_range_for_cleanup {
            if let Ok(ports) = parse_cluster_range(&cluster_str) {
                ports_to_clean.extend(ports);
            } else {
                eprintln!("Warning: Invalid cluster range format '{}' during initial storage port collection. It will be re-validated during actual storage start.", cluster_str);
            }
        }
    }

    if ports_to_clean.is_empty() {
        println!("No specific ports identified for cleanup or startup. Proceeding without port operations.");
    } else {
        println!("Identified ports for pre-launch cleanup: {:?}", ports_to_clean);

        // --- 2. Centralized Pre-Launch Cleanup for all identified ports ---
        println!("Initiating pre-launch port cleanup for GraphDB services. Please wait...");
        for &port_to_clear in &ports_to_clean {
            // Attempt to stop main daemon, REST, and Storage processes on this port.
            // `stop_process_by_port` uses `find_daemon_process_on_port` with a name hint.
            // It's crucial that these names (e.g., "graphdb-cli", "REST API", "Storage Daemon")
            // accurately reflect the executable names or substrings found in process names.

            // Try to stop a main GraphDB daemon (e.g., "graphdb-cli" process)
            if let Err(e) = stop_process_by_port("graphdb-cli", port_to_clear).await {
                eprintln!("Warning: Failed to stop 'graphdb-cli' on port {}: {:?}", port_to_clear, e);
            }
            // Try to stop a REST API server
            if let Err(e) = stop_process_by_port("REST API", port_to_clear).await {
                eprintln!("Warning: Failed to stop 'REST API' on port {}: {:?}", port_to_clear, e);
            }
            // Try to stop a Storage Daemon
            if let Err(e) = stop_process_by_port("Storage Daemon", port_to_clear).await {
                eprintln!("Warning: Failed to stop 'Storage Daemon' on port {}: {:?}", port_to_clear, e); // Corrected typo
            }
            // Add a small delay after attempting to stop a process for robustness
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        println!("Pre-launch port cleanup complete.");
    }


    // --- 3. Refined Component-Specific Launch Logic ---
    // Removed `check_port_conflicts` and `stop_process_by_port` from individual blocks
    // as global cleanup handles it upfront.
    match start_args.action {
        Some(StartAction::All {
            port,
            cluster,
            config_file: _, // Handled by load_storage_config earlier
            listen_port,
            storage_port,
            storage_config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
            storage,
        }) => {
            let actual_start_daemon = daemon.unwrap_or(true);
            let actual_start_rest = rest.unwrap_or(true);
            let actual_start_storage = storage.unwrap_or(true);

            if actual_start_daemon {
                let daemon_ports_to_start = if let Some(cluster_range_str) = cluster.clone() {
                    parse_cluster_range(&cluster_range_str).unwrap_or_default()
                } else {
                    vec![port.unwrap_or(DEFAULT_DAEMON_PORT)]
                };

                let mut successful_daemon_ports = Vec::new();
                for &p in &daemon_ports_to_start {
                    println!("Attempting to start main GraphDB Daemon on port {}...", p);
                    // Call start_daemon_instance_interactive for each port in the cluster
                    // The `check_port_conflicts` inside `start_daemon_instance_interactive`
                    // is now more lenient for other GraphDB daemons.
                    let daemon_instance_result = start_daemon_instance_interactive(Some(p), None, daemon_handles.clone()).await; // Pass None for cluster to avoid recursion
                    match daemon_instance_result {
                        Ok(()) => {
                            successful_daemon_ports.push(p);
                            println!("GraphDB Daemon launched on port {}. It should be running in the background.", p);
                        },
                        Err(e) => {
                            eprintln!("Failed to start GraphDB Daemon on port {}: {:?}", p, e);
                        }
                    }
                }
                if !successful_daemon_ports.is_empty() {
                    daemon_status_msg = format!("Running on port(s): {}", successful_daemon_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "));
                } else {
                    daemon_status_msg = "Failed to start any daemon instances.".to_string();
                }
            }

            if actual_start_rest {
                let rest_port_to_use = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
                if rest_port_to_use < 1024 || rest_port_to_use > 65535 {
                    eprintln!("Invalid REST API port: {}. Must be between 1024 and 65535.", rest_port_to_use);
                    rest_api_status_msg = format!("Invalid port: {}", rest_port_to_use);
                } else {
                    println!("Starting REST API server on port {}...", rest_port_to_use);
                    let current_storage_config = LibStorageConfig::default(); // Recheck if this is needed here.
                    let lib_storage_engine_type = current_storage_config.engine_type;
                    start_daemon_process(
                        true, // is_rest_api
                        false, // is_storage_daemon
                        Some(rest_port_to_use),
                        Some(PathBuf::from(current_storage_config.data_path)), // Ensure these paths are correct
                        Some(lib_storage_engine_type.into()), // Ensure this is correct
                    ).await?;
                    rest_api_status_msg = format!("Running on port: {}", rest_port_to_use);
                }
            }

            if actual_start_storage {
                let storage_port_to_use = storage_port.unwrap_or(config.default_port);
                let cluster_ports = if let Some(ref cluster_range) = cluster {
                    parse_cluster_range(cluster_range).unwrap_or_default()
                } else {
                    parse_cluster_range(&config.cluster_range).unwrap_or_default()
                };
                let all_ports = if cluster_ports.is_empty() {
                    vec![storage_port_to_use]
                } else {
                    cluster_ports
                };

                for &port in &all_ports {
                    if port < 1024 || port > 65535 {
                        eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", port);
                        storage_status_msg = format!("Invalid port: {}", port);
                        continue;
                    }
                    println!("Starting Storage daemon on port {}...", port);
                    let actual_storage_config_path = storage_config_file.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH));
                    start_storage_interactive(
                        Some(port),
                        Some(actual_storage_config_path),
                        cluster.clone(), // This is the cluster string from StartArgs::All
                        data_directory.as_ref().map(PathBuf::from),
                        log_directory.as_ref().map(PathBuf::from),
                        max_disk_space_gb,
                        min_disk_space_gb,
                        use_raft_for_scale,
                        storage_engine_type.clone(),
                        storage_daemon_shutdown_tx_opt.clone(),
                        storage_daemon_handle.clone(),
                        storage_daemon_port_arc.clone(),
                    ).await?;

                    let health_check_timeout = Duration::from_secs(10);
                    let poll_interval = Duration::from_millis(500);
                    let start_time = Instant::now();

                    while start_time.elapsed() < health_check_timeout {
                        if check_process_status_by_port("Storage Daemon", port).await {
                            println!("Storage daemon on port {} responded to health check.", port);
                            storage_status_msg = format!("Running on port(s): {}", all_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "));
                            break;
                        }
                        tokio::time::sleep(poll_interval).await;
                    }

                    if start_time.elapsed() >= health_check_timeout {
                        eprintln!("Warning: Storage daemon daemonized but did not become reachable on port {} within {:?}", port, health_check_timeout);
                        storage_status_msg = format!("Daemonized but failed to become reachable on port {}", port);
                    }
                }
            }
        }
        Some(StartAction::Daemon { port, cluster, daemon, rest: _, storage: _ }) => {
            if daemon.unwrap_or(true) {
                let daemon_ports_to_start = if let Some(cluster_range_str) = cluster.clone() {
                    parse_cluster_range(&cluster_range_str).unwrap_or_default()
                } else {
                    vec![port.unwrap_or(DEFAULT_DAEMON_PORT)]
                };

                let mut successful_daemon_ports = Vec::new();
                for &p in &daemon_ports_to_start {
                    println!("Attempting to start main GraphDB Daemon on port {}...", p);
                    // Call start_daemon_instance_interactive for each port
                    // The `check_port_conflicts` inside `start_daemon_instance_interactive`
                    // is now more lenient for other GraphDB daemons.
                    let daemon_instance_result = start_daemon_instance_interactive(Some(p), None, daemon_handles.clone()).await; // Pass None for cluster to avoid recursion
                    match daemon_instance_result {
                        Ok(()) => {
                            successful_daemon_ports.push(p);
                            println!("GraphDB Daemon launched on port {}. It should be running in the background.", p);
                        },
                        Err(e) => {
                            eprintln!("Failed to start GraphDB Daemon on port {}: {:?}", p, e);
                        }
                    }
                }
                if !successful_daemon_ports.is_empty() {
                    daemon_status_msg = format!("Running on port(s): {}", successful_daemon_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "));
                } else {
                    daemon_status_msg = "Failed to start any daemon instances.".to_string();
                }
            }
        }
        Some(StartAction::Rest { port, cluster: _, daemon: _, storage: _ }) => {
            let rest_port_to_use = port.unwrap_or(DEFAULT_REST_API_PORT);
            if rest_port_to_use < 1024 || rest_port_to_use > 65535 {
                eprintln!("Invalid REST API port: {}. Must be between 1024 and 65535.", rest_port_to_use);
                rest_api_status_msg = format!("Invalid port: {}", rest_port_to_use);
            } else {
                println!("Starting REST API server on port {}...", rest_port_to_use);
                let current_storage_config = LibStorageConfig::default();
                let lib_storage_engine_type = current_storage_config.engine_type;
                start_daemon_process(
                    true, // is_rest_api
                    false, // is_storage_daemon
                    Some(rest_port_to_use),
                    Some(PathBuf::from(current_storage_config.data_path)),
                    Some(lib_storage_engine_type.into()),
                ).await?;
                rest_api_status_msg = format!("Running on port: {}", rest_port_to_use);
            }
        }
        Some(StartAction::Storage {
            port,
            config_file,
            cluster,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon: _, // Ignored as per original code for this action
            rest: _, // Ignored as per original code for this action
        }) => {
            let storage_port_to_use = port.unwrap_or(config.default_port);
            if storage_port_to_use < 1024 || storage_port_to_use > 65535 {
                eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", storage_port_to_use);
                storage_status_msg = format!("Invalid port: {}", storage_port_to_use);
            } else {
                println!("Starting Storage daemon on port {}...", storage_port_to_use);
                start_storage_interactive(
                    port,
                    config_file,
                    cluster.clone(),
                    data_directory.map(PathBuf::from),
                    log_directory.map(PathBuf::from),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
                storage_status_msg = format!("Running on port: {}", storage_port_to_use);
            }
        }
        None => { // This case directly calls `handle_start_all_interactive`
            // Ensure `handle_start_all_interactive` itself does NOT perform cleanup,
            // relying on the global cleanup at the top of `handle_start_command`.
            handle_start_all_interactive(
                start_args.port,
                start_args.cluster,
                start_args.listen_port,
                start_args.storage_port,
                start_args.storage_config_file,
                start_args.data_directory,
                start_args.log_directory,
                start_args.max_disk_space_gb,
                start_args.min_disk_space_gb,
                start_args.use_raft_for_scale,
                start_args.storage_engine_type,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
            daemon_status_msg = "Running".to_string();
            rest_api_status_msg = "Running".to_string();
            storage_status_msg = "Running".to_string();
        }
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

pub async fn handle_stop_command(stop_args: StopArgs) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
                .parent()
                .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
                .to_path_buf(),
        });
    match stop_args.action {
        StopAction::Rest { port } => {
            let running_rest_ports = if let Some(p) = port {
                vec![p]
            } else {
                find_all_running_rest_api_ports().await
            };
            if running_rest_ports.is_empty() {
                println!("No REST API servers found running.");
            } else {
                for &port in &running_rest_ports {
                    stop_process_by_port("REST API", port).await?;
                    println!("REST API stop command processed for port {}.", port);
                }
            }
        }
        StopAction::Daemon { port } => {
            let p = port.unwrap_or(DEFAULT_DAEMON_PORT);
            stop_process_by_port("GraphDB Daemon", p).await?;
            println!("GraphDB Daemon stop command processed for port {}.", p);
        }
        StopAction::Storage { port } => {
            let ports_to_stop = if let Some(p) = port {
                vec![p]
            } else {
                let mut ports = vec![config.default_port];
                ports.extend(parse_cluster_range(&config.cluster_range).unwrap_or_default());
                ports
            };
            for &p in &ports_to_stop {
                stop_process_by_port("Storage Daemon", p).await?;
                println!("Storage daemon stop command processed for port {}.", p);
            }
        }
        StopAction::All => {
            println!("Attempting to stop all GraphDB components...");
            let running_rest_ports = find_all_running_rest_api_ports().await;
            for &port in &running_rest_ports {
                stop_process_by_port("REST API", port).await?;
                println!("REST API server on port {} stopped.", port);
            }

            let storage_ports = find_all_running_storage_daemon_ports().await;
            for &port in &storage_ports {
                stop_process_by_port("Storage Daemon", port).await?;
                println!("Storage daemon stop command processed for port {}.", port);
            }

            let daemon_ports = find_all_running_daemon_ports().await;
            for &port in &daemon_ports {
                stop_process_by_port("GraphDB Daemon", port).await?;
                println!("GraphDB Daemon stop command processed for port {}.", port);
            }
        }
        StopAction::Cluster => {
            println!("Stopping cluster configuration...");
            let cluster_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();
            for &port in &cluster_ports {
                stop_process_by_port("Storage Daemon", port).await?;
                println!("Cluster storage daemon stopped on port {}.", port);
            }
        }
    }
    Ok(())
}

// Placeholder for `make_rest_api_request` for version, health, query, etc.
pub async fn make_rest_api_request(port: u16, endpoint: &str, body: Option<&str>) -> Result<String> {
    println!("(Placeholder) Making request to REST API on port {} at endpoint '{}' with body: {:?}", port, endpoint, body);
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;
    Ok(format!("Response from {}: {}", endpoint, "Some data"))
}

/// Handles CLI commands related to the REST API daemon (start, stop, status).
pub async fn handle_rest_command(
    command: RestCliCommand,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>, // Consider if this is needed for REST API commands
) -> Result<()> {
    let cli_config_from_file = crate::cli::config::load_cli_config()
        .unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load CLI config: {}. Using default REST API port.", e);
            CliConfig {
                server: ServerConfig {
                    port: Some(get_default_rest_port_from_config()),
                    host: None,
                },
                // FIX: Corrected the type from `CliTomlStorageConfig` to `CliStorageConfig`.
                // This `CliStorageConfig` should be in scope from `crate::cli::config`.
                // Added explicit default initialization for other CliConfig fields to ensure completeness.
                app: Default::default(),
                rest: Default::default(),
                daemon: Default::default(),
                storage: Some(CliStorageConfig::default()), // Fix: Wrapped in Some() and used CliStorageConfig
                log: Default::default(), // Added missing fields
                paths: Default::default(), // Added missing fields
                security: Default::default(), // Added missing fields
                deployment: Default::default(), // Added missing fields
            }
        });

    let default_rest_port = cli_config_from_file.server.port.unwrap_or_else(|| {
        get_default_rest_port_from_config()
    });

    match command {
        RestCliCommand::Start { port } => {
            let target_port = port.unwrap_or(default_rest_port);
            println!("Starting REST API daemon on port {}...", target_port);
            // Swapped the order of `_rest_api_handle` and `_rest_api_port_arc`
            // in the `start_rest_api_interactive` call to match the expected argument types.
            start_rest_api_interactive( // Changed from start_rest_api_interactive to start_rest_api_interactive
                Some(target_port),
                _rest_api_shutdown_tx_opt,
                _rest_api_port_arc, // Now in the position expecting u16
                _rest_api_handle,    // Now in the position expecting JoinHandle<()>
            ).await?;
            println!("REST API start command executed (placeholder). Actual daemon launch logic is TBD.");
            Ok(())
        }
        RestCliCommand::Stop { port } => {
            let target_port = port.unwrap_or(default_rest_port);
            // Assuming send_stop_signal_to_rest_api now takes a port directly
            send_stop_signal_to_rest_api(target_port).await
        }
        RestCliCommand::Status { port } => {
            let target_port = port.unwrap_or(default_rest_port);
            if is_rest_api_running(target_port).await {
                println!("REST API daemon is RUNNING on port {}", target_port);
            } else {
                println!("REST API daemon is NOT running on port {}", target_port);
            }
            Ok(())
        }
        RestCliCommand::Version => {
            let target_port = default_rest_port;
            println!("Requesting REST API Version from port {}...", target_port);
            let response = rest::api::get_rest_api_version().await?; // Changed to use rest::api::get_rest_api_version
            println!("REST API Version: {}", response);
            Ok(())
        }
        RestCliCommand::Health => {
            let target_port = default_rest_port;
            println!("Requesting REST API Health status from port {}...", target_port);
            let response = rest::api::check_rest_api_status(target_port).await?; // Changed to use rest::api::check_rest_api_status
            println!("REST API Health: {}", response);
            Ok(())
        }
        RestCliCommand::Query { port, query } => {
            let target_port = port.unwrap_or(default_rest_port);
            println!("Executing query '{}' against REST API on port {}...", query, target_port);
            let response = rest::api::execute_graph_query(&query, None).await?; // Changed to use rest::api::execute_graph_query
            println!("Query Result: {}", response);
            Ok(())
        }
        RestCliCommand::RegisterUser { username, password } => {
            let target_port = default_rest_port;
            println!("Registering user '{}' via REST API on port {}...", username, target_port);
            rest::api::register_user(&username, &password).await?; // Changed to use rest::api::register_user
            println!("User Registration Result: Success"); // Simplified output
            Ok(())
        }
        RestCliCommand::Authenticate { username, password } => {
            let target_port = default_rest_port;
            println!("Authenticating user '{}' via REST API on port {}...", username, target_port);
            let token = rest::api::authenticate_user(&username, &password).await?; // Changed to use rest::api::authenticate_user
            println!("Authentication Result: Token: {}", token); // Simplified output
            Ok(())
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            let target_port = default_rest_port;
            println!("Executing Graph Query '{}' via REST API on port {}. Persist: {:?}...", query_string, target_port, persist);
            let response = rest::api::execute_graph_query(&query_string, persist).await?; // Changed to use rest::api::execute_graph_query
            println!("Graph Query Result: {}", response);
            Ok(())
        }
        RestCliCommand::StorageQuery => {
            let target_port = default_rest_port;
            println!("Executing Storage Query via REST API on port {}...", target_port);
            let response = storage::api::check_storage_daemon_status(target_port).await?; // Changed to use storage::api::check_storage_daemon_status
            println!("Storage Query Result: {}", response);
            Ok(())
        }
    }
}

pub async fn handle_rest_command_interactive(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port } => {
            start_rest_api_interactive(port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Stop { port } => {
            stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status { port } => {
            display_rest_api_status(rest_api_port_arc).await;
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
            execute_storage_query().await;
            Ok(())
        }
        RestCliCommand::Query { port, query } => {
            rest::api::execute_graph_query(&query, None).await?;
            Ok(())
        }
    }
}

/// Handles CLI commands related to the **Main** Daemon process (start, stop, status, restart, reload, list, clear-all).
/// This function should NOT manage storage or REST API daemons directly.
pub async fn handle_daemon_command(
    command: DaemonCliCommand,
    // The following parameters are likely passed down from a higher-level handler
    // that manages all daemon types. For handle_daemon_command, only daemon_handles
    // is directly relevant for its core responsibilities.
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    // Removed storage-specific parameters from the signature as they are not used here
    // storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    // storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    // storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    // Load config for default daemon port
    let cli_config_from_file = crate::cli::config::load_cli_config()
        .unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load CLI config: {}. Using default daemon port.", e);
            CliConfig::default() // FIX 1: CliConfig::default() now works after #[derive(Default)]
        });

    let default_daemon_port = cli_config_from_file.server.port.unwrap_or(DEFAULT_DAEMON_PORT);

    match command {
        DaemonCliCommand::Start { port, cluster } => {
            println!("{}", style("Starting main daemon service...").yellow());
            let ports = if let Some(ref cluster_range) = cluster {
                parse_cluster_range(cluster_range).unwrap_or_default()
            } else {
                vec![port.unwrap_or(default_daemon_port)]
            };

            for p in ports {
                println!("Starting main daemon on port {}...", p);
                // FIX 2: Added "main" as the service_type argument
                // We're now relying on start_daemon_instance_interactive to handle the check_port_conflicts
                // within its own logic, which has been updated to be more lenient for cluster daemons.
                start_daemon_instance_interactive(Some(p), cluster.clone(), daemon_handles.clone()).await?;
            }
            println!("{}", style("Main daemon start command executed.").yellow());
            Ok(())
        }
        // FIX: Changed to struct variant pattern { port: name }
        DaemonCliCommand::Stop { port } => {
            println!("{}", style("Stopping main daemon service...").yellow());
            let target_port = port.unwrap_or(default_daemon_port);
            send_stop_signal_to_daemon_main(target_port).await?; // Use main-daemon specific stop
            stop_daemon_instance_interactive(Some(target_port), daemon_handles).await?; // Assuming this sends a more definitive stop signal or waits for exit
            println!("{}", style("Main daemon stop command executed.").yellow());
            Ok(())
        }
        // FIX: Changed to struct variant pattern { port: name }
        DaemonCliCommand::Status { port } => {
            let target_port = port.unwrap_or(default_daemon_port);
            // FIX 3: Added "main" as the service_type argument
            if is_daemon_running(target_port, "main").await {
                println!("Main daemon is {} on port {}", target_port, style("RUNNING").green());
            } else {
                println!("Main daemon is {} on port {}", target_port, style("NOT running").red());
            }
            Ok(())
        }
        // FIX: Removed fields that DaemonCliCommand::Restart does not have
        DaemonCliCommand::Restart { port, cluster } => {
            println!("{}", style("Restarting main daemon service...").yellow());
            let target_port = port.unwrap_or(default_daemon_port);

            println!("{} {}", style("Stopping main daemon on port {}...").blue(), target_port);
            // It's good practice to try stopping even if it might not be running
            let _ = send_stop_signal_to_daemon_main(target_port).await; // Ignore error if already stopped
            stop_daemon_instance_interactive(Some(target_port), daemon_handles.clone()).await?;
            tokio::time::sleep(Duration::from_secs(1)).await; // Small delay for graceful shutdown

            println!("{} {}", style("Starting main daemon on port {}...").green(), target_port);
            start_daemon_instance_interactive(Some(target_port), cluster, daemon_handles.clone()).await?;

            println!("{}", style("Main daemon restarted successfully.").yellow());
            Ok(())
        }
        DaemonCliCommand::Reload { port: _, cluster: _ } => {
            println!("Reload command for main daemon is not yet implemented.");
            Ok(())
        }
        DaemonCliCommand::List => {
            let handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No main daemons are currently managed by this CLI instance.");
                return Ok(());
            }
            println!("Main daemons managed by this CLI instance:");
            for port in handles.keys() {
                println!("- Main Daemon on port {}", port);
            }
            Ok(())
        }
        DaemonCliCommand::ClearAll => {
            stop_daemon_instance_interactive(None, daemon_handles).await?;
            println!("Attempting to clear all external main daemon processes...");
            // You might need a function here to find and kill any lingering main daemon processes
            // e.g., `crate::cli::daemon_management::clear_all_daemon_processes().await?;`
            Ok(())
        }
    }
}

// --- Your existing handle_daemon_command_interactive (updated for previous fixes) ---
pub async fn handle_daemon_command_interactive(
    command: DaemonCliCommand,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match command {
        DaemonCliCommand::Start { port, cluster } => {
            let ports = if let Some(ref cluster_range) = cluster {
                parse_cluster_range(cluster_range).unwrap_or_default()
            } else {
                vec![port.unwrap_or(DEFAULT_DAEMON_PORT)]
            };
            for p in ports {
                // The `check_port_conflicts` inside `start_daemon_instance_interactive`
                // is now more lenient for other GraphDB daemons.
                start_daemon_instance_interactive(Some(p), cluster.clone(), daemon_handles.clone()).await?;
            }
            Ok(())
        }
        DaemonCliCommand::Stop { port } => {
            stop_daemon_instance_interactive(port, daemon_handles).await
        }
        DaemonCliCommand::Status { port } => {
            display_daemon_status(port).await;
            Ok(())
        }
        // This is the correct place for `daemon restart` if it's a subcommand of `daemon`
        // It only takes `port` and `cluster` as per DaemonCliCommand::Restart definition.
        DaemonCliCommand::Restart { port, cluster } => {
            println!("Attempting to restart main daemon(s) via 'daemon restart' command...");
            let stop_result = stop_daemon_instance_interactive(port, daemon_handles.clone()).await;
            if stop_result.is_err() {
                eprintln!("Warning: Failed to stop daemon during restart process: {:?}", stop_result.err());
            }

            let ports = if let Some(ref cluster_range) = cluster {
                parse_cluster_range(cluster_range).unwrap_or_default()
            } else {
                vec![port.unwrap_or(DEFAULT_DAEMON_PORT)]
            };
            for p in ports {
                println!("Starting main daemon on port {} after restart attempt...", p);
                // The `check_port_conflicts` inside `start_daemon_instance_interactive`
                // is now more lenient for other GraphDB daemons.
                start_daemon_instance_interactive(Some(p), cluster.clone(), daemon_handles.clone()).await?;
            }
            println!("Main daemon restart command executed.");
            Ok(())
        }
        // This is the correct place for `daemon reload` if it's a subcommand of `daemon`
        DaemonCliCommand::Reload { port, cluster: _cluster } => {
            let target_port_msg = if let Some(p) = port {
                format!(" on port {}", p)
            } else {
                " (all managed daemons)".to_string()
            };
            println!("Attempting to reload main daemon configuration{}. (Functionality TBD)", target_port_msg);
            Ok(())
        }
        DaemonCliCommand::List => {
            let handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No daemons are currently managed by this CLI instance.");
                return Ok(());
            }
            println!("Daemons managed by this CLI instance:");
            for port in handles.keys() {
                println!("- Daemon on port {}", port);
            }
            Ok(())
        }
        DaemonCliCommand::ClearAll => {
            stop_daemon_instance_interactive(None, daemon_handles).await?;
            println!("Attempting to clear all external daemon processes...");
            crate::cli::daemon_management::clear_all_daemon_processes().await?;
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_storage_command_interactive( // Renamed the function here
    storage_action: StorageAction,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    // Explicitly clone `daemon_handles` at the start of the function.
    let daemon_handles_cloned = daemon_handles.clone();

    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: get_current_exe_path().unwrap_or_default().parent().unwrap_or(&PathBuf::from("/")).to_path_buf(), // Added the missing field with a default value
        });
    match storage_action {
        StorageAction::Start {
            port,
            config_file,
            cluster,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
        } => {
            start_storage_interactive(
                port,
                config_file,
                cluster.clone(), // Ensure cluster.clone() is used here
                data_directory.map(PathBuf::from), // Convert Option<String> to Option<PathBuf>
                log_directory.map(PathBuf::from),  // Convert Option<String> to Option<PathBuf>
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;
            if daemon.unwrap_or(false) {
                // Use `daemon_handles_cloned` here
                start_daemon_instance_interactive(None, cluster.clone(), daemon_handles_cloned).await?; // Pass cluster.clone() as the second argument
            }
            if rest.unwrap_or(false) {
                start_rest_api_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_port_arc.clone(), storage_daemon_handle.clone()).await?;
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::List => {
            let running_ports = find_all_running_storage_daemon_ports().await;
            if running_ports.is_empty() {
                println!("No storage daemons are currently running.");
            } else {
                println!("Running storage daemons:");
                for port in running_ports {
                    println!("- Storage daemon on port {}", port);
                }
            }
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_storage_command(
    storage_action: StorageAction,
    // This `daemon_handles` is the function parameter.
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    // Explicitly clone `daemon_handles` at the start of the function.
    // This ensures that `daemon_handles_cloned` is always in scope for subsequent uses.
    let daemon_handles_cloned = daemon_handles.clone();

    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: get_current_exe_path().unwrap_or_default().parent().unwrap_or(&PathBuf::from("/")).to_path_buf(), // Added the missing field with a default value
        });
    match storage_action {
        StorageAction::Start {
            port,
            config_file,
            cluster, // `cluster` is available here as Option<String>
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
        } => {
            start_storage_interactive(
                port,
                config_file,
                cluster.clone(), // Use cluster.clone() for the cluster argument
                data_directory.map(PathBuf::from), // Convert Option<String> to Option<PathBuf>
                log_directory.map(PathBuf::from),  // Convert Option<String> to Option<PathBuf>
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;
            if daemon.unwrap_or(false) {
                // Use `daemon_handles_cloned` here instead of trying to access `daemon_handles` directly.
                // Reverted the second argument to `cluster.clone()` based on typical usage,
                // if `None` is intended, please adjust accordingly.
                start_daemon_instance_interactive(None, cluster.clone(), daemon_handles_cloned).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_port_arc.clone(), storage_daemon_handle.clone()).await?;
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::List => {
            let running_ports = find_all_running_storage_daemon_ports().await;
            if running_ports.is_empty() {
                println!("No storage daemons are currently running.");
            } else {
                println!("Running storage daemons:");
                for port in running_ports {
                    println!("- Storage daemon on port {}", port);
                }
            }
            Ok(())
        }
    }
}

pub async fn handle_status_command(status_args: StatusArgs, rest_api_port_arc: Arc<Mutex<Option<u16>>>, storage_daemon_port_arc: Arc<Mutex<Option<u16>>>) -> Result<()> {
    match status_args.action {
        StatusAction::Rest { port } => {
            let running_ports = if let Some(p) = port {
                vec![p]
            } else {
                find_all_running_rest_api_ports().await
            };
            if running_ports.is_empty() {
                println!("No REST API servers found running.");
            } else {
                for &port in &running_ports {
                    match rest::api::check_rest_api_status(port).await {
                        Ok(health) => println!("REST API Health on port {}: {}", port, health),
                        Err(e) => eprintln!("Failed to get REST API health on port {}: {}", port, e),
                    }
                }
            }
        }
        StatusAction::Daemon { port } => {
            display_daemon_status(port).await;
        }
        StatusAction::Storage { port } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
        }
        StatusAction::Cluster => {
            display_cluster_status().await;
        }
        StatusAction::All => {
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await;
        }
    }
    Ok(())
}

pub async fn handle_reload_command(
    reload_args: ReloadArgs,
) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: get_current_exe_path().unwrap_or_default().parent().unwrap_or(&PathBuf::from("/")).to_path_buf(), // Added the missing field with a default value
        });
    match reload_args.action {
        ReloadAction::All { .. } => {
            println!("Reloading all GraphDB components...");
            handle_stop_command(StopArgs { action: StopAction::All }).await?;

            println!("Restarting all GraphDB components after reload...");
            let daemon_result = start_daemon(None, None, vec![]).await;
            match daemon_result {
                Ok(()) => println!("GraphDB Daemon(s) restarted."),
                Err(e) => eprintln!("Failed to restart daemon(s): {:?}", e),
            }

            let rest_ports_to_restart = find_all_running_rest_api_ports().await;
            for &port in &rest_ports_to_restart {
                stop_process_by_port("REST API", port).await?;
                start_daemon_process(
                    true, false, Some(port),
                    Some(PathBuf::from(LibStorageConfig::default().data_path)),
                    Some(LibStorageConfig::default().engine_type.into()),
                ).await?;
                println!("REST API server restarted on port {}.", port);
            }
            if rest_ports_to_restart.is_empty() {
                let default_rest_port = DEFAULT_REST_API_PORT;
                stop_process_by_port("REST API", default_rest_port).await?;
                start_daemon_process(
                    true, false, Some(default_rest_port),
                    Some(PathBuf::from(LibStorageConfig::default().data_path)),
                    Some(LibStorageConfig::default().engine_type.into()),
                ).await?;
                println!("REST API server restarted on default port {}.", default_rest_port);
            }

            let storage_ports_to_restart = find_all_running_storage_daemon_ports().await;
            let actual_storage_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
            for &port in &storage_ports_to_restart {
                stop_process_by_port("Storage Daemon", port).await?;
                start_daemon_process(
                    false, true, Some(port),
                    Some(actual_storage_config_path.clone()),
                    None,
                ).await?;
                println!("Storage daemon restarted on port {}.", port);
            }
            if storage_ports_to_restart.is_empty() {
                stop_process_by_port("Storage Daemon", config.default_port).await?;
                start_daemon_process(
                    false, true, Some(config.default_port),
                    Some(actual_storage_config_path),
                    None,
                ).await?;
                println!("Storage daemon restarted on default port {}.", config.default_port);
            }

            println!("All GraphDB components reloaded (stopped and restarted).");
        }
        ReloadAction::Rest { port, .. } => {
            println!("Reloading REST API server...");
            let running_ports = if let Some(p) = port {
                vec![p]
            } else {
                find_all_running_rest_api_ports().await
            };
            if running_ports.is_empty() {
                println!("No REST API servers found running to reload. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                start_daemon_process(
                    true, false, Some(DEFAULT_REST_API_PORT),
                    Some(PathBuf::from(LibStorageConfig::default().data_path)),
                    Some(LibStorageConfig::default().engine_type.into()),
                ).await?;
            } else {
                for &port in &running_ports {
                    stop_process_by_port("REST API", port).await?;
                    start_daemon_process(
                        true, false, Some(port),
                        Some(PathBuf::from(LibStorageConfig::default().data_path)),
                        Some(LibStorageConfig::default().engine_type.into()),
                    ).await?;
                    println!("REST API server reloaded on port {}.", port);
                }
            }
        }
        ReloadAction::Storage { port, config_file, .. } => {
            println!("Reloading Storage daemon...");
            let storage_ports = if let Some(p) = port {
                vec![p]
            } else {
                find_all_running_storage_daemon_ports().await
            };
            let actual_storage_config_path = config_file.unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH));
            for &port in &storage_ports {
                stop_process_by_port("Storage Daemon", port).await?;
                start_daemon_process(
                    false, true, Some(port),
                    Some(actual_storage_config_path.clone()),
                    None,
                ).await?;
                println!("Storage daemon reloaded on port {}.", port);
            }
            if storage_ports.is_empty() {
                stop_process_by_port("Storage Daemon", config.default_port).await?;
                start_daemon_process(
                    false, true, Some(config.default_port),
                    Some(actual_storage_config_path),
                    None,
                ).await?;
                println!("Storage daemon reloaded on default port {}.", config.default_port);
            }
        }
        ReloadAction::Daemon { port, .. } => {
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Reloading GraphDB daemon on port {}...", actual_port);
            stop_process_by_port("GraphDB Daemon", actual_port).await?;
            let daemon_result = start_daemon(Some(actual_port), None, vec![]).await;
            match daemon_result {
                Ok(()) => println!("Daemon on port {} reloaded (stopped and restarted).", actual_port),
                Err(e) => eprintln!("Failed to restart daemon on port {}: {:?}", actual_port, e),
            }
        }
        ReloadAction::Cluster => {
            println!("Reloading cluster configuration...");
            println!("A full cluster reload involves coordinating restarts/reloads across multiple daemon instances.");
            println!("This is a placeholder for complex cluster management logic.");
        }
    }
    Ok(())
}

pub async fn display_rest_api_health() {
    println!("Performing REST API health check...");
    let running_ports = find_all_running_rest_api_ports().await;
    if running_ports.is_empty() {
        println!("No REST API servers found to check health.");
    } else {
        for &port in &running_ports {
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
        for &port in &running_ports {
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

pub async fn start_daemon_instance_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

    println!("Attempting to start daemon on port {}...", actual_port);

    // Proactively stop any other GraphDB components on this port
    // This allows the daemon to reclaim the port if it's held by REST API or Storage Daemon
    if let Err(e) = stop_process_by_port("REST API", actual_port).await {
        eprintln!("Warning: Failed to stop 'REST API' on port {} during daemon startup: {:?}", actual_port, e);
    }
    if let Err(e) = stop_process_by_port("Storage Daemon", actual_port).await {
        eprintln!("Warning: Failed to stop 'Storage Daemon' on port {} during daemon startup: {:?}", actual_port, e);
    }
    // Give a small moment for processes to release the port
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After attempting to clear, check for conflicts from non-GraphDB processes or if cleanup failed
    if let Err(e) = check_port_conflicts(actual_port, "GraphDB Daemon", &[]).await {
        eprintln!("Port {} is still in use after cleanup attempts: {}", actual_port, e);
        return Err(anyhow::anyhow!("Port conflict on: {}", actual_port));
    }
    
    let daemon_result = start_daemon(port, cluster, vec![]).await;
    match daemon_result {
        Ok(()) => {
            println!("GraphDB Daemon launched on port {}. It should be running in the background.", actual_port);
            let mut handles = daemon_handles.lock().await;
            handles.insert(actual_port, (tokio::spawn(async {}), oneshot::channel().0));
        }
        Err(e) => {
            eprintln!("Failed to launch GraphDB Daemon on port {}: {:?}", actual_port, e);
            return Err(e.into());
        }
    }
    Ok(())
}

pub async fn stop_daemon_instance_interactive(
    port: Option<u16>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let mut handles = daemon_handles.lock().await;

    if let Some(p) = port {
        handles.remove(&p);
        println!("Attempting to stop GraphDB Daemon on port {}...", p);
        stop_process_by_port("GraphDB Daemon", p).await?;
        println!("GraphDB Daemon on port {} stopped.", p);
    } else {
        handles.clear();
        println!("Attempting to stop all GraphDB Daemon instances...");
        let daemon_ports = find_all_running_daemon_ports().await;
        for &p in &daemon_ports {
            stop_process_by_port("GraphDB Daemon", p).await?;
            println!("GraphDB Daemon on port {} stopped.", p);
        }
    }
    Ok(())
}

pub async fn start_rest_api_interactive(
    port: Option<u16>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_REST_API_PORT);

    println!("Starting REST API server on port {}...", actual_port);

    // Proactively stop any other GraphDB components on this port
    if let Err(e) = stop_process_by_port("GraphDB Daemon", actual_port).await {
        eprintln!("Warning: Failed to stop 'GraphDB Daemon' on port {} during REST API startup: {:?}", actual_port, e);
    }
    if let Err(e) = stop_process_by_port("Storage Daemon", actual_port).await {
        eprintln!("Warning: Failed to stop 'Storage Daemon' on port {} during REST API startup: {:?}", actual_port, e);
    }
    // Also stop any existing REST API on this port (for clean restart/reclaim)
    if let Err(e) = stop_process_by_port("REST API", actual_port).await {
        eprintln!("Warning: Failed to stop existing 'REST API' on port {} during REST API startup: {:?}", actual_port, e);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Err(e) = check_port_conflicts(actual_port, "REST API", &[]).await {
        eprintln!("Port {} is still in use after cleanup attempts: {}", actual_port, e);
        return Err(anyhow::anyhow!("Port conflict on: {}", actual_port));
    }
    
    let current_storage_config = LibStorageConfig::default();
    let lib_storage_engine_type = current_storage_config.engine_type;

    start_daemon_process(
        true,
        false,
        Some(actual_port),
        Some(PathBuf::from(current_storage_config.data_path)),
        Some(lib_storage_engine_type.into()),
    ).await?;

    let health_check_timeout = Duration::from_secs(10);
    let poll_interval = Duration::from_millis(500);
    let start_time = Instant::now();

    while start_time.elapsed() < health_check_timeout {
        if check_process_status_by_port("REST API", actual_port).await {
            println!("REST API server on port {} responded to health check.", actual_port);
            *rest_api_port_arc.lock().await = Some(actual_port);
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    eprintln!("Warning: REST API server launched but did not become reachable on port {} within {:?}", actual_port, health_check_timeout);
    return Err(anyhow::anyhow!("REST API server failed to become reachable on port {}", actual_port));
}

pub async fn stop_rest_api_interactive(
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let running_ports = find_all_running_rest_api_ports().await;
    if running_ports.is_empty() {
        println!("No REST API servers found running to stop.");
    } else {
        for &port in &running_ports {
            println!("Attempting to stop REST API server on port {}...", port);
            stop_process_by_port("REST API", port).await?;
            println!("REST API server on port {} stopped.", port);
        }
    }
    *rest_api_port_arc.lock().await = None;
    Ok(())
}

/// Derives the absolute path to a configuration file, assuming it's at the workspace root.
/// This is robust for development environments. For production, consider system-wide paths.
/// This function is used to locate config files given their path relative to the workspace root.
fn get_absolute_config_path(config_file_name_relative_to_workspace: &str) -> PathBuf {
    let current_crate_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")); // e.g., /path/to/graph-db-beta/server
    // Navigate up from `server/` to the workspace root `graph-db-beta/`
    let workspace_root = current_crate_manifest_dir
        .parent().expect("Failed to get workspace root from CARGO_MANIFEST_DIR"); // e.g., /path/to/graph-db-beta
    workspace_root.join(config_file_name_relative_to_workspace) // e.g., /path/to/graph-db-beta/storage_daemon_server/storage_config.yaml
}

/// Starts the storage daemon process in interactive/background mode.
/// This function aggregates all parameters and spawns the external daemon process.
/// Starts the storage daemon process in interactive/background mode.
/// This function aggregates all parameters and spawns the external daemon process.
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    cluster: Option<String>,
    data_directory: Option<PathBuf>,
    log_directory: Option<PathBuf>,
    max_disk_space_gb: Option<u64>,
    min_disk_space_gb: Option<u64>,
    use_raft_for_scale: Option<bool>,
    storage_engine_type: Option<String>,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // Keep if needed for other logic
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // Keep if needed for other logic
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    // Determine the path to the storage daemon's config file for display/context.
    // `load_storage_config` internally uses its own default if `None` is passed.
    let daemon_config_path_for_display = config_file.clone().unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent().expect("Failed to get workspace root")
            .join("storage_daemon_server")
            .join("storage_config.yaml")
    });

    // --- Load Configuration Defaults from storage_config.yaml ---
    // `load_storage_config` is synchronous, so NO .await here.
    let storage_defaults = load_storage_config(config_file.clone())
        .context(format!("Failed to load storage configuration from {}", daemon_config_path_for_display.display()))?;

    // Determine the daemon's main port (CLI arg > config default)
    let daemon_display_port = port.unwrap_or(storage_defaults.default_port);

    println!("Starting Storage daemon process with executable `{:?}` on port {}...", daemon_display_port, daemon_display_port);

    // Proactively stop any other GraphDB components on this port
    if let Err(e) = stop_process_by_port("GraphDB Daemon", daemon_display_port).await {
        eprintln!("Warning: Failed to stop 'GraphDB Daemon' on port {} during storage startup: {:?}", daemon_display_port, e);
    }
    if let Err(e) = stop_process_by_port("REST API", daemon_display_port).await {
        eprintln!("Warning: Failed to stop 'REST API' on port {} during storage startup: {:?}", daemon_display_port, e);
    }
    // Also stop any existing Storage Daemon on this port (for clean restart/reclaim)
    if let Err(e) = stop_process_by_port("Storage Daemon", daemon_display_port).await {
        eprintln!("Warning: Failed to stop existing 'Storage Daemon' on port {} during storage startup: {:?}", daemon_display_port, e);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // After attempting to clear, check for conflicts from non-GraphDB processes or if cleanup failed
    if let Err(e) = check_port_conflicts(daemon_display_port, "Storage Daemon", &[]).await {
        eprintln!("Port {} is still in use after cleanup attempts: {}", daemon_display_port, e);
        return Err(anyhow::anyhow!("Port conflict on: {}", daemon_display_port));
    }

    // --- Cluster Port Range Parsing and Conflict Checks ---
    let mut final_cluster_range: Option<String> = None;
    let cluster_source_str = cluster.or(Some(storage_defaults.cluster_range.clone()));

    if let Some(cluster_arg_str) = cluster_source_str {
        let parts: Vec<&str> = cluster_arg_str.split('-').collect();
        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                if start <= end {
                    // This is the specific logic that causes the "port conflicts" error.
                    // If you want to allow the daemon's main port to be within the cluster range
                    // when no actual cluster is deployed, you would modify or remove this `if` block.
                    if daemon_display_port >= start && daemon_display_port <= end {
                        // FIX: Return an Err instead of Ok(()) for proper error propagation
                        return Err(anyhow::anyhow!(
                            "Port {} conflicts with the specified cluster range {}-{}. Please choose a different port or cluster range.",
                            daemon_display_port, start, end
                        ));
                    }
                    final_cluster_range = Some(cluster_arg_str.clone());
                } else {
                    // FIX: Return an Err instead of Ok(())
                    return Err(anyhow::anyhow!(
                        "Invalid cluster range '{}'. Start port ({}) must be less than or equal to end port ({}).",
                        cluster_arg_str, start, end
                    ));
                }
            } else {
                // FIX: Return an Err instead of Ok(())
                return Err(anyhow::anyhow!(
                    "Invalid cluster port format in '{}'. Expected numeric values (e.g., 9001-9003).",
                    cluster_arg_str
                ));
            }
        } else {
            // FIX: Return an Err instead of Ok(())
            return Err(anyhow::anyhow!("Invalid cluster format '{}'. Expected 'start_port-end_port' (e.g., 9001-9003).", cluster_arg_str));
        }
    }
    // --- End Cluster Handling ---

    // --- Resolve all daemon configuration parameters ---
    let final_data_directory = data_directory
        .unwrap_or_else(|| PathBuf::from(storage_defaults.data_directory.clone()));

    let final_log_directory = log_directory
        .unwrap_or_else(|| PathBuf::from(storage_defaults.log_directory.clone()));

    let final_max_disk_space_gb = max_disk_space_gb
        .unwrap_or(storage_defaults.max_disk_space_gb);

    let final_min_disk_space_gb = min_disk_space_gb
        .unwrap_or(storage_defaults.min_disk_space_gb);

    let final_use_raft_for_scale = use_raft_for_scale
        .unwrap_or(storage_defaults.use_raft_for_scale);

    let final_max_open_files = storage_defaults.max_open_files;

    let final_storage_engine_type_str = storage_engine_type
        .unwrap_or_else(|| storage_defaults.storage_engine_type.clone());

    // CORRECTED LINE: Use CliStorageEngineType and handle the Result
    let _parsed_engine_type = CliStorageEngineType::from_str(&final_storage_engine_type_str)
        .context(format!("Failed to parse storage engine type '{}'", final_storage_engine_type_str))?;

    // --- Determine the daemon executable path ---
    let daemon_exe_path = {
        let current_crate_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = current_crate_manifest_dir
            .parent().expect("Failed to get workspace root directory");

        #[cfg(target_family = "unix")]
        let system_bin_path = PathBuf::from("/usr/local/bin").join("storage_daemon_server");
        #[cfg(target_family = "windows")]
        let system_bin_path = PathBuf::from("C:\\Program Files\\GraphDB\\bin").join("storage_daemon_server.exe");
        #[cfg(not(any(target_family = "unix", target_family = "windows")))]
        let system_bin_path = PathBuf::from("/opt/graphdb/bin").join("storage_daemon_server");

        let dev_build_path = workspace_root
            .join("target")
            .join("debug")
            .join("storage_daemon_server");

        if system_bin_path.exists() {
            system_bin_path
        } else {
            dev_build_path
        }
    };

    if !daemon_exe_path.exists() {
        let current_crate_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = current_crate_manifest_dir
            .parent().expect("Failed to get workspace root directory");

        #[cfg(target_family = "unix")]
        let system_bin_path_display = PathBuf::from("/usr/local/bin").join("storage_daemon_server");
        #[cfg(target_family = "windows")]
        let system_bin_path_display = PathBuf::from("C:\\Program Files\\GraphDB\\bin").join("storage_daemon_server.exe");
        #[cfg(not(any(target_family = "unix", target_family = "windows")))]
        let system_bin_path_display = PathBuf::from("/opt/graphdb/bin").join("storage_daemon_server");

        let dev_build_path_display = workspace_root.join("target").join("debug").join("storage_daemon_server");

        // FIX: Return an Err instead of Ok(())
        return Err(anyhow::anyhow!(
            "Storage daemon executable not found. Tried the following paths:\n  - {}\n  - {}\nPlease ensure it is built by running `cargo build` in your `storage_daemon_server` crate or installed to a system path.",
            system_bin_path_display.display(),
            dev_build_path_display.display()
        ));
    }

    // Ensure log directory exists (using final_log_directory now)
    if !final_log_directory.exists() {
        fs::create_dir_all(&final_log_directory).context(format!("Failed to create log directory {:?}", final_log_directory))?;
    }
    let log_file_path = final_log_directory.join("storage_daemon.log");
    let daemon_log_file = fs::File::create(&log_file_path)
        .context(format!("Failed to create log file at {:?}", log_file_path))?;


    // Spawn the external daemon process
    let mut command = TokioCommand::new(&daemon_exe_path);

    // Pass all resolved configuration as command-line arguments to the daemon executable.
    command.arg("--port").arg(daemon_display_port.to_string());
    command.arg("--daemon"); // Indicates it should run in background

    command.arg("--data-directory").arg(final_data_directory.to_string_lossy().into_owned());
    command.arg("--log-directory").arg(final_log_directory.to_string_lossy().into_owned());
    command.arg("--max-disk-space-gb").arg(final_max_disk_space_gb.to_string());
    command.arg("--min-disk_space_gb").arg(final_min_disk_space_gb.to_string());
    command.arg("--max-open-files").arg(final_max_open_files.to_string());
    command.arg("--storage-engine-type").arg(final_storage_engine_type_str);

    if final_use_raft_for_scale {
        command.arg("--use-raft-for-scale"); // Flag argument
    }

    if let Some(cluster_val) = final_cluster_range {
        command.arg("--cluster").arg(cluster_val);
    }

    // Redirect standard I/O to a log file or null to daemonize the process
    command.stdin(Stdio::null());
    command.stdout(
        daemon_log_file.try_clone()
            .map(|file| Stdio::from(file))
            .unwrap_or_else(|_| Stdio::null())
    );
    command.stderr(Stdio::from(daemon_log_file));


    match command.spawn() {
        Ok(child) => {
            println!("[CLI Background] Storage daemon spawned (PID: {}). Output redirected to {:?}", child.id().unwrap_or(0), log_file_path);
        },
        Err(e) => {
            eprintln!("[CLI Background] Failed to spawn storage daemon process: {}. Ensure executable exists and has permissions.", e);
        }
    }

    // Store the requested port for CLI's internal state
    let mut port_lock = storage_daemon_port_arc.lock().await;
    *port_lock = Some(daemon_display_port);

    println!("Storage daemon launched in background on port {}. Its output is now redirected. Control returned to CLI.", daemon_display_port);
    Ok(())
}

// --- Utility functions (like display_process_table) remain the same ---
/// Displays a formatted table of running processes based on `sysinfo`.
pub fn display_process_table() {
    let mut sys = System::new_all();
    sys.refresh_processes(ProcessesToUpdate::All);

    println!("\n{}", style::style("--- Running Processes ---").bold().cyan());
    println!("{}", style::style(format!(
        "{:<8} {:<20} {:<10} {:<15} {:<10}",
        "PID", "Name", "CPU %", "Memory (MB)", "Status"
    )).bold().blue());

    // Assuming terminal_size() and style::style are imported correctly
    use crossterm::terminal::size as terminal_size; // Corrected import path for terminal_size
    use crossterm::style; // Corrected import path for style (assuming you use crossterm for styling)
    use std::borrow::Cow; // Needed for OsStrExt::to_string_lossy

    let (width, _height) = terminal_size().unwrap_or((80, 24)); // Get terminal width

    for (pid, process) in sys.processes() {
        let name = process.name(); // This is &OsStr
        let cpu_usage = process.cpu_usage();
        let memory_usage_mb = process.memory() / 1024; // Convert KB to MB
        let status = process.status().to_string();

        // Convert &OsStr to Cow<str> for display and string manipulation
        let name_lossy = name.to_string_lossy();

        let truncated_name = if name_lossy.len() > 18 {
            // Use &*name_lossy to get a &str reference from Cow for slicing/formatting
            format!("{:.18}..", &*name_lossy)
        } else {
            name_lossy.into_owned() // Convert Cow<str> to owned String
        };

        // Attempt to fit line within terminal width
        let line = format!(
            "{:<8} {:<20} {:<10.2} {:<15} {:<10}",
            pid, truncated_name, cpu_usage, memory_usage_mb, status
        );

        if line.len() as u16 > width {
            // If line is too long, truncate it
            let max_len = width as usize - 3; // -3 for "..."
            // Corrected: Slice the string directly to the calculated max_len
            // Ensure max_len doesn't exceed line.len() to prevent panic
            let actual_max_len = std::cmp::min(max_len, line.len());
            println!("{:.max_len$}...", &line[..actual_max_len], max_len = actual_max_len);
        } else {
            println!("{}", line);
        }
    }
    println!("{}", style::style("-------------------------").bold().cyan());
}

pub async fn stop_storage_interactive(
    port: Option<u16>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
                .parent()
                .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
                .to_path_buf(),
        });

    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        let mut ports = vec![config.default_port];
        ports.extend(parse_cluster_range(&config.cluster_range).unwrap_or_default());
        ports
    };

    for &p in &ports_to_stop {
        println!("Attempting to stop Storage daemon on port {}...", p);
        stop_process_by_port("Storage Daemon", p).await?;
        println!("Storage daemon on port {} stopped.", p);
    }

    *storage_daemon_port_arc.lock().await = None;
    Ok(())
}

pub async fn stop_all_interactive(
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    println!("Stopping all GraphDB components...");
    
    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;
    stop_storage_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;

    println!("All GraphDB components stop commands processed.");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_start_all_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    data_directory: Option<String>,
    log_directory: Option<String>,
    max_disk_space_gb: Option<u64>,
    min_disk_space_gb: Option<u64>,
    use_raft_for_scale: Option<bool>,
    storage_engine_type: Option<String>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
                .parent()
                .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
                .to_path_buf(),
        });

    let mut daemon_status_msg = "Not started".to_string();
    let mut rest_api_status_msg = "Not started".to_string();
    let mut storage_status_msg = "Not started".to_string();

    // Start daemon
    let daemon_result = start_daemon_instance_interactive(port, cluster.clone(), daemon_handles.clone()).await;
    match daemon_result {
        Ok(()) => {
            if let Some(ref cluster_range) = cluster {
                daemon_status_msg = format!("Running on cluster ports: {}", cluster_range);
            } else if let Some(p) = port {
                daemon_status_msg = format!("Running on port: {}", p);
            } else {
                daemon_status_msg = format!("Running on default port: {}", DEFAULT_DAEMON_PORT);
            }
        }
        Err(e) => {
            eprintln!("Failed to start daemon: {:?}", e);
            daemon_status_msg = format!("Failed to start ({:?})", e);
        }
    }

    // Start REST API
    let rest_port_to_use = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
    if rest_port_to_use < 1024 || rest_port_to_use > 65535 {
        eprintln!("Invalid REST API port: {}. Must be between 1024 and 65535.", rest_port_to_use);
        rest_api_status_msg = format!("Invalid port: {}", rest_port_to_use);
    } else if let Err(e) = check_port_conflicts(rest_port_to_use, "REST API", &[]).await {
        rest_api_status_msg = format!("Port conflict on: {}: {}", rest_port_to_use, e);
    } else {
        let rest_result = start_rest_api_interactive(
            Some(rest_port_to_use),
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
        ).await;
        match rest_result {
            Ok(()) => rest_api_status_msg = format!("Running on port: {}", rest_port_to_use),
            Err(e) => {
                eprintln!("Failed to start REST API: {:?}", e);
                rest_api_status_msg = format!("Failed to start ({:?})", e);
            }
        }
    }

    // Start storage daemon
    let storage_port_to_use = storage_port.unwrap_or(config.default_port);
    let cluster_ports = if let Some(ref cluster_range) = cluster {
        parse_cluster_range(cluster_range).unwrap_or_default()
    } else {
        parse_cluster_range(&config.cluster_range).unwrap_or_default()
    };
    let all_ports = if cluster_ports.is_empty() {
        vec![storage_port_to_use]
    } else {
        cluster_ports
    };

    for &port in &all_ports {
        if port < 1024 || port > 65535 {
            eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", port);
            storage_status_msg = format!("Invalid port: {}", port);
            continue;
        }
        if let Err(e) = check_port_conflicts(port, "Storage Daemon", &[]).await {
            storage_status_msg = format!("Port conflict on: {}: {}", port, e);
        } else {
            let storage_result = start_storage_interactive(
                Some(port),
                storage_config_file.clone(),
                cluster.clone(),
                data_directory.as_ref().map(PathBuf::from),
                log_directory.as_ref().map(PathBuf::from),
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await;

            match storage_result {
                Ok(()) => storage_status_msg = format!("Running on port(s): {}", all_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")),
                Err(e) => {
                    eprintln!("Failed to start storage daemon on port {}: {:?}", port, e);
                    storage_status_msg = format!("Failed to start on port {} ({:?})", port, e);
                }
            }
        }
    }

    println!("\n--- Component Startup Summary ---");
    println!("{:<15} {:<50}", "Component", "Status");
    println!("{:-<15} {:-<50}", "", "");
    println!("{:<15} {:<50}", "GraphDB Daemon", daemon_status_msg);
    println!("{:<15} {:<50}", "REST API", rest_api_status_msg);
    println!("{:<15} {:<50}", "Storage Daemon", storage_status_msg);
    println!("---------------------------------\n");
    Ok(())
}

pub async fn handle_restart_command(
    restart_args: RestartArgs,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
                .parent()
                .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
                .to_path_buf(),
        });

    match restart_args.action {
        Some(RestartAction::All {
            port,
            cluster,
            config_file,
            listen_port,
            storage_port,
            storage_config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
            storage,
        }) => {
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

            handle_start_all_interactive(
                port,
                cluster,
                listen_port,
                storage_port,
                storage_config_file,
                data_directory,
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;

            println!("All GraphDB components restarted.");
        }
        Some(RestartAction::Daemon { port, cluster, daemon, rest, storage }) => {
            println!("Restarting GraphDB Daemon...");
            stop_daemon_instance_interactive(port, daemon_handles.clone()).await?;
            if daemon.unwrap_or(true) {
                start_daemon_instance_interactive(port, cluster.clone(), daemon_handles.clone()).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_interactive(None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            }
            if storage.unwrap_or(false) {
                start_storage_interactive(None, None, cluster, None, None, None, None, None, None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
            }
            println!("GraphDB Daemon restarted on port {}.", port.unwrap_or(DEFAULT_DAEMON_PORT));
        }
        Some(RestartAction::Rest { port, cluster, daemon, storage }) => {
            println!("Restarting REST API...");
            stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            start_rest_api_interactive(port, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            println!("REST API restarted on port {}.", port.unwrap_or(DEFAULT_REST_API_PORT));
        }
        Some(RestartAction::Storage {
            port,
            config_file,
            cluster,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
        }) => {
            println!("Restarting Storage Daemon...");
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
            start_storage_interactive(
                port,
                config_file,
                cluster.clone(),
                data_directory.as_ref().map(PathBuf::from),
                log_directory.as_ref().map(PathBuf::from),
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
            if daemon.unwrap_or(false) {
                start_daemon_instance_interactive(None, cluster.clone(), daemon_handles.clone()).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_interactive(None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            }
            println!("Storage Daemon restarted on port {}.", port.unwrap_or(config.default_port));
        }
        Some(RestartAction::Cluster) => {
            println!("Restarting cluster configuration...");
            let cluster_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();
            for &p in &cluster_ports {
                stop_storage_interactive(Some(p), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
                start_storage_interactive(
                    Some(p),
                    None,
                    Some(config.cluster_range.clone()),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
                println!("Cluster storage daemon restarted on port {}.", p);
            }
        }
        None => {
            println!("Restarting all GraphDB components (default action)...");
            stop_all_interactive(
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;
            handle_start_all_interactive(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
            println!("All GraphDB components restarted.");
        }
    }
    Ok(())
}
pub async fn handle_restart_command_interactive(
    restart_args: RestartArgs,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: 8085,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH) // Fix: Added missing field
                .parent()
                .unwrap_or_else(|| Path::new("/")) // Provide a default if parent is none
                .to_path_buf(),
        });

    match restart_args.action {
        Some(RestartAction::All {
            port,
            cluster,
            config_file,
            listen_port,
            storage_port,
            storage_config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
            storage,
        }) => {
            println!("Restarting all GraphDB components interactively...");
            stop_all_interactive(
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;
            handle_start_all_interactive(
                port,
                cluster,
                listen_port,
                storage_port,
                storage_config_file,
                data_directory,
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
            println!("All GraphDB components restarted interactively.");
        }
        Some(RestartAction::Daemon { port, cluster, daemon, rest, storage }) => {
            println!("Restarting GraphDB Daemon interactively...");
            stop_daemon_instance_interactive(port, daemon_handles.clone()).await?;
            if daemon.unwrap_or(true) {
                start_daemon_instance_interactive(port, cluster.clone(), daemon_handles.clone()).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_interactive(None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            }
            if storage.unwrap_or(false) {
                start_storage_interactive(None, None, cluster, None, None, None, None, None, None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
            }
            println!("GraphDB Daemon restarted interactively on port {}.", port.unwrap_or(DEFAULT_DAEMON_PORT));
        }
        Some(RestartAction::Rest { port, cluster, daemon, storage }) => {
            println!("Restarting REST API interactively...");
            stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            start_rest_api_interactive(port, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            println!("REST API restarted interactively on port {}.", port.unwrap_or(DEFAULT_REST_API_PORT));
        }
        Some(RestartAction::Storage {
            port,
            config_file,
            cluster,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
        }) => {
            println!("Restarting Storage Daemon interactively...");
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
            start_storage_interactive(
                port,
                config_file,
                cluster.clone(),
                data_directory.as_ref().map(PathBuf::from), // Convert Option<String> to Option<PathBuf> by borrowing
                log_directory.as_ref().map(PathBuf::from),  // Convert Option<String> to Option<PathBuf> by borrowing
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;

            if daemon.unwrap_or(false) {
                start_daemon_instance_interactive(None, cluster.clone(), daemon_handles.clone()).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_interactive(None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            }
            println!("Storage Daemon restarted interactively on port {}.", port.unwrap_or(config.default_port));
        }
        Some(RestartAction::Cluster) => {
            println!("Restarting cluster configuration interactively...");
            let cluster_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();
            for &p in &cluster_ports {
                stop_storage_interactive(Some(p), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
                start_storage_interactive(
                    Some(p),
                    None,
                    Some(config.cluster_range.clone()),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
                println!("Cluster storage daemon restarted interactively on port {}.", p);
            }
        }
        None => {
            println!("Restarting all GraphDB components interactively (default action)...");
            stop_all_interactive(
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;
            handle_start_all_interactive(
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
            println!("All GraphDB components restarted interactively.");
        }
    }
    Ok(())
}

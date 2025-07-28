// server/src/cli/handlers.rs

// This file contains the handlers for various CLI commands, encapsulating
// the logic for interacting with daemon, REST API, and storage components.

use anyhow::{Result, Context, anyhow}; // Added `anyhow` macro import
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::str::FromStr;
use sysinfo::{System, Pid}; // Corrected: Added System and Pid, PidExt
use tokio::task::JoinHandle;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{sleep, timeout};
use tokio::process::Command as TokioCommand;
use log::{info, warn, error, debug};
use std::process::Stdio;
use std::time::{Duration, Instant};
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use crossterm::cursor::MoveTo;
use std::io::{self, Write};
use futures::future; // Added for parallel execution
use reqwest::Client;

// Import command structs from commands.rs
use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, StopAction, ReloadAction, RestartArgs, RestartAction};
use lib::storage_engine::config::{ StorageConfig as LibStorageConfig, StorageEngineType };
use crate::cli::config::{ CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                          DEFAULT_DAEMON_PORT,
                          DEFAULT_REST_API_PORT,
                          DEFAULT_STORAGE_PORT,
                          MAX_CLUSTER_SIZE,
                          DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
                          DEFAULT_CONFIG_ROOT_DIRECTORY_STR,   
                          //StorageEngineType,    
                          load_storage_config,  
                          load_cli_config, 
                          get_cli_storage_default_port,
                          load_main_daemon_config}; // Alias to avoid conflict
use crate::cli::daemon_management::{
    find_all_running_rest_api_ports, find_all_running_daemon_ports, find_all_running_storage_daemon_ports,
    is_main_daemon_running, is_rest_api_running, is_storage_daemon_running, parse_cluster_range,
    find_running_storage_daemon_port, start_daemon_process, is_port_free, check_process_status_by_port,
    load_storage_config_path_or_default, stop_process_by_port, stop_daemon_api_call, spawn_rest_api_server,
    kill_daemon_process, is_daemon_running, stop_managed_daemon, list_daemon_processes, launch_daemon_process, 
    find_process_by_port, find_daemon_process_on_port_with_args, is_port_listening,
};

use daemon_api::{stop_daemon, start_daemon};
use daemon_api; 

/*
impl std::str::FromStr for crate::cli::config::StorageEngineType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Sled" => Ok(crate::cli::config::StorageEngineType::Sled),
            "RocksDB" => Ok(crate::cli::config::StorageEngineType::RocksDB),
            _ => Err(anyhow::anyhow!("Unknown storage engine type: {}", s)),
        }
    }
}
*/
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

/// Helper to run an external command with a timeout.
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

// Helper to determine daemon status details
pub async fn get_daemon_status_details(component_name: &str, port: u16, expected_internal_arg: &str) -> (String, String, String) {
    let mut status = "Down".to_string();
    let mut details = format!("No {}. Check if it's running.", component_name);
    let mut actual_port = "N/A".to_string();

    if let Some(pid) = find_daemon_process_on_port_with_args(port, component_name, expected_internal_arg).await {
        // Confirm TCP is listening, as find_daemon_process_on_port_with_args already does this.
        // We re-check or just trust the previous check, depending on desired strictness.
        if is_port_listening(port).await {
            status = "Running".to_string();
            actual_port = port.to_string();
            details = format!("Health: OK ({}); PID: {}", port, pid);
            if component_name == "GraphDB Daemon" {
                // Add specific details for GraphDB Daemon
                details = format!("Core Graph Processing; Health: OK ({}); PID: {}", port, pid);
            }
        } else {
            status = "Stale PID/Not Listening".to_string();
            details = format!("Process with PID {} found, but port {} is not listening.", pid, port);
            actual_port = port.to_string();
        }
    }
    (status, actual_port, details)
}

pub async fn display_rest_api_status(rest_api_port_arc: Arc<TokioMutex<Option<u16>>>) {
    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let config = load_storage_config_path_or_default(None).unwrap_or_default();
    let cluster = if config.cluster_range.is_empty() { None } else { Some(config.cluster_range.clone()) };
    let rest_ports = tokio::time::timeout(Duration::from_secs(2), find_all_running_rest_api_ports(rest_api_port_arc.clone(), cluster))
        .await
        .unwrap_or_default();

    if rest_ports.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No REST API servers found.");
    } else {
        let client = Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to build reqwest client");
        for port in rest_ports {
            let mut _details = String::new();
            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match tokio::time::timeout(Duration::from_secs(1), client.get(&health_url).send()).await {
                Ok(Ok(resp)) if resp.status().is_success() => {
                    _details = "Health: OK".to_string();
                    let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
                    match tokio::time::timeout(Duration::from_secs(1), client.get(&version_url).send()).await {
                        Ok(Ok(v_resp)) if v_resp.status().is_success() => {
                            let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                            let version = v_json["version"].as_str().unwrap_or("N/A");
                            _details = format!("{}; Version: {}", _details, version);
                        }
                        _ => _details = format!("{}; Version: N/A", _details),
                    }
                }
                _ => _details = "Health: Down".to_string(),
            }
            println!("{:<15} {:<10} {:<40}", "Running", port, _details);
        }
    }
    println!("--------------------------------------------------");
}

/// Displays detailed status for a specific GraphDB daemon or lists common ones.
pub async fn display_daemon_status(port: Option<u16>) {
    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let main_daemon_config = load_main_daemon_config(None)
        .unwrap_or_else(|e| {
            error!("Could not load main daemon config for status: {}. Using default daemon port.", e);
            Default::default()
        });
    let configured_cluster_ports = if main_daemon_config.cluster_range.is_empty() {
        Vec::new()
    } else {
        parse_cluster_range(&main_daemon_config.cluster_range).unwrap_or_else(|e| {
            warn!("Failed to parse main daemon cluster range '{}': {}", main_daemon_config.cluster_range, e);
            Vec::new()
        })
    };

    let mut ports_to_check: Vec<u16> = Vec::new();
    if let Some(p) = port {
        ports_to_check.push(p);
    } else {
        let mut found_running_daemons = find_all_running_daemon_ports(None).await;
        ports_to_check.append(&mut found_running_daemons);

        let mut unique_ports: std::collections::HashSet<u16> = ports_to_check.drain(..).collect();
        for p in configured_cluster_ports {
            unique_ports.insert(p);
        }
        unique_ports.insert(main_daemon_config.default_port);
        
        ports_to_check = unique_ports.into_iter().collect();
        ports_to_check.sort_unstable();
    }


    let mut found_any_running = false;
    if ports_to_check.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No daemons found on common or configured ports.");
    } else {
        for p in &ports_to_check {
            let status = if is_main_daemon_running(*p).await {
                found_any_running = true;
                "Running".to_string()
            } else {
                "Down".to_string()
            };
            let details = "Core Graph Processing".to_string();
            println!("{:<15} {:<10} {:<40}", status, p, details);
        }
    }

    if !found_any_running && port.is_none() {
        println!("\nTo check a specific daemon, use 'status daemon --port <port>'.");
    }
    println!("--------------------------------------------------");
} 

/// Displays detailed status for the standalone Storage daemon.
pub async fn display_storage_daemon_status(
    port_arg: Option<u16>,
    _storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) {
    let config_result = load_storage_config_path_or_default(None);
    let storage_config = config_result.unwrap_or_default();

    let configured_cluster_ports = if storage_config.cluster_range.is_empty() {
        Vec::new()
    } else {
        parse_cluster_range(&storage_config.cluster_range).unwrap_or_else(|e| {
            warn!("Failed to parse storage cluster range '{}': {}", storage_config.cluster_range, e);
            Vec::new()
        })
    };

    let mut ports_to_check: Vec<u16> = Vec::new();
    if let Some(p) = port_arg {
        ports_to_check.push(p);
    } else {
        let mut found_running_storages = find_all_running_storage_daemon_ports(None).await;
        ports_to_check.append(&mut found_running_storages);

        let mut unique_ports: std::collections::HashSet<u16> = ports_to_check.drain(..).collect();
        for p in configured_cluster_ports {
            unique_ports.insert(p);
        }
        unique_ports.insert(storage_config.default_port);
        
        ports_to_check = unique_ports.into_iter().collect();
        ports_to_check.sort_unstable();
    }

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let mut _found_any = false;
    if ports_to_check.is_empty() {
            println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No storage daemons found on common or configured ports.");
    } else {
        for port in ports_to_check {
            let status = if is_storage_daemon_running(port).await {
                _found_any = true;
                "Running".to_string()
            } else {
                "Down".to_string()
            };

            // Use the loaded config for details
            println!("{:<15} {:<10} {:<40}", status, port, format!("Type: {}", storage_config.storage_engine_type));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Data Dir: {}", storage_config.data_directory));
            println!("{:<15} {:<10} {:<40}", "", "", "Engine Config: None");
            println!("{:<15} {:<10} {:<40}", "", "", format!("Max Open Files: {}", storage_config.max_open_files));
            if !storage_config.cluster_range.is_empty() {
                if let Ok(cluster_ports) = parse_cluster_range(&storage_config.cluster_range) {
                    if !cluster_ports.is_empty() {
                        println!("{:<15} {:<10} {:<40}", "", "", format!("Configured Cluster: {}", cluster_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")));
                    }
                }
            } else {
                    println!("{:<15} {:<10} {:<40}", "", "", "Configured Cluster: N/A");
            }
        }
    }
    println!("--------------------------------------------------");
}


/// Displays the status of the entire cluster. (Placeholder)
pub async fn display_cluster_status() {
    println!("\n--- Cluster Status ---");
    println!("Cluster status is a placeholder. In a real implementation, this would query all daemons in the cluster.");
    println!("--------------------------------------------------");
}

/// Displays a comprehensive status summary of all GraphDB components.
pub async fn display_full_status_summary(
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) {
    let main_daemon_config = load_main_daemon_config(None)
        .unwrap_or_else(|e| {
            error!("Could not load main daemon config for summary: {}. Using default daemon port.", e);
            Default::default()
        });
    let storage_config = load_storage_config_path_or_default(None).unwrap_or_default();

    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    // GraphDB Daemon Status
    let mut daemon_ports_to_check: Vec<u16> = find_all_running_daemon_ports(None).await;
    let mut unique_daemon_ports: std::collections::HashSet<u16> = daemon_ports_to_check.drain(..).collect();
    // Add configured main daemon cluster ports
    if !main_daemon_config.cluster_range.is_empty() {
        if let Ok(cluster_ports) = parse_cluster_range(&main_daemon_config.cluster_range) {
            for p in cluster_ports {
                unique_daemon_ports.insert(p);
            }
        }
    }
    unique_daemon_ports.insert(main_daemon_config.default_port); // Always include default main daemon port
    let final_daemon_ports: Vec<u16> = unique_daemon_ports.into_iter().collect();

    let mut actual_running_daemon_ports = Vec::new();
    for port in &final_daemon_ports {
        if is_main_daemon_running(*port).await {
            actual_running_daemon_ports.push(*port);
        }
    }

    let daemon_status = if actual_running_daemon_ports.is_empty() { "Down" } else { "Running" }.to_string();
    let daemon_port_display = if actual_running_daemon_ports.is_empty() {
        "N/A".to_string()
    } else {
        actual_running_daemon_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
    };
    let daemon_details = format!("Core Graph Processing; Configured Cluster: {}",
        if main_daemon_config.cluster_range.is_empty() { "N/A".to_string() } else { main_daemon_config.cluster_range.clone() }
    );
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status, daemon_port_display, daemon_details);

    let client = Client::builder()
        .timeout(Duration::from_secs(1))
        .build()
        .expect("Failed to build reqwest client");
    
    // REST API Status
    let rest_ports = tokio::time::timeout(Duration::from_secs(2), find_all_running_rest_api_ports(rest_api_port_arc.clone(), None)) // Pass None for cluster here, as REST API cluster not yet defined in config.rs
        .await
        .unwrap_or_default();
    let mut rest_details_list = Vec::new(); // Renamed to avoid conflict with `details` variable in `display_rest_api_status` (though they are different functions)
    let mut actual_running_rest_ports = Vec::new();
    for port in rest_ports {
        let health_url = format!("http://127.00.1:{}/api/v1/health", port);
        match tokio::time::timeout(Duration::from_secs(1), client.get(&health_url).send()).await {
            Ok(Ok(resp)) if resp.status().is_success() => {
                let mut detail = format!("Health: OK ({})", port);
                let version_url = format!("http://127.0.0.1:{}/api/v1/version", port);
                match tokio::time::timeout(Duration::from_secs(1), client.get(&version_url).send()).await {
                    Ok(Ok(v_resp)) if v_resp.status().is_success() => {
                        let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                        let version = v_json["version"].as_str().unwrap_or("N/A");
                        detail = format!("{}; Version: {}", detail, version);
                    }
                    _ => detail = format!("{}; Version: N/A", detail),
                }
                rest_details_list.push(detail);
                actual_running_rest_ports.push(port);
            }
            _ => rest_details_list.push(format!("Health: Down ({})", port)),
        }
    }
    let rest_status = if actual_running_rest_ports.is_empty() { "Down" } else { "Running" }.to_string();
    let rest_port_display = if actual_running_rest_ports.is_empty() {
        "N/A".to_string()
    } else {
        actual_running_rest_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
    };
    let rest_api_details = if rest_details_list.is_empty() { "".to_string() } else { rest_details_list.join("; ") }; // Join only if not empty
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_status, rest_port_display, rest_api_details);

    // Storage Daemon Status
    let mut storage_ports_to_check: Vec<u16> = find_all_running_storage_daemon_ports(None).await;
    let mut unique_storage_ports: std::collections::HashSet<u16> = storage_ports_to_check.drain(..).collect();
    // Add configured storage cluster ports
    if !storage_config.cluster_range.is_empty() {
        if let Ok(cluster_ports) = parse_cluster_range(&storage_config.cluster_range) {
            for p in cluster_ports {
                unique_storage_ports.insert(p);
            }
        }
    }
    unique_storage_ports.insert(storage_config.default_port); // Always include default storage port
    let final_storage_ports: Vec<u16> = unique_storage_ports.into_iter().collect();

    let mut actual_running_storage_ports = Vec::new();
    for port in &final_storage_ports {
        if is_storage_daemon_running(*port).await {
            actual_running_storage_ports.push(*port);
        }
    }

    let storage_status = if actual_running_storage_ports.is_empty() { "Down" } else { "Running" }.to_string();
    let storage_port_display = if actual_running_storage_ports.is_empty() {
        "N/A".to_string()
    } else {
        actual_running_storage_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")
    };
    let storage_details = format!(
        "Type: {}; Data Dir: {}; Max Open Files: {}; Configured Cluster: {}",
        storage_config.storage_engine_type,
        storage_config.data_directory,
        storage_config.max_open_files,
        if storage_config.cluster_range.is_empty() { "N/A".to_string() } else { storage_config.cluster_range.clone() }
    );
    println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_status, storage_port_display, storage_details);

    println!("--------------------------------------------------");
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

#[allow(clippy::too_many_arguments)]
pub async fn handle_start_command(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    _config: &crate::cli::config::CliConfig,
    _daemon_handles: Arc<tokio::sync::Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    _rest_api_shutdown_tx_opt: Arc<tokio::sync::Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<tokio::sync::Mutex<Option<u16>>>,
    _rest_api_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    use crate::cli::config::load_storage_config;
    use lib::storage_engine::config::StorageEngineType;
    use anyhow::anyhow;
    use crate::cli::daemon_management::{is_rest_api_running, is_main_daemon_running, is_storage_daemon_running, start_daemon_process, parse_cluster_range, is_port_free}; // Import necessary functions
    use std::collections::HashMap; // Ensure HashMap is in scope
    use tokio::task::JoinHandle; // Ensure JoinHandle is in scope
    use tokio::sync::oneshot; // Ensure oneshot is in scope
    use std::sync::Arc; // Ensure Arc is in scope


    let mut daemon_status_msg = "Not requested".to_string();
    let mut rest_api_status_msg = "Not requested".to_string();
    let mut storage_status_msg = "Not requested".to_string();

    let start_daemon_requested = port.is_some() || cluster.is_some();
    let start_rest_requested = listen_port.is_some();
    let start_storage_requested = storage_port.is_some() || storage_config_file.is_some();
    let mut skip_ports = Vec::new();

    if let Some(rest_p) = listen_port {
        skip_ports.push(rest_p);
    }
    if let Some(storage_p) = storage_port {
        skip_ports.push(storage_p);
    }

    // 1. Start GraphDB Daemon (main)
    if start_daemon_requested {
        let ports_to_start = if let Some(cluster_range_str) = cluster.clone() {
            parse_cluster_range(&cluster_range_str)?
        } else if let Some(p) = port {
            vec![p]
        } else {
            vec![DEFAULT_DAEMON_PORT]
        };

        let mut started_ports = Vec::new();
        for p in ports_to_start {
            if skip_ports.contains(&p) {
                println!("Skipping daemon on port {} as it's reserved for another service.", p);
                continue;
            }
            if is_main_daemon_running(p).await {
                eprintln!("GraphDB Daemon on port {} is already running.", p);
                continue; // Skip if already running
            }
            if !is_port_free(p).await {
                eprintln!("Port {} is in use by another process for daemon, but it's not a GraphDB Daemon process. Cannot start.", p);
                continue;
            }

            println!("Starting daemon on port {}...", p);
            match start_daemon_process(false, false, Some(p), None, None).await {
                Ok(_) => {
                    started_ports.push(p);
                },
                Err(e) => {
                    eprintln!("Failed to start daemon on port {}: {:?}", p, e);
                }
            }
        }
        if !started_ports.is_empty() {
            daemon_status_msg = format!("Running on port(s): {}", started_ports.iter().map(|p| p.to_string()).collect::<Vec<String>>().join(", "));
        } else {
            daemon_status_msg = "Failed to start any daemon instances or already running.".to_string();
        }
    }

    // 2. Start REST API
    if start_rest_requested {
        let rest_port_to_use = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        if rest_port_to_use < 1024 || rest_port_to_use > 65535 {
            eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port_to_use);
            rest_api_status_msg = format!("Invalid port: {}", rest_port_to_use);
        } else {
            // Check if REST API is already running *our* process
            if is_rest_api_running(rest_port_to_use).await {
                eprintln!("REST API server on port {} is already running.", rest_port_to_use);
                rest_api_status_msg = format!("Already running on port: {}", rest_port_to_use);
            } else if !is_port_free(rest_port_to_use).await {
                eprintln!("Port {} is in use by another process, but it's not a GraphDB REST API process. Cannot start.", rest_port_to_use);
                rest_api_status_msg = format!("Port {} in use by unknown process.", rest_port_to_use);
            }
            else {
                println!("Starting REST API server on port {}...", rest_port_to_use);
                let current_cli_storage_config = load_storage_config(None).unwrap_or_default();
                
                // Get the cli::config::StorageEngineType from the loaded config
                let cli_storage_engine_type_opt: Option<String> = Some(current_cli_storage_config.storage_engine_type);

                // Convert cli::config::StorageEngineType to daemon_api::StorageEngineType using the From trait

                let daemon_storage_engine_type: daemon_api::StorageEngineType = match cli_storage_engine_type_opt {
                    Some(ref engine) => match engine.to_lowercase().as_str() {
                        "sled" => crate::cli::config::StorageEngineType::Sled.into(),
                        "rocksdb" => crate::cli::config::StorageEngineType::RocksDB.into(),
                        other => panic!("Unknown storage engine: {}", other),
                    },
                    None => crate::cli::config::StorageEngineType::Sled.into(),
                };

 
                start_daemon_process(
                    true,
                    false,
                    Some(rest_port_to_use),
                    Some(PathBuf::from(current_cli_storage_config.data_directory.clone())),
                    Some(daemon_storage_engine_type),
                ).await?;
                rest_api_status_msg = format!("Running on port: {}", rest_port_to_use);
            }
        }
    }

    // 3. Start Storage Daemon
    if start_storage_requested {
        let storage_ports_to_start = if let Some(cluster_range_str) = cluster.clone() {
            parse_cluster_range(&cluster_range_str)?
        } else if let Some(_port) = storage_port {
            vec![_port]
        } else {
            let storage_config = load_storage_config(None).unwrap_or_default();
            vec![storage_config.default_port]
        };

        let mut started_storage_ports = Vec::new();
        for p in storage_ports_to_start {
            if skip_ports.contains(&p) {
                println!("Skipping storage daemon on port {} as it's reserved for another service.", p);
                continue;
            }
            if is_storage_daemon_running(p).await {
                eprintln!("Storage daemon on port {} is already running.", p);
                continue;
            }
            if !is_port_free(p).await {
                eprintln!("Port {} is in use by another process for storage daemon, but it's not a GraphDB Storage Daemon process. Cannot start.", p);
                continue;
            }

            println!("Starting Storage daemon on port {}...", p);
            let actual_storage_config_path = storage_config_file.clone().unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("Failed to get parent directory of server crate")
                    .join("storage_daemon_server")
                    .join("storage_config.yaml")
            });
            match start_daemon_process(
                false,
                true,
                Some(p),
                Some(actual_storage_config_path.clone()),
                None,
            ).await {
                Ok(_) => {
                    started_storage_ports.push(p);
                },
                Err(e) => {
                    eprintln!("Failed to start storage daemon on port {}: {:?}", p, e);
                }
            }
        }
        if !started_storage_ports.is_empty() {
            storage_status_msg = format!("Running on port(s): {}", started_storage_ports.iter().map(|p| p.to_string()).collect::<Vec<String>>().join(", "));
        } else {
            storage_status_msg = "Failed to start any storage daemon instances or already running.".to_string();
        }
    }

    // Summary output
    println!("\n--- Start Command Summary ---");
    println!("GraphDB Daemon: {}", daemon_status_msg);
    println!("REST API: {}", rest_api_status_msg);
    println!("Storage Daemon: {}", storage_status_msg);
    println!("-----------------------------");

    Ok(())
}

/// Handles stopping GraphDB components.
pub async fn handle_stop_command(
    stop_args: StopArgs,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    println!("Stopping all GraphDB components...");
    let client = Client::builder()
        .timeout(Duration::from_secs(5)) // Increased timeout for shutdown
        .build()
        .expect("Failed to build reqwest client");

    let mut system = System::new_all();
    system.refresh_all(); // Refresh system processes

    // --- Stop REST API ---
    // Use &stop_args.action to avoid moving the value, and ensure correct pattern for struct variants.
    if matches!(&stop_args.action, Some(StopAction::All)) || matches!(&stop_args.action, Some(StopAction::Rest { .. })) {
        let rest_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), None).await;
        if rest_ports.is_empty() {
            println!("No REST API servers found running.");
        } else {
            for port in rest_ports {
                println!("Attempting to stop REST API on port {}...", port);
                let shutdown_url = format!("http://127.0.0.1:{}/api/v1/shutdown", port); // Assuming a /shutdown endpoint
                match client.post(&shutdown_url).send().await {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            println!("Successfully sent shutdown signal to REST API on port {}.", port);
                            // Wait a bit for the service to actually stop
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            if !is_rest_api_running(port).await {
                                println!("REST API on port {} stopped.", port);
                            } else {
                                println!("REST API on port {} is still running after shutdown signal. Manual termination might be needed.", port);
                                // Attempt to kill the process if it's still running
                                if let Some(pid) = crate::cli::daemon_management::find_process_by_port(port, "rest_api_server", &system) {
                                    if let Some(process) = system.process(pid) {
                                        warn!("Attempting to kill REST API process (PID: {}) on port {}...", pid, port); 
                                        process.kill();
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                        if !is_rest_api_running(port).await {
                                            println!("REST API process (PID: {}) on port {} killed successfully.", pid, port); 
                                        } else {
                                            error!("Failed to kill REST API process (PID: {}) on port {}.", pid, port); 
                                        }
                                    }
                                }
                            }
                        } else {
                            eprintln!("Failed to send shutdown signal to REST API on port {}: Status {}", port, resp.status());
                        }
                    },
                    Err(e) => eprintln!("Error sending shutdown request to REST API on port {}: {}", port, e),
                }
            }
        }
    }

    // --- Stop Daemon ---
    // Use &stop_args.action to avoid moving the value, and ensure correct pattern for struct variants.
    if matches!(&stop_args.action, Some(StopAction::All)) || matches!(&stop_args.action, Some(StopAction::Daemon { .. })) {
        let daemon_ports = find_all_running_daemon_ports(None).await;
        if daemon_ports.is_empty() {
            println!("No GraphDB Daemons found running.");
        } else {
            for port in daemon_ports {
                println!("Attempting to stop GraphDB Daemon on port {}...", port);
                // For daemon, if no HTTP shutdown, we need to find PID and kill.
                // Assuming "graphdb_daemon" is part of the command line for daemon processes
                if let Some(pid) = crate::cli::daemon_management::find_process_by_port(port, "graphdb_daemon", &system) {
                    if let Some(process) = system.process(pid) {
                        println!("Found GraphDB Daemon process (PID: {}). Attempting to kill...", pid); 
                        process.kill();
                        tokio::time::sleep(Duration::from_secs(2)).await; // Give it time to terminate
                        if !is_main_daemon_running(port).await {
                            println!("GraphDB Daemon on port {} (PID: {}) stopped.", port, pid); 
                        } else {
                            error!("GraphDB Daemon on port {} (PID: {}) is still running after kill attempt.", port, pid); 
                        }
                    } else {
                        warn!("Could not find process with PID {} for daemon on port {}.", pid, port); 
                    }
                } else {
                    println!("Could not find process for GraphDB Daemon on port {}. It might not be running or its process name differs.", port);
                }
            }
        }
    }

    // --- Stop Storage Daemon ---
    // Use &stop_args.action to avoid moving the value, and ensure correct pattern for struct variants.
    if matches!(&stop_args.action, Some(StopAction::All)) || matches!(&stop_args.action, Some(StopAction::Storage { .. })) {
        let storage_ports = find_all_running_storage_daemon_ports(None).await;
        if storage_ports.is_empty() {
            println!("No Storage Daemons found running.");
        } else {
            for port in storage_ports {
                println!("Attempting to stop Storage Daemon on port {}...", port);
                // Similar to daemon, assume PID-based termination if no HTTP endpoint.
                if let Some(pid) = crate::cli::daemon_management::find_process_by_port(port, "storage_daemon", &system) {
                    if let Some(process) = system.process(pid) {
                        println!("Found Storage Daemon process (PID: {}). Attempting to kill...", pid); 
                        process.kill();
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        if !is_storage_daemon_running(port).await {
                            println!("Storage Daemon on port {} (PID: {}) stopped.", port, pid); 
                        } else {
                            error!("Storage Daemon on port {} (PID: {}) is still running after kill attempt.", port, pid); 
                        }
                    } else {
                        warn!("Could not find process with PID {} for storage daemon on port {}.", pid, port); 
                    }
                } else {
                    println!("Could not find process for Storage Daemon on port {}. It might not be running or its process name differs.", port);
                }
            }
        }
    }

    println!("All GraphDB components stop initiated.");
    Ok(())
}

/// Handles `rest` subcommand for direct CLI execution.
pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    _rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>, // Renamed
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    _rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>, // Renamed to _rest_api_handle as it's not directly used here
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port } => {
            handle_rest_command_interactive(
                RestCliCommand::Start { port },
                _rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                _rest_api_handle, // Pass the renamed handle
            ).await
        }
        RestCliCommand::Stop => {
            handle_rest_command_interactive(
                RestCliCommand::Stop,
                _rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                _rest_api_handle, // Pass the renamed handle
            ).await
        }
        RestCliCommand::Status => {
            display_rest_api_status(rest_api_port_arc.clone()).await;
            Ok(())
        }
        RestCliCommand::Health => {
            display_rest_api_health(rest_api_port_arc.clone()).await;
            Ok(())
        }
        RestCliCommand::Version => {
            display_rest_api_version(rest_api_port_arc.clone()).await;
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
    }
}

/// Handles `daemon` subcommand for direct CLI execution.
pub async fn handle_daemon_command(
    daemon_cmd: DaemonCliCommand,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match daemon_cmd {
        DaemonCliCommand::Start { port, cluster } => {
            start_daemon_instance_interactive(port, cluster, daemon_handles).await
        }
        DaemonCliCommand::Stop { port } => {
            stop_daemon_instance_interactive(port, daemon_handles).await
        }
        DaemonCliCommand::Status { port } => {
            display_daemon_status(port).await;
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

/// Handles `storage` subcommand for direct CLI execution.
pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file } => {
            let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);
            let actual_config_file = config_file.unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("Failed to get parent directory of server crate")
                    .join("storage_daemon_server")
                    .join("storage_config.yaml")
            });

            println!("Attempting to start Storage daemon on port {} with config file {:?}...", actual_port, actual_config_file);
            stop_process_by_port("Storage Daemon", actual_port).await?;

            let exe_path = get_current_exe_path()?;
            let mut command = TokioCommand::new(&exe_path);
            command.arg("storage").arg("start");
            command.arg("--port").arg(actual_port.to_string());
            command.arg("--config-file").arg(&actual_config_file);
            command.stdout(Stdio::inherit());
            command.stderr(Stdio::inherit());

            match command.spawn() {
                Ok(_) => {
                    println!("Storage daemon launched in background.");
                },
                Err(e) => {
                    eprintln!("Failed to spawn Storage daemon process on port {}: {}", actual_port, e);
                    anyhow::bail!("Failed to start storage daemon: {}", e);
                }
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);
            stop_process_by_port("Storage Daemon", actual_port).await?;
            println!("Standalone Storage daemon stop command processed for port {}.", actual_port);
            Ok(())
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port, Arc::new(TokioMutex::new(None))).await;
            Ok(())
        }
    }
}

/// Handles the top-level `status` command.
pub async fn handle_status_command(
    _daemon_port: Option<u16>,
    _rest_api_port: Option<u16>,
    _storage_port: Option<u16>,
    _all: bool,
    _daemon_cluster_range: Option<String>,
    _rest_cluster_range: Option<String>,
) -> Result<()> {
    println!("\n--- GraphDB System Status Summary ---");
    println!("Component           Status          Port       Details");
    println!("-------------------- --------------- ---------- ----------------------------------------");

    let main_daemon_config = load_main_daemon_config(None).unwrap_or_default();
    let daemon_ports_to_check = if let Some(cluster_arg) = _daemon_cluster_range {
        parse_cluster_range(&cluster_arg).unwrap_or_else(|_| vec![main_daemon_config.default_port])
    } else if !main_daemon_config.cluster_range.is_empty() {
        parse_cluster_range(&main_daemon_config.cluster_range).unwrap_or_else(|_| vec![main_daemon_config.default_port])
    } else {
        vec![main_daemon_config.default_port]
    };

    let mut running_daemon_count = 0;
    let mut running_daemon_ports: Vec<u16> = Vec::new();

    for p in &daemon_ports_to_check {
        if is_main_daemon_running(*p).await {
            running_daemon_count += 1;
            running_daemon_ports.push(*p);
        }
    }

    if running_daemon_count > 0 {
        running_daemon_ports.sort();
        let ports_str = if running_daemon_ports.len() > 1 {
            format!("{}-{}", running_daemon_ports.first().unwrap(), running_daemon_ports.last().unwrap())
        } else {
            running_daemon_ports.first().unwrap().to_string()
        };
        println!("GraphDB Daemon          Running         {}        Core Graph Processing ({} instances)",
                 ports_str, running_daemon_count);
    } else {
        println!("GraphDB Daemon          Down            N/A        Core Graph Processing");
    }

    let main_daemon_config_for_rest = load_main_daemon_config(None).unwrap_or_default();
    let rest_api_ports_to_check = if let Some(cluster_arg) = _rest_cluster_range {
        parse_cluster_range(&cluster_arg).unwrap_or_else(|_| vec![crate::cli::config::get_default_rest_port_from_config()])
    } else if !main_daemon_config_for_rest.cluster_range.is_empty() {
        parse_cluster_range(&main_daemon_config_for_rest.cluster_range).unwrap_or_else(|_| vec![crate::cli::config::get_default_rest_port_from_config()])
    }
    else {
        vec![crate::cli::config::get_default_rest_port_from_config()]
    };

    let mut running_rest_count = 0;
    let mut running_rest_ports: Vec<u16> = Vec::new();
    let mut rest_api_details = String::new();

    for p in &rest_api_ports_to_check {
        if is_rest_api_running(*p).await {
            running_rest_count += 1;
            running_rest_ports.push(*p);

            if rest_api_details.is_empty() {
                match rest::api::check_rest_api_status(*p).await { // Corrected: Used explicit path `rest::api`
                    Ok(status) => rest_api_details = format!("{}; Version: 0.1.0-alpha", status),
                    Err(e) => warn!("Could not get REST API health details for port {}: {:?}", p, e),
                }
            }
        }
    }

    if running_rest_count > 0 {
        running_rest_ports.sort();
        let ports_str = if running_rest_ports.len() > 1 {
            format!("{}-{}", running_rest_ports.first().unwrap(), running_rest_ports.last().unwrap())
        } else {
            running_rest_ports.first().unwrap().to_string()
        };
        println!("REST API                Running         {}        {}", ports_str, rest_api_details);
    } else {
        println!("REST API                Down            N/A         ");
    }

    let storage_cfg_for_status = crate::cli::config::load_storage_config(None).unwrap_or_default();
    let storage_daemon_ports_to_check = if !storage_cfg_for_status.cluster_range.is_empty() {
        parse_cluster_range(&storage_cfg_for_status.cluster_range).unwrap_or_else(|_| vec![storage_cfg_for_status.default_port, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS])
    } else {
        vec![storage_cfg_for_status.default_port, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS]
    };

    let mut running_storage_count = 0;
    let mut running_storage_ports: Vec<u16> = Vec::new();
    let mut storage_details = String::new();

    for _port in &storage_daemon_ports_to_check {
        if is_storage_daemon_running(*_port).await {
            running_storage_count += 1;
            running_storage_ports.push(*_port);

            storage_details = format!("Type: {}; Data Dir: {}; Max Open Files: {}; Configured Cluster: {}",
                                      storage_cfg_for_status.storage_engine_type,
                                      storage_cfg_for_status.data_directory,
                                      storage_cfg_for_status.max_open_files,
                                      storage_cfg_for_status.cluster_range);
            break;
        }
    }

    if running_storage_count > 0 {
        running_storage_ports.sort();
        let ports_str = if running_storage_ports.len() > 1 {
            format!("{}-{}", running_storage_ports.first().unwrap(), running_storage_ports.last().unwrap())
        } else {
            running_storage_ports.first().unwrap().to_string()
        };
        println!("Storage Daemon          Running         {}        {}", ports_str, storage_details);
    } else {
        println!("Storage Daemon          Down            N/A         {} ",
                 format!("Type: {}; Data Dir: {}; Max Open Files: {}; Configured Cluster: {}",
                         storage_cfg_for_status.storage_engine_type,
                         storage_cfg_for_status.data_directory,
                         storage_cfg_for_status.max_open_files,
                         storage_cfg_for_status.cluster_range));
    }

    println!("--------------------------------------------------");

    Ok(())
}

/// Handles the top-level `reload` command.
pub async fn handle_reload_command(
    reload_args: ReloadArgs,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, // Added this argument
) -> Result<()> {
    match reload_args.action {
        ReloadAction::All => {
            println!("Reloading all GraphDB components...");
            // Assuming handle_stop_command needs to be called with appropriate context or arguments
            // For now, retaining the existing call, but be aware this might need adjustments
            self::handle_stop_command(
                crate::cli::commands::StopArgs { action: Some(crate::cli::commands::StopAction::All) },
                rest_api_port_arc.clone(), // Pass the rest_api_port_arc here
            ).await?;

            println!("Restarting all GraphDB components after reload...");
            let daemon_result = start_daemon(None, None, vec![]).await;
            match daemon_result {
                Ok(()) => println!("GraphDB Daemon(s) restarted."),
                Err(e) => eprintln!("Failed to restart daemon(s): {:?}", e),
            }

            let rest_ports_to_restart = find_all_running_rest_api_ports(rest_api_port_arc.clone(), None).await; // Fixed: Pass rest_api_port_arc and None for cluster
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
                // If no REST API was running, start one on default port
                let default_rest_port = DEFAULT_REST_API_PORT;
                stop_process_by_port("REST API", default_rest_port).await?; // Ensure default is free
                start_daemon_process(
                    true, false, Some(default_rest_port),
                    Some(PathBuf::from(LibStorageConfig::default().data_path)),
                    Some(LibStorageConfig::default().engine_type.into()),
                ).await?;
                println!("REST API server restarted on default port {}.", default_rest_port);
            }

            let storage_port_to_restart = find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT);
            stop_process_by_port("Storage Daemon", storage_port_to_restart).await?;
            let actual_storage_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("Failed to get parent directory of server crate")
                .join("storage_daemon_server")
                .join("storage_config.yaml");
            start_daemon_process(
                false, true, Some(storage_port_to_restart),
                Some(actual_storage_config_path),
                None,
            ).await?;
            println!("Standalone Storage daemon restarted on port {}.", storage_port_to_restart);

            println!("All GraphDB components reloaded (stopped and restarted).");
        }
        ReloadAction::Rest => {
            println!("Reloading REST API server...");
            let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), None).await; // Fixed: Pass rest_api_port_arc and None for cluster
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
        ReloadAction::Storage => {
            println!("Reloading standalone Storage daemon...");
            let current_storage_port = find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT);

            stop_process_by_port("Storage Daemon", current_storage_port).await?;
            let actual_storage_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("Failed to get parent directory of server crate")
                .join("storage_daemon_server")
                .join("storage_config.yaml");
            start_daemon_process(
                false, true, Some(current_storage_port),
                Some(actual_storage_config_path),
                None,
            ).await?;

            println!("Standalone Storage daemon reloaded.");
        }
        ReloadAction::Daemon { port } => {
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
            println!("You might need to stop and restart individual daemons or use a cluster-wide command.");
        }
    }
    Ok(())
}

// --- Status Display Functions (simplified for brevity, ensure they exist elsewhere) ---

pub async fn display_rest_api_health(rest_api_port_arc: Arc<TokioMutex<Option<u16>>>) {
    println!("Performing REST API health check...");

    // The rest_api_port_arc is now passed as an argument, so no need to create it here.
    // let rest_api_port_arc = Arc::new(TokioMutex::new(None::<u16>)); // REMOVE THIS LINE

    let running_ports = find_all_running_rest_api_ports(rest_api_port_arc, None).await;

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

pub async fn display_rest_api_version(rest_api_port_arc: Arc<TokioMutex<Option<u16>>>) {
    println!("Getting REST API version...");
    // The find_all_running_rest_api_ports function needs rest_api_port_arc and an optional cluster.
    // Assuming no specific cluster for this action, pass None.
    let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), None).await;
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

// --- Interactive-specific Command Handlers (Moved back into handlers.rs) ---
// These functions manage the state of running daemons/REST API within the interactive CLI session.
// They are called from the interactive loop in interactive.rs.

/// Starts a daemon instance and manages its lifecycle in the interactive CLI.
pub async fn start_daemon_instance_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

    // Check if already running by port to avoid redundant starts.
    if check_process_status_by_port("GraphDB Daemon", actual_port).await {
        println!("Daemon on port {} is already running.", actual_port);
        return Ok(());
    }

    println!("Attempting to start daemon on port {}...", actual_port);
    
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

/// Stops a daemon instance managed by the interactive CLI.
pub async fn stop_daemon_instance_interactive(
    port: Option<u16>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
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
        crate::cli::daemon_management::stop_daemon_api_call().unwrap_or_else(|e| eprintln!("Warning: Failed to send global stop signal to daemons: {}", e));
        let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
        for &p in &common_daemon_ports {
            stop_process_by_port("GraphDB Daemon", p).await?;
        }
        println!("All GraphDB Daemon instances stopped (or attempted to stop).");
    }
    Ok(())
}

/// Starts the REST API server and manages its lifecycle in the interactive CLI.
pub async fn start_rest_api_interactive(
    port: Option<u16>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_REST_API_PORT);

    if check_process_status_by_port("REST API", actual_port).await {
        println!("REST API server on port {} is already running.", actual_port);
        return Ok(());
    }

    if actual_port < 1024 || actual_port > 65535 {
        eprintln!("Invalid port: {}. Must be between 1024 and 65535.", actual_port);
        return Ok(());
    }

    stop_process_by_port("REST API", actual_port).await?;
    
    println!("Starting REST API server on port {}...", actual_port);
    let current_storage_config = LibStorageConfig::default();
    let lib_storage_engine_type = current_storage_config.engine_type;

    start_daemon_process(
        true,
        false,
        Some(actual_port),
        Some(PathBuf::from(current_storage_config.data_path)),
        Some(lib_storage_engine_type.into()),
    ).await?;

    let addr_check = format!("127.0.0.1:{}", actual_port);
    let health_check_timeout = Duration::from_secs(10);
    let poll_interval = Duration::from_millis(500);
    let mut started_ok = false;
    let start_time = Instant::now();

    while start_time.elapsed() < health_check_timeout {
        match tokio::net::TcpStream::connect(&addr_check).await {
            Ok(_) => {
                println!("REST API server on port {} responded to health check.", actual_port);
                started_ok = true;
                break;
            }
            Err(_) => {
                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    if started_ok {
        println!("REST API server started on port {}.", actual_port);
        *rest_api_port_arc.lock().await = Some(actual_port);
    } else {
        eprintln!("Warning: REST API server launched but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
            actual_port, health_check_timeout);
        return Err(anyhow::anyhow!("REST API server failed to become reachable on port {}", actual_port));
    }
    Ok(())
}


/// Stops the REST API server managed by the interactive CLI.
pub async fn stop_rest_api_interactive(
    _rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    _rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    // The find_all_running_rest_api_ports function needs rest_api_port_arc and an optional cluster.
    // Assuming no specific cluster for this action, pass None.
    let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), None).await;
    if running_ports.is_empty() {
        println!("No REST API servers found running to stop.");
    } else {
        for port in running_ports {
            println!("Attempting to stop REST API server on port {}...", port);
            stop_process_by_port("REST API", port).await?;
            println!("REST API server on port {} stopped.", port);
        }
    }
    *rest_api_port_arc.lock().await = None; // Clear the tracked port after stopping all
    Ok(())
}


/// Starts a storage instance and manages its lifecycle in the interactive CLI.
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    _storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);
    let actual_config_file = config_file.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("Failed to get parent directory of server crate")
            .join("storage_daemon_server")
            .join("storage_config.yaml")
    });

    if check_process_status_by_port("Storage Daemon", actual_port).await {
        println!("Storage daemon on port {} is already running.", actual_port);
        return Ok(());
    }

    println!("Attempting to start Storage daemon on port {} with config file {:?}...", actual_port, actual_config_file);

    stop_process_by_port("Storage Daemon", actual_port).await?;
    
    start_daemon_process(
        false,
        true,
        Some(actual_port),
        Some(actual_config_file.clone()),
        None,
    ).await?;

    let addr_check = format!("127.0.0.1:{}", actual_port);
    let health_check_timeout = Duration::from_secs(10);
    let poll_interval = Duration::from_millis(500);
    let mut started_ok = false;
    let start_time = Instant::now();

    while start_time.elapsed() < health_check_timeout {
        match tokio::net::TcpStream::connect(&addr_check).await {
            Ok(_) => {
                println!("Storage daemon on port {} responded to health check.", actual_port);
                started_ok = true;
                break;
            }
            Err(_) => {
                tokio::time::sleep(poll_interval).await;
            }
        }
    }

    if started_ok {
        println!("Storage daemon started on port {}.", actual_port);
        *storage_daemon_port_arc.lock().await = Some(actual_port);
    } else {
        eprintln!("Warning: Storage daemon launched but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
            actual_port, health_check_timeout);
        return Err(anyhow::anyhow!("Storage daemon failed to become reachable on port {}", actual_port));
    }
    Ok(())
}

/// Stops a storage instance.
pub async fn stop_storage_interactive(
    port: Option<u16>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let p = port.unwrap_or(get_cli_storage_default_port()); // Use the new helper function

    let mut tx_guard = storage_daemon_shutdown_tx_opt.lock().await;
    if let Some(tx) = tx_guard.take() {
        println!("Stopping Storage daemon on port {}...", p);
        if tx.send(()).is_ok() {
            println!("Shutdown signal sent to Storage daemon.");
        } else {
            eprintln!("Failed to send shutdown signal to Storage daemon. It might already be stopping or stopped.");
        }
        if let Some(handle) = storage_daemon_handle.lock().await.take() {
            handle.await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
        }
        *storage_daemon_port_arc.lock().await = None; // Clear the port
        println!("Storage daemon on port {} stopped.", p);
    } else {
        println!("Storage daemon on port {} is not running or already stopped.", p);
    }
    Ok(())
}

// Add this `use` statement at the top of handlers.rs if not already present
// use crate::daemon_api; 
// Addressing unused variable warnings in handle_start_all_interactive and handle_start_command
pub async fn handle_start_all_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>, // Renamed to _storage_daemon_port_arc
) -> Result<()> {
    println!("Starting all GraphDB components...");

    // Start daemon
    handle_daemon_command_interactive(
        DaemonCliCommand::Start { port, cluster },
        daemon_handles.clone(),
    )
    .await?;

    // Start REST API
    handle_rest_command_interactive(
        RestCliCommand::Start { port: listen_port },
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    )
    .await?;

    // Start Storage Daemon
    handle_storage_command_interactive(
        StorageAction::Start {
            port: storage_port,
            config_file: storage_config_file,
        },
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        _storage_daemon_port_arc.clone(), // Use the renamed variable
    )
    .await?;

    println!("All components started.");
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
) -> Result<()> {
    println!("Stopping all GraphDB components...");
    
    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;
    stop_storage_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;

    println!("Sending global stop signal to all external daemon processes...");
    crate::cli::daemon_management::stop_daemon_api_call().unwrap_or_else(|e| eprintln!("Warning: Failed to send global stop signal to daemons: {}", e));

    println!("All GraphDB components stop commands processed.");
    Ok(())
}

/// Handles `DaemonCliCommand` variants in interactive mode.
pub async fn handle_daemon_command_interactive(
    command: DaemonCliCommand,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let cli_config = load_cli_config()?;
    let default_daemon_port = cli_config.daemon.port;

    match command {
        DaemonCliCommand::Start { port, cluster } => {
            let p = port.unwrap_or(default_daemon_port.unwrap_or(DEFAULT_DAEMON_PORT));
            if daemon_handles.lock().await.contains_key(&p) {
                println!("Daemon already managed on port {}.", p);
                return Ok(());
            }

            println!("Starting daemon on port {}...", p);
            let (tx, rx) = oneshot::channel();
            let handle = launch_daemon_process(p, cluster, rx)?; // Pass the receiver
            daemon_handles.lock().await.insert(p, (handle, tx));
            println!("Daemon started on port {}.", p);
        }
        DaemonCliCommand::Stop { port } => {
            let p = port.unwrap_or(default_daemon_port.unwrap_or(DEFAULT_DAEMON_PORT));
            if stop_managed_daemon(p, daemon_handles.clone()).await? {
                println!("Daemon on port {} stopped.", p);
            } else {
                println!("Daemon on port {} is not running or not managed by this CLI.", p);
            }
        }
        DaemonCliCommand::Status { port } => {
            let p = port.unwrap_or(default_daemon_port.unwrap_or(DEFAULT_DAEMON_PORT));
            if is_daemon_running(p).await {
                println!("Daemon is running on port {}.", p);
            } else {
                println!("Daemon is not running on port {}.", p);
            }
        }
        DaemonCliCommand::List => {
            println!("Listing managed daemons:");
            let handles = daemon_handles.lock().await;
            if handles.is_empty() {
                println!("No daemons currently managed by this CLI.");
            } else {
                for port in handles.keys() {
                    println!("  - Daemon on port {}", port);
                }
            }
            println!("\nAttempting to find other daemon processes...");
            match list_daemon_processes().await {
                Ok(pids) => {
                    if pids.is_empty() {
                        println!("No other daemon processes found.");
                    } else {
                        println!("Found external daemon processes with PIDs: {:?}", pids);
                    }
                }
                Err(e) => eprintln!("Error listing external daemon processes: {}", e),
            }
        }
        DaemonCliCommand::ClearAll => {
            println!("Stopping all managed daemons and clearing processes...");
            let mut handles = daemon_handles.lock().await;
            let ports: Vec<u16> = handles.keys().cloned().collect();
            for port in ports {
                if stop_managed_daemon(port, daemon_handles.clone()).await? {
                    println!("Stopped managed daemon on port {}.", port);
                }
            }
            handles.clear(); // Ensure the map is cleared
            println!("Attempting to kill any remaining external daemon processes...");
            match list_daemon_processes().await {
                Ok(pids) => {
                    for pid in pids {
                        if let Err(e) = kill_daemon_process(pid) {
                            eprintln!("Failed to kill daemon process {}: {}", pid, e);
                        } else {
                            println!("Killed external daemon process {}.", pid);
                        }
                    }
                }
                Err(e) => eprintln!("Error listing external daemon processes for cleanup: {}", e),
            }
            println!("All daemons cleared.");
        }
    }
    Ok(())
}

/// Handles `RestCliCommand` variants in interactive mode.
/// Handles `RestCliCommand` variants in interactive mode.
pub async fn handle_rest_command_interactive(
    command: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    // Import necessary traits/modules within this scope if not already at the crate root or parent module.
    // These would typically be at the top of the file or module.
    use std::str::FromStr; 
    use anyhow::anyhow;
    use std::path::PathBuf;
    use tokio::time::Duration;
    // Assuming load_cli_config and load_storage_config are accessible.
    // Assuming is_rest_api_running, is_port_free, start_daemon_process are accessible.
    // Assuming display_rest_api_status, display_rest_api_health, display_rest_api_version,
    // rest::api::register_user, rest::api::authenticate_user, rest::api::execute_graph_query,
    // execute_storage_query are accessible.
    // Assuming DEFAULT_REST_API_PORT is defined.

    let cli_config = load_cli_config()?;
    let default_rest_port = cli_config.rest.port;

    match command {
        RestCliCommand::Start { port } => {
            let p = port.unwrap_or(default_rest_port);
            if is_rest_api_running(p).await {
                println!("REST API server already running on port {}.", p);
                return Ok(());
            }

            // Before attempting to start, ensure the port is free from any other process
            if !is_port_free(p).await {
                eprintln!("Port {} is already in use by another process. Cannot start REST API server.", p);
                return Err(anyhow::anyhow!("Port {} is in use.", p));
            }

            println!("Starting REST API server on port {}...", p);
           
            let current_cli_storage_config = load_storage_config(None).unwrap_or_default();

            // If storage_engine_type is String (not Option)
            let cli_storage_engine_type_string = current_cli_storage_config.storage_engine_type;

            let daemon_storage_engine_type: daemon_api::StorageEngineType = if !cli_storage_engine_type_string.is_empty() {
                match crate::cli::config::StorageEngineType::from_str(&cli_storage_engine_type_string) {
                    Ok(cli_type) => cli_type.into(),
                    Err(e) => {
                        return Err(anyhow::anyhow!("Failed to parse StorageEngineType from config: {}", e));
                    }
                }
            } else {
                crate::cli::config::StorageEngineType::Sled.into()
            };

            start_daemon_process(
                true, // is_rest_api = true
                false, // is_storage_daemon = false
                Some(p),
                Some(PathBuf::from(current_cli_storage_config.data_directory.clone())),
                Some(daemon_storage_engine_type), // Pass the converted type
            ).await?;

            // After starting the daemon process, verify if it's actually running
            tokio::time::sleep(Duration::from_secs(1)).await; // Give it a moment to bind
            if is_rest_api_running(p).await {
                *rest_api_port_arc.lock().await = Some(p);
                println!("REST API server started and detected on port {}.", p);
                Ok(())
            } else {
                eprintln!("REST API server reported as started on port {}, but health check failed. It might have failed to bind or crashed shortly after starting.", p);
                Err(anyhow::anyhow!("REST API server failed to start or remain running on port {}.", p))
            }
        }
        RestCliCommand::Stop => {
            let mut tx_guard = rest_api_shutdown_tx_opt.lock().await;
            if let Some(tx) = tx_guard.take() {
                println!("Stopping REST API server...");
                if tx.send(()).is_ok() {
                    println!("Shutdown signal sent to REST API server.");
                } else {
                    eprintln!("Failed to send shutdown signal to REST API server. It might already be stopping or stopped.");
                }
                // Wait for the server to actually shut down if its handle exists
                if let Some(handle) = rest_api_handle.lock().await.take() {
                    handle.await.map_err(|e| anyhow::anyhow!(e.to_string()))?;
                }
                *rest_api_port_arc.lock().await = None; // Clear the port
                println!("REST API server stopped.");
                Ok(())
            } else {
                // If interactive handles are not present, attempt to stop via process kill
                let p = rest_api_port_arc.lock().await.unwrap_or(default_rest_port);
                if is_rest_api_running(p).await {
                    println!("Attempting to stop REST API on port {} via process kill...", p);
                    crate::cli::daemon_management::stop_process_by_port("rest_api_server", p).await?;
                    println!("REST API server on port {} stopped via process kill.", p);
                    *rest_api_port_arc.lock().await = None; // Clear the port
                    Ok(())
                } else {
                    println!("REST API server is not running or already stopped.");
                    Ok(())
                }
            }
        }
        RestCliCommand::Status => {
            display_rest_api_status(rest_api_port_arc.clone()).await;
            Ok(())
        }
        RestCliCommand::Health => {
            display_rest_api_health(rest_api_port_arc.clone()).await;
            Ok(())
        }
        RestCliCommand::Version => {
            display_rest_api_version(rest_api_port_arc.clone()).await;
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
        StorageAction::Start { port, config_file } => {
            start_storage_interactive(
                port,
                config_file,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(
                port,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
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
    start_rest_api_interactive(None, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
    start_storage_interactive(None, None, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await?;
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

    // The find_all_running_rest_api_ports function needs rest_api_port_arc and an optional cluster.
    // Assuming no specific cluster for this action, pass None.
    let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), None).await;

    // Stop all found REST API instances
    for &port in &running_ports { // Changed to iterate over reference
        stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    }

    // Start new instances. If no ports were running, start on default.
    if running_ports.is_empty() {
        start_rest_api_interactive(None, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
    } else {
        for &port in &running_ports { // Changed to iterate over reference
            start_rest_api_interactive(Some(port), rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
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
    
    stop_storage_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
    start_storage_interactive(None, None, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await?;
    
    println!("Standalone Storage daemon reloaded.");
    Ok(())
}

/// Handles the interactive 'reload daemon' command.
pub async fn reload_daemon_interactive(port: Option<u16>) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
    println!("Reloading GraphDB daemon on port {}...", actual_port);
    
    stop_process_by_port("GraphDB Daemon", actual_port).await?;
    
    let daemon_result = start_daemon(Some(actual_port), None, vec![]).await;
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
        RestartAction::All { port, cluster, listen_port, storage_port, storage_config_file } => {
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
        RestartAction::Rest { port: rest_restart_port } => {
            println!("Restarting REST API server...");
            let ports_to_restart = if let Some(p) = rest_restart_port {
                vec![p] // If a specific port is provided, only restart that one
            } else {
                // If no port is provided, find all running REST API instances
                // The find_all_running_rest_api_ports function needs rest_api_port_arc and an optional cluster.
                // Assuming no specific cluster for this action, pass None.
                find_all_running_rest_api_ports(rest_api_port_arc.clone(), None).await
            };

            if ports_to_restart.is_empty() {
                println!("No REST API servers found running to restart. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                start_rest_api_interactive(Some(DEFAULT_REST_API_PORT), rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
            } else {
                for &port in &ports_to_restart { // Changed to iterate over reference
                    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
                    start_rest_api_interactive(Some(port), rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
                    println!("REST API server restarted on port {}.", port);
                }
            }
        },
        RestartAction::Storage { port, config_file: storage_restart_config_file } => {
            println!("Restarting standalone Storage daemon...");
            let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);

            stop_storage_interactive(Some(actual_port), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
            start_storage_interactive(Some(actual_port), storage_restart_config_file, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await?;
            println!("Standalone Storage daemon restarted.");
        },
        RestartAction::Daemon { port, cluster } => {
            println!("Restarting GraphDB daemon...");
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

            stop_daemon_instance_interactive(Some(actual_port), daemon_handles.clone()).await?;
            start_daemon_instance_interactive(Some(actual_port), cluster, daemon_handles).await?;
            println!("GraphDB daemon restarted.");
        },
        RestartAction::Cluster => {
            println!("Restarting cluster configuration...");
            println!("A full cluster restart involves coordinating restarts/reloads across multiple daemon instances.");
            println!("This is a placeholder for complex cluster management logic.");
            println!("You might need to stop and restart individual daemons or use a cluster-wide command.");
        },
    }
    Ok(())
}

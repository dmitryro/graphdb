// server/src/cli/handlers.rs

use anyhow::{Result, Context};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use std::sync::Arc;
use tokio::process::Command as TokioCommand;
use std::process::Stdio;
use std::time::{Duration, Instant};
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use crossterm::cursor::MoveTo;
use std::io::{self, Write};

// Import command structs from commands.rs
use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, ReloadAction, RestartArgs, RestartAction};
use lib::storage_engine::config::StorageConfig;
use crate::cli::daemon_management::{find_running_storage_daemon_port, start_daemon_process};
use daemon_api::{stop_daemon_api_call, start_daemon};
// Removed unused import: use crate::cli::daemon_management::clear_all_daemon_processes;


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


// Constants for default ports
const DEFAULT_DAEMON_PORT: u16 = 8000;
const DEFAULT_REST_API_PORT: u16 = 8080;
const DEFAULT_STORAGE_PORT: u16 = 8090;

/// Helper to get the path to the current executable.
pub fn get_current_exe_path() -> Result<PathBuf> {
    std::env::current_exe()
        .context("Failed to get current executable path")
}

/// Helper to check if a port is in use. (Placeholder for actual implementation)
/// This function is primarily for checking if a port is *free* before attempting to bind.
pub async fn is_port_free(port: u16) -> bool {
    tokio::net::TcpListener::bind(format!("127.0.0.1:{}", port)).await.is_ok()
}

/// Helper to find and kill a process by port. This is used for all daemon processes.
pub async fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> {
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    let output = TokioCommand::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
        .output()
        .await
        .context(format!("Failed to run lsof to find {} process on port {}", process_name, port))?;

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

/// Helper to check if a process is running on a given port. This is used for all daemon processes.
pub async fn check_process_status_by_port(_process_name: &str, port: u16) -> bool {
    let output = TokioCommand::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
        .output()
        .await;

    if let Ok(output) = output {
        let pids = String::from_utf8_lossy(&output.stdout);
        if !pids.trim().is_empty() {
            return true;
        }
    }
    false
}

/// Helper to find a running REST API port by attempting health checks on common ports.
pub async fn find_running_rest_api_port() -> Option<u16> {
    // FIX: Added more common ports for discovery
    let common_rest_ports = [DEFAULT_REST_API_PORT, 8081, 8082, 8083, 8084, 8085, 8086];
    for &port in &common_rest_ports {
        if rest::api::check_rest_api_status(port).await.is_ok() {
            return Some(port);
        }
    }
    None
}


/// Displays detailed status for the REST API server.
pub async fn display_rest_api_status(rest_api_port_arc: Arc<Mutex<Option<u16>>>) {
    let mut rest_port_opt = *rest_api_port_arc.lock().await; // Get the port from the Arc if set by interactive mode

    let actual_rest_port = if let Some(p) = rest_port_opt {
        p
    } else {
        // If not set in Arc (e.g., direct CLI command), try to discover
        if let Some(found_port) = find_running_rest_api_port().await {
            // Update the Arc for potential future use within this CLI instance's lifetime
            *rest_api_port_arc.lock().await = Some(found_port);
            found_port
        } else {
            DEFAULT_REST_API_PORT // Fallback if nothing found
        }
    };

    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();

    let health_url = format!("http://127.0.0.1:{}/api/v1/health", actual_rest_port);
    match client.get(&health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            rest_api_status = "Running".to_string();
            rest_api_details = format!("Health: OK");

            let version_url = format!("http://127.0.0.1:{}/api/v1/version", actual_rest_port);
            match client.get(&version_url).send().await {
                Ok(v_resp) if v_resp.status().is_success() => {
                    let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                },
                _ => rest_api_details = format!("{}; Version: N/A", rest_api_details),
            }
        },
        _ => { /* Status remains "Down" */ },
    }
    println!("{:<15} {:<10} {:<40}", rest_api_status, actual_rest_port, rest_api_details);
    println!("--------------------------------------------------");
}

/// Displays detailed status for a specific GraphDB daemon or lists common ones.
pub async fn display_daemon_status(port_arg: Option<u16>) {
    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    if let Some(port) = port_arg {
        let status_message = if check_process_status_by_port("GraphDB Daemon", port).await {
            "Running".to_string()
        } else {
            "Down".to_string()
        };
        println!("{:<15} {:<10} {:<40}", status_message, port, "Core Graph Processing");
    } else {
        let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
        let mut found_any = false;
        for &port in &common_daemon_ports {
            if check_process_status_by_port("GraphDB Daemon", port).await {
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
pub async fn display_storage_daemon_status(port_arg: Option<u16>) { // Changed back to port_arg
    let port_to_check = if let Some(p) = port_arg { // Changed back to port_arg
        p
    } else {
        find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT)
    };

    let storage_config = StorageConfig::default(); // This might not reflect actual running config

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
    println!("--------------------------------------------------");
}

/// Displays the status of the entire cluster. (Placeholder)
pub async fn display_cluster_status() {
    println!("\n--- Cluster Status ---");
    println!("Cluster status is a placeholder. In a real implementation, this would query all daemons in the cluster.");
    println!("--------------------------------------------------");
}

/// Displays a comprehensive status summary of all GraphDB components.
pub async fn display_full_status_summary(rest_api_port_arc: Arc<Mutex<Option<u16>>>) {
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    // --- 1. GraphDB Daemon Status ---
    let mut daemon_status_msg = "Not launched".to_string();
    let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
    let mut running_daemon_ports = Vec::new();

    for &port in &common_daemon_ports {
        let output = TokioCommand::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output()
            .await;
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
    let mut rest_port_opt = *rest_api_port_arc.lock().await;

    let actual_rest_port = if let Some(p) = rest_port_opt {
        p
    } else {
        if let Some(found_port) = find_running_rest_api_port().await {
            *rest_api_port_arc.lock().await = Some(found_port);
            found_port
        } else {
            DEFAULT_REST_API_PORT // Fallback if nothing found
        }
    };

    let rest_health_url = format!("http://127.0.0.1:{}/api/v1/health", actual_rest_port);
    let rest_version_url = format!("http://127.0.0.1:{}/api/v1/version", actual_rest_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();

    match client.get(&rest_health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            rest_api_status = "Running".to_string();
            rest_api_details = format!("Health: OK");

            let version_info = client.get(&rest_version_url).send().await;
            match version_info {
                Ok(v_resp) if v_resp.status().is_success() => {
                    let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                },
                _ => rest_api_details = "Version: N/A".to_string(),
            }
        },
        _ => { /* Status remains "Down" */ },
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, actual_rest_port, rest_api_details);

    // --- 3. Storage Daemon Status ---
    let storage_config = StorageConfig::default(); // This might not reflect actual running config

    let mut storage_daemon_status = "Down".to_string();
    let mut actual_storage_port_reported = DEFAULT_STORAGE_PORT;

    if let Some(found_port) = find_running_storage_daemon_port().await {
        storage_daemon_status = "Running".to_string();
        actual_storage_port_reported = found_port;
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, actual_storage_port_reported, format!("Type: {:?}", storage_config.engine_type));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_path));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
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
        print!("{}", " ".repeat(internal_padding_chars / 2)); // Right internal padding
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
// These functions are called directly from cli.rs for non-interactive commands.

/// Handles the top-level `start` command.
pub async fn handle_start_command(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    _config: &crate::cli::config::CliConfig,
    _daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
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

    // 1. Start Daemon
    if start_daemon_requested {
        let daemon_result = start_daemon(port, cluster.clone(), skip_ports.clone()).await;
        match daemon_result {
            Ok(()) => {
                if let Some(cluster_range) = cluster {
                    daemon_status_msg = format!("Running on cluster ports: {}", cluster_range);
                } else if let Some(p) = port {
                    daemon_status_msg = format!("Running on port: {}", p);
                } else {
                    daemon_status_msg = format!("Running on default port: {}", DEFAULT_DAEMON_PORT);
                }
            }
            Err(e) => {
                eprintln!("Failed to start daemon(s): {:?}", e);
                daemon_status_msg = format!("Failed to start ({:?})", e);
            }
        }
    }

    // 2. Start REST API
    if start_rest_requested {
        let rest_port_to_use = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        if rest_port_to_use < 1024 || rest_port_to_use > 65535 {
            eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port_to_use);
            rest_api_status_msg = format!("Invalid port: {}", rest_port_to_use);
        } else {
            // Ensure port is free before starting
            stop_process_by_port("REST API", rest_port_to_use).await?;
            
            println!("Starting REST API server on port {}...", rest_port_to_use);
            let current_storage_config = StorageConfig::default();

            let lib_storage_engine_type = current_storage_config.engine_type; 
            start_daemon_process(
                true,
                false,
                Some(rest_port_to_use),
                Some(PathBuf::from(current_storage_config.data_path)),
                Some(lib_storage_engine_type.into()), // Convert to daemon_api::StorageEngineType
            ).await?;
            rest_api_status_msg = format!("Running on port: {}", rest_port_to_use);
        }
    }

    // 3. Start Storage
    if start_storage_requested {
        let storage_port_to_use = storage_port.unwrap_or(DEFAULT_STORAGE_PORT);
        if storage_port_to_use < 1024 || storage_port_to_use > 65535 {
            eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", storage_port_to_use);
            storage_status_msg = format!("Invalid port: {}", storage_port_to_use);
        } else {
            // Ensure port is free before starting
            stop_process_by_port("Storage Daemon", storage_port_to_use).await?;
            
            println!("Starting Storage daemon on port {}...", storage_port_to_use);
            let actual_storage_config_path = storage_config_file.unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("Failed to get parent directory of server crate")
                    .join("storage_daemon_server")
                    .join("storage_config.yaml")
            });

            start_daemon_process(
                false,
                true,
                Some(storage_port_to_use),
                Some(actual_storage_config_path.clone()),
                None,
            ).await?;
            
            let addr_check = format!("127.0.0.1:{}", storage_port_to_use);
            let health_check_timeout = Duration::from_secs(10); // Increased timeout
            let poll_interval = Duration::from_millis(500); // Increased poll interval
            let mut started_ok = false;
            let start_time = Instant::now();

            while start_time.elapsed() < health_check_timeout {
                match tokio::net::TcpStream::connect(&addr_check).await {
                    Ok(_) => {
                        println!("Storage daemon on port {} responded to health check.", storage_port_to_use);
                        started_ok = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if started_ok {
                storage_status_msg = format!("Running on port: {}", storage_port_to_use);
            } else {
                // FIX: Corrected variable name from `actual_port` to `storage_port_to_use`
                eprintln!("Warning: Storage daemon daemonized but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
                    storage_port_to_use, health_check_timeout);
                storage_status_msg = format!("Daemonized but failed to become reachable on port {}", storage_port_to_use);
            }
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

/// Handles the top-level `stop` command.
pub async fn handle_stop_command(stop_args: StopArgs) -> Result<()> {
    match stop_args.action {
        Some(crate::cli::commands::StopAction::Rest) => {
            // FIX: Use discovery for REST API port
            let rest_port = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);
            stop_process_by_port("REST API", rest_port).await?;
            println!("REST API stop command processed for port {}.", rest_port);
        }
        Some(crate::cli::commands::StopAction::Daemon { port }) => {
            let p = port.unwrap_or(DEFAULT_DAEMON_PORT); // Use port from action, or default
            stop_process_by_port("GraphDB Daemon", p).await?;
            println!("GraphDB Daemon stop command processed for port {}.", p);
        }
        Some(crate::cli::commands::StopAction::Storage { port }) => { // Corrected to use 'port'
            let p = port.unwrap_or(DEFAULT_STORAGE_PORT); // Use port from action, or default
            stop_process_by_port("Storage Daemon", p).await?;
            println!("Standalone Storage daemon stop command processed for port {}.", p);
        }
        Some(crate::cli::commands::StopAction::All) | None => {
            println!("Attempting to stop all GraphDB components...");
            // FIX: Use discovery for REST API port when stopping all
            let rest_port = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);
            stop_process_by_port("REST API", rest_port).await?;
            println!("REST API stop command processed for port {}.", rest_port);

            let storage_port_to_stop = find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT);
            stop_process_by_port("Storage Daemon", storage_port_to_stop).await?;
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
pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match rest_cmd {
        // FIX: Updated pattern to use 'port' (which is the listen-port)
        RestCliCommand::Start { port } => {
            start_rest_api_interactive(
                port, // Pass the 'port' argument (which is the listen-port)
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle
            ).await
        }
        RestCliCommand::Stop => {
            stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status => {
            display_rest_api_status(rest_api_port_arc).await;
            Ok(())
        }
        RestCliCommand::Health => {
            // FIX: Need to pass the actual port for health check
            let current_rest_port = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);
            match rest::api::check_rest_api_status(current_rest_port).await {
                Ok(status) => println!("REST API Health on port {}: {}", current_rest_port, status),
                Err(e) => eprintln!("Failed to check REST API health on port {}: {}", current_rest_port, e),
            }
            Ok(())
        }
        RestCliCommand::Version => {
            // FIX: Need to pass the actual port for version check if API supports it, or get it dynamically
            let current_rest_port = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);
            let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
            let version_url = format!("http://127.0.0.1:{}/api/v1/version", current_rest_port);
            match client.get(&version_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let v_json: serde_json::Value = resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    println!("REST API Version on port {}: {}", current_rest_port, version);
                },
                Err(e) => eprintln!("Failed to get REST API version on port {}: {}", current_rest_port, e),
                _ => eprintln!("Failed to get REST API version on port {}.", current_rest_port),
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
            execute_storage_query().await;
            Ok(())
        }
    }
}

/// Handles `daemon` subcommand for direct CLI execution.
pub async fn handle_daemon_command(
    daemon_cmd: DaemonCliCommand,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
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
        StorageAction::Start { port, config_file } => { // Corrected to use `port`
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
        StorageAction::Stop { port } => { // Corrected to use `port`
            let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);
            stop_process_by_port("Storage Daemon", actual_port).await?;
            println!("Standalone Storage daemon stop command processed for port {}.", actual_port);
            Ok(())
        }
        StorageAction::Status { port } => { // Corrected to use `port`
            display_storage_daemon_status(port).await;
            Ok(())
        }
    }
}

/// Handles the top-level `status` command.
pub async fn handle_status_command(status_args: StatusArgs, rest_api_port_arc: Arc<Mutex<Option<u16>>>) -> Result<()> {
    match status_args.action {
        Some(crate::cli::commands::StatusAction::Rest) => {
            display_rest_api_status(rest_api_port_arc).await;
        }
        Some(crate::cli::commands::StatusAction::Daemon { port }) => {
            display_daemon_status(port).await;
        }
        Some(crate::cli::commands::StatusAction::Storage { port }) => { // Corrected to use `port`
            display_storage_daemon_status(port).await;
        }
        Some(crate::cli::commands::StatusAction::Cluster) => {
            display_cluster_status().await;
        }
        Some(crate::cli::commands::StatusAction::All) | None => {
            display_full_status_summary(rest_api_port_arc).await;
        }
    }
    Ok(())
}

/// Handles the top-level `reload` command.
pub async fn handle_reload_command(
    reload_args: ReloadArgs,
) -> Result<()> {
    match reload_args.action {
        ReloadAction::All => {
            println!("Reloading all GraphDB components...");
            // Stop all components using the non-interactive stop handler
            self::handle_stop_command(crate::cli::commands::StopArgs { action: Some(crate::cli::commands::StopAction::All) }).await?;

            println!("Restarting all GraphDB components after reload...");
            // Restart daemons (direct call to daemon_api)
            let daemon_result = start_daemon(None, None, vec![]).await;
            match daemon_result {
                Ok(()) => println!("GraphDB Daemon(s) restarted."),
                Err(e) => eprintln!("Failed to restart daemon(s): {:?}", e),
            }

            // Restart REST API (direct call to start_daemon_process)
            let rest_port_to_restart = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);
            stop_process_by_port("REST API", rest_port_to_restart).await?;
            start_daemon_process(
                true, false, Some(rest_port_to_restart),
                Some(PathBuf::from(StorageConfig::default().data_path)), // Use default storage path
                Some(StorageConfig::default().engine_type.into()), // Use default storage engine type, converted
            ).await?;
            println!("REST API server restarted on port {}.", rest_port_to_restart);

            // Restart Storage (direct call to start_daemon_process)
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
            let current_rest_port = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);

            stop_process_by_port("REST API", current_rest_port).await?;
            start_daemon_process(
                true, false, Some(current_rest_port),
                Some(PathBuf::from(StorageConfig::default().data_path)),
                Some(StorageConfig::default().engine_type.into()),
            ).await?;
            
            println!("REST API server reloaded.");
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

pub async fn display_rest_api_health() {
    println!("Performing REST API health check...");
    let current_rest_port = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);
    match rest::api::check_rest_api_status(current_rest_port).await {
        Ok(health) => println!("REST API Health on port {}: {}", current_rest_port, health),
        Err(e) => eprintln!("Failed to get REST API health on port {}: {}", current_rest_port, e),
    }
}

pub async fn display_rest_api_version() {
    println!("Getting REST API version...");
    let current_rest_port = find_running_rest_api_port().await.unwrap_or(DEFAULT_REST_API_PORT);
    let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
    let version_url = format!("http://127.0.0.1:{}/api/v1/version", current_rest_port);
    match client.get(&version_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            let v_json: serde_json::Value = resp.json().await.unwrap_or_default();
            let version = v_json["version"].as_str().unwrap_or("N/A");
            println!("REST API Version on port {}: {}", current_rest_port, version);
        },
        Err(e) => eprintln!("Failed to get REST API version on port {}: {}", current_rest_port, e),
        _ => eprintln!("Failed to get REST API version on port {}.", current_rest_port),
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
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

    // Check if already running by port to avoid redundant starts.
    if check_process_status_by_port("GraphDB Daemon", actual_port).await {
        println!("Daemon on port {} is already running.", actual_port);
        return Ok(());
    }

    println!("Attempting to start daemon on port {}...", actual_port);
    
    // Call the daemon_api's start_daemon directly, which is expected to daemonize itself.
    // The CLI will not hold a JoinHandle for this process.
    let daemon_result = start_daemon(port, cluster, vec![]).await;
    match daemon_result {
        Ok(()) => {
            println!("GraphDB Daemon launched on port {}. It should be running in the background.", actual_port);
            // Add the port to daemon_handles just for tracking that we tried to start it.
            // This is a simplified tracking, not full process management via JoinHandle/Sender.
            let mut handles = daemon_handles.lock().await;
            // Insert a dummy entry as we don't manage the JoinHandle/Sender directly for daemonized processes
            handles.insert(actual_port, (tokio::spawn(async {}), oneshot::channel().0)); 
        }
        Err(e) => {
            eprintln!("Failed to launch GraphDB Daemon on port {}: {:?}", actual_port, e);
            return Err(e.into()); // Convert DaemonError to anyhow::Error
        }
    }
    Ok(())
}

/// Stops a daemon instance managed by the interactive CLI.
pub async fn stop_daemon_instance_interactive(
    port: Option<u16>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let mut handles = daemon_handles.lock().await;

    if let Some(p) = port {
        // Remove from our internal tracking map (if it was ever added)
        handles.remove(&p);
        println!("Attempting to stop GraphDB Daemon on port {}...", p);
        stop_process_by_port("GraphDB Daemon", p).await?;
        println!("GraphDB Daemon on port {} stopped.", p);
    } else {
        // Clear all from our internal tracking map
        handles.clear();
        println!("Attempting to stop all GraphDB Daemon instances...");
        // Send global stop signal to daemons via API
        crate::cli::daemon_management::stop_daemon_api_call().unwrap_or_else(|e| eprintln!("Warning: Failed to send global stop signal to daemons: {}", e));
        // Also try to kill any remaining processes by common ports (fallback)
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
    port: Option<u16>, // This now represents the --listen-port
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_REST_API_PORT);

    // Check if already running by port
    if check_process_status_by_port("REST API", actual_port).await {
        println!("REST API server on port {} is already running.", actual_port);
        return Ok(());
    }

    if actual_port < 1024 || actual_port > 65535 {
        eprintln!("Invalid port: {}. Must be between 1024 and 65535.", actual_port);
        return Ok(());
    }

    // Ensure port is free before starting
    stop_process_by_port("REST API", actual_port).await?;
    
    println!("Starting REST API server on port {}...", actual_port);
    let current_storage_config = StorageConfig::default();
    let lib_storage_engine_type = current_storage_config.engine_type;

    // Call start_daemon_process to launch in background.
    // The CLI will not hold a JoinHandle for this process.
    start_daemon_process(
        true, // is_rest_api
        false, // is_storage_daemon
        Some(actual_port),
        Some(PathBuf::from(current_storage_config.data_path)),
        Some(lib_storage_engine_type.into()), // Convert to daemon_api::StorageEngineType
    ).await?;

    // Perform health check to confirm it's reachable
    let addr_check = format!("127.0.0.1:{}", actual_port);
    let health_check_timeout = Duration::from_secs(10); // Increased timeout
    let poll_interval = Duration::from_millis(500); // Increased poll interval
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
        // Update the shared state to reflect that it's running
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
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // No longer used for direct management
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // No longer used for direct management
) -> Result<()> {
    let actual_port = rest_api_port_arc.lock().await.unwrap_or(DEFAULT_REST_API_PORT);
    
    println!("Attempting to stop REST API server on port {}...", actual_port);
    stop_process_by_port("REST API", actual_port).await?;
    println!("REST API server on port {} stopped.", actual_port);
    *rest_api_port_arc.lock().await = None; // Clear the tracked port
    Ok(())
}


/// Starts a storage instance and manages its lifecycle in the interactive CLI.
pub async fn start_storage_interactive(
    port: Option<u16>, // Changed to 'port' to match commands.rs
    config_file: Option<PathBuf>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // No longer used for direct management
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // No longer used for direct management
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT); // Changed to 'port'
    let actual_config_file = config_file.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("Failed to get parent directory of server crate")
            .join("storage_daemon_server")
            .join("storage_config.yaml")
    });

    // Check if already running by port
    if check_process_status_by_port("Storage Daemon", actual_port).await {
        println!("Storage daemon on port {} is already running.", actual_port);
        return Ok(());
    }

    println!("Attempting to start Storage daemon on port {} with config file {:?}...", actual_port, actual_config_file);

    // Ensure port is free before starting
    stop_process_by_port("Storage Daemon", actual_port).await?;
    
    // Call start_daemon_process to launch in background.
    // The CLI will not hold a JoinHandle for this process.
    start_daemon_process(
        false, // is_rest_api
        true, // is_storage_daemon
        Some(actual_port),
        Some(actual_config_file.clone()),
        None,
    ).await?;

    // Health check for the newly started storage daemon
    let addr_check = format!("127.0.0.1:{}", actual_port);
    let health_check_timeout = Duration::from_secs(10); // Increased timeout
    let poll_interval = Duration::from_millis(500); // Increased poll interval
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
    } else {
        eprintln!("Warning: Storage daemon launched but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
            actual_port, health_check_timeout);
        return Err(anyhow::anyhow!("Storage daemon failed to become reachable on port {}", actual_port));
    }
    Ok(())
}

/// Stops a storage instance.
pub async fn stop_storage_interactive(
    port: Option<u16>, // Changed to 'port' to match commands.rs
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // No longer used for direct management
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // No longer used for direct management
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT); // Changed to 'port'
    
    println!("Attempting to stop Storage daemon on port {}...", actual_port);
    stop_process_by_port("Storage Daemon", actual_port).await?;
    println!("Storage daemon on port {} stopped.", actual_port);
    Ok(())
}

/// Handles the interactive 'start all' command.
pub async fn handle_start_all_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>, // Still passed for consistency, but internal usage is minimal
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // Still passed for consistency, but internal usage is minimal
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // Still passed for consistency, but internal usage is minimal
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // Still passed for consistency, but internal usage is minimal
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // Still passed for consistency, but internal usage is minimal
) -> Result<()> {
    println!("Starting all GraphDB components...");

    // 1. Stop existing components gracefully before starting
    println!("Stopping existing components before starting...");
    // Pass None for port to stop_daemon_instance_interactive to signal "all managed" or "default"
    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?; 
    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    stop_storage_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone()).await?;
    println!("Existing components stopped.");

    // 2. Start components
    // Call start functions directly, they will handle background launching and health checks.
    start_daemon_instance_interactive(port, cluster, daemon_handles).await?;
    start_rest_api_interactive(listen_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?; // Pass listen_port
    start_storage_interactive(storage_port, storage_config_file, storage_daemon_shutdown_tx_opt, storage_daemon_handle).await?;

    println!("All GraphDB components start commands processed.");
    Ok(())
}


/// Stops all components managed by the interactive CLI, then attempts to stop any others.
pub async fn stop_all_interactive(
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>, // Still passed for consistency
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // Still passed for consistency
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // Still passed for consistency
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // Still passed for consistency
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // Still passed for consistency
) -> Result<()> {
    println!("Stopping all GraphDB components...");
    
    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;
    stop_storage_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone()).await?; 

    println!("Sending global stop signal to all external daemon processes...");
    crate::cli::daemon_management::stop_daemon_api_call().unwrap_or_else(|e| eprintln!("Warning: Failed to send global stop signal to daemons: {}", e));

    println!("All GraphDB components stop commands processed.");
    Ok(())
}

/// Handles `DaemonCliCommand` variants in interactive mode.
pub async fn handle_daemon_command_interactive(
    command: DaemonCliCommand,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match command {
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

/// Handles `RestCliCommand` variants in interactive mode.
pub async fn handle_rest_command_interactive(
    command: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match command {
        // FIX: Updated pattern to use 'port' (which is the listen-port)
        RestCliCommand::Start { port } => {
            start_rest_api_interactive(
                port, // Pass the 'port' argument (which is the listen-port)
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle
            ).await
        }
        RestCliCommand::Stop => {
            stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status => {
            display_rest_api_status(rest_api_port_arc).await;
            Ok(())
        }
        RestCliCommand::Health => {
            // FIX: Need to pass the actual port for health check
            let current_rest_port = rest_api_port_arc.lock().await.unwrap_or(DEFAULT_REST_API_PORT);
            match rest::api::check_rest_api_status(current_rest_port).await {
                Ok(status) => println!("REST API Health on port {}: {}", current_rest_port, status),
                Err(e) => eprintln!("Failed to check REST API health on port {}: {}", current_rest_port, e),
            }
            Ok(())
        }
        RestCliCommand::Version => {
            // FIX: Need to pass the actual port for version check if API supports it, or get it dynamically
            let current_rest_port = rest_api_port_arc.lock().await.unwrap_or(DEFAULT_REST_API_PORT);
            let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
            let version_url = format!("http://127.0.0.1:{}/api/v1/version", current_rest_port);
            match client.get(&version_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let v_json: serde_json::Value = resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    println!("REST API Version on port {}: {}", current_rest_port, version);
                },
                Err(e) => eprintln!("Failed to get REST API version on port {}: {}", current_rest_port, e),
                _ => eprintln!("Failed to get REST API version on port {}.", current_rest_port),
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
            execute_storage_query().await;
            Ok(())
        }
    }
}

/// Handles `StorageAction` variants in interactive mode.
pub async fn handle_storage_command_interactive(
    action: StorageAction,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match action {
        StorageAction::Start { port, config_file } => { // Corrected to use `port`
            start_storage_interactive(port, config_file, storage_daemon_shutdown_tx_opt, storage_daemon_handle).await
        }
        StorageAction::Stop { port } => { // Corrected to use `port`
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle).await
        }
        StorageAction::Status { port } => { // Corrected to use `port`
            display_storage_daemon_status(port).await;
            Ok(())
        }
    }
}

/// Handles the interactive 'reload all' command.
pub async fn reload_all_interactive(
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    println!("Reloading all GraphDB components...");
    
    // Use the interactive stop function, which correctly uses the shared state
    stop_all_interactive(
        daemon_handles.clone(),
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
    ).await?;

    println!("Restarting all GraphDB components after reload...");
    // Use the interactive start functions, which correctly use the shared state
    start_daemon_instance_interactive(None, None, daemon_handles).await?; 
    start_rest_api_interactive(None, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?; // Pass None for port
    start_storage_interactive(None, None, storage_daemon_shutdown_tx_opt, storage_daemon_handle).await?;
    println!("All GraphDB components reloaded (stopped and restarted).");
    Ok(())
}

/// Handles the interactive 'reload rest' command.
pub async fn reload_rest_interactive(
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    println!("Reloading REST API server...");
    let current_rest_port = *rest_api_port_arc.lock().await; // Get the currently tracked port

    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    
    start_rest_api_interactive(current_rest_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?; // Pass current_rest_port
    
    println!("REST API server reloaded.");
    Ok(())
}

/// Handles the interactive 'reload storage' command.
pub async fn reload_storage_interactive(
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    println!("Reloading standalone Storage daemon...");
    
    stop_storage_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone()).await?;
    start_storage_interactive(None, None, storage_daemon_shutdown_tx_opt, storage_daemon_handle).await?;
    
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
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
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
            ).await?;
            println!("All GraphDB components restarted.");
        },
        // FIX: Updated RestartAction::Rest destructuring and call to match commands.rs
        RestartAction::Rest { port: rest_restart_port } => { // 'port' is now the listen-port
            println!("Restarting REST API server...");
            // Use the provided port or the currently tracked one, prioritizing the provided one
            let actual_port = rest_restart_port.or_else(|| *rest_api_port_arc.blocking_lock()).unwrap_or(DEFAULT_REST_API_PORT);

            stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            start_rest_api_interactive(rest_restart_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?; // Pass rest_restart_port
            println!("REST API server restarted.");
        },
        // FIX: Updated RestartAction::Storage destructuring and call to match commands.rs
        RestartAction::Storage { port, config_file: storage_restart_config_file } => { // Corrected to use `port`
            println!("Restarting standalone Storage daemon...");
            let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT); // Corrected to use `port`

            stop_storage_interactive(Some(actual_port), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone()).await?;
            start_storage_interactive(Some(actual_port), storage_restart_config_file, storage_daemon_shutdown_tx_opt, storage_daemon_handle).await?;
            println!("Standalone Storage daemon restarted.");
        },
        RestartAction::Daemon { port, cluster } => {
            println!("Restarting GraphDB daemon...");
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT); // Use default if no port provided

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


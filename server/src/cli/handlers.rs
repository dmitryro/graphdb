// server/src/cli/handlers.rs

// This file contains the handlers for various CLI commands, encapsulating
// the logic for interacting with daemon, REST API, and storage components.

use anyhow::{Result, Context, anyhow}; // Added `anyhow` macro import
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::{oneshot, Mutex as TokioMutex};
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
use futures::future; // Added for parallel execution

// Import command structs from commands.rs
use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, ReloadAction, RestartArgs, RestartAction};
use lib::storage_engine::config::StorageConfig as LibStorageConfig; // Alias to avoid conflict
use crate::cli::daemon_management::{find_running_storage_daemon_port, start_daemon_process, stop_daemon_api_call, handle_internal_daemon_run}; // Added handle_internal_daemon_run
use daemon_api::{stop_daemon, start_daemon};

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

/// Helper to check if a process is running and listening on a given TCP port.
pub async fn check_process_status_by_port(_process_name: &str, port: u16) -> bool {
    let output_result = run_command_with_timeout(
        "lsof",
        &["-i", &format!("TCP:{}", port), "-s", "TCP:LISTEN", "-t"],
        Duration::from_secs(3), // Short timeout for lsof
    ).await;

    if let Ok(output) = output_result {
        let pids = String::from_utf8_lossy(&output.stdout);
        // If there's any PID in the output, a process is listening on that port
        if !pids.trim().is_empty() {
            return true;
        }
    }
    false
}

/// Helper to find all running REST API ports by attempting health checks on common ports.
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


/// Displays detailed status for the REST API server.
pub async fn display_rest_api_status(port_arg: Option<u16>, rest_api_port_arc: Arc<TokioMutex<Option<u16>>>) {
    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let running_ports = find_all_running_rest_api_ports().await;
    let mut found_any = false;

    if running_ports.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No REST API servers found on common ports.");
    } else {
        for &port in &running_ports {
            found_any = true;
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build().expect("Failed to build reqwest client");

            let mut rest_api_status = "Down".to_string();
            let mut rest_api_details = String::new();

            let health_url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    rest_api_status = "Running".to_string();
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
                _ => { /* Status remains "Down" */ },
            }
            println!("{:<15} {:<10} {:<40}", rest_api_status, port, rest_api_details);
        }
    }
    
    // Update the Arc with the first found port if any, for consistency with other parts
    // that might expect a single tracked port. This is a compromise for the current design.
    if let Some(first_port) = running_ports.first() {
        *rest_api_port_arc.lock().await = Some(*first_port);
    } else {
        *rest_api_port_arc.lock().await = None;
    }

    println!("--------------------------------------------------");
}

/// Displays detailed status for a specific GraphDB daemon or lists common ones.
pub async fn display_daemon_status(port_arg: Option<u16>) {
    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let ports_to_check = if let Some(p) = port_arg {
        vec![p]
    } else {
        vec![DEFAULT_DAEMON_PORT, 8081, 9001, 9002, 9003, 9004, 9005] // Common daemon ports
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

/// Displays detailed status for the standalone Storage daemon.
pub async fn display_storage_daemon_status(port_arg: Option<u16>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    let port_to_check = if let Some(p) = port_arg {
        p
    } else {
        // Prioritize the tracked port, then discover, then default
        let managed_port_guard = storage_daemon_port_arc.lock().await;
        if let Some(p) = *managed_port_guard {
            p
        } else {
            // Fallback to discovery if not tracked and no arg provided
            find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT)
        }
    };

    let storage_config = LibStorageConfig::default(); // Use aliased StorageConfig

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
pub async fn display_full_status_summary(rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", ""); // Removed "Details" from header for consistency with new output
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    // --- 1. GraphDB Daemon Status ---
    let common_daemon_ports = [DEFAULT_DAEMON_PORT, 8081, 9001, 9002, 9003, 9004, 9005];
    
    // Concurrently check all common daemon ports
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
                running_daemon_ports.push(port.to_string());
            }
        } else {
            eprintln!("Error checking daemon status: {:?}", result.unwrap_err());
        }
    }

    let daemon_status_msg = if !running_daemon_ports.is_empty() {
        format!("Running on: {}", running_daemon_ports.join(", "))
    } else {
        "Down".to_string()
    };
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status_msg, "N/A", "Core Graph Processing");

    // --- 2. REST API Status ---
    let running_rest_ports = find_all_running_rest_api_ports().await;
    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();
    let mut rest_ports_display = String::new();

    if running_rest_ports.is_empty() {
        rest_api_status = "Down".to_string();
        rest_ports_display = DEFAULT_REST_API_PORT.to_string(); // Show default if nothing found
    } else {
        rest_api_status = "Running".to_string();
        rest_ports_display = running_rest_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ");
        
        // For details, try to get version from the first running port
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
                _ => { /* Details remain empty if health check fails */ },
            }
        }
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, rest_ports_display, rest_api_details);


    // --- 3. Storage Daemon Status ---
    let storage_config = LibStorageConfig::default(); // Use aliased StorageConfig

    let mut storage_daemon_status = "Down".to_string();
    let actual_storage_port_reported = {
        let managed_port_guard = storage_daemon_port_arc.lock().await;
        if let Some(p) = *managed_port_guard {
            p
        } else {
            find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT)
        }
    };
    
    // Check if the storage daemon is actually running on the reported port
    if check_process_status_by_port("Storage Daemon", actual_storage_port_reported).await {
        storage_daemon_status = "Running".to_string();
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
#[allow(clippy::too_many_arguments)]
pub async fn handle_start_command(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    _config: &crate::cli::config::CliConfig,
    _daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    _rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    _rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
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
            let current_storage_config = LibStorageConfig::default(); // Use aliased StorageConfig

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
        Some(crate::cli::commands::StopAction::Rest { port }) => {
            if let Some(p) = port {
                // If a specific port is provided, stop only that one
                println!("Attempting to stop REST API server on port {}...", p);
                stop_process_by_port("REST API", p).await?;
                println!("REST API server on port {} stopped.", p);
            } else {
                // If no port is specified, stop all running REST API instances (current behavior)
                let running_rest_ports = find_all_running_rest_api_ports().await;
                if running_rest_ports.is_empty() {
                    println!("No REST API servers found running.");
                } else {
                    for p_found in running_rest_ports {
                        stop_process_by_port("REST API", p_found).await?;
                        println!("REST API server on port {} stopped.", p_found);
                    }
                }
            }
        }
        Some(crate::cli::commands::StopAction::Daemon { port }) => {
            let p = port.unwrap_or(DEFAULT_DAEMON_PORT); // Use port from action, or default
            stop_process_by_port("GraphDB Daemon", p).await?;
            println!("GraphDB Daemon stop command processed for port {}.", p);
        }
        Some(crate::cli::commands::StopAction::Storage { port }) => {
            let p = port.unwrap_or(DEFAULT_STORAGE_PORT); // Use port from action, or default
            stop_process_by_port("Storage Daemon", p).await?;
            println!("Standalone Storage daemon stop command processed for port {}.", p);
        }
        Some(crate::cli::commands::StopAction::All) | None => {
            println!("Attempting to stop all GraphDB components...");
            
            let running_rest_ports = find_all_running_rest_api_ports().await;
            for port in running_rest_ports {
                stop_process_by_port("REST API", port).await?;
                println!("REST API server on port {} stopped.", port);
            }

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

// Handles `storage` subcommand for direct CLI execution.
pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file, cluster } => { // Added `cluster` here
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
            if let Some(c) = cluster {
                command.arg("--cluster").arg(c);
            }
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
        StorageAction::StorageQuery => {
            println!("Performing Storage Query (simulated, non-interactive mode)...");
            // Add actual storage query logic here for direct CLI execution
            Ok(())
        }
        StorageAction::Health => {
            println!("Performing Storage Health Check (simulated, non-interactive mode)...");
            // Add actual storage health check logic here for direct CLI execution
            Ok(())
        }
        StorageAction::Version => {
            println!("Retrieving Storage Version (simulated, non-interactive mode)...");
            // Add actual storage version retrieval logic here for direct CLI execution
            Ok(())
        }
    }
}

/// Handles the top-level `status` command.
pub async fn handle_status_command(status_args: StatusArgs, rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) -> Result<()> {
    match status_args.action {
        Some(crate::cli::commands::StatusAction::Rest {port}) => {
            display_rest_api_status(port, rest_api_port_arc).await;
        }
        Some(crate::cli::commands::StatusAction::Daemon { port }) => {
            display_daemon_status(port).await;
        }
        Some(crate::cli::commands::StatusAction::Storage { port }) => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
        }
        Some(crate::cli::commands::StatusAction::Cluster) => {
            display_cluster_status().await;
        }
        Some(crate::cli::commands::StatusAction::All) | Some(crate::cli::commands::StatusAction::All) | None => {
            // Both 'All' and 'Summary' actions, or no action specified,
            // will display the full status summary.
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await;
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

/// Starts the REST API server and manages its lifecycle in the interactive CLI.
pub async fn start_rest_api_interactive(
    port: Option<u16>,
    cluster: Option<String>, // Added cluster argument based on context
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
    let current_storage_config = LibStorageConfig::default(); // Assuming LibStorageConfig::default() is available and provides necessary info
    let lib_storage_engine_type = current_storage_config.engine_type;

    start_daemon_process(
        true, // Assuming this is `is_rest_api_server` or similar flag
        false, // Assuming this is `is_daemon` or similar flag
        Some(actual_port),
        Some(PathBuf::from(current_storage_config.data_path)),
        Some(lib_storage_engine_type.into()), // Assuming conversion to expected type
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
        // In a real implementation, you'd spawn the actual REST API task here
        // and get its JoinHandle and Sender for shutdown.
        // For now, we'll use dummy handles.
        let (tx, rx) = oneshot::channel(); // Create a channel for shutdown signal
        *rest_api_shutdown_tx_opt.lock().await = Some(tx);
        let handle = tokio::spawn(async move {
            println!("Dummy REST API task running on port {}", actual_port);
            rx.await.ok(); // Wait for shutdown signal
            println!("Dummy REST API task on port {} shutting down.", actual_port);
        });
        *rest_api_handle.lock().await = Some(handle);
    } else {
        eprintln!("Warning: REST API server launched but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
            actual_port, health_check_timeout);
        return Err(anyhow::anyhow!("REST API server failed to become reachable on port {}", actual_port));
    }
    Ok(())
}

/// Stops a REST API instance.
pub async fn stop_rest_api_interactive(
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, // Swapped order to match original help
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>, // Swapped order to match original help
) -> Result<()> {
    let mut handle_lock = rest_api_handle.lock().await;
    let mut shutdown_tx_lock = rest_api_shutdown_tx_opt.lock().await;

    if let Some(tx) = shutdown_tx_lock.take() {
        if tx.send(()).is_ok() {
            println!("Sent shutdown signal to REST API process on port {}.", rest_api_port_arc.lock().await.unwrap_or(0));
        }
    }

    if let Some(handle) = handle_lock.take() {
        // Await the task to complete gracefully after sending shutdown signal
        let _ = handle.await;
        println!("REST API process on port {} terminated.", rest_api_port_arc.lock().await.unwrap_or(0));
        *rest_api_port_arc.lock().await = None;
    } else {
        println!("REST API is not running or not managed by this CLI.");
    }
    Ok(())
}

// Renamed to avoid conflict with potential start_storage from daemon_management
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    cluster_opt: Option<String>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);

    if check_process_status_by_port("GraphDB Storage Daemon", actual_port).await {
        println!("Storage Daemon on port {} is already running.", actual_port);
        return Ok(());
    }

    println!("Attempting to start Storage Daemon on port {}...", actual_port);

    // Call the actual start_daemon_process from daemon_management, assuming it handles storage
    let storage_result = start_daemon_process(
        false, // Not a REST API server
        true,  // Is a daemon (storage daemon)
        port,
        config_file,
        None, // Assuming no specific engine type is passed here for storage, or it's implicitly handled
    ).await;
    match storage_result {
        Ok(()) => {
            println!("GraphDB Storage Daemon launched on port {}. It should be running in the background.", actual_port);
            *storage_daemon_port_arc.lock().await = Some(actual_port);
            let (tx, rx) = oneshot::channel();
            *storage_daemon_shutdown_tx_opt.lock().await = Some(tx);
            let handle = tokio::spawn(async move {
                println!("Dummy Storage Daemon task running on port {}", actual_port);
                rx.await.ok();
                println!("Dummy Storage Daemon task on port {} shutting down.", actual_port);
            });
            *storage_daemon_handle.lock().await = Some(handle);
        }
        Err(e) => {
            eprintln!("Failed to launch GraphDB Storage Daemon on port {}: {:?}", actual_port, e);
            return Err(e.into());
        }
    }
    Ok(())
}

/// Stops a storage instance.
pub async fn stop_storage_interactive(
    port: Option<u16>,
    _storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>, // Corrected Mutex alias
    _storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>, // Corrected Mutex alias
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>, // Corrected Mutex alias
) -> Result<()> {
    let actual_port = if let Some(p) = port {
        p
    } else {
        storage_daemon_port_arc.lock().await.unwrap_or(DEFAULT_STORAGE_PORT)
    };

    println!("Attempting to stop Storage daemon on port {}...", actual_port);
    stop_process_by_port("Storage Daemon", actual_port).await?;
    println!("Storage daemon on port {} stopped.", actual_port);
    *storage_daemon_port_arc.lock().await = None;
    Ok(())
}

/// Handles the top-level `start all` command for interactive use.
#[allow(clippy::too_many_arguments)]
pub async fn handle_start_all_interactive(
    daemon_port: Option<u16>,
    daemon_cluster: Option<String>,
    rest_port: Option<u16>,
    rest_cluster: Option<String>, // Renamed to _rest_cluster to suppress warning
    storage_port: Option<u16>,
    storage_cluster: Option<String>, // Renamed to _storage_cluster to suppress warning
    storage_config: Option<PathBuf>, // Changed to PathBuf as per the original call
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    println!("Starting all GraphDB components...");

    // Start Daemon
    let actual_daemon_port = daemon_port.unwrap_or(DEFAULT_DAEMON_PORT);
    let daemon_result = start_daemon_instance_interactive(Some(actual_daemon_port), daemon_cluster, daemon_handles.clone()).await;
    match daemon_result {
        Ok(()) => println!("GraphDB Daemon started on port {}.", actual_daemon_port),
        Err(e) => eprintln!("Failed to start daemon: {:?}", e),
    }

    // Start REST API
    let actual_rest_port = rest_port.unwrap_or(DEFAULT_REST_API_PORT);
    // Ensure the port is free before starting
    if !is_port_free(actual_rest_port).await {
        println!("Port {} is already in use for REST API. Attempting to stop existing process.", actual_rest_port);
        stop_process_by_port("REST API", actual_rest_port).await?;
    }
    let rest_result = start_rest_api_interactive( // Corrected function call
        Some(actual_rest_port),
        rest_cluster, // Pass the rest_cluster argument
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await;
    match rest_result {
        Ok(()) => println!("REST API server started on port {}.", actual_rest_port),
        Err(e) => eprintln!("Failed to start REST API server: {:?}", e),
    }

    // Start Storage Daemon
    let actual_storage_port = storage_port.unwrap_or(DEFAULT_STORAGE_PORT);
    // Ensure the port is free before starting
    if !is_port_free(actual_storage_port).await {
        println!("Port {} is already in use for Storage Daemon. Attempting to stop existing process.", actual_storage_port);
        stop_process_by_port("Storage Daemon", actual_storage_port).await?;
    }
    let actual_storage_config_path = storage_config.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("Failed to get parent directory of server crate")
            .join("storage_daemon_server")
            .join("storage_config.yaml")
    });

    let storage_result = start_storage_interactive( // Corrected function call
        Some(actual_storage_port),
        Some(actual_storage_config_path),
        storage_cluster, // Pass the storage_cluster argument
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone(),
    ).await;
    match storage_result {
        Ok(()) => println!("Standalone Storage daemon started on port {}.", actual_storage_port),
        Err(e) => eprintln!("Failed to start Storage daemon: {:?}", e),
    }

    println!("All GraphDB components started.");
    Ok(())
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

/// Handles `rest` subcommand for direct CLI execution.
pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>, // Changed Mutex to TokioMutex
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, // Changed Mutex to TokioMutex
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>, // Changed Mutex to TokioMutex
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port, cluster } => { // Added cluster
            start_rest_api_interactive(
                port, // Pass the 'port' argument (which is the listen-port)
                cluster, // Pass the cluster argument
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle
            ).await
        }
        RestCliCommand::Stop => {
            stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status { port } => {
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
                        },
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
            // Assuming `execute_storage_query` is defined in `rest::api` or similar, or needs to be a placeholder
            // If it's a placeholder, keep it as is. If it's a real call, adjust to `rest::api::execute_storage_query().await;`
            // For now, assuming it's a direct call or placeholder based on prior code.
            // If `execute_storage_query` is not in `rest::api`, you'll need to define it or bring it into scope.
            println!("Executing storage query via REST API.");
            // Example if `execute_storage_query` were a simple placeholder:
            // execute_storage_query().await; // This line would need `execute_storage_query` to be in scope.
            // Assuming it's a simplified placeholder here.
            Ok(())
        }
    }
}


pub async fn handle_rest_command_interactive(
    action: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match action {
        RestCliCommand::Start { port, cluster } => {
            start_rest_api_interactive(
                port,
                cluster, // Pass the cluster argument
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle
            ).await
        }
        RestCliCommand::Stop => {
            stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status { port } => {
            display_rest_api_status(port, rest_api_port_arc).await; // Pass None for port_arg
            Ok(())
        }
        RestCliCommand::Health => {
            println!("Handling REST API Health check...");
            // Implement actual health check logic here
            Ok(())
        }
        RestCliCommand::Version => {
            println!("Handling REST API Version command...");
            // Implement actual version display logic here
            Ok(())
        }
        RestCliCommand::RegisterUser { username, password } => {
            println!("Handling REST API Register User command for user: {}", username);
            // Implement actual user registration logic here
            // Note: In a real application, handle password securely (e.g., hashing)
            Ok(())
        }
        RestCliCommand::Authenticate { username, password } => {
            println!("Handling REST API Authenticate command for user: {}", username);
            // Implement authentication logic here
            Ok(())
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            println!("Handling REST API GraphQuery: '{}', persist: {:?}", query_string, persist);
            // Implement graph query execution logic here
            Ok(())
        }
        RestCliCommand::StorageQuery => {
            println!("Handling REST API StorageQuery...");
            // Implement storage query logic here
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
        // Corrected pattern to include `cluster`
        StorageAction::Start { port, config_file, cluster } => {
            start_storage_interactive(
                port,
                config_file,
                cluster, // Pass the cluster argument
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc
            ).await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::StorageQuery => {
            println!("Performing Storage Query (simulated, interactive mode)...");
            // Add actual storage query logic here
            Ok(())
        }
        StorageAction::Health => {
            println!("Performing Storage Health Check (simulated, interactive mode)...");
            // Add actual storage health check logic here
            Ok(())
        }
        StorageAction::Version => {
            println!("Retrieving Storage Version (simulated, interactive mode)...");
            // Add actual storage version retrieval logic here
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
    for &port in &running_ports { // Changed to iterate over reference
        stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?; // Corrected arguments order
    }

    // Start new instances. If no ports were running, start on default.
    if running_ports.is_empty() {
        start_rest_api_interactive(None, None, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?; // Added None for cluster
    } else {
        for &port in &running_ports { // Changed to iterate over reference
            start_rest_api_interactive(Some(port), None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?; // Added None for cluster
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
#[allow(clippy::too_many_arguments)] // Added allow for too many arguments if needed
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
            port, // This 'port' and 'cluster' are top-level args for RestartAction::All
            cluster, // So they should be passed as the first two args to handle_start_all_interactive
            daemon_port,
            daemon_cluster,
            listen_port, // This will no longer be passed to handle_start_all_interactive based on the 14-arg signature
            rest_port,
            rest_cluster,
            storage_port, // This will no longer be passed to handle_start_all_interactive based on the 14-arg signature
            storage_cluster, // This will no longer be passed to handle_start_all_interactive based on the 14-arg signature
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

            // Convert Option<String> to Option<PathBuf> for storage_config
            let storage_config_pathbuf = storage_config_file.map(PathBuf::from);

            handle_start_all_interactive(
                port, // Pass the top-level 'port'
                cluster, // Pass the top-level 'cluster'
                daemon_port, // Use daemon_port from RestartAction::All
                daemon_cluster, // Use daemon_cluster from RestartAction::All
                rest_port, // Pass rest_port
                rest_cluster, // Pass rest_cluster
                storage_config_pathbuf, // Pass the converted PathBuf
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
        RestartAction::Rest { port: rest_restart_port, cluster: rest_restart_cluster } => { // Captured cluster
            println!("Restarting REST API server...");
            let ports_to_restart = if let Some(p) = rest_restart_port {
                vec![p] // If a specific port is provided, only restart that one
            } else {
                // If no port is provided, find all running REST API instances
                find_all_running_rest_api_ports().await
            };

            if ports_to_restart.is_empty() {
                println!("No REST API servers found running to restart. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                // Pass rest_restart_cluster to start_rest_api_interactive
                start_rest_api_interactive(Some(DEFAULT_REST_API_PORT), rest_restart_cluster, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
            } else {
                for &port in &ports_to_restart { // Changed to iterate over reference
                    // Stop the specific REST API instance
                    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?; // Corrected arguments order
                    // Start the specific REST API instance
                    // Pass rest_restart_cluster to start_rest_api_interactive
                    start_rest_api_interactive(Some(port), rest_restart_cluster.clone(), rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
                    println!("REST API server restarted on port {}.", port);
                }
            }
        },
        RestartAction::Storage { port, config_file: storage_restart_config_file, cluster: storage_restart_cluster } => { // Captured cluster
            println!("Restarting standalone Storage daemon...");
            let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);

            stop_storage_interactive(Some(actual_port), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
            start_storage_interactive(Some(actual_port), storage_restart_config_file, storage_restart_cluster, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await?; // Pass storage_restart_cluster
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

/// Handles the top-level `reload` command.
pub async fn handle_reload_command(
    reload_args: ReloadArgs,
) -> Result<()> {
    // Extract the action, defaulting to ReloadAction::All if None is provided.
    // This avoids the recursive call that caused the "recursion in an async fn requires boxing" error.
    let action_to_perform = reload_args.action.unwrap_or(ReloadAction::All);

    match action_to_perform {
        ReloadAction::All => {
            println!("Reloading all GraphDB components...");
            self::handle_stop_command(crate::cli::commands::StopArgs { action: Some(crate::cli::commands::StopAction::All) }).await?;

            println!("Restarting all GraphDB components after reload...");
            let daemon_result = start_daemon(None, None, vec![]).await;
            match daemon_result {
                Ok(()) => println!("GraphDB Daemon(s) restarted."),
                Err(e) => eprintln!("Failed to restart daemon(s): {:?}", e),
            }

            let rest_ports_to_restart = find_all_running_rest_api_ports().await; // Get all running ports
            for &port in &rest_ports_to_restart { // Changed to iterate over reference
                stop_process_by_port("REST API", port).await?;
                start_daemon_process(
                    true, false, Some(port),
                    Some(PathBuf::from(LibStorageConfig::default().data_path)),
                    Some(LibStorageConfig::default().engine_type.into()), // Corrected: Pass StorageEngineType directly
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
                    Some(LibStorageConfig::default().engine_type.into()), // Corrected: Pass StorageEngineType directly
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
            let running_ports = find_all_running_rest_api_ports().await;
            if running_ports.is_empty() {
                println!("No REST API servers found running to reload. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                start_daemon_process(
                    true, false, Some(DEFAULT_REST_API_PORT),
                    Some(PathBuf::from(LibStorageConfig::default().data_path)),
                    Some(LibStorageConfig::default().engine_type.into()), // Corrected: Pass StorageEngineType directly
                ).await?;
            } else {
                for &port in &running_ports { // Changed to iterate over reference
                    stop_process_by_port("REST API", port).await?;
                    start_daemon_process(
                        true, false, Some(port),
                        Some(PathBuf::from(LibStorageConfig::default().data_path)),
                        Some(LibStorageConfig::default().engine_type.into()), // Corrected: Pass StorageEngineType directly
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

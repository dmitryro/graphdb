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
use crossterm::terminal::{Clear, ClearType};
use crossterm::execute;
use crossterm::cursor::MoveTo; // Import MoveTo for cursor positioning
use std::io::{self, Write}; // Import io and Write for flushing

// Import command structs from commands.rs
use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, StopAction, ReloadArgs, ReloadAction, StatusAction};
use lib::storage_engine::config::StorageConfig;
use crate::cli::daemon_management::{find_running_storage_daemon_port, start_daemon_process};
use daemon_api::{stop_daemon_api_call, start_daemon};
use crate::cli::daemon_management::clear_all_daemon_processes;


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
        pub async fn check_rest_api_status() -> Result<String> { Ok("OK".to_string()) }
        pub async fn perform_rest_api_health_check() -> Result<String> { Ok("Healthy".to_string()) }
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
pub fn stop_process_by_port(process_name: &str, port: u16) -> Result<(), anyhow::Error> {
    println!("Attempting to find and kill process for {} on port {}...", process_name, port);
    let output = std::process::Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
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
    let rest_port = DEFAULT_REST_API_PORT;

    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut rest_api_status = "Down".to_string();
    let mut rest_api_details = String::new();

    let health_url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
    match client.get(&health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            rest_api_status = "Running".to_string();
            rest_api_details = format!("Health: OK");

            let version_url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
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
        find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT)
    };

    let storage_config = StorageConfig::default();

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let status_message = if check_process_status_by_port("Storage Daemon", port_to_check) {
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
    let rest_port = DEFAULT_REST_API_PORT;
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
                    rest_api_details = format!("{}; Version: {}", rest_api_details, version);
                },
                _ => rest_api_details = "Version: N/A".to_string(),
            }
        },
        _ => { /* Status remains "Down" */ },
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, rest_port, rest_api_details);

    // --- 3. Storage Daemon Status ---
    let storage_config = StorageConfig::default();

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
    let total_width = 120;
    let border_char = '#';

    let line_str = border_char.to_string().repeat(total_width);

    let title = "GraphDB Command Line Interface";
    let version = "Version 0.1.0 (Experimental)";
    let welcome_msg = "Welcome! Type 'help' for a list of commands.";
    let start_tip = "Tip: Use 'start all' to launch all components.";
    let status_tip = "Tip: Use 'status all' to check component health.";
    let clear_tip = "Use 'clear' or 'clean' to clear the terminal.";
    let exit_tip = "Type 'exit' or 'quit' to leave the CLI.";

    let print_centered_colored = |text: &str, text_color: style::Color| {
        let content_width = total_width - 4; // Account for two border chars and two spaces (one on each side)
        let padding_len = content_width.saturating_sub(text.len());
        let left_padding = padding_len / 2;
        let right_padding = padding_len - left_padding;

        // Print left border in Cyan
        print!("{}", style::SetForegroundColor(style::Color::Cyan));
        print!("{}", border_char);
        print!(" "); // Space after left border

        // Print left padding and text with its specific color
        print!("{}", " ".repeat(left_padding));
        print!("{}", text.with(text_color));
        print!("{}", " ".repeat(right_padding));

        print!(" "); // Space before right border
        // Print right border in Cyan and reset color
        println!("{}{}", border_char, style::ResetColor);
    };

    println!("\n\n\n\n\n\n\n"); // Increased spacing at the top
    println!("{}", line_str.clone().with(style::Color::Cyan));
    print_centered_colored("", style::Color::Blue); // Empty line for vertical spacing
    print_centered_colored(title, style::Color::DarkCyan);
    print_centered_colored(version, style::Color::White);
    print_centered_colored("", style::Color::Blue); // Empty line for vertical spacing
    print_centered_colored(welcome_msg, style::Color::Green);
    print_centered_colored(start_tip, style::Color::Yellow);
    print_centered_colored(status_tip, style::Color::Yellow);
    print_centered_colored(clear_tip, style::Color::Yellow);
    print_centered_colored(exit_tip, style::Color::Red);
    print_centered_colored("", style::Color::Blue); // Empty line for vertical spacing
    println!("{}", line_str.with(style::Color::Cyan));
    println!("\n\n\n\n\n\n\n"); // Increased spacing at the bottom
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
    _daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>, // Prefixed with _
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>, // Prefixed with _
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>, // Prefixed with _
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>, // Prefixed with _
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
            stop_process_by_port("REST API", rest_port_to_use)?;
            
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;
            while start_time.elapsed() < wait_timeout {
                if is_port_free(rest_port_to_use).await {
                    port_freed = true;
                    break;
                }
                tokio::time::sleep(poll_interval).await;
            }

            if !port_freed {
                eprintln!("Failed to free up port {} after killing processes. Try again.", rest_port_to_use);
                rest_api_status_msg = format!("Failed to free up port {}.", rest_port_to_use);
            } else {
                println!("Starting REST API server on port {}...", rest_port_to_use);
                let current_storage_config = StorageConfig::default();

                let lib_storage_engine_type = current_storage_config.engine_type; 
                start_daemon_process(
                    true,
                    false,
                    Some(rest_port_to_use),
                    Some(PathBuf::from(current_storage_config.data_path)),
                    Some(lib_storage_engine_type),
                ).await?;
                rest_api_status_msg = format!("Running on port: {}", rest_port_to_use);
            }
        }
    }

    // 3. Start Storage
    if start_storage_requested {
        let storage_port_to_use = storage_port.unwrap_or(DEFAULT_STORAGE_PORT);
        if storage_port_to_use < 1024 || storage_port_to_use > 65535 {
            eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", storage_port_to_use);
            storage_status_msg = format!("Invalid port: {}", storage_port_to_use);
        } else {
            stop_process_by_port("Storage Daemon", storage_port_to_use)?;
            
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;
            while start_time.elapsed() < wait_timeout {
                if is_port_free(storage_port_to_use).await {
                    port_freed = true;
                    break;
                }
                tokio::time::sleep(poll_interval).await;
            }

            if !port_freed {
                eprintln!("Failed to free up storage port {} after killing processes. Try again.", storage_port_to_use);
                storage_status_msg = format!("Failed to free up port {}.", storage_port_to_use);
            } else {
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
                let health_check_timeout = Duration::from_secs(5);
                let poll_interval = Duration::from_millis(200);
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
            let rest_port = DEFAULT_REST_API_PORT;
            stop_process_by_port("REST API", rest_port)?;
            println!("REST API stop command processed for port {}.", rest_port);
        }
        Some(StopAction::Daemon { port }) => {
            let p = port.unwrap_or(DEFAULT_DAEMON_PORT);
            stop_process_by_port("GraphDB Daemon", p)?;
            println!("GraphDB Daemon stop command processed for port {}.", p);
        }
        Some(StopAction::Storage { port }) => {
            let p = port.unwrap_or(DEFAULT_STORAGE_PORT);
            stop_process_by_port("Storage Daemon", p)?;
            println!("Standalone Storage daemon stop command processed for port {}.", p);
        }
        Some(StopAction::All) | None => { // Consolidated All and None (default)
            println!("Attempting to stop all GraphDB components...");
            let rest_port = DEFAULT_REST_API_PORT;
            stop_process_by_port("REST API", rest_port)?;
            println!("REST API stop command processed for port {}.", rest_port);

            let storage_port_to_stop = find_running_storage_daemon_port().await.unwrap_or(DEFAULT_STORAGE_PORT);
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
pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port, listen_port } => {
            start_rest_api_interactive(port, listen_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Stop => {
            stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status => {
            display_rest_api_status().await;
            Ok(())
        }
        RestCliCommand::Health => {
            self::rest::api::perform_rest_api_health_check().await?;
            Ok(())
        }
        RestCliCommand::Version => {
            self::rest::api::get_rest_api_version().await?;
            Ok(())
        }
        RestCliCommand::RegisterUser { username, password } => {
            self::rest::api::register_user(&username, &password).await?;
            Ok(())
        }
        RestCliCommand::Authenticate { username, password } => {
            self::rest::api::authenticate_user(&username, &password).await?;
            Ok(())
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            self::rest::api::execute_graph_query(&query_string, persist).await?;
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
            println!("Attempting to stop all GraphDB daemon processes...");
            let stop_result = stop_daemon_api_call();
            match stop_result {
                Ok(()) => println!("Global daemon stop signal sent successfully."),
                Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
            }
            clear_all_daemon_processes().await?;
            Ok(())
        }
    }
}

/// Handles `storage` subcommand for direct CLI execution.
pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file } => {
            start_storage_interactive(port, config_file).await?;
            Ok(())
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port).await?;
            Ok(())
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port).await;
            Ok(())
        }
    }
}

/// Handles the top-level `status` command.
pub async fn handle_status_command(status_args: StatusArgs) -> Result<()> {
    match status_args.action {
        Some(StatusAction::Rest) => {
            display_rest_api_status().await;
        }
        Some(StatusAction::Daemon { port }) => {
            display_daemon_status(port).await;
        }
        Some(StatusAction::Storage { port }) => {
            display_storage_daemon_status(port).await;
        }
        Some(StatusAction::Cluster) => {
            display_cluster_status().await;
        }
        Some(StatusAction::All) | None => { // Consolidated All and None (default)
            display_full_status_summary().await;
        }
    }
    Ok(())
}

/// Handles the top-level `reload` command.
pub async fn handle_reload_command(
    reload_args: ReloadArgs,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match reload_args.action {
        ReloadAction::All => {
            println!("Reloading all GraphDB components...");
            // Stop all components
            self::handle_stop_command(StopArgs { action: Some(StopAction::All) }).await?;

            // Restart components (simplified: assumes default ports/configs for restart)
            println!("Restarting all GraphDB components after reload...");
            // Daemon restart (needs original port/cluster info if not default)
            self::handle_daemon_command(DaemonCliCommand::Start { port: None, cluster: None }, daemon_handles.clone()).await?; 
            // REST API restart
            handle_rest_command_interactive(RestCliCommand::Start { port: None, listen_port: None }, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            // Storage restart
            self::handle_storage_command(StorageAction::Start { port: None, config_file: None }).await?;

            println!("All GraphDB components reloaded (stopped and restarted).");
        }
        ReloadAction::Rest => {
            println!("Reloading REST API server...");
            let current_rest_port = *rest_api_port_arc.lock().await;

            stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
            
            start_rest_api_interactive(current_rest_port, current_rest_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
            
            println!("REST API server reloaded.");
        }
        ReloadAction::Storage => {
            println!("Reloading standalone Storage daemon...");
            
            self::handle_storage_command(StorageAction::Stop { port: None }).await?;
            self::handle_storage_command(StorageAction::Start { port: None, config_file: None }).await?;
            
            println!("Standalone Storage daemon reloaded.");
        }
        ReloadAction::Daemon { port } => {
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Reloading GraphDB daemon on port {}...", actual_port);
            
            // Stop it externally
            stop_process_by_port("GraphDB Daemon", actual_port)?;
            
            let start_time = Instant::now();
            let wait_timeout = Duration::from_secs(3);
            let poll_interval = Duration::from_millis(100);
            let mut port_freed = false;
            while start_time.elapsed() < wait_timeout {
                if is_port_free(actual_port).await {
                    port_freed = true;
                    break;
                }
                tokio::time::sleep(poll_interval).await;
            }

            if !port_freed {
                eprintln!("Failed to free up daemon port {} after killing processes. Reload failed.", actual_port);
                return Err(anyhow::anyhow!("Failed to free daemon port {}", actual_port));
            }

            // Attempt to restart the daemon.
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
    match self::rest::api::perform_rest_api_health_check().await {
        Ok(health) => println!("REST API Health: {}", health),
        Err(e) => eprintln!("Failed to get REST API health: {}", e),
    }
}

pub async fn display_rest_api_version() {
    println!("Getting REST API version...");
    match self::rest::api::get_rest_api_version().await {
        Ok(version) => println!("REST API Version: {}", version),
        Err(e) => eprintln!("Failed to get REST API version: {}", e),
    }
}

pub async fn register_user(username: String, password: String) {
    println!("Registering user '{}'...", username);
    match self::rest::api::register_user(&username, &password).await {
        Ok(_) => println!("User '{}' registered successfully.", username),
        Err(e) => eprintln!("Failed to register user '{}': {}", username, e),
    }
}

pub async fn authenticate_user(username: String, password: String) {
    println!("Authenticating user '{}'...", username);
    match self::rest::api::authenticate_user(&username, &password).await {
        Ok(token) => println!("User '{}' authenticated successfully. Token: {}", username, token),
        Err(e) => eprintln!("Failed to authenticate user '{}': {}", username, e),
    }
}

pub async fn execute_graph_query(query_string: String, persist: Option<bool>) {
    println!("Executing graph query: '{}' (persist: {:?})", query_string, persist);
    match self::rest::api::execute_graph_query(&query_string, persist).await {
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

    let mut handles = daemon_handles.lock().await;
    if handles.contains_key(&actual_port) {
        println!("Daemon on port {} is already running (managed by this CLI).", actual_port);
        return Ok(());
    }

    println!("Attempting to start daemon on port {}...", actual_port);
    let (tx, rx) = oneshot::channel();
    let daemon_args = DaemonArgs {
        port: Some(actual_port),
        cluster,
    };

    let exe_path = get_current_exe_path()?;

    let handle = tokio::spawn(async move {
        let mut command = TokioCommand::new(&exe_path);
        command.arg("daemon").arg("start");
        command.arg("--port").arg(daemon_args.port.unwrap().to_string());

        if let Some(c) = daemon_args.cluster {
            command.arg("--cluster").arg(c);
        }

        command.stdout(Stdio::null());
        command.stderr(Stdio::null());

        println!("Launching daemon process: {:?}", command);

        let mut child = match command.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to spawn daemon process on port {}: {}", actual_port, e);
                return;
            }
        };

        tokio::select! {
            _ = rx => {
                println!("Received shutdown signal for daemon on port {}. Attempting to terminate.", actual_port);
                if let Err(e) = child.kill().await {
                    eprintln!("Error killing daemon process on port {}: {}", actual_port, e);
                }
            }
            status = child.wait() => {
                match status {
                    Ok(s) => println!("Daemon process on port {} exited with status: {:?}", actual_port, s),
                    Err(e) => eprintln!("Daemon process on port {} encountered an error: {}", actual_port, e),
                }
            }
        }
        println!("Daemon on port {} task finished.", actual_port);
    });

    handles.insert(actual_port, (handle, tx));
    println!("Daemon started on port {}. Managed by CLI.", actual_port);
    Ok(())
}

/// Stops a daemon instance managed by the interactive CLI.
pub async fn stop_daemon_instance_interactive(
    port: Option<u16>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let mut handles = daemon_handles.lock().await;

    if let Some(p) = port {
        if let Some((handle, tx)) = handles.remove(&p) {
            println!("Signaling daemon on port {} to stop...", p);
            if tx.send(()).is_err() {
                eprintln!("Warning: Daemon on port {} already stopped or signal failed.", p);
            }
            let _ = handle.await;
            println!("Daemon on port {} stopped.", p);
        } else {
            println!("No daemon found running on port {} managed by this CLI. Attempting external stop.", p);
            stop_process_by_port("GraphDB Daemon", p)?;
        }
    } else {
        if handles.is_empty() {
            println!("No daemons managed by this CLI are currently running.");
        } else {
            println!("Stopping all {} managed daemon instances...", handles.len());
            let mut join_handles = Vec::new();
            let ports_to_stop: Vec<u16> = handles.keys().cloned().collect();
            for p in ports_to_stop {
                if let Some((handle, tx)) = handles.remove(&p) {
                    println!("Signaling daemon on port {} to stop.", p);
                    if tx.send(()).is_err() {
                        eprintln!("Warning: Daemon on port {} already stopped or signal failed.", p);
                    }
                    join_handles.push(handle);
                }
            }
            for handle in join_handles {
                let _ = handle.await;
            }
            println!("All managed daemon instances stopped.");
        }
        crate::cli::daemon_management::stop_daemon_api_call().unwrap_or_else(|e| eprintln!("Warning: Failed to send global stop signal to daemons: {}", e));
    }
    Ok(())
}

/// Starts the REST API server and manages its lifecycle in the interactive CLI.
pub async fn start_rest_api_interactive(
    port: Option<u16>,
    listen_port: Option<u16>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_REST_API_PORT);
    let actual_listen_port = listen_port.unwrap_or(actual_port);

    let mut tx_guard = rest_api_shutdown_tx_opt.lock().await;
    let mut port_guard = rest_api_port_arc.lock().await;
    let mut handle_guard = rest_api_handle.lock().await;

    if port_guard.is_some() && port_guard.unwrap() == actual_port {
        println!("REST API server on port {} is already running (managed by this CLI).", actual_port);
        return Ok(());
    } else if port_guard.is_some() {
        println!("Warning: REST API server already managed by this CLI on a different port ({}). Stopping it first.", port_guard.unwrap());
        if let Some(tx) = tx_guard.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = handle_guard.take() {
            let _ = handle.await;
        }
        stop_process_by_port("REST API", port_guard.unwrap())?;
        *port_guard = None;
    }

    if actual_port < 1024 || actual_port > 65535 {
        eprintln!("Invalid port: {}. Must be between 1024 and 65535.", actual_port);
        return Ok(());
    }

    stop_process_by_port("REST API", actual_port)?;
    let start_time = Instant::now();
    let wait_timeout = Duration::from_secs(3);
    let poll_interval = Duration::from_millis(100);
    let mut port_freed = false;
    while start_time.elapsed() < wait_timeout {
        if is_port_free(actual_port).await {
            port_freed = true;
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }

    if !port_freed {
        eprintln!("Failed to free up port {} after killing processes. Try again.", actual_port);
        return Ok(());
    }

    println!("Starting REST API server on port {} (listening on {})...", actual_port, actual_listen_port);

    let (tx, rx) = oneshot::channel();
    let rest_args = RestArgs {
        port: Some(actual_port),
        listen_port: Some(actual_listen_port),
        api_key: None,
        storage_port: Some(DEFAULT_STORAGE_PORT),
    };

    let exe_path = get_current_exe_path()?;

    let handle = tokio::spawn(async move {
        let mut command = TokioCommand::new(&exe_path);
        command.arg("rest").arg("start");
        command.arg("--port").arg(rest_args.port.unwrap().to_string());
        command.arg("--listen-port").arg(rest_args.listen_port.unwrap().to_string());
        
        if let Some(storage_p) = rest_args.storage_port {
            command.arg("--storage-port").arg(storage_p.to_string());
        }
        if let Some(api_k) = rest_args.api_key {
            command.arg("--api-key").arg(api_k);
        }

        command.stdout(Stdio::null());
        command.stderr(Stdio::null());

        println!("Launching REST API process: {:?}", command);

        let mut child = match command.spawn() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to spawn REST API process on port {}: {}", actual_port, e);
                return;
            }
        };

        tokio::select! {
            _ = rx => {
                println!("Received shutdown signal for REST API on port {}. Attempting to terminate.", actual_port);
                if let Err(e) = child.kill().await {
                    eprintln!("Error killing REST API process on port {}: {}", actual_port, e);
                }
            }
            status = child.wait() => {
                match status {
                    Ok(s) => println!("REST API process on port {} exited with status: {:?}", actual_port, s),
                    Err(e) => eprintln!("REST API process on port {} encountered an error: {}", actual_port, e),
                }
            }
        }
        println!("REST API on port {} task finished.", actual_port);
    });

    *tx_guard = Some(tx);
    *port_guard = Some(actual_port);
    *handle_guard = Some(handle);
    println!("REST API server started on port {}. Managed by CLI.", actual_port);
    Ok(())
}

/// Stops the REST API server managed by the interactive CLI.
pub async fn stop_rest_api_interactive(
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let mut tx_guard = rest_api_shutdown_tx_opt.lock().await;
    let mut port_guard = rest_api_port_arc.lock().await;
    let mut handle_guard = rest_api_handle.lock().await;

    if let Some(port) = port_guard.take() {
        println!("Attempting to stop managed REST API server on port {}...", port);
        if let Some(tx) = tx_guard.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = handle_guard.take() {
            let _ = handle.await;
        }
        stop_process_by_port("REST API", port)?;
        println!("Managed REST API server on port {} stopped.", port);
    } else {
        println!("No REST API server managed by this CLI is currently running. Attempting external stop on default port.");
        stop_process_by_port("REST API", DEFAULT_REST_API_PORT)?;
    }
    Ok(())
}


/// Starts a storage instance and manages its lifecycle in the interactive CLI.
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);
    let actual_config_file = config_file.unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("Failed to get parent directory of server crate")
            .join("storage_daemon_server")
            .join("storage_config.yaml")
    });

    println!("Attempting to start Storage daemon on port {} with config file {:?}...", actual_port, actual_config_file);

    stop_process_by_port("Storage Daemon", actual_port)?;
    let start_time = Instant::now();
    let wait_timeout = Duration::from_secs(3);
    let poll_interval = Duration::from_millis(100);
    let mut port_freed = false;
    while start_time.elapsed() < wait_timeout {
        if is_port_free(actual_port).await {
            port_freed = true;
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }

    if !port_freed {
        eprintln!("Failed to free up storage port {} after killing processes. Try again.", actual_port);
        return Err(anyhow::anyhow!("Failed to free storage port {}", actual_port));
    }

    let exe_path = get_current_exe_path()?;

    let mut command = TokioCommand::new(&exe_path);
    command.arg("storage").arg("start");
    command.arg("--port").arg(actual_port.to_string());
    command.arg("--config-file").arg(&actual_config_file);

    command.stdout(Stdio::inherit());
    command.stderr(Stdio::inherit());

    println!("Launching Storage process: {:?}", command);

    match command.spawn() {
        Ok(_) => {
            println!("Storage daemon launched. Check its logs for details.");
            println!("Storage daemon started (possibly in background) on port {}.", actual_port);
        },
        Err(e) => {
            eprintln!("Failed to spawn Storage daemon process on port {}: {}", actual_port, e);
            anyhow::bail!("Failed to start storage daemon: {}", e);
        }
    }
    Ok(())
}

/// Stops a storage instance.
pub async fn stop_storage_interactive(
    port: Option<u16>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);
    println!("Attempting to stop Storage daemon on port {}...", actual_port);
    stop_process_by_port("Storage Daemon", actual_port)?;
    println!("Storage daemon on port {} stopped (or signal sent).", actual_port);
    Ok(())
}

/// Handles the interactive 'start all' command.
pub async fn handle_start_all_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    println!("Starting all GraphDB components...");

    start_daemon_instance_interactive(port, cluster, daemon_handles).await?;
    start_rest_api_interactive(listen_port, listen_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
    start_storage_interactive(storage_port, storage_config_file).await?;

    println!("All GraphDB components start commands processed.");
    Ok(())
}


/// Stops all components managed by the interactive CLI, then attempts to stop any others.
pub async fn stop_all_interactive(
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    println!("Stopping all GraphDB components...");
    
    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;
    stop_storage_interactive(None).await?; 

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
        RestCliCommand::Start { port, listen_port } => {
            start_rest_api_interactive(port, listen_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Stop => {
            stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status => {
            display_rest_api_status().await;
            Ok(())
        }
        RestCliCommand::Health => {
            rest::api::perform_rest_api_health_check().await?;
            Ok(())
        }
        RestCliCommand::Version => {
            rest::api::get_rest_api_version().await?;
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
) -> Result<()> {
    match action {
        StorageAction::Start { port, config_file } => {
            start_storage_interactive(port, config_file).await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port).await
        }
        StorageAction::Status { port } => {
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
) -> Result<()> {
    println!("Reloading all GraphDB components...");
    
    stop_all_interactive(
        daemon_handles.clone(),
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await?;

    println!("Restarting all GraphDB components after reload...");
    start_daemon_instance_interactive(None, None, daemon_handles).await?; 
    start_rest_api_interactive(None, None, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
    start_storage_interactive(None, None).await?;

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
    let current_rest_port = *rest_api_port_arc.lock().await;

    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await?;
    
    start_rest_api_interactive(current_rest_port, current_rest_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
    
    println!("REST API server reloaded.");
    Ok(())
}

/// Handles the interactive 'reload storage' command.
pub async fn reload_storage_interactive() -> Result<()> {
    println!("Reloading standalone Storage daemon...");
    
    stop_storage_interactive(None).await?;
    start_storage_interactive(None, None).await?;
    
    println!("Standalone Storage daemon reloaded.");
    Ok(())
}

/// Handles the interactive 'reload daemon' command.
pub async fn reload_daemon_interactive(port: Option<u16>) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
    println!("Reloading GraphDB daemon on port {}...", actual_port);
    
    stop_process_by_port("GraphDB Daemon", actual_port)?;
    
    let start_time = Instant::now();
    let wait_timeout = Duration::from_secs(3);
    let poll_interval = Duration::from_millis(100);
    let mut port_freed = false;
    while start_time.elapsed() < wait_timeout {
        if is_port_free(actual_port).await {
            port_freed = true;
            break;
        }
        tokio::time::sleep(poll_interval).await;
    }

    if !port_freed {
        eprintln!("Failed to free up daemon port {} after killing processes. Reload failed.", actual_port);
        return Err(anyhow::anyhow!("Failed to free daemon port {}", actual_port));
    }

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


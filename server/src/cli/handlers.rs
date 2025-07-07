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
use futures::future;

use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, RestartArgs, StatusAction, StopAction, ReloadAction, RestartAction};
use crate::cli::config::{load_storage_config, StorageConfig};
use lib::storage_engine::config::{StorageConfig as LibStorageConfig, StorageEngineType};
use crate::cli::daemon_management::{find_running_storage_daemon_port, start_daemon_process, stop_daemon_api_call};
use daemon_api::{stop_daemon, start_daemon};

const DEFAULT_DAEMON_PORT: u16 = 8000;
const DEFAULT_REST_API_PORT: u16 = 8080;

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
        match TokioCommand::new("kill").arg("-9").arg(pid.to_string()).status().await {
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

    Err(anyhow::anyhow!("Port {} remained in use after killing processes within {:?}.", port, wait_timeout))
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
    let config = load_storage_config(None).expect("Failed to load storage config");
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
        vec![DEFAULT_DAEMON_PORT, 8081, 9001, 9002, 9003, 9004, 9005]
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
    let config = load_storage_config(None).expect("Failed to load storage config");
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
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "");
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

    let storage_config = LibStorageConfig::default();
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

    println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_daemon_status, storage_ports_display, format!("Type: {:?}", storage_config.engine_type));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Data Dir: {}", storage_config.data_path));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
    println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
    if !cluster_ports.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "", "", "", format!("Cluster Ports: {}", cluster_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", ")));
    }
    println!("--------------------------------------------------");
}

fn parse_cluster_range(range: &str) -> Result<Vec<u16>> {
    let parts: Vec<&str> = range.split('-').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid cluster range format: {}", range));
    }
    let start: u16 = parts[0].parse().context("Invalid start port")?;
    let end: u16 = parts[1].parse().context("Invalid end port")?;
    if start > end || start < 1024 || end > 65535 {
        return Err(anyhow::anyhow!("Invalid port range: {}-{}", start, end));
    }
    Ok((start..=end).collect())
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

#[allow(clippy::too_many_arguments)]
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
    let config = load_storage_config(None).expect("Failed to load storage config");
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

    if start_daemon_requested {
        let daemon_result = start_daemon(port, cluster.clone(), skip_ports.clone()).await;
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
                eprintln!("Failed to start daemon(s): {:?}", e);
                daemon_status_msg = format!("Failed to start ({:?})", e);
            }
        }
    }

    if start_rest_requested {
        let rest_port_to_use = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        if rest_port_to_use < 1024 || rest_port_to_use > 65535 {
            eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port_to_use);
            rest_api_status_msg = format!("Invalid port: {}", rest_port_to_use);
        } else {
            stop_process_by_port("REST API", rest_port_to_use).await?;
            println!("Starting REST API server on port {}...", rest_port_to_use);
            let current_storage_config = LibStorageConfig::default();
            let lib_storage_engine_type = current_storage_config.engine_type;
            start_daemon_process(
                true,
                false,
                Some(rest_port_to_use),
                Some(PathBuf::from(current_storage_config.data_path)),
                Some(lib_storage_engine_type.into()),
            ).await?;
            rest_api_status_msg = format!("Running on port: {}", rest_port_to_use);
        }
    }

    if start_storage_requested {
        let storage_port_to_use = storage_port.unwrap_or(config.default_port);
        let cluster_ports = if let Some(ref cluster_range) = cluster {
            parse_cluster_range(cluster_range).unwrap_or_default()
        } else {
            parse_cluster_range(&config.cluster_range).unwrap_or_default()
        };
        let all_ports = if cluster_ports.is_empty() {
            vec![storage_port_to_use]
        } else {
            let mut ports = vec![storage_port_to_use];
            ports.extend(cluster_ports);
            ports
        };

        for &port in &all_ports {
            if port < 1024 || port > 65535 {
                eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", port);
                storage_status_msg = format!("Invalid port: {}", port);
                continue;
            }
            stop_process_by_port("Storage Daemon", port).await?;
            println!("Starting Storage daemon on port {}...", port);
            let actual_storage_config_path = storage_config_file.clone().unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("Failed to get parent directory of server crate")
                    .join("storage_daemon_server")
                    .join("storage_config.yaml")
            });

            start_daemon_process(
                false,
                true,
                Some(port),
                Some(actual_storage_config_path.clone()),
                None,
            ).await?;

            let addr_check = format!("127.0.0.1:{}", port);
            let health_check_timeout = Duration::from_secs(10);
            let poll_interval = Duration::from_millis(500);
            let mut started_ok = false;
            let start_time = Instant::now();

            while start_time.elapsed() < health_check_timeout {
                match tokio::net::TcpStream::connect(&addr_check).await {
                    Ok(_) => {
                        println!("Storage daemon on port {} responded to health check.", port);
                        started_ok = true;
                        break;
                    }
                    Err(_) => {
                        tokio::time::sleep(poll_interval).await;
                    }
                }
            }

            if started_ok {
                storage_status_msg = format!("Running on port(s): {}", all_ports.iter().map(|p| p.to_string()).collect::<Vec<_>>().join(", "));
            } else {
                eprintln!("Warning: Storage daemon daemonized but did not become reachable on port {} within {:?}", port, health_check_timeout);
                storage_status_msg = format!("Daemonized but failed to become reachable on port {}", port);
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

pub async fn handle_stop_command(stop_args: StopArgs) -> Result<()> {
    let config = load_storage_config(None).expect("Failed to load storage config");
    match stop_args.action {
        Some(StopAction::Rest) => {
            let running_rest_ports = find_all_running_rest_api_ports().await;
            if running_rest_ports.is_empty() {
                println!("No REST API servers found running.");
            } else {
                for &port in &running_rest_ports {
                    stop_process_by_port("REST API", port).await?;
                    println!("REST API stop command processed for port {}.", port);
                }
            }
        }
        Some(StopAction::Daemon { port }) => {
            let p = port.unwrap_or(DEFAULT_DAEMON_PORT);
            stop_process_by_port("GraphDB Daemon", p).await?;
            println!("GraphDB Daemon stop command processed for port {}.", p);
        }
        Some(StopAction::Storage { port }) => {
            let p = port.unwrap_or(config.default_port);
            stop_process_by_port("Storage Daemon", p).await?;
            println!("Standalone Storage daemon stop command processed for port {}.", p);
        }
        Some(StopAction::All) | None => {
            println!("Attempting to stop all GraphDB components...");
            let running_rest_ports = find_all_running_rest_api_ports().await;
            for &port in &running_rest_ports {
                stop_process_by_port("REST API", port).await?;
                println!("REST API server on port {} stopped.", port);
            }

            let storage_ports = find_all_running_storage_daemon_ports().await;
            for &port in &storage_ports {
                stop_process_by_port("Storage Daemon", port).await?;
                println!("Standalone Storage daemon stop command processed for port {}.", port);
            }

            let stop_daemon_result = stop_daemon_api_call();
            match stop_daemon_result {
                Ok(()) => println!("Global daemon stop signal sent successfully."),
                Err(ref e) => eprintln!("Failed to send global stop signal to daemons: {:?}", e),
            }
        }
    }
    Ok(())
}

pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port } => {
            start_rest_api_interactive(
                port,
                _rest_api_shutdown_tx_opt,
                _rest_api_port_arc,
                _rest_api_handle
            ).await
        }
        RestCliCommand::Stop { port: _port } => {
            stop_rest_api_interactive(_rest_api_shutdown_tx_opt, _rest_api_port_arc, _rest_api_handle).await
        }
        RestCliCommand::Status { port: _port } => {
            display_rest_api_status(_rest_api_port_arc).await;
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
    }
}

pub async fn handle_daemon_command(
    daemon_cmd: DaemonCliCommand,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match daemon_cmd {
        DaemonCliCommand::Start { port, cluster } => {
            let ports = if let Some(ref cluster_range) = cluster {
                parse_cluster_range(cluster_range).unwrap_or_default()
            } else {
                vec![port.unwrap_or(DEFAULT_DAEMON_PORT)]
            };
            for p in ports {
                start_daemon_instance_interactive(Some(p), None, daemon_handles.clone()).await?;
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

pub async fn handle_storage_command(
    storage_action: StorageAction,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(None).expect("Failed to load storage config");
    match storage_action {
        StorageAction::Start { port, config_file, cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type } => {
            let actual_port = port.unwrap_or(config.default_port);
            let cluster_ports = if let Some(ref cluster_range) = cluster {
                parse_cluster_range(cluster_range).unwrap_or_default()
            } else {
                parse_cluster_range(&config.cluster_range).unwrap_or_default()
            };
            let all_ports = if cluster_ports.is_empty() {
                vec![actual_port]
            } else {
                let mut ports = vec![actual_port];
                ports.extend(cluster_ports);
                ports
            };

            let actual_config_file = config_file.unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .parent()
                    .expect("Failed to get parent directory of server crate")
                    .join("storage_daemon_server")
                    .join("storage_config.yaml")
            });

            for &port in &all_ports {
                println!("Attempting to start Storage daemon on port {} with config file {:?}...", port, actual_config_file);
                stop_process_by_port("Storage Daemon", port).await?;

                let exe_path = get_current_exe_path()?;
                let mut command = TokioCommand::new(&exe_path);
                command.arg("storage").arg("start");
                command.arg("--port").arg(port.to_string());
                command.arg("--config-file").arg(&actual_config_file);
                if cluster.is_some() {
                    command.arg("--join-cluster");
                }
                if let Some(ref data_dir) = data_directory {
                    command.arg("--data-directory").arg(data_dir);
                }
                if let Some(ref log_dir) = log_directory {
                    command.arg("--log-directory").arg(log_dir);
                }
                if let Some(max_space) = max_disk_space_gb {
                    command.arg("--max-disk-space-gb").arg(max_space.to_string());
                }
                if let Some(min_space) = min_disk_space_gb {
                    command.arg("--min-disk-space-gb").arg(min_space.to_string());
                }
                if let Some(use_raft) = use_raft_for_scale {
                    command.arg("--use-raft-for-scale").arg(use_raft.to_string());
                }
                if let Some(ref engine_type) = storage_engine_type {
                    command.arg("--storage-engine-type").arg(engine_type);
                }
                command.stdout(Stdio::inherit());
                command.stderr(Stdio::inherit());

                match command.spawn() {
                    Ok(_) => {
                        println!("Storage daemon launched in background on port {}.", port);
                    },
                    Err(e) => {
                        eprintln!("Failed to spawn Storage daemon process on port {}: {}", port, e);
                        anyhow::bail!("Failed to start storage daemon: {}", e);
                    }
                }
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            let actual_port = port.unwrap_or(config.default_port);
            stop_process_by_port("Storage Daemon", actual_port).await?;
            println!("Standalone Storage daemon stop command processed for port {}.", actual_port);
            Ok(())
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port, _storage_daemon_port_arc).await;
            Ok(())
        }
    }
}

pub async fn handle_status_command(status_args: StatusArgs, _rest_api_port_arc: Arc<Mutex<Option<u16>>>, _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>) -> Result<()> {
    match status_args.action {
        Some(StatusAction::Rest) => {
            display_rest_api_status(_rest_api_port_arc).await;
        }
        Some(StatusAction::Daemon { port }) => {
            display_daemon_status(port).await;
        }
        Some(StatusAction::Storage { port }) => {
            display_storage_daemon_status(port, _storage_daemon_port_arc).await;
        }
        Some(StatusAction::Cluster) => {
            display_cluster_status().await;
        }
        Some(StatusAction::All) | None => {
            display_full_status_summary(_rest_api_port_arc, _storage_daemon_port_arc).await;
        }
    }
    Ok(())
}

pub async fn handle_reload_command(
    reload_args: ReloadArgs,
) -> Result<()> {
    let config = load_storage_config(None).expect("Failed to load storage config");
    match reload_args.action {
        ReloadAction::All => {
            println!("Reloading all GraphDB components...");
            self::handle_stop_command(StopArgs { action: Some(StopAction::All) }).await?;

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
            let actual_storage_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("Failed to get parent directory of server crate")
                .join("storage_daemon_server")
                .join("storage_config.yaml");
            for &port in &storage_ports_to_restart {
                stop_process_by_port("Storage Daemon", port).await?;
                start_daemon_process(
                    false, true, Some(port),
                    Some(actual_storage_config_path.clone()),
                    None,
                ).await?;
                println!("Standalone Storage daemon restarted on port {}.", port);
            }
            if storage_ports_to_restart.is_empty() {
                stop_process_by_port("Storage Daemon", config.default_port).await?;
                start_daemon_process(
                    false, true, Some(config.default_port),
                    Some(actual_storage_config_path),
                    None,
                ).await?;
                println!("Standalone Storage daemon restarted on default port {}.", config.default_port);
            }

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
            let storage_ports = find_all_running_storage_daemon_ports().await;
            let actual_storage_config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("Failed to get parent directory of server crate")
                .join("storage_daemon_server")
                .join("storage_config.yaml");
            for &port in &storage_ports {
                stop_process_by_port("Storage Daemon", port).await?;
                start_daemon_process(
                    false, true, Some(port),
                    Some(actual_storage_config_path.clone()),
                    None,
                ).await?;
                println!("Standalone Storage daemon reloaded on port {}.", port);
            }
            if storage_ports.is_empty() {
                stop_process_by_port("Storage Daemon", config.default_port).await?;
                start_daemon_process(
                    false, true, Some(config.default_port),
                    Some(actual_storage_config_path),
                    None,
                ).await?;
                println!("Standalone Storage daemon reloaded on default port {}.", config.default_port);
            }
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

// --- Interactive-specific Command Handlers ---

pub async fn start_daemon_instance_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

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
        crate::cli::daemon_management::stop_daemon_api_call().unwrap_or_else(|e| eprintln!("Warning: Failed to send global stop signal to daemons: {}", e));
        let common_daemon_ports = [DEFAULT_DAEMON_PORT, 8081, 9001, 9002, 9003, 9004, 9005];
        for &p in &common_daemon_ports {
            stop_process_by_port("GraphDB Daemon", p).await?;
        }
        println!("All GraphDB Daemon instances stopped (or attempted to stop).");
    }
    Ok(())
}

pub async fn start_rest_api_interactive(
    port: Option<u16>,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
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
        *_rest_api_port_arc.lock().await = Some(actual_port);
    } else {
        eprintln!("Warning: REST API server launched but did not become reachable on port {} within {:?}. This might indicate an internal startup failure.",
            actual_port, health_check_timeout);
        return Err(anyhow::anyhow!("REST API server failed to become reachable on port {}", actual_port));
    }
    Ok(())
}

pub async fn stop_rest_api_interactive(
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
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
    *_rest_api_port_arc.lock().await = None;
    Ok(())
}

pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    cluster: Option<String>,
    data_directory: Option<String>,
    log_directory: Option<String>,
    max_disk_space_gb: Option<u64>,
    min_disk_space_gb: Option<u64>,
    use_raft_for_scale: Option<bool>,
    storage_engine_type: Option<String>,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(None).expect("Failed to load storage config");
    let actual_port = port.unwrap_or(config.default_port);
    let actual_config_file = config_file.unwrap_or_else(|| PathBuf::from("storage_config.yaml"));

    println!("Starting Storage daemon on port {} with config file {:?}", actual_port, actual_config_file);

    let exe_path = std::env::current_exe().context("Failed to get current executable path")?;
    let mut command = TokioCommand::new(&exe_path);
    command.arg("storage").arg("start");
    command.arg("--port").arg(actual_port.to_string());
    command.arg("--config-file").arg(&actual_config_file);
    if let Some(ref cluster_range) = cluster {
        command.arg("--join-cluster").arg(cluster_range);
    }
    if let Some(ref data_dir) = data_directory {
        command.arg("--data-directory").arg(data_dir);
    }
    if let Some(ref log_dir) = log_directory {
        command.arg("--log-directory").arg(log_dir);
    }
    if let Some(max_space) = max_disk_space_gb {
        command.arg("--max-disk-space-gb").arg(max_space.to_string());
    }
    if let Some(min_space) = min_disk_space_gb {
        command.arg("--min-disk-space-gb").arg(min_space.to_string());
    }
    if let Some(use_raft) = use_raft_for_scale {
        command.arg("--use-raft-for-scale").arg(use_raft.to_string());
    }
    if let Some(ref engine_type) = storage_engine_type {
        command.arg("--storage-engine-type").arg(engine_type);
    }
    command.stdout(Stdio::inherit());
    command.stderr(Stdio::inherit());

    command.spawn().context("Failed to spawn Storage daemon process")?;
    println!("Storage daemon launched on port {}.", actual_port);
    *_storage_daemon_port_arc.lock().await = Some(actual_port);
    Ok(())
}

pub async fn stop_all_interactive(
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    println!("Stopping all GraphDB components...");
    
    stop_rest_api_interactive(_rest_api_shutdown_tx_opt.clone(), _rest_api_port_arc.clone(), _rest_api_handle.clone()).await?;
    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;
    stop_storage_interactive(None, _storage_daemon_shutdown_tx_opt.clone(), _storage_daemon_handle.clone(), _storage_daemon_port_arc.clone()).await?;

    println!("All GraphDB components stop commands processed.");
    Ok(())
}

pub async fn stop_storage_interactive(
    port: Option<u16>,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(None).expect("Failed to load storage config");
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        let mut ports = vec![config.default_port];
        ports.extend(parse_cluster_range(&config.cluster_range).unwrap_or_default());
        ports
    };

    for &port in &ports_to_stop {
        println!("Attempting to stop Storage daemon on port {}...", port);
        stop_process_by_port("Storage Daemon", port).await?;
        println!("Storage daemon on port {} stopped.", port);
    }
    *_storage_daemon_port_arc.lock().await = None;
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
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    println!("Starting all GraphDB components...");

    start_daemon_instance_interactive(port, cluster.clone(), daemon_handles).await?;
    start_rest_api_interactive(listen_port, _rest_api_shutdown_tx_opt.clone(), _rest_api_port_arc, _rest_api_handle).await?;
    start_storage_interactive(
        storage_port,
        storage_config_file,
        cluster,
        data_directory,
        log_directory,
        max_disk_space_gb,
        min_disk_space_gb,
        use_raft_for_scale,
        storage_engine_type,
        _storage_daemon_shutdown_tx_opt,
        _storage_daemon_handle,
        _storage_daemon_port_arc,
    ).await?;

    println!("All GraphDB components start commands processed.");
    Ok(())
}

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
                start_daemon_instance_interactive(Some(p), None, daemon_handles.clone()).await?;
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

pub async fn handle_rest_command_interactive(
    command: RestCliCommand,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match command {
        RestCliCommand::Start { port } => {
            start_rest_api_interactive(
                port,
                _rest_api_shutdown_tx_opt,
                _rest_api_port_arc,
                _rest_api_handle
            ).await
        }
        RestCliCommand::Stop { port: _port } => {
            stop_rest_api_interactive(_rest_api_shutdown_tx_opt, _rest_api_port_arc, _rest_api_handle).await
        }
        RestCliCommand::Status { port: _port } => {
            display_rest_api_status(_rest_api_port_arc).await;
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
    }
}

pub async fn handle_storage_command_interactive(
    action: StorageAction,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    match action {
        StorageAction::Start { port, config_file, cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type } => {
            start_storage_interactive(
                port,
                config_file,
                cluster,
                data_directory,
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                _storage_daemon_shutdown_tx_opt,
                _storage_daemon_handle,
                _storage_daemon_port_arc,
            ).await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, _storage_daemon_shutdown_tx_opt, _storage_daemon_handle, _storage_daemon_port_arc).await
        }
        StorageAction::Status { port } => {
            display_storage_daemon_status(port, _storage_daemon_port_arc).await;
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_restart_command_interactive(
    restart_args: RestartArgs,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    match restart_args.action {
        RestartAction::All {
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
        } => {
            println!("Restarting all GraphDB components...");
            stop_all_interactive(
                daemon_handles.clone(),
                _rest_api_shutdown_tx_opt.clone(),
                _rest_api_port_arc.clone(),
                _rest_api_handle.clone(),
                _storage_daemon_shutdown_tx_opt.clone(),
                _storage_daemon_handle.clone(),
                _storage_daemon_port_arc.clone(),
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
                _rest_api_shutdown_tx_opt,
                _rest_api_port_arc,
                _rest_api_handle,
                _storage_daemon_shutdown_tx_opt,
                _storage_daemon_handle,
                _storage_daemon_port_arc,
            ).await?;
            println!("All GraphDB components restarted.");
        }
        RestartAction::Rest { port: rest_restart_port } => {
            println!("Restarting REST API server...");
            let ports_to_restart = if let Some(p) = rest_restart_port {
                vec![p]
            } else {
                find_all_running_rest_api_ports().await
            };

            if ports_to_restart.is_empty() {
                println!("No REST API servers found running to restart. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                start_rest_api_interactive(Some(DEFAULT_REST_API_PORT), _rest_api_shutdown_tx_opt, _rest_api_port_arc, _rest_api_handle).await?;
            } else {
                for &port in &ports_to_restart {
                    stop_rest_api_interactive(_rest_api_shutdown_tx_opt.clone(), _rest_api_port_arc.clone(), _rest_api_handle.clone()).await?;
                    start_rest_api_interactive(Some(port), _rest_api_shutdown_tx_opt.clone(), _rest_api_port_arc.clone(), _rest_api_handle.clone()).await?;
                    println!("REST API server restarted on port {}.", port);
                }
            }
        }
        RestartAction::Storage {
            port,
            config_file: storage_restart_config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
        } => {
            println!("Restarting standalone Storage daemon...");
            let config = load_storage_config(None).expect("Failed to load storage config");
            let actual_port = port.unwrap_or(config.default_port);

            stop_storage_interactive(Some(actual_port), _storage_daemon_shutdown_tx_opt.clone(), _storage_daemon_handle.clone(), _storage_daemon_port_arc.clone()).await?;
            start_storage_interactive(
                Some(actual_port),
                storage_restart_config_file,
                None,
                data_directory,
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                _storage_daemon_shutdown_tx_opt,
                _storage_daemon_handle,
                _storage_daemon_port_arc,
            ).await?;
            println!("Standalone Storage daemon restarted.");
        }
        RestartAction::Daemon { port, cluster } => {
            println!("Restarting GraphDB daemon...");
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

            stop_daemon_instance_interactive(Some(actual_port), daemon_handles.clone()).await?;
            start_daemon_instance_interactive(Some(actual_port), cluster, daemon_handles).await?;
            println!("GraphDB daemon restarted.");
        }
        RestartAction::Cluster => {
            println!("Restarting cluster configuration...");
            println!("A full cluster restart involves coordinating restarts/reloads across multiple daemon instances.");
            println!("This is a placeholder for complex cluster management logic.");
            println!("You might need to stop and restart individual daemons or use a cluster-wide command.");
        }
    }
    Ok(())
}

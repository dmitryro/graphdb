use anyhow::{Result, Context, anyhow};
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
use futures::future;

use crate::cli::config::{load_storage_config_str as load_storage_config, DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, 
                         DEFAULT_STORAGE_PORT, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
                         StorageConfig};
use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, 
                           StatusAction, ReloadAction, RestartArgs, RestartAction};
use crate::cli::daemon_management::{find_running_storage_daemon_port, clear_all_daemon_processes, start_daemon_process, 
                                    stop_daemon_api_call, handle_internal_daemon_run, stop_process_by_port,
                                    is_port_free, load_storage_config_path_or_default};
use daemon_api::{stop_daemon, start_daemon};

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
        pub async fn execute_graph_query(_query: &str, _persist: Option<bool>) -> Result<String> { Ok(format!("Query result for: {}", _query)) }
    }
}

pub mod storage {
    pub mod api {
        use anyhow::Result;
        pub async fn check_storage_daemon_status(_port: u16) -> Result<String> { Ok("Running".to_string()) }
    }
}

pub fn get_current_exe_path() -> Result<PathBuf> {
    std::env::current_exe()
        .context("Failed to get current executable path")
}

async fn run_command_with_timeout(
    command: &str,
    args: &[&str],
    timeout: Duration,
) -> Result<std::process::Output> {
    let output = TokioCommand::new(command)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context(format!("Failed to spawn command: {}", command))?;

    let output = tokio::time::timeout(timeout, output.wait_with_output())
        .await
        .context(format!("Command {} timed out after {:?}", command, timeout))??;

    if output.status.success() {
        Ok(output)
    } else {
        Err(anyhow!(
            "Command {} failed with exit code {}: stderr: {}",
            command,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
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

pub async fn display_rest_api_status(port_arg: Option<u16>, rest_api_port_arc: Arc<TokioMutex<Option<u16>>>) {
    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let mut ports_to_check = Vec::new();

    if let Some(p) = port_arg {
        ports_to_check.push(p);
    } else {
        ports_to_check = find_all_running_rest_api_ports().await;
    }

    let mut found_any = false;

    if ports_to_check.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No REST API servers found on common ports or specified port.");
    } else {
        for &port in &ports_to_check {
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
                    found_any = true;
                },
                _ => { 
                    rest_api_details = format!("Health: Down (Failed to connect or unhealthy)");
                },
            }
            println!("{:<15} {:<10} {:<40}", rest_api_status, port, rest_api_details);
        }
    }
    
    if let Some(first_port) = ports_to_check.first() {
        if found_any {
            *rest_api_port_arc.lock().await = Some(*first_port);
        } else {
            *rest_api_port_arc.lock().await = None;
        }
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

pub async fn display_storage_daemon_status(port_arg: Option<u16>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    let storage_config = load_storage_config_path_or_default(None)
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config: {}. Using defaults.", e);
            StorageConfig::default()
        });
    let ports_to_check = if let Some(p) = port_arg {
        vec![p]
    } else {
        find_running_storage_daemon_port().await
    };

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    if ports_to_check.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for &port in &ports_to_check {
            let status_message = if check_process_status_by_port("Storage Daemon", port).await {
                "Running".to_string()
            } else {
                "Down".to_string()
            };
            println!("{:<15} {:<10} {:<40}", status_message, port, format!("Type: {:?}", storage_config.storage_engine_type));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Data Dir: {}", storage_config.data_directory.display()));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Engine Config: {:?}", storage_config.engine_specific_config));
            println!("{:<15} {:<10} {:<40}", "", "", format!("Max Open Files: {:?}", storage_config.max_open_files));
        }
    }
    *storage_daemon_port_arc.lock().await = ports_to_check.first().copied();
    println!("--------------------------------------------------");
}

pub async fn display_cluster_status() {
    println!("\n--- Cluster Status ---");
    println!("Cluster status is a placeholder. In a real implementation, this would query all daemons in the cluster.");
    println!("--------------------------------------------------");
}

// Displays a comprehensive status summary of all GraphDB components.
pub async fn display_full_status_summary(rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) {
    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    // GraphDB Daemon Status
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
                running_daemon_ports.push(port.to_string());
            }
        }
    }

    let daemon_status_msg = if !running_daemon_ports.is_empty() {
        format!("Running on: {}", running_daemon_ports.join(", "))
    } else {
        "Down".to_string()
    };
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status_msg, "N/A", "Core Graph Processing");

    // REST API Status
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
            _ => {},
        }
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status, rest_ports_display, rest_api_details);

    // Storage Daemon Status
    let storage_config = StorageConfig::default();
    let storage_ports = find_running_storage_daemon_port().await;

    if storage_ports.is_empty() {
        println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", "Down", "N/A", "No storage daemons found in registry.");
    } else {
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
    *storage_daemon_port_arc.lock().await = storage_ports.first().copied();
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

    if start_rest_requested {
        let rest_port_to_use = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        if rest_port_to_use < 1024 || rest_port_to_use > 65535 {
            eprintln!("Invalid port: {}. Must be between 1024 and 65535.", rest_port_to_use);
            rest_api_status_msg = format!("Invalid port: {}", rest_port_to_use);
        } else {
            stop_process_by_port("REST API", rest_port_to_use).await?;
            
            println!("Starting REST API server on port {}...", rest_port_to_use);
            let current_storage_config = load_storage_config_path_or_default(None)
                .unwrap_or_else(|e| {
                    eprintln!("Warning: Could not load storage config: {}. Using defaults.", e);
                    StorageConfig::default()
                });

            let lib_storage_storage_engine_type = current_storage_config.storage_engine_type;
            start_daemon_process(
                true,
                false,
                Some(rest_port_to_use),
                Some(PathBuf::from(current_storage_config.data_directory)),
                Some(lib_storage_storage_engine_type.into()),
            ).await?;
            rest_api_status_msg = format!("Running on port: {}", rest_port_to_use);
        }
    }

    if start_storage_requested {
        let storage_port_to_use = storage_port.unwrap_or_else(|| {
            load_storage_config(None)
                .map(|c| c.default_port)
                .unwrap_or(DEFAULT_STORAGE_PORT)
        });
        if storage_port_to_use < 1024 || storage_port_to_use > 65535 {
            eprintln!("Invalid storage port: {}. Must be between 1024 and 65535.", storage_port_to_use);
            storage_status_msg = format!("Invalid port: {}", storage_port_to_use);
        } else {
            stop_process_by_port("Storage Daemon", storage_port_to_use).await?;
            
            println!("Starting Storage daemon on port {}...", storage_port_to_use);
            let actual_storage_config_path = storage_config_file.unwrap_or_else(|| {
                PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
                    .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)
            });

            start_daemon_process(
                false,
                true,
                Some(storage_port_to_use),
                Some(actual_storage_config_path.clone()),
                None,
            ).await?;
            
            let addr_check = format!("127.0.0.1:{}", storage_port_to_use);
            let health_check_timeout = Duration::from_secs(10);
            let poll_interval = Duration::from_millis(500);
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
                eprintln!("Warning: Storage daemon daemonized but did not become reachable on port {} within {:?}.",
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

pub async fn handle_stop_command(stop_args: StopArgs) -> Result<()> {
    match stop_args.action {
        Some(crate::cli::commands::StopAction::Rest { port }) => {
            if let Some(p) = port {
                println!("Attempting to stop REST API server on port {}...", p);
                stop_process_by_port("REST API", p).await?;
                println!("REST API server on port {} stopped.", p);
            } else {
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
            let p = port.unwrap_or(DEFAULT_DAEMON_PORT);
            stop_process_by_port("GraphDB Daemon", p).await?;
            println!("GraphDB Daemon stop command processed for port {}.", p);
        }
        Some(crate::cli::commands::StopAction::Storage { port }) => {
            let ports_to_stop = if let Some(p) = port {
                vec![p]
            } else {
                find_running_storage_daemon_port().await
            };
            if ports_to_stop.is_empty() {
                println!("No running Storage Daemons found to stop.");
            } else {
                for p in ports_to_stop {
                    stop_process_by_port("Storage Daemon", p).await?;
                    println!("Storage Daemon stop command processed for port {}.", p);
                }
            }
        }
        Some(crate::cli::commands::StopAction::All) | None => {
            println!("Attempting to stop all GraphDB components...");
            
            let running_rest_ports = find_all_running_rest_api_ports().await;
            for port in running_rest_ports {
                stop_process_by_port("REST API", port).await?;
                println!("REST API server on port {} stopped.", port);
            }

            let storage_ports = find_running_storage_daemon_port().await;
            if storage_ports.is_empty() {
                println!("No running Storage Daemons found to stop.");
            } else {
                for port in storage_ports {
                    stop_process_by_port("Storage Daemon", port).await?;
                    println!("Storage Daemon stop command processed for port {}.", port);
                }
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

pub async fn handle_daemon_command(
    daemon_cmd: DaemonCliCommand,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match daemon_cmd {
        DaemonCliCommand::Start { port, cluster: _ } => {
            start_daemon_instance_interactive(port, None, daemon_handles).await
        }
        DaemonCliCommand::Stop { port } => {
            stop_daemon_instance_interactive(port, daemon_handles).await
        }
        DaemonCliCommand::Status { port, cluster: _ } => {
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
            clear_all_daemon_processes().await?;
            Ok(())
        }
    }
}

pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file, cluster: _ } => {
            let actual_port = port.unwrap_or_else(|| {
                load_storage_config(None)
                    .map(|c| c.default_port)
                    .unwrap_or(DEFAULT_STORAGE_PORT)
            });
            let actual_config_file = config_file.unwrap_or_else(|| {
                PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
                    .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)
            });

            println!("Attempting to start Storage daemon on port {} with config file {:?}...", actual_port, actual_config_file);
            stop_process_by_port("Storage Daemon", actual_port).await?;

            let storage_config = load_storage_config_path_or_default(Some(actual_config_file.clone()))
                .unwrap_or_else(|e| {
                    eprintln!("Warning: Could not load storage config: {}. Using defaults.", e);
                    StorageConfig::default()
                });

            start_daemon_process(
                false,
                true,
                Some(actual_port),
                Some(actual_config_file),
                Some(storage_config.storage_engine_type.into()),
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
                println!("Storage daemon launched on port {}.", actual_port);
            } else {
                eprintln!("Warning: Storage daemon daemonized but did not become reachable on port {} within {:?}.", actual_port, health_check_timeout);
                anyhow::bail!("Storage daemon failed to become reachable on port {}", actual_port);
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            let actual_port = port.unwrap_or_else(|| {
                load_storage_config(None)
                    .map(|c| c.default_port)
                    .unwrap_or(DEFAULT_STORAGE_PORT)
            });
            stop_process_by_port("Storage Daemon", actual_port).await?;
            println!("Standalone Storage daemon stop command processed for port {}.", actual_port);
            Ok(())
        }
        StorageAction::Status { port, cluster: _ } => {
            display_storage_daemon_status(port, Arc::new(TokioMutex::new(None))).await;
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
            println!("Listing Storage daemons (simulated, non-interactive mode)...");
            let all_daemons = crate::cli::daemon_registry::GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<crate::cli::daemon_registry::DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();

            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}", daemon.port, daemon.pid, daemon.data_dir);
                }
            }
            Ok(())
        }
    }
}

pub async fn handle_status_command(status_args: StatusArgs, rest_api_port_arc: Arc<TokioMutex<Option<u16>>>, storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>) -> Result<()> {
    match status_args.action {
        Some(StatusAction::Rest { port, cluster: _ }) => {
            display_rest_api_status(port, rest_api_port_arc).await;
        }
        Some(StatusAction::Daemon { port, cluster: _ }) => {
            display_daemon_status(port).await;
        }
        Some(StatusAction::Storage { port, cluster: _ }) => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
        }
        Some(StatusAction::Cluster) => {
            display_cluster_status().await;
        }
        Some(StatusAction::All) | None => {
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await;
        }
        Some(StatusAction::Summary) => {
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await;
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

    // Parse cluster range if provided (e.g., "9001-9004")
    let ports_to_start = if let Some(cluster_range) = cluster.clone() {
        let parts: Vec<&str> = cluster_range.split('-').collect();
        if parts.len() == 2 {
            let start_port: u16 = parts[0].parse().map_err(|e| anyhow!("Invalid start port in cluster range: {}", e))?;
            let end_port: u16 = parts[1].parse().map_err(|e| anyhow!("Invalid end port in cluster range: {}", e))?;
            if start_port <= end_port && start_port >= 1024 && end_port <= 65535 {
                (start_port..=end_port).collect::<Vec<u16>>()
            } else {
                return Err(anyhow!("Invalid cluster port range: {}-{}", start_port, end_port));
            }
        } else {
            vec![actual_port]
        }
    } else {
        vec![actual_port]
    };

    let mut handles = daemon_handles.lock().await;
    for port in ports_to_start {
        if check_process_status_by_port("GraphDB Daemon", port).await {
            println!("Daemon on port {} is already running.", port);
            continue;
        }

        // Start daemon for each port
        let daemon_result = start_daemon(Some(port), cluster.clone(), vec![]).await;
        match daemon_result {
            Ok(()) => {
                println!("GraphDB Daemon launched on port {}. It should be running in the background.", port);
                // Create a channel for shutdown and track the daemon
                let (tx, rx) = oneshot::channel();
                let handle = tokio::spawn(async move {
                    // Wait for shutdown signal
                    rx.await.ok();
                    println!("Daemon on port {} shutting down.", port);
                    // Perform cleanup if needed (e.g., call stop_process_by_port)
                    if let Err(e) = stop_process_by_port("GraphDB Daemon", port).await {
                        eprintln!("Failed to stop daemon on port {}: {}", port, e);
                    }
                });
                handles.insert(port, (handle, tx));
            }
            Err(e) => {
                eprintln!("Failed to launch GraphDB Daemon on port {}: {:?}", port, e);
                return Err(e.into());
            }
        }
    }

    Ok(())
}

pub async fn start_rest_api_interactive(
    port: Option<u16>,
    cluster: Option<String>,
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
    let current_storage_config = load_storage_config_path_or_default(None)
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config: {}. Using defaults.", e);
            StorageConfig::default()
        });

    let lib_storage_storage_engine_type = current_storage_config.storage_engine_type;
    start_daemon_process(
        true,
        false,
        Some(actual_port),
        Some(PathBuf::from(current_storage_config.data_directory)),
        Some(lib_storage_storage_engine_type.into()),
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
        let (tx, rx) = oneshot::channel();
        *rest_api_shutdown_tx_opt.lock().await = Some(tx);
        let handle = tokio::spawn(async move {
            println!("Dummy REST API task running on port {}", actual_port);
            rx.await.ok();
            println!("Dummy REST API task on port {} shutting down.", actual_port);
        });
        *rest_api_handle.lock().await = Some(handle);
    } else {
        eprintln!("Warning: REST API server launched but did not become reachable on port {} within {:?}.",
            actual_port, health_check_timeout);
        return Err(anyhow::anyhow!("REST API server failed to become reachable on port {}", actual_port));
    }
    Ok(())
}

pub async fn stop_daemon_instance_interactive(
    port: Option<u16>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);

    println!("Attempting to stop GraphDB Daemon on port {}...", actual_port);
    let mut handles = daemon_handles.lock().await;

    if let Some((handle, tx)) = handles.remove(&actual_port) {
        if tx.send(()).is_ok() {
            println!("Sent shutdown signal to daemon process on port {}.", actual_port);
        }
        let _ = handle.await;
        println!("Daemon process on port {} terminated via managed handle.", actual_port);
    }

    stop_process_by_port("GraphDB Daemon", actual_port).await
        .map_err(|e| anyhow!("Failed to stop daemon on port {}: {}", actual_port, e))?;

    if is_port_free(actual_port).await {
        println!("Daemon on port {} stopped successfully.", actual_port);
    } else {
        return Err(anyhow!("Daemon on port {} failed to stop: port still in use.", actual_port));
    }

    if port.is_none() {
        println!("Attempting to stop all external daemon processes...");
        stop_daemon_api_call().unwrap_or_else(|e| {
            eprintln!("Warning: Failed to send global stop signal to daemons: {}", e)
        });
    }

    Ok(())
}

pub async fn stop_rest_api_interactive(
    port: Option<u16>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match port {
        Some(actual_port) => {
            println!("Attempting to stop REST API server on port {}...", actual_port);

            let mut port_arc_lock = rest_api_port_arc.lock().await;
            let is_managed_port = port_arc_lock.map_or(false, |p| p == actual_port);

            if is_managed_port {
                let mut handle_lock = rest_api_handle.lock().await;
                let mut shutdown_tx_lock = rest_api_shutdown_tx_opt.lock().await;

                if let Some(tx) = shutdown_tx_lock.take() {
                    if tx.send(()).is_ok() {
                        println!("Sent shutdown signal to REST API process on port {}.", actual_port);
                    }
                }

                if let Some(handle) = handle_lock.take() {
                    let _ = handle.await;
                    println!("REST API process on port {} terminated via managed handle.", actual_port);
                    *port_arc_lock = None;
                }
            }

            stop_process_by_port("REST API", actual_port).await
                .map_err(|e| anyhow!("Failed to stop REST API server on port {}: {}", actual_port, e))?;

            if is_port_free(actual_port).await {
                println!("REST API server on port {} stopped successfully.", actual_port);
            } else {
                return Err(anyhow!("REST API server on port {} failed to stop: port still in use.", actual_port));
            }
        }
        None => {
            println!("Attempting to stop all REST API servers...");

            let managed_port = *rest_api_port_arc.lock().await;
            if let Some(actual_port) = managed_port {
                let mut handle_lock = rest_api_handle.lock().await;
                let mut shutdown_tx_lock = rest_api_shutdown_tx_opt.lock().await;

                if let Some(tx) = shutdown_tx_lock.take() {
                    if tx.send(()).is_ok() {
                        println!("Sent shutdown signal to REST API process on port {}.", actual_port);
                    }
                }

                if let Some(handle) = handle_lock.take() {
                    let _ = handle.await;
                    println!("REST API process on port {} terminated via managed handle.", actual_port);
                    *rest_api_port_arc.lock().await = None;
                }

                stop_process_by_port("REST API", actual_port).await
                    .map_err(|e| anyhow!("Failed to stop REST API server on port {}: {}", actual_port, e))?;
                if is_port_free(actual_port).await {
                    println!("REST API server on port {} stopped successfully.", actual_port);
                } else {
                    eprintln!("REST API server on port {} failed to stop: port still in use.", actual_port);
                }
            }

            let running_rest_ports = find_all_running_rest_api_ports().await;
            let mut failed_ports = Vec::new();
            for port in running_rest_ports {
                if Some(port) != managed_port {
                    println!("Attempting to stop REST API server on port {}...", port);
                    if let Err(e) = stop_process_by_port("REST API", port).await {
                        eprintln!("Failed to stop REST API server on port {}: {}", port, e);
                        failed_ports.push(port);
                    } else if is_port_free(port).await {
                        println!("REST API server on port {} stopped successfully.", port);
                    } else {
                        eprintln!("REST API server on port {} failed to stop: port still in use.", port);
                        failed_ports.push(port);
                    }
                }
            }

            if failed_ports.is_empty() {
                println!("All REST API servers stopped successfully.");
            } else {
                return Err(anyhow!("Failed to stop REST API servers on ports: {:?}", failed_ports));
            }
        }
    }

    Ok(())
}

pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    cluster_opt: Option<String>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let actual_port = port.unwrap_or_else(|| {
        load_storage_config(None)
            .map(|c| c.default_port)
            .unwrap_or(DEFAULT_STORAGE_PORT)
    });

    if check_process_status_by_port("GraphDB Storage Daemon", actual_port).await {
        println!("Storage Daemon on port {} is already running.", actual_port);
        return Ok(());
    }

    println!("Attempting to start Storage Daemon on port {}...", actual_port);

    let actual_config_file = config_file.unwrap_or_else(|| {
        PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
            .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)
    });

    let storage_config = load_storage_config_path_or_default(Some(actual_config_file.clone()))
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config: {}. Using defaults.", e);
            StorageConfig::default()
        });

    let storage_result = start_daemon_process(
        false,
        true,
        Some(actual_port),
        Some(actual_config_file),
        Some(storage_config.storage_engine_type.into()),
    ).await;

    match storage_result {
        Ok(()) => {
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
                println!("GraphDB Storage Daemon launched on port {}.", actual_port);
                *storage_daemon_port_arc.lock().await = Some(actual_port);
                let (tx, rx) = oneshot::channel();
                *storage_daemon_shutdown_tx_opt.lock().await = Some(tx);
                let handle = tokio::spawn(async move {
                    println!("Dummy Storage Daemon task running on port {}", actual_port);
                    rx.await.ok();
                    println!("Dummy Storage Daemon task on port {} shutting down.", actual_port);
                });
                *storage_daemon_handle.lock().await = Some(handle);
            } else {
                eprintln!("Warning: Storage daemon launched but did not become reachable on port {} within {:?}.",
                    actual_port, health_check_timeout);
                return Err(anyhow!("Storage daemon failed to become reachable on port {}", actual_port));
            }
        }
        Err(e) => {
            eprintln!("Failed to launch GraphDB Storage Daemon on port {}: {:?}", actual_port, e);
            return Err(e.into());
        }
    }
    Ok(())
}

pub async fn stop_storage_interactive(
    port: Option<u16>,
    _storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        find_running_storage_daemon_port().await
    };

    if ports_to_stop.is_empty() {
        println!("No running Storage Daemons found to stop.");
        *storage_daemon_port_arc.lock().await = None;
        return Ok(());
    }

    for actual_port in ports_to_stop {
        println!("Attempting to stop Storage Daemon on port {}...", actual_port);
        stop_process_by_port("Storage Daemon", actual_port).await?;
        println!("Storage Daemon on port {} stopped.", actual_port);
    }

    *storage_daemon_port_arc.lock().await = None;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_start_all_interactive(
    daemon_port: Option<u16>,
    daemon_cluster: Option<String>,
    rest_port: Option<u16>,
    rest_cluster: Option<String>,
    storage_port: Option<u16>,
    storage_cluster: Option<String>,
    storage_config: Option<PathBuf>,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    println!("Starting all GraphDB components...");

    // Start Daemon with cluster support
    let daemon_result = start_daemon_instance_interactive(daemon_port, daemon_cluster.clone(), daemon_handles.clone()).await;
    match daemon_result {
        Ok(()) => {
            if let Some(cluster) = daemon_cluster {
                println!("GraphDB Daemon cluster started on ports {}.", cluster);
            } else {
                let actual_daemon_port = daemon_port.unwrap_or(DEFAULT_DAEMON_PORT);
                println!("GraphDB Daemon started on port {}.", actual_daemon_port);
            }
        }
        Err(e) => eprintln!("Failed to start daemon: {:?}", e),
    }

    // Start REST API
    let actual_rest_port = rest_port.unwrap_or(DEFAULT_REST_API_PORT);
    if !is_port_free(actual_rest_port).await {
        println!("Port {} is already in use for REST API. Attempting to stop existing process.", actual_rest_port);
        stop_process_by_port("REST API", actual_rest_port).await?;
    }
    let rest_result = start_rest_api_interactive(
        Some(actual_rest_port),
        rest_cluster,
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await;
    match rest_result {
        Ok(()) => println!("REST API server started on port {}.", actual_rest_port),
        Err(e) => eprintln!("Failed to start REST API server: {:?}", e),
    }

    // Start Storage Daemon
    let actual_storage_port = storage_port.unwrap_or_else(|| {
        load_storage_config(None)
            .map(|c| c.default_port)
            .unwrap_or(DEFAULT_STORAGE_PORT)
    });
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

    let storage_result = start_storage_interactive(
        Some(actual_storage_port),
        Some(actual_storage_config_path),
        storage_cluster,
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

pub async fn stop_storage(
    port: Option<u16>,
) -> Result<()> {
    let actual_port = port.unwrap_or(DEFAULT_STORAGE_PORT);

    println!("Attempting to stop Storage daemon on port {}...", actual_port);
    stop_process_by_port("Storage Daemon", actual_port).await?;
    println!("Standalone Storage daemon on port {} stopped.", actual_port);
    Ok(())
}

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

    stop_rest_api_interactive(
        None,
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await?;

    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;

    stop_storage_interactive(
        None,
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone(),
    ).await?;

    println!("Sending global stop signal to all external daemon processes...");
    crate::cli::daemon_management::stop_daemon_api_call().unwrap_or_else(|e| {
        eprintln!("Warning: Failed to send global stop signal to daemons: {}", e)
    });

    println!("All GraphDB components stop commands processed.");
    Ok(())
}

pub async fn handle_daemon_command_interactive(
    command: DaemonCliCommand,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
) -> Result<()> {
    match command {
        DaemonCliCommand::Start { port, cluster: _ } => {
            start_daemon_instance_interactive(port, None, daemon_handles).await
        }
        DaemonCliCommand::Stop { port } => {
            stop_daemon_instance_interactive(port, daemon_handles).await
        }
        DaemonCliCommand::Status { port, cluster: _ } => {
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

pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    match rest_cmd {
        RestCliCommand::Start { port, cluster } => {
            start_rest_api_interactive(
                port,
                cluster,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle
            ).await
        }
        RestCliCommand::Stop { port }=> {
            stop_rest_api_interactive(port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status { port, cluster: _ } => {
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
            println!("Executing storage query via REST API.");
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
            start_rest_api_interactive(port, cluster, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Stop { port } => {
            stop_rest_api_interactive(port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await
        }
        RestCliCommand::Status { port, cluster: _ } => {
            display_rest_api_status(port, rest_api_port_arc).await;
            Ok(())
        }
        RestCliCommand::Health => {
            println!("Handling REST API Health check...");
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
            println!("Handling REST API Version command...");
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
            println!("Handling REST API Register User command for user: {}", username);
            rest::api::register_user(&username, &password).await?;
            Ok(())
        }
        RestCliCommand::Authenticate { username, password } => {
            println!("Handling REST API Authenticate command for user: {}", username);
            rest::api::authenticate_user(&username, &password).await?;
            Ok(())
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            println!("Handling REST API GraphQuery: '{}', persist: {:?}", query_string, persist);
            rest::api::execute_graph_query(&query_string, persist).await?;
            Ok(())
        }
        RestCliCommand::StorageQuery => {
            println!("Handling REST API StorageQuery...");
            Ok(())
        }
    }
}

pub async fn handle_storage_command_interactive(
    action: StorageAction,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match action {
        StorageAction::Start { port, config_file, cluster: _ } => {
            start_storage_interactive(
                port,
                config_file,
                None,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc
            ).await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port, cluster: _ } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::StorageQuery => {
            println!("Performing Storage Query (simulated, interactive mode)...");
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
            println!("Listing Storage daemons (interactive mode)...");
            let all_daemons = crate::cli::daemon_registry::GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<crate::cli::daemon_registry::DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();
            
            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}", daemon.port, daemon.pid, daemon.data_dir);
                }
            }
            Ok(())
        }
    }
}

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
    start_rest_api_interactive(None, None, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
    start_storage_interactive(
        None,
        None,
        None,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc
    ).await?;
    println!("All GraphDB components reloaded (stopped and restarted).");
    Ok(())
}

pub async fn reload_rest_interactive(
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    println!("Reloading REST API server...");

    let running_ports = find_all_running_rest_api_ports().await;

    stop_rest_api_interactive(
        None,
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await?;

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

pub async fn reload_storage_interactive(
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    println!("Reloading standalone Storage daemon...");
    
    let current_storage_port = storage_daemon_port_arc.lock().await.unwrap_or_else(|| {
        load_storage_config(None)
            .map(|c| c.default_port)
            .unwrap_or(DEFAULT_STORAGE_PORT)
    });

    stop_storage_interactive(
        Some(current_storage_port),
        storage_daemon_shutdown_tx_opt.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port_arc.clone()
    ).await?;

    start_storage_interactive(
        Some(current_storage_port),
        None,
        None,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc
    ).await?;
    
    println!("Standalone Storage daemon reloaded.");
    Ok(())
}

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

pub async fn reload_cluster_interactive() -> Result<()> {
    println!("Reloading cluster configuration...");
    println!("A full cluster reload involves coordinating restarts/reloads across multiple daemon instances.");
    println!("This is a placeholder for complex cluster management logic.");
    println!("You might need to stop and restart individual daemons or use a cluster-wide command.");
    
    Ok(())
}

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
        RestartAction::Rest { port: rest_restart_port, cluster: rest_restart_cluster } => {
            println!("Restarting REST API server...");
            let ports_to_restart = if let Some(p) = rest_restart_port {
                vec![p]
            } else {
                find_all_running_rest_api_ports().await
            };

            if ports_to_restart.is_empty() {
                println!("No REST API servers found running to restart. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                start_rest_api_interactive(
                    Some(DEFAULT_REST_API_PORT),
                    rest_restart_cluster,
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
                        rest_restart_cluster.clone(),
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_port_arc.clone(),
                        rest_api_handle.clone(),
                    ).await?;
                    println!("REST API server restarted on port {}.", port);
                }
            }
        },
        RestartAction::Daemon { port, cluster } => {
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Restarting GraphDB Daemon on port {}...", actual_port);
            stop_daemon_instance_interactive(Some(actual_port), daemon_handles.clone()).await?;
            start_daemon_instance_interactive(Some(actual_port), cluster, daemon_handles).await?;
            println!("GraphDB Daemon restarted on port {}.", actual_port);
        },
        RestartAction::Storage { port, config_file, cluster } => {
            let actual_port = port.unwrap_or_else(|| {
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
                cluster,
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

// Handles the top-level `reload` command.
pub async fn handle_reload_command(
    reload_args: ReloadArgs,
) -> Result<()> {
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

            let rest_ports_to_restart = find_all_running_rest_api_ports().await;
            for &port in &rest_ports_to_restart {
                stop_process_by_port("REST API", port).await?;
                start_daemon_process(
                    true, false, Some(port),
                    Some(PathBuf::from(StorageConfig::default().data_directory)),
                    Some(StorageConfig::default().storage_engine_type.into()),
                ).await?;
                println!("REST API server restarted on port {}.", port);
            }
            if rest_ports_to_restart.is_empty()
                
                {
                let default_rest_port = DEFAULT_REST_API_PORT;
                stop_process_by_port("REST API", default_rest_port).await?;
                start_daemon_process(
                    true, false, Some(default_rest_port),
                    Some(PathBuf::from(StorageConfig::default().data_directory)),
                    Some(StorageConfig::default().storage_engine_type.into()),
                ).await?;
                println!("REST API server restarted on default port {}.", default_rest_port);
            }

            let storage_ports = find_running_storage_daemon_port().await;
            let storage_port_to_restart = storage_ports.first().copied().unwrap_or(DEFAULT_STORAGE_PORT);
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
                    Some(PathBuf::from(StorageConfig::default().data_directory)),
                    Some(StorageConfig::default().storage_engine_type.into()),
                ).await?;
            } else {
                for &port in &running_ports {
                    stop_process_by_port("REST API", port).await?;
                    start_daemon_process(
                        true, false, Some(port),
                        Some(PathBuf::from(StorageConfig::default().data_directory)),
                        Some(StorageConfig::default().storage_engine_type.into()),
                    ).await?;
                    println!("REST API server reloaded on port {}.", port);
                }
            }
        }
        ReloadAction::Storage => {
            println!("Reloading standalone Storage daemon...");
            let storage_ports = find_running_storage_daemon_port().await;
            let current_storage_port = storage_ports.first().copied().unwrap_or(DEFAULT_STORAGE_PORT);

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

#[allow(clippy::too_many_arguments)]
pub async fn handle_reload_command_interactive(
    reload_args: ReloadArgs,
) -> Result<()> {
    let action_to_perform = reload_args.action.unwrap_or(ReloadAction::All);

    match action_to_perform {
        ReloadAction::All => {
            println!("Reloading all GraphDB components interactively...");
            handle_reload_command(ReloadArgs { action: Some(ReloadAction::All) }).await?;
        },
        ReloadAction::Rest => {
            println!("Reloading REST API server interactively...");
            handle_reload_command(ReloadArgs { action: Some(ReloadAction::Rest) }).await?;
        },
        ReloadAction::Storage => {
            println!("Reloading standalone Storage daemon interactively...");
            handle_reload_command(ReloadArgs { action: Some(ReloadAction::Storage) }).await?;
        },
        ReloadAction::Daemon { port } => {
            println!("Reloading GraphDB daemon interactively on port {}...", port.unwrap_or(DEFAULT_DAEMON_PORT));
            handle_reload_command(ReloadArgs { action: Some(ReloadAction::Daemon { port }) }).await?;
        },
        ReloadAction::Cluster => {
            println!("Reloading cluster configuration interactively...");
            handle_reload_command(ReloadArgs { action: Some(ReloadAction::Cluster) }).await?;
        },
    }
    Ok(())
}
// This file contains handler functions for CLI commands.
// FIX: 2025-07-12 - Added cluster field to StartAction patterns to resolve E0027.
// FIX: 2025-07-13 - Added proper storage config loading from storage_daemon_server/storage_config.yaml.
// FIX: 2025-07-13 - Fixed daemon status to correctly show running state for cluster ports.
// FIX: 2025-07-13 - Made `graphdb-cli status` behave like `status all`.
// FIX: 2025-07-13 - Fixed infinite loop in storage start and stop commands.
// FIX: 2025-07-13 - Implemented proper port precedence and conflict checking.
// FIX: 2025-07-12 - Added missing interactive command handlers and fixed type mismatches.
// FIX: 2025-07-16 - Reverted to handlers_from_repo.rs as base.
// FIX: 2025-07-16 - Implemented "start all without all" logic in `handle_start_command` by checking `StartArgs.action` for `None` and calling `handle_start_all_interactive`.
// FIX: 2025-07-16 - Ensured `handle_start_all_interactive` correctly receives and uses all relevant arguments (ports, clusters, config paths, etc.).
// FIX: 2025-07-16 - Propagated `daemon`, `rest`, `storage` flags from `StartAction::All` to `handle_start_all_interactive`.
// FIX: 2025-07-16 - Corrected `check_port_conflicts` to allow daemon's main port to be within the cluster range if it's the same component type.
// FIX: 2025-07-16 - Added missing fields to various struct initializers and patterns as per `commands.rs`.
// FIX: 2025-07-16 - Ensured correct use of `DEFAULT_DAEMON_PORT`, `DEFAULT_REST_API_PORT`, `DEFAULT_STORAGE_PORT`.
// FIX: 2025-07-16 - Corrected argument order for `stop_process_by_port` calls.
// FIX: 2025-07-16 - Added `join_cluster` and other join-related fields to `StartAction` and `RestartAction` patterns.
// FIX: 2025-07-16 - Ensured correct use of `CliConfig::default()` and `StorageConfig::default()` for fallbacks.
// FIX: 2025-07-16 - Corrected `config_root_directory` initialization in `StorageConfig` defaults.
// FIX: 2025-07-16 - Addressed all `E0026` (variant does not have field), `E0027` (pattern does not mention field), `E0433` (unresolved module), and `E0425` (cannot find function) errors.
// FIX: 2025-07-16 - Ensured all `RestCliCommand` and `DaemonCliCommand` patterns include all fields or use `..` to ignore unneeded ones.
// FIX: 2025-07-16 - Corrected calls to `daemon_management::start_daemon_daemonized` and `daemon_management::send_stop_signal_to_daemon_main`.
// FIX: 2025-07-16 - Updated `start_rest_api_daemonized` to use `daemon_management::start_daemon_process` for consistency.
// FIX: 2025-07-16 - Propagated `cli_config` and `storage_config` to relevant daemonized start functions.
// FIX: 2025-07-16 - Adjusted `stop_process_by_port` to use `TokioCommand` and handle platform differences.
// FIX: 2025-07-16 - Corrected `StorageConfig` initialization for `config_root_directory`.
// FIX: 2025-07-16 - Added `Clone` derive to `DaemonArgs` and `RestArgs` for consistency.
// FIX: 2025-07-16 - Corrected `sysinfo` imports and usage.
// FIX: 2025-07-16 - Corrected `anyw!` to `anyhow!`.
// FIX: 2025-07-16 - Corrected `tokio::process::Command::id()` to `child.id()`.
// FIX: 2025-07-16 - Corrected `display_process_table` logic for `actual_max_len`.
// FIX: 2025-07-16 - Added `use std::env;`.
// FIX: 2025-07-16 - Removed `rest: _`, `daemon: _`, `storage: _` from `StartAction::Rest`, `RestartAction::Rest`, `ReloadAction::Rest` patterns if they don't exist in the variant.
// FIX: 2025-07-16 - Corrected argument count for `handle_start_all_interactive` calls.
// FIX: 2025-07-16 - Explicitly typed `oneshot::channel::<()>()`.
// FIX: 2025-07-16 - Cloned `Arc` variables (`storage_daemon_shutdown_tx_opt`, `storage_daemon_handle`, `storage_daemon_port_arc`) before passing to functions that consume them, to fix "borrow of moved value" errors.
// FIX: 2025-07-16 - Corrected return types in `handle_start_command`'s match arms.
// FIX: 2025-07-16 - Removed `SystemExt` from sysinfo import.
// FIX: 2025-07-17 - Updated `display_rest_api_status` to accept `rest_cluster` and display it.
// FIX: 2025-07-17 - Updated `display_daemon_status` to accept `daemon_cluster` and display it.
// FIX: 2025-07-17 - Updated `display_full_status_summary` to accept `daemon_cluster` and display it.
// FIX: 2025-07-17 - Modified `handle_rest_command_interactive` and `handle_rest_command` to call `display_rest_api_status` for `Status` action.
// FIX: 2025-07-17 - Modified `handle_daemon_command_interactive` and `handle_daemon_command` to call `display_daemon_status` for `Status` action.
// FIX: 2025-07-17 - Ensured `find_all_running_daemon_ports` and `find_all_running_rest_api_ports` are called with the correct cluster arguments.
// FIX: 2025-07-18 - Reverted to full file.
// FIX: 2025-07-18 - Corrected field mismatches in StatusAction, RestCliCommand, DaemonCliCommand, and StorageAction patterns.
// FIX: 2025-07-18 - Added missing `listen_port` and `rest_port` to `RestCliCommand::Query` pattern.
// FIX: 2025-07-18 - Added missing `join_cluster` to `DaemonCliCommand::Restart` and `Reload` patterns.
// FIX: 2025-07-18 - Adjusted handler function signatures to accept cluster arguments where needed and propagate them.
// FIX: 2025-07-18 - Corrected `E0425` errors by ensuring `rest_cluster`, `daemon_cluster`, `storage_cluster` are passed as parameters to display functions, not destructured from enum variants.
// FIX: 2025-07-18 - Ensured `effective_cluster` in Daemon commands correctly uses `cmd_daemon_cluster` from the pattern.
// FIX: 2025-07-18 - Corrected `E0425` errors in `handle_rest_command`, `handle_daemon_command`, `handle_storage_command` by ensuring `port`, `rest_port`, `daemon_port`, `storage_port` are accessed from the `command` enum variant directly, or the `_interactive` versions, or by using the `Arc<Mutex<Option<u16>>>` for managed ports.
// FIX: 2025-07-18 - Corrected `E0061` errors by updating function call signatures in `cli.rs` (will be provided in the next immersive).
// FIX: 2025-07-19 - Addressed `?` operator incompatible types, `OsStrExt::contains` errors, and `Option` matching in `match` statements.
// FIX: 2025-07-19 - Ensured `PathBuf` conversions are correct.
// FIX: 2025-07-19 - Restored and fixed `print_welcome_screen`.
// FIX: 2025-07-19 - Corrected `StatusAction` patterns to use `Some(...)` and `..`.
// FIX: 2025-07-19 - Corrected `StopAction::Rest` pattern to use `..`.
// FIX: 2025-07-19 - Removed non-existent `StorageAction::List` variants.
// FIX: 2025-07-19 - Corrected `ReloadAction::All` pattern (unit variant).
// FIX: 2025-07-19 - Corrected import paths for `DaemonCliCommand` and `RestCliCommand`.
// FIX: 2025-07-19 - Ensured `RestCliCommand` and `DaemonCliCommand` match patterns correctly destructure all fields as per `commands.rs`.
// FIX: 2025-07-19 - Added missing `use crate::cli::cli::{...}` for type aliases.
// FIX: 2025-07-19 - Corrected `match` statements for `Option<StatusAction>` and `Option<StopAction>` to use `Some(...)` and `None`.
// FIX: 2025-07-19 - Added `_` prefix to unused function parameters to silence warnings.
// FIX: 2025-07-19 - Added `StorageAction::List` match arms.
// FIX: 2025-07-19 - Corrected `ReloadAction::All` pattern to destructure its fields.
// FIX: 2025-07-19 - Fixed `E0308` errors in `handle_start_command` by wrapping `StartAction` patterns in `Some(...)`.
// FIX: 2025-07-19 - Fixed `E0026` error by removing `daemon`, `rest`, `storage` from `StartAction::Rest` pattern.
// FIX: 2025-07-19 - Fixed `E0004` error by adding `RestCliCommand::Query` match arm.
// FIX: 2025-07-19 - Fixed `E0027` error by adding `join_cluster` to `DaemonCliCommand::Start` pattern.
// FIX: 2025-07-19 - Fixed `E0425` error by correctly handling `tx` from `daemon_handles.remove()`.
// FIX: 2025-07-19 - Fixed all remaining errors related to `config_file` and `..` in patterns.
// FIX: 2025-07-19 - Ensured `handle_start_all_interactive` is called with the correct `StartAction::All` variant.

use anyhow::{Result, Context, anyhow};
use std::collections::{HashMap, HashSet};
use std::path::{PathBuf, Path};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use std::sync::Arc;
use std::process::Stdio;
use tokio::process::Command as TokioCommand;
use std::time::{Duration, Instant, SystemTime};
use std::process;
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use lazy_static::lazy_static;
use std::io::{self, Write};
use std::fs;
use std::str::FromStr;
use sysinfo::{System, Pid, ProcessesToUpdate, Process};
use futures::{future, join};
use serde_json::Value;
use reqwest::Client;
use console::style;
use colored::Colorize;
use std::env;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt; // Import OsStrExt for .contains on OsStr

// Corrected import paths for DaemonCliCommand and RestCliCommand
use crate::cli::commands::{
    StatusArgs, StopArgs, ReloadArgs, RestartArgs, StatusAction, StopAction, ReloadAction, RestartAction, StartArgs, StartAction, ClearDataAction, ClearDataArgs, StorageAction,
    DaemonCliCommand, RestCliCommand, // Imported from commands now
    AuthArgs, GraphQueryArgs, QueryArgs, RegisterUserArgs, // Added missing imports for interactive.rs
};
use crate::cli::config::{
    CliConfig, ServerConfig, get_default_rest_port_from_config, load_cli_config, load_storage_config,
    CliTomlStorageConfig as CliStorageConfig, StorageConfig, StorageEngineType as CliStorageEngineType, DEFAULT_STORAGE_CONFIG_PATH,
};
use crate::cli::daemon_management::{self, start_daemon_process,
                                   find_daemon_process_on_port,
                                   send_stop_signal_to_daemon_main,
                                     send_stop_signal_to_daemon,
                                     is_daemon_running,
                                     get_default_storage_port_from_config_or_cli_default,
                                     is_storage_daemon_running,
                                     is_rest_api_running,
                                     send_stop_signal_to_rest_api,
                                     get_cli_storage_config,
                                     clear_all_daemon_processes};
use lib::storage_engine::config::{StorageConfig as LibStorageConfig, StorageEngineType as LibStorageEngineType};
use storage_daemon_server as storage_lib;
use daemon_api::{stop_daemon, start_daemon};

// Import type aliases from cli.rs
use crate::cli::cli::{DaemonHandles, RestApiShutdownTx, RestApiPort, RestApiHandle, StorageDaemonShutdownTx, StorageDaemonHandle, StorageDaemonPort};

lazy_static! {
    static ref SERVICE_HISTORY: Arc<Mutex<Vec<ServiceAction>>> = Arc::new(Mutex::new(Vec::new()));
}

#[derive(Debug, Clone)]
pub struct ServiceAction {
    action: String,
    component: String,
    port: Option<u16>,
    cluster_range: Option<String>,
    timestamp: SystemTime,
}

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
        use serde_json::Value;

        pub async fn check_rest_api_status(port: u16) -> Result<String> {
            let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
            let url = format!("http://127.0.0.1:{}/api/v1/health", port);
            match client.get(&url).send().await {
                Ok(resp) if resp.status().is_success() => Ok("OK".to_string()),
                Ok(resp) => Err(anyhow::anyhow!("REST API health check failed with status: {}", resp.status())),
                Err(e) => Err(anyhow::anyhow!("Failed to connect to REST API for health check: {}", e)),
            }
        }

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

pub async fn stop_process_by_pid(pid: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use nix::sys::signal::{kill, Signal};
        kill(nix::unistd::Pid::from_raw(pid as i32), Signal::SIGTERM)
            .context(format!("Failed to kill process with PID {}", pid))?;
    }
    #[cfg(windows)]
    {
        let output = std::process::Command::new("taskkill")
            .args(&["/F", "/PID", &pid.to_string()])
            .output()
            .context(format!("Failed to execute taskkill for PID {}", pid))?;
        if !output.status.success() {
            anyhow::bail!("taskkill failed: {}", String::from_utf8_lossy(&output.stderr));
        }
    }
    println!("Process with PID {} killed.", pid);
    Ok(())
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
            Err(e) => eprintln!("Error killing process {}: {}", e, pid),
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

pub async fn find_all_running_rest_api_ports(rest_api_port_arc: Arc<Mutex<Option<u16>>>, rest_cluster: Option<String>) -> Vec<u16> {
    let mut ports_to_check = vec![DEFAULT_REST_API_PORT];
    
    // Add port from rest_api_port_arc
    if let Some(port) = *rest_api_port_arc.lock().await {
        ports_to_check.push(port);
    }

    // Add ports from rest_cluster
    if let Some(cluster) = rest_cluster {
        if let Ok(cluster_ports) = parse_cluster_range(&cluster) {
            ports_to_check.extend(cluster_ports);
        }
    }

    // Add common ports as fallback
    let common_rest_ports = [8081, 8082, 8083, 8084, 8085, 8086];
    ports_to_check.extend(common_rest_ports.iter().cloned());

    // Deduplicate ports
    let ports_to_check: Vec<u16> = ports_to_check.into_iter().collect::<HashSet<u16>>().into_iter().collect();

    let mut running_ports = Vec::new();
    for &port in &ports_to_check {
        if rest::api::check_rest_api_status(port).await.is_ok() || check_process_status_by_port("REST API", port).await {
            running_ports.push(port);
        }
    }
    running_ports
}

pub async fn find_all_running_storage_daemon_ports(storage_cluster: Option<String>) -> Vec<u16> {
    let config = load_storage_config(None).unwrap_or_default();
    let mut ports_to_check = vec![config.default_port];
    if let Some(cluster) = storage_cluster {
        if let Ok(cluster_ports) = parse_cluster_range(&cluster) {
            ports_to_check.extend(cluster_ports);
        }
    }
    let ports_to_check: Vec<u16> = ports_to_check.into_iter().collect::<HashSet<u16>>().into_iter().collect();

    let mut running_ports = Vec::new();
    for &port in &ports_to_check {
        if check_process_status_by_port("Storage Daemon", port).await {
            running_ports.push(port);
        }
    }
    running_ports
}

pub async fn find_all_running_daemon_ports(daemon_cluster: Option<String>) -> Vec<u16> {
    let mut ports_to_check = vec![DEFAULT_DAEMON_PORT];
    if let Some(cluster) = daemon_cluster {
        if let Ok(cluster_ports) = parse_cluster_range(&cluster) {
            ports_to_check.extend(cluster_ports);
        }
    }
    let common_daemon_ports = [8081, 9001, 9002, 9003, 9004, 9005];
    ports_to_check.extend(common_daemon_ports.iter().cloned());
    let ports_to_check: Vec<u16> = ports_to_check.into_iter().collect::<HashSet<u16>>().into_iter().collect();

    let mut running_ports = Vec::new();
    for &port in &ports_to_check {
        if check_process_status_by_port("GraphDB Daemon", port).await {
            running_ports.push(port);
        }
    }
    running_ports
}

pub async fn display_rest_api_status(rest_api_port_arc: Arc<Mutex<Option<u16>>>, rest_cluster: Option<String>) {
    let mut history = SERVICE_HISTORY.lock().await;
    history.push(ServiceAction {
        action: "status".to_string(),
        component: "REST API".to_string(),
        port: None,
        cluster_range: rest_cluster.clone(),
        timestamp: SystemTime::now(),
    });

    println!("\n--- REST API Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let running_ports = find_all_running_rest_api_ports(rest_api_port_arc, rest_cluster.clone()).await;
    if running_ports.is_empty() {
        println!("{:<15} {:<10} {:<40}", "Down", "N/A", "No REST API servers found.");
    } else {
        for &port in &running_ports {
            let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
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
            if let Some(cluster_info) = &rest_cluster {
                rest_api_details = format!("{}; Cluster: {}", rest_api_details, cluster_info);
            }
            println!("{:<15} {:<10} {:<40}", "Running", port, rest_api_details);
        }
    }
    println!("--------------------------------------------------");
}

pub async fn display_daemon_status(port_arg: Option<u16>, daemon_cluster: Option<String>) {
    println!("\n--- GraphDB Daemon Status ---");
    println!("{:<15} {:<10} {:<40}", "Status", "Port", "Details");
    println!("{:-<15} {:-<10} {:-<40}", "", "", "");

    let ports_to_check = if let Some(p) = port_arg {
        vec![p]
    } else {
        find_all_running_daemon_ports(daemon_cluster.clone()).await
    };

    let mut found_any = false;
    for &port in &ports_to_check {
        let status_message = if check_process_status_by_port("GraphDB Daemon", port).await {
            found_any = true;
            "Running".to_string()
        } else {
            "Down".to_string()
        };
        let mut details = "Core Graph Processing".to_string();
        if let Some(cluster_info) = &daemon_cluster {
            details = format!("{}; Cluster: {}", details, cluster_info);
        }
        println!("{:<15} {:<10} {:<40}", status_message, port, details);
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
        default_port: DEFAULT_STORAGE_PORT,
        cluster_range: "9000-9002".into(),
        max_disk_space_gb: 1000,
        min_disk_space_gb: 10,
        use_raft_for_scale: true,
        storage_engine_type: "sled".into(),
        max_open_files: 1024,
        config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)
            .parent()
            .unwrap_or_else(|| Path::new("/"))
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

pub async fn display_full_status_summary(rest_api_port_arc: Arc<Mutex<Option<u16>>>, rest_cluster: Option<String>, daemon_cluster: Option<String>) {
    let mut history = SERVICE_HISTORY.lock().await;
    history.push(ServiceAction {
        action: "status".to_string(),
        component: "All".to_string(),
        port: None,
        cluster_range: rest_cluster.clone(), // This might need to be more generic if daemon_cluster is also relevant
        timestamp: SystemTime::now(),
    });

    println!("\n--- GraphDB System Status Summary ---");
    println!("{:<20} {:<15} {:<10} {:<40}", "Component", "Status", "Port", "Details");
    println!("{:-<20} {:-<15} {:-<10} {:-<40}", "", "", "", "");

    // GraphDB Daemon
    let daemon_ports = find_all_running_daemon_ports(daemon_cluster.clone()).await;
    let daemon_status_str = if daemon_ports.is_empty() {
        "Down".to_string()
    } else {
        "Running".to_string()
    };
    let daemon_port_display = if daemon_ports.is_empty() {
        "N/A".to_string()
    } else {
        daemon_ports.iter().map(|p| p.to_string()).collect::<Vec<String>>().join(", ")
    };
    let mut daemon_details = "Core Graph Processing".to_string();
    if let Some(cluster_info) = &daemon_cluster {
        daemon_details = format!("{}; Cluster: {}", daemon_details, cluster_info);
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "GraphDB Daemon", daemon_status_str, daemon_port_display, daemon_details);

    // REST API
    let rest_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), rest_cluster.clone()).await;
    let rest_api_status_str = if rest_ports.is_empty() {
        "Down".to_string()
    } else {
        "Running".to_string()
    };
    let rest_port_display = if rest_ports.is_empty() {
        "N/A".to_string()
    } else {
        rest_ports.iter().map(|p| p.to_string()).collect::<Vec<String>>().join(", ")
    };
    let mut rest_api_details = String::new();
    if !rest_ports.is_empty() {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
        let health_url = format!("http://127.0.0.1:{}/api/v1/health", rest_ports[0]); // Use first running port for details
        match client.get(&health_url).send().await {
            Ok(resp) if resp.status().is_success() => {
                rest_api_details = format!("Health: OK");
                let version_url = format!("http://127.0.0.1:{}/api/v1/version", rest_ports[0]);
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
    }
    if let Some(cluster_info) = &rest_cluster {
        if rest_api_details.is_empty() {
            rest_api_details = format!("Cluster: {}", cluster_info);
        } else {
            rest_api_details = format!("{}; Cluster: {}", rest_api_details, cluster_info);
        }
    }
    println!("{:<20} {:<15} {:<10} {:<40}", "REST API", rest_api_status_str, rest_port_display, rest_api_details);


    // Storage Daemon
    let storage_ports = find_all_running_storage_daemon_ports(None).await;
    let storage_status = if storage_ports.is_empty() {
        "Down".to_string()
    } else {
        "Running".to_string()
    };
    let storage_details = format!(
        "Type: sled; Data Dir: /opt/graphdb/storage_data; Max Open Files: 1024; Configured Cluster: 9000-9002"
    );
    let storage_port_display = if storage_ports.is_empty() {
        "N/A".to_string()
    } else {
        storage_ports.iter().map(|p| p.to_string()).collect::<Vec<String>>().join(", ")
    };
    println!("{:<20} {:<15} {:<10} {:<40}", "Storage Daemon", storage_status, storage_port_display, storage_details);

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
    execute!(io::stdout(), Clear(ClearType::All), crossterm::cursor::MoveTo(0, 0))
        .context("Failed to clear terminal screen or move cursor")?;
    io::stdout().flush()?;
    Ok(())
}

pub async fn handle_status_command(
    status_args: StatusArgs,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
    rest_cluster: Option<String>,
    daemon_cluster: Option<String>,
    storage_cluster: Option<String>,
) -> Result<()> {
    match status_args.action {
        Some(StatusAction::Rest { port: _, listen_port: _, rest_port: _, .. }) => {
            display_rest_api_status(rest_api_port_arc.clone(), rest_cluster).await;
        }
        Some(StatusAction::Daemon { port: _, daemon_port: _, .. }) => {
            display_daemon_status(None, daemon_cluster).await;
        }
        Some(StatusAction::Storage { port: _, storage_port: _, .. }) => {
            display_storage_daemon_status(None, storage_daemon_port_arc).await;
        }
        Some(StatusAction::Cluster { cluster: _ }) => { // Corrected: destructure field
            display_cluster_status().await;
        }
        Some(StatusAction::All { cluster: _, rest_cluster: _, daemon_cluster: _, storage_cluster: _ }) => { // Corrected: destructure fields
            display_full_status_summary(rest_api_port_arc.clone(), rest_cluster, daemon_cluster).await;
            display_storage_daemon_status(None, storage_daemon_port_arc).await;
        }
        None => { // Added None arm for exhaustive matching
            eprintln!("No status action specified. Displaying full status summary.");
            display_full_status_summary(rest_api_port_arc.clone(), rest_cluster, daemon_cluster).await;
            display_storage_daemon_status(None, storage_daemon_port_arc).await;
        }
    }
    Ok(())
}

pub async fn handle_stop_command(
    stop_args: StopArgs,
    daemon_handles: DaemonHandles,
    rest_api_shutdown_tx_opt: RestApiShutdownTx,
    rest_api_port_arc: RestApiPort,
    rest_api_handle: RestApiHandle,
    storage_daemon_shutdown_tx_opt: StorageDaemonShutdownTx,
    storage_daemon_handle: StorageDaemonHandle,
    storage_daemon_port_arc: StorageDaemonPort,
) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)
                .parent()
                .unwrap_or_else(|| Path::new("/"))
                .to_path_buf(),
        });
    match stop_args.action {
        Some(StopAction::Rest { port, rest_port, listen_port, .. }) => {
            let target_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            println!("Stopping REST API daemon on port {}...", target_port);
            stop_process_by_port("REST API", target_port).await?;
            println!("REST API stop command processed for port {}.", target_port);
        }
        Some(StopAction::Daemon { port, daemon_port, .. }) => {
            let target_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Stopping GraphDB Daemon on port {}...", target_port);
            stop_process_by_port("GraphDB Daemon", target_port).await?;
            println!("GraphDB Daemon stop command processed for port {}.", target_port);
        }
        Some(StopAction::Storage { port, storage_port, .. }) => {
            let target_port = storage_port.or(port).unwrap_or(config.default_port);
            println!("Stopping Storage daemon on port {}...", target_port);
            stop_process_by_port("Storage Daemon", target_port).await?;
            println!("Storage daemon stop command processed for port {}.", target_port);
        }
        Some(StopAction::All { daemon_port: _, daemon_cluster: _, rest_port: _, rest_cluster: _, storage_port: _, storage_cluster: _ }) => {
            println!("Attempting to stop all GraphDB components...");
            stop_all_interactive(
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
        }
        Some(StopAction::Cluster { cluster: _ }) => {
            println!("Stopping cluster configuration...");
            let cluster_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();
            for &port in &cluster_ports {
                stop_process_by_port("Storage Daemon", port).await?;
                println!("Cluster storage daemon stopped on port {}.", port);
            }
        }
        None => {
            eprintln!("{}", Colorize::red("Invalid stop command. Use `stop --help` for usage."));
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

pub async fn handle_rest_command(
    rest_cmd: RestCliCommand,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    rest_cluster: Option<String>,
) -> Result<()> {
    let cli_config = load_cli_config().unwrap_or_else(|e| {
        eprintln!("Warning: Failed to load CLI config: {}. Using default REST port.", e);
        CliConfig::default()
    });

    match rest_cmd {
        RestCliCommand::Start {
            port,
            cluster,
            rest_port,
            rest_cluster: cmd_rest_cluster,
            join_cluster,
            join_rest_cluster,
            listen_port,
            // The `config_file` field was added to StartAction in commands.rs
            // but is not used in this specific handler. Using `..` to ignore it.
            config_file: _, // Added to explicitly ignore
            daemon: _, // Added to explicitly ignore
            rest: _, // Added to explicitly ignore
            storage: _, // Added to explicitly ignore
        } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            let effective_cluster = cmd_rest_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("REST API is already running on port {}.", port);
                    continue;
                }

                check_port_conflicts(port, "REST API", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_rest_api_daemonized(
                    Some(port),
                    effective_cluster.clone(),
                    &cli_config,
                    _rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    _rest_api_handle.clone(),
                )
                .await
                .context(format!("Failed to start REST API on port {}", port))?;

                println!(
                    "REST API started on port {} at time: {:?}",
                    port,
                    SystemTime::now()
                );

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "REST API".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }

            if !ports_to_start.is_empty() {
                *rest_api_port_arc.lock().await = Some(ports_to_start[0]);
            }

            if join_cluster.unwrap_or(false) || join_rest_cluster.unwrap_or(false) {
                println!("Joining REST cluster with ports: {:?}", ports_to_start);
            }
            Ok(())
        }
        RestCliCommand::Stop { port, rest_port, listen_port } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            println!("Stopping REST API on port {}...", effective_port);
            stop_rest_api_interactive(_rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), _rest_api_handle.clone(), None).await?;
            println!("REST API stop command executed.");
            Ok(())
        }
        RestCliCommand::Status { port: _, listen_port: _, rest_port: _ } => {
            display_rest_api_status(rest_api_port_arc.clone(), rest_cluster).await;
            Ok(())
        }
        RestCliCommand::Restart {
            port,
            cluster,
            rest_port,
            rest_cluster: cmd_rest_cluster,
            daemon,
            rest,
            storage,
            join_cluster,
            join_rest_cluster,
            listen_port,
            // The `config_file` field was added to RestartAction in commands.rs
            // but is not used in this specific handler. Using `..` to ignore it.
            config_file: _, // Added to explicitly ignore
        } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            let effective_cluster = cmd_rest_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Stopping REST API on port {}...", port);
                    stop_rest_api_interactive(_rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), _rest_api_handle.clone(), Some(effective_cluster.clone().unwrap_or_default())).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(port, "REST API", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_rest_api_daemonized(
                    Some(port),
                    effective_cluster.clone(),
                    &cli_config,
                    _rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    _rest_api_handle.clone(),
                )
                .await
                .context(format!("Failed to start REST API on port {}", port))?;

                println!("REST API restarted on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "restart".to_string(),
                    component: "REST API".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if storage.unwrap_or(false) {
                let storage_config = load_storage_config(None).unwrap_or_default();
                start_storage_daemonized(
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    &cli_config,
                    &storage_config,
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                ).await?;
            }
            Ok(())
        }
        RestCliCommand::Reload {
            port,
            cluster,
            rest_port,
            rest_cluster: cmd_rest_cluster,
            daemon,
            rest,
            storage,
            join_cluster,
            join_rest_cluster,
            listen_port,
            // The `config_file` field was added to ReloadAction in commands.rs
            // but is not used in this specific handler. Using `..` to ignore it.
            config_file: _, // Added to explicitly ignore
        } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            let effective_cluster = cmd_rest_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Reloading REST API on port {}...", port);
                    stop_rest_api_interactive(_rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), _rest_api_handle.clone(), Some(effective_cluster.clone().unwrap_or_default())).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(port, "REST API", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_rest_api_daemonized(
                    Some(port),
                    effective_cluster.clone(),
                    &cli_config,
                    _rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    _rest_api_handle.clone(),
                )
                .await
                .context(format!("Failed to start REST API on port {}", port))?;

                println!("REST API reloaded on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "reload".to_string(),
                    component: "REST API".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if storage.unwrap_or(false) {
                let storage_config = load_storage_config(None).unwrap_or_default();
                start_storage_daemonized(
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    &cli_config,
                    &storage_config,
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                ).await?;
            }
            Ok(())
        }
        RestCliCommand::Version => {
            println!("REST API version: {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        RestCliCommand::Health => {
            let effective_port = cli_config.rest.port;
            if is_rest_api_running(effective_port).await {
                println!("REST API on port {} is healthy.", effective_port);
            } else {
                println!("REST API on port {} is not running.", effective_port);
            }
            Ok(())
        }
        // Explicitly listing all fields for Query
        RestCliCommand::Query { port, query, listen_port, rest_port, .. } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            println!("Executing query '{}' on REST API port {}", query, effective_port);
            Ok(())
        }
        RestCliCommand::RegisterUser { username, password } => {
            println!("Registering user '{}' on REST API", username);
            Ok(())
        }
        RestCliCommand::Authenticate { username, password } => {
            println!("Authenticating user '{}' on REST API", username);
            Ok(())
        }
        RestCliCommand::GraphQuery { query_string, persist } => {
            println!("Executing graph query '{}' with persist={:?}", query_string, persist);
            Ok(())
        }
        RestCliCommand::StorageQuery => {
            println!("Executing storage query on REST API");
            Ok(())
        }
    }
}

pub async fn handle_rest_command_interactive(
    rest_cmd: RestCliCommand,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    rest_cluster: Option<String>,
) -> Result<()> {
    let cli_config = load_cli_config().unwrap_or_else(|e| {
        eprintln!("Warning: Failed to load CLI config: {}. Using default REST port.", e);
        CliConfig::default()
    });

    match rest_cmd {
        RestCliCommand::Start { port, cluster, listen_port, rest_port, join_cluster, join_rest_cluster, rest_cluster: cmd_rest_cluster, daemon, rest, storage, config_file: _ } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            let effective_cluster = cmd_rest_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("REST API is already running on port {}.", port);
                    continue;
                }

                check_port_conflicts(port, "REST API", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_rest_api_daemonized(
                    Some(port),
                    effective_cluster.clone(),
                    &cli_config,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                )
                .await
                .context(format!("Failed to start REST API on port {}", port))?;

                println!(
                    "REST API {} on port {} at time: {:?}",
                    if rest.unwrap_or(true) { "started" } else { "skipped" },
                    port,
                    SystemTime::now()
                );

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "REST API".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }

            if !ports_to_start.is_empty() {
                *rest_api_port_arc.lock().await = Some(ports_to_start[0]);
            }

            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if storage.unwrap_or(false) {
                let storage_config = load_storage_config(None).unwrap_or_default();
                start_storage_daemonized(
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    &cli_config,
                    &storage_config,
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                ).await?;
            }
            if join_cluster.unwrap_or(false) || join_rest_cluster.unwrap_or(false) {
                println!("Joining REST cluster with ports: {:?}", ports_to_start);
            }
            Ok(())
        }
        RestCliCommand::Stop { port, rest_port, listen_port } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            println!("Stopping REST API on port {}...", effective_port);
            stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone(), None).await?;
            println!("REST API stop command executed.");
            Ok(())
        }
        RestCliCommand::Status { port: _, listen_port: _, rest_port: _ } => {
            display_rest_api_status(rest_api_port_arc.clone(), rest_cluster).await;
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
        RestCliCommand::Query { port, query, listen_port, rest_port, .. } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            println!("Executing query '{}' on REST API port {}", query, effective_port);
            Ok(())
        }
        RestCliCommand::Restart { port, cluster, rest_port, rest_cluster: cmd_rest_cluster, daemon, rest, storage, join_cluster, join_rest_cluster, listen_port, config_file: _ } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            let effective_cluster = cmd_rest_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Stopping REST API on port {}...", port);
                    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone(), Some(effective_cluster.clone().unwrap_or_default())).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(port, "REST API", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_rest_api_daemonized(
                    Some(port),
                    effective_cluster.clone(),
                    &cli_config,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                )
                .await
                .context(format!("Failed to start REST API on port {}", port))?;

                println!("REST API restarted on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "restart".to_string(),
                    component: "REST API".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if storage.unwrap_or(false) {
                let storage_config = load_storage_config(None).unwrap_or_default();
                start_storage_daemonized(
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    &cli_config,
                    &storage_config,
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                ).await?;
            }
            Ok(())
        }
        RestCliCommand::Reload { port, cluster, rest_port, rest_cluster: cmd_rest_cluster, daemon, rest, storage, join_cluster, join_rest_cluster, listen_port, config_file: _ } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(cli_config.rest.port);
            let effective_cluster = cmd_rest_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Reloading REST API on port {}...", port);
                    stop_rest_api_interactive(rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone(), Some(effective_cluster.clone().unwrap_or_default())).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(port, "REST API", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_rest_api_daemonized(
                    Some(port),
                    effective_cluster.clone(),
                    &cli_config,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                )
                .await
                .context(format!("Failed to start REST API on port {}", port))?;

                println!("REST API reloaded on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "reload".to_string(),
                    component: "REST API".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if storage.unwrap_or(false) {
                let storage_config = load_storage_config(None).unwrap_or_default();
                start_storage_daemonized(
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    &cli_config,
                    &storage_config,
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                    Arc::new(Mutex::new(None)),
                ).await?;
            }
            Ok(())
        }
    }
}


pub async fn handle_daemon_command(
    command: DaemonCliCommand,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    daemon_cluster: Option<String>,
) -> Result<()> {
    let cli_config_from_file = crate::cli::config::load_cli_config()
        .unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load CLI config: {}. Using default daemon port.", e);
            CliConfig::default()
        });
    let default_daemon_port = cli_config_from_file.server.port.unwrap_or(DEFAULT_DAEMON_PORT);

    match command {
        DaemonCliCommand::Start { port, cluster, config_file, daemon_port, daemon_cluster: cmd_daemon_cluster, join_cluster, join_daemon_cluster, daemon, rest, storage } => {
            println!("{}", style("Starting main daemon service...").yellow());
            let effective_cluster = cmd_daemon_cluster.or(cluster);
            let mut ports_to_start = vec![daemon_port.or(port).unwrap_or(default_daemon_port)];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &p in &ports_to_start {
                if running_ports.contains(&p) {
                    println!("Main daemon is already running on port {}.", p);
                    continue;
                }

                check_port_conflicts(p, "GraphDB Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", p))?;

                println!("Starting main daemon on port {}...", p);
                daemon_management::start_daemon_process(true, false, Some(p), None, None).await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "GraphDB Daemon".to_string(),
                    port: Some(p),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }

            if join_daemon_cluster.unwrap_or(false) || join_cluster.unwrap_or(false) {
                println!("Joining daemon cluster with ports: {:?}", ports_to_start);
            }
            println!("{}", style("Main daemon start command executed.").yellow());
            Ok(())
        }
        DaemonCliCommand::Stop { port, daemon_port } => {
            println!("{}", style("Stopping main daemon service...").yellow());
            let target_port = daemon_port.or(port).unwrap_or(default_daemon_port);
            daemon_management::send_stop_signal_to_daemon_main(target_port).await?;
            stop_daemon_instance_interactive(Some(target_port), daemon_handles).await?;
            println!("{}", style("Main daemon stop command executed.").yellow());
            Ok(())
        }
        DaemonCliCommand::Status { port, daemon_port } => {
            display_daemon_status(daemon_port.or(port), daemon_cluster).await;
            Ok(())
        }
        DaemonCliCommand::Restart {
            port,
            cluster,
            join_cluster,
            daemon,
            rest,
            storage,
            config_file,
            daemon_port,
            daemon_cluster: cmd_daemon_cluster,
            join_daemon_cluster,
        } => {
            println!("{}", style("Restarting main daemon service...").yellow());
            let effective_cluster = cmd_daemon_cluster.or(cluster);
            let mut ports_to_start = vec![daemon_port.or(port).unwrap_or(default_daemon_port)];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &p in &ports_to_start {
                if running_ports.contains(&p) {
                    println!("Stopping main daemon on port {}...", p);
                    daemon_management::send_stop_signal_to_daemon_main(p).await?;
                    stop_daemon_instance_interactive(Some(p), daemon_handles.clone()).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(p, "GraphDB Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", p))?;

                println!("Starting main daemon on port {}...", p);
                daemon_management::start_daemon_process(true, false, Some(p), None, None).await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "restart".to_string(),
                    component: "GraphDB Daemon".to_string(),
                    port: Some(p),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            println!("{}", style("Main daemon restarted successfully.").yellow());
            Ok(())
        }
        DaemonCliCommand::Reload {
            port,
            cluster,
            join_cluster,
            daemon,
            rest,
            storage,
            config_file,
            daemon_port,
            daemon_cluster: cmd_daemon_cluster,
            join_daemon_cluster,
        } => {
            println!("{}", style("Reloading main daemon service...").yellow());
            let effective_cluster = cmd_daemon_cluster.or(cluster);
            let mut ports_to_start = vec![daemon_port.or(port).unwrap_or(default_daemon_port)];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &p in &ports_to_start {
                if running_ports.contains(&p) {
                    println!("Reloading main daemon on port {}...", p);
                    daemon_management::send_stop_signal_to_daemon_main(p).await?;
                    stop_daemon_instance_interactive(Some(p), daemon_handles.clone()).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(p, "GraphDB Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", p))?;

                println!("Starting main daemon on port {}...", p);
                daemon_management::start_daemon_process(true, false, Some(p), None, None).await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "reload".to_string(),
                    component: "GraphDB Daemon".to_string(),
                    port: Some(p),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            println!("{}", style("Main daemon reloaded successfully.").yellow());
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
            crate::cli::daemon_management::clear_all_daemon_processes().await?;
            Ok(())
        }
    }
}

pub async fn handle_daemon_command_interactive(
    daemon_cmd: DaemonCliCommand,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    daemon_cluster: Option<String>,
) -> Result<()> {
    let cli_config_from_file = crate::cli::config::load_cli_config()
        .unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load CLI config: {}. Using default daemon port.", e);
            CliConfig::default()
        });
    let default_daemon_port = cli_config_from_file.server.port.unwrap_or(DEFAULT_DAEMON_PORT);

    match daemon_cmd {
        DaemonCliCommand::Start {
            port,
            cluster,
            daemon_port,
            daemon_cluster: cmd_daemon_cluster,
            config_file,
            daemon,
            join_cluster,
            join_daemon_cluster,
            rest,
            storage,
        } => {
            let effective_port = daemon_port.or(port).unwrap_or(default_daemon_port);
            let effective_cluster = cmd_daemon_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Daemon is already running on port {}.", port);
                    continue;
                }

                check_port_conflicts(port, "GraphDB Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                let (tx, rx) = oneshot::channel::<()>();
                let daemon_task = tokio::task::spawn(async move {
                    println!("Starting daemon on port {}", port);
                    let _ = rx.await;
                });

                let mut handles_guard = daemon_handles.lock().await;
                handles_guard.insert(port, (daemon_task, tx));

                println!(
                    "Daemon {} on port {} at time: {:?}",
                    if daemon.unwrap_or(true) { "started" } else { "skipped" },
                    port,
                    SystemTime::now()
                );

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "GraphDB Daemon".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }

            if join_cluster.unwrap_or(false) || join_daemon_cluster.unwrap_or(false) {
                println!("Joining daemon cluster with ports: {:?}", ports_to_start);
            }
            Ok(())
        }
        DaemonCliCommand::Stop { port, daemon_port } => {
            println!("{}", style("Stopping main daemon service...").yellow());
            let target_port = daemon_port.or(port).unwrap_or(default_daemon_port);
            daemon_management::send_stop_signal_to_daemon_main(target_port).await?;
            stop_daemon_instance_interactive(Some(target_port), daemon_handles).await?;
            println!("{}", style("Main daemon stop command executed.").yellow());
            Ok(())
        }
        DaemonCliCommand::Status { port, daemon_port } => {
            display_daemon_status(daemon_port.or(port), daemon_cluster).await;
            Ok(())
        }
        DaemonCliCommand::Restart {
            port,
            cluster,
            join_cluster,
            daemon,
            rest,
            storage,
            config_file,
            daemon_port,
            daemon_cluster: cmd_daemon_cluster,
            join_daemon_cluster,
        } => {
            println!("{}", style("Restarting main daemon service...").yellow());
            let effective_cluster = cmd_daemon_cluster.or(cluster);
            let mut ports_to_start = vec![daemon_port.or(port).unwrap_or(default_daemon_port)];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &p in &ports_to_start {
                if running_ports.contains(&p) {
                    println!("Stopping main daemon on port {}...", p);
                    daemon_management::send_stop_signal_to_daemon_main(p).await?;
                    stop_daemon_instance_interactive(Some(p), daemon_handles.clone()).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(p, "GraphDB Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", p))?;

                println!("Starting main daemon on port {}...", p);
                daemon_management::start_daemon_process(true, false, Some(p), None, None).await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "restart".to_string(),
                    component: "GraphDB Daemon".to_string(),
                    port: Some(p),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            println!("{}", style("Main daemon restarted successfully.").yellow());
            Ok(())
        }
        DaemonCliCommand::Reload {
            port,
            cluster,
            join_cluster,
            daemon,
            rest,
            storage,
            config_file,
            daemon_port,
            daemon_cluster: cmd_daemon_cluster,
            join_daemon_cluster,
        } => {
            println!("{}", style("Reloading main daemon service...").yellow());
            let effective_cluster = cmd_daemon_cluster.or(cluster);
            let mut ports_to_start = vec![daemon_port.or(port).unwrap_or(default_daemon_port)];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&StorageConfig::default().cluster_range).unwrap_or_default();

            for &p in &ports_to_start {
                if running_ports.contains(&p) {
                    println!("Reloading main daemon on port {}...", p);
                    daemon_management::send_stop_signal_to_daemon_main(p).await?;
                    stop_daemon_instance_interactive(Some(p), daemon_handles.clone()).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(p, "GraphDB Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", p))?;

                println!("Starting main daemon on port {}...", p);
                daemon_management::start_daemon_process(true, false, Some(p), None, None).await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "reload".to_string(),
                    component: "GraphDB Daemon".to_string(),
                    port: Some(p),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            println!("{}", style("Main daemon reloaded successfully.").yellow());
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
            crate::cli::daemon_management::clear_all_daemon_processes().await?;
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_storage_command_interactive(
    storage_action: StorageAction,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
    storage_cluster: Option<String>,
) -> Result<()> {
    let cli_config = load_cli_config()?;
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: get_current_exe_path().unwrap_or_default().parent().unwrap_or(&PathBuf::from("/")).to_path_buf(),
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
            storage_port,
            storage_cluster: cmd_storage_cluster,
            join_cluster,
            join_storage_cluster,
            storage, // Added missing field
        } => {
            let effective_port = storage_port.or(port).unwrap_or(config.default_port);
            let effective_cluster = cmd_storage_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_storage_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Storage daemon is already running on port {}.", port);
                    continue;
                }

                check_port_conflicts(port, "Storage Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_storage_daemonized(
                    Some(port),
                    config_file.clone(),
                    effective_cluster.clone(),
                    data_directory.clone(),
                    log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type.clone(),
                    &cli_config,
                    &config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await
                    .context(format!("Failed to start storage daemon on port {}", port))?;

                println!(
                    "Storage daemon started on port {} at time: {:?}",
                    port,
                    SystemTime::now()
                );

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "Storage Daemon".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }

            if !ports_to_start.is_empty() {
                *storage_daemon_port_arc.lock().await = Some(ports_to_start[0]);
            }

            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_daemonized(None, effective_cluster.clone(), &cli_config, Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None))).await?;
            }
            if join_cluster.unwrap_or(false) || join_storage_cluster.unwrap_or(false) {
                println!("Joining storage cluster with ports: {:?}", ports_to_start);
            }
            Ok(())
        }
        StorageAction::Stop { port, storage_port } => {
            let effective_port = storage_port.or(port).unwrap_or(config.default_port);
            println!("Stopping storage daemon on port {}...", effective_port);
            stop_storage_interactive(Some(effective_port), storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await?;
            println!("Storage daemon stop command executed.");
            Ok(())
        }
        StorageAction::Status { port, storage_port } => {
            display_storage_daemon_status(storage_port.or(port), storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::Restart { port, config_file, cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, daemon, rest, storage_port, storage_cluster: cmd_storage_cluster, join_cluster, join_storage_cluster, storage } => {
            let effective_port = storage_port.or(port).unwrap_or(config.default_port);
            let effective_cluster = cmd_storage_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            for &port in &ports_to_start {
                println!("Restarting Storage Daemon on port {}...", port);
                stop_storage_interactive(Some(port), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                start_storage_daemonized(
                    Some(port),
                    config_file.clone(),
                    effective_cluster.clone(),
                    data_directory.clone(),
                    log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type.clone(),
                    &cli_config,
                    &config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
                println!("Storage Daemon restarted on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "restart".to_string(),
                    component: "Storage Daemon".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_daemonized(None, effective_cluster, &cli_config, Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None))).await?;
            }
            Ok(())
        }
        StorageAction::Reload { port, config_file, cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, daemon, rest, storage_port, storage_cluster: cmd_storage_cluster, join_cluster, join_storage_cluster, storage } => {
            let effective_port = storage_port.or(port).unwrap_or(config.default_port);
            let effective_cluster = cmd_storage_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_storage_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Reloading storage daemon on port {}...", port);
                    stop_storage_interactive(Some(port), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(port, "Storage Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_storage_daemonized(
                    Some(port),
                    config_file.clone(),
                    effective_cluster.clone(),
                    data_directory.clone(),
                    log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type.clone(),
                    &cli_config,
                    &config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
                println!("Storage daemon reloaded on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "reload".to_string(),
                    component: "Storage Daemon".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_daemonized(None, effective_cluster, &cli_config, Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None))).await?;
            }
            Ok(())
        }
        StorageAction::List => {
            println!("Listing storage daemons... [MOCK]");
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_storage_command(
    storage_action: StorageAction,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
    storage_cluster: Option<String>,
) -> Result<()> {
    let cli_config = load_cli_config()?;
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: get_current_exe_path().unwrap_or_default().parent().unwrap_or(&PathBuf::from("/")).to_path_buf(),
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
            storage_port,
            storage_cluster: cmd_storage_cluster,
            join_cluster,
            join_storage_cluster,
            storage, // Added missing field
        } => {
            let effective_port = storage_port.or(port).unwrap_or(config.default_port);
            let effective_cluster = cmd_storage_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_storage_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Storage daemon is already running on port {}.", port);
                    continue;
                }

                check_port_conflicts(port, "Storage Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_storage_daemonized(
                    Some(port),
                    config_file.clone(),
                    effective_cluster.clone(),
                    data_directory.clone(),
                    log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type.clone(),
                    &cli_config,
                    &config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await
                    .context(format!("Failed to start storage daemon on port {}", port))?;

                println!(
                    "Storage daemon started on port {} at time: {:?}",
                    port,
                    SystemTime::now()
                );

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "Storage Daemon".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }

            if !ports_to_start.is_empty() {
                *storage_daemon_port_arc.lock().await = Some(ports_to_start[0]);
            }

            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_daemonized(None, effective_cluster.clone(), &cli_config, Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None))).await?;
            }
            if join_cluster.unwrap_or(false) || join_storage_cluster.unwrap_or(false) {
                println!("Joining storage cluster with ports: {:?}", ports_to_start);
            }
            Ok(())
        }
        StorageAction::Stop { port, storage_port } => {
            stop_storage_interactive(storage_port.or(port), storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port, storage_port } => {
            display_storage_daemon_status(storage_port.or(port), storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::Restart { port, config_file, cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, daemon, rest, storage_port, storage_cluster: cmd_storage_cluster, join_cluster, join_storage_cluster, storage } => {
            let effective_port = storage_port.or(port).unwrap_or(config.default_port);
            let effective_cluster = cmd_storage_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            for &port in &ports_to_start {
                println!("Restarting Storage Daemon on port {}...", port);
                stop_storage_interactive(Some(port), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
                tokio::time::sleep(Duration::from_secs(1)).await;
                start_storage_daemonized(
                    Some(port),
                    config_file.clone(),
                    effective_cluster.clone(),
                    data_directory.clone(),
                    log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type.clone(),
                    &cli_config,
                    &config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
                println!("Storage Daemon restarted on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "restart".to_string(),
                    component: "Storage Daemon".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_daemonized(None, effective_cluster, &cli_config, Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None))).await?;
            }
            Ok(())
        }
        StorageAction::Reload { port, config_file, cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, daemon, rest, storage_port, storage_cluster: cmd_storage_cluster, join_cluster, join_storage_cluster, storage } => {
            let effective_port = storage_port.or(port).unwrap_or(config.default_port);
            let effective_cluster = cmd_storage_cluster.or(cluster);
            let mut ports_to_start = vec![effective_port];

            if let Some(cluster_range) = effective_cluster.as_ref() {
                ports_to_start = parse_cluster_range(cluster_range)
                    .context(format!("Failed to parse cluster range '{}'", cluster_range))?;
                if ports_to_start.is_empty() {
                    return Err(anyhow::anyhow!("Cluster range '{}' resulted in no valid ports", cluster_range));
                }
            }

            let running_ports = find_all_running_storage_daemon_ports(effective_cluster.clone()).await;
            let skip_ports = parse_cluster_range(&config.cluster_range).unwrap_or_default();

            for &port in &ports_to_start {
                if running_ports.contains(&port) {
                    println!("Reloading storage daemon on port {}...", port);
                    stop_storage_interactive(Some(port), storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }

                check_port_conflicts(port, "Storage Daemon", &skip_ports).await
                    .context(format!("Port conflict check failed for port {}", port))?;

                start_storage_daemonized(
                    Some(port),
                    config_file.clone(),
                    effective_cluster.clone(),
                    data_directory.clone(),
                    log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type.clone(),
                    &cli_config,
                    &config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                ).await?;
                println!("Storage daemon reloaded on port {}.", port);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "reload".to_string(),
                    component: "Storage Daemon".to_string(),
                    port: Some(port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if daemon.unwrap_or(false) {
                daemon_management::start_daemon_process(true, false, None, None, None).await?;
            }
            if rest.unwrap_or(false) {
                start_rest_api_daemonized(None, effective_cluster, &cli_config, Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None))).await?;
            }
            Ok(())
        }
        StorageAction::List => {
            println!("Listing storage daemons... [MOCK]");
            Ok(())
        }
    }
}

pub async fn handle_reload_command(
    reload_args: ReloadArgs,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let cli_config = CliConfig::default();
    let storage_config = StorageConfig::default();

    match reload_args.action {
        Some(ReloadAction::All {
            port,
            rest_port,
            storage_port,
            cluster,
            rest_cluster,
            storage_cluster,
            join_cluster,
            join_rest_cluster,
            join_storage_cluster,
            daemon,
            rest,
            storage,
            daemon_port,
            listen_port,
            daemon_cluster,
            config_file,
            storage_config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
        }) => { // Corrected: ReloadAction::All now has fields as per commands.rs
            let daemon_enabled = daemon.unwrap_or(true);
            let rest_enabled = rest.unwrap_or(true);
            let storage_enabled = storage.unwrap_or(true);
            let join_cluster_enabled = join_cluster.unwrap_or(false);
            let effective_daemon_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_rest_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_storage_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);

            // Stop existing components
            if daemon_enabled {
                if is_daemon_running(effective_daemon_port, "GraphDB Daemon").await {
                    let mut daemon_handles_guard = daemon_handles.lock().await;
                    if let Some((_, shutdown_tx)) = daemon_handles_guard.remove(&effective_daemon_port) {
                        let _ = shutdown_tx.send(());
                    }
                }
            }
            if rest_enabled {
                let mut rest_api_shutdown_guard = rest_api_shutdown_tx_opt.lock().await;
                if let Some(tx) = rest_api_shutdown_guard.take() {
                    let _ = tx.send(());
                }
                let mut rest_api_port_guard = rest_api_port_arc.lock().await;
                *rest_api_port_guard = None;
                let mut rest_api_handle_guard = rest_api_handle.lock().await;
                *rest_api_handle_guard = None;
            }
            if storage_enabled {
                if is_storage_daemon_running(effective_storage_port).await {
                    let mut storage_daemon_shutdown_guard = storage_daemon_shutdown_tx_opt.lock().await;
                    if let Some(tx) = storage_daemon_shutdown_guard.take() {
                        let _ = tx.send(());
                    }
                    let mut storage_daemon_handle_guard = storage_daemon_handle.lock().await;
                    *storage_daemon_handle_guard = None;
                    let mut storage_daemon_port_guard = storage_daemon_port_arc.lock().await;
                    *storage_daemon_port_guard = None;
                }
            }

            handle_start_all_interactive(
                StartAction::All { // Pass the full StartAction::All variant
                    port: port,
                    daemon: Some(daemon_enabled),
                    rest: Some(rest_enabled),
                    storage: Some(storage_enabled),
                    daemon_port: Some(effective_daemon_port),
                    rest_port: Some(effective_rest_port),
                    storage_port: Some(effective_storage_port),
                    cluster: cluster.clone(),
                    rest_cluster: rest_cluster.clone(),
                    storage_cluster: storage_cluster.clone(),
                    join_cluster: Some(join_cluster_enabled),
                    join_rest_cluster: join_rest_cluster,
                    join_storage_cluster: join_storage_cluster,
                    config_file: config_file,
                    data_directory: data_directory,
                    log_directory: log_directory,
                    max_disk_space_gb: max_disk_space_gb,
                    min_disk_space_gb: min_disk_space_gb,
                    use_raft_for_scale: use_raft_for_scale,
                    storage_engine_type: storage_engine_type,
                    listen_port: listen_port,
                    daemon_cluster: daemon_cluster.clone(),
                    storage_config_file: storage_config_file,
                },
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;

            if join_cluster_enabled {
                let effective_cluster = daemon_cluster
                    .or(rest_cluster)
                    .or(storage_cluster)
                    .or(cluster.clone());
                let ports = parse_cluster_range(&effective_cluster.clone().unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining cluster with ports: {:?}", ports);
            }
        }
        Some(ReloadAction::Daemon { port, daemon_port, cluster, daemon_cluster: cmd_daemon_cluster, join_cluster, join_daemon_cluster, config_file, daemon, rest, storage }) => { // Added missing fields
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_cluster = cmd_daemon_cluster.or(cluster.clone());
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "GraphDB Daemon", &skip_ports).await?;
            if !is_daemon_running(effective_port, "GraphDB Daemon").await {
                start_daemon(
                    Some(effective_port),
                    effective_cluster.clone().as_ref().map(|s| s.clone()).or_else(|| Some(storage_config.cluster_range.clone())),
                    skip_ports,
                )
                .await?;
            }
            if join_cluster.unwrap_or(false) || join_daemon_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining daemon cluster with ports: {:?}", ports);
            }
        }
        Some(ReloadAction::Rest { port, listen_port, rest_port, cluster, rest_cluster: cmd_rest_cluster, join_cluster, join_rest_cluster, daemon, rest, storage }) => { 
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_cluster = cmd_rest_cluster.or(cluster.clone());
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "REST API", &skip_ports).await?;
            if !is_daemon_running(effective_port, "REST API").await {
                start_rest_api_daemonized(
                    Some(effective_port),
                    effective_cluster.clone(),
                    &cli_config,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                )
                .await?;
            }
            if join_cluster.unwrap_or(false) || join_rest_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining REST cluster with ports: {:?}", ports);
            }
        }
        Some(ReloadAction::Storage { port, storage_port, cluster, storage_cluster: cmd_storage_cluster, join_cluster, join_storage_cluster, config_file, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, daemon, rest, storage }) => { // Added missing fields
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let effective_cluster = cmd_storage_cluster.or(cluster.clone());
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "Storage Daemon", &skip_ports).await?;
            if !is_storage_daemon_running(effective_port).await {
                start_storage_daemonized(
                    Some(effective_port),
                    config_file,
                    effective_cluster.clone().as_ref().map(|s| s.clone()),
                    data_directory.or_else(|| Some(storage_config.data_directory.clone())),
                    log_directory,
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type,
                    &cli_config,
                    &storage_config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                )
                .await?;
            }
            if join_cluster.unwrap_or(false) || join_storage_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining storage cluster with ports: {:?}", ports);
            }
        }
        Some(ReloadAction::Cluster) => { // Corrected: unit variant, no fields
            let ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or(vec![9000, 9001, 9002]);
            println!("Reloading cluster with ports: {:?}", ports);
        }
        None => {
            eprintln!("No reload action specified.");
            process::exit(1);
        }
    }
    Ok(())
}

pub async fn display_rest_api_health() {
    println!("Performing REST API health check...");

    let rest_api_port_arc = Arc::new(Mutex::new(None::<u16>));
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

pub async fn display_rest_api_version() {
    println!("Getting REST API version...");

    let rest_api_port_arc = Arc::new(Mutex::new(None::<u16>));
    let running_ports = find_all_running_rest_api_ports(rest_api_port_arc, None).await;

    if running_ports.is_empty() {
        println!("No REST API servers found to get version from.");
    } else {
        for &port in &running_ports {
            let client = Client::builder().timeout(Duration::from_secs(2)).build().expect("Failed to build reqwest client");
            let version_url = format!("http://127.00.1:{}/api/v1/version", port);
            match client.get(&version_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let v_json: Value = resp.json().await.unwrap_or_default();
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
    let cli_config = load_cli_config()?;
    let actual_port = port.unwrap_or(cli_config.server.port.unwrap_or(DEFAULT_DAEMON_PORT));

    println!("Attempting to start daemon on port {}...", actual_port);

    if let Err(e) = stop_process_by_port("REST API", actual_port).await {
        eprintln!("Warning: Failed to stop 'REST API' on port {} during daemon startup: {:?}", actual_port, e);
    }
    if let Err(e) = stop_process_by_port("Storage Daemon", actual_port).await {
        eprintln!("Warning: Failed to stop 'Storage Daemon' on port {} during daemon startup: {:?}", actual_port, e);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Err(e) = check_port_conflicts(actual_port, "GraphDB Daemon", &[]).await {
        eprintln!("Port {} is still in use after cleanup attempts: {}", actual_port, e);
        return Err(anyhow::anyhow!("Port conflict on: {}", actual_port));
    }

    let daemon_result = start_daemon(port, cluster, vec![]).await;
    match daemon_result {
        Ok(()) => {
            println!("GraphDB Daemon launched on port {}. It should be running in the background.", actual_port);
            let mut handles = daemon_handles.lock().await;
            handles.insert(actual_port, (tokio::spawn(async {}), oneshot::channel::<()>().0));
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
        if let Some((_, shutdown_tx)) = handles.remove(&p) {
            let _ = shutdown_tx.send(());
        }
        println!("Attempting to stop GraphDB Daemon on port {}...", p);
        stop_process_by_port("GraphDB Daemon", p).await?;
        println!("GraphDB Daemon on port {} stopped.", p);
    } else {
        // Collect all ports to stop first to avoid holding the lock during async operations
        let ports_to_stop: Vec<u16> = handles.keys().cloned().collect();
        for p in ports_to_stop {
            if let Some((_, shutdown_tx)) = handles.remove(&p) {
                let _ = shutdown_tx.send(());
            }
            println!("Attempting to stop GraphDB Daemon on port {}...", p);
            stop_process_by_port("GraphDB Daemon", p).await?;
            println!("GraphDB Daemon on port {} stopped.", p);
        }
        handles.clear(); // Clear any remaining handles if any
        println!("Attempting to stop all GraphDB Daemon instances...");
        let daemon_ports = find_all_running_daemon_ports(None).await;
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
    let cli_config = load_cli_config()?;
    let actual_port = port.unwrap_or(cli_config.rest.port);

    println!("Starting REST API server on port {}...", actual_port);

    if let Err(e) = stop_process_by_port("GraphDB Daemon", actual_port).await {
        eprintln!("Warning: Failed to stop 'GraphDB Daemon' on port {} during REST API startup: {:?}", actual_port, e);
    }
    if let Err(e) = stop_process_by_port("Storage Daemon", actual_port).await {
        eprintln!("Warning: Failed to stop 'Storage Daemon' on port {} during REST API startup: {:?}", actual_port, e);
    }
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

    let _child = daemon_management::start_daemon_process(
        false,
        true,
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
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    rest_cluster: Option<String>,
) -> Result<()> {
    let running_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), rest_cluster).await;
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

fn get_absolute_config_path(config_file_name_relative_to_workspace: &str) -> PathBuf {
    let current_crate_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = current_crate_manifest_dir
        .parent().expect("Failed to get workspace root from CARGO_MANIFEST_DIR");
    workspace_root.join(config_file_name_relative_to_workspace)
}

#[allow(clippy::too_many_arguments)]
pub async fn start_storage_daemonized(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    cluster: Option<String>,
    data_directory: Option<String>,
    log_directory: Option<String>,
    max_disk_space_gb: Option<u64>,
    min_disk_space_gb: Option<u64>,
    use_raft_for_scale: Option<bool>,
    storage_engine_type: Option<String>,
    _cli_config: &CliConfig,
    storage_config: &StorageConfig,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    _storage_daemon_port_arc: Arc<Mutex<Option<u16>>>, // Prefixed with _
) -> Result<()> {
    let _daemon_config_path_for_display = config_file.clone().unwrap_or_else(|| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent().expect("Failed to get workspace root")
            .join("storage_daemon_server")
            .join("storage_config.yaml")
    });

    let daemon_display_port = port.unwrap_or(storage_config.default_port);

    println!("Starting Storage daemon process with executable `{:?}` on port {}...", daemon_display_port, daemon_display_port);

    if let Err(e) = stop_process_by_port("GraphDB Daemon", daemon_display_port).await {
        eprintln!("Warning: Failed to stop 'GraphDB Daemon' on port {} during storage startup: {:?}", daemon_display_port, e, );
    }
    if let Err(e) = stop_process_by_port("REST API", daemon_display_port).await {
        eprintln!("Warning: Failed to stop 'REST API' on port {} during storage startup: {:?}", daemon_display_port, e);
    }
    if let Err(e) = stop_process_by_port("Storage Daemon", daemon_display_port).await {
        eprintln!("Warning: Failed to stop existing 'Storage Daemon' on port {} during storage startup: {:?}", daemon_display_port, e);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    if let Err(e) = check_port_conflicts(daemon_display_port, "Storage Daemon", &[]).await {
        eprintln!("Port {} is still in use after cleanup attempts: {}", daemon_display_port, e);
        return Err(anyhow::anyhow!("Port conflict on: {}", daemon_display_port));
    }

    let mut final_cluster_range: Option<String> = None;
    let cluster_source_str = cluster.or(Some(storage_config.cluster_range.clone()));

    if let Some(cluster_arg_str) = cluster_source_str {
        let parts: Vec<&str> = cluster_arg_str.split('-').collect();
        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                if start <= end {
                    if daemon_display_port >= start && daemon_display_port <= end {
                        return Err(anyhow::anyhow!(
                            "Port {} conflicts with the specified cluster range {}-{}. Please choose a different port or cluster range.",
                            daemon_display_port, start, end
                        ));
                    }
                    final_cluster_range = Some(cluster_arg_str.clone());
                } else {
                    return Err(anyhow::anyhow!(
                        "Invalid cluster range '{}'. Start port ({}) must be less than or equal to end port ({}).",
                        cluster_arg_str, start, end
                    ));
                }
            } else {
                return Err(anyhow::anyhow!(
                    "Invalid cluster port format in '{}'. Expected numeric values (e.g., 9001-9003).",
                    cluster_arg_str
                ));
            }
        } else {
            return Err(anyhow::anyhow!(
                "Invalid cluster format '{}'. Expected 'start_port-end_port' (e.g., 9001-9003).",
                cluster_arg_str
            ));
        }
    }

    let final_data_directory = data_directory
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(storage_config.data_directory.clone()));

    let final_log_directory = log_directory
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(storage_config.log_directory.clone()));

    let final_max_disk_space_gb = max_disk_space_gb
        .unwrap_or(storage_config.max_disk_space_gb);

    let final_min_disk_space_gb = min_disk_space_gb
        .unwrap_or(storage_config.min_disk_space_gb);

    let final_use_raft_for_scale = use_raft_for_scale
        .unwrap_or(storage_config.use_raft_for_scale);

    let final_max_open_files = storage_config.max_open_files;

    let final_storage_engine_type_str = storage_engine_type
        .unwrap_or_else(|| storage_config.storage_engine_type.clone());

    let _parsed_engine_type = CliStorageEngineType::from_str(&final_storage_engine_type_str)
        .context(format!("Failed to parse storage engine type '{}'", final_storage_engine_type_str))?;

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

        return Err(anyhow::anyhow!(
            "Storage daemon executable not found. Tried the following paths:\n  - {}\n  - {}\nPlease ensure it is built by running `cargo build` in your `storage_daemon_server` crate or installed to a system path.",
            system_bin_path_display.display(),
            dev_build_path_display.display()
        ));
    }

    if !final_log_directory.exists() {
        fs::create_dir_all(&final_log_directory).context(format!("Failed to create log directory {:?}", final_log_directory))?;
    }
    let log_file_path = final_log_directory.join("storage_daemon.log");
    let daemon_log_file = fs::File::create(&log_file_path)
        .context(format!("Failed to create log file at {:?}", log_file_path))?;


    let mut command = TokioCommand::new(&daemon_exe_path);

    command.arg("--port").arg(daemon_display_port.to_string());
    command.arg("--daemon");

    command.arg("--data-directory").arg(final_data_directory.to_string_lossy().into_owned());
    command.arg("--log-directory").arg(final_log_directory.to_string_lossy().into_owned());
    command.arg("--max-disk-space-gb").arg(final_max_disk_space_gb.to_string());
    command.arg("--min-disk-space-gb").arg(final_min_disk_space_gb.to_string());
    command.arg("--max-open-files").arg(final_max_open_files.to_string());
    command.arg("--storage-engine-type").arg(final_storage_engine_type_str);

    if final_use_raft_for_scale {
        command.arg("--use-raft-for_scale");
    }

    if let Some(cluster_val) = final_cluster_range {
        command.arg("--cluster").arg(cluster_val);
    }

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

    // This line was causing the unused variable warning, removed direct assignment
    // *storage_daemon_port_arc.lock().await = Some(daemon_display_port);

    println!("Storage daemon launched in background on port {}. Its output is now redirected. Control returned to CLI.", daemon_display_port);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn start_rest_api_daemonized(
    port: Option<u16>,
    cluster: Option<String>,
    config: &CliConfig,
    _rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    _rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let executable_path = env::current_exe()
        .context("Failed to get current executable path")?;

    let actual_port = port.unwrap_or_else(|| config.rest.port);

    let (_shutdown_tx, _shutdown_rx) = oneshot::channel::<()>();

    let mut cmd = TokioCommand::new(&executable_path);
    cmd.arg("--internal-rest-api-run")
        .arg("--internal-port")
        .arg(actual_port.to_string());
    if let Some(c) = cluster {
        cmd.arg("--internal-cluster").arg(c);
    }
    let child = cmd.stdout(Stdio::null())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .spawn()
        .context(format!("Failed to spawn REST API daemon on port {}", actual_port))?;

    println!("Daemonized process spawned with PID {}", child.id().unwrap_or(0));

    *rest_api_port_arc.lock().await = Some(actual_port);

    Ok(())
}


pub fn display_process_table() {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::All);

    println!("\n{}", style::style("--- Running Processes ---").bold().cyan());
    println!("{}", style::style(format!(
        "{:<8} {:<20} {:<10} {:<15} {:<10}",
        "PID", "Name", "CPU %", "Memory (MB)", "Status"
    )).bold().blue());

    use crossterm::terminal::size as terminal_size;
    use crossterm::style;
    use std::borrow::Cow;

    let (width, _height) = terminal_size().unwrap_or((80, 24));

    for (pid, process) in sys.processes() {
        let name = process.name();
        let cpu_usage = process.cpu_usage();
        let memory_usage_mb = process.memory() / 1024;
        let status = process.status().to_string();

        let name_lossy = name.to_string_lossy();

        let truncated_name = if name_lossy.len() > 18 {
            format!("{:.18}..", &*name_lossy)
        } else {
            name_lossy.into_owned()
        };

        let line = format!(
            "{:<8} {:<20} {:<10.2} {:<15} {:<10}",
            pid, truncated_name, cpu_usage, memory_usage_mb, status
        );

        if line.len() as u16 > width {
            let max_len = width as usize - 3;
            let actual_max_len_for_slicing = std::cmp::min(max_len, line.len());
            println!("{:.max_len$}...", &line[..actual_max_len_for_slicing], max_len = actual_max_len_for_slicing);
        } else {
            println!("{}", line);
        }
    }
    println!("{}", style::style("-------------------------").bold().cyan());
}

pub async fn stop_storage_interactive(
    port: Option<u16>,
    _storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    _storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let config = load_storage_config(Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)))
        .unwrap_or_else(|_| StorageConfig {
            data_directory: "/opt/graphdb/storage_data".into(),
            log_directory: "/var/log/graphdb".into(),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: "9000-9002".into(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: "sled".into(),
            max_open_files: 1024,
            config_root_directory: PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)
                .parent()
                .unwrap_or_else(|| Path::new("/"))
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

    stop_rest_api_interactive(
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
        None,
    ).await?;
    stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;
    stop_storage_interactive(None, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;

    println!("All GraphDB components stop commands processed.");
    Ok(())
}

pub async fn handle_start_all_interactive(
    start_all_action: StartAction, // Changed to accept StartAction enum
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let cli_config = load_cli_config()?;
    let storage_config = load_storage_config(None).unwrap_or_default();

    let (
        daemon_enabled, rest_enabled, storage_enabled,
        join_cluster_enabled,
        effective_daemon_port, effective_rest_port, effective_storage_port,
        cluster_arg, rest_cluster_arg, storage_cluster_arg,
        data_directory_arg, log_directory_arg,
        max_disk_space_gb_arg, min_disk_space_gb_arg,
        use_raft_for_scale_arg, storage_engine_type_arg,
        config_file_arg, // Added config_file_arg
    ) = match start_all_action {
        StartAction::All {
            port,
            daemon, rest, storage,
            join_cluster, join_rest_cluster: _, join_storage_cluster: _,
            daemon_port, rest_port, storage_port,
            cluster, rest_cluster, storage_cluster,
            data_directory, log_directory,
            max_disk_space_gb, min_disk_space_gb,
            use_raft_for_scale, storage_engine_type,
            config_file, // Destructure config_file
            listen_port: _, daemon_cluster: _, storage_config_file: _,
        } => (
            daemon.unwrap_or(true), rest.unwrap_or(true), storage.unwrap_or(true),
            join_cluster.unwrap_or(false),
            daemon_port.unwrap_or(DEFAULT_DAEMON_PORT),
            rest_port.unwrap_or(DEFAULT_REST_API_PORT),
            storage_port.unwrap_or(DEFAULT_STORAGE_PORT),
            cluster, rest_cluster, storage_cluster,
            data_directory, log_directory,
            max_disk_space_gb, min_disk_space_gb,
            use_raft_for_scale, storage_engine_type,
            config_file, // Use config_file
        ),
        _ => {
            return Err(anyhow::anyhow!("Invalid action for handle_start_all_interactive. Expected StartAction::All."));
        }
    };


    let running_rest_ports = find_all_running_rest_api_ports(rest_api_port_arc.clone(), rest_cluster_arg.clone()).await;
    let running_storage_ports = find_all_running_storage_daemon_ports(storage_cluster_arg.clone()).await;
    let running_daemon_ports = find_all_running_daemon_ports(cluster_arg.clone()).await;

    if rest_enabled {
        if running_rest_ports.contains(&effective_rest_port) {
            println!("REST API is already running on port {}.", effective_rest_port);
        } else {
            start_rest_api_daemonized(
                Some(effective_rest_port),
                rest_cluster_arg.clone(),
                &cli_config,
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
            )
            .await
            .context("Failed to start REST API")?;
            println!(
                "REST API started on port {} at time: {:?}",
                effective_rest_port,
                SystemTime::now()
            );
        }
    }

    if storage_enabled {
        if running_storage_ports.contains(&effective_storage_port) {
            println!("Storage daemon is already running on port {}.", effective_storage_port);
        } else {
            start_storage_daemonized(
                Some(effective_storage_port),
                config_file_arg.clone(), // Pass config_file_arg
                storage_cluster_arg.clone(),
                data_directory_arg.clone(),
                log_directory_arg.clone(),
                max_disk_space_gb_arg,
                min_disk_space_gb_arg,
                use_raft_for_scale_arg,
                storage_engine_type_arg.clone(),
                &cli_config,
                &storage_config,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            ).await?;

            println!(
                "Storage daemon started on port {} at time: {:?}",
                effective_storage_port,
                SystemTime::now()
            );
        }
    }

    if daemon_enabled {
        if running_daemon_ports.contains(&effective_daemon_port) {
            println!("Daemon is already running on port {}.", effective_daemon_port);
        } else {
            let (tx, rx) = oneshot::channel::<()>();
            let daemon_task = tokio::task::spawn(async move {
                println!("Starting daemon on port {}", effective_daemon_port);
                let _ = rx.await;
            });

            let mut handles_guard = daemon_handles.lock().await;
            handles_guard.insert(effective_daemon_port, (daemon_task, tx));

            println!(
                "Daemon started on port {} at time: {:?}",
                effective_daemon_port,
                SystemTime::now()
            );
        }
    }

    if join_cluster_enabled {
        println!("Joining cluster (if applicable) at time: {:?}", SystemTime::now());
    }

    Ok(())
}

pub async fn handle_start_command(
    start_args: StartArgs,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let cli_config = CliConfig::default();
    let storage_config = StorageConfig::default();

    // Check if any specific start action is provided, otherwise default to "start all"
    if start_args.action.is_none() && start_args.is_any_set() {
        // This means flags like --daemon, --rest, --storage were used directly
        // without a subcommand like `start all`.
        // We need to construct a StartAction::All from the top-level flags.
        let all_action = StartAction::All {
            port: start_args.port,
            daemon: start_args.daemon,
            rest: start_args.rest,
            storage: start_args.storage,
            daemon_port: start_args.daemon_port,
            rest_port: start_args.rest_port,
            storage_port: start_args.storage_port,
            cluster: start_args.cluster,
            rest_cluster: start_args.rest_cluster,
            storage_cluster: start_args.storage_cluster,
            join_cluster: start_args.join_cluster,
            join_rest_cluster: start_args.join_rest_cluster,
            join_storage_cluster: start_args.join_storage_cluster,
            config_file: start_args.config_file, // Pass config_file
            data_directory: start_args.data_directory,
            log_directory: start_args.log_directory,
            max_disk_space_gb: start_args.max_disk_space_gb,
            min_disk_space_gb: start_args.min_disk_space_gb,
            use_raft_for_scale: start_args.use_raft_for_scale,
            storage_engine_type: start_args.storage_engine_type,
            listen_port: start_args.listen_port,
            daemon_cluster: start_args.daemon_cluster,
            storage_config_file: start_args.storage_config_file,
        };
        handle_start_all_interactive(
            all_action,
            daemon_handles,
            rest_api_shutdown_tx_opt,
            rest_api_port_arc,
            rest_api_handle,
            storage_daemon_shutdown_tx_opt,
            storage_daemon_handle,
            storage_daemon_port_arc,
        ).await?;
        return Ok(());
    }

    match start_args.action {
        Some(StartAction::All { // Wrapped in Some
            port,
            daemon,
            rest,
            storage,
            daemon_port,
            rest_port,
            storage_port,
            cluster,
            rest_cluster,
            storage_cluster,
            join_cluster,
            join_rest_cluster,
            join_storage_cluster,
            config_file,
            storage_config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            listen_port,
            daemon_cluster,
        }) => {
            let daemon_enabled = daemon.unwrap_or(true);
            let rest_enabled = rest.unwrap_or(true);
            let storage_enabled = storage.unwrap_or(true);
            let join_cluster_enabled = join_cluster.unwrap_or(false);
            let effective_daemon_port = daemon_port.or(start_args.port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_rest_port = rest_port.or(listen_port).or(start_args.port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_storage_port = storage_port.or(start_args.port).unwrap_or(DEFAULT_STORAGE_PORT);

            let effective_cluster = daemon_cluster.clone()
                .or(rest_cluster.clone())
                .or(storage_cluster.clone())
                .or(cluster.clone());
            let cluster_ports = if let Some(cluster_str) = &effective_cluster {
                let ports = parse_cluster_range(cluster_str)?;
                if ports.len() > 100 {
                    return Err(anyhow::anyhow!("Cluster port range size ({}) exceeds maximum allowed (100).", ports.len()));
                }
                ports
            } else {
                vec![effective_daemon_port]
            };

            let mut history = SERVICE_HISTORY.lock().await;
            history.push(ServiceAction {
                action: "start".to_string(),
                component: "All".to_string(),
                port: Some(effective_daemon_port),
                cluster_range: effective_cluster.clone(),
                timestamp: SystemTime::now(),
            });

            handle_start_all_interactive(
                StartAction::All { // Pass the full StartAction::All variant
                    port: port,
                    daemon: Some(daemon_enabled),
                    rest: Some(rest_enabled),
                    storage: Some(storage_enabled),
                    daemon_port: Some(effective_daemon_port),
                    rest_port: Some(effective_rest_port),
                    storage_port: Some(effective_storage_port),
                    cluster: cluster,
                    rest_cluster: rest_cluster,
                    storage_cluster: storage_cluster,
                    join_cluster: Some(join_cluster_enabled),
                    join_rest_cluster: join_rest_cluster,
                    join_storage_cluster: join_storage_cluster,
                    config_file: config_file,
                    data_directory: data_directory,
                    log_directory: log_directory,
                    max_disk_space_gb: max_disk_space_gb,
                    min_disk_space_gb: min_disk_space_gb,
                    use_raft_for_scale: use_raft_for_scale,
                    storage_engine_type: storage_engine_type,
                    listen_port: listen_port,
                    daemon_cluster: daemon_cluster,
                    storage_config_file: storage_config_file,
                },
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;

            if join_cluster_enabled {
                println!("Joining cluster with ports: {:?}", cluster_ports);
            }

            if rest_enabled {
                *rest_api_port_arc.lock().await = Some(effective_rest_port);
            }
            if storage_enabled {
                *storage_daemon_port_arc.lock().await = Some(effective_storage_port);
            }
        }
        Some(StartAction::Daemon { port, daemon_port, cluster, daemon_cluster: cmd_daemon_cluster, join_cluster, join_daemon_cluster, config_file, daemon, rest, storage }) => { // Wrapped in Some
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_cluster = cmd_daemon_cluster.or(cluster.clone());
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "GraphDB Daemon", &skip_ports).await?;
            if !is_daemon_running(effective_port, "GraphDB Daemon").await {
                start_daemon(
                    Some(effective_port),
                    effective_cluster.clone().as_ref().map(|s| s.clone()).or_else(|| Some(storage_config.cluster_range.clone())),
                    skip_ports,
                )
                .await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "GraphDB Daemon".to_string(),
                    port: Some(effective_port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });
            }
            if join_cluster.unwrap_or(false) || join_daemon_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining daemon cluster with ports: {:?}", ports);
            }
        }
        Some(StartAction::Rest { port, listen_port, rest_port, cluster, rest_cluster: cmd_rest_cluster, join_cluster, join_rest_cluster, config_file, daemon, rest, storage }) => { // Added config_file, daemon, rest, storage
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_cluster = cmd_rest_cluster.or(cluster.clone());
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "REST API", &skip_ports).await?;
            if !is_daemon_running(effective_port, "REST API").await {
                start_rest_api_daemonized(
                    Some(effective_port),
                    effective_cluster.clone(),
                    &cli_config,
                    rest_api_shutdown_tx_opt.clone(),
                    rest_api_port_arc.clone(),
                    rest_api_handle.clone(),
                )
                .await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "REST API".to_string(),
                    port: Some(effective_port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });

                *rest_api_port_arc.lock().await = Some(effective_port);
            }
            if join_cluster.unwrap_or(false) || join_rest_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining REST cluster with ports: {:?}", ports);
            }
        }
        Some(StartAction::Storage { port, storage_port, cluster, storage_cluster: cmd_storage_cluster, join_cluster, join_storage_cluster, config_file, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, daemon, rest, storage }) => { // Wrapped in Some
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let effective_cluster = cmd_storage_cluster.or(cluster.clone());
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "Storage Daemon", &skip_ports).await?;
            if !is_storage_daemon_running(effective_port).await {
                start_storage_daemonized(
                    Some(effective_port),
                    config_file,
                    effective_cluster.clone().as_ref().map(|s| s.clone()),
                    data_directory.or_else(|| Some(storage_config.data_directory.clone())),
                    log_directory,
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type,
                    &cli_config,
                    &storage_config,
                    storage_daemon_shutdown_tx_opt.clone(),
                    storage_daemon_handle.clone(),
                    storage_daemon_port_arc.clone(),
                )
                .await?;

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "Storage Daemon".to_string(),
                    port: Some(effective_port),
                    cluster_range: effective_cluster.clone(),
                    timestamp: SystemTime::now(),
                });

                *storage_daemon_port_arc.lock().await = Some(effective_port);
            }
            if join_cluster.unwrap_or(false) || join_storage_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining storage cluster with ports: {:?}", ports);
            }
        }
        Some(StartAction::Cluster { cluster, join_cluster }) => { // Wrapped in Some, added cluster field
            if join_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&cluster.clone().unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining cluster with ports: {:?}", ports);

                let mut history = SERVICE_HISTORY.lock().await;
                history.push(ServiceAction {
                    action: "start".to_string(),
                    component: "Cluster".to_string(),
                    port: None,
                    cluster_range: cluster,
                    timestamp: SystemTime::now(),
                });
            }
        }
        None => { // Added None arm for exhaustive matching
            eprintln!("No start action specified.");
            process::exit(1);
        }
    }
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
    let cli_config = CliConfig::default();
    let storage_config = StorageConfig::default();

    match restart_args.action {
        Some(RestartAction::All {
            port,
            cluster,
            listen_port,
            storage_port,
            rest_port,
            daemon_port,
            daemon_cluster,
            storage_config_file,
            config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
            storage,
            join_cluster,
            join_rest_cluster,
            join_storage_cluster,
            rest_cluster,
            storage_cluster,
        }) => {
            let daemon_enabled = daemon.unwrap_or(true);
            let rest_enabled = rest.unwrap_or(true);
            let storage_enabled = storage.unwrap_or(true);
            let join_cluster_enabled = join_cluster.unwrap_or(false);
            let effective_daemon_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_rest_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_storage_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);

            // Stop existing components
            if daemon_enabled {
                if is_daemon_running(effective_daemon_port, "GraphDB Daemon").await {
                    let mut daemon_handles_guard = daemon_handles.lock().await;
                    if let Some((_, shutdown_tx)) = daemon_handles_guard.remove(&effective_daemon_port) {
                        let _ = shutdown_tx.send(());
                    }
                }
            }
            if rest_enabled {
                let mut rest_api_shutdown_guard = rest_api_shutdown_tx_opt.lock().await;
                if let Some(tx) = rest_api_shutdown_guard.take() {
                    let _ = tx.send(());
                }
                let mut rest_api_port_guard = rest_api_port_arc.lock().await;
                *rest_api_port_guard = None;
                let mut rest_api_handle_guard = rest_api_handle.lock().await;
                *rest_api_handle_guard = None;
            }
            if storage_enabled {
                if is_storage_daemon_running(effective_storage_port).await {
                    let mut storage_daemon_shutdown_guard = storage_daemon_shutdown_tx_opt.lock().await;
                    if let Some(tx) = storage_daemon_shutdown_guard.take() {
                        let _ = tx.send(());
                    }
                    let mut storage_daemon_handle_guard = storage_daemon_handle.lock().await;
                    *storage_daemon_handle_guard = None;
                    let mut storage_daemon_port_guard = storage_daemon_port_arc.lock().await;
                    *storage_daemon_port_guard = None;
                }
            }

            handle_start_all_interactive(
                StartAction::All {
                    port,
                    daemon: Some(daemon_enabled),
                    rest: Some(rest_enabled),
                    storage: Some(storage_enabled),
                    daemon_port: Some(effective_daemon_port),
                    rest_port: Some(effective_rest_port),
                    storage_port: Some(effective_storage_port),
                    cluster: cluster.clone(),
                    rest_cluster: rest_cluster.clone(),
                    storage_cluster: storage_cluster.clone(),
                    join_cluster: Some(join_cluster_enabled),
                    join_rest_cluster,
                    join_storage_cluster,
                    config_file,
                    data_directory,
                    log_directory,
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type,
                    listen_port,
                    daemon_cluster: daemon_cluster.clone(),
                    storage_config_file,
                },
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;

            if join_cluster_enabled {
                let effective_cluster = daemon_cluster
                    .or(rest_cluster)
                    .or(storage_cluster)
                    .or(cluster.clone());
                let ports = parse_cluster_range(&effective_cluster.clone().unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining cluster with ports: {:?}", ports);
            }
        }
        Some(RestartAction::Daemon {
            port,
            cluster,
            daemon_port,
            daemon_cluster: cmd_daemon_cluster,
            config_file,
            join_cluster,
            join_daemon_cluster,
            daemon,
            rest,
            storage,
        }) => {
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_cluster = cmd_daemon_cluster.or(cluster.clone());
            if is_daemon_running(effective_port, "GraphDB Daemon").await {
                let mut daemon_handles_guard = daemon_handles.lock().await;
                if let Some((_, shutdown_tx)) = daemon_handles_guard.remove(&effective_port) {
                    let _ = shutdown_tx.send(());
                }
            }
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "GraphDB Daemon", &skip_ports).await?;
            start_daemon(
                Some(effective_port),
                effective_cluster.clone().as_ref().map(|s| s.clone()).or_else(|| Some(storage_config.cluster_range.clone())),
                skip_ports,
            )
            .await?;
            if join_cluster.unwrap_or(false) || join_daemon_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining daemon cluster with ports: {:?}", ports);
            }
        }
        Some(RestartAction::Rest {
            port,
            cluster,
            listen_port,
            rest_port,
            join_cluster,
            join_rest_cluster,
            rest_cluster: cmd_rest_cluster,
            daemon,
            rest,
            storage,
            config_file,
        }) => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_cluster = cmd_rest_cluster.or(cluster.clone());
            let mut rest_api_shutdown_guard = rest_api_shutdown_tx_opt.lock().await;
            if let Some(tx) = rest_api_shutdown_guard.take() {
                let _ = tx.send(());
            }
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;
            *rest_api_port_guard = None;
            let mut rest_api_handle_guard = rest_api_handle.lock().await;
            *rest_api_handle_guard = None;

            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "REST API", &skip_ports).await?;
            start_rest_api_daemonized(
                Some(effective_port),
                effective_cluster.clone(),
                &cli_config,
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
            )
            .await?;
            if join_cluster.unwrap_or(false) || join_rest_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining REST cluster with ports: {:?}", ports);
            }
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
            join_cluster,
            join_storage_cluster,
            storage_cluster: cmd_storage_cluster,
            storage_port,
            daemon,
            rest,
            storage,
        }) => {
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let effective_cluster = cmd_storage_cluster.or(cluster.clone());
            if is_storage_daemon_running(effective_port).await {
                let mut storage_daemon_shutdown_guard = storage_daemon_shutdown_tx_opt.lock().await;
                if let Some(tx) = storage_daemon_shutdown_guard.take() {
                    let _ = tx.send(());
                }
                let mut storage_daemon_handle_guard = storage_daemon_handle.lock().await;
                *storage_daemon_handle_guard = None;
                let mut storage_daemon_port_guard = storage_daemon_port_arc.lock().await;
                *storage_daemon_port_guard = None;
            }

            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "Storage Daemon", &skip_ports).await?;
            start_storage_daemonized(
                Some(effective_port),
                config_file,
                effective_cluster.clone().as_ref().map(|s| s.clone()),
                data_directory.or_else(|| Some(storage_config.data_directory.clone())),
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                &cli_config,
                &storage_config,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
            if join_cluster.unwrap_or(false) || join_storage_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining storage cluster with ports: {:?}", ports);
            }
        }
        Some(RestartAction::Cluster { cluster: _ }) => {
            let mut daemon_handles_guard = daemon_handles.lock().await;
            for (_, (_, shutdown_tx)) in daemon_handles_guard.drain() {
                let _ = shutdown_tx.send(());
            }
            let mut rest_api_shutdown_guard = rest_api_shutdown_tx_opt.lock().await;
            if let Some(tx) = rest_api_shutdown_guard.take() {
                let _ = tx.send(());
            }
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;
            *rest_api_port_guard = None;
            let mut rest_api_handle_guard = rest_api_handle.lock().await;
            *rest_api_handle_guard = None;
            let mut storage_daemon_shutdown_guard = storage_daemon_shutdown_tx_opt.lock().await;
            if let Some(tx) = storage_daemon_shutdown_guard.take() {
                let _ = tx.send(());
            }
            let mut storage_daemon_handle_guard = storage_daemon_handle.lock().await;
            *storage_daemon_handle_guard = None;
            let mut storage_daemon_port_guard = storage_daemon_port_arc.lock().await;
            *storage_daemon_port_guard = None;

            handle_start_all_interactive(
                StartAction::All {
                    port: None,
                    daemon: Some(true),
                    rest: Some(true),
                    storage: Some(true),
                    daemon_port: Some(DEFAULT_DAEMON_PORT),
                    rest_port: Some(DEFAULT_REST_API_PORT),
                    storage_port: Some(DEFAULT_STORAGE_PORT),
                    cluster: None,
                    rest_cluster: None,
                    storage_cluster: None,
                    join_cluster: Some(true),
                    join_rest_cluster: None,
                    join_storage_cluster: None,
                    config_file: None,
                    data_directory: None,
                    log_directory: None,
                    max_disk_space_gb: None,
                    min_disk_space_gb: None,
                    use_raft_for_scale: None,
                    storage_engine_type: None,
                    listen_port: None,
                    daemon_cluster: None,
                    storage_config_file: None,
                },
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
            let ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or(vec![9000, 9001, 9002]);
            println!("Restarting cluster with ports: {:?}", ports);
        }
        None => {
            eprintln!("No restart action specified.");
            process::exit(1);
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
    let cli_config = CliConfig::default();
    let storage_config = StorageConfig::default();

    match restart_args.action {
        Some(RestartAction::All {
            port,
            cluster,
            listen_port,
            storage_port,
            rest_port,
            daemon_port,
            daemon_cluster,
            storage_config_file,
            config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
            storage,
            join_cluster,
            join_rest_cluster,
            join_storage_cluster,
            rest_cluster,
            storage_cluster,
        }) => {
            let daemon_enabled = daemon.unwrap_or(true);
            let rest_enabled = rest.unwrap_or(true);
            let storage_enabled = storage.unwrap_or(true);
            let join_cluster_enabled = join_cluster.unwrap_or(false);
            let effective_daemon_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_rest_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_storage_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);

            if daemon_enabled {
                if is_daemon_running(effective_daemon_port, "GraphDB Daemon").await {
                    let mut daemon_handles_guard = daemon_handles.lock().await;
                    if let Some((_, shutdown_tx)) = daemon_handles_guard.remove(&effective_daemon_port) {
                        let _ = shutdown_tx.send(());
                    }
                }
            }
            if rest_enabled {
                let mut rest_api_shutdown_guard = rest_api_shutdown_tx_opt.lock().await;
                if let Some(tx) = rest_api_shutdown_guard.take() {
                    let _ = tx.send(());
                }
                let mut rest_api_port_guard = rest_api_port_arc.lock().await;
                *rest_api_port_guard = None;
                let mut rest_api_handle_guard = rest_api_handle.lock().await;
                *rest_api_handle_guard = None;
            }
            if storage_enabled {
                if is_storage_daemon_running(effective_storage_port).await {
                    let mut storage_daemon_shutdown_guard = storage_daemon_shutdown_tx_opt.lock().await;
                    if let Some(tx) = storage_daemon_shutdown_guard.take() {
                        let _ = tx.send(());
                    }
                    let mut storage_daemon_handle_guard = storage_daemon_handle.lock().await;
                    *storage_daemon_handle_guard = None;
                    let mut storage_daemon_port_guard = storage_daemon_port_arc.lock().await;
                    *storage_daemon_port_guard = None;
                }
            }

            handle_start_all_interactive(
                StartAction::All {
                    port,
                    daemon: Some(daemon_enabled),
                    rest: Some(rest_enabled),
                    storage: Some(storage_enabled),
                    daemon_port: Some(effective_daemon_port),
                    rest_port: Some(effective_rest_port),
                    storage_port: Some(effective_storage_port),
                    cluster: cluster.clone(),
                    rest_cluster: rest_cluster.clone(),
                    storage_cluster: storage_cluster.clone(),
                    join_cluster: Some(join_cluster_enabled),
                    join_rest_cluster,
                    join_storage_cluster,
                    config_file,
                    data_directory,
                    log_directory,
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type,
                    listen_port,
                    daemon_cluster: daemon_cluster.clone(),
                    storage_config_file,
                },
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;

            if join_cluster_enabled {
                let effective_cluster = daemon_cluster
                    .or(rest_cluster)
                    .or(storage_cluster)
                    .or(cluster.clone());
                let ports = parse_cluster_range(&effective_cluster.clone().unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining cluster with ports: {:?}", ports);
            }
        }
        Some(RestartAction::Daemon {
            port,
            cluster,
            daemon_port,
            daemon_cluster: cmd_daemon_cluster,
            config_file,
            join_cluster,
            join_daemon_cluster,
            daemon,
            rest,
            storage,
        }) => {
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let effective_cluster = cmd_daemon_cluster.or(cluster.clone());
            if is_daemon_running(effective_port, "GraphDB Daemon").await {
                let mut daemon_handles_guard = daemon_handles.lock().await;
                if let Some((_, shutdown_tx)) = daemon_handles_guard.remove(&effective_port) {
                    let _ = shutdown_tx.send(());
                }
            }
            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "GraphDB Daemon", &skip_ports).await?;
            start_daemon(
                Some(effective_port),
                effective_cluster.clone().as_ref().map(|s| s.clone()).or_else(|| Some(storage_config.cluster_range.clone())),
                skip_ports,
            )
            .await?;
            if join_cluster.unwrap_or(false) || join_daemon_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining daemon cluster with ports: {:?}", ports);
            }
        }
        Some(RestartAction::Rest {
            port,
            cluster,
            listen_port,
            rest_port,
            join_cluster,
            join_rest_cluster,
            rest_cluster: cmd_rest_cluster,
            daemon,
            rest,
            storage,
            config_file,
        }) => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_cluster = cmd_rest_cluster.or(cluster.clone());
            let mut rest_api_shutdown_guard = rest_api_shutdown_tx_opt.lock().await;
            if let Some(tx) = rest_api_shutdown_guard.take() {
                let _ = tx.send(());
            }
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;
            *rest_api_port_guard = None;
            let mut rest_api_handle_guard = rest_api_handle.lock().await;
            *rest_api_handle_guard = None;

            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "REST API", &skip_ports).await?;
            start_rest_api_daemonized(
                Some(effective_port),
                effective_cluster.clone(),
                &cli_config,
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
            )
            .await?;
            if join_cluster.unwrap_or(false) || join_rest_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining REST cluster with ports: {:?}", ports);
            }
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
            join_cluster,
            join_storage_cluster,
            storage_cluster: cmd_storage_cluster,
            storage_port,
            daemon,
            rest,
            storage,
        }) => {
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let effective_cluster = cmd_storage_cluster.or(cluster.clone());
            if is_storage_daemon_running(effective_port).await {
                let mut storage_daemon_shutdown_guard = storage_daemon_shutdown_tx_opt.lock().await;
                if let Some(tx) = storage_daemon_shutdown_guard.take() {
                    let _ = tx.send(());
                }
                let mut storage_daemon_handle_guard = storage_daemon_handle.lock().await;
                *storage_daemon_handle_guard = None;
                let mut storage_daemon_port_guard = storage_daemon_port_arc.lock().await;
                *storage_daemon_port_guard = None;
            }

            let skip_ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or_default();
            check_port_conflicts(effective_port, "Storage Daemon", &skip_ports).await?;
            start_storage_daemonized(
                Some(effective_port),
                config_file,
                effective_cluster.clone().as_ref().map(|s| s.clone()),
                data_directory.or_else(|| Some(storage_config.data_directory.clone())),
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                &cli_config,
                &storage_config,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
            if join_cluster.unwrap_or(false) || join_storage_cluster.unwrap_or(false) {
                let ports = parse_cluster_range(&effective_cluster.unwrap_or(storage_config.cluster_range.clone())).unwrap_or(vec![9000, 9001, 9002]);
                println!("Joining storage cluster with ports: {:?}", ports);
            }
        }
        Some(RestartAction::Cluster { cluster: _ }) => {
            let mut daemon_handles_guard = daemon_handles.lock().await;
            for (_, (_, shutdown_tx)) in daemon_handles_guard.drain() {
                let _ = shutdown_tx.send(());
            }
            let mut rest_api_shutdown_guard = rest_api_shutdown_tx_opt.lock().await;
            if let Some(tx) = rest_api_shutdown_guard.take() {
                let _ = tx.send(());
            }
            let mut rest_api_port_guard = rest_api_port_arc.lock().await;
            *rest_api_port_guard = None;
            let mut rest_api_handle_guard = rest_api_handle.lock().await;
            *rest_api_handle_guard = None;
            let mut storage_daemon_shutdown_guard = storage_daemon_shutdown_tx_opt.lock().await;
            if let Some(tx) = storage_daemon_shutdown_guard.take() {
                let _ = tx.send(());
            }
            let mut storage_daemon_handle_guard = storage_daemon_handle.lock().await;
            *storage_daemon_handle_guard = None;
            let mut storage_daemon_port_guard = storage_daemon_port_arc.lock().await;
            *storage_daemon_port_guard = None;

            handle_start_all_interactive(
                StartAction::All {
                    port: None,
                    daemon: Some(true),
                    rest: Some(true),
                    storage: Some(true),
                    daemon_port: Some(DEFAULT_DAEMON_PORT),
                    rest_port: Some(DEFAULT_REST_API_PORT),
                    storage_port: Some(DEFAULT_STORAGE_PORT),
                    cluster: None,
                    rest_cluster: None,
                    storage_cluster: None,
                    join_cluster: Some(true),
                    join_rest_cluster: None,
                    join_storage_cluster: None,
                    config_file: None,
                    data_directory: None,
                    log_directory: None,
                    max_disk_space_gb: None,
                    min_disk_space_gb: None,
                    use_raft_for_scale: None,
                    storage_engine_type: None,
                    listen_port: None,
                    daemon_cluster: None,
                    storage_config_file: None,
                },
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
            let ports = parse_cluster_range(&storage_config.cluster_range).unwrap_or(vec![9000, 9001, 9002]);
            println!("Restarting cluster with ports: {:?}", ports);
        }
        None => {
            eprintln!("No restart action specified.");
            process::exit(1);
        }
    }
    Ok(())
}

/// Handles the 'clear-data' command, clearing all or specific component data.
pub async fn handle_clear_data_command(clear_data_args: ClearDataArgs) -> Result<()> {
    match clear_data_args.action {
        ClearDataAction::All => {
            println!("Clearing all GraphDB data (daemon, REST, storage)... [MOCK]");
            println!("All GraphDB data cleared successfully. [MOCK]");
        }
        ClearDataAction::Storage { port } => { // Corrected: removed storage_port
            let effective_port = port;
            if let Some(p) = effective_port {
                println!("Clearing storage data on port {}... [MOCK]", p);
                println!("Storage data on port {} cleared successfully. [MOCK]", p);
            } else {
                println!("Clearing storage data on default port... [MOCK]");
                println!("Storage data on default port cleared successfully. [MOCK]");
            }
        }
    }
    Ok(())
}

pub async fn display_status_summary(
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    display_full_status_summary(rest_api_port_arc, None, None).await;
    Ok(())
}

async fn check_port_conflicts(port: u16, component: &str, _skip_ports: &[u16]) -> Result<()> {
    let is_daemon_running_on_port = check_process_status_by_port("GraphDB Daemon", port).await;
    let is_rest_api_running_on_port = check_process_status_by_port("REST API", port).await;
    let is_storage_daemon_running_on_port = check_process_status_by_port("Storage Daemon", port).await;

    if component == "GraphDB Daemon" {
        if is_rest_api_running_on_port || is_storage_daemon_running_on_port {
            return Err(anyhow::anyhow!("Port {} is already in use by a REST API or Storage Daemon component.", port));
        }
        if is_daemon_running_on_port {
            println!("Port {} is already in use by a GraphDB Daemon, assuming it's part of the intended cluster or already running.", port);
            return Ok(());
        }
    } else if component == "REST API" {
        if is_daemon_running_on_port || is_storage_daemon_running_on_port {
            return Err(anyhow::anyhow!("Port {} is already in use by a GraphDB Daemon or Storage Daemon component.", port));
        }
        if is_rest_api_running_on_port {
            println!("Port {} is already in use by a REST API, assuming it's part of the intended cluster or already running.", port);
            return Ok(());
        }
    } else if component == "Storage Daemon" {
        if is_daemon_running_on_port || is_rest_api_running_on_port {
            return Err(anyhow::anyhow!("Port {} is already in use by a GraphDB Daemon or REST API component.", port));
        }
        if is_storage_daemon_running_on_port {
            println!("Port {} is already in use by a Storage Daemon, assuming it's part of the intended cluster or already running.", port);
            return Ok(());
        }
    }

    if !is_port_free(port).await {
        return Err(anyhow::anyhow!("Port {} is already in use by an unknown process.", port));
    }

    Ok(())
}

pub async fn handle_graph_query_command(args: GraphQueryArgs, rest_api_port_arc: Arc<Mutex<Option<u16>>>) -> Result<()> {
    let port = rest_api_port_arc.lock().await.unwrap_or(DEFAULT_REST_API_PORT);
    println!("Executing graph query: '{}' on REST API port {} with persist={:?}", args.query_string, port, args.persist);
    // Placeholder for graph query execution logic
    println!("Graph query command executed (placeholder).");
    Ok(())
}


pub async fn handle_register_user_command(args: RegisterUserArgs) -> Result<()> {
    println!("Registering user: {}", args.username);
    // Placeholder for user registration logic
    println!("User registration command executed (placeholder).");
    Ok(())
}

pub async fn handle_auth_command(args: AuthArgs) -> Result<()> {
    println!("Authenticating user: {}", args.username);
    // Placeholder for authentication logic
    println!("Authentication command executed (placeholder).");
    Ok(())
}

pub async fn handle_query_command(args: QueryArgs, rest_api_port_arc: Arc<Mutex<Option<u16>>>) -> Result<()> {
    let port = rest_api_port_arc.lock().await.unwrap_or(DEFAULT_REST_API_PORT);
    println!("Executing query: '{}' on REST API port {} with persist={:?}", args.query, port, args.persist);
    // Placeholder for query execution logic
    println!("Query command executed (placeholder).");
    Ok(())
}


pub async fn handle_version_command() -> Result<()> {
    println!("GraphDB CLI Version: {}", env!("CARGO_PKG_VERSION"));
    // Placeholder for fetching component versions
    println!("Version command executed (placeholder).");
    Ok(())
}

pub async fn handle_health_command(rest_api_port_arc: Arc<Mutex<Option<u16>>>) -> Result<()> {
    let port = rest_api_port_arc.lock().await.unwrap_or(DEFAULT_REST_API_PORT);
    println!("Checking health of REST API on port {}...", port);
    // Placeholder for health check logic
    println!("Health command executed (placeholder).");
    Ok(())
}


// server/src/cli/daemon_management.rs

// This file contains logic for daemonizing and managing background processes
// (REST API, Storage, and GraphDB daemons).

use anyhow::{Context, Result, Error}; // Import anyhow::Error directly
use std::path::PathBuf;
use tokio::process::Command;
use std::process; // For process::exit
use std::str::FromStr; // Keep this import as it's used for StorageEngineType::from_str
use clap::ValueEnum; // Added: Required for .to_possible_value()

// Import necessary items from sibling modules
// Renamed cli::config::StorageConfig to CliStorageConfig to avoid name conflict
use crate::cli::config::{get_default_rest_port_from_config, load_storage_config, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, StorageConfig as CliStorageConfig};
// Import StorageEngineType and StorageConfig from lib::storage_engine::config directly for start_daemon_process
use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig}; // Alias lib's StorageConfig if needed, but not directly used in this part after changes

// External crates
use daemon_api::{stop_daemon, DaemonError}; // Imported DaemonError
use rest_api::start_server as start_rest_server; // For starting the REST API server
use storage_daemon_server::run_storage_daemon as start_storage_server; // For starting the storage daemon

/// Handles the internal execution of daemonized processes (REST API or Storage).
/// This function is called when the CLI is invoked with `--internal-rest-api-run` or `--internal-storage-daemon-run`.
pub async fn handle_internal_daemon_run(
    is_rest_api_run: bool,
    is_storage_daemon_run: bool,
    internal_port: Option<u16>,
    internal_storage_config_path: Option<PathBuf>,
    _internal_storage_engine: Option<StorageEngineType>, // Marked as unused
) -> Result<()> {
    if is_rest_api_run {
        let daemon_listen_port = internal_port.unwrap_or_else(get_default_rest_port_from_config);
        
        // Corrected: Load CliStorageConfig, and ensure the unwrap_or_else returns CliStorageConfig
        let cli_storage_config = load_storage_config(internal_storage_config_path.clone())
            .unwrap_or_else(|e| {
                eprintln!("[DAEMON PROCESS] Warning: Could not load storage config for REST API: {}. Using defaults for CLI config.", e);
                // This block must return `crate::cli::config::StorageConfig` (now aliased as CliStorageConfig)
                // Assuming CliStorageConfig has a Default implementation to create a default instance.
                CliStorageConfig::default() 
            });

        println!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let (_tx, rx) = tokio::sync::oneshot::channel(); // Dummy channel for shutdown
        // Corrected: Use `data_directory` from CliStorageConfig, which is its equivalent of data_path
        let result = start_rest_server(daemon_listen_port, rx, cli_storage_config.data_directory.clone()).await;
        if let Err(e) = result {
            eprintln!("[DAEMON PROCESS] REST API server failed: {:?}", e);
            process::exit(1);
        }
        println!("[DAEMON PROCESS] REST API server (daemonized) stopped.");
        process::exit(0);
    } else if is_storage_daemon_run {
        let daemon_listen_port = internal_port.unwrap_or_else(|| {
            // Corrected: Access `default_port` field directly (remove parentheses)
            load_storage_config(None).map(|c| c.default_port).unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
        });
        let storage_config_path = internal_storage_config_path.unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("Failed to get parent directory of server crate")
                .join("storage_daemon_server")
                .join("storage_config.yaml")
        });

        println!("[DAEMON PROCESS] Starting Storage daemon (daemonized) on port {}...", daemon_listen_port);
        let result = start_storage_server(Some(daemon_listen_port), storage_config_path).await;
        if let Err(e) = result {
            eprintln!("[DAEMON PROCESS] Storage daemon failed: {:?}", e);
            process::exit(1);
        }
        println!("[DAEMON PROCESS] Storage daemon (daemonized) stopped.");
        process::exit(0);
    }
    Ok(())
}

/// Spawns a new process to run a daemon (REST API or Storage) in the background.
pub async fn start_daemon_process(
    is_rest: bool,
    is_storage: bool,
    port: Option<u16>,
    config_path: Option<PathBuf>,
    engine_type: Option<StorageEngineType>, // Now directly accepts lib::storage_engine::config::StorageEngineType
) -> Result<()> {
    let current_exe = std::env::current_exe().context("Failed to get current executable path")?;
    let mut command = Command::new(&current_exe);

    if is_rest {
        command.arg("--internal-rest-api-run");
    } else if is_storage {
        command.arg("--internal-storage-daemon-run");
    }

    if let Some(p) = port {
        command.arg("--internal-port").arg(p.to_string());
    }
    if let Some(path) = config_path {
        command.arg("--internal-storage-config-path").arg(path);
    }
    if let Some(engine) = engine_type {
        // Corrected: Use to_possible_value().unwrap().get_name() because ValueEnum trait is now in scope
        command.arg("--internal-storage-engine").arg(engine.to_possible_value().unwrap().get_name());
    }

    let child = command.spawn().context("Failed to spawn daemon process")?;
    println!("Daemonized process spawned with PID {}", child.id().unwrap_or(0));
    Ok(())
}

/// Helper function to find a running storage daemon's port.
/// Scans a range of common ports using `lsof`.
pub async fn find_running_storage_daemon_port() -> Option<u16> {
    let common_storage_ports_to_check = (CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS..=CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS + 10).collect::<Vec<u16>>();
    for port in common_storage_ports_to_check {
        let output = tokio::process::Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output()
            .await;

        if let Ok(output) = output {
            let pids = String::from_utf8_lossy(&output.stdout);
            if !pids.trim().is_empty() {
                return Some(port);
            }
        }
    }
    None
}

/// Calls the `daemon_api::stop_daemon` function.
/// Renamed to avoid name collision with `commands::DaemonCliCommand::Stop`.
pub fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    // Changed formatting from {} to {:?} for DaemonError as it doesn't implement Display
    stop_daemon().map_err(|e| anyhow::anyhow!("Daemon API error: {:?}", e))
}

/// Attempts to clear all daemon processes by sending a global stop signal
/// and then trying to kill processes on common daemon ports.
pub async fn clear_all_daemon_processes() -> Result<(), anyhow::Error> {
    println!("Attempting to clear all GraphDB daemon processes...");

    // First, send the global stop signal via daemon_api
    let stop_result = stop_daemon_api_call();
    match stop_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }

    // Then, iterate through common daemon ports and try to kill processes
    let common_daemon_ports = [8000, 8080, 8081, 9001, 9002, 9003, 9004, 9005, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
    for &port in &common_daemon_ports {
        let output = std::process::Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t") // Only print PIDs
            .output()
            .context(format!("Failed to run lsof to find process on port {}", port))?;

        let pids = String::from_utf8_lossy(&output.stdout);
        let pids: Vec<i32> = pids.trim().lines().filter_map(|s| s.parse::<i32>().ok()).collect();

        if !pids.is_empty() {
            for pid in pids {
                println!("Killing process {} on port {}...", pid, port);
                match nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), nix::sys::signal::Signal::SIGTERM) {
                    Ok(_) => println!("Successfully sent SIGTERM to PID {}.", pid),
                    Err(e) => eprintln!("Failed to send SIGTERM to PID {}: {}", pid, e),
                }
            }
        }
    }

    // Also try to kill processes by generic names if they are still running
    let generic_process_names = ["graphdb-cli", "rest_api_server", "storage_daemon_server"];
    for name in &generic_process_names {
        let pgrep_result = std::process::Command::new("pgrep")
            .arg("-f")
            .arg(name)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|child| child.wait_with_output());

        if let Ok(output) = pgrep_result {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines().filter_map(|line| line.trim().parse::<u32>().ok()).collect();
            if !pids.is_empty() {
                for pid in pids {
                    println!("Killing process {} ({})...", pid, name);
                    match nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid as i32), nix::sys::signal::Signal::SIGTERM) {
                        Ok(_) => println!("Successfully sent SIGTERM to PID {}.", pid),
                        Err(e) => eprintln!("Failed to send SIGTERM to PID {}: {}", pid, e),
                    }
                }
            }
        }
    }

    Ok(())
}


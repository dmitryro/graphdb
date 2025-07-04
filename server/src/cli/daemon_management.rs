// server/src/cli/daemon_management.rs

// This file contains logic for daemonizing and managing background processes
// (REST API, Storage, and GraphDB daemons).

use anyhow::{Context, Result, Error}; // Import anyhow::Error directly
use std::path::PathBuf;
use tokio::process::Command;
use std::process; // For process::exit
use std::str::FromStr; // Keep this import as it's used for StorageEngineType::from_str

// Import necessary items from sibling modules
use crate::cli::config::{get_default_rest_port_from_config, load_storage_config, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, StorageConfig, StorageEngineType};

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
        
        let storage_config = load_storage_config(internal_storage_config_path.clone())
            .unwrap_or_else(|e| {
                eprintln!("[DAEMON PROCESS] Warning: Could not load storage config for REST API: {}. Using defaults.", e);
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

        println!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let (_tx, rx) = tokio::sync::oneshot::channel(); // Dummy channel for shutdown
        let result = start_rest_server(daemon_listen_port, rx, storage_config.data_directory.clone()).await;
        if let Err(e) = result {
            eprintln!("[DAEMON PROCESS] REST API server failed: {:?}", e);
            process::exit(1);
        }
        println!("[DAEMON PROCESS] REST API server (daemonized) stopped.");
        process::exit(0);
    } else if is_storage_daemon_run {
        let daemon_listen_port = internal_port.unwrap_or_else(|| {
            load_storage_config(None).map(|c| c.default_port).unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
        });
        let storage_config_path = internal_storage_config_path.unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
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
    engine_type: Option<StorageEngineType>,
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
        command.arg("--internal-storage-engine").arg(engine.to_string());
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

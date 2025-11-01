// server/src/cli/handlers.rs

// This file contains the handlers for top-level CLI commands, encapsulating
// logic for starting, stopping, reloading, and managing the status of GraphDB components,
// as well as cluster and plugin operations.

use anyhow::{Result, Context, anyhow};
use std::collections::HashMap;
use std::path::{PathBuf, Path};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use std::sync::Arc;
use std::time::Duration;
use futures::stream::StreamExt;
use log::{info, error, warn, debug};
use config::Config;
use std::fs;

// Import command structs from commands.rs
use lib::commands::{
    Commands, StatusArgs, StopArgs, StopAction, StatusAction, ReloadArgs, ReloadAction,
    RestartArgs, RestartAction
};

// Import configuration-related items
use lib::config::{
    DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, DEFAULT_STORAGE_PORT,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, load_storage_config_str as load_storage_config, 
    load_cli_config, CliConfig
};

// Import daemon management utilities
use crate::cli::daemon_management::{
    is_port_free, parse_cluster_range, stop_process_by_port, find_all_running_rest_api_ports,
    find_running_storage_daemon_port
};

pub use crate::cli::handlers_utils::{get_current_exe_path, format_engine_config, print_welcome_screen, write_registry_fallback, 
                                     read_registry_fallback, clear_terminal_screen, ensure_daemon_registry_paths_exist,
                                     execute_storage_query};
pub use crate::cli::handlers_storage::{storage, start_storage_interactive, stop_storage_interactive, show_storage,
                                       display_storage_daemon_status, handle_storage_command, handle_storage_command_interactive, 
                                       stop_storage, use_storage_engine, handle_save_storage, reload_storage_interactive,
                                       handle_use_storage_interactive, handle_use_storage_command, handle_show_storage_command,
                                       handle_show_storage_command_interactive, handle_show_storage_config_command,
                                       handle_migrate_interactive, handle_migrate_command};
pub use crate::cli::handlers_rest::{RestArgs, rest, display_rest_api_status, handle_rest_command, handle_rest_command_interactive, 
                                    start_rest_api_interactive, stop_rest_api_interactive,  display_rest_api_health,
                                    display_rest_api_version, register_user, authenticate_user, execute_graph_query,
                                    reload_rest_interactive, handle_show_rest_config_command};     
pub use crate::cli::handlers_all::{stop_all_interactive, reload_all_interactive, handle_start_all_interactive, 
                                   display_full_status_summary, handle_show_all_config_command};     
pub use crate::cli::handlers_main::{display_daemon_status, handle_daemon_command, handle_daemon_command_interactive, 
                                    start_daemon_instance_interactive, stop_main_interactive, reload_daemon_interactive,
                                    stop_daemon_instance_interactive, handle_show_main_config_command}; 
pub use crate::cli::handlers_queries::{handle_interactive_query, handle_unified_query, handle_kv_command, 
                                       handle_exec_command, handle_query_command};

use daemon_api::{stop_daemon, start_daemon};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};                                    

/// Displays the Raft status for a storage daemon running on the specified port.
pub async fn display_raft_status(port: Option<u16>) -> Result<()> {
    let port = port.unwrap_or(8081); // Default port for storage daemon
    // Placeholder: Implement actual Raft status checking logic here
    // For example, query the storage daemon's Raft node status via an API or internal call
    println!("Checking Raft status for storage daemon on port {}...", port);
    println!("Raft status: Not implemented yet (placeholder).");
    // TODO: Add actual Raft status checking logic, e.g., querying a Raft node's state
    Ok(())
}

/// Displays the status of the entire cluster. (Placeholder)
pub async fn display_cluster_status() {
    println!("\n--- Cluster Status ---");
    println!("Cluster status is a placeholder. In a real implementation, this would query all daemons in the cluster.");
    println!("--------------------------------------------------");
}

// --- Command Handlers for direct CLI execution (non-interactive) ---
/// Handles the top-level `start` command.
pub async fn handle_start_command(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    _config: &lib::config::CliConfig,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let mut daemon_status_msg = "Not requested".to_string();
    let mut rest_api_status_msg = "Not requested".to_string();
    let mut storage_status_msg = "Not requested".to_string();

    let start_daemon_requested = port.is_some() || cluster.is_some();
    let start_rest_requested = listen_port.is_some();
    let start_storage_requested = storage_port.is_some() || storage_config_file.is_some();

    let mut errors = Vec::new();

    if start_daemon_requested {
        let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
        let daemon_result = start_daemon_instance_interactive(Some(actual_port), cluster.clone(), daemon_handles.clone()).await;
        daemon_status_msg = match daemon_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_rest_requested {
        let actual_port = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        let rest_result = start_rest_api_interactive(Some(actual_port), None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await;
        rest_api_status_msg = match rest_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("REST API on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_storage_requested {
        let actual_port = storage_port.unwrap_or(DEFAULT_STORAGE_PORT);
        let actual_config_file = storage_config_file.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
        let storage_result = start_storage_interactive(
            Some(actual_port),
            Some(actual_config_file),
            None,
            None,
            rest_api_shutdown_tx_opt.clone(), // Reuse existing mutex to avoid registry conflicts
            rest_api_handle.clone(), // Reuse existing handle
            rest_api_port_arc.clone(), // Reuse existing port arc
        ).await;
        storage_status_msg = match storage_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Storage Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    println!("\n--- Component Startup Summary ---");
    println!("{:<15} {:<50}", "Component", "Status");
    println!("{:-<15} {:-<50}", "", "");
    println!("{:<15} {:<50}", "GraphDB", daemon_status_msg);
    println!("{:<15} {:<50}", "REST API", rest_api_status_msg);
    println!("{:<15} {:<50}", "Storage", storage_status_msg);
    println!("---------------------------------\n");

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to start one or more components: {:?}", errors))
    }
}

// --- Command Handlers for direct CLI execution (non-interactive) ---
/// Handles the top-level `start` command.
pub async fn handle_start_command_interactive(
    port: Option<u16>,
    cluster: Option<String>,
    listen_port: Option<u16>,
    storage_port: Option<u16>,
    storage_config_file: Option<PathBuf>,
    _config: &lib::config::CliConfig,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
) -> Result<()> {
    let mut daemon_status_msg = "Not requested".to_string();
    let mut rest_api_status_msg = "Not requested".to_string();
    let mut storage_status_msg = "Not requested".to_string();

    let start_daemon_requested = port.is_some() || cluster.is_some();
    let start_rest_requested = listen_port.is_some();
    let start_storage_requested = storage_port.is_some() || storage_config_file.is_some();

    let mut errors = Vec::new();

    if start_daemon_requested {
        let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
        let daemon_result = start_daemon_instance_interactive(Some(actual_port), cluster.clone(), daemon_handles.clone()).await;
        daemon_status_msg = match daemon_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_rest_requested {
        let actual_port = listen_port.unwrap_or(DEFAULT_REST_API_PORT);
        let rest_result = start_rest_api_interactive(Some(actual_port), None, rest_api_shutdown_tx_opt.clone(), rest_api_port_arc.clone(), rest_api_handle.clone()).await;
        rest_api_status_msg = match rest_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("REST API on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    if start_storage_requested {
        let actual_port = storage_port.unwrap_or(DEFAULT_STORAGE_PORT);
        let actual_config_file = storage_config_file.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
        let storage_result = start_storage_interactive(
            Some(actual_port),
            Some(actual_config_file),
            None,
            None,
            rest_api_shutdown_tx_opt.clone(), // Reuse existing mutex to avoid registry conflicts
            rest_api_handle.clone(), // Reuse existing handle
            rest_api_port_arc.clone(), // Reuse existing port arc
        ).await;
        storage_status_msg = match storage_result {
            Ok(()) => format!("Running on port: {}", actual_port),
            Err(e) => {
                errors.push(format!("Storage Daemon on port {}: {}", actual_port, e));
                format!("Failed to start ({:?})", e)
            }
        };
    }

    println!("\n--- Component Startup Summary ---");
    println!("{:<15} {:<50}", "Component", "Status");
    println!("{:-<15} {:-<50}", "", "");
    println!("{:<15} {:<50}", "GraphDB", daemon_status_msg);
    println!("{:<15} {:<50}", "REST API", rest_api_status_msg);
    println!("{:<15} {:<50}", "Storage", storage_status_msg);
    println!("---------------------------------\n");

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to start one or more components: {:?}", errors))
    }
}

/// Handles `stop` subcommand for direct CLI execution.
pub async fn handle_stop_command(args: StopArgs) -> Result<()> {
    println!("--->> STOP NON-INTERACTIVE");
    info!("Processing stop command with args: {:?}", args);
    match args.action.unwrap_or(StopAction::All) {
        StopAction::All => {
            println!("Stopping all GraphDB components...");
            info!("Initiating stop for all components");

            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            debug!("Initial daemon registry state: {:?}", all_daemons);
            if all_daemons.is_empty() {
                println!("No running components found in registry.");
                info!("No components registered in GLOBAL_DAEMON_REGISTRY");
                return Ok(());
            }

            let mut stopped_count = 0;
            let mut errors = Vec::new();

            // Stop REST API daemons
            let rest_daemons: Vec<DaemonMetadata> = all_daemons
                .iter()
                .filter(|d| d.service_type == "rest")
                .cloned()
                .collect();
            for daemon in rest_daemons {
                println!("Stopping REST API on port {} (PID: {})...", daemon.port, daemon.pid);
                info!("Attempting to stop REST API on port {} (PID: {})", daemon.port, daemon.pid);
                match stop_rest_api_interactive(
                    Some(daemon.port),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                )
                .await
                {
                    Ok(()) => {
                        if is_port_free(daemon.port).await {
                            info!("REST API on port {} stopped successfully", daemon.port);
                            stopped_count += 1;
                        } else {
                            warn!("REST API on port {} stopped but port is still in use", daemon.port);
                            errors.push(format!("REST API on port {}: port still in use", daemon.port));
                        }
                    }
                    Err(e) => {
                        println!("Failed to stop REST API on port {}: {}", daemon.port, e);
                        error!("Failed to stop REST API on port {}: {}", daemon.port, e);
                        errors.push(format!("REST API on port {}: {}", daemon.port, e));
                    }
                }
            }

            // Stop main daemons
            let main_daemons: Vec<DaemonMetadata> = all_daemons
                .iter()
                .filter(|d| d.service_type == "main")
                .cloned()
                .collect();
            for daemon in main_daemons {
                println!("Stopping main daemon on port {} (PID: {})...", daemon.port, daemon.pid);
                info!("Attempting to stop main daemon on port {} (PID: {})", daemon.port, daemon.pid);
                match stop_main_interactive(Some(daemon.port), Arc::new(TokioMutex::new(None))).await {
                    Ok(()) => {
                        if is_port_free(daemon.port).await {
                            info!("Main daemon on port {} stopped successfully", daemon.port);
                            stopped_count += 1;
                        } else {
                            warn!("Main daemon on port {} stopped but port is still in use", daemon.port);
                            errors.push(format!("Main daemon on port {}: port still in use", daemon.port));
                        }
                    }
                    Err(e) => {
                        println!("Failed to stop main daemon on port {}: {}", daemon.port, e);
                        error!("Failed to stop main daemon on port {}: {}", daemon.port, e);
                        errors.push(format!("Main daemon on port {}: {}", daemon.port, e));
                    }
                }
            }

            // Stop storage daemons
            let storage_daemons: Vec<DaemonMetadata> = all_daemons
                .iter()
                .filter(|d| d.service_type == "storage")
                .cloned()
                .collect();
            for daemon in storage_daemons {
                println!("Stopping storage daemon on port {} (PID: {})...", daemon.port, daemon.pid);
                info!("Attempting to stop storage daemon on port {} (PID: {})", daemon.port, daemon.pid);
                match stop_storage_interactive(
                    Some(daemon.port),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                )
                .await
                {
                    Ok(()) => {
                        if is_port_free(daemon.port).await {
                            info!("Storage daemon on port {} stopped successfully", daemon.port);
                            stopped_count += 1;
                        } else {
                            warn!("Storage daemon on port {} stopped but port is still in use", daemon.port);
                            errors.push(format!("Storage daemon on port {}: port still in use", daemon.port));
                        }
                    }
                    Err(e) => {
                        println!("Failed to stop storage daemon on port {}: {}", daemon.port, e);
                        error!("Failed to stop storage daemon on port {}: {}", daemon.port, e);
                        errors.push(format!("Storage daemon on port {}: {}", daemon.port, e));
                    }
                }
            }

            // Clear any remaining registry entries
            let remaining_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            if !remaining_daemons.is_empty() {
                warn!("Found stale registry entries after stopping components: {:?}", remaining_daemons);
                for daemon in remaining_daemons {
                    if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type(&daemon.service_type, daemon.port).await {
                        warn!("Failed to remove stale {} daemon on port {} from registry: {}", daemon.service_type, daemon.port, e);
                        errors.push(format!("Failed to remove stale {} daemon on port {}: {}", daemon.service_type, daemon.port, e));
                    } else {
                        info!("Removed stale {} daemon on port {} from registry", daemon.service_type, daemon.port);
                    }
                }
            }

            println!(
                "Stop all completed: {} components stopped, {} failed.",
                stopped_count,
                errors.len()
            );
            info!(
                "Stop all completed: {} components stopped, {} failed",
                stopped_count,
                errors.len()
            );

            if errors.is_empty() {
                Ok(())
            } else {
                Err(anyhow!("Failed to stop one or more components: {:?}", errors))
            }
        }
        StopAction::Storage { port } => {
            info!("Stopping storage daemon on port {:?}", port);
            stop_storage_interactive(
                port,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            )
            .await
            .map_err(|e| anyhow!("Failed to stop storage daemon: {}", e))?;
            if let Some(p) = port {
                if is_port_free(p).await {
                    info!("Storage daemon on port {} stopped successfully", p);
                    println!("Storage daemon on port {} stopped.", p);
                    Ok(())
                } else {
                    Err(anyhow!("Storage daemon on port {} stopped but port is still in use", p))
                }
            } else {
                Ok(())
            }
        }
        StopAction::Rest { port } => {
            info!("Stopping REST API on port {:?}", port);
            stop_rest_api_interactive(
                port,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            )
            .await
            .map_err(|e| anyhow!("Failed to stop REST API: {}", e))?;
            if let Some(p) = port {
                if is_port_free(p).await {
                    info!("REST API on port {} stopped successfully", p);
                    println!("REST API on port {} stopped.", p);
                    Ok(())
                } else {
                    Err(anyhow!("REST API on port {} stopped but port is still in use", p))
                }
            } else {
                Ok(())
            }
        }
        StopAction::Daemon { port } => {
            info!("Stopping main daemon on port {:?}", port);
            stop_main_interactive(port, Arc::new(TokioMutex::new(None)))
                .await
                .map_err(|e| anyhow!("Failed to stop main daemon: {}", e))?;
            if let Some(p) = port {
                if is_port_free(p).await {
                    info!("Main daemon on port {} stopped successfully", p);
                    println!("Main daemon on port {} stopped.", p);
                    Ok(())
                } else {
                    Err(anyhow!("Main daemon on port {} stopped but port is still in use", p))
                }
            } else {
                Ok(())
            }
        }
    }
}

pub async fn handle_status_command(
    status_args: StatusArgs,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    match status_args.action {
        Some(StatusAction::Rest { port, cluster }) => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_rest_api_status(Some(p), rest_api_port_arc.clone()).await;
                }
            } else {
                display_rest_api_status(port, rest_api_port_arc).await;
            }
        }
        Some(StatusAction::Daemon { port, cluster }) => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_daemon_status(Some(p)).await;
                }
            } else {
                display_daemon_status(port).await;
            }
        }
        Some(StatusAction::Storage { port, cluster }) => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_storage_daemon_status(Some(p), storage_daemon_port_arc.clone()).await;
                }
            } else {
                display_storage_daemon_status(port, storage_daemon_port_arc).await;
            }
        }
        Some(StatusAction::Cluster) => {
            display_cluster_status().await;
        }
        Some(StatusAction::All) | None => {
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await?;
        }
        Some(StatusAction::Summary) => {
            display_full_status_summary(rest_api_port_arc, storage_daemon_port_arc).await?;
        }
        Some(StatusAction::Raft { port }) => {
            display_raft_status(port).await?;
        }
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
        RestartAction::Rest { port, cluster, rest_port, rest_cluster } => {
            println!("Restarting REST API server...");
            let ports_to_restart = if let Some(p) = port.or(rest_port) {
                vec![p]
            } else {
                find_all_running_rest_api_ports().await
            };
            let effective_cluster = cluster.or(rest_cluster);

            if ports_to_restart.is_empty() {
                println!("No REST API servers found running to restart. Starting one on default port {}.", DEFAULT_REST_API_PORT);
                stop_process_by_port("REST API", DEFAULT_REST_API_PORT).await?;
                start_rest_api_interactive(
                    Some(DEFAULT_REST_API_PORT),
                    effective_cluster.clone(),
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
                        effective_cluster.clone(),
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_port_arc.clone(),
                        rest_api_handle.clone(),
                    ).await?;
                    println!("REST API server restarted on port {}.", port);
                }
            }
        },
        RestartAction::Daemon { port, cluster, daemon_port, daemon_cluster } => {
            let actual_port = port.or(daemon_port).unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Restarting GraphDB Daemon on port {}...", actual_port);
            stop_daemon_instance_interactive(Some(actual_port), daemon_handles.clone()).await?;
            start_daemon_instance_interactive(
                Some(actual_port),
                cluster.or(daemon_cluster),
                daemon_handles,
            ).await?;
            println!("GraphDB Daemon restarted on port {}.", actual_port);
        },
        RestartAction::Storage { port, config_file, cluster, storage_port, storage_cluster } => {
            // Move async config loading outside the closure
            let default_port = match load_storage_config(None).await {
                Ok(c) => c.default_port,
                Err(_) => DEFAULT_STORAGE_PORT,
            };
            let actual_port = port.or(storage_port).unwrap_or(default_port);
            
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
                None,
                cluster.or(storage_cluster),
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

pub async fn handle_reload_command(reload_args: ReloadArgs) -> Result<(), anyhow::Error> {
    let action_to_perform = reload_args.action.unwrap_or(ReloadAction::All);
    let mut errors = Vec::new();

    // Load configuration for cluster range
    let config_path_toml = "server/src/cli/config.toml";
    let main_config_yaml = "server/main_app_config.yaml";
    let mut config_builder = Config::builder();
    if Path::new(config_path_toml).exists() {
        config_builder = config_builder.add_source(config::File::with_name(config_path_toml));
    }
    if Path::new(main_config_yaml).exists() {
        config_builder = config_builder.add_source(config::File::with_name(main_config_yaml));
    }
    let config = config_builder.build().context("Failed to load configuration")?;
    let cluster_range = config.get_string("main_daemon.cluster_range").ok();

    match action_to_perform {
        ReloadAction::All => {
            println!("Reloading all GraphDB components...");
            if let Err(e) = stop_daemon().await {
                error!("Failed to stop all components: {}", e);
                errors.push(format!("Stop all components: {}", e));
            }

            // Define reserved ports
            let rest_port = DEFAULT_REST_API_PORT;
            let storage_port = CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
            let reserved_ports = vec![rest_port, storage_port];

            // Reload daemons
            let daemon_ports = if let Some(range) = cluster_range {
                parse_cluster_range(&range)?
            } else {
                vec![DEFAULT_DAEMON_PORT]
            };
            for port in daemon_ports {
                if reserved_ports.contains(&port) {
                    warn!("Skipping daemon reload on port {}: reserved for another service.", port);
                    errors.push(format!("Daemon on port {}: reserved for another service", port));
                    continue;
                }
                println!("Reloading GraphDB Daemon on port {}...", port);
                if let Err(e) = stop_process_by_port("GraphDB Daemon", port).await {
                    warn!("Failed to stop daemon on port {}: {}", port, e);
                }
                match start_daemon(Some(port), None, reserved_ports.clone(), "main", None).await {
                    Ok(_) => {
                        info!("GraphDB Daemon restarted on port {}.", port);
                        println!("GraphDB Daemon restarted on port {}.", port);
                    }
                    Err(e) => {
                        error!("Failed to restart daemon on port {}: {}", port, e);
                        errors.push(format!("Daemon on port {}: {}", port, e));
                    }
                }
            }

            // Reload REST API
            let rest_ports_to_restart = find_all_running_rest_api_ports().await;
            for &port in &rest_ports_to_restart {
                println!("Reloading REST API on port {}...", port);
                if let Err(e) = stop_process_by_port("REST API", port).await {
                    warn!("Failed to stop REST API on port {}: {}", port, e);
                }
                match start_daemon(Some(port), None, reserved_ports.clone(), "rest", None).await {
                    Ok(_) => {
                        info!("REST API server restarted on port {}.", port);
                        println!("REST API server restarted on port {}.", port);
                    }
                    Err(e) => {
                        error!("Failed to restart REST API on port {}: {}", port, e);
                        errors.push(format!("REST API on port {}: {}", port, e));
                    }
                }
            }
            if rest_ports_to_restart.is_empty() {
                println!("No REST API servers found running. Starting one on port {}.", rest_port);
                if let Err(e) = stop_process_by_port("REST API", rest_port).await {
                    warn!("Failed to stop REST API on port {}: {}", rest_port, e);
                }
                match start_daemon(Some(rest_port), None, reserved_ports.clone(), "rest", None).await {
                    Ok(_) => {
                        info!("REST API server started on port {}.", rest_port);
                        println!("REST API server started on port {}.", rest_port);
                    }
                    Err(e) => {
                        error!("Failed to start REST API on port {}: {}", rest_port, e);
                        errors.push(format!("REST API on port {}: {}", rest_port, e));
                    }
                }
            }

            // Reload Storage Daemon
            let storage_ports = find_running_storage_daemon_port().await;
            let storage_port_to_restart = storage_ports.first().copied().unwrap_or(storage_port);
            println!("Reloading Storage Daemon on port {}...", storage_port_to_restart);
            if let Err(e) = stop_process_by_port("Storage Daemon", storage_port_to_restart).await {
                warn!("Failed to stop Storage Daemon on port {}: {}", storage_port_to_restart, e);
            }
            match start_daemon(Some(storage_port_to_restart), None, reserved_ports.clone(), "storage", None).await {
                Ok(_) => {
                    info!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                    println!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                }
                Err(e) => {
                    error!("Failed to restart Storage Daemon on port {}: {}", storage_port_to_restart, e);
                    errors.push(format!("Storage Daemon on port {}: {}", storage_port_to_restart, e));
                }
            }
        }
        ReloadAction::Rest => {
            println!("Reloading REST API server...");
            let reserved_ports = vec![CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
            let rest_port = DEFAULT_REST_API_PORT;
            let running_ports = find_all_running_rest_api_ports().await;
            if running_ports.is_empty() {
                println!("No REST API servers found running. Starting one on port {}.", rest_port);
                if let Err(e) = stop_process_by_port("REST API", rest_port).await {
                    warn!("Failed to stop REST API on port {}: {}", rest_port, e);
                }
                match start_daemon(Some(rest_port), None, reserved_ports.clone(), "rest", None).await {
                    Ok(_) => {
                        info!("REST API server started on port {}.", rest_port);
                        println!("REST API server started on port {}.", rest_port);
                    }
                    Err(e) => {
                        error!("Failed to start REST API on port {}: {}", rest_port, e);
                        errors.push(format!("REST API on port {}: {}", rest_port, e));
                    }
                }
            } else {
                for &port in &running_ports {
                    println!("Reloading REST API on port {}...", port);
                    if let Err(e) = stop_process_by_port("REST API", port).await {
                        warn!("Failed to stop REST API on port {}: {}", port, e);
                    }
                    match start_daemon(Some(port), None, reserved_ports.clone(), "rest", None).await {
                        Ok(_) => {
                            info!("REST API server restarted on port {}.", port);
                            println!("REST API server restarted on port {}.", port);
                        }
                        Err(e) => {
                            error!("Failed to restart REST API on port {}: {}", port, e);
                            errors.push(format!("REST API on port {}: {}", port, e));
                        }
                    }
                }
            }
        }
        ReloadAction::Storage => {
            println!("Reloading standalone Storage daemon...");
            let reserved_ports = vec![DEFAULT_REST_API_PORT];
            let storage_port = CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
            let storage_ports = find_running_storage_daemon_port().await;
            let storage_port_to_restart = storage_ports.first().copied().unwrap_or(storage_port);
            println!("Reloading Storage Daemon on port {}...", storage_port_to_restart);
            if let Err(e) = stop_process_by_port("Storage Daemon", storage_port_to_restart).await {
                warn!("Failed to stop Storage Daemon on port {}: {}", storage_port_to_restart, e);
            }
            match start_daemon(Some(storage_port_to_restart), None, reserved_ports.clone(), "storage", None).await {
                Ok(_) => {
                    info!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                    println!("Standalone Storage Daemon restarted on port {}.", storage_port_to_restart);
                }
                Err(e) => {
                    error!("Failed to restart Storage Daemon on port {}: {}", storage_port_to_restart, e);
                    errors.push(format!("Storage Daemon on port {}: {}", storage_port_to_restart, e));
                }
            }
        }
        ReloadAction::Daemon { port } => {
            let actual_port = port.unwrap_or(DEFAULT_DAEMON_PORT);
            println!("Reloading GraphDB daemon on port {}...", actual_port);
            let reserved_ports = vec![DEFAULT_REST_API_PORT, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
            if reserved_ports.contains(&actual_port) {
                warn!("Skipping daemon reload on port {}: reserved for another service.", actual_port);
                errors.push(format!("Daemon on port {}: reserved for another service", actual_port));
            } else {
                if let Err(e) = stop_process_by_port("GraphDB Daemon", actual_port).await {
                    warn!("Failed to stop daemon on port {}: {}", actual_port, e);
                }
                match start_daemon(Some(actual_port), None, reserved_ports.clone(), "main", None).await {
                    Ok(_) => {
                        info!("Daemon on port {} reloaded (stopped and restarted).", actual_port);
                        println!("Daemon on port {} reloaded (stopped and restarted).", actual_port);
                    }
                    Err(e) => {
                        error!("Failed to restart daemon on port {}: {}", actual_port, e);
                        errors.push(format!("Daemon on port {}: {}", actual_port, e));
                    }
                }
            }
        }
        ReloadAction::Cluster => {
            println!("Reloading cluster configuration...");
            let reserved_ports = vec![DEFAULT_REST_API_PORT, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
            let daemon_ports = if let Some(range) = cluster_range {
                parse_cluster_range(&range)?
            } else {
                vec![DEFAULT_DAEMON_PORT]
            };
            for port in daemon_ports {
                if reserved_ports.contains(&port) {
                    warn!("Skipping daemon reload on port {}: reserved for another service.", port);
                    errors.push(format!("Daemon on port {}: reserved for another service", port));
                    continue;
                }
                println!("Reloading GraphDB Daemon on port {}...", port);
                if let Err(e) = stop_process_by_port("GraphDB Daemon", port).await {
                    warn!("Failed to stop daemon on port {}: {}", port, e);
                }
                match start_daemon(Some(port), None, reserved_ports.clone(), "main", None).await {
                    Ok(_) => {
                        info!("GraphDB Daemon reloaded on port {}.", port);
                        println!("GraphDB Daemon reloaded on port {}.", port);
                    }
                    Err(e) => {
                        error!("Failed to reload daemon on port {}: {}", port, e);
                        errors.push(format!("Daemon on port {}: {}", port, e));
                    }
                }
            }
        }
    }

    if errors.is_empty() {
        info!("Reload command completed successfully");
        Ok(())
    } else {
        error!("Failed to reload one or more components: {:?}", errors);
        Err(anyhow::anyhow!("Failed to reload one or more components: {:?}", errors))
    }
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

pub async fn handle_show_plugins_command() -> Result<()> {
     let config = load_cli_config().await?;
     println!("Plugins Enabled: {}", config.enable_plugins);
     println!("Plugins will be shown here...");
     Ok(())
}

pub async fn handle_show_plugins_command_interactive() -> Result<()> {
     let config = load_cli_config().await?;
     println!("Plugins Enabled: {}", config.enable_plugins);
     println!("Plugins will be shown here...");
     Ok(())
}


pub async fn handle_save_config() -> Result<()> {
    println!("Saved configuration...");
     Ok(())
}

/// Enables or disables plugins and persists the setting to the config file.
pub async fn use_plugin(enable: bool) -> Result<()> {
    // Load the current configuration from server/src/cli/config.toml
    let config_path = PathBuf::from("server/src/cli/config.toml");
    let mut config = load_cli_config().await
        .map_err(|e| anyhow!("Failed to load config from {}: {}", config_path.display(), e))?;
    // Update the plugins_enabled field
    config.enable_plugins = enable;

    // Save the updated configuration to /opt/graphdb/config.toml
    let save_path = PathBuf::from("/opt/graphdb/config.toml");
    config.save()
        .map_err(|e| anyhow!("Failed to save config to {}: {}", save_path.display(), e))?;

    println!("Plugins set to enabled: {}", enable);
    Ok(())
}

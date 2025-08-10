// server/src/cli/handlers_storage.rs
// Created: 2025-08-09 - Implemented handlers for storage-related CLI commands
// Updated: 2025-08-09 - Fixed E0308 type mismatch in handle_show_storage_config_command
// Fixed: 2025-08-09 - Converted CliTomlStorageConfig to lib::storage_engine::config::StorageConfig

use anyhow::{Result, anyhow};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use std::path::{PathBuf};
use std::time::Duration;
use std::net::{IpAddr, SocketAddr};
use fs2::FileExt;
use std::future::Future;
use chrono::Utc;
use log::{info, error, warn, debug};
use futures::stream::StreamExt;
use crate::cli::commands::{CommandType, Commands, StartAction, StorageAction};
use crate::cli::config::{
    CliConfig,
    StorageConfig,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_STORAGE_PORT,
    CliTomlStorageConfig,
    load_storage_config_from_yaml,
    load_cli_config,
    daemon_api_storage_engine_type_to_string,
};
use crate::cli::handlers_utils::{format_engine_config, write_registry_fallback, execute_storage_query};
use daemon_api::start_daemon;
use daemon_api::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::storage_engine::config::{StorageEngineType, StorageConfig as EngineStorageConfig};
use lib::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use crate::cli::daemon_management::{
    is_port_free,
    find_pid_by_port,
    check_process_status_by_port,
    stop_process_by_port,
    parse_cluster_range,
};

pub mod storage {
    pub mod api {
        use anyhow::Result;
        pub async fn check_storage_daemon_status(_port: u16) -> Result<String> { Ok("Running".to_string()) }
    }
}

// This version of the function adds detailed debug logging to pinpoint
// the exact line where the program is failing. The issue is likely
// related to loading configuration files.
pub async fn start_storage_interactive(
    port: Option<u16>,
    config_file: Option<PathBuf>,
    _cluster_opt: Option<String>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<(), anyhow::Error> {
    use fs2::FileExt;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;
    use anyhow::Context;

    // --- STEP 1: LOAD CLI CONFIGURATION ---
    // Construct CommandType for interactive mode
    let command = CommandType::StartStorage {
        port,
        config_file: config_file.clone(),
        cluster: _cluster_opt.clone(),
        storage_port: port,
        storage_cluster: _cluster_opt,
    };
    let cli_config = CliConfig::load(Some(command))
        .with_context(|| "Failed to load CLI configuration. Check file permissions or path.")?;
    println!("SUCCESS: CLI config loaded.");

    // --- STEP 2: DETERMINE CONFIG FILE PATH ---
    let get_config_path = |cli_config_file: Option<PathBuf>| {
        cli_config_file.or(config_file.clone()).unwrap_or_else(|| {
            PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join("storage_config.yaml")
        })
    };
    
    let config_path = match &cli_config.command {
        Commands::Start { action: Some(StartAction::All { storage_config, .. }), .. } => get_config_path(storage_config.clone()),
        Commands::Start { action: Some(StartAction::Storage { config_file, .. }), .. } => get_config_path(config_file.clone()),
        Commands::Storage(StorageAction::Start { config_file, .. }) => get_config_path(config_file.clone()),
        _ => get_config_path(None),
    };
    info!("=======> I WILL BE TRYING TO LOAD IT FROM {:?}", config_path);
    println!("SUCCESS: Determined config path: {:?}", config_path);

    // --- STEP 3: LOAD STORAGE CONFIGURATION ---
    // This is the next potential point of failure.
    let storage_config = match load_storage_config_from_yaml(Some(config_path.clone())) {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load storage config from {:?}: {}.", config_path, e);
            if config_path.exists() {
                if let Ok(content) = std::fs::read_to_string(&config_path) {
                    error!("Config file content: {}", content);
                } else {
                    error!("Config file exists but cannot be read at {:?}", config_path);
                }
            } else {
                error!("Config file does not exist at {:?}", config_path);
            }
            return Err(e).context("Failed to load storage configuration");
        }
    };
    info!("Loaded Storage Config: {:?}", storage_config);
    println!("SUCCESS: Storage config loaded.");

    // --- THE REST OF THE FUNCTION (PREVIOUSLY PROVIDED) ---
    let selected_port = port.or_else(|| {
        match &cli_config.command {
            Commands::Start { action: Some(StartAction::All { storage_port, port: cmd_port, .. }), .. } => storage_port.or(*cmd_port),
            Commands::Start { action: Some(StartAction::Storage { storage_port, port: cmd_port, .. }), .. } => storage_port.or(*cmd_port),
            Commands::Storage(StorageAction::Start { storage_port, port: cmd_port, .. }) => storage_port.or(*cmd_port),
            _ => None,
        }
    }).unwrap_or(storage_config.default_port);

    info!("===> SELECTED PORT {}", selected_port);
    println!("===> SELECTED PORT {}", selected_port);

    let ip = "127.0.0.1";
    let ip_addr: IpAddr = ip.parse().with_context(|| format!("Invalid IP address: {}", ip))?;
    let addr = SocketAddr::new(ip_addr, selected_port);
    info!("Starting storage daemon on {}", addr);

    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    let is_graphdb_process = all_daemons.iter().any(|d| d.port == selected_port && d.service_type == "storage");

    if is_graphdb_process {
        info!("Port {} is in use by an existing GraphDB storage daemon. Removing stale registry entry.", selected_port);
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", selected_port).await {
            warn!("Failed to remove stale storage daemon from registry: {}", e);
        }
    } else if check_process_status_by_port("Storage Daemon", selected_port).await {
        info!("Port {} is in use by a process. Attempting to stop it.", selected_port);
        stop_process_by_port("Storage Daemon", selected_port).await
            .with_context(|| format!("Failed to stop existing process on port {}", selected_port))?;
    } else {
        info!("No process found running on port {}", selected_port);
    }
    
    if !is_port_free(selected_port).await {
        warn!("Port {} is still in use after cleanup attempt. This may cause issues.", selected_port);
    }

    let data_dir = storage_config.data_directory;
    let engine_type = daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type);

    debug!("Calling start_daemon with port: {:?}", Some(selected_port));
    start_daemon(Some(selected_port), None, vec![], "storage").await
        .with_context(|| format!("Failed to start storage daemon on port {}", selected_port))?;

    let pid = match find_pid_by_port(selected_port).await {
        Some(p) if p > 0 => p,
        _ => {
            log::warn!("No valid PID found via lsof for port {}. Checking registry.", selected_port);
            match GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(selected_port).await {
                Ok(Some(metadata)) if metadata.service_type == "storage" => metadata.pid,
                _ => {
                    log::error!("Failed to find PID for Storage Daemon on port {}.", selected_port);
                    return Err(anyhow!("Failed to find PID for Storage Daemon on port {}. Ensure the process is binding to the port.", selected_port));
                }
            }
        }
    };
    
    let metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port: selected_port,
        pid,
        ip_address: ip.to_string(),
        data_dir: Some(data_dir.clone()),
        config_path: Some(config_path),
        engine_type: Some(engine_type.clone()),
        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
    };

    async fn retry_operation<F, Fut, T>(operation: F, max_attempts: u8, desc: &str) -> Result<T, anyhow::Error>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, anyhow::Error>>,
    {
        for attempt in 0..max_attempts {
            match operation().await {
                Ok(result) => {
                    debug!("Successfully completed {} on attempt {}", desc, attempt + 1);
                    return Ok(result);
                }
                Err(e) => {
                    error!("Failed to {} (attempt {}/{}): {}", desc, attempt + 1, max_attempts, e);
                    if attempt + 1 >= max_attempts {
                        return Err(e).context(format!("Failed to {} after {} attempts", desc, max_attempts));
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
        }
        unreachable!()
    }
    
    let fallback_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.json");
    let fallback_dir = fallback_path.parent().ok_or_else(|| anyhow!("Invalid fallback path: {:?}", fallback_path))?;
    
    retry_operation(|| async {
        tokio::fs::create_dir_all(fallback_dir).await.map_err(anyhow::Error::from)
    }, 3, "create fallback directory").await?;
    
    retry_operation(|| async {
        GLOBAL_DAEMON_REGISTRY.register_daemon(metadata.clone()).await.map_err(anyhow::Error::from)
    }, 3, "register daemon").await?;
    
    info!("Successfully registered storage daemon with PID {} on port {}", pid, selected_port);
    println!("==> Successfully registered storage daemon with PID {} on port {}", pid, selected_port);

    let lock_path = PathBuf::from("/tmp/graphdb/daemon_registry_fallback.lock");
    let new_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry state after registering storage daemon: {:?}", new_daemons);

    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .mode(0o666)
        .open(&lock_path)
        .with_context(|| format!("Failed to open lock file {:?}", lock_path))?;
        
    retry_operation(|| async {
        lock_file.try_lock_exclusive().map_err(anyhow::Error::from).map(|_| ())
    }, 3, "acquire exclusive lock").await?;
    
    retry_operation(|| async {
        write_registry_fallback(&new_daemons, &fallback_path).await
    }, 3, "write registry fallback").await?;
    
    fs2::FileExt::unlock(&lock_file).with_context(|| format!("Failed to release lock on {:?}", lock_path))?;
    let _ = std::fs::remove_file(&lock_path);

    let health_check_timeout = Duration::from_secs(15);
    let poll_interval = Duration::from_millis(500);
    let start_time = tokio::time::Instant::now();

    while start_time.elapsed() < health_check_timeout {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            info!("Storage daemon started on port {} (PID {}) with engine type {:?}", selected_port, pid, engine_type);
            *storage_daemon_port_arc.lock().await = Some(selected_port);
            let (tx, rx) = oneshot::channel();
            *storage_daemon_shutdown_tx_opt.lock().await = Some(tx);
            let handle = tokio::spawn(async move {
                rx.await.ok();
                info!("Storage daemon on port {} shutting down", selected_port);
            });
            *storage_daemon_handle.lock().await = Some(handle);
            return Ok(());
        }
        tokio::time::sleep(poll_interval).await;
    }

    error!("Storage daemon on port {} failed to become reachable within {} seconds", selected_port, health_check_timeout.as_secs());
    Err(anyhow!("Storage daemon failed to start on port {}", selected_port))
}

/// Stops the Storage Daemon on the specified port or all storage daemons if no port is provided.
pub async fn stop_storage_interactive(
    port: Option<u16>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let ports_to_stop = if let Some(p) = port {
        vec![p]
    } else {
        let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join("storage_config.yaml");
        let storage_config = load_storage_config_from_yaml(Some(config_path))
            .unwrap_or_else(|_| StorageConfig::default());
        let default_port = if storage_config.default_port != 0 {
            storage_config.default_port
        } else {
            DEFAULT_STORAGE_PORT
        };
        let mut ports = GLOBAL_DAEMON_REGISTRY
            .get_all_daemon_metadata()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|d| d.service_type == "storage")
            .map(|d| d.port)
            .collect::<Vec<u16>>();
        if !ports.contains(&default_port) {
            ports.push(default_port);
        }
        ports
    };

    let mut errors = Vec::new();
    for actual_port in ports_to_stop {
        info!("Attempting to stop storage daemon on port {}", actual_port);

        // Check if the port is managed
        let is_managed_port = storage_daemon_port_arc.lock().await.as_ref() == Some(&actual_port);
        if is_managed_port {
            let mut shutdown_tx_lock = storage_daemon_shutdown_tx_opt.lock().await;
            let mut handle_lock = storage_daemon_handle.lock().await;
            if let Some(tx) = shutdown_tx_lock.take() {
                if tx.send(()).is_ok() {
                    info!("Sent shutdown signal to storage daemon on port {}", actual_port);
                } else {
                    warn!("Failed to send shutdown signal to storage daemon on port {}", actual_port);
                }
            }
            if let Some(handle) = handle_lock.take() {
                if let Err(e) = handle.await {
                    warn!("Failed to join storage daemon handle on port {}: {}", actual_port, e);
                }
                *storage_daemon_port_arc.lock().await = None;
            }
        }

        // Check if the daemon is running
        let is_running = check_process_status_by_port("Storage Daemon", actual_port).await;
        if !is_running {
            info!("No storage daemon running on port {}", actual_port);
            println!("No storage daemon running on port {}.", actual_port);
        } else {
            // Stop the process
            if let Err(e) = stop_process_by_port("Storage Daemon", actual_port).await {
                errors.push(anyhow!("Failed to stop storage daemon process on port {}: {}", actual_port, e));
            }
        }

        // Always attempt to deregister the daemon
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", actual_port).await {
            warn!("Failed to remove storage daemon (port: {}) from registry: {}", actual_port, e);
        } else {
            info!("Successfully removed storage daemon on port {} from registry", actual_port);
        }

        // Verify port is free with retries
        let mut attempts = 0;
        let max_attempts = 7;
        let mut port_free = false;
        while attempts < max_attempts {
            if is_port_free(actual_port).await {
                port_free = true;
                info!("Storage daemon on port {} stopped successfully", actual_port);
                println!("Storage daemon on port {} stopped.", actual_port);
                break;
            }
            debug!("Port {} still in use after attempt {}", actual_port, attempts + 1);
            tokio::time::sleep(Duration::from_millis(500)).await;
            attempts += 1;
        }

        if !port_free {
            errors.push(anyhow!("Port {} is still in use after attempting to stop storage daemon", actual_port));
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("Failed to stop storage daemon: {:?}", errors))
    }
}

/// Displays status of storage daemons only.
// Enhanced version of display_storage_daemon_status with detailed engine configuration
pub async fn display_storage_daemon_status(
    port_arg: Option<u16>, 
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>
) {
    let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
    debug!("Registry contents for storage status: {:?}", all_daemons);
    
    // Load actual storage configuration instead of using default
    let storage_config = load_storage_config_from_yaml(None).unwrap_or_else(|e| {
        warn!("Failed to load storage config: {}, using default", e);
        StorageConfig::default()
    });
    
    let running_storage_ports: Vec<u16> = futures::stream::iter(all_daemons.iter().filter(|d| d.service_type == "storage"))
        .filter_map(|d| async move {
            let mut attempts = 0;
            let max_attempts = 5;
            while attempts < max_attempts {
                if check_process_status_by_port("Storage Daemon", d.port).await {
                    debug!("Found running Storage Daemon on port {}", d.port);
                    return Some(d.port);
                }
                debug!("No process found for Storage Daemon on port {} (attempt {})", d.port, attempts + 1);
                tokio::time::sleep(Duration::from_millis(500)).await;
                attempts += 1;
            }
            None
        })
        .collect::<Vec<u16>>()
        .await;

    let ports_to_display = match port_arg {
        Some(p) if running_storage_ports.contains(&p) => vec![p],
        Some(p) => {
            println!("No Storage Daemon found on port {}.", p);
            vec![]
        }
        None => running_storage_ports,
    };

    println!("\n--- Storage Daemon Status ---");
    println!("{:<15} {:<10} {:<50}", "Status", "Port", "Configuration Details");
    println!("{:-<15} {:-<10} {:-<50}", "", "", "");
    
    if ports_to_display.is_empty() {
        println!("{:<15} {:<10} {:<50}", "Down", "N/A", "No storage daemons found in registry.");
    } else {
        for &port in &ports_to_display {
            let storage_daemon_status = if check_process_status_by_port("Storage Daemon", port).await {
                "Running".to_string()
            } else {
                "Down".to_string()
            };
            
            let metadata = all_daemons.iter().find(|d| d.port == port && d.service_type == "storage");
            let pid_info = if let Some(meta) = metadata {
                format!("PID: {} | Started: {:?}", meta.pid, meta.data_dir)
            } else {
                "PID: Unknown".to_string()
            };
            
            println!("{:<15} {:<10} {:<50}", storage_daemon_status, port, pid_info);
            println!("{:<15} {:<10} {:<50}", "", "", "");
            
            // Display comprehensive storage configuration
            println!("{:<15} {:<10} {:<50}", "", "", "=== Storage Configuration ===");
            
            // Display engine configuration details
            let engine_config_lines = format_engine_config(&storage_config);
            for config_line in engine_config_lines {
                println!("{:<15} {:<10} {:<50}", "", "", config_line);
            }
            
            println!("{:<15} {:<10} {:<50}", "", "", "");
            println!("{:<15} {:<10} {:<50}", "", "", "=== Directory Configuration ===");
            println!("{:<15} {:<10} {:<50}", "", "", format!("Data Directory: {}", storage_config.data_directory.display()));
            println!("{:<15} {:<10} {:<50}", "", "", format!("Log Directory: {}", storage_config.log_directory));
            println!("{:<15} {:<10} {:<50}", "", "", format!("Config Root: {}", storage_config.config_root_directory.display()));
            
            println!("{:<15} {:<10} {:<50}", "", "", "");
            println!("{:<15} {:<10} {:<50}", "", "", "=== Network & Scaling ===");
            println!("{:<15} {:<10} {:<50}", "", "", format!("Default Port: {}", storage_config.default_port));
            println!("{:<15} {:<10} {:<50}", "", "", format!("Cluster Range: {}", storage_config.cluster_range));
            println!("{:<15} {:<10} {:<50}", "", "", format!("Use Raft for Scale: {}", storage_config.use_raft_for_scale));
            
            // Add separator between multiple storage daemons
            if ports_to_display.len() > 1 && port != *ports_to_display.last().unwrap() {
                println!("{:<15} {:<10} {:<50}", "", "", "");
                println!("{:-<15} {:-<10} {:-<50}", "", "", "");
            }
        }
    }

    if port_arg.is_none() && !ports_to_display.is_empty() {
        println!("\nTo check a specific Storage, use 'status storage --port <port>'.");
    }
    
    *storage_daemon_port_arc.lock().await = ports_to_display.first().copied();
    println!("--------------------------------------------------");
}
/// Displays the current storage engine configuration.
pub async fn show_storage() -> Result<()> {
    // Load storage configuration
    let config_path = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let storage_config = load_storage_config_from_yaml(Some(config_path.clone()))
        .unwrap_or_else(|e| {
            warn!("Failed to load storage config from {:?}: {}, using default", config_path, e);
            StorageConfig::default()
        });

    // Get current engine type from StorageEngineManager
    let current_engine = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?
        .lock()
        .map_err(|e| anyhow!("Failed to lock StorageEngineManager: {}", e))?
        .current_engine_type();

    // Format engine configuration
    let engine_config_lines = format_engine_config(&storage_config);

    // Display configuration
    println!("\n--- Storage Engine Configuration ---");
    println!("{:<30} {}", "Current Engine", current_engine);
    println!("{:<30} {}", "Config File", config_path.display());
    println!("{:-<30} {}", "", "");
    println!("{:<30} {}", "Configuration Details", "");
    for line in engine_config_lines {
        println!("{:<30} {}", "", line);
    }
    println!("{:<30} {}", "Data Directory", storage_config.data_directory.display());
    println!("{:<30} {}", "Log Directory", storage_config.log_directory);
    println!("{:<30} {}", "Config Root", storage_config.config_root_directory.display());
    println!("{:<30} {}", "Default Port", storage_config.default_port);
    println!("{:<30} {}", "Cluster Range", storage_config.cluster_range);
    println!("{:<30} {}", "Use Raft for Scale", storage_config.use_raft_for_scale);
    println!("-----------------------------------");

    Ok(())
}

/// Handles `storage` subcommand for direct CLI execution.
pub async fn handle_storage_command(storage_action: StorageAction) -> Result<()> {
    match storage_action {
        StorageAction::Start { port, config_file, cluster, storage_port, storage_cluster } => {
            // Only check for conflicts in `start storage` context (not `start all`)
            let is_start_storage = storage_port.is_some() || storage_cluster.is_some() || (port.is_some() && storage_port.is_none() && cluster.is_some() && storage_cluster.is_none());
            if is_start_storage {
                if port.is_some() && storage_port.is_some() {
                    log::error!("Cannot specify both --port and --storage-port in `start storage`");
                    return Err(anyhow!("Cannot specify both --port and --storage-port"));
                }
                if cluster.is_some() && storage_cluster.is_some() {
                    log::error!("Cannot specify both --cluster and --storage-cluster in `start storage`");
                    return Err(anyhow!("Cannot specify both --cluster and --storage-cluster"));
                }
            }
            let actual_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let actual_cluster = storage_cluster.or(cluster);
            let actual_config_file = config_file.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR).join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));
            log::info!("Starting storage daemon with port: {:?}, cluster: {:?}, config: {:?}", actual_port, actual_cluster, actual_config_file);
            if let Some(cluster) = actual_cluster {
                let ports = parse_cluster_range(&cluster)?;
                log::info!("Parsed cluster range for storage daemon: {:?}", ports);
                for p in ports {
                    start_storage_interactive(
                        Some(p),
                        Some(actual_config_file.clone()),
                        None,
                        Arc::new(TokioMutex::new(None)),
                        Arc::new(TokioMutex::new(None)),
                        Arc::new(TokioMutex::new(None)),
                    ).await?;
                }
            } else {
                start_storage_interactive(
                    Some(actual_port),
                    Some(actual_config_file),
                    None,
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                    Arc::new(TokioMutex::new(None)),
                ).await?;
            }
            Ok(())
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(
                port,
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await
        }
        StorageAction::Status { port, cluster } => {
            if let Some(cluster) = cluster {
                let ports = parse_cluster_range(&cluster)?;
                for p in ports {
                    display_storage_daemon_status(Some(p), Arc::new(TokioMutex::new(None))).await;
                }
            } else {
                display_storage_daemon_status(port, Arc::new(TokioMutex::new(None))).await;
            }
            Ok(())
        }
        StorageAction::StorageQuery => {
            println!("Performing Storage Query (simulated, non-interactive mode)...");
            Ok(())
        }
        StorageAction::Show => {
            show_storage().await;
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
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();
            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}, Engine: {:?}", 
                             daemon.port, daemon.pid, daemon.data_dir, daemon.engine_type);
                }
            }
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
        StorageAction::Start { port, config_file, cluster, storage_port, storage_cluster } => {
            // Only check for conflicts in `start storage` context (not `start all`)
            let is_start_storage = storage_port.is_some() || storage_cluster.is_some() || (port.is_some() && storage_port.is_none() && cluster.is_some() && storage_cluster.is_none());
            if is_start_storage {
                if port.is_some() && storage_port.is_some() {
                    log::error!("Cannot specify both --port and --storage-port in `start storage`");
                    return Err(anyhow!("Cannot specify both --port and --storage-port"));
                }
                if cluster.is_some() && storage_cluster.is_some() {
                    log::error!("Cannot specify both --cluster and --storage-cluster in `start storage`");
                    return Err(anyhow!("Cannot specify both --cluster and --storage-cluster"));
                }
            }
            let actual_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let actual_cluster = storage_cluster.or(cluster);
            log::info!("Starting storage daemon interactively with port: {:?}, cluster: {:?}", actual_port, actual_cluster);
            start_storage_interactive(
                Some(actual_port),
                config_file,
                actual_cluster,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await
        }
        StorageAction::Stop { port } => {
            stop_storage_interactive(port, storage_daemon_shutdown_tx_opt, storage_daemon_handle, storage_daemon_port_arc).await
        }
        StorageAction::Status { port, .. } => {
            display_storage_daemon_status(port, storage_daemon_port_arc).await;
            Ok(())
        }
        StorageAction::StorageQuery => {
            execute_storage_query().await;
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
        StorageAction::Show => {
            show_storage().await;
            Ok(())
        }
        StorageAction::List => {
            let all_daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await.unwrap_or_default();
            let all_storage_daemons: Vec<DaemonMetadata> = all_daemons.into_iter()
                .filter(|d| d.service_type == "storage")
                .collect();
            if all_storage_daemons.is_empty() {
                println!("No storage daemons found in registry.");
            } else {
                println!("Registered Storage Daemons:");
                for daemon in all_storage_daemons {
                    println!("- Port: {}, PID: {}, Data Dir: {:?}, Engine: {:?}", 
                             daemon.port, daemon.pid, daemon.data_dir, daemon.engine_type);
                }
            }
            Ok(())
        }
    }
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

/// Updates the storage engine configuration and applies it via StorageEngineManager.
pub async fn use_storage_engine(engine_type_str: &str, permanent: bool) -> Result<()> {
    // Map string to StorageEngineType
    let engine_type = match engine_type_str.to_lowercase().as_str() {
        "sled" => StorageEngineType::Sled,
        "rocksdb" => StorageEngineType::RocksDB,
        "inmemory" => StorageEngineType::InMemory,
        "redis" => StorageEngineType::Redis,
        "postgresql" => StorageEngineType::PostgreSQL,
        "mysql" => StorageEngineType::MySQL,
        _ => return Err(anyhow!("Unknown storage engine: {}", engine_type_str)),
    };

    // Validate engine type against available engines
    let available_engines = StorageEngineManager::available_engines();
    if !available_engines.contains(&engine_type) {
        return Err(anyhow!("Storage engine {} is not enabled. Available engines: {:?}", engine_type_str, available_engines));
    }

    // Lock and update the StorageEngineManager
    info!("Executing use storage command for {} (permanent: {})", engine_type_str, permanent);
    {
        let mut manager = GLOBAL_STORAGE_ENGINE_MANAGER
            .get()
            .ok_or_else(|| anyhow!("StorageEngineManager not initialized"))?
            .lock()
            .map_err(|e| anyhow!("Failed to lock StorageEngineManager: {}", e))?;
        manager.use_storage(engine_type, permanent).await
            .map_err(|e| anyhow!("Failed to switch storage engine: {}", e))?;
    }

    println!("Switched to storage engine {} (persisted: {})", engine_type_str, permanent);
    Ok(())
}

pub async fn handle_save_storage() -> Result<()> {
    println!("Saved storage...");
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

/// Handles the 'use storage' command.
/// This function is responsible for both updating the configuration file and
/// setting the in-memory state for the running CLI session.
pub async fn handle_use_storage_command(engine: StorageEngineType, permanent: bool) -> Result<()> {
    // Load the current CLI configuration
    let mut config = load_cli_config()
        .map_err(|e| anyhow!("Failed to load CLI config: {}", e))?;

    // Ensure the storage section exists in the config
    if config.storage.is_none() {
        config.storage = Some(CliTomlStorageConfig::default());
    }

    // Get a mutable reference to the storage config
    if let Some(storage_config) = config.storage.as_mut() {
        // Update the storage engine type
        storage_config.storage_engine_type = Some(engine.clone());
    }

    // If the 'permanent' flag is set, save the updated configuration to disk
    if permanent {
        config.save()
            .map_err(|e| anyhow!("Failed to save CLI config: {}", e))?;
        println!("Set storage engine to {:?} (persisted)", engine);
    } else {
        println!("Set storage engine to {:?} (non-persisted)", engine);
    }

    Ok(())
}

/// Handles the 'show storage' command.
pub async fn handle_show_storage_command() -> Result<()> {
    let config = load_cli_config()
        .map_err(|e| anyhow!("Failed to load CLI config: {}", e))?;
    let engine_type = config.storage.and_then(|s| s.storage_engine_type);
    println!("Current Storage Engine: {:?}", engine_type);
    Ok(())
}

pub async fn handle_show_storage_command_interactive() -> Result<()> {
    let config = load_cli_config()
        .map_err(|e| anyhow!("Failed to load CLI config: {}", e))?;
    let engine_type = config.storage.and_then(|s| s.storage_engine_type);
    println!("Current Storage Engine: {:?}", engine_type);
    Ok(())
}

/// Handles the interactive 'use storage' command.
/// This function first calls the core handler to save the config, then
/// reloads the storage daemon to apply the new settings.
pub async fn handle_use_storage_interactive(
    engine: StorageEngineType,
    permanent: bool,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    // 1. First, save the configuration to disk
    handle_use_storage_command(engine, permanent).await?;

    // 2. Then, reload the storage daemon to apply the new configuration
    reload_storage_interactive(
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc,
    ).await?;

    Ok(())
}

pub async fn handle_show_storage_config_command() -> Result<()> {
    let config = load_cli_config()
        .map_err(|e| anyhow!("Failed to load CLI config: {}", e))?;

    let engine_config = config
        .storage
        .as_ref()
        .map(|cli_storage| EngineStorageConfig::from(cli_storage))
        .unwrap_or_else(|| EngineStorageConfig {
            storage_engine_type: StorageEngineType::RocksDB,
            data_directory: PathBuf::from("/opt/graphdb/storage_data"),
            connection_string: None,
            max_open_files: None,
            engine_specific_config: None,
            default_port: DEFAULT_STORAGE_PORT,
            log_directory: "/opt/graphdb/logs".to_string(),
            config_root_directory: PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            cluster_range: "".into(),
            use_raft_for_scale: false,
        });

    println!("Current Storage Configuration:");
    println!("- storage_engine_type: {:?}", engine_config.storage_engine_type);
    println!("- data_directory: {}", engine_config.data_directory.display());
    println!("- default_port: {}", engine_config.default_port);
    println!("- log_directory: {}", engine_config.log_directory);
    println!("- config_root_directory: {}", engine_config.config_root_directory.display());
    println!("- cluster_range: {}", engine_config.cluster_range);
    println!("- use_raft_for_scale: {}", engine_config.use_raft_for_scale);
    println!(
        "- max_open_files: {}",
        engine_config.max_open_files
            .map(|v| v.to_string())
            .unwrap_or_else(|| "None".into())
    );

    Ok(())
}

// daemon_api/src/lib.rs
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    process::{Command as StdCommand, Stdio},
    sync::{Arc, Mutex},
};

use anyhow::Result;
use chrono::Utc;
use config::{Config, File as ConfigFile};
use daemon::{DaemonizeBuilder, DaemonizeError};  // **Keep original**
use log::{debug, error, info, trace, warn};
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid as NixPid,
};
use sysinfo::{Pid as SysinfoPid, ProcessesToUpdate, System};
use lazy_static::lazy_static;
use tokio::{
    fs as tokio_fs,
    process::Command,
    time::{sleep, Duration},
};
use serde_json;
use lib::storage_engine::config::{
    daemon_api_storage_engine_type_to_string, load_storage_config_from_yaml, EngineStorageConfig,
    StorageEngineType, DEFAULT_STORAGE_CONFIG_PATH,
};
use lib::config::{SelectedStorageConfig, StorageConfigInner, PID_FILE_DIR};
use storage_daemon_server::{start_storage_daemon_server_real, StorageSettings};
use simplelog::{
    ColorChoice, CombinedLogger, ConfigBuilder, TermLogger, TerminalMode, WriteLogger,
};
use log::LevelFilter;

pub mod cli_schema;
pub mod help_generator;

pub use cli_schema::{
    CliArgs, DaemonCliCommand, GraphDbCommands, HelpArgs, RestCliCommand, StatusAction,
    StatusArgs, StopAction, StopArgs, StorageAction,
};
pub use help_generator::{generate_full_help, generate_help_for_path};
pub use lib::daemon_registry::{
    DaemonMetadata, DaemonRegistry, GLOBAL_DAEMON_REGISTRY,
};
pub use lib::daemon_config::{
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, DAEMON_REGISTRY_DB_PATH,
    DAEMON_PID_FILE_NAME_PREFIX, DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT,
    REST_PID_FILE_NAME_PREFIX, STORAGE_PID_FILE_NAME_PREFIX,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonData {
    pub port: u16,
    pub pid: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonStartConfig {
    pub config_path: String,
    pub daemon_type: String,
    pub port: u16,
    pub skip_ports: Vec<u16>,
    pub host: String,
    pub storage_config: Option<EngineStorageConfig>,
}

lazy_static! {
    pub static ref SHUTDOWN_FLAG: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

pub fn engine_storage_config_to_storage_settings(config: EngineStorageConfig) -> StorageSettings {
    let mut engine_specific_config = HashMap::new();
    if let Some(esc) = config.engine_specific_config {
        if let Some(path) = esc.storage.path {
            engine_specific_config.insert(
                "path".to_string(),
                serde_json::Value::String(path.display().to_string()),
            );
        }
        if let Some(host) = esc.storage.host {
            engine_specific_config.insert("host".to_string(), serde_json::Value::String(host));
        }
        if let Some(port) = esc.storage.port {
            engine_specific_config.insert("port".to_string(), serde_json::Value::Number(port.into()));
        }
        if let Some(username) = esc.storage.username {
            engine_specific_config.insert(
                "username".to_string(),
                serde_json::Value::String(username),
            );
        }
        if let Some(password) = esc.storage.password {
            engine_specific_config.insert(
                "password".to_string(),
                serde_json::Value::String(password),
            );
        }
        if let Some(database) = esc.storage.database {
            engine_specific_config.insert(
                "database".to_string(),
                serde_json::Value::String(database),
            );
        }
        if let Some(pd_endpoints) = esc.storage.pd_endpoints {
            engine_specific_config.insert(
                "pd_endpoints".to_string(),
                serde_json::Value::String(pd_endpoints),
            );
        }
    }

    StorageSettings {
        config_root_directory: config
            .config_root_directory
            .unwrap_or_else(|| PathBuf::from("./storage_daemon_server")),
        data_directory: config
            .data_directory
            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data")),
        log_directory: config
            .log_directory
            .unwrap_or_else(|| PathBuf::from("/opt/graphdb/logs")),
        default_port: config.default_port,
        cluster_range: config.cluster_range,
        max_disk_space_gb: config.max_disk_space_gb,
        min_disk_space_gb: config.min_disk_space_gb,
        use_raft_for_scale: config.use_raft_for_scale,
        storage_engine_type: daemon_api_storage_engine_type_to_string(&config.storage_engine_type),
        engine_specific_config,
        max_open_files: config.max_open_files,
    }
}

pub async fn is_process_running(pid: u32) -> bool {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::Some(&[SysinfoPid::from_u32(pid)]), true);
    sys.process(SysinfoPid::from_u32(pid)).is_some()
}

pub async fn find_pid_by_port(port: u16) -> Option<u32> {
    for attempt in 0..5 {
        let output = Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-sTCP:LISTEN")
            .arg("-t")
            .output()
            .await
            .ok()?;

        if output.status.success() {
            let pid_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !pid_str.is_empty() {
                if let Ok(pid) = pid_str.parse::<u32>() {
                    if is_process_running(pid).await {
                        info!("Found PID {} for port {} on attempt {}", pid, port, attempt);
                        return Some(pid);
                    }
                }
            }
        }
        sleep(Duration::from_millis(200)).await;
    }
    info!("No valid PID found for port {} after 5 attempts", port);
    None
}

fn remove_pid_file(pid_file_path: &str) {
    if Path::new(pid_file_path).exists() {
        if let Err(e) = fs::remove_file(pid_file_path) {
            error!("Failed to remove PID file {}: {}", pid_file_path, e);
        } else {
            info!("Removed PID file {}", pid_file_path);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
    #[error("Signal handling error: {0}")]
    SignalError(String),
    #[error("Daemonize error: {0}")]
    Daemonize(String),
    #[error("Invalid port range: {0}")]
    InvalidPortRange(String),
    #[error("Invalid cluster format: {0}")]
    InvalidClusterFormat(String),
    #[error("No daemons started")]
    NoDaemonsStarted,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Config error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("Process error: {0}")]
    ProcessError(String),
    #[error("General error: {0}")]
    GeneralError(String),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

impl From<DaemonizeError> for DaemonError {
    fn from(err: DaemonizeError) -> Self {
        DaemonError::Daemonize(format!("{}", err))
    }
}

/* -------------------------------------------------------------------------- */
/*                         CORE START DAEMON (uses original daemonize)        */
/* -------------------------------------------------------------------------- */
pub async fn start_daemon(
    port: Option<u16>,
    cluster_range: Option<String>,
    skip_ports: Vec<u16>,
    daemon_type: &str,
    storage_config: Option<EngineStorageConfig>,
) -> Result<(), DaemonError> {
    ensure_pid_directory().await?;

    let config_path = "server/src/cli/config.toml";
    let main_config_yaml = "server/main_app_config.yaml";
    let rest_config_yaml = "rest_api/rest_api_config.yaml";
    let storage_config_yaml = DEFAULT_STORAGE_CONFIG_PATH;

    let mut host_to_use = "127.0.0.1".to_string();
    let mut default_port = match daemon_type {
        "main" => DEFAULT_DAEMON_PORT,
        "rest" => DEFAULT_REST_API_PORT,
        "storage" => CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
        _ => {
            return Err(DaemonError::GeneralError(format!(
                "Invalid daemon_type: {}",
                daemon_type
            )))
        }
    };
    let mut base_process_name = format!("graphdb-{}", daemon_type);

    let mut config_builder = Config::builder();
    if Path::new(config_path).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(config_path));
    }
    if daemon_type == "main" && Path::new(main_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(main_config_yaml));
    }
    if daemon_type == "rest" && Path::new(rest_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(rest_config_yaml));
    }

    if let Ok(cfg) = config_builder.build() {
        if let Ok(host) = cfg.get_string("server.host") {
            host_to_use = host;
        }
        if let Ok(p) = cfg.get_int(&format!("{}.port", daemon_type)) {
            default_port = p as u16;
        } else if daemon_type == "rest" {
            if let Ok(p) = cfg.get_int("rest_api.default_port") {
                default_port = p as u16;
            }
        } else if daemon_type == "storage" {
            if let Ok(p) = cfg.get_int("storage.default_port") {
                default_port = p as u16;
            }
        }
        if let Ok(name) = cfg.get_string(&format!("{}.process_name", daemon_type)) {
            base_process_name = name;
        }
    }

    let settings = if daemon_type == "storage" {
        if let Some(cfg) = storage_config {
            info!("Using in-memory storage configuration.");
            Some(cfg)
        } else {
            info!(
                "Loading storage configuration from file: {}",
                storage_config_yaml
            );
            let cfg = load_storage_config_from_yaml(Some(PathBuf::from(storage_config_yaml)))
                .await
                .map_err(|e| {
                    anyhow::Error::new(e)
                        .context(format!("Failed to load storage config from {}", storage_config_yaml))
                })?;
            Some(cfg)
        }
    } else {
        None
    };

    let mut ports_to_start: Vec<u16> = Vec::new();
    if let Some(range) = cluster_range {
        if daemon_type == "main" {
            let parts: Vec<&str> = range.split('-').collect();
            if parts.len() != 2 {
                return Err(DaemonError::InvalidClusterFormat(range));
            }
            let start = parts[0].parse::<u16>().map_err(|_| {
                DaemonError::InvalidPortRange(format!("Invalid start port: {}", parts[0]))
            })?;
            let end = parts[1].parse::<u16>().map_err(|_| {
                DaemonError::InvalidPortRange(format!("Invalid end port: {}", parts[1]))
            })?;
            if start == 0 || end == 0 || start > end {
                return Err(DaemonError::InvalidPortRange(range));
            }
            if end - start + 1 > 10 {
                return Err(DaemonError::InvalidPortRange(format!(
                    "Cluster port range size ({}) exceeds maximum allowed (10).",
                    end - start + 1
                )));
            }
            ports_to_start.extend(start..=end);
        } else {
            ports_to_start.push(port.unwrap_or(default_port));
        }
    } else {
        ports_to_start.push(port.unwrap_or(default_port));
    }

    let max_port_check_attempts = if daemon_type == "storage" { 30 } else { 15 };
    let port_check_interval_ms = 1000;
    let mut any_started = false;

    for current_port in ports_to_start {
        if skip_ports.contains(&current_port) {
            info!("Skipping reserved port {} for {}", current_port, daemon_type);
            continue;
        }

        if let Err(e) = stop_port_daemon(current_port, daemon_type).await {
            warn!("Failed to clean up stale daemon on port {}: {}", current_port, e);
        }

        let socket_addr = format!("{}:{}", host_to_use, current_port)
            .to_socket_addrs()
            .map_err(DaemonError::Io)?
            .next()
            .ok_or_else(|| {
                DaemonError::InvalidPortRange(format!("No socket for port {}", current_port))
            })?;

        if TcpStream::connect(&socket_addr).is_ok() {
            info!(
                "Port {} already in use. Stopping existing process.",
                current_port
            );
            if let Err(e) = stop_port_daemon(current_port, daemon_type).await {
                warn!("Failed to stop existing process on port {}: {}", current_port, e);
                continue;
            }
        }

        let pid_file_path = format!(
            "{}/graphdb-{}-{}.pid",
            PID_FILE_DIR, daemon_type, current_port
        );
        let stdout_file_path = format!(
            "{}/graphdb-{}-{}.out",
            PID_FILE_DIR, daemon_type, current_port
        );
        let stderr_file_path = format!(
            "{}/graphdb-{}-{}.err",
            PID_FILE_DIR, daemon_type, current_port
        );

        let stale_tmp = format!("/tmp/graphdb-{}-{}.pid", daemon_type, current_port);
        remove_pid_file(&stale_tmp);
        remove_pid_file(&pid_file_path);

        let stdout = File::create(&stdout_file_path)?;
        let stderr = File::create(&stderr_file_path)?;

        let mut daemonize = DaemonizeBuilder::new()
            .working_directory(PID_FILE_DIR)
            .umask(0o022)
            .stdout(stdout)
            .stderr(stderr)
            .process_name(&format!("graphdb-{}-{}", daemon_type, current_port))
            .host(&host_to_use)
            .port(current_port)
            .skip_ports(skip_ports.clone())
            .pid_file_path(&pid_file_path)
            .build()?;

        // Create PID directory before start()
        if let Some(parent) = Path::new(&pid_file_path).parent() {
            fs::create_dir_all(parent).map_err(DaemonError::Io)?;
        }

        let child_pid = daemonize.start()?;

        // ---------- CHILD PROCESS ----------
        if child_pid == 0 {
            if daemon_type == "storage" {
                // ✅ Child process can create its own runtime
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    error!("Failed to create Tokio runtime: {}", e);
                    DaemonError::GeneralError(format!("Failed to create Tokio runtime: {}", e))
                })?;
                let result = rt.block_on(async {
                    match settings.clone() {
                        Some(cfg) => {
                            let settings = engine_storage_config_to_storage_settings(cfg);
                            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

                            let _signal_handle = {
                                let mut term_signal = tokio::signal::unix::signal(
                                    tokio::signal::unix::SignalKind::terminate(),
                                )
                                .map_err(|e| DaemonError::SignalError(e.to_string()))?;
                                tokio::spawn(async move {
                                    tokio::select! {
                                        _ = tokio::signal::ctrl_c() => {
                                            let _ = shutdown_tx.send(());
                                        }
                                        _ = term_signal.recv() => {
                                            let _ = shutdown_tx.send(());
                                        }
                                    }
                                })
                            };

                            debug!("[Storage Daemon] Starting on port {}", current_port);
                            start_storage_daemon_server_real(current_port, settings, shutdown_rx)
                                .await
                                .map_err(DaemonError::Anyhow)
                        }
                        None => Err(DaemonError::GeneralError(
                            "Storage settings not loaded".to_string(),
                        )),
                    }
                });

                if let Err(e) = result {
                    error!("[Storage Daemon] Failed: {:?}", e);
                    std::process::exit(1);
                }
                std::process::exit(0);
            } else {
                let cfg = DaemonStartConfig {
                    daemon_type: daemon_type.to_string(),
                    port: current_port,
                    skip_ports: skip_ports.clone(),
                    host: host_to_use.clone(),
                    config_path: storage_config_yaml.to_string(),
                    storage_config: settings.clone(),
                };
                let json = serde_json::to_string(&cfg)?;
                let args = vec![
                    "--internal-run".to_string(),
                    "--config-json".to_string(),
                    json,
                ];
                let mut cmd = Command::new(std::env::current_exe()?);
                cmd.args(&args)
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                let status = cmd.spawn()?.wait().await?;
                std::process::exit(status.code().unwrap_or(1));
            }
        }

        // ---------- PARENT PROCESS ----------
        // ✅ FIX: Use spawn_blocking for parent process port checks
        // This prevents "runtime within runtime" errors in interactive mode
        let socket_addr_clone = socket_addr;
        let daemon_type_str = daemon_type.to_string();
        
        let (confirmed_pid, last_error) = tokio::task::spawn_blocking(move || {
            let mut confirmed_pid = 0;
            let mut last_error: Option<std::io::Error> = None;
            
            for attempt in 0..max_port_check_attempts {
                // ✅ Use std::thread::sleep instead of tokio::time::sleep
                std::thread::sleep(std::time::Duration::from_millis(port_check_interval_ms));

                match TcpStream::connect(&socket_addr_clone) {
                    Ok(_) => {
                        debug!("[Parent] Connected to {} on port {}", daemon_type_str, current_port);
                        // ✅ find_pid_by_port is async, but we're in spawn_blocking
                        // Create a new runtime for this single async operation
                        // This is safe because we're in a blocking context
                        match tokio::runtime::Runtime::new() {
                            Ok(rt) => {
                                confirmed_pid = rt.block_on(async {
                                    find_pid_by_port(current_port).await.unwrap_or(0)
                                });
                                if confirmed_pid != 0 {
                                    info!(
                                        "{} daemon started on port {} with PID {}",
                                        daemon_type_str, current_port, confirmed_pid
                                    );
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Failed to create runtime for PID check: {}", e);
                                // Continue trying other attempts
                            }
                        }
                    }
                    Err(e) => {
                        debug!("[Parent] Connect attempt {} failed: {}", attempt + 1, e);
                        last_error = Some(e);
                    }
                }
            }
            
            (confirmed_pid, last_error)
        }).await.map_err(|e| DaemonError::GeneralError(format!("Join error: {}", e)))?;

        if confirmed_pid == 0 {
            error!(
                "{} daemon failed to bind to port {} after {} attempts. Last error: {:?}",
                daemon_type, current_port, max_port_check_attempts, last_error
            );
            return Err(DaemonError::GeneralError(format!(
                "Daemon did not start on port {} after {} attempts",
                current_port, max_port_check_attempts
            )));
        }

        if let Err(e) = fs::write(&pid_file_path, confirmed_pid.to_string()) {
            error!("Parent failed to write PID file {}: {}", pid_file_path, e);
        } else {
            info!("Parent wrote PID file {} with PID {}", pid_file_path, confirmed_pid);
        }

        let metadata = DaemonMetadata {
            service_type: daemon_type.to_string(),
            port: current_port,
            pid: confirmed_pid,
            ip_address: host_to_use.clone(),
            data_dir: settings.as_ref().and_then(|s| s.data_directory.clone()),
            config_path: if daemon_type == "storage" && settings.is_none() {
                Some(PathBuf::from(storage_config_yaml))
            } else {
                None
            },
            engine_type: if daemon_type == "storage" {
                settings
                    .as_ref()
                    .map(|s| daemon_api_storage_engine_type_to_string(&s.storage_engine_type))
            } else {
                None
            },
            last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            zmq_ready: false,
            engine_synced: false,
        };

        if let Err(e) = GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await {
            error!(
                "Failed to register daemon {} on port {}: {}",
                daemon_type, current_port, e
            );
        }

        any_started = true;
    }

    if !any_started {
        return Err(DaemonError::NoDaemonsStarted);
    }
    Ok(())
}

/* -------------------------------------------------------------------------- */
/*                         STOP LOGIC (unchanged)                              */
/* -------------------------------------------------------------------------- */
pub async fn stop_port_daemon(port: u16, daemon_type: &str) -> Result<(), DaemonError> {
    info!("Attempting to stop {} daemon on port {}...", daemon_type, port);

    let pid_file_path = format!("/tmp/graphdb-{}-{}.pid", daemon_type, port);
    let legacy_pid_file_path = format!("/tmp/graphdb-daemon-{}.pid", port);
    remove_pid_file(&pid_file_path);
    remove_pid_file(&legacy_pid_file_path);

    if let Some(meta) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
        if meta.service_type == daemon_type {
            if is_process_running(meta.pid).await {
                let _ = kill(NixPid::from_raw(meta.pid as i32), Signal::SIGTERM);
                sleep(Duration::from_millis(500)).await;
                let addr = format!("127.0.0.1:{}", port);
                if addr.to_socket_addrs()?.next().and_then(|sa| {
                    std::net::TcpStream::connect_timeout(&sa, Duration::from_millis(500)).err()
                }).is_some() {
                    let _ = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await;
                    return Ok(());
                }
            } else {
                let _ = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await;
                return Ok(());
            }
        }
    }

    if let Some(pid) = find_pid_by_port(port).await {
        let _ = kill(NixPid::from_raw(pid as i32), Signal::SIGTERM);
        sleep(Duration::from_millis(500)).await;
        let addr = format!("127.0.0.1:{}", port);
        if addr.to_socket_addrs()?.next().and_then(|sa| {
            std::net::TcpStream::connect_timeout(&sa, Duration::from_millis(500)).err()
        }).is_some() {
            let _ = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await;
            return Ok(());
        }
    }

    let pgrep_arg = format!("graphdb-{} --internal-port {}", daemon_type, port);
    if let Ok(out) = StdCommand::new("pgrep")
        .arg("-f")
        .arg(&pgrep_arg)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|c| c.wait_with_output())
    {
        let pids: Vec<u32> = String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
            .collect();
        for pid in pids {
            let _ = kill(NixPid::from_raw(pid as i32), Signal::SIGTERM);
            sleep(Duration::from_millis(500)).await;
            let addr = format!("127.0.0.1:{}", port);
            if addr.to_socket_addrs()?.next().and_then(|sa| {
                std::net::TcpStream::connect_timeout(&sa, Duration::from_millis(500)).err()
            }).is_some() {
                let _ = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await;
                return Ok(());
            }
        }
    }

    Ok(())
}

pub async fn stop_daemon() -> Result<(), DaemonError> {
    info!("Stopping all GraphDB daemons...");
    let _ = stop_all_registered_daemons().await;

    let config_path_toml = "server/src/cli/config.toml";
    let main_config_yaml = "server/main_app_config.yaml";
    let rest_config_yaml = "rest_api/rest_api_config.yaml";
    let storage_config_yaml = "storage_daemon_server/storage_config.yaml";
    let mut known_ports = HashSet::new();

    let mut config_builder = Config::builder();
    if Path::new(config_path_toml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(config_path_toml));
    }
    if Path::new(main_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(main_config_yaml));
    }
    if Path::new(rest_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(rest_config_yaml));
    }
    if Path::new(storage_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(storage_config_yaml));
    }

    if let Ok(config) = config_builder.build() {
        if let Ok(st_port) = config.get_int("storage.default_port") {
            known_ports.insert(st_port as u16);
        }
        if let Ok(r_port) = config.get_int("rest_api.default_port") {
            known_ports.insert(r_port as u16);
        }
        if let Ok(cfg_port) = config.get_int("main_daemon.default_port") {
            known_ports.insert(cfg_port as u16);
        }
        if let Ok(range_str) = config.get_string("main_daemon.cluster_range") {
            let parts: Vec<&str> = range_str.split('-').collect();
            if parts.len() == 2 {
                if let (Ok(start_port), Ok(end_port)) =
                    (parts[0].parse::<u16>(), parts[1].parse::<u16>())
                {
                    if start_port != 0 && end_port != 0 && start_port <= end_port {
                        for p in start_port..=end_port {
                            known_ports.insert(p);
                        }
                    }
                }
            }
        }
    }

    known_ports.insert(DEFAULT_DAEMON_PORT);
    known_ports.insert(DEFAULT_REST_API_PORT);
    known_ports.insert(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS);

    info!("Fallback stop on known ports: {:?}", known_ports);

    for &port in &known_ports {
        if let Some(pid) = find_pid_by_port(port).await {
            info!(
                "Found unregistered process (PID: {}) on port {}. Stopping...",
                pid, port
            );
            let _ = kill(NixPid::from_raw(pid as i32), Signal::SIGTERM);
        }
    }

    for &port in &known_ports {
        let legacy_pid_file = format!("/tmp/graphdb-daemon-{}.pid", port);
        remove_pid_file(&legacy_pid_file);
    }

    let _ = GLOBAL_DAEMON_REGISTRY.clear_all_daemons().await;
    *SHUTDOWN_FLAG.lock().unwrap() = true;
    info!("All daemons stopped and registry cleared.");
    Ok(())
}

async fn stop_all_registered_daemons() -> Result<(), DaemonError> {
    info!("Stopping daemons from registry...");
    let daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await?;
    let mut stopped = Vec::new();

    for meta in daemons {
        info!(
            "Stopping registered {} on port {} (PID {})",
            meta.service_type, meta.port, meta.pid
        );
        if stop_port_daemon(meta.port, &meta.service_type).await.is_ok() {
            stopped.push(meta.port);
        }
    }

    for port in stopped {
        let _ = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await;
    }
    Ok(())
}

pub async fn ensure_pid_directory() -> Result<(), DaemonError> {
    let dir = Path::new(PID_FILE_DIR);
    if !dir.exists() {
        tokio_fs::create_dir_all(dir).await.map_err(|e| {
            error!("Failed to create PID directory {}: {}", PID_FILE_DIR, e);
            DaemonError::Io(e)
        })?;
        info!("Created PID directory {}", PID_FILE_DIR);
    }
    Ok(())
}

pub async fn find_running_storage_daemon_port() -> Option<u16> {
    let config_path_toml = "server/src/cli/config.toml";
    let storage_config_yaml = "storage_daemon_server/storage_config.yaml";
    let mut storage_port = CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;

    let mut config_builder = Config::builder();
    if Path::new(config_path_toml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(config_path_toml));
        info!("Loaded TOML config from {}", config_path_toml);
    }
    if Path::new(storage_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(storage_config_yaml));
        info!("Loaded storage YAML config from {}", storage_config_yaml);
    }

    if let Ok(config) = config_builder.build() {
        if let Ok(st_port) = config.get_int("storage.default_port") {
            storage_port = st_port as u16;
            info!("Using storage port: {}", storage_port);
        }
    }

    if let Some(metadata) = GLOBAL_DAEMON_REGISTRY.find_daemon_by_port(storage_port).await.ok().flatten() {
        if metadata.service_type == "storage" {
            info!("Found storage daemon on port {} with PID {}", storage_port, metadata.pid);
            return Some(storage_port);
        }
    }

    let output = Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", storage_port))
        .arg("-t")
        .output()
        .await;

    if let Ok(output) = output {
        let pids = String::from_utf8_lossy(&output.stdout);
        if !pids.trim().is_empty() {
            info!("Found storage daemon listening on port {}", storage_port);
            return Some(storage_port);
        }
    }

    let pgrep_result = StdCommand::new("pgrep")
        .arg("-f")
        .arg("graphdb-storage")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    if let Ok(output) = pgrep_result {
        let pids_str = String::from_utf8_lossy(&output.stdout);
        let pids: Vec<u32> = pids_str.lines()
            .filter_map(|line| line.trim().parse::<u32>().ok())
            .collect();
        if !pids.is_empty() {
            info!("Found storage daemon by process name. Returning configured port {}.", storage_port);
            return Some(storage_port);
        }
    }

    None
}

pub async fn stop_daemon_api_call() -> Result<(), anyhow::Error> {
    stop_daemon()
        .await
        .map_err(|e| anyhow::anyhow!("Daemon stop failed: {}", e))
}
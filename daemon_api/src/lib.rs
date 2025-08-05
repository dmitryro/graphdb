use serde::{Serialize, Deserialize};
use std::fs::{self, File};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use anyhow::Result;
use config::{Config, File as ConfigFile};
use daemon::{DaemonizeBuilder, DaemonizeError};
use log::{error, info};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid as NixPid;
use sysinfo::{System, Pid as SysinfoPid, ProcessesToUpdate};
use lazy_static::lazy_static;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

pub mod cli_schema;
pub mod help_generator;
pub mod daemon_config;
pub mod daemon_registry;

pub use cli_schema::{CliArgs, GraphDbCommands, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, StopAction, StatusAction, HelpArgs};
pub use help_generator::{generate_full_help, generate_help_for_path};
pub use daemon_registry::{
    GLOBAL_DAEMON_REGISTRY,
    DaemonRegistry, DaemonMetadata,
};
pub use crate::daemon_config::{
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS, 
    DEFAULT_DAEMON_PORT, 
    DEFAULT_REST_API_PORT,
    DAEMON_REGISTRY_DB_PATH,
    DAEMON_PID_FILE_NAME_PREFIX,
    REST_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_NAME_PREFIX
};

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonData {
    pub port: u16,
    pub pid: u32,
}

lazy_static! {
    pub static ref SHUTDOWN_FLAG: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
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

fn cleanup_existing_daemons(base_process_name: &str) {
    let _ = std::process::Command::new("pkill")
        .arg("-f")
        .arg(base_process_name)
        .status();
    std::thread::sleep(std::time::Duration::from_millis(1000));
}

#[derive(Debug, thiserror::Error)]
pub enum DaemonError {
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
}

impl From<DaemonizeError> for DaemonError {
    fn from(err: DaemonizeError) -> Self {
        DaemonError::Daemonize(format!("{}", err))
    }
}

pub async fn start_daemon(
    port: Option<u16>,
    cluster_range: Option<String>,
    skip_ports: Vec<u16>,
    daemon_type: &str,
) -> Result<(), DaemonError> {
    let config_path = "server/src/cli/config.toml";
    let rest_config_yaml = "rest_api/rest_api_config.yaml";
    let storage_config_yaml = "storage_daemon_server/storage_config.yaml";
    let mut host_to_use = "127.0.0.1".to_string();
    let mut default_port = match daemon_type {
        "main" => DEFAULT_DAEMON_PORT,
        "rest" => DEFAULT_REST_API_PORT,
        "storage" => CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
        _ => return Err(DaemonError::GeneralError(format!("Invalid daemon_type: {}", daemon_type))),
    };
    let mut base_process_name = format!("graphdb-{}", daemon_type);

    let mut config_builder = Config::builder();
    if Path::new(config_path).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(config_path));
    }
    if daemon_type == "rest" && Path::new(rest_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(rest_config_yaml));
    }
    if daemon_type == "storage" && Path::new(storage_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(storage_config_yaml));
    }

    if let Ok(config) = config_builder.build() {
        if let Ok(host) = config.get_string("server.host") {
            host_to_use = host;
        }
        if let Ok(cfg_port) = config.get_int(format!("{}.port", daemon_type).as_str()) {
            default_port = cfg_port as u16;
        } else if daemon_type == "rest" {
            if let Ok(cfg_port) = config.get_int("rest_api.default_port") {
                default_port = cfg_port as u16;
            }
        } else if daemon_type == "storage" {
            if let Ok(cfg_port) = config.get_int("storage.default_port") {
                default_port = cfg_port as u16;
            }
        }
        if let Ok(process_name) = config.get_string(format!("{}.process_name", daemon_type).as_str()) {
            base_process_name = process_name;
        }
    }

    let mut ports_to_start: Vec<u16> = Vec::new();
    if let Some(range_str) = cluster_range {
        if daemon_type == "rest" || daemon_type == "storage" {
            ports_to_start.push(port.unwrap_or(default_port));
        } else {
            let parts: Vec<&str> = range_str.split('-').collect();
            if parts.len() != 2 {
                return Err(DaemonError::InvalidClusterFormat(range_str));
            }
            let start_port = parts[0].parse::<u16>().map_err(|_| {
                DaemonError::InvalidPortRange(format!("Invalid start port: {}", parts[0]))
            })?;
            let end_port = parts[1].parse::<u16>().map_err(|_| {
                DaemonError::InvalidPortRange(format!("Invalid end port: {}", parts[1]))
            })?;

            if start_port == 0 || end_port == 0 || start_port > end_port {
                return Err(DaemonError::InvalidPortRange(range_str));
            }
            if end_port - start_port + 1 > 10 {
                return Err(DaemonError::InvalidPortRange(format!(
                    "Cluster port range size ({}) exceeds maximum allowed (10).",
                    end_port - start_port + 1
                )));
            }
            for p in start_port..=end_port {
                ports_to_start.push(p);
            }
        }
    } else {
        ports_to_start.push(port.unwrap_or(default_port));
    }

    let max_port_check_attempts = 5;
    let port_check_interval_ms = 200;
    let mut any_started = false;

    for current_port in ports_to_start {
        if skip_ports.contains(&current_port) {
            info!("Skipping reserved port {} for {}", current_port, daemon_type);
            continue;
        }

        let socket_addr = format!("{}:{}", host_to_use, current_port)
            .to_socket_addrs()
            .map_err(DaemonError::Io)?
            .next()
            .ok_or_else(|| DaemonError::InvalidPortRange(format!("No socket for port {}", current_port)))?;

        if TcpStream::connect(&socket_addr).is_ok() {
            info!("Port {} is already in use for {}. Skipping start.", current_port, daemon_type);
            any_started = true;
            continue;
        }

        let pid_file_path = format!("/tmp/graphdb-{}-{}.pid", daemon_type, current_port);
        if Path::new(&pid_file_path).exists() {
            info!("Removing stale PID file for {} on port {}: {}", daemon_type, current_port, pid_file_path);
            remove_pid_file(&pid_file_path);
        }

        let specific_process_name = format!("graphdb-{}-{}", daemon_type, current_port);
        let specific_stdout_file_path = format!("/tmp/graphdb-{}-{}.out", daemon_type, current_port);
        let specific_stderr_file_path = format!("/tmp/graphdb-{}-{}.err", daemon_type, current_port);

        let stdout = File::create(&specific_stdout_file_path)?;
        let stderr = File::create(&specific_stderr_file_path)?;

        let mut daemonize = DaemonizeBuilder::new()
            .working_directory("/tmp")
            .umask(0o027)
            .stdout(stdout)
            .stderr(stderr)
            .process_name(&specific_process_name)
            .host(&host_to_use)
            .port(current_port)
            .skip_ports(skip_ports.clone())
            .build()?;

        match daemonize.start() {
            Ok(child_pid) => {
                if child_pid == 0 {
                    // Child process
                    let args = vec![
                        format!("--internal-{}-run", daemon_type),
                        "--internal-port".to_string(),
                        current_port.to_string(),
                    ];
                    let mut cmd = Command::new(std::env::current_exe()?);
                    cmd.args(&args).stdout(Stdio::null()).stderr(Stdio::null());
                    cmd.spawn()?.wait().await?;
                    std::process::exit(0);
                }
                // Parent process: write PID file manually
                fs::write(&pid_file_path, child_pid.to_string())?;
                // Register in daemon registry
                let metadata = DaemonMetadata {
                    service_type: daemon_type.to_string(),
                    port: current_port,
                    pid: child_pid,
                    ip_address: host_to_use.clone(),
                    data_dir: if daemon_type == "storage" { Some(std::path::PathBuf::from("/opt/graphdb/storage_data")) } else { None },
                    config_path: None,
                    engine_type: if daemon_type == "storage" { Some("sled".to_string()) } else { None },
                    last_seen_nanos: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                };
                if let Err(e) = GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await {
                    error!("Failed to register daemon {} on port {}: {}", daemon_type, current_port, e);
                }
                // Verify the daemon is running
                for attempt in 0..max_port_check_attempts {
                    sleep(Duration::from_millis(port_check_interval_ms)).await;
                    if TcpStream::connect(&socket_addr).is_ok() {
                        info!("{} daemon successfully started on port {} with PID {}", daemon_type, current_port, child_pid);
                        any_started = true;
                        break;
                    }
                    if attempt == max_port_check_attempts - 1 {
                        error!("{} daemon (PID {}) failed to bind to port {} after {} attempts", daemon_type, child_pid, current_port, max_port_check_attempts);
                        return Err(DaemonError::GeneralError(format!(
                            "{} daemon (PID {}) failed to bind to port {} after {} attempts",
                            daemon_type, child_pid, current_port, max_port_check_attempts
                        )));
                    }
                }
            }
            Err(e) => {
                error!("Failed to start {} daemon on port {}: {}", daemon_type, current_port, e);
                continue;
            }
        }
    }

    if !any_started {
        return Err(DaemonError::NoDaemonsStarted);
    }
    Ok(())
}

pub async fn stop_port_daemon(port: u16, daemon_type: &str) -> Result<(), DaemonError> {
    info!("Attempting to stop {} daemon on port {}...", daemon_type, port);

    let pid_file_path = format!("/tmp/graphdb-{}-{}.pid", daemon_type, port);
    let legacy_pid_file_path = format!("/tmp/graphdb-daemon-{}.pid", port);

    // Remove both possible PID files
    remove_pid_file(&pid_file_path);
    remove_pid_file(&legacy_pid_file_path);

    // Check daemon registry first
    let metadata = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?;
    if let Some(metadata) = metadata {
        if metadata.service_type == daemon_type {
            info!("Found daemon in registry: {} on port {} with PID {}", daemon_type, port, metadata.pid);
            if is_process_running(metadata.pid).await {
                if let Err(e) = kill(NixPid::from_raw(metadata.pid as i32), Signal::SIGTERM) {
                    error!("Failed to send SIGTERM to PID {}: {}", metadata.pid, e);
                } else {
                    sleep(Duration::from_millis(500)).await;
                    let addr = format!("127.0.0.1:{}", port);
                    if let Some(socket_addr) = addr.to_socket_addrs()?.next() {
                        if std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_millis(500)).is_err() {
                            info!("Port {} is now free.", port);
                            if let Err(e) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await {
                                error!("Failed to unregister daemon on port {}: {}", port, e);
                            }
                            return Ok(());
                        } else {
                            error!("Process with PID {} stopped, but port {} is still in use.", metadata.pid, port);
                        }
                    }
                }
            } else {
                info!("Process with PID {} is not running. Unregistering from daemon registry.", metadata.pid);
                if let Err(e) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await {
                    error!("Failed to unregister daemon on port {}: {}", port, e);
                }
                return Ok(());
            }
        }
    }

    // Fallback to lsof
    let pid = find_pid_by_port(port).await;
    if let Some(pid) = pid {
        info!("Found PID {} for port {} via lsof. Attempting to kill...", pid, port);
        if let Err(e) = kill(NixPid::from_raw(pid as i32), Signal::SIGTERM) {
            error!("Failed to send SIGTERM to PID {}: {}. It might already be gone.", pid, e);
        } else {
            sleep(Duration::from_millis(500)).await;
            let addr = format!("127.0.0.1:{}", port);
            if let Some(socket_addr) = addr.to_socket_addrs()?.next() {
                if std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_millis(500)).is_err() {
                    info!("Port {} is now free.", port);
                    if let Err(e) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await {
                        error!("Failed to unregister daemon on port {}: {}", port, e);
                    }
                    return Ok(());
                } else {
                    return Err(DaemonError::ProcessError(format!(
                        "Process with PID {} stopped, but port {} is still in use.", pid, port
                    )));
                }
            }
        }
    }

    // Fallback to pgrep
    let pgrep_arg = format!("graphdb-{} --internal-port {}", daemon_type, port);
    let pgrep_result = std::process::Command::new("pgrep")
        .arg("-f")
        .arg(&pgrep_arg)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    match pgrep_result {
        Ok(output) => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if pids.is_empty() {
                info!("No {} daemon found running with command line matching '{}'.", daemon_type, pgrep_arg);
                if let Err(e) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await {
                    error!("Failed to unregister daemon on port {}: {}", port, e);
                }
                return Ok(());
            }

            for pid in pids {
                info!("Found PID {} matching '{}'. Attempting to kill...", pid, pgrep_arg);
                if let Err(e) = kill(NixPid::from_raw(pid as i32), Signal::SIGTERM) {
                    error!("Failed to send SIGTERM to PID {}: {}. It might already be gone.", pid, e);
                } else {
                    sleep(Duration::from_millis(500)).await;
                    let addr = format!("127.0.0.1:{}", port);
                    if let Some(socket_addr) = addr.to_socket_addrs()?.next() {
                        if std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_millis(500)).is_err() {
                            info!("Port {} is now free.", port);
                            if let Err(e) = GLOBAL_DAEMON_REGISTRY.unregister_daemon(port).await {
                                error!("Failed to unregister daemon on port {}: {}", port, e);
                            }
                            return Ok(());
                        } else {
                            return Err(DaemonError::ProcessError(format!(
                                "Process with PID {} stopped, but port {} is still in use.", pid, port
                            )));
                        }
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to execute `pgrep` command for port {}: {}.", port, e);
        }
    }

    Ok(())
}

pub async fn stop_daemon() -> Result<(), DaemonError> {
    info!("Attempting to stop all 'graphdb' related daemon instances...");

    let config_path_toml = "server/src/cli/config.toml";
    let main_config_yaml = "server/main_app_config.yaml";
    let rest_config_yaml = "rest_api/rest_api_config.yaml";
    let storage_config_yaml = "storage_daemon_server/storage_config.yaml";

    let mut storage_port = CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
    let mut rest_port = DEFAULT_REST_API_PORT;
    let mut default_port = DEFAULT_DAEMON_PORT;
    let mut known_ports = vec![DEFAULT_DAEMON_PORT, DEFAULT_REST_API_PORT, CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS];
    let mut config_cluster_range = None;

    let mut config_builder = Config::builder();
    if Path::new(config_path_toml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(config_path_toml));
        info!("Loaded TOML config from {}", config_path_toml);
    }
    if Path::new(main_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(main_config_yaml));
        info!("Loaded main daemon YAML config from {}", main_config_yaml);
    }
    if Path::new(rest_config_yaml).exists() {
        config_builder = config_builder.add_source(ConfigFile::with_name(rest_config_yaml));
        info!("Loaded REST API YAML config from {}", rest_config_yaml);
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
        if let Ok(r_port) = config.get_int("rest_api.default_port") {
            rest_port = r_port as u16;
            info!("Using REST API port: {}", rest_port);
        }
        if let Ok(cfg_port) = config.get_int("main_daemon.default_port") {
            default_port = cfg_port as u16;
            info!("Using main daemon port: {}", default_port);
        }
        if let Ok(range) = config.get_string("main_daemon.cluster_range") {
            config_cluster_range = Some(range);
            info!("Using main daemon cluster range: {}", config_cluster_range.as_ref().unwrap());
        }
    }

    if let Some(range_str) = config_cluster_range {
        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() == 2 {
            if let (Ok(start_port), Ok(end_port)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                if start_port != 0 && end_port != 0 && start_port <= end_port {
                    for p in start_port..=end_port {
                        if !known_ports.contains(&p) {
                            known_ports.push(p);
                        }
                    }
                }
            }
        }
    }
    if !known_ports.contains(&storage_port) {
        known_ports.push(storage_port);
    }
    if !known_ports.contains(&rest_port) {
        known_ports.push(rest_port);
    }
    if !known_ports.contains(&default_port) {
        known_ports.push(default_port);
    }
    info!("Known ports to stop: {:?}", known_ports);

    // Try stopping via daemon registry first
    let daemons = GLOBAL_DAEMON_REGISTRY.get_all_daemon_metadata().await?;
    for metadata in daemons {
        if let Err(e) = stop_port_daemon(metadata.port, &metadata.service_type).await {
            error!("Failed to stop {} daemon on port {}: {}", metadata.service_type, metadata.port, e);
        }
    }

    // Fallback to stopping known ports
    let daemon_types = vec!["main", "rest", "storage"];
    for &port in &known_ports {
        for daemon_type in &daemon_types {
            if let Err(e) = stop_port_daemon(port, daemon_type).await {
                error!("Failed to stop {} daemon on port {}: {}", daemon_type, port, e);
            }
        }
    }

    // Additional cleanup for legacy PID files
    for &port in &known_ports {
        let legacy_pid_file = format!("/tmp/graphdb-daemon-{}.pid", port);
        remove_pid_file(&legacy_pid_file);
    }

    // Clear registry
    if let Err(e) = GLOBAL_DAEMON_REGISTRY.clear_all_daemons().await {
        error!("Failed to clear daemon registry: {}", e);
    }

    *SHUTDOWN_FLAG.lock().unwrap() = true;
    info!("All daemon instances stopped and registry cleared.");
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

    let pgrep_result = std::process::Command::new("pgrep")
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
    stop_daemon().await.map_err(|e| anyhow::anyhow!("Daemon stop failed: {}", e))
}
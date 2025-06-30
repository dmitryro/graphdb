// daemon_api/src/lib.rs
use serde::{Serialize, Deserialize};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::fs::{self, File};
use std::path::Path;
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use config::{Config, File as ConfigFile};
use graphdb_daemon::{DaemonizeBuilder, DaemonizeError};
use lazy_static::lazy_static;
use regex::Regex;
use std::net::ToSocketAddrs;
use tokio::net::TcpStream; // Use tokio's TcpStream for async connect
use tokio::time::{sleep, Duration}; // Use tokio's sleep for async delays

#[derive(Serialize, Deserialize, Debug)]
pub struct DaemonData {
    pub port: u16,
    pub pid: u32,
}

lazy_static! {
    static ref SHUTDOWN_FLAG: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
}

fn remove_pid_file(pid_file_path: &str) {
    if Path::new(pid_file_path).exists() {
        let _ = fs::remove_file(pid_file_path);
    }
}

fn cleanup_existing_daemons(base_process_name: &str) {
    let _ = Command::new("pkill")
        .arg("-f")
        .arg(base_process_name)
        .status();
    std::thread::sleep(std::time::Duration::from_millis(1000)); // This is blocking, but for cleanup it might be acceptable.
}

#[derive(Debug)]
pub enum DaemonError {
    Daemonize(DaemonizeError),
    InvalidPortRange(String),
    InvalidClusterFormat(String),
    NoDaemonsStarted,
    Io(std::io::Error),
    Config(config::ConfigError),
}

impl From<DaemonizeError> for DaemonError {
    fn from(err: DaemonizeError) -> Self {
        DaemonError::Daemonize(err)
    }
}
impl From<std::io::Error> for DaemonError {
    fn from(err: std::io::Error) -> Self {
        DaemonError::Io(err)
    }
}
impl From<config::ConfigError> for DaemonError {
    fn from(err: config::ConfigError) -> Self {
        DaemonError::Config(err)
    }
}

/// Starts one or more GraphDB daemon processes.
/// Returns Result<(), DaemonError> for compatibility with code that matches on Result.
/// Accepts skip_ports: Vec<u16> to avoid binding reserved ports like the REST API port.
pub async fn start_daemon(port: Option<u16>, cluster_range: Option<String>, skip_ports: Vec<u16>) -> Result<(), DaemonError> {
    let config_path = "server/src/cli/config.toml";

    let mut host_to_use = "127.0.0.1".to_string();
    let mut default_port = 8080;
    let mut base_process_name = "graphdb-cli".to_string();

    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()?;

        if let Ok(host) = config.get_string("server.host") {
            host_to_use = host;
        }
        if let Ok(cfg_port) = config.get_int("server.port") {
            default_port = cfg_port as u16;
        }
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            base_process_name = process_name;
        }
    }

    cleanup_existing_daemons(&base_process_name);

    let mut ports_to_start: Vec<u16> = Vec::new();

    if let Some(range_str) = cluster_range {
        let parts: Vec<&str> = range_str.split('-').collect();
        if parts.len() != 2 {
            return Err(DaemonError::InvalidClusterFormat(range_str));
        }
        let start_port = parts[0].parse::<u16>().unwrap_or(0);
        let end_port = parts[1].parse::<u16>().unwrap_or(0);

        if start_port == 0 || end_port == 0 || start_port > end_port {
            return Err(DaemonError::InvalidPortRange(range_str));
        }

        let num_ports = end_port - start_port + 1;
        if num_ports == 0 {
            return Err(DaemonError::InvalidPortRange(range_str));
        }
        if num_ports > 10 {
            return Err(DaemonError::InvalidPortRange(format!(
                "Cluster port range size ({}) exceeds maximum allowed (10).",
                num_ports
            )));
        }

        for p in start_port..=end_port {
            ports_to_start.push(p);
        }
    } else {
        ports_to_start.push(port.unwrap_or(default_port));
    }

    let max_port_check_attempts = 10;
    let port_check_interval_ms = 200;
    let _lsof_regex = Regex::new(r"^\s*(\S+)\s+(\d+)\s+").expect("Invalid lsof regex");

    let mut any_started = false;
    for current_port in ports_to_start {
        // Skip ports in skip_ports vec (e.g., REST API port)
        if skip_ports.contains(&current_port) {
            println!("[INFO] Skipping reserved port {}: reserved for another service.", current_port);
            continue;
        }

        let specific_process_name = format!("{}-{}", base_process_name, current_port);
        let specific_stdout_file_path = format!("/tmp/daemon-{}.out", current_port);
        let specific_stderr_file_path = format!("/tmp/daemon-{}.err", current_port);

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
            Ok(_child_pid_from_fork) => {
                let socket_addr = format!("{}:{}", host_to_use, current_port)
                    .to_socket_addrs()
                    .map_err(DaemonError::Io)?
                    .next()
                    .ok_or_else(|| DaemonError::InvalidPortRange(format!("No socket for port {}", current_port)))?;

                for _ in 0..max_port_check_attempts {
                    sleep(Duration::from_millis(port_check_interval_ms)).await; // Use tokio sleep
                    if TcpStream::connect(
                        &socket_addr
                    ).await.is_ok() { // Use tokio TcpStream and await
                        any_started = true;
                        break;
                    }
                }
            }
            Err(e) => {
                return Err(DaemonError::Daemonize(e));
            }
        }
    }

    if !any_started {
        return Err(DaemonError::NoDaemonsStarted);
    }
    Ok(())
}

/// Stops all daemons, returns Result<(), DaemonError> for compatibility.
pub fn stop_daemon() -> Result<(), DaemonError> {
    let config_path = "server/src/cli/config.toml";
    let mut base_process_name = "graphdb-cli".to_string();

    if Path::new(config_path).exists() {
        let config = Config::builder()
            .add_source(ConfigFile::with_name(config_path))
            .build()?;
        if let Ok(process_name) = config.get_string("daemon.process_name") {
            base_process_name = process_name;
        }
    }

    println!("Attempting to stop all 'graphdb' related daemon instances...");

    let pgrep_result = Command::new("pgrep")
        .arg("-f")
        .arg("graphdb")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .and_then(|child| child.wait_with_output());

    let mut stopped_any = false;
    match pgrep_result {
        Ok(output) => {
            let pids_str = String::from_utf8_lossy(&output.stdout);
            let pids: Vec<u32> = pids_str.lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect();

            if !pids.is_empty() {
                for pid in pids {
                    let comm_output = Command::new("ps")
                        .arg("-o")
                        .arg("comm=")
                        .arg(format!("{}", pid))
                        .stdout(Stdio::piped())
                        .spawn()
                        .and_then(|child| child.wait_with_output());

                    if let Ok(comm_out) = comm_output {
                        let command_name = String::from_utf8_lossy(&comm_out.stdout).trim().to_lowercase();
                        if command_name.contains("graphdb") || command_name.contains(&base_process_name.to_lowercase()) {
                            if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                                eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                            } else {
                                stopped_any = true;
                            }
                        } else {
                            // not a daemon, skip
                        }
                    } else {
                        if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                            eprintln!("Failed to send SIGTERM to PID {}: {}. It might already be gone or permissions issue.", pid, e);
                        } else {
                            stopped_any = true;
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(1000));
            }
        }
        Err(e) => {
            eprintln!("Failed to execute `pgrep` command: {}. Manual intervention may be required.", e);
            eprintln!("Please ensure `pgrep` is installed and in your PATH.");
        }
    }

    // Remove known PID files (base and per-port)
    let base_pid_file_path = "/tmp/graphdb-cli.pid";
    remove_pid_file(base_pid_file_path);

    for port in 8000..=9010 {
        let port_pid_file = format!("/tmp/daemon-{}.pid", port);
        remove_pid_file(&port_pid_file);
    }

    *SHUTDOWN_FLAG.lock().unwrap() = true;
    if stopped_any {
        Ok(())
    } else {
        Ok(()) // Not an error if nothing stopped
    }
}

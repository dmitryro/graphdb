use anyhow::{anyhow, Context, Result};
use clap::ValueEnum;
use std::path::PathBuf;
use std::process;
use std::time::{Instant, SystemTime, UNIX_EPOCH, Duration};
use std::io;
use std::sync::{Arc, LazyLock};
use std::collections::{HashSet, HashMap};
use std::fmt;
use regex::Regex;
use sysinfo::{System, Pid, Process, ProcessStatus, ProcessesToUpdate};
use tokio::task::JoinHandle;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::net::TcpStream;
use tokio::process::{Command as TokioCommand, Child};
use tokio::time::{self, Duration as TokioDuration};
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader, ErrorKind};
use log::{info, error, warn, debug};
use chrono::Utc;
use nix::unistd::Pid as NixPid;
use nix::sys::signal::{self, kill, Signal};
use futures::future;
use std::process::Stdio;
use tokio::process::Command;
use std::os::unix::process::ExitStatusExt;
use std::fs;

use crate::config::config::{
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
    StorageConfig,
    DEFAULT_DAEMON_PORT,
    DEFAULT_REST_API_PORT,
    DEFAULT_STORAGE_PORT,
    MAX_CLUSTER_SIZE,
    DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
    EXECUTABLE_NAME,
    DAEMON_REGISTRY_DB_PATH,
    daemon_api_storage_engine_type_to_string,
};
use crate::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::storage_engine::config::StorageEngineType;

pub async fn stop_process(pid: u32) -> Result<(), io::Error> {
    let pid = NixPid::from_raw(pid as i32);
    signal::kill(pid, Signal::SIGTERM)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to send SIGTERM to PID {}: {}", pid, e)))?;
    Ok(())
}

pub fn parse_cluster_range(range_str: &str) -> Result<Vec<u16>, anyhow::Error> {
    if range_str.is_empty() || range_str == "null" {
        return Err(anyhow!("Cluster range is empty or null"));
    }

    // Validate input format
    if !range_str.chars().all(|c| c.is_digit(10) || c == '-') {
        return Err(anyhow!("Invalid characters in cluster range: {}", range_str));
    }

    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() == 2 {
        let start_port: u16 = parts[0]
            .trim()
            .parse()
            .context(format!("Invalid start port: {}", parts[0]))?;
        let end_port: u16 = parts[1]
            .trim()
            .parse()
            .context(format!("Invalid end port: {}", parts[1]))?;
        if start_port < 1024 || end_port < 1024 {
            return Err(anyhow!("Ports must be >= 1024: start={}, end={}", start_port, end_port));
        }
        if start_port > end_port {
            return Err(anyhow!("Start port cannot be greater than end port."));
        }
        let ports: Vec<u16> = (start_port..=end_port).collect();
        /*if ports.len() > MAX_CLUSTER_SIZE {
            return Err(anyhow!(
                "Cluster port range size ({}) exceeds maximum allowed ({})",
                ports.len(),
                MAX_CLUSTER_SIZE
            ));
        } */
        Ok(ports)
    } else if parts.len() == 1 {
        let port: u16 = range_str
            .trim()
            .parse()
            .context(format!("Invalid port: {}", range_str))?;
        if port < 1024 {
            return Err(anyhow!("Port must be >= 1024: {}", port));
        }
        Ok(vec![port])
    } else {
        Err(anyhow!(
            "Invalid cluster range format. Expected 'start-end' or a single port: {}",
            range_str
        ))
    }
}

/// Checks if a given port is within a specified cluster range.
/// If no range is provided, it returns true, indicating the port should be displayed.
pub fn is_port_in_range(port: u16, range_str_opt: Option<&String>) -> Result<bool> {
    if let Some(range_str) = range_str_opt {
        let ports = parse_cluster_range(range_str)?;
        Ok(ports.contains(&port))
    } else {
        // If no range is specified, assume we want to show all daemons.
        Ok(true)
    }
}

pub fn is_port_in_cluster_range(port: u16, range_str: &str) -> bool {
    let parts: Vec<&str> = range_str.split('-').collect();
    if parts.len() == 1 {
        // Single port range
        if let Ok(single_port) = parts[0].trim().parse::<u16>() {
            return port == single_port;
        }
    } else if parts.len() == 2 {
        // Ranged ports
        if let (Ok(start), Ok(end)) = (parts[0].trim().parse::<u16>(), parts[1].trim().parse::<u16>()) {
            return port >= start && port <= end;
        }
    }
    false
}

pub fn is_port_within_range(port: u16, cluster_ports: &[u16]) -> bool {
    cluster_ports.contains(&port)
}

pub async fn is_port_free(port: u16) -> bool {
    !is_port_listening(port).await
}

pub async fn is_port_listening(port: u16) -> bool {
    tokio::time::timeout(Duration::from_secs(1), async {
        TcpStream::connect(format!("127.0.0.1:{}", port)).await.is_ok()
    }).await.unwrap_or(false)
}

pub async fn is_daemon_running(port: u16) -> bool {
    !is_port_free(port).await
}

pub async fn is_process_running(pid: u32) -> bool {
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);
    sys.process(Pid::from_u32(pid)).is_some()
}

pub async fn find_pid_by_port(port: u16) -> Result<Option<u32>, io::Error> {
    let output = Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
        .output()
        .await?;

    if !output.status.success() {
        return Ok(None);
    }

    let pid_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if pid_str.is_empty() {
        return Ok(None);
    }

    match pid_str.parse::<u32>() {
        Ok(pid) => Ok(Some(pid)),
        Err(e) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Failed to parse PID from lsof output: {}", e),
        )),
    }
}

// Helper function to check if PID is still valid
pub async fn check_pid_validity(pid: u32) -> bool {
    use nix::sys::signal::{kill, Signal};
    use nix::unistd::Pid;
    match kill(Pid::from_raw(pid as i32), None) {
        Ok(_) => true,
        Err(nix::Error::ESRCH) => {
            debug!("PID {} is no longer valid (process does not exist)", pid);
            false
        }
        Err(e) => {
            warn!("Failed to check PID {} validity: {}", pid, e);
            false
        }
    }
}

pub async fn is_pid_running(pid: u32) -> bool {
    let mut sys = sysinfo::System::new();
    let sysinfo_pid = sysinfo::Pid::from_u32(pid);
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sysinfo_pid]), true);
    sys.process(sysinfo_pid).is_some()
}

pub async fn find_port_by_pid(pid: u32) -> Option<u16> {
    let output = TokioCommand::new("lsof")
        .arg("-P")
        .arg("-iTCP")
        .arg("-sTCP:LISTEN")
        .arg("-p")
        .arg(pid.to_string())
        .output().await.ok()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let re = Regex::new(r"TCP.*:(\d+)\s+\(LISTEN\)").unwrap();
        if let Some(captures) = re.captures(&stdout) {
            if let Some(port_match) = captures.get(1) {
                return port_match.as_str().parse::<u16>().ok();
            }
        }
    }
    None
}


// Helper function to check if a daemon is running on a given port
pub async fn is_storage_daemon_running(port: u16) -> bool {
    let addr = format!("127.0.0.1:{}", port);
    match TcpStream::connect(&addr).await {
        Ok(_) => {
            debug!("Port {} is in use, assuming daemon is running", port);
            true
        }
        Err(_) => {
            debug!("Port {} is not in use", port);
            false
        }
    }
}

// Helper function to check if a string is a valid cluster range (e.g., "8081-8084")
pub fn is_valid_cluster_range(range: &str) -> bool {
    range.contains('-') && range.split('-').all(|s| s.parse::<u16>().is_ok())
}

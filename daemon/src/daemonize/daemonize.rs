// daemon/src/daemonize/daemonize.rs
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::os::unix::io::{AsRawFd, IntoRawFd};
use std::path::{Path, PathBuf};
use std::process::{exit};
use nix::sys::signal::{self, SigHandler, Signal};
use nix::sys::stat::{umask, Mode};
use nix::unistd::{chdir, fork, getpid, setsid, Pid, ForkResult};
use proctitle::set_title;
use std::fmt::{self, Display, Formatter};

#[derive(Debug)]
pub enum DaemonizeError {
    Io(io::Error),
    Nix(nix::errno::Errno),
    Generic(String),
    SetTitleFailed(String),
}

impl From<io::Error> for DaemonizeError {
    fn from(err: io::Error) -> Self {
        DaemonizeError::Io(err)
    }
}

impl From<nix::errno::Errno> for DaemonizeError {
    fn from(err: nix::errno::Errno) -> Self {
        DaemonizeError::Nix(err)
    }
}

impl Display for DaemonizeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DaemonizeError::Io(e) => write!(f, "I/O error: {}", e),
            DaemonizeError::Nix(e) => write!(f, "Nix error: {}", e),
            DaemonizeError::Generic(s) => write!(f, "{}", s),
            DaemonizeError::SetTitleFailed(e) => write!(f, "Failed to set process title: {}", e),
        }
    }
}

#[derive(Default)]
pub struct DaemonizeBuilder {
    working_directory: Option<String>,
    umask: u32,
    process_name: Option<String>,
    stdout: Option<File>,
    stderr: Option<File>,
    pid: Option<u32>,
    host: String,
    port: u16,
    skip_ports: Vec<u16>,
    pid_file_path: Option<String>,
}

impl DaemonizeBuilder {
    pub fn new() -> Self {
        Self {
            working_directory: None,
            umask: 0o027,
            process_name: None,
            stdout: None,
            stderr: None,
            pid: None,
            host: "127.0.0.1".into(),
            port: 8080,
            skip_ports: vec![],
            pid_file_path: None,
        }
    }

    pub fn working_directory(self, dir: &str) -> Self {
        Self { working_directory: Some(dir.to_string()), ..self }
    }

    pub fn umask(self, umask: u32) -> Self {
        Self { umask, ..self }
    }

    pub fn process_name(self, name: &str) -> Self {
        Self { process_name: Some(name.to_string()), ..self }
    }

    pub fn stdout(self, stdout: File) -> Self {
        Self { stdout: Some(stdout), ..self }
    }

    pub fn stderr(self, stderr: File) -> Self {
        Self { stderr: Some(stderr), ..self }
    }

    pub fn pid(self, pid: u32) -> Self {
        Self { pid: Some(pid), ..self }
    }

    pub fn host(self, host: &str) -> Self {
        Self { host: host.to_string(), ..self }
    }

    pub fn port(self, port: u16) -> Self {
        Self { port, ..self }
    }

    pub fn skip_ports(self, skip_ports: Vec<u16>) -> Self {
        Self { skip_ports, ..self }
    }

    pub fn pid_file_path(self, path: &str) -> Self {
        Self { pid_file_path: Some(path.to_string()), ..self }
    }

    pub fn build(self) -> Result<Daemonize, DaemonizeError> {
        let pid = std::process::id();
        Ok(Daemonize {
            working_directory: self.working_directory,
            umask: self.umask,
            process_name: self.process_name,
            stdout: self.stdout,
            stderr: self.stderr,
            pid: Some(pid),
            host: self.host,
            port: self.port,
            skip_ports: self.skip_ports,
            pid_file_path: self.pid_file_path,
        })
    }

    /// Only fork/daemonize, do NOT run the stub TCP server.
    /// Returns 0 in the child, PID in the parent.
    pub fn fork_only(&self) -> Result<u32, DaemonizeError> {
        if self.skip_ports.contains(&self.port) {
            eprintln!(
                "[INFO] Not binding daemon stub on reserved port {}. This port is reserved for another service.",
                self.port
            );
            return Ok(0);
        }

        // Ensure PID directory exists
        if let Some(ref path) = self.pid_file_path {
            if let Some(parent) = Path::new(path).parent() {
                fs::create_dir_all(parent).map_err(DaemonizeError::Io)?;
            }
        }

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                setsid().map_err(DaemonizeError::Nix)?;

                match unsafe { fork() } {
                    Ok(ForkResult::Child) => {
                        // 2nd child (daemon)
                        if let Some(ref dir) = self.working_directory {
                            chdir(Path::new(dir)).map_err(DaemonizeError::Nix)?;
                        }
                        umask(Mode::from_bits_truncate(self.umask as u16)); // Fixed: u32 → u16

                        if let Some(ref process_name) = self.process_name {
                            set_title(process_name);
                        }

                        // Setup signal handlers
                        unsafe {
                            signal::signal(Signal::SIGTERM, SigHandler::Handler(handle_signal))
                                .map_err(DaemonizeError::Nix)?;
                            signal::signal(Signal::SIGHUP, SigHandler::SigIgn)
                                .map_err(DaemonizeError::Nix)?;
                        }

                        // Redirect std descriptors
                        let dev_null = File::options().read(true).write(true).open("/dev/null")?;
                        let stdout_fd = self.stdout.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();
                        let stderr_fd = self.stderr.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();

                        nix::unistd::dup2(stdout_fd, 1).ok();
                        nix::unistd::dup2(stderr_fd, 2).ok();
                        nix::unistd::dup2(dev_null.as_raw_fd(), 0).ok();

                        // Write PID file
                        if let Some(ref path) = self.pid_file_path {
                            let pid = getpid().as_raw() as u32;
                            fs::write(path, pid.to_string()).map_err(DaemonizeError::Io)?;
                        }

                        Ok(0)
                    }
                    Ok(ForkResult::Parent { .. }) => exit(0),
                    Err(e) => Err(DaemonizeError::Nix(e)),
                }
            }
            Ok(ForkResult::Parent { child }) => Ok(child.as_raw() as u32),
            Err(e) => Err(DaemonizeError::Nix(e)),
        }
    }
}

pub struct Daemonize {
    working_directory: Option<String>,
    umask: u32,
    process_name: Option<String>,
    stdout: Option<File>,
    stderr: Option<File>,
    pid: Option<u32>,
    host: String,
    port: u16,
    skip_ports: Vec<u16>,
    pid_file_path: Option<String>,
}

impl Daemonize {
    /// Start a stub daemon (normal mode, not for REST API)
    pub fn start(&self) -> Result<u32, DaemonizeError> {
        if self.skip_ports.contains(&self.port) {
            eprintln!(
                "[INFO] Not binding daemon stub on reserved port {}. This port is reserved for another service.",
                self.port
            );
            return Ok(0);
        }

        // Ensure PID directory exists
        if let Some(ref path) = self.pid_file_path {
            if let Some(parent) = Path::new(path).parent() {
                fs::create_dir_all(parent).map_err(DaemonizeError::Io)?;
            }
        }

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                setsid().map_err(DaemonizeError::Nix)?;

                match unsafe { fork() } {
                    Ok(ForkResult::Child) => {
                        // 2nd child (the daemon)
                        if let Some(ref dir) = self.working_directory {
                            chdir(Path::new(dir)).map_err(DaemonizeError::Nix)?;
                        }
                        umask(Mode::from_bits_truncate(self.umask as u16)); // Fixed: u32 → u16

                        // Redirect standard file descriptors
                        let dev_null = File::options().read(true).write(true).open("/dev/null")?;
                        let stdout_fd = self.stdout.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();
                        let stderr_fd = self.stderr.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();

                        nix::unistd::dup2(stdout_fd, 1).ok();
                        nix::unistd::dup2(stderr_fd, 2).ok();
                        nix::unistd::dup2(dev_null.as_raw_fd(), 0).ok();

                        // Set process title
                        if let Some(ref process_name) = self.process_name {
                            set_title(process_name);
                        }

                        // Set up signal handlers
                        unsafe {
                            signal::signal(Signal::SIGTERM, SigHandler::Handler(handle_signal))
                                .map_err(DaemonizeError::Nix)?;
                            signal::signal(Signal::SIGHUP, SigHandler::SigIgn)
                                .map_err(DaemonizeError::Nix)?;
                        }

                        // Write PID file
                        let current_pid = getpid().as_raw() as u32;
                        if let Some(ref path) = self.pid_file_path {
                            fs::write(path, current_pid.to_string()).map_err(DaemonizeError::Io)?;
                        }

                        // Bind TCP stub
                        let address = format!("{}:{}", self.host, self.port);
                        eprintln!(
                            "DEBUG: Daemon process '{}' binding to {} (PID {})",
                            self.process_name.as_deref().unwrap_or("unknown"),
                            address,
                            current_pid
                        );

                        match std::net::TcpListener::bind(&address) {
                            Ok(listener) => {
                                println!("Daemon (PID {}) is listening on {}", current_pid, address);
                                for stream in listener.incoming() {
                                    if let Ok(mut stream) = stream {
                                        handle_client(&mut stream);
                                    }
                                }
                                Ok(current_pid)
                            }
                            Err(e) => {
                                eprintln!("Daemonization failed: {}", e);
                                Err(DaemonizeError::Io(e))
                            }
                        }
                    }
                    Ok(ForkResult::Parent { child: _ }) => exit(0),
                    Err(e) => Err(DaemonizeError::Nix(e)),
                }
            }
            Ok(ForkResult::Parent { child }) => Ok(child.as_raw() as u32),
            Err(e) => Err(DaemonizeError::Nix(e)),
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.pid
    }
}

// Signal handler
extern "C" fn handle_signal(_sig: i32) {
    eprintln!("Daemon received SIGTERM, exiting gracefully...");
    exit(0);
}

fn handle_client(stream: &mut std::net::TcpStream) {
    let mut buffer = [0; 512];
    match stream.read(&mut buffer) {
        Ok(_) => {
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\n\r\nGraphDB Daemon says hello!");
        }
        Err(e) => eprintln!("Failed to read from connection: {}", e),
    }
}
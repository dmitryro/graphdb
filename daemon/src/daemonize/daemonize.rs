use std::fs::File;
use std::io::{self, Read, Write};
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::process::exit;
use nix::sys::signal::{self, SigHandler, Signal};
use nix::sys::stat::{umask, Mode};
use nix::unistd::{chdir, fork, getpid, setsid, ForkResult};
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
    skip_ports: Vec<u16>, // Ports that should NOT be bound by the stub daemon
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
        }
    }

    pub fn working_directory(mut self, dir: &str) -> Self {
        self.working_directory = Some(dir.to_string());
        self
    }

    pub fn umask(mut self, umask: u32) -> Self {
        self.umask = umask;
        self
    }

    pub fn process_name(mut self, name: &str) -> Self {
        self.process_name = Some(name.to_string());
        self
    }

    pub fn stdout(mut self, stdout: File) -> Self {
        self.stdout = Some(stdout);
        self
    }

    pub fn stderr(mut self, stderr: File) -> Self {
        self.stderr = Some(stderr);
        self
    }

    pub fn pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }

    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn skip_ports(mut self, skip_ports: Vec<u16>) -> Self {
        self.skip_ports = skip_ports;
        self
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
        })
    }

    /// Only fork/daemonize, do NOT run the stub TCP server (for REST API).
    /// Returns 0 in the child, PID in the parent.
    pub fn fork_only(&mut self) -> Result<u32, DaemonizeError> {
        // If this port is in the skip list, do not bind or fork!
        if self.skip_ports.contains(&self.port) {
            eprintln!(
                "[INFO] Not binding daemon stub on reserved port {}. This port is reserved for another service.",
                self.port
            );
            return Ok(0);
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
                        umask(Mode::from_bits_truncate(self.umask as u16));

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

                        // Redirect std descriptors if needed
                        let dev_null = File::options().read(true).write(true).open("/dev/null")?;
                        let _stdout_fd = self.stdout.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();
                        let _stderr_fd = self.stderr.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();

                        Ok(0)
                    }
                    Ok(ForkResult::Parent { .. }) => {
                        exit(0);
                    }
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
}

impl Daemonize {
    /// Start a stub daemon (normal mode, not for REST API)
    pub fn start(&mut self) -> Result<u32, DaemonizeError> {
        // If this port is in the skip list, don't bind/fork!
        if self.skip_ports.contains(&self.port) {
            eprintln!(
                "[INFO] Not binding daemon stub on reserved port {}. This port is reserved for another service.",
                self.port
            );
            return Ok(0);
        }

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // 1st child process
                setsid().map_err(DaemonizeError::Nix)?;

                match unsafe { fork() } {
                    Ok(ForkResult::Child) => {
                        // 2nd child (the daemon)
                        if let Some(ref dir) = self.working_directory {
                            chdir(Path::new(dir)).map_err(DaemonizeError::Nix)?;
                        }
                        umask(Mode::from_bits_truncate(self.umask as u16));

                        // Redirect standard file descriptors
                        let dev_null = File::options().read(true).write(true).open("/dev/null")?;
                        let _stdout_fd = self.stdout.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();
                        let _stderr_fd = self.stderr.as_ref().unwrap_or(&dev_null).try_clone()?.into_raw_fd();

                        // Set process title if provided
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

                        // Bind TCP stub (for non-REST API ports)
                        let address = format!("{}:{}", self.host, self.port);
                        eprintln!("DEBUG: Daemon process '{}' binding to {} (PID {})", self.process_name.as_deref().unwrap_or("unknown"), address, getpid());
                        match std::net::TcpListener::bind(&address) {
                            Ok(listener) => {
                                let current_pid = getpid().as_raw() as u32;
                                if let Some(ref process_name) = self.process_name {
                                    let pid_file_path = format!("/tmp/{}.pid", process_name);
                                    std::fs::write(&pid_file_path, current_pid.to_string())
                                        .map_err(DaemonizeError::Io)?;
                                }
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
                    },
                    Ok(ForkResult::Parent { child: _ }) => {
                        exit(0);
                    },
                    Err(e) => Err(DaemonizeError::Nix(e)),
                }
            },
            Ok(ForkResult::Parent { child }) => {
                self.pid = Some(child.as_raw() as u32);
                Ok(self.pid.unwrap())
            },
            Err(e) => Err(DaemonizeError::Nix(e)),
        }
    }

    pub fn pid(&self) -> Option<u32> {
        self.pid
    }
}

// Signal handler function
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

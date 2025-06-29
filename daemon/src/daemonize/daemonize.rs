use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::process::exit;
use nix::sys::signal::{self, SigHandler, Signal};
use nix::sys::stat::{umask, Mode};
use nix::unistd::{chdir, fork, getpid, setsid, ForkResult};
use nix::unistd::Pid;
use proctitle::set_title;
use std::fmt::{self, Display, Formatter};


// Define the DaemonizeError enum
#[derive(Debug)]
pub enum DaemonizeError {
    Io(io::Error),
    Nix(nix::errno::Errno),
    Generic(String),
    AddrInUse(String, u16),
    BindFailed(String, u16, io::Error),
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
            DaemonizeError::AddrInUse(host, port) => write!(f, "Address {}:{} already in use", host, port),
            DaemonizeError::BindFailed(host, port, e) => write!(f, "Failed to bind to {}:{}: {}", host, port, e),
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
    host: String,
    port: u16,
    pid: Option<u32>,
}

impl DaemonizeBuilder {
    pub fn new() -> Self {
        Self {
            working_directory: None,
            umask: 0o027,
            process_name: None,
            stdout: None,
            stderr: None,
            host: "127.0.0.1".to_string(),
            port: 8080,
            pid: None,
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

    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }

    pub fn build(mut self) -> Result<Daemonize, DaemonizeError> {
        let pid = std::process::id(); // Get the current process ID for the builder's pid field
        self.pid = Some(pid);

        Ok(Daemonize {
            working_directory: self.working_directory,
            umask: self.umask,
            process_name: self.process_name,
            stdout: self.stdout,
            stderr: self.stderr,
            host: self.host,
            port: self.port,
            pid: self.pid,
        })
    }
}

pub struct Daemonize {
    working_directory: Option<String>,
    umask: u32,
    process_name: Option<String>,
    stdout: Option<File>,
    stderr: Option<File>,
    host: String,
    port: u16,
    pid: Option<u32>,
}

impl Daemonize {
    pub fn start(&mut self) -> Result<u32, DaemonizeError> {
        // First fork
        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // Child 1 process
                setsid().map_err(DaemonizeError::Nix)?; // Create new session and become session leader

                // Second fork to prevent the daemon from acquiring a controlling terminal
                match unsafe { fork() } {
                    Ok(ForkResult::Child) => {
                        // Grandchild process - this is the actual daemon
                        if let Some(ref dir) = self.working_directory {
                            chdir(Path::new(dir)).map_err(DaemonizeError::Nix)?;
                        }

                        umask(Mode::from_bits_truncate(self.umask as u16));

                        // Redirect standard file descriptors
                        let dev_null = File::options().read(true).write(true).open("/dev/null")?;

                        // Use .take() to consume the Option<File> and pass ownership to the raw fd conversion
                        // If stdout/stderr were not set in the builder, default to /dev/null clone
                        let _stdout_fd = self.stdout.take().unwrap_or(dev_null.try_clone()?).into_raw_fd();
                        let _stderr_fd = self.stderr.take().unwrap_or(dev_null.try_clone()?).into_raw_fd();

                        // Set process title if provided
                        if let Some(ref process_name) = self.process_name {
                            set_title(process_name);
                        }

                        // Set up signal handlers
                        unsafe {
                            // Terminate gracefully on SIGTERM
                            signal::signal(Signal::SIGTERM, SigHandler::Handler(handle_signal))
                                .map_err(DaemonizeError::Nix)?;
                            // Ignore SIGHUP (hang-up) signal
                            signal::signal(Signal::SIGHUP, SigHandler::SigIgn)
                                .map_err(DaemonizeError::Nix)?;
                        }

                        // Attempt to bind to the specified address and port
                        let address = format!("{}:{}", self.host, self.port);
                        match TcpListener::bind(&address) {
                            Ok(listener) => {
                                let current_pid = getpid().as_raw() as u32;

                                // Write PID to file for later management (e.g., stopping)
                                if let Some(ref process_name) = self.process_name {
                                    let pid_file_path = format!("/tmp/{}.pid", process_name);
                                    std::fs::write(&pid_file_path, current_pid.to_string())
                                        .map_err(DaemonizeError::Io)?;
                                }

                                // Daemon specific logic starts here (e.g., event loop, server handling)
                                // In this example, it's a simple TCP listener
                                println!("Daemon (PID {}) is listening on {}", current_pid, address);
                                for stream in listener.incoming() {
                                    if let Ok(mut stream) = stream {
                                        handle_client(&mut stream); // Your application's client handling
                                    }
                                }
                                // This return will only be reached if the listener loop somehow ends,
                                // which typically means the daemon is exiting.
                                Ok(current_pid)
                            }
                            Err(e) => {
                                // Important: If binding fails, print to stderr (redirected to daemon.err)
                                // and return an error. The CLI will then handle this.
                                eprintln!("Daemonization failed: {}", e);
                                if e.kind() == io::ErrorKind::AddrInUse {
                                    Err(DaemonizeError::AddrInUse(self.host.clone(), self.port))
                                } else {
                                    Err(DaemonizeError::BindFailed(self.host.clone(), self.port, e))
                                }
                            }
                        }
                    },
                    Ok(ForkResult::Parent { child }) => {
                        // Child 1 exits, allowing the grandchild to be adopted by init
                        exit(0);
                    },
                    Err(e) => Err(DaemonizeError::Nix(e)),
                }
            },
            Ok(ForkResult::Parent { child }) => {
                // Original parent process exits, returning the PID of the first child
                self.pid = Some(child.as_raw() as u32);
                // The CLI uses this PID to track the daemon, but the actual binding
                // is done by the grandchild. The CLI will *poll* the port to confirm.
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
    // Perform any necessary cleanup here before exiting
    exit(0);
}

// Helper to set process title (note: proctitle::set_title is used directly above)
#[allow(dead_code)] // Keep if not used elsewhere, to avoid warnings.
fn set_process_title(title: &str) {
    set_title(title);
}

// Placeholder for your daemon's actual client handling logic
fn handle_client(stream: &mut TcpStream) {
    let mut buffer = [0; 512];
    match stream.read(&mut buffer) {
        Ok(_) => {
            // In a real daemon, you'd parse requests, interact with graphdb-lib, etc.
            // For now, it just prints and sends a simple response.
            // println!("Received request: {}", String::from_utf8_lossy(&buffer[..]));
            stream.write_all(b"HTTP/1.1 200 OK\r\n\r\nGraphDB Daemon says hello!").unwrap();
        }
        Err(e) => eprintln!("Failed to read from connection: {}", e),
    }
}

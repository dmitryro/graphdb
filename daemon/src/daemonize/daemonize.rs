use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::process::{exit, Command};
use std::thread;
use nix::sys::signal::{self, signal, SigHandler, Signal};
use nix::sys::stat::umask;
use nix::unistd::{chdir, fork, getpid, setsid, ForkResult};
use nix::unistd::Pid;
use proctitle::set_title;


#[derive(Default)]
pub struct DaemonizeBuilder {
    working_directory: Option<String>,
    umask: u32,
    process_name: Option<String>,
    stdout: Option<File>,
    stderr: Option<File>,
    host: String,
    port: u16,
    pid: Option<u32>, // Store the PID here
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

    pub fn build(mut self) -> Result<Daemonize, String> {
        // Capture the current process ID
        let pid = std::process::id();
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

    // New stop method for DaemonizeBuilder
    pub fn stop(self) -> Result<(), String> {
        if let Some(pid) = self.pid {
            println!("Attempting to stop daemon with PID: {}", pid);
            match signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                Ok(_) => {
                    println!("Daemon with PID {} has been terminated.", pid);
                    Ok(())
                }
                Err(e) => Err(format!("Failed to terminate daemon: {}", e)),
            }
        } else {
            Err("No PID specified for the daemon.".to_string())
        }
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
    pid: Option<u32>, // Store the PID here
}

impl Daemonize {
	pub fn start(&mut self) -> Result<u32, String> {
	    println!("Attempting to start daemon...");

	    match unsafe { fork() } {
	        Ok(ForkResult::Child) => {
	            if let Some(ref process_name) = self.process_name {
	                set_title(process_name);
	            }

	            let address = format!("{}:{}", self.host, self.port);
	            match TcpListener::bind(&address) {
	                Ok(listener) => {
	                    println!("Daemon is listening on {}", address);
	                    for stream in listener.incoming() {
	                        if let Ok(mut stream) = stream {
	                            handle_client(&mut stream);
	                        }
	                    }
	                }
	                Err(e) => return Err(format!("Failed to bind to {}: {}", address, e)),
	            }

	            Ok(getpid().as_raw() as u32) // Return child PID
	        }
	        Ok(ForkResult::Parent { child }) => {
	            self.pid = Some(child.as_raw() as u32);
	            println!("Daemon started with PID: {}", self.pid.unwrap());
	            Ok(self.pid.unwrap()) // Return PID instead of exiting
	        }
	        Err(e) => Err(format!("Fork failed: {}", e)),
	    }
	}

    pub fn stop(&self) -> Result<(), String> {
        if let Some(pid) = self.pid {
            println!("Attempting to stop daemon with PID: {}", pid);
            match signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM) {
                Ok(_) => {
                    println!("Daemon with PID {} has been terminated.", pid);
                    Ok(())
                }
                Err(e) => Err(format!("Failed to terminate daemon: {}", e)),
            }
        } else {
            Err("No PID specified for the daemon.".to_string())
        }
    }

    // Method to retrieve the PID
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }
}

extern "C" fn handle_signal(_sig: i32) {
    println!("Received SIGTERM, exiting...");
    exit(0);
}

fn set_process_title(title: &str) {
    // This function sets the process title.
    // Implementation may vary depending on the platform.
    // For example, on Unix systems, you might use the `prctl` function.
    // On Windows, you might use the `SetConsoleTitle` function.
    // Here, we'll just print the title for demonstration purposes.
    set_title(title);
    println!("Process title set to: {}", title);
}

fn handle_client(stream: &mut TcpStream) {
    // Handle the client connection.
    let mut buffer = [0; 512];
    match stream.read(&mut buffer) {
        Ok(_) => {
            println!("Received request: {}", String::from_utf8_lossy(&buffer[..]));
            stream.write_all(b"HTTP/1.1 200 OK\r\n\r\nHello, world!").unwrap();
        }
        Err(e) => eprintln!("Failed to read from connection: {}", e),
    }
}


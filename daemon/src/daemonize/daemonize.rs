use std::ffi::CString;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::IntoRawFd;
use std::path::Path;
use std::process::{exit, Command};
use std::thread;
use nix::sys::signal::{signal, SigHandler, Signal};
use nix::sys::stat::umask;
use nix::unistd::{chdir, fork, setsid, ForkResult};
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

    pub fn build(self) -> Result<Daemonize, String> {
        Ok(Daemonize {
            working_directory: self.working_directory,
            umask: self.umask,
            process_name: self.process_name,
            stdout: self.stdout,
            stderr: self.stderr,
            host: self.host,
            port: self.port,
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
}

impl Daemonize {
    pub fn start(&self) -> Result<(), String> {
        println!("Attempting to start daemon...");

        match unsafe { fork() } {
            Ok(ForkResult::Child) => {
                // ... [daemon initialization code] ...

                if let Some(ref process_name) = self.process_name {
                    set_title(process_name);
                }

                // Start listening on the specified host and port
                let address = format!("{}:{}", self.host, self.port);
                match TcpListener::bind(&address) {
                    Ok(listener) => {
                        println!("Daemon is listening on {}", address);
                        for stream in listener.incoming() {
                            match stream {
                                Ok(mut stream) => {
                                    // Spawn a new thread to handle the connection
                                        handle_client(&mut stream);
                                }
                                Err(e) => {
                                    eprintln!("Connection failed: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        return Err(format!("Failed to bind to {}: {}", address, e));
                    }
                }

                Ok(())
            }
            Ok(ForkResult::Parent { .. }) => {
                // Parent process exits
                exit(0);
            }
            Err(e) => Err(format!("Fork failed: {}", e)),
        }
    }

    pub fn stop(&self) -> Result<(), String> {
        if let Some(ref process_name) = self.process_name {
            let output = Command::new("pkill")
                .arg("-f")
                .arg(process_name)
                .output()
                .map_err(|e| format!("Failed to execute pkill: {}", e))?;

            if !output.status.success() {
                return Err(format!(
                    "Failed to send SIGTERM to daemon: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }

            println!("The daemon service was successfully stopped.");
        } else {
            println!("No process name specified.");
        }
        Ok(())
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

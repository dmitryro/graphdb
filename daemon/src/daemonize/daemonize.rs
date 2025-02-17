use std::path::Path;
use nix::unistd::{fork, ForkResult, setsid, chdir, setuid, setgid, Uid, Gid};
use nix::sys::stat::{umask, Mode};
use nix::sys::signal::{signal, SigHandler, Signal};
use std::process::Command;
use proctitle::set_title;
use std::fs::File;
use std::io::{self, Write};
use std::os::fd::IntoRawFd;
use users::{get_user_by_name, get_group_by_name, Users};
use std::ffi::CString;
use std::process::exit;

#[derive(Default)]
pub struct DaemonizeBuilder {
    working_directory: Option<String>,
    umask: u32,
    process_name: Option<String>,
    user: Option<String>,
    group: Option<String>,
    stdout: Option<File>,
    stderr: Option<File>,
}

impl DaemonizeBuilder {
    pub fn new() -> Self {
        Self {
            working_directory: None,
            umask: 0o777,
            process_name: None,
            user: None,
            group: None,
            stdout: None,
            stderr: None,
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

    pub fn user(mut self, user: &str) -> Self {
        self.user = Some(user.to_string());
        self
    }

    pub fn group(mut self, group: &str) -> Self {
        self.group = Some(group.to_string());
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

    pub fn build(self) -> Result<Daemonize, String> {
        Ok(Daemonize {
            working_directory: self.working_directory,
            umask: self.umask,
            process_name: self.process_name,
            user: self.user,
            group: self.group,
            stdout: self.stdout,
            stderr: self.stderr,
        })
    }
}

pub struct Daemonize {
    working_directory: Option<String>,
    umask: u32,
    process_name: Option<String>,
    user: Option<String>,
    group: Option<String>,
    stdout: Option<File>,
    stderr: Option<File>,
}

impl Daemonize {
    pub fn start(&self) -> Result<(), ()> {
        println!("Attempting to start daemon ...");

        match unsafe { fork() } {
            Ok(ForkResult::Child) => unsafe {
                if setsid().is_err() {
                    eprintln!("setsid() failed");
                    std::process::exit(1);
                }

                if let Some(dir) = &self.working_directory {
                    if chdir(Path::new(dir)).is_err() {
                        eprintln!("chdir() failed");
                        std::process::exit(1);
                    }
                }

                let umask_mode = Mode::from_bits_truncate(self.umask as u16);
                umask(umask_mode);

                if signal(Signal::SIGTERM, SigHandler::Handler(handle_signal)).is_err() {
                    eprintln!("Failed to register signal handler");
                    std::process::exit(1);
                }

                if let Some(ref process_name) = self.process_name {
                    set_title(process_name);
                }

                // Handle user and group changes (if applicable)
                if let Some(user) = &self.user {
                    match get_user_by_name(user) {
                        Some(usr) => {
                            if let Err(e) = setuid(Uid::from_raw(usr.uid() as u32)) {
                                eprintln!("Failed to set user ID: {}", e);
                                // Continue without setting the user ID
                            }
                        }
                        None => {
                            eprintln!("Failed to find user: {}", user);
                            // Continue without setting the user ID
                        }
                    }
                }

                if let Some(group) = &self.group {
                    match get_group_by_name(group) {
                        Some(grp) => {
                            if let Err(e) = setgid(Gid::from_raw(grp.gid() as u32)) {
                                eprintln!("Failed to set group ID: {}", e);
                                // Continue without setting the group ID
                            }
                        }
                        None => {
                            eprintln!("Failed to find group: {}", group);
                            // Continue without setting the group ID
                        }
                    }
                }

                // Redirect stdout and stderr if specified
                if let Some(ref stdout) = self.stdout {
                    if unsafe { libc::dup2(stdout.try_clone().unwrap().into_raw_fd(), libc::STDOUT_FILENO) } == -1 {
                        eprintln!("Failed to redirect stdout: {}", io::Error::last_os_error());
                        std::process::exit(1);
                    }
                }

                if let Some(ref stderr) = self.stderr {
                    if unsafe { libc::dup2(stderr.try_clone().unwrap().into_raw_fd(), libc::STDERR_FILENO) } == -1 {
                        eprintln!("Failed to redirect stderr: {}", io::Error::last_os_error());
                        std::process::exit(1);
                    }
                }

                Ok(())
            }
            Ok(ForkResult::Parent { .. }) => {
                // Parent process
                // Exit or perform other tasks as needed
                Ok(())
            }
            Err(e) => {
                eprintln!("Fork failed: {}", e);
                Err(())
            }
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
    // Handle the signal (e.g., gracefully stop the daemon)
    println!("Received SIGTERM, exiting...");
    std::process::exit(0);
}

// server/src/cli/interactive.rs

// This file handles the interactive CLI mode, including command parsing
// and displaying interactive help messages.

use anyhow::{Result, Context};
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, history::DefaultHistory};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use std::process; // For process::exit
use std::path::PathBuf; // Added PathBuf import
use clap::CommandFactory; // Added CommandFactory import
use std::collections::HashSet; // Added for HashSet
use strsim::jaro_winkler; // Added for jaro_winkler
use shlex; // Import shlex for robust argument splitting

// Import necessary items from sibling modules
use crate::cli::cli::CliArgs; // Corrected: Import CliArgs from cli.rs
use crate::cli::commands::{CommandType, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, ReloadAction, StartAction, RestartArgs, RestartAction};
use crate::cli::help_display::HelpArgs; // Corrected: Import HelpArgs from help_display
use crate::cli::config::CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS; // Re-added: Used in parse_command
use crate::cli::handlers;
// The following stop_daemon_api_call is now primarily for global cleanup on exit,
// interactive command handling uses handlers.rs functions.
use crate::cli::daemon_management::stop_daemon_api_call;
use crate::cli::help_display::{print_interactive_help, print_interactive_filtered_help, collect_all_cli_elements_for_suggestions, print_help_clap_generated, print_filtered_help_clap_generated}; // Imported functions

/// Parses a command string from the interactive CLI input.
/// This function now expects a `Vec<String>` (from shlex) as input.
pub fn parse_command(parts: &[String]) -> (CommandType, Vec<String>) {
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    let command_str = parts[0].to_lowercase();
    let args: Vec<String> = parts[1..].to_vec(); // `args` is already shlex-split

    let cmd_type = match command_str.as_str() {
        "start" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None; // Added cluster_arg
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Added cluster parsing
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start rest': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StartRest { port: port_arg, cluster: cluster_arg } // Included cluster
                    },
                    "storage" => {
                        let mut port_arg = None;
                        let mut config_file_arg = None;
                        let mut cluster_arg = None; // Added cluster_arg
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file_arg = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Added cluster parsing
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start storage': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StartStorage { port: port_arg, config_file: config_file_arg, cluster: cluster_arg } // Included cluster
                    },
                    "daemon" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'daemon start': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Daemon(DaemonCliCommand::Start { port: port_arg, cluster: cluster_arg })
                    },
                    "all" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None;
                        let mut listen_port_arg = None;
                        let mut storage_port_arg = None;
                        let mut storage_config_file_arg = None;
                        let mut daemon_cluster_arg = None; // Corrected variable name
                        let mut daemon_port_arg = None;     // Corrected variable name
                        let mut rest_cluster_arg = None;    // Corrected variable name
                        let mut rest_port_arg = None;       // Corrected variable name
                        let mut storage_cluster_arg = None; // Added storage_cluster_arg
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        listen_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        storage_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-config" => {
                                    if i + 1 < parts.len() {
                                        storage_config_file_arg = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-port" => {
                                    if i + 1 < parts.len() {
                                        daemon_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < parts.len() {
                                        daemon_cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-port" => {
                                    if i + 1 < parts.len() {
                                        rest_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => {
                                    if i + 1 < parts.len() {
                                        rest_cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => { // Added storage_cluster parsing
                                    if i + 1 < parts.len() {
                                        storage_cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start all': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StartAll {
                            port: port_arg,
                            cluster: cluster_arg,
                            listen_port: listen_port_arg,
                            storage_port: storage_port_arg,
                            storage_config_file: storage_config_file_arg,
                            daemon_cluster: daemon_cluster_arg,
                            daemon_port: daemon_port_arg,
                            rest_cluster: rest_cluster_arg,
                            rest_port: rest_port_arg,
                            storage_cluster: storage_cluster_arg,
                        }
                    },
                    _ => CommandType::Unknown,
                }
            } else {
                CommandType::StartAll {
                    port: None,
                    cluster: None,
                    listen_port: None,
                    storage_port: None,
                    storage_config_file: None,
                    daemon_cluster: None, // Added missing fields
                    daemon_port: None,    // Added missing fields
                    rest_cluster: None,   // Added missing fields
                    rest_port: None,      // Added missing fields
                    storage_cluster: None, // Added missing fields
                }
            }
        },
        "daemon" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "start" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'daemon start': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Daemon(DaemonCliCommand::Start { port: port_arg, cluster: cluster_arg })
                    },
                    "stop" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::Daemon(DaemonCliCommand::Stop { port: port_arg })
                    },
                    "status" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::Daemon(DaemonCliCommand::Status { port: port_arg })
                    },
                    "list" => CommandType::Daemon(DaemonCliCommand::List),
                    "clear-all" => CommandType::Daemon(DaemonCliCommand::ClearAll),
                    _ => CommandType::Unknown,
                }
            } else {
                CommandType::Unknown
            }
        },
        "rest" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "start" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None; // Added cluster_arg
                        let mut current_idx = 2;

                        while current_idx < parts.len() {
                            match parts[current_idx].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if current_idx + 1 < parts.len() {
                                        port_arg = parts[current_idx + 1].parse::<u16>().ok();
                                        current_idx += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[current_idx]);
                                        current_idx += 1;
                                    }
                                }
                                "--cluster" => { // Added cluster parsing
                                    if current_idx + 1 < parts.len() {
                                        cluster_arg = Some(parts[current_idx + 1].clone());
                                        current_idx += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[current_idx]);
                                        current_idx += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'rest start': {}", parts[current_idx]);
                                    current_idx += 1;
                                }
                            }
                        }
                        CommandType::Rest(RestCliCommand::Start { port: port_arg, cluster: cluster_arg }) // Included cluster
                    },
                    "stop" => CommandType::Rest(RestCliCommand::Stop),
                    "status" => CommandType::Rest(RestCliCommand::Status),
                    "health" => CommandType::Rest(RestCliCommand::Health),
                    "version" => CommandType::Rest(RestCliCommand::Version),
                    "register-user" => {
                        if args.len() >= 2 {
                            CommandType::Rest(RestCliCommand::RegisterUser {
                                username: args[0].clone(),
                                password: args[1].clone(),
                            })
                        } else {
                            CommandType::Unknown
                        }
                    },
                    "authenticate" => {
                        if args.len() >= 2 {
                            CommandType::Rest(RestCliCommand::Authenticate {
                                username: args[0].clone(),
                                password: args[1].clone(),
                            })
                        } else {
                            CommandType::Unknown
                        }
                    },
                    "graph-query" => {
                        if args.len() >= 1 { // Changed from 2 to 1, as query_string is first arg
                            let query_string = args[0].clone();
                            let persist = args.get(1).and_then(|s| s.parse::<bool>().ok());
                            CommandType::Rest(RestCliCommand::GraphQuery { query_string, persist })
                        } else {
                            CommandType::Unknown
                        }
                    },
                    "storage-query" => CommandType::Rest(RestCliCommand::StorageQuery),
                    _ => CommandType::Unknown,
                }
            } else {
                CommandType::Unknown
            }
        },
        "storage" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "start" => {
                        let mut port_arg = None;
                        let mut config_file_arg = None;
                        let mut cluster_arg = None; // Added cluster_arg
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file_arg = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Added cluster parsing
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'storage start': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Storage(StorageAction::Start { port: port_arg, config_file: config_file_arg, cluster: cluster_arg }) // Included cluster
                    },
                    "stop" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::Storage(StorageAction::Stop { port: port_arg })
                    },
                    "status" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::Storage(StorageAction::Status { port: port_arg })
                    },
                    _ => CommandType::Unknown,
                }
            } else {
                CommandType::Unknown
            }
        },
        "stop" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => CommandType::StopRest,
                    "daemon" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::StopDaemon(port_arg)
                    },
                    "storage" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::StopStorage(port_arg)
                    },
                    "all" => CommandType::StopAll,
                    _ => CommandType::StopAll,
                }
            } else {
                CommandType::StopAll
            }
        },
        "status" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => CommandType::StatusRest,
                    "daemon" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::StatusDaemon(port_arg)
                    },
                    "storage" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::StatusStorage(port_arg)
                    },
                    "cluster" => CommandType::StatusCluster,
                    "all" => CommandType::StatusSummary,
                    _ => CommandType::StatusSummary,
                }
            } else {
                CommandType::StatusSummary
            }
        },
        "auth" => {
            if args.len() >= 2 {
                CommandType::Auth { username: args[0].clone(), password: args[1].clone() }
            } else {
                eprintln!("Usage: auth <username> <password>");
                CommandType::Unknown
            }
        },
        "authenticate" => {
            if args.len() >= 2 {
                CommandType::Authenticate { username: args[0].clone(), password: args[1].clone() }
            } else {
                eprintln!("Usage: authenticate <username> <password>");
                CommandType::Unknown
            }
        },
        "register" => {
            if args.len() >= 2 {
                CommandType::RegisterUser { username: args[0].clone(), password: args[1].clone() }
            } else {
                eprintln!("Usage: register <username> <password>");
                CommandType::Unknown
            }
        },
        "version" => CommandType::Version,
        "health" => CommandType::Health,
        "reload" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "all" => CommandType::ReloadAll,
                    "rest" => CommandType::ReloadRest,
                    "storage" => CommandType::ReloadStorage,
                    "daemon" => {
                        let port_arg = parts.get(2).and_then(|s| s.parse::<u16>().ok());
                        CommandType::ReloadDaemon(port_arg)
                    },
                    "cluster" => CommandType::ReloadCluster,
                    _ => CommandType::Unknown,
                }
            } else {
                eprintln!("Usage: reload [all|rest|storage|daemon|cluster] [--port <port>]");
                CommandType::Unknown
            }
        },
        "restart" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "all" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None;
                        let mut listen_port_arg = None;
                        let mut storage_port_arg = None;
                        let mut storage_config_file_arg = None;
                        let mut daemon_cluster_arg = None;
                        let mut daemon_port_arg = None;
                        let mut rest_cluster_arg = None;
                        let mut rest_port_arg = None;
                        let mut storage_cluster_arg = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        listen_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        storage_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-config" => {
                                    if i + 1 < parts.len() {
                                        storage_config_file_arg = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-port" => {
                                    if i + 1 < parts.len() {
                                        daemon_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < parts.len() {
                                        daemon_cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-port" => {
                                    if i + 1 < parts.len() {
                                        rest_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => {
                                    if i + 1 < parts.len() {
                                        rest_cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => {
                                    if i + 1 < parts.len() {
                                        storage_cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart all': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartAll {
                            port: port_arg,
                            cluster: cluster_arg,
                            listen_port: listen_port_arg,
                            storage_port: storage_port_arg,
                            storage_config_file: storage_config_file_arg,
                            daemon_cluster: daemon_cluster_arg,
                            daemon_port: daemon_port_arg,
                            rest_cluster: rest_cluster_arg,
                            rest_port: rest_port_arg,
                            storage_cluster: storage_cluster_arg,
                        }
                    },
                    "rest" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None; // Added cluster_arg
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Added cluster parsing
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart rest': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartRest { port: port_arg, cluster: cluster_arg } // Included cluster
                    },
                    "storage" => {
                        let mut port_arg = None;
                        let mut config_file_arg = None;
                        let mut cluster_arg = None; // Added cluster_arg
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file_arg = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Added cluster parsing
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart storage': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartStorage { port: port_arg, config_file: config_file_arg, cluster: cluster_arg } // Included cluster
                    },
                    "daemon" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None;
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < parts.len() {
                                        cluster_arg = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart daemon': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartDaemon { port: port_arg, cluster: cluster_arg }
                    },
                    "cluster" => CommandType::RestartCluster,
                    _ => CommandType::Unknown,
                }
            } else {
                eprintln!("Usage: restart [all|rest|storage|daemon|cluster] ...");
                CommandType::Unknown
            }
        },
        "help" => {
            let mut help_command_string: Option<String> = None;
            let mut positional_args: Vec<String> = Vec::new();
            let mut i = 1;

            while i < parts.len() {
                match parts[i].to_lowercase().as_str() {
                    "--command" | "-command" | "--c" | "-c" => {
                        if i + 1 < parts.len() {
                            help_command_string = Some(parts[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                            i += 1;
                        }
                    },
                    _ => {
                        positional_args.push(parts[i].clone());
                        i += 1;
                    }
                }
            }

            let help_args = if let Some(cmd_str) = help_command_string {
                HelpArgs { filter_command: Some(cmd_str), command_path: Vec::new() }
            } else if !positional_args.is_empty() {
                HelpArgs { filter_command: Some(positional_args.join(" ")), command_path: Vec::new() }
            } else {
                HelpArgs { filter_command: None, command_path: Vec::new() }
            };
            CommandType::Help(help_args)
        }
        "clear" | "clean" => CommandType::Clear,
        "exit" | "quit" | "q" => CommandType::Exit,
        _ => CommandType::Unknown,
    };

    (cmd_type, args)
}


/// Main asynchronous loop for the CLI interactive mode.
pub async fn run_cli_interactive(
    daemon_handles: Arc<TokioMutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    // New parameters for Storage daemon management - keeping them as parameters
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>, // Keeping this as a parameter
) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let history_path = "graphdb_cli_history.txt";
    let _ = rl.load_history(history_path);

    handlers::print_welcome_screen(); // Display the welcome screen

    loop {
        let readline = rl.readline("GraphDB> ");
        match readline {
            Ok(line) => {
                let line_trim = line.trim();
                if line_trim.is_empty() {
                    continue; // Skip empty lines
                }

                rl.add_history_entry(line_trim).ok(); // Add to history

                // Use shlex to correctly split the input, handling quotes
                let args = match shlex::split(line_trim) {
                    Some(a) => a,
                    None => {
                        eprintln!("Error: Malformed input. Please check quoting.");
                        continue;
                    }
                };
                
                // If shlex returns an empty vec for some reason, or only whitespace was entered
                if args.is_empty() {
                    continue;
                }

                let (command, _parsed_args) = parse_command(&args); // Pass shlex-parsed args to parse_command

                // Handle exit command directly to ensure cleanup before breaking the loop
                if command == CommandType::Exit {
                    handle_interactive_command(
                        command,
                        daemon_handles.clone(),
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_port_arc.clone(),
                        rest_api_handle.clone(),
                        storage_daemon_shutdown_tx_opt.clone(),
                        storage_daemon_handle.clone(),
                        storage_daemon_port_arc.clone(), // Pass the parameter
                    ).await?;
                    break; // Exit the loop
                }

                // Clone Arc for each command handling to allow concurrent access
                let daemon_handles_clone = Arc::clone(&daemon_handles);
                let rest_api_shutdown_tx_opt_clone = Arc::clone(&rest_api_shutdown_tx_opt);
                let rest_api_port_arc_clone = Arc::clone(&rest_api_port_arc);
                let rest_api_handle_clone = Arc::clone(&rest_api_handle);
                let storage_daemon_shutdown_tx_opt_clone = Arc::clone(&storage_daemon_shutdown_tx_opt); // Use parameter
                let storage_daemon_handle_clone = Arc::clone(&storage_daemon_handle); // Use parameter
                let storage_daemon_port_arc_clone = Arc::clone(&storage_daemon_port_arc); // Use parameter


                // Dispatch the command to the interactive command handler
                handle_interactive_command(
                    command,
                    daemon_handles_clone,
                    rest_api_shutdown_tx_opt_clone,
                    rest_api_port_arc_clone,
                    rest_api_handle_clone,
                    storage_daemon_shutdown_tx_opt_clone,
                    storage_daemon_handle_clone,
                    storage_daemon_port_arc_clone,
                ).await?;
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C pressed
                println!("Ctrl-C received. Type 'exit' to quit or Ctrl-D to terminate.");
            }
            Err(ReadlineError::Eof) => {
                // Ctrl-D pressed
                println!("Ctrl-D received. Exiting GraphDB CLI. Goodbye!");
                break; // Exit the loop
            }
            Err(err) => {
                eprintln!("Error reading line: {:?}", err);
                break; // Exit on other errors
            }
        }
    }
    
    // Save history to a file on exit
    rl.save_history(&history_path).context("Failed to save history")?;

    Ok(())
}

/// Handler for CLI commands in interactive mode.
/// This function dispatches interactive commands to the appropriate handlers in the `handlers` module.
pub async fn handle_interactive_command(
    command: CommandType,
    daemon_handles: Arc<TokioMutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    // New parameters for Storage daemon management
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>, // Added: Shared state for storage port
) -> Result<()> {
    match command {
        CommandType::Daemon(daemon_cmd) => {
            handlers::handle_daemon_command_interactive(daemon_cmd, daemon_handles).await?;
        }
        CommandType::Rest(rest_cmd) => {
            // FIX: Corrected argument order
            handlers::handle_rest_command_interactive(
                rest_cmd,
                rest_api_shutdown_tx_opt,
                rest_api_handle, // Swapped
                rest_api_port_arc, // Swapped
            )
            .await?;
        }
        CommandType::Storage(storage_action) => {
            handlers::handle_storage_command_interactive(
                storage_action,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
        }
        CommandType::StartRest { port, cluster } => {
            // FIX: Added missing cluster argument
            handlers::start_rest_api_interactive(
                port,
                cluster,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            )
            .await?;
        }
        CommandType::StartStorage {
            port,
            config_file,
            cluster,
        } => {
            // FIX: Added missing cluster argument
            handlers::start_storage_interactive(
                port,
                config_file,
                cluster, // Added cluster argument
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await?;
        }
        CommandType::StartDaemon { port, cluster } => {
            handlers::start_daemon_instance_interactive(port, cluster, daemon_handles).await?;
        }
        CommandType::StartAll {
            port,
            cluster,
            listen_port,
            storage_port,
            storage_config_file,
            daemon_cluster,
            daemon_port,
            rest_cluster,
            rest_port,
            storage_cluster,
        } => {
            // FIX: Corrected argument order and type for storage_config_file
            handlers::handle_start_all_interactive(
                port.or(daemon_port), // Consolidated daemon port
                cluster.or(daemon_cluster), // Consolidated daemon cluster
                listen_port.or(rest_port), // Consolidated rest port
                rest_cluster,
                storage_port,
                storage_cluster,
                storage_config_file, // Directly use PathBuf, no map needed
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await?;
        }
        CommandType::StopAll => {
            handlers::stop_all_interactive(
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
        }
        CommandType::StopRest => {
            handlers::stop_rest_api_interactive(
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            )
            .await?;
        }
        CommandType::StopDaemon(port) => {
            handlers::stop_daemon_instance_interactive(port, daemon_handles).await?;
        }
        CommandType::StopStorage(port) => {
            handlers::stop_storage_interactive(
                port,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
        }
        CommandType::StatusSummary => {
            handlers::display_full_status_summary(
                rest_api_port_arc.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await;
        }
        CommandType::StatusRest => {
            handlers::display_rest_api_status(rest_api_port_arc.clone()).await;
        }
        CommandType::StatusDaemon(port) => {
            handlers::display_daemon_status(port).await;
        }
        CommandType::StatusStorage(port) => {
            handlers::display_storage_daemon_status(port, storage_daemon_port_arc.clone()).await;
        }
        CommandType::StatusCluster => {
            handlers::display_cluster_status().await;
        }
        CommandType::Auth { username, password } => {
            handlers::authenticate_user(username, password).await;
        }
        CommandType::Authenticate { username, password } => {
            handlers::authenticate_user(username, password).await;
        }
        CommandType::RegisterUser { username, password } => {
            handlers::register_user(username, password).await;
        }
        CommandType::Version => {
            handlers::display_rest_api_version().await;
        }
        CommandType::Health => {
            handlers::display_rest_api_health().await;
        }
        CommandType::ReloadAll => {
            handlers::reload_all_interactive(
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
        }
        CommandType::ReloadRest => {
            handlers::reload_rest_interactive(
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            )
            .await?;
        }
        CommandType::ReloadStorage => {
            handlers::reload_storage_interactive(
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await?;
        }
        CommandType::ReloadDaemon(port) => {
            handlers::reload_daemon_interactive(port).await?;
        }
        CommandType::ReloadCluster => {
            handlers::reload_cluster_interactive().await?;
        }
        CommandType::RestartAll {
            port,
            cluster,
            listen_port,
            storage_port,
            storage_config_file,
            daemon_cluster,
            daemon_port,
            rest_cluster,
            rest_port,
            storage_cluster,
        } => {
            let restart_args = RestartArgs {
                action: RestartAction::All {
                    port,
                    cluster,
                    listen_port,
                    storage_port,
                    storage_config_file,
                    daemon_cluster,
                    daemon_port,
                    rest_cluster,
                    rest_port,
                    storage_cluster,
                },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await?;
        }
        CommandType::RestartRest { port, cluster } => {
            // Added cluster
            let restart_args = RestartArgs {
                action: RestartAction::Rest { port, cluster }, // Added cluster
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await?;
        }
        CommandType::RestartStorage {
            port,
            config_file,
            cluster,
        } => {
            // Added cluster
            let restart_args = RestartArgs {
                action: RestartAction::Storage {
                    port,
                    config_file,
                    cluster,
                }, // Added cluster
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await?;
        }
        CommandType::RestartDaemon { port, cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Daemon { port, cluster },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await?;
        }
        CommandType::RestartCluster => {
            let restart_args = RestartArgs {
                action: RestartAction::Cluster,
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await?;
        }
        CommandType::Clear => {
            handlers::clear_terminal_screen().await?;
            handlers::print_welcome_screen();
        }
        CommandType::Help(help_args) => {
            let mut cmd = CliArgs::command();
            if let Some(command_filter) = help_args.filter_command {
                print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else if !help_args.command_path.is_empty() {
                let command_filter = help_args.command_path.join(" ");
                print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else {
                print_help_clap_generated();
            }
        }
        CommandType::Exit => {
            println!("Exiting CLI. Goodbye!");
            process::exit(0);
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
        }
    }
    Ok(())
}
// server/src/cli/interactive.rs

// This file handles the interactive CLI mode, including command parsing
// and displaying interactive help messages.

use anyhow::Result;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::io::{self, AsyncBufReadExt, BufReader};
use tokio::sync::{oneshot, Mutex};
use std::process; // For process::exit
use std::path::PathBuf; // Added PathBuf import
use clap::CommandFactory; // Added CommandFactory import
use std::collections::HashSet; // Added for HashSet
use strsim::jaro_winkler; // Added for jaro_winkler
use tokio::task::JoinHandle; // Required for shared state
use rustyline::DefaultEditor; // Added: Import DefaultEditor from rustyline

// Import necessary items from sibling modules
use crate::cli::commands::{CliArgs, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, ReloadArgs, ReloadAction, StartAction}; // Added ReloadArgs, ReloadAction, StartAction
use crate::cli::help_display::HelpArgs; // Corrected: Import HelpArgs from help_display
// Removed unused imports: use crate::cli::config::CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
use crate::cli::handlers;
// The following stop_daemon_api_call is now primarily for global cleanup on exit,
// interactive command handling uses handlers.rs functions.
use crate::cli::daemon_management::stop_daemon_api_call;
use crate::cli::help_display::{collect_all_cli_elements_for_suggestions, print_help_clap_generated, print_filtered_help_clap_generated}; // Imported functions

/// Enum representing the parsed command type in interactive mode.
#[derive(Debug, PartialEq)]
pub enum CommandType {
    Daemon(DaemonCliCommand),
    Rest(RestCliCommand),
    Storage(StorageAction),
    // Top-level Start command variants
    StartRest { port: Option<u16>, listen_port: Option<u16> },
    StartStorage { port: Option<u16>, config_file: Option<PathBuf> },
    StartDaemon { port: Option<u16>, cluster: Option<String> }, // Added for top-level 'start daemon'
    StartAll {
        port: Option<u16>,
        cluster: Option<String>,
        listen_port: Option<u16>,
        storage_port: Option<u16>,
        storage_config_file: Option<PathBuf>,
    }, // Added for top-level 'start all' or generic 'start'
    StopAll,
    StopRest,
    StopDaemon(Option<u16>),
    StopStorage(Option<u16>),
    StatusSummary, // Represents 'status' or 'status all'
    StatusRest,
    StatusDaemon(Option<u16>),
    StatusStorage(Option<u16>),
    StatusCluster, // Added for 'status cluster'
    Auth { username: String, password: String }, // Added for 'auth <username> <password>'
    Authenticate { username: String, password: String }, // Added for 'authenticate <username> <password>'
    RegisterUser { username: String, password: String }, // Added for 'register <username> <password>'
    Version, // Added for 'version'
    Health, // Added for 'health'
    ReloadAll, // Added for 'reload all'
    ReloadRest, // Added for 'reload rest'
    ReloadStorage, // Added for 'reload storage'
    ReloadDaemon(Option<u16>), // Added for 'reload daemon [--port <port>]'
    ReloadCluster, // Added for 'reload cluster'
    Help(HelpArgs),
    Clear, // Added for 'clear' and 'clean' commands
    Exit,
    Unknown,
}

/// Parses a command string from the interactive CLI input.
pub fn parse_command(input: &str) -> (CommandType, Vec<String>) {
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    let command_str = parts[0].to_lowercase();
    let args: Vec<String> = parts[1..].iter().map(|&s| s.to_string()).collect();

    let cmd_type = match command_str.as_str() {
        "start" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => {
                        let mut port_arg = None;
                        let mut listen_port_arg = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
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
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start rest': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StartRest { port: port_arg, listen_port: listen_port_arg }
                    },
                    "storage" => {
                        let mut port_arg = None;
                        let mut config_file_arg = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" => {
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
                                        config_file_arg = Some(PathBuf::from(parts[i + 1]));
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
                        // Corrected: Pass Option<PathBuf> directly
                        CommandType::StartStorage { port: port_arg, config_file: config_file_arg }
                    },
                    "daemon" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None;
                        let mut i = 2; // Start checking from after "daemon start"

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
                                        cluster_arg = Some(parts[i + 1].to_string());
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
                                        cluster_arg = Some(parts[i + 1].to_string());
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
                                        storage_config_file_arg = Some(PathBuf::from(parts[i + 1]));
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
                        }
                    },
                    _ => CommandType::Unknown,
                }
            } else {
                // If only "start" is given, default to "start all" with no specific args
                CommandType::StartAll {
                    port: None,
                    cluster: None,
                    listen_port: None,
                    storage_port: None,
                    storage_config_file: None,
                }
            }
        },
        "daemon" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "start" => {
                        let mut port_arg = None;
                        let mut cluster_arg = None;
                        let mut i = 2; // Start checking from after "daemon start"

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
                                        cluster_arg = Some(parts[i + 1].to_string());
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
                        let mut listen_port_arg = None;
                        let mut current_idx = 2;

                        while current_idx < parts.len() {
                            match parts[current_idx].to_lowercase().as_str() {
                                "--port" => {
                                    if current_idx + 1 < parts.len() {
                                        port_arg = parts[current_idx + 1].parse::<u16>().ok();
                                        current_idx += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[current_idx]);
                                        current_idx += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if current_idx + 1 < parts.len() {
                                        listen_port_arg = parts[current_idx + 1].parse::<u16>().ok();
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
                        CommandType::Rest(RestCliCommand::Start { port: port_arg, listen_port: listen_port_arg })
                    },
                    "stop" => CommandType::Rest(RestCliCommand::Stop),
                    "status" => CommandType::Rest(RestCliCommand::Status),
                    "health" => CommandType::Rest(RestCliCommand::Health),
                    "version" => CommandType::Rest(RestCliCommand::Version),
                    "register-user" => {
                        if args.len() >= 3 {
                            CommandType::Rest(RestCliCommand::RegisterUser {
                                username: args[1].clone(),
                                password: args[2].clone(),
                            })
                        } else {
                            CommandType::Unknown
                        }
                    },
                    "authenticate" => {
                        if args.len() >= 3 {
                            CommandType::Rest(RestCliCommand::Authenticate {
                                username: args[1].clone(),
                                password: args[2].clone(),
                            })
                        } else {
                            CommandType::Unknown
                        }
                    },
                    "graph-query" => {
                        if args.len() >= 2 {
                            let query_string = args[1].clone();
                            let persist = args.get(2).and_then(|s| s.parse::<bool>().ok());
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
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" => {
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
                                        config_file_arg = Some(PathBuf::from(parts[i + 1]));
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
                        // Corrected: Pass Option<PathBuf> directly
                        CommandType::Storage(StorageAction::Start { port: port_arg, config_file: config_file_arg })
                    },
                    "stop" => {
                        let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
                        CommandType::Storage(StorageAction::Stop { port: port_arg })
                    },
                    "status" => {
                        let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
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
                    "all" => CommandType::StopAll, // Explicit 'stop all'
                    _ => CommandType::StopAll, // Default to stop all if unknown subcommand
                }
            } else {
                CommandType::StopAll // Default to stop all if no subcommand
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
                    "cluster" => CommandType::StatusCluster, // Added for 'status cluster'
                    "all" => CommandType::StatusSummary, // Explicit 'status all'
                    _ => CommandType::StatusSummary, // Default to summary if unknown subcommand
                }
            } else {
                CommandType::StatusSummary // Default to summary if no subcommand
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
        "help" => {
            let mut help_command_string: Option<String> = None;
            let mut positional_args: Vec<String> = Vec::new();
            let mut i = 1;

            while i < parts.len() {
                match parts[i].to_lowercase().as_str() {
                    "--command" | "-command" | "--c" | "-c" => {
                        if i + 1 < parts.len() {
                            help_command_string = Some(parts[i + 1].to_string());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                            i += 1;
                        }
                    },
                    _ => {
                        positional_args.push(parts[i].to_string());
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
        "clear" | "clean" => CommandType::Clear, // Handle both "clear" and "clean"
        "exit" | "quit" | "q" => CommandType::Exit,
        _ => CommandType::Unknown,
    };

    (cmd_type, args)
}

/// Prints general help messages for the interactive CLI.
pub fn print_interactive_help() {
    println!("\nGraphDB CLI Commands:");
    println!("  start [rest|storage|daemon|all] [--port <port>] [--cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>] - Start GraphDB components");
    println!("  stop [rest|daemon|storage|all] [--port <port>] - Stop GraphDB components (all by default, or specific)");
    println!("  daemon start [--port <port>] [--cluster <range>] - Start a GraphDB daemon");
    println!("  daemon stop [--port <port>]                        - Stop a GraphDB daemon");
    println!("  daemon status [--port <port>]                      - Check status of a GraphDB daemon");
    println!("  daemon list                                        - List daemons managed by this CLI");
    println!("  daemon clear-all                                   - Stop all managed daemons and attempt to kill external ones");
    println!("  rest start [--port <port>] [--listen-port <port>] - Start the REST API server");
    println!("  rest stop                                          - Stop the REST API server");
    println!("  rest status                                        - Check the status of the REST API server");
    println!("  rest health                                        - Perform a health check on the REST API server");
    println!("  rest version                                       - Get the version of the REST API server");
    println!("  rest register-user <username> <password>           - Register a new user via REST API");
    println!("  rest authenticate <username> <password>            - Authenticate a user and get a token via REST API");
    println!("  rest graph-query \"<query_string>\" [persist]        - Execute a graph query via REST API");
    println!("  rest storage-query                                 - Execute a storage query via REST API (placeholder)");
    println!("  storage start [--port <port>] [--config-file <path>] - Start the standalone Storage daemon");
    println!("  storage stop [--port <port>]                       - Stop the standalone Storage daemon");
    println!("  storage status [--port <port>]                     - Check the status of the standalone Storage daemon");
    println!("  status [rest|daemon|storage|cluster|all] [--port <port>] - Get a comprehensive status summary or specific component status");
    println!("  auth <username> <password>                         - Authenticate a user and get a token (top-level)");
    println!("  authenticate <username> <password>                 - Authenticate a user and get a token (top-level)");
    println!("  register <username> <password>                     - Register a new user (top-level)");
    println!("  version                                            - Get the version of the REST API server (top-level)");
    println!("  health                                             - Perform a health check on the REST API server (top-level)");
    println!("  reload [all|rest|storage|daemon|cluster] [--port <port>] - Reload GraphDB components");
    println!("  clear | clean                                      - Clear the terminal screen"); // Added
    println!("  help [--command|-c <command_string>]               - Display this help message or help for a specific command");
    println!("  exit | quit | q                                    - Exit the CLI");
    println!("\nNote: Commands like 'view-graph', 'index-node', etc., are placeholders.");
}

/// Prints help messages filtered by a command string for interactive mode.
pub fn print_interactive_filtered_help(_cmd: &mut clap::Command, command_filter: &str) {
    let commands = [
        ("start rest [--port <port>] [--listen-port <port>]", "Start the REST API server"),
        ("start storage [--port <port>] [--config-file <path>]", "Start the standalone Storage daemon"),
        ("start daemon [--port <port>] [--cluster <range>]", "Start a GraphDB daemon instance"),
        ("start all [--port <port>] [--cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>]", "Start all core GraphDB components"),
        ("start", "Start all core GraphDB components (default)"), // Generic start
        ("stop rest", "Stop the REST API server"),
        ("stop daemon [--port <port>]", "Stop a GraphDB daemon"),
        ("stop storage [--port <port>]", "Stop the standalone Storage daemon"),
        ("stop all", "Stop all core GraphDB components"),
        ("stop", "Stop all GraphDB components (default)"), // Generic stop
        ("daemon start [--port <port>] [--cluster <range>]", "Start a GraphDB daemon"),
        ("daemon stop [--port <port>]", "Stop a GraphDB daemon"),
        ("daemon status [--port <port>]", "Check status of a GraphDB daemon"),
        ("daemon list", "List daemons managed by this CLI"),
        ("daemon clear-all", "Stop all managed daemons and attempt to kill external ones"),
        ("rest start [--port <port>] [--listen-port <port>]", "Start the REST API server"),
        ("rest stop", "Stop the REST API server"),
        ("rest status", "Check the status of the REST API server"),
        ("rest health", "Perform a health check on the REST API server"),
        ("rest version", "Get the version of the REST API server"),
        ("rest register-user <username> <password>", "Register a new user via REST API"),
        ("rest authenticate <username> <password>", "Authenticate a user and get a token via REST API"),
        ("rest graph-query \"<query_string>\" [persist]", "Execute a graph query via REST API"),
        ("rest storage-query", "Execute a storage query via REST API (placeholder)"),
        ("storage start [--port <port>] [--config-file <path>]", "Start the standalone Storage daemon"),
        ("storage stop [--port <port>]", "Stop the standalone Storage daemon"),
        ("storage status [--port <port>]", "Check the status of the standalone Storage daemon"),
        ("status rest", "Get detailed status of the REST API component"),
        ("status daemon [--port <port>]", "Get detailed status of a specific daemon or list common ones"),
        ("status storage [--port <port>]", "Get detailed status of the Storage component"),
        ("status cluster", "Get status of the cluster (placeholder)"), // Added
        ("status all", "Get a comprehensive status summary of all GraphDB components"),
        ("status", "Get a comprehensive status summary of all GraphDB components (default)"), // Generic status
        ("auth <username> <password>", "Authenticate a user and get a token"), // Added
        ("authenticate <username> <password>", "Authenticate a user and get a token"), // Added
        ("register <username> <password>", "Register a new user"), // Added
        ("version", "Get the version of the REST API server"), // Added
        ("health", "Perform a health check on the REST API server"), // Added
        ("reload all", "Reload all GraphDB components"), // Added
        ("reload rest", "Reload the REST API server"), // Added
        ("reload storage", "Reload the standalone Storage daemon"), // Added
        ("reload daemon [--port <port>]", "Reload a specific GraphDB daemon"), // Added
        ("reload cluster", "Reload the cluster configuration"), // Added
        ("clear", "Clear the terminal screen"), // Added
        ("clean", "Clear the terminal screen"), // Added
        ("help [--command|-c <command_string>]", "Display this help message or help for a specific command"),
        ("exit | quit | q", "Exit the CLI"),
    ];

    let filter_lower = command_filter.to_lowercase();
    let mut found_match = false;

    println!("\n--- Help for '{}' ---", command_filter);
    for (command_syntax, description) in commands.iter() {
        // Check for exact command match or a strong prefix match
        if command_syntax.to_lowercase() == filter_lower ||
           (command_syntax.to_lowercase().starts_with(&filter_lower) && filter_lower.contains(' ')) {
            println!("  {:<50} - {}", command_syntax, description);
            found_match = true;
        } else if description.to_lowercase().contains(&filter_lower) {
            // Also include matches in the description
            println!("  {:<50} - {}", command_syntax, description);
            found_match = true;
        }
    }

    if !found_match {
        // Fallback to fuzzy matching and general help.
        let mut all_known_elements = HashSet::<String>::new();
        collect_all_cli_elements_for_suggestions(&CliArgs::command(), &mut Vec::new(), &mut all_known_elements);

        let mut suggestions = Vec::new();
        const JARO_WINKLER_THRESHOLD: f64 = 0.75;

        for known_element in &all_known_elements {
            let similarity = jaro_winkler(&filter_lower, &known_element.to_lowercase());
            if similarity > JARO_WINKLER_THRESHOLD {
                suggestions.push((known_element.clone(), similarity));
            }
        }

        suggestions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        if !suggestions.is_empty() {
            println!("\nNo exact help found for '{}'. Did you mean one of these?", command_filter);
            for (suggestion, _) in suggestions.iter().take(5) {
                println!("  graphdb-cli {}", suggestion);
            }
            if command_filter.starts_with("--") || command_filter.ends_with("-") {
                println!("\nIf you were looking for an option for a command, try 'graphdb-cli <command> --help'.");
            }
        } else {
            println!("\nNo specific help found for '{}'. Displaying general help.", command_filter);
            print_help_clap_generated();
        }
    }
    println!("------------------------------------");
}

/// Main asynchronous loop for the CLI interactive mode.
pub async fn run_cli_interactive(
    daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let history_path = "graphdb_cli_history.txt";
    let _ = rl.load_history(history_path);

    handlers::print_welcome_screen(); // Display the welcome screen

    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut input = String::new();

    loop {
        print!("graphdb-cli> ");
        let _ = std::io::stdout().flush(); // Ensure prompt is displayed

        input.clear();
        if let Err(e) = reader.read_line(&mut input).await {
            eprintln!("Failed to read line: {}", e);
            break;
        }

        let (command, _args) = parse_command(&input); // Parse the input command

        // Handle exit command directly to ensure cleanup before breaking the loop
        if command == CommandType::Exit {
            handle_interactive_command(
                command,
                daemon_handles.clone(),
                rest_api_shutdown_tx_opt.clone(),
                rest_api_port_arc.clone(),
                rest_api_handle.clone(),
            ).await?;
            break; // Exit the loop
        }

        // Clone Arc for each command handling to allow concurrent access
        let daemon_handles_clone = Arc::clone(&daemon_handles);
        let rest_api_shutdown_tx_opt_clone = Arc::clone(&rest_api_shutdown_tx_opt);
        let rest_api_port_arc_clone = Arc::clone(&rest_api_port_arc);
        let rest_api_handle_clone = Arc::clone(&rest_api_handle);

        // Dispatch the command to the interactive command handler
        handle_interactive_command(
            command,
            daemon_handles_clone,
            rest_api_shutdown_tx_opt_clone,
            rest_api_port_arc_clone,
            rest_api_handle_clone,
        ).await?;
    }

    // Graceful shutdown logic when exiting the interactive loop
    println!("Shutting down GraphDB CLI components...");

    // Stop REST API server if it was started by this CLI
    handlers::stop_rest_api_interactive(
        rest_api_shutdown_tx_opt.clone(),
        rest_api_port_arc.clone(),
        rest_api_handle.clone(),
    ).await?;

    // Stop all managed daemons
    handlers::stop_daemon_instance_interactive(None, daemon_handles.clone()).await?;

    // Also send a global stop signal to ensure any external daemon processes are terminated
    println!("Sending global stop signal to all daemon processes...");
    let stop_result = stop_daemon_api_call(); // This is the general API call
    match stop_result {
        Ok(()) => println!("Global daemon stop signal sent successfully."),
        Err(ref e) => eprintln!("Failed to send global stop signal: {:?}", e),
    }

    println!("GraphDB CLI shutdown complete. Goodbye!");
    Ok(())
}

/// Handler for CLI commands in interactive mode.
/// This function dispatches interactive commands to the appropriate handlers in the `handlers` module.
pub async fn handle_interactive_command(
    command: CommandType,
    daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
) -> Result<()> {
    match command {
        CommandType::Daemon(daemon_cmd) => {
            handlers::handle_daemon_command_interactive(daemon_cmd, daemon_handles).await?;
        }
        CommandType::Rest(rest_cmd) => {
            handlers::handle_rest_command_interactive(rest_cmd, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
        }
        CommandType::Storage(storage_action) => {
            handlers::handle_storage_command_interactive(storage_action).await?;
        }
        CommandType::StartRest { port, listen_port } => {
            handlers::start_rest_api_interactive(port, listen_port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
        }
        CommandType::StartStorage { port, config_file } => {
            handlers::start_storage_interactive(port, config_file).await?;
        }
        CommandType::StartDaemon { port, cluster } => { // New handler for 'start daemon'
            handlers::start_daemon_instance_interactive(port, cluster, daemon_handles).await?;
        }
        CommandType::StartAll { port, cluster, listen_port, storage_port, storage_config_file } => { // New handler for 'start all'
            handlers::handle_start_all_interactive(
                port, cluster, listen_port, storage_port, storage_config_file,
                daemon_handles, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle
            ).await?;
        }
        CommandType::StopAll => {
            handlers::stop_all_interactive(
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            ).await?;
        }
        CommandType::StopRest => {
            handlers::stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
        }
        CommandType::StopDaemon(port) => {
            handlers::stop_daemon_instance_interactive(port, daemon_handles).await?;
        }
        CommandType::StopStorage(port) => {
            handlers::stop_storage_interactive(port).await?;
        }
        CommandType::StatusSummary => {
            handlers::display_full_status_summary().await;
        }
        CommandType::StatusRest => {
            handlers::display_rest_api_status().await;
        }
        CommandType::StatusDaemon(port) => {
            handlers::display_daemon_status(port).await;
        }
        CommandType::StatusStorage(port) => {
            handlers::display_storage_daemon_status(port).await;
        }
        CommandType::StatusCluster => { // Added
            handlers::display_cluster_status().await;
        }
        CommandType::Auth { username, password } => { // Added
            handlers::authenticate_user(username, password).await;
        }
        CommandType::Authenticate { username, password } => { // Added
            handlers::authenticate_user(username, password).await;
        }
        CommandType::RegisterUser { username, password } => { // Added
            handlers::register_user(username, password).await;
        }
        CommandType::Version => { // Added
            handlers::display_rest_api_version().await;
        }
        CommandType::Health => { // Added
            handlers::display_rest_api_health().await;
        }
        CommandType::ReloadAll => { // Added
            handlers::reload_all_interactive(
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            ).await?;
        }
        CommandType::ReloadRest => { // Added
            handlers::reload_rest_interactive(
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            ).await?;
        }
        CommandType::ReloadStorage => { // Added
            handlers::reload_storage_interactive().await?;
        }
        CommandType::ReloadDaemon(port) => { // Added
            handlers::reload_daemon_interactive(port).await?;
        }
        CommandType::ReloadCluster => { // Added
            handlers::reload_cluster_interactive().await?;
        }
        CommandType::Clear => { // Handle clear/clean command
            handlers::clear_terminal_screen().await?;
            handlers::print_welcome_screen(); // Re-print the welcome screen after clearing
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


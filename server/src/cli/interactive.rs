// server/src/cli/interactive.rs

// This file handles the interactive CLI mode, including command parsing
// and displaying interactive help messages.
// FIX: 2025-07-06 - Added missing arguments to handle_start_all_interactive call in CommandType::StartAll.
// FIX: 2025-07-06 - Updated parse_command to handle additional StorageAction::Start fields (cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type).
// FIX: 2025-07-06 - Added import for config_mod::StorageEngineType to support --storage-engine parsing.
// FIX: 2025-07-06 - Fixed RestartAction::All and RestartAction::Storage to include all required fields.
// FIX: 2025-07-06 - Changed storage_engine_type to Option<String> to match StorageAction::Start.
// FIX: 2025-07-06 - Fixed RestCliCommand::Stop and RestCliCommand::Status to include port field.
// FIX: 2025-07-06 - Fixed type mismatches for data_directory and log_directory to Option<String>.
// FIX: 2025-07-06 - Removed StartAction import as it does not exist.
// FIX: 2025-07-06 - Removed reload_* commands and handlers due to missing implementations in handlers.rs.
// FIX: 2025-07-06 - Fixed missing fields in RestartAction initializers and corrected CommandType variants.
// FIX: 2025-07-12 - Added missing fields (daemon, rest, storage) in RestartAction initializers to fix E0063 errors.
// FIX: 2025-07-12 - Ensured proper handling of --cluster, --port, --join-cluster in parse_command for start and restart commands.
// FIX: 2025-07-12 - Addressed unused port.arg warning by ensuring correct usage in parse_command.
// FIX: 2025-07-12 - Removed cluster field from CommandType::RestartStorage to fix E0026 and E0559.
// FIX: 2025-07-12 - Fixed E0425 by removing invalid rl.line() reference in CommandType::Unknown.
// FIX: 2025-07-12 - Added explicit --join-cluster flag support in parse_command and help messages.
// FIX: 2025-07-12 - Verified clear/clean commands consistency.
// FIX: 2025-07-12 - Fixed E0308 by wrapping RestartAction in Some() for RestartArgs.
// FIX: 2025-07-12 - Fixed E0063 by adding missing cluster field in RestartAction::Rest and RestartAction::Storage.
// FIX: 2025-07-12 - Fixed E0609 and E0277 by using help_args.filter_command instead of help_args.command.

use anyhow::{Result, Context};
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, history::DefaultHistory};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use std::process;
use std::path::PathBuf;
use clap::CommandFactory;
use std::collections::HashSet;
use strsim::jaro_winkler;
use shlex;

// Import necessary items from sibling modules
use crate::cli::cli::CliArgs;
use crate::cli::commands::{DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs, RestartArgs, RestartAction};
use crate::cli::help_display::HelpArgs;
use crate::cli::config::CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
use crate::cli::handlers;
use crate::cli::daemon_management::stop_daemon_api_call;

/// Enum representing the parsed command type in interactive mode.
#[derive(Debug, PartialEq)]
pub enum CommandType {
    Daemon(DaemonCliCommand),
    Rest(RestCliCommand),
    Storage(StorageAction),
    // Top-level Start command variants
    StartRest { port: Option<u16> },
    StartStorage {
        port: Option<u16>,
        config_file: Option<PathBuf>,
        cluster: Option<String>,
        data_directory: Option<String>,
        log_directory: Option<String>,
        max_disk_space_gb: Option<u64>,
        min_disk_space_gb: Option<u64>,
        use_raft_for_scale: Option<bool>,
        storage_engine_type: Option<String>,
    },
    StartDaemon { port: Option<u16>, cluster: Option<String> },
    StartAll {
        port: Option<u16>,
        cluster: Option<String>,
        listen_port: Option<u16>,
        storage_port: Option<u16>,
        storage_config_file: Option<PathBuf>,
        data_directory: Option<String>,
        log_directory: Option<String>,
        max_disk_space_gb: Option<u64>,
        min_disk_space_gb: Option<u64>,
        use_raft_for_scale: Option<bool>,
        storage_engine_type: Option<String>,
        config_file: Option<PathBuf>,
    },
    StopAll,
    StopRest,
    StopDaemon(Option<u16>),
    StopStorage(Option<u16>),
    StatusSummary,
    StatusRest,
    StatusDaemon(Option<u16>),
    StatusStorage(Option<u16>),
    StatusCluster,
    Auth { username: String, password: String },
    Authenticate { username: String, password: String },
    RegisterUser { username: String, password: String },
    Version,
    Health,
    RestartAll {
        port: Option<u16>,
        cluster: Option<String>,
        listen_port: Option<u16>,
        storage_port: Option<u16>,
        storage_config_file: Option<PathBuf>,
        data_directory: Option<String>,
        log_directory: Option<String>,
        max_disk_space_gb: Option<u64>,
        min_disk_space_gb: Option<u64>,
        use_raft_for_scale: Option<bool>,
        storage_engine_type: Option<String>,
        config_file: Option<PathBuf>,
    },
    RestartRest { port: Option<u16> },
    RestartStorage {
        port: Option<u16>,
        config_file: Option<PathBuf>,
        data_directory: Option<String>,
        log_directory: Option<String>,
        max_disk_space_gb: Option<u64>,
        min_disk_space_gb: Option<u64>,
        use_raft_for_scale: Option<bool>,
        storage_engine_type: Option<String>,
    },
    RestartDaemon { port: Option<u16>, cluster: Option<String> },
    RestartCluster,
    Help(HelpArgs),
    Clear,
    Exit,
    Unknown,
}

/// Parses a command string from the interactive CLI input.
/// This function now expects a `Vec<String>` (from shlex) as input.
pub fn parse_command(parts: &[String]) -> (CommandType, Vec<String>) {
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    let command_str = parts[0].to_lowercase();
    let args: Vec<String> = parts[1..].to_vec();

    let cmd_type = match command_str.as_str() {
        "start" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
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
                        CommandType::StartRest { port }
                    },
                    "storage" => {
                        let mut port = None;
                        let mut config_file = None;
                        let mut cluster = None;
                        let mut data_directory = None;
                        let mut log_directory = None;
                        let mut max_disk_space_gb = None;
                        let mut min_disk_space_gb = None;
                        let mut use_raft_for_scale = None;
                        let mut storage_engine_type = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" | "--join-cluster" | "-j" => {
                                    if i + 1 < parts.len() {
                                        cluster = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--data-directory" => {
                                    if i + 1 < parts.len() {
                                        data_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--log-directory" => {
                                    if i + 1 < parts.len() {
                                        log_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--max-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        max_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--min-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        min_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--use-raft-for-scale" => {
                                    if i + 1 < parts.len() {
                                        use_raft_for_scale = parts[i + 1].parse::<bool>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-engine" => {
                                    if i + 1 < parts.len() {
                                        storage_engine_type = Some(parts[i + 1].clone());
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
                        CommandType::StartStorage {
                            port,
                            config_file,
                            cluster,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
                        }
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" | "--join-cluster" | "-j" => {
                                    if i + 1 < parts.len() {
                                        cluster = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start daemon': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StartDaemon { port, cluster }
                    },
                    "all" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut listen_port = None;
                        let mut storage_port = None;
                        let mut storage_config_file = None;
                        let mut config_file = None;
                        let mut data_directory = None;
                        let mut log_directory = None;
                        let mut max_disk_space_gb = None;
                        let mut min_disk_space_gb = None;
                        let mut use_raft_for_scale = None;
                        let mut storage_engine_type = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" | "--join-cluster" | "-j" => {
                                    if i + 1 < parts.len() {
                                        cluster = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        listen_port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        storage_port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-config" => {
                                    if i + 1 < parts.len() {
                                        storage_config_file = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--data-directory" => {
                                    if i + 1 < parts.len() {
                                        data_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--log-directory" => {
                                    if i + 1 < parts.len() {
                                        log_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--max-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        max_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--min-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        min_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--use-raft-for-scale" => {
                                    if i + 1 < parts.len() {
                                        use_raft_for_scale = parts[i + 1].parse::<bool>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-engine" => {
                                    if i + 1 < parts.len() {
                                        storage_engine_type = Some(parts[i + 1].clone());
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
                            port,
                            cluster,
                            listen_port,
                            storage_port,
                            storage_config_file,
                            config_file,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
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
                    config_file: None,
                    data_directory: None,
                    log_directory: None,
                    max_disk_space_gb: None,
                    min_disk_space_gb: None,
                    use_raft_for_scale: None,
                    storage_engine_type: None,
                }
            }
        },
        "daemon" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "start" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" | "--join-cluster" | "-j" => {
                                    if i + 1 < parts.len() {
                                        cluster = Some(parts[i + 1].clone());
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
                        CommandType::Daemon(DaemonCliCommand::Start { port, cluster })
                    },
                    "stop" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'daemon stop': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Daemon(DaemonCliCommand::Stop { port })
                    },
                    "status" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'daemon status': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Daemon(DaemonCliCommand::Status { port })
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
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'rest start': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Rest(RestCliCommand::Start { port })
                    },
                    "stop" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'rest stop': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Rest(RestCliCommand::Stop { port })
                    },
                    "status" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'rest status': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Rest(RestCliCommand::Status { port })
                    },
                    "health" => CommandType::Rest(RestCliCommand::Health),
                    "version" => CommandType::Rest(RestCliCommand::Version),
                    "register-user" => {
                        if args.len() >= 2 {
                            CommandType::Rest(RestCliCommand::RegisterUser {
                                username: args[0].clone(),
                                password: args[1].clone(),
                            })
                        } else {
                            eprintln!("Usage: rest register-user <username> <password>");
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
                            eprintln!("Usage: rest authenticate <username> <password>");
                            CommandType::Unknown
                        }
                    },
                    "graph-query" => {
                        if args.len() >= 1 {
                            let query_string = args[0].clone();
                            let persist = args.get(1).and_then(|s| s.parse::<bool>().ok());
                            CommandType::Rest(RestCliCommand::GraphQuery { query_string, persist })
                        } else {
                            eprintln!("Usage: rest graph-query \"<query_string>\" [persist]");
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
                        let mut port = None;
                        let mut config_file = None;
                        let mut cluster = None;
                        let mut data_directory = None;
                        let mut log_directory = None;
                        let mut max_disk_space_gb = None;
                        let mut min_disk_space_gb = None;
                        let mut use_raft_for_scale = None;
                        let mut storage_engine_type = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" | "--join-cluster" | "-j" => {
                                    if i + 1 < parts.len() {
                                        cluster = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--data-directory" => {
                                    if i + 1 < parts.len() {
                                        data_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--log-directory" => {
                                    if i + 1 < parts.len() {
                                        log_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--max-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        max_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--min-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        min_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--use-raft-for-scale" => {
                                    if i + 1 < parts.len() {
                                        use_raft_for_scale = parts[i + 1].parse::<bool>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-engine" => {
                                    if i + 1 < parts.len() {
                                        storage_engine_type = Some(parts[i + 1].clone());
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
                        CommandType::Storage(StorageAction::Start {
                            port,
                            config_file,
                            cluster,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
                        })
                    },
                    "stop" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'storage stop': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Storage(StorageAction::Stop { port })
                    },
                    "status" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'storage status': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Storage(StorageAction::Status { port })
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
                    "rest" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'stop rest': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StopRest
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'stop daemon': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StopDaemon(port)
                    },
                    "storage" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'stop storage': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StopStorage(port)
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
                    "rest" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'status rest': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StatusRest
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'status daemon': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StatusDaemon(port)
                    },
                    "storage" => {
                        let mut port = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'status storage': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StatusStorage(port)
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
                CommandType::Auth {
                    username: args[0].clone(),
                    password: args[1].clone(),
                }
            } else {
                eprintln!("Usage: auth <username> <password>");
                CommandType::Unknown
            }
        },
        "authenticate" => {
            if args.len() >= 2 {
                CommandType::Authenticate {
                    username: args[0].clone(),
                    password: args[1].clone(),
                }
            } else {
                eprintln!("Usage: authenticate <username> <password>");
                CommandType::Unknown
            }
        },
        "register" => {
            if args.len() >= 2 {
                CommandType::RegisterUser {
                    username: args[0].clone(),
                    password: args[1].clone(),
                }
            } else {
                eprintln!("Usage: register <username> <password>");
                CommandType::Unknown
            }
        },
        "version" => CommandType::Version,
        "health" => CommandType::Health,
        "restart" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "all" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut listen_port = None;
                        let mut storage_port = None;
                        let mut storage_config_file = None;
                        let mut config_file = None;
                        let mut data_directory = None;
                        let mut log_directory = None;
                        let mut max_disk_space_gb = None;
                        let mut min_disk_space_gb = None;
                        let mut use_raft_for_scale = None;
                        let mut storage_engine_type = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" | "--join-cluster" | "-j" => {
                                    if i + 1 < parts.len() {
                                        cluster = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        listen_port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        storage_port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-config" => {
                                    if i + 1 < parts.len() {
                                        storage_config_file = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--data-directory" => {
                                    if i + 1 < parts.len() {
                                        data_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--log-directory" => {
                                    if i + 1 < parts.len() {
                                        log_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--max-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        max_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--min-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        min_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--use-raft-for-scale" => {
                                    if i + 1 < parts.len() {
                                        use_raft_for_scale = parts[i + 1].parse::<bool>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-engine" => {
                                    if i + 1 < parts.len() {
                                        storage_engine_type = Some(parts[i + 1].clone());
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
                            port,
                            cluster,
                            listen_port,
                            storage_port,
                            storage_config_file,
                            config_file,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
                        }
                    },
                    "rest" => {
                        let mut port = None;
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
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
                        CommandType::RestartRest { port }
                    },
                    "storage" => {
                        let mut port = None;
                        let mut config_file = None;
                        let mut data_directory = None;
                        let mut log_directory = None;
                        let mut max_disk_space_gb = None;
                        let mut min_disk_space_gb = None;
                        let mut use_raft_for_scale = None;
                        let mut storage_engine_type = None;
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file = Some(PathBuf::from(&parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--data-directory" => {
                                    if i + 1 < parts.len() {
                                        data_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--log-directory" => {
                                    if i + 1 < parts.len() {
                                        log_directory = Some(parts[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--max-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        max_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--min-disk-space-gb" => {
                                    if i + 1 < parts.len() {
                                        min_disk_space_gb = parts[i + 1].parse::<u64>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--use-raft-for-scale" => {
                                    if i + 1 < parts.len() {
                                        use_raft_for_scale = parts[i + 1].parse::<bool>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-engine" => {
                                    if i + 1 < parts.len() {
                                        storage_engine_type = Some(parts[i + 1].clone());
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
                        CommandType::RestartStorage {
                            port,
                            config_file,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
                        }
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        port = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" | "--join-cluster" | "-j" => {
                                    if i + 1 < parts.len() {
                                        cluster = Some(parts[i + 1].clone());
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
                        CommandType::RestartDaemon { port, cluster }
                    },
                    "cluster" => CommandType::RestartCluster,
                    _ => CommandType::Unknown,
                }
            } else {
                eprintln!("Usage: restart [all|rest|storage|daemon|cluster] ...");
                CommandType::Unknown
            }
        },
        "clear" | "clean" => CommandType::Clear,
        "exit" | "quit" | "q" => CommandType::Exit,
        "view-graph" | "index-node" => {
            eprintln!("Command '{}' is not yet implemented.", command_str);
            CommandType::Unknown
        },
        _ => CommandType::Unknown,
    };

    (cmd_type, args)
}

/// Prints general help messages for the interactive CLI.
pub fn print_interactive_help() {
    println!("\nGraphDB CLI Commands:");
    println!("  start [rest|storage|daemon|all] [--port <port>] [--cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] - Start GraphDB components");
    println!("  stop [rest|daemon|storage|all] [--port <port>] - Stop GraphDB components (all by default, or specific)");
    println!("  daemon start [--port <port>] [--cluster <range>] [--join-cluster <range>] - Start a GraphDB daemon");
    println!("  daemon stop [--port <port>]                       - Stop a GraphDB daemon");
    println!("  daemon status [--port <port>]                     - Check status of a GraphDB daemon");
    println!("  daemon list                                       - List daemons managed by this CLI");
    println!("  daemon clear-all                                  - Stop all managed daemons and attempt to kill external ones");
    println!("  rest start [--listen-port <port>]                 - Start the REST API server");
    println!("  rest stop [--port <port>]                         - Stop the REST API server");
    println!("  rest status [--port <port>]                       - Check the status of the REST API server");
    println!("  rest health                                       - Perform a health check on the REST API server");
    println!("  rest version                                      - Get the version of the REST API server");
    println!("  rest register-user <username> <password>          - Register a new user via REST API");
    println!("  rest authenticate <username> <password>           - Authenticate a user and get a token via REST API");
    println!("  rest graph-query \"<query_string>\" [persist]       - Execute a graph query via REST API");
    println!("  rest storage-query                                - Execute a storage query via REST API (placeholder)");
    println!("  storage start [--storage-port <port>] [--config-file <path>] [--cluster <range>] [--join-cluster <range>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] - Start the standalone Storage daemon");
    println!("  storage stop [--port <port>]                      - Stop the standalone Storage daemon");
    println!("  storage status [--port <port>]                    - Check the status of the standalone Storage daemon");
    println!("  status [rest|daemon|storage|cluster|all] [--port <port>] - Get a comprehensive status summary or specific component status");
    println!("  auth <username> <password>                        - Authenticate a user and get a token (top-level)");
    println!("  authenticate <username> <password>                - Authenticate a user and get a token (top-level)");
    println!("  register <username> <password>                    - Register a new user (top-level)");
    println!("  version                                           - Get the version of the REST API server (top-level)");
    println!("  health                                            - Perform a health check on the REST API server (top-level)");
    println!("  restart all [--port <port>] [--cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] - Restart all core GraphDB components");
    println!("  restart rest [--listen-port <port>]               - Restart the REST API server");
    println!("  restart storage [--storage-port <port>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] - Restart the standalone Storage daemon");
    println!("  restart daemon [--port <port>] [--cluster <range>] [--join-cluster <range>] - Restart a GraphDB daemon");
    println!("  restart cluster                                   - Restart cluster configuration (placeholder)");
    println!("  clear | clean                                     - Clear the terminal screen");
    println!("  help [--command|-c <command_string>]              - Display this help message or help for a specific command");
    println!("  exit | quit | q                                   - Exit the CLI");
    println!("\nNote: Commands like 'view-graph', 'index-node', etc., are placeholders and not yet implemented.");
}

/// Prints help messages filtered by a command string for interactive mode.
pub fn print_interactive_filtered_help(_cmd: &mut clap::Command, command_filter: &str) {
    let commands = [
        ("start rest [--listen-port <port>]", "Start the REST API server"),
        ("start storage [--storage-port <port>] [--config-file <path>] [--cluster <range>] [--join-cluster <range>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>]", "Start the standalone Storage daemon"),
        ("start daemon [--port <port>] [--cluster <range>] [--join-cluster <range>]", "Start a GraphDB daemon instance"),
        ("start all [--port <port>] [--cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>]", "Start all core GraphDB components"),
        ("start", "Start all core GraphDB components (default)"),
        ("stop rest [--port <port>]", "Stop the REST API server"),
        ("stop daemon [--port <port>]", "Stop a GraphDB daemon"),
        ("stop storage [--port <port>]", "Stop the standalone Storage daemon"),
        ("stop all", "Stop all core GraphDB components"),
        ("stop", "Stop all GraphDB components (default)"),
        ("daemon start [--port <port>] [--cluster <range>] [--join-cluster <range>]", "Start a GraphDB daemon"),
        ("daemon stop [--port <port>]", "Stop a GraphDB daemon"),
        ("daemon status [--port <port>]", "Check status of a GraphDB daemon"),
        ("daemon list", "List daemons managed by this CLI"),
        ("daemon clear-all", "Stop all managed daemons and attempt to kill external ones"),
        ("rest start [--listen-port <port>]", "Start the REST API server"),
        ("rest stop [--port <port>]", "Stop the REST API server"),
        ("rest status [--port <port>]", "Check the status of the REST API server"),
        ("rest health", "Perform a health check on the REST API server"),
        ("rest version", "Get the version of the REST API server"),
        ("rest register-user <username> <password>", "Register a new user via REST API"),
        ("rest authenticate <username> <password>", "Authenticate a user and get a token via REST API"),
        ("rest graph-query \"<query_string>\" [persist]", "Execute a graph query via REST API"),
        ("rest storage-query", "Execute a storage query via REST API (placeholder)"),
        ("storage start [--storage-port <port>] [--config-file <path>] [--cluster <range>] [--join-cluster <range>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>]", "Start the standalone Storage daemon"),
        ("storage stop [--port <port>]", "Stop the standalone Storage daemon"),
        ("storage status [--port <port>]", "Check the status of the standalone Storage daemon"),
        ("status rest", "Get detailed status of the REST API component"),
        ("status daemon [--port <port>]", "Get detailed status of a specific daemon or list common ones"),
        ("status storage [--port <port>]", "Get detailed status of the Storage component"),
        ("status cluster", "Get status of the cluster (placeholder)"),
        ("status all", "Get a comprehensive status summary of all GraphDB components"),
        ("status", "Get a comprehensive status summary of all GraphDB components (default)"),
        ("auth <username> <password>", "Authenticate a user and get a token"),
        ("authenticate <username> <password>", "Authenticate a user and get a token"),
        ("register <username> <password>", "Register a new user"),
        ("version", "Get the version of the REST API server"),
        ("health", "Perform a health check on the REST API server"),
        ("restart all [--port <port>] [--cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>]", "Restart all core GraphDB components"),
        ("restart rest [--listen-port <port>]", "Restart the REST API server"),
        ("restart storage [--storage-port <port>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>]", "Restart the standalone Storage daemon"),
        ("restart daemon [--port <port>] [--cluster <range>] [--join-cluster <range>]", "Restart a GraphDB daemon"),
        ("restart cluster", "Restart cluster configuration (placeholder)"),
        ("clear", "Clear the terminal screen"),
        ("clean", "Clear the terminal screen"),
        ("help [--command|-c <command_string>]", "Display this help message or help for a specific command"),
        ("exit | quit | q", "Exit the CLI"),
    ];

    let filter_lower = command_filter.to_lowercase();
    let mut found_match = false;

    println!("\n--- Help for '{}' ---", command_filter);
    for (command_syntax, description) in commands.iter() {
        if command_syntax.to_lowercase() == filter_lower ||
           (command_syntax.to_lowercase().starts_with(&filter_lower) && filter_lower.contains(' ')) {
            println!("  {:<80} - {}", command_syntax, description);
            found_match = true;
        } else if description.to_lowercase().contains(&filter_lower) {
            println!("  {:<80} - {}", command_syntax, description);
            found_match = true;
        }
    }

    if !found_match {
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
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let history_path = "graphdb_cli_history.txt";
    let _ = rl.load_history(history_path);

    handlers::print_welcome_screen();

    loop {
        let readline = rl.readline("GraphDB> ");
        match readline {
            Ok(line) => {
                let line_trim = line.trim();
                if line_trim.is_empty() {
                    continue;
                }

                rl.add_history_entry(line_trim).ok();

                let args = match shlex::split(line_trim) {
                    Some(a) => a,
                    None => {
                        eprintln!("Error: Malformed input. Please check quoting.");
                        continue;
                    }
                };
                
                if args.is_empty() {
                    continue;
                }

                let (command, _parsed_args) = parse_command(&args);

                if command == CommandType::Exit {
                    handle_interactive_command(
                        command,
                        &args,
                        daemon_handles.clone(),
                        rest_api_shutdown_tx_opt.clone(),
                        rest_api_port_arc.clone(),
                        rest_api_handle.clone(),
                        storage_daemon_shutdown_tx_opt.clone(),
                        storage_daemon_handle.clone(),
                        storage_daemon_port_arc.clone(),
                    ).await?;
                    break;
                }
                
                let daemon_handles_clone = Arc::clone(&daemon_handles);
                let rest_api_shutdown_tx_opt_clone = Arc::clone(&rest_api_shutdown_tx_opt);
                let rest_api_port_arc_clone = Arc::clone(&rest_api_port_arc);
                let rest_api_handle_clone = Arc::clone(&rest_api_handle);
                let storage_daemon_shutdown_tx_opt_clone = Arc::clone(&storage_daemon_shutdown_tx_opt);
                let storage_daemon_handle_clone = Arc::clone(&storage_daemon_handle);
                let storage_daemon_port_arc_clone = Arc::clone(&storage_daemon_port_arc);

                handle_interactive_command(
                    command,
                    &args,
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
                println!("Ctrl-C received. Type 'exit' to quit or Ctrl-D to terminate.");
            }
            Err(ReadlineError::Eof) => {
                println!("Ctrl-D received. Exiting GraphDB CLI. Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error reading line: {:?}", err);
                break;
            }
        }
    }

    rl.save_history(&history_path).context("Failed to save history")?;

    Ok(())
}

/// Handler for CLI commands in interactive mode.
/// This function dispatches interactive commands to the appropriate handlers in the `handlers` module.
pub async fn handle_interactive_command(
    command: CommandType,
    args: &[String],
    daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    match command {
        CommandType::Daemon(daemon_cmd) => {
            handlers::handle_daemon_command_interactive(daemon_cmd, daemon_handles).await?;
        }
        CommandType::Rest(rest_cmd) => {
            handlers::handle_rest_command_interactive(rest_cmd, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
        }
        CommandType::Storage(storage_action) => {
            handlers::handle_storage_command_interactive(storage_action, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
        }
        CommandType::StartRest { port } => {
            handlers::start_rest_api_interactive(port, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
        }
        CommandType::StartStorage { port, config_file, cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type } => {
            handlers::handle_storage_command_interactive(
                StorageAction::Start {
                    port,
                    config_file,
                    cluster,
                    data_directory,
                    log_directory,
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type,
                },
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
        }
        CommandType::StartDaemon { port, cluster } => {
            handlers::start_daemon_instance_interactive(port, cluster, daemon_handles).await?;
        }
        CommandType::StartAll { port, cluster, listen_port, storage_port, storage_config_file, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, config_file } => {
            handlers::handle_start_all_interactive(
                port,
                cluster,
                listen_port,
                storage_port,
                storage_config_file,
                data_directory,
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            ).await?;
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
            ).await?;
        }
        CommandType::StopRest => {
            handlers::stop_rest_api_interactive(rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
        }
        CommandType::StopDaemon(port) => {
            handlers::stop_daemon_instance_interactive(port, daemon_handles).await?;
        }
        CommandType::StopStorage(port) => {
            handlers::stop_storage_interactive(port, storage_daemon_shutdown_tx_opt.clone(), storage_daemon_handle.clone(), storage_daemon_port_arc.clone()).await?;
        }
        CommandType::StatusSummary => {
            handlers::display_full_status_summary(rest_api_port_arc.clone(), storage_daemon_port_arc.clone()).await;
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
        CommandType::RestartAll { port, cluster, listen_port, storage_port, storage_config_file, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type, config_file } => {
            let restart_args = RestartArgs {
                action: Some(RestartAction::All {
                    port,
                    cluster: cluster.clone(), // Added .clone()
                    config_file: config_file.clone(), // Added .clone()
                    listen_port,
                    storage_port,
                    storage_config_file: storage_config_file.clone(), // Added .clone()
                    data_directory: data_directory.clone(), // Added .clone()
                    log_directory: log_directory.clone(), // Added .clone()
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type: storage_engine_type.clone(), // Added .clone()
                    daemon: Some(true),
                    rest: Some(true),
                    storage: Some(true),
                }),
                // Initialize all top-level fields of RestartArgs with values from CommandType
                port,
                cluster,
                config_file,
                listen_port,
                storage_port,
                storage_config_file,
                data_directory,
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                daemon: Some(true), // Top-level daemon flag for RestartArgs
                rest: Some(true),   // Top-level rest flag for RestartArgs
                storage: Some(true), // Top-level storage flag for RestartArgs
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
            ).await?;
        }
        CommandType::RestartRest { port } => {
            let restart_args = RestartArgs {
                action: Some(RestartAction::Rest {
                    port,
                    cluster: None, // As per commands.rs, RestartAction::Rest has cluster
                    daemon: Some(false), // Rest restart typically doesn't restart daemon
                    storage: Some(false), // Rest restart typically doesn't restart storage
                }),
                // Initialize all top-level fields of RestartArgs
                port,
                cluster: None,
                config_file: None,
                listen_port: None,
                storage_port: None,
                storage_config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: Some(false), // Top-level daemon flag
                rest: Some(true),   // Top-level rest flag
                storage: Some(false),
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
            ).await?;
        }
        CommandType::RestartStorage { port, config_file, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type } => {
            let restart_args = RestartArgs {
                action: Some(RestartAction::Storage {
                    port,
                    config_file: config_file.clone(), // Added .clone()
                    cluster: None, // As per commands.rs, RestartAction::Storage has cluster
                    data_directory: data_directory.clone(), // Added .clone()
                    log_directory: log_directory.clone(), // Added .clone()
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type: storage_engine_type.clone(), // Added .clone()
                    daemon: Some(false), // Storage restart typically doesn't restart daemon
                    rest: Some(false),   // Storage restart typically doesn't restart rest
                }),
                // Initialize all top-level fields of RestartArgs
                port,
                cluster: None,
                config_file,
                listen_port: None,
                storage_port: None,
                storage_config_file: None, // Not directly passed here, so None
                data_directory,
                log_directory,
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type,
                daemon: Some(false), // Top-level daemon flag
                rest: Some(false),   // Top-level rest flag
                storage: Some(true), // Top-level storage flag
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
            ).await?;
        }
        CommandType::RestartDaemon { port, cluster } => {
            let restart_args = RestartArgs {
                action: Some(RestartAction::Daemon {
                    port,
                    cluster: cluster.clone(), // Added .clone()
                    daemon: Some(true), // Daemon restart implies daemon is true
                    rest: Some(false), // Daemon restart typically doesn't restart rest
                    storage: Some(false), // Daemon restart typically doesn't restart storage
                }),
                // Initialize all top-level fields of RestartArgs
                port,
                cluster,
                config_file: None,
                listen_port: None,
                storage_port: None,
                storage_config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: Some(true), // Top-level daemon flag
                rest: Some(false),   // Top-level rest flag
                storage: Some(false),
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
            ).await?;
        }
        CommandType::RestartCluster => {
            let restart_args = RestartArgs {
                action: Some(RestartAction::Cluster), // Unit variant
                // Initialize all top-level fields of RestartArgs
                port: None,
                cluster: None,
                config_file: None,
                listen_port: None,
                storage_port: None,
                storage_config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: Some(true), // A cluster restart usually implies restarting all components
                rest: Some(true),
                storage: Some(true),
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
            ).await?;
        }
        CommandType::Help(help_args) => {
            if let Some(command_filter) = help_args.filter_command {
                let mut cmd = CliArgs::command();
                print_interactive_filtered_help(&mut cmd, &command_filter);
            } else {
                print_interactive_help();
            }
        }
        CommandType::Clear => {
            handlers::clear_terminal_screen().await?;
            handlers::print_welcome_screen();
        }
        CommandType::Exit => {
            println!("Exiting GraphDB CLI. Goodbye!");
            process::exit(0);
        }
        CommandType::Unknown => {
            eprintln!("Unknown command: {}. Type 'help' for a list of commands.", args.join(" "));
            let mut cmd = CliArgs::command();
            print_interactive_filtered_help(&mut cmd, &args.join(" "));
        }
    }
    Ok(())
}

/// Collects all CLI elements for suggestions, used in filtered help.
fn collect_all_cli_elements_for_suggestions(
    cmd: &clap::Command,
    prefix: &mut Vec<String>,
    all_elements: &mut HashSet<String>,
) {
    let cmd_name = cmd.get_name().to_string();
    prefix.push(cmd_name.clone());
    all_elements.insert(prefix.join(" "));

    for subcmd in cmd.get_subcommands() {
        collect_all_cli_elements_for_suggestions(subcmd, prefix, all_elements);
    }

    for opt in cmd.get_arguments() {
        if let Some(long) = opt.get_long() {
            all_elements.insert(format!("--{}", long));
        }
        if let Some(short) = opt.get_short() {
            all_elements.insert(format!("-{}", short));
        }
    }

    prefix.pop();
}

/// Prints the default Clap-generated help message.
fn print_help_clap_generated() {
    let mut cmd = CliArgs::command();
    cmd.print_help().expect("Failed to print help");
}

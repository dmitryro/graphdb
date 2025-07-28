// server/src/cli/interactive.rs

// This file handles the interactive CLI mode, including command parsing
// and displaying interactive help messages.

use anyhow::{Result, Context, anyhow};
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
use shlex; // Import shlex for robust argument splitting

// Import necessary items from sibling modules
use crate::cli::cli::CliArgs; // Import CliArgs from cli.rs
use crate::cli::commands::{
    CommandType, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs,
    ReloadArgs, ReloadAction, StartAction, RestartArgs, RestartAction,
    HelpArgs // Ensure HelpArgs is imported from commands.rs if it's there, or define locally if not.
};
// Assuming these are defined in handlers.rs or help_display.rs as per previous context
use crate::cli::handlers;
use crate::cli::help_display::{
    print_interactive_help, print_interactive_filtered_help, collect_all_cli_elements_for_suggestions,
    print_help_clap_generated, print_filtered_help_clap_generated
};


// --- Levenshtein Distance Calculation ---
// Helper function to calculate Levenshtein distance between two strings
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();

    let m = s1_chars.len();
    let n = s2_chars.len();

    if m == 0 { return n; }
    if n == 0 { return m; }

    let mut dp = vec![vec![0; n + 1]; m + 1];

    for i in 0..=m {
        dp[i][0] = i;
    }
    for j in 0..=n {
        dp[0][j] = j;
    }

    for i in 1..=m {
        for j in 1..=n {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1) // deletion
                .min(dp[i][j - 1] + 1) // insertion
                .min(dp[i - 1][j - 1] + cost); // substitution
        }
    }
    dp[m][n]
}


/// Parses a command string from the interactive CLI input.
/// This function now expects a `Vec<String>` (from shlex) as input.
pub fn parse_command(parts: &[String]) -> (CommandType, Vec<String>) {
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    let command_str = parts[0].to_lowercase();
    let remaining_args: Vec<String> = parts[1..].to_vec(); // Arguments after the main command

    // List of common top-level commands for fuzzy matching
    let top_level_commands = vec![
        "start", "stop", "status", "auth", "authenticate", "register",
        "version", "health", "reload", "restart", "clear", "help", "exit",
        "daemon", "rest", "storage", // These are often subcommands but can also be typed first.
        "quit", "q", "clean" // Aliases
    ];

    // Define a threshold for Levenshtein distance for a "suggestion"
    const FUZZY_MATCH_THRESHOLD: usize = 2; // e.g., 'sta' vs 'start' (2 diff)

    let mut cmd_type = CommandType::Unknown; // Initialize with Unknown
    let mut parsed_remaining_args = remaining_args.clone(); // Clone to modify if arguments are consumed

    // Try exact match first
    match command_str.as_str() {
        "exit" | "quit" | "q" => cmd_type = CommandType::Exit,
        "clear" | "clean" => cmd_type = CommandType::Clear,
        "version" => cmd_type = CommandType::Version,
        "health" => cmd_type = CommandType::Health,
        "status" => {
            if remaining_args.is_empty() {
                cmd_type = CommandType::StatusSummary;
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "summary" | "all" => cmd_type = CommandType::StatusSummary,
                    "rest" => {
                        let port = remaining_args.get(1).and_then(|s| s.parse().ok());
                        cmd_type = CommandType::StatusRest(port);
                    },
                    "daemon" => {
                        let port = remaining_args.get(1).and_then(|s| s.parse().ok());
                        cmd_type = CommandType::StatusDaemon(port);
                    },
                    "storage" => {
                        let port = remaining_args.get(1).and_then(|s| s.parse().ok());
                        cmd_type = CommandType::StatusStorage(port);
                    },
                    "cluster" => cmd_type = CommandType::StatusCluster,
                    _ => { /* remains Unknown */ }
                }
            }
        },
        "start" => {
            if remaining_args.is_empty() {
                // If "start" is typed alone, assume "start all" with no specific args
                cmd_type = CommandType::StartAll {
                    port: None, cluster: None, listen_port: None, storage_port: None,
                    storage_config_file: None, daemon_cluster: None, daemon_port: None,
                    rest_cluster: None, rest_port: None, storage_cluster: None,
                };
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "rest" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 1; // Start parsing from the argument after "rest"

                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start rest': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::StartRest { port, cluster };
                    },
                    "storage" => {
                        let mut port = None;
                        let mut config_file = None;
                        let mut cluster = None;
                        let mut i = 1; // Start parsing from the argument after "storage"

                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < remaining_args.len() {
                                        config_file = Some(PathBuf::from(&remaining_args[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start storage': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::StartStorage { port, config_file, cluster };
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 1; // Start parsing from the argument after "daemon"

                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start daemon': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::StartDaemon { port, cluster };
                    },
                    "all" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut listen_port = None;
                        let mut storage_port = None;
                        let mut storage_config_file = None;
                        let mut daemon_cluster = None;
                        let mut daemon_port = None;
                        let mut rest_cluster = None;
                        let mut rest_port = None;
                        let mut storage_cluster = None;
                        let mut i = 1; // Start parsing from the argument after "all"

                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < remaining_args.len() {
                                        listen_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-config" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_config_file = Some(PathBuf::from(&remaining_args[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-port" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-port" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start all': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::StartAll {
                            port, cluster, listen_port, storage_port, storage_config_file,
                            daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
                        };
                    },
                    _ => { /* remains Unknown */ }
                }
            }
        },
        "stop" => {
            if remaining_args.is_empty() {
                cmd_type = CommandType::StopAll;
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "rest" => cmd_type = CommandType::StopRest,
                    "daemon" => {
                        let port = remaining_args.get(1).and_then(|s| s.parse().ok());
                        cmd_type = CommandType::StopDaemon(port);
                    },
                    "storage" => {
                        let port = remaining_args.get(1).and_then(|s| s.parse().ok());
                        cmd_type = CommandType::StopStorage(port);
                    },
                    "all" => cmd_type = CommandType::StopAll,
                    _ => { /* remains Unknown */ }
                }
            }
        },
        "reload" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: reload [all|rest|storage|daemon|cluster] [--port <port>]");
                // cmd_type remains Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "all" => cmd_type = CommandType::ReloadAll,
                    "rest" => cmd_type = CommandType::ReloadRest,
                    "storage" => cmd_type = CommandType::ReloadStorage,
                    "daemon" => {
                        let port = remaining_args.get(1).and_then(|s| s.parse().ok());
                        cmd_type = CommandType::ReloadDaemon(port);
                    },
                    "cluster" => cmd_type = CommandType::ReloadCluster,
                    _ => { /* remains Unknown */ }
                }
            }
        },
        "restart" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: restart [all|rest|storage|daemon|cluster] ...");
                // cmd_type remains Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "all" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut listen_port = None;
                        let mut storage_port = None;
                        let mut storage_config_file = None;
                        let mut daemon_cluster = None;
                        let mut daemon_port = None;
                        let mut rest_cluster = None;
                        let mut rest_port = None;
                        let mut storage_cluster = None;
                        let mut i = 1; // Start parsing from the argument after "all"

                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < remaining_args.len() {
                                        listen_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-config" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_config_file = Some(PathBuf::from(&remaining_args[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-port" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-port" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart all': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::RestartAll {
                            port, cluster, listen_port, storage_port, storage_config_file,
                            daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
                        };
                    },
                    "rest" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart rest': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::RestartRest { port, cluster };
                    },
                    "storage" => {
                        let mut port = None;
                        let mut config_file = None;
                        let mut cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < remaining_args.len() {
                                        config_file = Some(PathBuf::from(&remaining_args[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart storage': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::RestartStorage { port, config_file, cluster };
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart daemon': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        cmd_type = CommandType::RestartDaemon { port, cluster };
                    },
                    "cluster" => cmd_type = CommandType::RestartCluster,
                    _ => { /* remains Unknown */ }
                }
            }
        },
        "auth" | "authenticate" => {
            if remaining_args.len() >= 2 {
                cmd_type = CommandType::Auth { username: remaining_args[0].clone(), password: remaining_args[1].clone() };
            } else {
                eprintln!("Usage: auth/authenticate <username> <password>");
                // cmd_type remains Unknown
            }
        },
        "register" => {
            if remaining_args.len() >= 2 {
                cmd_type = CommandType::RegisterUser { username: remaining_args[0].clone(), password: remaining_args[1].clone() };
            } else {
                eprintln!("Usage: register <username> <password>");
                // cmd_type remains Unknown
            }
        },
        "help" => {
            let mut filter_command: Option<String> = None;
            let mut command_path: Vec<String> = Vec::new();
            let mut i = 0;

            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--filter" | "-f" | "--command" | "-c" => {
                        if i + 1 < remaining_args.len() {
                            filter_command = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    },
                    _ => {
                        command_path.push(remaining_args[i].clone());
                        i += 1;
                    }
                }
            }

            let help_args = HelpArgs { filter_command, command_path };
            cmd_type = CommandType::Help(help_args);
        },
        // Direct subcommand calls (e.g., "daemon list" where "daemon" is the first arg)
        "daemon" => {
            if remaining_args.first().map_or(false, |s| s.to_lowercase() == "list") {
                cmd_type = CommandType::Daemon(DaemonCliCommand::List);
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "clear-all") {
                cmd_type = CommandType::Daemon(DaemonCliCommand::ClearAll);
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "start") {
                let mut port = None;
                let mut cluster = None;
                let mut i = 1; // Start after "start"
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--port" | "-p" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        _ => { i += 1; }
                    }
                }
                cmd_type = CommandType::Daemon(DaemonCliCommand::Start { port, cluster });
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "stop") {
                let port = remaining_args.get(1).and_then(|s| s.parse::<u16>().ok());
                cmd_type = CommandType::Daemon(DaemonCliCommand::Stop { port });
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "status") {
                let port = remaining_args.get(1).and_then(|s| s.parse::<u16>().ok());
                cmd_type = CommandType::Daemon(DaemonCliCommand::Status { port });
            }
            else { /* remains Unknown */ }
        },
        "rest" => {
            // Declare port and cluster here, so they are in scope for all RestCliCommand variants
            let mut port: Option<u16> = None;
            let mut cluster: Option<String> = None;

            // Parse arguments for common flags first, regardless of subcommand
            let mut i = 0;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--port" | "-p" | "--listen-port" => {
                        if i + 1 < remaining_args.len() {
                            port = remaining_args[i + 1].parse::<u16>().ok();
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--cluster" => {
                        if i + 1 < remaining_args.len() {
                            cluster = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    _ => { i += 1; } // Move to the next argument if not a common flag
                }
            }

            // Now, parse the specific subcommand
            if remaining_args.first().map_or(false, |s| s.to_lowercase() == "start") {
                cmd_type = CommandType::Rest(RestCliCommand::Start { port, cluster });
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "stop") {
                cmd_type = CommandType::Rest(RestCliCommand::Stop);
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "status") {
                // 'port' is now in scope from the declaration above
                cmd_type = CommandType::Rest(RestCliCommand::Status { port });
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "health") {
                cmd_type = CommandType::Rest(RestCliCommand::Health);
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "version") {
                cmd_type = CommandType::Rest(RestCliCommand::Version);
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "register-user") {
                // ... (rest of your existing logic for register-user)
                if remaining_args.len() >= 3 { // "register-user username password"
                    cmd_type = CommandType::Rest(RestCliCommand::RegisterUser {
                        username: remaining_args[1].clone(),
                        password: remaining_args[2].clone(),
                    });
                } else {
                    eprintln!("Usage: rest register-user <username> <password>");
                    // cmd_type remains Unknown
                }
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "authenticate") {
                // ... (rest of your existing logic for authenticate)
                if remaining_args.len() >= 3 { // "authenticate username password"
                    cmd_type = CommandType::Rest(RestCliCommand::Authenticate {
                        username: remaining_args[1].clone(),
                        password: remaining_args[2].clone(),
                    });
                } else {
                    eprintln!("Usage: rest authenticate <username> <password>");
                    // cmd_type remains Unknown
                }
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "graph-query") {
                // ... (rest of your existing logic for graph-query)
                if remaining_args.len() >= 2 { // "graph-query <query_string> [persist]"
                    let query_string = remaining_args[1].clone();
                    let persist = remaining_args.get(2).and_then(|s| s.parse::<bool>().ok());
                    cmd_type = CommandType::Rest(RestCliCommand::GraphQuery { query_string, persist });
                } else {
                    eprintln!("Usage: rest graph-query <query_string> [persist]");
                    // cmd_type remains Unknown
                }
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "storage-query") {
                cmd_type = CommandType::Rest(RestCliCommand::StorageQuery);
            }
            else { /* remains Unknown */ }
        },
        "storage" => {
            if remaining_args.first().map_or(false, |s| s.to_lowercase() == "start") {
                let mut port = None;
                let mut config_file = None;
                let mut cluster = None;
                let mut i = 1; // Start after "start"
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--port" | "-p" | "--storage-port" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--config-file" => {
                            if i + 1 < remaining_args.len() {
                                config_file = Some(PathBuf::from(&remaining_args[i + 1]));
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        _ => { i += 1; }
                    }
                }
                cmd_type = CommandType::Storage(StorageAction::Start { port, config_file, cluster });
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "stop") {
                let port = remaining_args.get(1).and_then(|s| s.parse::<u16>().ok());
                cmd_type = CommandType::Storage(StorageAction::Stop { port });
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "status") {
                let port = remaining_args.get(1).and_then(|s| s.parse::<u16>().ok());
                cmd_type = CommandType::Storage(StorageAction::Status { port });
            }
            else { /* remains Unknown */ }
        },
        _ => { /* remains Unknown */ }
    }

    // Fuzzy matching for top-level commands if the initial match was Unknown
    if cmd_type == CommandType::Unknown {
        let mut best_match: Option<String> = None;
        let mut min_distance = usize::MAX;

        for cmd in &top_level_commands {
            let dist = levenshtein_distance(&command_str, cmd);
            if dist < min_distance {
                min_distance = dist;
                best_match = Some(cmd.to_string());
            }
        }

        if min_distance <= FUZZY_MATCH_THRESHOLD && best_match.is_some() {
            if let Some(suggestion) = best_match {
                eprintln!("Unknown command '{}'. Did you mean '{}'?", command_str, suggestion);
            }
        }
    }

    (cmd_type, parsed_remaining_args) // Return the determined command type and the (potentially modified) remaining args
}


/// Handler for CLI commands in interactive mode.
/// This function dispatches interactive commands to the appropriate handlers in the `handlers` module.
#[allow(clippy::too_many_arguments)]
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
            handlers::handle_daemon_command_interactive(daemon_cmd, daemon_handles).await
        }
        CommandType::Rest(rest_cmd) => {
            handlers::handle_rest_command_interactive(
                rest_cmd,
                rest_api_shutdown_tx_opt,
                rest_api_handle,
                rest_api_port_arc,
            )
            .await
        }
        CommandType::Storage(storage_action) => {
            handlers::handle_storage_command_interactive(
                storage_action,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::StartRest { port, cluster } => {
            handlers::start_rest_api_interactive(
                port,
                cluster,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            )
            .await
        }
        CommandType::StartStorage {
            port,
            config_file,
            cluster,
        } => {
            handlers::start_storage_interactive(
                port,
                config_file,
                cluster,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await
        }
        CommandType::StartDaemon { port, cluster } => {
            handlers::start_daemon_instance_interactive(port, cluster, daemon_handles).await
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
            handlers::handle_start_all_interactive(
                port.or(daemon_port),
                cluster.or(daemon_cluster),
                listen_port.or(rest_port),
                rest_cluster,
                storage_port,
                storage_cluster,
                storage_config_file,
                daemon_handles,
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
                storage_daemon_shutdown_tx_opt,
                storage_daemon_handle,
                storage_daemon_port_arc,
            )
            .await
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
            .await
        }
        CommandType::StopRest => {
            handlers::stop_rest_api_interactive(
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            )
            .await
        }
        CommandType::StopDaemon(port) => {
            handlers::stop_daemon_instance_interactive(port, daemon_handles).await
        }
        CommandType::StopStorage(port) => {
            handlers::stop_storage_interactive(
                port,
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::StatusSummary => {
            handlers::display_full_status_summary(
                rest_api_port_arc.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::StatusRest(port) => {
            handlers::display_rest_api_status(port, rest_api_port_arc.clone()).await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::StatusDaemon(port) => {
            handlers::display_daemon_status(port).await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::StatusStorage(port) => {
            handlers::display_storage_daemon_status(port, storage_daemon_port_arc.clone()).await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::StatusCluster => {
            handlers::display_cluster_status().await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::Auth { username, password } => {
            handlers::authenticate_user(username, password).await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::Authenticate { username, password } => {
            handlers::authenticate_user(username, password).await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::RegisterUser { username, password } => {
            handlers::register_user(username, password).await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::Version => {
            handlers::display_rest_api_version().await;
            Ok(()) // Ensure a Result is returned
        }
        CommandType::Health => {
            handlers::display_rest_api_health().await;
            Ok(()) // Ensure a Result is returned
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
            .await
        }
        CommandType::ReloadRest => {
            handlers::reload_rest_interactive(
                rest_api_shutdown_tx_opt,
                rest_api_port_arc,
                rest_api_handle,
            )
            .await
        }
        CommandType::ReloadStorage => {
            handlers::reload_storage_interactive(
                storage_daemon_shutdown_tx_opt.clone(),
                storage_daemon_handle.clone(),
                storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::ReloadDaemon(port) => {
            handlers::reload_daemon_interactive(port).await
        }
        CommandType::ReloadCluster => {
            handlers::reload_cluster_interactive().await
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
            .await
        }
        CommandType::RestartRest { port, cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Rest { port, cluster },
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
            .await
        }
        CommandType::RestartStorage {
            port,
            config_file,
            cluster,
        } => {
            let restart_args = RestartArgs {
                action: RestartAction::Storage {
                    port,
                    config_file,
                    cluster,
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
            .await
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
            .await
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
            .await
        }
        CommandType::Clear => {
            handlers::clear_terminal_screen().await?;
            handlers::print_welcome_screen();
            Ok(()) // Ensure a Result is returned
        }
        CommandType::Help(help_args) => {
            let mut cmd = CliArgs::command(); // Get the clap Command
            if let Some(command_filter) = help_args.filter_command {
                print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else if !help_args.command_path.is_empty() {
                let command_filter = help_args.command_path.join(" ");
                print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else {
                print_help_clap_generated();
            }
            Ok(()) // Ensure a Result is returned
        }
        CommandType::Exit => {
            // This is handled by the main loop breaking, no further action here.
            Ok(())
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
            Ok(()) // Ensure a Result is returned
        }
    }
}


// --- Main asynchronous loop for the CLI interactive mode. ---
#[allow(clippy::too_many_arguments)]
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

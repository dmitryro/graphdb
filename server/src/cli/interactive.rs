// server/src/cli/interactive.rs
// This file handles the interactive CLI mode, including command parsing
// and displaying interactive help messages.
// ADDED: 2025-07-31 - Added parsing and handling for new fields `daemon_port`, `daemon_cluster`, `rest_port`, `rest_cluster`, `storage_port`, `storage_cluster` in CommandType variants (StartRest, StartStorage, StartDaemon, RestartRest, RestartStorage, RestartDaemon) and subcommands (DaemonCliCommand::Start, RestCliCommand::Start, StorageAction::Start) to align with commands.rs (artifact ID 2caecb52-0a6c-46b0-a6b6-42c1b23388a1). Updated patterns and initializers to include these fields, resolving E0027 and E0063 errors at lines 331-333, 601, 643, 675, 757, 864, 943, 1030, 1040, 1055, 1234, 1250, 1274.
// UPDATED: 2025-07-31 - Fixed E0382 (use of moved value) for `rest_cluster`, `storage_cluster`, `daemon_cluster` by adding `.clone()` in `.or()` calls at lines 331, 337, 344, 635, 697, 749, 851, 978, 1077, 1369, 1394, 1413. Removed redundant `--storage-port` case at line 673 to fix unreachable pattern warning. Removed unused `cmd_type` assignment at line 87 to fix unused variable warning.
// ADDED: 2025-08-08 - Added parsing and handling for `use storage <engine>` and `use plugin [--enable <bool>]` commands in `parse_command`, mapping to `CommandType::UseStorage` and `CommandType::UsePlugin`. Added handling for `StatusRaft(Option<u16>)` in `parse_command` and `handle_interactive_command`. Ensured `StorageEngineType` is imported.
// UPDATED: 2025-08-08 - Fixed E0599 and E0308 for `UsePlugin`. In `parse_command`, set `enable` to `unwrap_or(true)` for `CommandType::UsePlugin`. In `handle_interactive_command`, removed redundant `unwrap_or(true)` since `enable` is now a `bool`.
// UPDATED: 2025-08-08 - Updated `parse_command` to handle `StorageEngineType` variants `sled`, `rocksdb`, `inmemory`, `redis`, `postgresql`, `mysql` in `use storage` command, aligning with updated `StorageEngineType` enum.
// UPDATED: 2025-08-08 - Fixed E0603 by ensuring `StorageEngineType` is imported from `crate::cli::commands`, which re-exports from `crate::cli::config`.
// ADDED: 2025-08-09 - Added parsing for `save storage` and `save config`/`save configuration` in `parse_command`, mapping to `CommandType::SaveStorage` and `CommandType::SaveConfig`. Updated `handle_interactive_command` to handle `CommandType::SaveStorage` and `CommandType::SaveConfig` to fix E0004. Updated `use storage` parsing to handle `--permanent` flag, setting `permanent` boolean for `CommandType::UseStorage`.
// ADDED: 2025-08-31 - Added parsing for `kv`, `exec`, `query`, and `unified` commands in `parse_command`, supporting flagless and flagged arguments (`--operation`, `--key`, `--value` for `kv`; `--command` for `exec`; `--query`, `--language` for `query` and `unified`). Updated `handle_interactive_command` to handle `CommandType::Kv`, `CommandType::Exec`, `CommandType::Query`, and `CommandType::Unified` by calling `handlers::handle_kv_command`, `handle_exec_command`, `handle_query_command`, and `handle_unified_query`. Added `query_engine` to `SharedState` and `run_cli_interactive`.
// UPDATED: 2025-08-31 - Fixed E0308 by reordering arguments in `handle_kv_command`, `handle_exec_command`, `handle_query_command`, and `handle_unified_query` calls to place `query_engine` first. Fixed E0308 for `listen_port` by removing redundant `Some` wrapping in `parse_command`.

use anyhow::{Result, Context, anyhow};
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, history::DefaultHistory};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use std::process;
use std::path::PathBuf;
use clap::CommandFactory;
use std::collections::HashSet;
use shlex;
use log::{info, error, warn, debug};
use crate::cli::cli::CliArgs;
use crate::cli::commands::{
    CommandType, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs,
    ReloadArgs, ReloadAction, StartAction, RestartArgs, RestartAction, HelpArgs, ShowAction,
    ConfigAction, parse_kv_operation, KvAction,
};
use crate::cli::handlers;
use crate::cli::help_display::{
    print_interactive_help, print_interactive_filtered_help, collect_all_cli_elements_for_suggestions,
    print_help_clap_generated, print_filtered_help_clap_generated
};
use crate::cli::handlers_utils::{parse_show_command};
pub use lib::storage_engine::config::{StorageEngineType};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;

struct SharedState {
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
    query_engine: Arc<QueryExecEngine>,
}

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
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[m][n]
}

pub fn parse_command(parts: &[String]) -> (CommandType, Vec<String>) {
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    let command_str = parts[0].to_lowercase();
    let remaining_args: Vec<String> = parts[1..].to_vec();

    let top_level_commands = vec![
        "start", "stop", "status", "auth", "authenticate", "register",
        "version", "health", "reload", "restart", "clear", "help", "exit",
        "daemon", "rest", "storage", "use", "quit", "q", "clean", "save", "show",
        "kv", "exec", "query", "unified",
    ];

    const FUZZY_MATCH_THRESHOLD: usize = 2;

    let cmd_type;
    let parsed_remaining_args = remaining_args.clone();

    cmd_type = match command_str.as_str() {
        "exit" | "quit" | "q" => CommandType::Exit,
        "clear" | "clean" => CommandType::Clear,
        "version" => CommandType::Version,
        "health" => CommandType::Health,
        "save" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: save [storage|config|configuration]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "storage" => CommandType::SaveStorage,
                    "config" | "configuration" => CommandType::SaveConfig,
                    _ => {
                        eprintln!("Usage: save [storage|config|configuration]");
                        CommandType::Unknown
                    }
                }
            }
        },
        "status" => {
            if remaining_args.is_empty() {
                CommandType::StatusSummary
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "summary" | "all" => CommandType::StatusSummary,
                    "rest" => {
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
                                _ => { i += 1; }
                            }
                        }
                        CommandType::Rest(RestCliCommand::Status { port, cluster })
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
                                _ => { i += 1; }
                            }
                        }
                        CommandType::Daemon(DaemonCliCommand::Status { port, cluster })
                    },
                    "storage" => {
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
                                _ => { i += 1; }
                            }
                        }
                        CommandType::Storage(StorageAction::Status { port, cluster })
                    },
                    "cluster" => CommandType::StatusCluster,
                    "raft" => {
                        let mut port = None;
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
                                _ => { i += 1; }
                            }
                        }
                        CommandType::StatusRaft(port)
                    },
                    _ => CommandType::Unknown,
                }
            }
        },
        "start" => {
            let mut port: Option<u16> = None;
            let mut cluster: Option<String> = None;
            let mut daemon_port: Option<u16> = None;
            let mut daemon_cluster: Option<String> = None;
            let mut listen_port: Option<u16> = None;
            let mut rest_port: Option<u16> = None;
            let mut rest_cluster: Option<String> = None;
            let mut storage_port: Option<u16> = None;
            let mut storage_cluster: Option<String> = None;
            let mut storage_config_file: Option<PathBuf> = None;

            let mut current_subcommand_index = 0;
            let mut explicit_subcommand: Option<String> = None;

            if !remaining_args.is_empty() {
                match remaining_args[0].to_lowercase().as_str() {
                    "all" | "daemon" | "rest" | "storage" => {
                        explicit_subcommand = Some(remaining_args[0].to_lowercase());
                        current_subcommand_index = 1;
                    }
                    _ => {
                        current_subcommand_index = 0;
                    }
                }
            }

            let mut i = current_subcommand_index;
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
                    "--listen-port" => {
                        if i + 1 < remaining_args.len() {
                            listen_port = remaining_args[i + 1].parse::<u16>().ok();
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
                    "--storage-port" => {
                        if i + 1 < remaining_args.len() {
                            storage_port = remaining_args[i + 1].parse::<u16>().ok();
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
                    "--storage-config" | "--config-file" => {
                        if i + 1 < remaining_args.len() {
                            storage_config_file = Some(PathBuf::from(remaining_args[i + 1].clone()));
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    _ => {
                        eprintln!("Warning: Unknown argument for 'start': {}", remaining_args[i]);
                        i += 1;
                    }
                }
            }

            match explicit_subcommand.as_deref() {
                Some("all") => CommandType::StartAll {
                    port, cluster, daemon_port, daemon_cluster,
                    listen_port, rest_port, rest_cluster,
                    storage_port, storage_cluster, storage_config_file,
                },
                Some("daemon") => CommandType::StartDaemon {
                    port: daemon_port.or(port),
                    cluster: daemon_cluster.clone().or(cluster),
                    daemon_port,
                    daemon_cluster,
                },
                Some("rest") => CommandType::StartRest {
                    port: rest_port.or(listen_port).or(port),
                    cluster: rest_cluster.clone().or(cluster),
                    rest_port,
                    rest_cluster,
                },
                Some("storage") => CommandType::StartStorage {
                    port: storage_port.or(port),
                    config_file: storage_config_file,
                    cluster: storage_cluster.clone().or(cluster),
                    storage_port,
                    storage_cluster,
                },
                None => CommandType::StartAll {
                    port, cluster, daemon_port, daemon_cluster,
                    listen_port, rest_port, rest_cluster,
                    storage_port, storage_cluster, storage_config_file,
                },
                _ => CommandType::Unknown,
            }
        },
        "stop" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() == "all" {
                CommandType::StopAll
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "rest" => {
                        let mut port = None;
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
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'stop rest': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StopRest(port)
                    }
                    "daemon" => {
                        let mut port = None;
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
                                _ => { i += 1; }
                            }
                        }
                        CommandType::StopDaemon(port)
                    }
                    "storage" => {
                        let mut port = None;
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
                                _ => { i += 1; }
                            }
                        }
                        CommandType::StopStorage(port)
                    }
                    "all" => CommandType::StopAll,
                    _ => CommandType::Unknown,
                }
            }
        },
        "reload" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: reload [all|rest|storage|daemon|cluster] [--port <port>]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "all" => CommandType::ReloadAll,
                    "rest" => CommandType::ReloadRest,
                    "storage" => CommandType::ReloadStorage,
                    "daemon" => {
                        let mut port = None;
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
                                _ => { i += 1; }
                            }
                        }
                        CommandType::ReloadDaemon(port)
                    },
                    "cluster" => CommandType::ReloadCluster,
                    _ => CommandType::Unknown,
                }
            }
        },
        "restart" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: restart [all|rest|storage|daemon|cluster] ...");
                CommandType::Unknown
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
                        CommandType::RestartAll {
                            port, cluster, listen_port, storage_port, storage_config_file,
                            daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
                        }
                    },
                    "rest" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut rest_port = None;
                        let mut rest_cluster = None;
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
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart rest': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartRest { port: rest_port.or(port), cluster: rest_cluster.clone().or(cluster), rest_port, rest_cluster }
                    },
                    "storage" => {
                        let mut port = None;
                        let mut config_file = None;
                        let mut cluster = None;
                        let mut storage_port = None;
                        let mut storage_cluster = None;
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
                                "--storage-port" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_port = remaining_args[i + 1].parse::<u16>().ok();
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
                                    eprintln!("Warning: Unknown argument for 'restart storage': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartStorage { port: storage_port.or(port), config_file, cluster: storage_cluster.clone().or(cluster), storage_port, storage_cluster }
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut daemon_port = None;
                        let mut daemon_cluster = None;
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
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart daemon': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartDaemon { port: daemon_port.or(port), cluster: daemon_cluster.clone().or(cluster), daemon_port, daemon_cluster }
                    },
                    "cluster" => CommandType::RestartCluster,
                    _ => CommandType::Unknown,
                }
            }
        },
        "auth" | "authenticate" => {
            if remaining_args.len() >= 2 {
                CommandType::Authenticate { username: remaining_args[0].clone(), password: remaining_args[1].clone() }
            } else {
                eprintln!("Usage: auth/authenticate <username> <password>");
                CommandType::Unknown
            }
        },
        "register" => {
            if remaining_args.len() >= 2 {
                CommandType::RegisterUser { username: remaining_args[0].clone(), password: remaining_args[1].clone() }
            } else {
                eprintln!("Usage: register <username> <password>");
                CommandType::Unknown
            }
        },
        "use" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: use [storage <engine> [--permanent]|plugin [--enable <bool>]]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "storage" => {
                        if remaining_args.len() < 2 {
                            eprintln!("Usage: use storage <engine> [--permanent]");
                            CommandType::Unknown
                        } else {
                            let engine = match remaining_args[1].to_lowercase().as_str() {
                                "sled" => StorageEngineType::Sled,
                                "rocksdb" | "rocks-db" => StorageEngineType::RocksDB,
                                "inmemory" | "in-memory" => StorageEngineType::InMemory,
                                "redis" => StorageEngineType::Redis,
                                "postgres" | "postgresql" | "postgre-sql" => StorageEngineType::PostgreSQL,
                                "mysql" | "my-sql" => StorageEngineType::MySQL,
                                _ => {
                                    eprintln!(
                                        "Unknown storage engine: {}. Supported: sled, rocksdb, rocks-db, inmemory, in-memory, redis, postgres, postgresql, postgre-sql, mysql, my-sql",
                                        remaining_args[1]
                                    );
                                    return (CommandType::Unknown, parsed_remaining_args);
                                }
                            };
                            let mut permanent = true; // Default to true as per commands.rs
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--permanent" => {
                                        permanent = true;
                                        i += 1;
                                    }
                                    _ => {
                                        eprintln!("Warning: Unknown argument for 'use storage': {}", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                            }
                            CommandType::UseStorage { engine, permanent }
                        }
                    },
                    "plugin" => {
                        let mut enable = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--enable" => {
                                    if i + 1 < remaining_args.len() {
                                        enable = Some(remaining_args[i + 1].parse::<bool>().ok().unwrap_or_default());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '--enable' requires a boolean value.");
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'use plugin': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::UsePlugin { enable: enable.unwrap_or(true) }
                    },
                    _ => {
                        eprintln!("Usage: use [storage <engine> [--permanent]|plugin [--enable <bool>]]");
                        CommandType::Unknown
                    }
                }
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
            CommandType::Help(help_args)
        },
        "daemon" => {
            if remaining_args.first().map_or(false, |s| s.to_lowercase() == "list") {
                CommandType::Daemon(DaemonCliCommand::List)
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "clear-all") {
                CommandType::Daemon(DaemonCliCommand::ClearAll)
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "start") {
                let mut port = None;
                let mut cluster = None;
                let mut daemon_port = None;
                let mut daemon_cluster = None;
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
                        _ => { i += 1; }
                    }
                }
                CommandType::Daemon(DaemonCliCommand::Start { port: daemon_port.or(port), cluster: daemon_cluster.clone().or(cluster), daemon_port, daemon_cluster })
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "stop") {
                let mut port = None;
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
                        _ => { i += 1; }
                    }
                }
                CommandType::Daemon(DaemonCliCommand::Stop { port })
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "status") {
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
                        _ => { i += 1; }
                    }
                }
                CommandType::Daemon(DaemonCliCommand::Status { port, cluster })
            }
            else { CommandType::Unknown }
        },
        "rest" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: rest <start|stop|status|health|version|register-user|authenticate|graph-query|storage-query>");
                CommandType::Unknown
            } else {
                let rest_subcommand = remaining_args[0].to_lowercase();
                let mut port: Option<u16> = None;
                let mut cluster: Option<String> = None;
                let mut rest_port: Option<u16> = None;
                let mut rest_cluster: Option<String> = None;
                let mut query_string: Option<String> = None;
                let mut persist: Option<bool> = None;
                let mut username: Option<String> = None;
                let mut password: Option<String> = None;

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
                        "--persist" => {
                            persist = Some(true);
                            i += 1;
                        }
                        _ => {
                            if rest_subcommand == "register-user" || rest_subcommand == "authenticate" {
                                if username.is_none() {
                                    username = Some(remaining_args[i].clone());
                                } else if password.is_none() {
                                    password = Some(remaining_args[i].clone());
                                }
                            } else if rest_subcommand == "graph-query" {
                                if query_string.is_none() {
                                    query_string = Some(remaining_args[i].clone());
                                }
                            }
                            i += 1;
                        }
                    }
                }

                match rest_subcommand.as_str() {
                    "start" => CommandType::Rest(RestCliCommand::Start { port: rest_port.or(port), cluster: rest_cluster.clone().or(cluster), rest_port, rest_cluster }),
                    "stop" => CommandType::Rest(RestCliCommand::Stop { port } ),
                    "status" => CommandType::Rest(RestCliCommand::Status { port, cluster }),
                    "health" => CommandType::Rest(RestCliCommand::Health),
                    "version" => CommandType::Rest(RestCliCommand::Version),
                    "register-user" => {
                        if let (Some(u), Some(p)) = (username, password) {
                            CommandType::Rest(RestCliCommand::RegisterUser { username: u, password: p })
                        } else {
                            eprintln!("Usage: rest register-user <username> <password>");
                            CommandType::Unknown
                        }
                    }
                    "authenticate" => {
                        if let (Some(u), Some(p)) = (username, password) {
                            CommandType::Rest(RestCliCommand::Authenticate { username: u, password: p })
                        } else {
                            eprintln!("Usage: rest authenticate <username> <password>");
                            CommandType::Unknown
                        }
                    }
                    "graph-query" => {
                        if let Some(q) = query_string {
                            CommandType::Rest(RestCliCommand::GraphQuery { query_string: q, persist })
                        } else {
                            eprintln!("Usage: rest graph-query <query_string> [--persist]");
                            CommandType::Unknown
                        }
                    }
                    "storage-query" => CommandType::Rest(RestCliCommand::StorageQuery),
                    _ => CommandType::Unknown,
                }
            }
        },
        "show" => {
            match parse_show_command(parts) {
                Ok(command) => command,
                Err(e) => {
                    eprintln!("Error parsing show command: {}", e);
                    return (CommandType::Unknown, vec![]);
                }
            }
        },
        "storage" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: storage <start|stop|status|health|version|storage-query>");
                CommandType::Unknown
            } else {
                let storage_subcommand = remaining_args[0].to_lowercase();
                let mut port = None;
                let mut config_file = None;
                let mut cluster = None;
                let mut storage_port = None;
                let mut storage_cluster = None;

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
                        "--config-file" => {
                            if i + 1 < remaining_args.len() {
                                config_file = Some(PathBuf::from(remaining_args[i + 1].clone()));
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
                        "--storage-port" => {
                            if i + 1 < remaining_args.len() {
                                storage_port = remaining_args[i + 1].parse::<u16>().ok();
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
                        _ => { i += 1; }
                    }
                }

                match storage_subcommand.as_str() {
                    "start" => CommandType::Storage(StorageAction::Start { port: storage_port.or(port), config_file, cluster: storage_cluster.clone().or(cluster), storage_port, storage_cluster }),
                    "stop" => CommandType::Storage(StorageAction::Stop { port }),
                    "status" => CommandType::Storage(StorageAction::Status { port, cluster }),
                    "health" => CommandType::Storage(StorageAction::Health),
                    "version" => CommandType::Storage(StorageAction::Version),
                    "storage-query" => CommandType::Storage(StorageAction::StorageQuery),
                    _ => CommandType::Unknown,
                }
            }
        },
        // The updated match arm for "kv"
"kv" => {
    let mut key = None;
    let mut value = None;
    let mut operation = None;
    let mut i = 0;
    
    // First, check for the operation which can be positional or flagged
    if let Some(op_str) = remaining_args.get(0) {
        if !op_str.starts_with('-') {
            operation = Some(op_str.clone());
            i += 1;
        }
    }
    
    // Now, parse the remaining arguments for flags and values
    while i < remaining_args.len() {
        match remaining_args[i].as_str() {
            "--key" => {
                if i + 1 < remaining_args.len() {
                    key = Some(remaining_args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '--key' requires a value.");
                    return (CommandType::Unknown, remaining_args);
                }
            }
            "--value" => {
                if i + 1 < remaining_args.len() {
                    value = Some(remaining_args[i + 1].clone());
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '--value' requires a value.");
                    return (CommandType::Unknown, remaining_args);
                }
            }
            // Handle positional arguments after the operation
            _ => {
                if operation.is_some() {
                    // Positional key
                    if key.is_none() {
                        key = Some(remaining_args[i].clone());
                        i += 1;
                    }
                    // Positional value (only for "set")
                    else if value.is_none() && operation.as_deref() == Some("set") {
                        value = Some(remaining_args[i].clone());
                        i += 1;
                    } else {
                        eprintln!("Warning: Unrecognized argument: {}", remaining_args[i]);
                        return (CommandType::Unknown, remaining_args);
                    }
                } else {
                    eprintln!("Warning: Unrecognized argument: {}", remaining_args[i]);
                    return (CommandType::Unknown, remaining_args);
                }
            }
        }
    }
    
    // Validate and construct the command based on parsed values
    if operation.is_none() {
        eprintln!("Usage: kv <get|set|delete> <key> [<value>]");
        return (CommandType::Unknown, remaining_args);
    }
    
    let op_str = operation.unwrap();
    let action = match op_str.to_lowercase().as_str() {
        "get" => {
            if let Some(k) = key {
                KvAction::Get { key: k }
            } else {
                eprintln!("Missing key for 'kv get' command.");
                return (CommandType::Unknown, remaining_args);
            }
        }
        "set" => {
            if let (Some(k), Some(v)) = (key, value) {
                KvAction::Set { key: k, value: v }
            } else {
                eprintln!("Missing key or value for 'kv set' command.");
                return (CommandType::Unknown, remaining_args);
            }
        }
        "delete" => {
            if let Some(k) = key {
                KvAction::Delete { key: k }
            } else {
                eprintln!("Missing key for 'kv delete' command.");
                return (CommandType::Unknown, remaining_args);
            }
        }
        _ => {
            eprintln!("Invalid KV operation: {}. Supported: get, set, delete", op_str);
            return (CommandType::Unknown, remaining_args);
        }
    };
    
    CommandType::Kv { action }
},




        "exec" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: exec <command> or exec --command <command>");
                CommandType::Unknown
            } else {
                let mut command = None;
                let mut i = 0;

                // Check for flagless syntax (e.g., exec <command>)
                if !remaining_args[0].starts_with('-') {
                    command = Some(remaining_args[0].clone());
                    i = remaining_args.len(); // Skip flag parsing
                }

                // Parse flagged arguments
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--command" => {
                            if i + 1 < remaining_args.len() {
                                command = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--command' requires a value.");
                                i += 1;
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for 'exec': {}", remaining_args[i]);
                            i += 1;
                        }
                    }
                }

                if let Some(cmd) = command {
                    CommandType::Exec { command: cmd }
                } else {
                    eprintln!("Missing command for 'exec' command. Usage: exec <command> or exec --command <command>");
                    CommandType::Unknown
                }
            }
        },
        "query" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: query <query> or query --query <query>");
                CommandType::Unknown
            } else {
                let mut query = None;
                let mut i = 0;

                // Check for flagless syntax (e.g., query <query>)
                if !remaining_args[0].starts_with('-') {
                    query = Some(remaining_args[0].clone());
                    i = remaining_args.len(); // Skip flag parsing
                }

                // Parse flagged arguments
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--query" => {
                            if i + 1 < remaining_args.len() {
                                query = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--query' requires a value.");
                                i += 1;
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for 'query': {}", remaining_args[i]);
                            i += 1;
                        }
                    }
                }

                if let Some(q) = query {
                    CommandType::Query { query: q }
                } else {
                    eprintln!("Missing query for 'query' command. Usage: query <query> or query --query <query>");
                    CommandType::Unknown
                }
            }
        },
        "unified" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: unified <query> [--language <language>]");
                CommandType::Unknown
            } else {
                let mut query = None;
                let mut language = None;
                let mut i = 0;

                // Check for flagless syntax (e.g., unified <query>)
                if !remaining_args[0].starts_with('-') {
                    query = Some(remaining_args[0].clone());
                    i = 1;
                }

                // Parse flagged arguments
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--query" => {
                            if i + 1 < remaining_args.len() {
                                query = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--query' requires a value.");
                                i += 1;
                            }
                        }
                        "--language" => {
                            if i + 1 < remaining_args.len() {
                                language = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--language' requires a value.");
                                i += 1;
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for 'unified': {}", remaining_args[i]);
                            i += 1;
                        }
                    }
                }

                if let Some(q) = query {
                    CommandType::Unified { query: q, language }
                } else {
                    eprintln!("Missing query for 'unified' command. Usage: unified <query> [--language <language>]");
                    CommandType::Unknown
                }
            }
        },
        _ => CommandType::Unknown,
    };

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

    (cmd_type, parsed_remaining_args)
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_interactive_command(
    command: CommandType,
    state: &SharedState,
) -> Result<()> {
    match command {
        CommandType::Daemon(daemon_cmd) => {
            handlers::handle_daemon_command_interactive(daemon_cmd, state.daemon_handles.clone()).await
        }
        CommandType::Rest(rest_cmd) => {
            match rest_cmd {
                RestCliCommand::Status { port, cluster: _ } => {
                    handlers::display_rest_api_status(port, state.rest_api_port_arc.clone()).await;
                    Ok(())
                },
                _ => handlers::handle_rest_command_interactive(
                    rest_cmd,
                    state.rest_api_shutdown_tx_opt.clone(),
                    state.rest_api_handle.clone(),
                    state.rest_api_port_arc.clone(),
                ).await,
            }
        }
        CommandType::Storage(storage_action) => {
            match storage_action {
                StorageAction::Status { port, cluster: _ } => {
                    handlers::display_storage_daemon_status(port, state.storage_daemon_port_arc.clone()).await;
                    Ok(())
                },
                _ => handlers::handle_storage_command_interactive(
                    storage_action,
                    state.storage_daemon_shutdown_tx_opt.clone(),
                    state.storage_daemon_handle.clone(),
                    state.storage_daemon_port_arc.clone(),
                ).await,
            }
        }
        CommandType::StartRest { port, cluster, .. } => {
            handlers::start_rest_api_interactive(
                port,
                cluster,
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            )
            .await
        }
        CommandType::StartStorage {
            port,
            config_file,
            cluster,
            ..
        } => {
            handlers::start_storage_interactive(
                port,
                config_file,
                None,
                cluster,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::StartDaemon { port, cluster, .. } => {
            handlers::start_daemon_instance_interactive(port, cluster, state.daemon_handles.clone()).await
        }
        CommandType::StartAll {
            port,
            cluster,
            daemon_port,
            daemon_cluster,
            listen_port,
            rest_port,
            rest_cluster,
            storage_port,
            storage_cluster,
            storage_config_file,
        } => {
            handlers::handle_start_all_interactive(
                daemon_port.or(port),
                daemon_cluster.or(cluster),
                rest_port.or(listen_port),
                rest_cluster,
                storage_port,
                storage_cluster,
                storage_config_file,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::StopAll => {
            handlers::stop_all_interactive(
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::StopRest(port) => {
            handlers::stop_rest_api_interactive(
                port,
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            )
            .await
        }
        CommandType::StopDaemon(port) => {
            handlers::stop_daemon_instance_interactive(port, state.daemon_handles.clone()).await
        }
        CommandType::StopStorage(port) => {
            handlers::stop_storage_interactive(
                port,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::StatusSummary => {
            handlers::display_full_status_summary(
                state.rest_api_port_arc.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await;
            Ok(())
        }
        CommandType::StatusDaemon(port) => {
            handlers::display_daemon_status(port).await;
            Ok(())
        }
        CommandType::StatusStorage(port) => {
            handlers::display_storage_daemon_status(port, state.storage_daemon_port_arc.clone()).await;
            Ok(())
        }
        CommandType::StatusCluster => {
            handlers::display_cluster_status().await;
            Ok(())
        }
        CommandType::StatusRaft(port) => {
            handlers::display_raft_status(port).await;
            Ok(())
        }
        CommandType::Auth { username, password } => {
            handlers::authenticate_user(username, password).await;
            Ok(())
        }
        CommandType::Authenticate { username, password } => {
            handlers::authenticate_user(username, password).await;
            Ok(())
        }
        CommandType::RegisterUser { username, password } => {
            handlers::register_user(username, password).await;
            Ok(())
        }
        CommandType::UseStorage { engine, permanent } => {
            handlers::handle_use_storage_interactive(
                engine,
                permanent,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await?;
            Ok(())
        }
        CommandType::UsePlugin { enable } => {
            handlers::use_plugin(enable).await;
            Ok(())
        }
        CommandType::Show(action) => {
            match action {
                ShowAction::Storage => {
                    handlers::handle_show_storage_command().await?;
                }
                ShowAction::Plugins => {
                    handlers::handle_show_plugins_command().await?;
                }
                ShowAction::Config { config_type } => {
                    match config_type {
                        ConfigAction::All => {
                             handlers::handle_show_all_config_command().await?;
                        }
                        ConfigAction::Rest => {
                             handlers::handle_show_rest_config_command().await?;
                        }
                        ConfigAction::Storage => {
                             handlers::handle_show_storage_config_command().await?;
                        }
                        ConfigAction::Main => {
                             handlers::handle_show_main_config_command().await?;
                        }
                    }
                }
            }
            Ok(())
        }
        CommandType::Version => {
            handlers::display_rest_api_version().await;
            Ok(())
        }
        CommandType::Health => {
            handlers::display_rest_api_health().await;
            Ok(())
        }
        CommandType::ReloadAll => {
            handlers::reload_all_interactive(
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::ReloadRest => {
            handlers::reload_rest_interactive(
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            )
            .await
        }
        CommandType::ReloadStorage => {
            handlers::reload_storage_interactive(
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
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
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::RestartRest { port, cluster, rest_port, rest_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Rest { port: rest_port.or(port), cluster: rest_cluster.clone().or(cluster), rest_port, rest_cluster },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::RestartStorage {
            port,
            config_file,
            cluster,
            storage_port,
            storage_cluster,
        } => {
            let restart_args = RestartArgs {
                action: RestartAction::Storage {
                    port: storage_port.or(port),
                    config_file,
                    cluster: storage_cluster.clone().or(cluster),
                    storage_port,
                    storage_cluster,
                },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::RestartDaemon { port, cluster, daemon_port, daemon_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Daemon { port: daemon_port.or(port), cluster: daemon_cluster.clone().or(cluster), daemon_port, daemon_cluster },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::RestartCluster => {
            let restart_args = RestartArgs {
                action: RestartAction::Cluster,
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            )
            .await
        }
        CommandType::Clear => {
            handlers::clear_terminal_screen().await?;
            handlers::print_welcome_screen();
            Ok(())
        }
        CommandType::Help(help_args) => {
            let mut cmd = CliArgs::command();
            if let Some(command_filter) = help_args.filter_command {
                crate::cli::help_display::print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else if !help_args.command_path.is_empty() {
                let command_filter = help_args.command_path.join(" ");
                crate::cli::help_display::print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else {
                crate::cli::help_display::print_help_clap_generated();
            }
            Ok(())
        }
        CommandType::SaveStorage => { 
            handlers::handle_save_storage().await;
            Ok(())
        }
        CommandType::SaveConfig => {
            handlers::handle_save_config().await;
            Ok(())
        }
        CommandType::Exit => {
            Ok(())
        }
        CommandType::Kv { action } => {
            // Determine the operation, key, and value from the nested KvAction enum
            let (operation, key, value) = match action {
                KvAction::Get { key } => ("get".to_string(), key, None),
                KvAction::Set { key, value } => ("set".to_string(), key, Some(value)),
                KvAction::Delete { key } => ("delete".to_string(), key, None),
            };

            // Call the existing unified handler function with the extracted parameters
            handlers::handle_kv_command(state.query_engine.clone(), operation, key, value).await?;
            
            Ok(())
        }
        CommandType::Exec { command } => {
            handlers::handle_exec_command(state.query_engine.clone(), command).await?;
            Ok(())
        }
        CommandType::Query { query } => {
            handlers::handle_query_command(state.query_engine.clone(), query).await?;
            Ok(())
        }
        CommandType::Unified { query, language } => {
            handlers::handle_unified_query(state.query_engine.clone(), query, language).await?;
            Ok(())
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
            Ok(())
        }
    }
}


#[allow(clippy::too_many_arguments)]
pub async fn run_cli_interactive(
    daemon_handles: Arc<TokioMutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
    query_engine: Arc<QueryExecEngine>,
) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let history_path = "graphdb_cli_history.txt";
    let _ = rl.load_history(history_path);

    handlers::print_welcome_screen();

    let state = SharedState {
        daemon_handles,
        rest_api_shutdown_tx_opt,
        rest_api_port_arc,
        rest_api_handle,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc,
        query_engine,
    };

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
                debug!("Parsed command: {:?}", command);

                if command == CommandType::Exit {
                    handle_interactive_command(command, &state).await?;
                    break;
                }

                if let Err(e) = handle_interactive_command(command, &state).await {
                    eprintln!("Error executing command: {:?}", e);
                    debug!("Detailed error: {:#}", e);
                    continue;
                }
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
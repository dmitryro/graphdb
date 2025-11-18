// server/src/cli/interactive.rs
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
use crate::cli::cli::{CliArgs, get_query_engine_singleton};
use lib::commands::{
    CommandType, DaemonCliCommand, RestCliCommand, StorageAction, StatusArgs, StopArgs,
    ReloadArgs, ReloadAction, StartAction, RestartArgs, RestartAction, HelpArgs, ShowAction,
    ConfigAction, parse_kv_operation, parse_storage_engine, KvAction, MigrateAction,
    // NEW: Graph & Index domain actions
    GraphAction, IndexAction,
};
use crate::cli::handlers;
use crate::cli::help_display::{
    print_interactive_help, print_interactive_filtered_help, collect_all_cli_elements_for_suggestions,
    print_help_clap_generated, print_filtered_help_clap_generated
};
use crate::cli::handlers_utils::{parse_show_command};
pub use lib::config::{StorageEngineType};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;

struct SharedState {
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
    query_engine: Arc<TokioMutex<Option<Arc<QueryExecEngine>>>>,
}

// === Levenshtein distance for fuzzy matching ===
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let m = s1_chars.len();
    let n = s2_chars.len();
    if m == 0 { return n; }
    if n == 0 { return m; }
    let mut dp = vec![vec![0; n + 1]; m + 1];
    for i in 0..=m { dp[i][0] = i; }
    for j in 0..=n { dp[0][j] = j; }
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

// === Helper: Strip surrounding double quotes ===
fn strip_surrounding_quotes(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].to_owned()
    } else {
        trimmed.to_owned()
    }
}

// === RAW QUERY AUTO-DETECTION (SQL / Cypher / GraphQL) ===
fn detect_raw_query_language(line: &str) -> Option<(&'static str, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_ascii_uppercase();
    let lower = trimmed.to_ascii_lowercase();

    // ---- SQL ----
    // Only match full SQL patterns: SELECT, INSERT, UPDATE, WITH, or DELETE FROM
    if upper.starts_with("SELECT ")
        || upper.starts_with("INSERT ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("WITH ")
        || upper.starts_with("CREATE ")
        || upper.starts_with("DROP ")
        || upper.starts_with("ALTER ")
        || upper.contains(" DELETE FROM ")
        || upper.starts_with("DELETE FROM ")
    {
        return Some(("sql", trimmed.to_string()));
    }

    // ---- Cypher ----
    if lower.starts_with("match ")
        || lower.starts_with("create ")
        || lower.starts_with("merge ")
        || lower.starts_with("return ")
        || lower.starts_with("optional match ")
        || lower.starts_with("detach delete ")
        || lower.starts_with("remove ")
        || lower.starts_with("where ")
        || lower.starts_with("order by ")
        || lower.starts_with("limit ")
    {
        return Some(("cypher", trimmed.to_string()));
    }

    // ---- GraphQL ----
    if trimmed.starts_with('{')
        || lower.starts_with("query ")
        || lower.starts_with("mutation ")
    {
        return Some(("graphql", trimmed.to_string()));
    }

    None
}

// === Main command parser (raw queries are detected BEFORE parsing) ===
pub fn parse_command(parts: &[String]) -> (CommandType, Vec<String>) {
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    // Join back the parts – we need the original line for raw-query detection
    let full_line = parts.join(" ");

    // ---- RAW QUERY SHORT-CIRCUIT ----
    if let Some((lang, query)) = detect_raw_query_language(&full_line) {
        return (
            CommandType::Query {
                query,
                language: Some(lang.to_string()),
            },
            Vec::new(),
        );
    }

    // Normal CLI command parsing
    let command_str = parts[0].to_lowercase();
    let remaining_args = parts[1..].to_vec();

    let top_level_commands = vec![
        "start", "stop", "status", "auth", "authenticate", "register", "version", "health",
        "reload", "restart", "clear", "help", "exit", "daemon", "rest", "storage", "use",
        "quit", "q", "clean", "save", "show", "kv", "query", "exec", "migrate",
        // NEW: Add graph and index for tab-completion & fuzzy help
        "graph", "index",
    ];
    const FUZZY_MATCH_THRESHOLD: usize = 2;

    let cmd_type = match command_str.as_str() {
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
                    _ => { current_subcommand_index = 0; }
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
                    port, cluster, daemon_port, daemon_cluster, listen_port,
                    rest_port, rest_cluster, storage_port, storage_cluster, storage_config_file,
                },
                Some("daemon") => CommandType::StartDaemon {
                    port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                    cluster: daemon_cluster.clone().or(cluster.clone()),
                    daemon_port, daemon_cluster,
                },
                Some("rest") => CommandType::StartRest {
                    port: Some(rest_port.unwrap_or(listen_port.unwrap_or(port.unwrap_or_default()))),
                    cluster: rest_cluster.clone().or(cluster.clone()),
                    rest_port, rest_cluster,
                },
                Some("storage") => CommandType::StartStorage {
                    port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                    config_file: storage_config_file,
                    cluster: storage_cluster.clone().or(cluster.clone()),
                    storage_port, storage_cluster,
                },
                None => CommandType::StartAll {
                    port, cluster, daemon_port, daemon_cluster, listen_port,
                    rest_port, rest_cluster, storage_port, storage_cluster, storage_config_file,
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
                eprintln!("Usage: reload [all|rest|storage|daemon|cluster] [--port ]");
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
                        CommandType::RestartRest {
                            port: Some(rest_port.unwrap_or(port.unwrap_or_default())),
                            cluster: rest_cluster.clone().or(cluster.clone()),
                            rest_port, rest_cluster
                        }
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
                        CommandType::RestartStorage {
                            port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                            config_file,
                            cluster: storage_cluster.clone().or(cluster.clone()),
                            storage_port,
                            storage_cluster
                        }
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
                        CommandType::RestartDaemon {
                            port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                            cluster: daemon_cluster.clone().or(cluster.clone()),
                            daemon_port,
                            daemon_cluster
                        }
                    },
                    "cluster" => CommandType::RestartCluster,
                    _ => CommandType::Unknown,
                }
            }
        },
        "auth" | "authenticate" => {
            if remaining_args.len() >= 2 {
                CommandType::Authenticate {
                    username: remaining_args[0].clone(),
                    password: remaining_args[1].clone()
                }
            } else {
                eprintln!("Usage: auth/authenticate <username> <password>");
                CommandType::Unknown
            }
        },
        "register" => {
            if remaining_args.len() >= 2 {
                CommandType::RegisterUser {
                    username: remaining_args[0].clone(),
                    password: remaining_args[1].clone()
                }
            } else {
                eprintln!("Usage: register <username> <password>");
                CommandType::Unknown
            }
        },
        "use" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: use [storage [--permanent] [--migrate]|plugin [--enable <bool>]]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "storage" => {
                        if remaining_args.len() < 2 {
                            eprintln!("Usage: use storage <engine> [--permanent] [--migrate]");
                            CommandType::Unknown
                        } else {
                            let engine = match remaining_args[1].to_lowercase().as_str() {
                                "sled" => StorageEngineType::Sled,
                                "rocksdb" | "rocks-db" => StorageEngineType::RocksDB,
                                "inmemory" | "in-memory" => StorageEngineType::InMemory,
                                "redis" => StorageEngineType::Redis,
                                "tikv" => StorageEngineType::TiKV,
                                "postgres" | "postgresql" | "postgre-sql" => StorageEngineType::PostgreSQL,
                                "mysql" | "my-sql" => StorageEngineType::MySQL,
                                _ => {
                                    eprintln!(
                                        "Unknown storage engine: {}. Supported: sled, rocksdb, rocks-db, inmemory, in-memory, redis, postgres, postgresql, postgre-sql, mysql, my-sql",
                                        remaining_args[1]
                                    );
                                    return (CommandType::Unknown, remaining_args.clone());
                                }
                            };
                            let mut permanent = false;
                            let mut migrate = false;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--permanent" => { permanent = true; i += 1; }
                                    "--migrate" => { migrate = true; i += 1; }
                                    _ => {
                                        eprintln!("Warning: Unknown argument for 'use storage': {}", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                            }
                            CommandType::UseStorage { engine, permanent, migrate }
                        }
                    },
                    "plugin" => {
                        let mut enable = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--enable" => {
                                    if i + 1 < remaining_args.len() {
                                        enable = remaining_args[i + 1].parse::<bool>().ok();
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
                CommandType::Daemon(DaemonCliCommand::Start {
                    port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                    cluster: daemon_cluster.clone().or(cluster.clone()),
                    daemon_port,
                    daemon_cluster
                })
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
            } else {
                CommandType::Unknown
            }
        },
        "rest" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: rest <subcommand>");
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
                    "start" => CommandType::Rest(RestCliCommand::Start {
                        port: Some(rest_port.unwrap_or(port.unwrap_or_default())),
                        cluster: rest_cluster.clone().or(cluster.clone()),
                        rest_port,
                        rest_cluster
                    }),
                    "stop" => CommandType::Rest(RestCliCommand::Stop { port }),
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
                            eprintln!("Usage: rest graph-query <query> [--persist]");
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
                eprintln!("Usage: storage <subcommand>");
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
                    "start" => CommandType::Storage(StorageAction::Start {
                        port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                        config_file,
                        cluster: storage_cluster.clone().or(cluster.clone()),
                        storage_port,
                        storage_cluster
                    }),
                    "stop" => CommandType::Storage(StorageAction::Stop { port }),
                    "status" => CommandType::Storage(StorageAction::Status { port, cluster }),
                    "health" => CommandType::Storage(StorageAction::Health),
                    "version" => CommandType::Storage(StorageAction::Version),
                    "storage-query" => CommandType::Storage(StorageAction::StorageQuery),
                    _ => CommandType::Unknown,
                }
            }
        },
        "kv" => {
            let mut key = None;
            let mut value = None;
            let mut operation = None;
            let mut i = 0;
            if let Some(op_str) = remaining_args.get(0) {
                if !op_str.starts_with('-') {
                    operation = Some(op_str.clone());
                    i += 1;
                }
            }
            while i < remaining_args.len() {
                match remaining_args[i].as_str() {
                    "--key" => {
                        if i + 1 < remaining_args.len() {
                            key = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '--key' requires a value.");
                            return (CommandType::Unknown, remaining_args.clone());
                        }
                    }
                    "--value" => {
                        if i + 1 < remaining_args.len() {
                            value = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '--value' requires a value.");
                            return (CommandType::Unknown, remaining_args.clone());
                        }
                    }
                    _ => {
                        if operation.is_some() {
                            if key.is_none() {
                                key = Some(remaining_args[i].clone());
                                i += 1;
                            } else if value.is_none() && operation.as_deref() == Some("set") {
                                value = Some(remaining_args[i].clone());
                                i += 1;
                            } else {
                                eprintln!("Warning: Unrecognized argument: {}", remaining_args[i]);
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        } else {
                            eprintln!("Warning: Unrecognized argument: {}", remaining_args[i]);
                            return (CommandType::Unknown, remaining_args.clone());
                        }
                    }
                }
            }
            match operation {
                Some(op) => match parse_kv_operation(&op) {
                    Ok(op) => match op.as_str() {
                        "get" => {
                            if let Some(key) = key {
                                CommandType::Kv { action: KvAction::Get { key } }
                            } else {
                                eprintln!("Usage: kv get [--key <key>]");
                                CommandType::Unknown
                            }
                        }
                        "set" => {
                            if let (Some(key), Some(value)) = (key, value) {
                                CommandType::Kv { action: KvAction::Set { key, value } }
                            } else {
                                eprintln!("Usage: kv set [--key <key>] [--value <value>]");
                                CommandType::Unknown
                            }
                        }
                        "delete" => {
                            if let Some(key) = key {
                                CommandType::Kv { action: KvAction::Delete { key } }
                            } else {
                                eprintln!("Usage: kv delete [--key <key>]");
                                CommandType::Unknown
                            }
                        }
                        _ => {
                            eprintln!("Invalid KV operation: '{}'. Supported operations: get, set, delete", op);
                            CommandType::Unknown
                        }
                    },
                    Err(e) => {
                        eprintln!("{}", e);
                        CommandType::Unknown
                    }
                },
                None => {
                    eprintln!("Usage: kv [get|set|delete] [--key <key>] [--value <value>]");
                    CommandType::Unknown
                }
            }
        },
        "set" => {
            if remaining_args.len() >= 2 {
                CommandType::Kv {
                    action: KvAction::Set {
                        key: remaining_args[0].clone(),
                        value: remaining_args[1].clone()
                    }
                }
            } else {
                eprintln!("Usage: set <key> <value>");
                CommandType::Unknown
            }
        },
        "get" => {
            if remaining_args.len() >= 1 {
                CommandType::Kv {
                    action: KvAction::Get {
                        key: remaining_args[0].clone()
                    }
                }
            } else {
                eprintln!("Usage: get <key>");
                CommandType::Unknown
            }
        },
        "delete" => {
            if remaining_args.len() >= 1 {
                CommandType::Kv {
                    action: KvAction::Delete {
                        key: remaining_args[0].clone()
                    }
                }
            } else {
                eprintln!("Usage: delete <key>");
                CommandType::Unknown
            }
        },
        // === query / exec with quote stripping ===
        "query" | "exec" => {
            if remaining_args.is_empty() {
                eprintln!(
                    "Usage: {} <query> | {} \"<query>\" | {} --query <query> [--language <lang>]",
                    command_str, command_str, command_str
                );
                CommandType::Unknown
            } else {
                let mut query: Option<String> = None;
                let mut language: Option<String> = None;
                let mut i = 0;
                // 1. Positional argument (may be quoted)
                if i < remaining_args.len() && !remaining_args[i].starts_with('-') {
                    query = Some(strip_surrounding_quotes(&remaining_args[i]));
                    i = remaining_args.len();
                }
                // 2. Flag parsing
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--query" => {
                            if i + 1 < remaining_args.len() {
                                query = Some(strip_surrounding_quotes(&remaining_args[i + 1]));
                                i += 2;
                            } else {
                                eprintln!("Warning: '--query' requires a value.");
                                i += 1;
                            }
                        }
                        "--language" => {
                            if i + 1 < remaining_args.len() {
                                language = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: '--language' requires a value.");
                                i += 1;
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for '{}': {}", command_str, remaining_args[i]);
                            i += 1;
                        }
                    }
                }
                if let Some(q) = query {
                    CommandType::Query { query: q, language }
                } else {
                    eprintln!("Missing query for '{}' command.", command_str);
                    CommandType::Unknown
                }
            }
        },
        "migrate" => {
            if remaining_args.len() < 2 && !remaining_args.iter().any(|arg| arg == "--from" || arg == "--source" || arg == "--to" || arg == "--dest") {
                eprintln!("Usage: migrate [--port <port>] [--cluster <cluster>] [--from <engine>] [--to <engine>] [--source <engine>] [--dest <engine>]");
                CommandType::Unknown
            } else {
                let mut from_engine = None;
                let mut to_engine = None;
                let mut from = None;
                let mut to = None;
                let mut source = None;
                let mut dest = None;
                let mut port = None;
                let mut cluster = None;
                let mut i = 0;
                if i < remaining_args.len() && !remaining_args[i].starts_with("--") {
                    from_engine = parse_storage_engine(&remaining_args[i]).ok();
                    i += 1;
                }
                if i < remaining_args.len() && !remaining_args[i].starts_with("--") {
                    to_engine = parse_storage_engine(&remaining_args[i]).ok();
                    i += 1;
                }
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--from" => {
                            if i + 1 < remaining_args.len() {
                                from = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--from' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--to" => {
                            if i + 1 < remaining_args.len() {
                                to = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--to' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--source" => {
                            if i + 1 < remaining_args.len() {
                                source = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--source' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--dest" => {
                            if i + 1 < remaining_args.len() {
                                dest = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--dest' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--port" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--port' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--cluster' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for 'migrate': {}", remaining_args[i]);
                            i += 1;
                        }
                    }
                }
                let from_engine_final = from_engine.or(from).or(source);
                let to_engine_final = to_engine.or(to).or(dest);
                CommandType::Migrate(MigrateAction {
                    from, to, source, dest, from_engine_pos: from_engine_final, to_engine_pos: to_engine_final, port, cluster,
                })
            }
        }
        // === NEW: Graph Domain Commands ===
        "graph" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: graph <insert-person|medical|delete|load> [args...]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "insert-person" => {
                        let mut name = None;
                        let mut age = None;
                        let mut city = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].as_str() {
                                "name" | "name=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        name = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "age" | "age=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        age = val.parse::<i32>().ok();
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "city" | "city=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        city = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                _ => i += 1,
                            }
                        }
                        CommandType::Graph(GraphAction::InsertPerson { name, age, city })
                    }
                    "medical" => {
                        let mut patient_name = None;
                        let mut patient_age = None;
                        let mut diagnosis_code = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].as_str() {
                                "patient_name" | "patient_name=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        patient_name = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "patient_age" | "patient_age=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        patient_age = val.parse::<i32>().ok();
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "diagnosis_code" | "diagnosis_code=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        diagnosis_code = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                _ => i += 1,
                            }
                        }
                        CommandType::Graph(GraphAction::MedicalRecord { patient_name, patient_age, diagnosis_code })
                    }
                    "delete" => {
                        if remaining_args.len() > 1 {
                            CommandType::Graph(GraphAction::DeleteNode { id: remaining_args[1].clone() })
                        } else {
                            eprintln!("Usage: graph delete <node_id>");
                            CommandType::Unknown
                        }
                    }
                    "load" => {
                        if remaining_args.len() > 1 {
                            CommandType::Graph(GraphAction::LoadData { path: remaining_args[1].clone().into() })
                        } else {
                            eprintln!("Usage: graph load <path/to/data.json>");
                            CommandType::Unknown
                        }
                    }
                    _ => {
                        eprintln!("Unknown graph action: {}. Use insert-person, medical, delete, load", remaining_args[0]);
                        CommandType::Unknown
                    }
                }
            }
        }
        // === NEW: Index & Full-Text Search Commands ===
        "index" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: index <create|search|rebuild|list|stats> [args...]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "create" => {
                        if remaining_args.len() >= 3 {
                            CommandType::Index(IndexAction::Create {
                                label: remaining_args[1].clone(),
                                property: remaining_args[2].clone(),
                            })
                        } else {
                            eprintln!("Usage: index create <Label> <property>");
                            CommandType::Unknown
                        }
                    }
                    "search" => {
                        let mut query = None;
                        let mut top_k = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            if query.is_none() && !remaining_args[i].starts_with("--") {
                                query = Some(remaining_args[i].clone());
                                i += 1;
                            } else if remaining_args[i] == "--top" {
                                if i + 1 < remaining_args.len() {
                                    top_k = remaining_args[i + 1].parse::<usize>().ok();
                                    i += 2;
                                } else { i += 1; }
                            } else {
                                i += 1;
                            }
                        }
                        if let Some(q) = query {
                            // FIXED: Use 'term' instead of 'query' and 'top' instead of 'top_k'
                            CommandType::Index(IndexAction::Search { term: q, top: top_k })
                        } else {
                            eprintln!("Usage: index search \"query text\" [--top N]");
                            CommandType::Unknown
                        }
                    }
                    "rebuild" => CommandType::Index(IndexAction::Rebuild),
                    "list" => CommandType::Index(IndexAction::List),
                    "stats" => CommandType::Index(IndexAction::Stats),
                    _ => {
                        eprintln!("Unknown index action. Use: create, search, rebuild, list, stats");
                        CommandType::Unknown
                    }
                }
            }
        }
        _ => CommandType::Unknown,
    };

    // === Fuzzy suggestion ===
    if cmd_type == CommandType::Unknown && !parts.is_empty() {
        let mut best_match: Option<String> = None;
        let mut min_distance = usize::MAX;
        for cmd in &top_level_commands {
            let dist = levenshtein_distance(&command_str, cmd);
            if dist < min_distance {
                min_distance = dist;
                best_match = Some(cmd.to_string());
            }
        }
        if min_distance <= FUZZY_MATCH_THRESHOLD {
            if let Some(suggestion) = best_match {
                eprintln!("Unknown command '{}'. Did you mean '{}'?", command_str, suggestion);
            }
        }
    }
    (cmd_type, remaining_args)
}

// === Command handler ===
#[allow(clippy::too_many_arguments)]
pub async fn handle_interactive_command(
    command: CommandType,
    state: &SharedState,
) -> Result<()> {
    async fn ensure_query_engine(state: &SharedState) -> Result<Arc<QueryExecEngine>> {
        let mut guard = state.query_engine.lock().await;
        if let Some(engine) = guard.as_ref() {
            Ok(Arc::clone(engine))
        } else {
            let engine = get_query_engine_singleton().await?;
            *guard = Some(Arc::clone(&engine));
            Ok(engine)
        }
    }

    // === Query execution (raw or via query/exec) ===
    if let CommandType::Query { query, language } = command {
        let query_engine = ensure_query_engine(state).await?;
        let lang = language.as_deref().unwrap_or("").to_ascii_lowercase();
        let trimmed = query.trim();

        let detected_lang = if lang == "cypher"
            || trimmed.to_ascii_lowercase().starts_with("match ")
            || trimmed.to_ascii_lowercase().starts_with("create ")
            || trimmed.to_ascii_lowercase().starts_with("merge ")
        {
            "cypher"
        } else if lang == "sql"
            || trimmed.to_ascii_uppercase().starts_with("SELECT ")
            || trimmed.to_ascii_uppercase().starts_with("INSERT ")
            || trimmed.to_ascii_uppercase().starts_with("UPDATE ")
            || trimmed.to_ascii_uppercase().starts_with("DELETE ")
        {
            "sql"
        } else if lang == "graphql"
            || trimmed.starts_with('{')
            || trimmed.to_ascii_lowercase().starts_with("query ")
            || trimmed.to_ascii_lowercase().starts_with("mutation ")
        {
            "graphql"
        } else {
            // Final fallback
            if trimmed.to_ascii_lowercase().starts_with("match ")
                || trimmed.to_ascii_lowercase().starts_with("create ")
                || trimmed.to_ascii_lowercase().starts_with("merge ")
            {
                "cypher"
            } else if trimmed.to_ascii_uppercase().starts_with("SELECT ")
                || trimmed.to_ascii_uppercase().starts_with("INSERT ")
                || trimmed.to_ascii_uppercase().starts_with("UPDATE ")
                || trimmed.to_ascii_uppercase().starts_with("DELETE ")
            {
                "sql"
            } else if trimmed.starts_with('{')
                || trimmed.to_ascii_lowercase().starts_with("query ")
                || trimmed.to_ascii_lowercase().starts_with("mutation ")
            {
                "graphql"
            } else {
                eprintln!("Warning: Could not detect query language. Use --language cypher|sql|graphql");
                return Ok(());
            }
        };

        match detected_lang {
            "cypher" => {
                println!("[Cypher] {}", trimmed);
                match query_engine.execute_cypher(trimmed).await {
                    Ok(res) => println!("{}", res),
                    Err(e) => eprintln!("Cypher Error: {}", e),
                }
            }
            "sql" => {
                println!("[SQL] {}", trimmed);
                match query_engine.execute_sql(trimmed).await {
                    Ok(res) => println!("{}", res),
                    Err(e) => eprintln!("SQL Error: {}", e),
                }
            }
            "graphql" => {
                println!("[GraphQL]\n{}", trimmed);
                match query_engine.execute_graphql(trimmed).await {
                    Ok(res) => println!("{}", res),
                    Err(e) => eprintln!("GraphQL Error: {}", e),
                }
            }
            _ => unreachable!(),
        }
        return Ok(());
    }

    // === All other CLI commands (unchanged) ===
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
            ).await
        }
        CommandType::StartStorage { port, config_file, cluster, .. } => {
            handlers::start_storage_interactive(
                port,
                config_file,
                None,
                cluster,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::StartDaemon { port, cluster, .. } => {
            handlers::start_daemon_instance_interactive(port, cluster, state.daemon_handles.clone()).await
        }
        CommandType::StartAll {
            port, cluster, daemon_port, daemon_cluster, listen_port,
            rest_port, rest_cluster, storage_port, storage_cluster, storage_config_file,
        } => {
            handlers::handle_start_all_interactive(
                daemon_port.or(port), daemon_cluster.or(cluster),
                rest_port.or(listen_port), rest_cluster,
                storage_port, storage_cluster, storage_config_file,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
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
            ).await
        }
        CommandType::StopRest(port) => {
            handlers::stop_rest_api_interactive(
                port,
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            ).await
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
            ).await
        }
        CommandType::StatusSummary => {
            handlers::display_full_status_summary(
                state.rest_api_port_arc.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await;
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
        CommandType::UseStorage { engine, permanent, migrate } => {
            handlers::handle_use_storage_interactive(
                engine,
                permanent,
                migrate,
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
                ShowAction::Storage => { handlers::handle_show_storage_command().await?; }
                ShowAction::Plugins => { handlers::handle_show_plugins_command().await?; }
                ShowAction::Config { config_type } => {
                    match config_type {
                        ConfigAction::All => { handlers::handle_show_all_config_command().await?; }
                        ConfigAction::Rest => { handlers::handle_show_rest_config_command().await?; }
                        ConfigAction::Storage => { handlers::handle_show_storage_config_command().await?; }
                        ConfigAction::Main => { handlers::handle_show_main_config_command().await?; }
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
            ).await
        }
        CommandType::ReloadRest => {
            handlers::reload_rest_interactive(
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            ).await
        }
        CommandType::ReloadStorage => {
            handlers::reload_storage_interactive(
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::ReloadDaemon(port) => {
            handlers::reload_daemon_interactive(port).await
        }
        CommandType::ReloadCluster => {
            handlers::reload_cluster_interactive().await
        }
        CommandType::RestartAll {
            port, cluster, listen_port, storage_port, storage_config_file,
            daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
        } => {
            let restart_args = RestartArgs {
                action: RestartAction::All {
                    port, cluster, listen_port, storage_port, storage_config_file,
                    daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
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
            ).await
        }
        CommandType::RestartRest { port, cluster, rest_port, rest_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Rest {
                    port: Some(rest_port.unwrap_or(port.unwrap_or_default())),
                    cluster: rest_cluster.clone().or(cluster.clone()),
                    rest_port,
                    rest_cluster
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
            ).await
        }
        CommandType::RestartStorage { port, config_file, cluster, storage_port, storage_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Storage {
                    port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                    config_file,
                    cluster: storage_cluster.clone().or(cluster.clone()),
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
            ).await
        }
        CommandType::RestartDaemon { port, cluster, daemon_port, daemon_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Daemon {
                    port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                    cluster: daemon_cluster.clone().or(cluster.clone()),
                    daemon_port,
                    daemon_cluster
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
            ).await
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
            ).await
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
            let query_engine = ensure_query_engine(state).await?;
            let (operation, key, value) = match action {
                KvAction::Get { key } => ("get".to_string(), key, None),
                KvAction::Set { key, value } => ("set".to_string(), key, Some(value)),
                KvAction::Delete { key } => ("delete".to_string(), key, None),
            };
            handlers::handle_kv_command(query_engine, operation, key, value).await?;
            Ok(())
        }
        CommandType::Query { query, language } => {
            let query_engine = ensure_query_engine(state).await?;
            // === Language Detection ===
            let lang = language.as_deref().unwrap_or("").to_ascii_lowercase();
            let trimmed = query.trim_start();
            let detected_lang = if lang == "cypher"
                || trimmed.starts_with("MATCH ")
                || trimmed.starts_with("CREATE ")
                || trimmed.starts_with("MERGE ")
            {
                "cypher"
            } else if lang == "sql"
                || trimmed.to_ascii_uppercase().starts_with("SELECT ")
                || trimmed.to_ascii_uppercase().starts_with("INSERT ")
            {
                "sql"
            } else if lang == "graphql"
                || trimmed.starts_with('{')
                || trimmed.starts_with("query ")
                || trimmed.starts_with("mutation ")
            {
                "graphql"
            } else {
                // Auto-detect fallback
                if trimmed.to_ascii_lowercase().starts_with("match ")
                    || trimmed.to_ascii_lowercase().starts_with("create ")
                    || trimmed.to_ascii_lowercase().starts_with("merge ")
                {
                    "cypher"
                } else if trimmed
                    .to_ascii_uppercase()
                    .starts_with("SELECT ")
                    || trimmed.to_ascii_uppercase().starts_with("INSERT ")
                    || trimmed.to_ascii_uppercase().starts_with("UPDATE ")
                    || trimmed.to_ascii_uppercase().starts_with("DELETE ")
                {
                    "sql"
                } else if trimmed.starts_with('{')
                    || trimmed.starts_with("query ")
                    || trimmed.starts_with("mutation ")
                {
                    "graphql"
                } else {
                    eprintln!("Warning: Could not detect query language. Use --language cypher|sql|graphql");
                    return Ok(());
                }
            };
            // === Execution ===
            match detected_lang {
                "cypher" => {
                    println!("[Cypher] {}", query);
                    match query_engine.execute_cypher(&query).await {
                        Ok(res) => println!("{}", res),
                        Err(e) => eprintln!("Cypher Error: {}", e),
                    }
                }
                "sql" => {
                    println!("[SQL] {}", query);
                    match query_engine.execute_sql(&query).await {
                        Ok(res) => println!("{}", res),
                        Err(e) => eprintln!("SQL Error: {}", e),
                    }
                }
                "graphql" => {
                    println!("[GraphQL]\n{}", query);
                    match query_engine.execute_graphql(&query).await {
                        Ok(res) => println!("{}", res),
                        Err(e) => eprintln!("GraphQL Error: {}", e),
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
            Ok(())
        }
        CommandType::Migrate(action) => {
            let from_engine = action.from_engine_pos.or(action.from).or(action.source).ok_or_else(|| {
                anyhow::anyhow!("No source engine specified. Use positional argument or --from/--source.")
            })?;
            let to_engine = action.to_engine_pos.or(action.to).or(action.dest).ok_or_else(|| {
                anyhow::anyhow!("No destination engine specified. Use positional argument or --to/--dest.")
            })?;
            handlers::handle_migrate_interactive(
                from_engine,
                to_engine,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await?;
            Ok(())
        }
        CommandType::Graph(action) => {
            let engine = ensure_query_engine(state).await?;
            crate::cli::handlers_graph::handle_graph_command(engine, action).await?;
            Ok(())
        }
        CommandType::Index(action) => {
            let engine = ensure_query_engine(state).await?;
            crate::cli::handlers_index::handle_index_command(engine, action).await?;
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_cli_interactive(
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
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
        query_engine: Arc::new(TokioMutex::new(None)),
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

                // === RAW QUERY DETECTION (before shlex) ===
                if let Some((language, query)) = detect_raw_query_language(line_trim) {
                    let cmd = CommandType::Query {
                        query,                       // String
                        language: Some(language.into()), // &str → String
                    };
                    if let Err(e) = handle_interactive_command(cmd, &state).await {
                        eprintln!("Query error: {}", e);
                    }
                    continue;
                }

                // === NORMAL COMMAND PATH ===
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
                    eprintln!("Error: {}", e);
                    debug!("Full error: {:#}", e);
                }
            }

            Err(ReadlineError::Interrupted) => {
                println!("Ctrl-C received. Type 'exit' to quit.");
            }
            Err(ReadlineError::Eof) => {
                println!("Ctrl-D received. Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Readline error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history(&history_path)
        .context("Failed to save history")?;
    Ok(())
}

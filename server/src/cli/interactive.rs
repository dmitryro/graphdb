// server/src/cli/interactive.rs

// This file handles the interactive CLI mode, including command parsing
// and displaying interactive help messages.

use anyhow::{Result, anyhow, Context};
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, history::DefaultHistory};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use std::process;
use std::path::PathBuf;
use clap::{CommandFactory, Parser}; // Import Parser for CliArgs::parse_from
use shlex;
use console::Style; // Import Style for colored output
// Corrected import paths for cluster and storage_engine
use crate::cli::network_utilities::{validate_cluster_range};
use lib::storage_engine::config as config_mod;

// Import necessary items from sibling modules
use crate::cli::commands::{
    DaemonCliCommand, RestCliCommand, StorageAction,
    StatusArgs, StopArgs, RestartArgs, ReloadArgs,
    ClearDataArgs, RegisterUserArgs, AuthArgs, QueryArgs, GraphQueryArgs, HelpArgs,
    StartArgs, // Import StartArgs for unification
    StatusAction, StopAction, ReloadAction, RestartAction, StartAction, CommandType, // Import Action enums and CommandType
    CliCommand, // Import CliCommand for parsing
};
use crate::cli::config::{
    DEFAULT_DAEMON_PORT,
    DEFAULT_REST_API_PORT,
    DEFAULT_STORAGE_PORT,
};
use crate::cli::handlers;
use crate::cli::help_display as help_display_mod; // Use an alias for clarity

/// Main asynchronous loop for the CLI interactive mode.
#[allow(clippy::too_many_arguments)]
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
    let history_path = PathBuf::from(".graphdb_history");
    if rl.load_history(&history_path).is_err() {
        println!("No previous history.");
    }

    handlers::print_welcome_screen();

    loop {
        let readline = rl.readline("GraphDB> ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                rl.add_history_entry(input)?;

                let parts: Vec<String> = shlex::split(input).unwrap_or_else(|| {
                    input.split_whitespace().map(|s| s.to_string()).collect()
                });

                if parts.is_empty() {
                    continue;
                }

                // Parse the command using the custom parse_command function
                let (cmd, args) = parse_command(&parts);

                // Handle Exit command directly to break the loop
                if let CommandType::Exit = cmd {
                    println!("{}", Style::new().green().apply_to("Exiting..."));
                    process::exit(0);
                }

                // Clone arcs for passing to handle_interactive_command
                let daemon_handles_clone = Arc::clone(&daemon_handles);
                let rest_api_shutdown_tx_opt_clone = Arc::clone(&rest_api_shutdown_tx_opt);
                let rest_api_port_arc_clone = Arc::clone(&rest_api_port_arc);
                let rest_api_handle_clone = Arc::clone(&rest_api_handle);
                let storage_daemon_shutdown_tx_opt_clone = Arc::clone(&storage_daemon_shutdown_tx_opt);
                let storage_daemon_handle_clone = Arc::clone(&storage_daemon_handle);
                let storage_daemon_port_arc_clone = Arc::clone(&storage_daemon_port_arc);

                // Dispatch the parsed CommandType to the interactive handler
                if let Err(e) = handle_interactive_command(
                    cmd,
                    args,
                    daemon_handles_clone,
                    rest_api_shutdown_tx_opt_clone,
                    rest_api_port_arc_clone,
                    rest_api_handle_clone,
                    storage_daemon_shutdown_tx_opt_clone,
                    storage_daemon_handle_clone,
                    storage_daemon_port_arc_clone,
                ).await {
                    eprintln!("{}", Style::new().red().apply_to(format!("Command execution error: {}", e)));
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", Style::new().yellow().apply_to("^C"));
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("{}", Style::new().green().apply_to("Exiting..."));
                break;
            }
            Err(err) => {
                eprintln!("{}", Style::new().red().apply_to(format!("Error reading input: {}", err)));
                break;
            }
        }
    }

    rl.save_history(&history_path).context("Failed to save history")?;

    Ok(())
}

// Helper function to parse common start/restart arguments
fn parse_start_restart_args(
    parts: &[String],
    start_index: usize,
) -> (
    Option<u16>, Option<String>, Option<u16>, Option<u16>, Option<u16>, Option<u16>, // port, cluster, listen_port, storage_port, rest_port, daemon_port
    Option<String>, Option<String>, Option<String>, // daemon_cluster, rest_cluster, storage_cluster
    Option<PathBuf>, Option<PathBuf>, // config_file, storage_config_file
    Option<String>, Option<String>, // data_directory, log_directory
    Option<u64>, Option<u64>, // max_disk_space_gb, min_disk_space_gb
    Option<bool>, Option<String>, // use_raft_for_scale, storage_engine_type
    Option<bool>, Option<bool>, Option<bool>, // daemon, rest, storage
    Option<bool>, Option<bool>, Option<bool>, Option<bool>, // join_cluster, join_rest_cluster, join_storage_cluster, join_daemon_cluster
) {
    let mut port = None;
    let mut cluster = None;
    let mut listen_port = None;
    let mut storage_port = None;
    let mut rest_port = None;
    let mut daemon_port = None;
    let mut daemon_cluster_val = None;
    let mut rest_cluster_val = None;
    let mut storage_cluster_val = None;
    let mut storage_config_file = None;
    let mut config_file = None;
    let mut data_directory = None;
    let mut log_directory = None;
    let mut max_disk_space_gb = None;
    let mut min_disk_space_gb = None;
    let mut use_raft_for_scale = None;
    let mut storage_engine_type = None;
    let mut daemon_flag = None;
    let mut rest_flag = None;
    let mut storage_flag = None;
    let mut join_cluster = None;
    let mut join_rest_cluster = None;
    let mut join_storage_cluster = None;
    let mut join_daemon_cluster = None;

    let mut i = start_index;
    while i < parts.len() {
        match parts[i].to_lowercase().as_str() {
            "--port" | "-p" => {
                if i + 1 < parts.len() {
                    if let Ok(p) = parts[i + 1].parse::<u16>() {
                        port = Some(p);
                    } else {
                        eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--cluster" => {
                if i + 1 < parts.len() {
                    if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                        cluster = Some(valid_cluster);
                    } else {
                        eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--listen-port" => {
                if i + 1 < parts.len() {
                    if let Ok(p) = parts[i + 1].parse::<u16>() {
                        listen_port = Some(p);
                    } else {
                        eprintln!("Warning: Invalid listen-port value: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--storage-port" => {
                if i + 1 < parts.len() {
                    if let Ok(p) = parts[i + 1].parse::<u16>() {
                        storage_port = Some(p);
                    } else {
                        eprintln!("Warning: Invalid storage-port value: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--rest-port" => {
                if i + 1 < parts.len() {
                    if let Ok(p) = parts[i + 1].parse::<u16>() {
                        rest_port = Some(p);
                    } else {
                        eprintln!("Warning: Invalid rest-port value: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--daemon-port" => {
                if i + 1 < parts.len() {
                    if let Ok(p) = parts[i + 1].parse::<u16>() {
                        daemon_port = Some(p);
                    } else {
                        eprintln!("Warning: Invalid daemon-port value: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--daemon-cluster" => {
                if i + 1 < parts.len() {
                    if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                        daemon_cluster_val = Some(valid_cluster);
                    } else {
                        eprintln!("Warning: Invalid daemon-cluster range: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--rest-cluster" => {
                if i + 1 < parts.len() {
                    if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                        rest_cluster_val = Some(valid_cluster);
                    } else {
                        eprintln!("Warning: Invalid rest-cluster range: {}", parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--storage-cluster" => {
                if i + 1 < parts.len() {
                    if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                        storage_cluster_val = Some(valid_cluster);
                    } else {
                        eprintln!("Warning: Invalid storage-cluster range: {}", parts[i + 1]);
                    }
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
                    if let Ok(val) = parts[i + 1].parse::<u64>() {
                        max_disk_space_gb = Some(val);
                    } else {
                        eprintln!("Warning: Invalid value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--min-disk-space-gb" => {
                if i + 1 < parts.len() {
                    if let Ok(val) = parts[i + 1].parse::<u64>() {
                        min_disk_space_gb = Some(val);
                    } else {
                        eprintln!("Warning: Invalid value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--use-raft-for-scale" => {
                if i + 1 < parts.len() {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        use_raft_for_scale = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
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
            "--daemon" => {
                if i + 1 < parts.len() {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        daemon_flag = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--rest" => {
                if i + 1 < parts.len() {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        rest_flag = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--storage" => {
                if i + 1 < parts.len() {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        storage_flag = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                    i += 1;
                }
            }
            "--join-cluster" | "-j" => {
                if i + 1 < parts.len() && (parts[i+1].to_lowercase() == "true" || parts[i+1].to_lowercase() == "false") {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        join_cluster = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    join_cluster = Some(true); // Default to true if no value or invalid value provided
                    i += 1;
                }
            }
            "--join-rest-cluster" => {
                if i + 1 < parts.len() && (parts[i+1].to_lowercase() == "true" || parts[i+1].to_lowercase() == "false") {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        join_rest_cluster = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    join_rest_cluster = Some(true);
                    i += 1;
                }
            }
            "--join-storage-cluster" => {
                if i + 1 < parts.len() && (parts[i+1].to_lowercase() == "true" || parts[i+1].to_lowercase() == "false") {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        join_storage_cluster = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    join_storage_cluster = Some(true);
                    i += 1;
                }
            }
            "--join-daemon-cluster" => {
                if i + 1 < parts.len() && (parts[i+1].to_lowercase() == "true" || parts[i+1].to_lowercase() == "false") {
                    if let Ok(val) = parts[i + 1].parse::<bool>() {
                        join_daemon_cluster = Some(val);
                    } else {
                        eprintln!("Warning: Invalid boolean value for '{}': {}", parts[i], parts[i + 1]);
                    }
                    i += 2;
                } else {
                    join_daemon_cluster = Some(true);
                    i += 1;
                }
            }
            _ => {
                eprintln!("Warning: Unknown argument: {}", parts[i]);
                i += 1;
            }
        }
    }

    (
        port, cluster, listen_port, storage_port, rest_port, daemon_port,
        daemon_cluster_val, rest_cluster_val, storage_cluster_val,
        config_file, storage_config_file,
        data_directory, log_directory,
        max_disk_space_gb, min_disk_space_gb,
        use_raft_for_scale, storage_engine_type,
        daemon_flag, rest_flag, storage_flag,
        join_cluster, join_rest_cluster, join_storage_cluster, join_daemon_cluster,
    )
}

/// Parses the command string into a CommandType and its arguments.
fn parse_command(parts: &[String]) -> (CommandType, Vec<String>) {
    let command_str = parts[0].to_lowercase();
    let args = parts.get(1..).unwrap_or(&[]).to_vec();

    let cmd_type = match command_str.as_str() {
        "start" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "daemon" => {
                        let (
                            port, cluster, _, _, _, daemon_port,
                            daemon_cluster, _, _,
                            config_file, _,
                            _, _, _, _, _, _,
                            daemon_flag, _, _,
                            join_cluster, _, _, join_daemon_cluster,
                        ) = parse_start_restart_args(&parts, 2);

                        CommandType::StartDaemon {
                            port,
                            cluster,
                            daemon_port,
                            daemon_cluster,
                            config_file,
                            join_cluster,
                            join_daemon_cluster,
                        }
                    }
                    "rest" => {
                        let (
                            port, cluster, listen_port, _, rest_port, _,
                            _, rest_cluster, _,
                            config_file, _,
                            _, _, _, _, _, _,
                            _, rest_flag, _,
                            join_cluster, join_rest_cluster, _, _,
                        ) = parse_start_restart_args(&parts, 2);

                        CommandType::StartRest {
                            port,
                            cluster,
                            listen_port,
                            rest_port,
                            join_cluster,
                            join_rest_cluster,
                            rest_cluster,
                            config_file,
                        }
                    }
                    "storage" => {
                        let (
                            port, cluster, _, storage_port, _, _,
                            _, _, storage_cluster,
                            config_file, storage_config_file,
                            data_directory, log_directory,
                            max_disk_space_gb, min_disk_space_gb,
                            use_raft_for_scale, storage_engine_type,
                            _, _, storage_flag,
                            join_cluster, _, join_storage_cluster, _,
                        ) = parse_start_restart_args(&parts, 2);

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
                            join_cluster,
                            join_storage_cluster,
                            storage_cluster,
                            storage_port,
                        }
                    }
                    "all" => {
                        let (
                            port, cluster, listen_port, storage_port, rest_port, daemon_port,
                            daemon_cluster, rest_cluster, storage_cluster,
                            config_file, storage_config_file,
                            data_directory, log_directory,
                            max_disk_space_gb, min_disk_space_gb,
                            use_raft_for_scale, storage_engine_type,
                            daemon_flag, rest_flag, storage_flag,
                            join_cluster, join_rest_cluster, join_storage_cluster, join_daemon_cluster,
                        ) = parse_start_restart_args(&parts, 2);

                        CommandType::StartAll {
                            port,
                            cluster,
                            listen_port,
                            storage_port,
                            rest_port,
                            daemon_port,
                            daemon_cluster,
                            storage_config_file,
                            config_file,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
                            daemon: daemon_flag,
                            rest: rest_flag,
                            storage: storage_flag,
                            join_cluster,
                            join_rest_cluster,
                            join_storage_cluster,
                            rest_cluster,
                            storage_cluster,
                        }
                    }
                    "cluster" => {
                        let (
                            _, cluster, _, _, _, _,
                            _, _, _,
                            _, _,
                            _, _,
                            _, _,
                            _, _, _,
                            join_cluster, _, _, _, _, _,
                        ) = parse_start_restart_args(&parts, 2);
                        CommandType::StartCluster {
                            cluster,
                            join_cluster,
                        }
                    }
                    _ => CommandType::Unknown,
                }
            } else {
                CommandType::StartAll {
                    port: None,
                    cluster: None,
                    listen_port: None,
                    storage_port: None,
                    rest_port: None,
                    daemon_port: None,
                    daemon_cluster: None,
                    storage_config_file: None,
                    config_file: None,
                    data_directory: None,
                    log_directory: None,
                    max_disk_space_gb: None,
                    min_disk_space_gb: None,
                    use_raft_for_scale: None,
                    storage_engine_type: None,
                    daemon: None,
                    rest: None,
                    storage: None,
                    join_cluster: None,
                    join_rest_cluster: None,
                    join_storage_cluster: None,
                    rest_cluster: None,
                    storage_cluster: None,
                }
            }
        },
        "stop" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "all" => {
                        let mut port = None;
                        let mut rest_port = None;
                        let mut daemon_port = None;
                        let mut storage_port = None;
                        let mut rest_cluster = None;
                        let mut daemon_cluster = None;
                        let mut storage_cluster = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            port = Some(p);
                                        } else {
                                            eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            rest_port = Some(p);
                                        } else {
                                            eprintln!("Warning: Invalid rest-port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            daemon_port = Some(p);
                                        } else {
                                            eprintln!("Warning: Invalid daemon-port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            storage_port = Some(p);
                                        } else {
                                            eprintln!("Warning: Invalid storage-port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            rest_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            daemon_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            storage_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'stop all': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StopAll { port, rest_port, daemon_port, storage_port, rest_cluster, daemon_cluster, storage_cluster }
                    }
                    "rest" => {
                        let mut port = None;
                        let mut listen_port = None;
                        let mut rest_port = None;
                        let mut rest_cluster = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" | "--rest-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            rest_port = Some(p);
                                            port = port.or(rest_port);
                                            listen_port = listen_port.or(rest_port);
                                        } else {
                                            eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" | "--cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            rest_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
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
                        CommandType::StopRest { port, listen_port, rest_port, rest_cluster }
                    }
                    "daemon" => {
                        let mut port = None;
                        let mut daemon_port = None;
                        let mut daemon_cluster = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--daemon-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            daemon_port = Some(p);
                                            port = port.or(daemon_port);
                                        } else {
                                            eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" | "--cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            daemon_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
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
                        CommandType::StopDaemon { port, daemon_port, daemon_cluster }
                    }
                    "storage" => {
                        let mut port = None;
                        let mut storage_port = None;
                        let mut storage_cluster = None;
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            storage_port = Some(p);
                                            port = port.or(storage_port);
                                        } else {
                                            eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" | "--cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            storage_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
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
                        CommandType::StopStorage { port, storage_port, storage_cluster }
                    }
                    "cluster" => { // Added for "stop cluster"
                        let mut cluster = None;
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'stop cluster': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StopCluster { cluster }
                    }
                    _ => CommandType::Unknown,
                }
            } else {
                CommandType::Unknown // "stop" without subcommand is ambiguous
            }
        },
        "status" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => {
                        let mut port = None;
                        let mut listen_port = None;
                        let mut rest_port = None;
                        let mut rest_cluster = None;
                        let mut cluster = None; // Added missing field
                        let mut i = 2; // Start checking from after "status rest"
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" | "--rest-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            rest_port = Some(p);
                                            port = port.or(rest_port);
                                            listen_port = listen_port.or(rest_port);
                                        } else {
                                            eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => { // Changed from | "--cluster" to avoid ambiguity with top-level cluster
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            rest_cluster = Some(valid_cluster);
                                            i += 2;
                                        } else {
                                            eprintln!("Warning: Invalid cluster range for 'status rest': {}", parts[i + 1]);
                                            i += 2;
                                        }
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Handle top-level cluster specifically
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            cluster = Some(valid_cluster);
                                            i += 2;
                                        } else {
                                            eprintln!("Warning: Invalid cluster range for 'status rest': {}", parts[i + 1]);
                                            i += 2;
                                        }
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
                        CommandType::StatusRest { port, listen_port, rest_port, rest_cluster, cluster }
                    }
                    "daemon" => {
                        let mut port = None;
                        let mut daemon_port = None;
                        let mut daemon_cluster = None;
                        let mut cluster = None; // Added missing field
                        let mut i = 2; // Start checking from after "status daemon"
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--daemon-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            daemon_port = Some(p);
                                            port = port.or(daemon_port);
                                        } else {
                                            eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => { // Changed from | "--cluster"
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            daemon_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range for 'status daemon': {}", parts[i + 1]);
                                            i += 2;
                                        }
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Handle top-level cluster specifically
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            cluster = Some(valid_cluster);
                                            i += 2;
                                        } else {
                                            eprintln!("Warning: Invalid cluster range for 'status daemon': {}", parts[i + 1]);
                                            i += 2;
                                        }
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
                        CommandType::StatusDaemon { port, daemon_port, daemon_cluster, cluster }
                    }
                    "storage" => {
                        let mut port = None;
                        let mut storage_port = None;
                        let mut storage_cluster = None;
                        let mut cluster = None; // Added missing field
                        let mut i = 2; // Start checking from after "status storage"
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--storage-port" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(p) = parts[i + 1].parse::<u16>() {
                                            storage_port = Some(p);
                                            port = port.or(storage_port);
                                        } else {
                                            eprintln!("Warning: Invalid port value: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => { // Changed from | "--cluster"
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            storage_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range for 'status storage': {}", parts[i + 1]);
                                            i += 2;
                                        }
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Handle top-level cluster specifically
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range for 'status storage': {}", parts[i + 1]);
                                            i += 2;
                                        }
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
                        CommandType::StatusStorage { port, storage_port, storage_cluster, cluster }
                    }
                    "cluster" => {
                        let mut cluster = None;
                        let mut i = 2;
                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                        }
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'status cluster': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StatusCluster { cluster }
                    },
                    "all" => { // This block is already good for parsing cluster flags for 'status all'
                        let mut rest_cluster = None;
                        let mut daemon_cluster = None;
                        let mut storage_cluster = None;
                        let mut cluster = None; // Added missing field
                        let mut i = 2;

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--rest-cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            rest_cluster = Some(valid_cluster);
                                            i += 2;
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                            i += 2;
                                        }
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            daemon_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                            i += 2;
                                        }
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => {
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            storage_cluster = Some(valid_cluster);
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                            i += 2;
                                        }
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => { // Handle top-level cluster specifically
                                    if i + 1 < parts.len() {
                                        if let Ok(valid_cluster) = validate_cluster_range(&parts[i + 1]) {
                                            cluster = Some(valid_cluster);
                                            i += 2;
                                        } else {
                                            eprintln!("Warning: Invalid cluster range: {}", parts[i + 1]);
                                            i += 2;
                                        }
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'status all': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StatusSummary { rest_cluster, daemon_cluster, storage_cluster, cluster }
                    },
                    _ => CommandType::Unknown, // Fallback for unrecognized status subcommands
                }
            } else {
                // Default to status all if no subcommand is given
                CommandType::StatusSummary {
                    rest_cluster: None,
                    daemon_cluster: None,
                    storage_cluster: None,
                    cluster: None, // Added missing field
                }
            }
        },
        "auth" | "authenticate" => {
            if args.len() >= 2 {
                CommandType::Authenticate { // Corrected from CommandType::Auth
                    username: args[0].clone(),
                    password: args[1].clone(),
                }
            } else {
                eprintln!("Usage: {} <username> <password>", command_str);
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
        "query" => {
            if args.len() >= 1 {
                let query_string = args[0].clone();
                let persist = args.get(1).and_then(|s| s.parse::<bool>().ok());
                CommandType::Query(QueryArgs { query: query_string, persist })
            } else {
                eprintln!("Usage: query <query_string> [--persist <true/false>]");
                CommandType::Unknown
            }
        },
        "graph-query" => {
            if args.len() >= 1 {
                let query_string = args[0].clone();
                let persist = args.get(1).and_then(|s| s.parse::<bool>().ok());
                CommandType::GraphQuery(GraphQueryArgs { query_string, persist })
            } else {
                eprintln!("Usage: graph-query <query_string> [--persist <true/false>]");
                CommandType::Unknown
            }
        },
        "version" => CommandType::Version,
        "health" => CommandType::Health,
        "restart" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "all" => {
                        let (
                            port, cluster, listen_port, storage_port, rest_port, daemon_port,
                            daemon_cluster, rest_cluster, storage_cluster,
                            config_file, storage_config_file,
                            data_directory, log_directory,
                            max_disk_space_gb, min_disk_space_gb,
                            use_raft_for_scale, storage_engine_type,
                            daemon_flag, rest_flag, storage_flag,
                            join_cluster, join_rest_cluster, join_storage_cluster, join_daemon_cluster,
                        ) = parse_start_restart_args(&parts, 2);

                        CommandType::RestartAll {
                            port,
                            cluster,
                            listen_port,
                            storage_port,
                            rest_port,
                            daemon_port,
                            daemon_cluster,
                            storage_config_file,
                            config_file,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
                            daemon: daemon_flag,
                            rest: rest_flag,
                            storage: storage_flag,
                            join_cluster,
                            join_rest_cluster,
                            join_storage_cluster,
                            rest_cluster,
                            storage_cluster,
                        }
                    }
                    "rest" => {
                        let (
                            port, cluster, listen_port, _, rest_port, _,
                            _, rest_cluster, _,
                            config_file, _, // Added config_file here
                            _, _, _, _, _, _,
                            _, rest_flag, _,
                            join_cluster, join_rest_cluster, _, _,
                        ) = parse_start_restart_args(&parts, 2);

                        CommandType::RestartRest {
                            port,
                            cluster,
                            listen_port,
                            rest_port,
                            join_cluster,
                            join_rest_cluster,
                            rest_cluster,
                            config_file, // Added config_file here
                        }
                    }
                    "storage" => {
                        let (
                            port, cluster, _, storage_port, _, _,
                            _, _, storage_cluster,
                            config_file, storage_config_file,
                            data_directory, log_directory,
                            max_disk_space_gb, min_disk_space_gb,
                            use_raft_for_scale, storage_engine_type,
                            daemon_flag, rest_flag, storage_flag, // Pass these flags
                            join_cluster, _, join_storage_cluster, _,
                        ) = parse_start_restart_args(&parts, 2);

                        CommandType::RestartStorage {
                            port,
                            config_file,
                            cluster,
                            data_directory,
                            log_directory,
                            max_disk_space_gb,
                            min_disk_space_gb,
                            use_raft_for_scale,
                            storage_engine_type,
                            join_cluster,
                            join_storage_cluster,
                            storage_cluster,
                            storage_port,
                            daemon: daemon_flag, // Pass the daemon flag
                            rest: rest_flag,     // Pass the rest flag
                            storage: storage_flag, // Pass the storage flag
                        }
                    }
                    "daemon" => {
                        let (
                            port, cluster, _, _, _, daemon_port,
                            daemon_cluster, _, _,
                            config_file, _,
                            _, _, _, _, _, _,
                            daemon_flag, _, _,
                            join_cluster, _, _, join_daemon_cluster,
                        ) = parse_start_restart_args(&parts, 2);

                        CommandType::RestartDaemon {
                            port,
                            cluster,
                            daemon_port,
                            daemon_cluster,
                            config_file,
                            join_cluster,
                            join_daemon_cluster,
                        }
                    }
                    "cluster" => { // Added for "restart cluster"
                        let (
                            _, cluster, _, _, _, _, // Use cluster from parse_start_restart_args
                            _, _, _,
                            _, _,
                            _, _,
                            _, _,
                            _, _,
                            _, _, _,
                            _, _, _, _,
                        ) = parse_start_restart_args(&parts, 2);
                        CommandType::RestartCluster {
                            cluster, // Corrected to use 'cluster' field
                        }
                    }
                    _ => CommandType::Unknown,
                }
            } else {
                // If "restart" is called without a subcommand, it implies "restart all"
                CommandType::RestartAll {
                    port: None,
                    cluster: None,
                    listen_port: None,
                    storage_port: None,
                    rest_port: None,
                    daemon_port: None,
                    daemon_cluster: None,
                    storage_config_file: None,
                    config_file: None,
                    data_directory: None,
                    log_directory: None,
                    max_disk_space_gb: None,
                    min_disk_space_gb: None,
                    use_raft_for_scale: None,
                    storage_engine_type: None,
                    daemon: None,
                    rest: None,
                    storage: None,
                    join_cluster: None,
                    join_rest_cluster: None,
                    join_storage_cluster: None,
                    rest_cluster: None,
                    storage_cluster: None,
                }
            }
        },
        "clear" | "clean" => CommandType::Clear,
        "help" => {
            let mut help_args = HelpArgs { filter_command: None, command_path: Vec::new() };
            let mut i = 1;

            while i < parts.len() {
                match parts[i].to_lowercase().as_str() {
                    "--command" | "-c" => {
                        if i + 1 < parts.len() {
                            help_args.filter_command = Some(parts[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", parts[i]);
                            i += 1;
                        }
                    }
                    _ => {
                        eprintln!("Warning: Unknown argument for 'help': {}", parts[i]);
                        i += 1;
                    }
                }
            }
            CommandType::Help(help_args)
        },
        "exit" | "quit" | "q" => CommandType::Exit,
        _ => CommandType::Unknown,
    };

    (cmd_type, args)
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_interactive_command(
    cmd: CommandType,
    args: Vec<String>,
    daemon_handles: Arc<Mutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<Mutex<Option<u16>>>,
    rest_api_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<Mutex<Option<u16>>>,
) -> Result<()> {
    let daemon_handles_clone = Arc::clone(&daemon_handles);
    let rest_api_shutdown_tx_opt_clone = Arc::clone(&rest_api_shutdown_tx_opt);
    let rest_api_port_arc_clone = Arc::clone(&rest_api_port_arc);
    let rest_api_handle_clone = Arc::clone(&rest_api_handle);
    let storage_daemon_shutdown_tx_opt_clone = Arc::clone(&storage_daemon_shutdown_tx_opt);
    let storage_daemon_handle_clone = Arc::clone(&storage_daemon_handle);
    let storage_daemon_port_arc_clone = Arc::clone(&storage_daemon_port_arc);

    match cmd {
        CommandType::Daemon(daemon_cmd) => {
            handlers::handle_daemon_command_interactive(daemon_cmd, daemon_handles_clone, None).await?;
        }
        CommandType::Rest(rest_cmd) => {
            handlers::handle_rest_command_interactive(
                rest_cmd,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                None,
            ).await?;
        }
        CommandType::Storage(storage_cmd) => {
            handlers::handle_storage_command_interactive(
                storage_cmd,
                daemon_handles_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
                None,
            ).await?;
        }
        CommandType::ReloadRest { port, cluster, join_cluster, daemon, rest, storage, listen_port, rest_port, rest_cluster, join_rest_cluster } => {
        }
        CommandType::ReloadAll { port, rest_port, storage_port, cluster, rest_cluster, storage_cluster, join_cluster, join_rest_cluster, join_storage_cluster,
                                 daemon, rest, storage, daemon_port, listen_port, daemon_cluster, config_file, storage_config_file, data_directory, log_directory,
                                 max_disk_space_gb, min_disk_space_gb, use_raft_for_scale, storage_engine_type } => {

        }
        CommandType::ReloadDaemon { port, cluster, join_cluster, daemon, rest, storage, daemon_port, daemon_cluster, join_daemon_cluster, config_file } => {

        }
        CommandType::ReloadStorage { port, config_file, cluster, join_cluster, data_directory, log_directory, max_disk_space_gb, min_disk_space_gb,
                                     use_raft_for_scale, storage_engine_type, daemon, rest, storage, storage_port, storage_cluster, join_storage_cluster } => {
            
        }
        CommandType::ReloadCluster { cluster } => {
            
        }
        CommandType::StorageQuery {} => {

        }
        CommandType::ClearAllData {} => {

        }
        CommandType::ClearStorageData { port } => {

        }
        CommandType::StartRest {
            port,
            cluster,
            listen_port,
            rest_port,
            join_cluster,
            join_rest_cluster,
            rest_cluster,
            config_file,
        } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            if effective_port < 1024 || effective_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port: {}. Must be between 1024 and 65535.", effective_port)));
                return Err(anyhow::anyhow!("Invalid port: {}", effective_port));
            }
            if let Some(c) = &rest_cluster {
                println!("Joining REST cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                 println!("Joining REST cluster (from general --cluster): {:?}", c);
            }
            let start_args = StartArgs {
                action: Some(StartAction::Rest {
                    port: Some(effective_port),
                    cluster: cluster.clone(),
                    listen_port: listen_port.or(Some(effective_port)),
                    rest_port: Some(effective_port),
                    daemon: None,
                    rest: Some(true),
                    storage: None,
                    join_cluster,
                    join_rest_cluster,
                    rest_cluster: rest_cluster.clone(),
                    config_file: config_file.clone(),
                }),
                port: Some(effective_port),
                cluster: cluster.clone(),
                listen_port: listen_port.or(Some(effective_port)),
                rest_port: Some(effective_port),
                daemon_port: None,
                daemon_cluster: None,
                storage_port: None,
                storage_cluster: None,
                storage_config_file: None,
                config_file: config_file.clone(),
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: None,
                rest: Some(true),
                storage: None,
                join_cluster,
                join_rest_cluster,
                join_storage_cluster: None,
                rest_cluster: rest_cluster.clone(),
            };
            {
                let mut port_guard = rest_api_port_arc_clone.lock().await;
                *port_guard = Some(effective_port);
            }
            handlers::handle_start_command(
                start_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
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
            join_cluster,
            join_storage_cluster,
            storage_cluster,
            storage_port,
        } => {
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            if effective_port < 1024 || effective_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port: {}. Must be between 1024 and 65535.", effective_port)));
                return Err(anyhow::anyhow!("Invalid port: {}", effective_port));
            }
            if let Some(c) = &storage_cluster {
                println!("Joining storage cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining storage cluster (from general --cluster): {:?}", c);
            }
            let start_args = StartArgs {
                action: Some(StartAction::Storage {
                    port: Some(effective_port),
                    config_file: config_file.clone(),
                    cluster: cluster.clone(),
                    data_directory: data_directory.clone(),
                    log_directory: log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type: storage_engine_type.clone(),
                    daemon: None,
                    rest: None,
                    storage: Some(true),
                    join_cluster,
                    join_storage_cluster,
                    storage_cluster: storage_cluster.clone(),
                    storage_port: Some(effective_port),
                }),
                port: Some(effective_port),
                config_file: config_file.clone(),
                cluster: cluster.clone(),
                data_directory: data_directory.clone(),
                log_directory: log_directory.clone(),
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type: storage_engine_type.clone(),
                daemon_port: None,
                daemon_cluster: None,
                rest_port: None,
                listen_port: None,
                storage_port: Some(effective_port),
                storage_cluster: storage_cluster.clone(),
                daemon: None,
                rest: None,
                storage: Some(true),
                join_cluster,
                join_rest_cluster: None,
                join_storage_cluster,
                rest_cluster: None,
                storage_config_file: None,
            };
            {
                let mut port_guard = storage_daemon_port_arc_clone.lock().await;
                *port_guard = Some(effective_port);
            }
            handlers::handle_start_command(
                start_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StartDaemon {
            port,
            cluster,
            daemon_port,
            daemon_cluster,
            config_file,
            join_cluster,
            join_daemon_cluster,
        } => {
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            if effective_port < 1024 || effective_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port: {}. Must be between 1024 and 65535.", effective_port)));
                return Err(anyhow::anyhow!("Invalid port: {}", effective_port));
            }
            if let Some(c) = &daemon_cluster {
                println!("Joining daemon cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining daemon cluster (from general --cluster): {:?}", c);
            }
            let start_args = StartArgs {
                action: Some(StartAction::Daemon {
                    port: Some(effective_port),
                    cluster: cluster.clone(),
                    daemon_port: Some(effective_port),
                    daemon_cluster: daemon_cluster.clone(),
                    config_file: config_file.clone(),
                    daemon: Some(true),
                    rest: None,
                    storage: None,
                    join_cluster,
                    join_daemon_cluster,
                }),
                port: Some(effective_port),
                cluster: cluster.clone(),
                daemon_port: Some(effective_port),
                daemon_cluster: daemon_cluster.clone(),
                config_file: config_file.clone(),
                rest_port: None,
                listen_port: None,
                storage_port: None,
                storage_cluster: None,
                storage_config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: Some(true),
                rest: None,
                storage: None,
                join_cluster,
                join_rest_cluster: None,
                join_storage_cluster: None,
                rest_cluster: None,
            };
            handlers::handle_start_command(
                start_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StartAll {
            port,
            cluster,
            listen_port,
            storage_port,
            rest_port,
            daemon_port,
            daemon_cluster,
            storage_config_file,
            config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
            storage,
            join_cluster,
            join_rest_cluster,
            join_storage_cluster,
            rest_cluster,
            storage_cluster,
        } => {
            let effective_rest_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_storage_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let effective_daemon_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            if effective_rest_port < 1024 || effective_rest_port > 65535 ||
               effective_storage_port < 1024 || effective_storage_port > 65535 ||
               effective_daemon_port < 1024 || effective_daemon_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port(sdiscovery_cluster). Must be between 1024 and 65535.")));
                return Err(anyhow::anyhow!("Invalid port(s)"));
            }
            if let Some(c) = &rest_cluster {
                println!("Joining REST cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining REST cluster (from general --cluster): {:?}", c);
            }
            if let Some(c) = &storage_cluster {
                println!("Joining storage cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining storage cluster (from general --cluster): {:?}", c);
            }
            if let Some(c) = &daemon_cluster {
                println!("Joining daemon cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining daemon cluster (from general --cluster): {:?}", c);
            }
            let start_args = StartArgs {
                action: Some(StartAction::All {
                    port,
                    cluster: cluster.clone(),
                    listen_port: listen_port.or(Some(effective_rest_port)),
                    storage_port: Some(effective_storage_port),
                    rest_port: Some(effective_rest_port),
                    daemon_port: Some(effective_daemon_port),
                    daemon_cluster: daemon_cluster.clone(),
                    storage_config_file: storage_config_file.clone(),
                    config_file: config_file.clone(),
                    data_directory: data_directory.clone(),
                    log_directory: log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type: storage_engine_type.clone(),
                    daemon,
                    rest,
                    storage,
                    join_cluster,
                    join_rest_cluster,
                    join_storage_cluster,
                    rest_cluster: rest_cluster.clone(),
                    storage_cluster: storage_cluster.clone(),
                }),
                port,
                cluster: cluster.clone(),
                listen_port: listen_port.or(Some(effective_rest_port)),
                storage_port: Some(effective_storage_port),
                rest_port: Some(effective_rest_port),
                daemon_port: Some(effective_daemon_port),
                daemon_cluster: daemon_cluster.clone(),
                storage_config_file: storage_config_file.clone(),
                config_file: config_file.clone(),
                data_directory: data_directory.clone(),
                log_directory: log_directory.clone(),
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type: storage_engine_type.clone(),
                daemon,
                rest,
                storage,
                join_cluster,
                join_rest_cluster,
                join_storage_cluster,
                rest_cluster: rest_cluster.clone(),
                storage_cluster: storage_cluster.clone(),
            };
            {
                let mut rest_port_guard = rest_api_port_arc_clone.lock().await;
                *rest_port_guard = Some(effective_rest_port);
                let mut storage_port_guard = storage_daemon_port_arc_clone.lock().await;
                *storage_port_guard = Some(effective_storage_port);
            }
            handlers::handle_start_command(
                start_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StartCluster { cluster, join_cluster } => {
            let start_args = StartArgs {
                action: Some(StartAction::Cluster { cluster: cluster.clone(), join_cluster }),
                port: None,
                cluster: cluster.clone(),
                listen_port: None,
                storage_port: None,
                rest_port: None,
                daemon_port: None,
                daemon_cluster: None,
                storage_config_file: None,
                config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: None,
                rest: None,
                storage: None,
                join_cluster,
                join_rest_cluster: None,
                join_storage_cluster: None,
                rest_cluster: None,
                storage_cluster: None,
            };
            handlers::handle_start_command(
                start_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StopAll { port, rest_port, daemon_port, storage_port, rest_cluster, daemon_cluster, storage_cluster } => {
            let stop_args = StopArgs {
                action: Some(StopAction::All {
                    daemon_port,
                    daemon_cluster,
                    rest_port,
                    rest_cluster,
                    storage_port,
                    storage_cluster,
                }),
                port,
                rest_port,
                daemon_port,
                storage_port,
                cluster: None,
                rest_cluster: None,
                daemon_cluster: None,
                storage_cluster: None,
            };
            handlers::handle_stop_command(
                stop_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StopRest { port, listen_port, rest_port, rest_cluster } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let stop_args = StopArgs {
                action: Some(StopAction::Rest {
                    port: Some(effective_port),
                    listen_port: listen_port.or(Some(effective_port)),
                    rest_port: Some(effective_port),
                    cluster: rest_cluster.clone(),
                }),
                port: Some(effective_port),
                rest_port: Some(effective_port),
                daemon_port: None,
                storage_port: None,
                cluster: None,
                rest_cluster: rest_cluster.clone(),
                daemon_cluster: None,
                storage_cluster: None,
            };
            handlers::handle_stop_command(
                stop_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StopDaemon { port, daemon_port, daemon_cluster } => {
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let stop_args = StopArgs {
                action: Some(StopAction::Daemon {
                    port: Some(effective_port),
                    daemon_port: Some(effective_port),
                    cluster: daemon_cluster.clone(),
                }),
                port: Some(effective_port),
                rest_port: None,
                daemon_port: Some(effective_port),
                storage_port: None,
                cluster: None,
                rest_cluster: None,
                daemon_cluster: daemon_cluster.clone(),
                storage_cluster: None,
            };
            handlers::handle_stop_command(
                stop_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StopStorage { port, storage_port, storage_cluster } => {
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let stop_args = StopArgs {
                action: Some(StopAction::Storage {
                    port: Some(effective_port),
                    storage_port: Some(effective_port),
                    cluster: storage_cluster.clone(),
                }),
                port: Some(effective_port),
                rest_port: None,
                daemon_port: None,
                storage_port: Some(effective_port),
                cluster: None,
                rest_cluster: None,
                daemon_cluster: None,
                storage_cluster: storage_cluster.clone(),
            };
            handlers::handle_stop_command(
                stop_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StopCluster { cluster } => {
            let stop_args = StopArgs {
                action: Some(StopAction::Cluster { cluster: cluster.clone() }),
                port: None,
                rest_port: None,
                daemon_port: None,
                storage_port: None,
                cluster: cluster.clone(),
                rest_cluster: None,
                daemon_cluster: None,
                storage_cluster: None,
            };
            handlers::handle_stop_command(
                stop_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::StatusSummary { rest_cluster, daemon_cluster, storage_cluster, cluster } => {
            let status_args = StatusArgs {
                action: Some(StatusAction::All {
                    cluster: cluster.clone(),
                    rest_cluster: rest_cluster.clone(),
                    daemon_cluster: daemon_cluster.clone(),
                    storage_cluster: storage_cluster.clone(),
                }),
                port: None,
                rest_port: None,
                storage_port: None,
                cluster: cluster.clone(),
                rest_cluster: rest_cluster.clone(),
                daemon_cluster: daemon_cluster.clone(),
                storage_cluster: storage_cluster.clone(),
            };
            handlers::handle_status_command(
                status_args,
                rest_api_port_arc_clone,
                storage_daemon_port_arc_clone,
                rest_cluster,
                daemon_cluster,
                storage_cluster,
            ).await?;
        }
        CommandType::StatusRest { port, listen_port, rest_port, rest_cluster, cluster } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let status_args = StatusArgs {
                action: Some(StatusAction::Rest {
                    port: Some(effective_port),
                    listen_port: listen_port.or(Some(effective_port)),
                    rest_port: Some(effective_port),
                    cluster: cluster.clone(),
                }),
                port: Some(effective_port),
                rest_port: Some(effective_port),
                storage_port: None,
                cluster: cluster.clone(),
                rest_cluster: rest_cluster.clone(),
                daemon_cluster: None,
                storage_cluster: None,
            };
            {
                let mut port_guard = rest_api_port_arc_clone.lock().await;
                *port_guard = Some(effective_port);
            }
            handlers::handle_status_command(
                status_args,
                rest_api_port_arc_clone,
                storage_daemon_port_arc_clone,
                rest_cluster,
                None,
                None,
            ).await?;
        }
        CommandType::StatusDaemon { port, daemon_port, daemon_cluster, cluster } => {
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            let status_args = StatusArgs {
                action: Some(StatusAction::Daemon {
                    port: Some(effective_port),
                    daemon_port: Some(effective_port),
                    cluster: cluster.clone(),
                }),
                port: Some(effective_port),
                rest_port: None,
                storage_port: None,
                cluster: cluster.clone(),
                rest_cluster: None,
                daemon_cluster: daemon_cluster.clone(),
                storage_cluster: None,
            };
            handlers::handle_status_command(
                status_args,
                rest_api_port_arc_clone,
                storage_daemon_port_arc_clone,
                None,
                daemon_cluster,
                None,
            ).await?;
        }
        CommandType::StatusStorage { port, storage_port, storage_cluster, cluster } => {
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            if effective_port < 1024 || effective_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port: {}. Must be between 1024 and 65535.", effective_port)));
                return Err(anyhow::anyhow!("Invalid port: {}", effective_port));
            }
            let status_args = StatusArgs {
                action: Some(StatusAction::Storage {
                    port: Some(effective_port),
                    storage_port: Some(effective_port),
                    cluster: cluster.clone(),
                }),
                port: Some(effective_port),
                rest_port: None,
                storage_port: Some(effective_port),
                cluster: cluster.clone(),
                rest_cluster: None,
                daemon_cluster: None,
                storage_cluster: storage_cluster.clone(),
            };
            {
                let mut port_guard = storage_daemon_port_arc_clone.lock().await;
                *port_guard = Some(effective_port);
            }
            handlers::handle_status_command(
                status_args,
                rest_api_port_arc_clone,
                storage_daemon_port_arc_clone,
                None,
                None,
                storage_cluster,
            ).await?;
        }
        CommandType::StatusCluster { cluster } => {
            let status_args = StatusArgs {
                action: Some(StatusAction::Cluster { cluster: cluster.clone() }),
                port: None,
                rest_port: None,
                storage_port: None,
                cluster: cluster.clone(),
                rest_cluster: None,
                daemon_cluster: None,
                storage_cluster: None,
            };
            handlers::handle_status_command(
                status_args,
                rest_api_port_arc_clone,
                storage_daemon_port_arc_clone,
                None,
                None,
                cluster,
            ).await?;
        }
        CommandType::Authenticate { username, password } => {
            let auth_args = AuthArgs { username, password };
            handlers::handle_auth_command(auth_args).await?;
        }
        CommandType::RegisterUser { username, password } => {
            let register_args = RegisterUserArgs { username, password };
            handlers::handle_register_user_command(register_args).await?;
        }
        CommandType::Query(query_args) => {
            handlers::handle_query_command(query_args, rest_api_port_arc_clone).await?;
        }
        CommandType::GraphQuery(graph_query_args) => {
            handlers::handle_graph_query_command(graph_query_args, rest_api_port_arc_clone).await?;
        }
        CommandType::Version => {
            handlers::handle_version_command().await?;
        }
        CommandType::Health => {
            handlers::handle_health_command(rest_api_port_arc_clone).await?;
        }
        CommandType::RestartAll {
            port,
            cluster,
            listen_port,
            storage_port,
            rest_port,
            daemon_port,
            daemon_cluster,
            storage_config_file,
            config_file,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            daemon,
            rest,
            storage,
            join_cluster,
            join_rest_cluster,
            join_storage_cluster,
            rest_cluster,
            storage_cluster,
        } => {
            let effective_rest_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            let effective_storage_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            let effective_daemon_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            if effective_rest_port < 1024 || effective_rest_port > 65535 ||
               effective_storage_port < 1024 || effective_storage_port > 65535 ||
               effective_daemon_port < 1024 || effective_daemon_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port(s). Must be between 1024 and 65535.")));
                return Err(anyhow::anyhow!("Invalid port(s)"));
            }
            if let Some(c) = &rest_cluster {
                println!("Joining REST cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining REST cluster (from general --cluster): {:?}", c);
            }
            if let Some(c) = &storage_cluster {
                println!("Joining storage cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining storage cluster (from general --cluster): {:?}", c);
            }
            if let Some(c) = &daemon_cluster {
                println!("Joining daemon cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining daemon cluster (from general --cluster): {:?}", c);
            }
            let restart_args = RestartArgs {
                action: Some(RestartAction::All {
                    port,
                    cluster: cluster.clone(),
                    listen_port: listen_port.or(Some(effective_rest_port)),
                    storage_port: Some(effective_storage_port),
                    rest_port: Some(effective_rest_port),
                    daemon_port: Some(effective_daemon_port),
                    daemon_cluster: daemon_cluster.clone(),
                    storage_config_file: storage_config_file.clone(),
                    config_file: config_file.clone(),
                    data_directory: data_directory.clone(),
                    log_directory: log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type: storage_engine_type.clone(),
                    daemon,
                    rest,
                    storage,
                    join_cluster,
                    join_rest_cluster,
                    join_storage_cluster,
                    rest_cluster: rest_cluster.clone(),
                    storage_cluster: storage_cluster.clone(),
                }),
                port,
                cluster: cluster.clone(),
                listen_port: listen_port.or(Some(effective_rest_port)),
                storage_port: Some(effective_storage_port),
                rest_port: Some(effective_rest_port),
                daemon_port: Some(effective_daemon_port),
                daemon_cluster: daemon_cluster.clone(),
                storage_config_file: storage_config_file.clone(),
                config_file: config_file.clone(),
                data_directory: data_directory.clone(),
                log_directory: log_directory.clone(),
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type: storage_engine_type.clone(),
                daemon,
                rest,
                storage,
                join_cluster,
                join_rest_cluster,
                join_storage_cluster,
                rest_cluster: rest_cluster.clone(),
                storage_cluster: storage_cluster.clone(),
            };
            {
                let mut rest_port_guard = rest_api_port_arc_clone.lock().await;
                *rest_port_guard = Some(effective_rest_port);
                let mut storage_port_guard = storage_daemon_port_arc_clone.lock().await;
                *storage_port_guard = Some(effective_storage_port);
            }
            handlers::handle_restart_command(
                restart_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::RestartRest { port, cluster, listen_port, rest_port, join_cluster, join_rest_cluster, rest_cluster, config_file } => {
            let effective_port = rest_port.or(listen_port).or(port).unwrap_or(DEFAULT_REST_API_PORT);
            if effective_port < 1024 || effective_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port: {}. Must be between 1024 and 65535.", effective_port)));
                return Err(anyhow::anyhow!("Invalid port: {}", effective_port));
            }
            if let Some(c) = &rest_cluster {
                println!("Joining REST cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining REST cluster (from general --cluster): {:?}", c);
            }
            let restart_args = RestartArgs {
                action: Some(RestartAction::Rest {
                    port: Some(effective_port),
                    cluster: cluster.clone(),
                    listen_port: listen_port.or(Some(effective_port)),
                    rest_port: Some(effective_port),
                    daemon: None,
                    rest: Some(true),
                    storage: None,
                    join_cluster,
                    join_rest_cluster,
                    rest_cluster: rest_cluster.clone(),
                    config_file: config_file.clone(),
                }),
                port: Some(effective_port),
                cluster: cluster.clone(),
                listen_port: listen_port.or(Some(effective_port)),
                rest_port: Some(effective_port),
                daemon_port: None,
                daemon_cluster: None,
                storage_port: None,
                storage_config_file: None,
                config_file: config_file.clone(),
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: None,
                rest: Some(true),
                storage: None,
                join_cluster,
                join_rest_cluster,
                join_storage_cluster: None,
                rest_cluster: rest_cluster.clone(),
                storage_cluster: None,
            };
            {
                let mut port_guard = rest_api_port_arc_clone.lock().await;
                *port_guard = Some(effective_port);
            }
            handlers::handle_restart_command(
                restart_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::RestartStorage {
            port,
            config_file,
            cluster,
            data_directory,
            log_directory,
            max_disk_space_gb,
            min_disk_space_gb,
            use_raft_for_scale,
            storage_engine_type,
            join_cluster,
            join_storage_cluster,
            storage_cluster,
            storage_port,
            daemon,
            rest,
            storage,
        } => {
            let effective_port = storage_port.or(port).unwrap_or(DEFAULT_STORAGE_PORT);
            if effective_port < 1024 || effective_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port: {}. Must be between 1024 and 65535.", effective_port)));
                return Err(anyhow::anyhow!("Invalid port: {}", effective_port));
            }
            if let Some(c) = &storage_cluster {
                println!("Joining storage cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining storage cluster (from general --cluster): {:?}", c);
            }
            let restart_args = RestartArgs {
                action: Some(RestartAction::Storage {
                    port: Some(effective_port),
                    config_file: config_file.clone(),
                    cluster: cluster.clone(),
                    data_directory: data_directory.clone(),
                    log_directory: log_directory.clone(),
                    max_disk_space_gb,
                    min_disk_space_gb,
                    use_raft_for_scale,
                    storage_engine_type: storage_engine_type.clone(),
                    daemon,
                    rest,
                    storage,
                    join_cluster,
                    join_storage_cluster,
                    storage_cluster: storage_cluster.clone(),
                    storage_port: Some(effective_port),
                }),
                port: Some(effective_port),
                config_file: config_file.clone(),
                cluster: cluster.clone(),
                data_directory: data_directory.clone(),
                log_directory: log_directory.clone(),
                max_disk_space_gb,
                min_disk_space_gb,
                use_raft_for_scale,
                storage_engine_type: storage_engine_type.clone(),
                daemon_port: None,
                daemon_cluster: None,
                rest_port: None,
                listen_port: None,
                storage_port: Some(effective_port),
                storage_cluster: storage_cluster.clone(),
                storage_config_file: config_file.clone(),
                daemon,
                rest,
                storage,
                join_cluster,
                join_rest_cluster: None,
                join_storage_cluster,
                rest_cluster: None,
            };
            {
                let mut port_guard = storage_daemon_port_arc_clone.lock().await;
                *port_guard = Some(effective_port);
            }
            handlers::handle_restart_command(
                restart_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::RestartDaemon { port, cluster, daemon_port, daemon_cluster, config_file, join_cluster, join_daemon_cluster } => {
            let effective_port = daemon_port.or(port).unwrap_or(DEFAULT_DAEMON_PORT);
            if effective_port < 1024 || effective_port > 65535 {
                eprintln!("{}", Style::new().red().apply_to(format!("Error: Invalid port: {}. Must be between 1024 and 65535.", effective_port)));
                return Err(anyhow::anyhow!("Invalid port: {}", effective_port));
            }
            if let Some(c) = &daemon_cluster {
                println!("Joining daemon cluster: {:?}", c);
            } else if let Some(c) = &cluster {
                println!("Joining daemon cluster (from general --cluster): {:?}", c);
            }
            let restart_args = RestartArgs {
                action: Some(RestartAction::Daemon {
                    port: Some(effective_port),
                    cluster: cluster.clone(),
                    daemon_port: Some(effective_port),
                    daemon_cluster: daemon_cluster.clone(),
                    config_file: config_file.clone(),
                    daemon: Some(true),
                    rest: None,
                    storage: None,
                    join_cluster,
                    join_daemon_cluster,
                }),
                port: Some(effective_port),
                cluster: cluster.clone(),
                daemon_port: Some(effective_port),
                daemon_cluster: daemon_cluster.clone(),
                config_file: config_file.clone(),
                rest_port: None,
                listen_port: None,
                storage_port: None,
                storage_cluster: None,
                storage_config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: Some(true),
                rest: None,
                storage: None,
                join_cluster,
                join_rest_cluster: None,
                join_storage_cluster: None,
                rest_cluster: None,
            };
            handlers::handle_restart_command(
                restart_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::RestartCluster { cluster } => {
            let restart_args = RestartArgs {
                action: Some(RestartAction::Cluster { cluster: cluster.clone() }),
                port: None,
                cluster: cluster.clone(),
                listen_port: None,
                storage_port: None,
                rest_port: None,
                daemon_port: None,
                daemon_cluster: None,
                storage_config_file: None,
                config_file: None,
                data_directory: None,
                log_directory: None,
                max_disk_space_gb: None,
                min_disk_space_gb: None,
                use_raft_for_scale: None,
                storage_engine_type: None,
                daemon: None,
                rest: None,
                storage: None,
                join_cluster: None,
                join_rest_cluster: None,
                join_storage_cluster: None,
                rest_cluster: None,
                storage_cluster: None,
            };
            handlers::handle_restart_command(
                restart_args,
                daemon_handles_clone,
                rest_api_shutdown_tx_opt_clone,
                rest_api_port_arc_clone,
                rest_api_handle_clone,
                storage_daemon_shutdown_tx_opt_clone,
                storage_daemon_handle_clone,
                storage_daemon_port_arc_clone,
            ).await?;
        }
        CommandType::Clear => {
            println!("{}", Style::new().green().apply_to("Clearing terminal..."));
            print!("\x1B[2J\x1B[1;1H");
        }
        CommandType::Help(help_args) => {
            help_display_mod::print_interactive_help();
        }
        CommandType::Exit => {
            println!("{}", Style::new().green().apply_to("Exiting..."));
            process::exit(0);
        }
        CommandType::Unknown => {
            if !args.is_empty() {
                eprintln!("{}", Style::new().red().apply_to(format!("Unknown command: {}", args.join(" "))));
                help_display_mod::print_interactive_help();
            }
        }
    }
    Ok(())
}
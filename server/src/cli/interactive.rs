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

// Import necessary items from sibling modules
use crate::cli::commands::{CliArgs, DaemonCliCommand, RestCliCommand, StorageAction};
use crate::cli::help_display::HelpArgs; // Corrected: Import HelpArgs from help_display
// Removed unused imports: use crate::cli::config::CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS;
use crate::cli::handlers;
use crate::cli::daemon_management::stop_daemon_api_call;
use crate::cli::help_display::{collect_all_cli_elements_for_suggestions, print_help_clap_generated, print_filtered_help_clap_generated}; // Imported functions

/// Enum representing the parsed command type in interactive mode.
#[derive(Debug, PartialEq)]
pub enum CommandType {
    Daemon(DaemonCliCommand),
    Rest(RestCliCommand),
    Storage(StorageAction),
    // Added top-level Start command variants
    StartRest { port: Option<u16>, listen_port: Option<u16> },
    StartStorage { port: Option<u16>, config_file: Option<PathBuf> },
    StopAll,
    StopRest,
    StopDaemon(Option<u16>),
    StopStorage(Option<u16>),
    StatusSummary,
    StatusRest,
    StatusDaemon(Option<u16>),
    StatusStorage(Option<u16>),
    Help(HelpArgs),
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
    let args: Vec<String> = parts[1..].iter().map(|&s| s.to_string()).collect(); // Removed mut as 'args' is not mutated after initialization

    let cmd_type = match command_str.as_str() {
        "start" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "rest" => {
                        let mut port_arg = None;
                        let mut listen_port_arg = None;
                        let mut i = 2; // Start parsing from the third part

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]); // Fixed: Added parts[i]
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < parts.len() {
                                        listen_port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]); // Fixed: Added parts[i]
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
                        let mut i = 2; // Start parsing from the third part

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]); // Fixed: Added parts[i]
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file_arg = Some(PathBuf::from(parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]); // Fixed: Added parts[i]
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'start storage': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StartStorage { port: port_arg, config_file: config_file_arg }
                    },
                    _ => CommandType::Unknown, // Handle other "start" subcommands if any
                }
            } else {
                // If only "start" is given, it's ambiguous or implies a default start
                // For now, treat as unknown or provide a general start behavior if defined.
                CommandType::Unknown
            }
        },
        "daemon" => {
            if parts.len() > 1 {
                match parts[1].to_lowercase().as_str() {
                    "start" => {
                        let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
                        let cluster_arg = args.iter().find(|&s| s.starts_with("--cluster=")).map(|s| s.trim_start_matches("--cluster=").to_string());
                        CommandType::Daemon(DaemonCliCommand::Start { port: port_arg, cluster: cluster_arg })
                    },
                    "stop" => {
                        let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
                        CommandType::Daemon(DaemonCliCommand::Stop { port: port_arg })
                    },
                    "status" => {
                        let port_arg = args.get(1).and_then(|s| s.parse::<u16>().ok());
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
                        let mut current_idx = 2; // Start parsing from the third part

                        while current_idx < parts.len() {
                            match parts[current_idx].to_lowercase().as_str() {
                                "--port" => {
                                    if current_idx + 1 < parts.len() {
                                        port_arg = parts[current_idx + 1].parse::<u16>().ok();
                                        current_idx += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[current_idx]); // Fixed: Added parts[current_idx]
                                        current_idx += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if current_idx + 1 < parts.len() {
                                        listen_port_arg = parts[current_idx + 1].parse::<u16>().ok();
                                        current_idx += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[current_idx]); // Fixed: Added parts[current_idx]
                                        current_idx += 1;
                                    }
                                }
                                _ => {
                                    // If it's not a recognized flag, it might be a positional argument
                                    // For "rest start", we only expect port arguments, so this is an unknown arg.
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
                        let mut i = 2; // Start parsing from the third part

                        while i < parts.len() {
                            match parts[i].to_lowercase().as_str() {
                                "--port" => {
                                    if i + 1 < parts.len() {
                                        port_arg = parts[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]); // Fixed: Added parts[i]
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < parts.len() {
                                        config_file_arg = Some(PathBuf::from(parts[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", parts[i]); // Fixed: Added parts[i]
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'storage start': {}", parts[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::Storage(StorageAction::Start { port: port_arg, config_file: config_file_arg.unwrap_or_else(|| PathBuf::from("storage_config.yaml")) })
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
        }
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
                    _ => CommandType::StopAll,
                }
            } else {
                CommandType::StopAll
            }
        }
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
                    _ => CommandType::StatusSummary,
                }
            } else {
                CommandType::StatusSummary
            }
        }
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
                            eprintln!("Warning: Flag '{}' requires a value.", parts[i]); // Fixed: Added parts[i]
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
        "exit" | "quit" | "q" => CommandType::Exit,
        _ => CommandType::Unknown,
    };

    (cmd_type, args)
}

/// Prints general help messages for the interactive CLI.
pub fn print_interactive_help() {
    println!("\nGraphDB CLI Commands:");
    println!("  start [rest|storage] [--port <port>] [--listen-port <port>] [--config-file <path>] - Start GraphDB components");
    println!("  stop [rest|daemon|storage] [--port <port>] - Stop GraphDB components (all by default, or specific)");
    println!("  daemon start [--port <port>] [--cluster <range>] - Start a GraphDB daemon");
    println!("  daemon stop [--port <port>]                       - Stop a GraphDB daemon");
    println!("  daemon status [--port <port>]                     - Check status of a GraphDB daemon");
    println!("  daemon list                                       - List daemons managed by this CLI");
    println!("  daemon clear-all                                  - Stop all managed daemons and attempt to kill external ones");
    println!("  rest start [--port <port>] [--listen-port <port>] - Start the REST API server");
    println!("  rest stop                                         - Stop the REST API server");
    println!("  rest status                                       - Check the status of the REST API server");
    println!("  rest health                                       - Perform a health check on the REST API server");
    println!("  rest version                                      - Get the version of the REST API server");
    println!("  rest register-user <username> <password>          - Register a new user via REST API");
    println!("  rest authenticate <username> <password>           - Authenticate a user and get a token via REST API");
    println!("  rest graph-query \"<query_string>\" [persist]       - Execute a graph query via REST API");
    println!("  rest storage-query                                - Execute a storage query via REST API (placeholder)");
    println!("  storage start [--port <port>] [--config-file <path>] - Start the standalone Storage daemon");
    println!("  storage stop [--port <port>]                      - Stop the standalone Storage daemon");
    println!("  storage status [--port <port>]                    - Check the status of the standalone Storage daemon");
    println!("  status                                            - Get a comprehensive status summary of all GraphDB components");
    println!("  status rest                                       - Get detailed status of the REST API component");
    println!("  status daemon [--port <port>]                     - Get detailed status of a specific daemon or list common ones");
    println!("  status storage [--port <port>]                    - Get detailed status of the Storage component");
    println!("  help [--command|-c <command_string>]              - Display this help message or help for a specific command");
    println!("  exit | quit | q                                   - Exit the CLI");
    println!("\nNote: Commands like 'view-graph', 'index-node', etc., are placeholders.");
}

/// Prints help messages filtered by a command string for interactive mode.
pub fn print_interactive_filtered_help(_cmd: &mut clap::Command, command_filter: &str) { // Added _ to cmd
    let commands = [
        ("start [rest|storage] [--port <port>] [--listen-port <port>] [--config-file <path>]", "Start GraphDB components"),
        ("stop [rest|daemon|storage] [--port <port>]", "Stop GraphDB components (all by default, or specific)"),
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
        ("status", "Get a comprehensive status summary of all GraphDB components"),
        ("status rest", "Get detailed status of the REST API component"),
        ("status daemon [--port <port>]", "Get detailed status of a specific daemon or list common ones"),
        ("status storage [--port <port>]", "Get detailed status of the Storage component"),
        ("help [--command|-c <command_string>]", "Display this help message or help for a specific command"),
        ("exit | quit | q", "Exit the CLI"),
    ];

    let filter_lower = command_filter.to_lowercase();
    let mut found_match = false;

    println!("\n--- Help for '{}' ---", command_filter);
    for (command_syntax, description) in commands.iter() {
        if command_syntax.to_lowercase().contains(&filter_lower) || description.to_lowercase().contains(&filter_lower) {
            println!("  {:<50} - {}", command_syntax, description);
            found_match = true;
        }
    }

    if !found_match {
        // If no exact subcommand path, or if the last segment was an option/bad input
        // Fallback to fuzzy matching and general help.

        // Collect all possible command paths and individual option names
        let mut all_known_elements = HashSet::<String>::new(); // Fixed: Added type annotation
        collect_all_cli_elements_for_suggestions(&CliArgs::command(), &mut Vec::new(), &mut all_known_elements);

        let mut suggestions = Vec::new();
        const JARO_WINKLER_THRESHOLD: f64 = 0.75; // Adjust as needed for strictness (0.75-0.85 is common)

        for known_element in &all_known_elements {
            let similarity = jaro_winkler(&filter_lower, &known_element.to_lowercase());
            if similarity > JARO_WINKLER_THRESHOLD {
                suggestions.push((known_element.clone(), similarity));
            }
        }

        // Sort suggestions by similarity (highest first)
        suggestions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        if !suggestions.is_empty() {
            println!("\nNo exact help found for '{}'. Did you mean one of these?", command_filter);
            for (suggestion, _) in suggestions.iter().take(5) { // Limit to top 5 suggestions
                println!("  graphdb-cli {}", suggestion);
            }
            // Add a general hint for options if the filter looked like an option
            if command_filter.starts_with("--") || command_filter.ends_with("-") { // Corrected starts_backwards to ends_with
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
pub async fn run_cli_interactive() -> Result<()> {
    // State variables for managing daemons and REST API in the interactive session
    let daemon_handles: Arc<Mutex<HashMap<u16, (tokio::task::JoinHandle<()>, oneshot::Sender<()>)>>> = Arc::new(Mutex::new(HashMap::new()));
    let rest_api_shutdown_tx_opt: Arc<Mutex<Option<oneshot::Sender<()>>>> = Arc::new(Mutex::new(None));
    let rest_api_port_arc: Arc<Mutex<Option<u16>>> = Arc::new(Mutex::<Option<u16>>::new(None));
    let rest_api_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>> = Arc::new(Mutex::new(None));

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
    let mut rest_tx_guard = rest_api_shutdown_tx_opt.lock().await;
    let mut rest_handle_guard = rest_api_handle.lock().await;
    let mut rest_api_port_guard = rest_api_port_arc.lock().await;

    if let Some(port) = rest_api_port_guard.take() {
        println!("Attempting to stop REST API server on port {} during exit...", port);
        if let Some(tx) = rest_tx_guard.take() {
            let _ = tx.send(()); // Signal the handle if it's waiting
        }
        handlers::stop_process_by_port("REST API", port)?; // Use the helper function
        if let Some(handle) = rest_handle_guard.take() {
            let _ = handle.await; // Wait for the task to finish
        }
        println!("REST API server on port {} stopped.", port);
    }

    // Stop all managed daemons
    let mut handles = daemon_handles.lock().await;
    if !handles.is_empty() {
        println!("Stopping all managed daemon instances...");
        let mut join_handles = Vec::new();
        for (port, (handle, tx)) in handles.drain() {
            println!("Signaling daemon on port {} to stop.", port);
            if tx.send(()).is_err() {
                eprintln!("Warning: Daemon on port {} already stopped or signal failed.", port);
            }
            join_handles.push(handle);
        }
        for handle in join_handles {
            let _ = handle.await; // Wait for each daemon task to complete
        }
        println!("All managed daemon instances stopped.");
    }

    // Send a global stop signal to ensure any external daemon processes are terminated
    println!("Sending global stop signal to all daemon processes...");
    let stop_result = stop_daemon_api_call();
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
        CommandType::StartRest { port, listen_port } => { // Added match arm for StartRest
            let rest_cmd = RestCliCommand::Start { port, listen_port };
            handlers::handle_rest_command_interactive(rest_cmd, rest_api_shutdown_tx_opt, rest_api_port_arc, rest_api_handle).await?;
        }
        CommandType::StartStorage { port, config_file } => { // Added match arm for StartStorage
            let storage_action = StorageAction::Start { port, config_file: config_file.unwrap_or_else(|| PathBuf::from("storage_config.yaml")) };
            handlers::handle_storage_command_interactive(storage_action).await?;
        }
        CommandType::StopAll => {
            handlers::handle_stop_all_interactive().await?;
        }
        CommandType::StopRest => {
            handlers::handle_stop_rest_interactive().await?;
        }
        CommandType::StopDaemon(port) => {
            handlers::handle_stop_daemon_interactive(port).await?;
        }
        CommandType::StopStorage(port) => {
            handlers::handle_stop_storage_interactive(port).await?;
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
        CommandType::Help(help_args) => {
            let mut cmd = CliArgs::command(); // Get the top-level Command object for clap's help generation
            // Corrected: Pass a reference to the String, or clone it if ownership is needed later.
            // Since print_filtered_help_clap_generated expects &str, we pass &String.
            if let Some(command_filter) = help_args.filter_command {
                print_filtered_help_clap_generated(&mut cmd, &command_filter); // Fixed: Removed help_display:: prefix
            } else if !help_args.command_path.is_empty() {
                let command_filter = help_args.command_path.join(" ");
                print_filtered_help_clap_generated(&mut cmd, &command_filter); // Fixed: Removed help_display:: prefix
            } else {
                print_help_clap_generated(); // Fixed: Removed help_display:: prefix
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


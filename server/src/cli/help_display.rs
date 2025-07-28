// server/src/cli/help_display.rs

// This module provides functions for displaying CLI help messages,
// leveraging clap's generated help.

use clap::{CommandFactory, Command, Arg, ValueHint, Args};
use crate::cli::cli::CliArgs; // Corrected: Import CliArgs from cli.rs
use std::collections::HashSet;
use strsim::jaro_winkler;

/// Arguments for the `help` command
#[derive(Args, Debug, PartialEq)]
pub struct HelpArgs {
    /// Specify a command string to get specific help for (e.g., "daemon start").
    #[arg(long = "command", short = 'c', value_name = "COMMAND_STRING", help = "Get help for a specific command.")]
    pub filter_command: Option<String>, 

    /// Positional arguments representing the command path (e.g., "daemon start").
    #[arg(raw = true)]
    pub command_path: Vec<String>,
}


/// Prints the full, auto-generated help message for the CLI.
pub fn print_help_clap_generated() {
    let mut cmd = CliArgs::command();
    cmd.print_help().expect("Failed to print help");
}

/// Helper to collect all valid command paths (e.g., "start", "stop rest")
/// and individual option names (e.g., "--port", "-p").
pub fn collect_all_cli_elements_for_suggestions( // Made public
    cmd: &Command,
    current_path_segments: &mut Vec<String>,
    all_known_elements: &mut HashSet<String>,
) {
    // Add the full command path as a potential suggestion
    if !current_path_segments.is_empty() {
        all_known_elements.insert(current_path_segments.join(" "));
    }

    // Add individual options for the current command
    for arg in cmd.get_arguments() {
        if let Some(long) = arg.get_long() {
            all_known_elements.insert(format!("--{}", long));
        }
        if let Some(short) = arg.get_short() {
            all_known_elements.insert(format!("-{}", short));
        }
    }

    // Recursively process subcommands
    for sub_cmd in cmd.get_subcommands() {
        current_path_segments.push(sub_cmd.get_name().to_string());
        collect_all_cli_elements_for_suggestions(sub_cmd, current_path_segments, all_known_elements);
        current_path_segments.pop(); // Backtrack
    }
}

/// Helper function to find a nested subcommand given a root command and a list of segments.
pub fn find_nested_subcommand_mut<'a>( // Made public
    cmd: &'a mut Command,
    segments: &[&str],
) -> Option<&'a mut Command> {
    let mut current_cmd = cmd;
    for segment in segments {
        if let Some(sub_cmd) = current_cmd.find_subcommand_mut(segment) {
            current_cmd = sub_cmd;
        } else {
            return None; // Segment not found
        }
    }
    Some(current_cmd) // Return the final found subcommand
}

/// Prints a filtered help message based on a command string, with suggestions.
pub fn print_filtered_help_clap_generated(_cmd_root: &mut clap::Command, command_filter: &str) {
    let filter_lower = command_filter.to_lowercase();
    let mut found_exact_subcommand_help = false;

    // Commands array should match the interactive.rs print_interactive_help
    // This array is now defined in interactive.rs and should be used there for filtering.
    // This function is specifically for clap-generated help, which means it relies on CliArgs::command()
    // and its subcommands.

    // Attempt to find exact match for a subcommand path (e.g., "stop rest")
    let filter_segments: Vec<&str> = command_filter.split_whitespace().collect();

    if !filter_segments.is_empty() {
        let mut cli_args_cmd = CliArgs::command(); // Create a new Command instance for searching
        if let Some(final_cmd) = find_nested_subcommand_mut(&mut cli_args_cmd, &filter_segments) {
            final_cmd.print_help().expect("Failed to print subcommand help");
            found_exact_subcommand_help = true;
        }
    }

    if !found_exact_subcommand_help {
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


/// Prints general help messages for the interactive CLI.
pub fn print_interactive_help() {
    println!("\nGraphDB CLI Commands:");
    println!("  start [rest|storage|daemon|all] [--port <port>] [--cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>] - Start GraphDB components");
    println!("  stop [rest|daemon|storage|all] [--port <port>] - Stop GraphDB components (all by default, or specific)");
    println!("  daemon start [--port <port>] [--cluster <range>] - Start a GraphDB daemon");
    println!("  daemon stop [--port <port>]                       - Stop a GraphDB daemon");
    println!("  daemon status [--port <port>]                     - Check status of a GraphDB daemon");
    println!("  daemon list                                       - List daemons managed by this CLI");
    println!("  daemon clear-all                                  - Stop all managed daemons and attempt to kill external ones");
    println!("  rest start [--listen-port <port>] - Start the REST API server");
    println!("  rest stop                                         - Stop the REST API server");
    println!("  rest status                                       - Check the status of the REST API server");
    println!("  rest health                                       - Perform a health check on the REST API server");
    println!("  rest version                                      - Get the version of the REST API server");
    println!("  rest register-user <username> <password>          - Register a new user via REST API");
    println!("  rest authenticate <username> <password>           - Authenticate a user and get a token via REST API");
    println!("  rest graph-query \"<query_string>\" [persist]       - Execute a graph query via REST API");
    println!("  rest storage-query                                - Execute a storage query via REST API (placeholder)");
    println!("  storage start [--storage-port <port>] [--config-file <path>] - Start the standalone Storage daemon");
    println!("  storage stop [--port <port>]                      - Stop the standalone Storage daemon");
    println!("  storage status [--port <port>]                    - Check the status of the standalone Storage daemon");
    println!("  status [rest|daemon|storage|cluster|all] [--port <port>] - Get a comprehensive status summary or specific component status");
    println!("  auth <username> <password>                        - Authenticate a user and get a token (top-level)");
    println!("  authenticate <username> <password>                - Authenticate a user and get a token (top-level)");
    println!("  register <username> <password>                    - Register a new user (top-level)");
    println!("  version                                           - Get the version of the REST API server (top-level)");
    println!("  health                                            - Perform a health check on the REST API server (top-level)");
    println!("  reload [all|rest|storage|daemon|cluster] [--port <port>] - Reload GraphDB components");
    println!("  restart all [--port <port>] [--cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>] - Restart all core GraphDB components");
    println!("  restart rest [--listen-port <port>] - Restart the REST API server");
    println!("  restart storage [--storage-port <port>] [--config-file <path>] - Restart the standalone Storage daemon");
    println!("  restart daemon [--port <port>] [--cluster <range>] - Restart a GraphDB daemon");
    println!("  restart cluster - Restart cluster configuration (placeholder)");
    println!("  clear | clean                                     - Clear the terminal screen");
    println!("  help [--command|-c <command_string>]              - Display this help message or help for a specific command");
    println!("  exit | quit | q                                   - Exit the CLI");
    println!("\nNote: Commands like 'view-graph', 'index-node', etc., are placeholders.");
}

/// Prints help messages filtered by a command string for interactive mode.
pub fn print_interactive_filtered_help(_cmd: &mut clap::Command, command_filter: &str) {
    let commands = [
        ("start rest [--listen-port <port>]", "Start the REST API server"),
        ("start storage [--storage-port <port>] [--config-file <path>]", "Start the standalone Storage daemon"),
        ("start daemon [--port <port>] [--cluster <range>]", "Start a GraphDB daemon instance"),
        ("start all [--port <port>] [--cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>]", "Start all core GraphDB components"),
        ("start", "Start all core GraphDB components (default)"),
        ("stop rest", "Stop the REST API server"),
        ("stop daemon [--port <port>]", "Stop a GraphDB daemon"),
        ("stop storage [--port <port>]", "Stop the standalone Storage daemon"),
        ("stop all", "Stop all core GraphDB components"),
        ("stop", "Stop all GraphDB components (default)"),
        ("daemon start [--port <port>] [--cluster <range>]", "Start a GraphDB daemon"),
        ("daemon stop [--port <port>]", "Stop a GraphDB daemon"),
        ("daemon status [--port <port>]", "Check status of a GraphDB daemon"),
        ("daemon list", "List daemons managed by this CLI"),
        ("daemon clear-all", "Stop all managed daemons and attempt to kill external ones"),
        ("rest start [--listen-port <port>]", "Start the REST API server"),
        ("rest stop", "Stop the REST API server"),
        ("rest status", "Check the status of the REST API server"),
        ("rest health", "Perform a health check on the REST API server"),
        ("rest version", "Get the version of the REST API server"),
        ("rest register-user <username> <password>", "Register a new user via REST API"),
        ("rest authenticate <username> <password>", "Authenticate a user and get a token via REST API"),
        ("rest graph-query \"<query_string>\" [persist]", "Execute a graph query via REST API"),
        ("rest storage-query", "Execute a storage query via REST API (placeholder)"),
        ("storage start [--storage-port <port>] [--config-file <path>]", "Start the standalone Storage daemon"),
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
        ("reload [all|rest|storage|daemon|cluster] [--port <port>]", "Reload GraphDB components"),
        ("restart all [--port <port>] [--cluster <range>] [--listen-port <port>] [--storage-port <port>] [--storage-config <path>]", "Restart all core GraphDB components"),
        ("restart rest [--listen-port <port>]", "Restart the REST API server"),
        ("restart storage [--storage-port <port>] [--config-file <path>]", "Restart the standalone Storage daemon"),
        ("restart daemon [--port <port>] [--cluster <range>]", "Restart a GraphDB daemon"),
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
            println!("  {:<50} - {}", command_syntax, description);
            found_match = true;
        } else if description.to_lowercase().contains(&filter_lower) {
            println!("  {:<50} - {}", command_syntax, description);
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


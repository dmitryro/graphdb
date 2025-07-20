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
/// This function is not typically called directly for interactive help,
/// but can be useful for debugging or full overview.
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

/// Prints the full help message for interactive mode.
pub fn print_interactive_help() {
    println!("\nGraphDB CLI Commands:");
    println!("  start [rest|storage|daemon|all] [--port <port>] [--cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--rest-port <port>] [--storage-port <port>] [--daemon-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--daemon <true/false>] [--rest <true/false>] [--storage <true/false>] [--join-rest-cluster <true/false>] [--join-storage-cluster <true/false>] [--rest-cluster <range>] [--storage-cluster <range>] - Start GraphDB components");
    println!("  stop [rest|daemon|storage|all] [--port <port>] [--rest-port <port>] [--listen-port <port>] [--storage-port <port>] [--daemon-port <port>] - Stop GraphDB components (all by default, or specific)");
    println!("  daemon start [--port <port>] [--daemon-port <port>] [--cluster <range>] [--daemon-cluster <range>] [--join-cluster <range>] [--config-file <path>] - Start a GraphDB daemon");
    println!("  daemon stop [--port <port>] [--daemon-port <port>] - Stop a GraphDB daemon");
    println!("  daemon status [--port <port>] [--daemon-port <port>] - Check status of a GraphDB daemon");
    println!("  daemon list - List daemons managed by this CLI");
    println!("  daemon clear-all - Stop all managed daemons and attempt to kill external ones");
    println!("  rest start [--listen-port <port>] [--rest-port <port>] [--port <port>] [--cluster <range>] [--rest-cluster <range>] [--join-cluster <range>] [--join-rest-cluster <true/false>] - Start the REST API server");
    println!("  rest stop [--port <port>] [--rest-port <port>] [--listen-port <port>] - Stop the REST API server");
    println!("  rest status [--port <port>] [--rest-port <port>] [--listen-port <port>] - Check the status of the REST API server");
    println!("  rest health - Perform a health check on the REST API server");
    println!("  rest version - Get the version of the REST API server");
    println!("  rest register-user <username> <password> - Register a new user via REST API");
    println!("  rest authenticate <username> <password> - Authenticate a user and get a token via REST API");
    println!("  rest graph-query \"<query_string>\" [persist] - Execute a graph query via REST API");
    println!("  rest storage-query - Execute a storage query via REST API (placeholder)");
    println!("  storage start [--storage-port <port>] [--port <port>] [--config-file <path>] [--cluster <range>] [--storage-cluster <range>] [--join-cluster <range>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--join-storage-cluster <true/false>] - Start the standalone Storage daemon");
    println!("  storage stop [--port <port>] [--storage-port <port>] - Stop the standalone Storage daemon");
    println!("  storage status [--port <port>] [--storage-port <port>] - Check the status of the standalone Storage daemon");
    println!("  status [rest|daemon|storage|cluster|all] [--port <port>] [--rest-port <port>] [--rest-cluster <range>] [--storage-port <port>] [--storage-cluster <range>] [--daemon-port <port>] - Check the status of components or cluster");
    println!("  auth <username> <password> - Authenticate a user and get a token (top-level)");
    println!("  authenticate <username> <password> - Authenticate a user and get a token (top-level)");
    println!("  register <username> <password> - Register a new user (top-level)");
    println!("  version - Get the version of the REST API server (top-level)");
    println!("  health - Perform a health check on the REST API server (top-level)");
    println!("  restart [all|rest|storage|daemon|cluster] [--port <port>] [--daemon-port <port>] [--cluster <range>] [--daemon-cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--rest-port <port>] [--storage-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--daemon <true/false>] [--rest <true/false>] [--storage <true/false>] [--join-rest-cluster <true/false>] [--join-storage-cluster <true/false>] [--rest-cluster <range>] [--storage-cluster <range>] - Restart GraphDB components");
    println!("  clear | clean - Clear the terminal screen");
    println!("  help [--command|-c <command_string>] - Display this help message or help for a specific command");
    println!("  exit | quit | q - Exit the CLI");
}

/// Prints filtered help messages based on a command filter.
pub fn print_interactive_filtered_help(_cmd_root: &mut clap::Command, command_filter: &str) {
    let commands = [
        ("start rest [--listen-port <port>] [--rest-port <port>] [--port <port>] [--cluster <range>] [--rest-cluster <range>] [--join-cluster <range>] [--join-rest-cluster <true/false>]", "Start the REST API server"),
        ("start storage [--storage-port <port>] [--port <port>] [--config-file <path>] [--cluster <range>] [--storage-cluster <range>] [--join-cluster <range>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--join-storage-cluster <true/false>]", "Start the standalone Storage daemon"),
        ("start daemon [--port <port>] [--daemon-port <port>] [--cluster <range>] [--daemon-cluster <range>] [--join-cluster <range>] [--config-file <path>]", "Start a GraphDB daemon"),
        ("start all [--port <port>] [--daemon-port <port>] [--cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--rest-port <port>] [--storage-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--daemon <true/false>] [--rest <true/false>] [--storage <true/false>] [--join-rest-cluster <true/false>] [--join-storage-cluster <true/false>] [--rest-cluster <range>] [--storage-cluster <range>]", "Start all GraphDB components"),
        ("stop rest [--port <port>] [--rest-port <port>] [--listen-port <port>]", "Stop the REST API server"),
        ("stop daemon [--port <port>] [--daemon-port <port>]", "Stop a GraphDB daemon"),
        ("stop storage [--port <port>] [--storage-port <port>]", "Stop the standalone Storage daemon"),
        ("stop all", "Stop all GraphDB components"),
        ("daemon start [--port <port>] [--daemon-port <port>] [--cluster <range>] [--daemon-cluster <range>] [--join-cluster <range>] [--config-file <path>]", "Start a GraphDB daemon"),
        ("daemon stop [--port <port>] [--daemon-port <port>]", "Stop a GraphDB daemon"),
        ("daemon status [--port <port>] [--daemon-port <port>]", "Check status of a GraphDB daemon"),
        ("daemon list", "List daemons managed by this CLI"),
        ("daemon clear-all", "Stop all managed daemons and attempt to kill external ones"),
        ("rest start [--listen-port <port>] [--rest-port <port>] [--port <port>] [--cluster <range>] [--rest-cluster <range>] [--join-cluster <range>] [--join-rest-cluster <true/false>]", "Start the REST API server"),
        ("rest stop [--port <port>] [--rest-port <port>] [--listen-port <port>]", "Stop the REST API server"),
        ("rest status [--port <port>] [--rest-port <port>] [--listen-port <port>]", "Check the status of the REST API server"),
        ("rest health", "Perform a health check on the REST API server"),
        ("rest version", "Get the version of the REST API server"),
        ("rest register-user <username> <password>", "Register a new user via REST API"),
        ("rest authenticate <username> <password>", "Authenticate a user and get a token via REST API"),
        ("rest graph-query \"<query_string>\" [persist]", "Execute a graph query via REST API"),
        ("rest storage-query", "Execute a storage query via REST API (placeholder)"),
        ("storage start [--storage-port <port>] [--port <port>] [--config-file <path>] [--cluster <range>] [--storage-cluster <range>] [--join-cluster <range>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--join-storage-cluster <true/false>]", "Start the standalone Storage daemon"),
        ("storage stop [--port <port>] [--storage-port <port>]", "Stop the standalone Storage daemon"),
        ("storage status [--port <port>] [--storage-port <port>]", "Check the status of the standalone Storage daemon"),
        ("status rest [--port <port>] [--rest-port <port>] [--rest-cluster <range>]", "Check the status of the REST API server"),
        ("status daemon [--port <port>] [--daemon-port <port>]", "Check status of a GraphDB daemon"),
        ("status storage [--port <port>] [--storage-port <port>] [--storage-cluster <range>]", "Check the status of the standalone Storage daemon"),
        ("status cluster [--cluster <range>] [--rest-cluster <range>] [--storage-cluster <range>]", "Check the status of the cluster"),
        ("status all", "Get a comprehensive status summary"),
        ("auth <username> <password>", "Authenticate a user and get a token (top-level)"),
        ("authenticate <username> <password>", "Authenticate a user and get a token (top-level)"),
        ("register <username> <password>", "Register a new user (top-level)"),
        ("version", "Get the version of the REST API server (top-level)"),
        ("health", "Perform a health check on the REST API server (top-level)"),
        ("restart all [--port <port>] [--daemon-port <port>] [--cluster <range>] [--daemon-cluster <range>] [--join-cluster <range>] [--listen-port <port>] [--rest-port <port>] [--storage-port <port>] [--storage-config <path>] [--config-file <path>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--daemon <true/false>] [--rest <true/false>] [--storage <true/false>] [--join-rest-cluster <true/false>] [--join-storage-cluster <true/false>] [--rest-cluster <range>] [--storage-cluster <range>]", "Restart all core GraphDB components"),
        ("restart rest [--listen-port <port>] [--rest-port <port>] [--port <port>] [--cluster <range>] [--rest-cluster <range>] [--join-cluster <range>] [--join-rest-cluster <true/false>]", "Restart the REST API server"),
        ("restart storage [--storage-port <port>] [--port <port>] [--config-file <path>] [--cluster <range>] [--storage-cluster <range>] [--join-cluster <range>] [--data-directory <path>] [--log-directory <path>] [--max-disk-space-gb <gb>] [--min-disk-space-gb <gb>] [--use-raft-for-scale <true/false>] [--storage-engine <type>] [--join-storage-cluster <true/false>]", "Restart the standalone Storage daemon"),
        ("restart daemon [--port <port>] [--daemon-port <port>] [--cluster <range>] [--daemon-cluster <range>] [--join-cluster <range>] [--config-file <path>]", "Restart a GraphDB daemon"),
        ("restart cluster", "Restart cluster configuration (placeholder)"),
        ("clear | clean", "Clear the terminal screen"),
        ("help [--command|-c <command_string>]", "Display this help message or help for a specific command"),
        ("exit | quit | q", "Exit the CLI"),
    ];

    let filtered_commands: Vec<_> = commands
        .iter()
        .filter(|(cmd, _)| cmd.contains(command_filter))
        .collect();

    if filtered_commands.is_empty() {
        println!("No commands found matching '{}'. Available commands:", command_filter);
        for (cmd, desc) in commands.iter() {
            println!("  {} - {}", cmd, desc);
        }
    } else {
        println!("Commands matching '{}':", command_filter);
        for (cmd, desc) in filtered_commands {
            println!("  {} - {}", cmd, desc);
        }
    }
}

/// Prints a filtered help message based on a command string, with suggestions.
/// This function uses Clap's internal command structure for more precise help.
pub fn print_filtered_help_clap_generated(_cmd_root: &mut clap::Command, command_filter: &str) {
    let filter_lower = command_filter.to_lowercase();
    let mut found_exact_subcommand_help = false;

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
            print_help_clap_generated(); // Call the full clap-generated help
        }
    }
    println!("------------------------------------");
}


// server/src/cli/help_display.rs

// This module provides functions for displaying CLI help messages,
// leveraging clap's generated help.

use clap::CommandFactory;
use crate::cli::commands::CliArgs; // Import CliArgs to generate help

/// Prints the full, auto-generated help message for the CLI.
pub fn print_help_clap_generated() {
    let mut cmd = CliArgs::command();
    cmd.print_help().expect("Failed to print help");
}

/// Prints a filtered help message based on a command string.
pub fn print_filtered_help_clap_generated(cmd: &mut clap::Command, filter_command: &str) {
    // This function would ideally use clap's filtering capabilities if available
    // or manually filter the help output. For now, we'll use a simplified approach
    // similar to what was in the original interactive.rs.
    // A more robust solution might involve iterating clap's commands programmatically.

    let commands = [
        ("start [--port <port>] [--cluster <range>] [--listen-port <port>] [--storage-port <port>]", "Start GraphDB components"),
        ("stop [rest|daemon|storage] [--port <port>]", "Stop GraphDB components (all by default, or specific)"),
        ("daemon start [--port <port>] [--cluster <range>]", "Start a GraphDB daemon"),
        ("daemon stop [--port <port>]", "Stop a GraphDB daemon"),
        ("daemon status [--port <port>]", "Check status of a GraphDB daemon"),
        ("daemon list", "List daemons managed by this CLI"),
        ("daemon clear-all", "Stop all managed daemons and attempt to kill external ones"),
        ("rest start [--port <port>]", "Start the REST API server"),
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

    let filter_lower = filter_command.to_lowercase();
    let mut found_match = false;

    println!("\n--- Help for '{}' ---", filter_command);
    for (command_syntax, description) in commands.iter() {
        if command_syntax.to_lowercase().contains(&filter_lower) || description.to_lowercase().contains(&filter_lower) {
            println!("  {:<50} - {}", command_syntax, description);
            found_match = true;
        }
    }

    if !found_match {
        println!("\nNo specific help found for '{}'. Displaying general help.", filter_command);
        print_help_clap_generated(); // Call the general help function
    }
    println!("------------------------------------");
}


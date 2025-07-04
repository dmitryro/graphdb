// server/src/cli/help_display.rs

// This module provides functions for displaying CLI help messages,
// leveraging clap's generated help.

use clap::{CommandFactory, Command, Arg, ValueHint, Args}; // Added Args for HelpArgs derive
use crate::cli::commands::CliArgs; // Import CliArgs to generate help
use std::collections::HashSet;
use strsim::{jaro_winkler}; // Import string similarity functions

/// Arguments for the `help` command
// Re-introduced HelpArgs here as it's no longer in commands.rs
#[derive(Args, Debug, PartialEq)]
pub struct HelpArgs {
    /// Specify a command string to get specific help for (e.g., "daemon start").
    #[arg(long = "command", short = 'c', value_name = "COMMAND_STRING", help = "Get help for a specific command.")]
    pub filter_command: Option<String>, 

    /// Positional arguments representing the command path (e.g., "daemon start").
    // This field is kept for compatibility with the interactive parser,
    // but its usage might be simplified now that filter_command handles the main logic.
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
pub fn collect_all_cli_elements_for_suggestions(
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
        // Also add positional argument names if they are distinct and not just placeholders
        // This part might need careful consideration depending on how you want to suggest positional args.
        // For now, focusing on named options and subcommands.
        // if !arg.is_positional() && arg.get_value_hint() == ValueHint::Other {
        //     if let Some(name) = arg.get_id().as_str() {
        //         all_known_elements.insert(name.to_string());
        //     }
        // }
    }

    // Recursively process subcommands
    for sub_cmd in cmd.get_subcommands() {
        current_path_segments.push(sub_cmd.get_name().to_string());
        collect_all_cli_elements_for_suggestions(sub_cmd, current_path_segments, all_known_elements);
        current_path_segments.pop(); // Backtrack
    }
}

/// Helper function to find a nested subcommand given a root command and a list of segments.
/// This avoids the mutable borrow conflict by managing the reference chain explicitly.
fn find_nested_subcommand_mut<'a>(
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
pub fn print_filtered_help_clap_generated(_cmd_root: &mut clap::Command, command_filter: &str) { // Changed cmd_root to _cmd_root to suppress warning
    let filter_lower = command_filter.to_lowercase();
    let mut found_exact_subcommand_help = false;

    // Commands array should match the interactive.rs print_interactive_help
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
        let mut all_known_elements = HashSet::new();
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
            if command_filter.starts_with("--") || command_filter.starts_with("-") {
                println!("\nIf you were looking for an option for a command, try 'graphdb-cli <command> --help'.");
            }
        } else {
            println!("\nNo specific help found for '{}'. Displaying general help.", command_filter);
            print_help_clap_generated();
        }
    }
    println!("------------------------------------");
}


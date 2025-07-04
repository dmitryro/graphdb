// server/src/cli/help_display.rs

// This module provides functions for displaying CLI help messages,
// leveraging clap's generated help.

use clap::{CommandFactory, Command, Arg, ValueHint, Args};
use crate::cli::commands::CliArgs;
use std::collections::HashSet;
use strsim::{jaro_winkler};

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


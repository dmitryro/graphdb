// daemon_api/src/help_generator.rs

// This module provides functions for generating CLI help messages programmatically,
// leveraging clap's capabilities.

use clap::CommandFactory;
use crate::cli_schema::CliArgs;

/// Generates the full, auto-generated help message for the CLI.
pub fn generate_full_help() -> String {
    let mut cmd = CliArgs::command(); // FIX: Use CliArgs::command() directly
    let mut buffer = Vec::new();
    cmd.write_help(&mut buffer).expect("Failed to write help to buffer");
    String::from_utf8(buffer).expect("Failed to convert help to UTF-8")
}

/// Generates a filtered help message based on a command path.
///
/// This function attempts to get the help for a specific subcommand path.
/// If the path is empty, it returns the full help.
/// If the path is invalid or no specific help is found, it activates the error case.
pub fn generate_help_for_path(command_path: &[String]) -> String {
    if command_path.is_empty() {
        return generate_full_help();
    }

    let mut cmd = CliArgs::command(); // FIX: Use CliArgs::command() directly
    let mut current_subcommand = Some(&mut cmd);
    let mut full_path_str = String::new();

    // Traverse the command structure to find the specific subcommand
    for (i, segment) in command_path.iter().enumerate() {
        if i > 0 {
            full_path_str.push(' ');
        }
        full_path_str.push_str(segment);

        if let Some(sub) = current_subcommand.take().and_then(|c| c.find_subcommand_mut(segment)) {
            current_subcommand = Some(sub);
        } else {
            // If a segment is not found as a subcommand, return an error message
            return format!(
                "Error: Command path '{}' not found. Displaying general help.\n\n{}",
                full_path_str,
                generate_full_help()
            );
        }
    }

    if let Some(final_cmd) = current_subcommand {
        let mut buffer = Vec::new();
        final_cmd.write_help(&mut buffer).expect("Failed to write filtered help to buffer");
        String::from_utf8(buffer).expect("Failed to convert filtered help to UTF-8")
    } else {
        // This case should ideally not be reached if the loop logic is correct,
        // but it's a fallback.
        format!(
            "Error: Could not retrieve help for '{}'. Displaying general help.\n\n{}",
            full_path_str,
            generate_full_help()
        )
    }
}

/// Generates status information for a given command path.
/// This is a mock/placeholder for now, as actual status checks involve network calls.
/// In a real scenario, this would likely interact with daemon_api or other health endpoints.
pub async fn generate_status_for_path(command_path: &[String]) -> String {
    let mut response_lines = vec![];
    response_lines.push(format!("--- Status for: {} ---", command_path.join(" ")));

    // Corrected matching on &[String] directly
    match command_path {
        [] => {
            // Full status summary
            response_lines.push("Comprehensive system status:".to_string());
            response_lines.push("  GraphDB Daemon: Checking...".to_string());
            response_lines.push("  REST API: Checking...".to_string());
            response_lines.push("  Storage Daemon: Checking...".to_string());
            response_lines.push("\nNote: For detailed status, use CLI 'status <component>' or REST '/api/v1/status/<component>'".to_string());
        }
        // FIX: Removed 'ref' binding modifier
        [rest_str] if rest_str == "rest" => {
            response_lines.push("Checking REST API status...".to_string());
            // In a real implementation, make an HTTP call to the REST API's /health endpoint
            // For now, a placeholder:
            response_lines.push("  REST API: Mock Status - Running".to_string());
        }
        // FIX: Removed 'ref' binding modifier
        [daemon_str] if daemon_str == "daemon" => {
            response_lines.push("Checking GraphDB Daemon status (all common ports)...".to_string());
            // Placeholder for checking multiple daemon ports
            response_lines.push("  Daemon (8080): Mock Status - Running".to_string());
            response_lines.push("  Daemon (9001): Mock Status - Down".to_string());
        }
        // FIX: Removed 'ref' binding modifier
        [daemon_str, port_str] if daemon_str == "daemon" => {
            if let Ok(port) = port_str.parse::<u16>() {
                response_lines.push(format!("Checking GraphDB Daemon status on port {}...", port));
                // Placeholder for checking a specific daemon port
                response_lines.push(format!("  Daemon ({}): Mock Status - Running", port));
            } else {
                response_lines.push(format!("Invalid port specified: {}", port_str));
            }
        }
        // FIX: Removed 'ref' binding modifier
        [storage_str] if storage_str == "storage" => {
            response_lines.push("Checking Storage Daemon status...".to_string());
            // Placeholder for checking storage daemon status
            response_lines.push("  Storage Daemon: Mock Status - Running (Sled)".to_string());
        }
        // FIX: Removed 'ref' binding modifier
        [storage_str, port_str] if storage_str == "storage" => {
            if let Ok(port) = port_str.parse::<u16>() {
                response_lines.push(format!("Checking Storage Daemon status on port {}...", port));
                // Placeholder for checking a specific storage daemon port
                response_lines.push(format!("  Storage Daemon ({}): Mock Status - Running (RocksDB)", port));
            } else {
                response_lines.push(format!("Invalid port specified: {}", port_str));
            }
        }
        _ => {
            response_lines.push(format!("Unknown status request: {:?}. Displaying general status summary.", command_path));
            response_lines.push("  GraphDB Daemon: Checking...".to_string());
            response_lines.push("  REST API: Checking...".to_string());
            response_lines.push("  Storage Daemon: Checking...".to_string());
        }
    }
    response_lines.join("\n")
}


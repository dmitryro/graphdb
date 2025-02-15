use clap::{Parser, command}; // Import the necessary clap macros and attributes
use crossterm::{
    terminal::{Clear, ClearType},
    cursor,
    style::{self, Color}, // Import only what's needed
    ExecutableCommand,
};
use std::io::{self, Write};
use std::collections::HashSet;

/// CLI entry point for GraphDB
#[derive(Parser, Debug)] // Derive Parser from clap
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI", long_about = None)]
struct CliArgs {}

/// Starts the interactive CLI.
pub fn start_cli() {
    let _args = CliArgs::parse(); // This will parse command line arguments

    // Define a set of valid commands, including exit/quit dynamically
    let mut valid_commands: HashSet<&str> = [
        "help", "status", "list", "connect", "clear", // Add other commands here
    ]
    .iter()
    .cloned()
    .collect();

    // Dynamically add exit/quit to the valid commands set
    valid_commands.insert("exit");
    valid_commands.insert("quit");
    valid_commands.insert("q");

    // Access the terminal output and print greeting using terminal features
    let mut stdout = io::stdout();
    stdout.execute(Clear(ClearType::All)).expect("Failed to clear screen"); // Clear screen
    stdout.execute(cursor::MoveTo(0, 0)).expect("Failed to move cursor"); // Move cursor to top

    // Use Cyan color for the greeting text
    stdout
        .execute(style::SetForegroundColor(Color::Cyan))
        .expect("Failed to set color");
    write!(
        stdout,
        "\nWelcome to GraphDB CLI\nType a command and press Enter. Type 'exit' or 'quit' or 'q' to quit.\n\n"
    )
    .expect("Failed to write greeting");
    stdout.execute(style::ResetColor).expect("Failed to reset color"); // Reset color after greeting

    // Ensure terminal message is displayed
    stdout.flush().expect("Failed to flush stdout");

    loop {
        // Display the prompt with colored command input hint
        stdout
            .execute(style::SetForegroundColor(Color::Cyan))
            .expect("Failed to set color");
        print!("=> ");
        stdout.execute(style::ResetColor).expect("Failed to reset color"); // Reset color after printing the prompt
        io::stdout().flush().expect("Failed to flush stdout"); // Ensure it prints

        let mut input = String::new();
        if let Err(e) = io::stdin().read_line(&mut input) {
            println!("Error reading input: {}", e);
            continue;
        }

        let command = input.trim(); // Trim spaces/newlines

        if valid_commands.contains(command) {
            // Check if the command is 'exit' or 'quit' (dynamically from the HashSet)
            if command == "exit" || command == "quit" || command == "q" {
                println!("\nExiting GraphDB CLI... Goodbye!\n"); // Print exit message
                break; // Graceful exit
            }

            // Handle other commands dynamically
            println!("Executing command: {}", command);
        } else if !command.is_empty() {
            // Apply color to the "Unknown command" message
            stdout
                .execute(style::SetForegroundColor(Color::Yellow))
                .expect("Failed to set color");
            println!("Unknown command: {}", command);
            stdout.execute(style::ResetColor).expect("Failed to reset color"); // Reset color after colored message
        }
    }
}


use clap::{Parser};
use crossterm::{
    execute,
    terminal::{Clear, ClearType}, // Removed unused imports
};
use std::io::{self, Write};

/// CLI entry point for GraphDB
#[derive(Parser, Debug)]
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI", long_about = None)]
struct CliArgs {}

/// Starts the interactive CLI.
pub fn start_cli() {
    let _args = CliArgs::parse();

    println!("\nWelcome to GraphDB CLI");
    println!("Type a command and press Enter. Type 'exit' to quit.\n");

    let mut stdout = io::stdout();
    execute!(stdout, Clear(ClearType::All)).expect("Failed to clear screen");

    loop {
        print!("=> "); // Display prompt
        io::stdout().flush().expect("Failed to flush stdout"); // Ensure it prints

        let mut input = String::new();
        if let Err(e) = io::stdin().read_line(&mut input) {
            println!("Error reading input: {}", e);
            continue;
        }

        let command = input.trim(); // Trim spaces/newlines

        match command {
            "exit" | "quit" => {
                println!("\nExiting GraphDB CLI... Goodbye!\n");
                break; // Graceful exit
            }
            _ if !command.is_empty() => println!("Executing command: {}", command), // Echo command
            _ => continue, // Ignore empty input
        }
    }
}


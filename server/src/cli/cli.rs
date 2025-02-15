use clap::{Parser, command};
use crossterm::{
    terminal::{Clear, ClearType},
    cursor,
    style::{self, Color},
    ExecutableCommand,
};
use std::io::{self, Write};
use std::collections::HashSet;
use graphdb_lib::query_parser::{parse_query_from_string, QueryType};
/// CLI entry point for GraphDB
#[derive(Parser, Debug)]
#[command(name = "graphdb-cli")]
#[command(version = "0.1.0")]
#[command(about = "Experimental Graph Database CLI", long_about = None)]
struct CliArgs {}

/// Starts the interactive CLI.
pub fn start_cli() {
    let _args = CliArgs::parse();

    let mut valid_commands: HashSet<&str> = [
        "help", "status", "list", "connect", "clear",
    ]
    .iter()
    .cloned()
    .collect();

    valid_commands.insert("exit");
    valid_commands.insert("quit");
    valid_commands.insert("q");

    let mut stdout = io::stdout();
    stdout.execute(Clear(ClearType::All)).expect("Failed to clear screen");
    stdout.execute(cursor::MoveTo(0, 0)).expect("Failed to move cursor");

    stdout
        .execute(style::SetForegroundColor(Color::Cyan))
        .expect("Failed to set color");
    write!(
        stdout,
        "\nWelcome to GraphDB CLI\nType a command and press Enter. Type 'exit' or 'quit' or 'q' to quit.\n\n"
    )
    .expect("Failed to write greeting");
    stdout.execute(style::ResetColor).expect("Failed to reset color");

    stdout.flush().expect("Failed to flush stdout");

    loop {
        stdout
            .execute(style::SetForegroundColor(Color::Cyan))
            .expect("Failed to set color");
        print!("=> ");
        stdout.execute(style::ResetColor).expect("Failed to reset color");
        io::stdout().flush().expect("Failed to flush stdout");

        let mut input = String::new();
        if let Err(e) = io::stdin().read_line(&mut input) {
            println!("Error reading input: {}", e);
            continue;
        }

        let command = input.trim();

        if valid_commands.contains(command) {
            if command == "exit" || command == "quit" || command == "q" {
                println!("\nExiting GraphDB CLI... Goodbye!\n");
                break;
            }
            println!("Executing command: {}", command);
        } else if !command.is_empty() {
            // Attempt to parse the command as a query
            match parse_query_from_string(command) {
                Ok(parsed_query) => {
                    // Here you would match on the type of query
                    match parsed_query {
                        QueryType::Cypher => {
                            println!("Cypher query detected: {}", command);
                        }
                        QueryType::SQL => {
                            println!("SQL query detected: {}", command);
                        }
                        QueryType::GraphQL => {
                            println!("GraphQL query detected: {}", command);
                        }
                    }
                }
                Err(_) => {
                    stdout
                        .execute(style::SetForegroundColor(Color::Yellow))
                        .expect("Failed to set color");
                    println!("Unknown command: {}", command);
                    stdout.execute(style::ResetColor).expect("Failed to reset color");
                }
            }
        }
    }
}


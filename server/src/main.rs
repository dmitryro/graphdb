// server/src/main.rs

// This is the main entry point for the GraphDB server application.
// It handles command-line argument parsing and dispatches to the CLI logic.

// Corrected: Import start_cli
use graphdb_server::cli::cli::start_cli;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging (optional, but good practice for server applications)
    env_logger::init();

    // Call the main CLI entry point from the cli module
    // This will now correctly await the Future returned by start_cli
    start_cli().await
}


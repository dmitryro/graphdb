// server/src/main.rs

// This is the main entry point for the GraphDB server application.
// It handles command-line argument parsing and dispatches to the CLI logic.

// Corrected: Import start_cli
use graphdb_server::cli::cli::start_cli;
use anyhow::Result;
use tokio::signal::unix::{signal, SignalKind};
use log::{info, debug, warn, error, trace};

async fn handle_signals() {
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => {
            info!("Received SIGTERM, shutting down gracefully...");
            // Perform cleanup, e.g., close SledStorage
        }
        _ = sigint.recv() => {
            info!("Received SIGINT, shutting down gracefully...");
            // Perform cleanup
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging (optional, but good practice for server applications)
    env_logger::init();

    // Spawn signal handler
    tokio::spawn(handle_signals());

    // Call the main CLI entry point from the cli module
    // This will now correctly await the Future returned by start_cli
    start_cli().await
}


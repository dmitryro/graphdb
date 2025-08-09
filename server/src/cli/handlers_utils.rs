use anyhow::{Result, Context, anyhow}; // Added `anyhow` macro import
use std::path::{PathBuf, Path};
use std::io::{self, Write};
use std::fs;
use log::{info, error, warn, debug};
use crate::cli::config::{StorageConfig, daemon_api_storage_engine_type_to_string, DAEMON_REGISTRY_DB_PATH};
use lib::storage_engine::config::{StorageEngineType};
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use crossterm::cursor::MoveTo;
use daemon_api::daemon_registry::{DaemonMetadata};

/// Helper to get the path to the current executable.
pub fn get_current_exe_path() -> Result<PathBuf> {
    std::env::current_exe()
        .context("Failed to get current executable path")
}

/// Helper function to format engine-specific configuration details
pub fn format_engine_config(storage_config: &StorageConfig) -> Vec<String> {
    let mut config_lines = Vec::new();
    
    // Display the storage engine type prominently
    config_lines.push(format!("Engine: {}", daemon_api_storage_engine_type_to_string(&storage_config.storage_engine_type)));
    
    // Display engine-specific configuration if available
    if let Some(ref engine_config) = storage_config.engine_specific_config {
        let storage_inner = &engine_config.storage;
        
        match storage_config.storage_engine_type {
            StorageEngineType::RocksDB | StorageEngineType::Sled => {
                // File-based storage engines
                if let Some(ref path) = storage_inner.path {
                    config_lines.push(format!("  Data Path: {}", path.display()));
                }
                if let Some(ref host) = storage_inner.host {
                    config_lines.push(format!("  Host: {}", host));
                }
                if let Some(port) = storage_inner.port {
                    config_lines.push(format!("  Port: {}", port));
                }
            },
            StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                // Database storage engines
                if let Some(ref host) = storage_inner.host {
                    config_lines.push(format!("  Host: {}", host));
                }
                if let Some(port) = storage_inner.port {
                    config_lines.push(format!("  Port: {}", port));
                }
                if let Some(ref database) = storage_inner.database {
                    config_lines.push(format!("  Database: {}", database));
                }
                if let Some(ref username) = storage_inner.username {
                    config_lines.push(format!("  Username: {}", username));
                }
                // Don't display password for security reasons
                if storage_inner.password.is_some() {
                    config_lines.push("  Password: [CONFIGURED]".to_string());
                }
            },
            StorageEngineType::Redis => {
                // Redis storage engine
                if let Some(ref host) = storage_inner.host {
                    config_lines.push(format!("  Host: {}", host));
                }
                if let Some(port) = storage_inner.port {
                    config_lines.push(format!("  Port: {}", port));
                }
                if let Some(ref database) = storage_inner.database {
                    config_lines.push(format!("  Database: {}", database));
                }
                if storage_inner.password.is_some() {
                    config_lines.push("  Password: [CONFIGURED]".to_string());
                }
            },
            StorageEngineType::InMemory => {
                // In-memory storage doesn't need additional config
                config_lines.push("  Config: In-memory storage (no additional configuration)".to_string());
            }
        }
    } else {
        config_lines.push("  Config: Using default configuration".to_string());
    }
    
    // Add general storage configuration
    config_lines.push(format!("  Max Open Files: {}", storage_config.max_open_files));
    config_lines.push(format!("  Max Disk Space: {} GB", storage_config.max_disk_space_gb));
    config_lines.push(format!("  Min Disk Space: {} GB", storage_config.min_disk_space_gb));
    config_lines.push(format!("  Use Raft: {}", storage_config.use_raft_for_scale));
    
    config_lines
}

/// Prints a visually appealing welcome screen for the CLI.
pub fn print_welcome_screen() {
    let (cols, rows) = terminal_size().unwrap_or((120, 40)); // Get actual terminal size, default to 120x40
    let total_width = cols as usize;
    let border_char = '#';

    let line_str = border_char.to_string().repeat(total_width);

    let title = "GraphDB Command Line Interface";
    let version = "Version 0.1.0 (Experimental)";
    let welcome_msg = "Welcome! Type 'help' for a list of commands.";
    let start_tip = "Tip: Use 'start all' to launch all components.";
    let status_tip = "Tip: Use 'status all' to check component health.";
    let clear_tip = "Use 'clear' or 'clean' to clear the terminal.";
    let exit_tip = "Type 'exit' or 'quit' to leave the CLI.";

    // Modified: print_centered_colored now takes an `is_bold` argument and adds more internal padding
    let print_centered_colored = |text: &str, text_color: style::Color, is_bold: bool| {
        let internal_padding_chars = 6; // 3 spaces on each side inside the borders
        let content_width = total_width.saturating_sub(2 + internal_padding_chars); // Account for 2 border chars and internal padding
        let padding_len = content_width.saturating_sub(text.len());
        let left_padding = padding_len / 2;
        let right_padding = padding_len - left_padding;

        print!("{}", style::SetForegroundColor(style::Color::Cyan));
        print!("{}", border_char);
        print!("{}", " ".repeat(internal_padding_chars / 2)); // Left internal padding

        print!("{}", style::ResetColor); // Reset color before text to apply text_color
        let styled_text = if is_bold {
            text.with(text_color).bold()
        } else {
            text.with(text_color)
        };

        print!("{}", " ".repeat(left_padding));
        print!("{}", styled_text);
        print!("{}", " ".repeat(right_padding));

        print!("{}", style::SetForegroundColor(style::Color::Cyan)); // Set color for right internal padding and border
        println!("{}{}", border_char, style::ResetColor);
    };

    // Calculate dynamic vertical padding
    let content_lines = 13; // Increased for more vertical spacing
    let available_rows = rows as usize;
    let top_bottom_padding = available_rows.saturating_sub(content_lines) / 2;

    for _ in 0..top_bottom_padding {
        println!();
    }

    println!("{}", line_str.clone().with(style::Color::Cyan));
    print_centered_colored("", style::Color::Blue, false); // Empty line for vertical spacing
    print_centered_colored(title, style::Color::DarkCyan, true); // Made title bold
    print_centered_colored(version, style::Color::White, true); // Made version bold
    print_centered_colored("", style::Color::Blue, false); // Empty line for vertical spacing
    print_centered_colored(welcome_msg, style::Color::Green, true); // Made welcome message bold
    print_centered_colored(start_tip, style::Color::Yellow, false);
    print_centered_colored(status_tip, style::Color::Yellow, false);
    print_centered_colored(clear_tip, style::Color::Yellow, false);
    print_centered_colored(exit_tip, style::Color::Red, false);
    print_centered_colored("", style::Color::Blue, false); // Empty line for vertical spacing
    println!("{}", line_str.with(style::Color::Cyan));
    
    for _ in 0..top_bottom_padding {
        println!();
    }
}

// Helper functions for registry fallback
pub async fn write_registry_fallback(daemons: &[DaemonMetadata], path: &PathBuf) -> Result<()> {
    let serialized = serde_json::to_string(daemons)
        .map_err(|e| anyhow!("Failed to serialize registry state: {}", e))?;
    tokio::fs::write(path, serialized)
        .await
        .map_err(|e| anyhow!("Failed to write registry fallback to {:?}: {}", path, e))?;
    Ok(())
}

pub async fn read_registry_fallback(path: &PathBuf) -> Result<Vec<DaemonMetadata>> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| anyhow!("Failed to read registry fallback from {:?}: {}", path, e))?;
    serde_json::from_str(&content)
        .map_err(|e| anyhow!("Failed to deserialize registry state: {}", e))
}

/// Clears the terminal screen.
pub async fn clear_terminal_screen() -> Result<()> {
    execute!(io::stdout(), Clear(ClearType::All), MoveTo(0, 0))
        .context("Failed to clear terminal screen or move cursor")?;
    io::stdout().flush()?; // Ensure the changes are immediately visible
    Ok(())
}

// A utility function to ensure the necessary parent directories for the daemon registry exist.
/// This prevents "No such file or directory" errors when creating PID files or the database.
// A utility function to ensure the necessary parent directories for the daemon registry exist.
/// This prevents "No such file or directory" errors when creating PID files or the database.
pub async fn ensure_daemon_registry_paths_exist() -> Result<()> {
    let db_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);
    if !db_path.exists() {
        info!("Creating daemon registry directory: {:?}", db_path);
        fs::create_dir_all(&db_path)
            .context(format!("Failed to create daemon registry directory: {:?}", db_path))?;
    }

    Ok(())
}

pub async fn execute_storage_query() {
    println!("Executing storage query...");
    println!("Storage query executed (placeholder).");
}

// Helper function to convert StorageEngineType to string
pub fn storage_engine_type_to_str(engine: StorageEngineType) -> &'static str {
    match engine {
        StorageEngineType::Sled => "sled",
        StorageEngineType::RocksDB => "rocksdb",
        StorageEngineType::InMemory => "inmemory",
        StorageEngineType::Redis => "redis",
        StorageEngineType::PostgreSQL => "postgresql",
        StorageEngineType::MySQL => "mysql",
    }
}
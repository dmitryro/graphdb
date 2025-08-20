use anyhow::{Result, Context, anyhow}; // Added `anyhow` macro import
use std::path::{PathBuf};
use std::io::{self, Write};
use std::collections::HashMap;
use std::fs;
use log::{info, error, warn, debug};
use serde_json::{self, Value};
use crate::cli::commands::{CommandType, ShowAction,  ConfigAction};
use crate::cli::config::{StorageConfig, SelectedStorageConfig, daemon_api_storage_engine_type_to_string, DAEMON_REGISTRY_DB_PATH};
use lib::storage_engine::config::{StorageEngineType};
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use crossterm::cursor::MoveTo;
use lib::daemon_registry::{DaemonMetadata};

/// Helper to get the path to the current executable.
pub fn get_current_exe_path() -> Result<PathBuf> {
    std::env::current_exe()
        .context("Failed to get current executable path")
}

// Helper function to convert HashMap<String, Value> to SelectedStorageConfig
pub fn convert_hashmap_to_selected_config(
    config_map: HashMap<String, Value>
) -> Result<SelectedStorageConfig, anyhow::Error> {
    // Wrap the config_map in a storage object since SelectedStorageConfig expects a storage field
    let wrapped_config = serde_json::json!({
        "storage": config_map
    });
    
    let selected_config: SelectedStorageConfig = serde_json::from_value(wrapped_config)
        .context("Failed to deserialize JSON to SelectedStorageConfig")?;
    
    Ok(selected_config)
}

/// Helper function to format engine-specific configuration details
/// Formats the engine configuration into a vector of strings for display.
pub fn format_engine_config(config: &StorageConfig) -> Vec<String> {
    let mut lines = Vec::new();

    // Log the input config for debugging
    debug!("Formatting engine config: {:?}", config);

    // Display the storage engine type
    let engine_type = config.storage_engine_type.to_string();
    lines.push(format!("Engine: {}", engine_type));

    // Display engine-specific configuration if available
    if let Some(engine_config) = &config.engine_specific_config {
        let storage_inner = &engine_config.storage;

        match config.storage_engine_type {
            StorageEngineType::RocksDB | StorageEngineType::Sled | StorageEngineType::TiKV => {
                // File-based storage engines
                if let Some(path) = &storage_inner.path {
                    lines.push(format!("Data Path: {}", path.display()));
                } else {
                    lines.push("Data Path: Not specified".to_string());
                }
                if let Some(host) = &storage_inner.host {
                    lines.push(format!("Host: {}", host));
                } else {
                    lines.push("Host: Not specified".to_string());
                }
                if let Some(port) = storage_inner.port {
                    lines.push(format!("Port: {}", port));
                } else {
                    lines.push("Port: Not specified".to_string());
                }
            },
            StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                // Database storage engines
                if let Some(host) = &storage_inner.host {
                    lines.push(format!("Host: {}", host));
                } else {
                    lines.push("Host: Not specified".to_string());
                }
                if let Some(port) = storage_inner.port {
                    lines.push(format!("Port: {}", port));
                } else {
                    lines.push("Port: Not specified".to_string());
                }
                if let Some(database) = &storage_inner.database {
                    lines.push(format!("Database: {}", database));
                } else {
                    lines.push("Database: Not specified".to_string());
                }
                if let Some(username) = &storage_inner.username {
                    lines.push(format!("Username: {}", username));
                } else {
                    lines.push("Username: Not specified".to_string());
                }
                if storage_inner.password.is_some() {
                    lines.push("Password: [CONFIGURED]".to_string());
                } else {
                    lines.push("Password: Not specified".to_string());
                }
            },
            StorageEngineType::Redis => {
                // Redis storage engine
                if let Some(host) = &storage_inner.host {
                    lines.push(format!("Host: {}", host));
                } else {
                    lines.push("Host: Not specified".to_string());
                }
                if let Some(port) = storage_inner.port {
                    lines.push(format!("Port: {}", port));
                } else {
                    lines.push("Port: Not specified".to_string());
                }
                if let Some(database) = &storage_inner.database {
                    lines.push(format!("Database: {}", database));
                } else {
                    lines.push("Database: Not specified".to_string());
                }
                if storage_inner.password.is_some() {
                    lines.push("Password: [CONFIGURED]".to_string());
                } else {
                    lines.push("Password: Not specified".to_string());
                }
            },
            StorageEngineType::InMemory => {
                lines.push("Config: In-memory storage (no additional configuration)".to_string());
            }
        }
    } else {
        lines.push("Config: Using default configuration".to_string());
    }

    // Add general storage configuration
    lines.push(format!("Max Open Files: {}", config.max_open_files));
    lines.push(format!("Max Disk Space: {} GB", config.max_disk_space_gb));
    lines.push(format!("Min Disk Space: {} GB", config.min_disk_space_gb));
    lines.push(format!("Use Raft: {}", config.use_raft_for_scale));

    lines
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
        StorageEngineType::TiKV => "tikv",
        StorageEngineType::InMemory => "inmemory",
        StorageEngineType::Redis => "redis",
        StorageEngineType::PostgreSQL => "postgresql",
        StorageEngineType::MySQL => "mysql",
    }
}

// Custom parser for storage engine to handle hyphenated and non-hyphenated aliases
pub fn parse_storage_engine(engine: &str) -> Result<StorageEngineType, String> {
    match engine.to_lowercase().as_str() {
        "sled" => Ok(StorageEngineType::Sled),
        "rocksdb" | "rocks-db" => Ok(StorageEngineType::RocksDB),
        "tikv" => Ok(StorageEngineType::TiKV),
        "inmemory" | "in-memory" => Ok(StorageEngineType::InMemory),
        "redis" => Ok(StorageEngineType::Redis),
        "postgres" | "postgresql" | "postgre-sql" => Ok(StorageEngineType::PostgreSQL),
        "mysql" | "my-sql" => Ok(StorageEngineType::MySQL),
        "config" | "configuration" => Err("Use 'save configuration' or 'save config' for configuration saving".to_string()),
        _ => Err(format!(
            "Invalid storage engine: {}. Supported: sled, rocksdb, rocks-db, inmemory, in-memory, redis, postgres, postgresql, postgre-sql, mysql, my-sql",
            engine
        )),
    }
}

/// Parses the 'show' command and its subcommands.
pub fn parse_show_command(args: &[String]) -> Result<CommandType, anyhow::Error> {
    if args.len() < 2 {
        return Err(anyhow!("Missing subcommand for 'show'. Available: storage, config, plugins"));
    }
    match args[1].as_str() {
        "storage" => Ok(CommandType::Show(ShowAction::Storage)),
        "config" => {
            if args.len() < 3 {
                return Err(anyhow!("Missing subcommand for 'show config'. Available: all, rest, storage, main"));
            }
            let config_type = match args[2].as_str() {
                "all" => ConfigAction::All,
                "rest" => ConfigAction::Rest,
                "storage" => ConfigAction::Storage,
                "main" => ConfigAction::Main,
                _ => return Err(anyhow!("Unknown subcommand for 'show config': {}", args[2])),
            };
            Ok(CommandType::Show(ShowAction::Config { config_type }))
        }
        "plugins" => Ok(CommandType::Show(ShowAction::Plugins)),
        _ => Err(anyhow!("Unknown subcommand for 'show': {}", args[1])),
    }
}


use anyhow::{Result, Context, anyhow}; // Added `anyhow` macro import
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex, OnceCell};
use tokio::task::JoinHandle;
use tokio::fs as tokio_fs;
use std::pin::Pin;
use std::path::{PathBuf, Path};
use std::io::{self, Write};
use std::collections::HashMap;
use std::fs;
use std::process;
use log::{info, error, warn, debug};
use tokio::time::{sleep, Duration as TokioDuration};
use serde_json::{self, Value};
use sysinfo::{System, Pid, ProcessesToUpdate};
use zmq::{Context as ZmqContext, SocketType};

use lib::commands::{CommandType, ShowAction,  ConfigAction};
use lib::config::{StorageConfig, SelectedStorageConfig, 
                         get_default_rest_port_from_config,
                         daemon_api_storage_engine_type_to_string, 
                         load_storage_config_from_yaml, 
                         load_main_daemon_config,
                         DEFAULT_CONFIG_ROOT_DIRECTORY_STR,
                         DAEMON_REGISTRY_DB_PATH,
                         DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
                         CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,};
use lib::config::{StorageEngineType};
use lib::daemon::daemon_management::{ start_graphdb_daemon_core };
use crossterm::style::{self, Stylize};
use crossterm::terminal::{Clear, ClearType, size as terminal_size};
use crossterm::execute;
use crossterm::cursor::MoveTo;
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::cli::handlers_storage::{ start_storage_interactive, stop_storage_interactive };

pub type StartStorageFn = fn(
    Option<u16>,
    Option<PathBuf>,
    Option<StorageConfig>,
    Option<String>,
    Arc<TokioMutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>>;

pub type StopStorageFn = fn(
    Option<u16>,
    Arc<TokioMutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>>;


// Global OnceCell to hold the required function pointers
// These must be set once by main.rs before any command execution.

pub static START_STORAGE_FN_SINGLETON: OnceCell<StartStorageFn> = OnceCell::const_new();
pub static STOP_STORAGE_FN_SINGLETON: OnceCell<StopStorageFn> = OnceCell::const_new();

/// Helper function to retrieve StartStorageFn, panics if not initialized.
pub fn get_start_storage_fn() -> StartStorageFn {
    *START_STORAGE_FN_SINGLETON
        .get()
        .expect("StartStorageFn must be initialized before command execution.")
}

/// Helper function to retrieve StopStorageFn, panics if not initialized.
pub fn get_stop_storage_fn() -> StopStorageFn {
    *STOP_STORAGE_FN_SINGLETON
        .get()
        .expect("StopStorageFn must be initialized before command execution.")
}


// Assuming the original functions (not shown here) have signatures like:
// pub async fn start_storage_interactive(...) -> Result<()> { ... }

/// Adapter function to wrap start_storage_interactive's returned future in Pin<Box<...>>.
#[allow(clippy::too_many_arguments)] // This signature is required by the StartStorageFn type
pub fn adapt_start_storage(
    port: Option<u16>,
    config_path: Option<PathBuf>,
    config: Option<StorageConfig>,
    command_name: Option<String>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
    Box::pin(start_storage_interactive(
        port,
        config_path,
        config,
        command_name,
        shutdown_tx,
        handle,
        port_arc,
    ))
}

/// Adapter function to wrap stop_storage_interactive's returned future in Pin<Box<...>>.
pub fn adapt_stop_storage(
    port: Option<u16>,
    shutdown_tx: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>> {
    Box::pin(stop_storage_interactive(
        port,
        shutdown_tx,
        handle,
        port_arc,
    ))
}


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

pub async fn retry_operation<F, Fut, T>(operation: F, max_attempts: u8, desc: &str) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
{
    for attempt in 0..max_attempts {
        match operation().await {
            Ok(result) => {
                log::debug!("Successfully completed {} on attempt {}", desc, attempt + 1);
                return Ok(result);
            }
            Err(e) => {
                log::error!("Failed to {} (attempt {}/{}): {}", desc, attempt + 1, max_attempts, e);
                if attempt + 1 >= max_attempts {
                    return Err(e).context(format!("Failed to {} after {} attempts", desc, max_attempts));
                }
                sleep(TokioDuration::from_millis(500)).await;
            }
        }
    }
    unreachable!()
}

/// Helper function to format engine-specific configuration details
/// Formats the engine configuration into a vector of strings for display.
pub fn format_engine_config(config: &StorageConfig, daemon_port: u16) -> Vec<String> {
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
            StorageEngineType::RocksDB | StorageEngineType::Sled | StorageEngineType::TiKV | StorageEngineType::Hybrid => {
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
                // Use the provided daemon_port instead of storage_inner.port
                lines.push(format!("Port: {}", daemon_port));
            },
            StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                // Database storage engines
                if let Some(host) = &storage_inner.host {
                    lines.push(format!("Host: {}", host));
                } else {
                    lines.push("Host: Not specified".to_string());
                }
                // Use the provided daemon_port instead of storage_inner.port
                lines.push(format!("Port: {}", daemon_port));
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
                // Use the provided daemon_port instead of storage_inner.port
                lines.push(format!("Port: {}", daemon_port));
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
        StorageEngineType::Hybrid => "hybrid",
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
        "hybrid" => Ok(StorageEngineType::Hybrid),
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

// Helper function to convert SelectedStorageConfig to HashMap<String, Value>
pub fn selected_storage_config_to_hashmap(config: &SelectedStorageConfig) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    map.insert(
        "storage_engine_type".to_string(),
        Value::String(config.storage_engine_type.to_string().to_lowercase()),
    );
    if let Some(path) = &config.storage.path {
        map.insert("path".to_string(), Value::String(path.to_string_lossy().to_string()));
    }
    if let Some(host) = &config.storage.host {
        map.insert("host".to_string(), Value::String(host.clone()));
    }
    if let Some(port) = config.storage.port {
        map.insert("port".to_string(), Value::Number(port.into()));
    }
    if let Some(username) = &config.storage.username {
        map.insert("username".to_string(), Value::String(username.clone()));
    }
    if let Some(password) = &config.storage.password {
        map.insert("password".to_string(), Value::String(password.clone()));
    }
    if let Some(database) = &config.storage.database {
        map.insert("database".to_string(), Value::String(database.clone()));
    }
    if let Some(pd_endpoints) = &config.storage.pd_endpoints {
        map.insert("pd_endpoints".to_string(), Value::String(pd_endpoints.clone()));
    }
    map
}

// Helper function to load TiKV PD port from configuration
pub async fn load_tikv_pd_port() -> Option<u16> {
    let config_path = PathBuf::from("./storage_daemon_server/storage_config_tikv.yaml");
    match load_storage_config_from_yaml(Some(config_path)).await {
        Ok(config) => {
            config.engine_specific_config
                .and_then(|c| c.storage.pd_endpoints)
                .and_then(|pd| pd.split(':').last().and_then(|p| p.parse::<u16>().ok()))
        }
        Err(e) => {
            warn!("Failed to load TiKV config: {}", e);
            None
        }
    }
}

pub async fn handle_internal_daemon_run(
    is_rest_api_run: bool,
    is_storage_daemon_run: bool,
    internal_port: Option<u16>,
    internal_storage_config_path: Option<PathBuf>,
    internal_storage_engine: Option<StorageEngineType>, // Fixed parameter syntax
) -> Result<(), anyhow::Error> {
    if is_rest_api_run {
        let daemon_listen_port = internal_port.unwrap_or_else(get_default_rest_port_from_config);
        let (tx_shutdown, rx_shutdown) = tokio::sync::oneshot::channel::<()>();
        let rest_api_shutdown_tx_opt = Arc::new(TokioMutex::new(Some(tx_shutdown)));
        let rest_api_port_arc = Arc::new(TokioMutex::new(None));
        let rest_api_handle = Arc::new(TokioMutex::new(None));
        info!("[DAEMON PROCESS] Starting REST API server (daemonized) on port {}...", daemon_listen_port);
        let result = crate::cli::handlers::start_rest_api_interactive(
            Some(daemon_listen_port),
            None,
            rest_api_shutdown_tx_opt.clone(),
            rest_api_port_arc.clone(),
            rest_api_handle.clone(),
        ).await;
        if let Err(e) = result {
            error!("[DAEMON PROCESS] REST API server failed: {:?}", e);
            return Err(e);
        }
        info!("[DAEMON PROCESS] REST API server (daemonized) stopped.");
        Ok(())
    } else if is_storage_daemon_run {
        let daemon_listen_port = internal_port.unwrap_or_else(|| {
            // Fixed: added .await and proper error handling
            match tokio::runtime::Handle::try_current() {
                Ok(_) => {
                    // We're in an async context, but can't await here in a closure
                    // Use a default value instead
                    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS
                }
                Err(_) => CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS
            }
        });
        let storage_config_path = internal_storage_config_path.unwrap_or_else(|| {
            PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
                .join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)
        });
        let storage_daemon_shutdown_tx_opt = Arc::new(TokioMutex::new(None));
        let storage_daemon_port_arc = Arc::new(TokioMutex::new(None));
        let storage_daemon_handle = Arc::new(TokioMutex::new(None));
        info!("[DAEMON PROCESS] Starting Storage daemon (daemonized) on port {}...", daemon_listen_port);
        let result = crate::cli::handlers::start_storage_interactive(
            Some(daemon_listen_port),
            Some(storage_config_path),
            None,
            None,
            storage_daemon_shutdown_tx_opt,
            storage_daemon_handle,
            storage_daemon_port_arc,
        ).await;
        if let Err(e) = result {
            error!("[DAEMON PROCESS] Storage daemon failed: {:?}", e);
            return Err(e);
        }
        info!("[DAEMON PROCESS] Storage daemon (daemonized) stopped.");
        Ok(())
    } else {
        let main_app_config = load_main_daemon_config(None)
            .unwrap_or_else(|e| {
                error!("[DAEMON PROCESS] Could not load main app config: {}. Using default daemon port.", e);
                Default::default()
            });
        let daemon_listen_port = internal_port.unwrap_or(main_app_config.default_port);
        info!("[DAEMON PROCESS] Starting GraphDB Daemon (daemonized) on port {}...", daemon_listen_port);
        let result = start_graphdb_daemon_core(daemon_listen_port).await;
        if let Err(e) = result {
            error!("[DAEMON PROCESS] GraphDB Daemon failed: {:?}", e);
            return Err(e);
        }
        info!("[DAEMON PROCESS] GraphDB Daemon (daemonized) stopped.");
        Ok(())
    }
}

// Helper function to map engine type string to StorageEngineType
pub fn map_engine_type_str(engine_str: &str) -> StorageEngineType {
    match engine_str.to_lowercase().as_str() {
        "hybrid" => StorageEngineType::Hybrid,
        "sled" => StorageEngineType::Sled,
        "rocksdb" => StorageEngineType::RocksDB,
        "tikv" => StorageEngineType::TiKV,
        "inmemory" => StorageEngineType::InMemory,
        "redis" => StorageEngineType::Redis,
        "postgresql" => StorageEngineType::PostgreSQL,
        "mysql" => StorageEngineType::MySQL,
        _ => {
            warn!("Unknown engine type '{}', defaulting to Sled", engine_str);
            StorageEngineType::Sled
        }
    }
}

/// Returns the current active storage daemon port from the registry.
/// Prioritizes healthy (zmq_ready + engine_synced) storage daemons.
/// Falls back gracefully if nothing is perfect.
pub async fn get_current_storage_port() -> u16 {
    // GLOBAL_DAEMON_REGISTRY.get().await returns &AsyncRegistryWrapper
    let registry = GLOBAL_DAEMON_REGISTRY.get().await;
    
    let all_daemons = match registry.get_all_daemon_metadata().await {
        Ok(daemons) => daemons,
        Err(e) => {
            warn!("Failed to read daemon metadata: {} — using fallback port 8052", e);
            return 8052;
        }
    };
    
    // 1. Prefer fully healthy storage daemon
    for meta in &all_daemons {
        if meta.service_type == "storage" && meta.zmq_ready && meta.engine_synced {
            info!("Using healthy storage daemon on port {} (ZMQ+sync ready)", meta.port);
            return meta.port;
        }
    }
    
    // 2. Fallback: any running storage daemon
    for meta in &all_daemons {
        if meta.service_type == "storage" && meta.pid > 0 {
            info!("Using running storage daemon on port {} (fallback)", meta.port);
            return meta.port;
        }
    }
    
    // 3. Ultimate fallback
    warn!("No storage daemon found in registry — defaulting to port 8052");
    8052
}

/// Sync version for Lazy — safe, no block_on on current runtime
/// Sync version — safe to call from Lazy, no nested runtime
pub fn get_current_storage_port_sync() -> u16 {
    use std::sync::OnceLock;
    static CACHED_PORT: OnceLock<u16> = OnceLock::new();
    
    // Fast path — return cached value
    if let Some(port) = CACHED_PORT.get() {
        return *port;
    }
    
    // Slow path — spawn a temporary runtime
    let port = std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let registry = GLOBAL_DAEMON_REGISTRY.get().await;
                
                let all = match registry.get_all_daemon_metadata().await {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("Failed to get daemon metadata: {} — using fallback port 8052", e);
                        return 8052;
                    }
                };
                
                // Prefer healthy storage daemon
                for meta in &all {
                    if meta.service_type == "storage" && meta.zmq_ready && meta.engine_synced {
                        info!("Using healthy storage daemon on port {}", meta.port);
                        return meta.port;
                    }
                }
                
                // Fallback to any storage daemon
                for meta in &all {
                    if meta.service_type == "storage" {
                        info!("Using storage daemon on port {} (fallback)", meta.port);
                        return meta.port;
                    }
                }
                
                warn!("No storage daemon found — using default port 8052");
                8052
            })
    })
    .join()
    .unwrap_or(8052);
    
    let _ = CACHED_PORT.set(port);
    port
}
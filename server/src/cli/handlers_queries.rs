// server/src/cli/handlers_queries.rs
// Created: 2025-08-10 - Implemented handlers for query-related CLI commands
// Corrected: 2025-08-10 - Removed .await from synchronous KV store methods and corrected argument types
// ADDED: 2025-08-13 - Added handle_exec_command, handle_query_command, and handle_kv_command
// FIXED: 2025-08-13 - Resolved E0277 by using {:?} formatter for QueryType
// FIXED: 2025-08-13 - Removed duplicate handle_kv_command with Kv to align with cli.rs
// ADDED: 2025-08-31 - Enhanced handle_exec_command to validate non-empty commands and execute via QueryExecEngine::execute_command
// ADDED: 2025-08-31 - Updated handle_query_command to support Cypher, SQL, and GraphQL using parse_query_from_string and specific QueryExecEngine methods
// ADDED: 2025-08-31 - Updated handle_kv_command to use parse_kv_operation, support flagless and flagged arguments, and align with cli.rs validation
// FIXED: 2025-08-31 - Added error handling for empty inputs and invalid operations, matching cli.rs error messaging style
// UPDATED: 2025-09-05 - Enhanced initialize_storage_for_query for sled persistence with safer lock cleanup and better logging
// FIXED: 2025-09-05 - Resolved StorageConfig type mismatch, function signature issues, removed flush_every_ms, and fixed PathBuf errors
// FIXED: 2025-09-05 - Corrected max_open_files conversion from Option<u64> to Option<i32>
// UPDATED: 2025-09-06 - Updated handle_kv_command to verify persistence for 'set' operation
// FIXED: 2025-09-06 - Added AsyncWriteExt and AsyncReadExt imports for TcpStream methods
// FIXED: 2025-09-06 - Corrected GLOBAL_DAEMON_REGISTRY access using get().await
// FIXED: 2025-09-06 - Updated handle_kv_sled_tcp and initialize_storage_for_query to use running Sled daemon ports (8051 or 8050)
// FIXED: 2025-09-06 - Ensured initialize_storage_for_query uses port 8051 for new daemons to align with cluster_range

use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::fs;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tokio::time::{self, timeout, Duration as TokioDuration};
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt}; // For write_all, flush, and read
use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::storage_engine::config::{StorageConfig as LibStorageConfig, SledConfig, StorageEngineType, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, default_data_directory, default_log_directory};
use lib::storage_engine::SledStorage;
use lib::storage_engine::TikvStorage;
use lib::storage_engine::storage_engine::{AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use super::commands::parse_kv_operation;
use crate::cli::config::{StorageConfig as CliStorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml};
use crate::cli::handlers_storage::{start_storage_interactive, stop_storage_interactive};
use crate::cli::daemon_management::is_storage_daemon_running;
pub use models::errors::GraphError;
pub use crate::cli::config_defaults::default_config_root_directory;

// Helper function to convert CliStorageConfig to LibStorageConfig
fn convert_to_lib_storage_config(cli_config: CliStorageConfig) -> LibStorageConfig {
    LibStorageConfig {
        config_root_directory: cli_config.config_root_directory.unwrap_or_else(default_config_root_directory),
        data_directory: cli_config.data_directory.unwrap_or_else(default_data_directory),
        log_directory: cli_config.log_directory.map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(default_log_directory),
        default_port: cli_config.default_port,
        cluster_range: cli_config.cluster_range,
        max_disk_space_gb: cli_config.max_disk_space_gb,
        min_disk_space_gb: cli_config.min_disk_space_gb,
        use_raft_for_scale: cli_config.use_raft_for_scale,
        storage_engine_type: cli_config.storage_engine_type,
        connection_string: None,
        engine_specific_config: cli_config.engine_specific_config.map(|sel| {
            let mut map = HashMap::new();
            map.insert("storage_engine_type".to_string(), Value::String(sel.storage_engine_type.to_string()));
            if let Some(path) = sel.storage.path {
                map.insert("path".to_string(), Value::String(path.to_string_lossy().into_owned()));
            }
            if let Some(host) = sel.storage.host {
                map.insert("host".to_string(), Value::String(host));
            }
            if let Some(port) = sel.storage.port {
                map.insert("port".to_string(), Value::Number(port.into()));
            }
            if let Some(username) = sel.storage.username {
                map.insert("username".to_string(), Value::String(username));
            }
            if let Some(password) = sel.storage.password {
                map.insert("password".to_string(), Value::String(password));
            }
            if let Some(database) = sel.storage.database {
                map.insert("database".to_string(), Value::String(database));
            }
            if let Some(pd_endpoints) = sel.storage.pd_endpoints {
                map.insert("pd_endpoints".to_string(), Value::String(pd_endpoints));
            }
            map
        }),
        max_open_files: Some(
            if cli_config.max_open_files <= i32::MAX as u64 {
                cli_config.max_open_files as i32
            } else {
                i32::MAX
            }
        ),
    }
}

/// Helper function to execute a query and print the result or error.
async fn execute_and_print(engine: &Arc<QueryExecEngine>, query_string: &str) -> Result<()> {
    match engine.execute(query_string).await {
        Ok(result) => {
            println!("Query Result:\n{}", serde_json::to_string_pretty(&result)?);
        }
        Err(e) => {
            eprintln!("Error executing query: {}", e);
            return Err(e);
        }
    }
    Ok(())
}

/// A unified handler for various query types (Cypher, SQL, GraphQL),
/// which can also take an optional language flag to explicitly route the query.
/// This function now directly uses the `QueryExecEngine` to run the query.
pub async fn handle_unified_query(
    engine: Arc<QueryExecEngine>,
    query_string: String,
    language: Option<String>,
) -> Result<()> {
    println!("Executing query: {}", query_string);

    let normalized_query = query_string.trim().to_uppercase();

    // 1. Determine the query type, either from the flag or by inference
    let query_type = if let Some(lang) = language {
        let lang_lower = lang.to_lowercase();
        match lang_lower.as_str() {
            "cypher" => {
                if normalized_query.starts_with("MATCH") || normalized_query.starts_with("CREATE") || normalized_query.starts_with("MERGE") {
                    Ok("cypher")
                } else {
                    Err(anyhow!("Syntax conflict: --language cypher provided, but query does not appear to be a valid Cypher statement."))
                }
            },
            "sql" | "graphql" => {
                Err(anyhow!("Unsupported language flag: {}. Only a subset of Cypher is currently supported by the QueryExecEngine.", lang_lower))
            },
            _ => Err(anyhow!("Unsupported language flag: {}. Supported language is: cypher.", lang_lower))
        }
    } else {
        if normalized_query.starts_with("MATCH") || normalized_query.starts_with("CREATE") || normalized_query.starts_with("MERGE") {
            Ok("cypher")
        } else {
            Err(anyhow!("Could not determine query type from input string. Please use a recognized Cypher format or an explicit --language flag."))
        }
    };

    // 2. Execute the query using the QueryExecEngine
    match query_type {
        Ok("cypher") => {
            info!("Detected Cypher query. Executing directly via QueryExecEngine.");
            execute_and_print(&engine, &query_string).await
        },
        Err(e) => {
            Err(e)
        }
        _ => unreachable!(),
    }
}

/// A new function to handle a generic query from interactive mode.
/// It will attempt to identify the query language and then dispatch it.
pub async fn handle_interactive_query(engine: Arc<QueryExecEngine>, query_string: String) -> Result<()> {
    let normalized_query = query_string.trim().to_uppercase();
    info!("Attempting to identify interactive query: '{}'", normalized_query);

    if normalized_query == "EXIT" || normalized_query == "QUIT" {
        return Ok(());
    }

    handle_unified_query(engine, query_string, None).await
}

/// Handles the `exec` command to execute a command on the query engine.
pub async fn handle_exec_command(engine: Arc<QueryExecEngine>, command: String) -> Result<()> {
    info!("Executing command '{}' on QueryExecEngine", command);
    println!("Executing command '{}'", command);

    if command.trim().is_empty() {
        return Err(anyhow!("Exec command cannot be empty. Usage: exec --command <command>"));
    }

    let result = engine
        .execute_command(&command)
        .await
        .map_err(|e| anyhow!("Failed to execute command '{}': {}", command, e))?;

    println!("Command Result: {}", result);
    Ok(())
}

/// Handles the `query` command to execute a query on the query engine.
pub async fn handle_query_command(engine: Arc<QueryExecEngine>, query: String) -> Result<()> {
    info!("Executing query '{}' on QueryExecEngine", query);
    println!("Executing query '{}'", query);

    if query.trim().is_empty() {
        return Err(anyhow!("Query cannot be empty. Usage: query --query <query>"));
    }

    let query_type = parse_query_from_string(&query)
        .map_err(|e| anyhow!("Failed to parse query '{}': {}", query, e))?;

    let result = match query_type {
        QueryType::Cypher => {
            info!("Detected Cypher query");
            engine
                .execute_cypher(&query)
                .await
                .map_err(|e| anyhow!("Failed to execute Cypher query '{}': {}", query, e))?
        }
        QueryType::SQL => {
            info!("Detected SQL query");
            engine
                .execute_sql(&query)
                .await
                .map_err(|e| anyhow!("Failed to execute SQL query '{}': {}", query, e))?
        }
        QueryType::GraphQL => {
            info!("Detected GraphQL query");
            engine
                .execute_graphql(&query)
                .await
                .map_err(|e| anyhow!("Failed to execute GraphQL query '{}': {}", query, e))?
        }
    };

    println!("Query Result:\n{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

/// Handles the `kv` command for key-value operations on the query engine.
pub async fn handle_kv_command(engine: Arc<QueryExecEngine>, operation: String, key: String, value: Option<String>) -> Result<()> {
    let validated_op = parse_kv_operation(&operation)
        .map_err(|e| anyhow!("Invalid KV operation: {}", e))?;

    // Check the storage engine type from the configuration
    let config = crate::cli::config::load_cli_config()?;
    if config.storage.storage_engine_type == Some(StorageEngineType::Sled) {
        info!("Using Sled-specific TCP handler for KV operation: {}", validated_op);
        handle_kv_sled_tcp(key, value, &validated_op).await
    } else {
        match validated_op.as_str() {
            "get" => {
                info!("Executing Key-Value GET for key: {}", key);
                let result = engine
                    .kv_get(&key)
                    .await
                    .map_err(|e| anyhow!("Failed to get key '{}': {}", key, e))?;
                match result {
                    Some(val) => {
                        println!("Value for key '{}': {}", key, val);
                        Ok(())
                    }
                    None => {
                        println!("Key '{}' not found", key);
                        Ok(())
                    }
                }
            }
            "set" => {
                let value = value.ok_or_else(|| {
                    anyhow!("Missing value for 'kv set' command. Usage: kv set <key> <value> or kv set --key <key> --value <value>")
                })?;
                info!("Executing Key-Value SET for key: {}, value: {}", key, value);
                let stored_value = engine
                    .kv_set(&key, &value)
                    .await
                    .map_err(|e| anyhow!("Failed to set key '{}': {}", key, e))?;
                println!("Successfully set and verified key '{:?}' to '{:?}'", key, stored_value);
                Ok(())
            }
            "delete" => {
                info!("Executing Key-Value DELETE for key: {}", key);
                let existed = engine
                    .kv_delete(&key)
                    .await
                    .map_err(|e| anyhow!("Failed to delete key '{}': {}", key, e))?;
                if existed {
                    println!("Successfully deleted key '{}'", key);
                } else {
                    println!("Key '{}' not found", key);
                }
                Ok(())
            }
            _ => {
                Err(anyhow!("Unsupported KV operation: '{}'. Supported operations: get, set, delete", operation))
            }
        }
    }
}

// Type aliases for the function pointers to avoid impl Trait in fn pointer
type StartStorageFn = fn(
    Option<u16>,
    Option<PathBuf>,
    Option<LibStorageConfig>,
    Option<String>,
    Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>;

type StopStorageFn = fn(
    Option<u16>,
    Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>;

/// This function ensures the `GLOBAL_STORAGE_ENGINE_MANAGER` is initialized
/// before any query-related commands are executed.
/// It loads the configuration and sets up the manager, but does not manage daemons.
pub async fn initialize_storage_for_query(
    start_storage_interactive: fn(
        Option<u16>,
        Option<PathBuf>,
        Option<CliStorageConfig>,
        Option<String>,
        Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
        Arc<TokioMutex<Option<JoinHandle<()>>>>,
        Arc<TokioMutex<Option<u16>>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>,
    stop_storage_interactive: fn(
        Option<u16>,
        Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
        Arc<TokioMutex<Option<JoinHandle<()>>>>,
        Arc<TokioMutex<Option<u16>>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), anyhow::Error>> + Send>>,
) -> Result<(), anyhow::Error> {
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_some() {
        debug!("StorageEngineManager is already initialized. Skipping initialization.");
        return Ok(());
    }

    info!("Initializing StorageEngineManager for non-interactive command execution.");
    println!("===> Initializing Storage Engine...");

    let running_daemons: Vec<DaemonMetadata> = GLOBAL_DAEMON_REGISTRY
        .get()
        .await
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|d| d.service_type == "storage" && d.engine_type == Some(StorageEngineType::Sled.to_string()))
        .collect();

    if let Some(daemon) = running_daemons.iter().find(|d| d.port == 8049).or_else(|| running_daemons.iter().find(|d| d.port == 8050)) {
        info!("Found running Sled storage daemon on port {}, connecting to it...", daemon.port);

        let engine_type = daemon.engine_type
            .as_ref()
            .and_then(|s| s.parse::<StorageEngineType>().ok())
            .ok_or_else(|| anyhow!("Failed to parse engine type from running daemon: {}", daemon.engine_type.as_deref().unwrap_or("n/a")))?;
        
        let data_dir_path = PathBuf::from(daemon.config_path.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)));
        let config_path = daemon.config_path.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));

        let cli_config = match load_storage_config_from_yaml(Some(config_path.clone())) {
            Ok(config) => {
                info!("Loaded storage config for engine {:?}: {:?}", engine_type, config);
                config
            },
            Err(e) => {
                warn!("Failed to load config from {:?}, using default: {}", config_path, e);
                CliStorageConfig::default()
            }
        };
        let lib_config = convert_to_lib_storage_config(cli_config.clone());

        match engine_type {
            StorageEngineType::Sled => {
                let lock_path = data_dir_path.join("db.lck");
                if lock_path.exists() {
                    info!("Removing Sled lock file at {:?}", lock_path);
                    fs::remove_file(&lock_path).context(format!("Failed to remove Sled lock file at {:?}", lock_path))?;
                }
            },
            StorageEngineType::TiKV => {
                if let Err(e) = TikvStorage::force_unlock().await {
                    warn!("Failed to clean up TiKV lock files at {:?}: {}", data_dir_path, e);
                }
            },
            _ => {
                debug!("No specific lock file cleanup implemented for engine type: {:?}", engine_type);
            }
        }

        let manager = StorageEngineManager::new(engine_type.clone(), &config_path, false)
            .await
            .context(format!("Failed to initialize StorageEngineManager for engine {:?}", engine_type))?;

        GLOBAL_STORAGE_ENGINE_MANAGER
            .set(Arc::new(AsyncStorageEngineManager::from_manager(
                Arc::try_unwrap(manager)
                    .map_err(|_| GraphError::ConfigurationError("Failed to unwrap Arc<StorageEngineManager>: multiple references exist".to_string()))?
            )))
            .context("Failed to set GLOBAL_STORAGE_ENGINE_MANAGER")?;

        info!("Successfully initialized storage engine {:?}", engine_type);
        println!("===> Storage Engine initialized successfully.");
    } else {
        info!("No running Sled daemon found, starting a new storage daemon on port 8051...");
        println!("===> No running Sled daemon found, starting a new storage daemon...");
        let cwd = std::env::current_dir().context("Failed to get current working directory")?;
        let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

        let mut cli_config = match load_storage_config_from_yaml(Some(config_path.clone())) {
            Ok(config) => {
                info!("Successfully loaded existing storage config: {:?}", config);
                config
            },
            Err(e) => {
                warn!("Failed to load existing config from {:?}, using default values. Error: {}", config_path, e);
                CliStorageConfig::default()
            }
        };
        // Explicitly set port to 8051 to align with cluster_range
        cli_config.default_port = 8049;
        cli_config.cluster_range = "8049".to_string(); // Align with cluster_range
        let lib_config = convert_to_lib_storage_config(cli_config.clone());

        let storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>> = Arc::new(TokioMutex::new(None));

        start_storage_interactive(
            Some(8051),
            Some(config_path),
            Some(cli_config),
            None,
            storage_daemon_shutdown_tx_opt,
            storage_daemon_handle,
            storage_daemon_port_arc,
        ).await.context("Failed to start storage daemon on port 8051")?;

        info!("Successfully started a new storage daemon on port 8051.");
        println!("===> New storage daemon started successfully on port 8051.");
    }

    Ok(())
}

async fn handle_kv_sled_tcp(key: String, value: Option<String>, operation: &str) -> Result<()> {
    const READ_TIMEOUT_SECS: u64 = 5;
    const MAX_MESSAGE_SIZE: usize = 10 * 15024 * 15024; // 10MB max message size
    const MAX_RETRIES: u32 = 2;

    // Load storage configuration
    let cli_config = load_storage_config_from_yaml(None)
        .map_err(|e| anyhow!("Failed to load storage config: {}", e))?;
    let default_port = cli_config.default_port;
    let cluster_range_start = cli_config.cluster_range.parse::<u16>().ok();

    // Find a running Sled daemon
    let registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let daemons = registry
        .get_all_daemon_metadata()
        .await
        .map_err(|e| anyhow!("Failed to retrieve daemon metadata: {}", e))?
        .into_iter()
        .filter(|metadata| metadata.engine_type == Some(StorageEngineType::Sled.to_string()))
        .collect::<Vec<_>>();

    let mut attempt = 0;
    while attempt < MAX_RETRIES {
        let daemon = daemons
            .iter()
            .find(|d| {
                d.port == default_port || cluster_range_start.map_or(false, |range_start| d.port == range_start)
            })
            .or_else(|| daemons.first());
        let port = match daemon {
            Some(metadata) => metadata.port,
            None => {
                return Err(anyhow!("No running Sled daemon found. Please start a daemon with 'storage start'"));
            }
        };

        // Connect to daemon
        let addr = format!("127.0.0.1:{}", port);
        info!("Connecting to Sled daemon at {}", addr);
        let mut stream = match TcpStream::connect(&addr).await {
            Ok(stream) => stream,
            Err(e) => {
                warn!("Failed to connect to {}: {}. Retrying...", addr, e);
                attempt += 1;
                continue;
            }
        };

        // Prepare request
        let request = match operation {
            "set" => {
                if let Some(ref value) = value {
                    json!({ "command": "set_key", "key": key, "value": value })
                } else {
                    return Err(anyhow!("Missing value for 'set' operation"));
                }
            }
            "get" => json!({ "command": "get_key", "key": key }),
            "delete" => json!({ "command": "delete_key", "key": key }),
            _ => return Err(anyhow!("Unsupported operation: {}", operation)),
        };

        // Send request with length prefix
        let request_data = serde_json::to_vec(&request)
            .map_err(|e| anyhow!("Failed to serialize request: {}", e))?;
        let request_length = request_data.len() as u32;
        let length_bytes = request_length.to_be_bytes();
        stream.write_all(&length_bytes).await
            .map_err(|e| anyhow!("Failed to send request length to {}: {}", addr, e))?;
        stream.write_all(&request_data).await
            .map_err(|e| anyhow!("Failed to send request to {}: {}", addr, e))?;
        stream.flush().await
            .map_err(|e| anyhow!("Failed to flush request to {}: {}", addr, e))?;

        // Read response
        let mut buffer = Vec::with_capacity(4096);
        let response = match timeout(TokioDuration::from_secs(READ_TIMEOUT_SECS), async {
            let mut length_buf = [0u8; 4];
            stream.read_exact(&mut length_buf).await
                .map_err(|e| anyhow!("Failed to read response length from {}: {}", addr, e))?;
            let response_length = u32::from_be_bytes(length_buf) as usize;

            if response_length > MAX_MESSAGE_SIZE {
                error!("Invalid response size {} exceeds maximum {} from {}", response_length, MAX_MESSAGE_SIZE, addr);
                return Err(anyhow!("Response size {} exceeds maximum {}", response_length, MAX_MESSAGE_SIZE));
            }
            if response_length == 0 {
                error!("Zero-length response from {}", addr);
                return Err(anyhow!("Received zero-length response"));
            }
            debug!("Received length prefix: {:?}", length_buf);

            buffer.clear();
            buffer.reserve(response_length.min(MAX_MESSAGE_SIZE));
            let mut bytes_read = 0;
            while bytes_read < response_length {
                let chunk_size = (response_length - bytes_read).min(4096);
                let mut chunk = vec![0u8; chunk_size];
                let n = stream.read_exact(&mut chunk).await
                    .map_err(|e| anyhow!("Failed to read response chunk from {}: {}", addr, e))?;
                if n == 0 {
                    return Err(anyhow!("Unexpected EOF from {}", addr));
                }
                buffer.extend_from_slice(&chunk[..n]);
                bytes_read += n;
            }
            debug!("Received response of length {} from {}", buffer.len(), addr);

            serde_json::from_slice::<Value>(&buffer)
                .map_err(|e| anyhow!("Failed to parse response from {}: {}", addr, e))
        })
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                warn!("Response error from {}: {}. Retrying...", addr, e);
                attempt += 1;
                continue;
            }
            Err(_) => {
                warn!("Timeout reading response from {}. Retrying...", addr);
                attempt += 1;
                continue;
            }
        };

        // Shutdown stream
        if let Err(e) = stream.shutdown().await {
            warn!("Failed to shutdown stream to {}: {}", addr, e);
        }

        // Process response
        match response.get("status").and_then(|s| s.as_str()) {
            Some("success") => {
                match operation {
                    "get" => {
                        if let Some(response_value) = response.get("value") {
                            println!(
                                "Key '{}': {}",
                                key,
                                if response_value.is_null() {
                                    "not found".to_string()
                                } else {
                                    response_value.as_str().unwrap_or("").to_string()
                                }
                            );
                        }
                    }
                    "set" => {
                        println!("Set key '{}' successfully", key);
                        // Verify persistence
                        let verify_request = json!({ "command": "get_key", "key": key });
                        let mut verify_stream = TcpStream::connect(&addr).await
                            .map_err(|e| anyhow!("Failed to connect for verification: {}", e))?;
                        let verify_data = serde_json::to_vec(&verify_request)
                            .map_err(|e| anyhow!("Failed to serialize verification request: {}", e))?;
                        let verify_length = verify_data.len() as u32;
                        verify_stream.write_all(&verify_length.to_be_bytes()).await
                            .map_err(|e| anyhow!("Failed to send verification length: {}", e))?;
                        verify_stream.write_all(&verify_data).await
                            .map_err(|e| anyhow!("Failed to send verification request: {}", e))?;
                        verify_stream.flush().await
                            .map_err(|e| anyhow!("Failed to flush verification request: {}", e))?;

                        let mut verify_buffer = Vec::with_capacity(4096);
                        let verify_response = timeout(TokioDuration::from_secs(READ_TIMEOUT_SECS), async {
                            let mut length_buf = [0u8; 4];
                            verify_stream.read_exact(&mut length_buf).await
                                .map_err(|e| anyhow!("Failed to read verification length: {}", e))?;
                            let verify_length = u32::from_be_bytes(length_buf) as usize;

                            if verify_length > MAX_MESSAGE_SIZE {
                                return Err(anyhow!("Verification response size {} exceeds maximum", verify_length));
                            }
                            if verify_length == 0 {
                                return Err(anyhow!("Zero-length verification response"));
                            }

                            verify_buffer.clear();
                            verify_buffer.reserve(verify_length.min(MAX_MESSAGE_SIZE));
                            let mut bytes_read = 0;
                            while bytes_read < verify_length {
                                let chunk_size = (verify_length - bytes_read).min(4096);
                                let mut chunk = vec![0u8; chunk_size];
                                let n = verify_stream.read_exact(&mut chunk).await
                                    .map_err(|e| anyhow!("Failed to read verification chunk: {}", e))?;
                                if n == 0 {
                                    return Err(anyhow!("Unexpected EOF in verification"));
                                }
                                verify_buffer.extend_from_slice(&chunk[..n]);
                                bytes_read += n;
                            }

                            serde_json::from_slice::<Value>(&verify_buffer)
                                .map_err(|e| anyhow!("Failed to parse verification response: {}", e))
                        })
                        .await
                        .map_err(|_| anyhow!("Timeout reading verification response"))??;

                        if let Err(e) = verify_stream.shutdown().await {
                            warn!("Failed to shutdown verification stream: {}", e);
                        }

                        if verify_response.get("status").and_then(|s| s.as_str()) == Some("success") {
                            if let Some(verify_value) = verify_response.get("value") {
                                if verify_value.as_str() == value.as_deref() {
                                    println!("Persistence verified for key '{}'", key);
                                } else {
                                    return Err(anyhow!(
                                        "Persistence verification failed for key '{}'. Expected: '{:?}', Got: '{:?}'",
                                        key, value.as_deref(), verify_value.as_str()
                                    ));
                                }
                            } else {
                                return Err(anyhow!("No value in verification response for key '{}'", key));
                            }
                        } else {
                            return Err(anyhow!(
                                "Verification failed: {}",
                                verify_response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error")
                            ));
                        }
                    }
                    "delete" => println!("Deleted key '{}' successfully", key),
                    _ => {}
                }
                return Ok(());
            }
            Some("error") => {
                let message = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                return Err(anyhow!("Daemon error: {}", message));
            }
            _ => {
                warn!("Invalid response from {}: {:?}", addr, response);
                attempt += 1;
                continue;
            }
        }
    }

    Err(anyhow!("Failed to get valid response after {} attempts", MAX_RETRIES))
}
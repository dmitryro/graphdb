use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::fs;
use tokio::task::{self, JoinHandle};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration};
use lib::daemon::daemon_management::{check_pid_validity, is_port_free};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{StorageEngineType, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, default_data_directory, default_log_directory};
use lib::storage_engine::SledStorage;
use lib::storage_engine::TikvStorage;
use lib::storage_engine::storage_engine::{AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use lib::commands::parse_kv_operation;
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml};
use lib::database::Database;
use crate::cli::handlers_storage::{start_storage_interactive, stop_storage_interactive};
pub use crate::cli::daemon_management::is_storage_daemon_running;
pub use models::errors::GraphError;
pub use lib::config::config_defaults::default_config_root_directory;
use zmq::{Context as ZmqContext};

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

pub async fn handle_unified_query(
    engine: Arc<QueryExecEngine>,
    query_string: String,
    language: Option<String>,
) -> Result<()> {
    info!("Executing query: {}", query_string);

    let normalized_query = query_string.trim().to_uppercase();

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

pub async fn handle_interactive_query(engine: Arc<QueryExecEngine>, query_string: String) -> Result<()> {
    let normalized_query = query_string.trim().to_uppercase();
    info!("Attempting to identify interactive query: '{}'", normalized_query);

    if normalized_query == "EXIT" || normalized_query == "QUIT" {
        return Ok(());
    }

    handle_unified_query(engine, query_string, None).await
}

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

// Helper function to perform a single ZMQ request within a blocking task.
fn do_zmq_request(addr: &str, request_data: &[u8]) -> Result<Value> {
    let zmq_context = zmq::Context::new();
    let client = zmq_context
        .socket(zmq::REQ)
        .context("Failed to create ZMQ socket")?;

    client.set_rcvtimeo((15 * 1000) as i32)
        .context("Failed to set receive timeout")?;
    client.set_sndtimeo((10 * 1000) as i32)
        .context("Failed to set send timeout")?;

    client.connect(addr)
        .context(format!("Failed to connect to {}", addr))?;
    
    debug!("Successfully connected to: {}", addr);

    client.send(request_data, 0)
        .context(format!("Failed to send request to {}", addr))?;

    debug!("Request sent successfully");

    let mut msg = zmq::Message::new();
    client.recv(&mut msg, 0)
        .context(format!("Failed to receive response from {}", addr))?;

    debug!("Received response");
    
    let response: Value = serde_json::from_slice(&msg)
        .context(format!("Failed to deserialize response from {}", addr))?;

    debug!("Parsed response: {:?}", response);

    Ok(response)
}

async fn handle_kv_sled_zmq(key: String, value: Option<String>, operation: &str) -> Result<()> {
    const CONNECT_TIMEOUT_SECS: u64 = 3;
    const REQUEST_TIMEOUT_SECS: u64 = 10;
    const RECEIVE_TIMEOUT_SECS: u64 = 15;
    const MAX_RETRIES: u32 = 3;
    const BASE_RETRY_DELAY_MS: u64 = 500;

    info!("Starting ZMQ KV operation: {} for key: {}", operation, key);

    let registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let daemons = registry
        .get_all_daemon_metadata()
        .await
        .map_err(|e| anyhow!("Failed to retrieve daemon metadata: {}", e))?
        .into_iter()
        .filter(|metadata| metadata.engine_type == Some(StorageEngineType::Sled.to_string()))
        .collect::<Vec<_>>();

    if daemons.is_empty() {
        return Err(anyhow!("No running Sled daemon found. Please start a daemon with 'storage start'"));
    }

    // Select the daemon with the highest port (most recent) for load balancing
    let daemon = daemons.iter().max_by_key(|m| m.port).unwrap_or(daemons.first().unwrap());
    info!("Selected daemon on port: {}", daemon.port);

    // Construct port-specific IPC socket path
    let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);

    // Check if socket file exists
    if !tokio::fs::metadata(&socket_path).await.is_ok() {
        return Err(anyhow!("IPC socket file {} does not exist. Daemon may not be running properly on port {}.", socket_path, daemon.port));
    }

    debug!("Connecting to Sled daemon at: {}", addr);

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

    debug!("Sending request: {:?}", request);
    let request_data = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("Failed to serialize request: {}", e))?;

    // Retry loop
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_RETRIES {
        debug!("Attempt {}/{} to send ZMQ request", attempt, MAX_RETRIES);

        let response_result = tokio::time::timeout(
            TokioDuration::from_secs(CONNECT_TIMEOUT_SECS + REQUEST_TIMEOUT_SECS + RECEIVE_TIMEOUT_SECS),
            tokio::task::spawn_blocking({
                let addr = addr.clone();
                let request_data = request_data.clone();
                move || do_zmq_request(&addr, &request_data)
            })
        )
        .await;

        match response_result {
            Ok(Ok(response)) => {
                // Extract the Value from the Result
                let response_value = response?;
                // Process the response
                match response_value.get("status").and_then(|s| s.as_str()) {
                    Some("success") => {
                        info!("Operation successful");
                        match operation {
                            "get" => {
                                if let Some(response_value) = response_value.get("value") {
                                    let display_value = if response_value.is_null() {
                                        "not found".to_string()
                                    } else {
                                        response_value.as_str().unwrap_or("<non-string value>").to_string()
                                    };
                                    println!("Key '{}': {}", key, display_value);
                                } else {
                                    println!("Key '{}': no value in response", key);
                                }
                            }
                            "set" => {
                                println!("Set key '{}' successfully", key);
                            }
                            "delete" => {
                                println!("Deleted key '{}' successfully", key);
                            }
                            _ => {}
                        }
                        return Ok(());
                    }
                    Some("error") => {
                        let message = response_value.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                        error!("Daemon error: {}", message);
                        return Err(anyhow!("Daemon error: {}", message));
                    }
                    _ => {
                        error!("Invalid response: {:?}", response_value);
                        return Err(anyhow!("Invalid response from {}: {:?}", addr, response_value));
                    }
                }
            }
            Ok(Err(e)) => {
                last_error = Some(e.into());
                warn!("Attempt {}/{} failed: {}", attempt, MAX_RETRIES, last_error.as_ref().unwrap());
            }
            Err(_) => {
                last_error = Some(anyhow!("ZMQ operation timed out after {} seconds", CONNECT_TIMEOUT_SECS + REQUEST_TIMEOUT_SECS + RECEIVE_TIMEOUT_SECS));
                warn!("Attempt {}/{} timed out", attempt, MAX_RETRIES);
            }
        }

        // Exponential backoff
        if attempt < MAX_RETRIES {
            let delay = BASE_RETRY_DELAY_MS * 2u64.pow(attempt - 1);
            debug!("Retrying after {}ms", delay);
            tokio::time::sleep(TokioDuration::from_millis(delay)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("Failed to complete ZMQ operation after {} attempts", MAX_RETRIES)))
}

// Updated main handler function
pub async fn handle_kv_command(engine: Arc<QueryExecEngine>, operation: String, key: String, value: Option<String>) -> Result<()> {
    debug!("In handle_kv_command: operation={}, key={}", operation, key);
    let validated_op = parse_kv_operation(&operation)
        .map_err(|e| anyhow!("Invalid KV operation: {}", e))?;

    let config = lib::config::load_cli_config().await?;
    debug!("Loaded config: {:?}", config);
    if config.storage.storage_engine_type == Some(StorageEngineType::Sled) {
        info!("Using Sled-specific ZeroMQ handler for KV operation: {}", validated_op);
        handle_kv_sled_zmq(key.clone(), value, &validated_op).await?;
        Ok(())
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

async fn check_daemon_health(addr: &str) -> Result<bool> {
    const HEALTH_TIMEOUT_SECS: u64 = 5;
    const MAX_RETRIES: u32 = 2;
    const BASE_RETRY_DELAY_MS: u64 = 200;

    let request = json!({ "command": "ping" });
    let request_data = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("Failed to serialize health check request: {}", e))?;

    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_RETRIES {
        debug!("Health check attempt {}/{} for {}", attempt, MAX_RETRIES, addr);

        let response_result = tokio::time::timeout(
            TokioDuration::from_secs(HEALTH_TIMEOUT_SECS),
            tokio::task::spawn_blocking({
                let addr = addr.to_string();
                let request_data = request_data.clone();
                move || {
                    let zmq_context = zmq::Context::new();
                    let client = zmq_context.socket(zmq::REQ)
                        .map_err(|e| anyhow!("Failed to create ZMQ socket: {}", e))?;

                    client.set_rcvtimeo((HEALTH_TIMEOUT_SECS * 1000) as i32)
                        .map_err(|e| anyhow!("Failed to set receive timeout: {}", e))?;
                    client.set_sndtimeo((HEALTH_TIMEOUT_SECS * 1000) as i32)
                        .map_err(|e| anyhow!("Failed to set send timeout: {}", e))?;

                    client.connect(&addr)
                        .map_err(|e| anyhow!("Failed to connect to {}: {}", addr, e))?;

                    client.send(&request_data, 0)
                        .map_err(|e| anyhow!("Failed to send ping request to {}: {}", addr, e))?;

                    let mut msg = zmq::Message::new();
                    client.recv(&mut msg, 0)
                        .map_err(|e| anyhow!("Failed to receive ping response from {}: {}", addr, e))?;

                    let response: Value = serde_json::from_slice(&msg.to_vec())
                        .map_err(|e| anyhow!("Failed to deserialize ping response from {}: {}", addr, e))?;

                    Ok::<bool, anyhow::Error>(response.get("status").and_then(|s| s.as_str()) == Some("success"))
                }
            })
        )
        .await;

        match response_result {
            Ok(Ok(Ok(true))) => {
                info!("Health check succeeded for {}", addr);
                return Ok(true);
            }
            Ok(Ok(Ok(false))) => {
                last_error = Some(anyhow!("Health check failed: Invalid response from {}", addr));
            }
            Ok(Ok(Err(e))) => {
                last_error = Some(e);
            }
            Ok(Err(e)) => {
                last_error = Some(e.into());
            }
            Err(_) => {
                last_error = Some(anyhow!("Health check timed out after {} seconds", HEALTH_TIMEOUT_SECS));
            }
        }

        if attempt < MAX_RETRIES {
            let delay = BASE_RETRY_DELAY_MS * 2u64.pow(attempt - 1);
            debug!("Retrying health check after {}ms", delay);
            tokio::time::sleep(TokioDuration::from_millis(delay)).await;
        }
    }

    error!("Health check failed after {} attempts", MAX_RETRIES);
    Err(last_error.unwrap_or_else(|| anyhow!("Health check failed after {} attempts", MAX_RETRIES)))
}

// ------------------- Refactored Function -------------------
// Note: This corrected code is a full file and should replace your current one.


type StartStorageFn = fn(
    Option<u16>,
    Option<PathBuf>,
    Option<StorageConfig>,
    Option<String>,
    Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;

type StopStorageFn = fn(
    Option<u16>,
    Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub async fn initialize_storage_for_query(
    start_storage_interactive: StartStorageFn,
    stop_storage_interactive: StopStorageFn,
) -> Result<Arc<QueryExecEngine>, anyhow::Error> {
    // Use a static mutex to ensure only one initialization happens at a time
    static INIT_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = INIT_MUTEX.lock().await;

    // Check if already initialized
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        debug!("StorageEngineManager is already initialized. Returning existing QueryExecEngine.");
        let engine = manager.get_persistent_engine().await;
        let config = load_storage_config_from_yaml(None).await.unwrap_or_else(|_| StorageConfig::default());
        let db = Database { storage: engine, config };
        return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
    }

    info!("Initializing StorageEngineManager for non-interactive command execution.");
    println!("Initializing Storage Engine...");

    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;

    // Clean up stale daemon entries to prevent conflicts
    debug!("Cleaning up stale daemon registry entries");
    daemon_registry
        .clear_stale_daemons()
        .await
        .context("Failed to clear stale daemon registry entries")?;

    let mut config = match load_storage_config_from_yaml(None).await {
        Ok(config) => {
            info!("Successfully loaded storage config: {:?}", config);
            config
        },
        Err(e) => {
            warn!("Failed to load storage config, using default values. Error: {}", e);
            StorageConfig::default()
        }
    };

    let desired_port = config.default_port;

    // Update config path to include port-specific directory
    if let Some(ref mut engine_config) = config.engine_specific_config {
        if engine_config.storage.path.is_some() {
            let base_path = engine_config.storage.path.as_ref().unwrap();
            engine_config.storage.path = Some(base_path.join(desired_port.to_string()));
            engine_config.storage.port = Some(desired_port);
        }
    }

    // Get all running storage daemons
    let running_daemons: Vec<DaemonMetadata> = daemon_registry
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|d| d.service_type == "storage" && d.engine_type.is_some())
        .collect();

    // Use the first storage daemon with a valid PID and listening port
    for daemon in running_daemons.iter() {
        let pid = daemon.pid;
        if pid == 0 || !check_pid_validity(pid).await {
            info!("Daemon on port {} has invalid PID ({}), stopping it...", daemon.port, pid);
            let storage_daemon_shutdown_tx = Arc::new(TokioMutex::new(None));
            let storage_daemon_handle = Arc::new(TokioMutex::new(None));
            let storage_daemon_port = Arc::new(TokioMutex::new(None));
            if let Err(e) = stop_storage_interactive(
                Some(daemon.port),
                storage_daemon_shutdown_tx,
                storage_daemon_handle,
                storage_daemon_port,
            )
            .await {
                warn!("Failed to stop daemon on port {}: {}", daemon.port, e);
            }
            continue;
        }

        // Check if the port is listening (not free)
        if is_port_free(daemon.port).await {
            info!("Daemon on port {} (PID {}) is not listening, stopping it...", daemon.port, pid);
            let storage_daemon_shutdown_tx = Arc::new(TokioMutex::new(None));
            let storage_daemon_handle = Arc::new(TokioMutex::new(None));
            let storage_daemon_port = Arc::new(TokioMutex::new(None));
            if let Err(e) = stop_storage_interactive(
                Some(daemon.port),
                storage_daemon_shutdown_tx,
                storage_daemon_handle,
                storage_daemon_port,
            )
            .await {
                warn!("Failed to stop daemon on port {}: {}", daemon.port, e);
            }
            continue;
        }

        let engine_type_str = daemon.engine_type.as_ref()
            .ok_or_else(|| anyhow!("Daemon on port {} has no engine type", daemon.port))?;
        info!("Found running {} daemon on port {} (PID {}), using it", engine_type_str, daemon.port, pid);

        let engine_type: StorageEngineType = engine_type_str
            .parse()
            .map_err(|_| anyhow!("Invalid engine type: {}", engine_type_str))?;
        let cwd = std::env::current_dir().context("Failed to get current working directory")?;
        let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

        // Update config for the daemon's port
        let mut daemon_config = config.clone();
        if let Some(ref mut engine_config) = daemon_config.engine_specific_config {
            if engine_config.storage.path.is_some() {
                let base_path = engine_config.storage.path.as_ref().unwrap();
                engine_config.storage.path = Some(base_path.join(daemon.port.to_string()));
                engine_config.storage.port = Some(daemon.port);
            }
        }

        let manager = StorageEngineManager::new(engine_type, &config_path, false, Some(daemon.port))
            .await
            .context(format!("Failed to initialize StorageEngineManager for engine {:?}", engine_type))?;

        let async_manager = AsyncStorageEngineManager::from_manager(manager);
        match GLOBAL_STORAGE_ENGINE_MANAGER.set(Arc::new(async_manager)) {
            Ok(_) => {
                info!("Successfully connected to existing storage engine on port {}", daemon.port);
                let engine = GLOBAL_STORAGE_ENGINE_MANAGER.get().unwrap().get_persistent_engine().await;
                let db = Database { storage: engine, config: daemon_config };
                return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
            }
            Err(_) => {
                debug!("GLOBAL_STORAGE_ENGINE_MANAGER was set by another thread - continuing");
                let engine = GLOBAL_STORAGE_ENGINE_MANAGER.get().unwrap().get_persistent_engine().await;
                let db = Database { storage: engine, config: daemon_config };
                return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
            }
        }
    }

    // No storage daemons with valid PIDs and listening ports found, start a new one
    info!("No storage daemons with valid PIDs and listening ports found, starting new storage daemon on port {}...", desired_port);
    println!("No running daemon found on port {}, starting new storage daemon...", desired_port);
    let cwd = std::env::current_dir().context("Failed to get current working directory")?;
    let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

    let storage_daemon_shutdown_tx = Arc::new(TokioMutex::new(None));
    let storage_daemon_handle = Arc::new(TokioMutex::new(None));
    let storage_daemon_port = Arc::new(TokioMutex::new(None));

    start_storage_interactive(
        Some(desired_port),
        Some(config_path.clone()),
        Some(config.clone()),
        None,
        storage_daemon_shutdown_tx.clone(),
        storage_daemon_handle.clone(),
        storage_daemon_port.clone(),
    )
    .await
    .context("Failed to start storage daemon")?;

    info!("Successfully started a new storage daemon on port {}.", desired_port);
    println!("New storage daemon started successfully on port {}.", desired_port);

    // Initialize StorageEngineManager for the new daemon
    let engine_type = config.storage_engine_type;
    let manager = StorageEngineManager::new(engine_type, &config_path, false, Some(desired_port))
        .await
        .context(format!("Failed to initialize StorageEngineManager for engine {:?}", engine_type))?;

    let async_manager = AsyncStorageEngineManager::from_manager(manager);
    match GLOBAL_STORAGE_ENGINE_MANAGER.set(Arc::new(async_manager)) {
        Ok(_) => {
            info!("Successfully connected to storage engine on port {}", desired_port);
            let engine = GLOBAL_STORAGE_ENGINE_MANAGER.get().unwrap().get_persistent_engine().await;
            let db = Database { storage: engine, config };
            Ok(Arc::new(QueryExecEngine::new(Arc::new(db))))
        }
        Err(_) => {
            debug!("GLOBAL_STORAGE_ENGINE_MANAGER was set by another thread - continuing");
            let engine = GLOBAL_STORAGE_ENGINE_MANAGER.get().unwrap().get_persistent_engine().await;
            let db = Database { storage: engine, config };
            Ok(Arc::new(QueryExecEngine::new(Arc::new(db))))
        }
    }
}

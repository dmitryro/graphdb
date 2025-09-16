use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::fs;
use tokio::task::{self, JoinHandle};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration}; 
use lib::daemon::daemon_management::{check_pid_validity, is_port_free, parse_port_cluster_range, 
                 check_process_status_by_port, find_pid_by_port};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{StorageEngineType, SledConfig, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, default_data_directory, default_log_directory};
use lib::storage_engine::SledStorage;
use lib::storage_engine::TikvStorage;
use lib::storage_engine::storage_engine::{GraphStorageEngine, AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use lib::commands::parse_kv_operation;
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml};
use lib::database::Database;
use crate::cli::handlers_storage::{start_storage_interactive, stop_storage_interactive};
pub use crate::cli::daemon_management::is_storage_daemon_running;
pub use models::errors::GraphError;
pub use lib::storage_engine::zmq_client::{self, ZmqClient};
pub use lib::config::config_defaults::default_config_root_directory;
pub use lib::storage_engine::sled_client::{ SledClient };
use zmq::{Context as ZmqContext, Message};

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

/// Initializes the storage engine manager for a query and returns a query execution engine.
/// Initializes the storage engine manager for a query and returns a query execution engine.
pub async fn initialize_storage_for_query(
    start_storage_interactive: StartStorageFn,
    stop_storage_interactive: StopStorageFn,
) -> Result<Arc<QueryExecEngine>, anyhow::Error> {
    // Use a static mutex to ensure only one initialization happens at a time
    static INIT_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = INIT_MUTEX.lock().await;

    // Check if already initialized before any other operations
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

    // Load CLI storage config
    let config = match load_storage_config_from_yaml(None).await {
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
    let cwd = std::env::current_dir().context("Failed to get current working directory")?;
    let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

    // Refresh storage daemon list from registry
    let all_daemons = daemon_registry.get_all_daemon_metadata().await.unwrap_or_default();
    let storage_daemons: Vec<_> = all_daemons.iter().filter(|d| d.service_type == "storage").collect();
    println!("===> TOTAL DAEMONS FOUND:  {:?}.", storage_daemons.iter().len());

    // Find and validate the first running storage daemon
    for daemon in storage_daemons.iter() {
        println!("=====> DAEMON FOUND {:?}", daemon);
        let pid = daemon.pid;

        // Check for daemon validity
        if pid == 0 || !check_pid_validity(pid).await || is_port_free(daemon.port).await {
            info!("Daemon on port {} (PID {}) is invalid, unregistering...", daemon.port, pid);
            let _ = daemon_registry.unregister_daemon(daemon.port).await;
            continue;
        }

        let expected_path = PathBuf::from("/opt/graphdb/storage_data/sled").join(daemon.port.to_string());
        if daemon.data_dir.as_ref() != Some(&expected_path) {
            info!("Daemon on port {} (PID {}) has incorrect data_dir ({:?}), unregistering...",
                    daemon.port, pid, daemon.data_dir);
            let _ = daemon_registry.unregister_daemon(daemon.port).await;
            continue;
        }

        // --- NEW ZMQ LIVENESS CHECK ---
        // Before connecting, ensure the daemon is alive and responsive by sending a ZMQ ping.
        info!("Performing ZMQ liveness check for daemon on port {}...", daemon.port);
        if !ping_daemon(daemon.port).await {
            info!("ZMQ ping failed for daemon on port {}. Unregistering invalid daemon...", daemon.port);
            let _ = daemon_registry.unregister_daemon(daemon.port).await;
            continue;
        }
        info!("ZMQ liveness check successful.");
        // --- END OF NEW CHECK ---

        let engine_type_str = daemon.engine_type.as_ref()
            .ok_or_else(|| anyhow!("Daemon on port {} has no engine type", daemon.port))?;
        let engine_type: StorageEngineType = engine_type_str
            .parse()
            .map_err(|_| anyhow!("Invalid engine type: {}", engine_type_str))?;

        // A valid daemon was found and verified, so connect to it and return.
        info!("Found valid {} daemon on port {} (PID {}), reusing it", engine_type_str, daemon.port, pid);
        println!("===> Reusing existing daemon on port {}.", daemon.port);

        let storage_engine: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            // Corrected logic: instantiate the correct client based on the communication method
            // SledClient for direct, in-process connections.
            StorageEngineType::Sled => {
                let sled_client = SledClient::new(expected_path.clone()).await?;
                Arc::new(sled_client)
            },
            // The other engine types are still handled here.
            StorageEngineType::RocksDB => {
                unimplemented!("RocksDB is not yet implemented");
            }
            StorageEngineType::TiKV => {
                unimplemented!("TiKV is not yet implemented");
            }
            _ => {
                return Err(anyhow!("Unsupported storage engine type: {:?}", engine_type));
            }
        };

        // This is the correct way to await an async function and handle the result
        let mut daemon_config = config.clone();
        if let Some(ref mut engine_config) = daemon_config.engine_specific_config {
            engine_config.storage.path = Some(PathBuf::from("/opt/graphdb/storage_data/sled").join(daemon.port.to_string()));
            engine_config.storage.port = Some(daemon.port);
        }

        let db = Database {
            storage: storage_engine,
            config: daemon_config
        };

        return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
    }

    // No valid storage daemons found, start a new one
    info!("No valid storage daemons found, starting new storage daemon on port {}...", desired_port);
    println!("No running daemon found, starting new storage daemon...");

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

    // Register the new daemon
    let instance_path = PathBuf::from("/opt/graphdb/storage_data/sled").join(desired_port.to_string());
    let pid = std::process::id();
    let daemon_metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port: desired_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(instance_path.clone()),
        config_path: config.config_root_directory.clone(),
        engine_type: Some(config.storage_engine_type.to_string()),
        last_seen_nanos: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    };

    if let Some(existing_metadata) = daemon_registry.get_daemon_metadata(desired_port).await? {
        if existing_metadata.data_dir != Some(instance_path.clone()) || existing_metadata.engine_type != Some(config.storage_engine_type.to_string()) {
            warn!("Updating daemon metadata for port {} from path {:?} to {:?}", desired_port, existing_metadata.data_dir, instance_path);
            daemon_registry.update_daemon_metadata(daemon_metadata).await?;
            info!("Updated daemon metadata for port {} with path {:?}", desired_port, instance_path);
        } else {
            info!("Daemon metadata for port {} already correct", desired_port);
        }
    } else {
        timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata))
            .await
            .map_err(|_| anyhow!("Timeout registering daemon on port {}", desired_port))??;
        info!("Registered daemon on port {} with path {:?}", desired_port, instance_path);
    }

    info!("Successfully started a new storage daemon on port {}.", desired_port);
    println!("New storage daemon started successfully on port {}.", desired_port);

    // This section was corrected to use the proper constructor
    let manager = StorageEngineManager::new(
        config.storage_engine_type,
        &config_path,
        false,
        Some(desired_port)
    )
    .await
    .context(format!("Failed to initialize StorageEngineManager for engine {:?}", config.storage_engine_type))?;

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

/// Helper function to perform a ZMQ ping to a specific daemon port.
/// Returns true if the ping is successful, false otherwise.
/// This function was refactored to correctly handle the nested `Result` types.
async fn ping_daemon(port: u16) -> bool {
    const CONNECT_TIMEOUT_SECS: u64 = 3;
    const REQUEST_TIMEOUT_SECS: u64 = 5;

    let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
    let addr = format!("ipc://{}", socket_path);

    // Check if socket file exists
    if !tokio::fs::metadata(&socket_path).await.is_ok() {
        debug!("IPC socket file {} does not exist.", socket_path);
        return false;
    }

    debug!("Attempting ZMQ ping to: {}", addr);

    let request = json!({ "command": "ping" });
    let request_data = match serde_json::to_vec(&request) {
        Ok(data) => data,
        Err(e) => {
            error!("Failed to serialize ping request: {}", e);
            return false;
        }
    };

    let spawned_task_result = tokio::task::spawn_blocking({
        let addr = addr.clone();
        let request_data = request_data.clone();
        move || {
            let context = zmq::Context::new();
            let socket = context.socket(zmq::REQ)?;
            socket.set_connect_timeout(CONNECT_TIMEOUT_SECS as i32 * 1000)?;
            socket.connect(&addr)?;
            socket.send(&request_data, 0)?;
            let mut response = Message::new();
            socket.recv(&mut response, 0)?;
            Ok(response) as Result<Message, anyhow::Error>
        }
    })
    .await;

    let response_msg = match spawned_task_result {
        Ok(msg) => match msg {
            Ok(message) => message,
            Err(e) => {
                error!("ZMQ operation failed in spawned thread: {}", e);
                return false;
            }
        },
        Err(e) => {
            error!("ZMQ request timed out or panicked: {}", e);
            return false;
        }
    };

    let json_response: serde_json::Value = match serde_json::from_slice(&response_msg) {
        Ok(value) => value,
        Err(e) => {
            error!("Failed to deserialize ZMQ response: {}", e);
            return false;
        }
    };

    json_response.get("status").and_then(|s| s.as_str()) == Some("success")
}
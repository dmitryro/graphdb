use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::{self, JoinHandle};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration};
use lib::daemon::daemon_management::{check_pid_validity, is_port_free};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{StorageEngineType, SledConfig, RocksDBConfig, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, 
                  default_data_directory, default_log_directory, daemon_api_storage_engine_type_to_string, load_cli_config};
use lib::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine, AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use lib::commands::parse_kv_operation;
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml, QueryPlan, QueryResult};
use lib::database::Database;
use crate::cli::handlers_storage::{start_storage_interactive};
pub use crate::cli::daemon_management::is_storage_daemon_running;
pub use models::errors::{GraphError, GraphResult};
use lib::storage_engine::sled_client::SledClient;
use lib::storage_engine::rocksdb_client::RocksDBClient;
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
        Err(e) => Err(e),
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
    
    let response: Value = serde_json::from_slice(msg.as_ref())
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

    let daemon = daemons.iter().max_by_key(|m| m.port).unwrap_or(daemons.first().unwrap());
    info!("Selected daemon on port: {}", daemon.port);

    let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);

    if !tokio::fs::metadata(&socket_path).await.is_ok() {
        return Err(anyhow!("IPC socket file {} does not exist. Daemon may not be running properly on port {}.", socket_path, daemon.port));
    }

    debug!("Connecting to Sled daemon at: {}", addr);

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
                let response_value = response?;
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

        if attempt < MAX_RETRIES {
            let delay = BASE_RETRY_DELAY_MS * 2u64.pow(attempt - 1);
            debug!("Retrying after {}ms", delay);
            tokio::time::sleep(TokioDuration::from_millis(delay)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("Failed to complete ZMQ operation after {} attempts", MAX_RETRIES)))
}

async fn handle_kv_rocksdb_zmq(key: String, value: Option<String>, operation: &str) -> Result<()> {
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
        .filter(|metadata| metadata.engine_type == Some(StorageEngineType::RocksDB.to_string()))
        .collect::<Vec<_>>();

    if daemons.is_empty() {
        return Err(anyhow!("No running RocksDB daemon found. Please start a daemon with 'storage start'"));
    }

    let daemon = daemons.iter().max_by_key(|m| m.port).unwrap_or(daemons.first().unwrap());
    info!("Selected RocksDB daemon on port: {}", daemon.port);

    let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);

    if !tokio::fs::metadata(&socket_path).await.is_ok() {
        return Err(anyhow!("IPC socket file {} does not exist. Daemon may not be running properly on port {}.", socket_path, daemon.port));
    }

    debug!("Connecting to RocksDB daemon at: {}", addr);

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
        "flush" => json!({ "command": "flush" }),
        "clear" => json!({ "command": "clear_data" }),
        _ => return Err(anyhow!("Unsupported operation: {}", operation)),
    };

    debug!("Sending request: {:?}", request);
    let request_data = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("Failed to serialize request: {}", e))?;

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
                let response_value = response?;
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
                            "flush" => {
                                println!("Flushed database successfully");
                            }
                            "clear" => {
                                println!("Cleared database successfully");
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

        if attempt < MAX_RETRIES {
            let delay = BASE_RETRY_DELAY_MS * 2u64.pow(attempt - 1);
            debug!("Retrying after {}ms", delay);
            tokio::time::sleep(TokioDuration::from_millis(delay)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("Failed to complete ZMQ operation after {} attempts", MAX_RETRIES)))
}


pub async fn handle_kv_command(engine: Arc<QueryExecEngine>, operation: String, key: String, value: Option<String>) -> Result<()> {
    debug!("In handle_kv_command: operation={}, key={}", operation, key);
    let validated_op = parse_kv_operation(&operation)
        .map_err(|e| anyhow!("Invalid KV operation: {}", e))?;

    let config = load_cli_config().await
        .map_err(|e| anyhow!("Failed to load CLI config: {}", e))?;
    debug!("Loaded config: {:?}", config);

    // Handle Sled and RocksDB via ZeroMQ, others via engine directly
    match config.storage.storage_engine_type {
        Some(StorageEngineType::Sled) => {
            info!("Using Sled-specific ZeroMQ handler for KV operation: {}", validated_op);
            handle_kv_sled_zmq(key.clone(), value, &validated_op).await
                .map_err(|e| anyhow!("Sled ZeroMQ operation failed: {}", e))?;
            Ok(())
        }
        Some(StorageEngineType::RocksDB) => {
            info!("Using RocksDB-specific ZeroMQ handler for KV operation: {}", validated_op);
            handle_kv_rocksdb_zmq(key.clone(), value, &validated_op).await
                .map_err(|e| anyhow!("RocksDB ZeroMQ operation failed: {}", e))?;
            Ok(())
        }
        _ => {
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

                    let response: Value = serde_json::from_slice(msg.as_ref())
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
    _stop_storage_interactive: StopStorageFn,
) -> Result<Arc<QueryExecEngine>, anyhow::Error> {
    static INIT_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = INIT_MUTEX.lock().await;

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

    let all_daemons = daemon_registry.get_all_daemon_metadata().await.unwrap_or_default();
    let storage_daemons: Vec<_> = all_daemons.iter().filter(|d| d.service_type == "storage").collect();
    println!("===> TOTAL DAEMONS FOUND: {}.", storage_daemons.len());

    for daemon in storage_daemons.iter() {
        println!("=====> DAEMON FOUND {:?}", daemon);
        let pid = daemon.pid;

        if pid == 0 || !check_pid_validity(pid).await || is_port_free(daemon.port).await {
            info!("Daemon on port {} (PID {}) is invalid, unregistering...", daemon.port, pid);
            let _ = daemon_registry.unregister_daemon(daemon.port).await;
            continue;
        }

        let engine_type: StorageEngineType = match daemon.engine_type.as_ref() {
            Some(engine_str) => match engine_str.parse() {
                Ok(parsed_type) => parsed_type,
                Err(_) => {
                    warn!("Invalid engine type '{}' for daemon on port {}, unregistering...", engine_str, daemon.port);
                    let _ = daemon_registry.unregister_daemon(daemon.port).await;
                    continue;
                }
            },
            None => {
                warn!("No engine type specified for daemon on port {}, unregistering...", daemon.port);
                let _ = daemon_registry.unregister_daemon(daemon.port).await;
                continue;
            }
        };

        if engine_type != config.storage_engine_type {
            info!("Daemon on port {} has engine type {:?}, but we need {:?}. Skipping...", 
                  daemon.port, engine_type, config.storage_engine_type);
            continue;
        }

        info!("Performing ZMQ liveness check for daemon on port {}...", daemon.port);
        let ping_result = timeout(TokioDuration::from_secs(5), check_daemon_health(&format!("ipc:///opt/graphdb/graphdb-{}.ipc", daemon.port))).await;
        match ping_result {
            Ok(Ok(true)) => {
                info!("ZMQ liveness check successful.");
            }
            Ok(Ok(false)) => {
                info!("ZMQ ping failed for daemon on port {}. Unregistering invalid daemon...", daemon.port);
                let _ = daemon_registry.unregister_daemon(daemon.port).await;
                continue;
            }
            Ok(Err(_)) => {
                info!("ZMQ ping failed for daemon on port {}. Unregistering invalid daemon...", daemon.port);
                let _ = daemon_registry.unregister_daemon(daemon.port).await;
                continue;
            }
            Err(_) => {
                info!("ZMQ ping timed out for daemon on port {}. Unregistering invalid daemon...", daemon.port);
                let _ = daemon_registry.unregister_daemon(daemon.port).await;
                continue;
            }
        }

        let engine_type_str = daemon_api_storage_engine_type_to_string(&engine_type);
        info!("Found valid {} daemon on port {} (PID {}), connecting to it", engine_type_str, daemon.port, pid);
        println!("===> Reusing existing daemon on port {}.", daemon.port);

        let storage_engine: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::Sled => {
                let sled_client = SledClient::new_with_port(daemon.port).await?;
                Arc::new(sled_client)
            },
            StorageEngineType::RocksDB => {
                let rocksdb_client = RocksDBClient::new_with_port(daemon.port).await?;
                Arc::new(rocksdb_client)
            },
            StorageEngineType::TiKV => {
                return Err(anyhow!("TiKV is not yet implemented"));
            },
            _ => {
                return Err(anyhow!("Unsupported storage engine type: {:?}", engine_type));
            }
        };

        let mut daemon_config = config.clone();
        if let Some(ref mut engine_config) = daemon_config.engine_specific_config {
            match engine_type {
                StorageEngineType::Sled => {
                    engine_config.storage.path = Some(PathBuf::from("/opt/graphdb/storage_data/sled").join(daemon.port.to_string()));
                },
                StorageEngineType::RocksDB => {
                    engine_config.storage.path = Some(PathBuf::from("/opt/graphdb/storage_data/rocksdb").join(daemon.port.to_string()));
                },
                _ => {}
            }
            engine_config.storage.port = Some(daemon.port);
        }

        let db = Database {
            storage: storage_engine,
            config: daemon_config
        };

        return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
    }

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

    let instance_path = match config.storage_engine_type {
        StorageEngineType::Sled => PathBuf::from("/opt/graphdb/storage_data/sled").join(desired_port.to_string()),
        StorageEngineType::RocksDB => PathBuf::from("/opt/graphdb/storage_data/rocksdb").join(desired_port.to_string()),
        StorageEngineType::TiKV => PathBuf::from("/opt/graphdb/storage_data/tikv").join(desired_port.to_string()),
        _ => return Err(anyhow!("Unsupported storage engine type: {:?}", config.storage_engine_type)),
    };

    let pid = std::process::id();
    let daemon_metadata = DaemonMetadata {
        service_type: "storage".to_string(),
        port: desired_port,
        pid,
        ip_address: "127.0.0.1".to_string(),
        data_dir: Some(instance_path.clone()),
        config_path: Some(PathBuf::from(config.config_root_directory.clone().unwrap_or_default())),
        engine_type: Some(config.storage_engine_type.to_string()),
        last_seen_nanos: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0),
    };

    daemon_registry.register_daemon(daemon_metadata).await
        .context("Failed to register new daemon")?;

    let storage_engine: Arc<dyn GraphStorageEngine + Send + Sync> = match config.storage_engine_type {
        StorageEngineType::Sled => {
            let sled_client = SledClient::new_with_port(desired_port).await?;
            Arc::new(sled_client)
        },
        StorageEngineType::RocksDB => {
            let rocksdb_client = RocksDBClient::new_with_port(desired_port).await?;
            Arc::new(rocksdb_client)
        },
        StorageEngineType::TiKV => {
            return Err(anyhow!("TiKV is not yet implemented"));
        },
        _ => {
            return Err(anyhow!("Unsupported storage engine type: {:?}", config.storage_engine_type));
        }
    };

    let mut final_config = config.clone();
    if let Some(ref mut engine_config) = final_config.engine_specific_config {
        engine_config.storage.path = Some(instance_path.clone());
        engine_config.storage.port = Some(desired_port);
    }

    let db = Database {
        storage: storage_engine,
        config: final_config
    };

    let engine = Arc::new(QueryExecEngine::new(Arc::new(db)));
    Ok(engine)
}

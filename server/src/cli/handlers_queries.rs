use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::os::unix::fs::PermissionsExt; // Added for from_mode
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::{self, JoinHandle};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration};
use lib::daemon::daemon_management::{check_pid_validity, is_port_free, is_storage_daemon_running, find_pid_by_port, check_daemon_health};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{StorageEngineType, SledConfig, RocksDBConfig, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, DEFAULT_DATA_DIRECTORY,
                  default_data_directory, default_log_directory, daemon_api_storage_engine_type_to_string, load_cli_config};
use lib::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine, AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use lib::commands::parse_kv_operation;
use lib::storage_engine::rocksdb_storage::{ROCKSDB_DB, ROCKSDB_POOL_MAP};
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml, QueryPlan, QueryResult};
use lib::database::Database;
use crate::cli::handlers_storage::{start_storage_interactive};
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

type StartStorageFn = fn(
    Option<u16>,
    Option<PathBuf>,
    Option<StorageConfig>,
    Option<String>,
    Arc<TokioMutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>>;

type StopStorageFn = fn(
    Option<u16>,
    Arc<TokioMutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    Arc<TokioMutex<Option<tokio::task::JoinHandle<()>>>>,
    Arc<TokioMutex<Option<u16>>>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), anyhow::Error>> + Send>>;

pub async fn initialize_storage_for_query(
    start_storage_interactive: StartStorageFn,
    _stop_storage_interactive: StopStorageFn,
) -> Result<Arc<QueryExecEngine>, anyhow::Error> {
    static INIT_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = INIT_MUTEX.lock().await;

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

    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
    info!("Daemon registry initialized, fetching metadata...");

    let all_daemons = daemon_registry.get_all_daemon_metadata().await.unwrap_or_default();
    let storage_daemons: Vec<_> = all_daemons.iter().filter(|d| d.service_type == "storage").collect();
    info!("Registry contents: {:?}", all_daemons);
    println!("===> TOTAL DAEMONS FOUND: {}.", storage_daemons.len());

    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        debug!("StorageEngineManager is already initialized. Checking engine compatibility...");
        let engine = manager.get_persistent_engine().await;
        let engine_type = engine.get_type();
        let engine_port = desired_port;

        if engine_type == config.storage_engine_type.to_string() && engine_port == desired_port {
            info!("Existing StorageEngineManager has compatible engine type {} and port {}. Reusing...", engine_type, desired_port);
            println!("===> Reusing existing StorageEngineManager for port {}.", desired_port);
            let db = Database { storage: engine, config };
            return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
        } else {
            info!("Existing StorageEngineManager has incompatible engine type {} or port {}. Proceeding to check daemons...", engine_type, engine_port);
        }
    }

    let daemon = storage_daemons.iter().find(|d| d.port == desired_port);

    if let Some(daemon) = daemon {
        println!("=====> DAEMON FOUND {:?}", daemon);
        let pid = daemon.pid;
        if pid == 0 || !check_pid_validity(pid).await || is_port_free(daemon.port).await {
            info!("Daemon on port {} (PID {}) is invalid, unregistering...", daemon.port, pid);
            let _ = daemon_registry.unregister_daemon(daemon.port).await;
        } else {
            let engine_type: StorageEngineType = match daemon.engine_type.as_ref() {
                Some(engine_str) => match engine_str.parse() {
                    Ok(parsed_type) => parsed_type,
                    Err(_) => {
                        warn!("Invalid engine type '{}' for daemon on port {}, falling back to config engine type {:?}", engine_str, daemon.port, config.storage_engine_type);
                        config.storage_engine_type.clone()
                    }
                },
                None => {
                    warn!("No engine type specified for daemon on port {}, falling back to config engine type {:?}", daemon.port, config.storage_engine_type);
                    config.storage_engine_type.clone()
                }
            };
            let engine_type_str = daemon_api_storage_engine_type_to_string(&engine_type);

            // Update daemon metadata with correct engine type and data_dir if necessary
            let data_dir = config.engine_specific_config
                .as_ref()
                .and_then(|c| c.storage.path.clone())
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY).join(engine_type_str.to_lowercase()).join(desired_port.to_string()));
            
            let mut updated = false;
            let updated_metadata = DaemonMetadata {
                service_type: daemon.service_type.clone(),
                port: daemon.port,
                pid: daemon.pid,
                ip_address: daemon.ip_address.clone(),
                data_dir: if daemon.data_dir != Some(data_dir.clone()) {
                    updated = true;
                    println!("===> PATH MISMATCH FOR {}: REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", engine_type_str.to_uppercase(), daemon.data_dir, data_dir);
                    Some(data_dir)
                } else {
                    daemon.data_dir.clone()
                },
                config_path: daemon.config_path.clone(),
                engine_type: if daemon.engine_type.as_ref() != Some(&engine_type.to_string()) {
                    updated = true;
                    Some(engine_type.to_string())
                } else {
                    daemon.engine_type.clone()
                },
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0),
            };

            if updated {
                let data_dir_for_log = updated_metadata.data_dir.clone();
                daemon_registry.update_daemon_metadata(updated_metadata).await?;
                info!("Updated daemon metadata on port {} with engine type {:?} and data_dir {:?}", daemon.port, engine_type, data_dir_for_log);
                println!("===> UPDATED DAEMON REGISTRY DATA_DIR TO {:?}", data_dir_for_log);
            }

            if engine_type != config.storage_engine_type {
                info!(
                    "Daemon on port {} has engine type {:?}, but we need {:?}. Starting new daemon...",
                    daemon.port, engine_type, config.storage_engine_type
                );
            } else {
                info!("Found valid {} daemon on port {} (PID {}), reusing...", engine_type_str, daemon.port, pid);
                println!("===> Reusing existing daemon on port {}.", daemon.port);
                let manager = StorageEngineManager::new(
                    engine_type,
                    &config_path,
                    false,
                    Some(daemon.port),
                ).await?;
                let arc_manager = Arc::new(AsyncStorageEngineManager::from_manager(manager));
                
                let arc_manager_clone = arc_manager.clone();
                GLOBAL_STORAGE_ENGINE_MANAGER
                    .set(arc_manager)
                    .map_err(|_| anyhow!("Failed to set StorageEngineManager"))?;
                let engine = arc_manager_clone.get_persistent_engine().await;
                let db = Database { storage: engine, config };
                return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
            }
        }
    }

    // No valid daemon found, start a new one
    info!("No valid daemon found on port {}. Starting new daemon...", desired_port);
    start_storage_interactive(
        Some(desired_port),
        Some(PathBuf::from(config_path.to_string_lossy().to_string())),
        Some(config.clone()),
        None,
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
        Arc::new(TokioMutex::new(None)),
    ).await?;

    let manager = GLOBAL_STORAGE_ENGINE_MANAGER
        .get()
        .ok_or_else(|| anyhow!("StorageEngineManager not initialized after starting daemon"))?;
    let engine = manager.get_persistent_engine().await;
    let db = Database { storage: engine, config };
    Ok(Arc::new(QueryExecEngine::new(Arc::new(db))))
}

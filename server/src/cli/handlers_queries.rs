// server/src/cli/handlers_queries.rs
use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::fs;
use tokio::task::JoinHandle;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{StorageEngineType, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, default_data_directory, default_log_directory};
use lib::storage_engine::SledStorage;
use lib::storage_engine::TikvStorage;
use lib::storage_engine::storage_engine::{AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use lib::commands::parse_kv_operation;
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml};
use crate::cli::handlers_storage::{start_storage_interactive, stop_storage_interactive};
pub use crate::cli::daemon_management::is_storage_daemon_running;
pub use models::errors::GraphError;
pub use lib::config::config_defaults::default_config_root_directory;
use zmq::{Context as ZmqContext};
use zmq::Error::EPROTO;

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
    println!("Executing query: {}", query_string);

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
pub async fn handle_kv_command(engine: Arc<QueryExecEngine>, operation: String, key: String, value: Option<String>) -> Result<()> {
    let validated_op = parse_kv_operation(&operation)
        .map_err(|e| anyhow!("Invalid KV operation: {}", e))?;

    let config = lib::config::load_cli_config().await?;
    if config.storage.storage_engine_type == Some(StorageEngineType::Sled) {
        info!("Using Sled-specific ZeroMQ handler for KV operation: {}", validated_op);
        handle_kv_sled_zmq(key, value, &validated_op).await
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

async fn handle_kv_sled_zmq(key: String, value: Option<String>, operation: &str) -> Result<()> {
    const CONNECT_TIMEOUT_SECS: u64 = 3;
    const REQUEST_TIMEOUT_SECS: u64 = 10;
    const RECEIVE_TIMEOUT_SECS: u64 = 15;

    println!("===> STARTING ZMQ KV OPERATION: {} for key: {}", operation, key);

    let registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let daemons = registry
        .get_all_daemon_metadata()
        .await
        .map_err(|e| anyhow!("Failed to retrieve daemon metadata: {}", e))?
        .into_iter()
        .filter(|metadata| metadata.engine_type == Some(StorageEngineType::Sled.to_string()))
        .collect::<Vec<_>>();

    println!("===> FOUND {} SLED DAEMONS", daemons.len());

    if daemons.is_empty() {
        return Err(anyhow!("No running Sled daemon found. Please start a daemon with 'storage start'"));
    }

    // Select the daemon with the highest port (most recent) for load balancing
    let daemon = daemons.iter().max_by_key(|m| m.port).unwrap_or(daemons.first().unwrap());
    println!("===> SELECTED DAEMON ON PORT: {}", daemon.port);

    // Use the standard IPC socket path for all GraphDB communication
    let socket_path = "/opt/graphdb/pgraphdb.ipc";
    let addr = format!("ipc://{}", socket_path);
    
    // Check if socket file exists
    if !tokio::fs::metadata(&socket_path).await.is_ok() {
        return Err(anyhow!("IPC socket file {} does not exist. Daemon may not be running properly.", socket_path));
    }

    println!("===> CONNECTING TO SLED DAEMON AT: {}", addr);

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

    println!("===> SENDING REQUEST: {:?}", request);
    let request_data = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("Failed to serialize request: {}", e))?;

    // Perform the entire ZMQ interaction in a single blocking task with timeout
    let response_result = tokio::time::timeout(
        TokioDuration::from_secs(CONNECT_TIMEOUT_SECS + REQUEST_TIMEOUT_SECS + RECEIVE_TIMEOUT_SECS),
        tokio::task::spawn_blocking({
            let addr = addr.clone();
            let request_data = request_data;
            move || {
                // Create zmq_context inside the blocking task as it is not Send
                let zmq_context = zmq::Context::new();
                let client_result = zmq_context.socket(zmq::REQ);

                if let Err(e) = client_result {
                    return Err(anyhow!("Failed to create ZMQ socket: {}", e));
                }

                let client = client_result.unwrap();

                // Set socket timeouts
                if let Err(e) = client.set_rcvtimeo((RECEIVE_TIMEOUT_SECS * 1000) as i32) {
                    return Err(anyhow!("Failed to set receive timeout: {}", e));
                }
                if let Err(e) = client.set_sndtimeo((REQUEST_TIMEOUT_SECS * 1000) as i32) {
                    return Err(anyhow!("Failed to set send timeout: {}", e));
                }

                if let Err(e) = client.connect(&addr) {
                    return Err(anyhow!("Failed to connect to {}: {}", addr, e));
                }

                println!("===> SUCCESSFULLY CONNECTED TO: {}", addr);

                if let Err(e) = client.send(&request_data, 0) {
                    return Err(anyhow!("Failed to send request to {}: {}", addr, e));
                }

                println!("===> REQUEST SENT SUCCESSFULLY");

                let mut msg = zmq::Message::new();
                if let Err(e) = client.recv(&mut msg, 0) {
                    return Err(anyhow!("Failed to receive response from {}: {}", addr, e));
                }
                
                println!("===> RECEIVED RESPONSE");
                let response_bytes = msg.to_vec();
                let response: Value = serde_json::from_slice(&response_bytes)
                    .map_err(|e| anyhow!("Failed to deserialize response from {}: {}", addr, e))?;

                println!("===> PARSED RESPONSE: {:?}", response);
                Ok(response)
            }
        })
    )
    .await
    .map_err(|_| anyhow!("ZMQ operation timed out after {} seconds", CONNECT_TIMEOUT_SECS + REQUEST_TIMEOUT_SECS + RECEIVE_TIMEOUT_SECS))?;

    // Process the response from the blocking task
    let response = match response_result {
        Ok(Ok(response)) => response,
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(anyhow!("Task join error: {}", e)),
    };

    // Process the final `Value` response
    match response.get("status").and_then(|s| s.as_str()) {
        Some("success") => {
            println!("===> OPERATION SUCCESSFUL");
            match operation {
                "get" => {
                    if let Some(response_value) = response.get("value") {
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
            Ok(())
        }
        Some("error") => {
            let message = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            println!("===> DAEMON ERROR: {}", message);
            Err(anyhow!("Daemon error: {}", message))
        }
        _ => {
            println!("===> INVALID RESPONSE: {:?}", response);
            Err(anyhow!("Invalid response from {}: {:?}", addr, response))
        }
    }
}

type StartStorageFn = fn(
    Option<u16>,
    Option<PathBuf>,
    Option<StorageConfig>,
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

pub async fn initialize_storage_for_query(
    start_storage_interactive: StartStorageFn,
    stop_storage_interactive: StopStorageFn,
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

    // Prioritize ports 8051, 8050, or 8049
    if let Some(daemon) = running_daemons.iter().find(|d| d.port == 8051)
        .or_else(|| running_daemons.iter().find(|d| d.port == 8050))
        .or_else(|| running_daemons.iter().find(|d| d.port == 8049))
        .or_else(|| running_daemons.first())
    {
        info!("Found running Sled storage daemon on port {}, connecting to it...", daemon.port);

        let engine_type = daemon.engine_type
            .as_ref()
            .and_then(|s| s.parse::<StorageEngineType>().ok())
            .ok_or_else(|| anyhow!("Failed to parse engine type from running daemon: {}", daemon.engine_type.as_deref().unwrap_or("n/a")))?;
        
        let config_path = daemon.config_path.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));

        if !config_path.exists() {
            error!("Config file does not exist: {:?}", config_path);
            return Err(anyhow!("Config file does not exist: {:?}", config_path));
        }

        
        let config = match load_storage_config_from_yaml(Some(config_path.clone())).await {
            Ok(config) => {
                info!("Loaded storage config for engine {:?}: {:?}", engine_type, config);
                config
            },
            Err(e) => {
                warn!("Failed to load config from {:?}, using default: {}", config_path, e);
                StorageConfig::default()
            }
        };

        // Update port in config to match running daemon
        let mut updated_config = config;
        if let Some(engine_specific_config) = &mut updated_config.engine_specific_config {
            engine_specific_config.storage.port = Some(daemon.port);
        }

        match engine_type {
            StorageEngineType::Sled => {
                let lock_path = updated_config.data_directory.unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data")).join("sled").join("db.lck");
                if lock_path.exists() {
                    info!("Removing Sled lock file at {:?}", lock_path);
                    fs::remove_file(&lock_path).context(format!("Failed to remove Sled lock file at {:?}", lock_path))?;
                }
            },
            StorageEngineType::TiKV => {
                if let Err(e) = TikvStorage::force_unlock().await {
                    warn!("Failed to clean up TiKV lock files: {}", e);
                }
            },
            _ => {
                debug!("No specific lock file cleanup implemented for engine type: {:?}", engine_type);
            }
        }

        let storage_engine = updated_config.storage_engine_type.clone();
        let port = updated_config.engine_specific_config
            .as_ref()
            .and_then(|c| c.storage.port)
            .unwrap_or_else(|| match storage_engine {
                StorageEngineType::TiKV => 2380,
                _ => 8052, // Default for Sled and others
            });

        let manager = StorageEngineManager::new(engine_type.clone(), &config_path, false, Some(port))
            .await
            .context(format!("Failed to initialize StorageEngineManager for engine {:?}", engine_type))?;

        GLOBAL_STORAGE_ENGINE_MANAGER
            .set(Arc::new(AsyncStorageEngineManager::from_manager(
                Arc::try_unwrap(manager)
                    .map_err(|_| GraphError::ConfigurationError("Failed to unwrap Arc<StorageEngineManager>: multiple references exist".to_string()))?
            )))
            .context("Failed to set GLOBAL_STORAGE_ENGINE_MANAGER")?;

        info!("Successfully connected to storage engine on port {:?}", daemon.port);
        println!("===> Connected to storage daemon on port {}.", daemon.port);
    } else {
        info!("No running Sled daemon found, starting a new storage daemon on port 8051...");
        println!("===> No running Sled daemon found, starting a new storage daemon...");

        let cwd = std::env::current_dir().context("Failed to get current working directory")?;
        let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

        if !config_path.exists() {
            error!("Config file does not exist: {:?}", config_path);
            return Err(anyhow!("Config file does not exist: {:?}", config_path));
        }

        let mut config = match load_storage_config_from_yaml(Some(config_path.clone())).await {
            Ok(config) => {
                info!("Successfully loaded existing storage config: {:?}", config);
                config
            },
            Err(e) => {
                warn!("Failed to load existing config from {:?}, using default values. Error: {}", config_path, e);
                StorageConfig::default()
            }
        };
        config.default_port = 8051; // Use CLI-specified port

        let storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>> = Arc::new(TokioMutex::new(None));

        start_storage_interactive(
            Some(8051),
            Some(config_path),
            Some(config),
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

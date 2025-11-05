use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::path::{PathBuf, Path};
use std::pin::Pin;
use std::sync::Arc;
use std::os::unix::fs::PermissionsExt; // Added for from_mode
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::{self, JoinHandle};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration};
use lib::daemon::daemon_management::{check_pid_validity, is_port_free, is_storage_daemon_running, find_pid_by_port, 
                                     check_daemon_health, restart_daemon_process, stop_storage_daemon_by_port,
                                     check_pid_validity_sync,                                       
                                     get_or_create_daemon_with_ipc,
                                     check_ipc_socket_exists,
                                     verify_and_recover_ipc,};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{StorageEngineType, SledConfig, RocksDBConfig, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, DEFAULT_DATA_DIRECTORY,
                  default_data_directory, default_log_directory, daemon_api_storage_engine_type_to_string, load_cli_config};
use lib::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine, AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use lib::commands::parse_kv_operation;
use lib::storage_engine::rocksdb_storage::{ROCKSDB_DB, ROCKSDB_POOL_MAP};
use lib::storage_engine::sled_storage::{ SLED_DB, SLED_POOL_MAP };
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml, QueryPlan, QueryResult, SledDbWithPath};
use lib::database::Database;
use crate::cli::handlers_storage::{start_storage_interactive};
use models::errors::{GraphError, GraphResult};
use daemon_api::{start_daemon, stop_port_daemon};
use lib::storage_engine::sled_client::SledClient;
use lib::storage_engine::rocksdb_client::RocksDBClient;
use zmq::{Context as ZmqContext, Message};
use base64::Engine;


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
    client.set_linger(500)
        .context("Failed to set linger")?;

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

    // Disconnect socket to release resources
    if let Err(e) = client.disconnect(addr) {
        warn!("Failed to disconnect ZMQ socket from {}: {}", addr, e);
    } else {
        debug!("Disconnected ZMQ socket from {}", addr);
    }
    
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

    // -----------------------------------------------------------------
    // 1. Find a daemon with verified IPC socket, or recover
    // -----------------------------------------------------------------
    let registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let mut daemons = registry
        .get_all_daemon_metadata()
        .await
        .map_err(|e| anyhow!("Failed to retrieve daemon metadata: {}", e))?
        .into_iter()
        .filter(|metadata| {
            metadata.engine_type == Some(StorageEngineType::Sled.to_string())
                && metadata.pid > 0
                && check_pid_validity_sync(metadata.pid)
        })
        .collect::<Vec<_>>();

    if daemons.is_empty() {
        error!("No running Sled daemon found");
        return Err(anyhow!(
            "No running Sled daemon found. Please start a daemon with 'storage start'"
        ));
    }

    // Sort by port (highest first) to prefer the default
    daemons.sort_by_key(|m| std::cmp::Reverse(m.port));

    // Try to find a daemon with a valid IPC socket
    let mut selected_daemon: Option<DaemonMetadata> = None;
    let mut broken_daemons: Vec<u16> = Vec::new();
    
    for daemon in &daemons {
        let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            selected_daemon = Some(daemon.clone());
            info!("Selected daemon on port {} with verified IPC socket", daemon.port);
            break;
        } else {
            warn!(
                "Daemon on port {} exists but IPC socket {} is missing",
                daemon.port, socket_path
            );
            broken_daemons.push(daemon.port);
        }
    }

    // If no daemon has IPC, attempt recovery on the first broken one
    if selected_daemon.is_none() && !broken_daemons.is_empty() {
        let recovery_port = broken_daemons[0];
        warn!(
            "No Sled daemon has valid IPC - attempting recovery on port {}",
            recovery_port
        );
        println!(
            "===> WARNING: No daemon with IPC found - attempting recovery on port {}",
            recovery_port
        );

        // Load config for recovery
        let config = load_storage_config_from_yaml(None).await.map_err(|e| {
            anyhow!("Failed to load config for IPC recovery: {}", e)
        })?;

        // Stop the broken daemon
        if let Err(e) = stop_storage_daemon_by_port(recovery_port).await {
            warn!("Failed to stop broken daemon on port {}: {}", recovery_port, e);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Start fresh daemon with IPC
        let cwd = std::env::current_dir().context("Failed to get current working directory")?;
        let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        
        let mut recovery_config = config.clone();
        recovery_config.default_port = recovery_port;
        if let Some(ref mut ec) = recovery_config.engine_specific_config {
            ec.storage.port = Some(recovery_port);
            let base_dir = config
                .data_directory
                .as_ref()
                .unwrap_or(&PathBuf::from("/opt/graphdb/storage_data"))
                .display()
                .to_string();
            ec.storage.path = Some(PathBuf::from(format!("{}/sled/{}", base_dir, recovery_port)));
        }

        let shutdown_tx = Arc::new(TokioMutex::new(None));
        let handle = Arc::new(TokioMutex::new(None));
        let port_arc = Arc::new(TokioMutex::new(None));

        start_storage_interactive(
            Some(recovery_port),
            Some(config_path),
            Some(recovery_config),
            Some("force_port".to_string()),
            shutdown_tx,
            handle,
            port_arc,
        )
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to restart Sled daemon on port {} for IPC recovery: {}",
                recovery_port, e
            )
        })?;

        // Wait for IPC to be ready
        let mut ipc_ready = false;
        for attempt in 0..30 {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            let socket_path = format!("/tmp/graphdb-{}.ipc", recovery_port);
            if tokio::fs::metadata(&socket_path).await.is_ok() {
                ipc_ready = true;
                info!("IPC recovered on port {} (attempt {})", recovery_port, attempt + 1);
                println!("===> IPC RECOVERED ON PORT {}", recovery_port);
                break;
            }
        }

        if !ipc_ready {
            return Err(anyhow!(
                "Failed to recover IPC on port {} after daemon restart",
                recovery_port
            ));
        }

        // Refresh daemon metadata
        if let Ok(Some(daemon_meta)) = registry.get_daemon_metadata(recovery_port).await {
            selected_daemon = Some(daemon_meta);
        } else {
            return Err(anyhow!(
                "Daemon restarted on port {} but not found in registry",
                recovery_port
            ));
        }
    }

    let daemon = selected_daemon.ok_or_else(|| {
        error!("No Sled daemon has a valid IPC socket and recovery failed");
        anyhow!(
            "Sled daemons found but none have valid IPC sockets and recovery failed. Try 'start-storage'."
        )
    })?;

    info!("Selected Sled daemon on port: {}", daemon.port);
    println!("===> Selected daemon on port: {}", daemon.port);

    let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);

    // Final verification
    if !tokio::fs::metadata(&socket_path).await.is_ok() {
        error!(
            "IPC socket file {} does not exist after all recovery attempts. Daemon port: {}.",
            socket_path, daemon.port
        );
        return Err(anyhow!(
            "IPC socket file {} does not exist after recovery attempts.",
            socket_path
        ));
    }

    // -----------------------------------------------------------------
    // 2. Build the request (Base64 encoding)
    // -----------------------------------------------------------------
    let request = match operation {
        "set" => {
            let value = value.as_ref().ok_or_else(|| {
                error!("Missing value for 'set' operation");
                anyhow!("Missing value for 'set' operation")
            })?;
            json!({
                "command": "set_key",
                "key":   base64::engine::general_purpose::STANDARD.encode(&key),
                "value": base64::engine::general_purpose::STANDARD.encode(&value),
                "cf":    "kv_pairs"
            })
        }
        "get" => json!({
            "command": "get_key",
            "key":   base64::engine::general_purpose::STANDARD.encode(&key),
            "cf":    "kv_pairs"
        }),
        "delete" => json!({
            "command": "delete_key",
            "key":   base64::engine::general_purpose::STANDARD.encode(&key),
            "cf":    "kv_pairs"
        }),
        "flush" => json!({ "command": "flush" }),
        _ => {
            error!("Unsupported operation: {}", operation);
            return Err(anyhow!("Unsupported operation: {}", operation));
        }
    };

    let request_data = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("Failed to serialize request: {}", e))?;

    // -----------------------------------------------------------------
    // 3. Send the request with retries
    // -----------------------------------------------------------------
    let mut last_error: Option<anyhow::Error> = None;
    for attempt in 1..=MAX_RETRIES {
        let response_result = timeout(
            TokioDuration::from_secs(CONNECT_TIMEOUT_SECS + REQUEST_TIMEOUT_SECS + RECEIVE_TIMEOUT_SECS),
            tokio::task::spawn_blocking({
                let addr = addr.clone();
                let request_data = request_data.clone();
                move || do_zmq_request(&addr, &request_data)
            }),
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
                                if let Some(encoded) = response_value.get("value") {
                                    if encoded.is_null() {
                                        println!("Key '{}': not found", key);
                                    } else {
                                        let b64 = encoded
                                            .as_str()
                                            .ok_or_else(|| anyhow!("'value' field is not a string"))?;
                                        let decoded = base64::engine::general_purpose::STANDARD
                                            .decode(b64)
                                            .map_err(|e| anyhow!("Base64 decode error: {}", e))?;
                                        let display = String::from_utf8_lossy(&decoded);
                                        println!("Key '{}': {}", key, display);
                                    }
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
                                if let Some(bytes) = response_value
                                    .get("bytes_flushed")
                                    .and_then(|b| b.as_u64())
                                {
                                    println!("Flushed database successfully: {} bytes", bytes);
                                } else {
                                    println!("Flushed database successfully");
                                }
                            }
                            _ => {}
                        }
                        return Ok(());
                    }
                    Some("error") => {
                        let msg = response_value
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Unknown error");
                        error!("Daemon error: {}", msg);
                        last_error = Some(anyhow!("Daemon error: {}", msg));
                    }
                    _ => {
                        error!("Invalid response: {:?}", response_value);
                        last_error = Some(anyhow!(
                            "Invalid response from {}: {:?}",
                            addr,
                            response_value
                        ));
                    }
                }
            }
            Ok(Err(e)) => {
                last_error = Some(anyhow!("Operation failed: {}", e));
                warn!(
                    "Attempt {}/{} failed: {}",
                    attempt,
                    MAX_RETRIES,
                    last_error.as_ref().unwrap()
                );
            }
            Err(_) => {
                last_error = Some(anyhow!(
                    "ZMQ operation timed out after {} seconds",
                    CONNECT_TIMEOUT_SECS + REQUEST_TIMEOUT_SECS + RECEIVE_TIMEOUT_SECS
                ));
                warn!("Attempt {}/{} timed out", attempt, MAX_RETRIES);
            }
        }

        if attempt < MAX_RETRIES {
            let delay = BASE_RETRY_DELAY_MS * 2u64.pow(attempt - 1);
            debug!("Retrying after {}ms", delay);
            tokio::time::sleep(TokioDuration::from_millis(delay)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow!(
            "Failed to complete ZMQ operation after {} attempts",
            MAX_RETRIES
        )
    }))
}

async fn handle_kv_rocksdb_zmq(key: String, value: Option<String>, operation: &str) -> Result<()> {
    const CONNECT_TIMEOUT_SECS: u64 = 3;
    const REQUEST_TIMEOUT_SECS: u64 = 10;
    const RECEIVE_TIMEOUT_SECS: u64 = 15;
    const MAX_RETRIES: u32 = 3;
    const BASE_RETRY_DELAY_MS: u64 = 500;

    info!("Starting ZMQ KV operation: {} for key: {}", operation, key);

    // -----------------------------------------------------------------
    // 1. Find a daemon with verified IPC socket, or recover
    // -----------------------------------------------------------------
    let registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let mut daemons = registry
        .get_all_daemon_metadata()
        .await
        .map_err(|e| anyhow!("Failed to retrieve daemon metadata: {}", e))?
        .into_iter()
        .filter(|metadata| {
            metadata.engine_type == Some(StorageEngineType::RocksDB.to_string())
                && metadata.pid > 0
                && check_pid_validity_sync(metadata.pid)
        })
        .collect::<Vec<_>>();

    if daemons.is_empty() {
        error!("No running RocksDB daemon found");
        return Err(anyhow!(
            "No running RocksDB daemon found. Please start a daemon with 'storage start'"
        ));
    }

    // Sort by port (highest first) to prefer the default
    daemons.sort_by_key(|m| std::cmp::Reverse(m.port));

    // Try to find a daemon with a valid IPC socket
    let mut selected_daemon: Option<DaemonMetadata> = None;
    let mut broken_daemons: Vec<u16> = Vec::new();
    
    for daemon in &daemons {
        let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            selected_daemon = Some(daemon.clone());
            info!("Selected daemon on port {} with verified IPC socket", daemon.port);
            break;
        } else {
            warn!(
                "Daemon on port {} exists but IPC socket {} is missing",
                daemon.port, socket_path
            );
            broken_daemons.push(daemon.port);
        }
    }

    // If no daemon has IPC, attempt recovery on the first broken one
    if selected_daemon.is_none() && !broken_daemons.is_empty() {
        let recovery_port = broken_daemons[0];
        warn!(
            "No RocksDB daemon has valid IPC - attempting recovery on port {}",
            recovery_port
        );
        println!(
            "===> WARNING: No daemon with IPC found - attempting recovery on port {}",
            recovery_port
        );

        // Load config for recovery
        let config = load_storage_config_from_yaml(None).await.map_err(|e| {
            anyhow!("Failed to load config for IPC recovery: {}", e)
        })?;

        // Stop the broken daemon
        if let Err(e) = stop_storage_daemon_by_port(recovery_port).await {
            warn!("Failed to stop broken daemon on port {}: {}", recovery_port, e);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Start fresh daemon with IPC
        let cwd = std::env::current_dir().context("Failed to get current working directory")?;
        let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        
        let mut recovery_config = config.clone();
        recovery_config.default_port = recovery_port;
        if let Some(ref mut ec) = recovery_config.engine_specific_config {
            ec.storage.port = Some(recovery_port);
            let base_dir = config
                .data_directory
                .as_ref()
                .unwrap_or(&PathBuf::from("/opt/graphdb/storage_data"))
                .display()
                .to_string();
            ec.storage.path = Some(PathBuf::from(format!("{}/rocksdb/{}", base_dir, recovery_port)));
        }

        let shutdown_tx = Arc::new(TokioMutex::new(None));
        let handle = Arc::new(TokioMutex::new(None));
        let port_arc = Arc::new(TokioMutex::new(None));

        start_storage_interactive(
            Some(recovery_port),
            Some(config_path),
            Some(recovery_config),
            Some("force_port".to_string()),
            shutdown_tx,
            handle,
            port_arc,
        )
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to restart RocksDB daemon on port {} for IPC recovery: {}",
                recovery_port, e
            )
        })?;

        // Wait for IPC to be ready
        let mut ipc_ready = false;
        for attempt in 0..30 {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            let socket_path = format!("/tmp/graphdb-{}.ipc", recovery_port);
            if tokio::fs::metadata(&socket_path).await.is_ok() {
                ipc_ready = true;
                info!("IPC recovered on port {} (attempt {})", recovery_port, attempt + 1);
                println!("===> IPC RECOVERED ON PORT {}", recovery_port);
                break;
            }
        }

        if !ipc_ready {
            return Err(anyhow!(
                "Failed to recover IPC on port {} after daemon restart",
                recovery_port
            ));
        }

        // Refresh daemon metadata
        if let Ok(Some(daemon_meta)) = registry.get_daemon_metadata(recovery_port).await {
            selected_daemon = Some(daemon_meta);
        } else {
            return Err(anyhow!(
                "Daemon restarted on port {} but not found in registry",
                recovery_port
            ));
        }
    }

    let daemon = selected_daemon.ok_or_else(|| {
        error!("No RocksDB daemon has a valid IPC socket and recovery failed");
        anyhow!(
            "RocksDB daemons found but none have valid IPC sockets and recovery failed. Try 'start-storage'."
        )
    })?;

    info!("Selected RocksDB daemon on port: {}", daemon.port);
    println!("===> Selected daemon on port: {}", daemon.port);

    let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);

    // Final verification
    if !tokio::fs::metadata(&socket_path).await.is_ok() {
        error!(
            "IPC socket file {} does not exist after all recovery attempts. Daemon port: {}.",
            socket_path, daemon.port
        );
        return Err(anyhow!(
            "IPC socket file {} does not exist after recovery attempts.",
            socket_path
        ));
    }

    debug!("Connecting to RocksDB daemon at: {}", addr);

    // -----------------------------------------------------------------
    // 2. Build the request (plain text, no Base64)
    // -----------------------------------------------------------------
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

    // -----------------------------------------------------------------
    // 3. Send the request with retries
    // -----------------------------------------------------------------
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
                        last_error = Some(anyhow!("Daemon error: {}", message));
                    }
                    _ => {
                        error!("Invalid response: {:?}", response_value);
                        last_error = Some(anyhow!("Invalid response from {}: {:?}", addr, response_value));
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
        Ok(c) => {
            info!("Loaded storage config: {:?}", c);
            println!("===> SUCCESSFULLY LOADED STORAGE CONFIG: {:?}", c);
            c
        },
        Err(e) => {
            warn!("Failed to load config, using default: {}", e);
            StorageConfig::default()
        }
    };
    
    let canonical_port = config
        .engine_specific_config
        .as_ref()
        .and_then(|esc| esc.storage.port)
        .unwrap_or(config.default_port);
    
    let cwd = std::env::current_dir().context("Failed to get current working directory")?;
    let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    
    // Early return if engine manager already exists
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        let engine = manager.get_persistent_engine().await;
        if engine.get_type() == config.storage_engine_type.to_string() {
            info!("StorageEngineManager already initialized. Reusing.");
            println!("===> STORAGE ENGINE MANAGER ALREADY INITIALIZED. REUSING.");
            let db = Database { storage: engine, config };
            return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
        }
    }
    
    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
    
    // ✅ NEW: Smart daemon selection - find one with working IPC
    info!("Finding daemon with working IPC for engine {:?}", config.storage_engine_type);
    println!("===> FINDING DAEMON WITH WORKING IPC FOR ENGINE {:?}", config.storage_engine_type);
    
    let working_port = match get_or_create_daemon_with_ipc(
        config.storage_engine_type.clone(),
        &config,
    ).await {
        Ok(port) => {
            info!("Found working daemon with IPC on port {}", port);
            println!("===> FOUND WORKING DAEMON WITH IPC ON PORT {}", port);
            port
        }
        Err(e) => {
            // No working daemon found - start one
            warn!("No working daemon found: {} - starting new daemon", e);
            println!("===> NO WORKING DAEMON FOUND - STARTING NEW DAEMON ON PORT {}", canonical_port);
            
            // Stop any broken daemon on canonical port
            if let Ok(Some(metadata)) = daemon_registry.get_daemon_metadata(canonical_port).await {
                if check_pid_validity(metadata.pid).await {
                    warn!("Stopping broken daemon on port {} before restart", canonical_port);
                    let _ = stop_storage_daemon_by_port(canonical_port).await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                }
            }
            
            // Start fresh daemon with IPC
            let shutdown_tx = Arc::new(TokioMutex::new(None));
            let handle = Arc::new(TokioMutex::new(None));
            let port_arc = Arc::new(TokioMutex::new(None));
            
            start_storage_interactive(
                Some(canonical_port),
                Some(config_path.clone()),
                Some(config.clone()),
                Some("force_port".to_string()),
                shutdown_tx,
                handle,
                port_arc,
            )
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to start storage daemon on port {}: {}",
                    canonical_port, e
                )
            })?;
            
            // Wait for daemon to be ready
            let mut ready = false;
            for attempt in 0..30 {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                
                if check_ipc_socket_exists(canonical_port).await {
                    if let Ok(Some(metadata)) = daemon_registry.get_daemon_metadata(canonical_port).await {
                        if check_pid_validity(metadata.pid).await {
                            ready = true;
                            break;
                        }
                    }
                }
                
                if attempt % 5 == 0 {
                    info!("Waiting for daemon IPC to be ready... (attempt {})", attempt + 1);
                }
            }
            
            if !ready {
                return Err(anyhow!(
                    "Daemon started on port {} but IPC socket not ready after 15 seconds",
                    canonical_port
                ));
            }
            
            println!("===> NEW DAEMON WITH IPC READY ON PORT {}", canonical_port);
            canonical_port
        }
    };
    
    // Verify IPC one more time
    verify_and_recover_ipc(working_port).await.map_err(|e| {
        anyhow!("IPC verification failed for port {}: {}", working_port, e)
    })?;
    
    // ✅ Create or reuse manager for the working port
    let manager = if let Some(m) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        m.clone()
    } else {
        let manager = StorageEngineManager::new(
            config.storage_engine_type.clone(),
            &config_path,
            false,
            Some(working_port),
        )
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to initialize StorageEngineManager for port {}: {}",
                working_port, e
            )
        })?;
        
        let arc_manager = Arc::new(AsyncStorageEngineManager::from_manager(manager));
        GLOBAL_STORAGE_ENGINE_MANAGER.set(arc_manager.clone())
            .map_err(|_| anyhow!("Failed to set StorageEngineManager"))?;
        arc_manager
    };
    
    let engine = manager.get_persistent_engine().await;
    let db = Database { storage: engine, config };
    Ok(Arc::new(QueryExecEngine::new(Arc::new(db))))
}
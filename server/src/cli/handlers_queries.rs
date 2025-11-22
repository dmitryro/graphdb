use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::path::{PathBuf, Path};
use std::pin::Pin;
use std::sync::Arc;
use std::os::unix::fs::PermissionsExt;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::{self, JoinHandle};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration};
use lib::daemon::daemon_management::{
    check_pid_validity, is_port_free, is_storage_daemon_running, find_pid_by_port,
    check_daemon_health, restart_daemon_process, stop_storage_daemon_by_port,
    check_pid_validity_sync, get_or_create_daemon_with_ipc, check_ipc_socket_exists,
    verify_and_recover_ipc, is_zmq_reachable,
};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{
    StorageEngineType, SledConfig, RocksDBConfig, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_DATA_DIRECTORY, default_data_directory, default_log_directory,
    daemon_api_storage_engine_type_to_string, load_cli_config,
};
use lib::storage_engine::storage_engine::{
    StorageEngine, GraphStorageEngine, AsyncStorageEngineManager, StorageEngineManager,
    GLOBAL_STORAGE_ENGINE_MANAGER,
};
use lib::commands::parse_kv_operation;
use lib::graph_engine::graph::Graph;
use lib::storage_engine::rocksdb_storage::{ROCKSDB_DB, ROCKSDB_POOL_MAP};
use lib::storage_engine::sled_storage::{SLED_DB, SLED_POOL_MAP};
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml, 
                  QueryPlan, QueryResult, SledDbWithPath, SledDaemon };
use lib::database::Database;
use lib::query_parser::{cypher_parser, sql_parser, graphql_parser};
use crate::cli::handlers_storage::{start_storage_interactive};
use crate::cli::handlers_utils::{ StartStorageFn, StopStorageFn };
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

pub async fn handle_cypher_query(
    engine: Arc<QueryExecEngine>,
    query: String,
) -> Result<()> {
    let trimmed_query = query.trim();
    
    if trimmed_query.is_empty() {
        return Err(anyhow!("Cypher query cannot be empty"));
    }

    info!("Executing Cypher query: {}", trimmed_query);

    let result = engine
        .execute_cypher(trimmed_query)
        .await
        .map_err(|e| anyhow!("Cypher execution failed: {e}"))?;

    let pretty_result = serde_json::to_string_pretty(&result)
        .map_err(|e| anyhow!("Failed to serialize query result: {e}"))?;

    println!("Cypher Query Result:\n{pretty_result}");
    
    Ok(())
}

pub async fn handle_sql_query(
    engine: Arc<QueryExecEngine>,
    query: String,
) -> Result<()> {
    info!("Executing SQL query: {}", query);
    println!("Executing SQL query: {}", query);

    if query.trim().is_empty() {
        return Err(anyhow!("SQL query cannot be empty"));
    }

    let result = engine
        .execute_sql(&query)
        .await
        .map_err(|e| anyhow!("Failed to execute SQL query: {}", e))?;

    println!("SQL Query Result:\n{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn handle_graphql_query(
    engine: Arc<QueryExecEngine>,
    query: String,
) -> Result<()> {
    info!("Executing GraphQL query: {}", query);
    println!("Executing GraphQL query: {}", query);

    if query.trim().is_empty() {
        return Err(anyhow!("GraphQL query cannot be empty"));
    }

    let result = engine
        .execute_graphql(&query)
        .await
        .map_err(|e| anyhow!("Failed to execute GraphQL query: {}", e))?;

    println!("GraphQL Query Result:\n{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn handle_unified_query(
    engine: Arc<QueryExecEngine>,
    query_string: String,
    language: Option<String>,
) -> Result<()> {
    info!("Executing unified query: {}", query_string);
    let query = query_string.trim().to_string();

    if query.is_empty() {
        return Err(anyhow!("Query cannot be empty"));
    }

    let effective_lang = language.as_deref().unwrap_or("").trim().to_lowercase();

    match effective_lang.as_str() {
        "cypher" | "" => handle_cypher_query(engine, query).await,
        "sql" => handle_sql_query(engine, query).await,
        "graphql" => handle_graphql_query(engine, query).await,
        _ => Err(anyhow!(
            "Unsupported language: '{}'. Use cypher, sql, or graphql",
            effective_lang
        )),
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
    let engine_type_lower = StorageEngineType::Sled.to_string().to_lowercase();
    
    let mut daemons = registry
        .get_all_daemon_metadata()
        .await
        .map_err(|e| anyhow!("Failed to retrieve daemon meta {}", e))?
        .into_iter()
        .filter(|metadata| {
            metadata.service_type == "storage"
                && metadata.engine_type.as_deref().map(|e| e.to_lowercase()) == Some(engine_type_lower.clone())
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
    
    info!("Found {} Sled daemon(s)", daemons.len());
    
    // Sort by port (highest first) to prefer the default
    daemons.sort_by_key(|m| std::cmp::Reverse(m.port));
    
    // First, try to find a daemon with a valid IPC socket
    let mut selected_daemon: Option<DaemonMetadata> = None;
    let mut daemon_with_process_no_ipc: Option<DaemonMetadata> = None;
    let mut broken_daemons: Vec<u16> = Vec::new();
   
    for daemon in &daemons {
        let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
        
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            selected_daemon = Some(daemon.clone());
            info!("Selected daemon on port {} with verified IPC socket", daemon.port);
            break;
        } else {
            // Check if process is running but IPC is missing
            if check_pid_validity_sync(daemon.pid) {
                warn!(
                    "Daemon on port {} exists (PID {}) but IPC socket {} is missing",
                    daemon.port, daemon.pid, socket_path
                );
                // Wait a bit to see if IPC socket appears
                for attempt in 0..5 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    if tokio::fs::metadata(&socket_path).await.is_ok() {
                        info!("IPC socket found after waiting (attempt {})", attempt + 1);
                        selected_daemon = Some(daemon.clone());
                        break;
                    }
                }
                if selected_daemon.is_none() {
                    daemon_with_process_no_ipc = Some(daemon.clone());
                }
            } else {
                warn!(
                    "Daemon on port {} has no running process, marking as broken",
                    daemon.port
                );
                broken_daemons.push(daemon.port);
            }
        }
    }
    
    // If no daemon with IPC, prioritize daemon that's running but missing IPC
    if selected_daemon.is_none() {
        if let Some(daemon_metadata) = daemon_with_process_no_ipc {
            warn!("Found daemon on port {} with running process but missing IPC, attempting extended wait", daemon_metadata.port);
            
            let socket_path = format!("/tmp/graphdb-{}.ipc", daemon_metadata.port);
            let mut ipc_ready = false;
            for attempt in 0..10 {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                if tokio::fs::metadata(&socket_path).await.is_ok() {
                    info!("IPC socket found after extended wait (attempt {})", attempt + 1);
                    selected_daemon = Some(daemon_metadata.clone());
                    ipc_ready = true;
                    break;
                }
            }
            
            if !ipc_ready {
                warn!("Daemon on port {} is running but IPC still not available", daemon_metadata.port);
                
                if let Ok(Some(refreshed_metadata)) = registry.get_daemon_metadata(daemon_metadata.port).await {
                    selected_daemon = Some(refreshed_metadata);
                } else {
                    broken_daemons.push(daemon_metadata.port);
                }
            }
        }
    }
    
    // If still no daemon selected and we have broken daemons, attempt recovery
    if selected_daemon.is_none() && !broken_daemons.is_empty() {
        let recovery_port = broken_daemons[0];
        warn!("No Sled daemon has valid IPC - attempting recovery on port {}", recovery_port);
        
        let daemon_metadata = registry.get_daemon_metadata(recovery_port).await
            .map_err(|e| anyhow!("Failed to get daemon metadata for port {}: {}", recovery_port, e))?
            .ok_or_else(|| anyhow!("No daemon found for port {} during recovery", recovery_port))?;
        
        if !check_pid_validity_sync(daemon_metadata.pid) {
            warn!("Daemon on port {} (PID {}) is not running, will restart", recovery_port, daemon_metadata.pid);
            
            let config = load_storage_config_from_yaml(None).await.map_err(|e| {
                anyhow!("Failed to load config for daemon restart: {}", e)
            })?;
            
            if let Err(e) = stop_storage_daemon_by_port(recovery_port).await {
                warn!("Failed to stop broken daemon on port {}: {}", recovery_port, e);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            
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
                    "Failed to restart Sled daemon on port {} for recovery: {}",
                    recovery_port, e
                )
            })?;
            
            let registry = GLOBAL_DAEMON_REGISTRY.get().await;
            let mut ready = false;
            for _ in 0..20 {
                if let Ok(Some(meta)) = registry.get_daemon_metadata(recovery_port).await {
                    if meta.zmq_ready {
                        ready = true;
                        break;
                    }
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            }
            if !ready {
                return Err(anyhow!("Daemon on port {} never became ready", recovery_port));
            }
        } else {
            warn!("Daemon on port {} (PID {}) is running but IPC socket is missing", recovery_port, daemon_metadata.pid);
            
            for attempt in 0..10 {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                let socket_path = format!("/tmp/graphdb-{}.ipc", recovery_port);
                if tokio::fs::metadata(&socket_path).await.is_ok() {
                    info!("IPC socket found after waiting (attempt {})", attempt + 1);
                    if let Ok(Some(daemon_meta)) = registry.get_daemon_metadata(recovery_port).await {
                        selected_daemon = Some(daemon_meta);
                    }
                    break;
                }
            }
        }
        
        let socket_path = format!("/tmp/graphdb-{}.ipc", recovery_port);
        let mut ipc_ready = false;
        for attempt in 0..30 {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            if tokio::fs::metadata(&socket_path).await.is_ok() {
                ipc_ready = true;
                info!("IPC ready on port {} (attempt {})", recovery_port, attempt + 1);
                break;
            }
        }
        
        if !ipc_ready {
            return Err(anyhow!(
                "Failed to establish IPC on port {} after recovery attempt",
                recovery_port
            ));
        }
        
        if let Ok(Some(daemon_meta)) = registry.get_daemon_metadata(recovery_port).await {
            selected_daemon = Some(daemon_meta);
        } else {
            return Err(anyhow!(
                "Daemon on port {} not found in registry after recovery",
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
    
    let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);
    
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
                "key": base64::engine::general_purpose::STANDARD.encode(&key),
                "value": base64::engine::general_purpose::STANDARD.encode(&value),
                "cf": "kv_pairs"
            })
        }
        "get" => json!({
            "command": "get_key",
            "key": base64::engine::general_purpose::STANDARD.encode(&key),
            "cf": "kv_pairs"
        }),
        "delete" => json!({
            "command": "delete_key",
            "key": base64::engine::general_purpose::STANDARD.encode(&key),
            "cf": "kv_pairs"
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
    let engine_type_lower = StorageEngineType::RocksDB.to_string().to_lowercase();
    
    let mut daemons = registry
        .get_all_daemon_metadata()
        .await
        .map_err(|e| anyhow!("Failed to retrieve daemon metadata: {}", e))?
        .into_iter()
        .filter(|metadata| {
            metadata.service_type == "storage"
                && metadata.engine_type.as_deref().map(|e| e.to_lowercase()) == Some(engine_type_lower.clone())
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
    
    info!("Found {} RocksDB daemon(s)", daemons.len());
    println!("===> FOUND {} ROCKSDB DAEMON(S)", daemons.len());
    
    // Sort by port (highest first) to prefer the default
    daemons.sort_by_key(|m| std::cmp::Reverse(m.port));
    
    // Try to find a daemon with a valid IPC socket
    let mut selected_daemon: Option<DaemonMetadata> = None;
    let mut daemon_with_process_no_ipc: Option<DaemonMetadata> = None;
    let mut broken_daemons: Vec<u16> = Vec::new();
   
    for daemon in &daemons {
        let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
        println!("===> CHECKING DAEMON ON PORT {} FOR IPC AT {}", daemon.port, socket_path);
        
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            selected_daemon = Some(daemon.clone());
            info!("Selected daemon on port {} with verified IPC socket", daemon.port);
            println!("===> SELECTED DAEMON ON PORT {} WITH VERIFIED IPC SOCKET", daemon.port);
            break;
        } else {
            // Check if process is running but IPC is missing
            if check_pid_validity_sync(daemon.pid) {
                warn!(
                    "Daemon on port {} exists (PID {}) but IPC socket {} is missing",
                    daemon.port, daemon.pid, socket_path
                );
                // Wait a bit to see if IPC socket appears
                for attempt in 0..5 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                    if tokio::fs::metadata(&socket_path).await.is_ok() {
                        info!("IPC socket found after waiting (attempt {})", attempt + 1);
                        selected_daemon = Some(daemon.clone());
                        break;
                    }
                }
                if selected_daemon.is_none() {
                    daemon_with_process_no_ipc = Some(daemon.clone());
                }
            } else {
                warn!(
                    "Daemon on port {} has no running process, marking as broken",
                    daemon.port
                );
                broken_daemons.push(daemon.port);
            }
        }
    }
    
    // If no daemon with IPC, prioritize daemon that's running but missing IPC
    if selected_daemon.is_none() {
        if let Some(daemon_metadata) = daemon_with_process_no_ipc {
            warn!("Found daemon on port {} with running process but missing IPC, attempting extended wait", daemon_metadata.port);
            println!("===> DAEMON ON PORT {} HAS NO IPC, ATTEMPTING EXTENDED WAIT", daemon_metadata.port);
            
            let socket_path = format!("/tmp/graphdb-{}.ipc", daemon_metadata.port);
            let mut ipc_ready = false;
            for attempt in 0..10 {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                if tokio::fs::metadata(&socket_path).await.is_ok() {
                    info!("IPC socket found after extended wait (attempt {})", attempt + 1);
                    println!("===> IPC SOCKET FOUND AFTER EXTENDED WAIT (ATTEMPT {})", attempt + 1);
                    selected_daemon = Some(daemon_metadata.clone());
                    ipc_ready = true;
                    break;
                }
            }
            
            if !ipc_ready {
                warn!("Daemon on port {} is running but IPC still not available", daemon_metadata.port);
                broken_daemons.push(daemon_metadata.port);
            }
        }
    }
    
    // If no daemon has IPC, attempt recovery on the first broken one
    if selected_daemon.is_none() && !broken_daemons.is_empty() {
        let recovery_port = broken_daemons[0];
        warn!("No RocksDB daemon has valid IPC - attempting recovery on port {}", recovery_port);
        println!("===> WARNING: No daemon with IPC found - attempting recovery on port {}", recovery_port);
        
        let config = load_storage_config_from_yaml(None).await.map_err(|e| {
            anyhow!("Failed to load config for IPC recovery: {}", e)
        })?;
        
        if let Err(e) = stop_storage_daemon_by_port(recovery_port).await {
            warn!("Failed to stop broken daemon on port {}: {}", recovery_port, e);
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        
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
    println!("===> SELECTED DAEMON ON PORT: {}", daemon.port);
    
    let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);
    
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
                        let message = response_value
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("Unknown error");
                        error!("Daemon error: {}", message);
                        last_error = Some(anyhow!("Daemon error: {}", message));
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
                last_error = Some(e.into());
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

pub async fn initialize_storage_for_query(
    start_storage_interactive: StartStorageFn,
    _stop_storage_interactive: StopStorageFn,
) -> Result<Arc<QueryExecEngine>, anyhow::Error> {
    static INIT_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = INIT_MUTEX.lock().await;

    // 1. load config
    let cwd = std::env::current_dir().context("cwd")?;
    let cfg_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let config = StorageConfig::load(&cfg_path).await?;

    // 2. reuse manager if it already serves the correct engine
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        let engine = manager.get_persistent_engine().await;
        if engine.get_type().to_lowercase() == config.storage_engine_type.to_string().to_lowercase() {
            info!("StorageEngineManager already initialized. Reusing.");
            println!("===> STORAGE ENGINE MANAGER ALREADY INITIALIZED. REUSING.");

            let db = Database {
                storage: engine,
                config: config.clone(),
            };
            return Ok(Arc::new(QueryExecEngine::new(Arc::new(db))));
        }
    }

    // 3. FIRST: look for ANY running daemon of the correct engine type
    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let engine_type_lower = config.storage_engine_type.to_string().to_lowercase();
    
    let running_daemons = daemon_registry
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|m| {
            m.service_type == "storage"
                && m.engine_type.as_deref().map(|e| e.to_lowercase()) == Some(engine_type_lower.clone())
                && m.pid > 0
                && check_pid_validity_sync(m.pid)
        })
        .collect::<Vec<_>>();

    info!("Found {} running daemon(s) for engine type {}", running_daemons.len(), config.storage_engine_type);
    println!("===> FOUND {} RUNNING DAEMON(S) FOR ENGINE TYPE {}", running_daemons.len(), config.storage_engine_type);

    // Determine canonical port: prefer a running daemon's port, fallback to config
    let canonical_port = if !running_daemons.is_empty() {
        // Sort by port (highest first) to prefer higher ports
        let mut sorted = running_daemons.clone();
        sorted.sort_by_key(|m| std::cmp::Reverse(m.port));
        
        let selected_port = sorted[0].port;
        info!("Using port {} from running daemon", selected_port);
        println!("===> USING PORT {} FROM RUNNING DAEMON", selected_port);
        selected_port
    } else {
        let fallback_port = config
            .engine_specific_config
            .as_ref()
            .and_then(|esc| esc.storage.port)
            .unwrap_or(config.default_port);
        info!("No running daemon found, using canonical port from config: {}", fallback_port);
        println!("===> NO RUNNING DAEMON FOUND, USING CANONICAL PORT FROM CONFIG: {}", fallback_port);
        fallback_port
    };

    // 4. ensure a daemon with working IPC exists on that exact port
    let working_port = find_or_create_working_daemon(
        config.storage_engine_type.clone(),
        &config,
        canonical_port,
        start_storage_interactive,
        cfg_path,
    )
    .await?;

    if !verify_and_wait_for_ipc(working_port).await {
        return Err(anyhow!("IPC not ready after verification for port {}", working_port));
    }

    // 5. create / reuse manager
    let manager = if let Some(m) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        m.clone()
    } else {
        let manager = StorageEngineManager::new(
            config.storage_engine_type.clone(),
            &cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE),
            false,
            Some(working_port),
        )
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to initialize StorageEngineManager for port {}: {}",
                working_port,
                e
            )
        })?;

        let arc_manager = Arc::new(AsyncStorageEngineManager::from_manager(manager));
        GLOBAL_STORAGE_ENGINE_MANAGER
            .set(arc_manager.clone())
            .map_err(|_| anyhow!("Failed to set StorageEngineManager"))?;
        arc_manager
    };

    let engine = manager.get_persistent_engine().await;
    let db = Database {
        storage: engine,
        config: config.clone(),
    };

    Ok(Arc::new(QueryExecEngine::new(Arc::new(db))))
}

pub async fn find_or_create_working_daemon(
    engine_type: StorageEngineType,
    config: &StorageConfig,
    canonical_port: u16,
    start_storage_interactive: StartStorageFn,
    config_path: PathBuf,
) -> Result<u16, anyhow::Error> {
    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let engine_type_lower = engine_type.to_string().to_lowercase();

    // 1. list running daemons of this engine
    let mut daemons = daemon_registry
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|m| {
            m.service_type == "storage"
                && m.engine_type.as_deref().map(|e| e.to_lowercase()) == Some(engine_type_lower.clone())
                && m.pid > 0
                && check_pid_validity_sync(m.pid)
        })
        .collect::<Vec<_>>();

    info!("Found {} daemon(s) for engine type {}", daemons.len(), engine_type);
    println!("===> FOUND {} DAEMON(S) FOR ENGINE TYPE {}", daemons.len(), engine_type);

    // 2. sort: canonical_port first, otherwise descending (prefer higher ports)
    daemons.sort_by(|a, b| {
        if a.port == canonical_port {
            std::cmp::Ordering::Less
        } else if b.port == canonical_port {
            std::cmp::Ordering::Greater
        } else {
            b.port.cmp(&a.port)  // descending order
        }
    });

    // 3. try to find a daemon that already has working IPC
    for daemon in &daemons {
        info!("Checking daemon on port {} for IPC", daemon.port);
        println!("===> CHECKING DAEMON ON PORT {} FOR IPC", daemon.port);

        if check_ipc_socket_exists(daemon.port).await && is_zmq_reachable(daemon.port).await {
            info!("Found daemon on port {} with working IPC", daemon.port);
            println!("===> FOUND DAEMON ON PORT {} WITH WORKING IPC", daemon.port);
            return Ok(daemon.port);
        }
        warn!("Daemon on port {} has no working IPC (exists: {}, reachable: {})",
              daemon.port,
              check_ipc_socket_exists(daemon.port).await,
              is_zmq_reachable(daemon.port).await);
        println!("===> DAEMON ON PORT {} HAS NO WORKING IPC", daemon.port);
    }

    // 4. if we have daemons but none with IPC, try to wait for the canonical port
    if !daemons.is_empty() {
        warn!("Found {} daemon(s) but none have working IPC, waiting for port {}", daemons.len(), canonical_port);
        println!("===> FOUND {} DAEMON(S) BUT NONE HAVE WORKING IPC, WAITING FOR PORT {}", daemons.len(), canonical_port);
        
        // Wait up to 15 seconds for IPC to appear
        for attempt in 0..30 {
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
            if check_ipc_socket_exists(canonical_port).await && is_zmq_reachable(canonical_port).await {
                info!("IPC appeared on port {} after waiting", canonical_port);
                println!("===> IPC APPEARED ON PORT {} AFTER WAITING (ATTEMPT {})", canonical_port, attempt + 1);
                return Ok(canonical_port);
            }
        }
    }

    // 5. none usable – start a fresh daemon on the canonical port
    warn!("No suitable daemon found, starting new daemon on port {}", canonical_port);
    println!("===> NO HEALTHY DAEMON FOUND - STARTING NEW ON PORT {}", canonical_port);

    if let Ok(Some(meta)) = daemon_registry.get_daemon_metadata(canonical_port).await {
        if check_pid_validity(meta.pid).await {
            let _ = stop_storage_daemon_by_port(canonical_port).await;
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    }

    let shutdown_tx = Arc::new(TokioMutex::new(None));
    let handle = Arc::new(TokioMutex::new(None));
    let port_arc = Arc::new(TokioMutex::new(None));

    start_storage_interactive(
        Some(canonical_port),
        Some(config_path),
        Some(config.clone()),
        Some("force_port".to_string()),
        shutdown_tx,
        handle,
        port_arc,
    )
    .await
    .map_err(|e| anyhow!("Failed to start daemon: {}", e))?;

    // wait until IPC is ready
    let mut ready = false;
    for attempt in 0..30 {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        if check_ipc_socket_exists(canonical_port).await && is_zmq_reachable(canonical_port).await {
            ready = true;
            println!("===> NEW DAEMON IPC READY ON PORT {} (ATTEMPT {})", canonical_port, attempt + 1);
            break;
        }
        if attempt > 0 && attempt % 5 == 0 {
            println!("===> WAITING FOR NEW DAEMON IPC ON PORT {} (ATTEMPT {})", canonical_port, attempt + 1);
        }
    }

    if !ready {
        return Err(anyhow!("New daemon on {} not ready with IPC after 30 attempts", canonical_port));
    }

    info!("New daemon started successfully on port {}", canonical_port);
    println!("===> NEW DAEMON STARTED SUCCESSFULLY ON PORT {}", canonical_port);
    Ok(canonical_port)
}

pub async fn verify_and_wait_for_ipc(port: u16) -> bool {
    // Verify IPC socket exists and is accessible
    for attempt in 0..30 {
        if check_ipc_socket_exists(port).await && is_zmq_reachable(port).await {
            info!("IPC verified and ready on port {}", port);
            println!("===> IPC VERIFIED AND READY ON PORT {}", port);
            return true;
        }
        
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        
        if attempt > 0 && attempt % 5 == 0 {
            println!("===> WAITING FOR IPC ON PORT {} (ATTEMPT {})", port, attempt + 1);
        }
    }
    
    warn!("IPC not ready after verification attempts for port {}", port);
    false
}

pub async fn execute_query(
    engine: Arc<QueryExecEngine>,
    query: String,          // ← Raw query string
    language: Option<String>,
) -> Result<(), anyhow::Error> {
    let lang = match language {
        Some(l) => l,
        None => return Err(anyhow!("Could not infer query language. Use --language sql|cypher")),
    };

    // PASS RAW QUERY STRING TO ENGINE — DO NOT PARSE HERE
    let result = match lang.as_str() {
        "cypher" => {
            // Engine handles parsing internally
            engine.execute_cypher(&query).await?
        },
        "sql" => {
            // Engine handles parsing internally
            engine.execute_sql(&query).await.map_err(|e| anyhow!(e))?
        },
        _ => return Err(anyhow!("Unsupported language: {}", lang)),
    };

    // Format and print result
    let output = serde_json::to_string_pretty(&result)
        .unwrap_or_else(|_| result.to_string());
    println!("{}", output);
    Ok(())
}

// Use this instead of format_result
fn format_json_result(result: &serde_json::Value) -> String {
    serde_json::to_string_pretty(result)
        .unwrap_or_else(|_| result.to_string())
}

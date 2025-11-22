use anyhow::{Result, anyhow, Context};
use std::path::PathBuf;
use tokio::sync::Mutex as TokioMutex;
use log::{info, debug, warn, error};
use serde_json::{json, Value};
use zmq::{Context as ZmqContext, Message}; // Using ZMQ directly
use tokio::time::{sleep, Duration as TokioDuration}; // Import for retry logic

// --- Project Imports ---
use lib::config::QueryResult; 
use lib::daemon::daemon_management::{
    check_pid_validity_sync,
};
use lib::daemon::daemon_registry::GLOBAL_DAEMON_REGISTRY;
// Updated import to include both IndexAction and SearchOrder
use lib::commands::{IndexAction, SearchOrder}; 
use lib::config::{ StorageConfig, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE };
use crate::cli::handlers_queries::{
    find_or_create_working_daemon,
    verify_and_wait_for_ipc,
};
use crate::cli::handlers_utils::{
    get_start_storage_fn,
    get_stop_storage_fn,
};
// -----------------------

// Global ZMQ context for ZMQ client operations, initialized once.
static ZMQ_CONTEXT: once_cell::sync::OnceCell<ZmqContext> = once_cell::sync::OnceCell::new();
fn zmq_context() -> &'static ZmqContext {
    ZMQ_CONTEXT.get_or_init(ZmqContext::new)
}

// Global cell to store the working port after initialization.
static STORAGE_PORT: once_cell::sync::OnceCell<u16> = once_cell::sync::OnceCell::new();

// Singleton mutex to prevent multiple simultaneous daemon startups/checks
static STARTUP_MUTEX: TokioMutex<()> = TokioMutex::const_new(());

const MAX_ZMQ_RETRIES: u32 = 5;
const INITIAL_RETRY_DELAY_MS: u64 = 500;

// Helper function to check if an index type string matches "FULLTEXT" case-insensitively.
// This logic would be used if IndexAction was a single generic CREATE variant.
// Currently, IndexAction variants separate the types, so this is illustrative.
fn is_fulltext_type(index_type: &str) -> bool {
    index_type.to_lowercase() == "fulltext"
}


/// Sends a raw JSON command string to the running storage daemon via ZMQ REQ/REP.
/// Includes retry logic for transient ZMQ connection/timeout failures.
async fn send_raw_zmq_request(port: u16, command: &str, params: Value) -> Result<QueryResult> {
    let payload_str = json!({
        "command": command,
        "params": params,
        "request_id": chrono::Utc::now().timestamp_nanos(), // Simple unique ID
    }).to_string();

    info!("Sending raw ZMQ request to port {}: {}...", port, command);

    for attempt in 0..MAX_ZMQ_RETRIES {
        let payload = payload_str.clone();

        let result = tokio::task::spawn_blocking(move || {
            let context = zmq_context();
            let socket = context.socket(zmq::REQ)
                .map_err(|e| anyhow!("ZMQ socket creation failed: {}", e))?;
            
            // Configure socket
            socket.set_linger(0).context("set_linger")?;
            socket.set_sndtimeo(5000).context("set_sndtimeo")?;
            socket.set_rcvtimeo(5000).context("set_rcvtimeo")?;
            
            // Connect via IPC
            let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
            socket.connect(&endpoint)
                .with_context(|| format!("ZMQ connect to {}", endpoint))?;
            
            // Send payload
            socket.send(&payload, 0).context("ZMQ send failed")?;
            
            // Receive response
            // The daemon panics and closes the connection if the IndexingService is not ready.
            // This causes the client's recv_bytes to fail with a ZMQ Error (ZMQ receive failed).
            let msg = socket.recv_bytes(0).context("ZMQ receive failed")?;
            
            Ok::<Vec<u8>, anyhow::Error>(msg)
        })
        .await
        .context("ZMQ spawn_blocking task failed");
        
        match result {
            Ok(Ok(msg_bytes)) => {
                let response_str = String::from_utf8(msg_bytes)
                    .context("Failed to decode ZMQ response as UTF-8")?;

                // Deserialize the response to check for status/errors
                let response_json: Value = serde_json::from_str(&response_str)
                    .context("Failed to parse ZMQ response JSON")?;
                
                // Check for explicit error status from the daemon
                if let Value::Object(map) = &response_json {
                    if map.get("status").and_then(|s| s.as_str()) == Some("error") {
                        let msg = map.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown daemon error");
                        error!("Daemon returned explicit error status: {}", msg);
                        // Do not retry on explicit internal error, only on connection/panic failures
                        return Err(anyhow!("Daemon error: {}", msg));
                    }
                }
                
                // Success
                return Ok(QueryResult::Success(response_str));
            }
            // Handle ZMQ connection or receive failure (often due to daemon panic/unprepared state)
            Ok(Err(e)) => {
                let error_message = format!("{}", e);
                if attempt < MAX_ZMQ_RETRIES - 1 {
                    let delay = INITIAL_RETRY_DELAY_MS * (1 << attempt);
                    warn!("ZMQ request failed (Attempt {}/{}) with: {}. Retrying in {}ms...", attempt + 1, MAX_ZMQ_RETRIES, error_message, delay);
                    sleep(TokioDuration::from_millis(delay)).await;
                    continue; // Retry the loop
                } else {
                    error!("ZMQ request failed after all {} attempts: {}", MAX_ZMQ_RETRIES, error_message);
                    return Err(anyhow!("ZMQ request failed after all attempts: {}", error_message));
                }
            }
            Err(e) => {
                // Handle tokio spawn_blocking JoinError
                return Err(anyhow!("ZMQ task join error: {}", e));
            }
        }
    }
    
    // Should be unreachable due to loop structure, but here for completeness
    Err(anyhow!("Exhausted ZMQ retries without success."))
}


/// Initializes the Storage Daemon and ensures it is running and accessible via ZMQ.
///
/// Returns the working ZMQ port of the running storage daemon.
pub async fn initialize_storage_for_index() -> Result<u16, anyhow::Error> {
    // Acquire the initialization guard to prevent race conditions
    let _guard = STARTUP_MUTEX.lock().await;

    // Check if port is already initialized (this block is needed for idempotent calls)
    if let Some(port) = STORAGE_PORT.get() {
        info!("Storage port already initialized to {}. Skipping full startup.", port);
        // Re-verify IPC access for safety
        if !verify_and_wait_for_ipc(*port).await {
            return Err(anyhow!("Pre-initialized IPC port {} is not responding.", port));
        }
        return Ok(*port);
    }

    // Retrieve function pointers from the global singletons
    let start_storage_interactive = get_start_storage_fn();
    let _stop_storage_interactive = get_stop_storage_fn(); 

    // 1. Load config
    let cwd = std::env::current_dir().context("cwd")?;
    let cfg_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let config = StorageConfig::load(&cfg_path).await?;
    
    // 2. Look for ANY running storage daemon of the correct engine type
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

    // 3. Determine canonical port: prefer a running daemon's port, fallback to config
    let canonical_port = if !running_daemons.is_empty() {
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

    // 4. Ensure a daemon with working IPC exists on that exact port (start one if necessary)
    let working_port = find_or_create_working_daemon(
        config.storage_engine_type.clone(),
        &config,
        canonical_port,
        start_storage_interactive,
        cfg_path,
    )
    .await?;

    // 5. Final verification of IPC readiness
    if !verify_and_wait_for_ipc(working_port).await {
        warn!("IPC not ready after verification for port {}", working_port);
        return Err(anyhow!("IPC not ready after verification for port {}", working_port));
    }
    
    // 6. NEW: Send an explicit, existing command (index_list) to confirm the IndexingService 
    // is initialized inside the daemon. The retry logic in send_raw_zmq_request handles the
    // possibility of a temporary panic during daemon initialization.
    info!("Sending index_list command to port {} as a functional readiness probe (with retries).", working_port);
    
    // We expect index_list to succeed eventually, which requires the IndexingService to be initialized.
    let readiness_result = send_raw_zmq_request(working_port, "index_list", json!({})).await;

    match readiness_result {
        Ok(_) => {
            println!("===> Indexing service confirmed initialized on daemon (using index_list probe).");
        }
        Err(e) => {
            // If index_list fails after all retries, it means the daemon failed
            // the core database/indexing initialization permanently.
            return Err(anyhow!("Daemon failed functional readiness check (index_list probe) on port {} after all retries: {}. Check daemon logs for initialization errors.", working_port, e));
        }
    }

    // Store the working port globally for subsequent calls to handle_index_command
    STORAGE_PORT.set(working_port)
        .map_err(|_| anyhow!("Failed to set global STORAGE_PORT instance."))?;

    println!("===> Storage daemon running and accessible on port {working_port}");

    Ok(working_port)
}

// Helper function to format and print the result, shared by both handlers
async fn format_and_print_result(command_name: &str, query_result: QueryResult, success_msg_prefix: String) -> Result<()> {
    // 4. Pattern match and parse the inner String payload into a mutable Value.
    let mut raw_result_value: Value = match query_result {
        QueryResult::Success(json_str) => {
            serde_json::from_str(&json_str)
                .context("Failed to parse JSON string from QueryResult::Success payload")?
        },
        QueryResult::Null => {
            // Null query results for index commands are handled as success with no data
            json!({ "status": "success", "message": "Command executed successfully with no explicit data payload." })
        }
        // Removed the unreachable `_` pattern as the compiler warned all variants were covered.
    };
    
    // --- FIX: Sanitize output by decoding inner 'result' field if it contains a JSON string ---
    if let Some(result_value) = raw_result_value.get_mut("result") {
        if let Value::String(inner_json_str) = result_value {
            match serde_json::from_str::<Value>(inner_json_str) {
                Ok(inner_value) => {
                    // Successfully parsed the inner JSON string into a structured Value.
                    // Replace the escaped string with the structured Value.
                    *result_value = inner_value;
                },
                Err(e) => {
                    // If it's a string but not valid JSON, just log a warning and keep it as a string.
                    warn!("Failed to parse inner 'result' string as JSON: {}", e);
                }
            }
        }
    }
    // -----------------------------------------------------------------------------------------

    // 5. Format and print result
    let output = serde_json::to_string_pretty(&raw_result_value)
        .unwrap_or_else(|_| raw_result_value.to_string());
    
    // Check for explicit error status in the result payload from the daemon
    if let Value::Object(map) = &raw_result_value {
        if map.get("status").and_then(|s| s.as_str()) == Some("error") {
            let msg = map.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown index error");
            error!("{} command failed: {}", command_name, msg);
            return Err(anyhow!("{} command failed on the storage daemon: {}", command_name, msg));
        }
    }
    
    // Successful execution
    println!("\n=== {} ===\n{}", success_msg_prefix, output);

    Ok(())
}

/// Handles all index-related CLI commands by routing them via ZMQ 
/// directly to the storage daemon using the globally stored port.
pub async fn handle_index_command(action: IndexAction) -> Result<()> {
    
    // 1. Get the port, initialize if necessary
    let port = match STORAGE_PORT.get() {
        Some(p) => *p,
        None => {
            // If the port isn't set, run the full initialization process.
            initialize_storage_for_index().await
                .context("Failed to initialize storage daemon before executing index command.")?
        }
    };
    
    // 2. Determine command, parameters, and expected output message
    let (command, params, success_msg_prefix) = match action {
        // FIX: Update pattern to match new field names (arg1, arg2, arg3_property)
        IndexAction::Create { arg1: label, arg2: property, arg3_property: _ } => {
            // NOTE: Command names use the daemon handler's expected 'index_' prefix.
            (
                "index_create".to_string(),
                json!({ "label": label, "property": property }),
                format!("B-Tree index created successfully on {}.{}", label, property),
            )
        },
        IndexAction::CreateFulltext { index_name, labels, properties } => {
            // This action already correctly routes to the full-text command.
            // If the index type name (e.g., "FULLTEXT") were passed here as a string argument
            // instead of being a separate enum variant, the case-insensitive logic (like
            // checking is_fulltext_type) would be applied before routing the command.
            (
                "index_create_fulltext".to_string(),
                json!({ 
                    "name": index_name,
                    "labels": labels, 
                    "properties": properties 
                }),
                format!("Full-text index '{}' created successfully", index_name),
            )
        },
        IndexAction::Drop { label, property } => {
            (
                "index_drop".to_string(),
                json!({ "label": label, "property": property }),
                format!("B-Tree index dropped successfully on {}.{}", label, property),
            )
        },
        IndexAction::DropFulltext { index_name } => {
            (
                "index_drop_fulltext".to_string(),
                json!({ "name": index_name }),
                format!("Full-text index '{}' dropped successfully", index_name),
            )
        },
        IndexAction::List => {
            (
                "index_list".to_string(),
                json!({}),
                "List of all indexes retrieved".to_string(),
            )
        },
        IndexAction::Search { term, order } => {
            // Default parameters: limit 10, descending order (highest score first)
            let mut limit = 10;
            let mut sort_order = "desc";

            if let Some(ord) = order {
                match ord {
                    SearchOrder::Top { count } | SearchOrder::Head { count } => {
                        limit = count;
                        sort_order = "desc";
                    }
                    SearchOrder::Bottom { count } | SearchOrder::Tail { count } => {
                        limit = count;
                        sort_order = "asc"; // Lowest score first
                    }
                }
            }
            
            (
                "index_search".to_string(),
                json!({ 
                    "query": term, 
                    "limit": limit,
                    "sort_order": sort_order 
                }),
                format!("Full-text search results for '{}' (limit {})", term, limit),
            )
        },
        IndexAction::Rebuild => {
            (
                "index_rebuild".to_string(),
                json!({}),
                "Index rebuild (All Indices) initiated".to_string(),
            )
        },
        IndexAction::Stats => {
            (
                "index_stats".to_string(),
                json!({}),
                "Index statistics retrieved".to_string(),
            )
        },
    };

    // 3. Execute the command directly via ZMQ (retries are handled internally)
    info!("Dispatching index command '{}' directly via ZMQ to port {}", command, port);
    let query_result: QueryResult = send_raw_zmq_request(port, &command, params).await?;

    // 4. Format and print result using the new helper
    format_and_print_result(&command, query_result, success_msg_prefix).await
}

// NOTE: handle_search_command has been removed as it relied on the now-removed SearchCommandData 
// struct and its functionality is duplicated by IndexAction::Search within handle_index_command.
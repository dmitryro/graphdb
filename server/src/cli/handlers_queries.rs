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

use anyhow::{Result, Context, anyhow};
use log::{info, debug, error, warn};
use serde_json::Value;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::fs;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration as TokioDuration};
use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::{QueryExecEngine};
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::storage_engine::config::{StorageEngineType,};
use lib::storage_engine::{TikvStorage,};
use lib::storage_engine::storage_engine::{AsyncStorageEngineManager, StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use super::commands::{KvAction, QueryArgs, parse_kv_operation};
use crate::cli::config::{DEFAULT_STORAGE_CONFIG_PATH_RELATIVE, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, StorageConfig, 
                         load_storage_config_from_yaml};
use crate::cli::handlers_storage::{ start_storage_interactive, stop_storage_interactive };
use crate::cli::daemon_management::{is_storage_daemon_running,};
pub use models::errors::GraphError;
// Note: `find_rest_api_port` and `GLOBAL_DAEMON_REGISTRY` are removed
// because we are no longer acting as a REST API client. The logic
// now executes the query directly.

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
    engine: Arc<QueryExecEngine>, // Now takes the engine directly
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
                // The QueryExecEngine will handle the actual parsing, but we can do a
                // basic syntax check here for better user feedback.
                if normalized_query.starts_with("MATCH") || normalized_query.starts_with("CREATE") || normalized_query.starts_with("MERGE") {
                    Ok("cypher")
                } else {
                    Err(anyhow!("Syntax conflict: --language cypher provided, but query does not appear to be a valid Cypher statement."))
                }
            },
            // Note: SQL and GraphQL are no longer supported in this refactored version
            // because the QueryExecEngine currently only supports a subset of Cypher.
            "sql" | "graphql" => {
                Err(anyhow!("Unsupported language flag: {}. Only a subset of Cypher is currently supported by the QueryExecEngine.", lang_lower))
            },
            _ => Err(anyhow!("Unsupported language flag: {}. Supported language is: cypher.", lang_lower))
        }
    } else {
        // Infer the language
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

    // Check for common commands first, to avoid treating them as invalid queries
    if normalized_query == "EXIT" || normalized_query == "QUIT" {
        return Ok(()); // This will be handled by the main loop.
    }

    // Since the QueryExecEngine currently only supports Cypher, we'll
    // directly call the unified handler and let it validate.
    handle_unified_query(engine, query_string, None).await
}

/// Handles the `exec` command to execute a command on the query engine.
pub async fn handle_exec_command(engine: Arc<QueryExecEngine>, command: String) -> Result<()> {
    info!("Executing command '{}' on QueryExecEngine", command);
    println!("Executing command '{}'", command);

    // Validate command is not empty
    if command.trim().is_empty() {
        return Err(anyhow!("Exec command cannot be empty. Usage: exec --command <command>"));
    }

    // Execute the command on the storage engine
    let result = engine
        .execute_command(&command)
        .await
        .map_err(|e| anyhow!("Failed to execute command '{}': {}", command, e))?;

    // Print the result
    println!("Command Result: {}", result);
    Ok(())
}

/// Handles the `query` command to execute a query on the query engine.
pub async fn handle_query_command(engine: Arc<QueryExecEngine>, query: String) -> Result<()> {
    info!("Executing query '{}' on QueryExecEngine", query);
    println!("Executing query '{}'", query);

    // Validate query is not empty
    if query.trim().is_empty() {
        return Err(anyhow!("Query cannot be empty. Usage: query --query <query>"));
    }

    // Parse the query to determine its type
    let query_type = parse_query_from_string(&query)
        .map_err(|e| anyhow!("Failed to parse query '{}': {}", query, e))?;

    // Execute the query based on its type
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

    // Print the query result
    println!("Query Result:\n{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

/// Handles the `kv` command for key-value operations on the query engine.
pub async fn handle_kv_command(engine: Arc<QueryExecEngine>, operation: String, key: String, value: Option<String>) -> Result<()> {
    // Validate the operation using parse_kv_operation
    println!("==> Time to do some kv queries {:?} - {:?}", key, value);
    let validated_op = parse_kv_operation(&operation)
        .map_err(|e| anyhow!("Invalid KV operation: {}", e))?;

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
            engine
                .kv_set(&key, &value)
                .await
                .map_err(|e| anyhow!("Failed to set key '{}': {}", key, e))?;
            println!("Successfully set key '{}' to '{}'", key, value);
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

/// This function ensures the `GLOBAL_STORAGE_ENGINE_MANAGER` is initialized
/// before any query-related commands are executed.
/// It loads the configuration and sets up the manager, but does not manage daemons.
pub async fn initialize_storage_for_query(
    start_storage_interactive: fn(
        Option<u16>,
        Option<PathBuf>,
        Option<StorageConfig>,
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
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|d| d.service_type == "storage")
        .collect();

    let mut successfully_stopped_ports: Vec<u16> = Vec::new();
    for daemon in &running_daemons {
        info!("Found running daemon on port {}, attempting to stop it...", daemon.port);
        let mut is_daemon_stopped = false;
        for attempt in 0..MAX_SHUTDOWN_RETRIES {
            debug!("Attempting to stop storage daemon on port {} (Attempt {} of {})", daemon.port, attempt + 1, MAX_SHUTDOWN_RETRIES);
            stop_storage_interactive(
                Some(daemon.port),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
                Arc::new(TokioMutex::new(None)),
            ).await.ok();
            if !is_storage_daemon_running(daemon.port).await {
                info!("Storage daemon on port {} is confirmed stopped.", daemon.port);
                is_daemon_stopped = true;
                break;
            }
            tokio::time::sleep(TokioDuration::from_millis(SHUTDOWN_RETRY_DELAY_MS)).await;
        }

        if is_daemon_stopped {
            successfully_stopped_ports.push(daemon.port);
        } else {
            return Err(anyhow!("Failed to stop existing storage daemon on port {}.", daemon.port));
        }
    }
    
    for daemon in &running_daemons {
        if successfully_stopped_ports.contains(&daemon.port) {
            let engine_type = daemon.engine_type
                .as_ref()
                .and_then(|s| s.parse::<StorageEngineType>().ok())
                .ok_or_else(|| anyhow!("Failed to parse engine type from running daemon: {}", daemon.engine_type.as_deref().unwrap_or("n/a")))?;
            
            let data_dir_path = PathBuf::from(daemon.config_path.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE)));
            
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
        }
    }
    
    if let Some(daemon) = running_daemons.first() {
        let expected_engine_type = daemon.engine_type
            .as_ref()
            .and_then(|s| s.parse::<StorageEngineType>().ok())
            .ok_or_else(|| anyhow!("Failed to parse engine type from running daemon: {}", daemon.engine_type.as_deref().unwrap_or("n/a")))?;

        let config_path = daemon.config_path.clone().unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE));

        let manager = StorageEngineManager::new(expected_engine_type.clone(), &config_path, false)
            .await
            .context("Failed to initialize StorageEngineManager")?;

        GLOBAL_STORAGE_ENGINE_MANAGER
            .set(Arc::new(AsyncStorageEngineManager::from_manager(
                Arc::try_unwrap(manager)
                    .map_err(|_| GraphError::ConfigurationError("Failed to unwrap Arc<StorageEngineManager>: multiple references exist".to_string()))?
            )))
            .context("Failed to set GLOBAL_STORAGE_ENGINE_MANAGER")?;

        info!("Successfully initialized storage engine {:?}", expected_engine_type);
        println!("===> Storage Engine initialized successfully.");
    } else {
        info!("No running daemon found, starting a new storage daemon.");
        println!("===> No running daemon found, starting a new storage daemon...");
        let cwd = std::env::current_dir().context("Failed to get current working directory")?;
        let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

        let current_config: StorageConfig = match load_storage_config_from_yaml(Some(config_path.clone())) {
            Ok(config) => {
                info!("Successfully loaded existing storage config: {:?}", config);
                config
            },
            Err(e) => {
                warn!("Failed to load existing config from {:?}, using default values. Error: {}", config_path, e);
                StorageConfig::default()
            }
        };

        let storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>> = Arc::new(TokioMutex::new(None));

        start_storage_interactive(
            Some(current_config.default_port),
            Some(config_path),
            Some(current_config),
            None,
            storage_daemon_shutdown_tx_opt,
            storage_daemon_handle,
            storage_daemon_port_arc,
        ).await.context("Failed to start storage daemon")?;

        info!("Successfully started a new storage daemon.");
        println!("===> New storage daemon started successfully.");
    }

    Ok(())
}



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

use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::fs;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration as TokioDuration};
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
        .get_all_daemon_metadata()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|d| d.service_type == "storage")
        .collect();

    if let Some(daemon) = running_daemons.first() {
        info!("Found running storage daemon on port {}, connecting to it...", daemon.port);

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
        info!("No running daemon found, starting a new storage daemon.");
        println!("===> No running daemon found, starting a new storage daemon...");
        let cwd = std::env::current_dir().context("Failed to get current working directory")?;
        let config_path = cwd.join(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);

        let cli_config = match load_storage_config_from_yaml(Some(config_path.clone())) {
            Ok(config) => {
                info!("Successfully loaded existing storage config: {:?}", config);
                config
            },
            Err(e) => {
                warn!("Failed to load existing config from {:?}, using default values. Error: {}", config_path, e);
                CliStorageConfig::default()
            }
        };
        let lib_config = convert_to_lib_storage_config(cli_config.clone());

        let storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>> = Arc::new(TokioMutex::new(None));
        let storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>> = Arc::new(TokioMutex::new(None));

        start_storage_interactive(
            Some(cli_config.default_port),
            Some(config_path),
            Some(cli_config),
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
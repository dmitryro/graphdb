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
use log::{info, debug, error};
use serde_json::Value;
use std::sync::Arc;
use lib::query_exec_engine::query_exec_engine::{QueryExecEngine};
use lib::query_parser::{parse_query_from_string, QueryType};
use super::commands::{KvAction, QueryArgs, parse_kv_operation};

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

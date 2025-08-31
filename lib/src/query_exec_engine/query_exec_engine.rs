// lib/src/query_exec_engine/query_exec_engine.rs
// Corrected: 2025-08-10 - Added a public method to retrieve the key-value store.
// Updated: 2025-08-31 - Added methods `execute_command`, `execute_cypher`, `execute_sql`, `execute_graphql`, `kv_get`, `kv_set`, `kv_delete` to support operations in handlers_queries.rs, fixing E0599 errors.
// Updated: 2025-08-31 - Fixed E0277 by removing `.await` from `kv_get`, `kv_set`, and `kv_delete` as KeyValueStore methods are synchronous. Fixed E0282 by adding explicit `GraphError` type annotations to `map_err` closures. Fixed E0308 by converting `&str` to `String` in `kv_set`.

use anyhow::{anyhow, Result};
use serde_json::Value;
use std::sync::Arc;

// Import the Cypher parsing and execution logic
use crate::query_parser::cypher_parser::{is_cypher, parse_cypher, execute_cypher};
// Import the database and key-value store components
pub use crate::database::Database;
pub use crate::query_parser::config::KeyValueStore;
use models::errors::GraphError;

/// The central query execution engine.
///
/// This struct holds references to the database and key-value store,
/// allowing it to execute queries and commands against them.
pub struct QueryExecEngine {
    db: Arc<Database>,
    kv_store: Arc<KeyValueStore>,
}

impl QueryExecEngine {
    /// Creates a new `QueryExecEngine` instance.
    ///
    /// # Arguments
    ///
    /// * `db` - An `Arc` to the core database instance.
    /// * `kv_store` - An `Arc` to the key-value store instance.
    pub fn new(db: Arc<Database>, kv_store: Arc<KeyValueStore>) -> Self {
        Self { db, kv_store }
    }

    /// Returns a shared reference to the internal `KeyValueStore`.
    ///
    /// This method provides a safe way for other components to interact
    /// with the key-value store without needing direct access to the
    /// engine's internal fields.
    pub fn get_kv_store(&self) -> Arc<KeyValueStore> {
        self.kv_store.clone()
    }

    /// Executes a given query string.
    ///
    /// This method first checks if the query is in a supported format (currently only Cypher).
    /// If it is, it parses the query and then executes it against the database and key-value store.
    ///
    /// # Arguments
    ///
    /// * `query` - The query string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the result of the query,
    /// or an `anyhow::Error` if parsing or execution fails.
    pub async fn execute(&self, query: &str) -> Result<Value> {
        // Trim whitespace from the query for robust parsing
        let trimmed_query = query.trim();

        // Check if the query is a Cypher query using the provided parser function
        if is_cypher(trimmed_query) {
            // Attempt to parse the Cypher query
            let parsed_query = parse_cypher(trimmed_query).map_err(|e| anyhow!(e))?;

            // Execute the parsed query against the database and key-value store
            let result = execute_cypher(parsed_query, &self.db, self.kv_store.clone()).await
                .map_err(|e| anyhow!(GraphError::from(e)))?;

            Ok(result)
        } else {
            // If the query is not recognized as Cypher, return an error
            Err(anyhow!("Unsupported query language or invalid syntax. Only a subset of Cypher is currently supported."))
        }
    }

    /// Executes a non-query command.
    ///
    /// This method handles arbitrary commands (e.g., shell-like commands) that are not
    /// specific to a query language.
    ///
    /// # Arguments
    ///
    /// * `command` - The command string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the result of the command,
    /// or an `anyhow::Error` if execution fails.
    pub async fn execute_command(&self, command: &str) -> Result<Value> {
        // Placeholder for command execution logic
        // This could be extended to handle specific non-query commands
        Err(anyhow!("Command execution not implemented for: {}", command))
    }

    /// Executes a Cypher query.
    ///
    /// This method explicitly handles Cypher queries, wrapping the `execute` method for clarity.
    ///
    /// # Arguments
    ///
    /// * `query` - The Cypher query string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the result of the query,
    /// or an `anyhow::Error` if parsing or execution fails.
    pub async fn execute_cypher(&self, query: &str) -> Result<Value> {
        self.execute(query).await
    }

    /// Executes an SQL query.
    ///
    /// This method is a placeholder for SQL query execution (not currently implemented).
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the result of the query,
    /// or an `anyhow::Error` if execution fails.
    pub async fn execute_sql(&self, query: &str) -> Result<Value> {
        Err(anyhow!("SQL query execution not implemented for: {}", query))
    }

    /// Executes a GraphQL query.
    ///
    /// This method is a placeholder for GraphQL query execution (not currently implemented).
    ///
    /// # Arguments
    ///
    /// * `query` - The GraphQL query string to execute.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `serde_json::Value` with the result of the query,
    /// or an `anyhow::Error` if execution fails.
    pub async fn execute_graphql(&self, query: &str) -> Result<Value> {
        Err(anyhow!("GraphQL query execution not implemented for: {}", query))
    }

    /// Retrieves a value from the key-value store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to retrieve.
    ///
    /// # Returns
    ///
    /// A `Result` containing an `Option<String>` with the value if found,
    /// or an `anyhow::Error` if the operation fails.
    pub async fn kv_get(&self, key: &str) -> Result<Option<String>> {
        self.kv_store.get(key).map_err(|e: GraphError| anyhow!(e))
    }

    /// Sets a value in the key-value store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to set.
    /// * `value` - The value to associate with the key.
    ///
    /// # Returns
    ///
    /// A `Result` indicating success or failure.
    pub async fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        self.kv_store.set(key.to_string(), value.to_string()).map_err(|e: GraphError| anyhow!(e))
    }

    /// Deletes a key from the key-value store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete.
    ///
    /// # Returns
    ///
    /// A `Result` containing a `bool` indicating whether the key existed and was deleted,
    /// or an `anyhow::Error` if the operation fails.
    pub async fn kv_delete(&self, key: &str) -> Result<bool> {
        self.kv_store.delete(key).map_err(|e: GraphError| anyhow!(e))
    }
}

use anyhow::{anyhow, Result};
use serde_json::Value;
use std::sync::Arc;
use crate::database::Database;
use models::errors::GraphError;
use crate::query_parser::cypher_parser::{is_cypher, parse_cypher, execute_cypher};

pub struct QueryExecEngine {
    db: Arc<Database>,
}

impl QueryExecEngine {
    /// Creates a new `QueryExecEngine` instance.
    ///
    /// # Arguments
    ///
    /// * `db` - An `Arc` to the core database instance.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Executes a given query string.
    ///
    /// This method checks if the query is in Cypher format, parses it, and executes it against the database.
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
        let trimmed_query = query.trim();
        if is_cypher(trimmed_query) {
            let parsed_query = parse_cypher(trimmed_query).map_err(|e| anyhow!(e))?;
            let result = execute_cypher(parsed_query, &self.db, self.db.get_storage_engine()).await
                .map_err(|e| anyhow!(GraphError::from(e)))?;
            Ok(result)
        } else {
            Err(anyhow!("Unsupported query language or invalid syntax. Only a subset of Cypher is currently supported."))
        }
    }

    /// Executes a non-query command.
    ///
    /// This method is a placeholder for non-query command execution.
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
        Err(anyhow!("Command execution not implemented for: {}", command))
    }

    /// Executes a Cypher query.
    ///
    /// Wraps the `execute` method for clarity.
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
    /// Placeholder for SQL query execution (not implemented).
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
    /// Placeholder for GraphQL query execution (not implemented).
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

    /// Retrieves a value from the storage engine.
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
        let kv_key = key.to_string().into_bytes();
        let storage = self.db.get_storage_engine();
        let value = storage.retrieve(&kv_key).await
            .map_err(|e: GraphError| anyhow!(e))?;
        Ok(value.map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    /// Sets a value in the storage engine.
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
        let kv_key = key.to_string().into_bytes();
        let storage = self.db.get_storage_engine();
        storage.insert(kv_key, value.as_bytes().to_vec()).await
            .map_err(|e: GraphError| anyhow!(e))?;
        storage.flush().await
            .map_err(|e: GraphError| anyhow!(e))
    }

    /// Deletes a key from the storage engine.
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
        let kv_key = key.to_string().into_bytes();
        let storage = self.db.get_storage_engine();
        let existed = storage.retrieve(&kv_key).await
            .map_err(|e: GraphError| anyhow!(e))?.is_some();
        if existed {
            storage.delete(&kv_key).await
                .map_err(|e: GraphError| anyhow!(e))?;
            storage.flush().await
                .map_err(|e: GraphError| anyhow!(e))?;
        }
        Ok(existed)
    }
}
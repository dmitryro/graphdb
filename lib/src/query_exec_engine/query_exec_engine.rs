// lib/src/query_exec_engine/query_exec_engine.rs
// Corrected: 2025-08-10 - Added a public method to retrieve the key-value store.
//
// This file defines the core query execution engine, which takes a raw query string,
// identifies the query language, and delegates the parsing and execution to the
// appropriate handler (e.g., the Cypher parser and executor).

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
/// allowing it to execute queries against them.
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
}
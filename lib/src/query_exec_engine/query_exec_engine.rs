use anyhow::{anyhow, Result};
use serde_json::Value;
use std::sync::Arc;
use log::{info, debug, warn};

use crate::database::Database;
use models::errors::GraphError;
use crate::query_parser::cypher_parser::{is_cypher, parse_cypher, execute_cypher};

// --- Added Imports for Index Command Handling ---
// Assuming these types are defined in the project configuration and storage engine modules.
use crate::config::QueryResult;
use crate::storage_engine::storage_engine::GraphStorageEngine;
// -----------------------------------------------

pub struct QueryExecEngine {
    db: Arc<Database>,
}

impl QueryExecEngine {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    /// Executes a generic index command by routing it to the underlying storage 
    /// engine's ZMQ client for daemon execution (SledDaemon/RocksDBDaemon).
    /// 
    /// This method delegates the ZMQ communication to the concrete 
    /// `GraphStorageEngine` implementation (SledStorage or RocksDBStorage), 
    /// which in turn uses its respective client to send the ZMQ request 
    /// to the daemon's `run_zmq_server_lazy`.
    ///
    /// # Arguments
    /// * `command` - The name of the index command (e.g., "create_index", "fulltext_rebuild").
    /// * `params` - A `serde_json::Value` containing command-specific parameters.
    pub async fn execute_index_command(
        &self, 
        command: &str, 
        params: Value
    ) -> Result<QueryResult> {
        info!("Executing index command: {} with params: {}", command, params);

        let storage = self.db.get_storage_engine();

        // DELEGATE TO THE STORAGE ENGINE
        // This relies on the GraphStorageEngine trait being updated to include 
        // the `execute_index_command` method which wraps the ZMQ client call.
        storage.execute_index_command(command, params).await
            .map_err(|e: GraphError| {
                warn!("Index command '{}' failed on storage engine: {}", command, e);
                // Convert GraphError to anyhow::Error for consistency with other public methods
                anyhow!(e)
            })
    }

    // --- Existing Methods ---

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

    pub async fn execute_command(&self, command: &str) -> Result<Value> {
        Err(anyhow!("Command execution not implemented for: {}", command))
    }

    pub async fn execute_cypher(&self, query: &str) -> Result<Value> {
        self.execute(query).await
    }

    pub async fn execute_sql(&self, query: &str) -> Result<Value> {
        Err(anyhow!("SQL query execution not implemented for: {}", query))
    }

    pub async fn execute_graphql(&self, query: &str) -> Result<Value> {
        Err(anyhow!("GraphQL query execution not implemented for: {}", query))
    }

    pub async fn kv_get(&self, key: &str) -> Result<Option<String>> {
        let kv_key = key.to_string().into_bytes();
        info!("Attempting to retrieve key '{}' from storage", key);
        println!("Attempting to retrieve key '{}' from storage", key);
        let storage = self.db.get_storage_engine();
        let value = storage.retrieve(&kv_key).await
            .map_err(|e: GraphError| {
                warn!("Failed to retrieve key '{}': {}", key, e);
                anyhow!(e)
            })?;
        info!("Retrieved value for key '{}': {:?}", key, value);
        println!("Retrieved value for key '{}': {:?}", key, value);
        Ok(value.map(|v| String::from_utf8_lossy(&v).to_string()))
    }

    pub async fn kv_set(&self, key: &str, value: &str) -> Result<()> {
        let kv_key = key.to_string().into_bytes();
        info!("Attempting to set key '{}' to value '{}'", key, value);
        println!("Attempting to set key '{}' to value '{}'", key, value);
        let storage = self.db.get_storage_engine();
        storage.insert(kv_key, value.as_bytes().to_vec()).await
            .map_err(|e: GraphError| {
                warn!("Failed to set key '{}': {}", key, e);
                anyhow!(e)
            })?;
        storage.flush().await
            .map_err(|e: GraphError| {
                warn!("Failed to flush after setting key '{}': {}", key, e);
                anyhow!(e)
            })?;
        info!("Successfully set key '{}' to '{}'", key, value);
        println!("Successfully set key '{}' to '{}'", key, value);
        Ok(())
    }

    pub async fn kv_delete(&self, key: &str) -> Result<bool> {
        let kv_key = key.to_string().into_bytes();
        info!("Attempting to delete key '{}'", key);
        println!("Attempting to delete key '{}'", key);
        let storage = self.db.get_storage_engine();
        let existed = storage.retrieve(&kv_key).await
            .map_err(|e: GraphError| {
                warn!("Failed to check if key '{}' exists: {}", key, e);
                anyhow!(e)
            })?.is_some();
        if existed {
            storage.delete(&kv_key).await
                .map_err(|e: GraphError| {
                    warn!("Failed to delete key '{}': {}", key, e);
                    anyhow!(e)
                })?;
            storage.flush().await
                .map_err(|e: GraphError| {
                    warn!("Failed to flush after deleting key '{}': {}", key, e);
                    anyhow!(e)
                })?;
            info!("Successfully deleted key '{}'", key);
        }
        Ok(existed)
    }
}

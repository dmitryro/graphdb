// lib/src/database.rs
// Refactored: 2025-07-04 - Updated to use new InMemoryGraphStorage and RocksdbGraphStorage.

use std::sync::Arc;
use std::path::{Path, PathBuf};

// Import the refactored storage engines
use crate::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngineType, open_sled_db};
use crate::storage_engine::sled_storage::SledStorage; // Explicitly import SledStorage
#[cfg(feature = "with-rocksdb")] // Apply cfg to the import itself
use crate::storage_engine::rocksdb_storage::RocksdbGraphStorage; // Explicitly import RocksdbGraphStorage
use crate::memory::InMemoryGraphStorage; // Import the new in-memory storage

use models::{Vertex, Edge, Identifier};
use models::errors::{GraphError, GraphResult}; // Use GraphResult directly
use uuid::Uuid;
use serde_json::Value; // For query results

/// A graph database wrapper that provides a simplified interface for
/// interacting with an underlying `GraphStorageEngine`.
///
/// This struct manages the storage engine instance and provides convenient methods
/// for graph operations.
#[derive(Debug)]
pub struct Database {
    storage_engine: Arc<dyn GraphStorageEngine>,
}

impl Database {
    /// Creates a new database instance based on the provided storage configuration.
    ///
    /// This function initializes and starts the chosen storage engine (Sled, RocksDB, or In-Memory).
    ///
    /// # Arguments
    /// * `config`: The configuration for the storage engine.
    ///
    /// # Returns
    /// A `GraphResult` indicating success or a `GraphError` if initialization fails.
    pub async fn new(config: StorageConfig) -> GraphResult<Self> {
        let storage_engine: Arc<dyn GraphStorageEngine> = match config.storage_engine_type {
            StorageEngineType::Sled => {
                let db_path = match &config.data_directory {
                    Some(path) => path.as_path(),
                    None => return Err(GraphError::ConfigError("Sled storage requires a data directory path.".to_string())),
                };
                let db = open_sled_db(db_path)?;
                Arc::new(SledStorage::new(db)?)
            },
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    Arc::new(RocksdbGraphStorage::new(&config)?)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    return Err(GraphError::ConfigError(
                        "RocksDB backend requested but 'with-rocksdb' feature is not enabled. \
                         Please enable it in lib/Cargo.toml".to_string()
                    ));
                }
            },
            StorageEngineType::InMemory => { // Added InMemory option
                let db_path = match &config.data_directory {
                    Some(path) => path.clone(),
                    None => return Err(GraphError::ConfigError("In-memory storage requires a data directory path.".to_string())),
                };
                Arc::new(InMemoryGraphStorage::new_with_path(db_path))
            }
            // Add other storage types here as they are implemented (e.g., PostgreSQL, Redis)
            _ => return Err(GraphError::ConfigError(
                format!("Unsupported storage engine type: {:?}", config.storage_engine_type)
            )),
        };

        // Start the chosen storage engine
        storage_engine.start().await?;

        Ok(Database { storage_engine })
    }

    /// Returns a reference to the underlying graph storage engine.
    pub fn storage(&self) -> &Arc<dyn GraphStorageEngine> {
        &self.storage_engine
    }

    // --- Proxy methods to GraphStorageEngine for convenience ---

    /// Creates a new vertex in the database.
    pub async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.storage_engine.as_ref().create_vertex(vertex).await
    }

    /// Retrieves a vertex by its ID.
    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        self.storage_engine.as_ref().get_vertex(id).await
    }

    /// Updates an existing vertex in the database.
    pub async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.storage_engine.as_ref().update_vertex(vertex).await
    }

    /// Deletes a vertex by its ID.
    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        self.storage_engine.as_ref().delete_vertex(id).await
    }

    /// Retrieves all vertices from the database.
    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        self.storage_engine.as_ref().get_all_vertices().await
    }

    /// Creates a new edge in the database.
    pub async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        self.storage_engine.as_ref().create_edge(edge).await
    }

    /// Retrieves an edge by its composite key (outbound ID, type, inbound ID).
    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        self.storage_engine.as_ref().get_edge(outbound_id, edge_type, inbound_id).await
    }

    /// Updates an existing edge in the database.
    pub async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.storage_engine.as_ref().update_edge(edge).await
    }

    /// Deletes an edge by its composite key.
    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        self.storage_engine.as_ref().delete_edge(outbound_id, edge_type, inbound_id).await
    }

    /// Retrieves all edges from the database.
    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        self.storage_engine.as_ref().get_all_edges().await
    }

    /// Executes a generic query string against the graph storage engine.
    /// The interpretation of the query string depends on the underlying engine.
    pub async fn query(&self, query_string: &str) -> GraphResult<Value> {
        self.storage_engine.as_ref().query(query_string).await
    }

    /// Returns the type of the underlying storage engine.
    pub fn get_type(&self) -> &'static str {
        self.storage_engine.as_ref().get_type()
    }

    /// Checks if the underlying storage engine is running.
    pub fn is_running(&self) -> bool {
        self.storage_engine.as_ref().is_running()
    }

    /// Stops the underlying storage engine gracefully.
    pub async fn stop(&self) -> GraphResult<()> {
        self.storage_engine.as_ref().stop().await
    }
}
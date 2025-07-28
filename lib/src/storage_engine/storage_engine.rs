// lib/src/storage_engine/storage_engine.rs
// Corrected: 2025-07-02 - This file now exclusively defines the StorageEngine and GraphStorageEngine traits.
// All concrete implementations (SledStorage, RocksDBStorage) and their specific helpers have been moved
// to their respective files (sled_storage.rs, rocksdb_storage.rs).

use async_trait::async_trait;
use serde_json::Value; // For the generic query result
use std::fmt::Debug;

// Import models for graph-specific operations
use models::{Edge, Identifier, Vertex};
use models::errors::GraphError; // Use GraphError from models crate
use uuid::Uuid; // For Vertex and Edge IDs

// Re-import StorageConfig from the new config module (used by implementations, not directly by traits here)
// use super::config::StorageConfig; // Not needed directly in this trait file

// --- Original `StorageEngine` Trait (Generic Key-Value Operations) ---
#[async_trait]
pub trait StorageEngine: Send + Sync + Debug + 'static {
    /// Connects to the storage backend.
    async fn connect(&self) -> Result<(), GraphError>;

    /// Inserts a key-value pair.
    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError>;
    
    /// Retrieves a value by key.
    async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError>;

    /// Deletes a key-value pair.
    async fn delete(&self, key: &[u8]) -> Result<(), GraphError>;

    /// Flushes any pending writes to persistent storage.
    async fn flush(&self) -> Result<(), GraphError>;
}

// --- New `GraphStorageEngine` Trait (Higher-Level Graph Operations) ---
/// A generic trait for graph data storage engines.
///
/// Implementations of this trait provide the core functionality for
/// interacting with the graph database, independent of the
/// underlying storage technology (e.g., Sled, RocksDB, PostgreSQL).
#[async_trait]
pub trait GraphStorageEngine: StorageEngine + Send + Sync + Debug + 'static { // GraphStorageEngine is now a supertrait of StorageEngine
    /// Initializes the storage engine, preparing it for operations.
    /// This might include opening connections, running migrations, etc.
    async fn start(&self) -> Result<(), GraphError>;

    /// Shuts down the storage engine gracefully, ensuring all data is persisted
    /// and resources are released.
    async fn stop(&self) -> Result<(), GraphError>;

    /// Returns a string identifier for the type of storage engine (e.g., "Sled", "RocksDB").
    fn get_type(&self) -> &'static str;

    /// Returns `true` if the storage engine is currently running and ready to accept operations.
    fn is_running(&self) -> bool;

    /// Executes a generic query against the graph storage engine.
    ///
    /// The `query_string` can be in any format (e.g., Cypher, SQL, custom DSL).
    /// The implementation will parse and execute it.
    /// Returns a `serde_json::Value` to represent flexible query results.
    async fn query(&self, query_string: &str) -> Result<Value, GraphError>;

    // --- Core Graph Operations ---
    // Vertex operations
    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError>;
    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError>;

    // Edge operations
    // Note: Edge ID is composite (outbound_id, t, inbound_id)
    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError>;
    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError>;

    // Additional operations for properties, etc., can be added here
}


// lib/src/storage_engine/storage_engine.rs

use serde::Deserialize;
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value; // Added: For the generic query result
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering}; // These are not strictly necessary in trait definition, but harmless.

// --- Original `StorageConfig` ---
#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    pub connection_string: String,
    // You might add more fields here relevant to generic storage configuration,
    // e.g., `db_path: Option<String>` for file-based databases.
}

// --- Updated Original `StorageEngine` Trait (Generic Key-Value Operations) ---
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Connects to the storage backend.
    async fn connect(&self) -> Result<()>;

    /// Inserts a key-value pair.
    async fn insert(&self, key: &str, value: &[u8]) -> Result<()>;

    /// Retrieves a value by key.
    async fn retrieve(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Deletes a key-value pair.
    async fn delete(&self, key: &str) -> Result<()>;
}

// --- New `GraphStorageEngine` Trait (Higher-Level Graph Operations) ---
// Assuming you have Vertex and Edge models defined elsewhere in lib::models
// For now, we'll keep them commented out to avoid compilation issues if they're not yet fully defined.
// use crate::models::graph::{Vertex, Edge};

/// A generic trait for graph data storage engines.
///
/// Implementations of this trait provide the core functionality for
/// interacting with the graph database, independent of the
/// underlying storage technology (e.g., Sled, RocksDB, PostgreSQL).
#[async_trait]
pub trait GraphStorageEngine: Send + Sync {
    /// Initializes the storage engine, preparing it for operations.
    /// This might include opening connections, running migrations, etc.
    async fn start(&self) -> Result<()>;

    /// Shuts down the storage engine gracefully, ensuring all data is persisted
    /// and resources are released.
    async fn stop(&self) -> Result<()>;

    /// Returns a string identifier for the type of storage engine (e.g., "sled", "rocksdb").
    fn get_type(&self) -> &'static str;

    /// Returns `true` if the storage engine is currently running and ready to accept operations.
    fn is_running(&self) -> bool;

    /// Executes a generic query against the graph storage engine.
    ///
    /// The `query_string` can be in any format (e.g., Cypher, SQL, custom DSL).
    /// The implementation will parse and execute it.
    /// Returns a `serde_json::Value` to represent flexible query results.
    async fn query(&self, query_string: &str) -> Result<Value>; // <-- This line was missing!

    // TODO: Add core graph operations here that are generic to any graph database.
    // Examples:
    // async fn add_vertex(&self, vertex: Vertex) -> Result<Vertex>;
    // async fn get_vertex(&self, id: &str) -> Result<Option<Vertex>>;
    // async fn update_vertex(&self, vertex: Vertex) -> Result<Vertex>;
    // async fn delete_vertex(&self, id: &str) -> Result<bool>;
    // async fn add_edge(&self, edge: Edge) -> Result<Edge>;
    // async fn get_edge(&self, id: &str) -> Result<Option<Edge>>;
    // async fn get_edges_from_vertex(&self, vertex_id: &str) -> Result<Vec<Edge>>;
}

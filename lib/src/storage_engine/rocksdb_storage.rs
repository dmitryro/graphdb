// lib/src/rocksdb_storage.rs
// Corrected: 2025-07-02 - Consolidated StorageEngine implementation into RocksDBStorage.
// Refactored: 2025-07-02 - Using common serialization/deserialization utilities from storage_utils.

#[cfg(feature = "with-rocksdb")]
use rocksdb::{DB, Options};
#[cfg(feature = "with-rocksdb")]
use async_trait::async_trait;
#[cfg(feature = "with-rocksdb")]
use std::path::Path;
#[cfg(feature = "with-rocksdb")]
use std::sync::Arc;
#[cfg(feature = "with-rocksdb")]
use std::fmt::Debug; // Required for Debug trait bound

#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::{GraphStorageEngine, StorageEngine, StorageConfig}; // Import both traits and StorageConfig
#[cfg(feature = "with-rocksdb")]
use models::{Edge, Identifier, Json, Vertex};
#[cfg(feature = "with-rocksdb")]
use models::errors::{GraphError, GraphResult};
#[cfg(feature = "with-rocksdb")]
use serde_json::Value; // For query results
#[cfg(feature = "with-rocksdb")]
use uuid::Uuid;

// Import the new utility functions
#[cfg(feature = "with-rocksdb")]
use super::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};


#[cfg(feature = "with-rocksdb")]
/// RocksDB-backed implementation of both `StorageEngine` and `GraphStorageEngine` traits.
/// This struct manages the underlying RocksDB database and provides methods for both
/// generic key-value operations and graph-specific operations.
#[derive(Debug)]
pub struct RocksDBStorage {
    db: Arc<DB>,
}

#[cfg(feature = "with-rocksdb")]
impl RocksDBStorage {
    /// Creates a new `RocksDBStorage` instance.
    pub fn new(config: &StorageConfig) -> GraphResult<Self> {
        let path = Path::new(&config.data_path);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(config.max_open_files.unwrap_or(-1)); // Use config value or default

        let db = DB::open(&options, path)
            .map_err(|e| GraphError::StorageError(format!("Failed to open RocksDB: {}", e)))?;

        Ok(RocksDBStorage { db: Arc::new(db) })
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl StorageEngine for RocksDBStorage {
    async fn connect(&self) -> GraphResult<()> {
        // RocksDB is "connected" when opened.
        Ok(())
    }

    async fn start(&self) -> GraphResult<()> {
        // RocksDB is "started" when opened.
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        // RocksDB handles flushing automatically. No explicit stop needed beyond dropping DB.
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "RocksDB"
    }

    fn is_running(&self) -> bool {
        // If the Arc<DB> is still valid, it's considered running.
        true
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.db.put(key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        self.db.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.db.delete(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        // RocksDB handles flushing automatically.
        Ok(())
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl GraphStorageEngine for RocksDBStorage {
    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let key = vertex.id.as_bytes();
        let value = serialize_vertex(&vertex)?; // Use utility function
        self.db.put(key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes();
        let result = self.db.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|bytes| deserialize_vertex(&bytes)).transpose() // Use utility function
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key = id.as_bytes();
        self.db.delete(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut vertices = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            // In a real scenario, you'd need to distinguish vertex keys from other keys
            // (e.g., by using column families or key prefixes).
            // For this example, we'll attempt to deserialize.
            if let Ok(vertex) = deserialize_vertex(&value) { // Use utility function
                vertices.push(vertex);
            }
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let key = create_edge_key(&edge.outbound_id, &edge.edge_type, &edge.inbound_id); // Use utility function
        let value = serialize_edge(&edge)?; // Use utility function
        self.db.put(key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let key = create_edge_key(outbound_id, edge_type, inbound_id); // Use utility function
        let result = self.db.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|bytes| deserialize_edge(&bytes)).transpose() // Use utility function
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let key = create_edge_key(outbound_id, edge_type, inbound_id); // Use utility function
        self.db.delete(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut edges = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start); // Iterate over all keys
        for item in iter {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            // In a real scenario, you'd need to distinguish edge keys from other keys.
            // For this example, we'll attempt to deserialize.
            if let Ok(edge) = deserialize_edge(&value) { // Use utility function
                edges.push(edge);
            }
        }
        Ok(edges)
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        // Placeholder for actual query logic.
        println!("Executing query: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "RocksDB query execution placeholder"
        }))
    }
}

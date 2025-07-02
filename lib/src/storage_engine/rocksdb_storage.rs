// lib/src/storage_engine/rocksdb_storage.rs
// Corrected: 2025-07-02 - Implemented the canonical StorageEngine and GraphStorageEngine traits.
// Fixed bincode serialization/deserialization imports and usage, and module imports.

#[cfg(feature = "with-rocksdb")]
use rocksdb::{DB, Options};
#[cfg(feature = "with-rocksdb")]
use async_trait::async_trait;
#[cfg(feature = "with-rocksdb")]
use std::path::Path;
#[cfg(feature = "with-rocksdb")]
use std::sync::Arc;
#[cfg(feature = "with-rocksdb")]
use std::sync::atomic::{AtomicBool, Ordering}; // For is_running state

#[cfg(feature = "with-rocksdb")]
use super::{GraphStorageEngine, StorageEngine}; // Corrected: Use super for sibling module imports
#[cfg(feature = "with-rocksdb")]
use crate::storage_engine::config::StorageConfig; // Corrected: Import StorageConfig from the config module
#[cfg(feature = "with-rocksdb")]
use models::{Edge, Identifier, Json, Vertex};
#[cfg(feature = "with-rocksdb")]
use models::errors::{GraphError, GraphResult}; // Use GraphResult from models::errors
#[cfg(feature = "with-rocksdb")]
use serde_json::Value; // For query results
#[cfg(feature = "with-rocksdb")]
use uuid::Uuid; // For Vertex and Edge IDs
#[cfg(feature = "with-rocksdb")]
use bincode::{encode_to_vec, decode_from_slice, config}; // Corrected: Use encode_to_vec and decode_from_slice from bincode 2.x


#[cfg(feature = "with-rocksdb")]
#[derive(Debug)]
pub struct RocksDBStorage {
    db: Arc<DB>, // Corrected: Changed to Arc<DB> for consistency and trait object usage
    is_started: AtomicBool, // Track if the storage is "started"
}

#[cfg(feature = "with-rocksdb")]
impl RocksDBStorage {
    /// Creates a new `RocksDBStorage` instance.
    pub fn new(config: &StorageConfig) -> GraphResult<Self> { // Changed return type to GraphResult
        let path = Path::new(&config.data_path); // Use data_path from StorageConfig
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_max_open_files(config.max_open_files.unwrap_or(-1)); // Use config value or default

        let db = DB::open(&options, path)
            .map_err(|e| GraphError::StorageError(format!("Failed to open RocksDB: {}", e)))?;

        Ok(RocksDBStorage {
            db: Arc::new(db), // Wrap DB in Arc
            is_started: AtomicBool::new(false), // Initialize as not started
        })
    }

    // Helper to get a key for RocksDB. RocksDB doesn't have "trees" like Sled,
    // so we'll prefix keys to differentiate vertex vs. edge data.
    fn vertex_key(id: &Uuid) -> Vec<u8> {
        format!("v_{}", id).into_bytes()
    }

    fn edge_key(outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Vec<u8> {
        // Corrected: Use edge_type instead of edge.t
        format!("e_{}_{}_{}", outbound_id, edge_type, inbound_id).into_bytes()
    }

    // Helper to serialize a Vertex to bytes
    fn serialize_vertex(vertex: &Vertex) -> GraphResult<Vec<u8>> {
        encode_to_vec(vertex, config::standard())
            .map_err(|e| GraphError::SerializationError(e.to_string()))
    }

    // Helper to deserialize bytes to a Vertex
    fn deserialize_vertex(bytes: &[u8]) -> GraphResult<Vertex> {
        decode_from_slice(bytes, config::standard())
            .map_err(|e| GraphError::DeserializationError(e.to_string()))
    }

    // Helper to serialize an Edge to bytes
    fn serialize_edge(edge: &Edge) -> GraphResult<Vec<u8>> {
        encode_to_vec(edge, config::standard())
            .map_err(|e| GraphError::SerializationError(e.to_string()))
    }

    // Helper to deserialize bytes to an Edge
    fn deserialize_edge(bytes: &[u8]) -> GraphResult<Edge> {
        decode_from_slice(bytes, config::standard())
            .map_err(|e| GraphError::DeserializationError(e.to_string()))
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl StorageEngine for RocksDBStorage {
    async fn start(&self) -> GraphResult<()> {
        if self.is_started.load(Ordering::SeqCst) {
            println!("RocksDB Storage already started.");
            return Ok(());
        }
        self.is_started.store(true, Ordering::SeqCst);
        println!("RocksDB Storage connected."); // Connection is established during new
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        if !self.is_started.load(Ordering::SeqCst) {
            println!("RocksDB Storage already stopped.");
            return Ok(());
        }
        // RocksDB handles persistence automatically. No explicit flush needed.
        self.is_started.store(false, Ordering::SeqCst);
        println!("RocksDB Storage stopped.");
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "RocksDB"
    }

    fn is_running(&self) -> bool {
        self.is_started.load(Ordering::SeqCst)
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.db
            .put(key, value)
            .map_err(|e| GraphError::StorageError(format!("Failed to insert data into RocksDB: {}", e)))?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        self.db.get(key)
            .map_err(|e| GraphError::StorageError(format!("Failed to retrieve data from RocksDB: {}", e)))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.db
            .delete(key)
            .map_err(|e| GraphError::StorageError(format!("Failed to delete data from RocksDB: {}", e)))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        self.db.flush()
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }
}

#[cfg(feature = "with-rocksdb")]
#[async_trait]
impl GraphStorageEngine for RocksDBStorage {
    async fn start(&self) -> GraphResult<()> {
        // Call the underlying StorageEngine's start
        <Self as StorageEngine>::start(self).await
    }

    async fn stop(&self) -> GraphResult<()> {
        // Call the underlying StorageEngine's stop
        <Self as StorageEngine>::stop(self).await
    }

    fn get_type(&self) -> &'static str {
        "RocksDBGraph"
    }

    fn is_running(&self) -> bool {
        <Self as StorageEngine>::is_running(self)
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let key = RocksDBStorage::vertex_key(&vertex.id);
        let value = Self::serialize_vertex(&vertex)?; // Use bincode
        self.db.put(key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = RocksDBStorage::vertex_key(id);
        let result = self.db.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|bytes| Self::deserialize_vertex(&bytes)).transpose() // Use bincode
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await // RocksDB put acts as upsert
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key = RocksDBStorage::vertex_key(id);
        self.db.delete(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut vertices = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start); // Iterate all keys
        for item in iter {
            let (key_bytes, value_bytes) = item
                .map_err(|e| GraphError::StorageError(format!("Error iterating RocksDB vertices: {}", e)))?;
            if String::from_utf8_lossy(&key_bytes).starts_with("v_") { // Filter for vertex keys
                let vertex: Vertex = Self::deserialize_vertex(&value_bytes)?; // Use bincode
                vertices.push(vertex);
            }
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let key = RocksDBStorage::edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id); // Corrected: edge.t
        let value = Self::serialize_edge(&edge)?; // Use bincode
        self.db.put(key, value)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let key = RocksDBStorage::edge_key(outbound_id, edge_type, inbound_id);
        let result = self.db.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|bytes| Self::deserialize_edge(&bytes)).transpose() // Use bincode
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let key = RocksDBStorage::edge_key(outbound_id, edge_type, inbound_id);
        self.db.delete(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut edges = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start); // Iterate all keys
        for item in iter {
            let (key_bytes, value_bytes) = item
                .map_err(|e| GraphError::StorageError(format!("Error iterating RocksDB edges: {}", e)))?;
            if String::from_utf8_lossy(&key_bytes).starts_with("e_") { // Filter for edge keys
                let edge: Edge = Self::deserialize_edge(&value_bytes)?; // Use bincode
                edges.push(edge);
            }
        }
        Ok(edges)
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        println!("Executing RocksDBGraphStorage query: {}", query_string);
        // Placeholder for actual query parsing and execution
        if query_string.trim().eq_ignore_ascii_case("count vertices") {
            let mut count = 0;
            let iter = self.db.iterator(rocksdb::IteratorMode::Start);
            for item in iter {
                let (key_bytes, _) = item
                    .map_err(|e| GraphError::StorageError(format!("Error iterating RocksDB for count: {}", e)))?;
                if String::from_utf8_lossy(&key_bytes).starts_with("v_") {
                    count += 1;
                }
            }
            return Ok(serde_json::json!({ "result": format!("Total vertices: {}", count) }));
        }
        if query_string.trim().eq_ignore_ascii_case("count edges") {
            let mut count = 0;
            let iter = self.db.iterator(rocksdb::IteratorMode::Start);
            for item in iter {
                let (key_bytes, _) = item
                    .map_err(|e| GraphError::StorageError(format!("Error iterating RocksDB for count: {}", e)))?;
                if String::from_utf8_lossy(&key_bytes).starts_with("e_") {
                    count += 1;
                }
            }
            return Ok(serde_json::json!({ "result": format!("Total edges: {}", count) }));
        }

        Ok(serde_json::json!({
            "status": "success",
            "query_executed": query_string,
            "result_type": "placeholder",
            "data": "Simulated query result from RocksDBGraphStorage"
        }))
    }
}


// lib/src/storage_engine/inmemory_storage.rs
// Created: 2025-07-04 - Implemented in-memory storage engine
// Fixed: 2025-08-14 - Corrected import path for `SerializableUuid`.
// Fixed: 2025-08-15 - Resolved E0308 mismatched types error in `is_running` function.
// Added: 2025-08-13 - Added `close` method to GraphStorageEngine implementation
// Fixed: 2025-08-19 - Resolved trait bound mismatch for StorageEngine methods.

use std::any::Any;
use async_trait::async_trait;
use crate::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngine};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use uuid::Uuid;
use log::info;

#[derive(Debug)]
pub struct InMemoryStorage {
    config: StorageConfig,
    vertices: Mutex<HashMap<Uuid, Vertex>>,
    edges: Mutex<HashMap<(Uuid, Identifier, Uuid), Edge>>,
    kv_store: Mutex<HashMap<Vec<u8>, Vec<u8>>>, // New field for generic key-value storage
    running: Mutex<bool>,
}

impl InMemoryStorage {
    pub fn new(config: &StorageConfig) -> Self {
        InMemoryStorage {
            config: config.clone(),
            vertices: Mutex::new(HashMap::new()),
            edges: Mutex::new(HashMap::new()),
            kv_store: Mutex::new(HashMap::new()),
            running: Mutex::new(false),
        }
    }

    pub fn reset(&mut self) -> GraphResult<()> {
        let mut vertices = self.vertices.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        let mut edges = self.edges.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        let mut kv_store = self.kv_store.lock().map_err(|e| GraphError::LockError(e.to_string()))?;

        vertices.clear();
        edges.clear();
        kv_store.clear();

        Ok(())
    }
}

#[async_trait]
impl StorageEngine for InMemoryStorage {
    async fn connect(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let mut kv_store = self.kv_store.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        kv_store.insert(key, value);
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let kv_store = self.kv_store.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        Ok(kv_store.get(key).cloned())
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let mut kv_store = self.kv_store.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        kv_store.remove(key);
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for InMemoryStorage {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let mut vertices = self.vertices.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        let mut edges = self.edges.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        
        vertices.clear();
        edges.clear();

        Ok(())
    }
    
    async fn start(&self) -> GraphResult<()> {
        let mut running = self.running.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        *running = true;
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        let mut running = self.running.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        *running = false;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "in-memory"
    }

    async fn is_running(&self) -> bool {
        *self.running.lock().unwrap()
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        println!("Executing query against InMemoryStorage: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "In-memory query execution placeholder"
        }))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut vertices = self.vertices.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        vertices.insert(vertex.id.0, vertex);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let vertices = self.vertices.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        Ok(vertices.get(id).cloned())
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut vertices = self.vertices.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        vertices.insert(vertex.id.0, vertex);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let mut vertices = self.vertices.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        vertices.remove(id);
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let vertices = self.vertices.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        Ok(vertices.values().cloned().collect())
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let mut edges = self.edges.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        edges.insert((edge.outbound_id.0, edge.t.clone(), edge.inbound_id.0), edge);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let edges = self.edges.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        Ok(edges.get(&(outbound_id.clone(), edge_type.clone(), inbound_id.clone())).cloned())
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let mut edges = self.edges.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        edges.remove(&(outbound_id.clone(), edge_type.clone(), inbound_id.clone()));
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let edges = self.edges.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        Ok(edges.values().cloned().collect())
    }

    async fn close(&self) -> GraphResult<()> {
        self.flush().await?;
        info!("InMemoryStorage closed");
        Ok(())
    }
}
// lib/src/storage_engine/inmemory_storage.rs
// ADDED: 2025-08-08 - Enhanced InMemoryStorage to support generic key-value operations for hybrid storage.
// UPDATED: 2025-08-08 - Improved thread safety and performance with RwLock.

use super::config::StorageConfig;
use super::storage_engine::{GraphStorageEngine, StorageEngine};
use async_trait::async_trait;
use models::{Edge, Identifier, Vertex};
use models::errors::GraphError;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[derive(Debug)]
pub struct InMemoryStorage {
    vertices: Arc<RwLock<HashMap<Uuid, Vertex>>>,
    edges: Arc<RwLock<HashMap<(Uuid, Identifier, Uuid), Edge>>>,
    kv_store: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>, // Added for generic K/V operations
    config: StorageConfig,
    running: Arc<RwLock<bool>>,
}

impl InMemoryStorage {
    pub fn new(config: StorageConfig) -> Result<Self, GraphError> {
        Ok(InMemoryStorage {
            vertices: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
            kv_store: Arc::new(RwLock::new(HashMap::new())),
            config,
            running: Arc::new(RwLock::new(false)),
        })
    }
}

#[async_trait]
impl StorageEngine for InMemoryStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        let mut running = self.running.write().await;
        *running = true;
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        let mut kv_store = self.kv_store.write().await;
        kv_store.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        let kv_store = self.kv_store.read().await;
        Ok(kv_store.get(key).cloned())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        let mut kv_store = self.kv_store.write().await;
        kv_store.remove(key);
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for InMemoryStorage {
    async fn start(&self) -> Result<(), GraphError> {
        let mut running = self.running.write().await;
        *running = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        let mut running = self.running.write().await;
        *running = false;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "InMemory"
    }

    fn is_running(&self) -> bool {
        *self.running.blocking_read()
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "InMemory query execution placeholder"
        }))
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let mut vertices = self.vertices.write().await;
        vertices.insert(vertex.id.0, vertex);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let vertices = self.vertices.read().await;
        Ok(vertices.get(id).cloned())
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let mut vertices = self.vertices.write().await;
        vertices.insert(vertex.id.0, vertex);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let mut vertices = self.vertices.write().await;
        vertices.remove(id);
        // Also remove associated edges
        let mut edges = self.edges.write().await;
        edges.retain(|(out_id, _, in_id), _| *out_id != *id && *in_id != *id);
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let vertices = self.vertices.read().await;
        Ok(vertices.values().cloned().collect())
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let mut edges = self.edges.write().await;
        edges.insert((edge.outbound_id.0, edge.t.clone(), edge.inbound_id.0), edge);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        let edges = self.edges.read().await;
        Ok(edges.get(&(*outbound_id, edge_type.clone(), *inbound_id)).cloned())
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let mut edges = self.edges.write().await;
        edges.insert((edge.outbound_id.0, edge.t.clone(), edge.inbound_id.0), edge);
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let mut edges = self.edges.write().await;
        edges.remove(&(*outbound_id, edge_type.clone(), *inbound_id));
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let edges = self.edges.read().await;
        Ok(edges.values().cloned().collect())
    }
}
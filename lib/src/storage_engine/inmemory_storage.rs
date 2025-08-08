// lib/src/storage_engine/inmemory_storage.rs
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
    config: StorageConfig,
    running: Arc<RwLock<bool>>,
}

impl InMemoryStorage {
    pub fn new(config: StorageConfig) -> Result<Self, GraphError> {
        Ok(InMemoryStorage {
            vertices: Arc::new(RwLock::new(HashMap::new())),
            edges: Arc::new(RwLock::new(HashMap::new())),
            config,
            running: Arc::new(RwLock::new(false)),
        })
    }
}

#[async_trait]
impl StorageEngine for InMemoryStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        Ok(())
    }

    async fn insert(&self, _key: &[u8], _value: &[u8]) -> Result<(), GraphError> {
        Ok(())
    }

    async fn retrieve(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        Ok(None)
    }

    async fn delete(&self, _key: &[u8]) -> Result<(), GraphError> {
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

    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        Ok(Value::Null)
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let mut vertices = self.vertices.write().await;
        vertices.insert(vertex.id.into(), vertex); // Convert SerializableUuid to Uuid
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let vertices = self.vertices.read().await;
        Ok(vertices.get(id).cloned())
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let mut vertices = self.vertices.write().await;
        vertices.insert(vertex.id.into(), vertex); // Convert SerializableUuid to Uuid
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let mut vertices = self.vertices.write().await;
        vertices.remove(id);
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let vertices = self.vertices.read().await;
        Ok(vertices.values().cloned().collect())
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let mut edges = self.edges.write().await;
        edges.insert(
            (edge.outbound_id.into(), edge.t.clone(), edge.inbound_id.into()), // Convert SerializableUuid to Uuid
            edge,
        );
        Ok(())
    }

    async fn get_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
    ) -> Result<Option<Edge>, GraphError> {
        let edges = self.edges.read().await;
        Ok(edges.get(&(*outbound_id, edge_type.clone(), *inbound_id)).cloned())
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let mut edges = self.edges.write().await;
        edges.insert(
            (edge.outbound_id.into(), edge.t.clone(), edge.inbound_id.into()), // Convert SerializableUuid to Uuid
            edge,
        );
        Ok(())
    }

    async fn delete_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
    ) -> Result<(), GraphError> {
        let mut edges = self.edges.write().await;
        edges.remove(&(*outbound_id, edge_type.clone(), *inbound_id));
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let edges = self.edges.read().await;
        Ok(edges.values().cloned().collect())
    }
}

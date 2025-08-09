// lib/src/sled_storage.rs
// Corrected: 2025-07-02 - Fixed compilation errors related to trait implementation,
// Uuid field access, and Edge type field name.
// Refactored: 2025-07-02 - Using common serialization/deserialization utilities from storage_utils.
// UNCHANGED: 2025-08-08 - Retained as-is for potential fallback or testing.

use async_trait::async_trait;
use sled::{Db, IVec};
use std::path::Path;
use std::sync::Arc;
use std::fmt::Debug;

use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use models::{Edge, Identifier, Json, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::Value;
use uuid::Uuid;

use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};

pub fn open_sled_db<P: AsRef<Path>>(path: P) -> GraphResult<Db> {
    sled::open(path).map_err(|e| GraphError::StorageError(format!("Failed to open Sled DB: {}", e)))
}

#[derive(Debug)]
pub struct SledStorage {
    db: Arc<Db>,
    vertex_tree: sled::Tree,
    edge_tree: sled::Tree,
}

impl SledStorage {
    pub fn new(db: Db) -> GraphResult<Self> {
        let db_arc = Arc::new(db);
        let vertex_tree = db_arc.open_tree("vertices")
            .map_err(|e| GraphError::StorageError(format!("Failed to open vertex tree: {}", e)))?;
        let edge_tree = db_arc.open_tree("edges")
            .map_err(|e| GraphError::StorageError(format!("Failed to open edge tree: {}", e)))?;
        Ok(SledStorage {
            db: db_arc,
            vertex_tree,
            edge_tree,
        })
    }
}

#[async_trait]
impl StorageEngine for SledStorage {
    async fn connect(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.db.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        self.db.get(key)
            .map(|opt_ivec| opt_ivec.map(|ivec| ivec.to_vec()))
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.db.remove(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        self.db.flush().map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SledStorage {
    async fn start(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        self.db.flush().map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "Sled"
    }

    fn is_running(&self) -> bool {
        true
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes();
        let value = serialize_vertex(&vertex)?;
        self.vertex_tree.insert(key, value.as_slice())
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes();
        let result = self.vertex_tree.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|ivec| deserialize_vertex(&ivec)).transpose()
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key = id.as_bytes();
        self.vertex_tree.remove(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut vertices = Vec::new();
        for item in self.vertex_tree.iter() {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            vertices.push(deserialize_vertex(&value)?);
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let key = create_edge_key(&edge.outbound_id.0, &edge.t, &edge.inbound_id.0);
        let value = serialize_edge(&edge)?;
        self.edge_tree.insert(key, value.as_slice())
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let key = create_edge_key(outbound_id, edge_type, inbound_id);
        let result = self.edge_tree.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|ivec| deserialize_edge(&ivec)).transpose()
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let key = create_edge_key(outbound_id, edge_type, inbound_id);
        self.edge_tree.remove(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut edges = Vec::new();
        for item in self.edge_tree.iter() {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            edges.push(deserialize_edge(&value)?);
        }
        Ok(edges)
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        println!("Executing query: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "Sled query execution placeholder"
        }))
    }
}

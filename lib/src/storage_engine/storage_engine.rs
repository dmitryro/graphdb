use async_trait::async_trait;
use sled::{Db, IVec};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;

use models::{Edge, Identifier, Json, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::Value;
use bincode::{encode_to_vec, decode_from_slice, config};
use uuid::Uuid;
use std::fmt::Debug;
use models::identifiers::{SerializableUuid, SerializableInternString};

#[async_trait]
pub trait StorageEngine: Send + Sync + Debug + 'static {
    async fn start(&self) -> GraphResult<()>;
    async fn stop(&self) -> GraphResult<()>;
    fn get_type(&self) -> &'static str;
    fn is_running(&self) -> bool;
    async fn set(&self, key: &[u8], value: &[u8]) -> GraphResult<()>;
    async fn get(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>>;
    async fn delete(&self, key: &[u8]) -> GraphResult<()>;
    async fn flush(&self) -> GraphResult<()>;
}

#[async_trait]
pub trait GraphStorageEngine: Send + Sync + Debug + 'static {
    async fn start(&self) -> GraphResult<()>;
    async fn stop(&self) -> GraphResult<()>;
    fn get_type(&self) -> &'static str;
    fn is_running(&self) -> bool;
    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()>;
    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>>;
    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()>;
    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()>;
    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>>;
    async fn create_edge(&self, edge: Edge) -> GraphResult<()>;
    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>>;
    async fn update_edge(&self, edge: Edge) -> GraphResult<()>;
    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()>;
    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>>;
    async fn query(&self, query_string: &str) -> GraphResult<Value>;
}

pub fn open_sled_db<P: AsRef<Path>>(path: P) -> GraphResult<Db> {
    sled::open(path).map_err(|e| GraphError::StorageError(format!("Failed to open Sled DB: {}", e)))
}

#[derive(Debug)]
pub struct SledStorage {
    db: Arc<Db>,
}

impl SledStorage {
    pub fn new(db: Db) -> GraphResult<Self> {
        Ok(SledStorage { db: Arc::new(db) })
    }
}

#[async_trait]
impl StorageEngine for SledStorage {
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

    async fn set(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.db.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
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

#[derive(Debug)]
pub struct SledGraphStorage {
    db: Arc<Db>,
    vertex_tree: sled::Tree,
    edge_tree: sled::Tree,
}

impl SledGraphStorage {
    pub fn new(db: Db) -> GraphResult<Self> {
        let db_arc = Arc::new(db);
        let vertex_tree = db_arc.open_tree("vertices")
            .map_err(|e| GraphError::StorageError(format!("Failed to open vertex tree: {}", e)))?;
        let edge_tree = db_arc.open_tree("edges")
            .map_err(|e| GraphError::StorageError(format!("Failed to open edge tree: {}", e)))?;

        Ok(SledGraphStorage {
            db: db_arc,
            vertex_tree,
            edge_tree,
        })
    }

    fn serialize_vertex(vertex: &Vertex) -> GraphResult<Vec<u8>> {
        encode_to_vec(vertex, config::standard())
            .map_err(|e| GraphError::SerializationError(e.to_string()))
    }

    fn deserialize_vertex(bytes: &[u8]) -> GraphResult<Vertex> {
        decode_from_slice(bytes, config::standard())
            .map(|(val, _)| val)
            .map_err(|e| GraphError::DeserializationError(e.to_string()))
    }

    fn serialize_edge(edge: &Edge) -> GraphResult<Vec<u8>> {
        encode_to_vec(edge, config::standard())
            .map_err(|e| GraphError::SerializationError(e.to_string()))
    }

    fn deserialize_edge(bytes: &[u8]) -> GraphResult<Edge> {
        decode_from_slice(bytes, config::standard())
            .map(|(val, _)| val)
            .map_err(|e| GraphError::DeserializationError(e.to_string()))
    }

    fn create_edge_key(outbound_id: &SerializableUuid, edge_type: &Identifier, inbound_id: &SerializableUuid) -> Vec<u8> {
        let mut key = Vec::new();
        key.extend_from_slice(outbound_id.0.as_bytes());
        key.extend_from_slice(edge_type.as_bytes());
        key.extend_from_slice(inbound_id.0.as_bytes());
        key
    }
}

#[async_trait]
impl GraphStorageEngine for SledGraphStorage {
    async fn start(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        self.db.flush().map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "SledGraph"
    }

    fn is_running(&self) -> bool {
        true
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes();
        let value = Self::serialize_vertex(&vertex)?;
        self.vertex_tree.insert(key, value.as_slice())
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes();
        let result = self.vertex_tree.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|ivec| Self::deserialize_vertex(&ivec)).transpose()
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
            vertices.push(Self::deserialize_vertex(&value)?);
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let key = Self::create_edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id);
        let value = Self::serialize_edge(&edge)?;
        self.edge_tree.insert(key, value.as_slice())
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let outbound_id = SerializableUuid(*outbound_id);
        let inbound_id = SerializableUuid(*inbound_id);
        let key = Self::create_edge_key(&outbound_id, edge_type, &inbound_id);
        let result = self.edge_tree.get(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        result.map(|ivec| Self::deserialize_edge(&ivec)).transpose()
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let outbound_id = SerializableUuid(*outbound_id);
        let inbound_id = SerializableUuid(*inbound_id);
        let key = Self::create_edge_key(&outbound_id, edge_type, &inbound_id);
        self.edge_tree.remove(key)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut edges = Vec::new();
        for item in self.edge_tree.iter() {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            edges.push(Self::deserialize_edge(&value)?);
        }
        Ok(edges)
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        println!("Executing query: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "query execution placeholder"
        }))
    }
}

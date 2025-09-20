//! This file contains a refactored and corrected Sled client implementation.
//! It addresses several compiler errors and warnings, primarily related to
//! Rust's "Orphan Rule" and incomplete trait implementations.

use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde_json::Value;
use sled::{open, Error, Db, Tree, IVec};
use tokio::sync::Mutex;
use uuid::Uuid;
use log::{info, debug, error, warn};

use models::{
    edges::Edge,
    errors::{GraphError, GraphResult},
    identifiers::{Identifier, SerializableUuid},
    vertices::Vertex,
};

use crate::config::{QueryResult, QueryPlan };
use crate::storage_engine::storage_engine::{GraphStorageEngine, StorageEngine};

// The SledClient struct now correctly implements the required traits.
#[derive(Clone, Debug)]
pub struct SledClient {
    // The inner database connection is wrapped in an Arc for shared ownership
    // and a Mutex for safe concurrent access.
    inner: Arc<Mutex<Db>>,
    db_path: PathBuf,
    is_running: bool,
}

impl SledClient {
    pub async fn new(db_path: PathBuf) -> Result<Self, Error> {
        let db = open(&db_path)?;
        Ok(SledClient {
            inner: Arc::new(Mutex::new(db)),
            db_path,
            is_running: false,
        })
    }

    async fn get_tree(&self, name: &str) -> GraphResult<Tree> {
        self.inner.lock().await.open_tree(name).map_err(GraphError::from)
    }

    // A utility function to save a key-value pair to a specific Sled tree.
    async fn insert_into_tree(&self, tree_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let tree = self.get_tree(tree_name).await?;
        tree.insert(key, value).map_err(GraphError::from)?;
        Ok(())
    }

    // A utility function to retrieve a value from a specific Sled tree.
    async fn retrieve_from_tree(&self, tree_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let tree = self.get_tree(tree_name).await?;
        // IVec must be converted to Vec<u8> to match the trait return type.
        tree.get(key).map_err(GraphError::from).map(|ivec| ivec.map(|v| v.to_vec()))
    }
}

// Implement the `StorageEngine` trait.
#[async_trait]
impl StorageEngine for SledClient {
    async fn flush(&self) -> Result<(), GraphError> {
        self.inner.lock().await.flush().map_err(GraphError::from)?;
        Ok(())
    }

    async fn connect(&self) -> Result<(), GraphError> {
        // This is a placeholder. A real implementation would handle connections.
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        self.insert_into_tree("data", &key, &value).await
    }

    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        self.retrieve_from_tree("data", &key).await
    }

    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        let tree = self.get_tree("data").await?;
        tree.remove(key).map_err(GraphError::from)?;
        Ok(())
    }
}

// Implement the `GraphStorageEngine` trait.
#[async_trait]
impl GraphStorageEngine for SledClient {
    async fn start(&self) -> Result<(), GraphError> {
        // Placeholder for starting the database
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        // Placeholder for stopping the database
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "sled"
    }

    async fn is_running(&self) -> bool {
        self.is_running
    }

    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        // Placeholder for query execution
        Err(GraphError::NotImplemented("SledClient query not implemented".to_string()))
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON Sled Client (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let _ = std::fs::remove_dir_all(&self.db_path);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        // Placeholder for closing the database connection.
        self.inner.lock().await.flush().map_err(GraphError::from)?;
        Ok(())
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let tree = self.get_tree("vertices").await?;
        // Use the SerializableUuid directly, which implements `Encode`.
        let key_bytes = bincode::encode_to_vec(&vertex.id, bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        let value_bytes = bincode::encode_to_vec(&vertex, bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        tree.insert(key_bytes, value_bytes).map_err(GraphError::from)?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let tree = self.get_tree("vertices").await?;
        // Create a temporary SerializableUuid to encode the key.
        let key_bytes = bincode::encode_to_vec(&SerializableUuid(*id), bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        let value_bytes = tree.get(key_bytes).map_err(GraphError::from)?.map(|ivec| ivec.to_vec());

        match value_bytes {
            Some(bytes) => {
                let (vertex, _): (Vertex, _) = bincode::decode_from_slice(&bytes, bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
                Ok(Some(vertex))
            }
            None => Ok(None),
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let tree = self.get_tree("vertices").await?;
        // Create a temporary SerializableUuid to encode the key.
        let key_bytes = bincode::encode_to_vec(&SerializableUuid(*id), bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        tree.remove(key_bytes).map_err(GraphError::from)?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let tree = self.get_tree("vertices").await?;
        let mut vertices = Vec::new();
        for item in tree.iter() {
            let (_, value_ivec) = item.map_err(GraphError::from)?;
            let value_bytes = value_ivec.to_vec();
            let (vertex, _): (Vertex, _) = bincode::decode_from_slice(&value_bytes, bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
            vertices.push(vertex);
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let tree = self.get_tree("edges").await?;
        // Use the SerializableUuid fields directly in the key tuple.
        let key_bytes = bincode::encode_to_vec((&edge.outbound_id, &edge.t, &edge.inbound_id), bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        let value_bytes = bincode::encode_to_vec(&edge, bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        tree.insert(key_bytes, value_bytes).map_err(GraphError::from)?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        let tree = self.get_tree("edges").await?;
        // Create temporary SerializableUuid wrappers to encode the key.
        let key_bytes = bincode::encode_to_vec((&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id)), bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        let value_bytes = tree.get(key_bytes).map_err(GraphError::from)?.map(|ivec| ivec.to_vec());

        match value_bytes {
            Some(bytes) => {
                let (edge, _): (Edge, _) = bincode::decode_from_slice(&bytes, bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
                Ok(Some(edge))
            }
            None => Ok(None),
        }
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let tree = self.get_tree("edges").await?;
        // Create temporary SerializableUuid wrappers to encode the key.
        let key_bytes = bincode::encode_to_vec((&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id)), bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
        tree.remove(key_bytes).map_err(GraphError::from)?;
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let tree = self.get_tree("edges").await?;
        let mut edges = Vec::new();
        for item in tree.iter() {
            let (_, value_ivec) = item.map_err(GraphError::from)?;
            let value_bytes = value_ivec.to_vec();
            let (edge, _): (Edge, _) = bincode::decode_from_slice(&value_bytes, bincode::config::standard()).map_err(|e| GraphError::InternalError(e.to_string()))?;
            edges.push(edge);
        }
        Ok(edges)
    }
}

// lib/src/storage_engine/redis_storage.rs
// Fixed: 2025-08-08 - Updated redis imports to match version 0.32.4
// Added: 2025-08-08 - Implemented GraphStorageEngine trait for RedisStorage
// Updated: 2025-08-13 - Made methods async, used tokio::sync::Mutex for connection
// Updated: 2025-08-13 - Aligned with GraphStorageEngine methods (create_vertex, etc.)
// Added: 2025-08-13 - Added close and as_any methods
// Fixed: 2025-08-13 - Used GraphError and &[u8] for key-value operations
// NOTE: Uses Redis SET/GET/DEL for key-value operations and separate namespaces (e.g., "vertex:", "edge:") for graph operations

use async_trait::async_trait;
use crate::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngine};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use serde_json::{Value, to_vec, from_slice};
use tokio::sync::Mutex;
use redis::{Client, AsyncCommands};
use uuid::Uuid;
use std::sync::Arc;

#[derive(Debug)]
pub struct RedisStorage {
    connection: Arc<Mutex<redis::aio::Connection>>,
}

impl RedisStorage {
    pub async fn new(connection: redis::aio::Connection) -> redis::RedisResult<Self> {
        Ok(RedisStorage {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub async fn from_config(config: &StorageConfig) -> GraphResult<Self> {
        let client = Client::open(config.connection_string.as_ref()
            .ok_or_else(|| GraphError::StorageError("Redis connection string is required".to_string()))?)
            .map_err(|e| GraphError::StorageError(format!("Failed to create Redis client: {}", e)))?;
        let connection = client.get_async_connection().await
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to Redis: {}", e)))?;
        Ok(RedisStorage {
            connection: Arc::new(Mutex::new(connection)),
        })
    }
}

#[async_trait]
impl StorageEngine for RedisStorage {
    async fn connect(&self) -> GraphResult<()> {
        // Connection is established during initialization
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let mut conn = self.connection.lock().await;
        conn.set(key, value).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let mut conn = self.connection.lock().await;
        conn.get(key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        let mut conn = self.connection.lock().await;
        conn.del(key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        // Redis doesn't have a flush operation in this context
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for RedisStorage {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn start(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "redis"
    }

    async fn is_running(&self) -> bool {
        true // Redis connection is always considered running
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        println!("Executing query against RedisStorage: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "Redis query execution placeholder"
        }))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let mut conn = self.connection.lock().await;
        let vertex_key = format!("vertex:{}", vertex.id.0);
        let value = to_vec(&vertex)
            .map_err(|e| GraphError::SerializationError(e.to_string()))?;
        conn.set(vertex_key, value).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let mut conn = self.connection.lock().await;
        let vertex_key = format!("vertex:{}", id);
        let result: Option<Vec<u8>> = conn.get(vertex_key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(result.map(|bytes| from_slice(&bytes))
            .transpose()
            .map_err(|e| GraphError::DeserializationError(e.to_string()))?)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let mut conn = self.connection.lock().await;
        let vertex_key = format!("vertex:{}", id);
        conn.del(vertex_key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        // Redis doesn't support iteration over keys easily; assume a pattern scan
        let mut conn = self.connection.lock().await;
        let keys: Vec<String> = conn.scan_match("vertex:*").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?
            .collect().await;
        let mut vertices = Vec::new();
        for key in keys {
            let value: Vec<u8> = conn.get(&key).await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertex: Vertex = from_slice(&value)
                .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
            vertices.push(vertex);
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let mut conn = self.connection.lock().await;
        let edge_key = format!("edge:{}:{}:{}", edge.outbound_id.0, edge.t, edge.inbound_id.0);
        let value = to_vec(&edge)
            .map_err(|e| GraphError::SerializationError(e.to_string()))?;
        conn.set(edge_key, value).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let mut conn = self.connection.lock().await;
        let edge_key = format!("edge:{}:{}:{}", outbound_id, edge_type, inbound_id);
        let result: Option<Vec<u8>> = conn.get(edge_key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(result.map(|bytes| from_slice(&bytes))
            .transpose()
            .map_err(|e| GraphError::DeserializationError(e.to_string()))?)
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let mut conn = self.connection.lock().await;
        let edge_key = format!("edge:{}:{}:{}", outbound_id, edge_type, inbound_id);
        conn.del(edge_key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut conn = self.connection.lock().await;
        let keys: Vec<String> = conn.scan_match("edge:*").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?
            .collect().await;
        let mut edges = Vec::new();
        for key in keys {
            let value: Vec<u8> = conn.get(&key).await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge: Edge = from_slice(&value)
                .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
            edges.push(edge);
        }
        Ok(edges)
    }

    async fn close(&self) -> GraphResult<()> {
        // Redis connection is dropped when the struct is dropped
        Ok(())
    }
}
use std::any::Any;
use std::collections::HashMap;
use std::path::PathBuf;
use std::str;
use std::sync::Arc;

use async_trait::async_trait;
use bincode::{Decode, Encode};
use serde_json::{json, Value};
use sled::{open, Db, Tree, IVec};
use tokio::sync::Mutex as TokioMutex;
use uuid::Uuid;
use log::{info, debug, error, warn};

use models::{
    edges::Edge,
    errors::{GraphError, GraphResult},
    identifiers::{Identifier, SerializableUuid},
    vertices::Vertex,
};

use crate::config::{QueryResult, QueryPlan, SledClientMode};
pub use crate::config::{ SledClient };
use crate::storage_engine::{ GraphStorageEngine, StorageEngine };

impl SledClient {
    pub async fn new(db_path: PathBuf) -> GraphResult<Self> {
        let db = open(&db_path).map_err(|e| GraphError::StorageError(format!("Failed to open Sled database: {}", e)))?;
        Ok(SledClient {
            inner: Arc::new(TokioMutex::new(Arc::new(db))),
            db_path,
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(SledClientMode::Direct),
        })
    }

    pub async fn new_with_port(port: u16) -> GraphResult<Self> {
        info!("Creating SledClient that connects to daemon on port {}", port);
        let dummy_path = PathBuf::from(format!("/tmp/sled-client-zmq-{}", port));
        Self::force_unlock(dummy_path.clone()).await?;

        if Self::ping_daemon(port).await.is_ok() {
            info!("Successfully connected to Sled daemon on port {}", port);
            let dummy_db = open(&dummy_path).map_err(|e| GraphError::StorageError(format!("Failed to create dummy Sled DB: {}", e)))?;
            Ok(SledClient {
                inner: Arc::new(TokioMutex::new(Arc::new(dummy_db))),
                db_path: dummy_path,
                is_running: Arc::new(TokioMutex::new(false)),
                mode: Some(SledClientMode::ZMQ(port)),
            })
        } else {
            Err(GraphError::StorageError(format!("Failed to connect to Sled daemon on port {}", port)))
        }
    }

    pub async fn new_with_db(db_path: PathBuf, db: Arc<Db>) -> GraphResult<Self> {
        Ok(SledClient {
            inner: Arc::new(TokioMutex::new(db)),
            db_path,
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(SledClientMode::Direct),
        })
    }

    pub async fn force_unlock(db_path: PathBuf) -> GraphResult<()> {
        let lock_path = db_path.join("db.lck");
        if tokio::fs::metadata(&lock_path).await.is_ok() {
            if let Err(e) = tokio::fs::remove_file(&lock_path).await {
                return Err(GraphError::StorageError(format!(
                    "Failed to remove stale db.lck file at {}: {}", lock_path.display(), e
                )));
            }
            info!("Removed stale Sled db.lck file at {}", lock_path.display());
        }
        Ok(())
    }

    async fn get_tree(&self, name: &str) -> GraphResult<Tree> {
        let db = self.inner.lock().await;
        (*db).open_tree(name).map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", name, e)))
    }

    async fn insert_into_tree(&self, tree_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let tree = self.get_tree(tree_name).await?;
        tree.insert(key, value).map_err(|e| GraphError::StorageError(format!("Failed to insert into tree {}: {}", tree_name, e)))?;
        Ok(())
    }

    async fn retrieve_from_tree(&self, tree_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let tree = self.get_tree(tree_name).await?;
        tree.get(key)
            .map_err(|e| GraphError::StorageError(format!("Failed to retrieve from tree {}: {}", tree_name, e)))
            .map(|ivec| ivec.map(|v| v.to_vec()))
    }

    async fn delete_from_tree(&self, tree_name: &str, key: &[u8]) -> GraphResult<()> {
        let tree = self.get_tree(tree_name).await?;
        tree.remove(key)
            .map_err(|e| GraphError::StorageError(format!("Failed to delete from tree {}: {}", tree_name, e)))?;
        Ok(())
    }

    async fn ping_daemon(port: u16) -> GraphResult<()> {
        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);

        if !tokio::fs::metadata(&socket_path).await.is_ok() {
            return Err(GraphError::StorageError(format!("IPC socket file {} does not exist", socket_path)));
        }

        let request = json!({ "command": "ping" });
        let request_data = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("Failed to serialize ping request: {}", e)))?;

        let response = tokio::task::spawn_blocking({
            let addr = addr.clone();
            let request_data = request_data.clone();
            move || -> Result<Value, GraphError> {
                let context = zmq::Context::new();
                let socket = context.socket(zmq::REQ)
                    .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;

                socket.set_rcvtimeo(5000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
                socket.set_sndtimeo(5000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

                socket.connect(&addr)
                    .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", addr, e)))?;

                socket.send(&request_data, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to send ping: {}", e)))?;

                let mut msg = zmq::Message::new();
                socket.recv(&mut msg, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to receive ping response: {}", e)))?;

                let response: Value = serde_json::from_slice(&msg)
                    .map_err(|e| GraphError::StorageError(format!("Failed to deserialize ping response: {}", e)))?;

                Ok(response)
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("ZMQ task failed: {}", e)))??;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            Ok(())
        } else {
            Err(GraphError::StorageError("Ping failed".to_string()))
        }
    }

    async fn send_zmq_request(&self, port: u16, request: Value) -> GraphResult<Value> {
        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);

        let request_data = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("Failed to serialize request: {}", e)))?;

        tokio::task::spawn_blocking({
            let addr = addr.clone();
            let request_data = request_data.clone();
            move || -> Result<Value, GraphError> {
                let context = zmq::Context::new();
                let socket = context.socket(zmq::REQ)
                    .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;

                socket.set_rcvtimeo(15000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
                socket.set_sndtimeo(10000)
                    .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

                socket.connect(&addr)
                    .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", addr, e)))?;

                socket.send(&request_data, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to send request: {}", e)))?;

                let mut msg = zmq::Message::new();
                socket.recv(&mut msg, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to receive response: {}", e)))?;

                let response: Value = serde_json::from_slice(&msg)
                    .map_err(|e| GraphError::StorageError(format!("Failed to deserialize response: {}", e)))?;

                Ok(response)
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("ZMQ task failed: {}", e)))?
    }

    async fn insert_into_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let request = json!({
            "command": "set_key",
            "key": String::from_utf8_lossy(key),
            "value": String::from_utf8_lossy(value),
            "cf": tree_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("ZMQ insert failed: {}", error_msg)))
        }
    }

    async fn retrieve_from_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let request = json!({
            "command": "get_key",
            "key": String::from_utf8_lossy(key),
            "cf": tree_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(value) = response.get("value") {
                if value.is_null() {
                    Ok(None)
                } else {
                    let value_str = value.as_str()
                        .ok_or_else(|| GraphError::StorageError("Invalid value format".to_string()))?;
                    Ok(Some(value_str.as_bytes().to_vec()))
                }
            } else {
                Ok(None)
            }
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("ZMQ retrieve failed: {}", error_msg)))
        }
    }

    async fn delete_from_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8]) -> GraphResult<()> {
        let request = json!({
            "command": "delete_key",
            "key": String::from_utf8_lossy(key),
            "cf": tree_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("ZMQ delete failed: {}", error_msg)))
        }
    }
}

#[async_trait]
impl StorageEngine for SledClient {
    async fn flush(&self) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.lock().await;
                (*db).flush().map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "flush" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(())
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ flush failed: {}", error_msg)))
                }
            }
            None => {
                let db = self.inner.lock().await;
                (*db).flush().map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                Ok(())
            }
        }
    }


    async fn connect(&self) -> GraphResult<()> {
        info!("Connecting to Sled");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.insert_into_tree("data", &key, &value).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.insert_into_cf_zmq(*port, "data", &key, &value).await
            }
            None => {
                self.insert_into_tree("data", &key, &value).await
            }
        }
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.retrieve_from_tree("data", key).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.retrieve_from_cf_zmq(*port, "data", key).await
            }
            None => {
                self.retrieve_from_tree("data", key).await
            }
        }
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.delete_from_tree("data", key).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.delete_from_cf_zmq(*port, "data", key).await
            }
            None => {
                self.delete_from_tree("data", key).await
            }
        }
    }
}

#[async_trait]
impl GraphStorageEngine for SledClient {
    async fn start(&self) -> GraphResult<()> {
        info!("Starting Sled database");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        info!("Stopping Sled database");
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        match &self.mode {
            Some(SledClientMode::Direct) => "sled",
            Some(SledClientMode::ZMQ(_)) => "sled_zmq",
            None => "sled",
        }
    }

    async fn is_running(&self) -> bool {
        let is_running = self.is_running.lock().await;
        *is_running
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        match &self.mode {
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "query", "query": query_string });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(response.get("value").cloned().unwrap_or(Value::Null))
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ query failed: {}", error_msg)))
                }
            }
            _ => {
                // Add repo version's NotImplemented error type
                Err(GraphError::NotImplemented("SledClient query not implemented for direct access".to_string()))
            }
        }
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> GraphResult<QueryResult> {
        match &self.mode {
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "execute_query", "query_plan": query_plan });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let result = response.get("value")
                        .map(|v| serde_json::from_value(v.clone()))
                        .transpose()
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize query result: {}", e)))?
                        .unwrap_or(QueryResult::Null);
                    Ok(result)
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ execute_query failed: {}", error_msg)))
                }
            }
            _ => {
                info!("Executing query on SledClient (returning null as not implemented)");
                println!("===> EXECUTING QUERY ON Sled Client (NOT IMPLEMENTED)");
                Ok(QueryResult::Null)
            }
        }
    }

    async fn clear_data(&self) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let _ = std::fs::remove_dir_all(&self.db_path);
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "clear_data" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(())
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ clear_data failed: {}", error_msg)))
                }
            }
            None => {
                let _ = std::fs::remove_dir_all(&self.db_path);
                Ok(())
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> GraphResult<()> {
        info!("Closing Sled database");
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.lock().await;
                (*db).flush().map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "close" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(())
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ close failed: {}", error_msg)))
                }
            }
            None => {
                let db = self.inner.lock().await;
                (*db).flush().map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                Ok(())
            }
        }
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        // Use repo version's approach with SerializableUuid for key encoding
        let key_bytes = bincode::encode_to_vec(&vertex.id, bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;
        let value_bytes = bincode::encode_to_vec(&vertex, bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.insert_into_tree("vertices", &key_bytes, &value_bytes).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.insert_into_cf_zmq(*port, "vertices", &key_bytes, &value_bytes).await
            }
            None => {
                self.insert_into_tree("vertices", &key_bytes, &value_bytes).await
            }
        }
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        // Use repo version's approach with temporary SerializableUuid
        let key_bytes = bincode::encode_to_vec(&SerializableUuid(*id), bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        let value_bytes = match &self.mode {
            Some(SledClientMode::Direct) => {
                self.retrieve_from_tree("vertices", &key_bytes).await?
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.retrieve_from_cf_zmq(*port, "vertices", &key_bytes).await?
            }
            None => {
                self.retrieve_from_tree("vertices", &key_bytes).await?
            }
        };

        match value_bytes {
            Some(bytes) => {
                let (vertex, _): (Vertex, _) = bincode::decode_from_slice(&bytes, bincode::config::standard())
                    .map_err(|e| GraphError::InternalError(e.to_string()))?;
                Ok(Some(vertex))
            }
            None => Ok(None),
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key_bytes = bincode::encode_to_vec(&SerializableUuid(*id), bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.delete_from_tree("vertices", &key_bytes).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.delete_from_cf_zmq(*port, "vertices", &key_bytes).await
            }
            None => {
                self.delete_from_tree("vertices", &key_bytes).await
            }
        }
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let tree = self.get_tree("vertices").await?;
                let mut vertices = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;
                    let value_bytes = value_ivec.to_vec();
                    let (vertex, _): (Vertex, _) = bincode::decode_from_slice(&value_bytes, bincode::config::standard())
                        .map_err(|e| GraphError::InternalError(e.to_string()))?;
                    vertices.push(vertex);
                }
                Ok(vertices)
            }
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "get_all_vertices" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let vertices: Vec<Vertex> = response.get("value")
                        .map(|v| serde_json::from_value(v.clone()))
                        .transpose()
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize vertices: {}", e)))?
                        .unwrap_or_default();
                    Ok(vertices)
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ get_all_vertices failed: {}", error_msg)))
                }
            }
            None => {
                let tree = self.get_tree("vertices").await?;
                let mut vertices = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;
                    let value_bytes = value_ivec.to_vec();
                    let (vertex, _): (Vertex, _) = bincode::decode_from_slice(&value_bytes, bincode::config::standard())
                        .map_err(|e| GraphError::InternalError(e.to_string()))?;
                    vertices.push(vertex);
                }
                Ok(vertices)
            }
        }
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        // Use repo version's direct field access approach
        let key_bytes = bincode::encode_to_vec((&edge.outbound_id, &edge.t, &edge.inbound_id), bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;
        let value_bytes = bincode::encode_to_vec(&edge, bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.insert_into_tree("edges", &key_bytes, &value_bytes).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.insert_into_cf_zmq(*port, "edges", &key_bytes, &value_bytes).await
            }
            None => {
                self.insert_into_tree("edges", &key_bytes, &value_bytes).await
            }
        }
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        // Use repo version's temporary SerializableUuid wrapper approach
        let key_bytes = bincode::encode_to_vec((&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id)), bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        let value_bytes = match &self.mode {
            Some(SledClientMode::Direct) => {
                self.retrieve_from_tree("edges", &key_bytes).await?
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.retrieve_from_cf_zmq(*port, "edges", &key_bytes).await?
            }
            None => {
                self.retrieve_from_tree("edges", &key_bytes).await?
            }
        };

        match value_bytes {
            Some(bytes) => {
                let (edge, _): (Edge, _) = bincode::decode_from_slice(&bytes, bincode::config::standard())
                    .map_err(|e| GraphError::InternalError(e.to_string()))?;
                Ok(Some(edge))
            }
            None => Ok(None),
        }
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let key_bytes = bincode::encode_to_vec((&SerializableUuid(*outbound_id), edge_type, &SerializableUuid(*inbound_id)), bincode::config::standard())
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.delete_from_tree("edges", &key_bytes).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.delete_from_cf_zmq(*port, "edges", &key_bytes).await
            }
            None => {
                self.delete_from_tree("edges", &key_bytes).await
            }
        }
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let tree = self.get_tree("edges").await?;
                let mut edges = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;
                    let value_bytes = value_ivec.to_vec();
                    let (edge, _): (Edge, _) = bincode::decode_from_slice(&value_bytes, bincode::config::standard())
                        .map_err(|e| GraphError::InternalError(e.to_string()))?;
                    edges.push(edge);
                }
                Ok(edges)
            }
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "get_all_edges" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let edges: Vec<Edge> = response.get("value")
                        .map(|v| serde_json::from_value(v.clone()))
                        .transpose()
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edges: {}", e)))?
                        .unwrap_or_default();
                    Ok(edges)
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ get_all_edges failed: {}", error_msg)))
                }
            }
            None => {
                let tree = self.get_tree("edges").await?;
                let mut edges = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;
                    let value_bytes = value_ivec.to_vec();
                    let (edge, _): (Edge, _) = bincode::decode_from_slice(&value_bytes, bincode::config::standard())
                        .map_err(|e| GraphError::InternalError(e.to_string()))?;
                    edges.push(edge);
                }
                Ok(edges)
            }
        }
    }
}
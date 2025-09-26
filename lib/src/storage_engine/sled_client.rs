use std::any::Any;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use sled::{open, Db, Tree, IVec};
use uuid::Uuid;
use log::{info, error, debug, warn};
use serde_json::{json, Value};
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
pub use crate::config::{QueryResult, QueryPlan, SledClient, SledClientMode, DEFAULT_STORAGE_PORT};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use tokio::time::{timeout, Duration as TokioDuration};
use base64::engine::general_purpose;
use base64::Engine;
use zmq::Socket;

// Wrapper for zmq::Socket to implement Debug
pub struct ZmqSocketWrapper(Socket);

impl ZmqSocketWrapper {
    pub fn new(socket: zmq::Socket) -> Self {
        ZmqSocketWrapper(socket)
    }
}

impl std::fmt::Debug for ZmqSocketWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqSocketWrapper")
            .field("socket", &"zmq::Socket")
            .finish()
    }
}

impl std::ops::Deref for ZmqSocketWrapper {
    type Target = Socket;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ZmqSocketWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl SledClient {
    pub async fn new(db_path: PathBuf) -> GraphResult<Self> {
        info!("Opening Sled database at {:?}", db_path);

        let db_path_clone = db_path.clone();
        let db = tokio::task::spawn_blocking(move || {
            let config = sled::Config::new()
                .path(&db_path_clone)
                .temporary(false);
            config.open()
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| GraphError::StorageError(format!("Failed to open Sled database: {}", e)))?;

        // Ensure necessary trees exist
        for tree_name in &["data", "vertices", "edges", "kv_pairs", "raft_log", "raft_snapshot", "raft_membership", "raft_vote"] {
            db.open_tree(tree_name)
                .map_err(|e| GraphError::StorageError(format!("Failed to create tree {}: {}", tree_name, e)))?;
        }

        // Log initial key count for debugging
        let kv_pairs = db.open_tree("kv_pairs")?;
        let key_count = kv_pairs.iter().count();
        info!("Opened database at {:?} with {} keys in kv_pairs", db_path, key_count);

        Ok(Self {
            inner: Arc::new(TokioMutex::new(Arc::new(db))),
            db_path,
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(SledClientMode::Direct),
        })
    }

    pub async fn new_with_db(db_path: PathBuf, db: Arc<Db>) -> GraphResult<Self> {
        info!("Creating SledClient with existing database at {:?}", db_path);
        // Ensure necessary trees exist
        for tree_name in &["data", "vertices", "edges", "kv_pairs", "raft_log", "raft_snapshot", "raft_membership", "raft_vote"] {
            db.open_tree(tree_name)
                .map_err(|e| GraphError::StorageError(format!("Failed to create tree {}: {}", tree_name, e)))?;
        }

        // Log initial key count for debugging
        let kv_pairs = db.open_tree("kv_pairs")?;
        let key_count = kv_pairs.iter().count();
        info!("Opened database at {:?} with {} keys in kv_pairs", db_path, key_count);

        Ok(Self {
            inner: Arc::new(TokioMutex::new(db)),
            db_path,
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(SledClientMode::Direct),
        })
    }

    pub async fn new_with_port(port: u16) -> GraphResult<(Self, Arc<TokioMutex<ZmqSocketWrapper>>)> {
        info!("Creating SledClient for ZMQ connection on port {}", port);

        // Create socket directory
        let socket_dir = PathBuf::from("/opt/graphdb");
        if let Err(e) = tokio::fs::create_dir_all(&socket_dir).await {
            error!("Failed to create socket directory {}: {}", socket_dir.display(), e);
            return Err(GraphError::StorageError(format!("Failed to create socket directory {}: {}", socket_dir.display(), e)));
        }
        info!("Ensured socket directory exists at {}", socket_dir.display());

        // Create a dummy database for `inner`
        let dummy_path = PathBuf::from(format!("/tmp/sled-client-zmq-{}", port));
        let db = tokio::task::spawn_blocking(move || {
            let config = sled::Config::new()
                .path(&dummy_path)
                .temporary(true);
            config.open()
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to create dummy DB: {}", e)))?
        .map_err(|e| GraphError::StorageError(format!("Failed to create dummy DB: {}", e)))?;

        // Ensure necessary trees exist in dummy database
        for tree_name in &["data", "vertices", "edges", "kv_pairs", "raft_log", "raft_snapshot", "raft_membership", "raft_vote"] {
            db.open_tree(tree_name)
                .map_err(|e| GraphError::StorageError(format!("Failed to create tree {}: {}", tree_name, e)))?;
        }

        // Create ZMQ socket
        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;
        socket.set_rcvtimeo(15000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        socket.set_sndtimeo(10000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;
        socket.set_linger(1000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set linger: {}", e)))?;
        socket.set_maxmsgsize(1024 * 1024)
            .map_err(|e| GraphError::StorageError(format!("Failed to set max message size: {}", e)))?;
        socket.connect(&addr)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", addr, e)))?;
        let socket_wrapper = Arc::new(TokioMutex::new(ZmqSocketWrapper::new(socket)));

        let client = Self {
            inner: Arc::new(TokioMutex::new(Arc::new(db))),
            db_path: PathBuf::from(format!("/opt/graphdb/storage_data/sled/{}", port)),
            is_running: Arc::new(TokioMutex::new(true)),
            mode: Some(SledClientMode::ZMQ(port)),
        };

        // Verify connection with ping
        const MAX_PING_RETRIES: u32 = 5;
        const PING_RETRY_DELAY_MS: u64 = 2000;
        let mut attempt = 0;
        loop {
            match Self::ping_daemon(port, &socket_path).await {
                Ok(_) => {
                    info!("Successfully pinged SledDaemon at port {}", port);
                    break;
                }
                Err(e) => {
                    attempt += 1;
                    if attempt >= MAX_PING_RETRIES {
                        error!("Failed to ping SledDaemon at port {} after {} attempts: {}", port, MAX_PING_RETRIES, e);
                        return Err(GraphError::StorageError(format!("Failed to connect to SledDaemon at port {} after {} attempts: {}", port, MAX_PING_RETRIES, e)));
                    }
                    warn!("Ping attempt {}/{} failed for port {}: {}. Retrying in {}ms", attempt, MAX_PING_RETRIES, port, e, PING_RETRY_DELAY_MS);
                    tokio::time::sleep(TokioDuration::from_millis(PING_RETRY_DELAY_MS)).await;
                }
            }
        }

        // Check daemon readiness via ZMQ
        let readiness_request = json!({ "command": "status" });
        let readiness_response = client.send_zmq_request(port, readiness_request).await?;
        if readiness_response.get("status").and_then(|s| s.as_str()) != Some("success") {
            let error_msg = readiness_response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("SledDaemon at port {} is not ready: {}", port, error_msg);
            return Err(GraphError::StorageError(format!("SledDaemon at port {} is not ready: {}", port, error_msg)));
        }

        info!("SledClient initialization complete for port {}", port);
        Ok((client, socket_wrapper))
    }

    pub async fn force_unlock(db_path: &PathBuf) -> GraphResult<()> {
        let lock_path = db_path.join("db.lck");
        info!("Checking for lock file at {}", lock_path.display());

        if tokio::fs::metadata(&lock_path).await.is_ok() {
            match sled::Config::new().path(db_path).open() {
                Ok(db) => {
                    info!("Database at {:?} is accessible, no need to remove lock file", db_path);
                    drop(db);
                    return Ok(());
                }
                Err(_) => {
                    warn!("Database at {:?} appears locked, removing lock file", db_path);
                    tokio::fs::remove_file(&lock_path).await
                        .map_err(|e| GraphError::StorageError(format!("Failed to remove lock file: {}", e)))?;
                    tokio::time::sleep(TokioDuration::from_millis(500)).await;
                }
            }
        } else {
            info!("No lock file found at {}", lock_path.display());
        }
        Ok(())
    }

    pub async fn ping_daemon(port: u16, socket_path: &str) -> GraphResult<()> {
        let addr = format!("ipc://{}", socket_path);

        // Ensure socket directory exists
        let socket_path_buf = PathBuf::from(socket_path);
        if let Some(parent) = socket_path_buf.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                error!("Failed to create socket directory {}: {}", parent.display(), e);
                return Err(GraphError::StorageError(format!("Failed to create socket directory {}: {}", parent.display(), e)));
            }
            info!("Ensured socket directory exists at {}", parent.display());
        }

        let request = json!({ "command": "ping", "check_db": true });
        let request_data = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("Failed to serialize ping request: {}", e)))?;

        let response = tokio::task::spawn_blocking(move || -> Result<Value, GraphError> {
            let context = zmq::Context::new();
            let socket = context.socket(zmq::REQ)
                .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;
            socket.set_rcvtimeo(10000) // Increased to 10s to handle delays
                .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
            socket.set_sndtimeo(10000)
                .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;
            socket.set_linger(1000)
                .map_err(|e| GraphError::StorageError(format!("Failed to set linger: {}", e)))?;

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
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("ZMQ task failed: {}", e)))??;

        // Check if the daemon is responsive
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("Ping successful for port {}", port);
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("Ping failed for port {}: {}", port, error_msg)))
        }
    }

    pub async fn apply_raft_entry(&self, data: Vec<u8>) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let tree = self.get_tree_zmq(port, "raft_log").await?;
                let (key, value) = data.split_at(data.len() / 2);
                tree.insert(key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to apply raft entry: {}", e)))?;
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after raft entry: {}", e)))?;
                info!("Flushed {} bytes after applying raft entry", bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "apply_raft_entry",
                    "data": data
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    if response.get("bytes_flushed").and_then(|b| b.as_u64()).is_some() {
                        Ok(())
                    } else {
                        Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
                    }
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ apply_raft_entry failed: {}", error_msg)))
                }
            }
            None => {
                let tree = self.get_tree_zmq(port, "raft_log").await?;
                let (key, value) = data.split_at(data.len() / 2);
                tree.insert(key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to apply raft entry: {}", e)))?;
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after raft entry: {}", e)))?;
                info!("Flushed {} bytes after applying raft entry", bytes_flushed);
                Ok(())
            }
        }
    }

    async fn get_tree_zmq(&self, port: u16, name: &str) -> GraphResult<Arc<Tree>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.lock().await;
                db.open_tree(name)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", name, e)))
                    .map(Arc::new)
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "open_tree",
                    "tree_name": name
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let db = self.inner.lock().await;
                    db.open_tree(name)
                        .map_err(|e| GraphError::StorageError(format!("Failed to open dummy tree {}: {}", name, e)))
                        .map(Arc::new)
                } else {
                    Err(GraphError::StorageError(format!("Failed to open tree {} via ZMQ", name)))
                }
            }
            None => {
                let db = self.inner.lock().await;
                db.open_tree(name)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", name, e)))
                    .map(Arc::new)
            }
        }
    }

    async fn insert_into_tree(&self, tree_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        self.insert_into_cf_zmq(port, tree_name, key, value).await
    }

    async fn retrieve_from_tree(&self, tree_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        self.retrieve_from_cf_zmq(port, tree_name, key).await
    }

    async fn delete_from_tree(&self, tree_name: &str, key: &[u8]) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        self.delete_from_cf_zmq(port, tree_name, key).await
    }

    async fn send_zmq_request(&self, port: u16, request: Value) -> GraphResult<Value> {
        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);

        let request_data = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("Failed to serialize request: {}", e)))?;

        debug!("Sending ZMQ request to {}: {:?}", socket_path, request);

        let response = tokio::task::spawn_blocking({
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

                debug!("Received ZMQ response: {:?}", response);
                Ok(response)
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("ZMQ task failed: {}", e)))??;

        Ok(response)
    }

    async fn insert_into_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        // Encode key and value as base64 to preserve binary data in JSON
        let key_base64 = general_purpose::STANDARD.encode(key);
        let value_base64 = general_purpose::STANDARD.encode(value);
        
        debug!("ZMQ insert: tree={}, key={:?}, value={:?}", tree_name, key_base64, value_base64);

        let request = json!({
            "command": "set_key",
            "key": key_base64,
            "value": value_base64,
            "cf": tree_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(bytes_flushed) = response.get("bytes_flushed").and_then(|b| b.as_u64()) {
                debug!("ZMQ insert successful: {} bytes flushed", bytes_flushed);
                Ok(())
            } else {
                Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
            }
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            Err(GraphError::StorageError(format!("ZMQ insert failed: {}", error_msg)))
        }
    }

    async fn retrieve_from_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        // Encode key as base64
        let key_base64 = general_purpose::STANDARD.encode(key);
        debug!("ZMQ retrieve: tree={}, key={:?}", tree_name, key_base64);

        let request = json!({
            "command": "get_key",
            "key": key_base64,
            "cf": tree_name
        });

        let response = self.send_zmq_request(port, request).await?;

        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(value) = response.get("value") {
                if value.is_null() {
                    debug!("ZMQ retrieve: key={:?} not found", key_base64);
                    Ok(None)
                } else {
                    let value_base64 = value.as_str()
                        .ok_or_else(|| GraphError::StorageError("Invalid value format".to_string()))?;
                    let value_bytes = general_purpose::STANDARD
                        .decode(value_base64)
                        .map_err(|e| GraphError::StorageError(format!("Failed to decode base64 value: {}", e)))?;
                    debug!("ZMQ retrieve: key={:?}, value={:?}", key_base64, value_bytes);
                    Ok(Some(value_bytes))
                }
            } else {
                debug!("ZMQ retrieve: key={:?} not found (no value field)", key_base64);
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
        // Encode key as base64
        let key_base64 = general_purpose::STANDARD.encode(key);
        debug!("ZMQ delete: tree={}, key={:?}", tree_name, key_base64);

        let request = json!({
            "command": "delete_key",
            "key": key_base64,
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

    pub async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let uuid = SerializableUuid(vertex.id.0);
        let key = uuid.0.as_bytes().to_vec();
        let value = serialize_vertex(&vertex)?;
        self.insert_into_cf_zmq(port, "vertices", &key, &value).await
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let uuid = SerializableUuid(*id);
        let key = uuid.0.as_bytes().to_vec();
        let result = self.retrieve_from_cf_zmq(port, "vertices", &key).await?;
        match result {
            Some(v) => Ok(Some(deserialize_vertex(&v)?)),
            None => Ok(None),
        }
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let uuid = SerializableUuid(*id);
        let key = uuid.0.as_bytes().to_vec();
        self.delete_from_cf_zmq(port, "vertices", &key).await
    }

    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let tree = self.get_tree_zmq(port, "vertices").await?;
                let mut vertices = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;
                    let vertex = deserialize_vertex(&value_ivec)?;
                    vertices.push(vertex);
                }
                Ok(vertices)
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "get_all_vertices" });
                let response = self.send_zmq_request(port, request).await?;
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
                let tree = self.get_tree_zmq(port, "vertices").await?;
                let mut vertices = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;
                    let vertex = deserialize_vertex(&value_ivec)?;
                    vertices.push(vertex);
                }
                Ok(vertices)
            }
        }
    }

    pub async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let key = create_edge_key(
            &SerializableUuid(edge.outbound_id.0),
            &edge.t,
            &SerializableUuid(edge.inbound_id.0)
        )?;
        let value = serialize_edge(&edge)?;
        self.insert_into_cf_zmq(port, "edges", &key, &value).await
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let key = create_edge_key(
            &SerializableUuid(*outbound_id),
            edge_type,
            &SerializableUuid(*inbound_id)
        )?;
        let result = self.retrieve_from_cf_zmq(port, "edges", &key).await?;
        match result {
            Some(v) => Ok(Some(deserialize_edge(&v)?)),
            None => Ok(None),
        }
    }

    pub async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let key = create_edge_key(
            &SerializableUuid(*outbound_id),
            edge_type,
            &SerializableUuid(*inbound_id)
        )?;
        self.delete_from_cf_zmq(port, "edges", &key).await
    }

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let tree = self.get_tree_zmq(port, "edges").await?;
                let mut edges = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;
                    let edge = deserialize_edge(&value_ivec)?;
                    edges.push(edge);
                }
                Ok(edges)
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "get_all_edges" });
                let response = self.send_zmq_request(port, request).await?;
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
                let tree = self.get_tree_zmq(port, "edges").await?;
                let mut edges = Vec::new();
                for item in tree.iter() {
                    let (_, value_ivec) = item.map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;
                    let edge = deserialize_edge(&value_ivec)?;
                    edges.push(edge);
                }
                Ok(edges)
            }
        }
    }

    pub async fn clear_data(&self) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let trees = ["vertices", "edges", "kv_pairs"];
                let db = self.inner.lock().await;
                for tree_name in trees.iter() {
                    let tree = self.get_tree_zmq(port, tree_name).await?;
                    tree.clear()
                        .map_err(|e| GraphError::StorageError(format!("Failed to clear tree {}: {}", tree_name, e)))?;
                }
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after clear: {}", e)))?;
                info!("Flushed {} bytes after clearing data", bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "clear_data" });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    if response.get("bytes_flushed").and_then(|b| b.as_u64()).is_some() {
                        Ok(())
                    } else {
                        Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
                    }
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ clear_data failed: {}", error_msg)))
                }
            }
            None => {
                let trees = ["vertices", "edges", "kv_pairs"];
                let db = self.inner.lock().await;
                for tree_name in trees.iter() {
                    let tree = self.get_tree_zmq(port, tree_name).await?;
                    tree.clear()
                        .map_err(|e| GraphError::StorageError(format!("Failed to clear tree {}: {}", tree_name, e)))?;
                }
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after clear: {}", e)))?;
                info!("Flushed {} bytes after clearing data", bytes_flushed);
                Ok(())
            }
        }
    }

    pub async fn connect(&self) -> GraphResult<()> {
        info!("Connecting to Sled");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    pub async fn start(&self) -> GraphResult<()> {
        info!("Starting Sled");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    pub async fn stop(&self) -> GraphResult<()> {
        info!("Stopping Sled");
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        if let Some(SledClientMode::ZMQ(_)) = &self.mode {
            let request = json!({ "command": "flush" });
            let response = self.send_zmq_request(port, request).await?;
            if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                if response.get("bytes_flushed").and_then(|b| b.as_u64()).is_none() {
                    warn!("Flush not confirmed by daemon during stop");
                }
            } else {
                warn!("Failed to flush daemon during stop: {}", response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error"));
            }
        }
        Ok(())
    }

    pub async fn close(&self) -> GraphResult<()> {
        info!("Closing Sled");
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes during close", bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "close" });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    if response.get("bytes_flushed").and_then(|b| b.as_u64()).is_some() {
                        Ok(())
                    } else {
                        Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
                    }
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ close failed: {}", error_msg)))
                }
            }
            None => {
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes during close", bytes_flushed);
                Ok(())
            }
        }
    }

    pub async fn flush(&self) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes", bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "flush" });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    if response.get("bytes_flushed").and_then(|b| b.as_u64()).is_some() {
                        Ok(())
                    } else {
                        Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
                    }
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ flush failed: {}", error_msg)))
                }
            }
            None => {
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes", bytes_flushed);
                Ok(())
            }
        }
    }

    pub async fn execute_query(&self) -> GraphResult<QueryResult> {
        info!("Executing query on SledClient (not implemented)");
        Ok(QueryResult::Null)
    }
}

#[async_trait]
impl StorageEngine for SledClient {
    async fn connect(&self) -> GraphResult<()> {
        info!("Connecting to SledClient");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        self.insert_into_cf_zmq(port, "kv_pairs", &key, &value).await
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        self.retrieve_from_cf_zmq(port, "kv_pairs", key).await
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        self.delete_from_cf_zmq(port, "kv_pairs", key).await
    }

    async fn flush(&self) -> GraphResult<()> {
        self.flush().await
    }
}

#[async_trait]
impl GraphStorageEngine for SledClient {
    async fn start(&self) -> GraphResult<()> {
        info!("Starting SledClient");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        info!("Stopping SledClient");
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        if let Some(SledClientMode::ZMQ(_)) = &self.mode {
            let request = json!({ "command": "flush" });
            let response = self.send_zmq_request(port, request).await?;
            if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                if response.get("bytes_flushed").and_then(|b| b.as_u64()).is_none() {
                    warn!("Flush not confirmed by daemon during stop");
                }
            } else {
                warn!("Failed to flush daemon during stop: {}", response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error"));
            }
        }
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        match &self.mode {
            Some(SledClientMode::Direct) => "sled_client",
            Some(SledClientMode::ZMQ(_)) => "sled_client_zmq",
            None => "sled_client",
        }
    }

    async fn is_running(&self) -> bool {
        let is_running = self.is_running.lock().await;
        *is_running
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "query", "query": query_string });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(response.get("value").cloned().unwrap_or(Value::Null))
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ query failed: {}", error_msg)))
                }
            }
            _ => Err(GraphError::StorageError("SledClient query not implemented for direct access".to_string())),
        }
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        self.get_vertex(id).await
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.update_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        self.delete_vertex(id).await
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        self.get_all_vertices().await
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        self.get_edge(outbound_id, edge_type, inbound_id).await
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.update_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        self.delete_edge(outbound_id, edge_type, inbound_id).await
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        self.get_all_edges().await
    }

    async fn clear_data(&self) -> GraphResult<()> {
        self.clear_data().await
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> GraphResult<QueryResult> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "execute_query", "query_plan": query_plan });
                let response = self.send_zmq_request(port, request).await?;
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
                Ok(QueryResult::Null)
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> GraphResult<()> {
        info!("Closing SledClient");
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes during close", bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({ "command": "close" });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    if response.get("bytes_flushed").and_then(|b| b.as_u64()).is_some() {
                        Ok(())
                    } else {
                        Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
                    }
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ close failed: {}", error_msg)))
                }
            }
            None => {
                let db = self.inner.lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes during close", bytes_flushed);
                Ok(())
            }
        }
    }
}
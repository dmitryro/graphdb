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
pub use crate::config::{QueryResult, QueryPlan, SledClient, SledClientMode, DEFAULT_STORAGE_PORT, DEFAULT_DATA_DIRECTORY};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use tokio::time::{timeout, sleep, Duration as TokioDuration};
use tokio::task::spawn_blocking; // FIX: Explicitly importing spawn_blocking
use base64::engine::general_purpose;
use base64::Engine;
use zmq::{ Context as ZmqContext, Socket as ZmqSocket, Message};
// Wrapper for ZmqSocket to implement Debug
pub struct ZmqSocketWrapper(ZmqSocket);
impl ZmqSocketWrapper {
    /// Public constructor required to initialize the private tuple field.
    pub fn new(socket: ZmqSocket) -> Self {
        ZmqSocketWrapper(socket)
    }
    /// Accesses the underlying ZMQ socket.
    /// You might need this helper method later for binding/sending/receiving.
    pub fn socket(&self) -> &ZmqSocket {
        &self.0
    }
}
impl std::fmt::Debug for ZmqSocketWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqSocketWrapper")
            .field("socket", &"ZmqSocket")
            .finish()
    }
}
impl std::ops::Deref for ZmqSocketWrapper {
    type Target = ZmqSocket;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for ZmqSocketWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
// --- ZMQ Connection Utility ---
/// Helper function to perform the blocking ZMQ connection with internal timeouts.
/// Must be called inside a spawn_blocking block.
fn connect_zmq_socket_with_timeout(context: &ZmqContext, ipc_endpoint: &str, timeout_ms: i32) -> zmq::Result<ZmqSocket> {
    let socket = context.socket(zmq::REQ)?;
   
    // Set connect, send, and receive timeouts to prevent the socket from blocking indefinitely.
    socket.set_connect_timeout(timeout_ms)?;
    socket.set_rcvtimeo(timeout_ms)?;
    socket.set_sndtimeo(timeout_ms)?;
   
    socket.connect(ipc_endpoint)?;
   
    debug!("Successfully connected ZMQ socket to {}", ipc_endpoint);
    Ok(socket)
}
impl SledClient {
    // A minimal constructor for the ZMQ mode.
    /// FIX: Updated constructor to initialize the required fields: `db_path`, `is_running`, `zmq_socket`.
    pub fn new_zmq(port: u16, db_path: PathBuf, socket_arc: Arc<TokioMutex<ZmqSocketWrapper>>) -> Self {
        SledClient {
            context: Arc::new(zmq::Context::new()),  // Create once
            mode: Some(SledClientMode::ZMQ(port)),
            inner: None, // Local Db instance not used in ZMQ client mode
            // FIX: Added 'db_path:' field label
            db_path: Some(db_path),
            // FIX: Correctly wrap 'true' in Arc<TokioMutex<bool>>
            is_running: Arc::new(TokioMutex::new(true)),
            zmq_socket: Some(socket_arc), // Store the connected ZMQ socket
        }
    }
    // Unchanged: new, new_with_db (they already set inner to Some)
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
        for tree_name in &["data", "vertices", "edges", "kv_pairs", "raft_log", "raft_snapshot", "raft_membership", "raft_vote"] {
            db.open_tree(tree_name)
                .map_err(|e| GraphError::StorageError(format!("Failed to create tree {}: {}", tree_name, e)))?;
        }
        let kv_pairs = db.open_tree("kv_pairs")?;
        let key_count = kv_pairs.iter().count();
        info!("Opened database at {:?} with {} keys in kv_pairs", db_path, key_count);
        Ok(Self {
            inner: Some(Arc::new(TokioMutex::new(Arc::new(db)))),
            context: Arc::new(zmq::Context::new()),  // Create once
            // FIX: Added 'db_path:' field label
            db_path: Some(db_path),
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(SledClientMode::Direct),
            zmq_socket: None,
        })
    }
    pub async fn new_with_db(db_path: PathBuf, db: Arc<Db>) -> GraphResult<Self> {
        info!("Creating SledClient with existing database at {:?}", db_path);
        for tree_name in &["data", "vertices", "edges", "kv_pairs", "raft_log", "raft_snapshot", "raft_membership", "raft_vote"] {
            db.open_tree(tree_name)
                .map_err(|e| GraphError::StorageError(format!("Failed to create tree {}: {}", tree_name, e)))?;
        }
        let kv_pairs = db.open_tree("kv_pairs")?;
        let key_count = kv_pairs.iter().count();
        info!("Opened database at {:?} with {} keys in kv_pairs", db_path, key_count);
        Ok(Self {
            inner: Some(Arc::new(TokioMutex::new(db))),
            // FIX: Added 'db_path:' field label
            context: Arc::new(zmq::Context::new()),  // Create once
            db_path: Some(db_path),
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(SledClientMode::Direct),
            zmq_socket: None,
        })
    }
   
    pub async fn new_with_port(port: u16) -> GraphResult<(Self, Arc<TokioMutex<ZmqSocketWrapper>>)> {
        info!("Creating SledClient for ZMQ connection on port {}", port);
        let socket_dir = PathBuf::from("/tmp");
        if let Err(e) = tokio::fs::create_dir_all(&socket_dir).await {
            error!("Failed to create socket directory {}: {}", socket_dir.display(), e);
            return Err(GraphError::StorageError(format!("Failed to create socket directory {}: {}", socket_dir.display(), e)));
        }
        info!("Ensured socket directory exists at {}", socket_dir.display());
        let socket_path = format!("/tmp/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;
        socket.set_rcvtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        socket.set_sndtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;
        socket.set_linger(500)
            .map_err(|e| GraphError::StorageError(format!("Failed to set linger: {}", e)))?;
        socket.set_maxmsgsize(1024 * 1024)
            .map_err(|e| GraphError::StorageError(format!("Failed to set max message size: {}", e)))?;
        socket.connect(&addr)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", addr, e)))?;
        let socket_wrapper = Arc::new(TokioMutex::new(ZmqSocketWrapper::new(socket)));
        let client = Self {
            context: Arc::new(zmq::Context::new()),  // Create once
            inner: None,
            db_path: Some(PathBuf::from(format!("/opt/graphdb/storage_data/sled/{}", port))),
            is_running: Arc::new(TokioMutex::new(true)),
            mode: Some(SledClientMode::ZMQ(port)),
            zmq_socket: Some(socket_wrapper.clone()),
        };
        const MAX_PING_RETRIES: u32 = 5;
        const PING_RETRY_DELAY_MS: u64 = 1000;
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
    /// Attempts to connect to a running ZMQ storage daemon on the specified port.
    /// This uses `spawn_blocking` to ensure the synchronous ZMQ calls do not hang the async runtime.
    pub async fn connect_zmq_client(port: u16) -> GraphResult<(SledClient, Arc<TokioMutex<ZmqSocketWrapper>>)> {
        let ipc_endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        info!("Attempting ZMQ client connection to {}", ipc_endpoint);
       
        // Use a default path for the ZMQ client struct initialization, as the client itself
        // doesn't manage the path, but the struct requires it.
        let default_db_path = PathBuf::from(format!("/opt/graphdb/storage_data/sled/{}", port));
        let connection_result = spawn_blocking(move || {
            let context = ZmqContext::new();
            // A short internal timeout (500ms) for the connect attempt.
            let connect_timeout_ms = 500;
            match connect_zmq_socket_with_timeout(&context, &ipc_endpoint, connect_timeout_ms) {
                Ok(socket) => {
                    // Successfully connected socket, now ping to confirm daemon is ready to process requests.
                    let ping_request = json!({ "command": "ping" });
                    match SledClient::send_zmq_request_sync(&socket, ping_request) {
                        Ok(response) => {
                            if response.get("status").and_then(|s| s.as_str()) == Some("pong") {
                                Ok(socket)
                            } else {
                                Err(format!("ZMQ ping failed: Unexpected response from daemon at {}: {:?}", ipc_endpoint, response))
                            }
                        }
                        Err(e) => Err(format!("ZMQ ping request failed: {}", e)),
                    }
                }
                Err(e) => Err(format!("Failed to connect ZMQ socket to {}: {}", ipc_endpoint, e)),
            }
        }).await;
        match connection_result {
            Ok(Ok(socket)) => {
                // Connection and ping successful
                let socket_arc = Arc::new(TokioMutex::new(ZmqSocketWrapper::new(socket)));
               
                // FIX: Initialize SledClient using the new constructor fields
                let client = SledClient::new_zmq(port, default_db_path, socket_arc.clone());
               
                // Return the client and a clone of the socket Arc to satisfy the caller's expected tuple return type.
                Ok((client, socket_arc))
            }
            Ok(Err(e)) => {
                // Connection failed (allows outer retry loop to catch it)
                Err(GraphError::ConnectionError(e))
            }
            Err(e) => {
                // Task failure (panic/join error)
                Err(GraphError::InternalError(format!("Internal task failure during ZMQ connection: {}", e)))
            }
        }
    }

    pub async fn connect_zmq_client_with_readiness_check(port: u16) -> GraphResult<(Self, Arc<TokioMutex<ZmqSocketWrapper>>)> {
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        info!("Attempting ZMQ client connection to {}", endpoint);
        println!("===> ATTEMPTING ZMQ CLIENT CONNECTION TO {}", endpoint);

        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| {
                error!("Failed to create ZMQ socket for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO CREATE ZMQ SOCKET FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e))
            })?;
        
        socket.set_rcvtimeo(5000)
            .map_err(|e| {
                error!("Failed to set receive timeout for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO SET RECEIVE TIMEOUT FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to set receive timeout: {}", e))
            })?;
        
        socket.set_sndtimeo(5000)
            .map_err(|e| {
                error!("Failed to set send timeout for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO SET SEND TIMEOUT FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to set send timeout: {}", e))
            })?;
        
        socket.set_linger(1000)
            .map_err(|e| {
                error!("Failed to set linger for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO SET LINGER FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to set linger: {}", e))
            })?;

        let socket_path = format!("/tmp/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);

        info!("Connecting to ZMQ endpoint {} for port {}", addr, port);
        println!("===> CONNECTING TO ZMQ ENDPOINT {} FOR PORT {}", addr, port);
        
        socket.connect(&addr)
            .map_err(|e| {
                error!("Failed to connect ZMQ socket to {}: {}", addr, e);
                println!("===> ERROR: FAILED TO CONNECT ZMQ SOCKET TO {}: {}", addr, e);
                GraphError::StorageError(format!("Failed to connect ZMQ socket to {}: {}", addr, e))
            })?;

        let request = json!({ "command": "initialize" });
        let request_data = serde_json::to_vec(&request)
            .map_err(|e| {
                error!("Failed to serialize initialize request: {}", e);
                println!("===> ERROR: FAILED TO SERIALIZE INITIALIZE REQUEST: {}", e);
                GraphError::SerializationError(format!("Failed to serialize initialize request: {}", e))
            })?;

        info!("Sending initialize request to ZMQ server on port {}", port);
        println!("===> SENDING INITIALIZE REQUEST TO ZMQ SERVER ON PORT {}", port);
        
        socket.send(&request_data, 0)
            .map_err(|e| {
                error!("Failed to send initialize request to {}: {}", addr, e);
                println!("===> ERROR: FAILED TO SEND INITIALIZE REQUEST TO {}: {}", addr, e);
                GraphError::StorageError(format!("Failed to send initialize request: {}", e))
            })?;

        let reply = socket.recv_bytes(0)
            .map_err(|e| {
                error!(
                    "Failed to receive initialize response from {}: {}. \
                     This usually means the Sled daemon is not running or ZMQ server failed to bind.",
                     addr, e
                );

                println!("===> ERROR: FAILED TO RECEIVE INITIALIZE RESPONSE FROM {}: {}", addr, e);
                GraphError::StorageError(format!("Failed to receive initialize response: {}", e))
            })?;
        
        let response: Value = serde_json::from_slice(&reply)
            .map_err(|e| {
                error!("Failed to parse initialize response from {}: {}", addr, e);
                println!("===> ERROR: FAILED TO PARSE INITIALIZE RESPONSE FROM {}: {}", addr, e);
                GraphError::DeserializationError(format!("Failed to parse initialize response: {}", e))
            })?;

        if response.get("status").and_then(|s| s.as_str()) != Some("success") {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("ZMQ server not ready on port {}: {}", port, error_msg);
            println!("===> ERROR: ZMQ SERVER NOT READY ON PORT {}: {}", port, error_msg);
            return Err(GraphError::StorageError(format!("ZMQ server not ready on port {}: {}", port, error_msg)));
        }

        info!("ZMQ server responded successfully for port {}", port);
        println!("===> ZMQ SERVER RESPONDED SUCCESSFULLY FOR PORT {}", port);

        let socket_wrapper = Arc::new(TokioMutex::new(ZmqSocketWrapper::new(socket)));
        let is_running = Arc::new(TokioMutex::new(true));

        Ok((
            SledClient {
                context: Arc::new(zmq::Context::new()),  // Create once
                inner: None,
                db_path: Some(PathBuf::from(format!("/opt/graphdb/storage_data/sled/{}", port))),
                is_running,
                zmq_socket: Some(socket_wrapper.clone()),
                mode: Some(SledClientMode::ZMQ(port)),
            },
            socket_wrapper,
        ))
    }

    pub fn send_zmq_request_sync(socket: &zmq::Socket, request: Value) -> GraphResult<Value> {
        let request_str = serde_json::to_string(&request)
            .map_err(|e| GraphError::SerializationError(format!("Failed to serialize request: {}", e)))?;
        socket.send(request_str.as_bytes(), 0)
            .map_err(|e| GraphError::StorageError(format!("ZMQ send error: {}", e)))?;
        let mut msg = Message::new();
        socket.recv(&mut msg, 0)
            .map_err(|e| GraphError::StorageError(format!("ZMQ recv error: {}", e)))?;
        let response_str = msg.as_str()
            .ok_or_else(|| GraphError::StorageError("ZMQ response was not valid UTF-8".to_string()))?;
        serde_json::from_str(response_str)
            .map_err(|e| GraphError::DeserializationError(format!("Failed to deserialize response: {}", e)))
    }

    /// Sends a request to the ZMQ daemon and awaits the response.
    /// This is where the core interaction logic is.
    pub async fn send_zmq_request(&self, port: u16, request: Value) -> GraphResult<Value> {
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("ZMQ socket create failed: {}", e)))?;

        // Connect to endpoint
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("ZMQ connect failed: {}", e)))?;

        // Set timeouts
        socket.set_sndtimeo(1000)
            .map_err(|e| GraphError::StorageError(format!("ZMQ set_sndtimeo failed: {}", e)))?;
        socket.set_rcvtimeo(10000)  // Increased to 10s to allow server processing
            .map_err(|e| GraphError::StorageError(format!("ZMQ set_rcvtimeo failed: {}", e)))?;

        // Serialize request
        let payload = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("JSON serialize failed: {}", e)))?;

        // Send request (NO RETRY)
        socket.send(&payload, 0)
            .map_err(|e| {
                error!("ZMQ send failed on port {}: {}", port, e);
                GraphError::StorageError(format!("ZMQ send error: {}", e))
            })?;

        // Receive response (NO RETRY)
        let bytes = socket.recv_bytes(0)
            .map_err(|e| {
                error!("ZMQ recv failed on port {}: {}", port, e);
                GraphError::StorageError(format!("ZMQ recv error: {}", e))
            })?;

        // Parse response
        let response: Value = serde_json::from_slice(&bytes)
            .map_err(|e| GraphError::StorageError(format!("ZMQ response parse error: {}", e)))?;

        Ok(response)
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
    // Update other methods that use `inner` to check for None
    pub async fn apply_raft_entry(&self, data: Vec<u8>) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::Direct) => {
                self.get_tree_zmq(port, "raft_log").await?;
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock().await;
                let tree = db.open_tree("raft_log")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open raft_log tree: {}", e)))?;
                let (key, value) = data.split_at(data.len() / 2);
                tree.insert(key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to apply raft entry: {}", e)))?;
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
                self.get_tree_zmq(port, "raft_log").await?;
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock().await;
                let tree = db.open_tree("raft_log")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open raft_log tree: {}", e)))?;
                let (key, value) = data.split_at(data.len() / 2);
                tree.insert(key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to apply raft entry: {}", e)))?;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after raft entry: {}", e)))?;
                info!("Flushed {} bytes after applying raft entry", bytes_flushed);
                Ok(())
            }
        }
    }
    // Update methods that access `inner` to handle the Option
    // Alternative get_tree_zmq that doesn't return Arc<Tree> in ZMQ mode
    async fn get_tree_zmq(&self, port: u16, name: &str) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock().await;
                db.open_tree(name)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", name, e)))?;
                Ok(())
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "open_tree",
                    "tree_name": name
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(())
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("Failed to open tree {} via ZMQ: {}", name, error_msg)))
                }
            }
            None => {
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock().await;
                db.open_tree(name)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", name, e)))?;
                Ok(())
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

    pub async fn insert_into_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let tree = (*db).open_tree(cf_name)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", cf_name, e)))?;
                tree.insert(key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to insert into {}: {}", cf_name, e)))?;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Inserted into {}: key={:?}, flushed {} bytes", cf_name, key, bytes_flushed);
                println!("===> Inserted into {}: key={:?}, flushed {} bytes", cf_name, key, bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.insert_into_cf_zmq(*port, cf_name, key, value).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

    pub async fn retrieve_from_cf(&self, cf_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let tree = (*db).open_tree(cf_name)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", cf_name, e)))?;
                let result = tree.get(key)
                    .map_err(|e| GraphError::StorageError(format!("Failed to retrieve from {}: {}", cf_name, e)))?
                    .map(|ivec| ivec.to_vec());
                info!("Retrieved from {}: key={:?}, value={:?}", cf_name, key, result);
                println!("===> Retrieved from {}: key={:?}, value={:?}", cf_name, key, result);
                Ok(result)
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.retrieve_from_cf_zmq(*port, cf_name, key).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

    pub async fn delete_from_cf(&self, cf_name: &str, key: &[u8]) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let tree = (*db).open_tree(cf_name)
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", cf_name, e)))?;
                tree.remove(key)
                    .map_err(|e| GraphError::StorageError(format!("Failed to delete from {}: {}", cf_name, e)))?;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after delete: {}", e)))?;
                info!("Deleted from {}: key={:?}, flushed {} bytes", cf_name, key, bytes_flushed);
                println!("===> Deleted from {}: key={:?}, flushed {} bytes", cf_name, key, bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.delete_from_cf_zmq(*port, cf_name, key).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }


    pub async fn insert_into_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
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
        let response = timeout(TokioDuration::from_secs(10), self.send_zmq_request(port, request))
            .await
            .map_err(|_| {
                error!("Timeout executing ZMQ insert for tree {} and key {:?}", tree_name, key_base64);
                GraphError::StorageError(format!("Timeout executing ZMQ insert for tree {}", tree_name))
            })??;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(bytes_flushed) = response.get("bytes_flushed").and_then(|b| b.as_u64()) {
                debug!("ZMQ insert successful: {} bytes flushed", bytes_flushed);
                info!("Completed ZMQ insert for tree {} and key {:?}", tree_name, key_base64);
                Ok(())
            } else {
                error!("ZMQ insert for tree {} and key {:?}: Flush not confirmed by daemon", tree_name, key_base64);
                Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
            }
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("ZMQ insert failed for tree {} and key {:?}: {}", tree_name, key_base64, error_msg);
            Err(GraphError::StorageError(format!("ZMQ insert failed: {}", error_msg)))
        }
    }
    pub async fn retrieve_from_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
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
    pub async fn delete_from_cf_zmq(&self, port: u16, tree_name: &str, key: &[u8]) -> GraphResult<()> {
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
    // --- NEW: Dedicated ZMQ Graph Commands ---
    // ------------------------------------------------------------------------
    //  create_vertex_zmq
    // ------------------------------------------------------------------------
    async fn create_vertex_zmq(&self, port: u16, vertex: &Vertex) -> GraphResult<()> {
        let request = json!({
            "command": "create_vertex",
            "vertex": vertex
        });
        
        let response = zmq_send_recv(self.context.clone(), port, request).await?;
        
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ create_vertex ok – id={}", vertex.id);
            Ok(())
        } else {
            let msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("unknown");
            error!("ZMQ create_vertex failed: {}", msg);
            Err(GraphError::StorageError(format!("create_vertex failed: {}", msg)))
        }
    }

    // ------------------------------------------------------------------------
    //  get_vertex_zmq
    // ------------------------------------------------------------------------
    async fn get_vertex_zmq(&self, port: u16, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let request = json!({
            "command": "get_vertex",
            "id": id.to_string()
        });
        
        let response = zmq_send_recv(self.context.clone(), port, request).await?;
        
        if response.get("status").and_then(|s| s.as_str()) != Some("success") {
            let msg = response
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown");
            return Err(GraphError::StorageError(format!("get_vertex failed: {}", msg)));
        }
        
        if let Some(v) = response.get("vertex") {
            if v.is_null() {
                Ok(None)
            } else {
                serde_json::from_value(v.clone())
                    .map_err(|e| GraphError::DeserializationError(format!("vertex: {}", e)))
                    .map(Some)
            }
        } else {
            Ok(None)
        }
    }

    async fn delete_vertex_zmq(&self, port: u16, id: &Uuid) -> GraphResult<()> {
        let request = json!({
            "command": "delete_vertex",
            "id": id.to_string()
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ delete_vertex successful for port {}: id={}", port, id);
            println!("===> ZMQ delete_vertex successful for port {}: id={}", port, id);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            error!("ZMQ delete_vertex failed for port {}: {}", port, error_msg);
            println!("===> ERROR: ZMQ delete_vertex failed for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ delete_vertex failed: {}", error_msg)))
        }
    }

    // ------------------------------------------------------------------------
    //  create_edge_zmq
    // ------------------------------------------------------------------------
    async fn create_edge_zmq(&self, port: u16, edge: &Edge) -> GraphResult<()> {
        let request = json!({
            "command": "create_edge",
            "edge": edge
        });
        
        let response = zmq_send_recv(self.context.clone(), port, request).await?;
        
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ create_edge ok");
            Ok(())
        } else {
            let msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("unknown");
            Err(GraphError::StorageError(format!("create_edge failed: {}", msg)))
        }
    }

    async fn get_edge_zmq(&self, port: u16, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let request = json!({
            "command": "get_edge",
            "outbound_id": outbound_id.to_string(),
            "edge_type": edge_type,
            "inbound_id": inbound_id.to_string()
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(edge_json) = response.get("edge") {
                if edge_json.is_null() {
                    Ok(None)
                } else {
                    let edge = serde_json::from_value(edge_json.clone())
                        .map_err(|e| GraphError::DeserializationError(format!("Failed to deserialize edge: {}", e)))?;
                    Ok(Some(edge))
                }
            } else {
                Ok(None)
            }
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            error!("ZMQ get_edge failed for port {}: {}", port, error_msg);
            println!("===> ERROR: ZMQ get_edge failed for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ get_edge failed: {}", error_msg)))
        }
    }

    async fn delete_edge_zmq(&self, port: u16, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let request = json!({
            "command": "delete_edge",
            "outbound_id": outbound_id.to_string(),
            "edge_type": edge_type,
            "inbound_id": inbound_id.to_string()
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ delete_edge successful for port {}: {} -> {} [{}]", port, outbound_id, inbound_id, edge_type);
            println!("===> ZMQ delete_edge successful for port {}: {} -> {} [{}]", port, outbound_id, inbound_id, edge_type);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            error!("ZMQ delete_edge failed for port {}: {}", port, error_msg);
            println!("===> ERROR: ZMQ delete_edge failed for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ delete_edge failed: {}", error_msg)))
        }
    }

    // --- High-Level Graph Operations ---
    pub async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db_guard = inner.lock().await;
                let db = db_guard.as_ref();
                let key = vertex.id.as_bytes().to_vec();
                let value = serialize_vertex(&vertex)?;
                let tree = db.open_tree("vertices")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree 'vertices': {}", e)))?;
                tree.insert(key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to insert vertex: {}", e)))?;
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.create_vertex_zmq(*port, &vertex).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let key = id.as_bytes().to_vec();
                let result = self.retrieve_from_cf("vertices", &key).await?;
                match result {
                    Some(v) => Ok(Some(deserialize_vertex(&v)?)),
                    None => Ok(None),
                }
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.get_vertex_zmq(*port, id).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

    pub async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db_guard = inner.lock().await;
                let db = db_guard.as_ref();
                let key = create_edge_key(
                    &SerializableUuid(edge.outbound_id.0),
                    &edge.edge_type,
                    &SerializableUuid(edge.inbound_id.0)
                )?;
                let value = serialize_edge(&edge)?;
                let tree = db.open_tree("edges")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open tree 'edges': {}", e)))?;
                tree.insert(key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to insert edge: {}", e)))?;
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.create_edge_zmq(*port, &edge).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let key = id.as_bytes().to_vec();
                self.delete_from_cf("vertices", &key).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.delete_vertex_zmq(*port, id).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

     // ------------------------------------------------------------------------
    //  get_all_vertices
    // ------------------------------------------------------------------------
    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self
                    .inner
                    .as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock()
                    .await;

                let tree = db
                    .open_tree("vertices")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open vertices tree: {}", e)))?;

                let mut vertices = Vec::new();
                for result in tree.iter() {
                    let (_, value_bytes) = result
                        .map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;

                    let vertex: Vertex = bincode::decode_from_slice(&value_bytes, bincode::config::standard())
                        .map_err(|e| GraphError::DeserializationError(format!("Failed to decode vertex: {}", e)))?
                        .0;

                    vertices.push(vertex);
                }

                info!("Direct mode: Retrieved {} vertices", vertices.len());
                Ok(vertices)
            }

            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "get_all_vertices" });
                let response = zmq_send_recv(self.context.clone(), *port, request).await?;
                
                if response.get("status").and_then(|s| s.as_str()) != Some("success") {
                    let msg = response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("unknown");
                    return Err(GraphError::StorageError(format!(
                        "get_all_vertices failed: {}",
                        msg
                    )));
                }
                
                let arr = response
                    .get("vertices")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| GraphError::StorageError("missing vertices array".into()))?;
                
                let mut verts = Vec::with_capacity(arr.len());
                for v in arr {
                    verts.push(
                        serde_json::from_value(v.clone())
                            .map_err(|e| GraphError::DeserializationError(format!("vertex: {}", e)))?,
                    );
                }
                
                info!("ZMQ get_all_vertices → {} vertices", verts.len());
                Ok(verts)
            }

            None => Err(GraphError::StorageError("mode not set".into())),
        }
    }

    // ------------------------------------------------------------------------
    //  get_all_edges
    // ------------------------------------------------------------------------
    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self
                    .inner
                    .as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock()
                    .await;

                let tree = db
                    .open_tree("edges")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open edges tree: {}", e)))?;

                let mut edges = Vec::new();
                for result in tree.iter() {
                    let (_, value_bytes) = result
                        .map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;

                    let edge: Edge = bincode::decode_from_slice(&value_bytes, bincode::config::standard())
                        .map_err(|e| GraphError::DeserializationError(format!("Failed to decode edge: {}", e)))?
                        .0;

                    edges.push(edge);
                }

                info!("Direct mode: Retrieved {} edges", edges.len());
                Ok(edges)
            }

            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "get_all_edges" });
                let response = zmq_send_recv(self.context.clone(), *port, request).await?;
                
                if response.get("status").and_then(|s| s.as_str()) != Some("success") {
                    let msg = response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("unknown");
                    return Err(GraphError::StorageError(format!(
                        "get_all_edges failed: {}",
                        msg
                    )));
                }
                
                let arr = response
                    .get("edges")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| GraphError::StorageError("missing edges array".into()))?;
                
                let mut edges = Vec::with_capacity(arr.len());
                for v in arr {
                    edges.push(
                        serde_json::from_value(v.clone())
                            .map_err(|e| GraphError::DeserializationError(format!("edge: {}", e)))?,
                    );
                }
                
                info!("ZMQ get_all_edges → {} edges", edges.len());
                Ok(edges)
            }

            None => Err(GraphError::StorageError("mode not set".into())),
        }
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let key = create_edge_key(
                    &SerializableUuid(*outbound_id),
                    edge_type,
                    &SerializableUuid(*inbound_id)
                )?;
                let result = self.retrieve_from_cf("edges", &key).await?;
                match result {
                    Some(v) => Ok(Some(deserialize_edge(&v)?)),
                    None => Ok(None),
                }
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.get_edge_zmq(*port, outbound_id, edge_type, inbound_id).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

    pub async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let key = create_edge_key(
                    &SerializableUuid(*outbound_id),
                    edge_type,
                    &SerializableUuid(*inbound_id)
                )?;
                self.delete_from_cf("edges", &key).await
            }
            Some(SledClientMode::ZMQ(port)) => {
                self.delete_edge_zmq(*port, outbound_id, edge_type, inbound_id).await
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
        }
    }

    pub async fn clear_data(&self) -> GraphResult<()> {
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                for name in &["vertices", "edges", "kv_pairs"] {
                    let tree = (*db).open_tree(name)
                        .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", name, e)))?;
                    tree.clear()
                        .map_err(|e| GraphError::StorageError(format!("Failed to clear tree {}: {}", name, e)))?;
                }
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after clear: {}", e)))?;
                info!("Cleared data from vertices, edges, kv_pairs; flushed {} bytes", bytes_flushed);
                println!("===> Cleared data from vertices, edges, kv_pairs; flushed {} bytes", bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(port)) => {
                let request = json!({ "command": "clear_data" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    info!("Cleared data via ZMQ on port {}", port);
                    println!("===> Cleared data via ZMQ on port {}", port);
                    Ok(())
                } else {
                    let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                    error!("ZMQ clear_data failed on port {}: {}", port, error_msg);
                    println!("===> ERROR: ZMQ clear_data failed on port {}: {}", port, error_msg);
                    Err(GraphError::StorageError(format!("ZMQ clear_data failed: {}", error_msg)))
                }
            }
            None => Err(GraphError::StorageError("SledClient mode not set".to_string()))
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
        info!("Closing SledClient");
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {:?}", e)))?;
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
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes during close", bytes_flushed);
                Ok(())
            }
        }
    }
    pub async fn flush_zmq(&self, port: u16) -> GraphResult<u64> {
        debug!("ZMQ flush: port={}", port);
        let request = json!({
            "command": "flush"
        });
        let response = timeout(TokioDuration::from_secs(10), self.send_zmq_request(port, request))
            .await
            .map_err(|_| {
                error!("Timeout executing ZMQ flush for port {}", port);
                GraphError::StorageError(format!("Timeout executing ZMQ flush for port {}", port))
            })??;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(bytes_flushed) = response.get("bytes_flushed").and_then(|b| b.as_u64()) {
                debug!("ZMQ flush successful: {} bytes flushed", bytes_flushed);
                info!("Completed ZMQ flush for port {}", port);
                Ok(bytes_flushed)
            } else {
                error!("ZMQ flush for port {}: Flush not confirmed by daemon", port);
                Err(GraphError::StorageError("Flush not confirmed by daemon".to_string()))
            }
        } else {
            let error_msg = response
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("ZMQ flush failed for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ flush failed: {}", error_msg)))
        }
    }
    // Update flush, close, and other methods similarly
    pub async fn flush(&self) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::Direct) => {
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock().await;
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
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock().await;
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes", bytes_flushed);
                Ok(())
            }
        }
    }
    pub async fn execute_query(&self, query_plan: QueryPlan) -> GraphResult<QueryResult> {
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
        *self.is_running.lock().await
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
                let _query_plan = query_plan;
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
                let db = self.inner
                    .as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock()
                    .await;
                let bytes_flushed = db.flush_async().await?;
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
                    let error_msg = response
                        .get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("ZMQ close failed: {}", error_msg)))
                }
            }
            None => {
                let db = self.inner
                    .as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock()
                    .await;
                let bytes_flushed = db.flush_async().await?;
                info!("Flushed {} bytes during close", bytes_flushed);
                Ok(())
            }
        }
    }

    // === INDEX METHODS (forward to daemon via ZMQ) ===
    async fn create_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "index_create",
                    "label": label,
                    "property": property
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(())
                } else {
                    let msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("index_create failed: {}", msg)))
                }
            }
            _ => Err(GraphError::StorageError("index_create not supported in direct mode".into()))
        }
    }

    async fn drop_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "index_drop",
                    "label": label,
                    "property": property
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(())
                } else {
                    let msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("index_drop failed: {}", msg)))
                }
            }
            _ => Err(GraphError::StorageError("index_drop not supported in direct mode".into()))
        }
    }

    async fn list_indexes(&self) -> GraphResult<Vec<(String, String)>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "index_list"
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let array = response["indexes"]
                        .as_array()
                        .ok_or(GraphError::StorageError("Invalid indexes response format".into()))?;

                    let mut indexes = Vec::with_capacity(array.len());
                    for item in array {
                        let arr = item.as_array()
                            .ok_or(GraphError::StorageError("Index item must be an array".into()))?;
                        if arr.len() == 2 {
                            let label = arr[0].as_str()
                                .ok_or(GraphError::StorageError("Label must be string".into()))?
                                .to_owned();
                            let property = arr[1].as_str()
                                .ok_or(GraphError::StorageError("Property must be string".into()))?
                                .to_owned();
                            indexes.push((label, property));
                        }
                    }
                    Ok(indexes)
                } else {
                    let msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("index_list failed: {}", msg)))
                }
            }
            _ => Ok(vec![]),
        }
    }

    async fn fulltext_search(&self, query: &str, limit: usize) -> GraphResult<Vec<(String, String)>> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "fulltext_search",
                    "query": query,
                    "limit": limit
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let results = response["results"]
                        .as_array()
                        .ok_or(GraphError::StorageError("Invalid fulltext results".into()))?
                        .iter()
                        .filter_map(|v| {
                            let obj = v.as_object()?;
                            let id = obj.get("id")?.as_str()?.to_string();
                            let content = obj.get("content")?.as_str()?.to_string();
                            Some((id, content))
                        })
                        .collect();
                    Ok(results)
                } else {
                    let msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("fulltext_search failed: {}", msg)))
                }
            }
            _ => Ok(vec![]),
        }
    }

    async fn fulltext_rebuild(&self) -> GraphResult<()> {
        let port = match &self.mode {
            Some(SledClientMode::ZMQ(port)) => *port,
            _ => DEFAULT_STORAGE_PORT,
        };
        match &self.mode {
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "fulltext_rebuild"
                });
                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    Ok(())
                } else {
                    let msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                    Err(GraphError::StorageError(format!("fulltext_rebuild failed: {}", msg)))
                }
            }
            _ => Ok(()),
        }
    }
}

/// Sends a request and receives **exactly one** reply using the shared ZMQ socket.
/// Fixes:
/// - Drains stale replies **before** send
/// - Retries send on `EAGAIN` or `current state`
/// - Retries recv on `EAGAIN` with back-off
/// - Handles "Operation cannot be accomplished in current state" gracefully
/// - **Never blocks**, **never moves socket**, **never violates borrow checker**
/// Sends a request and receives exactly one reply using a FRESH ZMQ socket.
/// This ensures we never violate ZMQ's REQ-REP state machine.
// ------------------------------------------------------------------------
//  zmq_send_recv - NEW IMPLEMENTATION WITH FRESH SOCKETS
// ------------------------------------------------------------------------
async fn zmq_send_recv(
    context: Arc<zmq::Context>,
    port: u16,
    request: Value,
) -> GraphResult<Value> {
    // Add request_id for server-side deduplication
    let mut request = request;
    request["request_id"] = json!(Uuid::new_v4().to_string());
    
    let payload = serde_json::to_vec(&request)
        .map_err(|e| GraphError::SerializationError(format!("serialize request: {}", e)))?;
    
    // Move to blocking thread to avoid blocking async runtime
    let result = tokio::task::spawn_blocking(move || {
        // CREATE FRESH SOCKET FOR THIS REQUEST
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("socket creation: {}", e)))?;
        
        // Configure socket
        socket.set_linger(0)
            .map_err(|e| GraphError::StorageError(format!("set_linger: {}", e)))?;
        socket.set_sndtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("set_sndtimeo: {}", e)))?;
        socket.set_rcvtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("set_rcvtimeo: {}", e)))?;
        
        // Connect
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("connect to {}: {}", endpoint, e)))?;
        
        // Send (blocking is OK, we're in spawn_blocking)
        socket.send(&payload, 0)
            .map_err(|e| GraphError::StorageError(format!("send: {}", e)))?;
        
        // Receive (blocking is OK)
        let msg = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("recv: {}", e)))?;
        
        // Socket is automatically dropped here - clean state for next request
        Ok::<Vec<u8>, GraphError>(msg)
    })
    .await
    .map_err(|e| GraphError::StorageError(format!("task join: {}", e)))??;
    
    // Parse response
    serde_json::from_slice(&result)
        .map_err(|e| GraphError::DeserializationError(format!("parse reply: {}", e)))
}

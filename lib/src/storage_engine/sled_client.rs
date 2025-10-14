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
use tokio::time::{timeout, Duration as TokioDuration};
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
        let context = zmq::Context::new();
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

        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        info!("Connecting to ZMQ endpoint {} for port {}", endpoint, port);
        println!("===> CONNECTING TO ZMQ ENDPOINT {} FOR PORT {}", endpoint, port);
        if let Err(e) = socket.connect(&endpoint) {
            error!("Failed to connect to ZMQ endpoint {}: {}", endpoint, e);
            println!("===> ERROR: FAILED TO CONNECT TO ZMQ ENDPOINT {}: {}", endpoint, e);
            return Err(GraphError::StorageError(format!("Failed to connect to ZMQ socket {}: {}", endpoint, e)));
        }

        let request = json!({ "command": "initialize" });
        info!("Sending initialize request to ZMQ server on port {}", port);
        println!("===> SENDING INITIALIZE REQUEST TO ZMQ SERVER ON PORT {}", port);
        if let Err(e) = socket.send(serde_json::to_vec(&request)?, 0) {
            error!("Failed to send initialize request: {}", e);
            println!("===> ERROR: FAILED TO SEND INITIALIZE REQUEST: {}", e);
            return Err(GraphError::StorageError(format!("Failed to send initialize request: {}", e)));
        }

        let reply = socket.recv_bytes(0)
            .map_err(|e| {
                error!("Failed to receive initialize response: {}", e);
                println!("===> ERROR: FAILED TO RECEIVE INITIALIZE RESPONSE: {}", e);
                GraphError::StorageError(format!("Failed to receive initialize response: {}", e))
            })?;
        let response: Value = serde_json::from_slice(&reply)
            .map_err(|e| {
                error!("Failed to parse initialize response: {}", e);
                println!("===> ERROR: FAILED TO PARSE INITIALIZE RESPONSE: {}", e);
                GraphError::StorageError(format!("Failed to parse initialize response: {}", e))
            })?;

        if response["status"] != "success" {
            error!("ZMQ server not ready for port {}: {:?}", port, response);
            println!("===> ERROR: ZMQ SERVER NOT READY FOR PORT {}: {:?}", port, response);
            return Err(GraphError::StorageError(format!("ZMQ server not ready for port {}: {:?}", port, response)));
        }

        info!("ZMQ server responded successfully for port {}", port);
        println!("===> ZMQ SERVER RESPONDED SUCCESSFULLY FOR PORT {}", port);

        let socket_wrapper = Arc::new(TokioMutex::new(ZmqSocketWrapper(socket)));
        let is_running = Arc::new(TokioMutex::new(true));
        Ok((
            SledClient {
                // FIX: Explicitly specify Option::None.
                inner: Option::None,
                // FIX: Explicitly specify the generic type of None as PathBuf to satisfy the compiler.
                db_path: Option::<PathBuf>::None,
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
        
        let socket_arc = self.zmq_socket.as_ref()
            .ok_or_else(|| GraphError::ConnectionError(format!("SledClient for port {} is not initialized in ZMQ mode.", port)))?
            .clone();

        // Perform the blocking ZMQ send/receive within a spawn_blocking task.
        let response_result = spawn_blocking(move || {
            let socket_wrapper = socket_arc.blocking_lock(); // Use blocking_lock() in spawn_blocking context
            let socket = &socket_wrapper.0; // Access the inner zmq::Socket
            
            SledClient::send_zmq_request_sync(socket, request)
        }).await;

        match response_result {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(GraphError::InternalError(format!("Task failure during ZMQ communication: {}", e))),
        }
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
                self.get_tree_zmq(port, "vertices").await?;
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock().await;

                let tree = db.open_tree("vertices")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open vertices tree: {}", e)))?;

                let mut vertices = Vec::new();
                for item in tree.iter() {
                    let (key, _) = item
                        .map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;

                    let (vertex, _): (Vertex, usize) =
                        bincode::decode_from_slice(&key, bincode::config::standard())
                            .map_err(|e| GraphError::StorageError(format!("Failed to deserialize vertex: {}", e)))?;

                    vertices.push(vertex);
                }
                Ok(vertices)
            }

            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "get_all_vertices"
                });
                let response = self.send_zmq_request(port, request).await?;

                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let vertices = response.get("vertices")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .filter_map(|s| hex::decode(s).ok())
                                .filter_map(|bytes| {
                                    bincode::decode_from_slice::<Vertex, _>(&bytes, bincode::config::standard())
                                        .ok()
                                        .map(|(v, _)| v)
                                })
                                .collect::<Vec<Vertex>>()
                        })
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
                self.get_tree_zmq(port, "vertices").await?;
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock().await;

                let tree = db.open_tree("vertices")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open vertices tree: {}", e)))?;

                let mut vertices = Vec::new();
                for item in tree.iter() {
                    let (key, _) = item
                        .map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;

                    let (vertex, _): (Vertex, usize) =
                        bincode::decode_from_slice(&key, bincode::config::standard())
                            .map_err(|e| GraphError::StorageError(format!("Failed to deserialize vertex: {}", e)))?;

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
                self.get_tree_zmq(port, "edges").await?;
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock().await;

                let tree = db.open_tree("edges")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open edges tree: {}", e)))?;

                let mut edges = Vec::new();
                for item in tree.iter() {
                    let (key, _) = item
                        .map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;

                    // bincode 2 decode
                    let (edge, _): (Edge, usize) = bincode::decode_from_slice(&key, bincode::config::standard())
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edge: {}", e)))?;

                    edges.push(edge);
                }
                Ok(edges)
            }

            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "get_all_edges"
                });

                let response = self.send_zmq_request(port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let edges = response.get("edges")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str())
                                .filter_map(|s| hex::decode(s).ok())
                                .filter_map(|bytes| {
                                    bincode::decode_from_slice::<Edge, _>(&bytes, bincode::config::standard())
                                        .ok()
                                        .map(|(edge, _)| edge)
                                })
                                .collect::<Vec<Edge>>()
                        })
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
                self.get_tree_zmq(port, "edges").await?;
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock().await;

                let tree = db.open_tree("edges")
                    .map_err(|e| GraphError::StorageError(format!("Failed to open edges tree: {}", e)))?;

                let mut edges = Vec::new();
                for item in tree.iter() {
                    let (key, _) = item
                        .map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;

                    let (edge, _): (Edge, usize) = bincode::decode_from_slice(&key, bincode::config::standard())
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edge: {}", e)))?;

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
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available in direct mode".to_string()))?
                    .lock().await;
                for tree_name in &["data", "vertices", "edges", "kv_pairs"] {
                    let tree = db.open_tree(tree_name)
                        .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", tree_name, e)))?;
                    tree.clear()
                        .map_err(|e| GraphError::StorageError(format!("Failed to clear tree {}: {}", tree_name, e)))?;
                }
                let bytes_flushed = (*db).flush_async().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after clear: {}", e)))?;
                info!("Flushed {} bytes after clearing data", bytes_flushed);
                Ok(())
            }
            Some(SledClientMode::ZMQ(_)) => {
                let request = json!({
                    "command": "clear_data"
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
                    Err(GraphError::StorageError(format!("ZMQ clear_data failed: {}", error_msg)))
                }
            }
            None => {
                let db = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?
                    .lock().await;
                for tree_name in &["data", "vertices", "edges", "kv_pairs"] {
                    let tree = db.open_tree(tree_name)
                        .map_err(|e| GraphError::StorageError(format!("Failed to open tree {}: {}", tree_name, e)))?;
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
                let _query_plan = query_plan; // Mark as used to suppress warning
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

                let bytes_flushed = db
                    .flush_async()
                    .await
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

                let bytes_flushed = db
                    .flush_async()
                    .await
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush: {}", e)))?;
                info!("Flushed {} bytes during close", bytes_flushed);
                Ok(())
            }
        }
    }

}

// Extended RocksDBClient implementation with ZMQ support while preserving original functionality
use std::any::Any;
use async_trait::async_trait;
use std::path::{PathBuf, Path};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use tokio::task::{self, spawn_blocking};
use rocksdb::{DB, ColumnFamily, Options, DBCompressionType, WriteBatch, WriteOptions};
use tokio::fs as tokio_fs;
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
pub use crate::config::{QueryResult, RocksDBClient, RaftCommand, RocksDBClientMode};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::daemon_management::is_pid_running;
use crate::storage_engine::storage_utils::{
    serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key
};
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::config::QueryPlan;
use uuid::Uuid;
use log::{info, error, debug, warn};
use serde_json::{json, Value};
use tokio::time::Duration as TokioDuration;
use base64::engine::general_purpose;
use base64::Engine;
use zmq::{Context as ZmqContext, Socket as ZmqSocket, Message, REQ, REP};

// Wrapper for ZmqSocket to implement Debug
pub struct ZmqSocketWrapper(ZmqSocket);
impl ZmqSocketWrapper {
    /// Public constructor required to initialize the private tuple field.
    pub fn new(socket: ZmqSocket) -> Self {
        ZmqSocketWrapper(socket)
    }
    /// Accesses the underlying ZMQ socket.
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
    socket.set_connect_timeout(timeout_ms)?;
    socket.set_rcvtimeo(timeout_ms)?;
    socket.set_sndtimeo(timeout_ms)?;
    socket.connect(ipc_endpoint)?;
    debug!("Successfully connected ZMQ socket to {}", ipc_endpoint);
    Ok(socket)
}

impl RocksDBClient {
    /// Creates a new RocksDBClient in ZMQ mode.
    pub fn new_zmq(port: u16, db_path: PathBuf, socket_arc: Arc<TokioMutex<ZmqSocketWrapper>>) -> Self {
        RocksDBClient {
            mode: Some(RocksDBClientMode::ZMQ(port)),
            inner: None,
            db_path: Some(db_path),
            is_running: Arc::new(TokioMutex::new(true)),
            zmq_socket: Some(socket_arc),
        }
    }

    /// Creates a new RocksDBClient in Direct mode.
    pub async fn new(db_path: PathBuf) -> GraphResult<Self> {
        let cf_names = vec![
            "data", "vertices", "edges", "kv_pairs",
            "raft_log", "raft_snapshot", "raft_membership", "raft_vote"
        ];
        let mut cf_opts = Options::default();
        cf_opts.create_if_missing(true);
        cf_opts.set_compression_type(DBCompressionType::Zstd);
        let cfs = cf_names
            .iter()
            .map(|name| rocksdb::ColumnFamilyDescriptor::new(*name, cf_opts.clone()))
            .collect::<Vec<_>>();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        Self::force_unlock(&db_path).await?;
        let db_path_clone = db_path.clone();
        let db = spawn_blocking(move || {
            DB::open_cf_descriptors(&db_opts, &db_path_clone, cfs)
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to spawn blocking task: {}", e)))?
        .map_err(|e| GraphError::StorageError(format!("Failed to open RocksDB: {}", e)))?;

        info!("Created RocksDBClient in Direct mode at {:?}", db_path);
        println!("===> Created RocksDBClient in Direct mode at {:?}", db_path);
        Ok(Self {
            inner: Some(Arc::new(TokioMutex::new(Arc::new(db)))),
            db_path: Some(db_path),
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(RocksDBClientMode::Direct),
            zmq_socket: None,
        })
    }

    /// Creates a new RocksDBClient with a provided DB in Direct mode.
    pub async fn new_with_db(db_path: PathBuf, db: Arc<DB>) -> GraphResult<Self> {
        info!("Created RocksDBClient with provided DB in Direct mode at {:?}", db_path);
        println!("===> Created RocksDBClient with provided DB in Direct mode at {:?}", db_path);
        Ok(Self {
            inner: Some(Arc::new(TokioMutex::new(db))),
            db_path: Some(db_path),
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(RocksDBClientMode::Direct),
            zmq_socket: None,
        })
    }

    /// Creates a new RocksDBClient that connects to an existing daemon via ZMQ.
    pub async fn new_with_port(port: u16) -> GraphResult<Self> {
        info!("Creating RocksDBClient in ZMQ mode for port {}", port);
        println!("===> Creating RocksDBClient in ZMQ mode for port {}", port);
        let dummy_path = PathBuf::from(format!("/tmp/rocksdb-client-zmq-{}", port));
        let max_retries = 3;
        let mut attempt = 0;
        let mut ping_success = false;
        while attempt < max_retries {
            attempt += 1;
            info!("Attempt {}/{} to ping daemon on port {}", attempt, max_retries, port);
            println!("===> Attempt {}/{} to ping daemon on port {}", attempt, max_retries, port);
            match Self::ping_daemon(port).await {
                Ok(_) => {
                    info!("Successfully pinged RocksDB daemon on port {} via IPC", port);
                    println!("===> Successfully pinged RocksDB daemon on port {} via IPC", port);
                    ping_success = true;
                    break;
                }
                Err(e) => {
                    warn!("Failed to ping daemon on port {} via IPC: {}", port, e);
                    println!("===> Warning: Failed to ping daemon on port {} via IPC: {}", port, e);
                    if attempt == max_retries {
                        info!("Attempting TCP ping for port {}", port);
                        println!("===> Attempting TCP ping for port {}", port);
                        match Self::ping_daemon_tcp(port).await {
                            Ok(_) => {
                                info!("Successfully pinged RocksDB daemon on port {} via TCP", port);
                                println!("===> Successfully pinged RocksDB daemon on port {} via TCP", port);
                                ping_success = true;
                                break;
                            }
                            Err(e) => {
                                warn!("Failed to ping daemon on port {} via TCP: {}", port, e);
                                println!("===> Warning: Failed to ping daemon on port {} via TCP: {}", port, e);
                            }
                        }
                    }
                    tokio::time::sleep(TokioDuration::from_millis(500)).await;
                }
            }
        }
        if !ping_success {
            error!("Failed to connect to RocksDB daemon on port {} after {} attempts", port, max_retries);
            println!("===> ERROR: Failed to connect to RocksDB daemon on port {} after {} attempts", port, max_retries);
            return Err(GraphError::StorageError(format!("Failed to connect to RocksDB daemon on port {}", port)));
        }
        let dummy_path_clone = dummy_path.clone();
        let dummy_db = spawn_blocking(move || {
            let opts = Options::default();
            DB::open_default(&dummy_path_clone)
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to create dummy DB: {}", e)))?
        .map_err(|e| GraphError::StorageError(format!("Failed to create dummy DB: {}", e)))?;

        info!("Successfully created RocksDBClient in ZMQ mode for port {}", port);
        println!("===> Successfully created RocksDBClient in ZMQ mode for port {}", port);
        Ok(Self {
            inner: Some(Arc::new(TokioMutex::new(Arc::new(dummy_db)))),
            db_path: Some(dummy_path),
            is_running: Arc::new(TokioMutex::new(false)),
            mode: Some(RocksDBClientMode::ZMQ(port)),
            zmq_socket: None,
        })
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

    /// Connects to a running ZMQ storage daemon.
    pub async fn connect_zmq_client(port: u16) -> GraphResult<(RocksDBClient, Arc<TokioMutex<ZmqSocketWrapper>>)> {
        let ipc_endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        info!("Attempting ZMQ client connection to {}", ipc_endpoint);
        let default_db_path = PathBuf::from(format!("/opt/graphdb/storage_data/rocksdb/{}", port));
        let connection_result = spawn_blocking(move || {
            let context = ZmqContext::new();
            let connect_timeout_ms = 500;
            match connect_zmq_socket_with_timeout(&context, &ipc_endpoint, connect_timeout_ms) {
                Ok(socket) => {
                    let ping_request = json!({ "command": "ping" });
                    match RocksDBClient::send_zmq_request_sync(&socket, ping_request) {
                        Ok(response) => {
                            if response.get("status").and_then(|s| s.as_str()) == Some("pong") {
                                Ok(socket)
                            } else {
                                Err(format!(
                                    "ZMQ ping failed: Unexpected response from daemon at {}: {:?}",
                                    ipc_endpoint, response
                                ))
                            }
                        }
                        Err(e) => Err(format!("ZMQ ping request failed: {}", e)),
                    }
                }
                Err(e) => Err(format!("Failed to connect ZMQ socket to {}: {}", ipc_endpoint, e)),
            }
        })
        .await
        .map_err(|e| {
            error!("Spawn blocking task failed for ZMQ connection: {}", e);
            println!("===> ERROR: Spawn blocking task failed for ZMQ connection: {}", e);
            GraphError::InternalError(format!("Internal task failure during ZMQ connection: {}", e))
        })?;

        match connection_result {
            Ok(socket) => {
                let socket_arc = Arc::new(TokioMutex::new(ZmqSocketWrapper::new(socket)));
                let client = RocksDBClient::new_zmq(port, default_db_path, socket_arc.clone());
                info!("Successfully created ZMQ client for port {}", port);
                println!("===> Successfully created ZMQ client for port {}", port);
                Ok((client, socket_arc))
            }
            Err(e) => {
                error!("ZMQ connection failed: {}", e);
                println!("===> ERROR: ZMQ connection failed: {}", e);
                Err(GraphError::ConnectionError(e))
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
        Ok((RocksDBClient {
            inner: None,
            db_path: None,
            is_running,
            zmq_socket: Some(socket_wrapper.clone()),
            mode: Some(RocksDBClientMode::ZMQ(port)),
        }, socket_wrapper))
    }

    pub async fn force_unlock(db_path: &Path) -> GraphResult<()> {
        let lock_path = db_path.join("LOCK");
        if lock_path.exists() {
            let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
            let current_pid = std::process::id() as u32;
            let metadatas = daemon_registry.get_all_daemon_metadata().await?;
            let metadata = metadatas
                .into_iter()
                .find(|m| m.data_dir.as_ref() == Some(&db_path.to_path_buf()));
            match metadata {
                Some(metadata) if metadata.pid == current_pid => {
                    warn!("Lock file at {:?} held by current process (PID {}). Skipping unlock.", lock_path, current_pid);
                    println!("===> WARNING: LOCK FILE AT {:?} HELD BY CURRENT PROCESS (PID {}). SKIPPING UNLOCK.", lock_path, current_pid);
                    return Ok(());
                }
                Some(metadata) if is_pid_running(metadata.pid).await => {
                    warn!("Lock file at {:?} held by active process (PID {}). Skipping unlock.", lock_path, metadata.pid);
                    println!("===> WARNING: LOCK FILE AT {:?} HELD BY ACTIVE PROCESS (PID {}). SKIPPING UNLOCK.", lock_path, metadata.pid);
                    return Ok(());
                }
                _ => {
                    info!("Removing stale lock file at {:?}", lock_path);
                    println!("===> REMOVING STALE LOCK FILE AT {:?}", lock_path);
                    tokio::fs::remove_file(&lock_path).await.map_err(|e| {
                        error!("Failed to remove lock file at {:?}: {}", lock_path, e);
                        println!("===> ERROR: FAILED TO REMOVE LOCK FILE AT {:?}: {}", lock_path, e);
                        GraphError::StorageError(format!("Failed to remove lock file at {:?}: {}", lock_path, e))
                    })?;
                }
            }
        }
        info!("No lock file found at {:?}", lock_path);
        println!("===> NO LOCK FILE FOUND AT {:?}", lock_path);
        Ok(())
    }

    pub async fn is_zmq_reachable(port: u16) -> GraphResult<bool> {
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        task::spawn_blocking(move || {
            let context = ZmqContext::new();
            let socket = match context.socket(REQ) {
                Ok(s) => s,
                Err(e) => {
                    debug!("ZMQ ping failed: Failed to create ZMQ socket: {}", e);
                    return Ok(false);
                }
            };
            let _ = socket.set_rcvtimeo(200);
            let _ = socket.set_sndtimeo(200);
            if let Err(e) = socket.connect(&endpoint) {
                debug!("ZMQ ping failed: Failed to connect to {}: {}", endpoint, e);
                return Ok(false);
            }
            let ping_request = json!({ "command": "ping" }).to_string();
            if let Err(e) = socket.send(&ping_request, 0) {
                debug!("ZMQ ping failed: Failed to send request to {}: {}", endpoint, e);
                return Ok(false);
            }
            let mut msg = zmq::Message::new();
            match socket.recv(&mut msg, 0) {
                Ok(_) => {
                    let response_str = msg.as_str().unwrap_or("{}");
                    match serde_json::from_str::<Value>(response_str) {
                        Ok(response) => {
                            let is_success = response.get("status").and_then(|s| s.as_str()) == Some("success");
                            if !is_success {
                                debug!("ZMQ ping failed: Response status not 'success' from {}", endpoint);
                            }
                            Ok(is_success)
                        }
                        Err(e) => {
                            debug!("ZMQ ping failed: Failed to parse JSON response from {}: {}", endpoint, e);
                            Ok(false)
                        }
                    }
                }
                Err(e) => {
                    debug!("ZMQ ping failed: Error receiving response from {}: {}", endpoint, e);
                    Ok(false)
                }
            }
        })
        .await
        .map_err(|e| GraphError::ZmqError(format!("ZMQ blocking task failed: {:?}", e)))?
    }

    pub async fn apply_raft_entry(&self, data: Vec<u8>) -> GraphResult<()> {
        let inner = self.inner.as_ref()
            .ok_or_else(|| GraphError::StorageError("No database available in ZMQ mode".to_string()))?;
        let db = inner.lock().await;
        let cf = (*db).cf_handle("kv_pairs")
            .ok_or_else(|| GraphError::StorageError("Missing column family: kv_pairs".to_string()))?;
        let (key, value) = data.split_at(data.len() / 2);
        (*db).put_cf(&cf, key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        info!("Applied raft entry to kv_pairs");
        println!("===> Applied raft entry to kv_pairs");
        Ok(())
    }

    pub async fn insert_into_cf(&self, cf_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let cf = (*db).cf_handle(cf_name)
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
                (*db).put_cf(&cf, key, value)
                    .map_err(|e| GraphError::StorageError(format!("Failed to insert into {}: {}", cf_name, e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
                info!("Inserted into {}: key={:?}", cf_name, key);
                println!("===> Inserted into {}: key={:?}", cf_name, key);
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.insert_into_cf_zmq(*port, cf_name, key, value).await
            }
            None => {
                error!("RocksDBClient mode not set");
                println!("===> ERROR: RocksDBClient mode not set");
                Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
            }
        }
    }

    pub async fn retrieve_from_cf(&self, cf_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let cf = (*db).cf_handle(cf_name)
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
                let result = (*db).get_cf(&cf, key)
                    .map_err(|e| GraphError::StorageError(format!("Failed to retrieve from {}: {}", cf_name, e)))?;
                info!("Retrieved from {}: key={:?}, value={:?}", cf_name, key, result);
                println!("===> Retrieved from {}: key={:?}, value={:?}", cf_name, key, result);
                Ok(result)
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.retrieve_from_cf_zmq(*port, cf_name, key).await
            }
            None => {
                error!("RocksDBClient mode not set");
                println!("===> ERROR: RocksDBClient mode not set");
                Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
            }
        }
    }

    pub async fn delete_from_cf(&self, cf_name: &str, key: &[u8]) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let cf = (*db).cf_handle(cf_name)
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
                let mut batch = WriteBatch::default();
                batch.delete_cf(&cf, key);
                (*db).write(batch)
                    .map_err(|e| GraphError::StorageError(format!("Failed to delete from {}: {}", cf_name, e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after delete: {}", e)))?;
                info!("Deleted from {}: key={:?}", cf_name, key);
                println!("===> Deleted from {}: key={:?}", cf_name, key);
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.delete_from_cf_zmq(*port, cf_name, key).await
            }
            None => {
                error!("RocksDBClient mode not set");
                println!("===> ERROR: RocksDBClient mode not set");
                Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
            }
        }
    }

    // --- NEW: Dedicated ZMQ Graph Commands (Preserve set_key/get_key/delete_key) ---

    async fn create_vertex_zmq(&self, port: u16, vertex: &Vertex) -> GraphResult<()> {
        let request = json!({
            "command": "create_vertex",
            "vertex": vertex
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ create_vertex successful for port {}: id={}", port, vertex.id);
            println!("===> ZMQ create_vertex successful for port {}: id={}", port, vertex.id);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            error!("ZMQ create_vertex failed for port {}: {}", port, error_msg);
            println!("===> ERROR: ZMQ create_vertex failed for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ create_vertex failed: {}", error_msg)))
        }
    }

    async fn get_vertex_zmq(&self, port: u16, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let request = json!({
            "command": "get_vertex",
            "id": id.to_string()
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(vertex_json) = response.get("vertex") {
                if vertex_json.is_null() {
                    Ok(None)
                } else {
                    let vertex = serde_json::from_value(vertex_json.clone())
                        .map_err(|e| GraphError::DeserializationError(format!("Failed to deserialize vertex: {}", e)))?;
                    Ok(Some(vertex))
                }
            } else {
                Ok(None)
            }
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            error!("ZMQ get_vertex failed for port {}: {}", port, error_msg);
            println!("===> ERROR: ZMQ get_vertex failed for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ get_vertex failed: {}", error_msg)))
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

    async fn create_edge_zmq(&self, port: u16, edge: &Edge) -> GraphResult<()> {
        let request = json!({
            "command": "create_edge",
            "edge": edge
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ create_edge successful for port {}: {} -> {} [{}]", port, edge.outbound_id, edge.inbound_id, edge.edge_type);
            println!("===> ZMQ create_edge successful for port {}: {} -> {} [{}]", port, edge.outbound_id, edge.inbound_id, edge.edge_type);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
            error!("ZMQ create_edge failed for port {}: {}", port, error_msg);
            println!("===> ERROR: ZMQ create_edge failed for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ create_edge failed: {}", error_msg)))
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

    // --- High-Level Graph Operations (Rerouted in ZMQ Mode) ---

    pub async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let uuid = SerializableUuid(vertex.id.0);
                let key = uuid.0.as_bytes();
                let value = serialize_vertex(&vertex)?;
                self.insert_into_cf("vertices", key, &value).await
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.create_vertex_zmq(*port, &vertex).await
            }
            None => Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
        }
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let uuid = SerializableUuid(*id);
                let key = uuid.0.as_bytes();
                let result = self.retrieve_from_cf("vertices", key).await?;
                match result {
                    Some(v) => Ok(Some(deserialize_vertex(&v)?)),
                    None => Ok(None),
                }
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.get_vertex_zmq(*port, id).await
            }
            None => Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
        }
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.create_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let uuid = SerializableUuid(*id);
                let key = uuid.0.as_bytes();
                self.delete_from_cf("vertices", key).await
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.delete_vertex_zmq(*port, id).await
            }
            None => Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
        }
    }

    pub async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let key = create_edge_key(
                    &SerializableUuid(edge.outbound_id.0),
                    &edge.edge_type,
                    &SerializableUuid(edge.inbound_id.0)
                )?;
                let value = serialize_edge(&edge)?;
                self.insert_into_cf("edges", &key, &value).await
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.create_edge_zmq(*port, &edge).await
            }
            None => Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
        }
    }

    pub async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
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
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.get_edge_zmq(*port, outbound_id, edge_type, inbound_id).await
            }
            None => Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
        }
    }

    pub async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    pub async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let key = create_edge_key(
                    &SerializableUuid(*outbound_id),
                    edge_type,
                    &SerializableUuid(*inbound_id)
                )?;
                self.delete_from_cf("edges", &key).await
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                self.delete_edge_zmq(*port, outbound_id, edge_type, inbound_id).await
            }
            None => Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
        }
    }

    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let cf = (*db).cf_handle("vertices")
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: vertices")))?;
                let iter = (*db).iterator_cf(&cf, rocksdb::IteratorMode::Start);
                let mut vertices = Vec::new();
                for res in iter {
                    let (_, value) = res.map_err(|e| GraphError::StorageError(format!("Failed to iterate vertices: {}", e)))?;
                    vertices.push(deserialize_vertex(&value)?);
                }
                info!("Retrieved {} vertices", vertices.len());
                println!("===> Retrieved {} vertices", vertices.len());
                Ok(vertices)
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "get_all_vertices" });
                let response = self.send_zmq_request(*port, request).await?;
               
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    if let Some(vertices_array) = response.get("vertices").and_then(|v| v.as_array()) {
                        let mut vertices = Vec::new();
                        for vertex_value in vertices_array {
                            if let Ok(vertex) = serde_json::from_value::<Vertex>(vertex_value.clone()) {
                                vertices.push(vertex);
                            } else {
                                warn!("Failed to deserialize vertex: {:?}", vertex_value);
                            }
                        }
                        info!("Retrieved {} vertices via ZMQ from port {}", vertices.len(), port);
                        println!("===> Retrieved {} vertices via ZMQ from port {}", vertices.len(), port);
                        Ok(vertices)
                    } else {
                        error!("ZMQ response missing 'vertices' array for get_all_vertices on port {}", port);
                        println!("===> ERROR: ZMQ response missing 'vertices' array for get_all_vertices on port {}", port);
                        Ok(Vec::new())
                    }
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    error!("ZMQ get_all_vertices failed on port {}: {}", port, error_msg);
                    println!("===> ERROR: ZMQ get_all_vertices failed on port {}: {}", port, error_msg);
                    Err(GraphError::StorageError(format!("ZMQ get_all_vertices failed: {}", error_msg)))
                }
            }
            None => {
                error!("RocksDBClient mode not set");
                println!("===> ERROR: RocksDBClient mode not set");
                Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
            }
        }
    }

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let cf = (*db).cf_handle("edges")
                    .ok_or_else(|| GraphError::StorageError(format!("Missing column family: edges")))?;
                let iter = (*db).iterator_cf(&cf, rocksdb::IteratorMode::Start);
                let mut edges = Vec::new();
                for res in iter {
                    let (_, value) = res.map_err(|e| GraphError::StorageError(format!("Failed to iterate edges: {}", e)))?;
                    edges.push(deserialize_edge(&value)?);
                }
                info!("Retrieved {} edges", edges.len());
                println!("===> Retrieved {} edges", edges.len());
                Ok(edges)
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "get_all_edges" });
                let response = self.send_zmq_request(*port, request).await?;
               
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    if let Some(edges_array) = response.get("edges").and_then(|v| v.as_array()) {
                        let mut edges = Vec::new();
                        for edge_value in edges_array {
                            if let Ok(edge) = serde_json::from_value::<Edge>(edge_value.clone()) {
                                edges.push(edge);
                            } else {
                                warn!("Failed to deserialize edge: {:?}", edge_value);
                            }
                        }
                        info!("Retrieved {} edges via ZMQ from port {}", edges.len(), port);
                        println!("===> Retrieved {} edges via ZMQ from port {}", edges.len(), port);
                        Ok(edges)
                    } else {
                        error!("ZMQ response missing 'edges' array for get_all_edges on port {}", port);
                        println!("===> ERROR: ZMQ response missing 'edges' array for get_all_edges on port {}", port);
                        Ok(Vec::new())
                    }
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    error!("ZMQ get_all_edges failed on port {}: {}", port, error_msg);
                    println!("===> ERROR: ZMQ get_all_edges failed on port {}: {}", port, error_msg);
                    Err(GraphError::StorageError(format!("ZMQ get_all_edges failed: {}", error_msg)))
                }
            }
            None => {
                error!("RocksDBClient mode not set");
                println!("===> ERROR: RocksDBClient mode not set");
                Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
            }
        }
    }

    pub async fn clear_data(&self) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                let cfs = ["vertices", "edges", "kv_pairs"];
                let mut batch = WriteBatch::default();
                for cf_name in cfs.iter() {
                    let cf = (*db).cf_handle(cf_name)
                        .ok_or_else(|| GraphError::StorageError(format!("Missing column family: {}", cf_name)))?;
                    let iter = (*db).iterator_cf(&cf, rocksdb::IteratorMode::Start);
                    for res in iter {
                        let (key, _) = res.map_err(|e| GraphError::StorageError(format!("Failed to iterate {}: {}", cf_name, e)))?;
                        batch.delete_cf(&cf, key);
                    }
                }
                (*db).write(batch)
                    .map_err(|e| GraphError::StorageError(format!("Failed to clear data: {}", e)))?;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush after clear: {}", e)))?;
                info!("Cleared data from vertices, edges, and kv_pairs");
                println!("===> Cleared data from vertices, edges, and kv_pairs");
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "clear_data" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    info!("Cleared data via ZMQ on port {}", port);
                    println!("===> Cleared data via ZMQ on port {}", port);
                    Ok(())
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    error!("ZMQ clear_data failed on port {}: {}", port, error_msg);
                    println!("===> ERROR: ZMQ clear_data failed on port {}: {}", port, error_msg);
                    Err(GraphError::StorageError(format!("ZMQ clear_data failed: {}", error_msg)))
                }
            }
            None => {
                error!("RocksDBClient mode not set");
                println!("===> ERROR: RocksDBClient mode not set");
                Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
            }
        }
    }

    pub async fn connect(&mut self) -> GraphResult<()> {
        info!("Connecting to RocksDB");
        println!("===> Connecting to RocksDB");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    pub async fn start(&mut self) -> GraphResult<()> {
        info!("Starting RocksDB");
        println!("===> Starting RocksDB");
        let mut is_running = self.is_running.lock().await;
        *is_running = true;
        Ok(())
    }

    pub async fn stop(&mut self) -> GraphResult<()> {
        info!("Stopping RocksDB");
        println!("===> Stopping RocksDB");
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        Ok(())
    }

    pub async fn close(&mut self) -> GraphResult<()> {
        info!("Closing RocksDB");
        println!("===> Closing RocksDB");
        let mut is_running = self.is_running.lock().await;
        *is_running = false;
        Ok(())
    }

    pub async fn flush(&self) -> GraphResult<()> {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => {
                let inner = self.inner.as_ref()
                    .ok_or_else(|| GraphError::StorageError("No database available".to_string()))?;
                let db = inner.lock().await;
                (*db).flush_wal(true)
                    .map_err(|e| GraphError::StorageError(format!("Failed to flush WAL: {}", e)))?;
                info!("Flushed WAL");
                println!("===> Flushed WAL");
                Ok(())
            }
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "flush" });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    info!("Flushed via ZMQ on port {}", port);
                    println!("===> Flushed via ZMQ on port {}", port);
                    Ok(())
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    error!("ZMQ flush failed on port {}: {}", port, error_msg);
                    println!("===> ERROR: ZMQ flush failed on port {}: {}", port, error_msg);
                    Err(GraphError::StorageError(format!("ZMQ flush failed: {}", error_msg)))
                }
            }
            None => {
                error!("RocksDBClient mode not set");
                println!("===> ERROR: RocksDBClient mode not set");
                Err(GraphError::StorageError("RocksDBClient mode not set".to_string()))
            }
        }
    }

    pub async fn execute_query(&self, query_plan: QueryPlan) -> GraphResult<QueryResult> {
        info!("Executing query on RocksDBClient (not implemented)");
        println!("===> Executing query on RocksDBClient (not implemented)");
        Ok(QueryResult::Null)
    }

    pub async fn ping_daemon(port: u16) -> GraphResult<()> {
        let socket_path = format!("/tmp/graphdb-{}.ipc", port);
        let addr = format!("ipc://{}", socket_path);
        if !tokio::fs::metadata(&socket_path).await.is_ok() {
            warn!("IPC socket file {} does not exist for port {}", socket_path, port);
            println!("===> Warning: IPC socket file {} does not exist for port {}", socket_path, port);
            return Err(GraphError::StorageError(format!("IPC socket file {} does not exist", socket_path)));
        }
        let request = json!({ "command": "ping" });
        let response = Self::send_zmq_request_inner(port, request, false).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("Ping successful via IPC for port {}", port);
            println!("===> Ping successful via IPC for port {}", port);
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("Ping failed via IPC for port {}: {}", port, error_msg);
            println!("===> ERROR: Ping failed via IPC for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("Ping failed: {}", error_msg)))
        }
    }

    pub async fn ping_daemon_tcp(port: u16) -> GraphResult<()> {
        let request = json!({ "command": "ping" });
        let response = Self::send_zmq_request_inner(port, request, true).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("Ping successful via TCP for port {}", port);
            println!("===> Ping successful via TCP for port {}", port);
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("Ping failed via TCP for port {}: {}", port, error_msg);
            println!("===> ERROR: Ping failed via TCP for port {}: {}", port, error_msg);
            Err(GraphError::StorageError(format!("TCP ping failed: {}", error_msg)))
        }
    }

    async fn send_zmq_request_inner(port: u16, request: Value, use_tcp: bool) -> GraphResult<Value> {
        let socket_path = format!("/tmp/graphdb-{}.ipc", port);
        let ipc_addr = format!("ipc://{}", socket_path);
        let tcp_addr = format!("tcp://127.0.0.1:{}", port);
        let addr = if use_tcp { &tcp_addr } else { &ipc_addr };
        let request_data = serde_json::to_vec(&request)
            .map_err(|e| GraphError::StorageError(format!("Failed to serialize request: {}", e)))?;
        let max_retries = 3;
        let mut attempt = 0;
        while attempt < max_retries {
            attempt += 1;
            info!("Attempt {}/{} to send ZMQ request to port {} via {}", attempt, max_retries, port, if use_tcp { "TCP" } else { "IPC" });
            println!("===> Attempt {}/{} to send ZMQ request to port {} via {}", attempt, max_retries, port, if use_tcp { "TCP" } else { "IPC" });
            let result = spawn_blocking({
                let addr = addr.to_string();
                let request_data = request_data.clone();
                move || -> Result<Value, GraphError> {
                    let context = zmq::Context::new();
                    let socket = context.socket(zmq::REQ)
                        .map_err(|e| GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)))?;
                    socket.set_rcvtimeo(15000)
                        .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
                    socket.set_sndtimeo(10000)
                        .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;
                    socket.set_linger(0)
                        .map_err(|e| GraphError::StorageError(format!("Failed to set linger: {}", e)))?;
                    socket.connect(&addr)
                        .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", addr, e)))?;
                    socket.send(&request_data, 0)
                        .map_err(|e| GraphError::StorageError(format!("Failed to send request to {}: {}", addr, e)))?;
                    let mut msg = zmq::Message::new();
                    socket.recv(&mut msg, 0)
                        .map_err(|e| GraphError::StorageError(format!("Failed to receive response from {}: {}", addr, e)))?;
                    let response: Value = serde_json::from_slice(&msg)
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize response from {}: {}", addr, e)))?;
                    Ok(response)
                }
            })
            .await;
            match result {
                Ok(Ok(response)) => {
                    info!("Successfully sent ZMQ request to port {} via {}: {:?}", port, if use_tcp { "TCP" } else { "IPC" }, response);
                    println!("===> Successfully sent ZMQ request to port {} via {}: {:?}", port, if use_tcp { "TCP" } else { "IPC" }, response);
                    return Ok(response);
                }
                Ok(Err(e)) => {
                    warn!("ZMQ request to port {} via {} failed on attempt {}/{}: {}", port, if use_tcp { "TCP" } else { "IPC" }, attempt, max_retries, e);
                    println!("===> Warning: ZMQ request to port {} via {} failed on attempt {}/{}: {}", port, if use_tcp { "TCP" } else { "IPC" }, attempt, max_retries, e);
                }
                Err(e) => {
                    warn!("ZMQ task failed for port {} via {} on attempt {}/{}: {}", port, if use_tcp { "TCP" } else { "IPC" }, attempt, max_retries, e);
                    println!("===> Warning: ZMQ task failed for port {} via {} on attempt {}/{}: {}", port, if use_tcp { "TCP" } else { "IPC" }, attempt, max_retries, e);
                }
            }
            if attempt < max_retries {
                tokio::time::sleep(TokioDuration::from_millis(100 * attempt as u64)).await;
            }
        }
        Err(GraphError::StorageError(format!(
            "Failed to send ZMQ request to port {} via {} after {} attempts",
            port, if use_tcp { "TCP" } else { "IPC" }, max_retries
        )))
    }

    pub async fn send_zmq_request(&self, port: u16, request: Value) -> GraphResult<Value> {
        let socket_path = format!("/tmp/graphdb-{}.ipc", port);
        let mut use_tcp = false;
        if !tokio::fs::metadata(&socket_path).await.is_ok() {
            warn!("IPC socket file {} does not exist for port {}, will try TCP", socket_path, port);
            println!("===> Warning: IPC socket file {} does not exist for port {}, will try TCP", socket_path, port);
            use_tcp = true;
        }
        let response = Self::send_zmq_request_inner(port, request.clone(), use_tcp).await;
        if response.is_ok() || use_tcp {
            return response;
        }
        warn!("Retrying ZMQ request to port {} via TCP after IPC failure", port);
        println!("===> Retrying ZMQ request to port {} via TCP after IPC failure", port);
        Self::send_zmq_request_inner(port, request, true).await
    }

    async fn insert_into_cf_zmq(&self, port: u16, cf_name: &str, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let request = json!({
            "command": "set_key",
            "key": String::from_utf8_lossy(key),
            "value": String::from_utf8_lossy(value),
            "cf": cf_name
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ insert into {} successful for port {}: key={:?}", cf_name, port, key);
            println!("===> ZMQ insert into {} successful for port {}: key={:?}", cf_name, port, key);
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("ZMQ insert into {} failed for port {}: {}", cf_name, port, error_msg);
            println!("===> ERROR: ZMQ insert into {} failed for port {}: {}", cf_name, port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ insert failed: {}", error_msg)))
        }
    }

    async fn retrieve_from_cf_zmq(&self, port: u16, cf_name: &str, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let request = json!({
            "command": "get_key",
            "key": String::from_utf8_lossy(key),
            "cf": cf_name
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            if let Some(value) = response.get("value") {
                if value.is_null() {
                    info!("ZMQ retrieve from {} for port {}: key={:?}, value=None", cf_name, port, key);
                    println!("===> ZMQ retrieve from {} for port {}: key={:?}, value=None", cf_name, port, key);
                    Ok(None)
                } else {
                    let value_str = value.as_str()
                        .ok_or_else(|| GraphError::StorageError("Invalid value format".to_string()))?;
                    info!("ZMQ retrieve from {} for port {}: key={:?}, value={}", cf_name, port, key, value_str);
                    println!("===> ZMQ retrieve from {} for port {}: key={:?}, value={}", cf_name, port, key, value_str);
                    Ok(Some(value_str.as_bytes().to_vec()))
                }
            } else {
                info!("ZMQ retrieve from {} for port {}: key={:?}, value=None", cf_name, port, key);
                println!("===> ZMQ retrieve from {} for port {}: key={:?}, value=None", cf_name, port, key);
                Ok(None)
            }
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("ZMQ retrieve from {} failed for port {}: {}", cf_name, port, error_msg);
            println!("===> ERROR: ZMQ retrieve from {} failed for port {}: {}", cf_name, port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ retrieve failed: {}", error_msg)))
        }
    }

    async fn delete_from_cf_zmq(&self, port: u16, cf_name: &str, key: &[u8]) -> GraphResult<()> {
        let request = json!({
            "command": "delete_key",
            "key": String::from_utf8_lossy(key),
            "cf": cf_name
        });
        let response = self.send_zmq_request(port, request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("ZMQ delete from {} successful for port {}: key={:?}", cf_name, port, key);
            println!("===> ZMQ delete from {} successful for port {}: key={:?}", cf_name, port, key);
            Ok(())
        } else {
            let error_msg = response.get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error");
            error!("ZMQ delete from {} failed for port {}: {}", cf_name, port, error_msg);
            println!("===> ERROR: ZMQ delete from {} failed for port {}: {}", cf_name, port, error_msg);
            Err(GraphError::StorageError(format!("ZMQ delete failed: {}", error_msg)))
        }
    }
}

#[async_trait]
impl StorageEngine for RocksDBClient {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to RocksDBClient");
        println!("===> Connecting to RocksDBClient");
        Ok(())
    }
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        self.insert_into_cf("kv_pairs", &key, &value).await
    }
    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        self.retrieve_from_cf("kv_pairs", key).await
    }
    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        self.delete_from_cf("kv_pairs", key).await
    }
    async fn flush(&self) -> Result<(), GraphError> {
        self.flush().await
    }
}

#[async_trait]
impl GraphStorageEngine for RocksDBClient {
    async fn start(&self) -> Result<(), GraphError> {
        info!("Starting RocksDBClient");
        println!("===> Starting RocksDBClient");
        Ok(())
    }
    async fn stop(&self) -> Result<(), GraphError> {
        info!("Stopping RocksDBClient");
        println!("===> Stopping RocksDBClient");
        Ok(())
    }
    fn get_type(&self) -> &'static str {
        match &self.mode {
            Some(RocksDBClientMode::Direct) => "rocksdb_client",
            Some(RocksDBClientMode::ZMQ(_)) => "rocksdb_client_zmq",
            None => "rocksdb_client",
        }
    }
    async fn is_running(&self) -> bool {
        *self.is_running.lock().await
    }
    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        match &self.mode {
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "query", "query": query_string });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    info!("ZMQ query successful for port {}: query={}", port, query_string);
                    println!("===> ZMQ query successful for port {}: query={}", port, query_string);
                    Ok(response.get("value").cloned().unwrap_or(Value::Null))
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    error!("ZMQ query failed for port {}: {}", port, error_msg);
                    println!("===> ERROR: ZMQ query failed for port {}: {}", port, error_msg);
                    Err(GraphError::StorageError(format!("ZMQ query failed: {}", error_msg)))
                }
            }
            _ => {
                error!("RocksDBClient query not implemented for direct access");
                println!("===> ERROR: RocksDBClient query not implemented for direct access");
                Err(GraphError::StorageError("RocksDBClient query not implemented for direct access".to_string()))
            }
        }
    }
    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.create_vertex(vertex).await
    }
    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        self.get_vertex(id).await
    }
    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.update_vertex(vertex).await
    }
    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.delete_vertex(id).await
    }
    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        self.get_all_vertices().await
    }
    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.create_edge(edge).await
    }
    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        self.get_edge(outbound_id, edge_type, inbound_id).await
    }
    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.update_edge(edge).await
    }
    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.delete_edge(outbound_id, edge_type, inbound_id).await
    }
    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        self.get_all_edges().await
    }
    async fn clear_data(&self) -> Result<(), GraphError> {
        self.clear_data().await
    }
    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        match &self.mode {
            Some(RocksDBClientMode::ZMQ(port)) => {
                let request = json!({ "command": "execute_query", "query_plan": query_plan });
                let response = self.send_zmq_request(*port, request).await?;
                if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                    let result = response.get("value")
                        .map(|v| serde_json::from_value(v.clone()))
                        .transpose()
                        .map_err(|e| GraphError::StorageError(format!("Failed to deserialize query result: {}", e)))?
                        .unwrap_or(QueryResult::Null);
                    info!("ZMQ execute_query successful for port {}", port);
                    println!("===> ZMQ execute_query successful for port {}", port);
                    Ok(result)
                } else {
                    let error_msg = response.get("message")
                        .and_then(|m| m.as_str())
                        .unwrap_or("Unknown error");
                    error!("ZMQ execute_query failed for port {}: {}", port, error_msg);
                    println!("===> ERROR: ZMQ execute_query failed for port {}: {}", port, error_msg);
                    Err(GraphError::StorageError(format!("ZMQ execute_query failed: {}", error_msg)))
                }
            }
            _ => {
                info!("Executing query on RocksDBClient (returning null as not implemented)");
                println!("===> Executing query on RocksDBClient (returning null as not implemented)");
                Ok(QueryResult::Null)
            }
        }
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    async fn close(&self) -> Result<(), GraphError> {
        info!("Closing RocksDBClient");
        println!("===> Closing RocksDBClient");
        Ok(())
    }
}

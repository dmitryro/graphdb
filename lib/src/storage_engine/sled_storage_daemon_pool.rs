use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io::{Cursor, Read, Write};
use sled::{Config, Db, IVec, Tree, Batch};
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::time::{Duration, timeout};
use tokio::fs;
use log::{info, debug, warn, error};
use crate::config::{SledConfig, SledDaemon, SledDaemonPool, SledStorage, StorageConfig, DAEMON_REGISTRY_DB_PATH};
use crate::storage_engine::storage_utils::{create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::{json, Value};
use crate::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;
use uuid::Uuid;
use sysinfo::{System, Pid};
use zmq::{Context as ZmqContext, Socket as ZmqSocket};

#[cfg(feature = "with-openraft-sled")]
use {
    async_trait::async_trait,
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode},
    openraft_sled::SledRaftStorage,
    tokio::net::TcpStream,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

impl SledDaemon {
    pub async fn new(config: &SledConfig) -> GraphResult<Self> {
        println!("===> TRYING TO INITIALIZE SLED DAEMON POOL");
        let db_path = config.path.join("db");
        info!("Initializing SledDaemon with path {:?}", db_path);
        println!("===> INITIALIZING SLED DAEMON WITH PATH {:?}", db_path);
        debug!("Caller stack trace: {:#?}", std::backtrace::Backtrace::capture());

        // Ensure the database directory exists
        if db_path.exists() {
            if !db_path.is_dir() {
                warn!("Path {:?} exists but is not a directory, removing it", db_path);
                println!("===> WARNING: PATH {:?} EXISTS BUT IS NOT A DIRECTORY, REMOVING IT", db_path);
                fs::remove_file(&db_path)
                    .await
                    .map_err(|e| GraphError::StorageError(format!("Failed to remove file at {:?}: {}", db_path, e)))?;
                info!("Creating directory at {:?}", db_path);
                println!("===> CREATING DIRECTORY AT {:?}", db_path);
                fs::create_dir_all(&db_path)
                    .await
                    .map_err(|e| GraphError::StorageError(format!("Failed to create directory at {:?}: {}", db_path, e)))?;
            }
        } else {
            info!("Creating database directory at {:?}", db_path);
            println!("===> CREATING DATABASE DIRECTORY AT {:?}", db_path);
            fs::create_dir_all(&db_path)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e)))?;
        }

        // Check for running processes that might conflict
        let port = config.port.ok_or_else(|| {
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;
        println!("===> USING PORT {} FOR SLED DAEMON", port);
        let mut system = System::new_all();
        system.refresh_all();
        let port_in_use = system.processes().values().any(|proc| {
            proc.cmd().iter().any(|arg| arg.to_string_lossy().contains(&port.to_string())) && 
            proc.name().to_string_lossy().contains("graphdb")
        });
        if port_in_use {
            error!("Port {} is already in use by another process", port);
            println!("===> ERROR: PORT {} IS ALREADY IN USE BY ANOTHER PROCESS", port);
            return Err(GraphError::StorageError(format!("Port {} is already in use", port)));
        }

        // Check daemon registry without cleanup
        let daemon_metadata = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?;
        if daemon_metadata.is_some() {
            warn!("Storage process already registered on port {}. Skipping cleanup.", port);
            println!("===> WARNING: STORAGE PROCESS ALREADY REGISTERED ON PORT {}. SKIPPING CLEANUP.", port);
            return Err(GraphError::StorageError(format!("Daemon already registered on port {}", port)));
        }

        // Open sled DB with compression enabled
        debug!("Attempting to open Sled DB at {:?}", db_path);
        println!("===> ATTEMPTING TO OPEN SLED DB AT {:?}", db_path);
        let db: Db = Config::new()
            .path(&db_path)
            .use_compression(true)
            .cache_capacity(config.cache_capacity.unwrap_or(1024 * 1024 * 1024))
            .flush_every_ms(Some(100))
            .open()
            .map_err(|e| {
                error!("Failed to open Sled DB at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO OPEN SLED DB AT {:?}: {}", db_path, e);
                GraphError::StorageError(format!(
                    "Failed to open Sled DB at {:?}: {}. Ensure no other process is using the database.",
                    db_path, e
                ))
            })?;
        let integrity_check = db.was_recovered();
        info!("Sled DB opened at {:?}, was_recovered: {}", db_path, integrity_check);
        println!("===> SLED DB OPENED AT {:?}, WAS_RECOVERED: {}", db_path, integrity_check);

        // Open trees used by the system
        let vertices: Tree = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edges: Tree = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let kv_pairs: Tree = db.open_tree("kv_pairs").map_err(|e| GraphError::StorageError(e.to_string()))?;

        // Log key count to avoid unnecessary retrieval
        let kv_key_count = kv_pairs.len();
        let kv_keys: Vec<_> = kv_pairs.iter().keys().filter_map(|k| k.ok()).collect();
        info!("Initial kv_pairs key count at {:?}: {}, keys: {:?}", db_path, kv_key_count, kv_keys);
        println!("===> INITIAL KV_PAIRS KEY COUNT AT {:?}: {}, KEYS: {:?}", db_path, kv_key_count, kv_keys);

        #[cfg(feature = "with-openraft-sled")]
        let (raft, raft_storage) = {
            let raft_db_path = db_path.join("raft");
            if !raft_db_path.exists() {
                tokio::fs::create_dir_all(&raft_db_path)
                    .await
                    .map_err(|e| GraphError::Io(e))?;
                println!("===> CREATED RAFT DB DIRECTORY AT {:?}", raft_db_path);
            }
            let raft_storage = timeout(Duration::from_secs(5), SledRaftStorage::new(&raft_db_path))
                .await
                .map_err(|_| GraphError::StorageError(format!(
                    "Timeout initializing Raft storage at {:?}", raft_db_path
                )))?
                .map_err(|e| GraphError::StorageError(format!(
                    "Failed to initialize Raft storage at {:?}: {}", raft_db_path, e
                )))?;
            let raft_config = RaftConfig {
                cluster_name: "graphdb-cluster".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            };
            let raft = Raft::new(
                port as u64,
                Arc::new(raft_config),
                Arc::new(raft_storage.clone()),
                Arc::new(RaftTcpNetwork {}),
            )
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to initialize Raft: {}", e)))?;
            println!("===> INITIALIZED RAFT FOR NODE {} ON PORT {}", port, port);
            (Arc::new(raft), Arc::new(raft_storage))
        };

        #[cfg(not(feature = "with-openraft-sled"))]
        let (raft, raft_storage, node_id) = (None::<()>, None::<()>, 0);

        let daemon = Self {
            port,
            db_path,
            db: Arc::new(db),
            vertices,
            edges,
            kv_pairs,
            running: Arc::new(TokioMutex::new(true)),
            #[cfg(feature = "with-openraft-sled")]
            raft_storage,
            #[cfg(feature = "with-openraft-sled")]
            raft,
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
        };

        // Start ZeroMQ server in a separate task
        let daemon_clone = Arc::new(daemon.clone());
        tokio::spawn(async move {
            if let Err(e) = daemon_clone.run_zmq_server().await {
                error!("ZeroMQ server failed: {}", e);
                println!("===> ERROR: ZEROMQ SERVER FAILED: {}", e);
            }
        });

        // Register SIGTERM handler to gracefully close
        #[cfg(unix)]
        {
            let daemon_clone = Arc::new(daemon.clone());
            tokio::spawn(async move {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                while sigterm.recv().await.is_some() {
                    warn!("Received SIGTERM in SledDaemon at {:?}", chrono::Local::now());
                    println!("===> WARNING: RECEIVED SIGTERM IN SLED DAEMON AT {:?}", chrono::Local::now());
                    if let Err(e) = daemon_clone.close().await {
                        error!("Failed to close SledDaemon on SIGTERM: {}", e);
                        println!("===> ERROR: FAILED TO CLOSE SLED DAEMON ON SIGTERM: {}", e);
                    } else {
                        info!("SledDaemon shutdown complete");
                        println!("===> SLED DAEMON SHUTDOWN COMPLETE");
                    }
                }
            });
        }

        Ok(daemon)
    }

    async fn run_zmq_server(&self) -> GraphResult<()> {
        let context = ZmqContext::new();
        let responder = context.socket(zmq::REP)
            .map_err(|e| {
                error!("Failed to create ZeroMQ socket: {}", e);
                println!("===> ERROR: FAILED TO CREATE ZEROMQ SOCKET: {}", e);
                GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e))
            })?;
        let endpoint = format!("ipc://graphdb_{}.ipc", self.port);
        let socket_path = format!("/tmp/graphdb_{}.ipc", self.port);
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            info!("Removing stale IPC socket file: {}", socket_path);
            println!("===> REMOVING STALE IPC SOCKET FILE: {}", socket_path);
            tokio::fs::remove_file(&socket_path)
                .await
                .map_err(|e| {
                    error!("Failed to remove stale IPC socket {}: {}", socket_path, e);
                    println!("===> ERROR: FAILED TO REMOVE STALE IPC SOCKET {}: {}", socket_path, e);
                    GraphError::StorageError(format!("Failed to remove stale IPC socket {}: {}", socket_path, e))
                })?;
        }
        info!("Attempting to bind ZeroMQ socket to {}", endpoint);
        println!("===> ATTEMPTING TO BIND ZEROMQ SOCKET TO {}", endpoint);
        responder.bind(&endpoint)
            .map_err(|e| {
                error!("Failed to bind ZeroMQ socket to {}: {}", endpoint, e);
                println!("===> ERROR: FAILED TO BIND ZEROMQ SOCKET TO {}: {}", endpoint, e);
                GraphError::StorageError(format!("Failed to bind ZeroMQ socket on port {}: {}", self.port, e))
            })?;
        info!("ZeroMQ server started on ipc://graphdb_{}.ipc", self.port);
        println!("===> ZEROMQ SERVER STARTED ON ipc://graphdb_{}.ipc", self.port);

        while *self.running.lock().await {
            let msg = match responder.recv_bytes(0) {
                Ok(msg) => {
                    debug!("Received ZeroMQ message: {:?}", String::from_utf8_lossy(&msg));
                    println!("===> RECEIVED ZEROMQ MESSAGE: {:?}", String::from_utf8_lossy(&msg));
                    msg
                }
                Err(e) => {
                    warn!("Failed to receive ZeroMQ message: {}", e);
                    println!("===> WARNING: FAILED TO RECEIVE ZEROMQ MESSAGE: {}", e);
                    continue;
                }
            };
            let request: Value = match serde_json::from_slice(&msg) {
                Ok(req) => {
                    debug!("Parsed request: {:?}", req);
                    println!("===> PARSED ZEROMQ REQUEST: {:?}", req);
                    req
                }
                Err(e) => {
                    error!("Failed to parse ZeroMQ request: {}", e);
                    println!("===> ERROR: FAILED TO PARSE ZEROMQ REQUEST: {}", e);
                    let response = json!({ "status": "error", "message": format!("Failed to parse request: {}", e) });
                    let response_data = serde_json::to_vec(&response)
                        .map_err(|e| {
                            error!("Failed to serialize error response: {}", e);
                            println!("===> ERROR: FAILED TO SERIALIZE ERROR RESPONSE: {}", e);
                            GraphError::StorageError(format!("Failed to serialize error response: {}", e))
                        })?;
                    if let Err(e) = responder.send(&response_data, 0) {
                        warn!("Failed to send error response: {}", e);
                        println!("===> WARNING: FAILED TO SEND ERROR RESPONSE: {}", e);
                    }
                    continue;
                }
            };

            let response = match request.get("command").and_then(|c| c.as_str()) {
                Some("set_key") => {
                    let key = request.get("key").and_then(|k| k.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing key in set_key request".to_string())
                    })?;
                    let value = request.get("value").and_then(|v| v.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing value in set_key request".to_string())
                    })?;
                    println!("===> PROCESSING SET_KEY: key={}, value={}", key, value);
                    match self.insert(key.as_bytes(), value.as_bytes()).await {
                        Ok(_) => {
                            println!("===> SET_KEY SUCCESS: key={}", key);
                            json!({ "status": "success" })
                        }
                        Err(e) => {
                            println!("===> SET_KEY ERROR: key={}, error={}", key, e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("get_key") => {
                    let key = request.get("key").and_then(|k| k.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing key in get_key request".to_string())
                    })?;
                    println!("===> PROCESSING GET_KEY: key={}", key);
                    match self.retrieve(key.as_bytes()).await {
                        Ok(Some(val)) => {
                            println!("===> GET_KEY SUCCESS: key={}, value={}", key, String::from_utf8_lossy(&val));
                            json!({ "status": "success", "value": String::from_utf8_lossy(&val).to_string() })
                        }
                        Ok(None) => {
                            println!("===> GET_KEY SUCCESS: key={} not found", key);
                            json!({ "status": "success", "value": null })
                        }
                        Err(e) => {
                            println!("===> GET_KEY ERROR: key={}, error={}", key, e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("delete_key") => {
                    let key = request.get("key").and_then(|k| k.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing key in delete_key request".to_string())
                    })?;
                    println!("===> PROCESSING DELETE_KEY: key={}", key);
                    match self.delete(key.as_bytes()).await {
                        Ok(_) => {
                            println!("===> DELETE_KEY SUCCESS: key={}", key);
                            json!({ "status": "success" })
                        }
                        Err(e) => {
                            println!("===> DELETE_KEY ERROR: key={}, error={}", key, e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("create_vertex") => {
                    let vertex = request.get("vertex").ok_or_else(|| {
                        GraphError::StorageError("Missing vertex in create_vertex request".to_string())
                    })?;
                    println!("===> PROCESSING CREATE_VERTEX: vertex={:?}", vertex);
                    let vertex: Vertex = serde_json::from_value(vertex.clone())
                        .map_err(|e| GraphError::StorageError(format!("Failed to parse vertex: {}", e)))?;
                    match self.create_vertex(&vertex).await {
                        Ok(_) => {
                            println!("===> CREATE_VERTEX SUCCESS: id={}", vertex.id);
                            json!({ "status": "success" })
                        }
                        Err(e) => {
                            println!("===> CREATE_VERTEX ERROR: id={}, error={}", vertex.id, e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("get_vertex") => {
                    let id = request.get("id").and_then(|i| i.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing id in get_vertex request".to_string())
                    })?;
                    println!("===> PROCESSING GET_VERTEX: id={}", id);
                    let uuid = Uuid::parse_str(id)
                        .map_err(|e| GraphError::StorageError(format!("Invalid UUID: {}", e)))?;
                    match self.get_vertex(&uuid).await {
                        Ok(Some(vertex)) => {
                            println!("===> GET_VERTEX SUCCESS: id={}", uuid);
                            json!({ "status": "success", "vertex": vertex })
                        }
                        Ok(None) => {
                            println!("===> GET_VERTEX SUCCESS: id={} not found", uuid);
                            json!({ "status": "success", "vertex": null })
                        }
                        Err(e) => {
                            println!("===> GET_VERTEX ERROR: id={}, error={}", uuid, e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("create_edge") => {
                    let edge = request.get("edge").ok_or_else(|| {
                        GraphError::StorageError("Missing edge in create_edge request".to_string())
                    })?;
                    println!("===> PROCESSING CREATE_EDGE: edge={:?}", edge);
                    let edge: Edge = serde_json::from_value(edge.clone())
                        .map_err(|e| GraphError::StorageError(format!("Failed to parse edge: {}", e)))?;
                    match self.create_edge(&edge).await {
                        Ok(_) => {
                            println!("===> CREATE_EDGE SUCCESS: outbound_id={}, edge_type={}, inbound_id={}",
                                edge.outbound_id, edge.t, edge.inbound_id);
                            json!({ "status": "success" })
                        }
                        Err(e) => {
                            println!("===> CREATE_EDGE ERROR: outbound_id={}, edge_type={}, inbound_id={}, error={}",
                                edge.outbound_id, edge.t, edge.inbound_id, e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("get_edge") => {
                    let outbound_id = request.get("outbound_id").and_then(|i| i.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing outbound_id in get_edge request".to_string())
                    })?;
                    let edge_type = request.get("edge_type").and_then(|t| t.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing edge_type in get_edge request".to_string())
                    })?;
                    let inbound_id = request.get("inbound_id").and_then(|i| i.as_str()).ok_or_else(|| {
                        GraphError::StorageError("Missing inbound_id in get_edge request".to_string())
                    })?;
                    println!("===> PROCESSING GET_EDGE: outbound_id={}, edge_type={}, inbound_id={}",
                        outbound_id, edge_type, inbound_id);
                    let outbound_uuid = Uuid::parse_str(outbound_id)
                        .map_err(|e| GraphError::StorageError(format!("Invalid outbound UUID: {}", e)))?;
                    let inbound_uuid = Uuid::parse_str(inbound_id)
                        .map_err(|e| GraphError::StorageError(format!("Invalid inbound UUID: {}", e)))?;
                    let edge_type = Identifier::new(edge_type.to_string())
                        .map_err(|e| GraphError::StorageError(format!("Invalid edge type: {}", e)))?;
                    match self.get_edge(&outbound_uuid, &edge_type, &inbound_uuid).await {
                        Ok(Some(edge)) => {
                            println!("===> GET_EDGE SUCCESS: outbound_id={}, edge_type={}, inbound_id={}",
                                outbound_uuid, edge_type, inbound_uuid);
                            json!({ "status": "success", "edge": edge })
                        }
                        Ok(None) => {
                            println!("===> GET_EDGE SUCCESS: outbound_id={}, edge_type={}, inbound_id={} not found",
                                outbound_uuid, edge_type, inbound_uuid);
                            json!({ "status": "success", "edge": null })
                        }
                        Err(e) => {
                            println!("===> GET_EDGE ERROR: outbound_id={}, edge_type={}, inbound_id={}, error={}",
                                outbound_uuid, edge_type, inbound_uuid, e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                _ => {
                    error!("Unsupported command: {:?}", request);
                    println!("===> ERROR: UNSUPPORTED ZEROMQ COMMAND: {:?}", request);
                    json!({ "status": "error", "message": "Unsupported command" })
                }
            };

            let response_data = serde_json::to_vec(&response)
                .map_err(|e| {
                    error!("Failed to serialize ZeroMQ response: {}", e);
                    println!("===> ERROR: FAILED TO SERIALIZE ZEROMQ RESPONSE: {}", e);
                    GraphError::StorageError(format!("Failed to serialize response: {}", e))
                })?;
            debug!("Sending ZeroMQ response: {:?}", response);
            println!("===> SENDING ZEROMQ RESPONSE: {:?}", response);
            if let Err(e) = responder.send(&response_data, 0) {
                warn!("Failed to send ZeroMQ response: {}", e);
                println!("===> WARNING: FAILED TO SEND ZEROMQ RESPONSE: {}", e);
            }
        }

        info!("ZeroMQ server stopped for port {}", self.port);
        println!("===> ZEROMQ SERVER STOPPED FOR PORT {}", self.port);
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    pub async fn close(&self) -> GraphResult<()> {
        info!("Closing SledDaemon at path {:?}", self.db_path);
        println!("===> CLOSING SLED DAEMON AT PATH {:?}", self.db_path);
        let db_flush = self.db.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush Sled DB: {}", e)))?;
        let vertices_flush = self.vertices.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush vertices tree: {}", e)))?;
        let edges_flush = self.edges.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush edges tree: {}", e)))?;
        let kv_pairs_flush = self.kv_pairs.flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush kv_pairs tree: {}", e)))?;
        info!("Flushed SledDaemon at {:?}: db={} bytes, vertices={} bytes, edges={} bytes, kv_pairs={} bytes",
            self.db_path, db_flush, vertices_flush, edges_flush, kv_pairs_flush);
        println!("===> FLUSHED SLED DAEMON AT {:?}: DB={} bytes, VERTICES={} bytes, EDGES={} bytes, KV_PAIRS={} bytes",
            self.db_path, db_flush, vertices_flush, edges_flush, kv_pairs_flush);
        *self.running.lock().await = false;
        Ok(())
    }

    pub fn db_path(&self) -> PathBuf {
        self.db_path.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    #[cfg(feature = "with-openraft-sled")]
    pub async fn is_leader(&self) -> GraphResult<bool> {
        let metrics = self.raft.metrics().await;
        let is_leader = matches!(metrics.raft_state, openraft::RaftState::Leader);
        info!("Checking Raft leader status for node {} at path {:?}", self.node_id, self.db_path);
        println!("===> CHECKING RAFT LEADER STATUS FOR NODE {} AT PATH {:?}", self.node_id, self.db_path);
        Ok(is_leader)
    }

    async fn ensure_write_access(&self) -> GraphResult<()> {
        if !self.is_running().await {
            error!("Daemon at path {:?} is not running", self.db_path);
            println!("===> ERROR: DAEMON AT PATH {:?} IS NOT RUNNING", self.db_path);
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        #[cfg(feature = "with-openraft-sled")]
        {
            if !self.is_leader().await? {
                error!("Node {} at path {:?} is not Raft leader, write access denied", self.node_id, self.db_path);
                println!("===> ERROR: NODE {} AT PATH {:?} IS NOT RAFT LEADER, WRITE ACCESS DENIED", self.node_id, self.db_path);
                return Err(GraphError::StorageError(
                    format!("Node {} at path {:?} is not Raft leader, write access denied", self.node_id, self.db_path)
                ));
            }
        }
        Ok(())
    }

    fn serialize_to_ivec<T: serde::Serialize>(data: &T) -> GraphResult<IVec> {
        let mut cursor = Cursor::new(Vec::new());
        let serialized = serde_json::to_vec(data)
            .map_err(|e| GraphError::StorageError(format!("Serialization failed: {}", e)))?;
        cursor.write_all(&serialized)
            .map_err(|e| GraphError::StorageError(format!("Failed to write to cursor: {}", e)))?;
        let bytes = cursor.into_inner();
        Ok(IVec::from(bytes))
    }

    fn deserialize_from_ivec<T: serde::de::DeserializeOwned>(ivec: IVec) -> GraphResult<T> {
        let mut cursor = Cursor::new(ivec.to_vec());
        let mut bytes = Vec::new();
        cursor
            .read_to_end(&mut bytes)
            .map_err(|e| GraphError::StorageError(format!("Failed to read from cursor: {}", e)))?;
        serde_json::from_slice(&bytes)
            .map_err(|e| GraphError::StorageError(format!("Deserialization failed: {}", e)))
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Inserting key into kv_pairs at path {:?}", self.db_path);
        println!("===> IN INSERT - TRYING TO INSERT KEY {:?}", key);

        timeout(Duration::from_secs(5), async {
            let pre_keys: Vec<_> = self.kv_pairs.iter().keys().filter_map(|k| k.ok()).collect();
            debug!("Keys before insert at {:?}: {:?}", self.db_path, pre_keys);
            println!("===> IN INSERT - KEYS BEFORE INSERT AT {:?}: {:?}", self.db_path, pre_keys);

            let mut batch = Batch::default();
            batch.insert(key, value);
            self.kv_pairs
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch: {}", e)))?;

            let bytes_flushed = self.db.flush_async().await
                .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;
            info!("Flushed {} bytes after insert at {:?}", bytes_flushed, self.db_path);
            println!("===> IN INSERT - FLUSHED {} BYTES", bytes_flushed);

            let persisted = self.kv_pairs
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify insert: {}", e)))?;
            println!("===> IN INSERT - INSERTED KEY {:?} AND VALUE {:?}", key, persisted);
            if persisted.is_none() || persisted.as_ref().map(|v| v.as_ref()) != Some(value) {
                error!("Persistence verification failed for key at {:?}", self.db_path);
                println!("===> IN INSERT - ERROR: PERSISTENCE VERIFICATION FAILED FOR KEY AT {:?}", self.db_path);
                return Err(GraphError::StorageError("Insert not persisted correctly".to_string()));
            }

            let keys: Vec<_> = self.kv_pairs
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current kv_pairs keys at {:?}: {:?}", self.db_path, keys);
            println!("===> IN INSERT - CURRENT KV_PAIRS KEYS AT {:?}: {:?}", self.db_path, keys);

            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft write failed: {}", e)))?;
                info!("Raft write replicated for key at {:?}", self.db_path);
                println!("===> IN INSERT - RAFT WRITE REPLICATED FOR KEY AT {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during insert".to_string()))?
    }

    pub async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        if !self.is_running().await {
            error!("Daemon at path {:?} is not running", self.db_path);
            println!("===> ERROR: DAEMON AT PATH {:?} IS NOT RUNNING", self.db_path);
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        if key == b"test_key" {
            warn!("Retrieving test_key, caller stack trace: {:#?}", std::backtrace::Backtrace::capture());
            println!("===> WARNING: RETRIEVING TEST_KEY, CALLER STACK TRACE: {:#?}", std::backtrace::Backtrace::capture());
        }
        info!("Retrieving key from kv_pairs at path {:?}", self.db_path);
        println!("===> RETRIEVE: RETRIEVING KEY FROM KV_PAIRS AT PATH {:?} AND KEY IS {:?}", self.db_path, key);

        let value = timeout(Duration::from_secs(5), async {
            let pre_keys: Vec<_> = self.kv_pairs.iter().keys().filter_map(|k| k.ok()).collect();
            debug!("Keys before retrieve at {:?}: {:?}", self.db_path, pre_keys);
            println!("===> RETRIEVE: KEYS BEFORE RETRIEVE AT {:?}: {:?}", self.db_path, pre_keys);

            let opt = self.kv_pairs
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to retrieve key: {}", e)))?;
            match opt {
                Some(ivec) => {
                    let mut cursor = Cursor::new(ivec.to_vec());
                    let mut bytes = Vec::new();
                    cursor
                        .read_to_end(&mut bytes)
                        .map_err(|e| GraphError::StorageError(format!("Failed to read from cursor: {}", e)))?;
                    Ok::<Option<Vec<u8>>, GraphError>(Some(bytes))
                }
                None => Ok::<Option<Vec<u8>>, GraphError>(None),
            }
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during retrieve".to_string()))??;

        let keys: Vec<_> = self.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current kv_pairs keys at {:?}: {:?}", self.db_path, keys);
        println!("===> RETRIEVE: KV_PAIRS KEYS AT {:?}: {:?} AND VALUE {:?}", self.db_path, keys, value);
        Ok(value)
    }

    pub async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Deleting key from kv_pairs at path {:?}", self.db_path);
        println!("===> DELETING KEY FROM KV_PAIRS AT PATH {:?}", self.db_path);
        timeout(Duration::from_secs(5), async {
            let pre_keys: Vec<_> = self.kv_pairs.iter().keys().filter_map(|k| k.ok()).collect();
            debug!("Keys before delete at {:?}: {:?}", self.db_path, pre_keys);
            println!("===> DELETE: KEYS BEFORE DELETE AT {:?}: {:?}", self.db_path, pre_keys);

            self.kv_pairs
                .remove(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let keys: Vec<_> = self.kv_pairs
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after delete at {:?}, current kv_pairs keys: {:?}", bytes_flushed, self.db_path, keys);
            println!("===> DELETE: FLUSHED {} BYTES, CURRENT KV_PAIRS KEYS: {:?}", bytes_flushed, keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft delete failed: {}", e)))?;
                info!("Raft delete replicated for key at {:?}", self.db_path);
                println!("===> DELETE: RAFT DELETE REPLICATED FOR KEY AT {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete".to_string()))?
    }

    pub async fn create_vertex(&self, vertex: &Vertex) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = vertex.id.0.as_bytes();
        let value = Self::serialize_to_ivec(vertex)?;
        info!("Creating vertex with id {} at path {:?}", vertex.id, self.db_path);
        println!("===> CREATING VERTEX WITH ID {} AT PATH {:?}", vertex.id, self.db_path);

        timeout(Duration::from_secs(5), async {
            let mut batch = Batch::default();
            batch.insert(key, value);
            self.vertices
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch for vertex: {}", e)))?;

            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;
            info!("Flushed {} bytes after creating vertex at {:?}", bytes_flushed, self.db_path);
            println!("===> CREATE_VERTEX: FLUSHED {} BYTES", bytes_flushed);

            let persisted = self.vertices
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify vertex insert: {}", e)))?;
            if persisted.is_none() {
                error!("Persistence verification failed for vertex id {} at {:?}", vertex.id, self.db_path);
                println!("===> CREATE_VERTEX: ERROR: PERSISTENCE VERIFICATION FAILED FOR VERTEX ID {} AT {:?}", vertex.id, self.db_path);
                return Err(GraphError::StorageError("Vertex insert not persisted".to_string()));
            }

            let vertex_keys: Vec<_> = self.vertices
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current vertices keys at {:?}: {:?}", self.db_path, vertex_keys);
            println!("===> CREATE_VERTEX: CURRENT VERTICES KEYS AT {:?}: {:?}", self.db_path, vertex_keys);

            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vertex.id.0.as_bytes().to_vec(),
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft vertex create failed: {}", e)))?;
                info!("Raft vertex create replicated at {:?}", self.db_path);
                println!("===> CREATE_VERTEX: RAFT VERTEX CREATE REPLICATED AT {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))?
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        if !self.is_running().await {
            error!("Daemon at path {:?} is not running", self.db_path);
            println!("===> ERROR: DAEMON AT PATH {:?} IS NOT RUNNING", self.db_path);
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        let key = id.as_bytes();
        info!("Retrieving vertex with id {} from path {:?}", id, self.db_path);
        println!("===> RETRIEVING VERTEX WITH ID {} FROM PATH {:?}", id, self.db_path);

        let res = timeout(Duration::from_secs(5), async {
            let opt = self.vertices
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to retrieve vertex: {}", e)))?;
            match opt {
                Some(ivec) => {
                    let vertex = Self::deserialize_from_ivec(ivec)?;
                    Ok::<Option<Vertex>, GraphError>(Some(vertex))
                }
                None => Ok::<Option<Vertex>, GraphError>(None),
            }
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))??;

        let vertex_keys: Vec<_> = self.vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current vertices keys at {:?}: {:?}", self.db_path, vertex_keys);
        println!("===> GET_VERTEX: CURRENT VERTICES KEYS AT {:?}: {:?}", self.db_path, vertex_keys);
        Ok(res)
    }

    pub async fn update_vertex(&self, vertex: &Vertex) -> GraphResult<()> {
        self.delete_vertex(&vertex.id.0).await?;
        self.create_vertex(vertex).await
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = id.as_bytes();
        info!("Deleting vertex with id {} from path {:?}", id, self.db_path);
        println!("===> DELETING VERTEX WITH ID {} FROM PATH {:?}", id, self.db_path);
        timeout(Duration::from_secs(5), async {
            self.vertices
                .remove(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            let mut batch = sled::Batch::default();
            let prefix = id.as_bytes();
            for item in self.edges.iter().keys() {
                let k = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                if k.starts_with(prefix) {
                    batch.remove(k);
                }
            }
            self.edges
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertex_keys: Vec<_> = self.vertices
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            let edge_keys: Vec<_> = self.edges
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after deleting vertex at {:?}, current vertices keys: {:?}, edges keys: {:?}", bytes_flushed, self.db_path, vertex_keys, edge_keys);
            println!("===> DELETE_VERTEX: FLUSHED {} BYTES, CURRENT VERTICES KEYS: {:?}, EDGES KEYS: {:?}", bytes_flushed, vertex_keys, edge_keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft vertex delete failed: {}", e)))?;
                info!("Raft vertex delete replicated at {:?}", self.db_path);
                println!("===> DELETE_VERTEX: RAFT VERTEX DELETE REPLICATED AT {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete_vertex".to_string()))?
    }

    pub async fn create_edge(&self, edge: &Edge) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into())?;
        let value = Self::serialize_to_ivec(edge)?;
        info!("Creating edge ({}, {}, {}) at path {:?}", edge.outbound_id, edge.t, edge.inbound_id, self.db_path);
        println!("===> CREATING EDGE ({}, {}, {}) AT PATH {:?}", edge.outbound_id, edge.t, edge.inbound_id, self.db_path);

        timeout(Duration::from_secs(5), async {
            let mut batch = Batch::default();
            batch.insert(&*key, value);
            self.edges
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch for edge: {}", e)))?;

            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;
            info!("Flushed {} bytes after creating edge at {:?}", bytes_flushed, self.db_path);
            println!("===> CREATE_EDGE: FLUSHED {} BYTES", bytes_flushed);

            let persisted = self.edges
                .get(&key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify edge insert: {}", e)))?;
            if persisted.is_none() {
                error!("Persistence verification failed for edge ({}, {}, {}) at {:?}", 
                    edge.outbound_id, edge.t, edge.inbound_id, self.db_path);
                println!("===> CREATE_EDGE: ERROR: PERSISTENCE VERIFICATION FAILED FOR EDGE ({}, {}, {}) AT {:?}", 
                    edge.outbound_id, edge.t, edge.inbound_id, self.db_path);
                return Err(GraphError::StorageError("Edge insert not persisted".to_string()));
            }

            let edge_keys: Vec<_> = self.edges
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current edges keys at {:?}: {:?}", self.db_path, edge_keys);
            println!("===> CREATE_EDGE: CURRENT EDGES KEYS AT {:?}: {:?}", self.db_path, edge_keys);

            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: edge.t.to_string().into_bytes(),
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft edge create failed: {}", e)))?;
                info!("Raft edge create replicated at {:?}", self.db_path);
                println!("===> CREATE_EDGE: RAFT EDGE CREATE REPLICATED AT {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during create_edge".to_string()))?
    }

    pub async fn get_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
    ) -> GraphResult<Option<Edge>> {
        if !self.is_running().await {
            error!("Daemon at path {:?} is not running", self.db_path);
            println!("===> ERROR: DAEMON AT PATH {:?} IS NOT RUNNING", self.db_path);
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        info!("Retrieving edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, self.db_path);
        println!("===> RETRIEVING EDGE ({}, {}, {}) FROM PATH {:?}", outbound_id, edge_type, inbound_id, self.db_path);

        let res = timeout(Duration::from_secs(5), async {
            let opt = self.edges
                .get(&key)
                .map_err(|e| GraphError::StorageError(format!("Failed to retrieve edge: {}", e)))?;
            match opt {
                Some(ivec) => {
                    let edge = Self::deserialize_from_ivec(ivec)?;
                    Ok::<Option<Edge>, GraphError>(Some(edge))
                }
                None => Ok::<Option<Edge>, GraphError>(None),
            }
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))??;

        let edge_keys: Vec<_> = self.edges
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current edges keys at {:?}: {:?}", self.db_path, edge_keys);
        println!("===> GET_EDGE: CURRENT EDGES KEYS AT {:?}: {:?}", self.db_path, edge_keys);
        Ok(res)
    }

    pub async fn update_edge(&self, edge: &Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    pub async fn delete_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
    ) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        info!("Deleting edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, self.db_path);
        println!("===> DELETING EDGE ({}, {}, {}) FROM PATH {:?}", outbound_id, edge_type, inbound_id, self.db_path);
        timeout(Duration::from_secs(5), async {
            self.edges
                .remove(key)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge_keys: Vec<_> = self.edges
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after deleting edge at {:?}, current edges keys: {:?}", bytes_flushed, self.db_path, edge_keys);
            println!("===> DELETE_EDGE: FLUSHED {} BYTES, CURRENT EDGES KEYS: {:?}", bytes_flushed, edge_keys);
            #[cfg(feature = "with-openraft-sled")]
            {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                self.raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft edge delete failed: {}", e)))?;
                info!("Raft edge delete replicated at {:?}", self.db_path);
                println!("===> DELETE_EDGE: RAFT EDGE DELETE REPLICATED AT {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))?
    }

    pub async fn force_reset(&self) -> GraphResult<()> {
        info!("Resetting SledDaemon at path {:?}", self.db_path);
        println!("===> RESETTING SLED DAEMON AT PATH {:?}", self.db_path);
        timeout(Duration::from_secs(5), async {
            self.db
                .clear()
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            info!("Flushed {} bytes after resetting daemon at {:?}", bytes_flushed, self.db_path);
            println!("===> FORCE_RESET: FLUSHED {} BYTES", bytes_flushed);
            #[cfg(feature = "with-openraft-sled")]
            {
                self.raft_storage
                    .reset()
                    .await
                    .map_err(|e| GraphError::StorageError(format!("Raft reset failed: {}", e)))?;
                info!("Raft storage reset at {:?}", self.db_path);
                println!("===> FORCE_RESET: RAFT STORAGE RESET AT {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during force_reset".to_string()))?
    }

    pub async fn force_unlock(&self) -> GraphResult<()> {
        Ok(())
    }

    pub async fn force_unlock_path(_path: &Path) -> GraphResult<()> {
        Ok(())
    }
}

impl SledDaemonPool {
    pub fn new() -> Self {
        Self {
            daemons: HashMap::new(),
            registry: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn add_daemon(
        &mut self,
        storage_config: &StorageConfig,
        port: u16,
        config: &SledConfig,
    ) -> GraphResult<()> {
        let mut system = System::new_all();
        system.refresh_all();
        let port_in_use = system.processes().values().any(|proc| {
            proc.cmd()
                .iter()
                .any(|arg| arg.to_string_lossy().contains(&port.to_string()))
                && proc.name().to_string_lossy().contains("graphdb")
        });
        if port_in_use {
            error!("Port {} is already in use by another process", port);
            println!("===> ERROR: PORT {} IS ALREADY IN USE BY ANOTHER PROCESS", port);
            return Err(GraphError::StorageError(format!(
                "Port {} is already in use",
                port
            )));
        }

        if self.daemons.contains_key(&port) {
            error!("Daemon already exists on port {}", port);
            println!("===> ERROR: DAEMON ALREADY EXISTS ON PORT {}", port);
            return Err(GraphError::StorageError(format!(
                "Daemon already exists on port {}",
                port
            )));
        }

        if GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?.is_some() {
            warn!(
                "Storage process already registered on port {}. Skipping cleanup.",
                port
            );
            println!(
                "===> WARNING: STORAGE PROCESS ALREADY REGISTERED ON PORT {}. SKIPPING CLEANUP.",
                port
            );
            return Err(GraphError::StorageError(format!(
                "Daemon already registered on port {}",
                port
            )));
        }

        let mut port_config = config.clone();
        port_config.port = Some(port);
        println!("===> ADDING DAEMON ON PORT {}", port);

        let daemon = Arc::new(SledDaemon::new(&port_config).await?);
        self.daemons.insert(port, daemon.clone());

        let metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(port_config.path.clone()),
            config_path: storage_config
                .config_root_directory
                .as_ref()
                .map(|p| p.join("storage_config.yaml")), // Fixed: Handle Option
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        };
        self.registry.write().await.insert(port, metadata.clone());
        GLOBAL_DAEMON_REGISTRY
            .register_daemon(metadata)
            .await
            .map_err(|e| {
                GraphError::StorageError(format!(
                    "Failed to register daemon in GLOBAL_DAEMON_REGISTRY: {}",
                    e
                ))
            })?;
        info!("Added daemon for port {} at path {:?}", port, port_config.path);
        println!(
            "===> ADDED DAEMON FOR PORT {} AT PATH {:?}",
            port, port_config.path
        );
        Ok(())
    }

    pub async fn initialize_cluster(
        &mut self,
        storage_config: &StorageConfig,
        config: &SledConfig,
        cli_port: Option<u16>,
    ) -> GraphResult<()> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!(
                "SledDaemonPool already initialized, skipping. Caller stack trace: {:#?}",
                std::backtrace::Backtrace::capture()
            );
            println!(
                "===> WARNING: SLED DAEMON POOL ALREADY INITIALIZED, SKIPPING. CALLER STACK TRACE: {:#?}",
                std::backtrace::Backtrace::capture()
            );
            return Ok(());
        }

        debug!(
            "Initializing cluster with use_raft_for_scale: {}",
            storage_config.use_raft_for_scale
        );
        println!(
            "===> INITIALIZING CLUSTER WITH USE_RAFT_FOR_SCALE: {}",
            storage_config.use_raft_for_scale
        );

        let registry_path = Path::new(DAEMON_REGISTRY_DB_PATH);
        if let Some(parent) = registry_path.parent() {
            debug!("Ensuring daemon registry directory exists: {:?}", parent);
            println!("===> ENSURING DAEMON REGISTRY DIRECTORY EXISTS: {:?}", parent);
            fs::create_dir_all(parent).await.map_err(|e| {
                error!(
                    "Failed to create daemon registry directory {:?}: {}",
                    parent, e
                );
                println!(
                    "===> ERROR: FAILED TO CREATE DAEMON REGISTRY DIRECTORY {:?}: {}",
                    parent, e
                );
                GraphError::Io(e)
            })?;
        }

        let port = cli_port.unwrap_or(8051);
        println!("===> INITIALIZING CLUSTER ON PORT {}", port);
        if storage_config.use_raft_for_scale {
            warn!(
                "Raft clustering is enabled, but only a single daemon will be initialized on port {}",
                port
            );
            println!(
                "===> WARNING: RAFT CLUSTERING ENABLED, BUT ONLY SINGLE DAEMON INITIALIZED ON PORT {}",
                port
            );
        }

        let mut system = System::new_all();
        system.refresh_all();
        let port_in_use = system.processes().values().any(|proc| {
            proc.cmd()
                .iter()
                .any(|arg| arg.to_string_lossy().contains(&port.to_string()))
                && proc.name().to_string_lossy().contains("graphdb")
        });
        if port_in_use {
            error!("Port {} is already in use by another process", port);
            println!("===> ERROR: PORT {} IS ALREADY IN USE BY ANOTHER PROCESS", port);
            return Err(GraphError::StorageError(format!(
                "Port {} is already in use",
                port
            )));
        }

        if GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await?.is_some() {
            warn!(
                "Storage process already registered on port {}. Skipping initialization.",
                port
            );
            println!(
                "===> WARNING: STORAGE PROCESS ALREADY REGISTERED ON PORT {}. SKIPPING INITIALIZATION.",
                port
            );
            return Err(GraphError::StorageError(format!(
                "Daemon already registered on port {}",
                port
            )));
        }

        info!("Initializing single daemon for port {}", port);
        println!("===> INITIALIZING SINGLE DAEMON FOR PORT {}", port);
        let mut port_config = config.clone();
        port_config.port = Some(port);
        let daemon = Arc::new(SledDaemon::new(&port_config).await?);
        self.daemons.insert(port, daemon.clone());
        let metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(port_config.path.clone()),
            config_path: storage_config
                .config_root_directory
                .as_ref()
                .map(|p| p.join("storage_config.yaml")), // Fixed: Handle Option
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        };
        self.registry.write().await.insert(port, metadata.clone());
        GLOBAL_DAEMON_REGISTRY
            .register_daemon(metadata)
            .await
            .map_err(|e| {
                error!("Failed to register daemon on port {}: {}", port, e);
                println!("===> ERROR: FAILED TO REGISTER DAEMON ON PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
            })?;

        #[cfg(feature = "with-openraft-sled")]
        {
            let node_id = port as u64;
            let raft = &daemon.raft;
            let initial_nodes = HashMap::from([(
                node_id,
                BasicNode {
                    addr: format!("127.0.0.1:{}", port),
                },
            )]);
            raft.initialize(initial_nodes).await.map_err(|e| {
                error!("Failed to initialize Raft for node {}: {}", node_id, e);
                println!(
                    "===> ERROR: FAILED TO INITIALIZE RAFT FOR NODE {}: {}",
                    node_id, e
                );
                GraphError::StorageError(format!("Failed to initialize Raft for node {}: {}", node_id, e))
            })?;
            info!("Initialized Raft for node {} on port {}", node_id, port);
            println!(
                "===> INITIALIZED RAFT FOR NODE {} ON PORT {}",
                node_id, port
            );
        }

        *initialized = true;
        info!("Successfully initialized daemon on port {}", port);
        println!("===> SUCCESSFULLY INITIALIZED DAEMON ON PORT {}", port);
        Ok(())
    }

    pub async fn any_daemon(&self) -> GraphResult<Arc<SledDaemon>> {
        if let Some(daemon) = self.daemons.values().next() {
            info!("Selected daemon on port {} at path {:?}", daemon.port(), daemon.db_path());
            println!("===> SELECTED DAEMON ON PORT {} AT PATH {:?}", daemon.port(), daemon.db_path());
            Ok(Arc::clone(daemon))
        } else {
            error!("No daemons available in the pool");
            println!("===> ERROR: NO DAEMONS AVAILABLE IN THE POOL");
            Err(GraphError::StorageError("No daemons available".to_string()))
        }
    }

    pub async fn close(&self, _port: Option<u16>) -> GraphResult<()> {
        info!("Closing all daemons in SledDaemonPool");
        println!("===> CLOSING ALL DAEMONS IN SLED DAEMON POOL");
        let futures = self.daemons.values().map(|daemon| async {
            let db_path = daemon.db_path();
            match timeout(Duration::from_secs(10), daemon.close()).await {
                Ok(Ok(())) => {
                    info!("Closed daemon at {:?}", db_path);
                    println!("===> CLOSED DAEMON AT {:?}", db_path);
                    Ok(())
                }
                Ok(Err(e)) => {
                    error!("Failed to close daemon at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO CLOSE DAEMON AT {:?}: {}", db_path, e);
                    Err(GraphError::StorageError(e.to_string()))
                }
                Err(_) => {
                    error!("Timeout closing daemon at {:?}", db_path);
                    println!("===> ERROR: TIMEOUT CLOSING DAEMON AT {:?}", db_path);
                    Err(GraphError::StorageError(format!("Timeout closing daemon at {:?}", db_path)))
                }
            }
        });
        let results = join_all(futures).await;
        let errors: Vec<_> = results.into_iter().filter_map(|r| r.err()).collect();
        if !errors.is_empty() {
            error!("Errors during close: {:?}", errors);
            println!("===> ERROR: ERRORS DURING CLOSE: {:?}", errors);
            return Err(GraphError::StorageError(format!("Close errors: {:?}", errors)));
        }
        info!("Successfully closed all daemons");
        println!("===> SUCCESSFULLY CLOSED ALL DAEMONS");
        Ok(())
    }

    pub async fn leader_daemon(&self) -> GraphResult<Arc<SledDaemon>> {
        for daemon in self.daemons.values() {
            #[cfg(feature = "with-openraft-sled")]
            {
                if daemon.is_leader().await? {
                    info!("Selected leader daemon on port {} at path {:?}", daemon.port(), daemon.db_path());
                    println!("===> SELECTED LEADER DAEMON ON PORT {} AT PATH {:?}", daemon.port(), daemon.db_path());
                    return Ok(Arc::clone(daemon));
                }
            }
            #[cfg(not(feature = "with-openraft-sled"))]
            {
                info!("Selected daemon on port {} at path {:?}", daemon.port(), daemon.db_path());
                println!("===> SELECTED DAEMON ON PORT {} AT PATH {:?}", daemon.port(), daemon.db_path());
                return Ok(Arc::clone(daemon));
            }
        }
        error!("No leader daemon found in the pool");
        println!("===> ERROR: NO LEADER DAEMON FOUND IN THE POOL");
        Err(GraphError::StorageError("No leader daemon found".to_string()))
    }

    pub async fn create_edge(&self, edge: Edge, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.create_edge(&edge)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_edge".to_string()))?
    }

    pub async fn get_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
        _use_raft: bool,
    ) -> GraphResult<Option<Edge>> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.get_edge(outbound_id, edge_type, inbound_id)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))?
    }

    pub async fn update_edge(&self, edge: Edge, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.update_edge(&edge)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_edge".to_string()))?
    }

    pub async fn delete_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
        _use_raft: bool,
    ) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.delete_edge(outbound_id, edge_type, inbound_id)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))?
    }

    pub async fn create_vertex(&self, vertex: Vertex, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.create_vertex(&vertex)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))?
    }

    pub async fn get_vertex(&self, id: &Uuid, _use_raft: bool) -> GraphResult<Option<Vertex>> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.get_vertex(id)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))?
    }

    pub async fn update_vertex(&self, vertex: Vertex, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.update_vertex(&vertex)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_vertex".to_string()))?
    }

    pub async fn delete_vertex(&self, id: &Uuid, _use_raft: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(Duration::from_secs(5), daemon.delete_vertex(id)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete_vertex".to_string()))?
    }
}

#[cfg(feature = "with-openraft-sled")]
#[async_trait]
impl RaftNetwork<NodeId, BasicNode> for RaftTcpNetwork {
    async fn send_append_entries(
        &self,
        target: NodeId,
        rpc: openraft::raft::AppendEntriesRequest<BasicNode>,
    ) -> Result<openraft::raft::AppendEntriesResponse, openraft::error::RPCError<NodeId, BasicNode>> {
        const MAX_RETRIES: u32 = 3;
        const BASE_DELAY_MS: u64 = 100;
        let addr = format!("127.0.0.1:{}", target);
        let mut attempt = 0;

        loop {
            match timeout(Duration::from_secs(2), async {
                let mut stream = TcpStream::connect(&addr).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let request_data = serde_json::to_vec(&rpc)
                    .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                        error: openraft::error::ClientError::new(&e, target, &addr),
                    })?;
                stream.write_all(&request_data).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                stream.flush().await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let mut buffer = Vec::new();
                stream.read_to_end(&mut buffer).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let response: openraft::raft::AppendEntriesResponse = serde_json::from_slice(&buffer)
                    .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                        error: openraft::error::ClientError::new(&e, target, &addr),
                    })?;
                Ok(response)
            })
            .await
            {
                Ok(Ok(response)) => {
                    println!("===> RAFT: SENT APPEND ENTRIES TO {} SUCCESSFULLY", addr);
                    return Ok(response);
                }
                Ok(Err(e)) if attempt < MAX_RETRIES => {
                    warn!("Failed to send append entries to {} on attempt {}: {}. Retrying.", addr, attempt + 1, e);
                    println!("===> RAFT: FAILED TO SEND APPEND ENTRIES TO {} ON ATTEMPT {}: {}. RETRYING.", addr, attempt + 1, e);
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                    continue;
                }
                Ok(Err(e)) => {
                    error!("Failed to send append entries to {} after {} attempts: {}", addr, attempt + 1, e);
                    println!("===> RAFT: FAILED TO SEND APPEND ENTRIES TO {} AFTER {} ATTEMPTS: {}", addr, attempt + 1, e);
                    return Err(e);
                }
                Err(_) => {
                    warn!("Timeout sending append entries to {} on attempt {}. Retrying.", addr, attempt + 1);
                    println!("===> RAFT: TIMEOUT SENDING APPEND ENTRIES TO {} ON ATTEMPT {}. RETRYING.", addr, attempt + 1);
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        error!("Timeout sending append entries to {} after {} attempts.", addr, MAX_RETRIES);
                        println!("===> RAFT: TIMEOUT SENDING APPEND ENTRIES TO {} AFTER {} ATTEMPTS.", addr, MAX_RETRIES);
                        return Err(openraft::error::RPCError::Network {
                            error: openraft::error::NetworkError::new(&std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Timeout sending append entries",
                            )),
                            target,
                            node: BasicNode { addr },
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                }
            }
        }
    }

    async fn send_install_snapshot(
        &self,
        target: NodeId,
        rpc: openraft::raft::InstallSnapshotRequest<BasicNode>,
    ) -> Result<openraft::raft::InstallSnapshotResponse, openraft::error::RPCError<NodeId, BasicNode>> {
        const MAX_RETRIES: u32 = 3;
        const BASE_DELAY_MS: u64 = 100;
        let addr = format!("127.0.0.1:{}", target);
        let mut attempt = 0;

        loop {
            match timeout(Duration::from_secs(2), async {
                let mut stream = TcpStream::connect(&addr).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let request_data = serde_json::to_vec(&rpc)
                    .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                        error: openraft::error::ClientError::new(&e, target, &addr),
                    })?;
                stream.write_all(&request_data).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                stream.flush().await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let mut buffer = Vec::new();
                stream.read_to_end(&mut buffer).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let response: openraft::raft::InstallSnapshotResponse = serde_json::from_slice(&buffer)
                    .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                        error: openraft::error::ClientError::new(&e, target, &addr),
                    })?;
                Ok(response)
            })
            .await
            {
                Ok(Ok(response)) => {
                    println!("===> RAFT: SENT INSTALL SNAPSHOT TO {} SUCCESSFULLY", addr);
                    return Ok(response);
                }
                Ok(Err(e)) if attempt < MAX_RETRIES => {
                    warn!("Failed to send install snapshot to {} on attempt {}: {}. Retrying.", addr, attempt + 1, e);
                    println!("===> RAFT: FAILED TO SEND INSTALL SNAPSHOT TO {} ON ATTEMPT {}: {}. RETRYING.", addr, attempt + 1, e);
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                    continue;
                }
                Ok(Err(e)) => {
                    error!("Failed to send install snapshot to {} after {} attempts: {}", addr, attempt + 1, e);
                    println!("===> RAFT: FAILED TO SEND INSTALL SNAPSHOT TO {} AFTER {} ATTEMPTS: {}", addr, attempt + 1, e);
                    return Err(e);
                }
                Err(_) => {
                    warn!("Timeout sending install snapshot to {} on attempt {}. Retrying.", addr, attempt + 1);
                    println!("===> RAFT: TIMEOUT SENDING INSTALL SNAPSHOT TO {} ON ATTEMPT {}. RETRYING.", addr, attempt + 1);
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        error!("Timeout sending install snapshot to {} after {} attempts.", addr, MAX_RETRIES);
                        println!("===> RAFT: TIMEOUT SENDING INSTALL SNAPSHOT TO {} AFTER {} ATTEMPTS.", addr, MAX_RETRIES);
                        return Err(openraft::error::RPCError::Network {
                            error: openraft::error::NetworkError::new(&std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Timeout sending install snapshot",
                            )),
                            target,
                            node: BasicNode { addr },
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                }
            }
        }
    }

    async fn send_vote(
        &self,
        target: NodeId,
        rpc: openraft::raft::VoteRequest<NodeId>,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, openraft::error::RPCError<NodeId, BasicNode>> {
        const MAX_RETRIES: u32 = 3;
        const BASE_DELAY_MS: u64 = 100;
        let addr = format!("127.0.0.1:{}", target);
        debug!("Sending vote to node {} at {}", target, addr);
        let mut attempt = 0;

        loop {
            match timeout(Duration::from_secs(2), async {
                let mut stream = TcpStream::connect(&addr).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let request_data = serde_json::to_vec(&rpc)
                    .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                        error: openraft::error::ClientError::new(&e, target, &addr),
                    })?;
                stream.write_all(&request_data).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                stream.flush().await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let mut buffer = Vec::new();
                stream.read_to_end(&mut buffer).await
                    .map_err(|e| openraft::error::RPCError::Network {
                        error: openraft::error::NetworkError::new(&e),
                        target,
                        node: BasicNode { addr: addr.clone() },
                    })?;
                let response: openraft::raft::VoteResponse<NodeId> = serde_json::from_slice(&buffer)
                    .map_err(|e| openraft::error::RPCError::PayloadTooLarge {
                        error: openraft::error::ClientError::new(&e, target, &addr),
                    })?;
                Ok(response)
            })
            .await
            {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(e)) if attempt < MAX_RETRIES => {
                    warn!("Failed to send vote to {} on attempt {}: {}. Retrying.", addr, attempt + 1, e);
                    attempt += 1;
                    tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                    continue;
                }
                Ok(Err(e)) => {
                    error!("Failed to send vote to {} after {} attempts: {}", addr, attempt + 1, e);
                    return Err(e);
                }
                Err(_) => {
                    warn!("Timeout sending vote to {} on attempt {}. Retrying.", addr, attempt + 1);
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        error!("Timeout sending vote to {} after {} attempts.", addr, MAX_RETRIES);
                        return Err(openraft::error::RPCError::Network {
                            error: openraft::error::NetworkError::new(&std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Timeout sending vote",
                            )),
                            target,
                            node: BasicNode { addr },
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                }
            }
        }
    }
}

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io::{Cursor, Read, Write};
use rocksdb::{DB, Cache, Options, WriteBatch, WriteOptions, ColumnFamilyDescriptor, DBCompressionType, DBCompactionStyle};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as TokioMutex, RwLock, mpsc};
use tokio::time::{sleep, Duration as TokioDuration, timeout, interval};
use tokio::task::JoinHandle;
use tokio::fs;
use log::{info, debug, warn, error};
use std::sync::atomic::{AtomicBool, Ordering};
pub use crate::config::{
    RocksDBConfig, RocksDBDaemon, RocksDBDaemonPool, StorageConfig, StorageEngineType,
    DAEMON_REGISTRY_DB_PATH, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB,
    ReplicationStrategy, NodeHealth, LoadBalancer, HealthCheckConfig, RaftTcpNetwork,
    RocksDBRaftStorage, RocksDBClient, TypeConfig,
};
use crate::storage_engine::storage_engine::{ ApplicationStateMachine as RocksDBStateMachine};
use crate::storage_engine::storage_utils::{create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::{json, Value};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::daemon_management::{parse_cluster_range, find_pid_by_port, 
                                       is_storage_daemon_running, stop_process_by_pid};
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;
use uuid::Uuid;
use zmq::{Context as ZmqContext, Socket as ZmqSocket, Error as ZmqError};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use async_trait::async_trait;
#[cfg(feature = "with-rocksdb")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode},
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};


const ZMQ_EAGAIN_SENTINEL: &str = "ZMQ_EAGAIN_SENTINEL"; 

impl RocksDBDaemon {
    /// Creates a new RocksDBDaemon with a fresh database

    pub async fn new(config: RocksDBConfig) -> GraphResult<Self> {
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        info!("===> Initializing RocksDB daemon on port {} (PID: {})", port, std::process::id());
        println!("===> Initializing RocksDB daemon on port {} (PID: {})", port, std::process::id());

        let db_path = if config.path.ends_with(&port.to_string()) {
            config.path.clone()
        } else {
            config.path.join(port.to_string())
        };
        
        if !db_path.exists() {
            info!("Creating DB path: {}", db_path.display());
            fs::create_dir_all(&db_path)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create DB path {}: {}", db_path.display(), e)))?;
        }
        
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        if let Some(cache_capacity) = config.cache_capacity {
            let cache = Cache::new_lru_cache(cache_capacity.try_into().unwrap());
            db_opts.set_row_cache(&cache);
        }
        db_opts.set_compaction_style(DBCompactionStyle::Level);
        db_opts.set_compression_type(DBCompressionType::Snappy);

        let db = Arc::new(DB::open(&db_opts, &db_path).map_err(|e| {
            error!("Failed to open RocksDB at {}: {}", db_path.display(), e);
            GraphError::StorageError(format!("Failed to open RocksDB at {}: {}", db_path.display(), e))
        })?);

        let zmq_context = Arc::new(ZmqContext::new());
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);

        let daemon = Self {
            port,
            db_path: db_path.clone(),
            db,
            running: Arc::new(TokioMutex::new(true)),
            shutdown_tx,
            zmq_context,
            zmq_thread: Arc::new(TokioMutex::new(None)),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id: None,
        };

        // Run ZeroMQ server in a dedicated OS thread
        let daemon_for_zmq = daemon.clone();
        let zmq_thread_ref = daemon.zmq_thread.clone();  // Clone the Arc from daemon
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                if let Err(e) = daemon_for_zmq.run_zmq_server(shutdown_rx).await {
                    error!("ZMQ server failed for port {}: {}", daemon_for_zmq.port, e);
                }
            });
        });

        // Store the thread handle
        *zmq_thread_ref.lock().await = Some(handle);

        // Register SIGTERM handler
        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            let shutdown_tx = daemon.shutdown_tx.clone();
            tokio::spawn(async move {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                info!("===> Received SIGTERM in RocksDB daemon on port {} (PID: {})", port, std::process::id());
                println!("===> Received SIGTERM in RocksDB daemon on port {} (PID: {})", port, std::process::id());
                let should_shutdown = {
                    let mut running = daemon_for_signal.running.lock().await;
                    if *running {
                        *running = false;
                        let _ = shutdown_tx.send(()).await;
                        true
                    } else {
                        false
                    }
                };
                if should_shutdown {
                    if let Err(e) = daemon_for_signal.shutdown().await {
                        error!("===> Failed to shutdown RocksDB daemon on port {}: {}", port, e);
                    } else {
                        info!("===> RocksDB daemon shutdown complete on port {}", port);
                        println!("===> RocksDB daemon shutdown complete on port {}", port);
                    }
                } else {
                    info!("===> Ignoring SIGTERM as daemon on port {} is not fully initialized", port);
                }
            });
        }

        GLOBAL_DAEMON_REGISTRY
            .register_daemon(DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: std::process::id(),
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(db_path.clone()),
                config_path: Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB)),
                engine_type: Some("RocksDB".to_string()),
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0),
            })
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to add daemon to registry: {}", e)))?;

        info!("===> RocksDB daemon initialized on port {} (PID: {})", port, std::process::id());
        println!("===> RocksDB daemon initialized on port {} (PID: {})", port, std::process::id());
        Ok(daemon)
    }

    /// Creates a new RocksDBDaemon with an existing database
    pub async fn new_with_db(config: RocksDBConfig, existing_db: Arc<DB>) -> GraphResult<Self> {
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        info!("===> Initializing RocksDB daemon with existing DB on port {} (PID: {})", port, std::process::id());
        println!("===> Initializing RocksDB daemon with existing DB on port {} (PID: {})", port, std::process::id());

        // FIX: Replaced `unwrap_or_else` (which is for Option<T>) with an explicit if/else
        // block. This handles `config.path` being a concrete PathBuf by checking if its
        // OsString representation is empty, and provides a default path if so.
        let db_path = if config.path.as_os_str().is_empty() {
            // Use default path if the configured path is empty
            PathBuf::from(format!("/opt/graphdb/storage_data/rocksdb/{}", port))
        } else {
            // Otherwise, use the configured path
            config.path
        };

        if !db_path.exists() {
            info!("Creating DB path: {}", db_path.display());
            fs::create_dir_all(&db_path)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create DB path {}: {}", db_path.display(), e)))?;
        }

        let zmq_context = Arc::new(ZmqContext::new());
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        let zmq_thread = Arc::new(TokioMutex::new(None::<JoinHandle<()>>));

        let zmq_context = Arc::new(ZmqContext::new());
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let daemon = Self {
            port,
            db_path: db_path.clone(),
            db: existing_db,
            running: Arc::new(TokioMutex::new(true)),
            shutdown_tx,
            zmq_context,
            zmq_thread: Arc::new(TokioMutex::new(None)),  // Initialize inline, don't clone
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id: None,
        };

        // Run ZeroMQ server in a dedicated OS thread (not Tokio task)
        let daemon_clone = daemon.clone();
        let zmq_thread = daemon.zmq_thread.clone();  // Clone it here instead
        let handle = std::thread::spawn(move || {
            // Create a new Tokio runtime for this thread
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                if let Err(e) = daemon_clone.run_zmq_server(shutdown_rx).await {
                    error!("ZMQ server failed for port {}: {}", daemon_clone.port, e);
                }
            });
        });

        // Store the thread handle
        *zmq_thread.lock().await = Some(handle);

        // Register SIGTERM handler
        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            let shutdown_tx = daemon.shutdown_tx.clone();
            tokio::spawn(async move {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                info!("===> Received SIGTERM in RocksDB daemon on port {} (PID: {})", port, std::process::id());
                println!("===> Received SIGTERM in RocksDB daemon on port {} (PID: {})", port, std::process::id());
                let should_shutdown = {
                    let mut running = daemon_for_signal.running.lock().await;
                    if *running {
                        *running = false;
                        let _ = shutdown_tx.send(()).await;
                        true
                    } else {
                        false
                    }
                };
                if should_shutdown {
                    if let Err(e) = daemon_for_signal.shutdown().await {
                        error!("===> Failed to shutdown RocksDB daemon on port {}: {}", port, e);
                    } else {
                        info!("===> RocksDB daemon shutdown complete on port {}", port);
                        println!("===> RocksDB daemon shutdown complete on port {}", port);
                    }
                } else {
                    info!("===> Ignoring SIGTERM as daemon on port {} is not fully initialized", port);
                }
            });
        }

        GLOBAL_DAEMON_REGISTRY
            .register_daemon(DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: std::process::id(),
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(db_path.clone()),
                config_path: Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB)),
                engine_type: Some("RocksDB".to_string()),
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0),
            })
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to add daemon to registry: {}", e)))?;

        info!("===> RocksDB daemon with existing DB initialized on port {} (PID: {})", port, std::process::id());
        println!("===> RocksDB daemon with existing DB initialized on port {} (PID: {})", port, std::process::id());
        Ok(daemon)
    }

    /// Runs the ZeroMQ server, inspired by sled_storage_daemon_pool.rs
    async fn run_zmq_server(&self, mut shutdown_rx: mpsc::Receiver<()>) -> GraphResult<()> {
        const SOCKET_TIMEOUT_MS: i32 = 1000;
        const MAX_MESSAGE_SIZE: i32 = 1024 * 1024;

        info!("===> STARTING ZMQ SERVER FOR PORT {}", self.port);
        println!("===> STARTING ZMQ SERVER FOR PORT {}", self.port);

        let context = ZmqContext::new();
        let responder = context.socket(zmq::REP)
            .map_err(|e| {
                error!("Failed to create ZeroMQ socket for port {}: {}", self.port, e);
                GraphError::StorageError(format!("Failed to create ZeroMQ socket for port {}: {}", self.port, e))
            })?;

        responder.set_linger(1000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set socket linger for port {}: {}", self.port, e)))?;
        responder.set_rcvtimeo(SOCKET_TIMEOUT_MS)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout for port {}: {}", self.port, e)))?;
        responder.set_sndtimeo(SOCKET_TIMEOUT_MS)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout for port {}: {}", self.port, e)))?;
        responder.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)
            .map_err(|e| GraphError::StorageError(format!("Failed to set max message size for port {}: {}", self.port, e)))?;

        let socket_path = format!("/tmp/graphdb-{}.ipc", self.port);
        let socket_dir = Path::new("/tmp");

        if !socket_dir.exists() {
            info!("Creating {} directory for IPC socket", socket_dir.display());
            tokio::fs::create_dir_all(socket_dir).await
                .map_err(|e| GraphError::StorageError(format!("Failed to create {} directory: {}", socket_dir.display(), e)))?;
            #[cfg(unix)]
            tokio::fs::set_permissions(socket_dir, std::fs::Permissions::from_mode(0o755))
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to set permissions on {} directory: {}", socket_dir.display(), e)))?;
        }

        let endpoint = format!("ipc://{}", socket_path);
        info!("Attempting to bind ZeroMQ socket to {} for port {}", endpoint, self.port);
        let bind_result = responder.bind(&endpoint);
        if let Err(e) = bind_result {
            error!("Failed to bind ZeroMQ socket to {} for port {}: {}", endpoint, self.port, e);
            if tokio::fs::metadata(&socket_path).await.is_ok() {
                tokio::fs::remove_file(&socket_path).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to remove IPC socket {} after bind failure: {}", socket_path, e)))?;
            }
            return Err(GraphError::StorageError(format!("Failed to bind ZeroMQ socket on port {}: {}", self.port, e)));
        }

        #[cfg(unix)]
        tokio::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o666))
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to set permissions on IPC socket {} for port {}: {}", socket_path, self.port, e)))?;

        info!("ZeroMQ server started on {} for port {}", endpoint, self.port);
        println!("===> ZEROMQ SERVER STARTED ON {} FOR PORT {}", endpoint, self.port);

        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        loop {
            tokio::select! {
                // Prioritize shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Received shutdown signal for ZeroMQ server on port {}", self.port);
                    println!("===> RECEIVED SHUTDOWN SIGNAL FOR ZEROMQ SERVER ON PORT {}", self.port);
                    break;
                }
                // Process incoming messages with timeout
                result = timeout(TokioDuration::from_millis(100), async {
                    self.process_zmq_message(&responder, &mut consecutive_errors).await
                }) => {
                    match result {
                        Ok(Ok(continue_running)) => {
                            if !continue_running {
                                info!("Too many consecutive errors, stopping ZeroMQ server on port {}", self.port);
                                println!("===> TOO MANY CONSECUTIVE ERRORS, STOPPING ZEROMQ SERVER ON PORT {}", self.port);
                                break;
                            }
                        }
                        Ok(Err(e)) if e.to_string() == ZMQ_EAGAIN_SENTINEL => {
                            // Normal case when no message is available
                            continue;
                        }
                        Ok(Err(e)) => {
                            warn!("Error processing ZMQ message on port {}: {}", self.port, e);
                            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                error!("Too many consecutive ZMQ errors for port {}, shutting down server", self.port);
                                println!("===> TOO MANY CONSECUTIVE ZMQ ERRORS FOR PORT {}, SHUTTING DOWN SERVER", self.port);
                                break;
                            }
                        }
                        Err(_) => {
                            // Timeout occurred, continue to check shutdown
                            continue;
                        }
                    }
                }
            }

            // Check running state
            if !*self.running.lock().await {
                info!("Shutdown signal detected for ZeroMQ server on port {}", self.port);
                println!("===> SHUTDOWN SIGNAL DETECTED FOR ZEROMQ SERVER ON PORT {}", self.port);
                break;
            }
        }

        // Cleanup
        info!("Cleaning up ZeroMQ server for port {}", self.port);
        println!("===> CLEANING UP ZEROMQ SERVER FOR PORT {}", self.port);
        if let Err(e) = responder.disconnect(&endpoint) {
            warn!("Failed to disconnect ZeroMQ socket for port {}: {}", self.port, e);
        }
        if tokio::fs::metadata(&socket_path).await.is_ok() {
            if let Err(e) = tokio::fs::remove_file(&socket_path).await {
                warn!("Failed to remove IPC socket file {} for port {}: {}", socket_path, self.port, e);
            }
        }

        info!("ZeroMQ server stopped for port {}", self.port);
        println!("===> ZEROMQ SERVER STOPPED FOR PORT {}", self.port);
        Ok(())
    }

    /// Processes a single ZMQ message
    async fn process_zmq_message(&self, responder: &ZmqSocket, consecutive_errors: &mut u32) -> GraphResult<bool> {
        const MAX_CONSECUTIVE_ERRORS: u32 = 100;

        let msg = match responder.recv_bytes(0) {
            Ok(msg) => {
                *consecutive_errors = 0;
                debug!("Received ZMQ message for port {}: {:?}", self.port, String::from_utf8_lossy(&msg));
                msg
            }
            Err(e) if e == ZmqError::EAGAIN => {
                return Err(GraphError::StorageError(ZMQ_EAGAIN_SENTINEL.to_string()));
            }
            Err(e) => {
                *consecutive_errors += 1;
                if *consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    error!("Too many consecutive ZMQ errors for port {}, requesting shutdown", self.port);
                    return Ok(false);
                }
                return Err(GraphError::StorageError(format!("ZMQ receive error: {}", e)));
            }
        };

        let request: Value = match serde_json::from_slice(&msg) {
            Ok(req) => req,
            Err(e) => {
                self.send_zmq_response(responder, &json!({
                    "status": "error",
                    "message": format!("Failed to parse request: {}", e)
                })).await;
                return Err(GraphError::StorageError(format!("JSON parse error: {}", e)));
            }
        };

        let response = match request.get("command").and_then(|c| c.as_str()) {
            Some("status") => json!({ "status": "success", "port": self.port }),
            Some("ping") => json!({ "status": "success", "message": "pong" }),
            Some("set_key") => {
                let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = match request.get("key").and_then(|k| k.as_str()) {
                    Some(k) => k,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": "Missing key in set_key request" })).await;
                        return Err(GraphError::StorageError("Missing key in set_key request".to_string()));
                    }
                };
                let value = match request.get("value").and_then(|v| v.as_str()) {
                    Some(v) => v,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": "Missing value in set_key request" })).await;
                        return Err(GraphError::StorageError("Missing value in set_key request".to_string()));
                    }
                };
                let cf_handle = match self.db.cf_handle(cf_name) {
                    Some(cf) => cf,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": format!("Column family {} not found", cf_name) })).await;
                        return Err(GraphError::StorageError(format!("Column family {} not found", cf_name)));
                    }
                };
                let write_opts = WriteOptions::default();
                match self.db.put_cf_opt(&cf_handle, key.as_bytes(), value.as_bytes(), &write_opts) {
                    Ok(_) => json!({ "status": "success" }),
                    Err(e) => json!({ "status": "error", "message": e.to_string() }),
                }
            }
            Some("get_key") => {
                let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = match request.get("key").and_then(|k| k.as_str()) {
                    Some(k) => k,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": "Missing key in get_key request" })).await;
                        return Err(GraphError::StorageError("Missing key in get_key request".to_string()));
                    }
                };
                let cf_handle = match self.db.cf_handle(cf_name) {
                    Some(cf) => cf,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": format!("Column family {} not found", cf_name) })).await;
                        return Err(GraphError::StorageError(format!("Column family {} not found", cf_name)));
                    }
                };
                match self.db.get_cf(&cf_handle, key.as_bytes()) {
                    Ok(Some(val)) => json!({ "status": "success", "value": String::from_utf8_lossy(&val).to_string() }),
                    Ok(None) => json!({ "status": "success", "value": Value::Null }),
                    Err(e) => json!({ "status": "error", "message": e.to_string() }),
                }
            }
            Some("delete_key") => {
                let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = match request.get("key").and_then(|k| k.as_str()) {
                    Some(k) => k,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": "Missing key in delete_key request" })).await;
                        return Err(GraphError::StorageError("Missing key in delete_key request".to_string()));
                    }
                };
                let cf_handle = match self.db.cf_handle(cf_name) {
                    Some(cf) => cf,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": format!("Column family {} not found", cf_name) })).await;
                        return Err(GraphError::StorageError(format!("Column family {} not found", cf_name)));
                    }
                };
                let write_opts = WriteOptions::default();
                match self.db.delete_cf_opt(&cf_handle, key.as_bytes(), &write_opts) {
                    Ok(_) => json!({ "status": "success" }),
                    Err(e) => json!({ "status": "error", "message": e.to_string() }),
                }
            }
            Some("flush") => {
                match self.db.flush() {
                    Ok(_) => json!({ "status": "success", "bytes_flushed": 0 }),
                    Err(e) => json!({ "status": "error", "message": e.to_string() }),
                }
            }
            Some("clear_data") => {
                let cf_handle = match self.db.cf_handle("kv_pairs") {
                    Some(cf) => cf,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": "Column family kv_pairs not found" })).await;
                        return Err(GraphError::StorageError("Column family kv_pairs not found".to_string()));
                    }
                };
                let iterator = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                let mut batch = WriteBatch::default();
                for item in iterator {
                    match item {
                        Ok((key, _)) => batch.delete_cf(&cf_handle, &key),
                        Err(e) => {
                            self.send_zmq_response(responder, &json!({ "status": "error", "message": format!("Iterator error: {}", e) })).await;
                            return Err(GraphError::StorageError(format!("Iterator error: {}", e)));
                        }
                    }
                }
                match self.db.write(batch) {
                    Ok(_) => {
                        match self.db.flush() {
                            Ok(_) => json!({ "status": "success", "bytes_flushed": 0 }),
                            Err(e) => json!({ "status": "error", "message": format!("Failed to flush after clearing: {}", e) }),
                        }
                    }
                    Err(e) => json!({ "status": "error", "message": format!("Failed to clear kv_pairs: {}", e) }),
                }
            }
            Some("get_all_vertices") => {
                let cf_handle = match self.db.cf_handle("vertices") {
                    Some(cf) => cf,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": "Column family vertices not found" })).await;
                        return Err(GraphError::StorageError("Column family vertices not found".to_string()));
                    }
                };
                let iterator = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                let mut vertices = Vec::new();
                for item in iterator {
                    match item {
                        Ok((_, value)) => {
                            match deserialize_vertex(&value) {
                                Ok(vertex) => vertices.push(vertex),
                                Err(e) => {
                                    warn!("Failed to deserialize vertex: {}", e);
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            self.send_zmq_response(responder, &json!({ "status": "error", "message": format!("Iterator error: {}", e) })).await;
                            return Err(GraphError::StorageError(format!("Iterator error: {}", e)));
                        }
                    }
                }
                json!({ "status": "success", "vertices": vertices })
            }
            Some("get_all_edges") => {
                let cf_handle = match self.db.cf_handle("edges") {
                    Some(cf) => cf,
                    None => {
                        self.send_zmq_response(responder, &json!({ "status": "error", "message": "Column family edges not found" })).await;
                        return Err(GraphError::StorageError("Column family edges not found".to_string()));
                    }
                };
                let iterator = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                let mut edges = Vec::new();
                for item in iterator {
                    match item {
                        Ok((_, value)) => {
                            match deserialize_edge(&value) {
                                Ok(edge) => edges.push(edge),
                                Err(e) => {
                                    warn!("Failed to deserialize edge: {}", e);
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            self.send_zmq_response(responder, &json!({ "status": "error", "message": format!("Iterator error: {}", e) })).await;
                            return Err(GraphError::StorageError(format!("Iterator error: {}", e)));
                        }
                    }
                }
                json!({ "status": "success", "edges": edges })
            }
            Some(cmd) => json!({ "status": "error", "message": format!("Unsupported command: {}", cmd) }),
            None => json!({ "status": "error", "message": "No command specified" }),
        };

        self.send_zmq_response(responder, &response).await;
        Ok(true)
    }

    /// Sends a ZMQ response
    async fn send_zmq_response(&self, responder: &ZmqSocket, response: &Value) {
        match serde_json::to_vec(response) {
            Ok(response_bytes) => {
                if let Err(e) = responder.send(&response_bytes, 0) {
                    error!("Failed to send ZeroMQ response for port {}: {}", self.port, e);
                    println!("===> ERROR: FAILED TO SEND ZEROMQ RESPONSE FOR PORT {}: {}", self.port, e);
                } else {
                    debug!("Sent ZeroMQ response for port {}: {:?}", self.port, response);
                    println!("===> SENT ZEROMQ RESPONSE FOR PORT {}: {:?}", self.port, response);
                }
            }
            Err(e) => {
                error!("Failed to serialize ZeroMQ response for port {}: {}", self.port, e);
                println!("===> ERROR: FAILED TO SERIALIZE ZEROMQ RESPONSE FOR PORT {}: {}", self.port, e);
                let error_response = json!({ "status": "error", "message": format!("Failed to serialize response: {}", e) });
                if let Ok(error_bytes) = serde_json::to_vec(&error_response) {
                    let _ = responder.send(&error_bytes, 0);
                }
            }
        }
    }

    /// Sends a ZMQ request to another daemon
    async fn send_zmq_request(&self, port: u16, request: Value) -> GraphResult<Value> {
        let socket = self.zmq_context.socket(zmq::REQ).map_err(|e| {
            GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e))
        })?;
        socket.set_rcvtimeo(10000)?;
        socket.set_sndtimeo(10000)?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        socket.connect(&endpoint).map_err(|e| {
            GraphError::StorageError(format!("Failed to connect to ZMQ socket {}: {}", endpoint, e))
        })?;

        socket.send(serde_json::to_vec(&request)?, 0).map_err(|e| {
            GraphError::StorageError(format!("Failed to send request to port {}: {}", port, e))
        })?;

        let reply = socket.recv_bytes(0).map_err(|e| {
            GraphError::StorageError(format!("Failed to receive response from port {}: {}", port, e))
        })?;

        serde_json::from_slice(&reply).map_err(|e| {
            GraphError::StorageError(format!("Failed to parse response from port {}: {}", port, e))
        })
    }

    /// Shuts down the daemon
    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("===> Starting graceful shutdown for daemon on port {}", self.port);
        println!("===> Starting graceful shutdown for daemon on port {}", self.port);
        
        // Signal shutdown
        {
            let mut running = self.running.lock().await;
            *running = false;
        }
        
        let _ = self.shutdown_tx.send(()).await;
        
        // Join the ZeroMQ thread if it exists
        {
            let mut thread_guard = self.zmq_thread.lock().await;
            if let Some(handle) = thread_guard.take() {
                // Spawn a task to join the thread with timeout
                let port = self.port;
                match tokio::task::spawn_blocking(move || {
                    handle.join()
                }).await {
                    Ok(Ok(())) => {
                        info!("Successfully joined ZeroMQ thread for port {}", port);
                        println!("===> SUCCESSFULLY JOINED ZEROMQ THREAD FOR PORT {}", port);
                    }
                    Ok(Err(e)) => {
                        warn!("Failed to join ZeroMQ thread for port {}: {:?}", port, e);
                    }
                    Err(e) => {
                        warn!("Task panic while joining ZeroMQ thread for port {}: {:?}", port, e);
                        println!("===> TASK PANIC JOINING ZEROMQ THREAD FOR PORT {}", port);
                    }
                }
            }
        }
        
        // Flush database
        if let Err(e) = self.db.flush() {
            warn!("Failed to flush database during shutdown: {}", e);
        }
        
        // Clean up socket file
        let socket_path = format!("/tmp/graphdb-{}.ipc", self.port);
        if fs::metadata(&socket_path).await.is_ok() {
            fs::remove_file(&socket_path).await.map_err(|e| {
                warn!("Failed to remove socket file {}: {}", socket_path, e);
                GraphError::StorageError(format!("Failed to remove socket file {}: {}", socket_path, e))
            })?;
        }
        
        // Remove from registry
        GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("storage", self.port).await.map_err(|e| {
            warn!("Failed to remove daemon from registry: {}", e);
            GraphError::StorageError(format!("Failed to remove daemon from registry: {}", e))
        })?;
        
        info!("===> Graceful shutdown complete for daemon on port {}", self.port);
        println!("===> Graceful shutdown complete for daemon on port {}", self.port);
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    pub fn db_path(&self) -> PathBuf {
        self.db_path.clone()
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    #[cfg(feature = "with-openraft-rocksdb")]
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
        #[cfg(feature = "with-openraft-rocksdb")]
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

    fn serialize_to_bytes<T: serde::Serialize>(data: &T) -> GraphResult<Vec<u8>> {
        let serialized = serde_json::to_vec(data)
            .map_err(|e| GraphError::StorageError(format!("Serialization failed: {}", e)))?;
        Ok(serialized)
    }

    fn deserialize_from_bytes<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> GraphResult<T> {
        serde_json::from_slice(bytes)
            .map_err(|e| GraphError::StorageError(format!("Deserialization failed: {}", e)))
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Inserting key into RocksDB at path {:?}", self.db_path);
        println!("===> IN INSERT - TRYING TO INSERT KEY {:?}", key);

        timeout(TokioDuration::from_secs(5), async {
            let mut batch = WriteBatch::default();
            batch.put(key, value);
            let write_opts = WriteOptions::default();
            self.db
                .write_opt(batch, &write_opts)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch: {}", e)))?;

            info!("Inserted key at {:?}", self.db_path);
            println!("===> IN INSERT - INSERTED KEY {:?}", key);

            #[cfg(feature = "with-openraft-rocksdb")]
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
        info!("Retrieving key from RocksDB at path {:?}", self.db_path);
        println!("===> RETRIEVE: RETRIEVING KEY FROM ROCKSDB AT PATH {:?} AND KEY IS {:?}", self.db_path, key);

        let value = timeout(TokioDuration::from_secs(5), async {
            self.db
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to retrieve key: {}", e)))
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during retrieve".to_string()))??;

        println!("===> RETRIEVE: VALUE {:?}", value);
        Ok(value)
    }

    pub async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Deleting key from RocksDB at path {:?}", self.db_path);
        println!("===> DELETING KEY FROM ROCKSDB AT PATH {:?}", self.db_path);
        timeout(TokioDuration::from_secs(5), async {
            let mut batch = WriteBatch::default();
            batch.delete(key);
            let write_opts = WriteOptions::default();
            self.db
                .write_opt(batch, &write_opts)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            #[cfg(feature = "with-openraft-rocksdb")]
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
        let value = Self::serialize_to_bytes(vertex)?;
        info!("Creating vertex with id {} at path {:?}", vertex.id, self.db_path);
        println!("===> CREATING VERTEX WITH ID {} AT PATH {:?}", vertex.id, self.db_path);

        timeout(TokioDuration::from_secs(5), async {
            let mut batch = WriteBatch::default();
            batch.put(key, value);
            let write_opts = WriteOptions::default();
            self.db
                .write_opt(batch, &write_opts)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch for vertex: {}", e)))?;

            info!("Created vertex at {:?}", self.db_path);
            println!("===> CREATE_VERTEX: CREATED VERTEX");

            #[cfg(feature = "with-openraft-rocksdb")]
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

        let res = timeout(TokioDuration::from_secs(5), async {
            let opt = self.db
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to retrieve vertex: {}", e)))?;
            match opt {
                Some(bytes) => {
                    let vertex = Self::deserialize_from_bytes(&bytes)?;
                    Ok::<Option<Vertex>, GraphError>(Some(vertex))
                }
                None => Ok::<Option<Vertex>, GraphError>(None),
            }
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))??;

        println!("===> GET_VERTEX: VERTEX {:?}", res);
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
        timeout(TokioDuration::from_secs(5), async {
            let mut batch = WriteBatch::default();
            batch.delete(key);
            let prefix = id.as_bytes();
            for item in self.db.iterator(rocksdb::IteratorMode::Start) {
                let (k, _) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                if k.starts_with(prefix) {
                    batch.delete(k);
                }
            }
            let write_opts = WriteOptions::default();
            self.db
                .write_opt(batch, &write_opts)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            #[cfg(feature = "with-openraft-rocksdb")]
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
        let value = Self::serialize_to_bytes(edge)?;
        info!("Creating edge ({}, {}, {}) at path {:?}", edge.outbound_id, edge.t, edge.inbound_id, self.db_path);
        println!("===> CREATING EDGE ({}, {}, {}) AT PATH {:?}", edge.outbound_id, edge.t, edge.inbound_id, self.db_path);

        timeout(TokioDuration::from_secs(5), async {
            let mut batch = WriteBatch::default();
            batch.put(&*key, value);
            let write_opts = WriteOptions::default();
            self.db
                .write_opt(batch, &write_opts)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch for edge: {}", e)))?;

            info!("Created edge at {:?}", self.db_path);
            println!("===> CREATE_EDGE: CREATED EDGE");

            #[cfg(feature = "with-openraft-rocksdb")]
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

        let res = timeout(TokioDuration::from_secs(5), async {
            let opt = self.db
                .get(&key)
                .map_err(|e| GraphError::StorageError(format!("Failed to retrieve edge: {}", e)))?;
            match opt {
                Some(bytes) => {
                    let edge = Self::deserialize_from_bytes(&bytes)?;
                    Ok::<Option<Edge>, GraphError>(Some(edge))
                }
                None => Ok::<Option<Edge>, GraphError>(None),
            }
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))??;

        println!("===> GET_EDGE: EDGE {:?}", res);
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
        timeout(TokioDuration::from_secs(5), async {
            let mut batch = WriteBatch::default();
            batch.delete(key);
            let write_opts = WriteOptions::default();
            self.db
                .write_opt(batch, &write_opts)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            #[cfg(feature = "with-openraft-rocksdb")]
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
        info!("Resetting RocksDBDaemon at path {:?}", self.db_path);
        println!("===> RESETTING ROCKSDB DAEMON AT PATH {:?}", self.db_path);
        timeout(TokioDuration::from_secs(5), async {
            let mut batch = WriteBatch::default();
            for item in self.db.iterator(rocksdb::IteratorMode::Start) {
                let (k, _) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                batch.delete(k);
            }
            let write_opts = WriteOptions::default();
            self.db
                .write_opt(batch, &write_opts)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            #[cfg(feature = "with-openraft-rocksdb")]
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

    pub async fn flush(&self) -> GraphResult<()> {
        info!("RocksDBDaemon::flush - Sending flush request to ZeroMQ server on port {}", self.port);
        println!("===> ROCKSDB DAEMON FLUSH - SENDING FLUSH REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", self.port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket {}: {}", endpoint, e)))?;

        let request = json!({ "command": "flush" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send flush request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive flush response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            info!("RocksDBDaemon::flush - Successfully flushed database via ZeroMQ");
            println!("===> ROCKSDB DAEMON FLUSH - SUCCESSFULLY FLUSHED DATABASE VIA ZEROMQ");
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("RocksDBDaemon::flush - Failed: {}", error_msg);
            println!("===> ROCKSDB DAEMON FLUSH - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        info!("RocksDBDaemon::get_all_vertices - Sending request to ZeroMQ server on port {}", self.port);
        println!("===> ROCKSDB DAEMON GET_ALL_VERTICES - SENDING REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", self.port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket {}: {}", endpoint, e)))?;

        let request = json!({ "command": "get_all_vertices" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send get_all_vertices request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive get_all_vertices response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            let vertices: Vec<Vertex> = serde_json::from_value(response["vertices"].clone())
                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize vertices: {}", e)))?;
            info!("RocksDBDaemon::get_all_vertices - Retrieved {} vertices", vertices.len());
            println!("===> ROCKSDB DAEMON GET_ALL_VERTICES - RETRIEVED {} VERTICES", vertices.len());
            Ok(vertices)
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("RocksDBDaemon::get_all_vertices - Failed: {}", error_msg);
            println!("===> ROCKSDB DAEMON GET_ALL_VERTICES - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        info!("RocksDBDaemon::get_all_edges - Sending request to ZeroMQ server on port {}", self.port);
        println!("===> ROCKSDB DAEMON GET_ALL_EDGES - SENDING REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", self.port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket {}: {}", endpoint, e)))?;

        let request = json!({ "command": "get_all_edges" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send get_all_edges request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive get_all_edges response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            let edges: Vec<Edge> = serde_json::from_value(response["edges"].clone())
                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edges: {}", e)))?;
            info!("RocksDBDaemon::get_all_edges - Retrieved {} edges", edges.len());
            println!("===> ROCKSDB DAEMON GET_ALL_EDGES - RETRIEVED {} EDGES", edges.len());
            Ok(edges)
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("RocksDBDaemon::get_all_edges - Failed: {}", error_msg);
            println!("===> ROCKSDB DAEMON GET_ALL_EDGES - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn clear_data(&self) -> GraphResult<()> {
        info!("RocksDBDaemon::clear_data - Sending clear_data request to ZeroMQ server on port {}", self.port);
        println!("===> ROCKSDB DAEMON CLEAR_DATA - SENDING CLEAR_DATA REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", self.port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket {}: {}", endpoint, e)))?;

        let request = json!({ "command": "clear_data" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send clear_data request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive clear_data response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            info!("RocksDBDaemon::clear_data - Successfully cleared database via ZeroMQ");
            println!("===> ROCKSDB DAEMON CLEAR_DATA - SUCCESSFULLY CLEARED DATABASE VIA ZEROMQ");
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("RocksDBDaemon::clear_data - Failed: {}", error_msg);
            println!("===> ROCKSDB DAEMON CLEAR_DATA - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }
}

impl RocksDBDaemonPool {
    pub fn new() -> Self {
        Self {
            // Correctly initialized thread-safe daemon map
            daemons: Arc::new(RwLock::new(HashMap::new())),
            
            // Initialization for required fields:
            registry: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
            load_balancer: Arc::new(LoadBalancer::new(3)),
            use_raft_for_scale: false,

            // REMOVED: is_running, primary_daemon_port, health_check_handle as per error message
        }
    }
    
    /// Creates a new pool instance and initializes it with a pre-existing database connection.
    pub async fn new_with_db(config: &RocksDBConfig, existing_db: Arc<rocksdb::DB>) -> GraphResult<Self> {
        let mut pool = Self::new();
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        
        // Initialize daemon with existing database
        let daemon = RocksDBDaemon::new_with_db(config.clone(), existing_db).await?;
        let daemon_arc = Arc::new(daemon);
        
        // Acquire write lock before inserting into the HashMap
        pool.daemons.write().await.insert(port, daemon_arc.clone());
        
        // Use the path directly from config, it's already a PathBuf
        let data_dir = config.path.clone();
        
        let db_path = data_dir.join(port.to_string());
        let metadata = DaemonMetadata {
            port,
            service_type: "RocksDB".to_string(),
            ip_address: "127.0.0.1".to_string(),
            config_path: Some(db_path.clone()),
            data_dir: Some(data_dir),
            engine_type: Some(StorageEngineType::RocksDB.to_string()),
            pid: std::process::id() as u32,
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| GraphError::StorageError(format!("Failed to get system time: {}", e)))?
                .as_nanos() as i64,
        };
        
        // Register metadata in the registry
        pool.registry.write().await.insert(port, metadata);
        Ok(pool)
    }

    /// Creates a new pool instance used only for client operations (no local daemon),
    /// primarily for Raft.
    pub async fn new_with_client(_client: RocksDBClient, db_path: &Path, port: u16) -> GraphResult<Self> {
        println!("===> Initializing RocksDBDaemonPool with client for port {}", port);
        
        // Initialize 'daemons' field with the correct complex type
        let pool = Self {
            daemons: Arc::new(RwLock::new(HashMap::new())), 
            registry: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
            load_balancer: Arc::new(LoadBalancer::new(3)),
            use_raft_for_scale: false,
        };

        let metadata = DaemonMetadata {
            port,
            service_type: "storage".to_string(),
            ip_address: "127.0.0.1".to_string(),
            config_path: Some(db_path.to_path_buf()),
            data_dir: Some(db_path.to_path_buf()),
            engine_type: Some(StorageEngineType::RocksDB.to_string()),
            pid: std::process::id() as u32,
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| GraphError::StorageError(format!("Failed to get system time: {}", e)))?
                .as_nanos() as i64,
        };

        {
            let mut registry = pool.registry.write().await;
            registry.insert(port, metadata.clone());
        }

        {
            let mut nodes = pool.load_balancer.nodes.write().await;
            nodes.insert(
                port,
                NodeHealth {
                    is_healthy: true,
                    last_check: SystemTime::now(),
                    response_time_ms: 0,
                    error_count: 0,
                    port,
                    request_count: 0, 
                },
            );
        }

        *pool.initialized.write().await = true;
        info!("Successfully initialized RocksDBDaemonPool with client for port {}", port);
        println!("===> Successfully initialized RocksDBDaemonPool with client for port {}", port);
        Ok(pool)
    }

    /// Adds a new daemon to the pool map.
    pub async fn add_daemon(&self, daemon: Arc<RocksDBDaemon>) {
        // 1. Acquire the asynchronous write lock.
        let mut map_guard = self.daemons.write().await;
        
        // 2. Clone the Arc before insertion to retain ownership for logging.
        let port_for_log = daemon.port;
        map_guard.insert(daemon.port, daemon.clone()); 
        
        info!("Successfully added daemon on port {} to the pool.", port_for_log);
    }

    pub async fn insert_replicated(&self, key: &[u8], value: &[u8], use_raft_for_scale: bool) -> GraphResult<()> {
        let strategy = if use_raft_for_scale && self.use_raft_for_scale {
            ReplicationStrategy::Raft
        } else {
            ReplicationStrategy::NNodes(self.load_balancer.replication_factor)
        };

        let write_nodes = self.load_balancer.get_write_nodes(strategy).await;

        if write_nodes.is_empty() {
            return Err(GraphError::StorageError("No healthy nodes available for write operation".to_string()));
        }

        println!("===> REPLICATED INSERT: Writing to {} nodes: {:?}", write_nodes.len(), write_nodes);

        #[cfg(feature = "with-openraft-rocksdb")]
        if matches!(strategy, ReplicationStrategy::Raft) && self.use_raft_for_scale {
            return self.insert_raft(key, value).await;
        }

        let mut success_count = 0;
        let mut errors = Vec::new();

        for port in &write_nodes {
            match self.insert_to_node(*port, key, value).await {
                Ok(_) => {
                    success_count += 1;
                    println!("===> REPLICATED INSERT: Success on node {}", port);
                    self.load_balancer.update_node_health(*port, true, 0).await;
                }
                Err(e) => {
                    errors.push((*port, e));
                    println!("===> REPLICATED INSERT: Failed on node {}: {:?}", port, errors.last().unwrap().1);
                    self.load_balancer.update_node_health(*port, false, 0).await;
                }
            }
        }

        let required_success = (write_nodes.len() / 2) + 1;
        if success_count >= required_success {
            println!("===> REPLICATED INSERT: Success! {}/{} nodes confirmed write", success_count, write_nodes.len());
            Ok(())
        } else {
            error!("===> REPLICATED INSERT: Failed! Only {}/{} nodes confirmed write", success_count, write_nodes.len());
            Err(GraphError::StorageError(format!(
                "Write failed: only {}/{} nodes succeeded. Errors: {:?}", success_count, write_nodes.len(), errors
            )))
        }
    }

    pub async fn delete_replicated(&self, key: &[u8], use_raft_for_scale: bool) -> GraphResult<()> {
        let strategy = if use_raft_for_scale && self.use_raft_for_scale {
            ReplicationStrategy::Raft
        } else {
            ReplicationStrategy::NNodes(self.load_balancer.replication_factor)
        };
        let write_nodes = self.load_balancer.get_write_nodes(strategy).await;

        if write_nodes.is_empty() {
            return Err(GraphError::StorageError("No healthy nodes available for delete operation".to_string()));
        }

        println!("===> REPLICATED DELETE: Deleting from {} nodes: {:?}", write_nodes.len(), write_nodes);

        #[cfg(feature = "with-openraft-rocksdb")]
        if matches!(strategy, ReplicationStrategy::Raft) && self.use_raft_for_scale {
            let leader_daemon = self.leader_daemon().await?;
            if let Some(raft) = &leader_daemon.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft delete failed: {}", e)))?;
                println!("===> REPLICATED DELETE: Successfully replicated via Raft consensus");
                return Ok(());
            } else {
                return Err(GraphError::StorageError("Raft is not initialized for leader daemon".to_string()));
            }
        }

        let mut success_count = 0;
        let mut errors = Vec::new();
        let write_nodes_len = write_nodes.len();

        for port in &write_nodes {
            let context = zmq::Context::new();
            let socket = context.socket(zmq::REQ)
                .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;

            socket.set_rcvtimeo(5000)
                .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
            socket.set_sndtimeo(5000)
                .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

            let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
            socket.connect(&endpoint)
                .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", endpoint, e)))?;

            let request = json!({
                "command": "delete_key",
                "key": String::from_utf8_lossy(key).to_string(),
                "replicated": true,
                "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
            });

            let start_time = SystemTime::now();
            socket.send(serde_json::to_vec(&request)?, 0)
                .map_err(|e| GraphError::StorageError(format!("Failed to send delete request: {}", e)))?;

            let reply = socket.recv_bytes(0)
                .map_err(|e| GraphError::StorageError(format!("Failed to receive delete response: {}", e)))?;

            let response_time = start_time.elapsed().unwrap().as_millis() as u64;
            let response: Value = serde_json::from_slice(&reply)?;

            if response["status"] == "success" {
                success_count += 1;
                self.load_balancer.update_node_health(*port, true, response_time).await;
                println!("===> REPLICATED DELETE: Success on node {}", port);
            } else {
                let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
                errors.push((*port, error_msg.clone()));
                self.load_balancer.update_node_health(*port, false, response_time).await;
                println!("===> REPLICATED DELETE: Failed on node {}: {}", port, error_msg);
            }
        }

        let required_success = (write_nodes_len / 2) + 1;
        if success_count >= required_success {
            println!("===> REPLICATED DELETE: Success! {}/{} nodes confirmed delete", success_count, write_nodes_len);
            Ok(())
        } else {
            error!("===> REPLICATED DELETE: Failed! Only {}/{} nodes confirmed delete", success_count, write_nodes_len);
            Err(GraphError::StorageError(format!(
                "Delete failed: only {}/{} nodes succeeded. Errors: {:?}", success_count, write_nodes_len, errors
            )))
        }
    }

    async fn insert_to_node(&self, port: u16, key: &[u8], value: &[u8]) -> GraphResult<()> {
        // Check daemon health before attempting connection
        let is_healthy = self.health_check_node(port, &HealthCheckConfig {
            interval: tokio::time::Duration::from_secs(10),
            connect_timeout: tokio::time::Duration::from_secs(1),
            response_buffer_size: 1024,
        }).await.unwrap_or(false);

        if !is_healthy {
            let load_balancer = self.load_balancer.clone();
            tokio::spawn(async move {
                load_balancer.update_node_health(port, false, 0).await;
            });
            return Err(GraphError::StorageError(format!("Daemon on port {} is not healthy or not running", port)));
        }

        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;

        socket.set_rcvtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        socket.set_sndtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        socket.connect(&endpoint)
            .map_err(|e| {
                let load_balancer = self.load_balancer.clone();
                tokio::spawn(async move {
                    load_balancer.update_node_health(port, false, 0).await;
                });
                GraphError::StorageError(format!("Failed to connect to {}: {}", endpoint, e))
            })?;

        let request = json!({
            "command": "set_key",
            "key": String::from_utf8_lossy(key).to_string(),
            "value": String::from_utf8_lossy(value).to_string(),
            "replicated": true,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        });

        let start_time = SystemTime::now();
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| {
                let load_balancer = self.load_balancer.clone();
                tokio::spawn(async move {
                    load_balancer.update_node_health(port, false, 0).await;
                });
                GraphError::StorageError(format!("Failed to send request: {}", e))
            })?;

        let reply = socket.recv_bytes(0)
            .map_err(|e| {
                let load_balancer = self.load_balancer.clone();
                tokio::spawn(async move {
                    load_balancer.update_node_health(port, false, 0).await;
                });
                GraphError::StorageError(format!("Failed to receive response: {}", e))
            })?;

        let response_time = start_time.elapsed().unwrap().as_millis() as u64;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            self.load_balancer.update_node_health(port, true, response_time).await;
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            let load_balancer = self.load_balancer.clone();
            tokio::spawn(async move {
                load_balancer.update_node_health(port, false, response_time).await;
            });
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn retrieve_with_failover(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 3;

        while attempts < MAX_ATTEMPTS {
            if let Some(port) = self.load_balancer.get_read_node().await {
                match self.retrieve_from_node(port, key).await {
                    Ok(result) => {
                        println!("===> RETRIEVE WITH FAILOVER: Success from node {} on attempt {}", port, attempts + 1);
                        return Ok(result);
                    }
                    Err(e) => {
                        warn!("===> RETRIEVE WITH FAILOVER: Failed from node {} on attempt {}: {}", port, attempts + 1, e);
                        self.load_balancer.update_node_health(port, false, 0).await;
                        attempts += 1;
                    }
                }
            } else {
                return Err(GraphError::StorageError("No healthy nodes available for read operation".to_string()));
            }
        }

        Err(GraphError::StorageError(format!("Failed to retrieve after {} attempts", MAX_ATTEMPTS)))
    }

    async fn retrieve_from_node(&self, port: u16, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;

        socket.set_rcvtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        socket.set_sndtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", endpoint, e)))?;

        let request = json!({
            "command": "get_key",
            "key": String::from_utf8_lossy(key).to_string()
        });

        let start_time = SystemTime::now();
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send request: {}", e)))?;

        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive response: {}", e)))?;

        let response_time = start_time.elapsed().unwrap().as_millis() as u64;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            self.load_balancer.update_node_health(port, true, response_time).await;

            if let Some(value_str) = response["value"].as_str() {
                Ok(Some(value_str.as_bytes().to_vec()))
            } else {
                Ok(None)
            }
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            self.load_balancer.update_node_health(port, false, response_time).await;
            Err(GraphError::StorageError(error_msg))
        }
    }

    #[cfg(feature = "with-openraft-rocksdb")]
    async fn insert_raft(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let leader_daemon = self.leader_daemon().await?;

        if let Some(raft) = &leader_daemon.raft {
            let request = openraft::raft::ClientWriteRequest::new(
                openraft::EntryPayload::AppWrite {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }
            );

            raft.client_write(request).await
                .map_err(|e| GraphError::StorageError(format!("Raft write failed: {}", e)))?;

            println!("===> RAFT INSERT: Successfully replicated via Raft consensus");
            Ok(())
        } else {
            Err(GraphError::StorageError("Raft is not initialized for leader daemon".to_string()))
        }
    }

    /// Attempts to connect to a single node and check its status.
    async fn health_check_node(&self, port: u16, config: &HealthCheckConfig) -> GraphResult<bool> {
        let address = format!("127.0.0.1:{}", port);
        let start_time = SystemTime::now();

        match timeout(config.connect_timeout, TcpStream::connect(&address)).await {
            Ok(Ok(mut stream)) => {
                let request = json!({"command": "status"});
                
                let request_bytes = serde_json::to_vec(&request)
                    .map_err(|e| {
                        warn!("Failed to serialize status request for port {}: {}", port, e);
                        GraphError::SerializationError(e.to_string())
                    })?;

                if let Err(e) = stream.write_all(&request_bytes).await {
                    self.load_balancer.update_node_health(port, false, 0).await;
                    warn!("Failed to send status request to daemon on port {}. Reason: {}", port, e);
                    return Ok(false);
                }

                let mut response_buffer = vec![0; config.response_buffer_size];
                let bytes_read = match timeout(config.connect_timeout, stream.read(&mut response_buffer)).await {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => {
                        self.load_balancer.update_node_health(port, false, 0).await;
                        warn!("Failed to read status response from daemon on port {}. Reason: {}", port, e);
                        return Ok(false);
                    }
                    Err(_) => {
                        self.load_balancer.update_node_health(port, false, 0).await;
                        warn!("Timeout reading status response from daemon on port {}", port);
                        return Ok(false);
                    }
                };

                let response_time = start_time.elapsed().unwrap().as_millis() as u64;
                
                // FIX E0728: Use a match block to handle the Result and call the async 
                // update_node_health() outside of the synchronous map_err closure.
                let response: Value = match serde_json::from_slice(&response_buffer[..bytes_read]) {
                    Ok(res) => res,
                    Err(e) => {
                        // This call to await is now within the async function body, not inside map_err
                        self.load_balancer.update_node_health(port, false, response_time).await;
                        warn!("Failed to parse status response from daemon on port {}: {}", port, e);
                        return Err(GraphError::SerializationError(e.to_string()));
                    }
                };
                
                let is_healthy = response["status"] == "success";
                self.load_balancer.update_node_health(port, is_healthy, response_time).await;
                Ok(is_healthy)
            }
            Ok(Err(e)) => {
                self.load_balancer.update_node_health(port, false, 0).await;
                warn!("Failed to connect to daemon on port {}. Reason: {}", port, e);
                Ok(false)
            }
            Err(_) => {
                self.load_balancer.update_node_health(port, false, 0).await;
                warn!("Timeout connecting to daemon on port {}", port);
                Ok(false)
            }
        }
    }
    
    /// Attempts to find the current Raft leader daemon in the pool.
    async fn leader_daemon(&self) -> GraphResult<Arc<RocksDBDaemon>> {
        // FIX: Acquire read lock to iterate over the HashMap values
        let daemons_map = self.daemons.read().await;

        // FIX: Iterate over daemons_map.values()
        for daemon in daemons_map.values() {
            #[cfg(feature = "with-openraft-rocksdb")]
            if daemon.is_leader().await? {
                return Ok(daemon.clone());
            }
        }
        
        Err(GraphError::StorageError("No leader daemon found".to_string()))
    }


    pub async fn initialize(&mut self, config: &RocksDBConfig) -> GraphResult<()> {
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let data_dir = config.path.clone();
        let db_path = data_dir.join(port.to_string());

        let daemon = RocksDBDaemon::new(config.clone()).await?;
        let daemon_arc = Arc::new(daemon);
        self.add_daemon(daemon_arc.clone());

        let metadata = DaemonMetadata {
            port: port,
            service_type: "RocksDB".to_string(),
            ip_address: "127.0.0.1".to_string(),
            config_path: Some(db_path.clone()),
            data_dir: Some(data_dir.clone()),
            engine_type: Some(StorageEngineType::RocksDB.to_string()),
            pid: std::process::id() as u32,
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| GraphError::StorageError(format!("Failed to get system time: {}", e)))?
                .as_nanos() as i64,
        };

        {
            let mut registry = self.registry.write().await;
            registry.insert(port, metadata);
        }

        {
            let mut nodes = self.load_balancer.nodes.write().await;
            nodes.insert(
                port,
                NodeHealth {
                    is_healthy: true,
                    last_check: SystemTime::now(),
                    response_time_ms: 0,
                    error_count: 0,
                    port,
                    request_count: 0, 
                },
            );
        }

        *self.initialized.write().await = true;
        Ok(())
    }

    pub async fn initialize_with_db(&mut self, config: &RocksDBConfig, existing_db: Arc<rocksdb::DB>) -> GraphResult<()> {
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let data_dir = config.path.clone();
        let db_path = data_dir.join(port.to_string());

        let daemon = RocksDBDaemon::new_with_db(config.clone(), existing_db).await?;
        let daemon_arc = Arc::new(daemon);
        self.add_daemon(daemon_arc.clone());

        let metadata = DaemonMetadata {
            port: port,
            service_type: "RocksDB".to_string(),
            ip_address: "127.0.0.1".to_string(),
            config_path: Some(db_path.clone()),
            data_dir: Some(data_dir.clone()),
            engine_type: Some(StorageEngineType::RocksDB.to_string()),
            pid: std::process::id() as u32,
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| GraphError::StorageError(format!("Failed to get system time: {}", e)))?
                .as_nanos() as i64,
        };

        {
            let mut registry = self.registry.write().await;
            registry.insert(port, metadata);
        }

        {
            let mut nodes = self.load_balancer.nodes.write().await;
            nodes.insert(
                port,
                NodeHealth {
                    is_healthy: true,
                    last_check: SystemTime::now(),
                    response_time_ms: 0,
                    error_count: 0,
                    port,
                    request_count: 0, 
                },
            );
        }

        *self.initialized.write().await = true;
        Ok(())
    }

    pub async fn create_vertex(&self, vertex: &Vertex, use_raft_for_scale: bool) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes();
        let value = RocksDBDaemon::serialize_to_bytes(vertex)?;
        self.insert_replicated(key, &value, use_raft_for_scale).await
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes();
        match self.retrieve_with_failover(key).await? {
            Some(bytes) => {
                let vertex = RocksDBDaemon::deserialize_from_bytes(&bytes)?;
                Ok(Some(vertex))
            }
            None => Ok(None),
        }
    }

    pub async fn update_vertex(&self, vertex: &Vertex, use_raft_for_scale: bool) -> GraphResult<()> {
        self.delete_vertex(&vertex.id.0, use_raft_for_scale).await?;
        self.create_vertex(vertex, use_raft_for_scale).await
    }

    pub async fn delete_vertex(&self, id: &Uuid, use_raft_for_scale: bool) -> GraphResult<()> {
        let key = id.as_bytes();
        self.delete_replicated(key, use_raft_for_scale).await
    }

    pub async fn create_edge(&self, edge: &Edge, use_raft_for_scale: bool) -> GraphResult<()> {
        let key = create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into())?;
        let value = RocksDBDaemon::serialize_to_bytes(edge)?;
        self.insert_replicated(&key, &value, use_raft_for_scale).await
    }

    pub async fn get_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
    ) -> GraphResult<Option<Edge>> {
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        match self.retrieve_with_failover(&key).await? {
            Some(bytes) => {
                let edge = RocksDBDaemon::deserialize_from_bytes(&bytes)?;
                Ok(Some(edge))
            }
            None => Ok(None),
        }
    }

    pub async fn update_edge(&self, edge: &Edge, use_raft_for_scale: bool) -> GraphResult<()> {
        self.create_edge(edge, use_raft_for_scale).await
    }

    pub async fn delete_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
        use_raft_for_scale: bool,
    ) -> GraphResult<()> {
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        self.delete_replicated(&key, use_raft_for_scale).await
    }

    /// Retrieves all vertices by querying all daemons in the pool.
    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut all_vertices = Vec::new();
        
        // FIX: Acquire read lock before accessing HashMap methods
        let daemons = self.daemons.read().await;
        let ports: Vec<u16> = daemons.keys().copied().collect();

        for port in ports {
            // FIX: Access HashMap via the guard 'daemons'
            if let Some(daemon) = daemons.get(&port) {
                match daemon.get_all_vertices().await {
                    Ok(vertices) => {
                        all_vertices.extend(vertices);
                    }
                    Err(e) => {
                        warn!("Failed to get vertices from daemon on port {}: {}", port, e);
                        self.load_balancer.update_node_health(port, false, 0).await;
                    }
                }
            }
        }

        Ok(all_vertices)
    }

    /// Retrieves all edges by querying all daemons in the pool.
    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut all_edges = Vec::new();
        
        // FIX: Acquire read lock before accessing HashMap methods
        let daemons = self.daemons.read().await;
        let ports: Vec<u16> = daemons.keys().copied().collect();

        for port in ports {
            // FIX: Access HashMap via the guard 'daemons'
            if let Some(daemon) = daemons.get(&port) {
                match daemon.get_all_edges().await {
                    Ok(edges) => {
                        all_edges.extend(edges);
                    }
                    Err(e) => {
                        warn!("Failed to get edges from daemon on port {}: {}", port, e);
                        self.load_balancer.update_node_health(port, false, 0).await;
                    }
                }
            }
        }

        Ok(all_edges)
    }

    /// Clears all data on every daemon in the pool.
    pub async fn clear_data(&self) -> GraphResult<()> {
        
        // FIX: Acquire read lock before accessing HashMap methods
        let daemons = self.daemons.read().await;
        let ports: Vec<u16> = daemons.keys().copied().collect();
        let mut errors = Vec::new();

        for port in ports {
            // FIX: Access HashMap via the guard 'daemons'
            if let Some(daemon) = daemons.get(&port) {
                if let Err(e) = daemon.clear_data().await {
                    errors.push((port, e));
                    self.load_balancer.update_node_health(port, false, 0).await;
                } else {
                    self.load_balancer.update_node_health(port, true, 0).await;
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(GraphError::StorageError(format!("Failed to clear data on some nodes: {:?}", errors)))
        }
    }

    /// Gracefully shuts down all managed storage daemons concurrently.
    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down RocksDBDaemonPool. Attempting graceful shutdown for all {} daemons...", 
            self.daemons.read().await.len()
        );

        let mut errors = Vec::<(u16, GraphError)>::new();

        // 1. Prepare daemon handles for concurrent shutdown
        // FIX: Change target type to Vec<(u16, Arc<RocksDBDaemon>)>
        let daemon_handles: Vec<(u16, Arc<RocksDBDaemon>)> = {
            let daemons = self.daemons.read().await;
            daemons.iter().map(|(&port, daemon)| (port, daemon.clone())).collect()
        };

        // 2. Create a vector of async tasks for concurrent execution
        let shutdown_futures = daemon_handles.into_iter().map(|(port, daemon)| {
            async move {
                if port == 8051 {
                    error!("DAEMON 8051 SHUTDOWN INITIATED: The daemon on port {} is being explicitly shut down. Check the call site of RocksDBDaemonPool::shutdown() if this is premature.", port);
                } else {
                    info!("Attempting to shut down RocksDB daemon on port {}", port);
                }

                if let Err(e) = daemon.shutdown().await {
                    error!("Failed to shut down daemon on port {}: {:?}", port, e);
                    Some((port, e))
                } else {
                    info!("Successfully shut down daemon on port {}", port);
                    None
                }
            }
        }).collect::<Vec<_>>();

        // 3. Execute all shutdown tasks concurrently
        let results = join_all(shutdown_futures).await;

        // 4. Collect errors
        for result in results.into_iter().filter_map(|r| r) {
            errors.push(result);
        }

        // 5. Clear the main daemons map
        {
            let mut daemons = self.daemons.write().await;
            daemons.clear();
            info!("Cleared daemons from the pool map.");
        }


        if errors.is_empty() {
            info!("RocksDBDaemonPool shutdown complete and successful.");
            Ok(())
        } else {
            error!("RocksDBDaemonPool shutdown completed with errors on some daemons.");
            Err(GraphError::StorageError(format!(
                "Failed to shutdown some daemons: {:?}",
                errors
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use std::time::TokioDuration;

    #[tokio::test]
    async fn test_delete_replicated() -> GraphResult<()> {
        let mut pool = RocksDBDaemonPool::new();
        let config = RocksDBConfig {
            port: Some(5555),
            path: PathBuf::from("/tmp/test_rocksdb"),
            cache_capacity: Some(1024 * 1024),
        };

        pool.initialize(&config).await?;

        let key = b"test_key";
        let value = b"test_value";

        // Insert a key
        pool.insert_replicated(key, value, false).await?;

        // Verify the key exists
        let retrieved = pool.retrieve_with_failover(key).await?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), value);

        // Delete the key
        pool.delete_replicated(key, false).await?;

        // Verify the key is deleted
        let retrieved = pool.retrieve_with_failover(key).await?;
        assert!(retrieved.is_none());

        // Clean up
        pool.shutdown().await?;
        Ok(())
    }
}

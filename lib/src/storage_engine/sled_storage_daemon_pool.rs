use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io::{Cursor, Read, Write};
use sled::{Config, Db, IVec, Tree, Batch};
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::time::{sleep, Duration, timeout};
use tokio::fs;
use log::{info, debug, warn, error};
use crate::config::{SledConfig, SledDaemon, SledDaemonPool, SledStorage, StorageConfig, StorageEngineType, 
                    DAEMON_REGISTRY_DB_PATH, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT};
use crate::storage_engine::storage_utils::{create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::{json, Value};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;
use uuid::Uuid;
use zmq::{Context as ZmqContext};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, RefreshKind, Pid};
use async_trait::async_trait;

#[cfg(feature = "with-openraft-sled")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode},
    openraft_sled::SledRaftStorage,
    tokio::net::TcpStream,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

impl SledDaemon {
    pub async fn new(config: &SledConfig) -> GraphResult<Self> {
        println!("===> TRYING TO INITIALIZE SLED DAEMON");
        let db_path = config.path.clone();
        info!("Initializing SledDaemon with path {:?}", db_path);
        println!("===> INITIALIZING SledDaemon WITH PATH {:?}", db_path);

        let port = config.port.ok_or_else(|| {
            error!("No port specified in SledConfig");
            println!("===> ERROR: NO PORT SPECIFIED IN SLED CONFIG");
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;
        println!("===> USING PORT {} FOR SLED DAEMON", port);

        // Ensure the database directory exists and is writable
        if !db_path.exists() {
            info!("Creating database directory at {:?}", db_path);
            println!("===> CREATING DATABASE DIRECTORY AT {:?}", db_path);
            fs::create_dir_all(&db_path)
                .await
                .map_err(|e| {
                    error!("Failed to create database directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO CREATE DATABASE DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to create database directory at {:?}: {}", db_path, e))
                })?;
        } else if !db_path.is_dir() {
            error!("Path {:?} exists but is not a directory", db_path);
            println!("===> ERROR: PATH {:?} EXISTS BUT IS NOT A DIRECTORY", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        // Check directory permissions
        let metadata = fs::metadata(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to access directory metadata at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
            })?;
        if metadata.permissions().readonly() {
            error!("Directory at {:?} is not writable", db_path);
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        // Enhanced lock file handling
        let lock_file = db_path.join("db.lck");
        if lock_file.exists() {
            warn!("Lock file exists at {:?}", lock_file);
            println!("===> WARNING: LOCK FILE EXISTS AT {:?}", lock_file);
            fs::remove_file(&lock_file)
                .await
                .map_err(|e| {
                    error!("Failed to remove lock file at {:?}: {}", lock_file, e);
                    println!("===> ERROR: FAILED TO REMOVE LOCK FILE AT {:?}", lock_file);
                    GraphError::StorageError(format!("Failed to remove lock file at {:?}: {}", lock_file, e))
                })?;
            info!("Successfully removed lock file {:?}", lock_file);
            println!("===> SUCCESSFULLY REMOVED LOCK FILE {:?}", lock_file);
        }

        // Open database
        let db_path_clone = db_path.clone();
        let cache_capacity = config.cache_capacity.unwrap_or(1024 * 1024 * 1024);
        let use_compression = config.use_compression;
        let db = Arc::new(
            tokio::time::timeout(
                Duration::from_secs(30),
                tokio::task::spawn_blocking(move || {
                    let mut sled_config = sled::Config::new()
                        .path(db_path_clone)
                        .cache_capacity(cache_capacity)
                        .flush_every_ms(Some(100));
                    if use_compression {
                        sled_config = sled_config.use_compression(use_compression);
                    }
                    sled_config.open()
                })
            )
            .await
            .map_err(|_| {
                error!("Timeout opening Sled DB at {:?}", db_path);
                println!("===> ERROR: TIMEOUT OPENING SLED DB AT {:?}", db_path);
                GraphError::StorageError(format!("Timeout opening Sled DB at {:?}", db_path))
            })?
            .map_err(|e| {
                error!("Task panic while opening Sled DB: {:?}", e);
                println!("===> ERROR: TASK PANIC WHILE OPENING SLED DB: {:?}", e);
                GraphError::StorageError(format!("Task panic while opening Sled DB: {:?}", e))
            })?
            .map_err(|e| {
                error!("Failed to open Sled DB at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO OPEN SLED DB AT {:?}: {}", db_path, e);
                GraphError::StorageError(format!(
                    "Failed to open Sled DB at {:?}: {}. Ensure no other process is using the database.",
                    db_path, e
                ))
            })?
        );

        let integrity_check = db.was_recovered();
        info!("Sled DB opened at {:?}, was_recovered: {}", db_path, integrity_check);
        println!("===> SLED DB OPENED AT {:?}, WAS_RECOVERED: {}", db_path, integrity_check);

        // Open trees
        println!("===> OPENING SLED TREES");
        let db_clone = Arc::clone(&db);
        let (vertices, edges, kv_pairs) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(move || {
                Ok::<_, sled::Error>((
                    db_clone.open_tree("vertices")?,
                    db_clone.open_tree("edges")?,
                    db_clone.open_tree("kv_pairs")?,
                ))
            })
        )
        .await
        .map_err(|_| {
            error!("Timeout opening Sled trees");
            println!("===> ERROR: TIMEOUT OPENING SLED TREES");
            GraphError::StorageError("Timeout opening Sled trees".to_string())
        })?
        .map_err(|e: tokio::task::JoinError| {
            error!("Task panic while opening Sled trees: {:?}", e);
            println!("===> ERROR: TASK PANIC WHILE OPENING SLED TREES: {:?}", e);
            GraphError::StorageError(format!("Task panic while opening Sled trees: {:?}", e))
        })?
        .map_err(|e: sled::Error| {
            error!("Failed to open Sled trees: {}", e);
            println!("===> ERROR: FAILED TO OPEN SLED TREES: {}", e);
            GraphError::StorageError(format!("Failed to open Sled trees: {}", e))
        })?;
        println!("===> SUCCESSFULLY OPENED SLED TREES");

        // Log key count
        let kv_key_count = kv_pairs.len();
        let kv_keys: Vec<_> = kv_pairs.iter().keys().filter_map(|k| k.ok()).collect();
        info!("Initial kv_pairs key count at {:?}: {}, keys: {:?}", db_path, kv_key_count, kv_keys);
        println!("===> INITIAL KV_PAIRS KEY COUNT AT {:?}: {}, KEYS: {:?}", db_path, kv_key_count, kv_keys);

        // Initialize Raft if enabled
        #[cfg(feature = "with-openraft-sled")]
        let (raft, raft_storage) = {
            let raft_db_path = db_path.join("raft");
            if !raft_db_path.exists() {
                fs::create_dir_all(&raft_db_path)
                    .await
                    .map_err(|e| {
                        error!("Failed to create Raft DB directory at {:?}: {}", raft_db_path, e);
                        println!("===> ERROR: FAILED TO CREATE RAFT DB DIRECTORY AT {:?}", raft_db_path);
                        GraphError::StorageError(format!("Failed to create Raft DB directory at {:?}: {}", raft_db_path, e))
                    })?;
                println!("===> CREATED RAFT DB DIRECTORY AT {:?}", raft_db_path);
            }
            let raft_storage = tokio::time::timeout(Duration::from_secs(5), crate::storage::openraft::SledRaftStorage::new(&raft_db_path))
                .await
                .map_err(|_| {
                    error!("Timeout initializing Raft storage at {:?}", raft_db_path);
                    println!("===> ERROR: TIMEOUT INITIALIZING RAFT STORAGE AT {:?}", raft_db_path);
                    GraphError::StorageError(format!("Timeout initializing Raft storage at {:?}", raft_db_path))
                })?
                .map_err(|e| {
                    error!("Failed to initialize Raft storage at {:?}: {}", raft_db_path, e);
                    println!("===> ERROR: FAILED TO INITIALIZE RAFT STORAGE AT {:?}", raft_db_path);
                    GraphError::StorageError(format!("Failed to initialize Raft storage at {:?}: {}", raft_db_path, e))
                })?;
            let raft_config = openraft::RaftConfig {
                cluster_name: "graphdb-cluster".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            };
            let raft = tokio::time::timeout(
                Duration::from_secs(10),
                openraft::Raft::new(
                    port as u64,
                    Arc::new(raft_config),
                    Arc::new(raft_storage.clone()),
                    Arc::new(crate::storage::openraft::RaftTcpNetwork {}),
                )
            )
            .await
            .map_err(|_| {
                error!("Timeout initializing Raft");
                println!("===> ERROR: TIMEOUT INITIALIZING RAFT");
                GraphError::StorageError("Timeout initializing Raft".to_string())
            })?
            .map_err(|e| {
                error!("Failed to initialize Raft: {}", e);
                println!("===> ERROR: FAILED TO INITIALIZE RAFT: {}", e);
                GraphError::StorageError(format!("Failed to initialize Raft: {}", e))
            })?;
            println!("===> INITIALIZED RAFT FOR NODE {} ON PORT {}", port, port);
            (Arc::new(raft), Arc::new(raft_storage))
        };

        #[cfg(not(feature = "with-openraft-sled"))]
        let (raft, raft_storage) = (None::<Arc<()>>, None::<Arc<()>>);

        let daemon = Self {
            port,
            db_path,
            db,
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

        // Start ZeroMQ server using regular thread spawn to avoid Send issues
        let daemon_for_zmq = daemon.clone();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                println!("===> STARTING ZMQ SERVER FOR PORT {}", daemon_for_zmq.port);
                if let Err(e) = daemon_for_zmq.run_zmq_server().await {
                    error!("ZeroMQ server failed: {}", e);
                    println!("===> ERROR: ZEROMQ SERVER FAILED: {}", e);
                }
            });
        });

        // Register SIGTERM handler
        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            tokio::spawn(async move {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                warn!("Received SIGTERM in SledDaemon at {:?}", chrono::Local::now());
                println!("===> WARNING: RECEIVED SIGTERM IN SLED DAEMON AT {:?}", chrono::Local::now());
                if let Err(e) = daemon_for_signal.shutdown().await {
                    error!("Failed to shutdown SledDaemon on SIGTERM: {}", e);
                    println!("===> ERROR: FAILED TO SHUTDOWN SLED DAEMON ON SIGTERM: {}", e);
                } else {
                    info!("SledDaemon shutdown complete");
                    println!("===> SLED DAEMON SHUTDOWN COMPLETE");
                }
            });
        }

        println!("===> SLED DAEMON INITIALIZATION COMPLETE FOR PORT {}", port);
        Ok(daemon)
    }

    async fn run_zmq_server(&self) -> GraphResult<()> {
        const SOCKET_TIMEOUT_MS: i32 = 5000; // 5 seconds
        const MAX_MESSAGE_SIZE: i32 = 1024 * 1024; // 1MB max message size

        println!("===> STARTING ZMQ SERVER FOR PORT {}", self.port);
        
        // Create ZMQ context inside this method (zmq::Context is not Send)
        let context = zmq::Context::new();
        let responder = context.socket(zmq::REP)
            .map_err(|e| {
                error!("Failed to create ZeroMQ socket: {}", e);
                println!("===> ERROR: FAILED TO CREATE ZEROMQ SOCKET: {}", e);
                GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e))
            })?;

        responder.set_linger(1000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set socket linger: {}", e)))?;
        
        responder.set_rcvtimeo(SOCKET_TIMEOUT_MS)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        
        responder.set_sndtimeo(SOCKET_TIMEOUT_MS)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

        responder.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)
            .map_err(|e| GraphError::StorageError(format!("Failed to set max message size: {}", e)))?;

        let socket_path = "/opt/graphdb/pgraphdb.ipc"; // Updated to match client expectation
        let socket_dir = Path::new("/opt/graphdb");

        if !socket_dir.exists() {
            info!("Creating {} directory for IPC socket", socket_dir.display());
            println!("===> CREATING {} DIRECTORY FOR IPC SOCKET", socket_dir.display());
            tokio::fs::create_dir_all(socket_dir)
                .await
                .map_err(|e| {
                    error!("Failed to create {} directory: {}", socket_dir.display(), e);
                    println!("===> ERROR: FAILED TO CREATE {} DIRECTORY: {}", socket_dir.display(), e);
                    GraphError::StorageError(format!("Failed to create {} directory: {}", socket_dir.display(), e))
                })?;
            
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(socket_dir, std::fs::Permissions::from_mode(0o755))
                    .await
                    .map_err(|e| {
                        error!("Failed to set permissions on {} directory: {}", socket_dir.display(), e);
                        println!("===> ERROR: FAILED TO SET PERMISSIONS ON {} DIRECTORY: {}", socket_dir.display(), e);
                        GraphError::StorageError(format!("Failed to set permissions on {} directory: {}", socket_dir.display(), e))
                    })?;
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir_metadata = tokio::fs::metadata(socket_dir)
                .await
                .map_err(|e| {
                    error!("Failed to read metadata for {} directory: {}", socket_dir.display(), e);
                    println!("===> ERROR: FAILED TO READ METADATA FOR {} DIRECTORY: {}", socket_dir.display(), e);
                    GraphError::StorageError(format!("Failed to read metadata for {} directory: {}", socket_dir.display(), e))
                })?;
            let dir_permissions = dir_metadata.permissions();
            info!("Directory {} permissions: {:o}", socket_dir.display(), dir_permissions.mode() & 0o777);
            println!("===> DIRECTORY {} PERMISSIONS: {:o}", socket_dir.display(), dir_permissions.mode() & 0o777);
        }

        if tokio::fs::metadata(&socket_path).await.is_ok() {
            info!("Removing existing IPC socket file: {}", socket_path);
            println!("===> REMOVING EXISTING IPC SOCKET FILE: {}", socket_path);
            match tokio::fs::remove_file(&socket_path).await {
                Ok(_) => {
                    info!("Successfully removed existing IPC socket file: {}", socket_path);
                    println!("===> SUCCESSFULLY REMOVED EXISTING IPC SOCKET FILE: {}", socket_path);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    info!("IPC socket file {} already removed", socket_path);
                    println!("===> IPC SOCKET FILE {} ALREADY REMOVED", socket_path);
                }
                Err(e) => {
                    error!("Failed to remove existing IPC socket {}: {}", socket_path, e);
                    println!("===> ERROR: FAILED TO REMOVE EXISTING IPC SOCKET {}: {}", socket_path, e);
                    return Err(GraphError::StorageError(format!("Failed to remove existing IPC socket {}: {}", socket_path, e)));
                }
            }
        }

        let endpoint = format!("ipc://{}", socket_path);
        info!("Attempting to bind ZeroMQ socket to {}", endpoint);
        println!("===> ATTEMPTING TO BIND ZEROMQ SOCKET TO {}", endpoint);
        
        responder.bind(&endpoint)
            .map_err(|e| {
                error!("Failed to bind ZeroMQ socket to {}: {}", endpoint, e);
                println!("===> ERROR: FAILED TO BIND ZEROMQ SOCKET TO {}: {}", endpoint, e);
                GraphError::StorageError(format!("Failed to bind ZeroMQ socket on port {}: {}", self.port, e))
            })?;

        if !tokio::fs::metadata(&socket_path).await.is_ok() {
            error!("IPC socket file {} was not created after binding", socket_path);
            println!("===> ERROR: IPC SOCKET FILE {} WAS NOT CREATED AFTER BINDING", socket_path);
            return Err(GraphError::StorageError(format!("IPC socket file {} was not created after binding", socket_path)));
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o666))
                .await
                .map_err(|e| {
                    error!("Failed to set permissions on IPC socket {}: {}", socket_path, e);
                    println!("===> ERROR: FAILED TO SET PERMISSIONS ON IPC SOCKET {}: {}", socket_path, e);
                    GraphError::StorageError(format!("Failed to set permissions on IPC socket {}: {}", socket_path, e))
                })?;
            info!("Set permissions on IPC socket {} to 0o666", socket_path);
            println!("===> SET PERMISSIONS ON IPC SOCKET {} TO 0o666", socket_path);
        }

        info!("ZeroMQ server started on {}", endpoint);
        println!("===> ZEROMQ SERVER STARTED ON {}", endpoint);

        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        while *self.running.lock().await {
            let msg = match responder.recv_bytes(zmq::DONTWAIT) {
                Ok(msg) => {
                    consecutive_errors = 0;
                    debug!("Received ZeroMQ message: {:?}", String::from_utf8_lossy(&msg));
                    println!("===> RECEIVED ZEROMQ MESSAGE: {:?}", String::from_utf8_lossy(&msg));
                    msg
                }
                Err(zmq::Error::EAGAIN) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    warn!("Failed to receive ZeroMQ message (attempt {}): {}", consecutive_errors, e);
                    println!("===> WARNING: FAILED TO RECEIVE ZEROMQ MESSAGE (ATTEMPT {}): {}", consecutive_errors, e);
                    
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive ZeroMQ errors, shutting down server");
                        println!("===> ERROR: TOO MANY CONSECUTIVE ZEROMQ ERRORS, SHUTTING DOWN SERVER");
                        break;
                    }
                    
                    tokio::time::sleep(Duration::from_millis(100)).await;
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
                    self.send_zmq_response(&responder, &json!({ 
                        "status": "error", 
                        "message": format!("Failed to parse request: {}", e) 
                    })).await;
                    continue;
                }
            };

            let response = match request.get("command").and_then(|c| c.as_str()) {
                Some("set_key") => {
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            let response = json!({ "status": "error", "message": "Missing key in set_key request" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    let value = match request.get("value").and_then(|v| v.as_str()) {
                        Some(v) => v,
                        None => {
                            let response = json!({ "status": "error", "message": "Missing value in set_key request" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    
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
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            let response = json!({ "status": "error", "message": "Missing key in get_key request" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    
                    println!("===> PROCESSING GET_KEY: key={}", key);
                    match self.retrieve(key.as_bytes()).await {
                        Ok(Some(val)) => {
                            let value_str = String::from_utf8_lossy(&val).to_string();
                            println!("===> GET_KEY SUCCESS: key={}, value={}", key, value_str);
                            json!({ "status": "success", "value": value_str })
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
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            let response = json!({ "status": "error", "message": "Missing key in delete_key request" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    
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
                Some("flush") => {
                    println!("===> PROCESSING FLUSH");
                    match self.db.flush_async().await {
                        Ok(bytes_flushed) => {
                            info!("Flushed {} bytes for database at {:?}", bytes_flushed, self.db_path);
                            println!("===> FLUSH SUCCESS: {} bytes", bytes_flushed);
                            
                            #[cfg(feature = "with-openraft-sled")]
                            {
                                let request = openraft::raft::ClientWriteRequest::new(
                                    openraft::EntryPayload::AppWrite {
                                        key: b"flush".to_vec(),
                                        value: vec![],
                                    }
                                );
                                match self.raft.client_write(request).await {
                                    Ok(_) => {
                                        info!("Raft flush replicated at {:?}", self.db_path);
                                        println!("===> FLUSH: RAFT FLUSH REPLICATED AT {:?}", self.db_path);
                                        json!({ "status": "success", "bytes_flushed": bytes_flushed })
                                    }
                                    Err(e) => {
                                        error!("Raft flush replication failed: {}", e);
                                        println!("===> FLUSH ERROR: RAFT FLUSH REPLICATION FAILED: {}", e);
                                        json!({ "status": "error", "message": format!("Raft flush replication failed: {}", e) })
                                    }
                                }
                            }
                            
                            #[cfg(not(feature = "with-openraft-sled"))]
                            json!({ "status": "success", "bytes_flushed": bytes_flushed })
                        }
                        Err(e) => {
                            error!("Failed to flush database at {:?}: {}", self.db_path, e);
                            println!("===> FLUSH ERROR: {}", e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("get_all_vertices") => {
                    println!("===> PROCESSING GET_ALL_VERTICES");
                    let vertices: Vec<Vertex> = self.vertices.iter()
                        .filter_map(|res| res.ok())
                        .filter_map(|(_k, v)| Self::deserialize_from_ivec(v).ok())
                        .collect();
                    match serde_json::to_value(&vertices) {
                        Ok(value) => {
                            println!("===> GET_ALL_VERTICES SUCCESS: {} vertices", vertices.len());
                            json!({ "status": "success", "vertices": value })
                        }
                        Err(e) => {
                            error!("Failed to serialize vertices: {}", e);
                            println!("===> GET_ALL_VERTICES ERROR: {}", e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("get_all_edges") => {
                    println!("===> PROCESSING GET_ALL_EDGES");
                    let edges: Vec<Edge> = self.edges.iter()
                        .filter_map(|res| res.ok())
                        .filter_map(|(_k, v)| Self::deserialize_from_ivec(v).ok())
                        .collect();
                    match serde_json::to_value(&edges) {
                        Ok(value) => {
                            println!("===> GET_ALL_EDGES SUCCESS: {} edges", edges.len());
                            json!({ "status": "success", "edges": value })
                        }
                        Err(e) => {
                            error!("Failed to serialize edges: {}", e);
                            println!("===> GET_ALL_EDGES ERROR: {}", e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("clear_data") => {
                    println!("===> PROCESSING CLEAR_DATA");
                    match self.db.clear() {
                        Ok(_) => {
                            // Clear all trees - handle errors without using ?
                            let vertices_result = self.vertices.clear();
                            let edges_result = self.edges.clear();
                            let kv_pairs_result = self.kv_pairs.clear();
                            
                            if let Err(e) = vertices_result {
                                json!({ "status": "error", "message": format!("Failed to clear vertices: {}", e) })
                            } else if let Err(e) = edges_result {
                                json!({ "status": "error", "message": format!("Failed to clear edges: {}", e) })
                            } else if let Err(e) = kv_pairs_result {
                                json!({ "status": "error", "message": format!("Failed to clear kv_pairs: {}", e) })
                            } else {
                                // All clears successful, now flush
                                match self.db.flush_async().await {
                                    Ok(bytes_flushed) => {
                                        info!("Cleared database at {:?}, flushed {} bytes", self.db_path, bytes_flushed);
                                        println!("===> CLEAR_DATA SUCCESS: Flushed {} bytes", bytes_flushed);
                                        
                                        #[cfg(feature = "with-openraft-sled")]
                                        {
                                            let request = openraft::raft::ClientWriteRequest::new(
                                                openraft::EntryPayload::AppWrite {
                                                    key: b"clear_data".to_vec(),
                                                    value: vec![],
                                                }
                                            );
                                            match self.raft.client_write(request).await {
                                                Ok(_) => {
                                                    info!("Raft clear_data replicated at {:?}", self.db_path);
                                                    println!("===> CLEAR_DATA: RAFT CLEAR_DATA REPLICATED AT {:?}", self.db_path);
                                                    json!({ "status": "success", "bytes_flushed": bytes_flushed })
                                                }
                                                Err(e) => {
                                                    error!("Raft clear_data replication failed: {}", e);
                                                    println!("===> CLEAR_DATA ERROR: RAFT CLEAR_DATA REPLICATION FAILED: {}", e);
                                                    json!({ "status": "error", "message": format!("Raft clear_data replication failed: {}", e) })
                                                }
                                            }
                                        }
                                        
                                        #[cfg(not(feature = "with-openraft-sled"))]
                                        json!({ "status": "success", "bytes_flushed": bytes_flushed })
                                    }
                                    Err(e) => {
                                        error!("Failed to flush after clearing database at {:?}: {}", self.db_path, e);
                                        println!("===> CLEAR_DATA ERROR: Failed to flush: {}", e);
                                        json!({ "status": "error", "message": e.to_string() })
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to clear database at {:?}: {}", self.db_path, e);
                            println!("===> CLEAR_DATA ERROR: {}", e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some(cmd) => {
                    error!("Unsupported command: {}", cmd);
                    println!("===> ERROR: UNSUPPORTED ZEROMQ COMMAND: {}", cmd);
                    json!({ "status": "error", "message": format!("Unsupported command: {}", cmd) })
                }
                None => {
                    error!("No command specified in request: {:?}", request);
                    println!("===> ERROR: NO COMMAND SPECIFIED IN ZEROMQ REQUEST: {:?}", request);
                    json!({ "status": "error", "message": "No command specified" })
                }
            };

            self.send_zmq_response(&responder, &response).await;
        }

        info!("ZeroMQ server stopped for port {}", self.port);
        println!("===> ZEROMQ SERVER STOPPED FOR PORT {}", self.port);
        Ok(())
    }

    // Helper method for sending ZMQ responses
    async fn send_zmq_response(&self, responder: &zmq::Socket, response: &Value) {
        match serde_json::to_vec(response) {
            Ok(response_data) => {
                debug!("Sending ZeroMQ response: {:?}", response);
                println!("===> SENDING ZEROMQ RESPONSE: {:?}", response);
                
                if let Err(e) = responder.send(&response_data, 0) {
                    warn!("Failed to send ZeroMQ response: {}", e);
                    println!("===> WARNING: FAILED TO SEND ZEROMQ RESPONSE: {}", e);
                } else {
                    println!("===> ZEROMQ RESPONSE SENT SUCCESSFULLY");
                }
            }
            Err(e) => {
                error!("Failed to serialize ZeroMQ response: {}", e);
                println!("===> ERROR: FAILED TO SERIALIZE ZEROMQ RESPONSE: {}", e);
                
                // Send error response as fallback
                let error_response = format!(r#"{{"status":"error","message":"Serialization error: {}"}}"#, e);
                if let Err(send_err) = responder.send(error_response.as_bytes(), 0) {
                    error!("Failed to send error response: {}", send_err);
                    println!("===> ERROR: FAILED TO SEND ERROR RESPONSE: {}", send_err);
                }
            }
        }
    }

    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down SledDaemon at path {:?}", self.db_path);
        println!("===> SHUTTING DOWN SLED DAEMON AT PATH {:?}", self.db_path);
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

    pub async fn is_running(&self) -> bool {
        *self.running.lock().await
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

    pub async fn flush(&self) -> GraphResult<()> {
        info!("SledDaemon::flush - Sending flush request to ZeroMQ server on port {}", self.port);
        println!("===> SLED DAEMON FLUSH - SENDING FLUSH REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        socket.connect("ipc:///opt/graphdb/graphdb.ipc")
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket: {}", e)))?;

        let request = json!({ "command": "flush" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send flush request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive flush response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            info!("SledDaemon::flush - Successfully flushed database via ZeroMQ");
            println!("===> SLED DAEMON FLUSH - SUCCESSFULLY FLUSHED DATABASE VIA ZEROMQ");
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemon::flush - Failed: {}", error_msg);
            println!("===> SLED DAEMON FLUSH - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        info!("SledDaemon::get_all_vertices - Sending request to ZeroMQ server on port {}", self.port);
        println!("===> SLED DAEMON GET_ALL_VERTICES - SENDING REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        socket.connect("ipc:///opt/graphdb/graphdb.ipc")
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket: {}", e)))?;

        let request = json!({ "command": "get_all_vertices" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send get_all_vertices request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive get_all_vertices response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            let vertices: Vec<Vertex> = serde_json::from_value(response["vertices"].clone())
                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize vertices: {}", e)))?;
            info!("SledDaemon::get_all_vertices - Retrieved {} vertices", vertices.len());
            println!("===> SLED DAEMON GET_ALL_VERTICES - RETRIEVED {} VERTICES", vertices.len());
            Ok(vertices)
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemon::get_all_vertices - Failed: {}", error_msg);
            println!("===> SLED DAEMON GET_ALL_VERTICES - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        info!("SledDaemon::get_all_edges - Sending request to ZeroMQ server on port {}", self.port);
        println!("===> SLED DAEMON GET_ALL_EDGES - SENDING REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        socket.connect("ipc:///opt/graphdb/graphdb.ipc")
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket: {}", e)))?;

        let request = json!({ "command": "get_all_edges" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send get_all_edges request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive get_all_edges response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            let edges: Vec<Edge> = serde_json::from_value(response["edges"].clone())
                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edges: {}", e)))?;
            info!("SledDaemon::get_all_edges - Retrieved {} edges", edges.len());
            println!("===> SLED DAEMON GET_ALL_EDGES - RETRIEVED {} EDGES", edges.len());
            Ok(edges)
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemon::get_all_edges - Failed: {}", error_msg);
            println!("===> SLED DAEMON GET_ALL_EDGES - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn clear_data(&self) -> GraphResult<()> {
        info!("SledDaemon::clear_data - Sending clear_data request to ZeroMQ server on port {}", self.port);
        println!("===> SLED DAEMON CLEAR_DATA - SENDING CLEAR_DATA REQUEST TO ZEROMQ SERVER ON PORT {}", self.port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        socket.connect("ipc:///opt/graphdb/graphdb.ipc")
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket: {}", e)))?;

        let request = json!({ "command": "clear_data" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send clear_data request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive clear_data response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            info!("SledDaemon::clear_data - Successfully cleared database via ZeroMQ");
            println!("===> SLED DAEMON CLEAR_DATA - SUCCESSFULLY CLEARED DATABASE VIA ZEROMQ");
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemon::clear_data - Failed: {}", error_msg);
            println!("===> SLED DAEMON CLEAR_DATA - FAILED: {}", error_msg);
            Err(GraphError::StorageError(error_msg))
        }
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
            println!("In Sled Daemon Pool ===> ERROR: DAEMON ALREADY EXISTS ON PORT {}", port);
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
                "In Sled Daemon Pool - Daemon already registered on port {}",
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
                .map(|p| p.join("storage_config.yaml")),
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

    // initialize_cluster method - simplified to work with existing data structure
    pub async fn initialize_cluster(
        &mut self,
        storage_config: &StorageConfig,
        config: &SledConfig,
        cli_port: Option<u16>,
    ) -> GraphResult<()> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("SledDaemonPool already initialized, skipping");
            println!("===> WARNING: SLED DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            return Ok(());
        }

        // Use CLI-provided port if available, otherwise use config port or default
        let port = cli_port.unwrap_or(config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        
        // Create port-specific path: sled/<port> structure to prevent lock contention
        let db_path = if let path = &config.path {
            // If path is explicitly set, ensure it follows sled/<port> structure
            if path.ends_with(&port.to_string()) {
                path.clone()
            } else {
                path.join(port.to_string())
            }
        } else {
            // Create default port-specific path: base_directory/sled/<port>
            storage_config
                .data_directory
                .as_ref()
                .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                .join("sled")
                .join(port.to_string())
        };

        info!("Initializing cluster on port {} with path {:?}", port, db_path);
        println!("===> INITIALIZING CLUSTER ON PORT {} WITH PATH {:?}", port, db_path);

        if storage_config.use_raft_for_scale {
            warn!("Raft clustering is enabled, but only a single daemon will be initialized on port {}", port);
            println!("===> WARNING: RAFT CLUSTERING ENABLED, BUT ONLY SINGLE DAEMON INITIALIZED ON PORT {}", port);
        }

        // Check for existing daemon in GLOBAL_DAEMON_REGISTRY
        if let Some(metadata) = GLOBAL_DAEMON_REGISTRY.get_daemon_metadata(port).await? {
            info!("Found existing daemon metadata on port {} at path {:?}", port, metadata.data_dir);
            println!("===> FOUND EXISTING DAEMON METADATA ON PORT {} AT PATH {:?}", port, metadata.data_dir);
            if let Some(registered_path) = &metadata.data_dir {
                if registered_path == &db_path {
                    // Path matches, skip initialization
                    info!("Reusing existing daemon on port {} with matching path {:?}", port, registered_path);
                    println!("===> REUSING EXISTING DAEMON ON PORT {} WITH MATCHING PATH {:?}", port, registered_path);
                    *initialized = true;
                    return Ok(());
                } else {
                    // Path mismatch: clean up old daemon
                    warn!("Path mismatch: daemon registry shows {:?}, but config specifies {:?}", registered_path, db_path);
                    println!("===> PATH MISMATCH: DAEMON REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", registered_path, db_path);
                    if registered_path.exists() {
                        warn!("Old path {:?} still exists. Attempting cleanup.", registered_path);
                        println!("===> OLD PATH {:?} STILL EXISTS. ATTEMPTING CLEANUP.", registered_path);
                        fs::remove_dir_all(registered_path)
                            .await
                            .map_err(|e| {
                                error!("Failed to remove old directory at {:?}: {}", registered_path, e);
                                println!("===> ERROR: FAILED TO REMOVE OLD DIRECTORY AT {:?}: {}", registered_path, e);
                                GraphError::StorageError(format!("Failed to remove old directory at {:?}: {}", registered_path, e))
                            })?;
                        info!("Successfully removed old directory at {:?}", registered_path);
                        println!("===> SUCCESSFULLY REMOVED OLD DIRECTORY AT {:?}", registered_path);
                    }
                    timeout(Duration::from_secs(5), GLOBAL_DAEMON_REGISTRY.unregister_daemon(port))
                        .await
                        .map_err(|_| {
                            warn!("Timeout unregistering daemon on port {}", port);
                            println!("===> WARNING: TIMEOUT UNREGISTERING DAEMON ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout unregistering daemon on port {}", port))
                        })?;
                    info!("Unregistered old daemon entry for port {}", port);
                    println!("===> UNREGISTERED OLD DAEMON ENTRY FOR PORT {}", port);
                }
            }
        }

        // Ensure directory exists and is writable
        if !db_path.exists() {
            info!("Creating Sled directory at {:?}", db_path);
            println!("===> CREATING SLED DIRECTORY AT {:?}", db_path);
            fs::create_dir_all(&db_path)
                .await
                .map_err(|e| {
                    error!("Failed to create Sled directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO CREATE SLED DIRECTORY AT {:?}: {}", db_path, e);
                    GraphError::Io(e)
                })?;
        } else {
            let metadata = fs::metadata(&db_path)
                .await
                .map_err(|e| {
                    error!("Failed to access directory metadata at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
                })?;
            if metadata.permissions().readonly() {
                error!("Directory at {:?} is not writable", db_path);
                println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
                return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
            }
        }

        // Initialize SledDaemon with updated config
        let mut daemon_config = config.clone();
        daemon_config.path = db_path.clone();
        daemon_config.port = Some(port);
        let daemon = SledDaemon::new(&daemon_config).await?;

        // Register daemon in GLOBAL_DAEMON_REGISTRY
        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(db_path.clone()),
            config_path: None, // No config_path field in StorageConfig
            engine_type: Some(StorageEngineType::Sled.to_string()),
            last_seen_nanos: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        };

        timeout(Duration::from_secs(5), GLOBAL_DAEMON_REGISTRY.register_daemon(daemon_metadata))
            .await
            .map_err(|_| {
                error!("Timeout registering daemon on port {}", port);
                println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", port);
                GraphError::StorageError(format!("Timeout registering daemon on port {}", port))
            })??;
        info!("Registered daemon on port {}", port);
        println!("===> REGISTERED DAEMON ON PORT {}", port);

        // Store daemon in pool
        self.daemons.insert(port, Arc::new(daemon));
        *initialized = true;
        info!("SledDaemonPool initialized successfully on port {}", port);
        println!("===> SLED DAEMON POOL INITIALIZED SUCCESSFULLY ON PORT {}", port);

        Ok(())
    }
    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down SledDaemonPool");
        println!("===> SHUTTING DOWN SLED DAEMON POOL");
        let mut daemons = Vec::new();
        for daemon in self.daemons.values() {
            daemons.push(daemon.shutdown());
        }
        join_all(daemons).await.into_iter().collect::<Result<Vec<_>, _>>()?;
        *self.initialized.write().await = false;
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

    pub async fn close(&self, target_port: Option<u16>) -> GraphResult<()> {
        info!("Closing SledDaemonPool");
        println!("===> CLOSING SLED DAEMON POOL");

        if let Some(port) = target_port {
            // Close specific daemon by port
            if let Some(daemon) = self.daemons.get(&port) {
                info!("Closing SledDaemon on port {}", port);
                println!("===> CLOSING SLED DAEMON ON PORT {}", port);
                daemon.shutdown().await?;
            } else {
                warn!("No daemon found on port {} to close", port);
                println!("===> WARNING: NO DAEMON FOUND ON PORT {} TO CLOSE", port);
            }
        } else {
            // Close all daemons
            let ports: Vec<u16> = self.daemons.keys().copied().collect();
            for port in ports {
                if let Some(daemon) = self.daemons.get(&port) {
                    info!("Closing SledDaemon on port {}", port);
                    println!("===> CLOSING SLED DAEMON ON PORT {}", port);
                    if let Err(e) = daemon.shutdown().await {
                        error!("Failed to close daemon on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO CLOSE DAEMON ON PORT {}: {}", port, e);
                    }
                }
            }
        }

        info!("SledDaemonPool closed successfully");
        println!("===> SLED DAEMON POOL CLOSED SUCCESSFULLY");
        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        !self.daemons.is_empty() && {
            let mut all_running = true;
            for daemon in self.daemons.values() {
                if !daemon.is_running().await {
                    all_running = false;
                    break;
                }
            }
            all_running
        }
    }

    pub async fn get_daemon_count(&self) -> usize {
        self.daemons.len()
    }

    pub async fn get_active_ports(&self) -> Vec<u16> {
        let mut active_ports = Vec::new();
        for (&port, daemon) in &self.daemons {
            if daemon.is_running().await {
                active_ports.push(port);
            }
        }
        active_ports.sort();
        active_ports
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
                        node: BasxicNode { addr: addr.clone() },
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

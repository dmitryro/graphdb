use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io::{Cursor, Read, Write};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::time::{sleep, Duration, timeout, interval};
use tokio::fs;
use log::{info, debug, warn, error};
pub use crate::config::{
    RocksDBConfig, RocksDBDaemon, RocksDBDaemonPool, StorageConfig, StorageEngineType,
    DAEMON_REGISTRY_DB_PATH, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT,
    ReplicationStrategy, NodeHealth, LoadBalancer, HealthCheckConfig,
};
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
use zmq::{Context as ZmqContext};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use async_trait::async_trait;
use super::rocksdb_client::{ RocksDBClient };
#[cfg(feature = "with-openraft-rocksdb")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode},
    openraft_rocksdb::RocksDBRaftStorage,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

impl RocksDBDaemon {
    pub async fn new(config: RocksDBConfig) -> GraphResult<Self> {
        println!("RocksDBDaemon =================> LET US SEE IF THIS WAS EVER CALLED");
        let port = config.port.ok_or_else(|| {
            GraphError::ConfigurationError("No port specified in RocksDBConfig".to_string())
        })?;

        let db_path = if config.path.ends_with(&port.to_string()) {
            config.path.clone()
        } else {
            config.path.join(port.to_string())
        };

        println!("===> Initializing RocksDB daemon at {:?}", db_path);

        if !db_path.exists() {
            println!("===> Creating database directory at {:?}", db_path);
            fs::create_dir_all(&db_path).await.map_err(|e| {
                GraphError::StorageError(format!("Failed to create directory at {:?}: {}", db_path, e))
            })?;
        } else if !db_path.is_dir() {
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        let metadata = fs::metadata(&db_path).await.map_err(|e| {
            GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
        })?;
        if metadata.permissions().readonly() {
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        let db_path_clone = db_path.clone();
        let cache_capacity = config.cache_capacity.unwrap_or(1024 * 1024 * 1024);
        let db = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::task::spawn_blocking(move || {
                let mut opts = Options::default();
                opts.create_if_missing(true);
                opts.set_write_buffer_size(cache_capacity as usize);
                opts.set_max_write_buffer_number(3);
                DB::open(&opts, db_path_clone)
            })
        )
        .await
        .map_err(|_| GraphError::StorageError("Timeout opening RocksDB".to_string()))??
        .map_err(|e| {
            GraphError::StorageError(format!("Failed to open RocksDB: {}. Ensure no other process is using the database.", e))
        })?;
        let db = Arc::new(db);
        println!("===> RocksDB opened at {:?}", db_path);

        #[cfg(feature = "with-openraft-rocksdb")]
        let (raft, raft_storage) = {
            let raft_db_path = db_path.join("raft");
            if !raft_db_path.exists() {
                fs::create_dir_all(&raft_db_path).await.map_err(|e| {
                    GraphError::StorageError(format!("Failed to create Raft DB directory at {:?}: {}", raft_db_path, e))
                })?;
            }
            let raft_storage = RocksDBRaftStorage::new(&raft_db_path).await?;

            let raft_config = Arc::new(RaftConfig {
                cluster_name: "graphdb-cluster".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            });

            let raft = Raft::new(
                port as u64,
                raft_config,
                Arc::new(raft_storage.clone()),
                Arc::new(crate::storage::openraft::RaftTcpNetwork {}),
            );
            println!("===> Initialized Raft for node {} on port {}", port, port);
            (Some(Arc::new(raft)), Some(Arc::new(raft_storage)))
        };

        let daemon = Self {
            port,
            db_path,
            db,
            running: Arc::new(TokioMutex::new(true)),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id: port as u64,
        };

        println!("===> Time to run ZeroMQ server");
        let daemon_for_zmq = daemon.clone();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                println!("===> Starting ZMQ server for port {}", daemon_for_zmq.port);
                if let Err(e) = daemon_for_zmq.run_zmq_server().await {
                    eprintln!("===> ERROR: ZeroMQ server failed: {}", e);
                }
            });
        });

        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            tokio::spawn(async move {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                println!("===> WARNING: Received SIGTERM in RocksDB daemon");
                if let Err(e) = daemon_for_signal.shutdown().await {
                    eprintln!("===> ERROR: Failed to shutdown RocksDB daemon on SIGTERM: {}", e);
                } else {
                    println!("===> RocksDB daemon shutdown complete");
                }
            });
        }

        println!("===> RocksDB daemon initialization complete for port {}", port);
        Ok(daemon)
    }

    pub async fn new_with_db(config: RocksDBConfig, existing_db: Arc<rocksdb::DB>) -> GraphResult<Self> {
        let port = config.port.ok_or_else(|| {
            GraphError::ConfigurationError("No port specified in RocksDBConfig".to_string())
        })?;

        let db_path = if config.path.ends_with(&port.to_string()) {
            config.path.clone()
        } else {
            config.path.join(port.to_string())
        };

        println!("===> Initializing RocksDB daemon with existing DB at {:?}", db_path);

        println!("===> Successfully opened RocksDB from existing DB");

        #[cfg(feature = "with-openraft-rocksdb")]
        let (raft, raft_storage) = {
            let raft_db_path = db_path.join("raft");
            if !raft_db_path.exists() {
                fs::create_dir_all(&raft_db_path).await.map_err(|e| {
                    GraphError::StorageError(format!("Failed to create Raft DB directory at {:?}: {}", raft_db_path, e))
                })?;
            }
            let raft_storage = RocksDBRaftStorage::new(&raft_db_path).await?;

            let raft_config = Arc::new(RaftConfig {
                cluster_name: "graphdb-cluster".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            });

            let raft = Raft::new(
                port as u64,
                raft_config,
                Arc::new(raft_storage.clone()),
                Arc::new(crate::storage::openraft::RaftTcpNetwork {}),
            );
            println!("===> Initialized Raft for node {} on port {}", port, port);
            (Some(Arc::new(raft)), Some(Arc::new(raft_storage)))
        };

        let daemon = Self {
            port,
            db_path,
            db: existing_db,
            running: Arc::new(TokioMutex::new(true)),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id: port as u64,
        };

        println!("===> Time to run ZeroMQ server");
        let daemon_for_zmq = daemon.clone();
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                println!("===> Starting ZMQ server for port {}", daemon_for_zmq.port);
                if let Err(e) = daemon_for_zmq.run_zmq_server().await {
                    eprintln!("===> ERROR: ZeroMQ server failed: {}", e);
                }
            });
        });

        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            tokio::spawn(async move {
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                println!("===> WARNING: Received SIGTERM in RocksDB daemon");
                if let Err(e) = daemon_for_signal.shutdown().await {
                    eprintln!("===> ERROR: Failed to shutdown RocksDB daemon on SIGTERM: {}", e);
                } else {
                    println!("===> RocksDB daemon shutdown complete");
                }
            });
        }

        println!("===> RocksDB daemon initialization complete for port {}", port);
        Ok(daemon)
    }

    async fn run_zmq_server(&self) -> GraphResult<()> {
        const SOCKET_TIMEOUT_MS: i32 = 10000;
        const MAX_MESSAGE_SIZE: i32 = 1024 * 1024;

        println!("===> STARTING ZMQ SERVER FOR PORT {}", self.port);
        
        let context = zmq::Context::new();
        let responder = context.socket(zmq::REP)
            .map_err(|e| {
                error!("Failed to create ZeroMQ socket for port {}: {}", self.port, e);
                println!("===> ERROR: FAILED TO CREATE ZEROMQ SOCKET FOR PORT {}: {}", self.port, e);
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

        let socket_path = format!("/opt/graphdb/graphdb-{}.ipc", self.port);
        let socket_dir = Path::new("/opt/graphdb");

        if !socket_dir.exists() {
            info!("Creating {} directory for IPC socket", socket_dir.display());
            println!("===> CREATING {} DIRECTORY FOR IPC SOCKET", socket_dir.display());
            fs::create_dir_all(socket_dir)
                .await
                .map_err(|e| {
                    error!("Failed to create {} directory: {}", socket_dir.display(), e);
                    println!("===> ERROR: FAILED TO CREATE {} DIRECTORY: {}", socket_dir.display(), e);
                    GraphError::StorageError(format!("Failed to create {} directory: {}", socket_dir.display(), e))
                })?;
            
            #[cfg(unix)]
            {
                fs::set_permissions(socket_dir, std::fs::Permissions::from_mode(0o755))
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
            let dir_metadata = fs::metadata(socket_dir)
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

        if fs::metadata(&socket_path).await.is_ok() {
            info!("Removing existing IPC socket file: {}", socket_path);
            println!("===> REMOVING EXISTING IPC SOCKET FILE: {}", socket_path);
            match fs::remove_file(&socket_path).await {
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
        info!("Attempting to bind ZeroMQ socket to {} for port {}", endpoint, self.port);
        println!("===> ATTEMPTING TO BIND ZEROMQ SOCKET TO {} FOR PORT {}", endpoint, self.port);
        
        if let Err(e) = responder.bind(&endpoint) {
            error!("Failed to bind ZeroMQ socket to {} for port {}: {}", endpoint, self.port, e);
            println!("===> ERROR: FAILED TO BIND ZEROMQ SOCKET TO {} FOR PORT {}: {}", endpoint, self.port, e);
            return Err(GraphError::StorageError(format!("Failed to bind ZeroMQ socket on port {}: {}", self.port, e)));
        }

        if !fs::metadata(&socket_path).await.is_ok() {
            error!("IPC socket file {} was not created after binding for port {}", socket_path, self.port);
            println!("===> ERROR: IPC SOCKET FILE {} WAS NOT CREATED AFTER BINDING FOR PORT {}", socket_path, self.port);
            return Err(GraphError::StorageError(format!("IPC socket file {} was not created after binding for port {}", socket_path, self.port)));
        }

        #[cfg(unix)]
        {
            fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o666))
                .await
                .map_err(|e| {
                    error!("Failed to set permissions on IPC socket {} for port {}: {}", socket_path, self.port, e);
                    println!("===> ERROR: FAILED TO SET PERMISSIONS ON IPC SOCKET {} FOR PORT {}: {}", socket_path, self.port, e);
                    GraphError::StorageError(format!("Failed to set permissions on IPC socket {} for port {}: {}", socket_path, self.port, e))
                })?;
            info!("Set permissions on IPC socket {} to 0o666 for port {}", socket_path, self.port);
            println!("===> SET PERMISSIONS ON IPC SOCKET {} TO 0o666 FOR PORT {}", socket_path, self.port);
        }

        info!("ZeroMQ server started on {} for port {}", endpoint, self.port);
        println!("===> ZEROMQ SERVER STARTED ON {} FOR PORT {}", endpoint, self.port);

        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        while *self.running.lock().await {
            let msg = match responder.recv_bytes(zmq::DONTWAIT) {
                Ok(msg) => {
                    consecutive_errors = 0;
                    debug!("Received ZeroMQ message for port {}: {:?}", self.port, String::from_utf8_lossy(&msg));
                    println!("===> RECEIVED ZEROMQ MESSAGE FOR PORT {}: {:?}", self.port, String::from_utf8_lossy(&msg));
                    msg
                }
                Err(zmq::Error::EAGAIN) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    warn!("Failed to receive ZeroMQ message for port {} (attempt {}): {}", self.port, consecutive_errors, e);
                    println!("===> WARNING: FAILED TO RECEIVE ZEROMQ MESSAGE FOR PORT {} (ATTEMPT {}): {}", self.port, consecutive_errors, e);
                    
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive ZeroMQ errors for port {}, shutting down server", self.port);
                        println!("===> ERROR: TOO MANY CONSECUTIVE ZEROMQ ERRORS FOR PORT {}, SHUTTING DOWN SERVER", self.port);
                        break;
                    }
                    
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            let request: Value = match serde_json::from_slice(&msg) {
                Ok(req) => {
                    debug!("Parsed request for port {}: {:?}", self.port, req);
                    println!("===> PARSED ZEROMQ REQUEST FOR PORT {}: {:?}", self.port, req);
                    req
                }
                Err(e) => {
                    error!("Failed to parse ZeroMQ request for port {}: {}", self.port, e);
                    println!("===> ERROR: FAILED TO PARSE ZEROMQ REQUEST FOR PORT {}: {}", self.port, e);
                    self.send_zmq_response(&responder, &json!({ 
                        "status": "error", 
                        "message": format!("Failed to parse request: {}", e) 
                    })).await;
                    continue;
                }
            };

            let response = match request.get("command").and_then(|c| c.as_str()) {
                Some("status") => {
                    println!("===> PROCESSING STATUS FOR PORT {}", self.port);
                    json!({ "status": "success", "port": self.port })
                }
                Some("ping") => {
                    println!("===> PROCESSING PING FOR PORT {}", self.port);
                    json!({ "status": "success", "port": self.port })
                }
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
                    
                    println!("===> PROCESSING SET_KEY FOR PORT {}: key={}, value={}", self.port, key, value);
                    let cf_handle = match self.db.cf_handle("kv_pairs") {
                        Some(cf) => cf,
                        None => {
                            let response = json!({ "status": "error", "message": "Column family kv_pairs not found" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    match self.db.put_cf(&cf_handle, key.as_bytes(), value.as_bytes()) {
                        Ok(_) => {
                            println!("===> SET_KEY SUCCESS FOR PORT {}: key={}", self.port, key);
                            json!({ "status": "success" })
                        }
                        Err(e) => {
                            println!("===> SET_KEY ERROR FOR PORT {}: key={}, error={}", self.port, key, e);
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
                    let cf_handle = match self.db.cf_handle("kv_pairs") {
                        Some(cf) => cf,
                        None => {
                            let response = json!({ "status": "error", "message": "Column family kv_pairs not found" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    match self.db.get_cf(&cf_handle, key.as_bytes()) {
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
                    let cf_handle = match self.db.cf_handle("kv_pairs") {
                        Some(cf) => cf,
                        None => {
                            let response = json!({ "status": "error", "message": "Column family kv_pairs not found" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    match self.db.delete_cf(&cf_handle, key.as_bytes()) {
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
                    match self.db.flush() {
                        Ok(_) => {
                            println!("===> FLUSH SUCCESS");
                            json!({ "status": "success", "bytes_flushed": 0 }) // RocksDB flush doesn't return bytes
                        }
                        Err(e) => {
                            error!("Failed to flush database at {:?}: {}", self.db_path, e);
                            println!("===> FLUSH ERROR: {}", e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some("clear_data") => {
                    println!("===> PROCESSING CLEAR_DATA");
                    let cf_handle = match self.db.cf_handle("kv_pairs") {
                        Some(cf) => cf,
                        None => {
                            let response = json!({ "status": "error", "message": "Column family kv_pairs not found" });
                            self.send_zmq_response(&responder, &response).await;
                            continue;
                        }
                    };
                    let iterator = self.db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                    let mut batch = rocksdb::WriteBatch::default();
                    for item in iterator {
                        match item {
                            Ok((key, _)) => batch.delete_cf(&cf_handle, &key),
                            Err(e) => {
                                let response = json!({ "status": "error", "message": format!("Iterator error: {}", e) });
                                self.send_zmq_response(&responder, &response).await;
                                continue;
                            }
                        }
                    }
                    match self.db.write(batch) {
                        Ok(_) => {
                            match self.db.flush() {
                                Ok(_) => {
                                    info!("Cleared kv_pairs at {:?}", self.db_path);
                                    println!("===> CLEAR_DATA SUCCESS");
                                    json!({ "status": "success", "bytes_flushed": 0 })
                                }
                                Err(e) => {
                                    error!("Failed to flush after clearing kv_pairs at {:?}: {}", self.db_path, e);
                                    println!("===> CLEAR_DATA ERROR: Failed to flush: {}", e);
                                    json!({ "status": "error", "message": e.to_string() })
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to clear kv_pairs at {:?}: {}", self.db_path, e);
                            println!("===> CLEAR_DATA ERROR: {}", e);
                            json!({ "status": "error", "message": e.to_string() })
                        }
                    }
                }
                Some(cmd) => {
                    error!("Unsupported command for port {}: {}", self.port, cmd);
                    println!("===> ERROR: UNSUPPORTED ZEROMQ COMMAND FOR PORT {}: {}", self.port, cmd);
                    json!({ "status": "error", "message": format!("Unsupported command: {}", cmd) })
                }
                None => {
                    error!("No command specified in request for port {}: {:?}", self.port, request);
                    println!("===> ERROR: NO COMMAND SPECIFIED IN ZEROMQ REQUEST FOR PORT {}: {:?}", self.port, request);
                    json!({ "status": "error", "message": "No command specified" })
                }
            };

            self.send_zmq_response(&responder, &response).await;
        }

        info!("ZeroMQ server stopped for port {}", self.port);
        println!("===> ZEROMQ SERVER STOPPED FOR PORT {}", self.port);
        Ok(())
    }

    async fn send_zmq_response(&self, responder: &zmq::Socket, response: &Value) {
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

    async fn send_zmq_request(&self, port: u16, request: Value) -> GraphResult<Value> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;

        socket.set_rcvtimeo(10000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        socket.set_sndtimeo(10000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

        let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", port);
        println!("===> ROCKSDB STORAGE: CONNECTING TO ZMQ ENDPOINT {} FOR PORT {}", endpoint, port);

        socket.connect(&endpoint)
            .map_err(|e| {
                error!("Failed to connect to ZeroMQ socket {} for port {}: {}", endpoint, port, e);
                println!("===> ERROR: FAILED TO CONNECT TO ZMQ SOCKET {} FOR PORT {}: {}", endpoint, port, e);
                GraphError::StorageError(format!("Failed to connect to ZeroMQ socket {}: {}", endpoint, e))
            })?;

        println!("===> ROCKSDB STORAGE: SENDING REQUEST TO PORT {}: {:?}", port, request);
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| {
                error!("Failed to send request to port {}: {}", port, e);
                println!("===> ERROR: FAILED TO SEND REQUEST TO PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to send request: {}", e))
            })?;

        println!("===> ROCKSDB STORAGE: REQUEST SENT SUCCESSFULLY TO PORT {}", port);

        let reply = socket.recv_bytes(0)
            .map_err(|e| {
                error!("Failed to receive response from port {}: {}", port, e);
                println!("===> ERROR: FAILED TO RECEIVE RESPONSE FROM PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to receive response: {}", e))
            })?;

        let response: Value = serde_json::from_slice(&reply)
            .map_err(|e| {
                error!("Failed to parse response from port {}: {}", port, e);
                println!("===> ERROR: FAILED TO PARSE RESPONSE FROM PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to parse response: {}", e))
            })?;

        println!("===> ROCKSDB STORAGE: RECEIVED RESPONSE FROM PORT {}: {:?}", port, response);
        Ok(response)
    }

    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down RocksDBDaemon at path {:?}", self.db_path);
        println!("===> SHUTTING DOWN ROCKSDB DAEMON AT PATH {:?}", self.db_path);

        // Mark daemon as not running
        let mut running_guard = self.running.lock().await;
        *running_guard = false;
        drop(running_guard);

        // Flush database
        if let Err(e) = self.db.flush() {
            warn!("Failed to flush RocksDB: {}", e);
            println!("===> WARNING: FAILED TO FLUSH ROCKSDB: {}", e);
        } else {
            info!("Flushed RocksDBDaemon at {:?}", self.db_path);
            println!("===> FLUSHED ROCKSDB DAEMON AT {:?}", self.db_path);
        }

        // Check if process is still running
        if let Some(pid) = find_pid_by_port(self.port).await {
            if is_storage_daemon_running(self.port).await {
                info!(
                    "Terminating daemon process with PID {} on port {}",
                    pid, self.port
                );
                println!(
                    "===> TERMINATING DAEMON PROCESS WITH PID {} ON PORT {}",
                    pid, self.port
                );
                if let Err(e) = stop_process_by_pid("RocksDB", pid).await {
                    warn!("Failed to terminate daemon with PID {}: {}", pid, e);
                    println!(
                        "===> WARNING: FAILED TO TERMINATE DAEMON WITH PID {}: {}",
                        pid, e
                    );
                }
            }
        }

        // Remove daemon from registry
        if let Err(e) = GLOBAL_DAEMON_REGISTRY.remove_daemon_by_type("rocksdb", self.port).await {
            warn!("Failed to remove daemon from registry for port {}: {}", self.port, e);
            println!("===> WARNING: FAILED TO REMOVE DAEMON FROM REGISTRY FOR PORT {}: {}", self.port, e);
        }

        // Remove lock file
        if let Err(e) = Self::force_unlock_path(&self.db_path).await {
            warn!("Failed to remove lock file at {}: {}", self.db_path.display(), e);
            println!("===> WARNING: FAILED TO REMOVE LOCK FILE AT {}: {}", self.db_path.display(), e);
        }

        info!("Successfully shut down RocksDBDaemon on port {}", self.port);
        println!("===> SUCCESSFULLY SHUT DOWN ROCKSDB DAEMON ON PORT {}", self.port);
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

        timeout(Duration::from_secs(5), async {
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

        let value = timeout(Duration::from_secs(5), async {
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
        timeout(Duration::from_secs(5), async {
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

        timeout(Duration::from_secs(5), async {
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

        let res = timeout(Duration::from_secs(5), async {
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
        timeout(Duration::from_secs(5), async {
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

        timeout(Duration::from_secs(5), async {
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

        let res = timeout(Duration::from_secs(5), async {
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
        timeout(Duration::from_secs(5), async {
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
        timeout(Duration::from_secs(5), async {
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
        let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", self.port);
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
        let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", self.port);
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
        let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", self.port);
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
        let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", self.port);
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
            daemons: HashMap::new(),
            registry: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
            load_balancer: Arc::new(LoadBalancer::new(3)),
            use_raft_for_scale: false,
        }
    }

    pub async fn new_with_db(config: &RocksDBConfig, existing_db: Arc<rocksdb::DB>) -> GraphResult<Self> {
        let mut pool = Self::new();
        pool.initialize_with_db(config, existing_db).await?;
        Ok(pool)
    }

    pub async fn new_with_client(client: RocksDBClient, db_path: &Path, port: u16) -> GraphResult<Self> {
        println!("===> Initializing RocksDBDaemonPool with client for port {}", port);
        let mut pool = Self {
            daemons: HashMap::new(),
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
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
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
                },
            );
        }

        *pool.initialized.write().await = true;
        info!("Successfully initialized RocksDBDaemonPool with client for port {}", port);
        println!("===> Successfully initialized RocksDBDaemonPool with client for port {}", port);
        Ok(pool)
    }

    pub fn add_daemon(&mut self, daemon: Arc<RocksDBDaemon>) {
        self.daemons.insert(daemon.port, daemon);
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

            let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", port);
            socket.connect(&endpoint)
                .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", endpoint, e)))?;

            let request = json!({
                "command": "delete_key",
                "key": String::from_utf8_lossy(key).to_string(),
                "replicated": true,
                "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
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
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;

        socket.set_rcvtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        socket.set_sndtimeo(5000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;

        let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to {}: {}", endpoint, e)))?;

        let request = json!({
            "command": "set_key",
            "key": String::from_utf8_lossy(key).to_string(),
            "value": String::from_utf8_lossy(value).to_string(),
            "replicated": true,
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
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
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            self.load_balancer.update_node_health(port, false, response_time).await;
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

        let endpoint = format!("ipc:///opt/graphdb/graphdb-{}.ipc", port);
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

    pub async fn health_check_all_nodes(&self) -> GraphResult<()> {
        let all_ports: Vec<u16> = self.daemons.keys().copied().collect();

        println!("===> HEALTH CHECK: Checking {} total nodes", all_ports.len());

        let health_config = HealthCheckConfig {
            interval: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
            response_buffer_size: 1024,
        };

        for port in all_ports {
            let is_healthy = self.health_check_node(port, &health_config).await.unwrap_or(false);
            println!("===> HEALTH CHECK: Node {} is {}", port, if is_healthy { "healthy" } else { "unhealthy" });
        }

        let current_healthy = self.load_balancer.get_healthy_nodes().await;
        println!("===> HEALTH CHECK: {}/{} nodes are healthy: {:?}", 
                current_healthy.len(), self.daemons.len(), current_healthy);

        Ok(())
    }

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

                if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut stream, &request_bytes).await {
                    self.load_balancer.update_node_health(port, false, 0).await;
                    warn!("Failed to send status request to daemon on port {}. Reason: {}", port, e);
                    return Ok(false);
                }

                let mut response_buffer = vec![0; config.response_buffer_size];
                let bytes_read = match timeout(config.connect_timeout, tokio::io::AsyncReadExt::read(&mut stream, &mut response_buffer)).await {
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
                let response: Value = serde_json::from_slice(&response_buffer[..bytes_read])
                    .map_err(|e| {
                        warn!("Failed to parse status response from daemon on port {}: {}", port, e);
                        GraphError::SerializationError(e.to_string())
                    })?;

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

    async fn leader_daemon(&self) -> GraphResult<Arc<RocksDBDaemon>> {
        for daemon in self.daemons.values() {
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
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
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
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
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

    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut all_vertices = Vec::new();
        let ports: Vec<u16> = self.daemons.keys().copied().collect();

        for port in ports {
            if let Some(daemon) = self.daemons.get(&port) {
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

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut all_edges = Vec::new();
        let ports: Vec<u16> = self.daemons.keys().copied().collect();

        for port in ports {
            if let Some(daemon) = self.daemons.get(&port) {
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

    pub async fn clear_data(&self) -> GraphResult<()> {
        let ports: Vec<u16> = self.daemons.keys().copied().collect();
        let mut errors = Vec::new();

        for port in ports {
            if let Some(daemon) = self.daemons.get(&port) {
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

    pub async fn shutdown(&self) -> GraphResult<()> {
        let ports: Vec<u16> = self.daemons.keys().copied().collect();
        let mut errors = Vec::new();

        for port in ports {
            if let Some(daemon) = self.daemons.get(&port) {
                if let Err(e) = daemon.shutdown().await {
                    errors.push((port, e));
                }
            }
        }

        {
            let mut nodes = self.load_balancer.nodes.write().await;
            nodes.clear();
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(GraphError::StorageError(format!("Failed to shutdown some daemons: {:?}", errors)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use std::time::Duration;

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

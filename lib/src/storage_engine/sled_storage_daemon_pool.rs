use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::io::{Cursor, Read, Write, ErrorKind};
use sled::{Config, Db, IVec, Tree, Batch};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex as TokioMutex, RwLock, mpsc};
use tokio::time::{sleep, Duration as TokioDuration, timeout, interval};
use tokio::task::{self, JoinError,  spawn_blocking };
use tokio::fs as tokio_fs;
use log::{info, debug, warn, error};
use crate::config::{SledConfig, SledDaemon, SledDaemonPool, SledStorage, StorageConfig, StorageEngineType, 
                    DAEMON_REGISTRY_DB_PATH, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, ReplicationStrategy, 
                    NodeHealth, LoadBalancer, HealthCheckConfig, SledClientMode, SledClient, DbArc };
use crate::storage_engine::sled_client::{ ZmqSocketWrapper };
use crate::storage_engine::storage_utils::{create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex};
// FIX: Import the GraphStorageEngine trait. 
// This is the trait that the concrete SledClient struct implements,
// and is the type required for dynamic dispatch (`Arc<dyn Trait>`).
use crate::storage_engine::storage_engine::GraphStorageEngine;
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use serde_json::{json, Value};
use serde::{Deserialize, Serialize};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::daemon_management::{parse_cluster_range, is_daemon_running,  get_ipc_addr, };
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;
use uuid::Uuid;
use zmq::{Context as ZmqContext, Socket as ZmqSocket, Error as ZmqError, REP, REQ};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use nix::unistd::{ Pid as NixPid };
use nix::sys::signal::{kill, Signal};
use async_trait::async_trait;
use base64::engine::general_purpose;
use base64::Engine;
use std::fs::{self};

#[cfg(feature = "with-openraft-sled")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode},
    openraft_sled::SledRaftStorage,
    tokio::net::TcpStream,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

// Constants for waiting for the IPC file to exist
const MAX_WAIT_ATTEMPTS: u8 = 20; // 10 seconds total wait time (20 * 500ms)
const WAIT_DELAY_MS: u64 = 500;
// Key prefixes for distinguishing data types in the Sled Tree
const V_PREFIX: &[u8] = b"v_";
const E_PREFIX: &[u8] = b"e_";
const TIMEOUT_SECS: u64 = 5;

#[macro_export]
macro_rules! handle_sled_op {
    ($op:expr, $err_msg:expr) => {
        $op.map_err(|e| {
            error!("{}: {}", $err_msg, e);
            GraphError::StorageError(format!("{}: {}", $err_msg, e))
        })
    };
}

impl SledDaemon {
    /// Helper function to encapsulate the blocking Sled I/O.
    /// **CRITICAL FIX:** Implements a retry mechanism to handle transient lock contention 
    /// from the main thread's pre-initialization cleanup logic (e.g., unlocking the DB).
    pub async fn open_sled_db_and_trees(
        config: &SledConfig,
        db_path: &Path,
        cache_capacity: u64,
        use_compression: bool,
    ) -> Result<(DbArc, Tree, Tree, Tree), GraphError> {
        const MAX_RETRIES: usize = 5;
        let mut attempts = 0;

        loop {
            let mut sled_config = sled::Config::new()
                .path(db_path)
                .cache_capacity(cache_capacity)
                .flush_every_ms(Some(100));

            if use_compression {
                sled_config = sled_config.use_compression(true).compression_factor(10);
            }
            match sled_config.open() {
                Ok(db) => {
                    let db = Arc::new(db);
                    
                    let vertices = db.open_tree("vertices")
                        .map_err(|e| GraphError::StorageError(format!("Failed to open vertices tree: {}", e)))?;
                    let edges = db.open_tree("edges")
                        .map_err(|e| GraphError::StorageError(format!("Failed to open edges tree: {}", e)))?;
                    let kv_pairs = db.open_tree("kv_pairs")
                        .map_err(|e| GraphError::StorageError(format!("Failed to open kv_pairs tree: {}", e)))?;
                    info!("Sled DB and trees successfully opened at {:?} after {} attempts", db_path, attempts + 1);
                    println!("===> SLED DB AND TREES SUCCESSFULLY OPENED AT {:?} AFTER {} ATTEMPTS", db_path, attempts + 1);
                    return Ok((db, vertices, edges, kv_pairs));
                }
                Err(e) => {
                    attempts += 1;
                    
                    // Check if it's an I/O error by converting to string and checking the message
                    let error_msg = e.to_string();
                    let is_lock_error = error_msg.contains("WouldBlock") 
                        || error_msg.contains("Resource temporarily unavailable")
                        || error_msg.contains("already in use")
                        || error_msg.contains("lock");
                    
                    if attempts >= MAX_RETRIES || !is_lock_error {
                        error!("Failed to open Sled DB at {:?}: {}", db_path, e);
                        println!("===> ERROR: FAILED TO OPEN SLED DB AT {:?}: {}", db_path, e);
                        return Err(GraphError::StorageError(format!(
                            "Failed to open Sled DB: {}. Ensure no other process is using the database.", e
                        )));
                    }
                    warn!("Sled DB lock contention detected at {:?}. Retrying in 100ms (Attempt {}/{})", db_path, attempts, MAX_RETRIES);
                    // Use tokio::time::sleep instead of sleep
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                }
            }
        }
    }

    pub async fn new(config: SledConfig) -> GraphResult<(Self, mpsc::Receiver<()>)> {
        println!("===> SledDaemon new CALLED");
        let port = config.port.ok_or_else(|| {
            error!("No port specified in SledConfig");
            println!("===> ERROR: NO PORT SPECIFIED IN SLED CONFIG");
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;

        // Construct the full database path
        let db_path = if config.path.ends_with(&port.to_string()) {
            PathBuf::from(config.path.clone())
        } else {
            PathBuf::from(config.path.clone()).join(port.to_string())
        };

        info!("Initializing Sled daemon at {:?}", db_path);
        println!("===> INITIALIZING SLED DAEMON AT {:?}", db_path);

        // Directory check logic
        if !db_path.exists() {
            info!("Creating database directory at {:?}", db_path);
            println!("===> CREATING DATABASE DIRECTORY AT {:?}", db_path);
            tokio_fs::create_dir_all(&db_path)
                .await
                .map_err(|e| {
                    error!("Failed to create directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO CREATE DIRECTORY AT {:?}: {}", db_path, e);
                    GraphError::StorageError(format!("Failed to create directory at {:?}: {}", db_path, e))
                })?;
        } else if !db_path.is_dir() {
            error!("Path {:?} is not a directory", db_path);
            println!("===> ERROR: PATH {:?} IS NOT A DIRECTORY", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        let metadata = tokio_fs::metadata(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to access directory metadata at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}: {}", db_path, e);
                GraphError::StorageError(format!("Failed to access directory metadata at {:?}", db_path))
            })?;
        if metadata.permissions().readonly() {
            error!("Directory at {:?} is not writable", db_path);
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        // Open sled database in a blocking task
        let cache_capacity = config.cache_capacity.unwrap_or(1024 * 1024 * 1024);
        let use_compression = config.use_compression;
        let db_path_clone = db_path.clone();
        let (db, vertices, edges, kv_pairs) = spawn_blocking(move || {
            let sled_config = sled::Config::new()
                .path(&db_path_clone)
                .cache_capacity(cache_capacity)
                .use_compression(use_compression);
            let db = sled_config.open()?;
            let vertices = db.open_tree("vertices")?;
            let edges = db.open_tree("edges")?;
            let kv_pairs = db.open_tree("kv_pairs")?;
            Ok::<(Db, Tree, Tree, Tree), sled::Error>((db, vertices, edges, kv_pairs))
        })
        .await
        .map_err(|e| {
            error!("Failed to spawn blocking task for sled open: {}", e);
            println!("===> ERROR: FAILED TO SPAWN BLOCKING TASK FOR SLED OPEN: {}", e);
            GraphError::StorageError(format!("Failed to spawn blocking task: {}", e))
        })?
        .map_err(|e| {
            error!("Failed to open sled database at {:?}: {}", db_path, e);
            println!("===> ERROR: FAILED TO OPEN SLED DATABASE AT {:?}: {}", db_path, e);
            GraphError::StorageError(format!("Failed to open sled database at {:?}: {}", db_path, e))
        })?;

        info!("SLED DB AND TREES SUCCESSFULLY OPENED AT {:?}", db_path);
        println!("===> SLED DB AND TREES SUCCESSFULLY OPENED AT {:?}", db_path);

        // Use mpsc channel for readiness signaling
        let (tx, rx) = mpsc::channel(1);
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);

        // Clean up stale IPC socket
        let ipc_path = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
        if std::path::Path::new(ipc_path).exists() {
            warn!("Stale IPC socket found at {}. Attempting cleanup.", ipc_path);
            println!("===> WARNING: STALE IPC SOCKET FOUND AT {}. ATTEMPTING CLEANUP.", ipc_path);
            if let Err(e) = fs::remove_file(ipc_path) {
                error!("Failed to remove stale IPC socket {}: {}", ipc_path, e);
                println!("===> ERROR: FAILED TO REMOVE STALE IPC SOCKET {}: {}", ipc_path, e);
            } else {
                info!("Successfully removed stale IPC socket {}", ipc_path);
                println!("===> SUCCESSFULLY REMOVED STALE IPC SOCKET {}", ipc_path);
            }
        }

        // Start ZMQ server in a separate thread, creating the socket inside
        let running = Arc::new(TokioMutex::new(true));
        let running_clone = running.clone();
        let db_arc = Arc::new(db);
        let db_arc_for_thread = db_arc.clone();
        let vertices_for_thread = vertices.clone();
        let edges_for_thread = edges.clone();
        let kv_pairs_for_thread = kv_pairs.clone();
        let endpoint_clone = endpoint.clone();
        let context = ZmqContext::new();
        let zmq_socket = context
            .socket(REP)
            .map_err(|e| {
                error!("Failed to create ZMQ socket for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO CREATE ZMQ SOCKET FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e))
            })?;
        let zmq_socket = Arc::new(TokioMutex::new(zmq_socket));

        let daemon = SledDaemon {
            port,
            db_path,
            db: db_arc,
            vertices,
            edges,
            kv_pairs,
            running,
            #[cfg(feature = "with-openraft-sled")]
            raft_storage: Arc::new(openraft_sled::SledRaftStorage::new(db_arc.clone())),
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
        };

        let daemon_clone = daemon.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
            rt.block_on(async {
                // Create ZMQ socket inside the thread
                let context = ZmqContext::new();
                let zmq_socket_inner = context
                    .socket(REP)
                    .map_err(|e| {
                        error!("Failed to create ZMQ socket for port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO CREATE ZMQ SOCKET FOR PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e))
                    })
                    .expect("Failed to create ZMQ socket");
                let zmq_socket_inner = Arc::new(TokioMutex::new(zmq_socket_inner));

                // Bind the socket
                const BIND_RETRIES: usize = 5;
                for i in 0..BIND_RETRIES {
                    let endpoint_for_bind = endpoint_clone.clone();
                    match zmq_socket_inner.lock().await.bind(&endpoint_for_bind) {
                        Ok(_) => {
                            info!("ZMQ socket successfully bound to {}. Signaling readiness.", endpoint_clone);
                            println!("===> ZMQ SOCKET SUCCESSFULLY BOUND TO {}. SIGNALING READINESS.", endpoint_clone);
                            let _ = tx.send(()).await;
                            break;
                        }
                        Err(e) => {
                            if i < BIND_RETRIES - 1 {
                                warn!(
                                    "Failed to bind ZMQ socket to {} (attempt {}/{}): {}",
                                    endpoint_clone, i + 1, BIND_RETRIES, e
                                );
                                tokio::time::sleep(TokioDuration::from_millis(100 * (i as u64 + 1))).await;
                            } else {
                                error!(
                                    "Failed to bind ZMQ socket to {} after {} attempts: {}",
                                    endpoint_clone, BIND_RETRIES, e
                                );
                                let _ = tx.send(()).await; // Signal even on failure to avoid hanging
                            }
                        }
                    }
                }

                // Run the ZMQ server
                info!("Starting ZMQ server for endpoint {}", endpoint_clone);
                println!("===> STARTING ZMQ SERVER FOR ENDPOINT {}", endpoint_clone);
                let endpoint_for_zmq_call = endpoint_clone.clone();
                if let Err(e) = Self::run_zmq_server_static(
                        port,
                        db_arc_for_thread,
                        vertices_for_thread,
                        edges_for_thread,
                        kv_pairs_for_thread,
                        running_clone,
                        zmq_socket,
                        endpoint_for_zmq_call,
                    )
                    .await
                {
                    error!("ZMQ server failed for endpoint {}: {}", endpoint_clone, e);
                    println!("===> ERROR: ZMQ SERVER FAILED FOR ENDPOINT {}: {}", endpoint_clone, e);
                } else {
                    info!("ZMQ server finished successfully for endpoint {}", endpoint_clone);
                    println!("===> ZMQ SERVER FINISHED SUCCESSFULLY FOR ENDPOINT {}", endpoint_clone);
                }
            });
        });

        info!("Sled daemon initialization complete for port {}", port);
        println!("===> SLED DAEMON INITIALIZATION COMPLETE FOR PORT {}", port);
        Ok((daemon, rx))
    }

    /// Initializes a SledDaemon instance using an existing, already opened sled::Db instance.
    /// This method performs directory and permissions checks, opens the required trees, and
    /// sets up the ZMQ server and signal handlers.
    pub async fn new_with_db(config: SledConfig, existing_db: Arc<sled::Db>) -> GraphResult<Self> {
        let port = config.port.ok_or_else(|| {
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;

        let db_path = {
            let base_path = PathBuf::from(&config.path);
            if base_path.file_name().and_then(|name| name.to_str()) == Some(&port.to_string()) {
                 base_path
            } else {
                 base_path.join(port.to_string())
            }
        };

        println!("===> Initializing Sled daemon with existing DB at {:?}", db_path);

        if !db_path.exists() {
            // Assuming tokio_fs is aliased to tokio::fs
            tokio::fs::create_dir_all(&db_path).await.map_err(|e| {
                GraphError::StorageError(format!("Failed to create directory at {:?}: {}", db_path, e))
            })?;
            
            // FIX E0433: Permissions type and from_mode are in std::fs
            // We use conditional compilation to ensure PermissionsExt is in scope on Unix systems.
            let permissions = if cfg!(unix) {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::Permissions::from_mode(0o755)
                }
                #[cfg(not(unix))]
                {
                    std::fs::Permissions::from_mode(0o755)
                }
            } else {
                std::fs::Permissions::from_mode(0o755)
            };
            
            tokio_fs::set_permissions(&db_path, permissions)
                .await
                .map_err(|e| {
                    GraphError::StorageError(format!("Failed to set permissions on directory at {:?}: {}", db_path, e))
                })?;
        } else if !db_path.is_dir() {
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        let db_clone = Arc::clone(&existing_db);
        let (vertices, edges, kv_pairs) = tokio::task::spawn_blocking(move || {
            Ok::<_, sled::Error>((
                db_clone.open_tree("vertices")?,
                db_clone.open_tree("edges")?,
                db_clone.open_tree("kv_pairs")?,
            ))
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Task panic while opening trees: {:?}", e)))?
        .map_err(|e| GraphError::StorageError(format!("Failed to open trees from existing DB: {}", e)))?;

        println!("===> Successfully opened Sled trees from existing DB");

        let kv_key_count = kv_pairs.len();
        println!("===> Initial kv_pairs key count at {:?}: {}", db_path, kv_key_count);

        // FIX E0382: Clone the Tree and Arc objects needed by the ZMQ thread
        // before the original variables are moved into the 'daemon' struct below.
        let vertices_clone = vertices.clone();
        let edges_clone = edges.clone();
        let kv_pairs_clone = kv_pairs.clone();
        
        let running = Arc::new(TokioMutex::new(true));
        let running_clone = running.clone(); // Clone running before it's moved into daemon

        #[cfg(feature = "with-openraft-sled")]
        let (raft, raft_storage) = {
            let raft_db_path = db_path.join("raft");
            if !raft_db_path.exists() {
                tokio::fs::create_dir_all(&raft_db_path).await.map_err(|e| {
                    GraphError::StorageError(format!("Failed to create Raft DB directory at {:?}: {}", raft_db_path, e))
                })?;
                
                // FIX E0433: Permissions is in std::fs
                let permissions = if cfg!(unix) {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::Permissions::from_mode(0o755)
                    }
                    #[cfg(not(unix))]
                    {
                        std::fs::Permissions::from_mode(0o755)
                    }
                } else {
                    std::fs::Permissions::from_mode(0o755)
                };

                tokio_fs::set_permissions(&raft_db_path, permissions)
                    .await
                    .map_err(|e| {
                        GraphError::StorageError(format!("Failed to set permissions on Raft directory at {:?}: {}", raft_db_path, e))
                    })?;
            }
            let raft_storage = SledRaftStorage::new(&raft_db_path).await?;
            let raft_config = Arc::new(RaftConfig {
                cluster_name: "graphdb-cluster".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(500),
                ..Default::default()
            });

            let raft = openraft::Raft::new(
                port as u64,
                raft_config,
                Arc::new(raft_storage.clone()),
                Arc::new(crate::storage::openraft::RaftTcpNetwork {}),
            );
            println!("===> Initialized Raft for node {} on port {}", port, port);
            (Some(Arc::new(raft)), Some(Arc::new(raft_storage)))
        };

        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let context = ZmqContext::new();
        let zmq_socket = context
            .socket(REP)
            .map_err(|e| {
                error!("Failed to create ZMQ socket for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO CREATE ZMQ SOCKET FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e))
            })?;
        let zmq_socket = Arc::new(TokioMutex::new(zmq_socket));

        let daemon = Self {
            port,
            db_path,
            db: existing_db,
            vertices, // Original values are now moved into the struct
            edges,
            kv_pairs,
            running,
            #[cfg(feature = "with-openraft-sled")]
            raft_storage,
            #[cfg(feature = "with-openraft-sled")]
            raft,
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
        };

        // --- ZMQ IPC Binding Logic for new_with_db ---
        let ipc_addr = endpoint.clone();
        println!("===> Time to run ZeroMQ server on IPC: {}", ipc_addr);

        let daemon_for_zmq = daemon.clone();
        let db_clone = Arc::clone(&daemon.db);
        let zmq_socket_clone = zmq_socket.clone();
        let endpoint_clone = endpoint.clone();

        std::thread::spawn(move || {
            let ipc_addr_thread = ipc_addr; // Move the IPC address into the thread
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                tokio::time::sleep(TokioDuration::from_millis(100)).await;
                println!("===> Starting ZMQ server for IPC {}", ipc_addr_thread);
                
                if let Err(e) = Self::run_zmq_server_static(
                        port,
                        db_clone,
                        vertices_clone, // Uses the clone created earlier
                        edges_clone,    // Uses the clone created earlier
                        kv_pairs_clone, // Uses the clone created earlier
                        running_clone,  // Uses the clone created earlier
                        zmq_socket_clone,
                        endpoint_clone,
                    )
                    .await
                {
                    eprintln!("===> ERROR: ZeroMQ server failed: {}", e);
                }
            });
        });
        // --- End ZMQ IPC Binding Logic for new_with_db ---

        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            tokio::spawn(async move {
                // FIX E0596: 'sigterm' must be mutable to call 'recv().await'
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                println!("===> WARNING: Received SIGTERM in Sled daemon");
                if let Err(e) = daemon_for_signal.shutdown().await {
                    eprintln!("===> ERROR: Failed to shutdown Sled daemon on SIGTERM: {}", e);
                } else {
                    println!("===> Sled daemon shutdown complete");
                }
            });
        }

        println!("===> Sled daemon initialization complete for port {}", port);
        Ok(daemon)
    }
    /// Initializes a SledDaemon instance when the system is operating in a client/server model.
    /// This method is primarily responsible for starting the daemon process which serves
    /// the Sled database over ZMQ. The provided client/socket are typically unused by the daemon itself.
    pub async fn new_with_client(
        config: SledConfig,
        _client: SledClient, // Parameter is accepted but now unused by the instance
        _socket: Arc<TokioMutex<ZmqSocketWrapper>>,
    ) -> GraphResult<Self> {
        println!("SledDaemon new_with_client =================> LET US SEE IF THIS WAS EVER CALLED");

        // NOTE: Imports are usually handled at the module level, but kept here for clarity
        // and to fix the use of PermissionsExt inside the conditional compilation block.
        use sled::Config;
        use tokio::fs;
        use std::path::{Path, PathBuf}; 
        
        let port = config.port.ok_or_else(|| {
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;

        // Construct proper path structure, ensuring consistent PathBuf usage
        let db_path = {
            let base_path = PathBuf::from(&config.path);
            if base_path.file_name().and_then(|name| name.to_str()) == Some(&port.to_string()) {
                base_path
            } else {
                base_path.join(port.to_string())
            }
        };

        println!("===> Initializing Sled daemon at {:?}", db_path);

        // Ensure the database directory exists and is writable
        if !db_path.exists() {
            println!("===> Creating database directory at {:?}", db_path);
            tokio::fs::create_dir_all(&db_path).await.map_err(|e| {
                GraphError::StorageError(format!("Failed to create directory at {:?}: {}", db_path, e))
            })?;
            
            // FIX: Use std::fs::Permissions and conditional PermissionsExt for Unix mode
            let permissions = if cfg!(unix) {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::Permissions::from_mode(0o755)
                }
                #[cfg(not(unix))]
                {
                    std::fs::Permissions::from_mode(0o755)
                }
            } else {
                std::fs::Permissions::from_mode(0o755)
            };
            
            tokio_fs::set_permissions(&db_path, permissions)
                .await
                .map_err(|e| {
                    GraphError::StorageError(format!("Failed to set permissions on directory at {:?}: {}", db_path, e))
                })?;
        } else if !db_path.is_dir() {
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        let metadata = tokio::fs::metadata(&db_path).await.map_err(|e| {
            GraphError::StorageError(format!("Failed to access directory metadata at {:?}: {}", db_path, e))
        })?;
        if metadata.permissions().readonly() {
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        // --- Initialize non-optional Sled fields ---
        let sled_config = Config::new().path(&db_path);
        let db = sled_config.open().map_err(|e| {
            GraphError::StorageError(format!("Failed to open Sled database at {:?}: {}", db_path, e))
        })?;

        let vertices = db.open_tree("vertices").map_err(|e| {
            GraphError::StorageError(format!("Failed to open vertices tree: {}", e))
        })?;
        let edges = db.open_tree("edges").map_err(|e| {
            GraphError::StorageError(format!("Failed to open edges tree: {}", e))
        })?;
        let kv_pairs = db.open_tree("kv_pairs").map_err(|e| {
            GraphError::StorageError(format!("Failed to open kv_pairs tree: {}", e))
        })?;

        // Wrap the Db in Arc as required by the SledDaemon struct definition
        let db_arc = Arc::new(db);

        println!("===> ZMQ client and socket parameters are unused in SledDaemon instance.");

        // FIX E0382: Clone items BEFORE they are moved into the `daemon` struct initialization
        let vertices_clone = vertices.clone();
        let edges_clone = edges.clone();
        let kv_pairs_clone = kv_pairs.clone();
        let running = Arc::new(TokioMutex::new(true));
        let running_clone = running.clone(); 
        let db_path_for_metadata = db_path.clone(); // Clone db_path for metadata registry

        // --- ZMQ Setup (Server-side) ---
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let context = ZmqContext::new();
        let zmq_socket = context
            .socket(REP)
            .map_err(|e| {
                error!("Failed to create ZMQ socket for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO CREATE ZMQ SOCKET FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e))
            })?;
        let zmq_socket = Arc::new(TokioMutex::new(zmq_socket));
        let zmq_socket_clone = zmq_socket.clone();
        let endpoint_clone = endpoint.clone();
        let ipc_addr = endpoint.clone();

        // Initialize SledDaemon struct
        let daemon = Self {
            port,
            db_path, // Original db_path is MOVED here
            db: db_arc, // Arc<Db>
            vertices, // Tree (Originals are moved here)
            edges, // Tree
            kv_pairs, // Tree
            running,
            #[cfg(feature = "with-openraft-sled")]
            raft_storage: None,
            #[cfg(feature = "with-openraft-sled")]
            raft: None,
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
        };

        // Register daemon in global registry
        let daemon_registry = crate::daemon::daemon_registry::GLOBAL_DAEMON_REGISTRY.get().await;
        let daemon_metadata = crate::daemon::daemon_registry::DaemonMetadata {
            service_type: "storage".to_string(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            // Use the cloned path for metadata
            data_dir: Some(db_path_for_metadata),
            config_path: None,
            engine_type: Some("sled".to_string()),
            last_seen_nanos: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as i64,
            pid: std::process::id(),
            port,
        };
        daemon_registry.register_daemon(daemon_metadata).await
            .map_err(|e| {
                println!("===> ERROR: Failed to register daemon for port {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
            })?;
        println!("===> Registered daemon for port {} in global registry", port);

        // Spawn ZMQ server thread
        println!("===> Time to run ZeroMQ server");
        let daemon_for_zmq = daemon.clone();
        let db_clone = Arc::clone(&daemon.db);

        std::thread::spawn(move || {
            let ipc_addr_thread = ipc_addr; // Move the IPC address into the thread
            tokio::runtime::Runtime::new().unwrap().block_on(async move {
                tokio::time::sleep(TokioDuration::from_millis(100)).await;
                println!("===> Starting ZMQ server for IPC {}", ipc_addr_thread);
                if let Err(e) = Self::run_zmq_server_static(
                        port,
                        db_clone,
                        vertices_clone, // Uses the clone created before 'daemon' init
                        edges_clone,    // Uses the clone created before 'daemon' init
                        kv_pairs_clone, // Uses the clone created before 'daemon' init
                        running_clone,  // Uses the clone created before 'daemon' init
                        zmq_socket_clone,
                        endpoint_clone,
                    )
                    .await
                {
                    eprintln!("===> ERROR: ZeroMQ server failed: {}", e);
                }
            });
        });

        // Register SIGTERM handler on Unix
        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            tokio::spawn(async move {
                // FIX E0596: 'sigterm' must be mutable
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                println!("===> WARNING: Received SIGTERM in Sled daemon");
                if let Err(e) = daemon_for_signal.shutdown().await {
                    eprintln!("===> ERROR: Failed to shutdown Sled daemon on SIGTERM: {}", e);
                } else {
                    println!("===> Sled daemon shutdown complete");
                }
            });
        }

        println!("===> Sled daemon initialization complete for port {}", port);
        Ok(daemon)
    }

    async fn send_zmq_response_static(socket: &ZmqSocket, response: &Value, port: u16) -> GraphResult<()> {
        let response_data = serde_json::to_vec(response)
            .map_err(|e| {
                error!("Failed to serialize ZMQ response for port {}: {}", port, e);
                GraphError::StorageError(format!("Failed to serialize response: {}", e))
            })?;
        socket
            .send(response_data, 0)
            .map_err(|e| {
                error!("Failed to send ZMQ response for port {}: {}", port, e);
                GraphError::StorageError(format!("Failed to send response: {}", e))
            })?;
        Ok(())
    }

    async fn run_zmq_server_static(
        port: u16,
        db: Arc<Db>,
        vertices: Tree,
        edges: Tree,
        kv_pairs: Tree,
        running: Arc<TokioMutex<bool>>,
        zmq_socket: Arc<TokioMutex<ZmqSocket>>,
        endpoint: String,
    ) -> GraphResult<()> {
        const SOCKET_TIMEOUT_MS: i32 = 10_000;
        const MAX_MESSAGE_SIZE: i32 = 1_024 * 1_024;

        // Construct db_path using DEFAULT_DATA_DIRECTORY and port
        let db_path = PathBuf::from(format!("{}/sled/{}", DEFAULT_DATA_DIRECTORY, port));

        println!("==============================> Starting ZMQ server for port {}", port);

        // Configure socket
        {
            let socket = zmq_socket.lock().await;
            socket
                .set_linger(1000)
                .map_err(|e| GraphError::StorageError(format!("Failed to set socket linger for port {}: {}", port, e)))?;
            socket
                .set_rcvtimeo(SOCKET_TIMEOUT_MS)
                .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout for port {}: {}", port, e)))?;
            socket
                .set_sndtimeo(SOCKET_TIMEOUT_MS)
                .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout for port {}: {}", port, e)))?;
            socket
                .set_maxmsgsize(MAX_MESSAGE_SIZE as i64)
                .map_err(|e| GraphError::StorageError(format!("Failed to set max message size for port {}: {}", port, e)))?;

            let socket_path = format!("/tmp/graphdb-{}.ipc", port);
            if tokio_fs::metadata(&socket_path).await.is_ok() {
                tokio_fs::remove_file(&socket_path).await
                    .map_err(|e| {
                        error!("Failed to remove existing IPC socket file {}: {}", socket_path, e);
                        GraphError::StorageError(format!("Failed to remove IPC socket file {}: {}", socket_path, e))
                    })?;
            }

            socket.bind(&endpoint).map_err(|e| {
                error!("Failed to bind ZMQ socket to {}: {}", endpoint, e);
                GraphError::StorageError(format!("Failed to bind ZMQ socket to {}: {}", endpoint, e))
            })?;
        }

        tokio::time::sleep(TokioDuration::from_millis(100)).await;

        info!("ZMQ server configured and bound for port {}", port);
        println!("===> ZMQ SERVER CONFIGURED AND BOUND FOR PORT {}", port);

        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        while *running.lock().await {
            let msg_result = {
                let socket = zmq_socket.lock().await;
                socket.recv_bytes(zmq::DONTWAIT)
            };

            let msg: Vec<u8> = match msg_result {
                Ok(msg_bytes) => {
                    consecutive_errors = 0;
                    debug!("Received ZeroMQ message for port {}: {:?}", port, String::from_utf8_lossy(&msg_bytes));
                    msg_bytes
                }
                Err(zmq::Error::EAGAIN) => {
                    tokio::time::sleep(TokioDuration::from_millis(10)).await;
                    continue;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    warn!("Failed to receive ZeroMQ message for port {} (attempt {}): {}", port, consecutive_errors, e);
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive ZeroMQ errors for port {}, shutting down server", port);
                        break;
                    }
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                    continue;
                }
            };

            if msg.is_empty() {
                error!("Received empty message for port {}", port);
                let response = json!({"status": "error", "message": "Received empty message"});
                let socket = zmq_socket.lock().await;
                Self::send_zmq_response_static(&socket, &response, port).await?;
                continue;
            }

            let request: Value = match serde_json::from_slice(&msg) {
                Ok(req) => {
                    debug!("Parsed request for port {}: {:?}", port, req);
                    req
                }
                Err(e) => {
                    error!("Failed to parse ZeroMQ request for port {}: {}", port, e);
                    let response = json!({"status": "error", "message": format!("Failed to parse request: {}", e)});
                    let socket = zmq_socket.lock().await;
                    Self::send_zmq_response_static(&socket, &response, port).await?;
                    continue;
                }
            };

            let response = match request.get("command").and_then(|c| c.as_str()) {
                Some("initialize") => {
                    info!("Initialization command received for port {}", port);
                    json!({
                        "status": "success",
                        "message": "ZMQ server is bound and ready.",
                        "port": port,
                        "ipc_path": endpoint
                    })
                }
                Some("status") | Some("ping") => {
                    json!({"status": "success", "port": port})
                }
                Some("set_key") => {
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            let response = json!({"status": "error", "message": "Missing key in set_key request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let value = match request.get("value").and_then(|v| v.as_str()) {
                        Some(v) => v,
                        None => {
                            let response = json!({"status": "error", "message": "Missing value in set_key request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    // Decode the base64-encoded value
                    let decoded_value = match general_purpose::STANDARD.decode(value) {
                        Ok(decoded) => decoded,
                        Err(e) => {
                            error!("Failed to decode base64 value for key {}: {}", key, e);
                            let response = json!({"status": "error", "message": format!("Invalid base64 value: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::insert_static(&kv_pairs, &db, db_path.as_path(), key.as_bytes(), &decoded_value).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_key") => {
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            let response = json!({"status": "error", "message": "Missing key in get_key request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::retrieve_static(&kv_pairs, db_path.as_path(), key.as_bytes()).await {
                        Ok(Some(val)) => {
                            let value_str = String::from_utf8_lossy(&val).to_string();
                            json!({"status": "success", "value": value_str})
                        }
                        Ok(None) => json!({"status": "success", "value": null}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("delete_key") => {
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            let response = json!({"status": "error", "message": "Missing key in delete_key request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::delete_static(&kv_pairs, &db, db_path.as_path(), key.as_bytes()).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("flush") => {
                    match Self::flush_static(&db, db_path.as_path()).await {
                        Ok(_) => json!({"status": "success", "bytes_flushed": 0}), // bytes_flushed not returned by flush_static
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_all_vertices") => {
                    match Self::get_all_vertices_static(&vertices, db_path.as_path()).await {
                        Ok(vertices_list) => json!({"status": "success", "vertices": vertices_list}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_all_edges") => {
                    match Self::get_all_edges_static(&edges, db_path.as_path()).await {
                        Ok(edges_list) => json!({"status": "success", "edges": edges_list}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("clear_data") => {
                    match Self::clear_data_static(&kv_pairs, &db, db_path.as_path()).await {
                        Ok(_) => json!({"status": "success", "bytes_flushed": 0}), // bytes_flushed not returned by clear_data_static
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("create_vertex") => {
                    let vertex = match request.get("vertex").and_then(|v| serde_json::from_value::<Vertex>(v.clone()).ok()) {
                        Some(v) => v,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing vertex in create_vertex request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::create_vertex_static(&vertices, &db, db_path.as_path(), &vertex).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_vertex") => {
                    let id = match request.get("id").and_then(|i| Uuid::parse_str(i.as_str().unwrap_or("")).ok()) {
                        Some(i) => i,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing id in get_vertex request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::get_vertex_static(&vertices, db_path.as_path(), &id).await {
                        Ok(Some(vertex)) => json!({"status": "success", "vertex": vertex}),
                        Ok(None) => json!({"status": "success", "vertex": null}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("update_vertex") => {
                    let vertex = match request.get("vertex").and_then(|v| serde_json::from_value::<Vertex>(v.clone()).ok()) {
                        Some(v) => v,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing vertex in update_vertex request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::update_vertex_static(&vertices, &db, db_path.as_path(), &vertex).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("delete_vertex") => {
                    let id = match request.get("id").and_then(|i| Uuid::parse_str(i.as_str().unwrap_or("")).ok()) {
                        Some(i) => i,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing id in delete_vertex request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::delete_vertex_static(&vertices, &edges, &db, db_path.as_path(), &id).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("create_edge") => {
                    let edge = match request.get("edge").and_then(|e| serde_json::from_value::<Edge>(e.clone()).ok()) {
                        Some(e) => e,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing edge in create_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::create_edge_static(&edges, &db, db_path.as_path(), &edge).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_edge") => {
                    let outbound_id = match request.get("outbound_id").and_then(|i| Uuid::parse_str(i.as_str().unwrap_or("")).ok()) {
                        Some(i) => i,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing outbound_id in get_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let edge_type = match request.get("edge_type").and_then(|t| serde_json::from_value::<Identifier>(t.clone()).ok()) {
                        Some(t) => t,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing edge_type in get_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let inbound_id = match request.get("inbound_id").and_then(|i| Uuid::parse_str(i.as_str().unwrap_or("")).ok()) {
                        Some(i) => i,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing inbound_id in get_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::get_edge_static(&edges, db_path.as_path(), &outbound_id, &edge_type, &inbound_id).await {
                        Ok(Some(edge)) => json!({"status": "success", "edge": edge}),
                        Ok(None) => json!({"status": "success", "edge": null}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("update_edge") => {
                    let edge = match request.get("edge").and_then(|e| serde_json::from_value::<Edge>(e.clone()).ok()) {
                        Some(e) => e,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing edge in update_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::update_edge_static(&edges, &db, db_path.as_path(), &edge).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("delete_edge") => {
                    let outbound_id = match request.get("outbound_id").and_then(|i| Uuid::parse_str(i.as_str().unwrap_or("")).ok()) {
                        Some(i) => i,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing outbound_id in delete_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let edge_type = match request.get("edge_type").and_then(|t| serde_json::from_value::<Identifier>(t.clone()).ok()) {
                        Some(t) => t,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing edge_type in delete_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let inbound_id = match request.get("inbound_id").and_then(|i| Uuid::parse_str(i.as_str().unwrap_or("")).ok()) {
                        Some(i) => i,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing inbound_id in delete_edge request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };

                    match Self::delete_edge_static(&edges, &db, db_path.as_path(), &outbound_id, &edge_type, &inbound_id).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("force_reset") => {
                    match Self::force_reset_static(&db, db_path.as_path()).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("force_unlock") => {
                    match Self::force_unlock_static(db_path.as_path()).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("force_unlock_path") => {
                    match Self::force_unlock_path_static(db_path.as_path()).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some(cmd) => {
                    error!("Unsupported command for port {}: {}", port, cmd);
                    json!({"status": "error", "message": format!("Unsupported command: {}", cmd)})
                }
                None => {
                    error!("No command specified in request for port {}: {:?}", port, request);
                    json!({"status": "error", "message": "No command specified"})
                }
            };

            let socket = zmq_socket.lock().await;
            Self::send_zmq_response_static(&socket, &response, port).await?;
        }

        info!("ZMQ server shutting down for port {}", port);
        let socket = zmq_socket.lock().await;
        if let Err(e) = socket.disconnect(&endpoint) {
            error!("Failed to disconnect ZMQ socket for port {}: {}", port, e);
        }
        Ok(())
    }


    // Assuming necessary imports are in scope, including:
    // use base64::{Engine as _, general_purpose};
    // use tokio::time::{Duration as TokioDuration};
    // use tokio::sync::Mutex as TokioMutex;
    // use std::sync::Arc;
    // use serde_json::{json, Value};
    // use zmq;
    // use log::{info, debug, warn, error};
    // use sled::{Tree, Db};
    // use crate::models::errors::{GraphError, GraphResult}; // Assuming your error types
    // use general_purpose::STANDARD; // Assuming this alias is used for brevity
    // Method 1: run_zmq_server_static (for SledDaemonPool)
    // Method 1: run_zmq_server_static (for SledDaemonPool)

    // Method 2: run_zmq_server (for SledDaemon instance method)
    // Method 2: run_zmq_server (for SledDaemon instance method) - MODIFIED
    // This version is updated to ensure the command logic for persistence (flush) and
    // argument error handling matches the run_zmq_server_static version.
    async fn run_zmq_server(&self) -> GraphResult<()> {
        const SOCKET_TIMEOUT_MS: i32 = 10_000;
        const MAX_MESSAGE_SIZE: i32 = 1_024 * 1_024;

        println!("===> STARTING ZMQ SERVER FOR PORT {}", self.port);

        let context = ZmqContext::new();
        let responder = context.socket(zmq::REP)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket for port {}: {}", self.port, e)))?;

        responder.set_linger(1000)
            .map_err(|e| GraphError::StorageError(format!("Failed to set socket linger: {}", e)))?;
        responder.set_rcvtimeo(SOCKET_TIMEOUT_MS)
            .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
        responder.set_sndtimeo(SOCKET_TIMEOUT_MS)
            .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;
        responder.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)
            .map_err(|e| GraphError::StorageError(format!("Failed to set max message size: {}", e)))?;

        let socket_path = format!("/tmp/graphdb-{}.ipc", self.port);
        if tokio_fs::metadata(&socket_path).await.is_ok() {
            tokio_fs::remove_file(&socket_path).await
                .map_err(|e| GraphError::StorageError(format!("Failed to remove IPC socket file: {}", e)))?;
        }

        let endpoint = format!("ipc://{}", socket_path);
        responder.bind(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to bind ZMQ socket to {}: {}", endpoint, e)))?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio_fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o666)).await
                .map_err(|e| GraphError::StorageError(format!("Failed to set socket permissions: {}", e)))?;
        }

        tokio::time::sleep(TokioDuration::from_millis(100)).await;

        info!("ZMQ server configured and bound for port {}", self.port);
        println!("===> ZMQ SERVER CONFIGURED AND BOUND FOR PORT {}", self.port);

        let mut consecutive_errors = 0;
        const MAX_CONSECUTIVE_ERRORS: u32 = 10;

        while *self.running.lock().await {
            let msg_result = responder.recv_bytes(zmq::DONTWAIT);
            
            let msg: Vec<u8> = match msg_result {
                Ok(msg_bytes) => {
                    consecutive_errors = 0;
                    debug!("Received message for port {}", self.port);
                    msg_bytes
                }
                Err(zmq::Error::EAGAIN) => {
                    tokio::time::sleep(TokioDuration::from_millis(10)).await;
                    continue;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    warn!("Failed to receive message for port {} (attempt {}): {}", self.port, consecutive_errors, e);
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive errors for port {}, shutting down", self.port);
                        break;
                    }
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                    continue;
                }
            };

            if msg.is_empty() {
                let response = json!({"status": "error", "message": "Empty message"});
                let _ = responder.send(serde_json::to_vec(&response)?, 0);
                continue;
            }

            let request: Value = match serde_json::from_slice(&msg) {
                Ok(req) => req,
                Err(e) => {
                    error!("Failed to parse request for port {}: {}", self.port, e);
                    let response = json!({"status": "error", "message": format!("Parse error: {}", e)});
                    let _ = responder.send(serde_json::to_vec(&response)?, 0);
                    continue;
                }
            };

            let response = match request.get("command").and_then(|c| c.as_str()) {
                Some("initialize") | Some("status") | Some("ping") => {
                    json!({"status": "success", "port": self.port, "ipc_path": endpoint})
                }
                Some("set_key") => {
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            // Replicated error flow: send response and continue loop
                            let response = json!({"status": "error", "message": "Missing key in set_key request"});
                            let _ = responder.send(serde_json::to_vec(&response)?, 0);
                            continue;
                        }
                    };
                    let value = match request.get("value").and_then(|v| v.as_str()) {
                        Some(v) => v,
                        None => {
                            // Replicated error flow: send response and continue loop
                            let response = json!({"status": "error", "message": "Missing value in set_key request"});
                            let _ = responder.send(serde_json::to_vec(&response)?, 0);
                            continue;
                        }
                    };
                    
                    // Use self.insert (instance method)
                    match self.insert(key.as_bytes(), value.as_bytes()).await {
                        Ok(_) => {
                            // REPLICATE: Explicit flush and error handling from run_zmq_server_static
                            if let Err(e) = self.db.flush() {
                                json!({"status": "error", "message": format!("Failed to flush: {}", e)})
                            } else {
                                json!({"status": "success"})
                            }
                        }
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_key") => {
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            // Replicated error flow: send response and continue loop
                            let response = json!({"status": "error", "message": "Missing key in get_key request"});
                            let _ = responder.send(serde_json::to_vec(&response)?, 0);
                            continue;
                        }
                    };

                    // Use self.retrieve (instance method)
                    match self.retrieve(key.as_bytes()).await {
                        Ok(Some(val)) => {
                            let value_str = String::from_utf8_lossy(&val).to_string();
                            json!({"status": "success", "value": value_str})
                        }
                        Ok(None) => json!({"status": "success", "value": null}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("delete_key") => {
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            // Replicated error flow: send response and continue loop
                            let response = json!({"status": "error", "message": "Missing key in delete_key request"});
                            let _ = responder.send(serde_json::to_vec(&response)?, 0);
                            continue;
                        }
                    };
                    
                    // Use self.delete (instance method)
                    match self.delete(key.as_bytes()).await {
                        Ok(_) => {
                            // REPLICATE: Explicit flush and error handling from run_zmq_server_static
                            if let Err(e) = self.db.flush() {
                                json!({"status": "error", "message": format!("Failed to flush: {}", e)})
                            } else {
                                json!({"status": "success"})
                            }
                        }
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("flush") => {
                    match self.db.flush() {
                        Ok(bytes) => json!({"status": "success", "bytes_flushed": bytes}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_all_vertices") => {
                    let vertices_list: Vec<Value> = self.vertices.iter()
                        .filter_map(|res| res.ok())
                        .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
                        .collect();
                    json!({"status": "success", "vertices": vertices_list})
                }
                Some("get_all_edges") => {
                    let edges_list: Vec<Value> = self.edges.iter()
                        .filter_map(|res| res.ok())
                        .filter_map(|(_, v)| serde_json::from_slice(&v).ok())
                        .collect();
                    json!({"status": "success", "edges": edges_list})
                }
                Some("clear_data") => {
                    match (|| -> Result<usize, GraphError> {
                        self.vertices.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
                        self.edges.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
                        self.kv_pairs.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
                        let bytes = self.db.flush().map_err(|e| GraphError::StorageError(e.to_string()))?;
                        Ok(bytes)
                    })() {
                        Ok(bytes) => json!({"status": "success", "bytes_flushed": bytes}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some(cmd) => json!({"status": "error", "message": format!("Unsupported command: {}", cmd)}),
                None => json!({"status": "error", "message": "No command specified"}),
            };

            let _ = responder.send(serde_json::to_vec(&response)?, 0);
        }

        info!("ZMQ server shutting down for port {}", self.port);
        let _ = responder.disconnect(&endpoint);
        if tokio_fs::metadata(&socket_path).await.is_ok() {
            let _ = tokio_fs::remove_file(&socket_path).await;
        }
        Ok(())
    }

    async fn send_zmq_response(&self, socket: &ZmqSocket, response: &Value) {
        let response_data = serde_json::to_vec(response)
            .map_err(|e| {
                error!("Failed to serialize ZMQ response for port {}: {}", self.port, e);
                GraphError::StorageError(format!("Failed to serialize response: {}", e))
            })
            .expect("Failed to serialize response");
        if let Err(e) = socket.send(response_data, 0) {
            error!("Failed to send ZMQ response for port {}: {}", self.port, e);
        }
    }

    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down SledDaemon at path {:?}", self.db_path);
        let mut running = self.running.lock().await;
        if !*running {
            info!("SledDaemon already shut down at {:?}", self.db_path);
            return Ok(());
        }
        *running = false;
        drop(running);

        let socket_path = format!("/tmp/graphdb-{}.ipc", self.port);
        if tokio_fs::metadata(&socket_path).await.is_ok() {
            if let Err(e) = tokio_fs::remove_file(&socket_path).await {
                error!("Failed to remove IPC socket file {}: {}", socket_path, e);
            } else {
                info!("Successfully removed IPC socket file {}", socket_path);
            }
        }

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
        let metrics = self.raft.as_ref().ok_or_else(|| GraphError::StorageError("Raft not initialized".to_string()))?
            .metrics().await;
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
        if let Some(raft) = &self.raft {
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

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Inserting key into kv_pairs at path {:?}", self.db_path);
        timeout(TokioDuration::from_secs(5), async {
            let mut batch = Batch::default();
            batch.insert(key, value);
            self.kv_pairs
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch: {}", e)))?;

            let bytes_flushed = self.db.flush_async().await
                .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;
            info!("Flushed {} bytes after insert at {:?}", bytes_flushed, self.db_path);

            let persisted = self.kv_pairs
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify insert: {}", e)))?;
            if persisted.is_none() || persisted.as_ref().map(|v| v.as_ref()) != Some(value) {
                error!("Persistence verification failed for key at {:?}", self.db_path);
                return Err(GraphError::StorageError("Insert not persisted correctly".to_string()));
            }

            let keys: Vec<_> = self.kv_pairs
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current kv_pairs keys at {:?}: {:?}", self.db_path, keys);

            #[cfg(feature = "with-openraft-sled")]
            if let Some(raft) = &self.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft write failed: {}", e)))?;
                info!("Raft write replicated for key at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during insert".to_string()))?
    }

    pub async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        if !self.is_running().await {
            error!("Daemon at path {:?} is not running", self.db_path);
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        if key == b"test_key" {
            warn!("Retrieving test_key, caller stack trace: {:#?}", std::backtrace::Backtrace::capture());
        }
        info!("Retrieving key from kv_pairs at path {:?}", self.db_path);
        let value = timeout(TokioDuration::from_secs(5), async {
            let opt = self.kv_pairs
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to retrieve key: {}", e)))?;
            Ok::<Option<Vec<u8>>, GraphError>(opt.map(|ivec| ivec.to_vec()))
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during retrieve".to_string()))??;

        let keys: Vec<_> = self.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current kv_pairs keys at {:?}: {:?}", self.db_path, keys);
        Ok(value)
    }

    pub async fn delete(&self, key: &[u8]) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Deleting key from kv_pairs at path {:?}", self.db_path);
        timeout(TokioDuration::from_secs(5), async {
            let mut batch = Batch::default();
            batch.remove(key);
            self.kv_pairs
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch: {}", e)))?;

            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;

            let persisted = self.kv_pairs
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify delete: {}", e)))?;
            if persisted.is_some() {
                error!("Persistence verification failed for key delete at {:?}", self.db_path);
                return Err(GraphError::StorageError("Delete not persisted correctly".to_string()));
            }

            let keys: Vec<_> = self.kv_pairs
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after delete at {:?}, current kv_pairs keys: {:?}", bytes_flushed, self.db_path, keys);

            #[cfg(feature = "with-openraft-sled")]
            if let Some(raft) = &self.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft delete failed: {}", e)))?;
                info!("Raft delete replicated for key at {:?}", self.db_path);
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
        timeout(TokioDuration::from_secs(5), async {
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

            let persisted = self.vertices
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify vertex insert: {}", e)))?;
            if persisted.is_none() {
                error!("Persistence verification failed for vertex id {} at {:?}", vertex.id, self.db_path);
                return Err(GraphError::StorageError("Vertex insert not persisted".to_string()));
            }

            let vertex_keys: Vec<_> = self.vertices
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current vertices keys at {:?}: {:?}", self.db_path, vertex_keys);

            #[cfg(feature = "with-openraft-sled")]
            if let Some(raft) = &self.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: serialize_vertex(vertex)?,
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft vertex create failed: {}", e)))?;
                info!("Raft vertex create replicated at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))?
    }

    pub async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        if !self.is_running().await {
            error!("Daemon at path {:?} is not running", self.db_path);
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        let key = id.as_bytes();
        info!("Retrieving vertex with id {} from path {:?}", id, self.db_path);
        let res = timeout(TokioDuration::from_secs(5), async {
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
        Ok(res)
    }

    pub async fn update_vertex(&self, vertex: &Vertex) -> GraphResult<()> {
        self.ensure_write_access().await?;
        info!("Updating vertex with id {} at path {:?}", vertex.id, self.db_path);
        timeout(TokioDuration::from_secs(5), async {
            let key = vertex.id.0.as_bytes();
            let value = Self::serialize_to_ivec(vertex)?;
            let mut batch = Batch::default();
            batch.insert(key, value);
            self.vertices
                .apply_batch(batch)
                .map_err(|e| GraphError::StorageError(format!("Failed to apply batch for vertex update: {}", e)))?;
            let bytes_flushed = self.db
                .flush_async()
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;
            let persisted = self.vertices
                .get(key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify vertex update: {}", e)))?;
            if persisted.is_none() {
                error!("Persistence verification failed for vertex update id {} at {:?}", vertex.id, self.db_path);
                return Err(GraphError::StorageError("Vertex update not persisted".to_string()));
            }
            let vertex_keys: Vec<_> = self.vertices
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after updating vertex at {:?}, current vertices keys: {:?}", bytes_flushed, self.db_path, vertex_keys);
            #[cfg(feature = "with-openraft-sled")]
            if let Some(raft) = &self.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: serialize_vertex(vertex)?,
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft vertex update failed: {}", e)))?;
                info!("Raft vertex update replicated at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during update_vertex".to_string()))??;
        let vertex_keys: Vec<_> = self.vertices
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current vertices keys at {:?}: {:?}", self.db_path, vertex_keys);
        Ok(())
    }

    pub async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        self.ensure_write_access().await?;
        let key = id.as_bytes();
        info!("Deleting vertex with id {} from path {:?}", id, self.db_path);
        timeout(TokioDuration::from_secs(5), async {
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
            #[cfg(feature = "with-openraft-sled")]
            if let Some(raft) = &self.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft vertex delete failed: {}", e)))?;
                info!("Raft vertex delete replicated at {:?}", self.db_path);
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
        timeout(TokioDuration::from_secs(5), async {
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

            let persisted = self.edges
                .get(&key)
                .map_err(|e| GraphError::StorageError(format!("Failed to verify edge insert: {}", e)))?;
            if persisted.is_none() {
                error!("Persistence verification failed for edge ({}, {}, {}) at {:?}", 
                    edge.outbound_id, edge.t, edge.inbound_id, self.db_path);
                return Err(GraphError::StorageError("Edge insert not persisted".to_string()));
            }

            let edge_keys: Vec<_> = self.edges
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current edges keys at {:?}: {:?}", self.db_path, edge_keys);

            #[cfg(feature = "with-openraft-sled")]
            if let Some(raft) = &self.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: edge.t.to_string().into_bytes(),
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft edge create failed: {}", e)))?;
                info!("Raft edge create replicated at {:?}", self.db_path);
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
            return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
        }
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        info!("Retrieving edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, self.db_path);
        let res = timeout(TokioDuration::from_secs(5), async {
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
        timeout(TokioDuration::from_secs(5), async {
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
            #[cfg(feature = "with-openraft-sled")]
            if let Some(raft) = &self.raft {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                raft.client_write(request).await
                    .map_err(|e| GraphError::StorageError(format!("Raft edge delete failed: {}", e)))?;
                info!("Raft edge delete replicated at {:?}", self.db_path);
            }
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))?
    }

    pub async fn force_reset(&self) -> GraphResult<()> {
        info!("Resetting SledDaemon at path {:?}", self.db_path);
        println!("===> RESETTING SLED DAEMON AT PATH {:?}", self.db_path);
        timeout(TokioDuration::from_secs(5), async {
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

    fn serialize_to_ivec<T: serde::Serialize>(data: &T) -> GraphResult<IVec> {
        let serialized = serde_json::to_vec(data)
            .map_err(|e| GraphError::StorageError(format!("Serialization failed: {}", e)))?;
        Ok(IVec::from(serialized))
    }

    fn deserialize_from_ivec<T: serde::de::DeserializeOwned>(ivec: IVec) -> GraphResult<T> {
        serde_json::from_slice(&ivec)
            .map_err(|e| GraphError::StorageError(format!("Deserialization failed: {}", e)))
    }

    pub async fn insert_static(
        kv_pairs_tree: &Tree,
        db: &Db,
        db_path: &Path,
        key: &[u8],
        value: &[u8],
    ) -> GraphResult<()> {
        info!("Inserting key into kv_pairs at path {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            // 1. Apply batch
            let mut batch = Batch::default();
            batch.insert(key, value);
            handle_sled_op!(kv_pairs_tree.apply_batch(batch), "Failed to apply batch")?;

            // 2. Flush DB asynchronously
            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            info!("Flushed {} bytes after insert at {:?}", bytes_flushed, db_path);

            // 3. Verification
            let persisted = handle_sled_op!(kv_pairs_tree.get(key), "Failed to verify insert")?;
            if persisted.is_none() || persisted.as_ref().map(|v| v.as_ref()) != Some(value) {
                error!("Persistence verification failed for key at {:?}", db_path);
                return Err(GraphError::StorageError("Insert not persisted correctly".to_string()));
            }

            // 4. Log current keys
            let keys: Vec<_> = kv_pairs_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current kv_pairs keys at {:?}: {:?}", db_path, keys);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during insert".to_string()))?
    }

    pub async fn retrieve_static(
        kv_pairs_tree: &Tree,
        db_path: &Path,
        key: &[u8],
    ) -> GraphResult<Option<Vec<u8>>> {
        if key == b"test_key" {
            warn!("Retrieving test_key, caller stack trace: {:#?}", std::backtrace::Backtrace::capture());
        }
        info!("Retrieving key from kv_pairs at path {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let opt = handle_sled_op!(kv_pairs_tree.get(key), "Failed to retrieve key")?;
            let keys: Vec<_> = kv_pairs_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current kv_pairs keys at {:?}: {:?}", db_path, keys);
            Ok::<Option<Vec<u8>>, GraphError>(opt.map(|ivec| ivec.to_vec()))
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during retrieve".to_string()))?
    }

    pub async fn delete_static(
        kv_pairs_tree: &Tree,
        db: &Db,
        db_path: &Path,
        key: &[u8],
    ) -> GraphResult<()> {
        info!("Deleting key from kv_pairs at path {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            // 1. Apply batch (removal)
            let mut batch = Batch::default();
            batch.remove(key);
            handle_sled_op!(kv_pairs_tree.apply_batch(batch), "Failed to apply delete batch")?;

            // 2. Flush DB asynchronously
            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            info!("Flushed {} bytes after delete at {:?}", bytes_flushed, db_path);

            // 3. Verification
            let persisted = handle_sled_op!(kv_pairs_tree.get(key), "Failed to verify delete")?;
            if persisted.is_some() {
                error!("Persistence verification failed for key delete at {:?}", db_path);
                return Err(GraphError::StorageError("Delete not persisted correctly".to_string()));
            }

            // 4. Log current keys
            let keys: Vec<_> = kv_pairs_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current kv_pairs keys at {:?}: {:?}", db_path, keys);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete".to_string()))?
    }

    pub async fn create_vertex_static(
        vertices_tree: &Tree,
        db: &Db,
        db_path: &Path,
        vertex: &Vertex,
    ) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes();
        let value = Self::serialize_to_ivec(vertex)?;
        info!("Creating vertex with id {} at path {:?}", vertex.id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let mut batch = Batch::default();
            batch.insert(key, value);
            handle_sled_op!(vertices_tree.apply_batch(batch), "Failed to apply batch for vertex")?;

            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            info!("Flushed {} bytes after creating vertex at {:?}", bytes_flushed, db_path);

            let persisted = handle_sled_op!(vertices_tree.get(key), "Failed to verify vertex insert")?;
            if persisted.is_none() {
                error!("Persistence verification failed for vertex id {} at {:?}", vertex.id, db_path);
                return Err(GraphError::StorageError("Vertex insert not persisted".to_string()));
            }

            let vertex_keys: Vec<_> = vertices_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current vertices keys at {:?}: {:?}", db_path, vertex_keys);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))?
    }

    pub async fn get_vertex_static(
        vertices_tree: &Tree,
        db_path: &Path,
        id: &Uuid,
    ) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes();
        info!("Retrieving vertex with id {} from path {:?}", id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let opt = handle_sled_op!(vertices_tree.get(key), "Failed to retrieve vertex")?;
            let vertex_keys: Vec<_> = vertices_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current vertices keys at {:?}: {:?}", db_path, vertex_keys);
            match opt {
                Some(ivec) => {
                    let vertex = Self::deserialize_from_ivec(ivec)?;
                    Ok::<Option<Vertex>, GraphError>(Some(vertex))
                }
                None => Ok::<Option<Vertex>, GraphError>(None),
            }
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))?
    }

    pub async fn update_vertex_static(
        vertices_tree: &Tree,
        db: &Db,
        db_path: &Path,
        vertex: &Vertex,
    ) -> GraphResult<()> {
        info!("Updating vertex with id {} at path {:?}", vertex.id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let key = vertex.id.0.as_bytes();
            let value = Self::serialize_to_ivec(vertex)?;
            let mut batch = Batch::default();
            batch.insert(key, value);
            handle_sled_op!(vertices_tree.apply_batch(batch), "Failed to apply batch for vertex update")?;

            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            let persisted = handle_sled_op!(vertices_tree.get(key), "Failed to verify vertex update")?;
            if persisted.is_none() {
                error!("Persistence verification failed for vertex update id {} at {:?}", vertex.id, db_path);
                return Err(GraphError::StorageError("Vertex update not persisted".to_string()));
            }

            let vertex_keys: Vec<_> = vertices_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after updating vertex at {:?}, current vertices keys: {:?}", bytes_flushed, db_path, vertex_keys);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during update_vertex".to_string()))?
    }

    pub async fn delete_vertex_static(
        vertices_tree: &Tree,
        edges_tree: &Tree,
        db: &Db,
        db_path: &Path,
        id: &Uuid,
    ) -> GraphResult<()> {
        let key = id.as_bytes();
        info!("Deleting vertex with id {} from path {:?}", id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            handle_sled_op!(vertices_tree.remove(key), "Failed to remove vertex")?;

            let mut batch = Batch::default();
            let prefix = id.as_bytes();
            for item in edges_tree.iter().keys() {
                let k = handle_sled_op!(item, "Failed to iterate edges")?;
                if k.starts_with(prefix) {
                    batch.remove(k);
                }
            }
            handle_sled_op!(edges_tree.apply_batch(batch), "Failed to apply batch for edge removal")?;

            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;

            let vertex_keys: Vec<_> = vertices_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            let edge_keys: Vec<_> = edges_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after deleting vertex at {:?}, current vertices keys: {:?}, edges keys: {:?}", bytes_flushed, db_path, vertex_keys, edge_keys);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete_vertex".to_string()))?
    }

    fn create_edge_key(outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Vec<u8>> {
        let mut key = Vec::new();
        key.extend_from_slice(outbound_id.as_bytes());
        key.extend_from_slice(b":");
        key.extend_from_slice(edge_type.to_string().as_bytes());
        key.extend_from_slice(b":");
        key.extend_from_slice(inbound_id.as_bytes());
        Ok(key)
    }

    pub async fn create_edge_static(
        edges_tree: &Tree,
        db: &Db,
        db_path: &Path,
        edge: &Edge,
    ) -> GraphResult<()> {
        let key = Self::create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into())?;
        let value = Self::serialize_to_ivec(edge)?;
        info!("Creating edge ({}, {}, {}) at path {:?}", edge.outbound_id, edge.t, edge.inbound_id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let mut batch = Batch::default();
            batch.insert(&*key, value);
            handle_sled_op!(edges_tree.apply_batch(batch), "Failed to apply batch for edge")?;

            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            info!("Flushed {} bytes after creating edge at {:?}", bytes_flushed, db_path);

            let persisted = handle_sled_op!(edges_tree.get(&key), "Failed to verify edge insert")?;
            if persisted.is_none() {
                error!("Persistence verification failed for edge ({}, {}, {}) at {:?}", 
                    edge.outbound_id, edge.t, edge.inbound_id, db_path);
                return Err(GraphError::StorageError("Edge insert not persisted".to_string()));
            }

            let edge_keys: Vec<_> = edges_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current edges keys at {:?}: {:?}", db_path, edge_keys);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during create_edge".to_string()))?
    }

    pub async fn get_edge_static(
        edges_tree: &Tree,
        db_path: &Path,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
    ) -> GraphResult<Option<Edge>> {
        let key = Self::create_edge_key(outbound_id, edge_type, inbound_id)?;
        info!("Retrieving edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let opt = handle_sled_op!(edges_tree.get(&key), "Failed to retrieve edge")?;
            let edge_keys: Vec<_> = edges_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Current edges keys at {:?}: {:?}", db_path, edge_keys);
            match opt {
                Some(ivec) => {
                    let edge = Self::deserialize_from_ivec(ivec)?;
                    Ok::<Option<Edge>, GraphError>(Some(edge))
                }
                None => Ok::<Option<Edge>, GraphError>(None),
            }
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))?
    }

    pub async fn update_edge_static(
        edges_tree: &Tree,
        db: &Db,
        db_path: &Path,
        edge: &Edge,
    ) -> GraphResult<()> {
        Self::create_edge_static(edges_tree, db, db_path, edge).await
    }

    pub async fn delete_edge_static(
        edges_tree: &Tree,
        db: &Db,
        db_path: &Path,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
    ) -> GraphResult<()> {
        let key = Self::create_edge_key(outbound_id, edge_type, inbound_id)?;
        info!("Deleting edge ({}, {}, {}) from path {:?}", outbound_id, edge_type, inbound_id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            handle_sled_op!(edges_tree.remove(&key), "Failed to remove edge")?;
            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;

            let edge_keys: Vec<_> = edges_tree
                .iter()
                .keys()
                .filter_map(|k| k.ok())
                .collect();
            info!("Flushed {} bytes after deleting edge at {:?}, current edges keys: {:?}", bytes_flushed, db_path, edge_keys);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))?
    }

    pub async fn force_reset_static(
        db: &Db,
        db_path: &Path,
    ) -> GraphResult<()> {
        info!("Resetting SledDaemon at path {:?}", db_path);
        println!("===> RESETTING SLED DAEMON AT PATH {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            handle_sled_op!(db.clear(), "Failed to clear database")?;
            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            info!("Flushed {} bytes after resetting daemon at {:?}", bytes_flushed, db_path);
            println!("===> FORCE_RESET: FLUSHED {} BYTES", bytes_flushed);

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during force_reset".to_string()))?
    }

    pub async fn force_unlock_static(_db_path: &Path) -> GraphResult<()> {
        // No-op as per non-static version
        Ok(())
    }

    pub async fn force_unlock_path_static(_db_path: &Path) -> GraphResult<()> {
        // No-op as per non-static version
        Ok(())
    }

    pub async fn flush_static(db: &Db, db_path: &Path) -> GraphResult<()> {
        info!("SledDaemon::flush_static - Flushing database at {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            info!("SledDaemon::flush_static - Successfully flushed {} bytes", bytes_flushed);
            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during flush".to_string()))?
    }

    pub async fn get_all_vertices_static(
        vertices_tree: &Tree,
        db_path: &Path,
    ) -> GraphResult<Vec<Vertex>> {
        info!("SledDaemon::get_all_vertices_static - Retrieving all vertices from path {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let vertices: Vec<Vertex> = vertices_tree
                .iter()
                .filter_map(|res| res.ok())
                .filter_map(|(_k, v)| Self::deserialize_from_ivec(v).ok())
                .collect();
            info!("SledDaemon::get_all_vertices_static - Retrieved {} vertices", vertices.len());
            println!("===> SLED DAEMON GET_ALL_VERTICES_STATIC - RETRIEVED {} VERTICES", vertices.len());
            Ok(vertices)
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_all_vertices".to_string()))?
    }

    pub async fn get_all_edges_static(
        edges_tree: &Tree,
        db_path: &Path,
    ) -> GraphResult<Vec<Edge>> {
        info!("SledDaemon::get_all_edges_static - Retrieving all edges from path {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let edges: Vec<Edge> = edges_tree
                .iter()
                .filter_map(|res| res.ok())
                .filter_map(|(_k, v)| Self::deserialize_from_ivec(v).ok())
                .collect();
            info!("SledDaemon::get_all_edges_static - Retrieved {} edges", edges.len());
            println!("===> SLED DAEMON GET_ALL_EDGES_STATIC - RETRIEVED {} EDGES", edges.len());
            Ok(edges)
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during get_all_edges".to_string()))?
    }

    pub async fn clear_data_static(
        kv_pairs_tree: &Tree,
        db: &Db,
        db_path: &Path,
    ) -> GraphResult<()> {
        info!("SledDaemon::clear_data_static - Clearing all data in tree at {:?}", db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            // Clear the tree
            handle_sled_op!(kv_pairs_tree.clear(), "Failed to clear data tree")?;

            // Flush DB
            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB after clear")?;
            info!("Flushed {} bytes after clear at {:?}", bytes_flushed, db_path);

            // Verify
            if kv_pairs_tree.iter().next().is_some() {
                return Err(GraphError::StorageError("Failed to clear all data".to_string()));
            }

            Ok(())
        })
        .await
        .map_err(|_| GraphError::StorageError("Timeout during clear_data".to_string()))?
    }

}


impl SledDaemonPool {
    // Fixes:
    // 1. Removed the erroneous `self.load_balancer.lock().await` call.
    // 2. Access the protected fields (`healthy_nodes`, `current_index`, `nodes`) directly via `self.load_balancer`.
    // 3. Renamed lock guards (e.g., `healthy_nodes_lock`) for clarity.

    pub async fn select_daemon(&self) -> Option<u16> {
        // Acquire a write lock on healthy_nodes to prevent changes while we are reading length
        // and calculating the next index.
        let mut healthy_nodes_lock = self.load_balancer.healthy_nodes.write().await;

        if healthy_nodes_lock.is_empty() {
            return None;
        }

        // Acquire lock on the current index counter.
        let mut index_guard = self.load_balancer.current_index.lock().await;
        
        let selected_port = healthy_nodes_lock[*index_guard % healthy_nodes_lock.len()].port;
        
        // Update the index for the next request in a Round-Robin fashion.
        *index_guard = (*index_guard + 1) % healthy_nodes_lock.len();
        
        // index_guard lock is implicitly released here.
        
        // Acquire a write lock on the main nodes map to update statistics.
        let mut nodes_lock = self.load_balancer.nodes.write().await;
        
        if let Some(node) = nodes_lock.get_mut(&selected_port) {
            node.request_count += 1;
            node.last_check = SystemTime::now();
        }
        
        // healthy_nodes_lock and nodes_lock are implicitly released here.
        Some(selected_port)
    }

    async fn update_node_health(&self, port: u16, is_healthy: bool, response_time_ms: u64) {
        // Acquire locks on the two data structures that need updating.
        // NOTE: Order matters to avoid deadlocks (e.g., always lock nodes before healthy_nodes).
        let mut nodes_lock = self.load_balancer.nodes.write().await;
        let mut healthy_nodes_lock = self.load_balancer.healthy_nodes.write().await;
        
        let now = SystemTime::now();

        // 1. Update/Insert the node in the main `nodes` map
        if let Some(node) = nodes_lock.get_mut(&port) {
            node.is_healthy = is_healthy;
            node.last_check = now;
            node.response_time_ms = response_time_ms;
            node.error_count = if is_healthy { 0 } else { node.error_count + 1 };
        } else {
            nodes_lock.insert(port, NodeHealth {
                port,
                is_healthy,
                last_check: now,
                response_time_ms,
                error_count: if is_healthy { 0 } else { 1 },
                request_count: 0,
            });
        }

        // 2. Update the `healthy_nodes` list
        if is_healthy {
            // Retrieve the full, updated NodeHealth data from the main map.
            let node_data = nodes_lock.get(&port).expect("Node must exist in map after update/insert.");

            // Check if the node is already in the healthy list (retaining original inefficient but functional check).
            if !healthy_nodes_lock.iter().any(|n| n.port == port) {
                // Push a fresh copy of the NodeHealth state into the healthy list.
                healthy_nodes_lock.push_back(node_data.clone()); // Assuming NodeHealth implements Clone
            }
            // Note: If the node was already present, its state in healthy_nodes is now stale.
        } else {
            // Remove the unhealthy node from the healthy list.
            healthy_nodes_lock.retain(|n| n.port != port);
        }
    }

    /// Check if the ZMQ server is running for a given port by attempting to connect and send a ping.
    ///
    /// The method signature is fixed by using `self` to call the instance method.
    async fn is_zmq_server_running(&self, port: u16) -> GraphResult<bool> {
        let selected_port = self.select_daemon().await.unwrap_or(port);
        info!("===> Checking if ZMQ server is running on selected port {}", selected_port);
        println!("===> CHECKING IF ZMQ SERVER IS RUNNING ON SELECTED PORT {}", selected_port);
        let start = SystemTime::now();
        match self.check_zmq_readiness(selected_port).await {
            Ok(()) => {
                let response_time_ms = start.elapsed().map(|d| d.as_millis() as u64).unwrap_or(0);
                info!("===> ZMQ server is running on port {}", selected_port);
                println!("===> ZMQ SERVER IS RUNNING ON PORT {}", selected_port);
                self.update_node_health(selected_port, true, response_time_ms).await;
                Ok(true)
            }
            Err(e) => {
                let response_time_ms = start.elapsed().map(|d| d.as_millis() as u64).unwrap_or(0);
                warn!("===> ZMQ server not running on port {}: {}", selected_port, e);
                println!("===> ZMQ SERVER NOT RUNNING ON PORT {}: {}", selected_port, e);
                self.update_node_health(selected_port, false, response_time_ms).await;
                Ok(false)
            }
        }
    }

    /// Checks the ZMQ connection and response readiness.
    ///
    /// FIX: This method now wraps all blocking ZMQ calls in `tokio::task::spawn_blocking`
    /// to resolve the `*mut c_void cannot be shared between threads safely` (E0277) error.
    // This function signature (and the surrounding struct/impl) remains the same, 
    // but the body is updated to use spawn_blocking.

    async fn check_zmq_readiness(&self, port: u16) -> GraphResult<()> {
        info!("===> Checking ZMQ readiness for port {}", port);
        println!("===> CHECKING ZMQ READINESS FOR PORT {}", port);

        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);

        // Move ZMQ operations to a blocking task to isolate non-Send zmq::Socket
        let result = tokio::task::spawn_blocking(move || {
            let context = ZmqContext::new();
            let socket = context.socket(zmq::REQ).map_err(|e| {
                error!("Failed to create ZMQ socket for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO CREATE ZMQ SOCKET FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e))
            })?;

            socket.set_rcvtimeo(2000).map_err(|e| {
                error!("Failed to set receive timeout for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO SET RECEIVE TIMEOUT FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to set receive timeout: {}", e))
            })?;
            socket.set_sndtimeo(2000).map_err(|e| {
                error!("Failed to set send timeout for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO SET SEND TIMEOUT FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to set send timeout: {}", e))
            })?;

            socket.connect(&endpoint).map_err(|e| {
                error!("Failed to connect to ZMQ endpoint {}: {}", endpoint, e);
                println!("===> ERROR: FAILED TO CONNECT TO ZMQ ENDPOINT {}: {}", endpoint, e);
                GraphError::StorageError(format!("Failed to connect to ZMQ endpoint {}: {}", endpoint, e))
            })?;

            // Send JSON-formatted status request
            let request = json!({ "command": "status" });
            let request_data = serde_json::to_vec(&request).map_err(|e| {
                error!("Failed to serialize status request for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO SERIALIZE STATUS REQUEST FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to serialize status request: {}", e))
            })?;
            socket.send(request_data, 0).map_err(|e| {
                error!("Failed to send status request to {}: {}", endpoint, e);
                println!("===> ERROR: FAILED TO SEND STATUS REQUEST TO {}: {}", endpoint, e);
                GraphError::StorageError(format!("Failed to send status request to {}: {}", endpoint, e))
            })?;

            let reply = socket.recv_bytes(0).map_err(|e| {
                error!("Failed to receive status response from {}: {}", endpoint, e);
                println!("===> ERROR: FAILED TO RECEIVE STATUS RESPONSE FROM {}: {}", endpoint, e);
                GraphError::StorageError(format!("Failed to receive status response from {}: {}", endpoint, e))
            })?;

            let response: Value = serde_json::from_slice(&reply).map_err(|e| {
                error!("Failed to parse status response from {}: {}", endpoint, e);
                println!("===> ERROR: FAILED TO PARSE STATUS RESPONSE FROM {}: {}", endpoint, e);
                GraphError::StorageError(format!("Failed to parse status response: {}", e))
            })?;

            if response.get("status").and_then(|s| s.as_str()) == Some("success") {
                info!("ZMQ server responded with success for port {}", port);
                println!("===> ZMQ SERVER RESPONDED WITH SUCCESS FOR PORT {}", port);
                Ok(())
            } else {
                let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
                error!("ZMQ server returned unexpected response: {}", error_msg);
                println!("===> ERROR: ZMQ SERVER RETURNED UNEXPECTED RESPONSE: {}", error_msg);
                Err(GraphError::StorageError(format!("Unexpected response from ZMQ server: {}", error_msg)))
            }
        })
        .await
        .map_err(|e| {
            error!("Failed to execute blocking task for ZMQ check on port {}: {}", port, e);
            println!("===> ERROR: FAILED TO EXECUTE BLOCKING TASK FOR ZMQ CHECK ON PORT {}: {}", port, e);
            GraphError::StorageError(format!("Failed to execute blocking task: {}", e))
        })?;

        tokio::time::timeout(TokioDuration::from_secs(2), async { result })
            .await
            .map_err(|_| {
                error!("Timeout waiting for ZMQ readiness on port {}", port);
                println!("===> ERROR: TIMEOUT WAITING FOR ZMQ READINESS ON PORT {}", port);
                GraphError::StorageError(format!("Timeout waiting for ZMQ readiness on port {}", port))
            })?
    }

    pub async fn is_zmq_reachable(&self, port: u16) -> GraphResult<bool> {
        
        // 1. Acquire lock on the clients map for safe read access.
        let clients_guard = self.clients.lock().await;

        // 2. Try to get the client. We check for existence, but cannot use the trait object
        // (Arc<dyn GraphStorageEngine>) directly for ZMQ communication as it lacks the 
        // necessary methods.
        let _client_cache_entry = match clients_guard.get(&port) {
            Some(c) => Some(c.clone()),
            None => {
                // Client doesn't exist for this port.
                return Ok(false)
            },
        };
        
        // 3. IMPORTANT: Release the lock immediately before making the network call.
        drop(clients_guard);

        // 4. Perform a raw ZMQ ping using a blocking task. Raw ZMQ operations (connect, send, recv) 
        // are synchronous and must not block the Tokio runtime.
        task::spawn_blocking(move || {
            let context = ZmqContext::new();
            let socket_address = format!("tcp://127.0.0.1:{}", port);
            
            // Create a temporary REQ socket for the ping.
            let socket = match context.socket(zmq::REQ) {
                Ok(s) => s,
                Err(e) => {
                    debug!("ZMQ ping failed: Failed to create ZMQ socket: {}", e);
                    return Ok(false);
                },
            };

            // Set a short timeout for the connection and request to ensure the check is fast.
            // These timeouts are critical for a fast health check.
            let _ = socket.set_rcvtimeo(200); 
            let _ = socket.set_sndtimeo(200);

            // Try to connect to the daemon's port
            if let Err(e) = socket.connect(&socket_address) {
                debug!("ZMQ ping failed: Failed to connect to {}: {}", socket_address, e);
                // Connection failure means the socket is not open/daemon is down.
                return Ok(false);
            }

            let ping_request = json!({ "command": "ping" }).to_string();
            
            // Send the ping
            if let Err(e) = socket.send(&ping_request, 0) {
                debug!("ZMQ ping failed: Failed to send request to {}: {}", socket_address, e);
                return Ok(false);
            }

            // Receive the response
            let mut msg = zmq::Message::new();
            match socket.recv(&mut msg, 0) {
                Ok(_) => {
                    let response_str = msg.as_str().unwrap_or("{}");
                    match serde_json::from_str::<Value>(response_str) {
                        Ok(response) => {
                            // Check for a success response from the daemon
                            let is_success = response.get("status").and_then(|s| s.as_str()) == Some("success");
                            if !is_success {
                                debug!("ZMQ ping failed: Response status not 'success' from {}", socket_address);
                            }
                            Ok(is_success)
                        },
                        Err(e) => {
                            debug!("ZMQ ping failed: Failed to parse JSON response from {}: {}", socket_address, e);
                            Ok(false)
                        },
                    }
                }
                // FIX: Remove specific ETIMEDOUT match, as any error on recv (including timeout) 
                // means the ping failed.
                Err(e) => {
                    // This branch handles all ZMQ errors, including timeouts (due to set_rcvtimeo)
                    debug!("ZMQ ping failed: Error receiving response from {}: {}", socket_address, e);
                    Ok(false)
                }
            }
        })
        .await
        .map_err(|e| GraphError::ZmqError(format!("ZMQ blocking task failed: {:?}", e)))?
    }


    /// Waits for the Sled Daemon on the given port to become fully ready.
    /// This is done by checking for the existence of the expected ZMQ IPC socket file.
    async fn wait_for_daemon_ready(&self, port: u16) -> GraphResult<()> {
        let ipc_path_str = format!("/tmp/graphdb-{}.ipc", port);
        let ipc_path = std::path::Path::new(&ipc_path_str);
        
        info!("Waiting for daemon IPC socket to appear at: {}", ipc_path_str);
        println!("===> Waiting for daemon IPC socket to appear at: {}", ipc_path_str);

        for attempt in 0..MAX_WAIT_ATTEMPTS {
            if ipc_path.exists() {
                info!("IPC socket found at {} after {} attempts. Daemon is ready.", ipc_path_str, attempt + 1);
                println!("===> DAEMON IPC SOCKET FOUND AT {} AFTER {} ATTEMPTS. DAEMON IS READY.", ipc_path_str, attempt + 1);
                return Ok(());
            }

            debug!("Waiting for IPC socket {} to appear (attempt {}/{})", ipc_path_str, attempt + 1, MAX_WAIT_ATTEMPTS);
            sleep(TokioDuration::from_millis(WAIT_DELAY_MS)).await;
        }
        
        error!("Sled Daemon on port {} failed to create IPC socket {} within the timeout.", port, ipc_path_str);
        println!("===> Sled Daemon on port {} FAILED to create IPC socket {} within the timeout.", port, ipc_path_str);
        Err(GraphError::DaemonStartError(format!(
            "Daemon on port {} started but failed to bind ZMQ IPC socket at {} within timeout ({} attempts).",
            port, ipc_path_str, MAX_WAIT_ATTEMPTS
        )))
    }

    async fn start_new_daemon(
        &mut self,
        _engine_config: &StorageConfig,
        port: u16,
        sled_path: &PathBuf,
    ) -> GraphResult<DaemonMetadata> {
        info!("Starting new daemon for port {}", port);
        println!("===> STARTING NEW DAEMON FOR PORT {}", port);

        let daemon_config = SledConfig {
            path: sled_path.clone(), // Use Some(sled_path.clone())
            port: Some(port),
            ..Default::default()
        };
        // NOTE: The line `daemon_config.path = sled_path.clone();` is redundant since it's set above.
        // It's removed to clean up the code.

        // FIX: Destructure the result from SledDaemon::new to get the daemon and the receiver.
        let (daemon, _shutdown_rx) = SledDaemon::new(daemon_config).await?;

        // Wait for ZMQ server to start
        tokio::time::timeout(TokioDuration::from_secs(10), async {
            // Check if the server is running. If `is_zmq_server_running` returns a Result,
            // we need to explicitly handle the error type in the async block.
            while !self.is_zmq_server_running(port).await? {
                tokio::time::sleep(TokioDuration::from_millis(100)).await;
            }
            // Explicitly return Ok to match the expected inner Result type after unwrapping
            Ok::<(), GraphError>(())
        })
        .await
        .map_err(|_| {
            error!("Timeout waiting for ZMQ server to start on port {}", port);
            println!("===> ERROR: TIMEOUT WAITING FOR ZMQ SERVER TO START ON PORT {}", port);
            GraphError::StorageError(format!("Timeout waiting for ZMQ server on port {}", port))
        })??;

        // The type of 'daemon' is now SledDaemon, which matches the expected type for daemons.
        self.daemons.insert(port, Arc::new(daemon));
        info!("Added new daemon to pool for port {}", port);
        println!("===> ADDED NEW DAEMON TO POOL FOR PORT {}", port);

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(sled_path.clone()),
            config_path: None,
            engine_type: Some(StorageEngineType::Sled.to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        };

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        tokio::time::timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata.clone()))
            .await
            .map_err(|_| {
                error!("Timeout registering daemon on port {}", port);
                println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", port);
                GraphError::StorageError(format!("Timeout registering daemon on port {}", port))
            })?
            .map_err(|e| {
                error!("Failed to register daemon on port {}: {}", port, e);
                println!("===> ERROR: FAILED TO REGISTER DAEMON ON PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
            })?;

        self.load_balancer.update_node_health(port, true, 0).await;
        Ok(daemon_metadata)
    }
   
    pub fn new() -> Self {
        println!("SledDaemonPool new =================> LET US SEE IF THIS WAS EVER CALLED");
        Self {
            daemons: HashMap::new(),
            registry: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
            load_balancer: Arc::new(LoadBalancer::new(3)), // Default replication factor of 3
            use_raft_for_scale: false,
            clients: Arc::new(TokioMutex::new(HashMap::new())),
        }
    }

    pub async fn new_with_db(config: &SledConfig, existing_db: Arc<sled::Db>) -> GraphResult<Self> {
        let mut pool = Self::new();
        pool.initialize_with_db(config, existing_db).await?;
        Ok(pool)
    }

    pub async fn new_with_client(client: SledClient, db_path: &Path, port: u16) -> GraphResult<Self> {
        println!("============> In pool new_with_client - this must start zeromq server");

        let mut pool = Self::new();

        // Safely unwrap the Option to get the Arc<Mutex<Db>>
        let db = client.inner
            .as_ref()
            .ok_or_else(|| GraphError::StorageError("No database available in client".to_string()))?
            .lock()
            .await
            .clone();

        let vertices = db
            .open_tree("vertices")
            .map_err(|e| GraphError::StorageError(format!("Failed to open vertices tree: {}", e)))?;
        let edges = db
            .open_tree("edges")
            .map_err(|e| GraphError::StorageError(format!("Failed to open edges tree: {}", e)))?;
        let kv_pairs = db
            .open_tree("kv_pairs")
            .map_err(|e| GraphError::StorageError(format!("Failed to open kv_pairs tree: {}", e)))?;

        pool.daemons.insert(
            port,
            Arc::new(SledDaemon {
                port,
                db_path: db_path.to_path_buf(),
                db,
                vertices,
                edges,
                kv_pairs,
                running: Arc::new(TokioMutex::new(true)),
                #[cfg(feature = "with-openraft-sled")]
                raft_storage: Arc::new(
                    SledRaftStorage::new(db.clone())
                        .await
                        .map_err(|e| GraphError::StorageError(format!("Failed to create Raft storage: {}", e)))?,
                ),
                #[cfg(feature = "with-openraft-sled")]
                node_id: port as u64, // Use port as node_id for simplicity
            }),
        );

        Ok(pool)
    }

    /// Adds a new SledDaemon instance to the pool.
    /// This method is essential for `SledStorage::new_with_client` to work.
    pub fn add_daemon(&mut self, daemon: Arc<SledDaemon>) {
        self.daemons.insert(daemon.port, daemon);
    }

    async fn delete_replicated(&self, key: &[u8], use_raft_for_scale: bool, _mode: Option<SledClientMode>) -> GraphResult<()> {
        if use_raft_for_scale {
            #[cfg(feature = "with-openraft-sled")]
            {
                // Handle Raft deletion
                let daemon = self.daemons.iter().next();
                if let Some((_port, daemon)) = daemon {
                    let raft_storage = &daemon.raft_storage;
                    // Placeholder for Raft deletion logic
                    return Err(GraphError::StorageError("Raft deletion not implemented".to_string()));
                } else {
                    return Err(GraphError::StorageError("No daemon available for Raft deletion".to_string()));
                }
            }
            #[cfg(not(feature = "with-openraft-sled"))]
            {
                return Err(GraphError::StorageError("Raft support not enabled".to_string()));
            }
        } else {
            // Direct deletion
            let daemon = self.daemons.iter().next();
            if let Some((_port, daemon)) = daemon {
                daemon.db.remove(key).map_err(|e| GraphError::StorageError(format!("Failed to delete key: {}", e)))?;
                Ok(())
            } else {
                Err(GraphError::StorageError("No daemon available for deletion".to_string()))
            }
        }
    }

    /// Enhanced insert with replication across multiple nodes
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
        
        // For Raft, use leader-based replication
         #[cfg(feature = "with-openraft-sled")]
        if matches!(strategy, ReplicationStrategy::Raft) && self.use_raft_for_scale {
            return self.insert_raft(key, value).await;
        }
        
        // For non-Raft replication, write to multiple nodes
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
        
        // Require majority success for write confirmation
        let required_success = (write_nodes.len() / 2) + 1;
        if success_count >= required_success {
            println!("===> REPLICATED INSERT: Success! {}/{} nodes confirmed write", success_count, write_nodes.len());
            Ok(())
        } else {
            error!("===> REPLICATED INSERT: Failed! Only {}/{} nodes confirmed write", success_count, write_nodes.len());
            Err(GraphError::StorageError(format!(
                "Write failed: only {}/{} nodes succeeded. Errors: {:?}",
                success_count, write_nodes.len(), errors
            )))
        }
    }

    /// Insert data to a specific node
    async fn insert_to_node(&self, port: u16, key: &[u8], value: &[u8]) -> GraphResult<()> {
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

    /// Enhanced retrieve with failover across multiple nodes
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

    /// Retrieve data from a specific node
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

    #[cfg(feature = "with-openraft-sled")]
    /// Insert using Raft consensus
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

    /// Comprehensive health check of all nodes
    pub async fn health_check_all_nodes(&self) -> GraphResult<()> {
        let _healthy_ports = self.load_balancer.get_healthy_nodes().await;
        let all_ports: Vec<u16> = self.daemons.keys().copied().collect();

        println!("===> HEALTH CHECK: Checking {} total nodes", all_ports.len());

        let health_config = HealthCheckConfig {
            interval: TokioDuration::from_secs(10),
            connect_timeout: TokioDuration::from_secs(2),
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


    /// Health check for a single node
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
                        warn!("Failed to read response from daemon on port {}. Reason: {}", port, e);
                        return Ok(false);
                    },
                    Err(_) => {
                        self.load_balancer.update_node_health(port, false, 0).await;
                        warn!("Timeout waiting for response from daemon on port {}.", port);
                        return Ok(false);
                    }
                };

                let response_time = start_time.elapsed().unwrap_or(TokioDuration::from_millis(0)).as_millis() as u64;

                let response: Value = serde_json::from_slice(&response_buffer[..bytes_read])
                    .map_err(|e| GraphError::DeserializationError(e.to_string()))?;

                let is_healthy = response["status"] == "ok";
                self.load_balancer.update_node_health(port, is_healthy, response_time).await;

                if is_healthy {
                    info!("Health check successful for node on port {}. Response time: {}ms. Status: {}", port, response_time, response);
                } else {
                    warn!("Health check failed for node on port {}. Reason: Status is not 'ok'. Full response: {}", port, response);
                }

                Ok(is_healthy)
            },
            Ok(Err(e)) => {
                self.load_balancer.update_node_health(port, false, 0).await;
                warn!("Health check failed to connect to node on port {}. Reason: {}", port, e);
                Ok(false)
            },
            Err(_) => {
                self.load_balancer.update_node_health(port, false, 0).await;
                warn!("Health check connection timed out for node on port {}.", port);
                Ok(false)
            },
        }
    }

    pub async fn initialize_with_db(&mut self, config: &SledConfig, existing_db: Arc<sled::Db>) -> GraphResult<()> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("SledDaemonPool already initialized, skipping");
            println!("===> WARNING: SLED DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            return Ok::<(), GraphError>(());
        }

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let base_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Initializing SledDaemonPool with existing DB on port {} with path {:?}", port, db_path);
        println!("===> INITIALIZING SLED DAEMON POOL WITH EXISTING DB ON PORT {} WITH PATH {:?}", port, db_path);

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        if let Some(metadata) = daemon_registry.get_daemon_metadata(port).await? {
            info!("Found existing daemon metadata on port {} at path {:?}", port, metadata.data_dir);
            println!("===> FOUND EXISTING DAEMON METADATA ON PORT {} AT PATH {:?}", port, metadata.data_dir);

            if self.is_zmq_server_running(port).await? {
                info!("ZMQ server is running on port {}, reusing existing daemon", port);
                println!("===> ZMQ SERVER IS RUNNING ON PORT {}, REUSING EXISTING DAEMON", port);
                if let Some(registered_path) = &metadata.data_dir {
                    if registered_path == &db_path {
                        *initialized = true;
                        self.load_balancer.update_node_health(port, true, 0).await;
                        let health_config = HealthCheckConfig {
                            interval: TokioDuration::from_secs(10),
                            connect_timeout: TokioDuration::from_secs(2),
                            response_buffer_size: 1024,
                        };
                        self.start_health_monitoring(health_config).await;
                        info!("Started health monitoring for port {}", port);
                        println!("===> STARTED HEALTH MONITORING FOR PORT {}", port);
                        return Ok::<(), GraphError>(());
                    } else {
                        warn!("Path mismatch: daemon registry shows {:?}, but config specifies {:?}", registered_path, db_path);
                        println!("===> PATH MISMATCH: DAEMON REGISTRY SHOWS {:?}, BUT CONFIG SPECIFIES {:?}", registered_path, db_path);
                        if registered_path.exists() {
                            warn!("Old path {:?} still exists. Attempting cleanup.", registered_path);
                            println!("===> OLD PATH {:?} STILL EXISTS. ATTEMPTING CLEANUP.", registered_path);
                            if let Err(e) = tokio_fs::remove_dir_all(registered_path).await {
                                error!("Failed to remove old directory at {:?}: {}", registered_path, e);
                                println!("===> ERROR: FAILED TO REMOVE OLD DIRECTORY AT {:?}: {}", registered_path, e);
                                warn!("Continuing initialization despite cleanup failure: {}", e);
                            } else {
                                info!("Successfully removed old directory at {:?}", registered_path);
                                println!("===> SUCCESSFULLY REMOVED OLD DIRECTORY AT {:?}", registered_path);
                            }
                        }
                        timeout(TokioDuration::from_secs(5), daemon_registry.unregister_daemon(port))
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
            } else {
                warn!("Daemon registered on port {} but ZMQ server is not running. Unregistering and restarting.", port);
                println!("===> WARNING: DAEMON REGISTERED ON PORT {} BUT ZMQ SERVER IS NOT RUNNING. UNREGISTERING AND RESTARTING.", port);
                timeout(TokioDuration::from_secs(5), daemon_registry.unregister_daemon(port))
                    .await
                    .map_err(|_| {
                        warn!("Timeout unregistering daemon on port {}", port);
                        println!("===> WARNING: TIMEOUT UNREGISTERING DAEMON ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout unregistering daemon on port {}", port))
                    })?;
            }
        }

        if !db_path.exists() {
            info!("Creating Sled directory at {:?}", db_path);
            println!("===> CREATING SLED DIRECTORY AT {:?}", db_path);
            tokio_fs::create_dir_all(&db_path)
                .await
                .map_err(|e| {
                    error!("Failed to create directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO CREATE DIRECTORY AT {:?}: {}", db_path, e);
                    GraphError::StorageError(format!("Failed to create directory at {:?}", db_path))
                })?;
            tokio_fs::set_permissions(&db_path, std::fs::Permissions::from_mode(0o700))
                .await
                .map_err(|e| {
                    error!("Failed to set permissions on directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO SET PERMISSIONS ON DIRECTORY AT {:?}: {}", db_path, e);
                    GraphError::StorageError(format!("Failed to set permissions on directory at {:?}", db_path))
                })?;
        } else if !db_path.is_dir() {
            error!("Path {:?} is not a directory", db_path);
            println!("===> ERROR: PATH {:?} IS NOT A DIRECTORY", db_path);
            return Err(GraphError::StorageError(format!("Path {:?} is not a directory", db_path)));
        }

        let metadata = tokio_fs::metadata(&db_path).await.map_err(|e| {
            error!("Failed to access directory metadata at {:?}: {}", db_path, e);
            println!("===> ERROR: FAILED TO ACCESS DIRECTORY METADATA AT {:?}: {}", db_path, e);
            GraphError::StorageError(format!("Failed to access directory metadata at {:?}", db_path))
        })?;
        if metadata.permissions().readonly() {
            error!("Directory at {:?} is not writable", db_path);
            println!("===> ERROR: DIRECTORY AT {:?} IS NOT WRITABLE", db_path);
            return Err(GraphError::StorageError(format!("Directory at {:?} is not writable", db_path)));
        }

        info!("Creating new SledDaemon for port {}", port);
        println!("===> CREATING NEW SLED DAEMON FOR PORT {}", port);
        let mut updated_config = config.clone();
        updated_config.path = db_path.clone();
        let daemon = SledDaemon::new_with_db(updated_config, existing_db).await?;

        timeout(TokioDuration::from_secs(10), async {
            while !self.is_zmq_server_running(port).await? {
                tokio::time::sleep(TokioDuration::from_millis(100)).await;
            }
            Ok::<(), GraphError>(())
        })
        .await
        .map_err(|_| {
            error!("Timeout waiting for ZMQ server to start on port {}", port);
            println!("===> ERROR: TIMEOUT WAITING FOR ZMQ SERVER TO START ON PORT {}", port);
            GraphError::StorageError(format!("Timeout waiting for ZMQ server on port {}", port))
        })?;

        info!("SledDaemon created successfully for port {}", port);
        println!("===> SLED DAEMON CREATED SUCCESSFULLY FOR PORT {}", port);

        self.daemons.insert(port, Arc::new(daemon));
        info!("Added daemon to pool for port {}", port);
        println!("===> ADDED DAEMON TO POOL FOR PORT {}", port);

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(db_path.clone()),
            config_path: None,
            engine_type: Some(StorageEngineType::Sled.to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
        };

        timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata))
            .await
            .map_err(|_| {
                error!("Timeout registering daemon on port {}", port);
                println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", port);
                GraphError::StorageError(format!("Timeout registering daemon on port {}", port))
            })?
            .map_err(|e| {
                error!("Failed to register daemon on port {}: {}", port, e);
                println!("===> ERROR: FAILED TO REGISTER DAEMON ON PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
            })?;
        info!("Registered daemon for port {} with path {:?}", port, db_path);
        println!("===> REGISTERED DAEMON FOR PORT {} WITH PATH {:?}", port, db_path);

        let health_config = HealthCheckConfig {
            interval: TokioDuration::from_secs(10),
            connect_timeout: TokioDuration::from_secs(2),
            response_buffer_size: 1024,
        };
        self.start_health_monitoring(health_config).await;
        info!("Started health monitoring for port {}", port);
        println!("===> STARTED HEALTH MONITORING FOR PORT {}", port);

        *initialized = true;
        info!("SledDaemonPool initialization complete for port {}", port);
        println!("===> SLED DAEMON POOL INITIALIZATION COMPLETE FOR PORT {}", port);
        Ok::<(), GraphError>(())
    }

    pub async fn get_or_create_sled_client(
        &mut self,
        engine_config: &StorageConfig,
        _engine_type: &StorageEngineType,
        port: u16,
        sled_path: &PathBuf,
        daemon_metadata_ref: Option<DaemonMetadata>,
    ) -> GraphResult<Arc<dyn GraphStorageEngine>> {
        // Check for existing client
        {
            let clients_guard = self.clients.lock().await;
            if let Some(client) = clients_guard.get(&port) {
                if self.is_zmq_server_running(port).await? {
                    debug!("Reusing existing Sled client for port {}", port);
                    println!("===> REUSING EXISTING SLED CLIENT FOR PORT {}", port);
                    return Ok(client.clone());
                }
                warn!("Existing client for port {} was not reachable via ZMQ. Restarting daemon.", port);
                println!("===> WARNING: EXISTING CLIENT FOR PORT {} WAS NOT REACHABLE VIA ZMQ. RESTARTING DAEMON.", port);
                // Drop the lock before modifying clients
                drop(clients_guard);
                let mut clients_guard = self.clients.lock().await;
                clients_guard.remove(&port);
            }
        }

        // Handle daemon metadata
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        if let Some(_metadata) = daemon_metadata_ref {
            if self.is_zmq_server_running(port).await? {
                info!("REUSING EXISTING DAEMON ON PORT {}", port);
                println!("===> REUSING EXISTING DAEMON ON PORT {}", port);
            } else {
                warn!("Daemon metadata found for port {}, but ZMQ server is not running. Starting new daemon.", port);
                println!("===> WARNING: DAEMON METADATA FOUND FOR PORT {} BUT ZMQ SERVER IS NOT RUNNING. STARTING NEW DAEMON.", port);
                timeout(TokioDuration::from_secs(5), daemon_registry.unregister_daemon(port))
                    .await
                    .map_err(|_| {
                        warn!("Timeout unregistering daemon on port {}", port);
                        println!("===> WARNING: TIMEOUT UNREGISTERING DAEMON ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout unregistering daemon on port {}", port))
                    })?;
            }
        }

        // Start new daemon if needed
        let _ = self.start_new_daemon(engine_config, port, sled_path).await?;

        // Create and store new client
        let mut clients_guard = self.clients.lock().await;
        let (sled_client, _socket_wrapper) = SledClient::connect_zmq_client(port).await?;
        let client_arc = Arc::new(sled_client);
        clients_guard.insert(port, client_arc.clone());

        Ok(client_arc)
    }   

    async fn _initialize_cluster_core(
        &mut self,
        storage_config: &StorageConfig,
        config: &SledConfig,
        cli_port: Option<u16>,
    ) -> GraphResult<()> {
        println!("===> IN _initialize_cluster_core");
        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("SledDaemonPool already initialized, skipping");
            println!("===> WARNING: SLED DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            return Ok(());
        }

        // Set DEFAULT_STORAGE_PORT to 8052 to align with intended port
        const DEFAULT_STORAGE_PORT: u16 = 8052;

        // Determine the intended port
        let intended_port = cli_port.unwrap_or(config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        info!("Intended port: {} (cli_port: {:?}, config.port: {:?}, DEFAULT_STORAGE_PORT: {})",
              intended_port, cli_port, config.port, DEFAULT_STORAGE_PORT);
        println!("===> INTENDED PORT: {} (cli_port: {:?}, config.port: {:?}, DEFAULT_STORAGE_PORT: {})",
                 intended_port, cli_port, config.port, DEFAULT_STORAGE_PORT);

        // Clean up stale IPC sockets for all possible ports in cluster_range and intended port
        let cluster_ports: Vec<u16> = if !storage_config.cluster_range.is_empty() {
            let range: Vec<&str> = storage_config.cluster_range.split('-').collect();
            let start: u16 = range[0].parse().unwrap_or(intended_port);
            let end: u16 = range.get(1).and_then(|s| s.parse().ok()).unwrap_or(start);
            (start..=end).collect()
        } else {
            vec![intended_port]
        };

        for port in cluster_ports.iter().chain(std::iter::once(&intended_port)) {
            let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
            if Path::new(&ipc_path).exists() {
                warn!("Stale IPC socket found at {}. Attempting cleanup.", ipc_path);
                println!("===> WARNING: STALE IPC SOCKET FOUND AT {}. ATTEMPTING CLEANUP.", ipc_path);
                if let Err(e) = tokio_fs::remove_file(&ipc_path).await {
                    warn!("Failed to remove stale IPC socket at {}: {}", ipc_path, e);
                    println!("===> WARNING: FAILED TO REMOVE STALE IPC SOCKET AT {}: {}", ipc_path, e);
                } else {
                    info!("Successfully removed stale IPC socket at {}", ipc_path);
                    println!("===> SUCCESSFULLY REMOVED STALE IPC SOCKET AT {}", ipc_path);
                }
            }
        }

        // Get active storage daemons from GLOBAL_DAEMON_REGISTRY
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let all_daemons = daemon_registry.get_all_daemon_metadata().await?;
        let mut active_ports: Vec<u16> = all_daemons
            .into_iter()
            .filter(|metadata| metadata.service_type == "storage")
            .map(|metadata| metadata.port)
            .collect();

        // Ensure the intended port is included
        if !active_ports.contains(&intended_port) {
            warn!("Intended port {} not found in registry, adding it", intended_port);
            println!("===> WARNING: INTENDED PORT {} NOT FOUND IN REGISTRY, ADDING IT", intended_port);
            active_ports.push(intended_port);
        }

        // Initialize daemons and validate ports
        let mut valid_ports: Vec<u16> = Vec::new();
        for port in active_ports {
            let metadata = daemon_registry.get_daemon_metadata(port).await?;
            let mut should_initialize = port == intended_port || valid_ports.is_empty();

            // Check if daemon is running only if it exists in registry and not the intended port
            if let Some(metadata) = metadata {
                if metadata.port != intended_port {
                    let is_running = match timeout(TokioDuration::from_secs(5), self.is_zmq_server_running(port)).await {
                        Ok(Ok(is_running)) => is_running,
                        Ok(Err(e)) => {
                            warn!("Error checking ZMQ server on port {}: {}", port, e);
                            println!("===> WARNING: ERROR CHECKING ZMQ SERVER ON PORT {}: {}", port, e);
                            false
                        }
                        Err(_) => {
                            warn!("Timeout checking ZMQ server on port {}", port);
                            println!("===> WARNING: TIMEOUT CHECKING ZMQ SERVER ON PORT {}", port);
                            false
                        }
                    };

                    if is_running {
                        info!("Active storage daemon found on port {}, adding to load balancer", port);
                        println!("===> ACTIVE STORAGE DAEMON FOUND ON PORT {}, ADDING TO LOAD BALANCER", port);
                        valid_ports.push(port);
                        self.load_balancer.update_node_health(port, true, 0).await;
                        continue;
                    } else {
                        warn!("Stale storage daemon found on port {}, cleaning up", port);
                        println!("===> WARNING: STALE STORAGE DAEMON FOUND ON PORT {}, CLEANING UP", port);
                        let pid = metadata.pid;
                        #[cfg(unix)]
                        {
                            info!("Attempting to send SIGTERM to process {}", pid);
                            println!("===> ATTEMPTING TO SEND SIGTERM TO PROCESS {}", pid);
                            if let Err(e) = kill(NixPid::from_raw(pid as i32), Signal::SIGTERM) {
                                warn!("Failed to send SIGTERM to process {}: {}", pid, e);
                                println!("===> WARNING: FAILED TO SEND SIGTERM TO PROCESS {}: {}", pid, e);
                            } else {
                                info!("Sent SIGTERM to process {}", pid);
                                println!("===> SENT SIGTERM TO PROCESS {}", pid);
                                tokio::time::sleep(TokioDuration::from_millis(500)).await;
                            }
                        }
                        timeout(TokioDuration::from_secs(5), daemon_registry.unregister_daemon(port))
                            .await
                            .map_err(|_| {
                                warn!("Timeout unregistering daemon on port {}", port);
                                println!("===> WARNING: TIMEOUT UNREGISTERING DAEMON ON PORT {}", port);
                                GraphError::StorageError(format!("Timeout unregistering daemon on port {}", port))
                            })?;
                        info!("Unregistered stale daemon on port {}", port);
                        println!("===> UNREGISTERED STALE DAEMON ON PORT {}", port);
                    }
                }
            } else {
                should_initialize = true; // No metadata, initialize if intended or needed
            }

            // Initialize new daemon
            if should_initialize {
                let db_path = storage_config
                    .data_directory
                    .as_ref()
                    .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                    .join("sled")
                    .join(port.to_string());

                info!("Initializing daemon on port {} with path {:?}", port, db_path);
                println!("===> INITIALIZING DAEMON ON PORT {} WITH PATH {:?}", port, db_path);

                // Create directory if it doesn't exist
                if !db_path.exists() {
                    info!("Creating Sled directory at {:?}", db_path);
                    println!("===> CREATING SLED DIRECTORY AT {:?}", db_path);
                    tokio_fs::create_dir_all(&db_path).await
                        .map_err(|e| {
                            error!("Failed to create directory at {:?}: {}", db_path, e);
                            println!("===> ERROR: FAILED TO CREATE DIRECTORY AT {:?}: {}", db_path, e);
                            GraphError::Io(e.to_string())
                        })?;
                    tokio_fs::set_permissions(&db_path, fs::Permissions::from_mode(0o700))
                        .await
                        .map_err(|e| {
                            error!("Failed to set permissions on directory at {:?}: {}", db_path, e);
                            println!("===> ERROR: FAILED TO SET PERMISSIONS ON DIRECTORY AT {:?}: {}", db_path, e);
                            GraphError::Io(e.to_string())
                        })?;
                }

                let mut daemon_config = config.clone();
                daemon_config.path = db_path.clone();
                daemon_config.port = Some(port);
                info!("Creating SledDaemon with config: {:?}", daemon_config);
                println!("===> CREATING SLED DAEMON WITH CONFIG: {:?}", daemon_config);

                let (daemon, mut ready_rx) = timeout(TokioDuration::from_secs(10), SledDaemon::new(daemon_config.clone()))
                    .await
                    .map_err(|_| {
                        error!("Timeout creating SledDaemon on port {}", port);
                        println!("===> ERROR: TIMEOUT CREATING SLED DAEMON ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout creating SledDaemon on port {}", port))
                    })?
                    .map_err(|e| {
                        error!("Failed to create SledDaemon on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO CREATE SLED DAEMON ON PORT {}: {}", port, e);
                        e
                    })?;

                // Wait for daemon readiness
                info!("Waiting for ZMQ server readiness signal on port {}", port);
                println!("===> WAITING FOR ZMQ SERVER READINESS SIGNAL ON PORT {}", port);
                timeout(TokioDuration::from_secs(10), ready_rx.recv())
                    .await
                    .map_err(|_| {
                        error!("Timeout waiting for ZeroMQ server readiness signal on port {}", port);
                        println!("===> ERROR: TIMEOUT WAITING FOR ZEROMQ SERVER READINESS SIGNAL ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout waiting for ZeroMQ server readiness signal on port {}", port))
                    })?
                    .ok_or_else(|| {
                        error!("ZeroMQ server readiness channel closed for port {}", port);
                        println!("===> ERROR: ZEROMQ SERVER READINESS CHANNEL CLOSED FOR PORT {}", port);
                        GraphError::StorageError(format!("ZeroMQ server readiness channel closed for port {}", port))
                    })?;

                // Verify ZMQ server responsiveness after creation
                info!("Checking ZMQ server responsiveness on port {}", port);
                println!("===> CHECKING ZMQ SERVER RESPONSIVENESS ON PORT {}", port);
                timeout(TokioDuration::from_secs(10), async {
                    let mut attempts = 0;
                    const MAX_ATTEMPTS: usize = 20;
                    while !self.is_zmq_server_running(port).await? {
                        attempts += 1;
                        if attempts >= MAX_ATTEMPTS {
                            error!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS);
                            println!("===> ERROR: ZMQ SERVER FAILED TO START ON PORT {} AFTER {} ATTEMPTS", port, MAX_ATTEMPTS);
                            return Err(GraphError::StorageError(format!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS)));
                        }
                        info!("ZMQ server not ready on port {}, attempt {}/{}", port, attempts, MAX_ATTEMPTS);
                        println!("===> ZMQ SERVER NOT READY ON PORT {}, ATTEMPT {}/{}", port, attempts, MAX_ATTEMPTS);
                        tokio::time::sleep(TokioDuration::from_millis(500)).await;
                    }
                    info!("ZMQ server is ready for port {}", port);
                    println!("===> ZEROMQ SERVER IS READY FOR PORT {}", port);
                    Ok(())
                })
                .await
                .map_err(|_| {
                    error!("Timeout waiting for ZMQ server to start on port {}", port);
                    println!("===> ERROR: TIMEOUT WAITING FOR ZMQ SERVER TO START ON PORT {}", port);
                    GraphError::StorageError(format!("Timeout waiting for ZMQ server on port {}", port))
                })??;

                let daemon_metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: std::process::id(),
                    ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                    data_dir: Some(db_path.clone()),
                    config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                    engine_type: Some(StorageEngineType::Sled.to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                };

                timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata))
                    .await
                    .map_err(|_| {
                        error!("Timeout registering daemon on port {}", port);
                        println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout registering daemon on port {}", port))
                    })?
                    .map_err(|e| {
                        error!("Failed to register daemon on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO REGISTER DAEMON ON PORT {}: {}", port, e);
                        GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
                    })?;

                self.daemons.insert(port, Arc::new(daemon));
                valid_ports.push(port);
                self.load_balancer.update_node_health(port, true, 0).await;
                info!("Initialized and registered new daemon on port {}", port);
                println!("===> INITIALIZED AND REGISTERED NEW DAEMON ON PORT {}", port);
            }
        }

        // If no valid ports were found, spin up new daemons from cluster_range
        if valid_ports.is_empty() && !storage_config.cluster_range.is_empty() {
            warn!("No active storage daemons found in registry, using cluster_range: {}", storage_config.cluster_range);
            println!("===> WARNING: NO ACTIVE STORAGE DAEMONS FOUND IN REGISTRY, USING CLUSTER_RANGE: {}", storage_config.cluster_range);
            let range: Vec<&str> = storage_config.cluster_range.split('-').collect();
            let start: u16 = range[0].parse().unwrap_or(intended_port);
            let end: u16 = range.get(1).and_then(|s| s.parse().ok()).unwrap_or(start);
            let cluster_ports: Vec<u16> = (start..=end)
                .filter(|port| !self.daemons.contains_key(port))
                .collect();

            for port in cluster_ports {
                if port == intended_port || valid_ports.is_empty() {
                    let db_path = storage_config
                        .data_directory
                        .as_ref()
                        .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                        .join("sled")
                        .join(port.to_string());

                    info!("Initializing daemon from cluster_range on port {} with path {:?}", port, db_path);
                    println!("===> INITIALIZING DAEMON FROM CLUSTER_RANGE ON PORT {} WITH PATH {:?}", port, db_path);

                    // Create directory if it doesn't exist
                    if !db_path.exists() {
                        info!("Creating Sled directory at {:?}", db_path);
                        println!("===> CREATING SLED DIRECTORY AT {:?}", db_path);
                        tokio_fs::create_dir_all(&db_path).await
                            .map_err(|e| {
                                error!("Failed to create directory at {:?}: {}", db_path, e);
                                println!("===> ERROR: FAILED TO CREATE DIRECTORY AT {:?}: {}", db_path, e);
                                GraphError::Io(e.to_string())
                            })?;
                        tokio_fs::set_permissions(&db_path, fs::Permissions::from_mode(0o700))
                            .await
                            .map_err(|e| {
                                error!("Failed to set permissions on directory at {:?}: {}", db_path, e);
                                println!("===> ERROR: FAILED TO SET PERMISSIONS ON DIRECTORY AT {:?}: {}", db_path, e);
                                GraphError::Io(e.to_string())
                            })?;
                    }

                    let mut daemon_config = config.clone();
                    daemon_config.path = db_path.clone();
                    daemon_config.port = Some(port);
                    info!("Creating SledDaemon with config: {:?}", daemon_config);
                    println!("===> CREATING SLED DAEMON WITH CONFIG: {:?}", daemon_config);

                    let (daemon, mut ready_rx) = timeout(TokioDuration::from_secs(10), SledDaemon::new(daemon_config.clone()))
                        .await
                        .map_err(|_| {
                            error!("Timeout creating SledDaemon on port {}", port);
                            println!("===> ERROR: TIMEOUT CREATING SLED DAEMON ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout creating SledDaemon on port {}", port))
                        })?
                        .map_err(|e| {
                            error!("Failed to create SledDaemon on port {}: {}", port, e);
                            println!("===> ERROR: FAILED TO CREATE SLED DAEMON ON PORT {}: {}", port, e);
                            e
                        })?;

                    // Wait for daemon readiness
                    info!("Waiting for ZMQ server readiness signal on port {}", port);
                    println!("===> WAITING FOR ZMQ SERVER READINESS SIGNAL ON PORT {}", port);
                    timeout(TokioDuration::from_secs(10), ready_rx.recv())
                        .await
                        .map_err(|_| {
                            error!("Timeout waiting for ZeroMQ server readiness signal on port {}", port);
                            println!("===> ERROR: TIMEOUT WAITING FOR ZEROMQ SERVER READINESS SIGNAL ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout waiting for ZeroMQ server readiness signal on port {}", port))
                        })?
                        .ok_or_else(|| {
                            error!("ZeroMQ server readiness channel closed for port {}", port);
                            println!("===> ERROR: ZEROMQ SERVER READINESS CHANNEL CLOSED FOR PORT {}", port);
                            GraphError::StorageError(format!("ZeroMQ server readiness channel closed for port {}", port))
                        })?;

                    // Verify ZMQ server responsiveness
                    info!("Checking ZMQ server responsiveness on port {}", port);
                    println!("===> CHECKING ZMQ SERVER RESPONSIVENESS ON PORT {}", port);
                    // In _initialize_cluster_core and start_new_daemon
                    timeout(TokioDuration::from_secs(10), async {
                        let mut attempts = 0;
                        const MAX_ATTEMPTS: usize = 30;
                        while !self.is_zmq_server_running(port).await? {
                            attempts += 1;
                            if attempts >= MAX_ATTEMPTS {
                                error!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS);
                                println!("===> ERROR: ZMQ SERVER FAILED TO START ON PORT {} AFTER {} ATTEMPTS", port, MAX_ATTEMPTS);
                                return Err(GraphError::StorageError(format!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS)));
                            }
                            info!("ZMQ server not ready on port {}, attempt {}/{}", port, attempts, MAX_ATTEMPTS);
                            println!("===> ZMQ SERVER NOT READY ON PORT {}, ATTEMPT {}/{}", port, attempts, MAX_ATTEMPTS);
                            tokio::time::sleep(TokioDuration::from_millis(200)).await;
                        }
                        info!("ZMQ server is ready for port {}", port);
                        println!("===> ZEROMQ SERVER IS READY FOR PORT {}", port);
                        Ok(())
                    }).await
                    .map_err(|_| {
                        error!("Timeout waiting for ZMQ server to start on port {}", port);
                        println!("===> ERROR: TIMEOUT WAITING FOR ZMQ SERVER TO START ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout waiting for ZMQ server on port {}", port))
                    })??;

                    let daemon_metadata = DaemonMetadata {
                        service_type: "storage".to_string(),
                        port,
                        pid: std::process::id(),
                        ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
                        data_dir: Some(db_path.clone()),
                        config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
                        engine_type: Some(StorageEngineType::Sled.to_string()),
                        last_seen_nanos: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(0),
                    };

                    timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata))
                        .await
                        .map_err(|_| {
                            error!("Timeout registering daemon on port {}", port);
                            println!("===> ERROR: TIMEOUT REGISTERING DAEMON ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout registering daemon on port {}", port))
                        })?
                        .map_err(|e| {
                            error!("Failed to register daemon on port {}: {}", port, e);
                            println!("===> ERROR: FAILED TO REGISTER DAEMON ON PORT {}: {}", port, e);
                            GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e))
                        })?;

                    self.daemons.insert(port, Arc::new(daemon));
                    valid_ports.push(port);
                    self.load_balancer.update_node_health(port, true, 0).await;
                    info!("Initialized and registered new daemon from cluster_range on port {}", port);
                    println!("===> INITIALIZED AND REGISTERED NEW DAEMON FROM CLUSTER_RANGE ON PORT {}", port);
                }
            }
        }

        if valid_ports.is_empty() {
            error!("No valid ports available for load balancing");
            println!("===> ERROR: NO VALID PORTS AVAILABLE FOR LOAD BALANCING");
            return Err(GraphError::StorageError("No valid ports available for load balancing".to_string()));
        }

        *initialized = true;
        let health_config = HealthCheckConfig {
            interval: TokioDuration::from_secs(10),
            connect_timeout: TokioDuration::from_secs(2),
            response_buffer_size: 1024,
        };
        self.start_health_monitoring(health_config).await;
        info!("Started health monitoring for ports {:?}", valid_ports);
        println!("===> STARTED HEALTH MONITORING FOR PORTS {:?}", valid_ports);

        info!("SledDaemonPool initialized successfully with load balancing on ports {:?}", valid_ports);
        println!("===> SLED DAEMON POOL INITIALIZED SUCCESSFULLY WITH LOAD BALANCING ON PORTS {:?}", valid_ports);
        Ok(())
    }

    /// Start periodic health monitoring
    pub async fn start_health_monitoring(&self, config: HealthCheckConfig) {
        let load_balancer = self.load_balancer.clone();
        let daemons = self.daemons.clone();
        let running = self.initialized.clone();
        let health_config = config.clone(); // Clone to own the config
        let pool = Arc::new(self.clone()); // Clone SledDaemonPool and wrap in Arc

        tokio::spawn(async move {
            let mut interval = interval(health_config.interval);

            while *running.read().await {
                interval.tick().await;

                let ports: Vec<u16> = daemons.keys().copied().collect();
                let health_checks = ports.into_iter().map(|port| {
                    let pool = pool.clone();
                    let health_config = health_config.clone();
                    async move {
                        let is_healthy = pool.health_check_node(port, &health_config).await.unwrap_or(false);
                        (port, is_healthy)
                    }
                });

                let start_time = SystemTime::now();
                let results = join_all(health_checks).await;

                for (port, is_healthy) in results {
                    let response_time = start_time.elapsed().unwrap_or(TokioDuration::from_millis(0)).as_millis() as u64;
                    load_balancer.update_node_health(port, is_healthy, response_time).await;

                    if is_healthy {
                        info!("Health check successful for node on port {}. Response time: {}ms", port, response_time);
                    } else {
                        warn!("Health check failed for node on port {}.", port);
                    }

                    #[cfg(feature = "with-openraft-sled")]
                    if is_healthy {
                        if let Some(daemon) = daemons.get(&port) {
                            if let Ok(is_leader) = daemon.is_leader().await {
                                info!("Node {} Raft leader status: {}", port, is_leader);
                            }
                        }
                    }
                }

                let healthy_nodes = load_balancer.get_healthy_nodes().await;
                info!("===> HEALTH MONITOR: {}/{} nodes healthy: {:?}", 
                      healthy_nodes.len(), daemons.len(), healthy_nodes);

                if healthy_nodes.len() <= daemons.len() / 2 {
                    warn!("Cluster health degraded: only {}/{} nodes healthy", 
                          healthy_nodes.len(), daemons.len());
                }
            }

            info!("Health monitoring stopped due to pool shutdown");
        });
    }

    pub async fn initialize_cluster(
        &mut self,
        storage_config: &StorageConfig,
        config: &SledConfig,
        cli_port: Option<u16>,
    ) -> GraphResult<()> {
        println!("====> IN initialize_cluster");
        self._initialize_cluster_core(
            storage_config,
            config,
            cli_port,
        ).await
    }

    pub async fn initialize_cluster_with_db(
        &mut self,
        storage_config: &StorageConfig,
        config: &SledConfig,
        cli_port: Option<u16>,
        existing_db: Arc<sled::Db>,
    ) -> GraphResult<()> {
        println!("====> IN initialize_cluster_with_db");
        let existing_db_clone = existing_db.clone();
        self._initialize_cluster_core(
            storage_config,
            config,
            cli_port,
        ).await
    }

    pub async fn initialize_cluster_with_client(
        &mut self,
        storage_config: &StorageConfig,
        config: &SledConfig,
        cli_port: Option<u16>,
        client: SledClient,
        socket: Arc<TokioMutex<ZmqSocketWrapper>>, 
    ) -> GraphResult<()> {
        let port = cli_port.unwrap_or(config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        info!("Initializing SledDaemonPool with ZMQ client for port {}", port);
        println!("===> INITIALIZING SledDaemonPool WITH ZMQ CLIENT FOR PORT {}", port);

        // Clean up stale daemons
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let all_daemons = daemon_registry.get_all_daemon_metadata().await?;
        for metadata in all_daemons {
            if metadata.service_type == "storage" && metadata.port != port {
                let is_running = self.is_zmq_server_running(metadata.port).await?;
                if !is_running {
                    warn!("Found stale storage daemon on port {}, cleaning up", metadata.port);
                    println!("===> WARNING: FOUND STALE STORAGE DAEMON ON PORT {}, CLEANING UP", metadata.port);
                    timeout(TokioDuration::from_secs(5), daemon_registry.unregister_daemon(metadata.port))
                        .await
                        .map_err(|_| {
                            warn!("Timeout unregistering daemon on port {}", metadata.port);
                            println!("===> WARNING: TIMEOUT UNREGISTERING DAEMON ON PORT {}", metadata.port);
                            GraphError::StorageError(format!("Timeout unregistering daemon on port {}", metadata.port))
                        })?;
                    let ipc_path = format!("/tmp/graphdb-{}.ipc", metadata.port);
                    if Path::new(&ipc_path).exists() {
                        if let Err(e) = tokio_fs::remove_file(&ipc_path).await {
                            warn!("Failed to remove stale IPC socket at {}: {}", ipc_path, e);
                            println!("===> WARNING: FAILED TO REMOVE STALE IPC SOCKET AT {}: {}", ipc_path, e);
                        } else {
                            info!("Successfully removed stale IPC socket at {}", ipc_path);
                            println!("===> SUCCESSFULLY REMOVED STALE IPC SOCKET AT {}", ipc_path);
                        }
                    }
                }
            }
        }

        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());
        info!("Using Sled path for daemon communication: {:?}", db_path);
        println!("===> USING SLED PATH FOR DAEMON COMMUNICATION: {:?}", db_path);

        let socket_slot = Arc::new(TokioMutex::new(Some(socket)));

        self._initialize_cluster_core(storage_config, config, Some(port)).await?;

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            pid: std::process::id(),
            port,
        };
        daemon_registry.register_daemon(daemon_metadata).await
            .map_err(|e| {
                error!("Failed to register daemon for port {}: {}", port, e);
                println!("===> ERROR: FAILED TO REGISTER DAEMON FOR PORT {}: {}", port, e);
                GraphError::StorageError(format!("Failed to register daemon for port {}: {}", port, e))
            })?;
        info!("Registered daemon for port {} in global registry", port);
        println!("===> REGISTERED DAEMON FOR PORT {} IN GLOBAL REGISTRY", port);

        info!("Successfully initialized SledDaemonPool with ZMQ client for port {}", port);
        println!("===> SUCCESSFULLY INITIALIZED SledDaemonPool WITH ZMQ CLIENT FOR PORT {}", port);
        Ok(())
    }

    /// Public interface methods that use the enhanced replication
    pub async fn insert(&self, key: &[u8], value: &[u8], use_raft_for_scale: bool) -> GraphResult<()> {
        self.insert_replicated(key, value, use_raft_for_scale).await
    }

    pub async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        self.retrieve_with_failover(key).await
    }
    /// Get cluster status for monitoring
    pub async fn get_cluster_status(&self) -> GraphResult<serde_json::Value> {
        let healthy_nodes = self.load_balancer.get_healthy_nodes().await;
        let total_nodes = self.daemons.len();
        
        Ok(json!({
            "total_nodes": total_nodes,
            "healthy_nodes": healthy_nodes.len(),
            "healthy_node_ports": healthy_nodes,
            "replication_factor": self.load_balancer.replication_factor,
            "use_raft_for_scale": false,
            "cluster_health": if healthy_nodes.len() > total_nodes / 2 { "healthy" } else { "degraded" }
        }))
    }

    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down SledDaemonPool with {} daemons", self.daemons.len());
        println!("===> SHUTTING DOWN SLED DAEMON POOL WITH {} DAEMONS", self.daemons.len());
        let mut futures = Vec::new();
        for (port, daemon) in self.daemons.iter() {
            info!("Shutting down daemon on port {}", port);
            println!("===> SHUTTING DOWN DAEMON ON PORT {}", port);
            futures.push(async move {
                match daemon.shutdown().await {
                    Ok(_) => {
                        info!("Successfully shut down daemon on port {}", port);
                        println!("===> SUCCESSFULLY SHUT DOWN DAEMON ON PORT {}", port);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to shut down daemon on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO SHUT DOWN DAEMON ON PORT {}: {}", port, e);
                        Err(e)
                    }
                }
            });
        }
        let results = join_all(futures).await;
        let mut errors = Vec::new();
        for result in results {
            if let Err(e) = result {
                errors.push(e);
            }
        }
        *self.initialized.write().await = false;
        if errors.is_empty() {
            info!("SledDaemonPool shutdown complete");
            println!("===> SLED DAEMON POOL SHUTDOWN COMPLETE");
            Ok(())
        } else {
            error!("SledDaemonPool shutdown encountered errors: {:?}", errors);
            println!("===> ERROR: SLED DAEMON POOL SHUTDOWN ENCOUNTERED ERRORS: {:?}", errors);
            Err(GraphError::StorageError(format!("Shutdown errors: {:?}", errors)))
        }
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
        let daemons = self.daemons.clone();
        let ports: Vec<u16> = daemons.keys().copied().collect();
        info!("Checking for leader daemon among ports {:?}", ports);
        println!("===> CHECKING FOR LEADER DAEMON AMONG PORTS {:?}", ports);

        let mut system = System::new_with_specifics(
            RefreshKind::everything().with_processes(ProcessRefreshKind::everything())
        );
        system.refresh_processes(ProcessesToUpdate::All, true);

        for port in ports {
            // Verify TCP listener is active
            let is_port_active = match TcpStream::connect(format!("127.0.0.1:{}", port)).await {
                Ok(_) => {
                    info!("TCP listener active on port {}", port);
                    println!("===> TCP LISTENER ACTIVE ON PORT {}", port);
                    true
                }
                Err(_) => {
                    warn!("No TCP listener found on port {}", port);
                    println!("===> WARNING: NO TCP LISTENER FOUND ON PORT {}", port);
                    false
                }
            };

            // Verify process is running
            let is_process_running = system.processes().values().any(|process| {
                if let Some(name_str) = process.name().to_str() {
                    if name_str.contains("graphdb") {
                        return process.cmd().iter().any(|arg| {
                            if let Some(arg_str) = arg.to_str() {
                                arg_str.contains(&port.to_string())
                            } else {
                                false
                            }
                        });
                    }
                }
                false
            });

            if !is_port_active || !is_process_running {
                warn!("Skipping daemon on port {}: no active TCP listener or running process", port);
                println!("===> SKIPPING DAEMON ON PORT {}: NO ACTIVE TCP LISTENER OR RUNNING PROCESS", port);
                continue;
            }

            if let Some(daemon) = daemons.get(&port) {
                #[cfg(feature = "with-openraft-sled")]
                {
                    if daemon.is_leader().await? {
                        info!("Selected leader daemon on port {}", port);
                        println!("===> SELECTED LEADER DAEMON ON PORT {}", port);
                        return Ok(Arc::clone(daemon));
                    } else {
                        info!("Daemon on port {} is not leader", port);
                        println!("===> DAEMON ON PORT {} IS NOT LEADER", port);
                    }
                }
                #[cfg(not(feature = "with-openraft-sled"))]
                {
                    info!("Selected daemon on port {} (no Raft, using first active daemon)", port);
                    println!("===> SELECTED DAEMON ON PORT {} (NO RAFT, USING FIRST ACTIVE DAEMON)", port);
                    return Ok(Arc::clone(daemon));
                }
            }
        }

        error!("No leader daemon found among ports {:?}", daemons.keys());
        println!("===> ERROR: NO LEADER DAEMON FOUND AMONG PORTS {:?}", daemons.keys());
        Err(GraphError::StorageError("No leader daemon found".to_string()))
    }

    pub async fn create_edge(&self, edge: Edge, _use_raft_for_scale: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.create_edge(&edge)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_edge".to_string()))?
    }

    pub async fn get_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
        _use_raft_for_scale: bool,
    ) -> GraphResult<Option<Edge>> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.get_edge(outbound_id, edge_type, inbound_id)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_edge".to_string()))?
    }

    pub async fn update_edge(&self, edge: Edge, _use_raft_for_scale: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.update_edge(&edge)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_edge".to_string()))?
    }

    pub async fn delete_edge(
        &self,
        outbound_id: &Uuid,
        edge_type: &Identifier,
        inbound_id: &Uuid,
        _use_raft_for_scale: bool,
    ) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.delete_edge(outbound_id, edge_type, inbound_id)).await
            .map_err(|_| GraphError::StorageError("Timeout during delete_edge".to_string()))?
    }

    pub async fn create_vertex(&self, vertex: Vertex, _use_raft_for_scale: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.create_vertex(&vertex)).await
            .map_err(|_| GraphError::StorageError("Timeout during create_vertex".to_string()))?
    }

    pub async fn get_vertex(&self, id: &Uuid, _use_raft_for_scale: bool) -> GraphResult<Option<Vertex>> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.get_vertex(id)).await
            .map_err(|_| GraphError::StorageError("Timeout during get_vertex".to_string()))?
    }

    pub async fn update_vertex(&self, vertex: Vertex, _use_raft_for_scale: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.update_vertex(&vertex)).await
            .map_err(|_| GraphError::StorageError("Timeout during update_vertex".to_string()))?
    }

    pub async fn delete_vertex(&self, id: &Uuid, _use_raft_for_scale: bool) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        timeout(TokioDuration::from_secs(5), daemon.delete_vertex(id)).await
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

    pub async fn flush(&self) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        let port = daemon.port();
        info!("SledDaemonPool::flush - Sending flush request to ZeroMQ server on port {}", port);
        println!("===> SLED DAEMON POOL FLUSH - SENDING FLUSH REQUEST TO ZEROMQ SERVER ON PORT {}", port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket {}: {}", endpoint, e)))?;

        let request = json!({ "command": "flush" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send flush request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive flush response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            info!("SledDaemonPool::flush - Successfully flushed database via ZeroMQ on port {}", port);
            println!("===> SLED DAEMON POOL FLUSH - SUCCESSFULLY FLUSHED DATABASE VIA ZEROMQ ON PORT {}", port);
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemonPool::flush - Failed on port {}: {}", port, error_msg);
            println!("===> SLED DAEMON POOL FLUSH - FAILED ON PORT {}: {}", port, error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let daemon = self.leader_daemon().await?;
        let port = daemon.port();
        info!("SledDaemonPool::get_all_vertices - Sending request to ZeroMQ server on port {}", port);
        println!("===> SLED DAEMON POOL GET_ALL_VERTICES - SENDING REQUEST TO ZEROMQ SERVER ON PORT {}", port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
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
            info!("SledDaemonPool::get_all_vertices - Retrieved {} vertices on port {}", vertices.len(), port);
            println!("===> SLED DAEMON POOL GET_ALL_VERTICES - RETRIEVED {} VERTICES ON PORT {}", vertices.len(), port);
            Ok(vertices)
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemonPool::get_all_vertices - Failed on port {}: {}", port, error_msg);
            println!("===> SLED DAEMON POOL GET_ALL_VERTICES - FAILED ON PORT {}: {}", port, error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let daemon = self.leader_daemon().await?;
        let port = daemon.port();
        info!("SledDaemonPool::get_all_edges - Sending request to ZeroMQ server on port {}", port);
        println!("===> SLED DAEMON POOL GET_ALL_EDGES - SENDING REQUEST TO ZEROMQ SERVER ON PORT {}", port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
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
            info!("SledDaemonPool::get_all_edges - Retrieved {} edges on port {}", edges.len(), port);
            println!("===> SLED DAEMON POOL GET_ALL_EDGES - RETRIEVED {} EDGES ON PORT {}", edges.len(), port);
            Ok(edges)
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemonPool::get_all_edges - Failed on port {}: {}", port, error_msg);
            println!("===> SLED DAEMON POOL GET_ALL_EDGES - FAILED ON PORT {}: {}", port, error_msg);
            Err(GraphError::StorageError(error_msg))
        }
    }

    pub async fn clear_data(&self) -> GraphResult<()> {
        let daemon = self.leader_daemon().await?;
        let port = daemon.port();
        info!("SledDaemonPool::clear_data - Sending clear_data request to ZeroMQ server on port {}", port);
        println!("===> SLED DAEMON POOL CLEAR_DATA - SENDING CLEAR_DATA REQUEST TO ZEROMQ SERVER ON PORT {}", port);
        let context = ZmqContext::new();
        let socket = context.socket(zmq::REQ)
            .map_err(|e| GraphError::StorageError(format!("Failed to create ZeroMQ socket: {}", e)))?;
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        socket.connect(&endpoint)
            .map_err(|e| GraphError::StorageError(format!("Failed to connect to ZeroMQ socket {}: {}", endpoint, e)))?;

        let request = json!({ "command": "clear_data" });
        socket.send(serde_json::to_vec(&request)?, 0)
            .map_err(|e| GraphError::StorageError(format!("Failed to send clear_data request: {}", e)))?;
        let reply = socket.recv_bytes(0)
            .map_err(|e| GraphError::StorageError(format!("Failed to receive clear_data response: {}", e)))?;
        let response: Value = serde_json::from_slice(&reply)?;

        if response["status"] == "success" {
            info!("SledDaemonPool::clear_data - Successfully cleared database via ZeroMQ on port {}", port);
            println!("===> SLED DAEMON POOL CLEAR_DATA - SUCCESSFULLY CLEARED DATABASE VIA ZEROMQ ON PORT {}", port);
            Ok(())
        } else {
            let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
            error!("SledDaemonPool::clear_data - Failed on port {}: {}", port, error_msg);
            println!("===> SLED DAEMON POOL CLEAR_DATA - FAILED ON PORT {}: {}", port, error_msg);
            Err(GraphError::StorageError(error_msg))
        }
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
            match timeout(TokioDuration::from_secs(2), async {
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
                    tokio::time::sleep(TokioDuration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
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
                    tokio::time::sleep(TokioDuration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
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
            match timeout(TokioDuration::from_secs(2), async {
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
                    tokio::time::sleep(TokioDuration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
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
                    tokio::time::sleep(TokioDuration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
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
            match timeout(TokioDuration::from_secs(2), async {
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
                Ok(Ok(response)) => {
                    println!("===> RAFT: SENT VOTE TO {} SUCCESSFULLY", addr);
                    return Ok(response);
                }
                Ok(Err(e)) if attempt < MAX_RETRIES => {
                    warn!("Failed to send vote to {} on attempt {}: {}. Retrying.", addr, attempt + 1, e);
                    println!("===> RAFT: FAILED TO SEND VOTE TO {} ON ATTEMPT {}: {}. RETRYING.", addr, attempt + 1, e);
                    attempt += 1;
                    tokio::time::sleep(TokioDuration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                    continue;
                }
                Ok(Err(e)) => {
                    error!("Failed to send vote to {} after {} attempts: {}", addr, attempt + 1, e);
                    println!("===> RAFT: FAILED TO SEND VOTE TO {} AFTER {} ATTEMPTS: {}", addr, attempt + 1, e);
                    return Err(e);
                }
                Err(_) => {
                    warn!("Timeout sending vote to {} on attempt {}. Retrying.", addr, attempt + 1);
                    println!("===> RAFT: TIMEOUT SENDING VOTE TO {} ON ATTEMPT {}. RETRYING.", addr, attempt + 1);
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        error!("Timeout sending vote to {} after {} attempts.", addr, MAX_RETRIES);
                        println!("===> RAFT: TIMEOUT SENDING VOTE TO {} AFTER {} ATTEMPTS.", addr, MAX_RETRIES);
                        return Err(openraft::error::RPCError::Network {
                            error: openraft::error::NetworkError::new(&std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "Timeout sending vote",
                            )),
                            target,
                            node: BasicNode { addr },
                        });
                    }
                    tokio::time::sleep(TokioDuration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                }
            }
        }
    }
}
/*

pub async fn insert(key: &[u8], value: &[u8]) -> GraphResult<()> {
    self.ensure_write_access().await?;
    info!("Inserting key into kv_pairs at path {:?}", self.db_path);
    timeout(Duration::from_secs(5), async {
        let mut batch = Batch::default();
        batch.insert(key, value);
        self.kv_pairs
            .apply_batch(batch)
            .map_err(|e| GraphError::StorageError(format!("Failed to apply batch: {}", e)))?;

        let bytes_flushed = self.db.flush_async().await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;
        info!("Flushed {} bytes after insert at {:?}", bytes_flushed, self.db_path);

        let persisted = self.kv_pairs
            .get(key)
            .map_err(|e| GraphError::StorageError(format!("Failed to verify insert: {}", e)))?;
        if persisted.is_none() || persisted.as_ref().map(|v| v.as_ref()) != Some(value) {
            error!("Persistence verification failed for key at {:?}", self.db_path);
            return Err(GraphError::StorageError("Insert not persisted correctly".to_string()));
        }

        let keys: Vec<_> = self.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Current kv_pairs keys at {:?}: {:?}", self.db_path, keys);

        #[cfg(feature = "with-openraft-sled")]
        if let Some(raft) = &self.raft {
            let request = openraft::raft::ClientWriteRequest::new(
                openraft::EntryPayload::AppWrite {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }
            );
            raft.client_write(request).await
                .map_err(|e| GraphError::StorageError(format!("Raft write failed: {}", e)))?;
            info!("Raft write replicated for key at {:?}", self.db_path);
        }
        Ok(())
    })
    .await
    .map_err(|_| GraphError::StorageError("Timeout during insert".to_string()))?
}

pub async fn retrieve(key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
    if !self.is_running().await {
        error!("Daemon at path {:?} is not running", self.db_path);
        return Err(GraphError::StorageError(format!("Daemon at path {:?} is not running", self.db_path)));
    }
    if key == b"test_key" {
        warn!("Retrieving test_key, caller stack trace: {:#?}", std::backtrace::Backtrace::capture());
    }
    info!("Retrieving key from kv_pairs at path {:?}", self.db_path);
    let value = timeout(Duration::from_secs(5), async {
        let opt = self.kv_pairs
            .get(key)
            .map_err(|e| GraphError::StorageError(format!("Failed to retrieve key: {}", e)))?;
        Ok::<Option<Vec<u8>>, GraphError>(opt.map(|ivec| ivec.to_vec()))
    })
    .await
    .map_err(|_| GraphError::StorageError("Timeout during retrieve".to_string()))??;

    let keys: Vec<_> = self.kv_pairs
        .iter()
        .keys()
        .filter_map(|k| k.ok())
        .collect();
    info!("Current kv_pairs keys at {:?}: {:?}", self.db_path, keys);
    Ok(value)
}

pub async fn delete(key: &[u8]) -> GraphResult<()> {
    self.ensure_write_access().await?;
    info!("Deleting key from kv_pairs at path {:?}", self.db_path);
    timeout(Duration::from_secs(5), async {
        let mut batch = Batch::default();
        batch.remove(key);
        self.kv_pairs
            .apply_batch(batch)
            .map_err(|e| GraphError::StorageError(format!("Failed to apply batch: {}", e)))?;

        let bytes_flushed = self.db
            .flush_async()
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to flush DB: {}", e)))?;

        let persisted = self.kv_pairs
            .get(key)
            .map_err(|e| GraphError::StorageError(format!("Failed to verify delete: {}", e)))?;
        if persisted.is_some() {
            error!("Persistence verification failed for key delete at {:?}", self.db_path);
            return Err(GraphError::StorageError("Delete not persisted correctly".to_string()));
        }

        let keys: Vec<_> = self.kv_pairs
            .iter()
            .keys()
            .filter_map(|k| k.ok())
            .collect();
        info!("Flushed {} bytes after delete at {:?}, current kv_pairs keys: {:?}", bytes_flushed, self.db_path, keys);

        #[cfg(feature = "with-openraft-sled")]
        if let Some(raft) = &self.raft {
            let request = openraft::raft::ClientWriteRequest::new(
                openraft::EntryPayload::AppWrite {
                    key: key.to_vec(),
                    value: vec![],
                }
            );
            raft.client_write(request).await
                .map_err(|e| GraphError::StorageError(format!("Raft delete failed: {}", e)))?;
            info!("Raft delete replicated for key at {:?}", self.db_path);
        }
        Ok(())
    })
    .await
    .map_err(|_| GraphError::StorageError("Timeout during delete".to_string()))?
}*/
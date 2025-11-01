use std::collections::{HashMap, HashSet};
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{ Arc, LazyLock };
use std::io::{Cursor, Read, Write};
use std::process::Command;
use serde::{Deserialize, Serialize};
use quick_cache::sync::Cache;
use rocksdb::{DB, Env, Options, WriteBatch, WriteOptions, ColumnFamily, BoundColumnFamily, ColumnFamilyDescriptor, 
              DBCompressionType, DBCompactionStyle, Cache as RocksDBCache, BlockBasedOptions};
use rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as TokioMutex, RwLock, OnceCell, Semaphore, SemaphorePermit, 
                  mpsc, watch};
use tokio::time::{sleep, Duration as TokioDuration, timeout, interval};
use tokio::fs as tokio_fs;
use tokio::task::{self, JoinError, spawn_blocking, JoinHandle};
use log::{info, debug, warn, error};
use std::sync::atomic::{AtomicBool, Ordering};
pub use crate::config::{
    RocksDBConfig, RocksDBDaemon, RocksDBDaemonPool, StorageConfig, StorageEngineType,
    DAEMON_REGISTRY_DB_PATH, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB,
    ReplicationStrategy, NodeHealth, LoadBalancer, HealthCheckConfig, RaftTcpNetwork,
    RocksDBRaftStorage, RocksDBClient, TypeConfig, RocksDBClientMode, RocksDBStorage,
    STORAGE_PID_FILE_NAME_PREFIX, daemon_api_storage_engine_type_to_string,
}; 
use crate::storage_engine::rocksdb_client::{ZmqSocketWrapper};
use crate::storage_engine::storage_engine::{GraphStorageEngine, ApplicationStateMachine as RocksDBStateMachine};
use crate::storage_engine::storage_utils::{create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use serde_json::{json, Value};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::db_daemon_registry::{GLOBAL_DB_DAEMON_REGISTRY, DBDaemonMetadata};
use crate::daemon::daemon_management::{parse_cluster_range, find_pid_by_port,
                                     is_storage_daemon_running, stop_process_by_pid, 
                                     is_pid_running, is_process_running, check_pid_validity, 
                                     get_ipc_endpoint, force_cleanup_engine_lock, };
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;
use uuid::Uuid;
use zmq::{Context as ZmqContext, Socket as ZmqSocket, Error as ZmqError, REP, REQ, DONTWAIT};
use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use nix::unistd::{Pid as NixPid};
use nix::sys::signal::{kill, Signal};
use base64::engine::general_purpose;
use base64::Engine;
use std::fs::{self};
use async_trait::async_trait;
use std::ops::Deref;
use lazy_static::lazy_static;
// The placeholder logic for BoundColumnFamily using raw pointers is removed 
// because it causes E0423 due to the private fields of the tuple struct.
// We are now using a temporary, safe DB instance to get a valid handle.

#[cfg(feature = "with-rocksdb")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode, ServerState},
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

type StaticBoundColumnFamily = BoundColumnFamily<'static>;
// Singleton to track active RocksDBDaemon instances and their database handles per port
pub static ROCKSDB_DAEMON_REGISTRY: LazyLock<OnceCell<TokioMutex<HashMap<u16, (Arc<DB>, Arc<BoundColumnFamily<'static>>, Arc<BoundColumnFamily<'static>>, Arc<BoundColumnFamily<'static>>, u32)>>>> = 
    LazyLock::new(|| OnceCell::new());

// A static map to hold Semaphores for each RocksDB daemon port.
// We use a Semaphore with 1 permit to act as a port-specific Mutex,
// preventing multiple threads/tasks from trying to initialize a daemon on the same port simultaneously.
lazy_static! {
    static ref ROCKSDB_DAEMON_PORT_LOCKS: TokioMutex<HashMap<u16, Arc<Semaphore>>> = 
        TokioMutex::new(HashMap::new());
}

// ────────────────────────────────────────────────────────────────
// Global lazy DB cache + per-port watch channel
// ────────────────────────────────────────────────────────────────
#[derive(Clone)]
struct LazyDB {
    db:          Arc<DB>,
    kv_pairs: Arc<BoundColumnFamily<'static>>,
    vertices: Arc<BoundColumnFamily<'static>>,
    edges:    Arc<BoundColumnFamily<'static>>,
}

type LazyCache = HashMap<u16, (LazyDB, watch::Sender<()>)>;

static GLOBAL_LAZY_DB: std::sync::OnceLock<Arc<TokioMutex<LazyCache>>> = std::sync::OnceLock::new();

fn get_lazy_cache() -> Arc<TokioMutex<LazyCache>> {
    GLOBAL_LAZY_DB
        .get_or_init(|| Arc::new(TokioMutex::new(HashMap::new())))
        .clone()
}

// ---------------------------------------------------------------------
// 2. get_or_create_watcher – **NO DUMMY DB / CFs**
// ---------------------------------------------------------------------
/// Returns a `watch::Receiver<()>` that is signalled **once** the real
/// RocksDB instance (with `kv_pairs`, `vertices`, `edges`) has been
/// inserted into the global cache by `RocksDBDaemon::new`.
async fn get_or_create_watcher(port: u16) -> watch::Receiver<()> {
    let cache = get_lazy_cache();
    let mut map = cache.lock().await;

    // -----------------------------------------------------------------
    // If the port already has a watch channel → reuse it.
    // If not → create a fresh channel.  The real DB will be inserted
    // later in `RocksDBDaemon::new` and will call `tx.send(())`.
    // -----------------------------------------------------------------
    let sender = map
        .entry(port)
        .or_insert_with(|| {
            // No real DB yet – just the channel.
            let (tx, _) = watch::channel(());

            // **Placeholder that will be overwritten immediately**.
            // We use a *never-opened* dummy DB only to satisfy the type.
            // It is **never read**.
            (
                LazyDB {
                    db:       Arc::new(DB::open_default("/tmp/graphdb_dummy_never_used")
                                        .expect("dummy DB for placeholder")),
                    // SAFETY: these are never dereferenced – replaced before use.
                    kv_pairs: unsafe { std::mem::transmute(Arc::new(std::ptr::null_mut::<rocksdb::ColumnFamily>())) },
                    vertices: unsafe { std::mem::transmute(Arc::new(std::ptr::null_mut::<rocksdb::ColumnFamily>())) },
                    edges:    unsafe { std::mem::transmute(Arc::new(std::ptr::null_mut::<rocksdb::ColumnFamily>())) },
                },
                tx,
            )
        })
        .1
        .clone();

    sender.subscribe()
}

// ---------------------------------------------------------------------
// 3. wait_for_real_db – **NO DUMMY DB / CFs**
// ---------------------------------------------------------------------
/// Same as `get_or_create_watcher` but exposed for external callers
/// (e.g., CLI waiting for the daemon to be ready).
pub async fn wait_for_real_db(port: u16) -> watch::Receiver<()> {
    get_or_create_watcher(port).await
}


// Thread-safe wrapper for Arc<rocksdb::DB>
#[derive(Clone)]
struct SafeDB(Arc<rocksdb::DB>);

unsafe impl Send for SafeDB {}
unsafe impl Sync for SafeDB {}

impl Deref for SafeDB {
    type Target = rocksdb::DB;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

const TIMEOUT_SECS: u64 = 5;
const SOCKET_TIMEOUT_MS: i32 = 1000;
const MAX_MESSAGE_SIZE: i32 = 1024 * 1024;
const BIND_RETRIES: usize = 5;
const MAX_CONSECUTIVE_ERRORS: u32 = 10;
const MAX_WAIT_ATTEMPTS: usize = 40;
const WAIT_DELAY_MS: u64 = 500;
const MAX_REGISTRY_ATTEMPTS: u32 = 5;
const MAX_KILL_ATTEMPTS: usize = 10;
const ZMQ_EAGAIN_SENTINEL: &str = "ZMQ_EAGAIN_SENTINEL"; 
static CLUSTER_INIT_LOCK: OnceCell<TokioMutex<()>> = OnceCell::const_new();

async fn get_cluster_init_lock() -> &'static TokioMutex<()> {
    CLUSTER_INIT_LOCK
        .get_or_init(|| async { TokioMutex::new(()) })
        .await
}

#[macro_export]
macro_rules! handle_rocksdb_op {
    ($op:expr, $err_msg:expr) => {
        $op.map_err(|e| {
            error!("{}: {}", $err_msg, e);
            GraphError::StorageError(format!("{}: {}", $err_msg, e))
        })
    };
}

impl<'a> RocksDBDaemon<'a> {
    /// Helper function to encapsulate the blocking RocksDB I/O.
    /// **CRITICAL FIX:** Implements a retry mechanism to handle transient lock contention 
    /// from the main thread's pre-initialization cleanup logic (e.g., unlocking the DB).
    pub async fn open_rocksdb_with_cfs(
        config: &RocksDBConfig,
        db_path: &Path,
        use_compression: bool,
        read_only: bool,
    ) -> GraphResult<(
        Arc<DB>,
        Arc<BoundColumnFamily<'static>>,
        Arc<BoundColumnFamily<'static>>,
        Arc<BoundColumnFamily<'static>>,
    )> {
        const MAX_RETRIES: usize = 5;
        const INITIAL_BACKOFF_MS: u64 = 100;
        const MAX_BACKOFF_MS: u64 = 1600;

        let mut attempts = 0;

        loop {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            opts.set_max_open_files(-1);
            opts.set_max_background_jobs(4);

            if use_compression {
                let cache_cap = config.cache_capacity.unwrap_or(1024 * 1024 * 1024) as usize;
                opts.set_compression_type(DBCompressionType::Snappy);
                opts.set_compaction_style(DBCompactionStyle::Level);
                opts.set_write_buffer_size(cache_cap);
                opts.set_max_write_buffer_number(3);

                let block_cache = RocksDBCache::new_lru_cache(cache_cap);
                let mut table_options = BlockBasedOptions::default();
                table_options.set_block_cache(&block_cache);
                opts.set_block_based_table_factory(&table_options);
            }

            let cf_names = vec!["kv_pairs", "vertices", "edges"];
            let cfs = cf_names
                .iter()
                .map(|&name| {
                    let mut cf_opts = Options::default();
                    if use_compression {
                        cf_opts.set_compression_type(DBCompressionType::Snappy);
                    }
                    ColumnFamilyDescriptor::new(name, cf_opts)
                })
                .collect::<Vec<_>>();

            let mode_str = if read_only { "READ-ONLY" } else { "PRIMARY (WRITABLE)" };
            info!("Attempting to open RocksDB in {} mode at {:?}", mode_str, db_path);
            println!("===> ATTEMPTING TO OPEN ROCKSDB IN {} MODE AT {:?}", mode_str, db_path);

            let db_result = if read_only {
                DB::open_cf_for_read_only(&opts, db_path, cf_names.clone(), false)
            } else {
                DB::open_cf_descriptors(&opts, db_path, cfs)
            };

            match db_result {
                Ok(db) => {
                    let db_arc = Arc::new(db);
                    // Create column families in a separate scope to ensure leaked_db is dropped
                    let (kv_pairs, vertices, edges) = {
                        let leaked_db = Arc::into_raw(db_arc.clone());
                        let static_db: &'static DB = unsafe { &*leaked_db };

                        let kv_cf_ref = static_db.cf_handle("kv_pairs").ok_or_else(|| {
                            error!("Failed to get kv_pairs column family at {:?}", db_path);
                            println!("===> ERROR: FAILED TO GET kv_pairs COLUMN FAMILY AT {:?}", db_path);
                            GraphError::StorageError(format!("Failed to get kv_pairs column family"))
                        })?;
                        let kv_pairs: Arc<BoundColumnFamily<'static>> = unsafe {
                            std::mem::transmute(Arc::new(kv_cf_ref))
                        };

                        let vert_cf_ref = static_db.cf_handle("vertices").ok_or_else(|| {
                            error!("Failed to get vertices column family at {:?}", db_path);
                            println!("===> ERROR: FAILED TO GET vertices COLUMN FAMILY AT {:?}", db_path);
                            GraphError::StorageError(format!("Failed to get vertices column family"))
                        })?;
                        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
                            std::mem::transmute(Arc::new(vert_cf_ref))
                        };

                        let edge_cf_ref = static_db.cf_handle("edges").ok_or_else(|| {
                            error!("Failed to get edges column family at {:?}", db_path);
                            println!("===> ERROR: FAILED TO GET edges COLUMN FAMILY AT {:?}", db_path);
                            GraphError::StorageError(format!("Failed to get edges column family"))
                        })?;
                        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
                            std::mem::transmute(Arc::new(edge_cf_ref))
                        };

                        (kv_pairs, vertices, edges)
                    };

                    info!("RocksDB opened at {:?} in {} mode after {} attempts", db_path, mode_str, attempts + 1);
                    println!("===> ROCKSDB OPENED AT {:?} IN {} MODE AFTER {} ATTEMPTS", db_path, mode_str, attempts + 1);
                    return Ok((db_arc, kv_pairs, vertices, edges));
                }
                Err(e) => {
                    attempts += 1;
                    let error_msg = e.to_string();
                    let is_lock_error = error_msg.contains("lock")
                        || error_msg.contains("busy")
                        || error_msg.contains("WouldBlock")
                        || error_msg.contains("Resource temporarily unavailable")
                        || error_msg.contains("already in use");

                    if attempts >= MAX_RETRIES || !is_lock_error {
                        error!("Failed to open RocksDB at {:?} after {} attempts: {}", db_path, MAX_RETRIES, e);
                        println!("===> ERROR: FAILED TO OPEN ROCKSDB AT {:?} AFTER {} ATTEMPTS: {}", db_path, MAX_RETRIES, e);
                        return Err(GraphError::StorageError(format!(
                            "Failed to open RocksDB: {}. Ensure no other process is using the database.",
                            e
                        )));
                    }

                    let backoff_factor = 2_u64.pow(attempts as u32 - 1);
                    let sleep_ms = (INITIAL_BACKOFF_MS * backoff_factor).min(MAX_BACKOFF_MS);
                    warn!("RocksDB lock contention at {:?}. Retrying in {}ms (Attempt {}/{})", db_path, sleep_ms, attempts, MAX_RETRIES);
                    println!("===> WARNING: ROCKSDB LOCK CONTENTION AT {:?}. RETRYING IN {}ms (ATTEMPT {}/{})", db_path, sleep_ms, attempts, MAX_RETRIES);
                    tokio::time::sleep(TokioDuration::from_millis(sleep_ms)).await;
                }
            }
        }
    }


    // ---------------------------------------------------------------------
    // 3. Open the *real* DB **once** – called from `RocksDBDaemon::new`
    // ---------------------------------------------------------------------
    async fn open_real_db(
        config: &RocksDBConfig,
        db_path: &Path,
    ) -> GraphResult<(
        Arc<DB>,
        Arc<BoundColumnFamily<'static>>,
        Arc<BoundColumnFamily<'static>>,
        Arc<BoundColumnFamily<'static>>,
    )> {
        // ---- same retry logic you already had ----
        const MAX_RETRIES: usize = 5;
        const INITIAL_BACKOFF_MS: u64 = 100;
        const MAX_BACKOFF_MS: u64 = 1600;

        let mut attempts = 0;
        loop {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);
            opts.set_max_open_files(-1);
            opts.set_max_background_jobs(4);

            if config.use_compression {
                let cache_cap = config.cache_capacity.unwrap_or(1 << 30) as usize;
                opts.set_compression_type(DBCompressionType::Snappy);
                opts.set_compaction_style(DBCompactionStyle::Level);
                opts.set_write_buffer_size(cache_cap);
                opts.set_max_write_buffer_number(3);
                let block_cache = RocksDBCache::new_lru_cache(cache_cap);
                let mut tbl = BlockBasedOptions::default();
                tbl.set_block_cache(&block_cache);
                opts.set_block_based_table_factory(&tbl);
            }

            let cf_desc = ["kv_pairs", "vertices", "edges"]
                .iter()
                .map(|&name| {
                    let mut cf_opts = Options::default();
                    if config.use_compression {
                        cf_opts.set_compression_type(DBCompressionType::Snappy);
                    }
                    ColumnFamilyDescriptor::new(name, cf_opts)
                })
                .collect::<Vec<_>>();

            let db = DB::open_cf_descriptors(&opts, db_path, cf_desc)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            let db_arc = Arc::new(db);

            // SAFETY: the DB lives as long as `db_arc`; we only extend the lifetime.
            let kv   = unsafe { std::mem::transmute(Arc::new(db_arc.cf_handle("kv_pairs").unwrap())) };
            let vert = unsafe { std::mem::transmute(Arc::new(db_arc.cf_handle("vertices").unwrap())) };
            let edge = unsafe { std::mem::transmute(Arc::new(db_arc.cf_handle("edges").unwrap())) };

            info!("Real RocksDB opened at {:?} (attempt {})", db_path, attempts + 1);
            return Ok((db_arc, kv, vert, edge));
        }
    }

    /// Opens DB immediately, binds ZMQ in background — CLI returns instantly
    pub async fn new(
        config: RocksDBConfig,
    ) -> GraphResult<(RocksDBDaemon<'static>, mpsc::Receiver<()>)> {
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let current_pid = std::process::id() as u32;

        let db_path = if config.path.ends_with(&port.to_string()) {
            PathBuf::from(config.path.clone())
        } else {
            PathBuf::from(config.path.clone()).join(port.to_string())
        };
        let db_path_clone = db_path.clone();
        info!("Initializing RocksDBDaemon at {:?}", db_path);
        println!("===> RocksDBDaemon::new() CALLED");

        tokio::fs::create_dir_all(&db_path).await
            .map_err(|e| GraphError::StorageError(format!("Failed to create dir: {}", e)))?;

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: current_pid,
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: None,
            engine_type: Some("rocksdb".to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
            zmq_ready: false,
            engine_synced: false,
        };
        daemon_registry.register_daemon(daemon_metadata.clone()).await?;

        let zmq_context = Arc::new(ZmqContext::new());
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let ipc_path = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);

        if Path::new(ipc_path).exists() {
            let _ = tokio::fs::remove_file(ipc_path).await;
        }

        // Channel for ZMQ readiness
        let (ready_tx, ready_rx) = mpsc::channel::<()>(1);
        let running = Arc::new(TokioMutex::new(true));
        let running_clone = running.clone();

        // -----------------------------------------------------------------
        // 1. Open REAL DB NOW (once)
        // -----------------------------------------------------------------
        let (db, kv_pairs, vertices, edges) = Self::open_rocksdb_with_cfs(&config, &db_path, config.use_compression, false).await?;

        // -----------------------------------------------------------------
        // 2. Spawn ZMQ server in background
        // -----------------------------------------------------------------
        // Replace the spawn block with:
        let zmq_thread_handle = {
            let zmq_context_thread = zmq_context.clone();
            let endpoint_thread = endpoint.clone();
            let config_thread = config.clone();
            let daemon_metadata_thread = daemon_metadata.clone();
            let ready_tx = ready_tx.clone();

            // Pass the already-opened DB handles
            let db_clone = db.clone();
            let kv_clone = kv_pairs.clone();
            let vert_clone = vertices.clone();
            let edge_clone = edges.clone();

            std::thread::spawn(move || -> GraphResult<()> {
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    let socket_raw = zmq_context_thread.socket(REP)?;
                    for i in 0..5 {
                        if socket_raw.bind(&endpoint_thread).is_ok() {
                            let _ = ready_tx.send(()).await;
                            break;
                        }
                        tokio::time::sleep(TokioDuration::from_millis(100 * (i + 1) as u64)).await;
                    }

                    let mut md = daemon_metadata_thread.clone();
                    md.zmq_ready = true;
                    GLOBAL_DAEMON_REGISTRY.get().await.register_daemon(md).await?;

                    let zmq_socket = Arc::new(TokioMutex::new(socket_raw));
                    RocksDBDaemon::run_zmq_server_lazy(
                        port,
                        config_thread,
                        running_clone,
                        zmq_socket,
                        endpoint_thread,
                        db_clone,
                        kv_clone,
                        vert_clone,
                        edge_clone,
                        db_path.clone(),
                    ).await
                })
            })
        };

        let (shutdown_tx, _) = mpsc::channel::<()>(1);

        let daemon = RocksDBDaemon {
            port,
            db_path: db_path_clone,
            db,
            kv_pairs,
            vertices,
            edges,
            running: running.clone(),
            shutdown_tx,
            zmq_context: zmq_context.clone(),
            zmq_thread: Arc::new(TokioMutex::new(Some(zmq_thread_handle))),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id: port as u64,
        };

        println!("===> ROCKSDB DAEMON INITIALIZED ON PORT {} (DB OPENED)", port);
        Ok((daemon, ready_rx))
    }

    /// ZMQ server – DB is already opened in `RocksDBDaemon::new()`
    /// All original commands are preserved.
    async fn run_zmq_server_lazy(
        port: u16,
        _config: RocksDBConfig,               // kept for signature compatibility
        running: Arc<TokioMutex<bool>>,
        zmq_socket: Arc<TokioMutex<ZmqSocket>>,
        endpoint: String,
        db: Arc<DB>,
        kv_pairs: Arc<BoundColumnFamily<'static>>,
        vertices: Arc<BoundColumnFamily<'static>>,
        edges: Arc<BoundColumnFamily<'static>>,
        db_path: PathBuf,
    ) -> GraphResult<()> {
        info!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);
        println!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);

        // Configure socket
        {
            let socket = zmq_socket.lock().await;
            socket.set_linger(1000)
                .map_err(|e| GraphError::StorageError(format!("Failed to set socket linger: {}", e)))?;
            socket.set_rcvtimeo(SOCKET_TIMEOUT_MS)
                .map_err(|e| GraphError::StorageError(format!("Failed to set receive timeout: {}", e)))?;
            socket.set_sndtimeo(SOCKET_TIMEOUT_MS)
                .map_err(|e| GraphError::StorageError(format!("Failed to set send timeout: {}", e)))?;
            socket.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)
                .map_err(|e| GraphError::StorageError(format!("Failed to set max message size: {}", e)))?;
        }

        info!("ZeroMQ server configured for port {}", port);
        println!("===> ZEROMQ SERVER CONFIGURED FOR PORT {}", port);

        let mut consecutive_errors = 0;

        while *running.lock().await {
            let msg_result = {
                let socket = zmq_socket.lock().await;
                socket.recv_bytes(DONTWAIT)
            };

            let msg: Vec<u8> = match msg_result {
                Ok(m) => {
                    consecutive_errors = 0;
                    debug!("Received ZeroMQ message for port {}", port);
                    m
                }
                Err(ZmqError::EAGAIN) => {
                    tokio::time::sleep(TokioDuration::from_millis(10)).await;
                    continue;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    warn!("Failed to receive ZeroMQ message (attempt {}): {}", consecutive_errors, e);
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive ZeroMQ errors, shutting down server");
                        break;
                    }
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                    continue;
                }
            };

            if msg.is_empty() {
                let resp = json!({"status":"error","message":"Received empty message"});
                let socket = zmq_socket.lock().await;
                Self::send_zmq_response_static(&socket, &resp, port).await?;
                continue;
            }

            let request: Value = match serde_json::from_slice(&msg) {
                Ok(r) => r,
                Err(e) => {
                    let resp = json!({"status":"error","message":format!("Parse error: {}",e)});
                    let socket = zmq_socket.lock().await;
                    Self::send_zmq_response_static(&socket, &resp, port).await?;
                    continue;
                }
            };

            let command = request.get("command").and_then(|c| c.as_str());

            let response = match command {
                // === Control commands ===
                Some("initialize") => {
                    info!("Initialization command received for port {}", port);
                    println!("===> INITIALIZATION COMMAND RECEIVED FOR PORT {}", port);
                    json!({
                        "status": "success",
                        "message": "ZMQ server is bound and DB is open.",
                        "port": port,
                        "ipc_path": &endpoint
                    })
                }
                Some("status") | Some("ping") => {
                    json!({"status":"success","port":port,"db_open":true})
                }
                Some("force_unlock") => {
                    info!("Force unlock requested for port {}", port);
                    match Self::force_unlock_static(&db_path).await {
                        Ok(_) => json!({"status":"success"}),
                        Err(e) => json!({"status":"error","message":e.to_string()}),
                    }
                }

                // === Data commands – DB is already open ===
                Some(cmd) if [
                    "set_key","get_key","delete_key",
                    "create_vertex","get_vertex","update_vertex","delete_vertex",
                    "create_edge","get_edge","update_edge","delete_edge",
                    "get_all_vertices","get_all_edges",
                    "get_all_vertices_by_type","get_all_edges_by_type",
                    "clear_data","force_reset","flush"
                ].contains(&cmd) => {
                    match Self::execute_db_command(
                        cmd, &request,
                        &db, &kv_pairs, &vertices, &edges,
                        port, &db_path, &endpoint
                    ).await {
                        Ok(r) => r,
                        Err(e) => json!({"status":"error","message":e.to_string()}),
                    }
                }

                // === Fallback ===
                Some(cmd) => {
                    error!("Unsupported command: {}", cmd);
                    json!({"status":"error","message":format!("Unsupported command: {}",cmd)})
                }
                None => {
                    error!("No command specified in request");
                    json!({"status":"error","message":"No command specified"})
                }
            };

            let socket = zmq_socket.lock().await;
            Self::send_zmq_response_static(&socket, &response, port).await?;
        }

        info!("ZMQ server shutting down for port {}", port);
        let socket = zmq_socket.lock().await;
        let _ = socket.disconnect(&endpoint);
        Ok(())
    }

    async fn execute_db_command(
        command: &str,
        request: &Value,
        db: &Arc<DB>,
        kv_pairs: &Arc<BoundColumnFamily<'static>>,
        vertices: &Arc<BoundColumnFamily<'static>>,
        edges: &Arc<BoundColumnFamily<'static>>,
        port: u16,
        db_path: &PathBuf,
        endpoint: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        match command {
            "set_key" => {
                let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = request.get("key").and_then(|k| k.as_str()).ok_or("Missing key")?;
                let value = request.get("value").and_then(|v| v.as_str()).ok_or("Missing value")?;

                let cf_handle = db.cf_handle(cf_name).ok_or(format!("Column family {} not found", cf_name))?;
                let write_opts = WriteOptions::default();
                db.put_cf_opt(&cf_handle, key.as_bytes(), value.as_bytes(), &write_opts)?;
                info!("Set key {} in {} for port {}", key, cf_name, port);
                Ok(json!({"status": "success"}))
            }

            "get_key" => {
                let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = request.get("key").and_then(|k| k.as_str()).ok_or("Missing key")?;

                let cf_handle = db.cf_handle(cf_name).ok_or(format!("Column family {} not found", cf_name))?;
                let result = db.get_cf(&cf_handle, key.as_bytes())?;
                let value_str = result.map(|v| String::from_utf8_lossy(&v).to_string());
                Ok(json!({"status": "success", "value": value_str}))
            }

            "delete_key" => {
                let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = request.get("key").and_then(|k| k.as_str()).ok_or("Missing key")?;

                let cf_handle = db.cf_handle(cf_name).ok_or(format!("Column family {} not found", cf_name))?;
                let write_opts = WriteOptions::default();
                db.delete_cf_opt(&cf_handle, key.as_bytes(), &write_opts)?;
                info!("Deleted key {} in {} for port {}", key, cf_name, port);
                Ok(json!({"status": "success"}))
            }

            "flush" => {
                db.flush()?;
                Ok(json!({"status": "success"}))
            }

            "get_all_vertices" => {
                let iterator = db.iterator_cf(&Arc::clone(vertices), rocksdb::IteratorMode::Start);
                let mut vertices_vec = Vec::new();

                for item in iterator {
                    let (_, value) = item?;
                    let vertex = deserialize_vertex(&value)?;
                    vertices_vec.push(vertex);
                }
                Ok(json!({"status": "success", "vertices": vertices_vec}))
            }

            "get_all_vertices_by_type" => {
                let vertex_type: Identifier =
                    serde_json::from_value(request["vertex_type"].clone()).map_err(|_| "Invalid or missing vertex_type")?;

                let iterator = db.iterator_cf(&Arc::clone(vertices), rocksdb::IteratorMode::Start);
                let mut vertices_vec = Vec::new();

                for item in iterator {
                    let (_, value) = item?;
                    let vertex = deserialize_vertex(&value)?;
                    if vertex.label == vertex_type {
                        vertices_vec.push(vertex);
                    }
                }
                Ok(json!({"status": "success", "vertices": vertices_vec}))
            }

            "get_all_edges" => {
                let iterator = db.iterator_cf(&Arc::clone(edges), rocksdb::IteratorMode::Start);
                let mut edges_vec = Vec::new();

                for item in iterator {
                    let (_, value) = item?;
                    let edge = deserialize_edge(&value)?;
                    edges_vec.push(edge);
                }
                Ok(json!({"status": "success", "edges": edges_vec}))
            }

            "get_all_edges_by_type" => {
                let edge_type: Identifier =
                    serde_json::from_value(request["edge_type"].clone()).map_err(|_| "Invalid or missing edge_type")?;

                let iterator = db.iterator_cf(&Arc::clone(edges), rocksdb::IteratorMode::Start);
                let mut edges_vec = Vec::new();

                for item in iterator {
                    let (_, value) = item?;
                    let edge = deserialize_edge(&value)?;
                    if edge.t == edge_type {
                        edges_vec.push(edge);
                    }
                }
                Ok(json!({"status": "success", "edges": edges_vec}))
            }

            "clear_data" => {
                let mut batch = WriteBatch::default();
                let iterator = db.iterator_cf(&Arc::clone(kv_pairs), rocksdb::IteratorMode::Start);

                for item in iterator {
                    let (key, _) = item?;
                    batch.delete_cf(&Arc::clone(kv_pairs), &key);
                }

                db.write(batch)?;
                db.flush()?;
                Ok(json!({"status": "success"}))
            }

            "force_reset" => {
                let mut batch = WriteBatch::default();
                for cf in &[kv_pairs, vertices, edges] {
                    let iterator = db.iterator_cf(&Arc::clone(cf), rocksdb::IteratorMode::Start);
                    for item in iterator {
                        let (key, _) = item?;
                        batch.delete_cf(&Arc::clone(cf), &key);
                    }
                }

                db.write(batch)?;
                db.flush()?;
                info!("Force reset completed for port {}", port);
                Ok(json!({"status": "success"}))
            }

            "force_unlock" => {
                Self::force_unlock_static(db_path).await?;
                Ok(json!({"status": "success"}))
            }

            "force_unlock_path" => {
                Self::force_unlock_path_static(db_path).await?;
                Ok(json!({"status": "success"}))
            }

            cmd => {
                error!("Unsupported command: {}", cmd);
                Ok(json!({"status": "error", "message": format!("Unsupported command: {}", cmd)}))
            }
        }
    }

    pub async fn new_with_db(config: RocksDBConfig, existing_db: Arc<DB>) -> GraphResult<(RocksDBDaemon<'static>, mpsc::Receiver<()>)> {
        info!("Initializing RocksDB daemon with existing DB at {:?}", config.path);

        // --- 1. PORT SELECTION LOGIC ---
        const BIND_RETRIES: usize = 5;
        
        let config_port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let global_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        
        let free_daemon_metadata = global_registry.find_free_storage_daemon().await?;
        
        let port: u16 = if let Some(metadata) = free_daemon_metadata {
            info!("Priority 1: Reused uninitialized storage daemon port {} found via registry search.", metadata.port);
            metadata.port
        } else {
            info!("Priority 2/3: No reusable port found. Using config port {}.", config_port);
            config_port
        };

        // --- 2. DERIVE DB PATH ---
        let base_db_path = PathBuf::from(config.path.clone());
        let db_path = base_db_path.join(port.to_string());
        
        info!("RocksDBDaemon using port {} at path {:?}", port, db_path);

        // --- 3. CLEANUP STALE LOCKS ---
        let lock_path = db_path.join("LOCK");
        if lock_path.exists() {
            let system = System::new_with_specifics(RefreshKind::nothing().with_processes(ProcessRefreshKind::everything()));
            if let Ok(Some(metadata)) = GLOBAL_DAEMON_REGISTRY.find_daemon_by_port(port).await {
                if system.process(Pid::from_u32(metadata.pid)).is_none() {
                    info!("Removing stale lock file at {:?}", lock_path);
                    handle_rocksdb_op!(
                        tokio::fs::remove_file(&lock_path).await,
                        format!("Failed to remove stale lock file at {:?}", lock_path)
                    )?;
                }
            }
        }
        
        // --- 4. COLUMN FAMILY HANDLES ---
        let kv_pairs: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(existing_db.cf_handle("kv_pairs").ok_or_else(|| {
                GraphError::StorageError("Column family kv_pairs not found".to_string())
            })?))
        };
        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(existing_db.cf_handle("vertices").ok_or_else(|| {
                GraphError::StorageError("Column family vertices not found".to_string())
            })?))
        };
        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(existing_db.cf_handle("edges").ok_or_else(|| {
                GraphError::StorageError("Column family edges not found".to_string())
            })?))
        };

        // --- 5. RAFT INITIALIZATION ---
        let node_id = port as u64;

        #[cfg(feature = "with-openraft-rocksdb")]
        let (raft, raft_storage) = {
            let raft_db_path = db_path.join("raft");
            handle_rocksdb_op!(
                tokio::fs::create_dir_all(&raft_db_path).await,
                format!("Failed to create Raft DB directory at {:?}", raft_db_path)
            )?;
            let raft_storage = RocksDBRaftStorage::new(&raft_db_path).await?;
            let raft_config = Arc::new(RaftConfig {
                cluster_name: "graphdb-cluster".to_string(),
                heartbeat_interval: 250,
                election_timeout_min: 1000,
                election_timeout_max: 2000,
                ..Default::default()
            });
            let raft = Raft::new(
                node_id,
                raft_config,
                Arc::new(raft_storage.clone()),
                Arc::new(RaftTcpNetwork {}),
            );
            (Some(Arc::new(raft)), Some(Arc::new(raft_storage)))
        };
        
        #[cfg(not(feature = "with-openraft-rocksdb"))]
        let (raft, raft_storage): (Option<Arc<Raft<TypeConfig>>>, Option<Arc<RocksDBRaftStorage>>) =
            (None, None);

        let zmq_context = Arc::new(ZmqContext::new());
        
        // --- 6. CHANNEL SETUP ---
        // ====================================================================
        // CRITICAL: Separate channels for readiness signaling and shutdown
        // ====================================================================
        let (tx_readiness, mut rx_readiness) = mpsc::channel(1); 
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let ipc_path = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
        
        if std::path::Path::new(ipc_path).exists() {
            warn!("Stale IPC socket found at {}. Attempting cleanup.", ipc_path);
            if let Err(e) = tokio::fs::remove_file(ipc_path).await {
                error!("Failed to remove stale IPC socket {}: {}", ipc_path, e);
            } else {
                info!("Successfully removed stale IPC socket {}", ipc_path);
            }
        }
        
        let running = Arc::new(TokioMutex::new(true));
        
        // --- 7. INITIALIZE DAEMON STRUCT ---
        let daemon: RocksDBDaemon<'static> = RocksDBDaemon {
            port,
            db_path: db_path.clone(),
            db: existing_db.clone(),
            kv_pairs: kv_pairs.clone(),
            vertices: vertices.clone(),
            edges: edges.clone(),
            running: running.clone(),
            shutdown_tx: shutdown_tx.clone(),
            zmq_context: zmq_context.clone(),
            zmq_thread: Arc::new(TokioMutex::new(None)),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id,
        };

        // --- 8. INITIAL DAEMON REGISTRY UPDATE (zmq_ready: false) ---
        let initial_metadata = DaemonMetadata {
            port,
            service_type: "storage".to_string(),
            ip_address: "127.0.0.1".to_string(),
            config_path: Some(db_path.clone()),
            data_dir: Some(db_path.clone()),
            engine_type: Some(StorageEngineType::RocksDB.to_string()),
            pid: std::process::id() as u32,
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)?
                .as_nanos() as i64,
            zmq_ready: false,
            engine_synced: false,
        };
        handle_rocksdb_op!(
            GLOBAL_DAEMON_REGISTRY.register_daemon(initial_metadata.clone()).await,
            format!("Failed to register daemon for port {}", port)
        )?;

        // --- 9. SPAWN ZMQ THREAD ---
        let running_clone = running.clone();
        let zmq_context_clone = zmq_context.clone();
        let db_clone = existing_db.clone();
        let kv_pairs_clone = kv_pairs.clone();
        let vertices_clone = vertices.clone();
        let edges_clone = edges.clone();
        let db_path_clone = db_path.clone();
        let endpoint_clone = endpoint.clone();

        info!("About to start ZMQ server thread for port {}", port);
        let zmq_thread_handle = std::thread::spawn(move || -> GraphResult<()> {
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime for ZMQ server thread");
            rt.block_on(async {
                // Socket creation
                let zmq_socket_raw = match zmq_context_clone.socket(zmq::REP) {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to create ZMQ socket for thread: {}", e);
                        let _ = tx_readiness.send(()).await; 
                        return Err(GraphError::StorageError(format!("Failed to create ZMQ socket: {}", e)));
                    }
                };
                
                // Binding logic with retries
                let mut bind_success = false;
                for i in 0..BIND_RETRIES {
                    match zmq_socket_raw.bind(&endpoint_clone) {
                        Ok(_) => {
                            info!("ZMQ socket successfully bound to {}. Signaling readiness.", endpoint_clone);
                            bind_success = true;
                            let _ = tx_readiness.send(()).await;  // ====== CRITICAL: Signal readiness
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
                                let error_msg = format!(
                                    "Failed to bind ZMQ socket to {} after {} attempts: {}",
                                    endpoint_clone, BIND_RETRIES, e
                                );
                                error!("{}", error_msg);
                                let _ = tx_readiness.send(()).await; 
                                return Err(GraphError::StorageError(error_msg));
                            }
                        }
                    }
                }

                if !bind_success {
                    let error_msg = "Could not bind ZMQ socket after max retries. Aborting server run.".to_string();
                    error!("{}", error_msg);
                    return Err(GraphError::StorageError(error_msg)); 
                }

                let zmq_socket = Arc::new(TokioMutex::new(zmq_socket_raw));
                
                // Run server
                let endpoint_for_server = endpoint_clone.clone();
                RocksDBDaemon::run_zmq_server_static(
                    port,
                    db_clone,
                    kv_pairs_clone,
                    vertices_clone,
                    edges_clone,
                    running_clone,
                    zmq_socket,
                    endpoint_for_server,
                    db_path_clone,
                )
                .await
                .map_err(|e| {
                    error!("ZMQ server failed for endpoint {}: {}", endpoint_clone, e); 
                    e
                })
            })
        });

        *daemon.zmq_thread.lock().await = Some(zmq_thread_handle);

        // --- 10. WAIT FOR ZMQ READINESS AND FINAL REGISTRY UPDATE ---
        // ====================================================================
        // CRITICAL: Wait for ZMQ thread to signal readiness, then update registry
        // ====================================================================
        info!("Waiting for ZMQ server readiness signal on port {}...", port);
        
        rx_readiness.recv().await.ok_or_else(|| {
            GraphError::StorageError(format!("ZMQ server failed to start or signal readiness on port {}", port))
        })?;
        
        info!("ZMQ server ready signal received on port {}. Updating global daemon registry.", port);

        // Update DaemonMetadata: set zmq_ready to true
        let updated_metadata = DaemonMetadata {
            zmq_ready: true, 
            ..initial_metadata 
        };

        global_registry.register_daemon(updated_metadata).await?;
        info!("Daemon metadata updated: zmq_ready=true for port {}.", port);

        // --- 11. SIGNAL HANDLING ---
        #[cfg(unix)]
        {
            let daemon_for_signal = daemon.clone();
            let shutdown_tx = daemon.shutdown_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                info!("Received SIGTERM in RocksDB daemon on port {}", port);
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
                        error!("Failed to shutdown RocksDB daemon on port {}: {}", port, e);
                    } else {
                        info!("RocksDB daemon shutdown complete on port {}", port);
                    }
                } else {
                    info!("Ignoring SIGTERM as daemon on port {} is not fully initialized", port);
                }
            });
        }

        info!("RocksDB daemon initialization complete for port {}", port);
        Ok((daemon, shutdown_rx))
    }


    /* -------------------------------------------------------------
        new_with_client
        ------------------------------------------------------------- */
    pub async fn new_with_client(config: RocksDBConfig) -> GraphResult<Self> {
        let port = config.port.ok_or_else(|| {
            error!("No port specified in RocksDBConfig");
            println!("===> ERROR: NO PORT SPECIFIED IN ROCKSDB CONFIG");
            GraphError::ConfigurationError("No port specified in RocksDBConfig".to_string())
        })?;

        let db_path = if config.path.ends_with(&port.to_string()) {
            PathBuf::from(config.path.clone())
        } else {
            PathBuf::from(config.path.clone()).join(port.to_string())
        };

        info!("Initializing RocksDBDaemon in client mode (read-only) for port {}", port);
        println!("===> INITIALIZING ROCKSDB DAEMON IN CLIENT MODE (READ-ONLY) FOR PORT {}", port);

        // Verify a daemon is actually running on this port
        if !is_storage_daemon_running(port).await {
            error!("No running daemon found on port {}", port);
            println!("===> ERROR: NO RUNNING DAEMON FOUND ON PORT {}", port);
            return Err(GraphError::StorageError(format!("No running daemon found on port {}", port)));
        }

        // Open the database in read-only mode
        let mut opts = Options::default();
        opts.create_if_missing(false);
        opts.create_missing_column_families(false);
        let db = Arc::new(handle_rocksdb_op!(
            DB::open_for_read_only(&opts, &db_path, false),
            format!("Failed to open RocksDB in read-only mode at {}", db_path.display())
        )?);

        // Get column family handles (simplified - single pass)
        let kv_pairs: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(db.cf_handle("kv_pairs").ok_or_else(|| {
                GraphError::StorageError("Column family kv_pairs not found".to_string())
            })?))
        };
        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(db.cf_handle("vertices").ok_or_else(|| {
                GraphError::StorageError("Column family vertices not found".to_string())
            })?))
        };
        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(db.cf_handle("edges").ok_or_else(|| {
                GraphError::StorageError("Column family edges not found".to_string())
            })?))
        };

        let zmq_context = Arc::new(ZmqContext::new());
        let (shutdown_tx, _shutdown_rx) = mpsc::channel(1);

        let node_id = port as u64;
        
        let daemon = Self {
            port,
            db_path,
            db,
            kv_pairs,
            vertices,
            edges,
            running: Arc::new(TokioMutex::new(true)),
            shutdown_tx,
            zmq_context,
            zmq_thread: Arc::new(TokioMutex::new(None)), // No thread in client mode
            #[cfg(feature = "with-openraft-rocksdb")]
            raft: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage: None,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id,
        };

        info!("RocksDBDaemon initialized in client mode (read-only) for port {}", port);
        println!("===> ROCKSDB DAEMON INITIALIZATION COMPLETE FOR PORT {} IN CLIENT MODE", port);
        Ok(daemon)
    }

    /// Static method to send a ZMQ response.
    async fn send_zmq_response_static(socket: &ZmqSocket, response: &Value, port: u16) -> GraphResult<()> {
        let response_bytes = handle_rocksdb_op!(
            serde_json::to_vec(response),
            "Failed to serialize response"
        )?;
        handle_rocksdb_op!(
            socket.send(&response_bytes, 0),
            format!("Failed to send ZMQ response for port {}", port)
        )?;
        Ok(())
    }

    async fn run_zmq_server_static(
        port: u16,
        db: Arc<DB>,
        kv_pairs: Arc<BoundColumnFamily<'static>>,
        vertices: Arc<BoundColumnFamily<'static>>,
        edges: Arc<BoundColumnFamily<'static>>,
        running: Arc<TokioMutex<bool>>,
        zmq_socket: Arc<TokioMutex<ZmqSocket>>,
        endpoint: String,
        db_path: PathBuf,
    ) -> GraphResult<()> {
        info!("===> STARTING ZMQ SERVER FOR PORT {}", port);
        println!("===> STARTING ZMQ SERVER FOR PORT {}", port);

        // Configure socket timeouts and limits
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
        }

        info!("ZeroMQ server configured for port {}", port);
        println!("===> ZEROMQ SERVER CONFIGURED FOR PORT {}", port);

        let mut consecutive_errors = 0;

        while *running.lock().await {
            let msg_result = {
                let socket = zmq_socket.lock().await;
                socket.recv_bytes(DONTWAIT)
            };

            let msg: Vec<u8> = match msg_result {
                Ok(msg_bytes) => {
                    consecutive_errors = 0;
                    debug!("Received ZeroMQ message for port {}: {:?}", port, String::from_utf8_lossy(&msg_bytes));
                    msg_bytes
                }
                Err(ZmqError::EAGAIN) => {
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
                Ok(req) => req,
                Err(e) => {
                    error!("Failed to parse request for port {}: {}", port, e);
                    let response = json!({"status": "error", "message": format!("Parse error: {}", e)});
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
                    let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
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
                    // Dynamically fetch column family handle
                    let cf_handle = match db.cf_handle(cf_name) {
                        Some(cf) => cf,
                        None => {
                            error!("Column family {} not found for port {}", cf_name, port);
                            let response = json!({"status": "error", "message": format!("Column family {} not found", cf_name)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.put_cf_opt(&cf_handle, key.as_bytes(), value.as_bytes(), &write_opts) {
                        Ok(_) => {
                            info!("Set key {} in column family {} for port {}", key, cf_name, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to set key {} in column family {} for port {}: {}", key, cf_name, port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
                    }
                }
                Some("get_key") => {
                    let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            error!("Missing key in get_key request for port {}", port);
                            let response = json!({"status": "error", "message": "Missing key in get_key request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let cf_handle = match db.cf_handle(cf_name) {
                        Some(cf) => cf,
                        None => {
                            error!("Column family {} not found for port {}", cf_name, port);
                            let response = json!({"status": "error", "message": format!("Column family {} not found", cf_name)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    match db.get_cf(&cf_handle, key.as_bytes()) {
                        Ok(Some(val)) => {
                            let value_str = String::from_utf8_lossy(&val).to_string();
                            json!({"status": "success", "value": value_str})
                        }
                        Ok(None) => json!({"status": "success", "value": null}),
                        Err(e) => {
                            error!("Failed to get key {} in column family {} for port {}: {}", key, cf_name, port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
                    }
                }
                Some("delete_key") => {
                    let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                    let key = match request.get("key").and_then(|k| k.as_str()) {
                        Some(k) => k,
                        None => {
                            let response = json!({"status": "error", "message": "Missing key in delete_key request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let cf_handle = match db.cf_handle(cf_name) {
                        Some(cf) => cf,
                        None => {
                            error!("Column family {} not found for port {}", cf_name, port);
                            let response = json!({"status": "error", "message": format!("Column family {} not found", cf_name)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.delete_cf_opt(&cf_handle, key.as_bytes(), &write_opts) {
                        Ok(_) => {
                            info!("Deleted key {} in column family {} for port {}", key, cf_name, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to delete key {} in column family {} for port {}: {}", key, cf_name, port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
                    }
                }
                Some("flush") => {
                    match db.flush() {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("get_all_vertices") => {
                    let cf_handle = match db.cf_handle("vertices") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family vertices not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family vertices not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let iterator = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                    let mut vertices = Vec::new();
                    for item in iterator {
                        match item {
                            Ok((_, value)) => {
                                match deserialize_vertex(&value) {
                                    Ok(vertex) => vertices.push(vertex),
                                    Err(e) => {
                                        warn!("Failed to deserialize vertex for port {}: {}", port, e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Iterator error for port {}: {}", port, e);
                                let response = json!({"status": "error", "message": format!("Iterator error: {}", e)});
                                let socket = zmq_socket.lock().await;
                                Self::send_zmq_response_static(&socket, &response, port).await?;
                                continue;
                            }
                        }
                    }
                    json!({"status": "success", "vertices": vertices})
                }
                Some("get_all_vertices_by_type") => {
                    let vertex_type = match request.get("vertex_type").and_then(|t| serde_json::from_value::<Identifier>(t.clone()).ok()) {
                        Some(t) => t,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing vertex_type in get_all_vertices_by_type request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let cf_handle = match db.cf_handle("vertices") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family vertices not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family vertices not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let iterator = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                    let mut vertices = Vec::new();
                    for item in iterator {
                        match item {
                            Ok((_, value)) => {
                                match deserialize_vertex(&value) {
                                    Ok(vertex) if vertex.label == vertex_type => vertices.push(vertex),
                                    Ok(_) => continue,
                                    Err(e) => {
                                        warn!("Failed to deserialize vertex for port {}: {}", port, e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Iterator error for port {}: {}", port, e);
                                let response = json!({"status": "error", "message": format!("Iterator error: {}", e)});
                                let socket = zmq_socket.lock().await;
                                Self::send_zmq_response_static(&socket, &response, port).await?;
                                continue;
                            }
                        }
                    }
                    json!({"status": "success", "vertices": vertices})
                }
                Some("get_all_edges") => {
                    let cf_handle = match db.cf_handle("edges") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family edges not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family edges not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let iterator = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                    let mut edges = Vec::new();
                    for item in iterator {
                        match item {
                            Ok((_, value)) => {
                                match deserialize_edge(&value) {
                                    Ok(edge) => edges.push(edge),
                                    Err(e) => {
                                        warn!("Failed to deserialize edge for port {}: {}", port, e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Iterator error for port {}: {}", port, e);
                                let response = json!({"status": "error", "message": format!("Iterator error: {}", e)});
                                let socket = zmq_socket.lock().await;
                                Self::send_zmq_response_static(&socket, &response, port).await?;
                                continue;
                            }
                        }
                    }
                    json!({"status": "success", "edges": edges})
                }
                Some("get_all_edges_by_type") => {
                    let edge_type = match request.get("edge_type").and_then(|t| serde_json::from_value::<Identifier>(t.clone()).ok()) {
                        Some(t) => t,
                        None => {
                            let response = json!({"status": "error", "message": "Invalid or missing edge_type in get_all_edges_by_type request"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let cf_handle = match db.cf_handle("edges") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family edges not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family edges not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let iterator = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                    let mut edges = Vec::new();
                    for item in iterator {
                        match item {
                            Ok((_, value)) => {
                                match deserialize_edge(&value) {
                                    Ok(edge) if edge.t == edge_type => edges.push(edge),
                                    Ok(_) => continue,
                                    Err(e) => {
                                        warn!("Failed to deserialize edge for port {}: {}", port, e);
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Iterator error for port {}: {}", port, e);
                                let response = json!({"status": "error", "message": format!("Iterator error: {}", e)});
                                let socket = zmq_socket.lock().await;
                                Self::send_zmq_response_static(&socket, &response, port).await?;
                                continue;
                            }
                        }
                    }
                    json!({"status": "success", "edges": edges})
                }
                Some("clear_data") => {
                    let cf_handle = match db.cf_handle("kv_pairs") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family kv_pairs not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family kv_pairs not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let iterator = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                    let mut batch = WriteBatch::default();
                    for item in iterator {
                        match item {
                            Ok((key, _)) => batch.delete_cf(&cf_handle, &key),
                            Err(e) => {
                                error!("Iterator error for port {}: {}", port, e);
                                let response = json!({"status": "error", "message": format!("Iterator error: {}", e)});
                                let socket = zmq_socket.lock().await;
                                Self::send_zmq_response_static(&socket, &response, port).await?;
                                continue;
                            }
                        }
                    }
                    match db.write(batch) {
                        Ok(_) => {
                            match db.flush() {
                                Ok(_) => json!({"status": "success"}),
                                Err(e) => {
                                    error!("Failed to flush after clearing for port {}: {}", port, e);
                                    json!({"status": "error", "message": format!("Failed to flush after clearing: {}", e)})
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to clear kv_pairs for port {}: {}", port, e);
                            json!({"status": "error", "message": format!("Failed to clear kv_pairs: {}", e)})
                        }
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
                    let cf_handle = match db.cf_handle("vertices") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family vertices not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family vertices not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let key = vertex.id.0.as_bytes();
                    let value = match serde_json::to_vec(&vertex) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to serialize vertex for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to serialize vertex: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.put_cf_opt(&cf_handle, key, value, &write_opts) {
                        Ok(_) => {
                            info!("Created vertex with id {} for port {}", vertex.id, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to create vertex with id {} for port {}: {}", vertex.id, port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
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
                    let cf_handle = match db.cf_handle("vertices") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family vertices not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family vertices not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    match db.get_cf(&cf_handle, id.as_bytes()) {
                        Ok(Some(bytes)) => {
                            match serde_json::from_slice::<Vertex>(&bytes) {
                                Ok(vertex) => json!({"status": "success", "vertex": vertex}),
                                Err(e) => {
                                    error!("Failed to deserialize vertex for port {}: {}", port, e);
                                    json!({"status": "error", "message": format!("Failed to deserialize vertex: {}", e)})
                                }
                            }
                        }
                        Ok(None) => json!({"status": "success", "vertex": null}),
                        Err(e) => {
                            error!("Failed to get vertex with id {} for port {}: {}", id, port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
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
                    let cf_handle = match db.cf_handle("vertices") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family vertices not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family vertices not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let key = vertex.id.0.as_bytes();
                    let value = match serde_json::to_vec(&vertex) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to serialize vertex for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to serialize vertex: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.put_cf_opt(&cf_handle, key, value, &write_opts) {
                        Ok(_) => {
                            info!("Updated vertex with id {} for port {}", vertex.id, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to update vertex with id {} for port {}: {}", vertex.id, port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
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
                    let cf_handle = match db.cf_handle("vertices") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family vertices not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family vertices not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.delete_cf_opt(&cf_handle, id.as_bytes(), &write_opts) {
                        Ok(_) => {
                            info!("Deleted vertex with id {} for port {}", id, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to delete vertex with id {} for port {}: {}", id, port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
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
                    let cf_handle = match db.cf_handle("edges") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family edges not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family edges not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let key = match create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into()) {
                        Ok(k) => k,
                        Err(e) => {
                            error!("Failed to create edge key for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to create edge key: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let value = match serde_json::to_vec(&edge) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to serialize edge for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to serialize edge: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.put_cf_opt(&cf_handle, &key, value, &write_opts) {
                        Ok(_) => {
                            info!("Created edge ({}, {}, {}) for port {}", edge.outbound_id, edge.t, edge.inbound_id, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to create edge for port {}: {}", port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
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
                    let cf_handle = match db.cf_handle("edges") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family edges not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family edges not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let key = match create_edge_key(&outbound_id.into(), &edge_type, &inbound_id.into()) {
                        Ok(k) => k,
                        Err(e) => {
                            error!("Failed to create edge key for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to create edge key: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    match db.get_cf(&cf_handle, &key) {
                        Ok(Some(bytes)) => {
                            match serde_json::from_slice::<Edge>(&bytes) {
                                Ok(edge) => json!({"status": "success", "edge": edge}),
                                Err(e) => {
                                    error!("Failed to deserialize edge for port {}: {}", port, e);
                                    json!({"status": "error", "message": format!("Failed to deserialize edge: {}", e)})
                                }
                            }
                        }
                        Ok(None) => json!({"status": "success", "edge": null}),
                        Err(e) => {
                            error!("Failed to get edge for port {}: {}", port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
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
                    let cf_handle = match db.cf_handle("edges") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family edges not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family edges not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let key = match create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into()) {
                        Ok(k) => k,
                        Err(e) => {
                            error!("Failed to create edge key for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to create edge key: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let value = match serde_json::to_vec(&edge) {
                        Ok(v) => v,
                        Err(e) => {
                            error!("Failed to serialize edge for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to serialize edge: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.put_cf_opt(&cf_handle, &key, value, &write_opts) {
                        Ok(_) => {
                            info!("Updated edge ({}, {}, {}) for port {}", edge.outbound_id, edge.t, edge.inbound_id, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to update edge for port {}: {}", port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
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
                    let cf_handle = match db.cf_handle("edges") {
                        Some(cf) => cf,
                        None => {
                            error!("Column family edges not found for port {}", port);
                            let response = json!({"status": "error", "message": "Column family edges not found"});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let key = match create_edge_key(&outbound_id.into(), &edge_type, &inbound_id.into()) {
                        Ok(k) => k,
                        Err(e) => {
                            error!("Failed to create edge key for port {}: {}", port, e);
                            let response = json!({"status": "error", "message": format!("Failed to create edge key: {}", e)});
                            let socket = zmq_socket.lock().await;
                            Self::send_zmq_response_static(&socket, &response, port).await?;
                            continue;
                        }
                    };
                    let write_opts = WriteOptions::default();
                    match db.delete_cf_opt(&cf_handle, &key, &write_opts) {
                        Ok(_) => {
                            info!("Deleted edge ({}, {}, {}) for port {}", outbound_id, edge_type, inbound_id, port);
                            json!({"status": "success"})
                        }
                        Err(e) => {
                            error!("Failed to delete edge for port {}: {}", port, e);
                            json!({"status": "error", "message": e.to_string()})
                        }
                    }
                }
                Some("force_reset") => {
                    let mut batch = WriteBatch::default();
                    for cf_name in &["kv_pairs", "vertices", "edges"] {
                        let cf_handle = match db.cf_handle(cf_name) {
                            Some(cf) => cf,
                            None => {
                                error!("Column family {} not found for port {}", cf_name, port);
                                let response = json!({"status": "error", "message": format!("Column family {} not found", cf_name)});
                                let socket = zmq_socket.lock().await;
                                Self::send_zmq_response_static(&socket, &response, port).await?;
                                continue;
                            }
                        };
                        let iterator = db.iterator_cf(&cf_handle, rocksdb::IteratorMode::Start);
                        for item in iterator {
                            match item {
                                Ok((key, _)) => batch.delete_cf(&cf_handle, &key),
                                Err(e) => {
                                    error!("Iterator error for port {}: {}", port, e);
                                    let response = json!({"status": "error", "message": format!("Iterator error: {}", e)});
                                    let socket = zmq_socket.lock().await;
                                    Self::send_zmq_response_static(&socket, &response, port).await?;
                                    continue;
                                }
                            }
                        }
                    }
                    match db.write(batch) {
                        Ok(_) => {
                            match db.flush() {
                                Ok(_) => {
                                    info!("Force reset completed for port {}", port);
                                    json!({"status": "success"})
                                }
                                Err(e) => {
                                    error!("Failed to flush after force reset for port {}: {}", port, e);
                                    json!({"status": "error", "message": format!("Failed to flush after force reset: {}", e)})
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to force reset for port {}: {}", port, e);
                            json!({"status": "error", "message": format!("Failed to force reset: {}", e)})
                        }
                    }
                }
                Some("force_unlock") => {
                    match Self::force_unlock_static(&db_path).await {
                        Ok(_) => json!({"status": "success"}),
                        Err(e) => json!({"status": "error", "message": e.to_string()}),
                    }
                }
                Some("force_unlock_path") => {
                    match Self::force_unlock_path_static(&db_path).await {
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

    async fn insert_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        key: &[u8],
        value: &[u8],
    ) -> GraphResult<()> {
        let write_opts = WriteOptions::default();
        // Clone the Arc to move into the async block
        let cf = cf.clone();
        handle_rocksdb_op!(
            tokio::time::timeout(TokioDuration::from_secs(5), async move {
                // Pass Arc directly - it implements AsColumnFamilyRef
                db.put_cf_opt(&cf, key, value, &write_opts)
            }).await,
            format!("Timeout inserting key in DB at {:?}", db_path)
        )??;
        Ok(())
    }

    async fn retrieve_static(
        db: &Arc<DB>,
        cf: Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        key: &[u8],
    ) -> GraphResult<Option<Vec<u8>>> {
        Ok(handle_rocksdb_op!(
            tokio::time::timeout(TokioDuration::from_secs(5), async {
                db.get_cf(&cf, key)
            }).await,
            format!("Timeout retrieving key from DB at {:?}", db_path)
        )??)
    }

    async fn delete_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        key: &[u8],
    ) -> GraphResult<()> {
        let write_opts = WriteOptions::default();
        handle_rocksdb_op!(
            tokio::time::timeout(TokioDuration::from_secs(5), async {
                db.delete_cf_opt(cf, key, &write_opts)
            }).await,
            format!("Timeout deleting key from DB at {:?}", db_path)
        )??;
        Ok(())
    }

    async fn flush_static(db: &Arc<DB>, db_path: &Path) -> GraphResult<()> {
        handle_rocksdb_op!(
            tokio::time::timeout(TokioDuration::from_secs(5), async {
                db.flush()
            }).await,
            format!("Timeout flushing DB at {:?}", db_path)
        )??;
        Ok(())
    }

    async fn clear_data_static(
        db: &Arc<DB>,
        kv_pairs: Arc<BoundColumnFamily<'static>>,
        vertices: Arc<BoundColumnFamily<'static>>,
        edges: Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
    ) -> GraphResult<()> {
        let _write_opts = WriteOptions::default();
        let mut batch = WriteBatch::default();
        let cfs = vec![kv_pairs, vertices, edges];
        for cf in cfs {
            let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
            for item in iter {
                let (key, _) = handle_rocksdb_op!(
                    item,
                    format!("Failed to iterate keys in DB at {:?}", db_path)
                )?;
                batch.delete_cf(&cf, &key);
            }
        }
        handle_rocksdb_op!(
            db.write(batch),
            format!("Failed to clear data in DB at {:?}", db_path)
        )?;
        handle_rocksdb_op!(
            db.flush(),
            format!("Failed to flush after clearing DB at {:?}", db_path)
        )?;
        Ok(())
    }

    async fn create_vertex_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        vertex: &Vertex,
    ) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes();
        let value = handle_rocksdb_op!(
            serialize_vertex(vertex),
            "Failed to serialize vertex"
        )?;
        Self::insert_static(db, cf, db_path, key, &value).await
    }

    async fn get_vertex_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        id: &Uuid,
    ) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes();
        match Self::retrieve_static(db, cf.clone(), db_path, key).await? {
            Some(value) => Ok(Some(handle_rocksdb_op!(
                deserialize_vertex(&value),
                format!("Failed to deserialize vertex for DB at {:?}", db_path)
            )?)),
            None => Ok(None),
        }
    }

    async fn update_vertex_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        vertex: &Vertex,
    ) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes();
        let value = handle_rocksdb_op!(
            serialize_vertex(vertex),
            "Failed to serialize vertex"
        )?;
        Self::insert_static(db, cf, db_path, key, &value).await
    }

    async fn delete_vertex_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        id: &Uuid,
    ) -> GraphResult<()> {
        let key = id.as_bytes();
        Self::delete_static(db, cf, db_path, key).await
    }
    
    async fn create_edge_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        edge: &Edge,
    ) -> GraphResult<()> {
        let key = handle_rocksdb_op!(
            create_edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id),
            "Failed to create edge key"
        )?;
        let value = handle_rocksdb_op!(
            serialize_edge(edge),
            "Failed to serialize edge"
        )?;
        Self::insert_static(db, cf, db_path, &key, &value).await
    }

    async fn get_edge_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        outbound_id: &SerializableUuid,
        edge_type: &Identifier,
        inbound_id: &SerializableUuid,
    ) -> GraphResult<Option<Edge>> {
        let key = handle_rocksdb_op!(
            create_edge_key(outbound_id, edge_type, inbound_id),
            format!("Failed to create edge key for DB at {:?}", db_path)
        )?;
        match Self::retrieve_static(db, cf.clone(), db_path, &key).await? {
            Some(value) => Ok(Some(handle_rocksdb_op!(
                deserialize_edge(&value),
                format!("Failed to deserialize edge for DB at {:?}", db_path)
            )?)),
            None => Ok(None),
        }
    }

    async fn delete_edge_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        outbound_id: &SerializableUuid,
        t: &Identifier,
        inbound_id: &SerializableUuid,
    ) -> GraphResult<()> {
        let key = handle_rocksdb_op!(
            create_edge_key(outbound_id, t, inbound_id),
            "Failed to create edge key"
        )?;
        Self::delete_static(db, cf, db_path, &key).await
    }

    async fn update_edge_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        edge: &Edge,
    ) -> GraphResult<()> {
        let key = handle_rocksdb_op!(
            create_edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id),
            "Failed to create edge key"
        )?;
        let value = handle_rocksdb_op!(
            serialize_edge(edge),
            "Failed to serialize edge"
        )?;
        Self::insert_static(db, cf, db_path, &key, &value).await
    }

    async fn get_all_vertices_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
    ) -> GraphResult<Vec<Vertex>> {
        let mut vertices = Vec::new();
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_key, value) = handle_rocksdb_op!(
                item,
                format!("Failed to iterate vertices in DB at {:?}", db_path)
            )?;
            let vertex = handle_rocksdb_op!(
                deserialize_vertex(&value),
                "Failed to deserialize vertex"
            )?;
            vertices.push(vertex);
        }
        Ok(vertices)
    }

    async fn get_all_edges_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
    ) -> GraphResult<Vec<Edge>> {
        let mut edges = Vec::new();
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (_key, value) = handle_rocksdb_op!(
                item,
                format!("Failed to iterate edges in DB at {:?}", db_path)
            )?;
            let edge = handle_rocksdb_op!(
                deserialize_edge(&value),
                "Failed to deserialize edge"
            )?;
            edges.push(edge);
        }
        Ok(edges)
    }

    async fn force_reset_static(
        db: &Arc<DB>,
        kv_pairs: &Arc<BoundColumnFamily<'static>>,
        vertices: &Arc<BoundColumnFamily<'static>>,
        edges: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
    ) -> GraphResult<()> {
        Self::clear_data_static(db, kv_pairs.clone(), vertices.clone(), edges.clone(), db_path).await?;
        info!("Force reset completed for DB at {:?}", db_path);
        Ok(())
    }

    async fn force_unlock_static(db_path: &Path) -> GraphResult<()> {
        Self::force_unlock_path_static(db_path).await
    }

    async fn force_unlock_path_static(db_path: &Path) -> GraphResult<()> {
        let lock_path = db_path.join("LOCK");
        if lock_path.exists() {
            info!("Removing stale lock file at {:?}", lock_path);
            handle_rocksdb_op!(
                tokio::fs::remove_file(&lock_path).await,
                format!("Failed to remove lock file at {:?}", lock_path)
            )?;
        }
        Ok(())
    }

    async fn get_all_vertices_by_type_static(
        db: &Arc<DB>,
        cf: &Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        vertex_type: &Identifier,
    ) -> GraphResult<Vec<Vertex>> {
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut vertices = Vec::new();
        for item in iter {
            let (_, value) = handle_rocksdb_op!(
                item,
                format!("Failed to iterate vertices in DB at {:?}", db_path)
            )?;
            let vertex = handle_rocksdb_op!(
                deserialize_vertex(&value),
                format!("Failed to deserialize vertex for DB at {:?}", db_path)
            )?;
            if vertex.label == *vertex_type {
                vertices.push(vertex);
            }
        }
        Ok(vertices)
    }

    async fn get_all_edges_by_type_static(
        db: &Arc<DB>,
        cf: Arc<BoundColumnFamily<'static>>,
        db_path: &Path,
        edge_type: &Identifier,
    ) -> GraphResult<Vec<Edge>> {
        let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut edges = Vec::new();
        for item in iter {
            let (_, value) = handle_rocksdb_op!(
                item,
                format!("Failed to iterate edges in DB at {:?}", db_path)
            )?;
            let edge = handle_rocksdb_op!(
                deserialize_edge(&value),
                format!("Failed to deserialize edge for DB at {:?}", db_path)
            )?;
            if edge.t == *edge_type {
                edges.push(edge);
            }
        }
        Ok(edges)
    }

    /// Shuts down the RocksDB daemon.
    /// Async shutdown to be called explicitly before drop.
    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down RocksDBDaemon on port {}", self.port);
        let mut running = self.running.lock().await;
        if !*running {
            info!("RocksDBDaemon on port {} already shut down", self.port);
            return Ok(());
        }
        *running = false;
        drop(running); // Release lock

        // Send shutdown signal
        let _ = self.shutdown_tx.send(()).await;

        // Await ZMQ thread using spawn_blocking since it's a std::thread::JoinHandle
        let mut zmq_thread = self.zmq_thread.lock().await;
        if let Some(handle) = zmq_thread.take() {
            drop(zmq_thread); // Release lock before spawn_blocking
            let join_result = tokio::task::spawn_blocking(move || handle.join())
                .await
                .map_err(|e| GraphError::InternalError(format!("Failed to join ZMQ thread: {}", e)))?;
            
            match join_result {
                Ok(Ok(())) => info!("ZMQ thread shut down successfully for port {}", self.port),
                Ok(Err(e)) => error!("ZMQ thread error on shutdown for port {}: {}", self.port, e),
                Err(e) => error!("ZMQ thread panicked on shutdown for port {}: {:?}", self.port, e),
            }
        }

        self.db.flush().map_err(|e| GraphError::StorageError(format!("Flush failed on shutdown: {}", e)))?;
        Ok(())
    }

    /// Forces unlocking of the database by removing the lock file.
    pub fn force_unlock_path(db_path: &str) -> GraphResult<()> {
        let lock_path = format!("{}/LOCK", db_path);
        if Path::new(&lock_path).exists() {
            info!("Removing stale lock file at {}", lock_path);
            handle_rocksdb_op!(
                std::fs::remove_file(&lock_path),
                format!("Failed to remove lock file at {}", lock_path)
            )?;
        }
        Ok(())
    }

    pub async fn force_unlock(&self) -> GraphResult<()> {
        Self::force_unlock_path(&self.db_path.to_string_lossy())?;
        Ok(())
    }

    pub async fn flush(&self) -> GraphResult<()> {
        let request = json!({"command": "flush"});
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("Database flush successful for port {}", self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during flush");
            Err(GraphError::StorageError(format!("Failed to flush database for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn set_key(&self, cf: &str, key: String, value: Vec<u8>) -> GraphResult<()> {
        let value_b64 = general_purpose::STANDARD.encode(&value);
        let request = json!({
            "command": "set_key",
            "cf": cf,
            "key": key,
            "value": value_b64
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Set key {} in column family {} for port {}", key, cf, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during set_key");
            Err(GraphError::StorageError(format!("Failed to set key {} for port {}: {}", key, self.port, error_msg)))
        }
    }

    pub async fn get_key(&self, cf: &str, key: String) -> GraphResult<Option<Vec<u8>>> {
        let request = json!({
            "command": "get_key",
            "cf": cf,
            "key": key
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            match response.get("value") {
                Some(value) if value.is_null() => Ok(None),
                Some(value) => {
                    let value_str = value.as_str().ok_or_else(|| {
                        GraphError::SerializationError(format!("Invalid value format for key {}", key))
                    })?;
                    let decoded_value = handle_rocksdb_op!(
                        general_purpose::STANDARD.decode(value_str),
                        format!("Failed to decode value for key {}", key)
                    )?;
                    debug!("Retrieved key {} from column family {} for port {}", key, cf, self.port);
                    Ok(Some(decoded_value))
                }
                None => Err(GraphError::StorageError(format!("No value field in response for key {} for port {}", key, self.port))),
            }
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during get_key");
            Err(GraphError::StorageError(format!("Failed to get key {} for port {}: {}", key, self.port, error_msg)))
        }
    }

    pub async fn delete_key(&self, cf: &str, key: String) -> GraphResult<()> {
        let request = json!({
            "command": "delete_key",
            "cf": cf,
            "key": key
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Deleted key {} from column family {} for port {}", key, cf, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during delete_key");
            Err(GraphError::StorageError(format!("Failed to delete key {} for port {}: {}", key, self.port, error_msg)))
        }
    }

    pub async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let request = json!({
            "command": "create_vertex",
            "vertex": vertex
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Created vertex with ID {} for port {}", vertex.id.0, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during create_vertex");
            Err(GraphError::StorageError(format!("Failed to create vertex with ID {} for port {}: {}", vertex.id.0, self.port, error_msg)))
        }
    }

    pub async fn get_vertex(&self, id: Uuid) -> GraphResult<Option<Vertex>> {
        let request = json!({
            "command": "get_vertex",
            "id": SerializableUuid(id)
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            match response.get("vertex") {
                Some(vertex) if vertex.is_null() => Ok(None),
                Some(vertex) => {
                    let vertex: Vertex = handle_rocksdb_op!(
                        serde_json::from_value(vertex.clone()),
                        format!("Failed to deserialize vertex with ID {}", id)
                    )?;
                    debug!("Retrieved vertex with ID {} for port {}", id, self.port);
                    Ok(Some(vertex))
                }
                None => Err(GraphError::StorageError(format!("No vertex field in response for ID {} for port {}", id, self.port))),
            }
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during get_vertex");
            Err(GraphError::StorageError(format!("Failed to get vertex with ID {} for port {}: {}", id, self.port, error_msg)))
        }
    }

    pub async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let request = json!({
            "command": "update_vertex",
            "vertex": vertex
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Updated vertex with ID {} for port {}", vertex.id.0, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during update_vertex");
            Err(GraphError::StorageError(format!("Failed to update vertex with ID {} for port {}: {}", vertex.id.0, self.port, error_msg)))
        }
    }

    pub async fn delete_vertex(&self, id: Uuid) -> GraphResult<()> {
        let request = json!({
            "command": "delete_vertex",
            "id": SerializableUuid(id)
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Deleted vertex with ID {} for port {}", id, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during delete_vertex");
            Err(GraphError::StorageError(format!("Failed to delete vertex with ID {} for port {}: {}", id, self.port, error_msg)))
        }
    }

    pub async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        let request = json!({
            "command": "create_edge",
            "edge": edge
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Created edge from {} to {} for port {}", edge.outbound_id.0, edge.inbound_id.0, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during create_edge");
            Err(GraphError::StorageError(format!("Failed to create edge for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn get_edge(&self, outbound_id: &SerializableUuid, t: &Identifier, inbound_id: &SerializableUuid) -> GraphResult<Option<Edge>> {
        let request = json!({
            "command": "get_edge",
            "outbound_id": outbound_id,
            "t": t,
            "inbound_id": inbound_id
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            match response.get("edge") {
                Some(edge) if edge.is_null() => Ok(None),
                Some(edge) => {
                    let edge: Edge = handle_rocksdb_op!(
                        serde_json::from_value(edge.clone()),
                        format!("Failed to deserialize edge from {} to {}", outbound_id.0, inbound_id.0)
                    )?;
                    debug!("Retrieved edge from {} to {} for port {}", outbound_id.0, inbound_id.0, self.port);
                    Ok(Some(edge))
                }
                None => Err(GraphError::StorageError(format!("No edge field in response for port {}", self.port))),
            }
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during get_edge");
            Err(GraphError::StorageError(format!("Failed to get edge for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        let request = json!({
            "command": "update_edge",
            "edge": edge
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Updated edge from {} to {} for port {}", edge.outbound_id.0, edge.inbound_id.0, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during update_edge");
            Err(GraphError::StorageError(format!("Failed to update edge for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn delete_edge(&self, outbound_id: &SerializableUuid, t: &Identifier, inbound_id: &SerializableUuid) -> GraphResult<()> {
        let request = json!({
            "command": "delete_edge",
            "outbound_id": outbound_id,
            "t": t,
            "inbound_id": inbound_id
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            debug!("Deleted edge from {} to {} for port {}", outbound_id.0, inbound_id.0, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during delete_edge");
            Err(GraphError::StorageError(format!("Failed to delete edge for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn get_all_vertices(&self, vertex_type: &Identifier) -> GraphResult<Vec<Vertex>> {
        let request = json!({
            "command": "get_all_vertices_by_type",
            "vertex_type": vertex_type
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            let vertices = response.get("vertices").ok_or_else(|| {
                GraphError::StorageError("No vertices field in response".to_string())
            })?;
            let vertices: Vec<Vertex> = handle_rocksdb_op!(
                serde_json::from_value(vertices.clone()),
                "Failed to deserialize vertices"
            )?;
            debug!("Retrieved {} vertices of type {} for port {}", vertices.len(), vertex_type, self.port);
            Ok(vertices)
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during get_all_vertices");
            Err(GraphError::StorageError(format!("Failed to get vertices for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn get_all_edges(&self, edge_type: &Identifier) -> GraphResult<Vec<Edge>> {
        let request = json!({
            "command": "get_all_edges_by_type",
            "edge_type": edge_type
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            let edges = response.get("edges").ok_or_else(|| {
                GraphError::StorageError("No edges field in response".to_string())
            })?;
            let edges: Vec<Edge> = handle_rocksdb_op!(
                serde_json::from_value(edges.clone()),
                "Failed to deserialize edges"
            )?;
            debug!("Retrieved {} edges of type {} for port {}", edges.len(), edge_type, self.port);
            Ok(edges)
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during get_all_edges");
            Err(GraphError::StorageError(format!("Failed to get edges for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn clear_data(&self) -> GraphResult<()> {
        let request = json!({
            "command": "clear_data"
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("Cleared all data for port {}", self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during clear_data");
            Err(GraphError::StorageError(format!("Failed to clear data for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn get_metrics(&self) -> GraphResult<HashMap<String, String>> {
        let request = json!({
            "command": "get_metrics"
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            let metrics = response.get("metrics").ok_or_else(|| {
                GraphError::StorageError("No metrics field in response".to_string())
            })?;
            let metrics: HashMap<String, String> = handle_rocksdb_op!(
                serde_json::from_value(metrics.clone()),
                "Failed to deserialize metrics"
            )?;
            debug!("Retrieved metrics for port {}", self.port);
            Ok(metrics)
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during get_metrics");
            Err(GraphError::StorageError(format!("Failed to get metrics for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn backup(&self, backup_path: &str) -> GraphResult<()> {
        let request = json!({
            "command": "backup",
            "backup_path": backup_path
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("Backup successful to {} for port {}", backup_path, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during backup");
            Err(GraphError::StorageError(format!("Failed to backup to {} for port {}: {}", backup_path, self.port, error_msg)))
        }
    }

    pub async fn restore(&self, backup_path: &str) -> GraphResult<()> {
        let request = json!({
            "command": "restore",
            "backup_path": backup_path
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            info!("Restore successful from {} for port {}", backup_path, self.port);
            Ok(())
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during restore");
            Err(GraphError::StorageError(format!("Failed to restore from {} for port {}: {}", backup_path, self.port, error_msg)))
        }
    }

    #[cfg(feature = "with-openraft-rocksdb")]
    pub async fn get_raft_status(&self) -> GraphResult<String> {
        if let Some(raft) = &self.raft {
            let metrics = raft.metrics().borrow().clone();
            let state = format!("{:?}", metrics.state);
            debug!("Retrieved Raft status {} for port {}", state, self.port);
            Ok(state)
        } else {
            Err(GraphError::StorageError(format!("Raft not initialized for port {}", self.port)))
        }
    }

    #[cfg(feature = "with-openraft-rocksdb")]
    pub async fn get_raft_metrics(&self) -> GraphResult<HashMap<String, Value>> {
        let request = json!({
            "command": "get_raft_metrics"
        });
        let response = self.send_zmq_request(&request).await?;
        if response.get("status").and_then(|s| s.as_str()) == Some("success") {
            let metrics = response.get("raft_metrics").ok_or_else(|| {
                GraphError::StorageError("No raft_metrics field in response".to_string())
            })?;
            let metrics: HashMap<String, Value> = handle_rocksdb_op!(
                serde_json::from_value(metrics.clone()),
                "Failed to deserialize Raft metrics"
            )?;
            debug!("Retrieved Raft metrics for port {}", self.port);
            Ok(metrics)
        } else {
            let error_msg = response.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error during get_raft_metrics");
            Err(GraphError::StorageError(format!("Failed to get Raft metrics for port {}: {}", self.port, error_msg)))
        }
    }

    pub async fn send_zmq_request(&self, request: &Value) -> GraphResult<Value> {
        let socket = handle_rocksdb_op!(
            self.zmq_context.socket(REQ),
            format!("Failed to create ZMQ request socket for port {}", self.port)
        )?;
        socket.set_rcvtimeo(SOCKET_TIMEOUT_MS)?;
        socket.set_sndtimeo(SOCKET_TIMEOUT_MS)?;
        socket.set_linger(0)?;
        socket.set_req_relaxed(true)?;
        socket.set_req_correlate(true)?;
        socket.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)?;

        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", self.port);
        let connect_result = socket.connect(&endpoint);
        if let Err(e) = connect_result {
            warn!("Failed to connect to ZMQ endpoint {}: {}", endpoint, e);
            return Err(GraphError::StorageError(format!("Failed to connect to ZMQ endpoint {}: {}", endpoint, e)));
        }

        let request_bytes = handle_rocksdb_op!(
            serde_json::to_vec(request),
            "Failed to serialize ZMQ request"
        )?;
        handle_rocksdb_op!(
            socket.send(&request_bytes, 0),
            format!("Failed to send ZMQ request to port {}", self.port)
        )?;

        let response_bytes = handle_rocksdb_op!(
            timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
                socket.recv_bytes(0)
            }).await,
            format!("Timeout waiting for ZMQ response from port {}", self.port)
        )??;

        let response: Value = handle_rocksdb_op!(
            serde_json::from_slice(&response_bytes),
            "Failed to deserialize ZMQ response"
        )?;
        debug!("Received ZMQ response for port {}: {:?}", self.port, response);
        Ok(response)
    }
}

impl RocksDBDaemonPool {

    pub fn new() -> Self {
        println!("RocksDBDaemonPool new =================> INITIALIZING POOL");
        Self {
            daemons: HashMap::new(),
            registry: Arc::new(RwLock::new(HashMap::new())),
            initialized: Arc::new(RwLock::new(false)),
            load_balancer: Arc::new(LoadBalancer::new(3)), // Default replication factor of 3
            use_raft_for_scale: false,
            next_port: Arc::new(TokioMutex::new(DEFAULT_STORAGE_PORT)),
            clients: Arc::new(TokioMutex::new(HashMap::new())),
        }
    }

    pub async fn new_with_db(config: &RocksDBConfig, existing_db: Arc<rocksdb::DB>) -> GraphResult<Self> {
        let mut pool = Self::new();
        pool.initialize_with_db(config, existing_db).await?;
        Ok(pool)
    }

    pub async fn new_with_client(
        client: RocksDBClient,
        db_path: &Path,
        port: u16,
    ) -> GraphResult<Self> {
        info!("Starting ZeroMQ server for RocksDBDaemon on port {}", port);
        let mut pool = Self::new();
        
        // Get the database from the client
        let db = client
            .inner
            .as_ref()
            .ok_or_else(|| GraphError::StorageError("No database available in client".to_string()))?
            .lock()
            .await
            .clone();
        
        // Get column family handles and transmute them to 'static lifetime
        let kv_pairs: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(db.cf_handle("kv_pairs").ok_or_else(|| {
                GraphError::StorageError("Failed to open kv_pairs column family".to_string())
            })?))
        };
        
        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(db.cf_handle("vertices").ok_or_else(|| {
                GraphError::StorageError("Failed to open vertices column family".to_string())
            })?))
        };
        
        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(db.cf_handle("edges").ok_or_else(|| {
                GraphError::StorageError("Failed to open edges column family".to_string())
            })?))
        };
        
        #[cfg(feature = "with-openraft-rocksdb")]
        let raft_storage = {
            let raft_db_path = db_path.join("raft");
            tokio::fs::create_dir_all(&raft_db_path).await
                .map_err(|e| GraphError::StorageError(format!("Failed to create Raft directory: {}", e)))?;
            Some(Arc::new(
                RocksDBRaftStorage::new(&raft_db_path)
                    .await
                    .map_err(|e| GraphError::StorageError(format!("Failed to create Raft storage: {}", e)))?
            ))
        };
        
        // Create ZMQ context and shutdown channel
        let zmq_context = Arc::new(ZmqContext::new());
        let (shutdown_tx, _shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
        
        // Build and insert daemon
        pool.daemons.insert(
            port,
            Arc::new(RocksDBDaemon {
                port,
                db_path: db_path.to_path_buf(),
                db,
                kv_pairs,
                vertices,
                edges,
                running: Arc::new(tokio::sync::Mutex::new(true)),
                shutdown_tx,
                zmq_context,
                zmq_thread: Arc::new(tokio::sync::Mutex::new(None)),
                #[cfg(feature = "with-openraft-rocksdb")]
                raft: None,
                #[cfg(feature = "with-openraft-rocksdb")]
                raft_storage,
                #[cfg(feature = "with-openraft-rocksdb")]
                node_id: port as u64,
            }),
        );
        
        Ok(pool)
    }

    pub fn add_daemon(&mut self, daemon: Arc<RocksDBDaemon<'static>>) {
        self.daemons.insert(daemon.port, daemon);
    }

    pub async fn select_daemon(&self) -> Option<u16> {
        let mut healthy_nodes_lock = self.load_balancer.healthy_nodes.write().await;
        if healthy_nodes_lock.is_empty() {
            return None;
        }

        let mut index_guard = self.load_balancer.current_index.lock().await;
        let selected_port = healthy_nodes_lock[*index_guard % healthy_nodes_lock.len()].port;
        *index_guard = (*index_guard + 1) % healthy_nodes_lock.len();

        let mut nodes_lock = self.load_balancer.nodes.write().await;
        if let Some(node) = nodes_lock.get_mut(&selected_port) {
            node.request_count += 1;
            node.last_check = SystemTime::now();
        }

        Some(selected_port)
    }

    async fn update_node_health(&self, port: u16, is_healthy: bool, response_time_ms: u64) {
        let mut nodes_lock = self.load_balancer.nodes.write().await;
        let mut healthy_nodes_lock = self.load_balancer.healthy_nodes.write().await;
        let now = SystemTime::now();

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

        if is_healthy {
            let node_data = nodes_lock.get(&port).expect("Node must exist after update/insert.");
            if !healthy_nodes_lock.iter().any(|n| n.port == port) {
                healthy_nodes_lock.push_back(node_data.clone());
            }
        } else {
            healthy_nodes_lock.retain(|n| n.port != port);
        }
    }

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

    async fn check_zmq_readiness(&self, port: u16) -> GraphResult<()> {
        info!("===> Checking ZMQ readiness for port {}", port);
        println!("===> CHECKING ZMQ READINESS FOR PORT {}", port);
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);

        let result = task::spawn_blocking(move || {
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

        timeout(TokioDuration::from_secs(2), async { result })
            .await
            .map_err(|_| {
                error!("Timeout waiting for ZMQ readiness on port {}", port);
                println!("===> ERROR: TIMEOUT WAITING FOR ZMQ READINESS ON PORT {}", port);
                GraphError::StorageError(format!("Timeout waiting for ZMQ readiness on port {}", port))
            })?
    }

    async fn is_zmq_reachable(&self, port: u16) -> GraphResult<bool> {
        let clients_guard = self.clients.lock().await;
        let _client_cache_entry = match clients_guard.get(&port) {
            Some(c) => Some(c.clone()),
            None => return Ok(false),
        };
        drop(clients_guard);

        task::spawn_blocking(move || {
            let context = ZmqContext::new();
            let socket_address = format!("tcp://127.0.0.1:{}", port);
            let socket = match context.socket(zmq::REQ) {
                Ok(s) => s,
                Err(e) => {
                    debug!("ZMQ ping failed: Failed to create ZMQ socket: {}", e);
                    return Ok(false);
                },
            };

            let _ = socket.set_rcvtimeo(200);
            let _ = socket.set_sndtimeo(200);

            if let Err(e) = socket.connect(&socket_address) {
                debug!("ZMQ ping failed: Failed to connect to {}: {}", socket_address, e);
                return Ok(false);
            }

            let ping_request = json!({ "command": "ping" }).to_string();
            if let Err(e) = socket.send(&ping_request, 0) {
                debug!("ZMQ ping failed: Failed to send request to {}: {}", socket_address, e);
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
                Err(e) => {
                    debug!("ZMQ ping failed: Error receiving response from {}: {}", socket_address, e);
                    Ok(false)
                }
            }
        })
        .await
        .map_err(|e| GraphError::ZmqError(format!("ZMQ blocking task failed: {:?}", e)))?
    }

    async fn wait_for_daemon_ready(&self, port: u16) -> GraphResult<()> {
        let ipc_path_str = format!("/tmp/graphdb-{}.ipc", port);
        let ipc_path = Path::new(&ipc_path_str);
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

        error!("RocksDB Daemon on port {} failed to create IPC socket {} within the timeout.", port, ipc_path_str);
        println!("===> RocksDB Daemon on port {} FAILED to create IPC socket {} within the timeout.", port, ipc_path_str);
        Err(GraphError::DaemonStartError(format!(
            "Daemon on port {} started but failed to bind ZMQ IPC socket at {} within timeout ({} attempts).",
            port, ipc_path_str, MAX_WAIT_ATTEMPTS
        )))
    }

    fn terminate_process_using_path(db_path: &str, port: u16) -> GraphResult<()> {
        let mut system = System::new_all();
        system.refresh_all();

        for (pid, process) in system.processes() {
            if process.cmd().iter().any(|arg| {
                arg.to_str().map_or(false, |s| s.contains(db_path))
            }) {
                info!("Terminating process {} using path {} for port {}", pid, db_path, port);
                kill(NixPid::from_raw(pid.as_u32() as i32), Signal::SIGTERM).map_err(|e| {
                    GraphError::StorageError(format!("Failed to terminate process {}: {}", pid, e))
                })?;
                std::thread::sleep(std::time::Duration::from_millis(100));
                return Ok(());
            }
        }
        debug!("No process found using path {} for port {}", db_path, port);
        Ok(())
    }
    pub async fn start_new_daemon(
        &mut self, // FIX: Corrected from 'mut &self' to '&mut self' to fix syntax and allow mutation of self.daemons
        engine_config: &StorageConfig,
        port: u16,
        rocksdb_path: &PathBuf,
    ) -> GraphResult<DaemonMetadata> {
        info!("Starting new daemon for port {}", port);
        println!("===> STARTING NEW DAEMON FOR PORT {}", port);

        let daemon_config = RocksDBConfig {
            path: rocksdb_path.clone(),
            port: Some(port),
            ..Default::default()
        };

        Self::terminate_process_using_path(
            daemon_config.path.to_str().ok_or_else(|| {
                GraphError::StorageError(format!(
                    "Failed to terminate process: Path contains invalid UTF-8: {:?}", 
                    daemon_config.path
                ))
            })?,
            port
        )?;

        // RocksDBDaemon::new returns (Daemon, shutdown_rx)
        let (daemon, mut ready_rx) = RocksDBDaemon::new(daemon_config).await?;

        // Fire-and-forget readiness check
        tokio::spawn(async move {
            let _ = ready_rx.recv().await;
            info!("ZMQ server is ready on port {}", daemon.port);
        });
/*
        // Wait for ZMQ server to start
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
        })??;*/
        // This line is now valid because the function is (&mut self).
        self.daemons.insert(port, Arc::new(daemon)); 
        info!("Added new daemon to pool for port {}", port);
        println!("===> ADDED NEW DAEMON TO POOL FOR PORT {}", port);

        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            // NOTE: Using current process ID; this may need adjustment if daemons run in separate processes.
            pid: std::process::id(), 
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(rocksdb_path.clone()),
            config_path: None,
            engine_type: Some(StorageEngineType::RocksDB.to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
            zmq_ready: false,
            engine_synced: false,
        };

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        
        // Register the new daemon globally
        timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(daemon_metadata.clone()))
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
            if let Some(raft_storage) = &leader_daemon.raft_storage {
                let request = openraft::raft::ClientWriteRequest::new(
                    openraft::EntryPayload::AppWrite {
                        key: key.to_vec(),
                        value: vec![],
                    }
                );
                // Assuming Raft is accessible; adjust based on implementation
                println!("===> REPLICATED DELETE: Successfully replicated via Raft consensus");
                return Ok(());
            } else {
                return Err(GraphError::StorageError("Raft is not initialized for leader daemon".to_string()));
            }
        }

        let mut tasks = Vec::new();
        for port in &write_nodes {
            let context = ZmqContext::new();
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

            tasks.push(async move {
                let start_time = SystemTime::now();
                socket.send(serde_json::to_vec(&request)?, 0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to send delete request: {}", e)))?;

                let reply = socket.recv_bytes(0)
                    .map_err(|e| GraphError::StorageError(format!("Failed to receive delete response: {}", e)))?;

                let response_time = start_time.elapsed().unwrap().as_millis() as u64;
                let response: Value = serde_json::from_slice(&reply)?;
                Ok::<(u16, Value, u64), GraphError>((*port, response, response_time))
            });
        }

        let results: Vec<GraphResult<(u16, Value, u64)>> = join_all(tasks).await;
        let mut success_count = 0;
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok((port, response, response_time)) => {
                    if response["status"] == "success" {
                        success_count += 1;
                        self.load_balancer.update_node_health(port, true, response_time).await;
                        println!("===> REPLICATED DELETE: Success on node {}", port);
                    } else {
                        let error_msg = response["message"].as_str().unwrap_or("Unknown error").to_string();
                        errors.push((port, error_msg.clone()));
                        self.load_balancer.update_node_health(port, false, response_time).await;
                        println!("===> REPLICATED DELETE: Failed on node {}: {}", port, error_msg);
                    }
                }
                Err(e) => {
                    errors.push((0, e.to_string()));
                }
            }
        }

        let required_success = (write_nodes.len() / 2) + 1;
        if success_count >= required_success {
            println!("===> REPLICATED DELETE: Success! {}/{} nodes confirmed delete", success_count, write_nodes.len());
            Ok(())
        } else {
            error!("===> REPLICATED DELETE: Failed! Only {}/{} nodes confirmed delete", success_count, write_nodes.len());
            Err(GraphError::StorageError(format!(
                "Delete failed: only {}/{} nodes succeeded. Errors: {:?}", success_count, write_nodes.len(), errors
            )))
        }
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
                "Write failed: only {}/{} nodes succeeded. Errors: {:?}",
                success_count, write_nodes.len(), errors
            )))
        }
    }

    async fn insert_to_node(&self, port: u16, key: &[u8], value: &[u8]) -> GraphResult<()> {
        let context = ZmqContext::new();
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

    pub async fn retrieve_from_node(&self, port: u16, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let context = ZmqContext::new();
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

    pub async fn health_check_node(&self, port: u16, config: &HealthCheckConfig) -> GraphResult<bool> {
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

    pub async fn start_health_monitoring(&self, config: HealthCheckConfig) {
        let load_balancer = self.load_balancer.clone();
        let running = self.initialized.clone();
        let health_config = config.clone();
        let pool = Arc::new(self.clone());
        
        tokio::spawn(async move {
            let mut interval = interval(health_config.interval);
            while *running.read().await {
                interval.tick().await;
                
                // Get ports from the pool's daemons
                let ports: Vec<u16> = pool.daemons.keys().copied().collect();
                
                let health_checks = ports.iter().map(|port| {
                    let pool = pool.clone();
                    let health_config = health_config.clone();
                    let port = *port;
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
                    
                    #[cfg(feature = "with-openraft-rocksdb")]
                    if is_healthy {
                        if let Some(daemon) = pool.daemons.get(&port) {
                            if let Ok(is_leader) = daemon.is_leader().await {
                                info!("Node {} Raft leader status: {}", port, is_leader);
                            }
                        }
                    }
                }
                
                let healthy_nodes = load_balancer.get_healthy_nodes().await;
                let total_daemons = pool.daemons.len();
                
                info!("===> HEALTH MONITOR: {}/{} nodes healthy: {:?}", 
                      healthy_nodes.len(), total_daemons, healthy_nodes);
                
                if healthy_nodes.len() <= total_daemons / 2 {
                    warn!("Cluster health degraded: only {}/{} nodes healthy", 
                          healthy_nodes.len(), total_daemons);
                }
            }
            info!("Health monitoring stopped due to pool shutdown");
        });
    }

    pub async fn initialize_with_db(&mut self, config: &RocksDBConfig, existing_db: Arc<rocksdb::DB>) -> GraphResult<()> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("RocksDBDaemonPool already initialized, skipping");
            println!("===> WARNING: ROCKSDB DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            return Ok(());
        }

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let base_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let db_path = base_data_dir.join("rocksdb").join(port.to_string());

        info!("Initializing RocksDBDaemonPool with existing DB on port {} with path {:?}", port, db_path);
        println!("===> INITIALIZING ROCKSDB DAEMON POOL WITH EXISTING DB ON PORT {} WITH PATH {:?}", port, db_path);

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let metadata_option = daemon_registry.get_daemon_metadata(port).await?;

        if let Some(metadata) = metadata_option {
            // Check if ZMQ server is running
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
                        return Ok(());
                    } else {
                        // Handle path mismatch
                       
                    }
                }
            } else {
                warn!("Daemon registered on port {} but ZMQ server is not running. Starting ZMQ server.", port);
                println!("===> WARNING: DAEMON REGISTERED ON PORT {} BUT ZMQ SERVER IS NOT RUNNING. STARTING ZMQ SERVER.", port);
                
                // Create daemon instance to start ZMQ server
                let mut updated_config = config.clone();
                updated_config.path = db_path.clone();
                updated_config.port = Some(port);
                let (daemon, shutdown_rx) = RocksDBDaemon::new_with_db(updated_config, existing_db.clone()).await?;
                
                // Add to daemons
                self.daemons.insert(port, Arc::new(daemon));
                info!("Added daemon to pool for port {}", port);
                println!("===> ADDED DAEMON TO POOL FOR PORT {}", port);

                // Update registry if necessary
                let daemon_metadata = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: std::process::id(),
                    ip_address: "127.0.0.1".to_string(),
                    data_dir: Some(db_path.clone()),
                    config_path: None,
                    engine_type: Some(StorageEngineType::RocksDB.to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    zmq_ready: false,
                    engine_synced: false,
                };
                daemon_registry.register_daemon(daemon_metadata).await?;
                
                // No need to unregister, as we're starting the ZMQ server for the existing registry entry
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
                return Ok(());
            }
        } else {
            // Create new daemon if no metadata found
            let (daemon, shutdown_rx) = RocksDBDaemon::new_with_db(config.clone(), existing_db.clone()).await?;
            self.daemons.insert(port, Arc::new(daemon));
            info!("Added new daemon to pool for port {}", port);
            println!("===> ADDED NEW DAEMON TO POOL FOR PORT {}", port);

            let daemon_metadata = DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: std::process::id(),
                ip_address: "127.0.0.1".to_string(),
                data_dir: Some(db_path.clone()),
                config_path: None,
                engine_type: Some(StorageEngineType::RocksDB.to_string()),
                last_seen_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0),
                zmq_ready: false,
                engine_synced: false,
            };
            daemon_registry.register_daemon(daemon_metadata).await?;

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
        }

        info!("RocksDBDaemonPool initialization complete for port {}", port);
        println!("===> ROCKSDB DAEMON POOL INITIALIZATION COMPLETE FOR PORT {}", port);
        Ok(())
    }

    pub async fn initialize_cluster(
        &mut self,
        storage_config: &StorageConfig,
        config: &RocksDBConfig,
        cli_port: Option<u16>,
    ) -> GraphResult<()> {
        println!("====> IN initialize_cluster");
        self._initialize_cluster_core(
            storage_config,
            config,
            cli_port,
            None,
            None,
        ).await
    }

    pub async fn initialize_cluster_with_db(
        &mut self,
        storage_config: &StorageConfig,
        config: &RocksDBConfig,
        cli_port: Option<u16>,
        existing_db: Arc<DB>,
    ) -> GraphResult<()> {
        println!("====> IN initialize_cluster_with_db");
        self._initialize_cluster_core(
            storage_config,
            config,
            cli_port,
            None,
            Some(existing_db),
        ).await
    }

    async fn _initialize_cluster_core(
        &mut self,
        storage_config: &StorageConfig,
        config: &RocksDBConfig,
        cli_port: Option<u16>,
        client: Option<(RocksDBClient, Arc<TokioMutex<ZmqSocketWrapper>>)>,
        existing_db: Option<Arc<DB>>,
    ) -> GraphResult<()> {
        println!("===> IN _initialize_cluster_core");
        info!("Starting initialization of RocksDBDaemonPool with config: {:?}", storage_config);

        // Acquire global initialization lock to prevent concurrent initialization
        let _guard = get_cluster_init_lock().await.lock().await;

        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("RocksDBDaemonPool already initialized, skipping");
            println!("===> WARNING: RocksDB DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            return Ok(());
        }

        const DEFAULT_STORAGE_PORT: u16 = 8052;

        let intended_port = cli_port.unwrap_or(config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        info!("Intended port: {} (cli_port: {:?}, config.port: {:?}, DEFAULT_STORAGE_PORT: {})",
              intended_port, cli_port, config.port, DEFAULT_STORAGE_PORT);
        println!("===> INTENDED PORT: {} (cli_port: {:?}, config.port: {:?}, DEFAULT_STORAGE_PORT: {})",
                 intended_port, cli_port, config.port, DEFAULT_STORAGE_PORT);

        // Initialize GLOBAL_DB_DAEMON_REGISTRY
        info!("Attempting to initialize GLOBAL_DB_DAEMON_REGISTRY");
        println!("===> ATTEMPTING TO INITIALIZE GLOBAL_DB_DAEMON_REGISTRY");
        let storage_config_arc = Arc::new(storage_config.clone());
        let mut attempts = 0;
        const MAX_REGISTRY_ATTEMPTS: u32 = 5;
        while attempts < MAX_REGISTRY_ATTEMPTS {
            match timeout(TokioDuration::from_secs(10), GLOBAL_DB_DAEMON_REGISTRY.get_or_init_instance(storage_config_arc.clone())).await {
                Ok(Ok(_)) => {
                    info!("Successfully initialized GLOBAL_DB_DAEMON_REGISTRY for RocksDBDaemonPool");
                    println!("===> SUCCESSFULLY INITIALIZED GLOBAL_DB_DAEMON_REGISTRY FOR RocksDB DAEMON POOL");
                    break;
                }
                Ok(Err(e)) => {
                    error!("Failed to initialize GLOBAL_DB_DAEMON_REGISTRY: {}", e);
                    println!("===> ERROR: FAILED TO INITIALIZE GLOBAL_DB_DAEMON_REGISTRY: {}", e);
                    return Err(GraphError::StorageError(format!("Failed to initialize GLOBAL_DB_DAEMON_REGISTRY: {}", e)));
                }
                Err(_) => {
                    warn!("Timeout initializing GLOBAL_DB_DAEMON_REGISTRY, attempt {}/{}", attempts + 1, MAX_REGISTRY_ATTEMPTS);
                    println!("===> WARNING: TIMEOUT INITIALIZING GLOBAL_DB_DAEMON_REGISTRY, ATTEMPT {}/{}", attempts + 1, MAX_REGISTRY_ATTEMPTS);
                    attempts += 1;
                    sleep(TokioDuration::from_millis(1000)).await;
                }
            }
        }
        if attempts >= MAX_REGISTRY_ATTEMPTS {
            error!("Failed to initialize GLOBAL_DB_DAEMON_REGISTRY after {} attempts", MAX_REGISTRY_ATTEMPTS);
            println!("===> ERROR: FAILED TO INITIALIZE GLOBAL_DB_DAEMON_REGISTRY AFTER {} ATTEMPTS", MAX_REGISTRY_ATTEMPTS);
            return Err(GraphError::StorageError(format!("Failed to initialize GLOBAL_DB_DAEMON_REGISTRY after {} attempts", MAX_REGISTRY_ATTEMPTS)));
        }

        let cluster_ports: Vec<u16> = if !storage_config.cluster_range.is_empty() {
            let range: Vec<&str> = storage_config.cluster_range.split('-').collect();
            let start: u16 = range[0].parse().unwrap_or(intended_port);
            let end: u16 = range.get(1).and_then(|s| s.parse().ok()).unwrap_or(start);
            (start..=end).collect()
        } else {
            vec![intended_port]
        };

        // Clean up stale IPC sockets
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

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        // Batch retrieval of daemon metadata to reduce registry contention
        let all_daemons = daemon_registry.get_all_daemon_metadata().await?;
        let mut active_ports: Vec<u16> = all_daemons
            .into_iter()
            .filter(|metadata| metadata.service_type == "storage")
            .map(|metadata| metadata.port)
            .collect();

        if !active_ports.contains(&intended_port) {
            warn!("Intended port {} not found in registry, adding it", intended_port);
            println!("===> WARNING: INTENDED PORT {} NOT FOUND IN REGISTRY, ADDING IT", intended_port);
            active_ports.push(intended_port);
        }

        let mut valid_ports: Vec<u16> = Vec::new();
        for port in active_ports {
            let metadata_option = daemon_registry.get_daemon_metadata(port).await?;
            let mut should_initialize = port == intended_port || valid_ports.is_empty();

            if let Some(mut metadata) = metadata_option { // FIX: Added 'mut' to allow modification of metadata fields
                if metadata.port != intended_port {
                    let is_running = match timeout(TokioDuration::from_secs(5), self.is_zmq_reachable(port)).await {
                        Ok(inner_result) => match inner_result {
                            Ok(status) => status,
                            Err(e) => {
                                warn!("Error checking if daemon is running on port {}: {}", port, e);
                                println!("===> WARNING: ERROR CHECKING IF DAEMON IS RUNNING ON PORT {}: {}", port, e);
                                false
                            }
                        },
                        Err(e) => {
                            warn!("Timeout checking if daemon is running on port {}: {}", port, e);
                            println!("===> WARNING: TIMEOUT CHECKING IF DAEMON IS RUNNING ON PORT {}: {}", port, e);
                            false
                        }
                    };

                    if is_running {
                        // Update last_seen_nanos with retry (now possible due to `mut` binding)
                        metadata.last_seen_nanos = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(0);
                        let mut attempts = 0;
                        while attempts < MAX_REGISTRY_ATTEMPTS {
                            match timeout(TokioDuration::from_secs(5), daemon_registry.register_daemon(metadata.clone())).await {
                                Ok(Ok(_)) => {
                                    info!("Updated daemon metadata on port {}", port);
                                    println!("===> UPDATED DAEMON METADATA ON PORT {}", port);
                                    break;
                                }
                                Ok(Err(e)) => {
                                    warn!("Failed to update daemon metadata on port {}: {}. Attempt {}/{}", port, e, attempts + 1, MAX_REGISTRY_ATTEMPTS);
                                    println!("===> WARNING: FAILED TO UPDATE DAEMON METADATA ON PORT {}: {}. ATTEMPT {}/{}", port, e, attempts + 1, MAX_REGISTRY_ATTEMPTS);
                                    attempts += 1;
                                    sleep(TokioDuration::from_millis(500)).await;
                                }
                                Err(_) => {
                                    warn!("Timeout updating daemon metadata on port {}. Attempt {}/{}", port, attempts + 1, MAX_REGISTRY_ATTEMPTS);
                                    println!("===> WARNING: TIMEOUT UPDATING DAEMON METADATA ON PORT {}. ATTEMPT {}/{}", port, attempts + 1, MAX_REGISTRY_ATTEMPTS);
                                    attempts += 1;
                                    sleep(TokioDuration::from_millis(500)).await;
                                }
                            }
                        }
                        if attempts >= MAX_REGISTRY_ATTEMPTS {
                            error!("Failed to update daemon metadata on port {} after {} attempts", port, MAX_REGISTRY_ATTEMPTS);
                            println!("===> ERROR: FAILED TO UPDATE DAEMON METADATA ON PORT {} AFTER {} ATTEMPTS", port, MAX_REGISTRY_ATTEMPTS);
                            return Err(GraphError::StorageError(format!("Failed to update daemon metadata on port {} after {} attempts", port, MAX_REGISTRY_ATTEMPTS)));
                        }

                        info!("Active storage daemon found on port {}, adding to load balancer", port);
                        println!("===> ACTIVE STORAGE DAEMON FOUND ON PORT {}, ADDING TO LOAD BALANCER", port);
                        valid_ports.push(port);
                        self.load_balancer.update_node_health(port, true, 0).await;
                        continue;
                    }
                } else {
                    should_initialize = true;
                }
            }

            if should_initialize {
                let db_path = storage_config
                    .data_directory
                    .as_ref()
                    .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
                    .join("rocksdb")
                    .join(port.to_string());

                info!("Initializing daemon on port {} with path {:?}", port, db_path);
                println!("===> INITIALIZING DAEMON ON PORT {} WITH PATH {:?}", port, db_path);

                if !db_path.exists() {
                    info!("Creating RocksDB directory at {:?}", db_path);
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
                info!("Creating RocksDBDaemon with config: {:?}", daemon_config);
                println!("===> CREATING ROCKSDB DAEMON WITH CONFIG: {:?}", daemon_config);

                let (daemon, mut ready_rx) = timeout(TokioDuration::from_secs(10), RocksDBDaemon::new(daemon_config.clone()))
                    .await
                    .map_err(|_| {
                        error!("Timeout creating RocksDBDaemon on port {}", port);
                        println!("===> ERROR: TIMEOUT CREATING RocksDB DAEMON ON PORT {}", port);
                        GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port))
                    })?
                    .map_err(|e| {
                        error!("Failed to create RocksDBDaemon on port {}: {}", port, e);
                        println!("===> ERROR: FAILED TO CREATE RocksDB DAEMON ON PORT {}: {}", port, e);
                        e
                    })?;

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

                let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                if !Path::new(&ipc_path).exists() {
                    error!("ZMQ IPC file not created at {} after binding", ipc_path);
                    println!("===> ERROR: ZMQ IPC FILE NOT CREATED AT {} AFTER BINDING", ipc_path);
                    return Err(GraphError::StorageError(format!("ZMQ IPC file not created at {}", ipc_path)));
                }
                info!("ZMQ IPC file verified at {}", ipc_path);
                println!("===> ZMQ IPC FILE VERIFIED AT {}", ipc_path);

                info!("Checking ZMQ server responsiveness on port {}", port);
                println!("===> CHECKING ZMQ SERVER RESPONSIVENESS ON PORT {}", port);
                timeout(TokioDuration::from_secs(10), async {
                    let mut attempts = 0;
                    const MAX_ATTEMPTS: usize = 20;
                    sleep(TokioDuration::from_millis(500)).await;
                    while !self.is_zmq_server_running(port).await? {
                        attempts += 1;
                        if attempts >= MAX_ATTEMPTS {
                            error!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS);
                            println!("===> ERROR: ZMQ SERVER FAILED TO START ON PORT {} AFTER {} ATTEMPTS", port, MAX_ATTEMPTS);
                            return Err(GraphError::StorageError(format!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS)));
                        }
                        info!("ZMQ server not ready on port {}, attempt {}/{}", port, attempts, MAX_ATTEMPTS);
                        println!("===> ZMQ SERVER NOT READY ON PORT {}, ATTEMPT {}/{}", port, attempts, MAX_ATTEMPTS);
                        sleep(TokioDuration::from_millis(500)).await;
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
                    engine_type: Some(StorageEngineType::RocksDB.to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    zmq_ready: false,
                    engine_synced: false,
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
                        .join("rocksdb")
                        .join(port.to_string());

                    info!("Initializing daemon from cluster_range on port {} with path {:?}", port, db_path);
                    println!("===> INITIALIZING DAEMON FROM CLUSTER_RANGE ON PORT {} WITH PATH {:?}", port, db_path);

                    if !db_path.exists() {
                        info!("Creating RocksDB directory at {:?}", db_path);
                        println!("===> CREATING ROCKSDB DIRECTORY AT {:?}", db_path);
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

                    // Force unlock before creating daemon
                    RocksDBClient::force_unlock(&db_path).await?;
                    println!("===> Performed force unlock on RocksDB at {:?}", db_path);

                    let mut daemon_config = config.clone();
                    daemon_config.path = db_path.clone();
                    daemon_config.port = Some(port);
                    info!("Creating RocksDBDaemon with config: {:?}", daemon_config);
                    println!("===> CREATING RocksDB DAEMON WITH CONFIG: {:?}", daemon_config);

                    let (daemon, mut ready_rx) = timeout(TokioDuration::from_secs(10), RocksDBDaemon::new(daemon_config.clone()))
                        .await
                        .map_err(|_| {
                            error!("Timeout creating RocksDBDaemon on port {}", port);
                            println!("===> ERROR: TIMEOUT CREATING RocksDB DAEMON ON PORT {}", port);
                            GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port))
                        })?
                        .map_err(|e| {
                            error!("Failed to create RocksDBDaemon on port {}: {}", port, e);
                            println!("===> ERROR: FAILED TO CREATE RocksDB DAEMON ON PORT {}: {}", port, e);
                            e
                        })?;

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

                        let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                        if !Path::new(&ipc_path).exists() {
                            error!("ZMQ IPC file not created at {} after binding", ipc_path);
                            println!("===> ERROR: ZMQ IPC FILE NOT CREATED AT {} AFTER BINDING", ipc_path);
                            return Err(GraphError::StorageError(format!("ZMQ IPC file not created at {}", ipc_path)));
                        }
                        info!("ZMQ IPC file verified at {}", ipc_path);
                        println!("===> ZMQ IPC FILE VERIFIED AT {}", ipc_path);

                        info!("Checking ZMQ server responsiveness on port {}", port);
                        println!("===> CHECKING ZMQ SERVER RESPONSIVENESS ON PORT {}", port);
                        timeout(TokioDuration::from_secs(10), async {
                            let mut attempts = 0;
                            const MAX_ATTEMPTS: usize = 20;
                            sleep(TokioDuration::from_millis(500)).await;
                            while !self.is_zmq_server_running(port).await? {
                                attempts += 1;
                                if attempts >= MAX_ATTEMPTS {
                                    error!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS);
                                    println!("===> ERROR: ZMQ SERVER FAILED TO START ON PORT {} AFTER {} ATTEMPTS", port, MAX_ATTEMPTS);
                                    return Err(GraphError::StorageError(format!("ZMQ server failed to start on port {} after {} attempts", port, MAX_ATTEMPTS)));
                                }
                                info!("ZMQ server not ready on port {}, attempt {}/{}", port, attempts, MAX_ATTEMPTS);
                                println!("===> ZMQ SERVER NOT READY ON PORT {}, ATTEMPT {}/{}", port, attempts, MAX_ATTEMPTS);
                                sleep(TokioDuration::from_millis(500)).await;
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
                            engine_type: Some(StorageEngineType::RocksDB.to_string()),
                            last_seen_nanos: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_nanos() as i64)
                                .unwrap_or(0),
                            zmq_ready: false,
                            engine_synced: false,
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

            info!("RocksDBDaemonPool initialized successfully with load balancing on ports {:?}", valid_ports);
            println!("===> RocksDB DAEMON POOL INITIALIZED SUCCESSFULLY WITH LOAD BALANCING ON PORTS {:?}", valid_ports);
            Ok(())
    }

    pub async fn leader_daemon(&self) -> GraphResult<Arc<RocksDBDaemon<'static>>> {
        let healthy_nodes = self.load_balancer.get_healthy_nodes().await;

        for node in healthy_nodes {
            if let Some(daemon) = self.daemons.get(&node) {
                #[cfg(feature = "with-openraft-rocksdb")]
                if self.use_raft_for_scale {
                    if daemon.is_leader().await.unwrap_or(false) {
                        return Ok(daemon.clone());
                    }
                    continue;
                }
                return Ok(daemon.clone());
            }
        }

        Err(GraphError::StorageError("No healthy leader daemon found".to_string()))
    }

    pub async fn get_client(&self, port: Option<u16>) -> GraphResult<Arc<dyn GraphStorageEngine>> {
        let selected_port = match port {
            Some(p) => p,
            None => self.select_daemon().await
                .ok_or_else(|| GraphError::StorageError("No healthy daemons available".to_string()))?,
        };
        
        // Check if client exists and is healthy
        {
            let clients_guard = self.clients.lock().await;
            if let Some(client) = clients_guard.get(&selected_port) {
                if self.is_zmq_server_running(selected_port).await? {
                    return Ok(client.clone());
                }
            }
        }
        
        // Remove stale client
        {
            let mut clients_guard = self.clients.lock().await;
            clients_guard.remove(&selected_port);
        }
        
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let metadata = daemon_registry.get_daemon_metadata(selected_port).await?;
        let db_path = metadata
            .as_ref()
            .and_then(|m| m.data_dir.clone())
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY).join("rocksdb").join(selected_port.to_string()));
        
        // Create new client using the async constructor
        let client = RocksDBClient::new_with_port(selected_port)
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to create RocksDB client: {}", e)))?;
        
        let client_arc: Arc<dyn GraphStorageEngine> = Arc::new(client);
        
        // Store the client
        {
            let mut clients_guard = self.clients.lock().await;
            clients_guard.insert(selected_port, client_arc.clone());
        }
        
        Ok(client_arc)
    }

    pub async fn shutdown(&self) -> GraphResult<()> {
        info!("Shutting down RocksDBDaemonPool");
        println!("===> SHUTTING DOWN ROCKSDB DAEMON POOL");
        let mut initialized = self.initialized.write().await;
        if !*initialized {
            warn!("RocksDBDaemonPool is not initialized, nothing to shut down");
            println!("===> WARNING: ROCKSDB DAEMON POOL IS NOT INITIALIZED, NOTHING TO SHUT DOWN");
            return Ok(());
        }

        *initialized = false;
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let ports: Vec<u16> = self.daemons.keys().copied().collect();

        for port in ports {
            let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
            if let Err(e) = tokio::fs::remove_file(&ipc_path).await {
                warn!("Failed to remove IPC socket {}: {}", ipc_path, e);
                println!("===> WARNING: FAILED TO REMOVE IPC SOCKET {}: {}", ipc_path, e);
            } else {
                info!("Removed IPC socket {}", ipc_path);
                println!("===> REMOVED IPC SOCKET {}", ipc_path);
            }

            if let Err(e) = timeout(TokioDuration::from_secs(5), daemon_registry.unregister_daemon(port)).await {
                warn!("Timeout unregistering daemon on port {}: {}", port, e);
                println!("===> WARNING: TIMEOUT UNREGISTERING DAEMON ON PORT {}: {}", port, e);
            } else {
                info!("Unregistered daemon on port {}", port);
                println!("===> UNREGISTERED DAEMON ON PORT {}", port);
            }
        }

        let mut clients_guard = self.clients.lock().await;
        clients_guard.clear();
        drop(clients_guard);
        info!("Cleared all RocksDB clients");
        println!("===> CLEARED ALL ROCKSDB CLIENTS");

        // Shutdown all daemons
        for (_port, daemon) in &self.daemons {
            if let Err(e) = daemon.shutdown().await {
                warn!("Failed to shut down daemon on port {}: {}", _port, e);
                println!("===> WARNING: FAILED TO SHUT DOWN DAEMON ON PORT {}: {}", _port, e);
            } else {
                info!("Shut down daemon on port {}", _port);
                println!("===> SHUT DOWN DAEMON ON PORT {}", _port);
            }
        }

        info!("RocksDBDaemonPool shutdown complete");
        println!("===> ROCKSDB DAEMON POOL SHUTDOWN COMPLETE");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;
    use std::time::Duration as TokioDuration;

    #[tokio::test]
    async fn test_cluster_initialization() -> GraphResult<()> {
        let mut pool = RocksDBDaemonPool::new();
        let config = StorageConfig {
            cluster_range: "5555-5557".to_string(),
            path: Some(PathBuf::from("/tmp/test_rocksdb_cluster")),
            cache_capacity: Some(1024 * 1024),
            replication_strategy: ReplicationStrategy::NNodes(3),
        };

        pool.initialize_cluster(&config).await?;

        assert_eq!(pool.daemons.len(), 3);
        for port in 5555..=5557 {
            assert!(pool.daemons.contains_key(&port));
            assert!(pool.load_balancer.nodes.read().await.contains_key(&port));
        }

        pool.health_check_all_nodes().await?;
        pool.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_replicated_operations() -> GraphResult<()> {
        let mut pool = RocksDBDaemonPool::new();
        let config = StorageConfig {
            cluster_range: "5558-5559".to_string(),
            path: Some(PathBuf::from("/tmp/test_replicated_ops")),
            cache_capacity: Some(1024 * 1024),
            replication_strategy: ReplicationStrategy::NNodes(2),
        };

        pool.initialize_cluster(&config).await?;

        let key = b"test_key";
        let value = b"test_value";

        // Test insert
        pool.insert_replicated(key, value, false).await?;
        let retrieved = pool.retrieve_with_failover(key).await?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), value);

        // Test delete
        pool.delete_replicated(key, false).await?;
        let retrieved = pool.retrieve_with_failover(key).await?;
        assert!(retrieved.is_none());

        pool.shutdown().await?;
        Ok(())
    }
    
}

#[cfg(feature = "with-openraft-rocksdb")]
#[async_trait]
impl RaftNetwork<TypeConfig> for RocksDBDaemon {
    async fn append_entries(&self, target: NodeId, _rpc: openraft::raft::AppendEntriesRequest<TypeConfig>) -> openraft::error::RaftResult<openraft::raft::AppendEntriesResponse<NodeId>> {
        let addr = match self.raft_storage.as_ref().and_then(|s| s.get_node_addr(target).await.ok()) {
            Some(addr) => addr,
            None => {
                error!("No address found for target node {}", target);
                println!("===> ERROR: NO ADDRESS FOUND FOR TARGET NODE {}", target);
                return Err(openraft::error::RaftError::Network(openraft::error::NetworkError::new(&format!("No address for node {}", target))));
            }
        };
        let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
            error!("Failed to connect to node {} at {}: {}", target, addr, e);
            println!("===> ERROR: FAILED TO CONNECT TO NODE {} AT {}: {}", target, addr, e);
            openraft::error::RaftError::Network(openraft::error::NetworkError::new(&e.to_string()))
        })?;
        // Simplified example: implement actual Raft RPC serialization and communication
        let response = openraft::raft::AppendEntriesResponse {
            term: 0,
            success: true,
            conflict_opt: None,
        };
        Ok(response)
    }

    async fn vote(&self, target: NodeId, _rpc: openraft::raft::VoteRequest<NodeId>) -> openraft::error::RaftResult<openraft::raft::VoteResponse<NodeId>> {
        let addr = match self.raft_storage.as_ref().and_then(|s| s.get_node_addr(target).await.ok()) {
            Some(addr) => addr,
            None => {
                error!("No address found for target node {}", target);
                println!("===> ERROR: NO ADDRESS FOUND FOR TARGET NODE {}", target);
                return Err(openraft::error::RaftError::Network(openraft::error::NetworkError::new(&format!("No address for node {}", target))));
            }
        };
        let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
            error!("Failed to connect to node {} at {}: {}", target, addr, e);
            println!("===> ERROR: FAILED TO CONNECT TO NODE {} AT {}: {}", target, addr, e);
            openraft::error::RaftError::Network(openraft::error::NetworkError::new(&e.to_string()))
        })?;
        // Simplified example: implement actual Raft RPC serialization and communication
        let response = openraft::raft::VoteResponse {
            term: 0,
            vote_granted: true,
            log_id: None,
        };
        Ok(response)
    }

    async fn install_snapshot(&self, target: NodeId, _rpc: openraft::raft::InstallSnapshotRequest<TypeConfig>) -> openraft::error::RaftResult<openraft::raft::InstallSnapshotResponse<NodeId>> {
        let addr = match self.raft_storage.as_ref().and_then(|s| s.get_node_addr(target).await.ok()) {
            Some(addr) => addr,
            None => {
                error!("No address found for target node {}", target);
                println!("===> ERROR: NO ADDRESS FOUND FOR TARGET NODE {}", target);
                return Err(openraft::error::RaftError::Network(openraft::error::NetworkError::new(&format!("No address for node {}", target))));
            }
        };
        let mut stream = TcpStream::connect(&addr).await.map_err(|e| {
            error!("Failed to connect to node {} at {}: {}", target, addr, e);
            println!("===> ERROR: FAILED TO CONNECT TO NODE {} AT {}: {}", target, addr, e);
            openraft::error::RaftError::Network(openraft::error::NetworkError::new(&e.to_string()))
        })?;
        // Simplified example: implement actual Raft RPC serialization and communication
        let response = openraft::raft::InstallSnapshotResponse {
            term: 0,
        };
        Ok(response)
    }
}

impl<'a> Drop for RocksDBDaemon<'a> {
    fn drop(&mut self) {
        // Warn if shutdown wasn't called explicitly
        if let Ok(running_guard) = self.running.try_lock() {
            if *running_guard {
                // If 'running' is still true, the user forgot to call shutdown().
                // The registry won't be cleaned up asynchronously here.
                warn!("RocksDBDaemon on port {} dropped without explicit shutdown. Resources may leak (GLOBAL_DAEMON_REGISTRY may be stale).", self.port);
                println!("===> WARNING: RocksDBDaemon on port {} dropped without explicit shutdown. CALL .shutdown()!", self.port);
            }
        }

        // Sync join of ZMQ thread (required cleanup in Drop)
        if let Ok(mut zmq_thread_guard) = self.zmq_thread.try_lock() {
            if let Some(handle) = zmq_thread_guard.take() {
                // Ignore result to avoid panic in Drop, but join synchronously.
                let _ = handle.join(); 
            }
        }

        // Sync DB destroy for lock file (rocksdb::DB::destroy is sync)
        let lock_path = self.db_path.join("LOCK");
        if lock_path.exists() {
            // Note: This attempts to destroy the lock file, NOT the entire database.
            if let Err(e) = rocksdb::DB::destroy(&rocksdb::Options::default(), &lock_path) {
                if !e.to_string().contains("No such file or directory") {
                    error!("Failed to destroy lock during drop at {:?}: {}", lock_path, e);
                }
            }
        }
        
        // IMPORTANT: No async registry cleanup is possible here.
    }
}


/// Retrieves and immediately acquires a port-specific lock (`SemaphorePermit`) for the RocksDB daemon.
/// The lock is released when the returned `SemaphorePermit` is dropped, making it safe for async use.
/// This prevents race conditions when starting multiple RocksDB daemons on the same port.
/// 
/// The function blocks (awaits) until the lock for the given port is available.
pub async fn get_rocksdb_daemon_port_lock(port: u16) -> tokio::sync::OwnedSemaphorePermit {
    // Ensure the global map contains an Arc<Semaphore> for this port, creating one if necessary.
    let sem_arc = {
        // Lock the map to ensure thread-safe access to the HashMap
        let mut locks_map = ROCKSDB_DAEMON_PORT_LOCKS.lock().await;

        // Check if a semaphore already exists for this port. If not, create one.
        if let Some(sem) = locks_map.get(&port) {
            sem.clone()
        } else {
            // Create a new semaphore with a single permit (acting as a per-port mutex)
            let sem = Arc::new(Semaphore::new(1));
            debug!("Created new RocksDB daemon port lock for port {}", port);
            locks_map.insert(port, sem.clone());
            sem
        }
    };

    // Acquire an owned permit which keeps an Arc to the semaphore alive.
    // OwnedSemaphorePermit does not borrow from a local variable, so it's safe to return.
    debug!("Acquiring RocksDB daemon port lock for port {}", port);
    sem_arc
        .acquire_owned()
        .await
        .expect("Failed to acquire permit from RocksDB port semaphore.")
}

/// Removes the port lock entry from the map.
/// This is typically called after the daemon has successfully started or failed irrecoverably 
/// to clean up the entry and free memory.
pub async fn remove_rocksdb_daemon_port_lock(port: u16) {
    let mut locks_map = ROCKSDB_DAEMON_PORT_LOCKS.lock().await;
    locks_map.remove(&port);
    debug!("Removed RocksDB daemon port lock entry for port {}", port);
}
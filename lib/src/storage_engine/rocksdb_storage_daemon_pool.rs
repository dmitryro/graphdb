use std::collections::{HashMap, HashSet};
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{ Arc, LazyLock };
use std::io::{Cursor, Read, Write};
use std::process::Command;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use quick_cache::sync::Cache;
use rocksdb::{DB, Env, Options, WriteBatch, WriteOptions, ColumnFamily, BoundColumnFamily, ColumnFamilyDescriptor, 
              DBCompressionType, DBCompactionStyle, Cache as RocksDBCache, BlockBasedOptions,};
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
    STORAGE_PID_FILE_NAME_PREFIX, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    daemon_api_storage_engine_type_to_string,
}; 
use crate::storage_engine::rocksdb_client::{ZmqSocketWrapper};
use crate::storage_engine::storage_engine::{GraphStorageEngine, ApplicationStateMachine as RocksDBStateMachine};
pub use crate::storage_engine::rocksdb_wal_manager::{ RocksDBWalManager, RocksDBWalOperation };
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
                                     get_ipc_endpoint, force_cleanup_engine_lock,
                                     check_process_status_by_port };
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
// Add these imports at the top of the file
use bincode::{Encode, Decode};
use bincode::config::standard;

// The placeholder logic for BoundColumnFamily using raw pointers is removed 
// because it causes E0423 due to the private fields of the tuple struct.
// We are now using a temporary, safe DB instance to get a valid handle.

#[cfg(feature = "with-rocksdb")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode, ServerState},
    tokio::io::{AsyncWriteExt, AsyncSeekExt, AsyncBufReadExt, BufReader, SeekFrom, ErrorKind},
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

lazy_static::lazy_static! {
    /// canonical data directory to list of ports that belong to it
    static ref CANONICAL_DB_MAP: RwLock<HashMap<PathBuf, Vec<u16>>> = RwLock::new(HashMap::new());
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

// ---------------------------------------------------------------------------
// Shared WAL – one file, many readers, one writer (leader)
// ---------------------------------------------------------------------------
static SHARED_WAL_DIR: Lazy<PathBuf> = Lazy::new(|| {
    PathBuf::from(format!(
        "{}/{}/shared_wal",
        DEFAULT_DATA_DIRECTORY,
        StorageEngineType::RocksDB.to_string().to_lowercase()
    ))
});

/// **CORRECT**: Full path to the WAL **file**
static SHARED_WAL_PATH: Lazy<PathBuf> = Lazy::new(|| {
    SHARED_WAL_DIR.join("log.wal")  // ← Only once!
});

/// Helper – returns the **file path**
fn get_shared_wal_file_path() -> PathBuf {
    SHARED_WAL_PATH.clone()
}

/// Leader election – first daemon that creates the lock wins.
// ──────────────────────────────────────────────────────────────
// 1. become_wal_leader – uses config.port, no env, no ZMQ
// ──────────────────────────────────────────────────────────────
pub async fn become_wal_leader(canonical: &Path, my_port: u16) -> GraphResult<bool> {
    let leader_file = canonical.join("wal_leader.info");
    let lock_file = canonical.join("wal_leader.lock");

    loop {
        match tokio_fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_file)
            .await
        {
            Ok(mut file) => {
                let info = format!(
                    "leader_port={}\npid={}\nts={}\n",
                    my_port,
                    std::process::id(),
                    chrono::Utc::now().timestamp()
                );
                if file.write_all(info.as_bytes()).await.is_err() {
                    let _ = tokio_fs::remove_file(&lock_file).await;
                    return Ok(false);
                }
                let _ = file.sync_all().await;
                info!("Became WAL leader (port {}) for cluster {:?}", my_port, canonical);
                return Ok(true);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                match tokio_fs::read_to_string(&leader_file).await {
                    Ok(content) => {
                        let leader_port = content
                            .lines()
                            .find_map(|l| l.strip_prefix("leader_port=").map(|s| s.trim()))
                            .and_then(|s| s.parse::<u16>().ok())
                            .unwrap_or(0);
                        info!("WAL leader is on port {} (we are follower)", leader_port);
                        return Ok(false);
                    }
                    Err(_) => {
                        warn!("Leader lock exists but no leader.info — forcing promotion");
                        let _ = tokio_fs::remove_file(&lock_file).await;
                        // Continue loop to retry
                        continue;
                    }
                }
            }
            Err(e) => return Err(GraphError::Io(e.to_string())),
        }
    }
}

// ──────────────────────────────────────────────────────────────
// 2. get_leader_port – read-only, safe
// ──────────────────────────────────────────────────────────────
pub async fn get_leader_port(canonical: &Path) -> GraphResult<Option<u16>> {
    let leader_file = canonical.join("wal_leader.info");
    match tokio_fs::read_to_string(&leader_file).await {
        Ok(content) => {
            let port = content
                .lines()
                .find_map(|l| l.strip_prefix("leader_port=").map(|s| s.trim()))
                .and_then(|s| s.parse::<u16>().ok());
            Ok(port)
        }
        Err(_) => Ok(None),
    }
}

/// Append a serialized operation to the shared WAL (leader only).
async fn append_wal(op: &RocksDBWalOperation) -> GraphResult<u64> {
    let data = bincode::encode_to_vec(op, bincode::config::standard())
        .map_err(|e| GraphError::StorageError(e.to_string()))?;
    let mut file = tokio_fs::OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(&*SHARED_WAL_PATH)
        .await
        .map_err(|e| GraphError::Io(e.to_string()))?;
    let offset = file.metadata().await?.len();
    file.write_all(&data).await?;
    file.write_all(b"\n").await?;
    file.flush().await?;
    info!("Appended WAL operation at offset {} (len {})", offset, data.len());
    Ok(offset)
}

/// Tail the WAL from `start_offset` and replay locally./// Tail the WAL from `start_offset` and replay locally.
pub async fn replay_wal_from(
    db: &Arc<DB>,
    cfs: &(Arc<BoundColumnFamily<'static>>, Arc<BoundColumnFamily<'static>>, Arc<BoundColumnFamily<'static>>),
    start_offset: u16,
) -> GraphResult<()> {
    let file = match tokio_fs::File::open(&*SHARED_WAL_PATH).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            info!("WAL file not found at {:?}, skipping replay", *SHARED_WAL_PATH);
            return Ok(());
        }
        Err(e) => return Err(GraphError::Io(e.to_string())),
    };
    let mut file = file;

    // Fixed: cast u16 → u64 for SeekFrom::Start
    if file.seek(SeekFrom::Start(start_offset as u64)).await.is_err() {
        warn!("Failed to seek to offset {} in WAL, starting from beginning", start_offset);
    }

    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut replayed = 0;
    while let Some(line) = lines.next_line().await.map_err(|e| GraphError::Io(e.to_string()))? {
        let line_bytes = line.as_bytes();
        if line_bytes.is_empty() {
            continue;
        }
        let (op, _): (RocksDBWalOperation, _) = match bincode::decode_from_slice(line_bytes, bincode::config::standard()) {
            Ok(decoded) => decoded,
            Err(e) => {
                warn!("Failed to decode WAL entry, skipping: {}", e);
                continue;
            }
        };
        if let Err(e) = RocksDBDaemon::apply_op_locally(db, cfs, &op).await {
            error!("Failed to apply WAL op during replay: {}", e);
            return Err(e);
        }
        replayed += 1;
    }
    info!("WAL replay completed from offset {} (replayed {} ops)", start_offset, replayed);
    Ok(())
}

/// WAL operation definition
#[derive(Encode, Decode, Clone)]
enum WalOp {
    Put { cf: String, key: Vec<u8>, value: Vec<u8> },
    Delete { cf: String, key: Vec<u8> },
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
    /// Background task to periodically sync WAL entries
    /// Background WAL sync task – **full original logic** with CFs passed in.

    pub fn start_background_wal_sync(
        port: u16,
        wal_manager: Arc<RocksDBWalManager>,
        canonical_path: &PathBuf,
        db: Arc<DB>,
        kv_pairs: Arc<BoundColumnFamily<'static>>,
        vertices: Arc<BoundColumnFamily<'static>>,
        edges: Arc<BoundColumnFamily<'static>>,
        running: Arc<TokioMutex<bool>>,
    ) {
        let canonical_path = canonical_path.clone();
        let db = db.clone();
        let kv_pairs = kv_pairs.clone();
        let vertices = vertices.clone();
        let edges = edges.clone();
        let wal_manager = wal_manager.clone();

        tokio::spawn(async move {
            println!("===> STARTING BACKGROUND WAL SYNC FOR PORT {}", port);
            let mut last_leader_check = std::time::Instant::now();

            while *running.lock().await {
                tokio::time::sleep(TokioDuration::from_secs(2)).await;

                // ──────────────────────────────────────────────────────────────
                // LEADER HEARTBEAT
                // ──────────────────────────────────────────────────────────────
                if last_leader_check.elapsed() > std::time::Duration::from_secs(10) {
                    if let Ok(Some(leader_port)) = get_leader_port(&canonical_path).await {
                        if leader_port != port {
                            let lock_path = canonical_path.join("wal_leader.lock");
                            if !tokio::fs::metadata(&lock_path).await.is_ok() {
                                warn!("Leader {} lock missing — promoting self", leader_port);
                                let _ = tokio::fs::remove_file(&lock_path).await;
                                let _ = become_wal_leader(&canonical_path, port).await;
                            }
                        }
                    }
                    last_leader_check = std::time::Instant::now();
                }

                // ──────────────────────────────────────────────────────────────
                // WAL REPLICATION
                // ──────────────────────────────────────────────────────────────
                let offset_key = format!("__wal_offset_port_{}", port);

                let last_offset: u64 = match db.get(&offset_key.as_bytes()) {
                    Ok(Some(v)) => String::from_utf8_lossy(&v).parse().unwrap_or(0),
                    _ => 0,
                };

                let current_lsn: u64 = match wal_manager.current_lsn().await {
                    Ok(lsn) => lsn,
                    Err(e) => {
                        warn!("Failed to get WAL LSN: {}", e);
                        continue;
                    }
                };

                if current_lsn <= last_offset {
                    continue;
                }

                let operations = match wal_manager.read_since(last_offset).await {
                    Ok(ops) => ops,
                    Err(e) => {
                        warn!("Failed to read WAL from {}: {}", last_offset, e);
                        continue;
                    }
                };

                if operations.is_empty() {
                    continue;
                }

                let mut latest_offset = last_offset;
                let mut applied = 0;

                let cfs = (kv_pairs.clone(), vertices.clone(), edges.clone());

                for (offset, op) in operations {
                    let op_size = bincode::encode_to_vec(&op, bincode::config::standard())
                        .map(|v| v.len() as u64)
                        .unwrap_or(0);

                    // APPLY TO ROCKSDB
                    if let Err(e) = RocksDBDaemon::apply_op_locally(&db, &cfs, &op).await {
                        error!("Failed to apply op at {}: {}", offset, e);
                        break;
                    }

                    applied += 1;
                    latest_offset = offset + 4 + op_size;
                }

                if latest_offset > last_offset && applied > 0 {
                    let _ = db.put(&offset_key.as_bytes(), latest_offset.to_string().as_bytes());
                    let _ = db.flush();
                    println!(
                        "===> PORT {} SYNCED {} OPERATIONS (OFFSET: {} -> {})",
                        port, applied, last_offset, latest_offset
                    );
                }
            }

            println!("===> BACKGROUND WAL SYNC STOPPED FOR PORT {}", port);
        });
    }

    async fn discover_other_ports(canonical_path: &PathBuf, exclude_port: u16) -> Vec<u16> {
        let mut ports = Vec::new();
        if let Ok(mut entries) = tokio_fs::read_dir(canonical_path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Some(name) = entry.file_name().to_str() {
                    if let Ok(p) = name.parse::<u16>() {
                        if p != exclude_port && p >= 8049 && p <= 8054 {
                            let db_path = entry.path();
                            if db_path.join("CURRENT").exists() {
                                ports.push(p);
                            }
                        }
                    }
                }
            }
        }
        ports
    }

    /// Apply a WAL operation locally to the RocksDB database using pre-fetched CF handles
    pub async fn apply_op_locally(
        db: &Arc<DB>,
        cfs: &(
            Arc<BoundColumnFamily<'static>>,
            Arc<BoundColumnFamily<'static>>,
            Arc<BoundColumnFamily<'static>>,
        ),
        op: &RocksDBWalOperation,
    ) -> GraphResult<()> {
        let (kv_pairs, vertices, edges) = cfs;

        info!("APPLYING WAL OP LOCALLY: {:?}", op);

        match op {
            RocksDBWalOperation::Put { cf, key, value } => {
                let cf_handle = match cf.as_str() {
                    "kv_pairs" => kv_pairs,
                    "vertices" => vertices,
                    "edges" => edges,
                    _ => {
                        warn!("UNKNOWN CF IN WAL OP: {}", cf);
                        return Err(GraphError::StorageError(format!("Unknown column family: {}", cf)));
                    }
                };

                db.put_cf(cf_handle, key, value)
                    .map_err(|e| GraphError::StorageError(format!("Put failed on {}: {}", cf, e)))?;

                // Explicitly flush the column family to ensure data is visible to reads
                db.flush_cf(cf_handle)
                    .map_err(|e| GraphError::StorageError(format!("Flush failed on {}: {}", cf, e)))?;

                info!("PUT SUCCESS: CF={}, key_len={}, value_len={}", cf, key.len(), value.len());
            }
            RocksDBWalOperation::Delete { cf, key } => {
                let cf_handle = match cf.as_str() {
                    "kv_pairs" => kv_pairs,
                    "vertices" => vertices,
                    "edges" => edges,
                    _ => {
                        warn!("UNKNOWN CF IN WAL DELETE: {}", cf);
                        return Err(GraphError::StorageError(format!("Unknown column family: {}", cf)));
                    }
                };

                db.delete_cf(cf_handle, key)
                    .map_err(|e| GraphError::StorageError(format!("Delete failed on {}: {}", cf, e)))?;

                // Explicitly flush the column family to ensure deletion is visible
                db.flush_cf(cf_handle)
                    .map_err(|e| GraphError::StorageError(format!("Flush failed on {}: {}", cf, e)))?;

                info!("DELETE SUCCESS: CF={}, key_len={}", cf, key.len());
            }
            RocksDBWalOperation::Flush { cf } => {
                if cf == "default" || cf.is_empty() {
                    db.flush().map_err(|e| GraphError::StorageError(format!("Flush all failed: {}", e)))?;
                    info!("FLUSH SUCCESS: all column families");
                } else {
                    let cf_handle = match cf.as_str() {
                        "kv_pairs" => Some(kv_pairs),
                        "vertices" => Some(vertices),
                        "edges" => Some(edges),
                        _ => {
                            warn!("FLUSH IGNORED: unknown CF {}", cf);
                            None
                        }
                    };
                    if let Some(h) = cf_handle {
                        db.flush_cf(h).map_err(|e| GraphError::StorageError(format!("Flush {} failed: {}", cf, e)))?;
                        info!("FLUSH SUCCESS: CF={}", cf);
                    }
                }
            }
        }
        Ok(())
    }

    /// Replay WAL operations from shared WAL using per-port offset tracking
    pub async fn replay_from_all_wals(
        canonical_path: &PathBuf,
        current_port: u16,
        db: &Arc<DB>,
        kv_pairs: &Arc<BoundColumnFamily<'static>>,
        vertices: &Arc<BoundColumnFamily<'static>>,
        edges: &Arc<BoundColumnFamily<'static>>,
    ) -> GraphResult<()> {
        info!("Starting WAL replay for port {}", current_port);
        println!("===> STARTING WAL REPLAY FOR PORT {}", current_port);

        let wal_path = canonical_path.join("wal_shared").join("shared.wal");
        if !wal_path.exists() {
            info!("No WAL file at {:?}, skipping", wal_path);
            println!("===> NO WAL FILE, SKIPPING REPLAY");
            return Ok(());
        }

        let offset_key = format!("__wal_offset_port_{}", current_port);
        let last_offset: u64 = db.get(&offset_key.as_bytes())?
            .and_then(|v| String::from_utf8(v.to_vec()).ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let wal_mgr = RocksDBWalManager::new(canonical_path.clone(), current_port).await?;
        let current_lsn = wal_mgr.current_lsn().await?;

        println!("===> LAST OFFSET: {}, WAL LSN: {}", last_offset, current_lsn);

        if last_offset >= current_lsn {
            println!("===> PORT {} IS UP TO DATE", current_port);
            return Ok(());
        }

        let operations = wal_mgr.read_since(last_offset).await?;
        if operations.is_empty() {
            return Ok(());
        }

        println!("===> REPLAYING {} OPS ({} → {})", operations.len(), last_offset, current_lsn);

        let cfs = (kv_pairs.clone(), vertices.clone(), edges.clone());
        let mut latest_offset = last_offset;
        let mut applied = 0;

        for (offset, op) in operations {
            let op_size = bincode::encode_to_vec(&op, bincode::config::standard())
                .map(|v| v.len() as u64)
                .unwrap_or(0);

            if let Err(e) = RocksDBDaemon::apply_op_locally(db, &cfs, &op).await {
                error!("Replay failed at {}: {}", offset, e);
                break;
            }

            applied += 1;
            latest_offset = offset + 4 + op_size;
        }

        if latest_offset > last_offset {
            let _ = db.put(&offset_key.as_bytes(), latest_offset.to_string().as_bytes());
            let _ = db.flush();
            println!("===> REPLAYED {} OPS ({} → {})", applied, last_offset, latest_offset);
        }

        Ok(())
    }

    /// Start background task to replicate WAL to other ports
    pub fn start_wal_replication(
        port: u16,
        wal_manager: Arc<RocksDBWalManager>,
        db: Arc<rocksdb::DB>,
        running: Arc<TokioMutex<bool>>,
    ) {
        tokio::spawn(async move {
            while *running.lock().await {
                tokio::time::sleep(TokioDuration::from_secs(1)).await;
                if let Ok(current_lsn) = wal_manager.current_lsn().await {
                    debug!("Port {} WAL at LSN {}", port, current_lsn);
                }
            }
        });
    }

    async fn open_rocksdb_readonly(path: &Path, use_compression: bool) -> GraphResult<Arc<DB>> {
        let mut opts = Options::default();
        opts.create_if_missing(false);
        opts.create_missing_column_families(false);
        if use_compression {
            opts.set_compression_type(DBCompressionType::Lz4);
        }
        let cfs = vec!["kv_pairs", "vertices", "edges"];
        let db = DB::open_cf_for_read_only(&opts, path, cfs, false)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(Arc::new(db))
    }

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
        use std::time::Duration as TokioDuration;

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

                    // ✅ FIXED: cf_handle is &Arc<BoundColumnFamily<'_>>, so .clone() gives Arc<BoundColumnFamily<'_>>
                    macro_rules! get_static_cf {
                        ($name:expr) => {{
                            let cf_handle = db_arc
                                .cf_handle($name)
                                .ok_or_else(|| {
                                    let msg = format!("Failed to get column family '{}'", $name);
                                    error!("{}", msg);
                                    println!("===> ERROR: {}", msg);
                                    GraphError::StorageError(msg)
                                })?;

                            // cf_handle is &Arc<BoundColumnFamily<'_>>
                            // cf_handle.clone() → Arc<BoundColumnFamily<'_>>
                            unsafe {
                                std::mem::transmute::<
                                    Arc<BoundColumnFamily<'_>>,
                                    Arc<BoundColumnFamily<'static>>,
                                >(cf_handle.clone())
                            }
                        }};
                    }

                    let kv_pairs = get_static_cf!("kv_pairs");
                    let vertices = get_static_cf!("vertices");
                    let edges    = get_static_cf!("edges");

                    info!(
                        "RocksDB opened at {:?} in {} mode after {} attempts",
                        db_path, mode_str, attempts + 1
                    );
                    println!(
                        "===> ROCKSDB OPENED AT {:?} IN {} MODE AFTER {} ATTEMPTS",
                        db_path, mode_str, attempts + 1
                    );

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
                        error!(
                            "Failed to open RocksDB at {:?} after {} attempts: {}",
                            db_path, MAX_RETRIES, e
                        );
                        println!(
                            "===> ERROR: FAILED TO OPEN ROCKSDB AT {:?} AFTER {} ATTEMPTS: {}",
                            db_path, MAX_RETRIES, e
                        );
                        return Err(GraphError::StorageError(format!(
                            "Failed to open RocksDB: {}. Ensure no other process is using the database.",
                            e
                        )));
                    }

                    let backoff_factor = 2_u64.pow(attempts as u32 - 1);
                    let sleep_ms = (INITIAL_BACKOFF_MS * backoff_factor).min(MAX_BACKOFF_MS);
                    warn!(
                        "RocksDB lock contention at {:?}. Retrying in {}ms (Attempt {}/{})",
                        db_path, sleep_ms, attempts, MAX_RETRIES
                    );
                    println!(
                        "===> WARNING: ROCKSDB LOCK CONTENTION AT {:?}. RETRYING IN {}ms (ATTEMPT {}/{})",
                        db_path, sleep_ms, attempts, MAX_RETRIES
                    );
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

    /// Opens DB immediately under per-port lock, binds ZMQ in background — returns instantly
    pub async fn new(
        config: RocksDBConfig,
    ) -> GraphResult<(RocksDBDaemon<'static>, mpsc::Receiver<()>)> {
        println!("===> RocksDBDaemon::new() CALLED");
        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let _guard = get_rocksdb_daemon_port_lock(port).await;
        println!("===> ACQUIRED PER-PORT INIT LOCK FOR PORT {}", port);
        let db_path = PathBuf::from(config.path.clone());
        let db_path_clone = db_path.clone();
        info!("Initializing RocksDBDaemon at {:?}", db_path);
        println!("===> INITIALIZING ROCKSDB DAEMON AT {:?}", db_path);
        tokio_fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to create directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO CREATE DIRECTORY AT {:?}: {}", db_path, e);
                GraphError::StorageError(format!("Failed to create dir: {}", e))
            })?;

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        if config.use_compression {
            opts.set_compression_type(DBCompressionType::Lz4);
        }

        let existing_cfs = if db_path.exists() {
            DB::list_cf(&opts, &db_path).unwrap_or_default()
        } else {
            Vec::new()
        };

        let cf_descriptors = if existing_cfs.is_empty() {
            vec![
                ColumnFamilyDescriptor::new("kv_pairs", Options::default()),
                ColumnFamilyDescriptor::new("vertices", Options::default()),
                ColumnFamilyDescriptor::new("edges", Options::default()),
            ]
        } else {
            existing_cfs
                .iter()
                .map(|name| ColumnFamilyDescriptor::new(name, Options::default()))
                .collect()
        };

        let db = DB::open_cf_descriptors(&opts, &db_path, cf_descriptors)
            .map_err(|e| GraphError::StorageError(format!("Failed to open DB with CFs: {}", e)))?;
        let db = Arc::new(db);

        // Correct: Arc<BoundColumnFamily<'static>>
        let kv_pairs: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(db.cf_handle("kv_pairs").ok_or_else(|| {
                GraphError::StorageError("CF kv_pairs missing".into())
            })?)
        };
        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(db.cf_handle("vertices").ok_or_else(|| {
                GraphError::StorageError("CF vertices missing".into())
            })?)
        };
        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(db.cf_handle("edges").ok_or_else(|| {
                GraphError::StorageError("CF edges missing".into())
            })?)
        };

        info!("ROCKSDB AND COLUMN FAMILIES SUCCESSFULLY OPENED AT {:?}", db_path);
        println!("===> ROCKSDB AND COLUMN FAMILIES SUCCESSFULLY OPENED AT {:?}", db_path);
        let canonical_path = db_path.parent().unwrap().to_path_buf();
        let wal_manager = Arc::new(
            RocksDBWalManager::new(canonical_path.clone(), port)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create RocksDB WAL manager: {}", e)))?
        );
        // REPLAY WAL ON STARTUP
        RocksDBDaemon::replay_from_all_wals(&canonical_path, port, &db, &kv_pairs, &vertices, &edges).await?;
        // ──────────────────────────────────────────────────────────────
        // LEADER ELECTION ON STARTUP
        // ──────────────────────────────────────────────────────────────
        let _ = become_wal_leader(&canonical_path, port).await;
        let (ready_tx, ready_rx) = mpsc::channel::<()>(1);
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let ipc_path = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
        if Path::new(ipc_path).exists() {
            let _ = tokio::fs::remove_file(ipc_path).await;
            info!("Removed stale IPC socket at {}", ipc_path);
            println!("===> REMOVED STALE IPC SOCKET AT {}", ipc_path);
        }
        let zmq_context = Arc::new(ZmqContext::new());
        let running = Arc::new(TokioMutex::new(true));
        let running_clone = running.clone();
        let zmq_thread_handle = {
            let zmq_context_thread = zmq_context.clone();
            let endpoint_thread = endpoint.clone();
            let config_thread = config.clone();
            let ready_tx = ready_tx.clone();
            let db_path_thread = db_path.clone();
            let db_clone = db.clone();
            let kv_clone = kv_pairs.clone();
            let vert_clone = vertices.clone();
            let edge_clone = edges.clone();
            let wal_manager_clone = wal_manager.clone();
            let canonical_path_thread = canonical_path.clone();
            std::thread::spawn(move || -> GraphResult<()> {
                let rt = tokio::runtime::Runtime::new()
                    .expect("Failed to create runtime for ZMQ thread");
                rt.block_on(async {
                    let socket_raw = zmq_context_thread
                        .socket(REP)
                        .map_err(|e| GraphError::StorageError(format!("ZMQ socket create failed: {}", e)))?;
                    let mut bound = false;
                    for i in 0..5 {
                        match socket_raw.bind(&endpoint_thread) {
                            Ok(_) => {
                                bound = true;
                                info!("ZMQ socket bound successfully on attempt {}", i + 1);
                                println!("===> ZMQ SOCKET BOUND SUCCESSFULLY ON ATTEMPT {}", i + 1);
                                break;
                            }
                            Err(e) => {
                                warn!("ZMQ bind attempt {} failed: {}", i + 1, e);
                                println!("===> WARNING: ZMQ BIND ATTEMPT {} FAILED: {}", i + 1, e);
                                if i < 4 {
                                    tokio::time::sleep(TokioDuration::from_millis(100 * (i + 1) as u64)).await;
                                }
                            }
                        }
                    }
                    if bound {
                        let _ = ready_tx.send(()).await;
                    } else {
                        let _ = ready_tx.send(()).await;
                        return Err(GraphError::StorageError("ZMQ failed to bind after retries".to_string()));
                    }
                    info!("ZMQ server bound successfully at {}", endpoint_thread);
                    println!("===> ZMQ SERVER BOUND SUCCESSFULLY AT {}", endpoint_thread);
                    let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
                    let metadata = DaemonMetadata {
                        service_type: "storage".to_string(),
                        port,
                        pid: std::process::id(),
                        ip_address: config_thread.host.clone().unwrap_or("127.0.0.1".to_string()),
                        data_dir: Some(db_path_thread.clone()),
                        config_path: None,
                        engine_type: Some("rocksdb".to_string()),
                        last_seen_nanos: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_nanos() as i64)
                            .unwrap_or(0),
                        zmq_ready: true,
                        engine_synced: true,
                    };
                    if let Err(e) = daemon_registry.update_daemon_metadata(metadata).await {
                        warn!("Failed to update daemon registry with zmq_ready=true: {}", e);
                        println!("===> WARNING: FAILED TO UPDATE DAEMON REGISTRY WITH ZMQ_READY=TRUE: {}", e);
                    } else {
                        info!("Updated daemon registry: zmq_ready=true, engine_synced=true");
                        println!("===> UPDATED DAEMON REGISTRY: ZMQ_READY=TRUE, ENGINE_SYNCED=TRUE");
                    }
                    let zmq_socket = Arc::new(TokioMutex::new(socket_raw));
                    // START BACKGROUND SYNC
                    RocksDBDaemon::start_background_wal_sync(
                        port,
                        wal_manager_clone.clone(),
                        &canonical_path_thread,
                        db_clone.clone(),
                        kv_clone.clone(),
                        vert_clone.clone(),
                        edge_clone.clone(),
                        running_clone.clone(),
                    );
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
                        wal_manager_clone,
                        db_path_thread,
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
            wal_manager,
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
        info!("RocksDBDaemon initialized on port {} (real DB in ZMQ thread)", port);
        println!("===> ROCKSDB DAEMON INITIALIZED ON PORT {} (REAL DB IN ZMQ THREAD)", port);
        Ok((daemon, ready_rx))
    }

    /// Initializes a RocksDB daemon using an **already-opened** `rocksdb::DB`.
    /// The DB must contain the three column families: `"kv_pairs"`, `"vertices"`, `"edges"`.
    /// WAL replay is performed using the **same DB + CF handles** that the daemon will use.
    pub async fn new_with_db(
        config: RocksDBConfig,
        existing_db: Arc<DB>,
    ) -> GraphResult<(RocksDBDaemon<'static>, mpsc::Receiver<()>)> {
        info!("RocksDBDaemon::new_with_db called – using supplied DB");
        let config_port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let global_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let free_daemon_metadata = global_registry.find_free_storage_daemon().await?;
        let port: u16 = if let Some(metadata) = free_daemon_metadata {
            info!("Reusing uninitialized storage daemon port {} (registry).", metadata.port);
            metadata.port
        } else {
            info!("Using config port {}.", config_port);
            config_port
        };
        let base_db_path = PathBuf::from(config.path.clone());
        let db_path = base_db_path.join(port.to_string());
        info!("RocksDBDaemon using port {} at path {:?}", port, db_path);
        let lock_path = db_path.join("LOCK");
        if lock_path.exists() {
            if let Ok(Some(metadata)) = GLOBAL_DAEMON_REGISTRY.find_daemon_by_port(port).await {
                let system = System::new_with_specifics(
                    RefreshKind::nothing().with_processes(ProcessRefreshKind::everything()),
                );
                if system.process(Pid::from_u32(metadata.pid)).is_none() {
                    info!("Removing stale lock file at {:?}", lock_path);
                    handle_rocksdb_op!(
                        tokio::fs::remove_file(&lock_path).await,
                        format!("Failed to remove stale lock file at {:?}", lock_path)
                    )?;
                }
            }
        }

        // Correct: transmute raw handle to 'static
        let kv_pairs: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(existing_db.cf_handle("kv_pairs").ok_or_else(|| {
                GraphError::StorageError("CF kv_pairs missing".into())
            })?)
        };
        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(existing_db.cf_handle("vertices").ok_or_else(|| {
                GraphError::StorageError("CF vertices missing".into())
            })?)
        };
        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(existing_db.cf_handle("edges").ok_or_else(|| {
                GraphError::StorageError("CF edges missing".into())
            })?)
        };

        // === WAL MANAGER & REPLAY ===
        let wal_manager = Arc::new(RocksDBWalManager::new(db_path.parent().unwrap().to_path_buf(), port).await
            .map_err(|e| GraphError::StorageError(format!("Failed to create RocksDB WAL manager: {}", e)))?);
        let canonical_path = db_path.parent().unwrap().to_path_buf();
        RocksDBDaemon::replay_from_all_wals(&canonical_path, port, &existing_db, &kv_pairs, &vertices, &edges).await?;
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
        let (ready_tx, mut ready_rx) = mpsc::channel::<()>(1);
        let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let ipc_path = endpoint.strip_prefix("ipc://").unwrap_or(&endpoint);
        if Path::new(ipc_path).exists() {
            warn!("Stale IPC socket at {}. Removing.", ipc_path);
            let _ = tokio::fs::remove_file(ipc_path).await;
        }
        let running = Arc::new(TokioMutex::new(true));
        let daemon = RocksDBDaemon {
            port,
            db_path: db_path.clone(),
            db: existing_db.clone(),
            kv_pairs: kv_pairs.clone(),
            vertices: vertices.clone(),
            edges: edges.clone(),
            wal_manager: wal_manager.clone(),
            running: running.clone(),
            shutdown_tx: shutdown_tx.clone(),
            zmq_context: Arc::new(ZmqContext::new()),
            zmq_thread: Arc::new(TokioMutex::new(None)),
            #[cfg(feature = "with-openraft-rocksdb")]
            raft,
            #[cfg(feature = "with-openraft-rocksdb")]
            raft_storage,
            #[cfg(feature = "with-openraft-rocksdb")]
            node_id,
        };
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
            engine_synced: true,
        };
        GLOBAL_DAEMON_REGISTRY
            .register_daemon(initial_metadata.clone())
            .await
            .map_err(|e| GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e)))?;
        let zmq_context_thread = daemon.zmq_context.clone();
        let endpoint_thread = endpoint.clone();
        let config_thread = config.clone();
        let ready_tx_thread = ready_tx.clone();
        let db_path_thread = db_path.clone();
        let db_clone = existing_db.clone();
        let kv_clone = kv_pairs.clone();
        let vert_clone = vertices.clone();
        let edge_clone = edges.clone();
        let running_clone = running.clone();
        let wal_manager_clone = wal_manager.clone();
        let zmq_thread_handle = std::thread::spawn(move || -> GraphResult<()> {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                let socket_raw = zmq_context_thread.socket(REP).map_err(|e| {
                    GraphError::StorageError(format!("Failed to create ZMQ REP socket: {}", e))
                })?;
                let mut bound = false;
                for i in 0..5 {
                    match socket_raw.bind(&endpoint_thread) {
                        Ok(()) => {
                            bound = true;
                            info!("ZMQ bound on attempt {}", i + 1);
                            break;
                        }
                        Err(e) => {
                            warn!("ZMQ bind attempt {} failed: {}", i + 1, e);
                            if i < 4 {
                                tokio::time::sleep(TokioDuration::from_millis(100 * (i + 1) as u64)).await;
                            }
                        }
                    }
                }
                let _ = ready_tx_thread.send(()).await;
                if !bound {
                    return Err(GraphError::StorageError("ZMQ failed to bind after retries".into()));
                }
                let metadata = DaemonMetadata {
                    zmq_ready: true,
                    ..initial_metadata
                };
                let _ = GLOBAL_DAEMON_REGISTRY.get().await.update_daemon_metadata(metadata).await;
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
                    wal_manager_clone,
                    db_path_thread,
                )
                .await
            })
        });
        *daemon.zmq_thread.lock().await = Some(zmq_thread_handle);
        info!("Waiting for ZMQ readiness on port {}...", port);
        match tokio::time::timeout(TokioDuration::from_secs(10), ready_rx.recv()).await {
            Ok(Some(_)) => {
                info!("ZMQ readiness confirmed on port {}", port);
            }
            Ok(None) => {
                warn!("ZMQ readiness channel closed on port {}", port);
            }
            Err(_) => {
                warn!("ZMQ readiness timeout on port {}", port);
            }
        }
        info!("RocksDB daemon fully initialized on port {}", port);
        Ok((daemon, shutdown_rx))
    }

    /* -------------------------------------------------------------
        new_with_client — CLIENT MODE (READ-ONLY)
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

        if !is_storage_daemon_running(port).await {
            error!("No running daemon found on port {}", port);
            println!("===> ERROR: NO RUNNING DAEMON FOUND ON PORT {}", port);
            return Err(GraphError::StorageError(format!("No running daemon found on port {}", port)));
        }

        let mut opts = Options::default();
        opts.create_if_missing(false);
        opts.create_missing_column_families(false);

        let db = Arc::new(handle_rocksdb_op!(
            DB::open_for_read_only(&opts, &db_path, false),
            format!("Failed to open RocksDB in read-only mode at {}", db_path.display())
        )?);

        let kv_pairs: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(
                db.cf_handle("kv_pairs")
                    .ok_or_else(|| GraphError::StorageError("Column family kv_pairs not found".to_string()))?
            ))
        };

        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(
                db.cf_handle("vertices")
                    .ok_or_else(|| GraphError::StorageError("Column family vertices not found".to_string()))?
            ))
        };

        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(
                db.cf_handle("edges")
                    .ok_or_else(|| GraphError::StorageError("Column family edges not found".to_string()))?
            ))
        };

        // WAL MANAGER — REQUIRED EVEN IN CLIENT MODE
        let canonical_path = db_path.parent().unwrap_or(&db_path).to_path_buf();
        let wal_manager = Arc::new(
            RocksDBWalManager::new(canonical_path, port)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create RocksDBWalManager: {}", e)))?
        );

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
            wal_manager, // ← FIXED: MISSING FIELD
            running: Arc::new(TokioMutex::new(true)),
            shutdown_tx,
            zmq_context,
            zmq_thread: Arc::new(TokioMutex::new(None)),
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

    /// ZMQ server – DB is already opened in `RocksDBDaemon::new()`
    /// All original commands are preserved.
    async fn run_zmq_server_lazy(
        port: u16,
        _config: RocksDBConfig,
        running: Arc<TokioMutex<bool>>,
        zmq_socket: Arc<TokioMutex<ZmqSocket>>,
        endpoint: String,
        db: Arc<DB>,
        kv_pairs: Arc<BoundColumnFamily<'static>>,
        vertices: Arc<BoundColumnFamily<'static>>,
        edges: Arc<BoundColumnFamily<'static>>,
        wal_manager: Arc<RocksDBWalManager>,
        db_path: PathBuf,
    ) -> GraphResult<()> {
        info!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);
        println!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);

        let mut socket = zmq_socket.lock().await;
        socket.set_linger(1000)?;
        socket.set_rcvtimeo(SOCKET_TIMEOUT_MS)?;
        socket.set_sndtimeo(SOCKET_TIMEOUT_MS)?;
        socket.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)?;
        drop(socket);

        info!("ZeroMQ server configured for port {}", port);
        println!("===> ZEROMQ SERVER CONFIGURED FOR PORT {}", port);

        let mut consecutive_errors = 0;
        while *running.lock().await {
            let msg_result = {
                let s = zmq_socket.lock().await;
                s.recv_bytes(DONTWAIT)
            };

            let msg: Vec<u8> = match msg_result {
                Ok(m) => {
                    consecutive_errors = 0;
                    debug!("Received ZMQ message for port {}", port);
                    m
                }
                Err(ZmqError::EAGAIN) => {
                    tokio::time::sleep(TokioDuration::from_millis(10)).await;
                    continue;
                }
                Err(e) => {
                    consecutive_errors += 1;
                    warn!(
                        "Failed to receive ZMQ message (attempt {}): {}",
                        consecutive_errors, e
                    );
                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!("Too many consecutive ZMQ errors, shutting down server");
                        break;
                    }
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                    continue;
                }
            };

            if msg.is_empty() {
                let resp = json!({"status":"error","message":"Received empty message"});
                let s = zmq_socket.lock().await;
                Self::send_zmq_response_static(&s, &resp, port).await?;
                continue;
            }

            let request: Value = match serde_json::from_slice(&msg) {
                Ok(r) => r,
                Err(e) => {
                    let resp = json!({"status":"error","message":format!("Parse error: {}",e)});
                    let s = zmq_socket.lock().await;
                    Self::send_zmq_response_static(&s, &resp, port).await?;
                    continue;
                }
            };

            let command = request.get("command").and_then(|c| c.as_str());

            macro_rules! send_resp {
                ($resp:expr) => {{
                    let s = zmq_socket.lock().await;
                    Self::send_zmq_response_static(&s, &$resp, port).await?;
                    continue;
                }};
            }

            let response = match command {
                Some("initialize") => json!({
                    "status": "success",
                    "message": "ZMQ server is bound and DB is open.",
                    "port": port,
                    "ipc_path": &endpoint
                }),

                Some("status") => json!({"status":"success","port":port,"db_open":true}),

                Some("ping") => json!({
                    "status": "pong",
                    "message": "ZMQ server is bound and DB is open.",
                    "port": port,
                    "ipc_path": &endpoint,
                    "db_open":true
                }),

                Some("force_unlock") => match Self::force_unlock_static(&db_path).await {
                    Ok(_) => json!({"status":"success"}),
                    Err(e) => json!({"status":"error","message":e.to_string()}),
                },

                // ──────────────────────────────────────────────────────────────
                // MUTATING COMMANDS – WAL + LEADER ELECTION + FORWARDING
                // ──────────────────────────────────────────────────────────────
                Some(cmd) if [
                    "set_key","delete_key",
                    "create_vertex","update_vertex","delete_vertex",
                    "create_edge","update_edge","delete_edge",
                    "flush"
                ].contains(&cmd) => {
                    let canonical = db_path.parent().unwrap().to_path_buf();
                    let is_leader = become_wal_leader(&canonical, port).await
                        .map_err(|e| GraphError::StorageError(e.to_string()))?;
                    println!("====> IN run_zmq_server_lazy - command {:?}", cmd);
                    if is_leader {
                        // ---------- BUILD WAL OPERATION ----------
                        let op = match cmd {
                            "set_key" => {
                                let cf = request["cf"].as_str().unwrap_or("kv_pairs").to_string();
                                let key = request["key"]
                                    .as_str()
                                    .ok_or_else(|| GraphError::StorageError("missing key".into()))?
                                    .as_bytes()
                                    .to_vec();
                                let value = request["value"]
                                    .as_str()
                                    .ok_or_else(|| GraphError::StorageError("missing value".into()))?
                                    .as_bytes()
                                    .to_vec();
                                RocksDBWalOperation::Put { cf, key, value }
                            }
                            "delete_key" => {
                                let cf = request["cf"].as_str().unwrap_or("kv_pairs").to_string();
                                let key = request["key"]
                                    .as_str()
                                    .ok_or_else(|| GraphError::StorageError("missing key".into()))?
                                    .as_bytes()
                                    .to_vec();
                                RocksDBWalOperation::Delete { cf, key }
                            }

                            // ---------- VERTEX ----------
                            "create_vertex" | "update_vertex" => {
                                let vertex: Vertex = serde_json::from_value(request["vertex"].clone())
                                    .map_err(|e| GraphError::StorageError(format!("Invalid vertex: {}", e)))?;
                                let key = vertex.id.as_bytes().to_vec();
                                let value = serialize_vertex(&vertex)?;
                                RocksDBWalOperation::Put {
                                    cf: "vertices".to_string(),
                                    key,
                                    value,
                                }
                            }
                            "delete_vertex" => {
                                let id: Identifier = serde_json::from_value(request["id"].clone())
                                    .map_err(|e| GraphError::StorageError(format!("Invalid id: {}", e)))?;
                                let uuid = SerializableUuid::from(id.as_ref())?;
                                RocksDBWalOperation::Delete {
                                    cf: "vertices".to_string(),
                                    key: uuid.as_bytes().to_vec(),
                                }
                            }

                            // ---------- EDGE ----------
                            "create_edge" | "update_edge" => {
                                let edge: Edge = serde_json::from_value(request["edge"].clone())
                                    .map_err(|e| GraphError::StorageError(format!("Invalid edge: {}", e)))?;
                                let key = create_edge_key(&edge.outbound_id, &edge.t, &edge.inbound_id)?;
                                let value = serialize_edge(&edge)?;
                                RocksDBWalOperation::Put {
                                    cf: "edges".to_string(),
                                    key,
                                    value,
                                }
                            }
                            "delete_edge" => {
                                let from: Identifier = serde_json::from_value(request["from"].clone())
                                    .map_err(|e| GraphError::StorageError(format!("Invalid from: {}", e)))?;
                                let to: Identifier = serde_json::from_value(request["to"].clone())
                                    .map_err(|e| GraphError::StorageError(format!("Invalid to: {}", e)))?;
                                let t: Identifier = serde_json::from_value(request["type"].clone())
                                    .map_err(|e| GraphError::StorageError(format!("Invalid type: {}", e)))?;
                                let from_uuid = SerializableUuid::from(from.as_ref())?;
                                let to_uuid   = SerializableUuid::from(to.as_ref())?;
                                let key = create_edge_key(&from_uuid, &t, &to_uuid)?;
                                RocksDBWalOperation::Delete {
                                    cf: "edges".to_string(),
                                    key,
                                }
                            }

                            "flush" => {
                                let cf = request["cf"].as_str().unwrap_or("default").to_string();
                                RocksDBWalOperation::Flush { cf }
                            }
                            _ => unreachable!(),
                        };

                        // ---------- APPLY ----------
                        let lsn = wal_manager.append(&op).await
                            .map_err(|e| GraphError::StorageError(e.to_string()))?;
                        let cfs = (kv_pairs.clone(), vertices.clone(), edges.clone());
                        RocksDBDaemon::apply_op_locally(&db, &cfs, &op).await
                            .map_err(|e| GraphError::StorageError(e.to_string()))?;

                        let offset_key = format!("__wal_offset_port_{}", port);
                        db.put(offset_key.as_bytes(), lsn.to_string().as_bytes())?;
                        db.flush()?;

                        json!({"status":"success", "offset": lsn, "leader": true})
                    } else {
                        // ---------- FORWARD TO LEADER ----------
                        let leader_port = match get_leader_port(&canonical).await {
                            Ok(Some(p)) => p,
                            Ok(None) => {
                                send_resp!(json!({"status":"error","message":"No leader found"}));
                                continue;
                            }
                            Err(e) => {
                                send_resp!(json!({"status":"error","message":format!("Leader lookup failed: {}", e)}));
                                continue;
                            }
                        };
                        if leader_port == 0 {
                            send_resp!(json!({"status":"error","message":"Leader election in progress"}));
                            continue;
                        }

                        let context = zmq::Context::new();
                        let client = context.socket(zmq::REQ)
                            .map_err(|e| GraphError::StorageError(format!("ZMQ socket error: {}", e)))?;
                        client.connect(&format!("ipc:///tmp/graphdb-{}.ipc", leader_port))
                            .map_err(|_| GraphError::StorageError(format!("Failed to connect to leader {}", leader_port)))?;
                        client.set_sndtimeo(5000).ok();
                        client.set_rcvtimeo(5000).ok();

                        let mut forward_req = request.clone();
                        forward_req["forwarded_from"] = json!(port);
                        forward_req["command"] = json!(cmd);
                        let payload = serde_json::to_vec(&forward_req)
                            .map_err(|e| GraphError::StorageError(format!("JSON serialize error: {}", e)))?;

                        client.send(&payload, 0)
                            .map_err(|_| GraphError::StorageError("ZMQ send failed".into()))?;
                        let resp_msg = client.recv_bytes(0)
                            .map_err(|e| GraphError::StorageError(format!("ZMQ recv error: {}", e)))?;
                        let resp: Value = serde_json::from_slice(&resp_msg)
                            .map_err(|e| GraphError::StorageError(format!("Leader response parse error: {}", e)))?;

                        info!("Forwarded write to leader port {} to {:?}", leader_port, resp);
                        resp
                    }
                }

                // ──────────────────────────────────────────────────────────────
                // READ-ONLY COMMANDS (delegated)
                // ──────────────────────────────────────────────────────────────
                Some(cmd) if [
                    "get_key",
                    "get_vertex","get_edge",
                    "get_all_vertices","get_all_edges",
                    "get_all_vertices_by_type","get_all_edges_by_type"
                ].contains(&cmd) => {
                    match Self::execute_db_command(
                        cmd,
                        &request,
                        &db,
                        &kv_pairs,
                        &vertices,
                        &edges,
                        port,
                        &db_path,
                        &endpoint,
                    ).await {
                        Ok(r) => r,
                        Err(e) => json!({"status":"error","message":e.to_string()}),
                    }
                }

                // ──────────────────────────────────────────────────────────────
                // DATA CLEAR / RESET
                // ──────────────────────────────────────────────────────────────
                Some("clear_data") | Some("force_reset") => {
                    match Self::execute_db_command(
                        command.unwrap(),
                        &request,
                        &db,
                        &kv_pairs,
                        &vertices,
                        &edges,
                        port,
                        &db_path,
                        &endpoint,
                    ).await {
                        Ok(r) => r,
                        Err(e) => json!({"status":"error","message":e.to_string()}),
                    }
                }

                Some("force_ipc_init") => {
                    info!("Received force_ipc_init command for port {}", port);
                    println!("===> FORCING IPC INITIALIZATION FOR PORT {}", port);
                    let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                    match tokio::fs::metadata(&ipc_path).await {
                        Ok(_) => json!({
                            "status": "success",
                            "message": "IPC socket already exists",
                            "ipc_path": &ipc_path
                        }),
                        Err(_) => {
                            error!(
                                "IPC socket missing at {} even though ZMQ server is running",
                                ipc_path
                            );
                            json!({
                                "status": "error",
                                "message": "IPC socket missing - server restart required",
                                "ipc_path": &ipc_path
                            })
                        }
                    }
                }

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
            // ---------- GET KEY ----------
            "get_key" => {
                let cf_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = request.get("key").and_then(|k| k.as_str()).ok_or("Missing key")?;
                let cf_handle = db.cf_handle(cf_name).ok_or(format!("Column family {} not found", cf_name))?;
                let result = db.get_cf(&cf_handle, key.as_bytes())?;
                let value_str = result.map(|v| String::from_utf8_lossy(&v).to_string());
                Ok(json!({"status": "success", "value": value_str}))
            }

            // ---------- VERTICES ----------
            "get_all_vertices" => {
                println!("===========> in execute_db_command - will try to get all vertices {:?}", command);
                let iterator = db.iterator_cf(&Arc::clone(vertices), rocksdb::IteratorMode::Start);
                let mut vec = Vec::new();
                for item in iterator {
                    let (_, value) = item?;
                    let v = deserialize_vertex(&value)?;
                    vec.push(v);
                }
                Ok(json!({"status": "success", "vertices": vec}))
            }
            "get_all_vertices_by_type" => {
                let vertex_type: Identifier = serde_json::from_value(request["vertex_type"].clone())
                    .map_err(|_| "Invalid or missing vertex_type")?;
                let iterator = db.iterator_cf(&Arc::clone(vertices), rocksdb::IteratorMode::Start);
                let mut vec = Vec::new();
                for item in iterator {
                    let (_, value) = item?;
                    let v = deserialize_vertex(&value)?;
                    if v.label == vertex_type {
                        vec.push(v);
                    }
                }
                Ok(json!({"status": "success", "vertices": vec}))
            }

            // ---------- EDGES ----------
            "get_all_edges" => {
                let iterator = db.iterator_cf(&Arc::clone(edges), rocksdb::IteratorMode::Start);
                let mut vec = Vec::new();
                for item in iterator {
                    let (_, value) = item?;
                    let e = deserialize_edge(&value)?;
                    vec.push(e);
                }
                Ok(json!({"status": "success", "edges": vec}))
            }
            "get_all_edges_by_type" => {
                let edge_type: Identifier = serde_json::from_value(request["edge_type"].clone())
                    .map_err(|_| "Invalid or missing edge_type")?;
                let iterator = db.iterator_cf(&Arc::clone(edges), rocksdb::IteratorMode::Start);
                let mut vec = Vec::new();
                for item in iterator {
                    let (_, value) = item?;
                    let e = deserialize_edge(&value)?;
                    if e.t == edge_type {
                        vec.push(e);
                    }
                }
                Ok(json!({"status": "success", "edges": vec}))
            }

            // ---------- CLEAR / RESET ----------
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

            // ---------- NOT ALLOWED HERE ----------
            "set_key" | "delete_key" | "create_vertex" | "update_vertex"
            | "delete_vertex" | "create_edge" | "update_edge"
            | "delete_edge" | "flush" => {
                Err("Mutating commands are not allowed in execute_db_command. Use ZMQ mutating path with WAL.".into())
            }

            cmd => {
                error!("Unsupported command in execute_db_command: {}", cmd);
                Ok(json!({"status": "error", "message": format!("Unsupported command: {}", cmd)}))
            }
        }
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

    /// Pull the latest SST files from any other live follower.
    // FIX: Changed my_path to accept &PathBuf to match copy_data_files_only signature.
    async fn pre_synchronize(my_port: u16, my_path: &PathBuf) -> GraphResult<()> {
        let canonical = {
            let map = CANONICAL_DB_MAP.read().await;
            map.iter()
                .find_map(|(dir, ports)| ports.contains(&my_port).then(|| dir.clone()))
        };

        let Some(canonical) = canonical else { return Ok(()); };

        let src_port = RocksDBDaemonPool::followers_of(&canonical).await
            .into_iter()
            .find(|&p| p != my_port);

        let Some(src) = src_port else { return Ok(()); };

        let src_path = canonical.join(src.to_string());
        info!("pre_synchronize: pulling from port {} to {}", src, my_port);
        // This call is now correct since my_path is &PathBuf
        RocksDBDaemonPool::copy_data_files_only(&src_path, my_path).await
    }

    /// Push our new SST files to **every** other follower.
    // FIX: Changed my_path to accept &PathBuf to match copy_data_files_only signature.
    async fn post_synchronize(my_port: u16, my_path: &PathBuf) -> GraphResult<()> {
        let canonical = {
            let map = CANONICAL_DB_MAP.read().await;
            map.iter()
                .find_map(|(dir, ports)| ports.contains(&my_port).then(|| dir.clone()))
        };

        let Some(canonical) = canonical else { return Ok(()); };

        let targets = RocksDBDaemonPool::followers_of(&canonical).await
            .into_iter()
            .filter(|&p| p != my_port)
            .collect::<Vec<_>>();

        for tgt in targets {
            let tgt_path = canonical.join(tgt.to_string());
            info!("post_synchronize: pushing to port {}", tgt);
            // This call is now correct since my_path is &PathBuf
            RocksDBDaemonPool::copy_data_files_only(my_path, &tgt_path).await?;
        }
        Ok(())
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
        println!("=============> in rocksdb_storage_daemon_pool ===============================+> NO, IT SHOULD NEVER COME HERE");
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

        // Clone db_path EARLY to own it
        let db_path_owned = db_path.to_path_buf();

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
            std::mem::transmute(Arc::new(
                db.cf_handle("kv_pairs")
                    .ok_or_else(|| GraphError::StorageError("Failed to open kv_pairs column family".to_string()))?
            ))
        };

        let vertices: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(
                db.cf_handle("vertices")
                    .ok_or_else(|| GraphError::StorageError("Failed to open vertices column family".to_string()))?
            ))
        };

        let edges: Arc<BoundColumnFamily<'static>> = unsafe {
            std::mem::transmute(Arc::new(
                db.cf_handle("edges")
                    .ok_or_else(|| GraphError::StorageError("Failed to open edges column family".to_string()))?
            ))
        };

        // WAL MANAGER — REQUIRED FIELD
        let canonical_path = db_path_owned.parent().unwrap_or(&db_path_owned).to_path_buf();
        let wal_manager = Arc::new(
            RocksDBWalManager::new(canonical_path, port)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create RocksDBWalManager: {}", e)))?
        );

        #[cfg(feature = "with-openraft-rocksdb")]
        let raft_storage = {
            let raft_db_path = db_path_owned.join("raft");
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
        let running = Arc::new(tokio::sync::Mutex::new(true));

        // Build and insert daemon — CLONE ALL ARC VALUES
        pool.daemons.insert(
            port,
            Arc::new(RocksDBDaemon {
                port,
                db_path: db_path_owned.clone(),
                db: db.clone(),
                kv_pairs: kv_pairs.clone(),
                vertices: vertices.clone(),
                edges: edges.clone(),
                wal_manager: wal_manager.clone(),
                running: running.clone(),
                shutdown_tx,
                zmq_context: zmq_context.clone(),
                zmq_thread: Arc::new(tokio::sync::Mutex::new(None)),
                #[cfg(feature = "with-openraft-rocksdb")]
                raft: None,
                #[cfg(feature = "with-openraft-rocksdb")]
                raft_storage,
                #[cfg(feature = "with-openraft-rocksdb")]
                node_id: port as u64,
            }),
        );

        // Start ZMQ server in background — ALL VALUES OWNED OR CLONED
        let zmq_thread_handle = {
            let zmq_context_thread = zmq_context.clone();
            let db_clone = db.clone();
            let kv_clone = kv_pairs.clone();
            let vert_clone = vertices.clone();
            let edge_clone = edges.clone();
            let wal_manager_clone = wal_manager.clone();
            let running_clone = running.clone();
            let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
            let db_path_thread = db_path_owned.clone();

            std::thread::spawn(move || -> GraphResult<()> {
                let rt = tokio::runtime::Runtime::new()
                    .expect("Failed to create runtime for ZMQ thread");
                rt.block_on(async {
                    let socket_raw = zmq_context_thread
                        .socket(REP)
                        .map_err(|e| GraphError::StorageError(format!("ZMQ socket create failed: {}", e)))?;

                    let mut bound = false;
                    for i in 0..5 {
                        match socket_raw.bind(&endpoint) {
                            Ok(_) => {
                                bound = true;
                                info!("ZMQ bound on attempt {}", i + 1);
                                break;
                            }
                            Err(e) => {
                                warn!("ZMQ bind attempt {} failed: {}", i + 1, e);
                                if i < 4 {
                                    tokio::time::sleep(TokioDuration::from_millis(100 * (i + 1) as u64)).await;
                                }
                            }
                        }
                    }

                    if !bound {
                        return Err(GraphError::StorageError("ZMQ failed to bind after retries".into()));
                    }

                    let zmq_socket = Arc::new(TokioMutex::new(socket_raw));
                    RocksDBDaemon::run_zmq_server_lazy(
                        port,
                        RocksDBConfig::default(),
                        running_clone,
                        zmq_socket,
                        endpoint,
                        db_clone,
                        kv_clone,
                        vert_clone,
                        edge_clone,
                        wal_manager_clone,
                        db_path_thread,
                    )
                    .await
                })
            })
        };

        // Store thread handle
        if let Some(daemon) = pool.daemons.get(&port) {
            *daemon.zmq_thread.lock().await = Some(zmq_thread_handle);
        }

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
            let start_time = std::time::Instant::now();
            let result = self.insert_to_node(*port, key, value).await;
            let elapsed_ms = start_time.elapsed().as_millis() as u64;
            match result {
                Ok(_) => {
                    success_count += 1;
                    println!("===> REPLICATED INSERT: Success on node {}", port);
                    self.load_balancer.update_node_health(*port, true, elapsed_ms).await;
                }
                Err(e) => {
                    let error_clone = e.clone(); // Clone the error before using it
                    errors.push((*port, error_clone));
                    println!("===> REPLICATED INSERT: Failed on node {}: {:?}", port, e);
                    self.load_balancer.update_node_health(*port, false, elapsed_ms).await;
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
        let _lock = get_cluster_init_lock().await.lock().await;  // HOLD HERE
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

    // Assuming necessary imports are present, specifically:
    // use tokio::fs as tokio_fs;
    // use std::io::ErrorKind; 
    // use std::path::Path; // Although we now avoid Path::exists()

    // ─────────────────────────────────────────────────────────────────────────────
    // UPDATED _initialize_cluster_core — FULL WAL REPLAY + LEADER LOGIC
    // ─────────────────────────────────────────────────────────────────────────────
    async fn _initialize_cluster_core(
        &mut self,
        storage_config: &StorageConfig,
        config: &RocksDBConfig,
        cli_port: Option<u16>,
        client: Option<(RocksDBClient, Arc<TokioMutex<ZmqSocketWrapper>>)>,
        existing_db: Option<Arc<DB>>,
    ) -> GraphResult<()> {
        println!("===> IN _initialize_cluster_core");
        info!("Starting initialization of RocksDBDaemonPool");

        // REMOVED: let _guard = get_cluster_init_lock().await.lock().await;
        // → Lock is already held in initialize_cluster()

        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("RocksDBDaemonPool already initialized, skipping");
            println!("===> WARNING: ROCKSDB DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            
            // Ensure the node is marked healthy in the load balancer even if already initialized
            let port = cli_port.unwrap_or(config.port.unwrap_or(DEFAULT_STORAGE_PORT));
            self.load_balancer.update_node_health(port, true, 0).await;
            println!("===> [LB] NODE {} MARKED HEALTHY IN LOAD BALANCER (ALREADY INITIALIZED)", port);
            return Ok(());
        }

        const DEFAULT_STORAGE_PORT: u16 = 8052;
        let port = cli_port.unwrap_or(config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        info!("Canonical port: {}", port);
        println!("===> CANONICAL PORT: {}", port);

        // 1. REACHABILITY CHECK
        if self.is_zmq_reachable(port).await.unwrap_or(false) {
            info!("Daemon already running on port {}", port);
            println!("===> DAEMON ALREADY RUNNING ON PORT {}", port);
            self.load_balancer.update_node_health(port, true, 0).await;
            *initialized = true;
            return Ok(());
        }

        // 2. CLEAN STALE IPC
        let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
        if tokio_fs::metadata(&ipc_path).await.is_ok() {
            warn!("Stale IPC socket found at {}. Removing.", ipc_path);
            println!("===> WARNING: STALE IPC SOCKET FOUND AT {}. REMOVING.", ipc_path);
            let _ = tokio_fs::remove_file(&ipc_path).await;
        }

        // 3. PER-PORT PATH
        let db_path = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&PathBuf::from(DEFAULT_DATA_DIRECTORY))
            .join("rocksdb")
            .join(port.to_string());
        info!("Using RocksDB path: {:?}", db_path);
        println!("===> USING ROCKSDB PATH: {:?}", db_path);

        // 4. CREATE DIR
        if tokio_fs::metadata(&db_path).await.is_err() {
            tokio_fs::create_dir_all(&db_path).await
                .map_err(|e| GraphError::Io(e.to_string()))?;
            tokio_fs::set_permissions(&db_path, fs::Permissions::from_mode(0o700))
                .await
                .map_err(|e| GraphError::Io(e.to_string()))?;
        }

        // 5. FORCE UNLOCK
        RocksDBClient::force_unlock(&db_path).await?;
        info!("Performed force unlock on RocksDB at {:?}", db_path);
        println!("===> PERFORMED FORCE UNLOCK ON ROCKSDB AT {:?}", db_path);

        // 6. CREATE DAEMON
        let mut daemon_config = config.clone();
        daemon_config.path = db_path.clone();
        daemon_config.port = Some(port);
        info!("Creating RocksDBDaemon with config: {:?}", daemon_config);
        println!("===> CREATING ROCKSDB DAEMON WITH CONFIG: {:?}", daemon_config);

        let (daemon, mut ready_rx) = timeout(
            TokioDuration::from_secs(15),
            RocksDBDaemon::new(daemon_config.clone())
        )
        .await
        .map_err(|_| GraphError::StorageError(format!("Timeout creating RocksDBDaemon on port {}", port)))?
        .map_err(|e| e)?;

        // 7. WAIT FOR ZMQ (NON-BLOCKING)
        let zmq_ready = if let Ok(Some(_)) = timeout(TokioDuration::from_secs(10), ready_rx.recv()).await {
            info!("ZMQ readiness confirmed for port {}", port);
            println!("===> ZMQ READINESS CONFIRMED FOR PORT {}", port);
            // Mark the node as healthy after ZMQ readiness is confirmed
            self.load_balancer.update_node_health(port, true, 0).await;
            println!("===> [LB] NODE {} MARKED HEALTHY IN LOAD BALANCER", port);
            true
        } else {
            warn!("ZMQ readiness not confirmed for port {}, proceeding anyway", port);
            println!("===> WARNING: ZMQ READINESS NOT CONFIRMED FOR PORT {}, PROCEEDING ANYWAY", port);
            false
        };

        // 8. VERIFY IPC
        if tokio_fs::metadata(&ipc_path).await.is_err() {
            warn!("ZMQ IPC file not created at {}, proceeding anyway", ipc_path);
            println!("===> WARNING: ZMQ IPC FILE NOT CREATED AT {}, PROCEEDING ANYWAY", ipc_path);
        } else {
            info!("ZMQ IPC file verified at {}", ipc_path);
            println!("===> ZMQ IPC FILE VERIFIED AT {}", ipc_path);
        }

        // 9. REGISTER
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id() as u32,
            ip_address: config.host.clone().unwrap_or("127.0.0.1".to_string()),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("rocksdb".to_string()),
            last_seen_nanos: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64,
            zmq_ready,
            engine_synced: true,
        };
        daemon_registry.register_daemon(daemon_metadata).await
            .map_err(|e| GraphError::StorageError(format!("Failed to register daemon on port {}: {}", port, e)))?;

        // 10. STORE
        self.daemons.insert(port, Arc::new(daemon));
        // Make sure the node is marked healthy in the load balancer after storing the daemon
        self.load_balancer.update_node_health(port, true, 0).await;
        *initialized = true;

        // 11. HEALTH MONITOR
        let health_config = HealthCheckConfig {
            interval: TokioDuration::from_secs(10),
            connect_timeout: TokioDuration::from_secs(2),
            response_buffer_size: 1024,
        };
        self.start_health_monitoring(health_config).await;

        info!("RocksDBDaemonPool initialized successfully on port {}", port);
        println!("===> ROCKSDB DAEMON POOL INITIALIZED SUCCESSFULLY ON PORT {}", port);
        Ok(())
    }

    /// ---------------------------------------------------------------------------
    /// Helper utilities
    /// ---------------------------------------------------------------------------
    
    async fn is_dir_empty(path: &Path) -> bool {
        if let Ok(mut dir) = tokio_fs::read_dir(path).await {
            dir.next_entry().await.ok().flatten().is_none()
        } else {
            true
        }
    }

    /// Copy data files only (no database opening) to avoid lock contention
    fn copy_data_files_only<'a>(
        source: &'a PathBuf, 
        target: &'a PathBuf
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = GraphResult<()>> + Send + 'a>> {
        Box::pin(async move {
            info!("Copying data files from {:?} to {:?}", source, target);
            println!("===> COPYING DATA FILES FROM {:?} TO {:?}", source, target);

            if !source.exists() {
                return Err(GraphError::StorageError(format!("Source does not exist: {:?}", source)));
            }

            tokio_fs::create_dir_all(target).await
                .map_err(|e| GraphError::Io(format!("Failed to create target: {}", e)))?;

            let mut entries = tokio_fs::read_dir(source).await
                .map_err(|e| GraphError::Io(format!("Failed to read source: {}", e)))?;

            while let Some(entry) = entries.next_entry().await
                .map_err(|e| GraphError::Io(format!("Failed to read entry: {}", e)))? {
                
                let source_path = entry.path();
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();
                let target_path = target.join(&file_name);

                // Skip lock files and temp files
                if file_name_str.contains("LOCK") || file_name_str.contains(".lck") 
                    || file_name_str.contains(".tmp") || file_name_str.contains(".temp") {
                    info!("Skipping lock/temp file: {}", file_name_str);
                    continue;
                }

                let metadata = entry.metadata().await
                    .map_err(|e| GraphError::Io(format!("Failed to get metadata: {}", e)))?;

                if metadata.is_dir() {
                    tokio_fs::create_dir_all(&target_path).await
                        .map_err(|e| GraphError::Io(format!("Failed to create dir: {}", e)))?;
                    Self::copy_data_files_only(&source_path, &target_path).await?;
                } else {
                    tokio_fs::copy(&source_path, &target_path).await
                        .map_err(|e| GraphError::Io(format!("Failed to copy file: {}", e)))?;
                }
            }

            info!("Successfully copied data to {:?}", target);
            println!("===> SUCCESSFULLY COPIED DATA TO {:?}", target);
            Ok(())
        })
    }

    /// Register a newly-started daemon as a follower of the canonical directory.
    async fn register_follower(canonical_path: &Path, port: u16) {
        let mut map = CANONICAL_DB_MAP.write().await;
        map.entry(canonical_path.to_path_buf()).or_default().push(port);
    }

    /// Return **all** ports that share the same canonical directory (including self).
    async fn followers_of(canonical_path: &Path) -> Vec<u16> {
        let map = CANONICAL_DB_MAP.read().await;
        map.get(canonical_path).cloned().unwrap_or_default()
    }

    /// ---------------------------------------------------------------------------
    /// Helper utilities
    /// ---------------------------------------------------------------------------

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
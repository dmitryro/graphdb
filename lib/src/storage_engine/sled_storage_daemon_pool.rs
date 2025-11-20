use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::str::FromStr; 
use std::io::{Cursor, Read, Write};
use sled::{Config, Db, IVec, Tree, Batch};
use tokio::io::{AsyncWriteExt, AsyncSeekExt, AsyncBufReadExt, BufReader, SeekFrom, ErrorKind};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex as TokioMutex, OnceCell, RwLock, mpsc};
use tokio::time::{sleep, Duration as TokioDuration, timeout, interval};
use tokio::task::{self, JoinError,  JoinHandle, spawn_blocking };
use tokio::fs as tokio_fs;
use log::{info, debug, warn, error};
use crate::config::{SledConfig, SledDaemon, SledDaemonPool, SledStorage, StorageConfig, StorageEngineType, 
                    DAEMON_REGISTRY_DB_PATH, DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, ReplicationStrategy, 
                    NodeHealth, LoadBalancer, HealthCheckConfig, SledClientMode, SledClient,
                    DbArc, daemon_api_storage_engine_type_to_string };
use crate::database::Database;
use crate::storage_engine::sled_client::{ ZmqSocketWrapper };
use crate::storage_engine::storage_utils::{create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex};
use crate::storage_engine::storage_engine::{ GraphStorageEngine, get_global_storage_registry };
use crate::query_parser::cypher_parser::execute_cypher_from_string;
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
pub use crate::storage_engine::sled_wal_manager::{ SledWalManager, SledWalOperation };
use models::{Edge, Identifier, Vertex, };
use models::errors::{GraphError, GraphResult};
use models::identifiers::{SerializableUuid};
use serde_json::{json, Value};
use serde::{Deserialize, Serialize};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::db_daemon_registry::{GLOBAL_DB_DAEMON_REGISTRY, DBDaemonMetadata};
use crate::daemon::daemon_management::{parse_cluster_range, is_daemon_running,  get_ipc_addr, 
                                       find_pid_by_port, is_port_free, is_pid_running,
                                       check_process_status_by_port,}; 
use std::time::{SystemTime, UNIX_EPOCH};
use futures::future::join_all;
use uuid::Uuid;
use zmq::{self, Context as ZmqContext, Socket as ZmqSocket, Error as ZmqError, REP, REQ, DONTWAIT, PollEvents, PollItem};
use std::fs::Permissions;
use std::os::unix::{fs::PermissionsExt, io::AsRawFd, io::RawFd};
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use nix::unistd::{ Pid as NixPid };
use nix::sys::signal::{kill, Signal};
use async_trait::async_trait;
use base64::engine::general_purpose;
use base64::Engine;
use std::fs::{self};
use lazy_static::lazy_static;
use std::sync::Mutex as StdMutex;
use once_cell::sync::Lazy;
use bincode::{Encode, Decode, decode_from_slice, config::standard};
use indexing_service::fulltext::FullTextIndex;

#[cfg(feature = "with-openraft-sled")]
use {
    openraft::{Config as RaftConfig, NodeId, Raft, RaftNetwork, RaftStorage, BasicNode},
    openraft_sled::SledRaftStorage,
    tokio::net::TcpStream,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

lazy_static! {
    static ref SLED_DAEMON_PORT_LOCKS: StdMutex<HashMap<u16, Arc<TokioMutex<()>>>> = 
        StdMutex::new(HashMap::new());
}

pub async fn get_sled_daemon_port_lock(port: u16) -> Arc<TokioMutex<()>> {
    let mut locks = SLED_DAEMON_PORT_LOCKS.lock().unwrap();
    locks.entry(port)
        .or_insert_with(|| Arc::new(TokioMutex::new(())))
        .clone()
}

// Key prefixes for distinguishing data types in the Sled Tree
const V_PREFIX: &[u8] = b"v_";
const E_PREFIX: &[u8] = b"e_";
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

static PROCESSED_REQUESTS: Lazy<TokioMutex<HashSet<String>>> = 
    Lazy::new(|| TokioMutex::new(HashSet::new()));
static CLUSTER_INIT_LOCK: OnceCell<TokioMutex<()>> = OnceCell::const_new();

async fn get_cluster_init_lock() -> &'static TokioMutex<()> {
    CLUSTER_INIT_LOCK
        .get_or_init(|| async { TokioMutex::new(()) })
        .await
}

pub static FULLTEXT_INDEX: Lazy<Arc<FullTextIndex>> = Lazy::new(|| {
    let index_dir = std::path::PathBuf::from("/opt/graphdb/indexes/fulltext");
    std::fs::create_dir_all(&index_dir).expect("Failed to create fulltext index directory");
    Arc::new(FullTextIndex::new(&index_dir).expect("Failed to initialize Tantivy full-text index"))
});


#[macro_export]
macro_rules! handle_sled_op {
    ($op:expr, $err_msg:expr) => {
        $op.map_err(|e| {
            error!("{}: {}", $err_msg, e);
            GraphError::StorageError(format!("{}: {}", $err_msg, e))
        })
    };
}

// ---------------------------------------------------------------------------
// Shared WAL – one file, many readers, one writer (leader)
// ---------------------------------------------------------------------------
static SHARED_WAL_DIR: Lazy<PathBuf> = Lazy::new(|| {
    PathBuf::from(format!(
        "{}/{}/shared_wal",
        DEFAULT_DATA_DIRECTORY,
        StorageEngineType::Sled.to_string().to_lowercase()
    ))
});

/// Full path to the WAL **file**
static SHARED_WAL_PATH: Lazy<PathBuf> = Lazy::new(|| {
    SHARED_WAL_DIR.join("log.wal")
});

/// Helper – returns the WAL file path
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
            Ok(_) => {
                // Successfully created lock file, now write leader info
                let info = format!(
                    "leader_port={}\npid={}\nts={}\n",
                    my_port,
                    std::process::id(),
                    chrono::Utc::now().timestamp()
                );
                
                // Write to leader_file (not lock_file!)
                match tokio_fs::write(&leader_file, info.as_bytes()).await {
                    Ok(_) => {
                        info!("Became WAL leader (port {}) for cluster {:?}", my_port, canonical);
                        return Ok(true);
                    }
                    Err(e) => {
                        error!("Failed to write leader info: {}", e);
                        let _ = tokio_fs::remove_file(&lock_file).await;
                        return Ok(false);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Lock exists, read leader info
                match tokio_fs::read_to_string(&leader_file).await {
                    Ok(content) => {
                        let leader_port = content
                            .lines()
                            .find_map(|l| l.strip_prefix("leader_port=").map(|s| s.trim()))
                            .and_then(|s| s.parse::<u16>().ok())
                            .unwrap_or(0);
                        
                        // CRITICAL FIX: If we ARE the leader port, we are the leader!
                        if leader_port == my_port {
                            info!("We are already the WAL leader on port {}", my_port);
                            return Ok(true);
                        }
                        
                        info!("WAL leader is on port {} (we are follower on port {})", leader_port, my_port);
                        return Ok(false);
                    }
                    Err(_) => {
                        warn!("Leader lock exists but no leader.info — forcing promotion");
                        let _ = tokio_fs::remove_file(&lock_file).await;
                        let _ = tokio_fs::remove_file(&leader_file).await;
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
async fn append_wal(op: &WalOp) -> GraphResult<u64> {
    let data = bincode::encode_to_vec(op, standard())
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
    Ok(offset)
}

/// Tail the WAL from `start_offset` and replay locally.
async fn replay_wal_from(
    db: &Arc<sled::Db>,
    start_offset: u64,
) -> GraphResult<()> {
    let file = match tokio_fs::File::open(&*SHARED_WAL_PATH).await {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(GraphError::Io(e.to_string())),
    };

    let mut file = file;
    file.seek(SeekFrom::Start(start_offset)).await?;

    let reader = BufReader::new(file);
    let mut stream = tokio::io::BufReader::new(reader);
    let mut buf = Vec::new();

    loop {
        buf.clear();
        match stream.read_until(b'\n', &mut buf).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                if buf.is_empty() || buf == b"\n" {
                    continue;
                }
                if buf.last() == Some(&b'\n') {
                    buf.pop();
                }
                // Decode as SledWalOperation
                let (op, _) = bincode::decode_from_slice::<SledWalOperation, _>(&buf, standard())
                    .map_err(|e| GraphError::StorageError(e.to_string()))?;
                SledDaemon::apply_op_locally(db, &op).await?;
            }
            Err(e) => {
                error!("Error reading WAL: {}", e);
                return Err(GraphError::Io(e.to_string()));
            }
        }
    }
    Ok(())
}

/// WAL operation definition
#[derive(Encode, Decode, Clone)]
pub enum WalOp {
    Put { tree: String, key: Vec<u8>, value: Vec<u8> },
    Delete { tree: String, key: Vec<u8> },
}

impl SledDaemon {
    /* -------------------------------------------------------------------------
       Background WAL sync – unchanged except it now receives **Arc<Tree>**
       ------------------------------------------------------------------------- */
    pub fn start_background_wal_sync(
        port: u16,
        wal_manager: Arc<SledWalManager>,
        canonical_path: &PathBuf,
        db: Arc<sled::Db>,
        vertices: Arc<sled::Tree>,
        edges: Arc<sled::Tree>,
        kv_pairs: Arc<sled::Tree>,
        running: Arc<TokioMutex<bool>>,
    ) {
        let canonical_path = canonical_path.clone();
        tokio::spawn(async move {
            println!("===> STARTING BACKGROUND WAL SYNC FOR PORT {}", port);
            while *running.lock().await {
                sleep(TokioDuration::from_secs(2)).await;

                let offset_key = format!("__wal_offset_port_{}", port);
                let last_offset = db.get(&offset_key)
                    .ok()
                    .flatten()
                    .and_then(|v| String::from_utf8(v.to_vec()).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                let current_lsn = match wal_manager.current_lsn().await {
                    Ok(lsn) => lsn,
                    Err(_) => continue,
                };
                if current_lsn <= last_offset { continue; }

                let ops = match wal_manager.read_since(last_offset).await {
                    Ok(o) => o,
                    Err(_) => continue,
                };
                if ops.is_empty() { continue; }

                let mut latest_offset = last_offset;
                let mut applied = 0;

                for (offset, op) in ops {
                    let op_size = bincode::encode_to_vec(&op, bincode::config::standard())
                                    .map(|v| v.len() as u64)
                                    .unwrap_or(0);

                    // All arms now return the **same** type:
                    // Option<Result<Option<IVec>, GraphError>>
                    let result: Option<Result<Option<IVec>, GraphError>> = match &op {
                        SledWalOperation::Put { tree, key, value } => {
                            let t = match tree.as_str() {
                                "kv_pairs" => Some(&kv_pairs),
                                "vertices" => Some(&vertices),
                                "edges"    => Some(&edges),
                                _          => None,
                            };
                            t.map(|t| {
                                t.insert(key.clone(), value.clone().to_vec())
                                    .map_err(|e| e.into())
                            })
                        }
                        SledWalOperation::Delete { tree, key } => {
                            let t = match tree.as_str() {
                                "kv_pairs" => Some(&kv_pairs),
                                "vertices" => Some(&vertices),
                                "edges"    => Some(&edges),
                                _          => None,
                            };
                            t.map(|t| t.remove(key.clone()).map_err(|e| e.into()))
                        }
                        SledWalOperation::Flush { .. } => {
                            // flush_async returns Result<usize, sled::Error>
                            // On success: return Some(Ok(None))
                            // On error: return None
                            match db.flush_async().await {
                                Ok(_) => Some(Ok(None)),
                                Err(e) => {
                                    error!("Flush failed during WAL sync: {}", e);
                                    None
                                }
                            }
                        }
                    };

                    // `result` is now `Option<Result<…>>`
                    if let Some(Ok(_)) = result {
                        applied += 1;
                    } else {
                        break;
                    }

                    latest_offset = offset + 4 + op_size;
                }

                if latest_offset > last_offset && applied > 0 {
                    let _ = db.insert(&offset_key, latest_offset.to_string().as_bytes());
                    let _ = db.flush_async().await;
                    println!(
                        "===> PORT {} SYNCED {} OPS (OFFSET {} to {})",
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
                            if db_path.join("db").exists() || db_path.join("CURRENT").exists() {
                                ports.push(p);
                            }
                        }
                    }
                }
            }
        }
        ports
    }

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
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                }
            }
        }
    }
    
    /// Apply a WAL operation locally to the Sled database
    pub async fn apply_op_locally(
        db: &Arc<sled::Db>,
        op: &SledWalOperation,
    ) -> GraphResult<()> {
        info!("APPLYING WAL OP LOCALLY: {:?}", op);
        println!("====> APPLYING WAL OP LOCALLY: {:?}", op);

        match op {
            SledWalOperation::Put { tree, key, value } => {
                println!("====> Opening tree: {}", tree);
                let t = match tree.as_str() {
                    "kv_pairs" => db.open_tree("kv_pairs")?,
                    "vertices" => db.open_tree("vertices")?,
                    "edges" => db.open_tree("edges")?,
                    _ => {
                        warn!("UNKNOWN TREE IN WAL OP: {}", tree);
                        return Err(GraphError::StorageError(format!("Unknown tree: {}", tree)));
                    }
                };
                println!("====> Tree opened successfully, inserting key (len={})", key.len());

                //  >>>  MISSING MUTATION ADDED BACK  <<<
                t.insert(key.as_slice(), value.as_slice())
                    .map_err(|e| {
                        error!("PUT FAILED: {}", e);
                        GraphError::StorageError(format!("Put failed on {}: {}", tree, e))
                    })?;

                println!("====> INSERT COMPLETE");
                info!("PUT SUCCESS: TREE={}, key_len={}, value_len={}", tree, key.len(), value.len());
            }

            SledWalOperation::Delete { tree, key } => {
                println!("====> Opening tree for delete: {}", tree);
                let t = match tree.as_str() {
                    "kv_pairs" => db.open_tree("kv_pairs")?,
                    "vertices" => db.open_tree("vertices")?,
                    "edges" => db.open_tree("edges")?,
                    _ => {
                        warn!("UNKNOWN TREE IN WAL DELETE: {}", tree);
                        return Err(GraphError::StorageError(format!("Unknown tree: {}", tree)));
                    }
                };

                //  >>>  MISSING MUTATION ADDED BACK  <<<
                t.remove(key.as_slice())
                    .map_err(|e| GraphError::StorageError(format!("Delete failed on {}: {}", tree, e)))?;

                println!("====> DELETE COMPLETE");
                info!("DELETE SUCCESS: TREE={}, key_len={}", tree, key.len());
            }

            SledWalOperation::Flush { tree } => {
                println!("====> Flushing tree: {}", tree);
                if tree == "default" || tree.is_empty() {
                    //  >>>  FLUSH REALLY EXECUTED  <<<
                    db.flush_async().await
                        .map_err(|e| GraphError::StorageError(format!("Flush all failed: {}", e)))?;
                    info!("FLUSH SUCCESS: all trees");
                } else {
                    let tree_handle = match tree.as_str() {
                        "kv_pairs" => Some(db.open_tree("kv_pairs")?),
                        "vertices" => Some(db.open_tree("vertices")?),
                        "edges" => Some(db.open_tree("edges")?),
                        _ => {
                            warn!("FLUSH IGNORED: unknown tree {}", tree);
                            None
                        }
                    };
                    if let Some(t) = tree_handle {
                        //  >>>  FLUSH REALLY EXECUTED  <<<
                        t.flush_async().await
                            .map_err(|e| GraphError::StorageError(format!("Flush {} failed: {}", tree, e)))?;
                        info!("FLUSH SUCCESS: TREE={}", tree);
                    }
                }
                println!("====> FLUSH COMPLETE");
            }
        }
        println!("====> apply_op_locally COMPLETED SUCCESSFULLY");
        Ok(())
    }

    /// Replay WAL operations from shared WAL using per-port offset tracking
    pub async fn replay_from_all_wals(
        canonical_path: &PathBuf,
        current_port: u16,
        db: &Arc<sled::Db>,
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

        let wal_mgr = SledWalManager::new(canonical_path.clone(), current_port).await?;
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

        let mut latest_offset = last_offset;
        let mut applied = 0;

        for (offset, op) in operations {
            let op_size = bincode::encode_to_vec(&op, bincode::config::standard())
                            .map(|v| v.len() as u64)
                            .unwrap_or(0);

            if let Err(e) = SledDaemon::apply_op_locally(db, &op).await {
                error!("Replay failed at {}: {}", offset, e);
                break;
            }

            applied += 1;
            latest_offset = offset + 4 + op_size;
        }

        if latest_offset > last_offset {
            let _ = db.insert(&offset_key.as_bytes(), latest_offset.to_string().as_bytes());
            let _ = db.flush_async().await;
            println!("===> REPLAYED {} OPS ({} → {})", applied, last_offset, latest_offset);
        }

        Ok(())
    }

    /// Start background task to replicate WAL to other ports
    pub fn start_wal_replication(
        port: u16,
        wal_manager: Arc<SledWalManager>,
        db: Arc<sled::Db>,
        vertices: Tree,
        edges: Tree,
        kv_pairs: Tree,
        running: Arc<TokioMutex<bool>>,
    ) {
        tokio::spawn(async move {
            while *running.lock().await {
                // Sleep for 1 second between replication cycles
                tokio::time::sleep(TokioDuration::from_secs(1)).await;

                // Replication is passive - other daemons will read our WAL
                // Just ensure our WAL is flushed
                if let Ok(current_lsn) = wal_manager.current_lsn().await {
                    debug!("Port {} WAL at LSN {}", port, current_lsn);
                }
            }
        });
    }

    // ---------------------------------------------------------------------------
    //  SledDaemon::new
    // ---------------------------------------------------------------------------
    pub async fn new(config: SledConfig) -> GraphResult<(Self, mpsc::Receiver<()>)> {
        println!("===> SledDaemon::new CALLED");
        let port = config.port.ok_or_else(|| {
            error!("No port specified in SledConfig");
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;

        let port_lock = get_sled_daemon_port_lock(port).await;
        let _guard = port_lock.lock().await;
        println!("===> ACQUIRED PER-PORT INIT LOCK FOR PORT {}", port);

        let db_path = {
            let base = PathBuf::from(&config.path);
            if base.file_name().and_then(|n| n.to_str()) == Some(&port.to_string()) {
                base
            } else {
                base.join(port.to_string())
            }
        };

        if !db_path.exists() {
            tokio_fs::create_dir_all(&db_path)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create directory: {}", e)))?;
        }

        let cache_capacity = config.cache_capacity.unwrap_or(1024 * 1024 * 1024);
        let (db_arc, vertices, edges, kv_pairs) = Self::open_sled_db_and_trees(
            &config,
            &db_path,
            cache_capacity,
            config.use_compression,
        )
        .await?;

        let wal_dir = db_path.parent().unwrap_or(&db_path).to_path_buf();
        let wal_manager = Arc::new(
            SledWalManager::new(wal_dir.clone(), port)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create WAL manager: {}", e)))?,
        );

        let canonical_path = db_path.parent().unwrap().to_path_buf();
        let replay_path = canonical_path.clone();
        let replay_db = db_arc.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::replay_from_all_wals(&replay_path, port, &replay_db).await {
                error!("WAL replay failed for port {}: {}", port, e);
            }
        });

        let (ready_tx, ready_rx) = mpsc::channel(1);
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let running = Arc::new(TokioMutex::new(false));

        // ----  clones for the OS thread  ----
        let config_cloned     = config.clone();
        let db_path_cloned    = db_path.clone();
        let db_arc_cloned     = db_arc.clone();
        let vertices_cloned   = vertices.clone();
        let edges_cloned      = edges.clone();
        let kv_pairs_cloned   = kv_pairs.clone();
        let wal_manager_cloned = wal_manager.clone();
        let running_clone     = running.clone();

        let zmq_thread = std::thread::spawn(move || {
            // 1.  create runtime
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("new current-thread runtime for ZMQ");

            // 2.  single block_on: everything async inside
            rt.block_on(async {
                // clean stale socket
                let ipc_path = endpoint.strip_prefix("ipc://").unwrap();
                let _ = std::fs::remove_file(ipc_path);

                let ctx = ZmqContext::new();
                let socket = ctx.socket(REP).expect("create ZMQ socket");

                let mut bound = false;
                for i in 0..5 {
                    if socket.bind(&endpoint).is_ok() {
                        bound = true;
                        // signal readiness
                        let _ = ready_tx.send(()).await;
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * (i + 1) as u64)).await;
                }
                if !bound {
                    eprintln!("===> ZMQ bind failed after retries on port {}", port);
                    return;
                }

                // mark running
                *running_clone.lock().await = true;

                // registry
                let registry = GLOBAL_DAEMON_REGISTRY.get().await;
                let meta = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: std::process::id(),
                    ip_address: "127.0.0.1".to_string(),
                    data_dir: Some(db_path_cloned.clone()),
                    config_path: None,
                    engine_type: Some("sled".to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    zmq_ready: true,
                    engine_synced: true,
                };
                let _ = registry.update_daemon_metadata(meta).await;

                // serve
                let socket_arc = Arc::new(TokioMutex::new(socket));
                let _ = SledDaemon::run_zmq_server_lazy(
                    port,
                    config_cloned,
                    running_clone,
                    socket_arc,
                    endpoint,
                    db_arc_cloned,
                    Arc::new(kv_pairs_cloned),
                    Arc::new(vertices_cloned),
                    Arc::new(edges_cloned),
                    wal_manager_cloned,
                    db_path_cloned,
                )
                .await;
            });
        });

        // background WAL sync (main runtime)
        Self::start_background_wal_sync(
            port,
            wal_manager.clone(),
            &canonical_path,
            db_arc.clone(),
            Arc::new(vertices.clone()),
            Arc::new(edges.clone()),
            Arc::new(kv_pairs.clone()),
            running.clone(),
        );

        let daemon = Self {
            port,
            db_path,
            db: db_arc,
            vertices,
            edges,
            kv_pairs,
            running,
            wal_manager,
            #[cfg(feature = "with-openraft-sled")]
            raft_storage: Arc::new(openraft_sled::SledRaftStorage::new(db_arc.clone())),
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
            #[cfg(feature = "with-openraft-sled")]
            raft: None,
            zmq_thread: Some(zmq_thread),
        };

        info!("SledDaemon created on port {}", port);
        Ok((daemon, ready_rx))
    }

    // ─────────────────────────────────────────────────────────────────────────────
    //  SledDaemon::new_with_client
    // ─────────────────────────────────────────────────────────────────────────────
    pub async fn new_with_db(
        config: SledConfig,
        existing_db: Arc<sled::Db>,
    ) -> GraphResult<(Self, mpsc::Receiver<()>)> {
        println!("===> SledDaemon::new_with_db CALLED - STEP 1");
        let port = config.port.ok_or_else(|| {
            error!("No port specified in SledConfig");
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;

        let port_lock = get_sled_daemon_port_lock(port).await;
        let _guard = port_lock.lock().await;
        println!("===> ACQUIRED PER-PORT INIT LOCK FOR PORT {}", port);

        let db_path = {
            let base = PathBuf::from(&config.path);
            if base.file_name().and_then(|n| n.to_str()) == Some(&port.to_string()) {
                base
            } else {
                base.join(port.to_string())
            }
        };
        println!("===> SledDaemon::new_with_db CALLED - STEP 2");
        if !db_path.exists() {
            tokio_fs::create_dir_all(&db_path)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create directory: {}", e)))?;
        }

        let db_clone = Arc::clone(&existing_db);
        let (vertices, edges, kv_pairs) = tokio::task::spawn_blocking(move || {
            let vertices = db_clone.open_tree("vertices")?;
            let edges = db_clone.open_tree("edges")?;
            let kv_pairs = db_clone.open_tree("kv_pairs")?;
            Ok::<_, sled::Error>((vertices, edges, kv_pairs))
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Task panic: {:?}", e)))?
        .map_err(|e| GraphError::StorageError(format!("Tree open failed: {}", e)))?;
        println!("===> SledDaemon::new_with_db CALLED - STEP 3");
        let wal_dir = db_path.parent().unwrap_or(&db_path).to_path_buf();
        let wal_manager = Arc::new(
            SledWalManager::new(wal_dir.clone(), port)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create WAL manager: {}", e)))?,
        );
        println!("===> SledDaemon::new_with_db CALLED - STEP 4");
        let canonical_path = db_path.parent().unwrap().to_path_buf();
        let replay_path = canonical_path.clone();
        let replay_db = existing_db.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::replay_from_all_wals(&replay_path, port, &replay_db).await {
                error!("WAL replay failed for port {}: {}", port, e);
            }
        });

        let (ready_tx, ready_rx) = mpsc::channel(1);
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
        let running = Arc::new(TokioMutex::new(false));
        let running_clone = running.clone();
        println!("===> SledDaemon::new_with_db CALLED - STEP 5");
        let config_cloned = config.clone();
        let db_path_cloned = db_path.clone();
        let db_arc_cloned = existing_db.clone();
        let vertices_cloned = vertices.clone();
        let edges_cloned = edges.clone();
        let kv_pairs_cloned = kv_pairs.clone();
        let wal_manager_cloned = wal_manager.clone();

        let zmq_thread = std::thread::spawn(move || {
            // Temporary runtime for ZMQ thread
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create ZMQ runtime");

            rt.block_on(async {
                // Remove stale IPC with retry on EINTR
                let ipc_path = endpoint.strip_prefix("ipc://").unwrap();
                for _ in 0..5 {
                    match std::fs::remove_file(ipc_path) {
                        Ok(_) => break,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(e) => {
                            warn!("Failed to remove stale IPC {}: {}", ipc_path, e);
                            break;
                        }
                    }
                }

                let ctx = ZmqContext::new();
                let socket = ctx.socket(REP).expect("Failed to create ZMQ socket");

                // Bind with retry
                let mut bound = false;
                for attempt in 1..=10 {
                    if socket.bind(&endpoint).is_ok() {
                        bound = true;
                        let _ = ready_tx.try_send(());
                        break;
                    }
                    warn!("ZMQ bind attempt {} failed, retrying...", attempt);
                    tokio::time::sleep(tokio::time::Duration::from_millis(200 * attempt)).await;
                }

                if !bound {
                    error!("===> ZMQ bind failed after retries on port {}", port);
                    return;
                }

                *running.lock().await = true;

                // Update registry
                let registry = GLOBAL_DAEMON_REGISTRY.get().await;
                let meta = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: std::process::id(),
                    ip_address: config_cloned.host.clone().unwrap_or("127.0.0.1".to_string()),
                    data_dir: Some(db_path_cloned.clone()),
                    config_path: None,
                    engine_type: Some("sled".to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    zmq_ready: true,
                    engine_synced: true,
                };
                let _ = registry.update_daemon_metadata(meta).await;

                let socket_arc = Arc::new(TokioMutex::new(socket));
                let _ = SledDaemon::run_zmq_server_lazy(
                    port,
                    config_cloned,
                    running,
                    socket_arc,
                    endpoint,
                    db_arc_cloned,
                    Arc::new(kv_pairs_cloned),
                    Arc::new(vertices_cloned),
                    Arc::new(edges_cloned),
                    wal_manager_cloned,
                    db_path_cloned,
                )
                .await;
            });
        });
        println!("===> SledDaemon::new_with_db CALLED - STEP 6");
        Self::start_background_wal_sync(
            port,
            wal_manager.clone(),
            &canonical_path,
            existing_db.clone(),
            Arc::new(vertices.clone()),
            Arc::new(edges.clone()),
            Arc::new(kv_pairs.clone()),
            running_clone.clone(),
        );

        let daemon = Self {
            port,
            db_path,
            db: existing_db,
            vertices,
            edges,
            kv_pairs,
            running: running_clone,
            wal_manager,
            #[cfg(feature = "with-openraft-sled")]
            raft_storage: Arc::new(openraft_sled::SledRaftStorage::new(existing_db.clone())),
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
            #[cfg(feature = "with-openraft-sled")]
            raft: None,
            zmq_thread: Some(zmq_thread),
        };

        info!("SledDaemon created with existing DB on port {}", port);
        Ok((daemon, ready_rx))
    }

    // ─────────────────────────────────────────────────────────────────────────────
    //  SledDaemon::new_with_client
    // ─────────────────────────────────────────────────────────────────────────────
    pub async fn new_with_client(
        config: SledConfig,
        _client: SledClient,
        _socket: Arc<TokioMutex<ZmqSocketWrapper>>,
    ) -> GraphResult<Self> {
        println!("SledDaemon =================> LET US SEE IF THIS WAS EVER CALLED");
        let port = config.port.ok_or_else(|| {
            error!("No port specified in SledConfig");
            GraphError::ConfigurationError("No port specified in SledConfig".to_string())
        })?;

        let port_lock = get_sled_daemon_port_lock(port).await;
        let _guard = port_lock.lock().await;

        let db_path = {
            let base = PathBuf::from(&config.path);
            if base.file_name().and_then(|n| n.to_str()) == Some(&port.to_string()) {
                base
            } else {
                base.join(port.to_string())
            }
        };

        if !db_path.exists() {
            tokio_fs::create_dir_all(&db_path)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create directory: {}", e)))?;
        }

        let cache_capacity = config.cache_capacity.unwrap_or(1024 * 1024 * 1024);
        let (db_arc, vertices, edges, kv_pairs) = Self::open_sled_db_and_trees(
            &config,
            &db_path,
            cache_capacity,
            config.use_compression,
        )
        .await?;

        let wal_dir = db_path.parent().unwrap_or(&db_path).to_path_buf();
        let wal_manager = Arc::new(
            SledWalManager::new(wal_dir.clone(), port)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create WAL manager: {}", e)))?,
        );

        let canonical_path = db_path.parent().unwrap().to_path_buf();
        let replay_path = canonical_path.clone();
        let replay_db = db_arc.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::replay_from_all_wals(&replay_path, port, &replay_db).await {
                error!("WAL replay failed for port {}: {}", port, e);
            }
        });

        let running = Arc::new(TokioMutex::new(false));
        let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);

        // ──────── Clone for ZMQ thread ────────
        let config_cloned = config.clone();
        let db_path_cloned = db_path.clone();
        let db_arc_cloned = db_arc.clone();
        let vertices_cloned = vertices.clone();
        let edges_cloned = edges.clone();
        let kv_pairs_cloned = kv_pairs.clone();
        let wal_manager_cloned = wal_manager.clone();
        let running_cloned = running.clone();

        // ──────── Dedicated ZMQ thread ────────
        let zmq_thread = std::thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("===> FATAL: Failed to create ZMQ runtime: {}", e);
                    return;
                }
            };

            rt.block_on(async {
                let ipc_path = endpoint.strip_prefix("ipc://").unwrap();
                let _ = std::fs::remove_file(ipc_path);

                let ctx = ZmqContext::new();
                let socket = match ctx.socket(REP) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("===> ZMQ socket error: {}", e);
                        return;
                    }
                };

                let mut bound = false;
                for i in 0..5 {
                    if socket.bind(&endpoint).is_ok() {
                        bound = true;
                        break;
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * (i + 1) as u64)).await;
                }
                if !bound {
                    eprintln!("===> ZMQ bind failed after retries");
                    return;
                }

                *running_cloned.lock().await = true;

                let registry = GLOBAL_DAEMON_REGISTRY.get().await;
                let meta = DaemonMetadata {
                    service_type: "storage".to_string(),
                    port,
                    pid: std::process::id(),
                    ip_address: "127.0.0.1".to_string(),
                    data_dir: Some(db_path_cloned.clone()),
                    config_path: None,
                    engine_type: Some("sled".to_string()),
                    last_seen_nanos: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                    zmq_ready: true,
                    engine_synced: true,
                };
                let _ = registry.update_daemon_metadata(meta).await;

                let socket_arc = Arc::new(TokioMutex::new(socket));
                let _ = SledDaemon::run_zmq_server_lazy(
                    port,
                    config_cloned,
                    running_cloned,
                    socket_arc,
                    endpoint,
                    db_arc_cloned,
                    Arc::new(kv_pairs_cloned),
                    Arc::new(vertices_cloned),
                    Arc::new(edges_cloned),
                    wal_manager_cloned,
                    db_path_cloned,
                )
                .await;
            });
        });

        // Background sync
        Self::start_background_wal_sync(
            port,
            wal_manager.clone(),
            &canonical_path,
            db_arc.clone(),
            Arc::new(vertices.clone()),
            Arc::new(edges.clone()),
            Arc::new(kv_pairs.clone()),
            running.clone(),
        );

        let daemon = Self {
            port,
            db_path,
            db: db_arc,
            vertices,
            edges,
            kv_pairs,
            running,
            wal_manager,
            #[cfg(feature = "with-openraft-sled")]
            raft_storage: Arc::new(openraft_sled::SledRaftStorage::new(db_arc.clone())),
            #[cfg(feature = "with-openraft-sled")]
            node_id: port as u64,
            #[cfg(feature = "with-openraft-sled")]
            raft: None,
            zmq_thread: Some(zmq_thread),
        };

        info!("SledDaemon created on port {}", port);
        Ok(daemon)
    }
    
    async fn run_zmq_server_lazy(
        port: u16,
        _config: SledConfig,
        running: Arc<TokioMutex<bool>>,
        zmq_socket: Arc<TokioMutex<ZmqSocket>>,
        endpoint: String,
        db: Arc<sled::Db>,
        kv_pairs: Arc<Tree>,
        vertices: Arc<Tree>,
        edges: Arc<Tree>,
        wal_manager: Arc<SledWalManager>,
        db_path: PathBuf,
    ) -> GraphResult<()> {
        info!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);
        println!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);

        // Configure socket once
        {
            let mut socket = zmq_socket.lock().await;
            socket.set_linger(0)?;
            socket.set_rcvtimeo(100)?; // Short – we poll + DONTWAIT
            socket.set_sndtimeo(1000)?;
            socket.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)?;
            socket.set_sndhwm(10000)?;
            socket.set_rcvhwm(10000)?;
            socket.set_immediate(true)?;
        }
        info!("ZeroMQ server configured for port {}", port);
        println!("===> ZEROMQ SERVER CONFIGURED FOR PORT {}", port);

        let mut consecutive_errors = 0;
        let poll_timeout_ms = 10;
        let raw_fd: RawFd = {
            let s = zmq_socket.lock().await;
            (&*s).as_raw_fd()
        };
        let mut poll_items = [PollItem::from_fd(raw_fd, zmq::POLLIN)];

        while *running.lock().await {
            let poll_result = zmq::poll(&mut poll_items, poll_timeout_ms);
            let msg: Vec<u8> = match poll_result {
                Ok(n) if n > 0 && poll_items[0].is_readable() => {
                    let mut full_msg = Vec::new();
                    let mut s = zmq_socket.lock().await;
                    loop {
                        match s.recv_msg(zmq::DONTWAIT) {
                            Ok(part) => {
                                full_msg.extend_from_slice(&part);
                                if !s.get_rcvmore()? {
                                    break;
                                }
                            }
                            Err(zmq::Error::EAGAIN) => {
                                drop(s);
                                tokio::time::sleep(TokioDuration::from_millis(1)).await;
                                s = zmq_socket.lock().await;
                                continue;
                            }
                            Err(e) => {
                                consecutive_errors += 1;
                                warn!("ZMQ recv error (attempt {}): {}", consecutive_errors, e);
                                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                    error!("Too many ZMQ errors – shutting down server on port {}", port);
                                    return Err(GraphError::ZmqError(String::from(format!("{:?}", e))));
                                }
                                full_msg.clear();
                                break;
                            }
                        }
                    }
                    if full_msg.is_empty() {
                        continue;
                    }
                    consecutive_errors = 0;
                    debug!("Received ZMQ message ({} bytes) on port {}", full_msg.len(), port);
                    full_msg
                }
                Ok(_) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(e) => {
                    error!("ZMQ poll error: {}", e);
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                    continue;
                }
            };

            if msg.is_empty() {
                let resp = json!({"status":"error","message":"Received empty message"});
                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                continue;
            }

            let request: Value = match serde_json::from_slice(&msg) {
                Ok(r) => r,
                Err(e) => {
                    let resp = json!({"status":"error","message":format!("Parse error: {}", e)});
                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                    continue;
                }
            };

            let request_id = request.get("request_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            {
                let mut processed = PROCESSED_REQUESTS.lock().await;
                if processed.contains(&request_id) {
                    debug!("Duplicate request_id {} on port {} – skipping", request_id, port);
                    let resp = json!({"status": "success", "note": "idempotent", "request_id": request_id});
                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                    continue;
                }
                processed.insert(request_id.clone());
            }

            let command = request.get("command").and_then(|c| c.as_str());

            let response = if let Some(cypher) = request.get("query").and_then(|q| q.as_str()) {
                info!("Executing Cypher query via ZMQ: {}", cypher);
                // Create SledStorage and pass it directly to execute_cypher_from_string
                let storage = Arc::new(SledStorage::new_with_db(
                    &_config,
                    &StorageConfig::default(),
                    db.clone(),
                ).await.map_err(|e| {
                    error!("Failed to create SledStorage for Cypher execution: {}", e);
                    GraphError::StorageError(e.to_string())
                })?);
                
                match crate::query_parser::cypher_parser::execute_cypher_from_string(cypher, storage).await {
                    Ok(result) => json!({"status": "success", "data": result, "request_id": request_id}),
                    Err(e) => json!({"status": "error", "message": e.to_string(), "request_id": request_id}),
                }
            } else {
                match command {
                    Some("initialize") => json!({
                        "status": "success",
                        "message": "ZMQ server is bound and DB is open.",
                        "port": port,
                        "ipc_path": &endpoint,
                        "request_id": request_id
                    }),
                    Some("status") => json!({"status":"success","port":port,"db_open":true,"request_id": request_id}),
                    Some("ping") => json!({
                        "status": "pong",
                        "message": "ZMQ server is bound and DB is open.",
                        "port": port,
                        "ipc_path": &endpoint,
                        "db_open":true,
                        "request_id": request_id
                    }),
                    Some("force_unlock") => match Self::force_unlock_static(&db_path).await {
                        Ok(_) => json!({"status":"success","request_id": request_id}),
                        Err(e) => json!({"status":"error","message":e.to_string(),"request_id": request_id}),
                    },
                    // === MUTATING COMMANDS (WAL + LEADER) — YOUR ORIGINAL LOGIC 100% PRESERVED ===
                    Some(cmd) if [
                        "set_key", "delete_key",
                        "create_vertex", "update_vertex", "delete_vertex",
                        "create_edge", "update_edge", "delete_edge",
                        "flush"
                    ].contains(&cmd) => {
                        let canonical = db_path.parent().unwrap().to_path_buf();
                        let is_leader = match become_wal_leader(&canonical, port).await {
                            Ok(l) => l,
                            Err(e) => {
                                let resp = json!({"status":"error","message":format!("Leader election failed: {}", e),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                        };
                        println!("====> IN run_zmq_server_lazy - command {:?}, is_leader: {}", cmd, is_leader);
                        let response = if is_leader {
                            let op = match cmd {
                                "set_key" => {
                                    let tree = request["cf"].as_str().unwrap_or("kv_pairs").to_string();
                                    let key = request["key"].as_str().ok_or_else(|| GraphError::StorageError("missing key".into()))?.as_bytes().to_vec();
                                    let value = request["value"].as_str().ok_or_else(|| GraphError::StorageError("missing value".into()))?.as_bytes().to_vec();
                                    SledWalOperation::Put { tree, key, value }
                                }
                                "delete_key" => {
                                    let tree = request["cf"].as_str().unwrap_or("kv_pairs").to_string();
                                    let key = request["key"].as_str().ok_or_else(|| GraphError::StorageError("missing key".into()))?.as_bytes().to_vec();
                                    SledWalOperation::Delete { tree, key }
                                }
                                "create_vertex" | "update_vertex" => {
                                    let vertex: Vertex = serde_json::from_value(request["vertex"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid vertex: {}", e)))?;
                                    let key = vertex.id.as_bytes().to_vec();
                                    let value = serialize_vertex(&vertex)?;
                                    SledWalOperation::Put { tree: "vertices".to_string(), key, value }
                                }
                                "delete_vertex" => {
                                    let id: Identifier = serde_json::from_value(request["id"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid id: {}", e)))?;
                                    let uuid = SerializableUuid::from_str(id.as_ref())
                                        .map_err(|_| GraphError::StorageError("Invalid UUID in vertex".into()))?;
                                    SledWalOperation::Delete { tree: "vertices".to_string(), key: uuid.as_bytes().to_vec() }
                                }
                                "create_edge" | "update_edge" => {
                                    let edge: Edge = serde_json::from_value(request["edge"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid edge: {}", e)))?;
                                    let key = create_edge_key(&edge.outbound_id, &edge.edge_type, &edge.inbound_id)?;
                                    let value = serialize_edge(&edge)?;
                                    SledWalOperation::Put { tree: "edges".to_string(), key, value }
                                }
                                "delete_edge" => {
                                    let from: Identifier = serde_json::from_value(request["from"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid from: {}", e)))?;
                                    let to: Identifier = serde_json::from_value(request["to"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid to: {}", e)))?;
                                    let t: Identifier = serde_json::from_value(request["type"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid type: {}", e)))?;
                                    let from_uuid = SerializableUuid::from_str(from.as_ref())
                                        .map_err(|_| GraphError::StorageError("Invalid UUID from".into()))?;
                                    let to_uuid = SerializableUuid::from_str(to.as_ref())
                                        .map_err(|_| GraphError::StorageError("Invalid UUID to".into()))?;
                                    let key = create_edge_key(&from_uuid, &t, &to_uuid)?;
                                    SledWalOperation::Delete { tree: "edges".to_string(), key }
                                }
                                "flush" => {
                                    let tree = request["cf"].as_str().unwrap_or("default").to_string();
                                    SledWalOperation::Flush { tree }
                                }
                                _ => unreachable!(),
                            };
                            println!("====> LEADER: About to append to WAL");
                            let lsn = match wal_manager.append(&op).await {
                                Ok(lsn) => {
                                    println!("====> LEADER: WAL append successful, lsn: {}", lsn);
                                    lsn
                                }
                                Err(e) => {
                                    println!("====> LEADER: WAL append failed: {}", e);
                                    let resp = json!({"status":"error","message":e.to_string(),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            println!("====> LEADER: About to apply operation locally");
                            if let Err(e) = SledDaemon::apply_op_locally(&db, &op).await {
                                println!("====> LEADER: Apply operation failed: {}", e);
                                let resp = json!({"status":"error","message":e.to_string(),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> LEADER: Operation applied successfully");
                            let offset_key = format!("__wal_offset_port_{}", port);
                            if let Err(e) = db.insert(offset_key.as_bytes(), lsn.to_string().as_bytes()) {
                                warn!("Failed to update WAL offset for port {}: {}", port, e);
                            }
                            tokio::spawn({
                                let db_clone = db.clone();
                                async move { let _ = db_clone.flush_async().await; }
                            });
                            println!("====> LEADER: Returning success response");
                            json!({"status":"success", "offset": lsn, "leader": true, "request_id": request_id})
                        } else {
                            // === FOLLOWER: FORWARD TO LEADER — YOUR ORIGINAL LOGIC 100% PRESERVED ===
                            println!("====> FOLLOWER: Attempting to get leader port");
                            let leader_port = match get_leader_port(&canonical).await {
                                Ok(Some(p)) => {
                                    println!("====> FOLLOWER: Leader port is {}", p);
                                    p
                                }
                                Ok(None) => {
                                    println!("====> FOLLOWER: No leader available");
                                    let resp = json!({"status":"error","message":"No leader available","request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                                Err(e) => {
                                    println!("====> FOLLOWER: Leader lookup failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("Leader lookup failed: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            if leader_port == 0 {
                                println!("====> FOLLOWER: Leader port is 0, election pending");
                                let resp = json!({"status":"error","message":"Leader election pending","request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            let leader_ipc_path = format!("/tmp/graphdb-{}.ipc", leader_port);
                            if tokio::fs::metadata(&leader_ipc_path).await.is_err() {
                                println!("====> FOLLOWER: Leader IPC socket doesn't exist at {}", leader_ipc_path);
                                error!("Leader IPC socket missing at {} - leader might not be running", leader_ipc_path);
                                let resp = json!({
                                    "status":"error",
                                    "message":format!("Leader on port {} not reachable (IPC socket missing)", leader_port),
                                    "request_id": request_id
                                });
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> FOLLOWER: Leader IPC socket exists at {}", leader_ipc_path);
                            println!("====> FOLLOWER: Creating new ZMQ context and socket");
                            let context = ZmqContext::new();
                            let client = match context.socket(zmq::REQ) {
                                Ok(c) => {
                                    println!("====> FOLLOWER: Socket created successfully");
                                    c
                                }
                                Err(e) => {
                                    println!("====> FOLLOWER: Socket creation failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("ZMQ socket error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            let leader_endpoint = format!("ipc:///tmp/graphdb-{}.ipc", leader_port);
                            println!("====> FOLLOWER: Connecting to leader at {}", leader_endpoint);
                            if let Err(e) = client.connect(&leader_endpoint) {
                                println!("====> FOLLOWER: Connection failed: {}", e);
                                let resp = json!({"status":"error","message":format!("Failed to connect to leader {}: {}", leader_port, e),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> FOLLOWER: Connected to leader");
                            client.set_sndtimeo(2000).ok();
                            client.set_rcvtimeo(3000).ok();
                            let mut forward_req = request.clone();
                            forward_req["forwarded_from"] = json!(port);
                            forward_req["command"] = json!(cmd);
                            forward_req["request_id"] = json!(request_id);
                            let payload = match serde_json::to_vec(&forward_req) {
                                Ok(p) => p,
                                Err(e) => {
                                    println!("====> FOLLOWER: Serialization failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("JSON serialize error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            println!("====> FOLLOWER: Sending request to leader ({} bytes)", payload.len());
                            if let Err(e) = client.send(&payload, 0) {
                                println!("====> FOLLOWER: Send to leader failed: {}", e);
                                let resp = json!({"status":"error","message":format!("ZMQ send failed: {}", e),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> FOLLOWER: Request sent, waiting for leader response...");
                            let resp_msg = match client.recv_bytes(0) {
                                Ok(m) => {
                                    println!("====> FOLLOWER: Received response from leader ({} bytes)", m.len());
                                    m
                                }
                                Err(e) => {
                                    println!("====> FOLLOWER: Recv from leader failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("ZMQ recv error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            println!("====> FOLLOWER: Parsing leader response");
                            match serde_json::from_slice(&resp_msg) {
                                Ok(resp) => resp,
                                Err(e) => {
                                    let resp = json!({"status":"error","message":format!("Leader response parse error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            }
                        };
                        response
                    }
                    // === READ COMMANDS — YOUR ORIGINAL LOGIC
                    Some(cmd) if [
                        "get_key", "get_vertex", "get_edge",
                        "get_all_vertices", "get_all_edges",
                        "get_all_vertices_by_type", "get_all_edges_by_type"
                    ].contains(&cmd) => {
                        match Self::execute_db_command(cmd, &request, &db, &kv_pairs, &vertices, &edges, port, &db_path, &endpoint).await {
                            Ok(mut r) => {
                                r["request_id"] = json!(request_id);
                                r
                            }
                            Err(e) => json!({"status":"error","message":e.to_string(),"request_id": request_id}),
                        }
                    }
                    Some("clear_data") | Some("force_reset") => {
                        match Self::execute_db_command(command.unwrap(), &request, &db, &kv_pairs, &vertices, &edges, port, &db_path, &endpoint).await {
                            Ok(mut r) => {
                                r["request_id"] = json!(request_id);
                                r
                            }
                            Err(e) => json!({"status":"error","message":e.to_string(),"request_id": request_id}),
                        }
                    }
                    Some("force_ipc_init") => {
                        let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                        match tokio::fs::metadata(&ipc_path).await {
                            Ok(_) => json!({"status": "success", "message": "IPC socket already exists", "ipc_path": &ipc_path, "request_id": request_id}),
                            Err(_) => json!({"status": "error", "message": "IPC socket missing - server restart required", "ipc_path": &ipc_path, "request_id": request_id}),
                        }
                    }
                    Some(cmd) => json!({"status":"error","message":format!("Unsupported command: {}", cmd),"request_id": request_id}),
                    None => json!({"status":"error","message":"No command specified","request_id": request_id}),
                }
            };

            let s = zmq_socket.lock().await;
            if let Err(e) = Self::send_zmq_response_static(&*s, &response, port).await {
                error!("Failed to send ZMQ response on port {}: {}", port, e);
            }
        }

        info!("ZMQ server shutting down for port {}", port);
        let s = zmq_socket.lock().await;
        let _ = s.disconnect(&endpoint);
        Ok(())
    }

    async fn execute_db_command(
        command: &str,
        request: &Value,
        db: &Arc<sled::Db>,
        kv_pairs: &Arc<Tree>,
        vertices: &Arc<Tree>,
        edges: &Arc<Tree>,
        port: u16,
        db_path: &PathBuf,
        endpoint: &str,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        use bincode::{decode_from_slice, config::standard};

        match command {
            "get_key" => {
                let tree_name = request.get("cf").and_then(|c| c.as_str()).unwrap_or("kv_pairs");
                let key = request.get("key").and_then(|k| k.as_str()).ok_or("Missing key")?;
                let tree = match tree_name {
                    "kv_pairs" => kv_pairs,
                    "vertices" => vertices,
                    "edges" => edges,
                    _ => return Err(format!("Tree {} not found", tree_name).into()),
                };
                let result = tree.get(key.as_bytes())?;
                let value_str = result.map(|v| String::from_utf8_lossy(&v).to_string());
                Ok(json!({"status": "success", "value": value_str}))
            }

            "get_all_vertices" => {
                println!("===========> in execute_db_command - will try to get all vertices {:?}", command);
                let mut vec = Vec::new();
                for item in vertices.iter() {
                    let (_, value) = item?;
                    let v = deserialize_vertex(&value)?;
                    vec.push(v);
                }
                Ok(json!({"status": "success", "vertices": vec}))
            }
            
            "get_all_vertices_by_type" => {
                let vertex_type: Identifier = serde_json::from_value(request["vertex_type"].clone())
                    .map_err(|_| "Invalid or missing vertex_type")?;
                let mut vec = Vec::new();
                for item in vertices.iter() {
                    let (_, value) = item?;
                    let v = deserialize_vertex(&value)?;
                    if v.label == vertex_type {
                        vec.push(v);
                    }
                }
                Ok(json!({"status": "success", "vertices": vec}))
            }

            "get_all_edges" => {
                let mut vec = Vec::new();
                for item in edges.iter() {
                    let (_, value) = item?;
                    let e = deserialize_edge(&value)?;
                    vec.push(e);
                }
                Ok(json!({"status": "success", "edges": vec}))
            }
            
            "get_all_edges_by_type" => {
                let edge_type: Identifier = serde_json::from_value(request["edge_type"].clone())
                    .map_err(|_| "Invalid or missing edge_type")?;
                let mut vec = Vec::new();
                for item in edges.iter() {
                    let (_, value) = item?;
                    let e = deserialize_edge(&value)?;
                    if e.edge_type == edge_type {
                        vec.push(e);
                    }
                }
                Ok(json!({"status": "success", "edges": vec}))
            }

            "clear_data" => {
                kv_pairs.clear()?;
                db.flush_async().await?;
                Ok(json!({"status": "success"}))
            }
            
            "force_reset" => {
                kv_pairs.clear()?;
                vertices.clear()?;
                edges.clear()?;
                db.flush_async().await?;
                info!("Force reset completed for port {}", port);
                Ok(json!({"status": "success"}))
            }

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

    async fn send_zmq_response_static(
        socket: &ZmqSocket,
        response: &Value,
        port: u16,
    ) -> GraphResult<()> {
        let response_data = serde_json::to_vec(response)
            .map_err(|e| {
                error!("Failed to serialize ZMQ response for port {}: {}", port, e);
                GraphError::StorageError(format!("Failed to serialize response: {}", e))
            })?;

        let response_len = response_data.len();

        socket.send(response_data, 0)
            .map_err(|e| {
                error!("CRITICAL: Failed to send ZMQ response for port {}: {}", port, e);
                GraphError::StorageError(format!("Failed to send response: {}", e))
            })?;

        debug!("Successfully sent ZMQ response for port {} ({} bytes)", port, response_len);
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
        let key = create_edge_key(&edge.outbound_id.into(), &edge.edge_type, &edge.inbound_id.into())?;
        let value = Self::serialize_to_ivec(edge)?;
        info!("Creating edge ({}, {}, {}) at path {:?}", edge.outbound_id, edge.edge_type, edge.inbound_id, self.db_path);
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
                    edge.outbound_id, edge.edge_type, edge.inbound_id, self.db_path);
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
                        value: edge.edge_type.to_string().into_bytes(),
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
        let key = Self::create_edge_key(&edge.outbound_id.into(), &edge.edge_type, &edge.inbound_id.into())?;
        let value = Self::serialize_to_ivec(edge)?;
        info!("Creating edge ({}, {}, {}) at path {:?}", edge.outbound_id, edge.edge_type, edge.inbound_id, db_path);

        timeout(TokioDuration::from_secs(TIMEOUT_SECS), async {
            let mut batch = Batch::default();
            batch.insert(&*key, value);
            handle_sled_op!(edges_tree.apply_batch(batch), "Failed to apply batch for edge")?;

            let bytes_flushed = handle_sled_op!(db.flush_async().await, "Failed to flush DB")?;
            info!("Flushed {} bytes after creating edge at {:?}", bytes_flushed, db_path);

            let persisted = handle_sled_op!(edges_tree.get(&key), "Failed to verify edge insert")?;
            if persisted.is_none() {
                error!("Persistence verification failed for edge ({}, {}, {}) at {:?}", 
                    edge.outbound_id, edge.edge_type, edge.inbound_id, db_path);
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

impl Drop for SledDaemon {
    fn drop(&mut self) {
        if let Some(handle) = self.zmq_thread.take() {
            // Fire-and-forget: detach the thread — OS will clean it up on process exit
            // This is the safest, simplest, and most common pattern for daemon threads
            info!("SledDaemon dropped — ZMQ thread detached (normal on engine switch)");
            // No join, no timeout, no blocking — just drop
            // The OS will terminate the thread when the process exits
        }
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
            zmq_ready: false,
            engine_synced: false,
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

    pub async fn new_with_client(
        client: SledClient,
        db_path: &Path,
        port: u16,
    ) -> GraphResult<Self> {
        println!("============> In pool new_with_client - this must start zeromq server");

        let mut pool = Self::new();

        // Extract DB from client
        let db = client
            .inner
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

        // Create WAL manager
        let wal_manager = Arc::new(
            SledWalManager::new(db_path.to_path_buf(), port)
                .await
                .map_err(|e| GraphError::StorageError(format!("Failed to create WAL manager: {}", e)))?,
        );

        // Insert daemon into pool
        pool.daemons.insert(
            port,
            Arc::new(SledDaemon {
                port,
                db_path: db_path.to_path_buf(),
                db,
                vertices,
                edges,
                kv_pairs,
                wal_manager,
                running: Arc::new(TokioMutex::new(true)),
                zmq_thread: None, // ← ZMQ not started here
                #[cfg(feature = "with-openraft-sled")]
                raft_storage: Arc::new(
                    openraft_sled::SledRaftStorage::new(db.clone())
                        .await
                        .map_err(|e| GraphError::StorageError(format!("Failed to create Raft storage: {}", e)))?,
                ),
                #[cfg(feature = "with-openraft-sled")]
                node_id: port as u64,
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

        // ──────────────────────────────────────────────────────────────
        // 1. GET WRITE NODES — BUT ONLY AFTER HEALTH IS MARKED
        // ──────────────────────────────────────────────────────────────
        let write_nodes = self.load_balancer.get_write_nodes(strategy.clone()).await;

        // ──────────────────────────────────────────────────────────────
        // 2. DEBUG: SHOW WHAT WE SEE
        // ──────────────────────────────────────────────────────────────
        let all_nodes = self.load_balancer.get_all_nodes().await;
        let healthy_nodes = self.load_balancer.get_healthy_nodes().await;
        println!("===> [INSERT_REPLICATED] ALL NODES: {:?}", all_nodes);
        println!("===> [INSERT_REPLICATED] HEALTHY NODES: {:?}", healthy_nodes);
        println!("===> [INSERT_REPLICATED] WRITE NODES (strategy={:?}): {:?}", strategy, write_nodes);

        if write_nodes.is_empty() {
            error!("===> [INSERT_REPLICATED] NO HEALTHY NODES — WRITE FAILED");
            return Err(GraphError::StorageError("No healthy nodes available for write operation".to_string()));
        }

        println!("===> REPLICATED INSERT: Writing to {} nodes: {:?}", write_nodes.len(), write_nodes);

        // ──────────────────────────────────────────────────────────────
        // 3. RAFT PATH
        // ──────────────────────────────────────────────────────────────
        #[cfg(feature = "with-openraft-rocksdb")]
        if matches!(strategy, ReplicationStrategy::Raft) && self.use_raft_for_scale {
            return self.insert_raft(key, value).await;
        }

        // ──────────────────────────────────────────────────────────────
        // 4. NON-RAFT: WRITE TO ALL SELECTED NODES
        // ──────────────────────────────────────────────────────────────
        let mut success_count = 0;
        let mut errors = Vec::new();

        for &port in &write_nodes {
            let start_time = std::time::Instant::now();
            let result = self.insert_to_node(port, key, value).await;
            let elapsed_ms = start_time.elapsed().as_millis() as u64;

            match result {
                Ok(_) => {
                    success_count += 1;
                    println!("===> REPLICATED INSERT: Success on node {}", port);
                    // Mark as healthy with actual response time
                    self.load_balancer.update_node_health(port, true, elapsed_ms).await;
                }
                Err(e) => {
                    errors.push((port, e));
                    println!("===> REPLICATED INSERT: Failed on node {}: {:?}", port, errors.last().unwrap().1);
                    // Mark as unhealthy with response time
                    self.load_balancer.update_node_health(port, false, elapsed_ms).await;
                }
            }
        }

        // ──────────────────────────────────────────────────────────────
        // 5. QUORUM CHECK
        // ──────────────────────────────────────────────────────────────
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

        // FIX: Destructure the result of `SledDaemon::new_with_db`.
        // It returns a tuple `(SledDaemon, Receiver<()>)`, but `self.daemons` only expects the SledDaemon.
        // We use `let (daemon, _)` to discard the Receiver (shutdown signal).
        let (daemon, _) = SledDaemon::new_with_db(updated_config, existing_db).await?;


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

    // ============================================================================
    // SledDaemonPool::_initialize_cluster_core
    // ============================================================================
    async fn _initialize_cluster_core(
        &mut self,
        storage_config: &StorageConfig,
        config: &SledConfig,
        cli_port: Option<u16>,
    ) -> GraphResult<()> {
        println!("===> IN _initialize_cluster_core");
        info!("Starting initialization of SledDaemonPool");
        let _guard = get_cluster_init_lock().await.lock().await;
        let mut initialized = self.initialized.write().await;
        if *initialized {
            warn!("SledDaemonPool already initialized, skipping");
            println!("===> WARNING: SLED DAEMON POOL ALREADY INITIALIZED, SKIPPING");
            
            // Ensure the node is marked healthy in the load balancer even if already initialized
            let port = cli_port.unwrap_or(config.port.unwrap_or(8052));
            self.load_balancer.update_node_health(port, true, 0).await;
            println!("===> [LB] NODE {} MARKED HEALTHY IN SLED LOAD BALANCER (ALREADY INITIALIZED)", port);
            return Ok(());
        }
        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let port = cli_port.unwrap_or(config.port.unwrap_or(8052));
       
        info!("Canonical port: {}", port);
        println!("===> CANONICAL PORT: {}", port);

        // ---------- 0. Check if daemon already exists in this pool ----------
        if self.daemons.contains_key(&port) {
            info!("Daemon already exists in pool for port {}", port);
            println!("===> DAEMON ALREADY EXISTS IN POOL FOR PORT {}", port);
            self.load_balancer.update_node_health(port, true, 0).await;
            *initialized = true;
            return Ok(());
        }

        let ipc_path = format!("/tmp/graphdb-{}.ipc", port);

        // ---------- 1. Check for existing live daemon in registry ----------
        let mut daemon_exists_and_is_live = false;
        let mut daemon_zmq_ready = false;
       
        if let Ok(Some(metadata)) = daemon_registry.get_daemon_metadata(port).await {
            if metadata.service_type == "storage"
                && metadata.engine_type.as_ref().map(|e| e.to_lowercase()) == Some("sled".to_string())
                && metadata.pid > 0
                && is_pid_running(metadata.pid).await
            {
                info!("Daemon already running on port {} (PID {})", port, metadata.pid);
                println!("===> DAEMON ALREADY RUNNING ON PORT {} (PID {})", port, metadata.pid);
                daemon_exists_and_is_live = true;
                daemon_zmq_ready = metadata.zmq_ready;
            } else {
                // Stale metadata
                warn!("Found stale daemon registration for port {}, unregistering", port);
                println!("===> WARNING: FOUND STALE DAEMON REGISTRY FOR PORT {} (PID {} NOT RUNNING)", port, metadata.pid);
                let _ = daemon_registry.unregister_daemon(port).await;
            }
        }

        // Also check if ANY process is listening (even if not in registry)
        if !daemon_exists_and_is_live && check_process_status_by_port("Storage Daemon", port).await {
            info!("Process listening on port {}, but not in registry. Waiting for registration...", port);
            println!("===> PROCESS LISTENING ON PORT {}, BUT NOT IN REGISTRY. WAITING FOR REGISTRATION...", port);
           
            for attempt in 0..10 {
                tokio::time::sleep(TokioDuration::from_millis(500)).await;
                if let Ok(Some(metadata)) = daemon_registry.get_daemon_metadata(port).await {
                    if metadata.service_type == "storage"
                        && metadata.engine_type.as_ref().map(|e| e.to_lowercase()) == Some("sled".to_string())
                        && is_pid_running(metadata.pid).await
                    {
                        info!("Daemon registered itself on port {} after {} attempts", port, attempt + 1);
                        println!("===> DAEMON REGISTERED ON PORT {} AFTER {} ATTEMPTS", port, attempt + 1);
                        daemon_exists_and_is_live = true;
                        daemon_zmq_ready = metadata.zmq_ready;
                        break;
                    }
                }
            }
        }

        // ---------- 2. REUSE IF IPC EXISTS (EVEN IF ZMQ NOT READY) ----------
        if Path::new(&ipc_path).exists() {
            info!("IPC FILE EXISTS — REUSING DAEMON ON PORT {} (ZMQ may be starting)", port);
            println!("===> IPC FILE EXISTS — REUSING DAEMON ON PORT {} (ZMQ MAY BE STARTING)", port);
            self.load_balancer.update_node_health(port, true, 0).await;
            *initialized = true;
            return Ok(());
        }

        // ---------- 3. No daemon exists or not ready → proceed with creation ----------
       
        // Clean stale IPC only if no live daemon
        if !daemon_exists_and_is_live && Path::new(&ipc_path).exists() {
            warn!("Removing stale IPC socket at {}", ipc_path);
            println!("===> REMOVING STALE IPC SOCKET AT {}", ipc_path);
            let _ = tokio_fs::remove_file(&ipc_path).await;
        }

        // Per-port path
        let db_path = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&PathBuf::from("/opt/graphdb/storage_data"))
            .join("sled")
            .join(port.to_string());
        info!("Using Sled path: {:?}", db_path);
        println!("===> USING SLED PATH: {:?}", db_path);
        if !db_path.exists() {
            info!("Creating Sled directory at {:?}", db_path);
            println!("===> CREATING SLED DIRECTORY AT {:?}", db_path);
            tokio_fs::create_dir_all(&db_path).await.map_err(|e| GraphError::Io(e.to_string()))?;
        }
        SledClient::force_unlock(&db_path).await?;
        info!("Force unlocked database at {:?}", db_path);
        println!("===> FORCE UNLOCKED DATABASE AT {:?}", db_path);

        // WAL setup
        let canonical_path = storage_config
            .data_directory
            .as_ref()
            .unwrap_or(&PathBuf::from("/opt/graphdb/storage_data"))
            .join("sled");
        let is_leader = become_wal_leader(&canonical_path, port).await?;
        let offset_file = db_path.join("wal.offset");
        let start_offset = match tokio_fs::read_to_string(&offset_file).await {
            Ok(s) => s.trim().parse::<u64>().unwrap_or(0),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(GraphError::Io(e.to_string())),
        };

        // Create new daemon — USE PER-PORT PATH
        let mut daemon_config = config.clone();
        daemon_config.path = PathBuf::from(db_path.to_string_lossy().into_owned());  // CHANGED: use per-port path
        daemon_config.port = Some(port);
        info!("Creating SledDaemon with config: {:?}", daemon_config);
        println!("===> CREATING SLED DAEMON WITH CONFIG: {:?}", daemon_config);

        let (daemon, mut ready_rx) = timeout(
            TokioDuration::from_secs(15),
            SledDaemon::new(daemon_config)
        )
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

        // ------------------------------------------------------------------
        //  NEW: wait until the daemon signals it is *really* ready
        // ------------------------------------------------------------------
        timeout(TokioDuration::from_secs(10), ready_rx.recv())
            .await
            .map_err(|_| {
                error!("Timeout waiting for ZMQ readiness signal on port {}", port);
                println!("===> ERROR: TIMEOUT WAITING FOR ZMQ READINESS SIGNAL ON PORT {}", port);
                GraphError::StorageError("Timeout waiting for ZMQ readiness signal".into())
            })?
            .ok_or_else(|| {
                error!("ZMQ readiness channel closed prematurely on port {}", port);
                println!("===> ERROR: ZMQ READINESS CHANNEL CLOSED PREMATURELY ON PORT {}", port);
                GraphError::StorageError("ZMQ readiness channel closed prematurely".into())
            })?;

        // Verify IPC exists
        let mut ipc_check_attempts = 0;
        while !Path::new(&ipc_path).exists() && ipc_check_attempts < 10 {
            warn!("IPC file not found at {}, waiting... (attempt {})", ipc_path, ipc_check_attempts + 1);
            println!("===> WARNING: IPC FILE NOT FOUND AT {}, WAITING... (ATTEMPT {})", ipc_path, ipc_check_attempts + 1);
            tokio::time::sleep(TokioDuration::from_millis(500)).await;
            ipc_check_attempts += 1;
        }
        if !Path::new(&ipc_path).exists() {
            error!("IPC file not created at {} after {} attempts", ipc_path, ipc_check_attempts);
            println!("===> ERROR: IPC FILE NOT CREATED AT {} AFTER {} ATTEMPTS", ipc_path, ipc_check_attempts);
            return Err(GraphError::StorageError(format!("IPC file not created: {}", ipc_path)));
        }
        info!("IPC file verified at {}", ipc_path);
        println!("===> ZMQ IPC FILE VERIFIED AT {}", ipc_path);

        // Register daemon
        let daemon_metadata = DaemonMetadata {
            service_type: "storage".to_string(),
            port,
            pid: std::process::id(),
            ip_address: "127.0.0.1".to_string(),
            data_dir: Some(db_path.clone()),
            config_path: storage_config.config_root_directory.clone().map(PathBuf::from),
            engine_type: Some("sled".to_string()),
            last_seen_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0),
            zmq_ready: true,
            engine_synced: true,
        };

        if is_leader {
            let _ = tokio_fs::write(offset_file, start_offset.to_string().as_bytes()).await;
        }

        match daemon_registry.register_daemon(daemon_metadata.clone()).await {
            Ok(_) => {
                info!("Successfully registered daemon on port {}", port);
                println!("===> SUCCESSFULLY REGISTERED DAEMON ON PORT {}", port);
            }
            Err(e) => {
                warn!("Failed to register daemon (may already exist), attempting update: {}", e);
                println!("===> WARNING: FAILED TO REGISTER DAEMON, ATTEMPTING UPDATE: {}", e);
                daemon_registry.update_daemon_metadata(daemon_metadata).await
                    .map_err(|e2| GraphError::StorageError(format!("Failed to update daemon metadata on port {}: {}", port, e2)))?;
                info!("Successfully updated daemon metadata on port {}", port);
                println!("===> SUCCESSFULLY UPDATED DAEMON METADATA ON PORT {}", port);
            }
        }

        // Store daemon
        self.daemons.insert(port, Arc::new(daemon));
        self.load_balancer.update_node_health(port, true, 0).await;
        *initialized = true;
        info!("SledDaemonPool initialized successfully on port {}", port);
        println!("===> SLED DAEMON POOL INITIALIZED SUCCESSFULLY ON PORT {}", port);
        Ok(())
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
                    || file_name_str.contains(".tmp") || file_name_str.contains(".temp") 
                    || file_name_str == "db" { // Skip Sled's lock file
                    info!("Skipping lock/temp file: {}", file_name_str);
                    println!("===> SKIPPING LOCK/TEMP FILE: {}", file_name_str);
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
                    info!("Copied file: {}", file_name_str);
                }
            }

            info!("Successfully copied data to {:?}", target);
            println!("===> SUCCESSFULLY COPIED DATA TO {:?}", target);
            Ok(())
        })
    }

    /// ---------------------------------------------------------------------------
    /// Small utilities used above
    /// ---------------------------------------------------------------------------
    fn leader_port_is_canonical(leader: &Path, canon_port: u16, cfg: &StorageConfig) -> bool {
        leader.ends_with(&format!("{}", canon_port))
            && leader.starts_with(
                cfg.data_directory
                    .as_ref()
                    .unwrap_or(&PathBuf::from("/opt/graphdb/storage_data"))
                    .join("sled"),
            )
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
            zmq_ready: false,
            engine_synced: false,
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

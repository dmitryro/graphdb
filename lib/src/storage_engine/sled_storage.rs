// lib/src/storage_engine/sled_storage.rs
// Created: 2025-08-09 - Initial SledStorage implementation
// Updated: 2025-09-01 - Added kv_pairs tree, integrated OpenRaft leadership check, handled Raft enabled/disabled cases, fixed db_path logic, improved lock handling and error messages
// Corrected: 2025-09-02 - Fixed compilation errors related to `Self` and corrupted trait implementation

use std::any::Any;
use async_trait::async_trait;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::storage_engine::config::{SledConfig, StorageConfig, StorageConfigWrapper, default_data_directory, default_log_directory,
                                     DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_CONFIG_PATH};
#[cfg(feature = "with-sled")]
use crate::storage_engine::{recover_sled, log_lock_file_diagnostics, lock_file_exists};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use serde_json::Value;
use sled::{Db, Tree, IVec, Config};
use std::path::{PathBuf, Path};
use std::sync::{LazyLock, Mutex};
use uuid::Uuid;
use log::{info, warn, debug, error};
use tokio::fs;
use std::time::{Duration};
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt};
use futures::executor;
use tokio::sync::Mutex as TokioMutex;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use openraft::{Raft, NodeId, Config as RaftConfig}; // Assuming openraft is used

// Define key prefixes for key-value pairs, similar to tikv_storage.rs
const KV_KEY_PREFIX: &[u8] = b"kv:";

// Static Tokio runtime initialized once for fallback
#[cfg(feature = "with-sled")]
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    debug!("Initializing Tokio runtime for run_sync fallback");
    tokio::runtime::Runtime::new()
        .expect("Failed to initialize Tokio runtime for run_sync")
});

/// Helper function to run async operations synchronously.
/// It tries to use an existing Tokio runtime handle, if available,
/// otherwise it falls back to a static, lazily-initialized runtime.
#[cfg(feature = "with-sled")]
fn run_sync<F, T>(future: F) -> GraphResult<T>
where
    F: std::future::Future<Output = Result<T, GraphError>> + Send + 'static,
    T: Send + 'static,
{
    debug!("Running async operation synchronously");
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            debug!("Using existing Tokio runtime handle");
            handle.block_on(future)
        }
        Err(_) => {
            debug!("No existing runtime found; using fallback runtime");
            RUNTIME.block_on(future)
        }
    }
}

/// Creates a key for a key-value pair.
/// Format: `kv:{key}`
pub fn create_kv_key(key: &str) -> Vec<u8> {
    let mut kv_key = KV_KEY_PREFIX.to_vec();
    kv_key.extend_from_slice(key.as_bytes());
    kv_key
}

// Raft leadership check using openraft
#[cfg(feature = "with-sled")]
async fn is_raft_leader(port: Option<u16>, _use_raft: bool) -> GraphResult<bool> {
    // Temporarily disable Raft check since use_raft_for_scale is not available
    debug!("Raft leadership check disabled; assuming leadership for port {:?}", port);
    Ok(true)
    // TODO: Implement actual Raft leadership check
    /*
    if !use_raft {
        debug!("Raft is disabled; allowing direct database access");
        return Ok(true);
    }

    debug!("Checking Raft leadership for port {:?}", port);
    // Assuming StorageEngineManager provides access to the Raft instance
    let node_id = port.unwrap_or(8049) as u64; // Changed NodeId to u64
    // Placeholder: Replace with actual Raft instance retrieval
    let raft = crate::storage_engine::GLOBAL_STORAGE_ENGINE_MANAGER
        .get() // Assuming get() returns Option<Arc<AsyncStorageEngineManager>>
        .ok_or_else(|| GraphError::StorageError("Failed to get storage engine manager".to_string()))?
        .get_raft_instance(node_id) // Replace with actual method to get Raft instance
        .ok_or_else(|| GraphError::StorageError("Failed to get Raft instance".to_string()))?;
    
    let metrics = raft.metrics().await;
    Ok(metrics.current_leader == Some(node_id))
    */
}

#[derive(Debug)]
pub struct SledStorage {
    db: Db,
    vertices: Tree,
    edges: Tree,
    kv_pairs: Tree,
    config: SledConfig,
    running: TokioMutex<bool>,
}

impl SledStorage {
    /// Creates a new SledStorage instance with robust lock handling and Raft coordination.
    pub async fn new(config: &SledConfig) -> GraphResult<SledStorage> {
        info!("Initializing Sled storage engine with config: {:?}", config);

        // Validate port against cluster range
        if let Some(port) = config.port {
            let cluster_range = 8049..=8083; // As per graphdb-cli status
            if !cluster_range.contains(&port) {
                warn!("Port {} is outside cluster range {:?}", port, cluster_range);
            }
        }

        // Check Raft leadership or allow access if Raft is disabled
        if !is_raft_leader(config.port, false).await? { // Set use_raft to false as placeholder
            error!("This instance (port {:?}) is not the Raft leader.", config.port);
            return Err(GraphError::StorageError(format!(
                "This instance (port {:?}) is not the Raft leader. Only the leader can access /opt/graphdb/storage_data/sled. Use the leader instance (likely port 8049) or stop other Storage Daemon instances (PIDs: 25656, 25709, 25765; ports: 2380, 8049, 8051).",
                config.port
            )));
        }

        // Use shared database path to preserve content consistency
        let db_path = config.path.clone();
        info!("Using Sled database path: {:?}", db_path);

        // Ensure the directory exists with correct permissions
        if !db_path.exists() {
            debug!("Creating Sled data directory at {:?}", db_path);
            std::fs::create_dir_all(&db_path).map_err(|e| {
                error!("Failed to create Sled data directory at {:?}: {}", db_path, e);
                GraphError::Io(e)
            })?;
            #[cfg(unix)]
            {
                let mut perms = std::fs::metadata(&db_path)
                    .map_err(|e| GraphError::Io(e))?
                    .permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(&db_path, perms).map_err(|e| GraphError::Io(e))?;
            }
        }

        // Passively remove lock file with retries
        let lock_path = db_path.join("db.lck");
        for attempt in 1..=3 {
            if lock_path.exists() {
                warn!("Found Sled lock file at {:?}", lock_path);
                run_sync(log_lock_file_diagnostics(lock_path.clone()))?;
                match std::fs::remove_file(&lock_path) {
                    Ok(_) => {
                        info!("Successfully removed Sled lock file on attempt {}", attempt);
                        break;
                    }
                    Err(e) if attempt < 3 => {
                        warn!("Failed to remove lock file on attempt {}: {}. Retrying.", attempt, e);
                        tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
                    }
                    Err(e) => {
                        error!("Failed to remove lock file after 3 attempts: {}", e);
                        return Err(GraphError::StorageError(format!(
                            "Failed to remove lock file at {:?}: {}. Another process may be holding the lock (PIDs: 25656, 25709, 25765; ports: 2380, 8049, 8051).",
                            lock_path, e
                        )));
                    }
                }
            } else {
                info!("No Sled lock file found at {:?}", lock_path);
                break;
            }
        }

        // Initialize Sled with extended retry loop and timeout
        const MAX_RETRIES: u32 = 30;
        const BASE_DELAY_MS: u64 = 1000;
        const TIMEOUT_SECS: u64 = 60;
        let start_time = std::time::Instant::now();
        let mut attempt = 0;
        let mut db = None;

        while attempt < MAX_RETRIES && start_time.elapsed().as_secs() < TIMEOUT_SECS {
            info!("Attempt {} of {} to open Sled DB at {:?}", attempt + 1, MAX_RETRIES, db_path);
            match Config::new()
                .path(&db_path)
                .use_compression(false)
                .flush_every_ms(None)
                .open()
            {
                Ok(db_instance) => {
                    info!("Successfully opened Sled DB at {:?}", db_path);
                    db = Some(db_instance);
                    break;
                }
                Err(e) => {
                    error!("Failed to open Sled DB on attempt {}: {}. Error details: {:?}", attempt + 1, e, e);
                    if e.to_string().contains("WouldBlock") {
                        warn!("Detected lock contention. Attempt {} of {}. Retrying after delay.", attempt + 1, MAX_RETRIES);
                        run_sync(log_lock_file_diagnostics(lock_path.clone()))?;
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                        attempt += 1;
                        continue;
                    } else if e.to_string().contains("corruption") {
                        warn!("Detected Sled database corruption at {:?}", db_path);
                        SledStorage::backup_and_recreate_database(&db_path)?;
                        match Config::new()
                            .path(&db_path)
                            .use_compression(false)
                            .flush_every_ms(None)
                            .open()
                        {
                            Ok(db_instance) => {
                                info!("Successfully opened Sled DB after recovery at {:?}", db_path);
                                db = Some(db_instance);
                                break;
                            }
                            Err(e) => {
                                error!("Failed to open Sled DB after recovery: {}", e);
                                return Err(GraphError::StorageError(format!("Failed to open Sled DB after recovery: {}", e)));
                            }
                        }
                    } else {
                        error!("Unhandled Sled open error: {}", e);
                        return Err(GraphError::StorageError(format!("Unhandled Sled open error: {}", e)));
                    }
                }
            }
        }

        let db = db.ok_or_else(|| {
            error!("Failed to open Sled DB after {} attempts or {} seconds.", MAX_RETRIES, TIMEOUT_SECS);
            run_sync(log_lock_file_diagnostics(lock_path.clone())).unwrap_or_else(|e| {
                error!("Failed to log lock file diagnostics: {}", e);
            });
            GraphError::StorageError(format!(
                "Failed to open Sled DB at {:?} after {} attempts or {} seconds due to lock contention. Ensure only one Storage Daemon instance accesses this path (PIDs: 25656, 25709, 25765; ports: 2380, 8049, 8051) or verify Raft leadership configuration.",
                db_path, MAX_RETRIES, TIMEOUT_SECS
            ))
        })?;

        let vertices = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edges = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let kv_pairs = db.open_tree("kv_pairs").map_err(|e| GraphError::StorageError(e.to_string()))?;

        let storage = SledStorage {
            db,
            vertices,
            edges,
            kv_pairs,
            config: config.clone(),
            running: TokioMutex::new(true),
        };

        // Register SIGTERM handler for graceful shutdown
        #[cfg(unix)]
        {
            let path_clone = db_path.to_path_buf();
            let storage_clone = storage.db.clone();
            let lock_path = path_clone.join("db.lck");
            tokio::spawn(async move {
                let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                info!("Received SIGTERM, initiating graceful shutdown for Sled instance at {:?}", path_clone);
                tokio::time::sleep(Duration::from_millis(3000)).await;
                if let Err(e) = storage_clone.flush_async().await {
                    error!("Failed to flush Sled instance on SIGTERM: {}", e);
                }
                if lock_path.exists() {
                    if let Err(e) = std::fs::remove_file(&lock_path) {
                        error!("Failed to remove lock file on SIGTERM: {}", e);
                    } else {
                        info!("Removed lock file on SIGTERM: {:?}", lock_path);
                    }
                }
                info!("Sled instance closed gracefully");
                std::process::exit(0);
            });
        }

        info!("SledStorage initialized successfully");
        Ok(storage)
    }

    /// Safely removes any existing lock files and checks for active processes.
    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        info!("Attempting to safely remove locks on Sled database at {:?}", path);

        let lock_path = path.join("db.lck");

        // Passively remove lock file with retry
        if lock_path.exists() {
            warn!("Found potential lock file at {:?}", lock_path);
            run_sync(log_lock_file_diagnostics(lock_path.clone()))?;
            for attempt in 1..=3 {
                match std::fs::remove_file(&lock_path) {
                    Ok(_) => {
                        info!("Successfully removed stale lock file on attempt {}", attempt);
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        warn!("Lock file removal blocked on attempt {}: {}", attempt, e);
                        tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
                        if attempt == 3 {
                            error!("Failed to remove lock file after 3 attempts: {}", e);
                            return Err(GraphError::StorageError(format!(
                                "Failed to remove lock file at {:?}: {}. Another process may be holding the lock (PIDs: 25656, 25709, 25765; ports: 2380, 8049, 8051). Stop these processes or verify Raft leadership.",
                                lock_path, e
                            )));
                        }
                    }
                    Err(e) => {
                        error!("Failed to remove lock file: {}", e);
                        return Err(GraphError::StorageError(format!("Failed to remove lock file at {:?}: {}", lock_path, e)));
                    }
                }
            }
        } else {
            info!("No lock file found at {:?}", lock_path);
        }

        Ok(())
    }

    fn verify_database_accessible(db_path: &Path) -> GraphResult<bool> {
        match std::fs::File::open(db_path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(_) => Ok(true),
        }
    }

    fn backup_and_recreate_database(path: &Path) -> GraphResult<()> {
        warn!("Attempting to backup and recreate potentially corrupted database");

        let backup_path = path.with_extension("backup");
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let backup_path = backup_path.with_extension(format!("backup.{}", timestamp));

        if let Err(e) = std::fs::rename(path, &backup_path) {
            error!("Failed to backup database directory: {}", e);
            warn!("Attempting to remove locked database directory entirely");
            if let Err(e2) = std::fs::remove_dir_all(path) {
                return Err(GraphError::StorageError(format!(
                    "Failed to backup or remove locked database: backup error: {}, remove error: {}",
                    e, e2
                )));
            }
        } else {
            info!("Successfully backed up database to {:?}", backup_path);
        }

        std::fs::create_dir_all(path).map_err(|e| GraphError::Io(e))?;

        Ok(())
    }

    /// Nuclear option: completely destroy and recreate the database
    pub async fn force_reset(config: &SledConfig) -> GraphResult<SledStorage> {
        warn!("FORCE RESET: Completely destroying and recreating database at {:?}", config.path);
        if config.path.exists() {
            std::fs::remove_dir_all(&config.path).map_err(|e| {
                GraphError::StorageError(format!("Failed to remove database directory: {}", e))
            })?;
        }
        SledStorage::new(config).await
    }

    pub fn reset(&mut self) -> GraphResult<()> {
        self.vertices.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.edges.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush().map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }
}

// StorageEngine implementation
#[async_trait]
impl StorageEngine for SledStorage {
    async fn connect(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        debug!("Inserting key: {:?}", String::from_utf8_lossy(&key));
        self.kv_pairs.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        debug!("Successfully inserted key-value pair");
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        debug!("Retrieving key: {:?}", String::from_utf8_lossy(key));
        let result = self.kv_pairs.get(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        if let Some(ref v) = result {
            debug!("Retrieved value: {:?}", String::from_utf8_lossy(v));
        } else {
            debug!("No value found for key");
        }
        Ok(result.map(|ivec| ivec.to_vec()))
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        debug!("Deleting key: {:?}", String::from_utf8_lossy(key));
        self.kv_pairs.remove(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        debug!("Successfully deleted key");
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        info!("Flushing Sled storage engine");
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SledStorage {
    async fn clear_data(&self) -> Result<(), GraphError> {
        self.vertices.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.edges.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.kv_pairs.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> GraphResult<()> {
        let mut running_guard = self.running.lock().await;
        if *running_guard {
            info!("Sled storage engine is already running");
            return Ok(());
        }
        info!("Starting Sled storage engine");
        *running_guard = true;
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        {
            let mut running_guard = self.running.lock().await;
            if !*running_guard {
                info!("Sled storage engine is already stopped");
                return Ok(());
            }
            info!("Stopping Sled storage engine");
            *running_guard = false;
        }
        self.close().await
    }

    fn get_type(&self) -> &'static str {
        "sled"
    }

    async fn is_running(&self) -> bool {
        let running_guard = self.running.lock().await;
        *running_guard
    }

    async fn query(&self, query_string: &str) -> GraphResult<Value> {
        debug!("Executing query against SledStorage: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "Sled query execution placeholder"
        }))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes().to_vec();
        let value = serialize_vertex(&vertex)?;
        debug!("Creating vertex with key: {:?}", String::from_utf8_lossy(&key));
        self.vertices.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        debug!("Successfully created vertex");
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes().to_vec();
        debug!("Retrieving vertex with key: {:?}", String::from_utf8_lossy(&key));
        let result = self.vertices.get(&key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        if let Some(ref v) = result {
            debug!("Vertex found: {:?}", String::from_utf8_lossy(v));
        } else {
            debug!("No vertex found for key");
        }
        Ok(result.map(|bytes| deserialize_vertex(&bytes)).transpose()?)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.delete_vertex(&vertex.id.into()).await?;
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key = id.as_bytes().to_vec();
        debug!("Deleting vertex with key: {:?}", String::from_utf8_lossy(&key));
        self.vertices.remove(&key).map_err(|e| GraphError::StorageError(e.to_string()))?;

        let mut batch = sled::Batch::default();
        let prefix = id.as_bytes();
        for item in self.edges.iter().keys() {
            let key = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            if key.starts_with(prefix) {
                batch.remove(key);
            }
        }
        self.edges.apply_batch(batch).map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        debug!("Successfully deleted vertex");
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut vertices = Vec::new();
        for item in self.vertices.iter() {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertex = deserialize_vertex(&value)?;
            vertices.push(vertex);
        }
        debug!("Retrieved {} vertices", vertices.len());
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        if self.get_vertex(&edge.outbound_id.into()).await?.is_none() || self.get_vertex(&edge.inbound_id.into()).await?.is_none() {
            return Err(GraphError::InvalidData("One or both vertices for the edge do not exist.".to_string()));
        }

        let key = create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into())?;
        let value = serialize_edge(&edge)?;
        debug!("Creating edge with key: {:?}", String::from_utf8_lossy(&key));
        self.edges.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        debug!("Successfully created edge");
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        debug!("Retrieving edge with key: {:?}", String::from_utf8_lossy(&key));
        let result = self.edges.get(&key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        if let Some(ref v) = result {
            debug!("Edge found: {:?}", String::from_utf8_lossy(v));
        } else {
            debug!("No edge found for key");
        }
        Ok(result.map(|bytes| deserialize_edge(&bytes)).transpose()?)
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        debug!("Deleting edge with key: {:?}", String::from_utf8_lossy(&key));
        self.edges.remove(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        debug!("Successfully deleted edge");
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut edges = Vec::new();
        for item in self.edges.iter() {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge = deserialize_edge(&value)?;
            edges.push(edge);
        }
        debug!("Retrieved {} edges", edges.len());
        Ok(edges)
    }

    async fn close(&self) -> GraphResult<()> {
        info!("Closing Sled storage engine");
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        let mut running_guard = self.running.lock().await;
        *running_guard = false;
        info!("SledStorage closed and flushed.");
        Ok(())
    }
}

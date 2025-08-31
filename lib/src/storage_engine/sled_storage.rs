use std::any::Any;
use async_trait::async_trait;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::storage_engine::config::{SledConfig, StorageConfig, StorageConfigWrapper, default_data_directory, default_log_directory, DEFAULT_STORAGE_CONFIG_PATH};
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

// Static Tokio runtime initialized once for fallback
#[cfg(feature = "with-sled")]
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    debug!("Initializing Tokio runtime for run_sync fallback");
    tokio::runtime::Runtime::new()
        .expect("Failed to initialize Tokio runtime for run_sync")
});

// Helper function to run async operations synchronously
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
            tokio::task::block_in_place(|| {
                let task = handle.spawn(future);
                executor::block_on(task).map_err(|e| {
                    error!("Task join error: {}", e);
                    GraphError::Io(std::io::Error::new(std::io::ErrorKind::Other, e))
                })?
            })
        }
        Err(_) => {
            debug!("No existing runtime found; using fallback runtime");
            RUNTIME.block_on(future)
        }
    }
}

#[derive(Debug)]
pub struct SledStorage {
    db: Db,
    vertices: Tree,
    edges: Tree,
    config: SledConfig,
    running: TokioMutex<bool>,
}

impl SledStorage {
    /// Creates a new SledStorage instance with robust lock handling.
    pub async fn new(config: &SledConfig) -> GraphResult<Self> {
        info!("Initializing Sled storage engine with config: {:?}", config);
        let path = &config.path;

        // Ensure the directory exists with correct permissions
        if !path.exists() {
            debug!("Creating Sled data directory at {:?}", path);
            std::fs::create_dir_all(&path).map_err(|e| {
                error!("Failed to create Sled data directory at {:?}: {}", path, e);
                GraphError::Io(e)
            })?;
            #[cfg(unix)]
            {
                let mut perms = std::fs::metadata(path)
                    .map_err(|e| GraphError::Io(e))?
                    .permissions();
                perms.set_mode(0o755);
                std::fs::set_permissions(path, perms).map_err(|e| GraphError::Io(e))?;
            }
        }

        // Passively remove lock file with retries
        let lock_path = path.join("db.lck");
        for attempt in 1..=3 {
            if lock_path.exists() {
                warn!("Found Sled lock file at {:?}", lock_path);
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
                        return Err(GraphError::StorageError(format!("Failed to remove lock file: {}", e)));
                    }
                }
            } else {
                info!("No Sled lock file found at {:?}", lock_path);
                break;
            }
        }

        // Initialize Sled with extended retry loop
        const MAX_RETRIES: u32 = 30;
        const BASE_DELAY_MS: u64 = 1000;
        let mut attempt = 0;
        let mut db = None;

        while attempt < MAX_RETRIES {
            info!("Attempt {} of {} to open Sled DB at {:?}", attempt + 1, MAX_RETRIES, path);
            match Config::new()
                .path(path)
                .use_compression(false)
                .flush_every_ms(None)
                .open()
            {
                Ok(db_instance) => {
                    info!("Successfully opened Sled DB at {:?}", path);
                    db = Some(db_instance);
                    break;
                }
                Err(e) => {
                    error!("Failed to open Sled DB on attempt {}: {}. Error details: {:?}", attempt + 1, e, e);
                    if e.to_string().contains("WouldBlock") {
                        warn!("Detected lock contention. Attempt {} of {}. Retrying after delay.", attempt + 1, MAX_RETRIES);
                        run_sync(log_lock_file_diagnostics(lock_path.clone()))?;
                        // Do not call kill_processes; rely on passive lock removal
                        tokio::time::sleep(Duration::from_millis(BASE_DELAY_MS * (attempt as u64 + 1))).await;
                        attempt += 1;
                        continue;
                    } else if e.to_string().contains("corruption") {
                        warn!("Detected Sled database corruption at {:?}", path);
                        Self::backup_and_recreate_database(path)?;
                        match Config::new()
                            .path(path)
                            .use_compression(false)
                            .flush_every_ms(None)
                            .open()
                        {
                            Ok(db_instance) => {
                                info!("Successfully opened Sled DB after recovery at {:?}", path);
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
            error!("Failed to open Sled DB after {} attempts.", MAX_RETRIES);
            run_sync(log_lock_file_diagnostics(lock_path.clone())).unwrap_or_else(|e| {
                error!("Failed to log lock file diagnostics: {}", e);
            });
            GraphError::StorageError(format!("Failed to open Sled DB after {} attempts due to lock error.", MAX_RETRIES))
        })?;

        let vertices = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edges = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;

        let storage = SledStorage {
            db,
            vertices,
            edges,
            config: config.clone(),
            running: TokioMutex::new(true),
        };

        // Register SIGTERM handler for graceful shutdown
        #[cfg(unix)]
        {
            let path_clone = path.to_path_buf();
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
    
    /// Safely removes any existing lock files without killing processes.
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
                            return Err(GraphError::StorageError(format!("Failed to remove lock file: {}", e)));
                        }
                    }
                    Err(e) => {
                        error!("Failed to remove lock file: {}", e);
                        return Err(GraphError::StorageError(format!("Failed to remove lock file: {}", e)));
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
    pub async fn force_reset(config: &SledConfig) -> GraphResult<Self> {
        warn!("FORCE RESET: Completely destroying and recreating database at {:?}", config.path);

        if config.path.exists() {
            std::fs::remove_dir_all(&config.path).map_err(|e| {
                GraphError::StorageError(format!("Failed to remove database directory: {}", e))
            })?;
        }

        Self::new(config).await
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
        self.db.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let result = self.db.get(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(result.map(|ivec| ivec.to_vec()))
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        self.db.remove(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn flush(&self) -> GraphResult<()> {
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SledStorage {
    async fn clear_data(&self) -> Result<(), GraphError> {
        self.vertices.clear()?;
        self.edges.clear()?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> GraphResult<()> {
        let mut running_guard = self.running.lock().await;
        *running_guard = true;
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        {
            let mut running_guard = self.running.lock().await;
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
        println!("Executing query against SledStorage: {}", query_string);
        Ok(serde_json::json!({
            "status": "success",
            "query": query_string,
            "result": "Sled query execution placeholder"
        }))
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        let key = vertex.id.0.as_bytes().to_vec();
        let value = serialize_vertex(&vertex)?;
        self.vertices.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes().to_vec();
        let result = self.vertices.get(&key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(result.map(|bytes| deserialize_vertex(&bytes)).transpose()?)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.delete_vertex(&vertex.id.into()).await?;
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key = id.as_bytes().to_vec();
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
        Ok(())
    }

    async fn get_all_vertices(&self) -> GraphResult<Vec<Vertex>> {
        let mut vertices = Vec::new();
        for item in self.vertices.iter() {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertex = deserialize_vertex(&value)?;
            vertices.push(vertex);
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        if self.get_vertex(&edge.outbound_id.into()).await?.is_none() || self.get_vertex(&edge.inbound_id.into()).await?.is_none() {
            return Err(GraphError::InvalidData("One or both vertices for the edge do not exist.".to_string()));
        }

        let key = create_edge_key(&edge.outbound_id.into(), &edge.t, &edge.inbound_id.into())?;
        let value = serialize_edge(&edge)?;
        self.edges.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        let result = self.edges.get(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(result.map(|bytes| deserialize_edge(&bytes)).transpose()?)
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        self.create_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let key = create_edge_key(&(*outbound_id).into(), edge_type, &(*inbound_id).into())?;
        self.edges.remove(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_all_edges(&self) -> GraphResult<Vec<Edge>> {
        let mut edges = Vec::new();
        for item in self.edges.iter() {
            let (_key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edge = deserialize_edge(&value)?;
            edges.push(edge);
        }
        Ok(edges)
    }

    async fn close(&self) -> GraphResult<()> {
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        info!("SledStorage closed and flushed.");
        Ok(())
    }
}
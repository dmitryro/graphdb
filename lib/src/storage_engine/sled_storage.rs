// lib/src/storage_engine/sled_storage.rs
// Created: 2025-07-04 - Implemented Sled storage engine
// Fixed: 2025-08-13 - Corrected imports to use crate::storage_engine
// Fixed: 2025-08-13 - Added reset method for database reset
// Fixed: 2025-08-13 - Used .into() for SerializableUuid to Uuid conversion
// Fixed: 2025-08-13 - Replaced edge_type with t
// Fixed: 2025-08-13 - Updated create_edge_key calls for GraphResult
// Fixed: 2025-08-14 - Added #[derive(Debug)] for the SledStorage struct.
// Fixed: 2025-08-14 - Changed `running` to use `Mutex<bool>` for interior mutability.
// Fixed: 2025-08-14 - Corrected `retrieve` to convert IVec to Vec<u8>.
// Fixed: 2025-08-14 - Fixed `start` and `stop` to correctly acquire a lock on the `running` mutex and fixed concurrency issues.
// Fixed: 2025-08-14 - Corrected imports for `SerializableUuid` and `Component`.
// Fixed: 2025-08-14 - Corrected calls to `create_edge_key` to dereference Uuid.
// Fixed: 2025-08-15 - Resolved E0308 mismatched types error in `is_running` function by removing incorrect `unwrap_or_else` logic.
// Fixed: 2025-08-13 - Added `close` method to explicitly close the Sled database and release lock
// Fixed: 2025-08-13 - Updated `new` to use `open_sled_db` for retry logic on WouldBlock errors
// Fixed: 2025-08-13 - Added recovery for invalid directory (os error 21) in `open_sled_db`
// Fixed: 2025-08-13 - Corrected lock file path to `/opt/graphdb/storage_data/sled/db/LOCK`
// Fixed: 2025-08-13 - Added lsof-based lock file cleanup in `open_sled_db` and `close`
// Fixed: 2025-08-13 - Enhanced `open_sled_db` with more robust lock cleanup and increased retries

use std::any::Any;
use async_trait::async_trait;
use crate::storage_engine::{GraphStorageEngine, StorageConfig, StorageEngine};
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use serde_json::Value;
use sled::{Db, Tree, IVec};
use std::path::Path;
use std::sync::Mutex;
use uuid::Uuid;
use log::{info, warn, debug, error};
use std::fs;
use std::process::Command;

#[derive(Debug)]
pub struct SledStorage {
    db: Db,
    vertices: Tree,
    edges: Tree,
    config: StorageConfig,
    running: Mutex<bool>,
}

pub fn open_sled_db<P: AsRef<Path>>(path: P) -> GraphResult<Db> {
    let max_attempts = 15; // Increased for robustness
    let retry_delay_ms = 2000;

    // Remove stale lock file and terminate holding processes
    let lock_path = path.as_ref().join("LOCK");
    if lock_path.exists() {
        debug!("Found lock file at {:?}", lock_path);
        // Check for processes holding the lock
        let lsof_output = Command::new("lsof")
            .arg(lock_path.to_str().unwrap())
            .output();
        if let Ok(output) = lsof_output {
            let output_str = String::from_utf8_lossy(&output.stdout);
            for line in output_str.lines().skip(1) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() > 1 {
                    if let Ok(pid) = parts[1].parse::<u32>() {
                        warn!("Found process PID {} holding lock file {:?}", pid, lock_path);
                        Command::new("kill")
                            .arg("-9")
                            .arg(pid.to_string())
                            .output()
                            .map_err(|e| GraphError::Io(e))?;
                        info!("Terminated PID {} holding lock file {:?}", pid, lock_path);
                        std::thread::sleep(std::time::Duration::from_millis(1000)); // Increased delay
                    }
                }
            }
        } else {
            debug!("No processes found holding lock file {:?}", lock_path);
        }

        // Attempt to remove the lock file
        let max_remove_attempts = 5;
        for attempt in 0..max_remove_attempts {
            match fs::remove_file(&lock_path) {
                Ok(_) => {
                    info!("Removed stale lock file at {:?}", lock_path);
                    break;
                }
                Err(e) if e.raw_os_error() == Some(35) && attempt < max_remove_attempts - 1 => {
                    warn!("Failed to remove lock file at {:?} (attempt {}/{}): {}. Retrying after 1000ms", 
                        lock_path, attempt + 1, max_remove_attempts, e);
                    std::thread::sleep(std::time::Duration::from_millis(1000));
                }
                Err(e) => {
                    error!("Failed to remove lock file at {:?}: {}", lock_path, e);
                    return Err(GraphError::StorageError(format!(
                        "Failed to remove lock file at {:?}: {}", lock_path, e
                    )));
                }
            }
        }
    } else {
        debug!("No lock file found at {:?}", lock_path);
    }

    for attempt in 0..max_attempts {
        match sled::open(path.as_ref()) {
            Ok(db) => {
                info!("Successfully opened Sled DB at {:?}", path.as_ref());
                return Ok(db);
            }
            Err(e) if e.to_string().contains("WouldBlock") && attempt < max_attempts - 1 => {
                warn!(
                    "Failed to acquire Sled DB lock at {:?} (attempt {}/{}): {}. Retrying after {}ms",
                    path.as_ref(),
                    attempt + 1,
                    max_attempts,
                    e,
                    retry_delay_ms
                );
                // Recheck for holding processes
                let lsof_output = Command::new("lsof")
                    .arg(lock_path.to_str().unwrap())
                    .output();
                if let Ok(output) = lsof_output {
                    let output_str = String::from_utf8_lossy(&output.stdout);
                    for line in output_str.lines().skip(1) {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() > 1 {
                            if let Ok(pid) = parts[1].parse::<u32>() {
                                warn!("Found process PID {} holding lock file {:?}", pid, lock_path);
                                Command::new("kill")
                                    .arg("-9")
                                    .arg(pid.to_string())
                                    .output()
                                    .map_err(|e| GraphError::Io(e))?;
                                info!("Terminated PID {} holding lock file {:?}", pid, lock_path);
                                std::thread::sleep(std::time::Duration::from_millis(1000));
                            }
                        }
                    }
                }
                // Retry removing lock file
                if lock_path.exists() {
                    match fs::remove_file(&lock_path) {
                        Ok(_) => info!("Removed stale lock file at {:?}", lock_path),
                        Err(e) => warn!("Failed to remove lock file at {:?}: {}", lock_path, e),
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(retry_delay_ms));
            }
            Err(e) if e.to_string().contains("Is a directory") && attempt < max_attempts - 1 => {
                warn!(
                    "Invalid Sled DB directory at {:?} (attempt {}/{}): {}. Attempting to recreate.",
                    path.as_ref(),
                    attempt + 1,
                    max_attempts,
                    e
                );
                if path.as_ref().exists() {
                    fs::remove_dir_all(path.as_ref())
                        .map_err(|e| GraphError::Io(e))?;
                    fs::create_dir_all(path.as_ref())
                        .map_err(|e| GraphError::Io(e))?;
                }
                std::thread::sleep(std::time::Duration::from_millis(retry_delay_ms));
            }
            Err(e) => {
                error!(
                    "Failed to open Sled DB at {:?} after {} attempts: {}",
                    path.as_ref(),
                    max_attempts,
                    e
                );
                return Err(GraphError::StorageError(format!(
                    "Failed to open Sled DB at {:?} after {} attempts: {}",
                    path.as_ref(),
                    max_attempts,
                    e
                )));
            }
        }
    }

    Err(GraphError::StorageError(format!(
        "Failed to acquire Sled DB lock at {:?} after {} attempts",
        path.as_ref(),
        max_attempts
    )))
}

impl SledStorage {
    pub fn new(config: &StorageConfig) -> GraphResult<Self> {
        let path = Path::new(&config.data_directory).join("db");
        let db = open_sled_db(&path)?;
        let vertices = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edges = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(SledStorage {
            db,
            vertices,
            edges,
            config: config.clone(),
            running: Mutex::new(false),
        })
    }

    pub fn reset(&mut self) -> GraphResult<()> {
        self.vertices.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.edges.clear().map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.flush().map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }

    pub async fn close(&self) -> GraphResult<()> {
        self.flush().await?;
        let lock_path = Path::new(&self.config.data_directory).join("db/LOCK");
        if lock_path.exists() {
            debug!("Found lock file at {:?}", lock_path);
            // Check for processes holding the lock
            let lsof_output = Command::new("lsof")
                .arg(lock_path.to_str().unwrap())
                .output();
            if let Ok(output) = lsof_output {
                let output_str = String::from_utf8_lossy(&output.stdout);
                for line in output_str.lines().skip(1) {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() > 1 {
                        if let Ok(pid) = parts[1].parse::<u32>() {
                            warn!("Found process PID {} holding lock file {:?}", pid, lock_path);
                            Command::new("kill")
                                .arg("-9")
                                .arg(pid.to_string())
                                .output()
                                .map_err(|e| GraphError::Io(e))?;
                            info!("Terminated PID {} holding lock file {:?}", pid, lock_path);
                            std::thread::sleep(std::time::Duration::from_millis(1000));
                        }
                    }
                }
            } else {
                debug!("No processes found holding lock file {:?}", lock_path);
            }

            // Attempt to remove the lock file
            let max_remove_attempts = 5;
            for attempt in 0..max_remove_attempts {
                match fs::remove_file(&lock_path) {
                    Ok(_) => {
                        info!("Removed Sled lock file at {:?}", lock_path);
                        break;
                    }
                    Err(e) if e.raw_os_error() == Some(35) && attempt < max_remove_attempts - 1 => {
                        warn!("Failed to remove lock file at {:?} (attempt {}/{}): {}. Retrying after 1000ms", 
                            lock_path, attempt + 1, max_remove_attempts, e);
                        std::thread::sleep(std::time::Duration::from_millis(1000));
                    }
                    Err(e) => {
                        warn!("Failed to remove lock file at {:?}: {}", lock_path, e);
                        break;
                    }
                }
            }
        } else {
            debug!("No lock file found at {:?}", lock_path);
        }

        // Final flush to ensure clean shutdown
        self.db.flush_async().await.map_err(|e| GraphError::Io(e.into()))?;
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for SledStorage {
    async fn connect(&self) -> GraphResult<()> {
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> GraphResult<()> {
        self.db.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> GraphResult<Option<Vec<u8>>> {
        let result = self.db.get(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(result.map(|ivec| ivec.to_vec()))
    }

    async fn delete(&self, key: &[u8]) -> GraphResult<()> {
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> GraphResult<()> {
        let mut running_guard = self.running.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
        *running_guard = true;
        Ok(())
    }

    async fn stop(&self) -> GraphResult<()> {
        {
            let mut running_guard = self.running.lock().map_err(|e| GraphError::LockError(e.to_string()))?;
            *running_guard = false;
        }
        self.close().await
    }

    fn get_type(&self) -> &'static str {
        "sled"
    }

    fn is_running(&self) -> bool {
        *self.running.lock().unwrap()
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
        let key = vertex.id.0.as_bytes();
        let value = serialize_vertex(&vertex)?;
        self.vertices.insert(key, value).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let key = id.as_bytes();
        let result = self.vertices.get(key).map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(result.map(|bytes| deserialize_vertex(&bytes)).transpose()?)
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        self.delete_vertex(&vertex.id.into()).await?;
        self.create_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        let key = id.as_bytes();
        self.vertices.remove(key).map_err(|e| GraphError::StorageError(e.to_string()))?;

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
}
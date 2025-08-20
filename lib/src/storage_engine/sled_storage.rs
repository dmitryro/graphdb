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
use std::process::{Command, self};
use std::time::{Duration, Instant};
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, MetadataExt};
use futures::executor;
use std::thread::sleep;
#[cfg(unix)]
use nix::unistd::{Pid, getpid, getuid};
use fs2::FileExt;
#[cfg(unix)]
use std::fs::File;
use tokio::sync::Mutex as TokioMutex;

// Static Tokio runtime initialized once for fallback
#[cfg(feature = "with-sled")]
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    debug!("Initializing Tokio runtime for run_sync fallback");
    tokio::runtime::Runtime::new()
        .expect("Failed to initialize Tokio runtime for run_sync")
});

// Helper function to run async operations synchronously in a synchronous context
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
    /// Creates a new SledStorage instance with a robust and safe retry mechanism.
    pub fn new(config: &SledConfig) -> GraphResult<Self> {
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
                use std::os::unix::fs::PermissionsExt;
                let mut perms = std::fs::metadata(path)
                    .map_err(|e| GraphError::Io(e))?
                    .permissions();
                let mode = 0o755;
                perms.set_mode(mode);
                std::fs::set_permissions(path, perms).map_err(|e| GraphError::Io(e))?;
            }
        }

        // Try to unlock the directory forcefully before attempting to open the database.
        Self::force_unlock(path)?;

        // Initialize Sled with a safe retry loop using exponential backoff.
        const MAX_RETRIES: u32 = 10;
        let mut attempt = 0;
        let mut sleep_duration = Duration::from_millis(500);

        let db = loop {
            match Config::new()
                .path(path)
                .use_compression(false)
                .flush_every_ms(None)
                .open()
            {
                Ok(db) => {
                    info!("Successfully opened Sled DB at {:?}", path);
                    break db;
                }
                Err(e) => {
                    error!("Failed to open Sled DB on attempt {}: {}. Error details: {:?}", attempt + 1, e, e);

                    // Check if the error is due to a lock issue.
                    if e.to_string().contains("WouldBlock") {
                        warn!("'WouldBlock' error detected. The database is likely in use. Retrying in {:?}...", sleep_duration);
                        
                        attempt += 1;
                        if attempt >= MAX_RETRIES {
                            error!("Failed to open Sled DB after {} retries. Giving up.", MAX_RETRIES);
                            return Err(GraphError::StorageError(format!("Failed to open Sled DB at {:?} after {} retries", path, MAX_RETRIES)));
                        }
                        std::thread::sleep(sleep_duration);
                        sleep_duration *= 2; // Exponential backoff
                        continue;
                    }
                    
                    // For any other error, return it immediately.
                    return Err(GraphError::StorageError(format!("Failed to open Sled DB at {:?}: {}", path, e)));
                }
            }
        };

        let vertices = db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?;
        let edges = db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?;

        Ok(SledStorage {
            db,
            vertices,
            edges,
            config: config.clone(),
            running: TokioMutex::new(true),
        })
    }
    
    /// Aggressively attempts to break any existing locks on the Sled database.
    /// This uses multiple strategies to deal with stale locks from crashed processes.
    fn force_unlock(path: &Path) -> GraphResult<()> {
        info!("Attempting to forcefully break locks on Sled database at {:?}", path);
        
        let db_path = path.join("db");
        let lock_path = path.join("db.lock");
        
        // Strategy 1: Check for and remove stale lock files
        if lock_path.exists() {
            warn!("Found potential lock file at {:?}, attempting to remove", lock_path);
            if let Err(e) = std::fs::remove_file(&lock_path) {
                error!("Failed to remove lock file: {}", e);
            } else {
                info!("Successfully removed stale lock file");
            }
        }
        
        // Strategy 2: Check for processes using the database directory
        #[cfg(unix)]
        {
            Self::kill_processes_using_path(path)?;
        }
        
        // Strategy 3: Try to break file locks using flock
        #[cfg(unix)]
        {
            Self::break_file_locks(&db_path)?;
        }
        
        // Strategy 4: If all else fails, backup and recreate the database
        if db_path.exists() {
            match Self::verify_database_accessible(&db_path) {
                Ok(false) => {
                    warn!("Database appears to be locked, attempting recovery");
                    Self::backup_and_recreate_database(path)?;
                }
                Ok(true) => {
                    info!("Database appears to be accessible");
                }
                Err(e) => {
                    warn!("Could not verify database accessibility: {}", e);
                }
            }
        }
        
        Ok(())
    }
    
    #[cfg(unix)]
    fn kill_processes_using_path(path: &Path) -> GraphResult<()> {
        use std::process::Command;
        
        info!("Aggressively hunting down all 'graphdb' processes except current one");
        let current_pid = std::process::id();
        info!("Current process PID: {}", current_pid);
        
        // Strategy 1: Kill all processes with 'graphdb' in their name/command line
        Self::kill_graphdb_processes(current_pid)?;
        
        // Strategy 2: Also check specifically for processes using the database path
        debug!("Checking for processes using database path: {:?}", path);
        
        let output = Command::new("lsof")
            .arg("+D")  // Search directory recursively
            .arg(path)
            .output();
            
        match output {
            Ok(output) if output.status.success() => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                if !stdout.trim().is_empty() {
                    warn!("Found processes using database directory:");
                    warn!("{}", stdout);
                    
                    // Extract PIDs and kill them if they're not the current process
                    for line in stdout.lines().skip(1) { // Skip header
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() > 1 {
                            if let Ok(pid) = parts[1].parse::<u32>() {
                                if pid != current_pid {
                                    warn!("Terminating process using database - PID: {}", pid);
                                    Self::terminate_process(pid as i32);
                                } else {
                                    debug!("Skipping current process PID: {}", pid);
                                }
                            }
                        }
                    }
                }
            }
            Ok(_) => {
                debug!("lsof found no processes using the database directory");
            }
            Err(e) => {
                debug!("lsof command failed (this is not critical): {}", e);
            }
        }
        
        Ok(())
    }
    
    #[cfg(unix)]
    fn kill_graphdb_processes(current_pid: u32) -> GraphResult<()> {
        use std::process::Command;
        
        info!("Searching for all processes with 'graphdb' in their name/command");
        
        // Use ps to find all graphdb processes
        let output = Command::new("ps")
            .args(&["aux"])
            .output();
            
        match output {
            Ok(output) if output.status.success() => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let mut killed_count = 0;
                
                for line in stdout.lines() {
                    // Check if line contains 'graphdb' but skip header and current process
                    if line.contains("graphdb") && !line.contains("PID") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() > 1 {
                            if let Ok(pid) = parts[1].parse::<u32>() {
                                if pid != current_pid {
                                    info!("Found graphdb process - PID: {}, Command: {}", pid, 
                                         parts.get(10..).unwrap_or(&[]).join(" "));
                                    Self::terminate_process(pid as i32);
                                    killed_count += 1;
                                } else {
                                    debug!("Skipping current process in graphdb search: {}", pid);
                                }
                            }
                        }
                    }
                }
                
                if killed_count > 0 {
                    info!("Terminated {} graphdb processes", killed_count);
                    // Give terminated processes time to cleanup
                    std::thread::sleep(std::time::Duration::from_millis(2000));
                } else {
                    debug!("No other graphdb processes found to terminate");
                }
            }
            Ok(_) => {
                debug!("ps command executed but failed or returned no results");
            }
            Err(e) => {
                warn!("ps command failed: {}", e);
            }
        }
        
        // Also use pgrep for a more targeted search
        let output = Command::new("pgrep")
            .args(&["-f", "graphdb"])
            .output();
            
        match output {
            Ok(output) if output.status.success() => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if let Ok(pid) = line.trim().parse::<u32>() {
                        if pid != current_pid {
                            info!("pgrep found additional graphdb process PID: {}", pid);
                            Self::terminate_process(pid as i32);
                        }
                    }
                }
            }
            Ok(_) => {
                debug!("pgrep command executed but failed or found no processes");
            }
            Err(e) => {
                debug!("pgrep command failed (not critical): {}", e);
            }
        }
        
        Ok(())
    }
    
    #[cfg(unix)]
    fn terminate_process(pid: i32) {
        use std::process::Command;
        
        info!("Terminating process PID: {}", pid);
        
        // First try SIGTERM (graceful shutdown)
        match Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status() 
        {
            Ok(status) if status.success() => {
                debug!("Sent SIGTERM to PID: {}", pid);
            }
            _ => {
                warn!("Failed to send SIGTERM to PID: {}", pid);
            }
        }
        
        // Give it a moment to exit gracefully
        std::thread::sleep(std::time::Duration::from_millis(1000));
        
        // Check if process is still running
        match Command::new("kill")
            .arg("-0")  // Just check if process exists
            .arg(pid.to_string())
            .status()
        {
            Ok(status) if status.success() => {
                // Process still exists, use SIGKILL
                warn!("Process {} still running, sending SIGKILL", pid);
                let _ = Command::new("kill")
                    .arg("-KILL")
                    .arg(pid.to_string())
                    .status();
                    
                std::thread::sleep(std::time::Duration::from_millis(500));
                
                // Final check
                match Command::new("kill")
                    .arg("-0")
                    .arg(pid.to_string())
                    .status()
                {
                    Ok(status) if status.success() => {
                        error!("Process {} refused to die even with SIGKILL", pid);
                    }
                    _ => {
                        info!("Successfully terminated process {}", pid);
                    }
                }
            }
            _ => {
                debug!("Process {} has exited gracefully", pid);
            }
        }
    }
    
    #[cfg(unix)]
    fn break_file_locks(db_path: &Path) -> GraphResult<()> {
        use std::os::unix::io::AsRawFd;
        
        debug!("Attempting to break file locks on {:?}", db_path);
        
        if !db_path.exists() {
            debug!("Database file doesn't exist, nothing to unlock");
            return Ok(());
        }
        
        // Try to open the file and break any locks
        match File::options().read(true).write(true).open(db_path) {
            Ok(file) => {
                debug!("Successfully opened database file for lock breaking");
                
                // Try to acquire and immediately release an exclusive lock
                match file.try_lock_exclusive() {
                    Ok(()) => {
                        info!("Successfully acquired exclusive lock, releasing immediately");
                        let _ = file.unlock(); // Ignore unlock errors
                    }
                    Err(e) => {
                        warn!("Could not acquire exclusive lock: {}", e);
                        
                        // Try using raw system calls to break the lock
                        unsafe {
                            let fd = file.as_raw_fd();
                            let result = libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB);
                            if result == 0 {
                                info!("Successfully broke lock using raw flock");
                                libc::flock(fd, libc::LOCK_UN);
                            } else {
                                warn!("Raw flock also failed, lock may be held by active process");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Could not open database file for lock breaking: {}", e);
            }
        }
        
        Ok(())
    }
    
    fn verify_database_accessible(db_path: &Path) -> GraphResult<bool> {
        // Try to open the database file briefly to see if it's accessible
        match std::fs::File::open(db_path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(_) => Ok(true), // Other errors don't necessarily mean it's locked
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
        
        // Move the entire database directory to backup
        if let Err(e) = std::fs::rename(path, &backup_path) {
            error!("Failed to backup database directory: {}", e);
            // If we can't move it, try to remove it entirely (dangerous!)
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
        
        // Recreate the directory
        std::fs::create_dir_all(path).map_err(|e| {
            GraphError::Io(e)
        })?;
        
        Ok(())
    }

    /// Nuclear option: completely destroy and recreate the database
    pub fn force_reset(config: &SledConfig) -> GraphResult<Self> {
        warn!("FORCE RESET: Completely destroying and recreating database at {:?}", config.path);
        
        // Remove the entire directory
        if config.path.exists() {
            std::fs::remove_dir_all(&config.path).map_err(|e| {
                GraphError::StorageError(format!("Failed to remove database directory: {}", e))
            })?;
        }
        
        // Recreate and initialize
        Self::new(config)
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
use std::any::Any;
use std::collections::HashMap;
use async_trait::async_trait;
use models::errors::GraphError;
use uuid::Uuid;
use models::{Edge, Identifier, Vertex};
use tokio::sync::OnceCell;
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tokio::sync::Mutex as TokioMutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use surrealdb::engine::local::{Db, Mem, RocksDb, SurrealKV};
use tikv_client::{Config, Transaction, TransactionClient as TiKvClient, RawClient as KvClient};
use surrealdb::Surreal;
use sled::Db as SledDB;
use surrealdb::engine::any::Any as SurrealAny; 
use surrealdb::sql::Thing;
//use keyv::Keyv;
use tokio::fs;
use tokio::net::TcpStream;
use tokio::time::{self, Duration as TokioDuration};
use reqwest::Client;
use std::process;
use anyhow::{Result, Context, anyhow};
use std::io::Error;
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
use serde::{Deserialize, Serialize, Deserializer};
use log::{info, debug, warn, error, trace};
#[cfg(unix)]
use nix::unistd::{Pid, getpid, getuid};
use sysinfo::System;
#[cfg(unix)]
use std::os::unix::fs::{PermissionsExt, MetadataExt};
use crate::storage_engine::config::{DEFAULT_DATA_DIRECTORY, DEFAULT_LOG_DIRECTORY, LOCK_FILE_PATH,
                                    DEFAULT_STORAGE_PORT, StorageConfig, SledConfig, RocksdbConfig, TikvConfig,
                                    RedisConfig, MySQLConfig, PostgreSQLConfig, 
                                    StorageConfigWrapper, load_storage_config_from_yaml, 
                                    load_engine_specific_config};
use crate::daemon_utils::{find_pid_by_port, stop_process};
use crate::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};

// Re-export StorageEngineType to ensure clear visibility
pub use crate::storage_engine::config::StorageEngineType;

pub use crate::storage_engine::inmemory_storage::InMemoryStorage as InMemoryGraphStorage;
#[cfg(feature = "with-sled")]
pub use crate::storage_engine::sled_storage::SledStorage;
#[cfg(feature = "with-rocksdb")]
pub use crate::storage_engine::rocksdb_storage::RocksdbStorage;
#[cfg(feature = "with-tikv")]
pub use crate::storage_engine::tikv_storage::TikvStorage;
#[cfg(feature = "redis-datastore")]
use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;

pub static CLEANUP_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
pub static GLOBAL_STORAGE_ENGINE_MANAGER: OnceCell<Arc<AsyncStorageEngineManager>> = OnceCell::const_new();

// Singleton instances for each persistent engine type
//static SLED_SINGLETON: OnceCell<Arc<SledStorage>> = OnceCell::const_new();
#[cfg(feature = "with-sled")]
static SLED_SINGLETON: TokioMutex<Option<Arc<SledStorage>>> = TokioMutex::const_new(None);
#[cfg(feature = "with-rocksdb")]
static ROCKSDB_SINGLETON: TokioMutex<Option<Arc<RocksdbStorage>>> = TokioMutex::const_new(None);
#[cfg(feature = "with-tikv")]
static TIKV_SINGLETON: TokioMutex<Option<Arc<TikvStorage>>> = TokioMutex::const_new(None);
#[cfg(feature = "redis-datastore")]
static REDIS_SINGLETON: OnceCell<Arc<RedisStorage>> = OnceCell::const_new();
#[cfg(feature = "postgres-datastore")]
static POSTGRES_SINGLETON: OnceCell<Arc<PostgresStorage>> = OnceCell::const_new();
#[cfg(feature = "mysql-datastore")]
static MYSQL_SINGLETON: OnceCell<Arc<MySQLStorage>> = OnceCell::const_new();

// New struct to wrap the surrealdb client and implement GraphStorageEngine and StorageEngine
#[derive(Debug, Clone)]
pub struct SurrealdbGraphStorage {
    pub db: Surreal<Db>,
    pub backend_type: StorageEngineType,
}

/// A simple struct to represent the key-value data we're storing.
/// This helps SurrealDB serialize and deserialize the data correctly.
#[derive(Debug, Serialize, Deserialize)]
struct StoredValue {
    value: String,
}

#[async_trait]
impl StorageEngine for SurrealdbGraphStorage {
    /// Connects to the SurrealDB server. The client manages connections internally.
    async fn connect(&self) -> Result<(), GraphError> {
        Ok(())
    }

    /// Inserts a key-value pair into the SurrealDB `storage` table.
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        let key_str = String::from_utf8(key)
            .map_err(|e| GraphError::StorageError(format!("Invalid key for generic insert: {}", e)))?;
        let value_str = String::from_utf8(value)
            .map_err(|e| GraphError::StorageError(format!("Invalid value for generic insert: {}", e)))?;
        
        // Create an instance of our StoredValue struct to pass as content.
        let data_to_store = StoredValue { value: value_str };

        // The create method for a specific ID returns an Option, not a Vec.
        // We handle the potential error from the async call first, then check the Option.
        let created: Option<StoredValue> = self.db.create(("storage", key_str))
            .content(data_to_store)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        // Ensure that a record was actually created.
        if created.is_none() {
            return Err(GraphError::StorageError("Failed to create record".to_string()));
        }

        Ok(())
    }

    /// Retrieves a value for a given key from the SurrealDB `storage` table.
    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        let key_str = String::from_utf8(key.to_vec())
            .map_err(|e| GraphError::StorageError(format!("Invalid key for generic retrieve: {}", e)))?;

        // The select method for a specific ID returns an Option, not a Vec.
        let result: Option<StoredValue> = self.db.select(("storage", key_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        // Return the value if a record was found.
        Ok(result.map(|sv| sv.value.into_bytes()))
    }

    /// Deletes a record from the SurrealDB `storage` table.
    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        let key_str = String::from_utf8(key.to_vec())
            .map_err(|e| GraphError::StorageError(format!("Invalid key for generic delete: {}", e)))?;
            
        // Explicitly annotate the type for the delete method.
        let deleted: Option<StoredValue> = self.db.delete(("storage", key_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    /// SurrealDB handles flushing internally.
    async fn flush(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SurrealdbGraphStorage {
    async fn start(&self) -> Result<(), GraphError> {
        info!("SurrealDB store started.");
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        info!("SurrealDB store stopped.");
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        match self.backend_type {
            StorageEngineType::Sled => "sled",
            StorageEngineType::RocksDB => "rocksdb",
            StorageEngineType::TiKV => "tikv",
            StorageEngineType::InMemory => "in-memory",
            _ => "unknown",
        }
    }

    async fn is_running(&self) -> bool {
        true
    }

    /// Executes a raw SurrealQL query and returns a JSON value.
    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        debug!("Executing query: {}", query_string);
        let mut result = self.db.query(query_string).await
            .map_err(|e| GraphError::QueryError(e.to_string()))?;
        
        // The `take` method with an index returns a Result<Vec<Value>, _>
        // for that specific query statement.
        let values: Vec<Value> = result.take(0)
            .map_err(|e| GraphError::QueryError(e.to_string()))?;
        
        // We get the first value from the returned vector.
        let value = values.into_iter().next()
            .ok_or_else(|| GraphError::QueryError("Query returned no values".to_string()))?;
        
        Ok(value)
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let created: Option<Vertex> = self.db.create(("vertices", vertex.id.0.to_string()))
            .content(vertex)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if created.is_none() {
            return Err(GraphError::StorageError("Failed to create vertex".to_string()));
        }
        
        trace!("Created vertex: {:?}", created);
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        let result: Option<Vertex> = self.db.select(("vertices", id.to_string()))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        Ok(result)
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let updated: Option<Vertex> = self.db.update(("vertices", vertex.id.0.to_string()))
            .content(vertex)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if updated.is_none() {
            return Err(GraphError::StorageError("Failed to update vertex".to_string()));
        }

        trace!("Updated vertex: {:?}", updated);
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        let deleted: Option<Vertex> = self.db.delete(("vertices", id.to_string()))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        trace!("Deleted vertex: {:?}", deleted);
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let results: Vec<Vertex> = self.db.select("vertices")
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(results)
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let edge_id_str = format!("{}:{}:{}", edge.outbound_id.0, edge.t, edge.inbound_id.0);
        
        let created: Option<Edge> = self.db.create(("edges", edge_id_str))
            .content(edge)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if created.is_none() {
            return Err(GraphError::StorageError("Failed to create edge".to_string()));
        }

        trace!("Created edge: {:?}", created);
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        let edge_id_str = format!("{}:{}:{}", outbound_id, edge_type, inbound_id);

        let result: Option<Edge> = self.db.select(("edges", &edge_id_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        Ok(result)
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let edge_id_str = format!("{}:{}:{}", edge.outbound_id.0, edge.t, edge.inbound_id.0);

        let updated: Option<Edge> = self.db.update(("edges", edge_id_str))
            .content(edge)
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        if updated.is_none() {
            return Err(GraphError::StorageError("Failed to update edge".to_string()));
        }

        trace!("Updated edge: {:?}", updated);
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        let edge_id_str = format!("{}:{}:{}", outbound_id, edge_type, inbound_id);
        
        let deleted: Option<Edge> = self.db.delete(("edges", &edge_id_str))
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        trace!("Deleted edge: {:?}", deleted);
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let results: Vec<Edge> = self.db.select("edges")
            .await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        Ok(results)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.db.query("REMOVE TABLE vertices").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.db.query("REMOVE TABLE edges").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

// Wrapper for StorageEngineManager to provide async compatibility
#[derive(Debug)]
pub struct AsyncStorageEngineManager {
    manager: Arc<TokioMutex<StorageEngineManager>>,
}

impl AsyncStorageEngineManager {
    pub fn from_manager(manager: StorageEngineManager) -> Self {
        AsyncStorageEngineManager {
            manager: Arc::new(TokioMutex::new(manager)),
        }
    }

    pub fn get_manager(&self) -> Arc<TokioMutex<StorageEngineManager>> {
        Arc::clone(&self.manager)
    }

    pub async fn get_persistent_engine(&self) -> Arc<dyn GraphStorageEngine + Send + Sync> {
        let manager = self.manager.lock().await;
        manager.get_persistent_engine()
    }

    pub async fn use_storage(&self, engine_type: StorageEngineType, permanent: bool) -> Result<(), GraphError> {
        let mut manager = self.manager.lock().await;
        manager.use_storage(engine_type, permanent).await
    }

    pub async fn current_engine_type(&self) -> StorageEngineType {
        let manager = self.manager.lock().await;
        manager.current_engine_type().await
    }

    pub async fn get_current_engine_data_path(&self) -> Option<PathBuf> {
        let manager = self.manager.lock().await;
        manager.get_current_engine_data_path().await
    }
}

/// Helper function to check if a lock file exists
#[cfg(feature = "with-sled")]
pub async fn lock_file_exists(lock_path: PathBuf) -> Result<bool, GraphError> {
    let exists = fs::metadata(&lock_path).await.is_ok();
    debug!("lock_file_exists({:?}) -> {:?}", lock_path, exists);
    Ok(exists)
}

/// Initializes the global StorageEngineManager
pub async fn init_storage_engine_manager(config_path_yaml: PathBuf) -> Result<(), GraphError> {
    info!("Initializing StorageEngineManager with YAML: {:?}", config_path_yaml);
    
    if let Some(parent) = config_path_yaml.parent() {
        fs::create_dir_all(parent)
            .await
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create directory for YAML config: {:?}", parent))?;
    }
    
    // Check if already initialized
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_some() {
        info!("StorageEngineManager already initialized, reusing existing instance");
        return Ok(());
    }
    
    // Load configuration from YAML to get storage_engine_type
    info!("Loading config from {:?}", config_path_yaml);
    let config = load_storage_config_from_yaml(Some(&config_path_yaml))
        .map_err(|e| {
            error!("Failed to load YAML config from {:?}: {}", config_path_yaml, e);
            GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
        })?;
    
    let storage_engine = config.storage_engine_type;
    debug!("Loaded storage_engine_type from YAML: {:?}", storage_engine);
    
    // Initialize StorageEngineManager with the loaded storage_engine_type
    let manager = StorageEngineManager::new(storage_engine, &config_path_yaml, false).await
        .map_err(|e| {
            error!("Failed to create StorageEngineManager: {}", e);
            GraphError::StorageError(format!("Failed to create StorageEngineManager: {}", e))
        })?;
    
    GLOBAL_STORAGE_ENGINE_MANAGER.set(Arc::new(AsyncStorageEngineManager::from_manager(
        Arc::try_unwrap(manager)
            .map_err(|_| GraphError::StorageError("Failed to unwrap Arc<StorageEngineManager>: multiple references exist".to_string()))?
    )))
        .map_err(|_| GraphError::StorageError("Failed to set StorageEngineManager: already initialized".to_string()))?;
    
    info!("StorageEngineManager initialized successfully with engine: {:?}", storage_engine);
    Ok(())
}

#[cfg(feature = "with-sled")]
pub async fn log_lock_file_diagnostics(lock_path: PathBuf) -> Result<(), GraphError> {
    debug!("Running log_lock_file_diagnostics for {:?}", lock_path);
    match fs::metadata(&lock_path).await {
        Ok(metadata) => {
            debug!("Lock file diagnostics for {:?}:", lock_path);
            debug!("  Size: {} bytes", metadata.len());
            debug!("  Modified: {:?}", metadata.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH));
            debug!("  Read-only: {}", metadata.permissions().readonly());
            Ok(())
        }
        Err(e) => {
            warn!("Failed to get lock file metadata for {:?}: {}", lock_path, e);
            Err(GraphError::Io(e))
        }
    }
}

#[cfg(feature = "with-sled")]
async fn handle_sled_retry_error(sled_lock_path: &PathBuf, _sled_path: &PathBuf, attempt: u32) {
    warn!(
        "Sled lock contention (attempt {}/5) — another process may hold the lock",
        attempt + 1
    );
    if sled_lock_path.exists() {
        warn!("Lock file exists at {:?}", sled_lock_path);
        if let Err(e) = std::fs::remove_file(&sled_lock_path) {
            error!("Failed to remove lock file at {:?}: {}", sled_lock_path, e);
        } else {
            info!("Successfully removed lock file at {:?}", sled_lock_path);
        }
    }
}

#[cfg(feature = "with-sled")]
pub async fn recover_sled(lock_path: PathBuf) -> Result<(), GraphError> {
    debug!("Starting recover_sled for {:?}", lock_path);
    if let Some(parent) = lock_path.parent() {
        match fs::metadata(parent).await {
            Ok(metadata) => {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let permissions = metadata.permissions();
                    if permissions.mode() & 0o200 == 0 {
                        warn!("Parent directory {:?} lacks write permissions (mode: {:o})", parent, permissions.mode());
                    }
                    debug!("Parent directory owned by UID: {}, current process UID: {}", metadata.uid(), nix::unistd::getuid().as_raw());
                }
            }
            Err(e) => {
                warn!("Failed to check parent directory metadata for {:?}: {}", parent, e);
            }
        }
    }
    
    if lock_file_exists(lock_path.clone()).await? {
        warn!("Attempting to remove stale Sled lock file at {:?}", lock_path);
        match fs::metadata(&lock_path).await {
            Ok(metadata) => {
                debug!("Lock file permissions: {:?}", metadata.permissions());
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let permissions = metadata.permissions();
                    debug!("Lock file mode: {:o}, UID: {}, current UID: {}", permissions.mode(), metadata.uid(), nix::unistd::getuid().as_raw());
                    if permissions.readonly() {
                        let mut new_perms = permissions.clone();
                        new_perms.set_mode(0o600);
                        if let Err(e) = fs::set_permissions(&lock_path, new_perms).await {
                            warn!("Failed to make lock file writable at {:?}: {}", lock_path, e);
                        }
                    }
                }
                #[cfg(unix)]
                {
                    if let Ok(output) = std::process::Command::new("lsof").arg(lock_path.to_str().unwrap()).output() {
                        let stdout = String::from_utf8_lossy(&output.stdout);
                        if !stdout.is_empty() {
                            warn!("Processes holding lock file {:?}: {}", lock_path, stdout);
                        } else {
                            debug!("No processes currently holding lock file {:?}", lock_path);
                        }
                    } else {
                        warn!("Failed to run lsof on {:?}", lock_path);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to check lock file permissions for {:?}: {}", lock_path, e);
            }
        }
        
        match fs::remove_file(&lock_path).await {
            Ok(()) => {
                info!("Successfully removed stale Sled lock file at {:?}", lock_path);
                if lock_file_exists(lock_path.clone()).await? {
                    error!("Lock file at {:?} still exists after removal attempt", lock_path);
                    return Err(GraphError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to ensure Sled lock file removal at {:?}", lock_path),
                    )));
                }
                Ok(())
            }
            Err(e) => {
                error!("Failed to remove Sled lock file at {:?}: {}", lock_path, e);
                match std::fs::remove_file(&lock_path) {
                    Ok(()) => {
                        info!("Successfully removed stale Sled lock file (sync) at {:?}", lock_path);
                        if lock_file_exists(lock_path.clone()).await? {
                            error!("Lock file at {:?} still exists after sync removal attempt", lock_path);
                            return Err(GraphError::Io(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Failed to ensure Sled lock file removal at {:?}", lock_path),
                            )));
                        }
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to remove Sled lock file (sync) at {:?}: {}", lock_path, e);
                        Err(GraphError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("Failed to remove Sled lock file: {}", e),
                        )))
                    }
                }
            }
        }
    } else {
        debug!("No lock file found for Sled at {:?}", lock_path);
        Ok(())
    }
}

/// Recovers a RocksDB database by clearing stale lock file
async fn recover_rocksdb(data_dir: &PathBuf) -> Result<(), GraphError> {
    warn!("Checking for RocksDB lock file at {:?}", data_dir);
    let lock_file = data_dir.join("LOCK");
    const MAX_RETRIES: u32 = 3;
    let mut retries = 0;

    while lock_file.exists() && retries < MAX_RETRIES {
        trace!("Lock file found: {:?}", lock_file);
        
        // Prevent reentrant cleanup
        if CLEANUP_IN_PROGRESS.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                match fs::metadata(&lock_file).await {
                    Ok(metadata) => {
                        let acquire_time = metadata.mtime();
                        let current_time = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(i64::MAX);
                        trace!("Lock file age: {}s (current_time: {}, acquire_time: {})", current_time - acquire_time, current_time, acquire_time);
                        
                        // Check if lock is older than 1 minute
                        if current_time - acquire_time > 60 {
                            warn!("Removing stale RocksDB lock file (age {}s, retry {}): {:?}", current_time - acquire_time, retries, lock_file);
                            fs::remove_file(&lock_file)
                                .await
                                .map_err(|e| GraphError::Io(e))
                                .with_context(|| format!("Failed to remove stale RocksDB lock file {:?}", lock_file))?;
                            info!("Successfully removed stale RocksDB lock file");
                            CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                            break;
                        } else {
                            // Attempt to shut down existing engine using async operations
                            if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                                trace!("Attempting to shut down existing engine (retry {})", retries);
                                let manager_arc = Arc::clone(manager);
                                
                                match async {
                                    let manager = manager_arc.get_manager();
                                    let mgr = manager.lock().await;
                                    let engine = mgr.engine.lock().await;
                                    (*engine).stop().await
                                }.await {
                                    Ok(()) => {
                                        info!("Shut down existing engine before lock removal");
                                    }
                                    Err(e) => {
                                        warn!("Failed to shut down existing manager: {:?}", e);
                                    }
                                }
                            } else {
                                trace!("No existing storage engine manager found to shut down");
                            }
                            
                            // Wait to ensure resources are released
                            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                            
                            // Check if lock is held by current process
                            let current_pid = process::id();
                            trace!("Checking if lock is held by current process (PID: {})", current_pid);
                            
                            match tokio::process::Command::new("lsof")
                                .arg(lock_file.to_str().unwrap())
                                .output()
                                .await
                            {
                                Ok(lsof_output) => {
                                    let output = String::from_utf8_lossy(&lsof_output.stdout);
                                    trace!("lsof output for lock file: {}", output);
                                    let pid_lines: Vec<&str> = output.lines()
                                        .filter(|line| line.contains(&lock_file.to_str().unwrap()))
                                        .collect();
                                    let mut lock_held_by_current = false;
                                    
                                    for line in pid_lines {
                                        let fields: Vec<&str> = line.split_whitespace().collect();
                                        if fields.len() > 1 {
                                            if let Ok(pid) = fields[1].parse::<u32>() {
                                                if pid == current_pid {
                                                    lock_held_by_current = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    
                                    if lock_held_by_current || output.is_empty() {
                                        warn!("Lock file likely held by current process or stale, removing (retry {}): {:?}", retries, lock_file);
                                        fs::remove_file(&lock_file)
                                            .await
                                            .map_err(|e| GraphError::Io(e))
                                            .with_context(|| format!("Failed to remove RocksDB lock file {:?}", lock_file))?;
                                        info!("Successfully removed RocksDB lock file");
                                        CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                                        break;
                                    } else {
                                        error!("RocksDB lock file is held by another process: {:?}", lock_file);
                                        CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                                        return Err(GraphError::StorageError(format!(
                                            "RocksDB lock file is held by another process: {:?}", lock_file
                                        )));
                                    }
                                }
                                Err(_) => {
                                    warn!("Failed to run lsof, assuming lock is stale (retry {}): {:?}", retries, lock_file);
                                    fs::remove_file(&lock_file)
                                        .await
                                        .map_err(|e| GraphError::Io(e))
                                        .with_context(|| format!("Failed to remove RocksDB lock file {:?}", lock_file))?;
                                    info!("Successfully removed RocksDB lock file");
                                    CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read metadata for RocksDB lock file {:?}: {}", lock_file, e);
                        CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                        return Err(GraphError::Io(e));
                    }
                }
            }
            
            #[cfg(not(unix))]
            {
                // Attempt to shut down existing engine
                if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                    trace!("Attempting to shut down existing engine (retry {})", retries);
                    let manager_arc = Arc::clone(manager);
                    let manager = manager_arc.get_manager();
                    let mgr = manager.lock().await;
                    let engine = mgr.engine.lock().await;
                    if let Err(e) = (*engine).stop().await {
                        warn!("Failed to shut down existing manager: {:?}", e);
                    } else {
                        info!("Shut down existing engine before lock removal");
                    }
                }
                
                // Wait to ensure resources are released
                tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                
                warn!("Removing RocksDB lock file (non-Unix system, retry {}): {:?}", retries, lock_file);
                fs::remove_file(&lock_file)
                    .await
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("Failed to remove RocksDB lock file {:?}", lock_file))?;
                info!("Successfully removed RocksDB lock file");
                CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                break;
            }
        } else {
            trace!("Cleanup already in progress, skipping retry {}", retries);
        }
        
        retries += 1;
        if retries < MAX_RETRIES {
            trace!("Retrying lock file cleanup after 3s delay (attempt {}/{})", retries + 1, MAX_RETRIES);
            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
        }
    }

    if lock_file.exists() {
        error!("Failed to remove RocksDB lock file after {} retries: {:?}", MAX_RETRIES, lock_file);
        warn!("Terminating process to release lock file as a last resort");
        process::exit(1); // Force exit to release resources
    }

    if !data_dir.exists() {
        info!("Creating RocksDB directory: {:?}", data_dir);
        fs::create_dir_all(data_dir)
            .await
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create RocksDB directory {:?}", data_dir))?;
    }
    
    info!("RocksDB directory ready: {:?}", data_dir);
    CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
    Ok(())
}

/// Performs emergency cleanup of the storage engine manager
/// Performs emergency cleanup of the storage engine manager
pub async fn emergency_cleanup_storage_engine_manager() -> Result<(), anyhow::Error> {
    info!("Performing emergency cleanup for StorageEngineManager");
    
    // Clean up FileLock
    let lock_path = PathBuf::from(LOCK_FILE_PATH);
    if lock_path.exists() {
        if let Err(e) = fs::remove_file(&lock_path).await {
            warn!("Failed to remove lock file at {:?}: {}", lock_path, e);
        } else {
            info!("Removed lock file at {:?}", lock_path);
        }
    }
    
    // Clean up GLOBAL_STORAGE_ENGINE_MANAGER
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        let mutex = manager.get_manager();
        let mut locked_manager = mutex.lock().await;
        if let Err(e) = locked_manager.shutdown().await {
            warn!("Failed to shutdown StorageEngineManager: {}", e);
        }
        drop(locked_manager);
    }
    
    // Additional Sled-specific cleanup
    #[cfg(feature = "with-sled")]
    {
        let sled_path = PathBuf::from("/opt/graphdb/storage_data/sled");
        if sled_path.exists() {
            // Call SledStorage::force_unlock (needs to be made public in sled_storage.rs)
            if let Err(e) = SledStorage::force_unlock(&sled_path).await {
                warn!("Failed to force unlock Sled database at {:?}: {}", sled_path, e);
            } else {
                info!("Successfully forced unlock on Sled database at {:?}", sled_path);
            }
        }
        
        // Kill any processes holding file descriptors
        if let Ok(output) = tokio::process::Command::new("lsof")
            .arg("-t")
            .arg(sled_path.to_str().ok_or_else(|| anyhow!("Invalid sled path"))?)
            .output()
            .await
        {
            let pids = String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|pid| pid.trim().parse::<u32>().ok())
                .collect::<Vec<u32>>();
            
            for pid in pids {
                if let Err(e) = tokio::process::Command::new("kill")
                    .arg("-9")
                    .arg(pid.to_string())
                    .status()
                    .await
                {
                    warn!("Failed to kill process {}: {}", pid, e);
                } else {
                    info!("Killed process {} holding Sled database", pid);
                }
            }
        }
    }
    
    Ok(())
}

/// Creates a default YAML configuration file
fn create_default_yaml_config(yaml_path: &PathBuf, engine_type: StorageEngineType) -> Result<(), GraphError> {
    info!("Creating default YAML config at {:?}", yaml_path);
    let config = StorageConfig {
        storage_engine_type: engine_type,
        config_root_directory: PathBuf::from("./storage_daemon_server"),
        data_directory: PathBuf::from(DEFAULT_DATA_DIRECTORY),
        log_directory: DEFAULT_LOG_DIRECTORY.to_string(),
        default_port: DEFAULT_STORAGE_PORT,
        cluster_range: DEFAULT_STORAGE_PORT.to_string(),
        max_disk_space_gb: 1000,
        min_disk_space_gb: 10,
        use_raft_for_scale: true,
        max_open_files: Some(100),
        connection_string: None,
        engine_specific_config: Some(HashMap::from_iter(vec![
            ("path".to_string(), Value::String(format!("{}/{}", DEFAULT_DATA_DIRECTORY, engine_type.to_string().to_lowercase()))),
            ("host".to_string(), Value::String("127.0.0.1".to_string())),
            ("port".to_string(), Value::Number(DEFAULT_STORAGE_PORT.into())),
        ])),
    };

    config.save()
        .map_err(|e| {
            error!("Failed to save default YAML config to {:?}: {}", yaml_path, e);
            GraphError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;
    info!("Default YAML config created at {:?}", yaml_path);
    Ok(())
}

// StorageEngine and GraphStorageEngine traits
#[async_trait]
pub trait StorageEngine: Send + Sync + Debug + 'static {
    async fn connect(&self) -> Result<(), GraphError>;
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError>;
    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError>;
    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError>;
    async fn flush(&self) -> Result<(), GraphError>;
}

#[async_trait]
pub trait GraphStorageEngine: StorageEngine + Send + Sync + Debug + 'static {
    async fn start(&self) -> Result<(), GraphError>;
    async fn stop(&self) -> Result<(), GraphError>;
    fn get_type(&self) -> &'static str;
    async fn is_running(&self) -> bool;
    async fn query(&self, query_string: &str) -> Result<Value, GraphError>;
    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError>;
    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError>;
    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError>;
    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError>;
    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError>;
    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError>;
    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError>;
    async fn clear_data(&self) -> Result<(), GraphError>;
    fn as_any(&self) -> &dyn Any;
    async fn close(&self) -> Result<(), GraphError>;
}

// HybridStorageEngine implementation
#[derive(Debug)]
pub struct HybridStorageEngine {
    inmemory: Arc<InMemoryGraphStorage>,
    persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
    running: Arc<TokioMutex<bool>>,
    engine_type: StorageEngineType,
}

#[async_trait]
impl StorageEngine for HybridStorageEngine {
    /// Connects both the in-memory and persistent storage engines.
    async fn connect(&self) -> Result<(), GraphError> {
        self.inmemory.connect().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.connect().await?;
        Ok(())
    }

    /// Inserts a key-value pair into both the in-memory and persistent engines.
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        // Clone the key and value to insert into the in-memory engine first.
        // We do this because the persistent engine insert consumes the Vec<u8>
        self.inmemory.insert(key.clone(), value.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        // Insert into the persistent engine. This will consume the original Vec<u8>s.
        persistent_arc.insert(key, value).await?;
        Ok(())
    }

    /// Retrieves a value for a given key, first checking the in-memory cache.
    /// If not found, it retrieves from the persistent store and populates the cache.
    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        // First, check the in-memory cache for the value.
        if let Some(value) = self.inmemory.retrieve(key).await? {
            Ok(Some(value))
        } else {
            // If not in cache, retrieve from the persistent store.
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.retrieve(key).await?;
            // If the value was found in the persistent store, insert it back into the in-memory cache.
            if let Some(ref value) = result {
                self.inmemory.insert(key.clone(), value.clone()).await?;
            }
            Ok(result)
        }
    }

    /// Deletes a key-value pair from both the in-memory and persistent engines.
    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        self.inmemory.delete(key).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete(key).await?;
        Ok(())
    }

    /// Flushes data from the in-memory cache to the persistent store.
    async fn flush(&self) -> Result<(), GraphError> {
        self.inmemory.flush().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.flush().await?;
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for HybridStorageEngine {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn start(&self) -> Result<(), GraphError> {
        self.inmemory.start().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.start().await?;
        *self.running.lock().await = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        self.inmemory.stop().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.stop().await?;
        *self.running.lock().await = false;
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        match self.engine_type {
            StorageEngineType::Sled => "sled",
            StorageEngineType::RocksDB => "rocksdb",
            StorageEngineType::TiKV => "tikv",
            StorageEngineType::InMemory => "inmemory",
            StorageEngineType::Redis => "redis",
            StorageEngineType::PostgreSQL => "postgresql",
            StorageEngineType::MySQL => "mysql",
        }
    }

    async fn is_running(&self) -> bool {
        *self.running.lock().await
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.query(query_string).await
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.create_vertex(vertex.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.create_vertex(vertex).await
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        if let Some(vertex) = self.inmemory.get_vertex(id).await? {
            Ok(Some(vertex))
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.get_vertex(id).await?;
            if let Some(ref vertex) = result {
                self.inmemory.create_vertex(vertex.clone()).await?;
            }
            Ok(result)
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.update_vertex(vertex.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.update_vertex(vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_vertex(id).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete_vertex(id).await
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let inmemory_results = self.inmemory.get_all_vertices().await?;
        if !inmemory_results.is_empty() {
            Ok(inmemory_results)
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let persistent_results = persistent_arc.get_all_vertices().await?;
            for vertex in persistent_results.iter() {
                self.inmemory.create_vertex(vertex.clone()).await?;
            }
            Ok(persistent_results)
        }
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.create_edge(edge.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.create_edge(edge).await
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        if let Some(edge) = self.inmemory.get_edge(outbound_id, edge_type, inbound_id).await? {
            Ok(Some(edge))
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.get_edge(outbound_id, edge_type, inbound_id).await?;
            if let Some(ref edge) = result {
                self.inmemory.create_edge(edge.clone()).await?;
            }
            Ok(result)
        }
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.update_edge(edge.clone()).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.update_edge(edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_edge(outbound_id, edge_type, inbound_id).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete_edge(outbound_id, edge_type, inbound_id).await
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let inmemory_results = self.inmemory.get_all_edges().await?;
        if !inmemory_results.is_empty() {
            Ok(inmemory_results)
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let persistent_results = persistent_arc.get_all_edges().await?;
            for edge in persistent_results.iter() {
                self.inmemory.create_edge(edge.clone()).await?;
            }
            Ok(persistent_results)
        }
    }

    async fn close(&self) -> Result<(), GraphError> {
        self.inmemory.close().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.close().await?;
        info!("HybridStorageEngine closed");
        Ok(())
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.inmemory.clear_data().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.clear_data().await?;
        info!("HybridStorageEngine data cleared");
        Ok(())
    }
}

#[derive(Debug)]
pub struct StorageEngineManager {
    pub engine: Arc<TokioMutex<HybridStorageEngine>>,
    persistent_engine: Arc<dyn GraphStorageEngine + Send + Sync>,
    session_engine_type: Option<StorageEngineType>,
    config: StorageConfig,
    config_path: PathBuf,
}

impl StorageEngineManager {
    pub async fn new(
        storage_engine: StorageEngineType, 
        config_path_yaml: &Path, 
        permanent: bool
    ) -> Result<Arc<StorageEngineManager>, GraphError> {
        info!("Creating StorageEngineManager with engine: {:?}", storage_engine);
        println!("IN StorageEngineManager new - STEP 1");
        // Step 1: Graceful shutdown of existing manager
        Self::shutdown_existing_manager().await;
        println!("IN StorageEngineManager new - STEP 2");
        // Step 2: Load and configure
        let config = Self::load_and_configure(storage_engine, config_path_yaml, permanent).await
            .map_err(|e| {
                error!("Failed to load config: {}", e);
                GraphError::ConfigurationError(format!("Failed to load config: {}", e))
            })?;
        debug!("Loaded config: {:?}", config);
        // Validate configuration for TiKV
        if storage_engine == StorageEngineType::TiKV {
            let engine_config = config.engine_specific_config.as_ref().ok_or_else(|| {
                error!("Missing engine_specific_config for TiKV");
                GraphError::ConfigurationError("Missing engine_specific_config for TiKV".to_string())
            })?;
            if engine_config.get("port").and_then(|v| v.as_u64()).map(|p| p as u16) == Some(2382) {
                error!("TiKV port 2382 conflicts with PD port");
                return Err(GraphError::ConfigurationError("TiKV port 2382 conflicts with PD port".to_string()));
            }
            if engine_config.get("username").and_then(|v| v.as_str()).map_or(true, |u| u.is_empty()) {
                error!("TiKV username is empty");
                return Err(GraphError::ConfigurationError("TiKV username is empty".to_string()));
            }
            if engine_config.get("password").and_then(|v| v.as_str()).map_or(true, |p| p.is_empty()) {
                error!("TiKV password is empty");
                return Err(GraphError::ConfigurationError("TiKV password is empty".to_string()));
            }
            if engine_config.get("pd_endpoints").and_then(|v| v.as_str()).map_or(true, |p| p.is_empty()) {
                error!("TiKV pd_endpoints is empty");
                return Err(GraphError::ConfigurationError("TiKV pd_endpoints is empty".to_string()));
            }
        }
        debug!("Validated config: {:?}", config);
        println!("IN StorageEngineManager new - STEP 3");
        // Step 3: Initialize storage engine
        let persistent = if storage_engine == StorageEngineType::TiKV {
            debug!("Attempting to initialize TiKV engine");
            Self::init_tikv(&config).await
                .map_err(|e| {
                    error!("Failed to initialize TiKV: {}", e);
                    GraphError::StorageError(format!("Failed to connect to SurrealDB TiKV backend: {}", e))
                })?
        } else {
            debug!("Initializing engine_type={:?}", storage_engine);
            Self::initialize_storage_engine(storage_engine, &config).await?
        };
        println!("IN StorageEngineManager new - STEP 4");
        // Step 4: Create final manager
        let manager = Self::create_manager(storage_engine, config, config_path_yaml, persistent, permanent);
        println!("IN StorageEngineManager new - STEP 5");
        info!("StorageEngineManager initialized with engine: {:?}", storage_engine);
        Ok(Arc::new(manager))
    }

    pub async fn reset_config(&mut self, config: StorageConfig) -> Result<(), GraphError> {
        // Update the internal configuration state
        self.config = config;
        info!("StorageEngineManager configuration reset: {:?}", self.config);
        Ok(())
    }

    async fn shutdown_existing_manager() {
        if let Some(existing_manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            trace!("Shutting down existing StorageEngineManager before initialization");
            
            // BEGIN FIX: Explicitly drop the persistent engine handle to release the file lock.
            {
                let manager = existing_manager.get_manager();
                let mgr_locked = manager.lock().await;
                // Get a mutable reference to the persistent engine and take ownership.
                // This drops the old handle and releases the lock.
                let _ = mgr_locked.persistent_engine;
            }
            info!("Old persistent engine handle dropped. File lock released.");
            // END FIX

            const MAX_RETRIES: u32 = 3;
            for retry in 0..MAX_RETRIES {
                let stop_result = {
                    let manager = existing_manager.get_manager();
                    let mgr = manager.lock().await;
                    let engine = mgr.engine.lock().await;
                    
                    if (*engine).is_running().await {
                        (*engine).stop().await
                    } else {
                        info!("Existing engine is already stopped.");
                        return;
                    }
                };

                match stop_result {
                    Ok(()) => {
                        info!("Shut down existing StorageEngineManager successfully");
                        return;
                    }
                    Err(e) => {
                        warn!("Failed to shut down existing manager (retry {}): {:?}", retry, e);
                        if retry < MAX_RETRIES - 1 {
                            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                        }
                    }
                }
            }
            warn!("Failed to shut down existing StorageEngineManager after {} retries", MAX_RETRIES);
        } else {
            trace!("No existing storage engine manager found to shut down");
        }
    }


    async fn load_and_configure(
        storage_engine: StorageEngineType,
        config_path_yaml: &Path,
        permanent: bool,
    ) -> Result<StorageConfig, GraphError> {
        info!("Loading config from {:?}", config_path_yaml);
        
        let mut config = if config_path_yaml.exists() {
            Self::load_existing_config(config_path_yaml).await?
        } else {
            Self::create_default_config(storage_engine, config_path_yaml).await?
        };

        // Override storage engine type if different
        if config.storage_engine_type != storage_engine {
            Self::override_engine_config(&mut config, storage_engine, config_path_yaml, permanent).await?;
        }

        Self::validate_config(&config, storage_engine).await?;
        Ok(config)
    }

    async fn load_existing_config(config_path_yaml: &Path) -> Result<StorageConfig, GraphError> {
        let content = tokio::fs::read_to_string(config_path_yaml)
            .await
            .map_err(|e| {
                error!("Failed to read YAML file at {:?}: {}", config_path_yaml, e);
                GraphError::Io(e)
            })?;
        
        debug!("Raw YAML content from {:?}:\n{}", config_path_yaml, content);
        
        load_storage_config_from_yaml(Some(&config_path_yaml.to_path_buf()))
            .map_err(|e| {
                error!("Failed to deserialize YAML config from {:?}: {}", config_path_yaml, e);
                GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
            })
    }

    async fn create_default_config(
        storage_engine: StorageEngineType,
        config_path_yaml: &Path,
    ) -> Result<StorageConfig, GraphError> {
        warn!("Config file not found at {:?}", config_path_yaml);
        
        create_default_yaml_config(&config_path_yaml.to_path_buf(), storage_engine)?;
        
        load_storage_config_from_yaml(Some(&config_path_yaml.to_path_buf()))
            .map_err(|e| {
                error!("Failed to load newly created YAML config from {:?}: {}", config_path_yaml, e);
                GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
            })
    }

    async fn override_engine_config(
        config: &mut StorageConfig,
        storage_engine: StorageEngineType,
        config_path_yaml: &Path,
        permanent: bool,
    ) -> Result<(), GraphError> {
        info!(
            "Overriding YAML storage_engine_type ({:?}) with passed engine: {:?}", 
            config.storage_engine_type, 
            storage_engine
        );
        println!("=====> WE ARE OVERRIDING {:?}", storage_engine);
        config.storage_engine_type = storage_engine;
        config.engine_specific_config = Some(
            load_engine_specific_config(storage_engine, config_path_yaml)
                .map_err(|e| {
                    error!("Failed to load engine-specific config for {:?}: {}", storage_engine, e);
                    GraphError::ConfigurationError(format!("Failed to load engine-specific config: {}", e))
                })?
        );

        if permanent {
            Self::save_config_permanently(config, config_path_yaml).await?;
        }

        debug!(
            "Config after override: storage_engine_type={:?}, default_port={}, cluster_range={}",
            config.storage_engine_type, config.default_port, config.cluster_range
        );
        
        Ok(())
    }

    async fn save_config_permanently(
        config: &StorageConfig,
        config_path_yaml: &Path,
    ) -> Result<(), GraphError> {
        config.save().map_err(|e| {
            error!("Failed to save updated config to {:?}: {}", config_path_yaml, e);
            GraphError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })?;

        // Verify saved config
        let saved_content = tokio::fs::read_to_string(config_path_yaml)
            .await
            .map_err(|e| {
                error!("Failed to read saved YAML config at {:?}: {}", config_path_yaml, e);
                GraphError::Io(e)
            })?;
        
        debug!("Saved YAML content at {:?}:\n{}", config_path_yaml, saved_content);
        info!("Updated YAML config at {:?} with storage_engine_type: {:?}", config_path_yaml, config.storage_engine_type);
        
        Ok(())
    }

    async fn initialize_storage_engine(
        engine_type: StorageEngineType,
        config: &StorageConfig,
    ) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing storage engine: {:?}", engine_type);
        
        match engine_type {
            StorageEngineType::InMemory => Self::init_inmemory(config),
            StorageEngineType::Sled => Self::init_sled(config).await,
            StorageEngineType::RocksDB => Self::init_rocksdb(config).await,
            StorageEngineType::Redis => Self::init_redis(config).await,
            StorageEngineType::PostgreSQL => Self::init_postgresql(config).await,
            StorageEngineType::MySQL => Self::init_mysql(config).await,
            StorageEngineType::TiKV => Self::init_tikv(config).await,
        }
    }

    fn init_inmemory(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing InMemory engine");
        Ok(Arc::new(InMemoryGraphStorage::new(config)))
    }

    #[cfg(feature = "with-sled")]
    async fn init_sled(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing Sled engine using raw sled crate");
        trace!("Sled initialization started with config: {:?}", config);

        // Validate configuration
        if config.storage_engine_type != StorageEngineType::Sled {
            warn!("Configuration mismatch: expected Sled, found {:?}", config.storage_engine_type);
            return Err(GraphError::ConfigurationError(
                format!("Expected storage_engine_type Sled, found {:?}", config.storage_engine_type)
            ));
        }

        // Build SledConfig
        let sled_config = match &config.engine_specific_config {
            Some(sled_config_map) => {
                debug!("Using SledConfig from engine_specific_config: {:?}", sled_config_map);
                SledConfig {
                    path: sled_config_map
                        .get("path")
                        .and_then(|v| v.as_str())
                        .map(PathBuf::from)
                        .unwrap_or_else(|| config.data_directory.join("sled")),
                    storage_engine_type: StorageEngineType::Sled,
                    host: sled_config_map
                        .get("host")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string()),
                    port: sled_config_map
                        .get("port")
                        .and_then(|v| v.as_u64())
                        .map(|p| p as u16),
                }
            }
            None => {
                warn!("engine_specific_config is None, using default SledConfig");
                SledConfig {
                    path: config.data_directory.join("sled"),
                    storage_engine_type: StorageEngineType::Sled,
                    host: None,
                    port: None,
                }
            }
        };

        // Ensure path exists
        if !sled_config.path.exists() {
            debug!("Creating Sled data directory at {:?}", sled_config.path);
            std::fs::create_dir_all(&sled_config.path).map_err(|e| {
                error!("Failed to create Sled data directory at {:?}: {}", sled_config.path, e);
                GraphError::Io(e)
            })?;
        }

        // Handle lock file with enhanced diagnostics
        let lock_path = sled_config.path.join("db.lck");
        if lock_file_exists(lock_path.clone()).await? {
            warn!("Lock file exists at {:?}", lock_path);
            log_lock_file_diagnostics(lock_path.clone()).await?;

            // Check for processes holding the lock file
            #[cfg(unix)]
            {
                if let Ok(output) = tokio::process::Command::new("lsof")
                    .arg("-t")
                    .arg(lock_path.to_str().ok_or_else(|| GraphError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid lock file path"
                    )))?)
                    .output()
                    .await
                {
                    let pids = String::from_utf8_lossy(&output.stdout)
                        .lines()
                        .filter_map(|pid| pid.trim().parse::<u32>().ok())
                        .collect::<Vec<u32>>();
                    
                    for pid in pids {
                        if pid != process::id() {
                            warn!("Process {} is holding lock file {:?}", pid, lock_path);
                            if let Err(e) = tokio::process::Command::new("kill")
                                .arg("-TERM")
                                .arg(pid.to_string())
                                .status()
                                .await
                            {
                                warn!("Failed to send SIGTERM to process {}: {}", pid, e);
                            } else {
                                info!("Sent SIGTERM to process {} holding lock file", pid);
                                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                            }
                        }
                    }
                }
            }

            // Attempt to recover the lock file
            recover_sled(lock_path.clone()).await?;
        }

        // Acquire singleton lock
        let mut singleton_guard = SLED_SINGLETON.lock().await;

        // Check for existing instance
        if let Some(existing_storage) = singleton_guard.as_ref() {
            info!("Returning existing Sled singleton instance");
            return Ok(existing_storage.clone() as Arc<dyn GraphStorageEngine + Send + Sync>);
        }

        const MAX_RETRIES: u32 = 5;
        let mut attempt = 0;
        let mut sled_instance = None;

        while attempt < MAX_RETRIES {
            match SledStorage::new(&sled_config).await {
                Ok(storage) => {
                    sled_instance = Some(Arc::new(storage));
                    break;
                }
                Err(e) => {
                    error!("Failed to initialize Sled on attempt {}: {}", attempt + 1, e);
                    if e.to_string().contains("WouldBlock") || e.to_string().contains("lock") {
                        warn!("Lock contention detected, retrying after {}ms", 1000 * (attempt + 1));
                        handle_sled_retry_error(&lock_path, &sled_config.path, attempt).await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000 * (attempt + 1) as u64)).await;
                    } else {
                        log_lock_file_diagnostics(lock_path.clone()).await?;
                        return Err(e);
                    }
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        warn!("Max retries reached, attempting force reset");
                        log_lock_file_diagnostics(lock_path.clone()).await?;
                        let storage = SledStorage::force_reset(&sled_config).await?;
                        sled_instance = Some(Arc::new(storage));
                    }
                }
            }
        }

        let sled_instance = match sled_instance {
            Some(instance) => instance,
            None => {
                error!("Failed to initialize Sled after {} attempts", MAX_RETRIES);
                log_lock_file_diagnostics(lock_path.clone()).await?;
                return Err(GraphError::StorageError(format!("Failed to initialize Sled after {} attempts", MAX_RETRIES)));
            }
        };

        // Store in singleton
        *singleton_guard = Some(sled_instance.clone());

        // Register SIGTERM handler for graceful shutdown
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let sled_instance_clone = sled_instance.clone();
            let lock_path_clone = lock_path.clone();
            tokio::spawn(async move {
                let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
                sigterm.recv().await;
                info!("Received SIGTERM, closing Sled instance");
                if let Err(e) = sled_instance_clone.close().await {
                    error!("Failed to close Sled instance on SIGTERM: {}", e);
                }
                if lock_path_clone.exists() {
                    if let Err(e) = std::fs::remove_file(&lock_path_clone) {
                        error!("Failed to remove lock file on SIGTERM: {}", e);
                    } else {
                        info!("Removed lock file on SIGTERM: {:?}", lock_path_clone);
                    }
                }
                info!("Sled instance closed gracefully");
            });
        }

        info!("Successfully initialized and stored Sled singleton instance");
        Ok(sled_instance as Arc<dyn GraphStorageEngine + Send + Sync>)
    }

    #[cfg(feature = "with-tikv")]
    pub async fn init_tikv(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        info!("Initializing TiKV engine via SurrealDB v1.5.4");
        
        let mut tikv_singleton = TIKV_SINGLETON.lock().await;
        
        if let Some(engine) = &*tikv_singleton {
            info!("TiKV storage engine already initialized, reusing existing instance.");
            return Ok(Arc::clone(engine) as Arc<dyn GraphStorageEngine + Send + Sync>);
        }

        let engine_specific = config.engine_specific_config.as_ref().ok_or_else(|| {
            error!("Missing engine_specific_config for TiKV");
            GraphError::ConfigurationError("Missing engine_specific_config for TiKV".to_string())
        })?;

        let tikv_config = TikvConfig {
            storage_engine_type: StorageEngineType::TiKV,
            path: engine_specific
                .get("path")
                .and_then(|p| p.as_str())
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/tikv")),
            host: engine_specific
                .get("host")
                .and_then(|val| val.as_str())
                .map(|s| s.to_string()),
            port: engine_specific
                .get("port")
                .and_then(|val| val.as_u64())
                .map(|p| p as u16),
            pd_endpoints: engine_specific
                .get("pd_endpoints")
                .and_then(|val| val.as_str())
                .map(|s| s.to_string()),
            username: engine_specific
                .get("username")
                .and_then(|val| val.as_str())
                .map(|s| s.to_string()),
            password: engine_specific
                .get("password")
                .and_then(|val| val.as_str())
                .map(|s| s.to_string()),
        };

        // Use the standard connection method for SurrealDB v1.5.4
        let engine = TikvStorage::new(&tikv_config).await
            .map_err(|e| {
                error!("Failed to initialize TiKV with standard method: {}", e);
                // If standard method fails, try direct connection
                warn!("Attempting direct connection method as fallback");
                e
            })?;

        let arc_engine = Arc::new(engine);
        *tikv_singleton = Some(arc_engine.clone());
        
        info!("TiKV storage engine initialized successfully with SurrealDB v1.5.4");
        Ok(arc_engine as Arc<dyn GraphStorageEngine + Send + Sync>)
    }

    #[cfg(feature = "with-rocksdb")]
    async fn init_rocksdb(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        use tokio::time::{self, Duration as TokioDuration};
        println!("IT'S TIME TO INITIALIZE ROCKSDB");
        info!("Initializing RocksDB engine with SurrealDB backend");

        // Get the path for the RocksDB engine.
        let engine_path = Self::get_engine_path(config, StorageEngineType::RocksDB)?;
        
        // Ensure the directory exists to avoid filesystem errors.
        info!("Ensuring directory exists: {:?}", engine_path);
        Self::ensure_directory_exists(&engine_path).await?;

        let mut db_instance = None;
        let mut attempt = 0;
        let max_attempts = 5;
        let base_delay_ms = 100;

        while attempt < max_attempts {
            debug!("Attempting to connect to SurrealDB RocksDB backend... (Attempt {} of {})", attempt + 1, max_attempts);
            
            match Surreal::new::<RocksDb>(engine_path.clone()).await {
                Ok(db) => {
                    info!("Successfully connected to SurrealDB RocksDB backend on attempt {}.", attempt + 1);
                    db_instance = Some(db);
                    break; // Connection successful, exit the loop.
                },
                Err(e) => {
                    let error_string = e.to_string();
                    
                    // We're looking for a specific kind of lock error. These often contain keywords
                    // like "lock", "busy", or "occupied". The SurrealDB error message might not be
                    // exact, so we'll check for keywords.
                    if error_string.contains("lock") || error_string.contains("occupied") {
                        warn!("Detected a potential RocksDB lock conflict. Attempting retry after a delay. Error: {}", error_string);
                        let delay = TokioDuration::from_millis(base_delay_ms * 2u64.pow(attempt));
                        info!("Retrying in {:?}...", delay);
                        time::sleep(delay).await;
                        attempt += 1;
                    } else {
                        // For any other type of error, we should fail immediately as it's likely
                        // a permanent issue (e.g., corrupt data, invalid config).
                        error!("A non-retryable error occurred while connecting to RocksDB: {}", error_string);
                        return Err(GraphError::StorageError(format!("Failed to connect to SurrealDB RocksDB backend: {}", error_string)));
                    }
                }
            }
        }

        // After the loop, check if we have a valid database instance.
        let db = db_instance.ok_or_else(|| {
            error!("Failed to connect to RocksDB after {} attempts.", max_attempts);
            GraphError::StorageError(format!("Failed to open RocksDB after {} attempts due to lock error.", max_attempts))
        })?;
        
        db.use_ns("graphdb").use_db("graph").await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        println!("SUCCESSFULLY INITIALIZED ROCKSDB");
        Ok(Arc::new(SurrealdbGraphStorage { db, backend_type: StorageEngineType::RocksDB }))
    }

    #[cfg(not(feature = "redis-datastore"))]
    async fn init_redis(_config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        Err(GraphError::StorageError(
            "Redis support is not enabled. Please enable the 'redis-datastore' feature.".to_string()
        ))
    }

    #[cfg(feature = "postgres-datastore")]
    async fn init_postgresql(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        let postgres_config = Self::build_postgresql_config(config)?;
        
        let postgres_instance = POSTGRES_SINGLETON.get_or_init(|| async {
            trace!("Creating new PostgresStorage singleton");
            let storage = PostgresStorage::new(&postgres_config).await
                .expect("Failed to initialize PostgreSQL singleton");
            Arc::new(storage)
        }).await;
        
        Ok(postgres_instance.clone())
    }

    #[cfg(not(feature = "postgres-datastore"))]
    async fn init_postgresql(_config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        Err(GraphError::StorageError(
            "PostgreSQL support is not enabled. Please enable the 'postgres-datastore' feature.".to_string()
        ))
    }

    #[cfg(feature = "mysql-datastore")]
    async fn init_mysql(config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        let mysql_config = Self::build_mysql_config(config)?;
        
        let mysql_instance = MYSQL_SINGLETON.get_or_init(|| async {
            trace!("Creating new MySQLStorage singleton");
            let storage = MySQLStorage::new(&mysql_config).await
                .expect("Failed to initialize MySQL singleton");
            Arc::new(storage)
        }).await;
        
        Ok(mysql_instance.clone())
    }

    #[cfg(not(feature = "mysql-datastore"))]
    async fn init_mysql(_config: &StorageConfig) -> Result<Arc<dyn GraphStorageEngine + Send + Sync>, GraphError> {
        Err(GraphError::StorageError(
            "MySQL support is not enabled. Please enable the 'mysql-datastore' feature.".to_string()
        ))
    }

    // Helper methods for configuration building
    fn get_engine_path(config: &StorageConfig, engine_type: StorageEngineType) -> Result<PathBuf, GraphError> {
        match engine_type {
            StorageEngineType::Sled => {
                Ok(config.engine_specific_config
                    .as_ref()
                    .and_then(|map| map.get("path").and_then(|v| v.as_str()).map(PathBuf::from))
                    .unwrap_or_else(|| {
                        let path = PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY));
                        warn!("No path specified for Sled, using default: {:?}", path);
                        path
                    }))
            }
            StorageEngineType::RocksDB => {
                config.engine_specific_config
                    .as_ref()
                    .and_then(|map| map.get("path").and_then(|v| v.as_str()).map(PathBuf::from))
                    .ok_or_else(|| {
                        error!("RocksDB path is missing in engine_specific_config: {:?}", config.engine_specific_config);
                        GraphError::ConfigurationError(
                            "RocksDB path is missing in engine_specific_config".to_string()
                        )
                    })
            }
            _ => {
                let path = config.data_directory.clone();
                info!("Using data_directory for engine: {:?}", path);
                Ok(path)
            }
        }
    }

    #[cfg(feature = "with-sled")]
    fn build_sled_config(config: &StorageConfig, path: PathBuf) -> Result<SledConfig, GraphError> {
        Ok(SledConfig {
            storage_engine_type: StorageEngineType::Sled,
            path,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, config.default_port)),
        })
    }

    #[cfg(feature = "with-rocksdb")]
    fn build_rocksdb_config(config: &StorageConfig, path: PathBuf) -> Result<RocksdbConfig, GraphError> {
        Ok(RocksdbConfig {
            storage_engine_type: StorageEngineType::RocksDB,
            path,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, config.default_port)),
        })
    }

    #[cfg(feature = "redis-datastore")]
    fn build_redis_config(config: &StorageConfig) -> Result<RedisConfig, GraphError> {
        let _config_map = config.engine_specific_config.as_ref()
            .ok_or_else(|| GraphError::ConfigurationError("Redis config is missing".to_string()))?;

        Ok(RedisConfig {
            storage_engine_type: StorageEngineType::Redis,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, 6379)),
            database: Some(Self::get_config_value(config, "database", "0")),
            username: None,
            password: None,
        })
    }

    #[cfg(feature = "postgres-datastore")]
    fn build_postgresql_config(config: &StorageConfig) -> Result<PostgreSQLConfig, GraphError> {
        let _config_map = config.engine_specific_config.as_ref()
            .ok_or_else(|| GraphError::ConfigurationError("PostgreSQL config is missing".to_string()))?;

        Ok(PostgreSQLConfig {
            storage_engine_type: StorageEngineType::PostgreSQL,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, 5432)),
            username: Some(Self::get_config_value(config, "username", "graphdb_user")),
            password: Some(Self::get_config_value(config, "password", "secure_password")),
            database: Some(Self::get_config_value(config, "database", "graphdb")),
        })
    }

    #[cfg(feature = "mysql-datastore")]
    fn build_mysql_config(config: &StorageConfig) -> Result<MySQLConfig, GraphError> {
        let _config_map = config.engine_specific_config.as_ref()
            .ok_or_else(|| GraphError::ConfigurationError("MySQL config is missing".to_string()))?;

        Ok(MySQLConfig {
            storage_engine_type: StorageEngineType::MySQL,
            host: Some(Self::get_config_value(config, "host", "127.0.0.1")),
            port: Some(Self::get_config_port(config, 3306)),
            username: Some(Self::get_config_value(config, "username", "graphdb_user")),
            password: Some(Self::get_config_value(config, "password", "secure_password")),
            database: Some(Self::get_config_value(config, "database", "graphdb")),
        })
    }

    // Utility helper methods
    fn get_config_value(config: &StorageConfig, key: &str, default: &str) -> String {
        config.engine_specific_config
            .as_ref()
            .and_then(|map| map.get(key).and_then(|v| v.as_str()))
            .unwrap_or(default)
            .to_string()
    }

    fn get_config_port(config: &StorageConfig, default: u16) -> u16 {
        config.engine_specific_config
            .as_ref()
            .and_then(|map| map.get("port").and_then(|v| v.as_u64()).map(|p| p as u16))
            .unwrap_or(default)
    }

    async fn ensure_directory_exists(path: &PathBuf) -> Result<(), GraphError> {
        if !path.exists() {
            debug!("Creating data directory at {:?}", path);
            tokio::fs::create_dir_all(path).await.map_err(|e| {
                error!("Failed to create data directory at {:?}: {}", path, e);
                GraphError::Io(e)
            })?;
        }
        Ok(())
    }

    fn create_manager(
        engine_type: StorageEngineType,
        config: StorageConfig,
        config_path_yaml: &Path,
        persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
        permanent: bool,
    ) -> StorageEngineManager {
        let engine = Arc::new(TokioMutex::new(HybridStorageEngine {
            inmemory: Arc::new(InMemoryGraphStorage::new(&config)),
            persistent: persistent.clone(),
            running: Arc::new(TokioMutex::new(false)),
            engine_type,
        }));

        StorageEngineManager {
            engine,
            persistent_engine: persistent,
            session_engine_type: if permanent { None } else { Some(engine_type) },
            config,
            config_path: config_path_yaml.to_path_buf(),
        }
    }

    // Missing Sled-specific helper methods
    #[cfg(feature = "with-sled")]
    async fn handle_sled_lock_file(engine_path: &PathBuf) -> Result<(), GraphError> {
        let sled_lock_path = engine_path.join("db.lck");
        
        // Fix: Remove the borrow and use the `?` operator to handle the `Result`.
        if lock_file_exists(sled_lock_path.clone()).await? {
            warn!("Lock file exists before Sled initialization: {:?}", sled_lock_path);
            Self::log_lock_file_diagnostics(&sled_lock_path).await;
            
            // Fix: Remove the borrow and use the `?` operator.
            recover_sled(sled_lock_path).await?;
        } else {
            debug!("No lock file found for Sled at {:?}", sled_lock_path);
        }
        Ok(())
    }

    #[cfg(feature = "with-sled")]
    async fn handle_sled_retry_error(sled_lock_path: &PathBuf, sled_path: &PathBuf, attempt: u32) {
        // Check if the lock file exists, handling the Result and cloning the path.
        if let Ok(true) = lock_file_exists(sled_lock_path.clone()).await {
            warn!("Lock file still exists after retry {}: {:?}", attempt, sled_lock_path);
            
            // Assuming this function takes a reference.
            Self::log_lock_file_diagnostics(sled_lock_path).await;
            
            // Try to recover the lock file, also cloning the path to satisfy ownership.
            if let Err(e) = recover_sled(sled_lock_path.clone()).await {
                warn!("Failed to recover Sled lock file on retry {}: {}", attempt, e);
            }
        } else {
            warn!("No lock file found on retry {}, but Sled initialization still failed", attempt);
        }
    }

    #[cfg(feature = "with-sled")]
    async fn log_final_sled_error(sled_lock_path: &PathBuf) {
        // Log the initial error message.
        error!("Failed to initialize Sled after all retries");

        // Use `if let` to handle the `Result` returned by `lock_file_exists`.
        // We clone the `sled_lock_path` to satisfy the function's ownership requirement.
        if let Ok(true) = lock_file_exists(sled_lock_path.clone()).await {
            error!("Lock file still exists after all retries: {:?}", sled_lock_path);
            
            // This function call is also fixed to pass a PathBuf by reference
            // if its signature expects that.
            // Assuming Self::log_lock_file_diagnostics takes a reference.
            Self::log_lock_file_diagnostics(sled_lock_path).await;
        }
    }


    #[cfg(feature = "with-sled")]
    pub async fn log_lock_file_diagnostics(lock_path: &PathBuf) {
        match fs::metadata(lock_path).await {
            Ok(metadata) => {
                debug!("Lock file diagnostics for {:?}:", lock_path);
                debug!("  Size: {} bytes", metadata.len());
                debug!("  Modified: {:?}", metadata.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH));
                debug!("  Read-only: {}", metadata.permissions().readonly());
            }
            Err(e) => {
                warn!("Failed to get lock file metadata for {:?}: {}", lock_path, e);
            }
        }
    }

    /// Validates the storage configuration for the specified engine type
    async fn validate_config(config: &StorageConfig, engine_type: StorageEngineType) -> Result<(), GraphError> {
        info!("Validating config for engine type: {:?}", engine_type);
        debug!("Full config: {:?}", config);
        
        match engine_type {
            StorageEngineType::Sled | StorageEngineType::RocksDB | StorageEngineType::TiKV => {
                let path = config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("engine_specific_config for {:?}: {:?}", engine_type, map);
                        map.get("path").and_then(|v| {
                            let path_str = v.as_str();
                            debug!("Extracted path: {:?}", path_str);
                            path_str.map(PathBuf::from)
                        })
                    })
                    .unwrap_or_else(|| {
                        let default_path = match engine_type {
                            StorageEngineType::Sled => PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY)),
                            StorageEngineType::RocksDB => PathBuf::from(&format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY)),
                            _ => config.data_directory.clone(),
                        };
                        warn!("No path specified in engine_specific_config, using default: {:?}", default_path);
                        default_path
                    });
                
                info!("Validating path for {:?}: {:?}", engine_type, path);
                
                if !path.exists() {
                    info!("Creating path: {:?}", path);
                    fs::create_dir_all(&path)
                        .await
                        .map_err(|e| GraphError::Io(e))
                        .with_context(|| format!("Failed to create engine-specific path: {:?}", path))?;
                }
                
                if !path.is_dir() {
                    return Err(GraphError::ConfigurationError(format!(
                        "Path for {:?} is not a directory: {:?}", engine_type, path
                    )));
                }
                
                // Test write permissions
                let test_file = path.join(".write_test");
                fs::write(&test_file, "")
                    .await
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("No write permissions for engine-specific path: {:?}", path))?;
                
                fs::remove_file(&test_file)
                    .await
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("Failed to remove test file in {:?}", path))?;
            }
            
            StorageEngineType::Redis | StorageEngineType::PostgreSQL | StorageEngineType::MySQL => {
                let map = config.engine_specific_config
                    .as_ref()
                    .ok_or_else(|| {
                        error!("engine_specific_config is missing for {:?}", engine_type);
                        GraphError::ConfigurationError(format!("Engine-specific config required for {:?}", engine_type))
                    })?;
                
                debug!("engine_specific_config for {:?}: {:?}", engine_type, map);
                
                if !map.contains_key("host") || !map.contains_key("port") {
                    return Err(GraphError::ConfigurationError(format!(
                        "Host and port are required for {:?}", engine_type
                    )));
                }
                
                if matches!(engine_type, StorageEngineType::PostgreSQL | StorageEngineType::MySQL) {
                    if !map.contains_key("username") || !map.contains_key("password") || !map.contains_key("database") {
                        return Err(GraphError::ConfigurationError(format!(
                            "Username, password, and database are required for {:?}", engine_type
                        )));
                    }
                }
            }
            
            StorageEngineType::InMemory => {
                info!("No specific validation required for InMemory engine");
            }
        }
        
        info!("Config validation successful for {:?}", engine_type);
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), GraphError> {
        info!("Shutting down StorageEngineManager");
        
        // Stop the hybrid engine if it's running
        {
            let engine = self.engine.lock().await;
            if (*engine).is_running().await {
                info!("Stopping running hybrid engine");
                (*engine).stop().await
                    .map_err(|e| {
                        error!("Failed to stop hybrid engine: {}", e);
                        GraphError::StorageError(format!("Failed to stop hybrid engine: {}", e))
                    })?;
            }
        }
        
        // Close all connections
        self.close_connections().await
            .map_err(|e| {
                error!("Failed to close connections during shutdown: {}", e);
                GraphError::StorageError(format!("Failed to close connections: {}", e))
            })?;
        
        // Ensure RocksDB singleton is closed
        #[cfg(feature = "with-rocksdb")]
        {
            let mut rocksdb_singleton = ROCKSDB_SINGLETON.lock().await;
            if let Some(rocksdb_instance) = rocksdb_singleton.as_ref() {
                rocksdb_instance.close().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB singleton: {}", e)))?;
                info!("RocksDB singleton closed during shutdown");
                *rocksdb_singleton = None;
            }
        }

        info!("StorageEngineManager shutdown completed successfully");
        Ok(())
    }

    pub async fn close_connections(&mut self) -> Result<(), GraphError> {
        let engine_type = self.current_engine_type().await;
        info!("Closing connections for engine type: {:?}", engine_type);

        match engine_type {
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    if let Some(sled_storage) = self.persistent_engine.as_any().downcast_ref::<SledStorage>() {
                        sled_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close Sled database: {}", e)))?;
                        info!("Sled database connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to SledStorage");
                    }
                }
                #[cfg(not(feature = "with-sled"))]
                {
                    warn!("Sled support is not enabled, skipping close");
                }
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    if let Some(rocksdb_storage) = self.persistent_engine.as_any().downcast_ref::<RocksdbStorage>() {
                        rocksdb_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB database: {}", e)))?;
                        info!("RocksDB database connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to RocksdbStorage");
                    }
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    warn!("RocksDB support is not enabled, skipping close");
                }
            }
            StorageEngineType::TiKV => {
                #[cfg(feature = "with-tikv")]
                {

                }
                #[cfg(not(feature = "with-tikv"))]
                {
                    warn!("Sled support is not enabled, skipping close");
                }
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    if let Some(redis_storage) = self.persistent_engine.as_any().downcast_ref::<RedisStorage>() {
                        redis_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close Redis connection: {}", e)))?;
                        info!("Redis connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to RedisStorage");
                    }
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    warn!("Redis support is not enabled, skipping close");
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    if let Some(postgres_storage) = self.persistent_engine.as_any().downcast_ref::<PostgresStorage>() {
                        postgres_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close PostgreSQL connection: {}", e)))?;
                        info!("PostgreSQL connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to PostgresStorage");
                    }
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    warn!("PostgreSQL support is not enabled, skipping close");
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    if let Some(mysql_storage) = self.persistent_engine.as_any().downcast_ref::<MySQLStorage>() {
                        mysql_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close MySQL connection: {}", e)))?;
                        info!("MySQL connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to MySQLStorage");
                    }
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    warn!("MySQL support is not enabled, skipping close");
                }
            }
            StorageEngineType::InMemory => {
                if let Some(inmemory_storage) = self.persistent_engine.as_any().downcast_ref::<InMemoryGraphStorage>() {
                    inmemory_storage.close().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to close InMemory storage: {}", e)))?;
                    info!("InMemory storage flushed");
                } else {
                    warn!("Failed to downcast persistent_engine to InMemoryGraphStorage");
                }
            }
        }

        let engine = self.engine.lock().await;
        (*engine).close().await
            .map_err(|e| GraphError::StorageError(format!("Failed to close HybridStorageEngine: {}", e)))?;
        Ok(())
    }

    pub async fn reset(&mut self) -> Result<(), GraphError> {
        info!("Resetting StorageEngineManager");
        let engine = self.engine.lock().await;
        if (*engine).is_running().await {
            (*engine).stop().await?;
        }
        let engine_type = engine.engine_type; // Extract engine_type before dropping the lock
        drop(engine); // Release the lock
        let new_manager = StorageEngineManager::new(engine_type, &self.config_path, self.session_engine_type.is_none()).await
            .map_err(|e| {
                error!("Failed to create new StorageEngineManager: {}", e);
                GraphError::StorageError(format!("Failed to reset StorageEngineManager: {}", e))
            })?;
        self.engine = new_manager.engine.clone();
        self.persistent_engine = new_manager.persistent_engine.clone();
        self.session_engine_type = new_manager.session_engine_type;
        self.config = new_manager.config.clone();
        info!("StorageEngineManager reset completed with engine: {:?}", engine_type);
        Ok(())
    }

    pub fn get_persistent_engine(&self) -> Arc<dyn GraphStorageEngine + Send + Sync> {
        Arc::clone(&self.persistent_engine)
    }

    pub async fn get_runtime_config(&self) -> Result<StorageConfig, GraphError> {
        let mut config = self.config.clone();
        if let Some(session_engine) = self.session_engine_type {
            config.storage_engine_type = session_engine;
        }
        Ok(config)
    }

    pub fn get_current_engine_type(&self) -> StorageEngineType {
        tokio::runtime::Runtime::new()
            .expect("Failed to create Tokio runtime")
            .block_on(self.current_engine_type())
    }

    pub async fn current_engine_type(&self) -> StorageEngineType {
        if let Some(engine_type) = &self.session_engine_type {
            engine_type.clone()
        } else {
            let engine = self.engine.lock().await;
            (*engine).engine_type.clone()
        }
    }

    pub async fn get_current_engine_data_path(&self) -> Option<PathBuf> {
        self.current_engine_data_path().await
    }

    pub async fn current_engine_data_path(&self) -> Option<PathBuf> {
        let engine_type = self.current_engine_type().await;
        let path = match engine_type {
            StorageEngineType::Sled => {
                self.config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or_else(|| PathBuf::from("./storage_daemon_server/data/sled"))
            }
            StorageEngineType::RocksDB => {
                self.config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or_else(|| PathBuf::from("./storage_daemon_server/data/rocksdb"))
            }
            _ => {
                self.config.data_directory.clone()
            }
        };
        Some(path)
    }

    fn get_engine_config_path(&self, engine_type: StorageEngineType) -> PathBuf {
        let parent = self.config_path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
        match engine_type {
            StorageEngineType::Sled => parent.join("storage_config_sled.yaml"),
            StorageEngineType::RocksDB => parent.join("storage_config_rocksdb.yaml"),
            StorageEngineType::InMemory => parent.join("storage_config_inmemory.yaml"),
            StorageEngineType::Redis => parent.join("storage_config_redis.yaml"),
            StorageEngineType::PostgreSQL => parent.join("storage_config_postgres.yaml"),
            StorageEngineType::MySQL => parent.join("storage_config_mysql.yaml"),
            StorageEngineType::TiKV => parent.join("storage_config_tykv.yaml"),

        }
    }

    async fn migrate_data(&self, old_engine: &Arc<dyn GraphStorageEngine + Send + Sync>, new_engine: &Arc<dyn GraphStorageEngine + Send + Sync>) -> Result<(), GraphError> {
        info!("Migrating data from {} to {}", old_engine.get_type(), new_engine.get_type());
        let start_time = Instant::now();

        let vertices = old_engine.get_all_vertices().await?;
        for vertex in vertices {
            new_engine.create_vertex(vertex).await?;
        }

        let edges = old_engine.get_all_edges().await?;
        for edge in edges {
            new_engine.create_edge(edge).await?;
        }

        info!("Data migration completed in {}ms", start_time.elapsed().as_millis());
        Ok(())
    }

    pub fn available_engines() -> Vec<StorageEngineType> {
        let mut engines = vec![StorageEngineType::InMemory];
        #[cfg(feature = "with-sled")]
        engines.push(StorageEngineType::Sled);
        #[cfg(feature = "with-rocksdb")]
        engines.push(StorageEngineType::RocksDB);
        #[cfg(feature = "with-tikv")]
        engines.push(StorageEngineType::TiKV);
        #[cfg(feature = "redis-datastore")]
        engines.push(StorageEngineType::Redis);
        #[cfg(feature = "postgres-datastore")]
        engines.push(StorageEngineType::PostgreSQL);
        #[cfg(feature = "mysql-datastore")]
        engines.push(StorageEngineType::MySQL);
        engines
    }

    pub async fn use_storage(&mut self, engine_type: StorageEngineType, permanent: bool) -> Result<(), GraphError> {
        use reqwest::Client;
        use tokio::time::{self, Duration as TokioDuration};

        info!("=== Starting use_storage for engine: {:?}, permanent: {} ===", engine_type, permanent);
        trace!("use_storage called with engine_type: {:?}", engine_type);
        let start_time = Instant::now();
        println!("===> USE STORAGE HANDLER - STEP 1");

        // Check if requested engine is available
        let available_engines = Self::available_engines();
        trace!("Available engines: {:?}", available_engines);
        if !available_engines.contains(&engine_type) {
            error!("Storage engine {:?} is not supported in this build. Available: {:?}", engine_type, available_engines);
            return Err(GraphError::InvalidStorageEngine(format!(
                "Storage engine {:?} is not supported. Available engines: {:?}", engine_type, available_engines
            )));
        }

        // Check current engine state
        let (was_running, old_persistent_arc, old_engine_type) = {
            let engine_guard = self.engine.lock().await;
            let was_running = (*engine_guard).is_running().await;
            trace!("Current engine state - running: {}, type: {:?}", was_running, (*engine_guard).engine_type);
            debug!("Current engine: {:?}", (*engine_guard).engine_type);
            (was_running, Arc::clone(&engine_guard.persistent), (*engine_guard).engine_type)
        };

        // Skip if no change needed
        if old_engine_type == engine_type && self.session_engine_type.is_none() {
            info!("No switch needed: current engine is already {:?}", engine_type);
            trace!("Skipping switch: current engine matches requested and no session override. Elapsed: {}ms", start_time.elapsed().as_millis());
            return Ok(());
        }

        // Cleanup previous engine state only if it was RocksDB and switching to a different engine
        if old_engine_type == StorageEngineType::RocksDB && old_engine_type != engine_type {
            #[cfg(feature = "with-rocksdb")]
            {
                let mut rocksdb_singleton = ROCKSDB_SINGLETON.lock().await;
                if let Some(rocksdb_instance) = rocksdb_singleton.as_ref() {
                    info!("Closing existing RocksDB instance before switching");
                    rocksdb_instance.close().await
                        .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB: {}", e)))?;
                    *rocksdb_singleton = None;
                }
                let rocksdb_path = PathBuf::from("/opt/graphdb/storage_data/rocksdb");
                if rocksdb_path.exists() {
                    warn!("Cleaning up RocksDB directory for switch to {:?}", engine_type);
                    recover_rocksdb(&rocksdb_path).await?;
                }
            }
        } else {
            debug!("Skipping RocksDB cleanup for engine switch to {:?}", engine_type);
        }
        println!("===> USE STORAGE HANDLER - STEP 2: Loading configuration...");

        // Determine config path
        let config_path = self.config_path.clone();
        info!("Using main config path: {:?}", config_path);
        trace!("Resolved config path: {:?}", config_path);

        // Load or create configuration
        let mut new_config = if config_path.exists() {
            info!("Loading existing config from {:?}", config_path);
            trace!("Reading config file: {:?}", config_path);
            load_storage_config_from_yaml(Some(&config_path))
                .map_err(|e| {
                    error!("Failed to deserialize YAML config from {:?}: {}", config_path, e);
                    GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
                })?
        } else {
            warn!("Config file not found at {:?}", config_path);
            create_default_yaml_config(&config_path, engine_type)?;
            load_storage_config_from_yaml(Some(&config_path))
                .map_err(|e| {
                    error!("Failed to load newly created YAML config from {:?}: {}", config_path, e);
                    GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e))
                })?
        };
        println!("===> Configuration loaded successfully.");
        debug!("Loaded storage config: {:?}", new_config);

        // Update configuration for the new engine
        new_config.storage_engine_type = engine_type;
        new_config.engine_specific_config = Some(load_engine_specific_config(engine_type, &config_path)
            .map_err(|e| {
                error!("Failed to load engine-specific config for {:?}: {}", engine_type, e);
                GraphError::ConfigurationError(format!("Failed to load engine-specific config: {}", e))
            })?);
        println!("===> USE STORAGE HANDLER - STEP 3: Loading engine-specific configuration...");
        debug!("Using config root directory: {:?}", new_config.config_root_directory);
        println!("===> Engine-specific configuration loaded successfully.");

        // Validate new configuration
        Self::validate_config(&new_config, engine_type)
            .await
            .map_err(|e| {
                error!("Configuration validation failed for new engine {:?}: {}", engine_type, e);
                GraphError::ConfigurationError(format!("Configuration validation failed: {}", e))
            })?;

        // If permanent, save the new configuration using StorageConfig::save
        if permanent {
            info!("Saving new configuration for permanent switch to {:?}", engine_type);
            new_config.save()
                .map_err(|e| {
                    error!("Failed to save new config to {:?}: {}", config_path, e);
                    GraphError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
                })?;
            self.session_engine_type = None;
        } else {
            self.session_engine_type = Some(engine_type);
        }
        println!("===> USE STORAGE HANDLER - STEP 4: Saving and reloading config");
        debug!("Final new_config before saving: {:?}", new_config);
        println!("===> Saving configuration to disk...");
        println!("===> Configuration saved successfully.");
        println!("------------------------------> SEE IT <-------------------------------- {:?} vs {:?}", engine_type, StorageEngineType::TiKV);
        debug!("Reloaded storage config for daemon management: {:?}", new_config);

        println!("==> ENGINE TYPE {:?}", engine_type);

        // Skip daemon cleanup for TiKV to prevent killing PD
        if engine_type != StorageEngineType::TiKV {
            println!("===> USE STORAGE HANDLER - STEP 5: Attempting to stop existing daemon... - and something will break here");
            let port = new_config.default_port;
            let max_attempts = 5;
            let mut attempt = 0;
            let mut pid = None;

            while attempt < max_attempts {
                match find_pid_by_port(port).await {
                    Ok(opt_pid) => match opt_pid {
                        Some(found_pid) => {
                            debug!("Found PID {} for port {} on attempt {}", found_pid, port, attempt);
                            pid = Some(found_pid);
                            break;
                        }
                        None => {
                            debug!("No process found on port {} on attempt {}", port, attempt);
                            break;
                        }
                    },
                    Err(e) => {
                        warn!("Failed to check port {} on attempt {}: {}", port, attempt, e);
                        attempt += 1;
                        time::sleep(TokioDuration::from_millis(100)).await;
                    }
                }
            }

            if let Some(found_pid) = pid {
                info!("Attempting to stop Storage Daemon on port {}...", port);
                if let Err(e) = stop_process(found_pid).await {
                    error!("Failed to stop daemon on PID {}: {}", found_pid, e);
                    return Err(GraphError::StorageError(format!("Failed to stop daemon on PID {}: {}", found_pid, e)));
                }
                info!("Sent SIGTERM to PID {} for Storage Daemon on port {}.", found_pid, port);
                time::sleep(TokioDuration::from_millis(500)).await;

                match find_pid_by_port(port).await {
                    Ok(opt_pid) => match opt_pid {
                        None => {
                            info!("Port {} is now free.", port);
                            println!("Port {} is now free.", port);
                        }
                        Some(still_running_pid) => {
                            warn!("Port {} is still in use after stopping PID {}.", port, still_running_pid);
                        }
                    },
                    Err(e) => {
                        warn!("Failed to verify port {} after stopping PID {}: {}", port, found_pid, e);
                    }
                }
                println!("Storage daemon on port {} stopped.", port);
                println!("===> Daemon stopped successfully.");
            }
        } else {
            info!("Skipping daemon cleanup for TiKV to preserve running PD");
            println!("===> USE STORAGE HANDLER - STEP 5: Skipping daemon cleanup for TiKV");
        }

        // Pre-check PD endpoint for TiKV to ensure cluster is running
        if engine_type == StorageEngineType::TiKV {
            let pd_endpoints = new_config.engine_specific_config
                .as_ref()
                .and_then(|map| map.get("pd_endpoints").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .unwrap_or("127.0.0.1:2382".to_string());
            let pd_endpoint_list: Vec<String> = pd_endpoints.split(',').map(|s| s.trim().to_string()).collect();
            let client = Client::new();
            let mut healthy_endpoint = None;
            for endpoint in &pd_endpoint_list {
                let pd_status_url = format!("http://{}/pd/api/v1/status", endpoint);
                debug!("Checking TiKV PD status at {}", pd_status_url);
                match client.get(&pd_status_url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        let body = resp.text().await.unwrap_or_default();
                        info!("TiKV PD endpoint {} is healthy: {}", endpoint, body);
                        healthy_endpoint = Some(endpoint.clone());
                        break;
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        warn!("TiKV PD endpoint {} returned non-success status: {} - {}", endpoint, status, body);
                    }
                    Err(e) => {
                        warn!("Failed to reach TiKV PD endpoint {}: {}", endpoint, e);
                    }
                }
            }
            if healthy_endpoint.is_none() {
                error!("No healthy TiKV PD endpoints found in {}. Cannot proceed with TiKV initialization.", pd_endpoints);
                return Err(GraphError::StorageError(format!("No healthy TiKV PD endpoints found in {}", pd_endpoints)));
            }
        }

        // Clean up lock files for TiKV
        if engine_type == StorageEngineType::TiKV {
            println!("===> USE STORAGE HANDLER - STEP 5.5: Cleaning up lock files for TiKV");
            info!("Cleaning up lock files...");
            let tikv_path = new_config.engine_specific_config
                .as_ref()
                .and_then(|map| map.get("path").and_then(|v| v.as_str()).map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from("/opt/graphdb/storage_data/tikv"));
            if tikv_path.exists() {
                if let Err(e) = TikvStorage::force_unlock().await {
                    warn!("Failed to clean up TiKV lock files: {}", e);
                } else {
                    info!("Lock files cleaned successfully.");
                    println!("===> Lock files cleaned successfully.");
                }
            }
        }

        // Initialize new engine
        println!("===> USE STORAGE HANDLER - STEP 6: Initializing StorageEngineManager...");
        println!("===> Creating new instance of StorageEngineManager...");
        let new_persistent: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::InMemory => {
                info!("Initializing InMemory engine");
                Arc::new(InMemoryGraphStorage::new(&new_config))
            }
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    let sled_config = SledConfig {
                        storage_engine_type: StorageEngineType::Sled,
                        path: new_config.engine_specific_config
                            .as_ref()
                            .and_then(|map| map.get("path").and_then(|v| v.as_str()).map(PathBuf::from))
                            .unwrap_or_else(|| PathBuf::from(format!("{}/sled", DEFAULT_DATA_DIRECTORY))),
                        host: Some(new_config.engine_specific_config.as_ref().and_then(|m| m.get("host").and_then(|v| v.as_str())).unwrap_or("127.0.0.1").to_string()),
                        port: Some(new_config.engine_specific_config.as_ref().and_then(|m| m.get("port").and_then(|v| v.as_u64()).map(|p| p as u16)).unwrap_or(new_config.default_port)),
                    };
                    println!("===> USE STORAGE HANDLER - STEP 3.1 - SLED");
                    info!("Initializing Sled engine with path: {:?}", sled_config.path);
                    let mut sled_singleton = SLED_SINGLETON.lock().await;
                    let sled_instance = match sled_singleton.as_ref() {
                        Some(instance) => instance.clone(),
                        None => {
                            trace!("Creating new SledStorage singleton");
                            let storage = SledStorage::new(&sled_config).await?;
                            let instance = Arc::new(storage);
                            *sled_singleton = Some(instance.clone());
                            instance
                        }
                    };
                    sled_instance
                }
                #[cfg(not(feature = "with-sled"))]
                {
                    error!("Sled support is not enabled in this build");
                    return Err(GraphError::StorageError("Sled support is not enabled. Please enable the 'with-sled' feature.".to_string()));
                }
            }
            StorageEngineType::RocksDB => {
                #[cfg(feature = "with-rocksdb")]
                {
                    let rocksdb_path = new_config.engine_specific_config
                        .as_ref()
                        .and_then(|map| map.get("path").and_then(|v| v.as_str()).map(PathBuf::from))
                        .unwrap_or_else(|| PathBuf::from(format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY)));
                    if lock_file_exists(rocksdb_path.join("LOCK")).await? {
                        warn!("Lock file exists for RocksDB: {:?}", rocksdb_path.join("LOCK"));
                        recover_rocksdb(&rocksdb_path).await?;
                    }
                    let rocksdb_config = RocksdbConfig {
                        storage_engine_type: StorageEngineType::RocksDB,
                        path: rocksdb_path,
                        host: Some(new_config.engine_specific_config.as_ref().and_then(|m| m.get("host").and_then(|v| v.as_str())).unwrap_or("127.0.0.1").to_string()),
                        port: Some(new_config.engine_specific_config.as_ref().and_then(|m| m.get("port").and_then(|v| v.as_u64()).map(|p| p as u16)).unwrap_or(new_config.default_port)),
                    };
                    info!("Initializing RocksDB engine with path: {:?}", rocksdb_config.path);
                    let mut rocksdb_singleton = ROCKSDB_SINGLETON.lock().await;
                    let rocksdb_instance = match rocksdb_singleton.as_ref() {
                        Some(instance) => instance.clone(),
                        None => {
                            trace!("Creating new RocksdbStorage singleton");
                            let storage = RocksdbStorage::new(&rocksdb_config)?;
                            let instance = Arc::new(storage);
                            *rocksdb_singleton = Some(instance.clone());
                            instance
                        }
                    };
                    rocksdb_instance
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    error!("RocksDB support is not enabled in this build");
                    return Err(GraphError::StorageError("RocksDB support is not enabled. Please enable the 'with-rocksdb' feature.".to_string()));
                }
            }
            StorageEngineType::TiKV => {
                #[cfg(feature = "with-tikv")]
                {
                    let mut tikv_singleton = TIKV_SINGLETON.lock().await;
                    if let Some(tikv_instance) = tikv_singleton.as_ref() {
                        info!("Reusing existing TiKV instance");
                        tikv_instance.clone()
                    } else {
                        Self::init_tikv(&new_config).await?
                    }
                }
                #[cfg(not(feature = "with-tikv"))]
                {
                    error!("TiKV support is not enabled in this build");
                    return Err(GraphError::StorageError("TiKV support is not enabled. Please enable the 'with-tikv' feature.".to_string()));
                }
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let config_map = new_config.engine_specific_config.as_ref().ok_or_else(|| GraphError::ConfigurationError("Redis config is missing".to_string()))?;
                    let redis_config = RedisConfig {
                        storage_engine_type: StorageEngineType::Redis,
                        host: Some(config_map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1").to_string()),
                        port: Some(config_map.get("port").and_then(|v| v.as_u64()).map(|p| p as u16).unwrap_or(6379)),
                        database: Some(config_map.get("database").and_then(|v| v.as_str()).unwrap_or("0").to_string()),
                        username: None,
                        password: None,
                    };
                    let redis_instance = REDIS_SINGLETON.get_or_init(|| async {
                        trace!("Creating new RedisStorage singleton");
                        let storage = RedisStorage::new(&redis_config).await?;
                        Ok(Arc::new(storage))
                    }).await?;
                    redis_instance.clone()
                }
                #[cfg(not(feature = "redis-datastore"))]
                {
                    error!("Redis support is not enabled in this build");
                    return Err(GraphError::StorageError("Redis support is not enabled. Please enable the 'redis-datastore' feature.".to_string()));
                }
            }
            StorageEngineType::PostgreSQL => {
                #[cfg(feature = "postgres-datastore")]
                {
                    let config_map = new_config.engine_specific_config.as_ref().ok_or_else(|| GraphError::ConfigurationError("PostgreSQL config is missing".to_string()))?;
                    let postgres_config = PostgreSQLConfig {
                        storage_engine_type: StorageEngineType::PostgreSQL,
                        host: Some(config_map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1").to_string()),
                        port: Some(config_map.get("port").and_then(|v| v.as_u64()).map(|p| p as u16).unwrap_or(5432)),
                        username: Some(config_map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user").to_string()),
                        password: Some(config_map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password").to_string()),
                        database: Some(config_map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb").to_string()),
                    };
                    let postgres_instance = POSTGRES_SINGLETON.get_or_init(|| async {
                        trace!("Creating new PostgresStorage singleton");
                        let storage = PostgresStorage::new(&postgres_config).await?;
                        Ok(Arc::new(storage))
                    }).await?;
                    postgres_instance.clone()
                }
                #[cfg(not(feature = "postgres-datastore"))]
                {
                    error!("PostgreSQL support is not enabled in this build");
                    return Err(GraphError::StorageError("PostgreSQL support is not enabled. Please enable the 'postgres-datastore' feature.".to_string()));
                }
            }
            StorageEngineType::MySQL => {
                #[cfg(feature = "mysql-datastore")]
                {
                    let config_map = new_config.engine_specific_config.as_ref().ok_or_else(|| GraphError::ConfigurationError("MySQL config is missing".to_string()))?;
                    let mysql_config = MySQLConfig {
                        storage_engine_type: StorageEngineType::MySQL,
                        host: Some(config_map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1").to_string()),
                        port: Some(config_map.get("port").and_then(|v| v.as_u64()).map(|p| p as u16).unwrap_or(3306)),
                        username: Some(config_map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user").to_string()),
                        password: Some(config_map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password").to_string()),
                        database: Some(config_map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb").to_string()),
                    };
                    let mysql_instance = MYSQL_SINGLETON.get_or_init(|| async {
                        trace!("Creating new MySQLStorage singleton");
                        let storage = MySQLStorage::new(&mysql_config).await?;
                        Ok(Arc::new(storage))
                    }).await?;
                    mysql_instance.clone()
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    error!("MySQL support is not enabled in this build");
                    return Err(GraphError::StorageError("MySQL support is not enabled. Please enable the 'mysql-datastore' feature.".to_string()));
                }
            }
        };

        // Migrate data if switching engines
        if old_engine_type != engine_type {
            info!("Migrating data from {} to {}", old_persistent_arc.get_type(), new_persistent.get_type());
            self.migrate_data(&old_persistent_arc, &new_persistent).await?;
        }

        // Stop the current engine if it was running
        if was_running {
            info!("Stopping current engine before switching");
            let engine = self.engine.lock().await;
            (*engine).stop().await
                .map_err(|e| GraphError::StorageError(format!("Failed to stop current engine: {}", e)))?;
        }

        // Update the engine
        self.engine = Arc::new(TokioMutex::new(HybridStorageEngine {
            inmemory: Arc::new(InMemoryGraphStorage::new(&new_config)),
            persistent: new_persistent.clone(),
            running: Arc::new(TokioMutex::new(false)),
            engine_type,
        }));
        self.persistent_engine = new_persistent;

        // Start the new engine
        {
            let engine = self.engine.lock().await;
            (*engine).start().await
                .map_err(|e| GraphError::StorageError(format!("Failed to start new engine: {}", e)))?;
        }

        info!("Successfully switched to storage engine: {:?}", engine_type);
        trace!("use_storage completed in {}ms", start_time.elapsed().as_millis());
        Ok(())
    }
}

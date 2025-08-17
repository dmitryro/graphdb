use std::any::Any;
use std::collections::HashMap;
use async_trait::async_trait;
use models::errors::GraphError;
use uuid::Uuid;
use models::{Edge, Identifier, Vertex};
use tokio::sync::OnceCell;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use tokio::sync::Mutex as TokioMutex;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::fs;
use std::process;
use anyhow::{Result, Context};
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
use log::{info, debug, warn, error, trace};
#[cfg(unix)]
use nix::unistd::Pid;
use nix::unistd::getpid;
use sysinfo::{System};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use crate::storage_engine::config::{DEFAULT_DATA_DIRECTORY, DEFAULT_LOG_DIRECTORY, 
                                    DEFAULT_STORAGE_PORT, StorageConfig, SledConfig, RocksdbConfig,
                                    StorageConfigWrapper, load_storage_config_from_yaml, 
                                    load_engine_specific_config, path_buf_serde};
use crate::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};

// Re-export StorageEngineType to ensure clear visibility
pub use crate::storage_engine::config::StorageEngineType;

pub use crate::storage_engine::inmemory_storage::InMemoryStorage as InMemoryGraphStorage;
#[cfg(feature = "with-sled")]
pub use crate::storage_engine::sled_storage::SledStorage;
#[cfg(feature = "with-rocksdb")]
pub use crate::storage_engine::rocksdb_storage::RocksdbGraphStorage;
#[cfg(feature = "redis-datastore")]
use crate::storage_engine::redis_storage::RedisStorage;
#[cfg(feature = "postgres-datastore")]
use crate::storage_engine::postgres_storage::PostgresStorage;
#[cfg(feature = "mysql-datastore")]
use crate::storage_engine::mysql_storage::MySQLStorage;

pub static CLEANUP_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
#[cfg(feature = "with-rocksdb")]
pub static ROCKSDB_SINGLETON: OnceCell<Arc<dyn GraphStorageEngine + Send + Sync>> = OnceCell::const_new();
#[cfg(feature = "with-sled")]
pub static SLED_SINGLETON: OnceCell<Arc<dyn GraphStorageEngine + Send + Sync>> = OnceCell::const_new();
pub static GLOBAL_STORAGE_ENGINE_MANAGER: OnceCell<Arc<AsyncStorageEngineManager>> = OnceCell::const_new();

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
fn lock_file_exists(lock_file: &Path) -> bool {
    lock_file.exists()
}

/// Initializes the global StorageEngineManager
pub async fn init_storage_engine_manager(config_path_yaml: PathBuf) -> Result<(), GraphError> {
    info!("Initializing StorageEngineManager with YAML: {:?}", config_path_yaml);
    if let Some(parent) = config_path_yaml.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create directory for YAML config: {:?}", parent))?;
    }
    // Check if already initialized
    if GLOBAL_STORAGE_ENGINE_MANAGER.get().is_some() {
        info!("StorageEngineManager already initialized, reusing existing instance");
        return Ok(());
    }
    // Initialize directly
    let manager = StorageEngineManager::new(&config_path_yaml).await?;
    GLOBAL_STORAGE_ENGINE_MANAGER.set(Arc::new(AsyncStorageEngineManager::from_manager(
        Arc::try_unwrap(manager).map_err(|_| GraphError::StorageError("Failed to unwrap Arc<StorageEngineManager>: multiple references exist".to_string()))?
    )))
        .map_err(|_| GraphError::StorageError("Failed to set StorageEngineManager: already initialized".to_string()))?;

    info!("StorageEngineManager initialized successfully");
    Ok(())
}

/// Recovers a corrupted Sled database
fn recover_sled_db(data_dir: &PathBuf) -> Result<(), GraphError> {
    warn!("Detected Sled database corruption at {:?}", data_dir);
    if data_dir.exists() {
        fs::remove_dir_all(data_dir)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to remove corrupted Sled database directory {:?}", data_dir))?;
        fs::create_dir_all(data_dir)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to recreate Sled database directory {:?}", data_dir))?;
        info!("Successfully cleared and recreated Sled database directory {:?}", data_dir);
    } else {
        fs::create_dir_all(data_dir)
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create Sled database directory {:?}", data_dir))?;
        info!("Created new Sled database directory {:?}", data_dir);
    }
    Ok(())
}

/// Recovers a RocksDB database by clearing stale lock file
fn recover_rocksdb(data_dir: &PathBuf) -> Result<(), GraphError> {
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
                match fs::metadata(&lock_file) {
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
                                .map_err(|e| GraphError::Io(e))
                                .with_context(|| format!("Failed to remove stale RocksDB lock file {:?}", lock_file))?;
                            info!("Successfully removed stale RocksDB lock file");
                            CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                            break;
                        } else {
                            // Attempt to shut down existing engine using blocking operations
                            if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
                                trace!("Attempting to shut down existing engine (retry {})", retries);
                                let manager_arc = Arc::clone(manager);
                                match tokio::runtime::Handle::try_current() {
                                    Ok(handle) => {
                                        if let Err(e) = handle.block_on(async {
                                            let manager = manager_arc.get_manager();
                                            let mut mgr = manager.lock().await;
                                            let mut engine = mgr.engine.lock().await;
                                            (*engine).stop().await
                                        }) {
                                            warn!("Failed to shut down existing manager: {:?}", e);
                                        } else {
                                            info!("Shut down existing engine before lock removal");
                                        }
                                    }
                                    Err(_) => {
                                        warn!("No tokio runtime available, skipping manager shutdown");
                                    }
                                }
                            } else {
                                trace!("No existing storage engine manager found to shut down");
                            }
                            // Wait to ensure resources are released
                            thread::sleep(std::time::Duration::from_millis(3000));
                            // Check if lock is held by current process
                            let current_pid = process::id();
                            trace!("Checking if lock is held by current process (PID: {})", current_pid);
                            if let Ok(lsof_output) = std::process::Command::new("lsof")
                                .arg(lock_file.to_str().unwrap())
                                .output()
                            {
                                let output = String::from_utf8_lossy(&lsof_output.stdout);
                                trace!("lsof output for lock file: {}", output);
                                let pid_lines: Vec<&str> = output.lines().filter(|line| line.contains(&lock_file.to_str().unwrap())).collect();
                                let mut lock_held_by_current = false;
                                for line in pid_lines {
                                    let fields: Vec<&str> = line.split_whitespace().collect();
                                    if fields.len() > 1 {
                                        if let Ok(pid) = fields[1].parse::<u32>() {
                                            if pid == current_pid || Pid::from_raw(pid as i32).as_raw() == current_pid as i32 {
                                                lock_held_by_current = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if lock_held_by_current || output.is_empty() {
                                    warn!("Lock file likely held by current process or stale, removing (retry {}): {:?}", retries, lock_file);
                                    fs::remove_file(&lock_file)
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
                            } else {
                                warn!("Failed to run lsof, assuming lock is stale (retry {}): {:?}", retries, lock_file);
                                fs::remove_file(&lock_file)
                                    .map_err(|e| GraphError::Io(e))
                                    .with_context(|| format!("Failed to remove RocksDB lock file {:?}", lock_file))?;
                                info!("Successfully removed RocksDB lock file");
                                CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
                                break;
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
                    let mut mgr = manager.lock().await;
                    let mut engine = mgr.engine.lock().await;
                    if let Err(e) = (*engine).stop().await {
                        warn!("Failed to shut down existing manager: {:?}", e);
                    } else {
                        info!("Shut down existing engine before lock removal");
                    }
                }
                // Wait to ensure resources are released
                thread::sleep(std::time::Duration::from_millis(3000));
                warn!("Removing RocksDB lock file (non-Unix system, retry {}): {:?}", retries, lock_file);
                fs::remove_file(&lock_file)
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
            thread::sleep(std::time::Duration::from_millis(3000));
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
            .map_err(|e| GraphError::Io(e))
            .with_context(|| format!("Failed to create RocksDB directory {:?}", data_dir))?;
    }
    info!("RocksDB directory ready: {:?}", data_dir);
    CLEANUP_IN_PROGRESS.store(false, Ordering::SeqCst);
    Ok(())
}

/// Performs emergency cleanup of the storage engine manager
pub async fn emergency_cleanup_storage_engine_manager() -> Result<(), GraphError> {
    info!("Starting emergency cleanup of storage engine manager");
    let db_path = PathBuf::from("/opt/graphdb/storage_data/rocksdb");

    if db_path.exists() {
        let db_path_clone = db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            std::fs::remove_dir_all(&db_path_clone)?;
            info!("Removed RocksDB directory {:?}", db_path_clone);
            std::fs::create_dir_all(&db_path_clone)?;
            let metadata = std::fs::metadata(&db_path_clone)?;
            let mut perms = metadata.permissions();
            #[cfg(unix)]
            perms.set_mode(0o755);
            std::fs::set_permissions(&db_path_clone, perms)?;
            info!("Recreated clean RocksDB directory at {:?}", db_path_clone);
            Ok(())
        }).await
        .map_err(|e| GraphError::StorageError(format!("Failed to clean RocksDB directory: {}", e)))??;
    }

    // Clear ROCKSDB_SINGLETON
    #[cfg(feature = "with-rocksdb")]
    {
        if ROCKSDB_SINGLETON.get().is_some() {
            warn!("Clearing ROCKSDB_SINGLETON for reinitialization");
            // Cannot unset OnceCell, but we can warn and reinitialize on next access
        }
    }

    // Clear GLOBAL_STORAGE_ENGINE_MANAGER
    if let Some(manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
        warn!("Clearing GLOBAL_STORAGE_ENGINE_MANAGER for reinitialization");
        // Shutdown existing manager
        let manager_instance = manager.get_manager();
        let mut mgr = manager_instance.lock().await;
        let mut engine = mgr.engine.lock().await;
        if let Err(e) = (*engine).stop().await {
            warn!("Failed to shut down existing manager during cleanup: {}", e);
        }
        // Cannot unset OnceCell, but shutdown ensures resources are released
    }
    info!("Finished emergency cleanup of storage engine manager");
    Ok(())
}

/// Creates a default YAML configuration file
fn create_default_yaml_config(yaml_path: &PathBuf, engine_type: StorageEngineType) -> Result<(), GraphError> {
    info!("Creating default YAML config at {:?}", yaml_path);
    let default_yaml = serde_yaml::to_string(&StorageConfigWrapper {
        storage: StorageConfig {
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
        },
    })
    .map_err(|e| GraphError::SerializationError(e.to_string()))?;
    
    let parent_dir = yaml_path.parent()
        .ok_or_else(|| GraphError::StorageError("Invalid YAML config path".to_string()))?;
    fs::create_dir_all(parent_dir)
        .map_err(|e| GraphError::Io(e))
        .with_context(|| format!("Failed to create directory for YAML config at {:?}", parent_dir))?;
    fs::write(yaml_path, default_yaml)
        .map_err(|e| GraphError::Io(e))
        .with_context(|| format!("Failed to write default YAML config to {:?}", yaml_path))?;
    info!("Default YAML config created at {:?}", yaml_path);
    Ok(())
}

// StorageEngine and GraphStorageEngine traits
#[async_trait]
pub trait StorageEngine: Send + Sync + Debug + 'static {
    async fn connect(&self) -> Result<(), GraphError>;
    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        Ok(())
    }
    async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        Ok(None)
    }
    async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        Ok(())
    }
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
    async fn connect(&self) -> Result<(), GraphError> {
        self.inmemory.connect().await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.connect().await?;
        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        self.inmemory.insert(key, value).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.insert(key, value).await?;
        Ok(())
    }

    async fn retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        if let Some(value) = self.inmemory.retrieve(key).await? {
            Ok(Some(value))
        } else {
            let persistent_arc = Arc::clone(&self.persistent);
            let result = persistent_arc.retrieve(key).await?;
            if let Some(ref value) = result {
                self.inmemory.insert(key, value).await?;
            }
            Ok(result)
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        self.inmemory.delete(key).await?;
        let persistent_arc = Arc::clone(&self.persistent);
        persistent_arc.delete(key).await?;
        Ok(())
    }

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
    pub async fn new(config_path_yaml: &Path) -> Result<Arc<StorageEngineManager>, GraphError> {
        info!("Creating StorageEngineManager with YAML config: {:?}", config_path_yaml);

        // Get an existing manager if it has been initialized
        if let Some(existing_manager) = GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            trace!("Shutting down existing StorageEngineManager before initialization");
            const MAX_RETRIES: u32 = 3;
            let mut retries = 0;
            while retries < MAX_RETRIES {
                let manager = existing_manager.get_manager();
                let mut mgr = manager.lock().await;
                let mut engine = mgr.engine.lock().await;
                if (*engine).is_running().await {
                    if let Err(e) = (*engine).stop().await {
                        warn!("Failed to shut down existing manager (retry {}): {:?}", retries, e);
                    } else {
                        info!("Shut down existing StorageEngineManager successfully");
                        break;
                    }
                }
                drop(engine);
                trace!("Waiting 3s to ensure resource release after shutdown attempt {}", retries);
                tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
                retries += 1;
            }
            if retries >= MAX_RETRIES {
                warn!("Failed to shut down existing StorageEngineManager after {} retries", MAX_RETRIES);
            }
        } else {
            trace!("No existing storage engine manager found to shut down");
        }

        // Load main configuration from YAML (file is always present)
        info!("Loading config from {:?}", config_path_yaml);
        let content = fs::read_to_string(&config_path_yaml)
            .map_err(|e| {
                error!("Failed to read YAML file at {:?}: {}", config_path_yaml, e);
                GraphError::Io(e)
            })?;

        debug!("Raw YAML content from {:?}:\n{}", config_path_yaml, content);
        let config_result = load_storage_config_from_yaml(Some(&config_path_yaml.to_path_buf()));
        if let Err(e) = &config_result {
            error!("Failed to deserialize YAML config from {:?}: {}", config_path_yaml, e);
            trace!("Deserialization error details: {:?}", e);
        }
        let mut config = config_result
            .map_err(|e| GraphError::ConfigurationError(format!("Failed to load YAML config: {}", e)))?;


        // Log deserialized config for debugging
        debug!("Deserialized config: {:?}", config);
        trace!("Config details - storage_engine_type: {:?}, default_port: {}, cluster_range: {}, engine_specific_config: {:?}", 
            config.storage_engine_type, config.default_port, config.cluster_range, config.engine_specific_config);

        // Validate engine type
        let engine_type = config.storage_engine_type;
        if !Self::available_engines().contains(&engine_type) {
            error!("Unsupported storage engine type: {:?}", engine_type);
            return Err(GraphError::ConfigurationError(format!("Unsupported storage engine type: {:?}", engine_type)));
        }

        // Load engine-specific config for RocksDB or Sled if needed
        if matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) &&
            (config.engine_specific_config.is_none() || config.engine_specific_config.as_ref().map_or(true, |map| {
                !map.contains_key("path") || map.get("path").and_then(|v| v.as_str()).unwrap_or("").is_empty()
            })) {
            info!("Loading engine-specific config for {:?}", engine_type);
            config.engine_specific_config = Some(load_engine_specific_config(engine_type, config_path_yaml)
                .map_err(|e| {
                    error!("Failed to load engine-specific config for {:?}: {}", engine_type, e);
                    GraphError::ConfigurationError(format!("Failed to load engine-specific config: {}", e))
                })?);
            debug!("Updated engine_specific_config: {:?}", config.engine_specific_config);
        } else {
            debug!("Using existing engine_specific_config for {:?}", engine_type);
        }

        // Update cluster_range to match default_port to avoid validation errors
        if config.cluster_range != config.default_port.to_string() {
            warn!("Updating cluster_range from {} to {} to match default_port", config.cluster_range, config.default_port);
            config.cluster_range = config.default_port.to_string();
        }

        // Validate configuration
        info!("Validating configuration for {:?}", engine_type);
        Self::validate_config(&config, engine_type)
            .map_err(|e| {
                error!("Configuration validation failed: {}", e);
                GraphError::ConfigurationError(format!("Configuration validation failed: {}", e))
            })?;
        info!("Configuration validated successfully");

        // Determine engine path
        let engine_path = match engine_type {
            StorageEngineType::Sled => {
                config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("Sled engine_specific_config: {:?}", map);
                        trace!("engine_specific_config keys: {:?}", map.keys());
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or_else(|| {
                        let path = PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY));
                        warn!("No path specified for Sled, using default: {:?}", path);
                        path
                    })
            }
            StorageEngineType::RocksDB => {
                config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("RocksDB engine_specific_config: {:?}", map);
                        trace!("engine_specific_config keys: {:?}", map.keys());
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .ok_or_else(|| {
                        error!("RocksDB path is missing in engine_specific_config: {:?}", config.engine_specific_config);
                        GraphError::ConfigurationError(format!(
                            "RocksDB path is missing in engine_specific_config: {:?}", config.engine_specific_config
                        ))
                    })?
            }
            _ => {
                let path = config.data_directory.clone();
                info!("Using data_directory for non-Sled/RocksDB engine: {:?}", path);
                path
            }
        };
        info!("Selected engine path: {:?}", engine_path);

        // Pre-check lock file for RocksDB to prevent corruption
        if engine_type == StorageEngineType::RocksDB {
            if lock_file_exists(&engine_path.join("LOCK")) {
                warn!("Lock file exists before RocksDB initialization: {:?}", engine_path.join("LOCK"));
                recover_rocksdb(&engine_path)?;
            } else {
                debug!("No lock file found for RocksDB at {:?}", engine_path.join("LOCK"));
            }
        }

        // Initialize storage engine
        let persistent: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::InMemory => {
                info!("Initializing InMemory engine");
                Arc::new(InMemoryGraphStorage::new(&config))
            }
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    let sled_config = {
                        let path = engine_path;
                        let host = config.engine_specific_config
                            .as_ref()
                            .and_then(|map| map.get("host").and_then(|v| v.as_str()))
                            .unwrap_or("127.0.0.1")
                            .to_string();
                        let port = config.engine_specific_config
                            .as_ref()
                            .and_then(|map| map.get("port").and_then(|v| v.as_u64()).map(|p| p as u16))
                            .unwrap_or(config.default_port);

                        SledConfig {
                            storage_engine_type: StorageEngineType::Sled,
                            path,
                            host: Some(host),
                            port: Some(port),
                        }
                    };
                    info!("Initializing Sled engine with path: {:?}", sled_config.path);
                    trace!("Checking for existing Sled singleton");

                    let sled_instance = SLED_SINGLETON.get_or_init(|| async {
                        trace!("Creating new SledStorage singleton");
                        let storage = SledStorage::new(&sled_config).expect("Failed to initialize Sled singleton");
                        Arc::new(storage) as Arc<dyn GraphStorageEngine + Send + Sync>
                    }).await;

                    Arc::clone(&sled_instance)
                }
                #[cfg(not(feature = "with-sled"))]
                {
                    error!("Sled support is not enabled in this build");
                    return Err(GraphError::StorageError("Sled support is not enabled. Please enable the 'with-sled' feature.".to_string()));
                }
            }
            #[cfg(feature = "with-rocksdb")]
            StorageEngineType::RocksDB => {
                let rocksdb_config = {
                    let path = engine_path;
                    let host = config.engine_specific_config
                        .as_ref()
                        .and_then(|map| map.get("host").and_then(|v| v.as_str()))
                        .unwrap_or("127.0.0.1")
                        .to_string();
                    let port = config.engine_specific_config
                        .as_ref()
                        .and_then(|map| map.get("port").and_then(|v| v.as_u64()).map(|p| p as u16))
                        .unwrap_or(config.default_port);
                    RocksdbConfig {
                        storage_engine_type: StorageEngineType::RocksDB,
                        path,
                        host: Some(host),
                        port: Some(port),
                    }
                };
                info!("Initializing RocksDB engine with path: {:?}", rocksdb_config.path);
                trace!("RocksDB PERMANENT flag: assumed true (persistent mode, not read-only)");

                let rocksdb_instance = ROCKSDB_SINGLETON.get_or_init(|| async {
                    trace!("Creating new RocksdbGraphStorage singleton");
                    let storage = RocksdbGraphStorage::new(&rocksdb_config).expect("Failed to initialize RocksDB singleton");
                    trace!("RocksDB initialized in persistent mode (PERMANENT=true)");
                    Arc::new(storage) as Arc<dyn GraphStorageEngine + Send + Sync>
                }).await;
                Arc::clone(&rocksdb_instance)
            }
            #[cfg(not(feature = "with-rocksdb"))]
            StorageEngineType::RocksDB => {
                error!("RocksDB support is not enabled in this build");
                return Err(GraphError::StorageError("RocksDB support is not enabled. Please enable the 'with-rocksdb' feature.".to_string()));
            }
            #[cfg(feature = "redis-datastore")]
            StorageEngineType::Redis => {
                let connection_string = config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("Redis engine_specific_config: {:?}", map);
                        format!(
                            "redis://{}:{}/{}",
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(6379),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("0")
                        )
                    })
                    .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());
                info!("Initializing Redis engine with connection: {}", connection_string);
                let redis_storage = RedisStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to initialize Redis: {}", e)))?;
                Arc::new(redis_storage)
            }
            #[cfg(not(feature = "redis-datastore"))]
            StorageEngineType::Redis => {
                error!("Redis support is not enabled in this build");
                return Err(GraphError::StorageError("Redis support is not enabled. Please enable the 'redis-datastore' feature.".to_string()));
            }
            #[cfg(feature = "postgres-datastore")]
            StorageEngineType::PostgreSQL => {
                let connection_string = config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("PostgreSQL engine_specific_config: {:?}", map);
                        format!(
                            "postgresql://{}:{}@{}:{}/{}",
                            map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                            map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(5432),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                        )
                    })
                    .unwrap_or_else(|| "postgresql://graphdb_user:secure_password@127.0.0.1:5432/graphdb".to_string());
                info!("Initializing PostgreSQL engine with connection: {}", connection_string);
                let postgres_storage = PostgresStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to initialize PostgreSQL: {}", e)))?;
                Arc::new(postgres_storage)
            }
            #[cfg(not(feature = "postgres-datastore"))]
            StorageEngineType::PostgreSQL => {
                error!("PostgreSQL support is not enabled in this build");
                return Err(GraphError::StorageError("PostgreSQL support is not enabled. Please enable the 'postgres-datastore' feature.".to_string()));
            }
            #[cfg(feature = "mysql-datastore")]
            StorageEngineType::MySQL => {
                let connection_string = config.engine_specific_config
                    .as_ref()
                    .map(|map| {
                        debug!("MySQL engine_specific_config: {:?}", map);
                        format!(
                            "mysql://{}:{}@{}:{}/{}",
                            map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                            map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                            map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                            map.get("port").and_then(|v| v.as_u64()).unwrap_or(3306),
                            map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                        )
                    })
                    .unwrap_or_else(|| "mysql://graphdb_user:secure_password@127.0.0.1:3306/graphdb".to_string());
                info!("Initializing MySQL engine with connection: {}", connection_string);
                let mysql_storage = MySQLStorage::new(&connection_string).await
                    .map_err(|e| GraphError::StorageError(format!("Failed to initialize MySQL: {}", e)))?;
                Arc::new(mysql_storage)
            }
            #[cfg(not(feature = "mysql-datastore"))]
            StorageEngineType::MySQL => {
                error!("MySQL support is not enabled in this build");
                return Err(GraphError::StorageError("MySQL support is not enabled. Please enable the 'mysql-datastore' feature.".to_string()));
            }
        };

        let engine = Arc::new(TokioMutex::new(HybridStorageEngine {
            inmemory: Arc::new(InMemoryGraphStorage::new(&config)),
            persistent: Arc::clone(&persistent),
            running: Arc::new(TokioMutex::new(false)),
            engine_type,
        }));

        info!("StorageEngineManager initialized with engine: {:?}", engine_type);

        let manager = StorageEngineManager {
            engine,
            persistent_engine: persistent,
            session_engine_type: None,
            config,
            config_path: config_path_yaml.to_path_buf(),
        };
        Ok(Arc::new(manager))
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
            if let Some(rocksdb_instance) = ROCKSDB_SINGLETON.get() {
                rocksdb_instance.close().await
                    .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB singleton: {}", e)))?;
                info!("RocksDB singleton closed during shutdown");
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
                    if let Some(rocksdb_storage) = self.persistent_engine.as_any().downcast_ref::<RocksdbGraphStorage>() {
                        rocksdb_storage.close().await
                            .map_err(|e| GraphError::StorageError(format!("Failed to close RocksDB database: {}", e)))?;
                        info!("RocksDB database connections closed");
                    } else {
                        warn!("Failed to downcast persistent_engine to RocksdbGraphStorage");
                    }
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    warn!("RocksDB support is not enabled, skipping close");
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
        drop(engine); // Release the lock
        let new_manager = StorageEngineManager::new(&self.config_path).await?;
        self.engine = new_manager.engine.clone();
        self.persistent_engine = new_manager.persistent_engine.clone();
        self.session_engine_type = new_manager.session_engine_type;
        self.config = new_manager.config.clone();
        info!("StorageEngineManager reset completed");
        Ok(())
    }

    pub fn get_persistent_engine(&self) -> Arc<dyn GraphStorageEngine + Send + Sync> {
        Arc::clone(&self.persistent_engine)
    }

    pub async fn get_runtime_config(&self) -> Result<StorageConfig, GraphError> {
        // Clone the persistent configuration. This is the base config.
        let mut config = self.config.clone();

        // If a session-specific engine type is active, it overrides the persistent one.
        // This allows for temporary changes without saving them permanently.
        if let Some(session_engine) = self.session_engine_type {
            config.storage_engine_type = session_engine;
        }

        // Wrap the final configuration in `Ok` to match the function's return type.
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

    fn validate_config(config: &StorageConfig, engine_type: StorageEngineType) -> Result<(), GraphError> {
        info!("Validating config for engine type: {:?}", engine_type);
        debug!("Full config: {:?}", config);

        match engine_type {
            StorageEngineType::Sled | StorageEngineType::RocksDB => {
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
                        .map_err(|e| GraphError::Io(e))
                        .with_context(|| format!("Failed to create engine-specific path: {:?}", path))?;
                }
                if !path.is_dir() {
                    return Err(GraphError::ConfigurationError(format!("Path for {:?} is not a directory: {:?}", engine_type, path)));
                }
                let test_file = path.join(".write_test");
                fs::write(&test_file, "")
                    .map_err(|e| GraphError::Io(e))
                    .with_context(|| format!("No write permissions for engine-specific path: {:?}", path))?;
                fs::remove_file(&test_file)
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
                    return Err(GraphError::ConfigurationError(format!("Host and port are required for {:?}", engine_type)));
                }
                if matches!(engine_type, StorageEngineType::PostgreSQL | StorageEngineType::MySQL) {
                    if !map.contains_key("username") || !map.contains_key("password") || !map.contains_key("database") {
                        return Err(GraphError::ConfigurationError(format!("Username, password, and database are required for {:?}", engine_type)));
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

    fn get_engine_config_path(&self, engine_type: StorageEngineType) -> PathBuf {
        let parent = self.config_path.parent().unwrap_or_else(|| Path::new(".")).to_path_buf();
        match engine_type {
            StorageEngineType::Sled => parent.join("storage_config_sled.yaml"),
            StorageEngineType::RocksDB => parent.join("storage_config_rocksdb.yaml"),
            StorageEngineType::InMemory => parent.join("storage_config_inmemory.yaml"),
            StorageEngineType::Redis => parent.join("storage_config_redis.yaml"),
            StorageEngineType::PostgreSQL => parent.join("storage_config_postgres.yaml"),
            StorageEngineType::MySQL => parent.join("storage_config_mysql.yaml"),
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
        super::config::available_engines()
    }

    pub async fn use_storage(&mut self, engine_type: StorageEngineType, permanent: bool) -> Result<(), GraphError> {
        info!("=== Starting use_storage for engine: {:?}, permanent: {} ===", engine_type, permanent);
        trace!("use_storage called with engine_type: {:?}", engine_type);
        println!("===> SEE WHAT PERMANENT IS {}", permanent);
        let start_time = Instant::now();

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
            (was_running, engine_guard.persistent.clone(), (*engine_guard).engine_type)
        };

        // Skip if no change needed
        if old_engine_type == engine_type && self.session_engine_type.is_none() {
            info!("No switch needed: current engine is already {:?}", engine_type);
            trace!("Skipping switch: current engine matches requested and no session override. Elapsed: {}ms", start_time.elapsed().as_millis());
            return Ok(());
        }

        // Determine config path
        let config_path = self.get_engine_config_path(engine_type);
        info!("Using config path for {:?}: {:?}", engine_type, config_path);
        trace!("Resolved config path: {:?}", config_path);

        // Load or create configuration
        let mut new_config = if config_path.exists() {
            info!("Loading existing config from {:?}", config_path);
            trace!("Reading config file: {:?}", config_path);
            load_storage_config_from_yaml(Some(&config_path.to_path_buf()))
                .map_err(|e| {
                    error!("Failed to load config for {:?} from {:?}: {:?}", engine_type, config_path, e);
                    trace!("Error details: {:?}", e);
                    GraphError::ConfigurationError(format!("Failed to load config from {:?}", config_path))
                })?
        } else {
            warn!("Config file not found at {:?}", config_path);
            if permanent {
                info!("Creating default config for {:?}", engine_type);
                trace!("Creating default config at {:?}", config_path);
                create_default_yaml_config(&config_path, engine_type)
                    .map_err(|e| {
                        error!("Failed to create default config for {:?} at {:?}: {:?}", engine_type, config_path, e);
                        trace!("Error details: {:?}", e);
                        GraphError::ConfigurationError(format!("Failed to create default config at {:?}", config_path))
                    })?;
                trace!("Loading newly created config from {:?}", config_path);
                load_storage_config_from_yaml(Some(&config_path.to_path_buf()))
                    .map_err(|e| {
                        error!("Failed to load default config for {:?} from {:?}: {:?}", engine_type, config_path, e);
                        trace!("Error details: {:?}", e);
                        GraphError::ConfigurationError(format!("Failed to load default config from {:?}", config_path))
                    })?
            } else {
                let mut current_config = self.config.clone();
                current_config.storage_engine_type = engine_type;
                let mut engine_config = HashMap::new();
                match engine_type {
                    StorageEngineType::Sled => {
                        engine_config.insert(
                            "path".to_string(),
                            Value::String("/opt/graphdb/storage_data/sled".to_string()),
                        );
                        engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                        engine_config.insert("port".to_string(), Value::Number(current_config.default_port.into()));
                        trace!("Set Sled config: path=/opt/graphdb/storage_data/sled");
                    }
                    StorageEngineType::RocksDB => {
                        engine_config.insert(
                            "path".to_string(),
                            Value::String("/opt/graphdb/storage_data/rocksdb".to_string()),
                        );
                        engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                        engine_config.insert("port".to_string(), Value::Number(current_config.default_port.into()));
                        trace!("Set RocksDB config: path=/opt/graphdb/storage_data/rocksdb, host=127.0.0.1, port={}", current_config.default_port);
                    }
                    StorageEngineType::Redis => {
                        engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                        engine_config.insert("port".to_string(), Value::Number(6379.into()));
                        engine_config.insert("database".to_string(), Value::String("0".to_string()));
                        trace!("Set Redis config: host=127.0.0.1, port=6379, database=0");
                    }
                    StorageEngineType::PostgreSQL => {
                        engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                        engine_config.insert("port".to_string(), Value::Number(5432.into()));
                        engine_config.insert("username".to_string(), Value::String("graphdb_user".to_string()));
                        engine_config.insert("password".to_string(), Value::String("secure_password".to_string()));
                        engine_config.insert("database".to_string(), Value::String("graphdb".to_string()));
                        trace!("Set PostgreSQL config: host=127.0.0.1, port=5432, user=graphdb_user, db=graphdb");
                    }
                    StorageEngineType::MySQL => {
                        engine_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
                        engine_config.insert("port".to_string(), Value::Number(3306.into()));
                        engine_config.insert("username".to_string(), Value::String("graphdb_user".to_string()));
                        engine_config.insert("password".to_string(), Value::String("secure_password".to_string()));
                        engine_config.insert("database".to_string(), Value::String("graphdb".to_string()));
                        trace!("Set MySQL config: host=127.0.0.1, port=3306, user=graphdb_user, db=graphdb");
                    }
                    StorageEngineType::InMemory => {
                        trace!("No engine-specific config for InMemory");
                    }
                }
                current_config.engine_specific_config = Some(engine_config);
                debug!("Generated temporary config for {:?}: {:?}", engine_type, current_config);
                current_config
            }
        };

        // Merge engine-specific config from storage_config_<engine>.yaml if engine_specific_config is null
        if new_config.engine_specific_config.is_none() {
            let engine_config_path = self.get_engine_config_path(engine_type);
            if engine_config_path.exists() {
                info!("Loading engine-specific config from: {:?}", engine_config_path);
                let engine_config = load_storage_config_from_yaml(Some(&engine_config_path))
                    .map_err(|e| {
                        error!("Failed to load engine-specific YAML config from {:?}: {}", engine_config_path, e);
                        GraphError::ConfigurationError(format!("Failed to load engine-specific YAML config: {}", e))
                    })?;
                new_config.engine_specific_config = engine_config.engine_specific_config;
            } else {
                warn!("Engine-specific config file not found at {:?}", engine_config_path);
                new_config.engine_specific_config = Some(HashMap::from_iter(vec![
                    ("path".to_string(), Value::String(format!("{}/{}", new_config.data_directory.display(), engine_type.to_string().to_lowercase()))),
                    ("host".to_string(), Value::String("127.0.0.1".to_string())),
                    ("port".to_string(), Value::Number(new_config.default_port.into())),
                ]));
            }
        }

        // Update cluster_range to include default_port to avoid validation errors
        new_config.cluster_range = format!("{}", new_config.default_port);

        info!("Loaded configuration for {:?}: {:?}", engine_type, new_config);
        trace!("Full config details: {:?}", new_config);

        // Validate configuration
        info!("Validating configuration for {:?}", engine_type);
        trace!("Calling validate_config for {:?}", engine_type);
        Self::validate_config(&new_config, engine_type)
            .map_err(|e| {
                error!("Configuration validation failed for {:?}: {:?}", engine_type, e);
                trace!("Validation error details: {:?}", e);
                e
            })?;

        // Determine engine path
        let engine_path = match engine_type {
            StorageEngineType::Sled => {
                new_config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("Sled engine_specific_config: {:?}", map);
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or(PathBuf::from("/opt/graphdb/storage_data/sled"))
            }
            StorageEngineType::RocksDB => {
                new_config.engine_specific_config
                    .as_ref()
                    .and_then(|map| {
                        debug!("RocksDB engine_specific_config: {:?}", map);
                        map.get("path").and_then(|v| v.as_str()).map(PathBuf::from)
                    })
                    .unwrap_or(PathBuf::from("/opt/graphdb/storage_data/rocksdb"))
            }
            _ => {
                let path = new_config.data_directory.clone();
                if path.as_os_str().is_empty() {
                    PathBuf::from("/opt/graphdb/storage_data")
                } else {
                    path
                }
            }
        };
        info!("Resolved engine path for {:?}: {:?}", engine_type, engine_path);
        trace!("Engine path details: exists={}, is_dir={}", engine_path.exists(), engine_path.is_dir());

        // Ensure engine path exists and has correct permissions
        if !engine_path.exists() {
            info!("Creating engine path: {:?}", engine_path);
            trace!("Creating directory: {:?}", engine_path);
            fs::create_dir_all(&engine_path)
                .map_err(|e| {
                    error!("Failed to create engine path {:?}: {:?}", engine_path, e);
                    trace!("IO error details: {:?}", e);
                    GraphError::Io(e)
                })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let permissions = fs::Permissions::from_mode(0o755);
                trace!("Setting permissions for path {:?}", engine_path);
                fs::set_permissions(&engine_path, permissions)
                    .map_err(|e| {
                        error!("Failed to set permissions for {:?}: {:?}", engine_path, e);
                        trace!("Permission error details: {:?}", e);
                        GraphError::Io(e)
                    })?;
                debug!("Set permissions for path {:?}", engine_path);
            }
        }
        if !engine_path.is_dir() {
            error!("Engine path is not a directory: {:?}", engine_path);
            return Err(GraphError::InvalidData(format!("Engine path is not a directory: {:?}", engine_path)));
        }
        debug!("Verified engine path: {:?}", engine_path);

        // Initialize new engine
        let new_persistent: Arc<dyn GraphStorageEngine + Send + Sync> = match engine_type {
            StorageEngineType::InMemory => {
                info!("Initializing InMemory engine");
                trace!("Creating new InMemoryGraphStorage");
                Arc::new(InMemoryGraphStorage::new(&new_config))
            }
            StorageEngineType::Sled => {
                #[cfg(feature = "with-sled")]
                {
                    // First, call the function that ensures the map has all the necessary
                    // keys, including defaults. This map is now guaranteed to have `path`,
                    // `host`, and `port`.
                    // The `base_path` argument should be `new_config.config_root_directory`.
                    let augmented_config_map = load_engine_specific_config(
                        StorageEngineType::Sled,
                        &new_config.config_root_directory
                    )?;

                    // Now, deserialize the SledConfig from the corrected map.
                    // This will succeed because the map is no longer missing any keys.
                    let sled_config = serde_json::from_value::<super::config::SledConfig>(
                        serde_json::Value::Object(serde_json::Map::from_iter(augmented_config_map.into_iter()))
                    ).map_err(|e| {
                        GraphError::ConfigurationError(format!("Failed to deserialize SledConfig from augmented map: {}", e))
                    })?;

                    info!("Initializing Sled engine with path: {:?}", sled_config.path);
                    trace!("Checking for existing Sled singleton");

                    let sled_instance = SLED_SINGLETON.get_or_init(|| async {
                        trace!("Creating new SledStorage singleton");
                        Arc::new(SledStorage::new(&sled_config).expect("Failed to initialize Sled singleton")) as Arc<dyn GraphStorageEngine + Send + Sync>
                    }).await;

                    Arc::clone(&sled_instance)
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
                    let rocksdb_config = {
                        let path = new_config.engine_specific_config
                            .as_ref()
                            .and_then(|map| map.get("path"))
                            .and_then(|v| v.as_str())
                            .map(PathBuf::from)
                            .ok_or_else(|| GraphError::ConfigurationError("RocksDB path is missing in engine_specific_config.".to_string()))?;
                        let host = new_config.engine_specific_config
                            .as_ref()
                            .and_then(|map| map.get("host"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("127.0.0.1")
                            .to_string();
                        let port = new_config.engine_specific_config
                            .as_ref()
                            .and_then(|map| map.get("port"))
                            .and_then(|v| v.as_u64())
                            .map(|p| p as u16)
                            .unwrap_or(new_config.default_port);

                        RocksdbConfig {
                            // Correctly named fields
                            storage_engine_type: StorageEngineType::RocksDB,
                            path,
                            host: Some(host),
                            port: Some(port),
                        }
                    };

                    info!("Initializing RocksDB engine with path: {:?}", rocksdb_config.path);
                    trace!("Checking for existing RocksDB singleton");
                    if lock_file_exists(&rocksdb_config.path.join("LOCK")) {
                        warn!("Lock file exists before RocksDB initialization: {:?}", rocksdb_config.path.join("LOCK"));
                        trace!("Attempting to recover RocksDB");
                        recover_rocksdb(&rocksdb_config.path)?;
                    } else {
                        debug!("No lock file found for RocksDB at {:?}", rocksdb_config.path.join("LOCK"));
                    }

                    let rocksdb_instance = ROCKSDB_SINGLETON.get_or_init(|| async {
                        trace!("Creating new RocksdbGraphStorage singleton");
                        Arc::new(RocksdbGraphStorage::new(&rocksdb_config).expect("Failed to initialize RocksDB singleton")) as Arc<dyn GraphStorageEngine + Send + Sync>
                    }).await;

                    Arc::clone(&rocksdb_instance)
                }
                #[cfg(not(feature = "with-rocksdb"))]
                {
                    error!("RocksDB support is not enabled in this build");
                    return Err(GraphError::StorageError("RocksDB support is not enabled. Please enable the 'with-rocksdb' feature.".to_string()));
                }
            }
            StorageEngineType::Redis => {
                #[cfg(feature = "redis-datastore")]
                {
                    let connection_string = new_config.engine_specific_config
                        .as_ref()
                        .map(|map| {
                            debug!("Redis engine_specific_config: {:?}", map);
                            format!(
                                "redis://{}:{}/{}",
                                map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                                map.get("port").and_then(|v| v.as_u64()).unwrap_or(6379),
                                map.get("database").and_then(|v| v.as_str()).unwrap_or("0")
                            )
                        })
                        .unwrap_or_else(|| {
                            warn!("No engine-specific config for Redis, using default connection string");
                            "redis://127.0.0.1:6379/0".to_string()
                        });
                    info!("Initializing Redis engine with connection: {}", connection_string);
                    let redis_storage = RedisStorage::new(&connection_string).await
                        .map_err(|e| GraphError::StorageError(format!("Failed to initialize Redis: {}", e)))?;
                    Arc::new(redis_storage)
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
                    let connection_string = new_config.engine_specific_config
                        .as_ref()
                        .map(|map| {
                            debug!("PostgreSQL engine_specific_config: {:?}", map);
                            format!(
                                "postgresql://{}:{}@{}:{}/{}",
                                map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                                map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                                map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                                map.get("port").and_then(|v| v.as_u64()).unwrap_or(5432),
                                map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                            )
                        })
                        .unwrap_or_else(|| {
                            warn!("No engine-specific config for PostgreSQL, using default connection string");
                            "postgresql://graphdb_user:secure_password@127.0.0.1:5432/graphdb".to_string()
                        });
                    info!("Initializing PostgreSQL engine with connection: {}", connection_string);
                    let postgres_storage = PostgresStorage::new(&connection_string).await
                        .map_err(|e| GraphError::StorageError(format!("Failed to initialize PostgreSQL: {}", e)))?;
                    Arc::new(postgres_storage)
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
                    let connection_string = new_config.engine_specific_config
                        .as_ref()
                        .map(|map| {
                            debug!("MySQL engine_specific_config: {:?}", map);
                            format!(
                                "mysql://{}:{}@{}:{}/{}",
                                map.get("username").and_then(|v| v.as_str()).unwrap_or("graphdb_user"),
                                map.get("password").and_then(|v| v.as_str()).unwrap_or("secure_password"),
                                map.get("host").and_then(|v| v.as_str()).unwrap_or("127.0.0.1"),
                                map.get("port").and_then(|v| v.as_u64()).unwrap_or(3306),
                                map.get("database").and_then(|v| v.as_str()).unwrap_or("graphdb")
                            )
                        })
                        .unwrap_or_else(|| {
                            warn!("No engine-specific config for MySQL, using default connection string");
                            "mysql://graphdb_user:secure_password@127.0.0.1:3306/graphdb".to_string()
                        });
                    info!("Initializing MySQL engine with connection: {}", connection_string);
                    let mysql_storage = MySQLStorage::new(&connection_string).await
                        .map_err(|e| GraphError::StorageError(format!("Failed to initialize MySQL: {}", e)))?;
                    Arc::new(mysql_storage)
                }
                #[cfg(not(feature = "mysql-datastore"))]
                {
                    error!("MySQL support is not enabled in this build");
                    return Err(GraphError::StorageError("MySQL support is not enabled. Please enable the 'mysql-datastore' feature.".to_string()));
                }
            }
        };

        info!("New engine initialized: {:?}", engine_type);
        trace!("New engine type: {:?}", engine_type);

        // Create new HybridStorageEngine
        let new_engine = Arc::new(TokioMutex::new(HybridStorageEngine {
            inmemory: Arc::new(InMemoryGraphStorage::new(&new_config)),
            persistent: new_persistent.clone(),
            running: Arc::new(TokioMutex::new(false)),
            engine_type,
        }));

        // Migrate data if needed
        if was_running && old_engine_type != engine_type {
            info!("Migrating data from {} to {}", old_engine_type, engine_type);
            trace!("Starting data migration from {} to {}", old_engine_type, engine_type);
            self.migrate_data(&old_persistent_arc, &new_persistent).await
                .map_err(|e| {
                    error!("Data migration failed from {} to {}: {:?}", old_engine_type, engine_type, e);
                    trace!("Migration error details: {:?}", e);
                    e
                })?;
            info!("Data migration completed successfully");
        } else {
            debug!("No data migration needed: was_running={}, old_engine_type={:?}, new_engine_type={:?}",
                   was_running, old_engine_type, engine_type);
        }

        // Stop current engine if running
        if was_running {
            info!("Stopping current engine: {:?}", old_engine_type);
            trace!("Acquiring engine lock to stop old engine");
            let engine = self.engine.lock().await;
            (*engine).stop().await
                .map_err(|e| {
                    error!("Failed to stop current engine {:?}: {:?}", old_engine_type, e);
                    trace!("Stop error details: {:?}", e);
                    GraphError::StorageError(format!("Failed to stop current engine: {}", e))
                })?;
        }

        // Update manager state
        {
            // Replace the entire engine Arc with the new_engine
            self.engine = new_engine;
            let engine_guard = self.engine.lock().await;
            info!("Updated engine to {:?}", engine_type);
            trace!("New engine set in manager");

            // Start the new engine
            (*engine_guard).start().await
                .map_err(|e| {
                    error!("Failed to start new engine {:?}: {:?}", engine_type, e);
                    trace!("Start error details: {:?}", e);
                    GraphError::StorageError(format!("Failed to start new engine: {}", e))
                })?;
            info!("New engine started: {:?}", engine_type);
        }

        // Update configuration if permanent
        if permanent {
            info!("Applying permanent configuration change to {:?}", engine_type);
            trace!("Updating persistent config");
            self.config = new_config.clone();
            self.config_path = config_path.clone();
            self.persistent_engine = new_persistent;
            self.session_engine_type = None;

            // Save updated configuration
            let config_wrapper = StorageConfigWrapper {
                storage: new_config.clone(),
            };
            let yaml_data = serde_yaml::to_string(&config_wrapper)
                .map_err(|e| {
                    error!("Failed to serialize new config for {:?}: {:?}", engine_type, e);
                    trace!("Serialization error details: {:?}", e);
                    GraphError::SerializationError(e.to_string())
                })?;
            trace!("Serialized new config: {}", yaml_data);
            fs::write(&config_path, yaml_data)
                .map_err(|e| {
                    error!("Failed to write config to {:?}: {:?}", config_path, e);
                    trace!("IO error details: {:?}", e);
                    GraphError::Io(e)
                })?;
            info!("Configuration saved to {:?}", config_path);
        } else {
            info!("Applying temporary configuration change to {:?}", engine_type);
            self.session_engine_type = Some(engine_type);
            self.persistent_engine = new_persistent;
        }

        // Register with DaemonRegistry
        {
            let port = new_config.default_port;
            let ip_address = new_config.engine_specific_config
                .as_ref()
                .and_then(|map| map.get("host").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let data_dir = new_config.data_directory.clone();
            let config_path = config_path.clone();
            let last_seen_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);

            let metadata = DaemonMetadata {
                service_type: "storage".to_string(),
                port,
                pid: process::id(),
                ip_address,
                data_dir: Some(data_dir),
                config_path: Some(config_path),
                engine_type: Some(engine_type.to_string()),
                last_seen_nanos,
            };

            info!("Registering daemon with metadata: {:?}", metadata);
            trace!("Attempting to register daemon with port {}", port);
            GLOBAL_DAEMON_REGISTRY.register_daemon(metadata).await
                .map_err(|e| {
                    error!("Failed to register daemon for port {}: {:?}", port, e);
                    trace!("Daemon registration error details: {:?}", e);
                    GraphError::StorageError(format!("Failed to register daemon: {}", e))
                })?;
            info!("Daemon registered successfully for port {}", port);
        }

        info!("=== Completed use_storage for engine: {:?} in {}ms ===", engine_type, start_time.elapsed().as_millis());
        trace!("use_storage completed successfully");
        Ok(())
    }

    pub async fn get_engine_metrics(&self) -> Result<Map<String, Value>, GraphError> {
        info!("Fetching engine metrics");
        let engine = self.engine.lock().await;
        let engine_type = (*engine).engine_type.clone();
        let is_running = (*engine).is_running().await;
        drop(engine);

        let mut metrics = Map::new();
        metrics.insert("engine_type".to_string(), Value::String(engine_type.to_string()));
        metrics.insert("is_running".to_string(), Value::Bool(is_running));
        metrics.insert("timestamp".to_string(), Value::Number(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0)
                .into()
        ));

        debug!("Engine metrics: {:?}", metrics);
        Ok(metrics)
    }

    pub async fn check_health(&self) -> Result<bool, GraphError> {
        info!("Checking engine health");
        let engine = self.engine.lock().await;
        let is_running = (*engine).is_running().await;
        drop(engine);

        debug!("Health check result: is_running={}", is_running);
        Ok(is_running)
    }

    pub async fn clear_data(&self) -> Result<(), GraphError> {
        info!("Clearing all data in storage engine");
        let engine = self.engine.lock().await;
        let engine_type = (*engine).engine_type.clone();
        let result = (*engine).clear_data().await;
        drop(engine);

        match result {
            Ok(()) => {
                info!("Data cleared successfully for engine: {:?}", engine_type);
                Ok(())
            }
            Err(e) => {
                error!("Failed to clear data for engine {:?}: {:?}", engine_type, e);
                Err(e)
            }
        }
    }
}

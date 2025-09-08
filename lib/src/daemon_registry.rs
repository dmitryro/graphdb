use anyhow::Result;
use serde::{Serialize, Deserialize};
use std::path::{Path, PathBuf};
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore, OnceCell};
use std::collections::HashMap;
use log::{info, warn, debug, error};
use sysinfo::{Pid, System};
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode};
use std::time::Duration;
use tokio::fs;
use sled::{Db, IVec, Config};

use crate::daemon_config::{
    DAEMON_REGISTRY_DB_PATH,
    DAEMON_PID_FILE_NAME_PREFIX,
    REST_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_NAME_PREFIX,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Encode, Decode)]
pub struct DaemonMetadata {
    pub service_type: String,
    pub port: u16,
    pub pid: u32,
    pub ip_address: String,
    pub data_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub engine_type: Option<String>,
    pub last_seen_nanos: i64,
}

#[derive(Clone)]
struct ImprovedSledPool {
    db: Arc<Db>,
    _semaphore: Arc<Semaphore>,
}

impl ImprovedSledPool {
    async fn new(db_path: PathBuf, max_concurrent: usize) -> Result<Self> {
        Self::validate_environment(&db_path).await?;

        let db = tokio::task::spawn_blocking(move || {
            Config::new()
                .path(db_path)
                .cache_capacity(16 * 1024 * 1024)
                .flush_every_ms(Some(1000))
                .use_compression(true)
                .open()
        }).await??;

        Ok(ImprovedSledPool {
            db: Arc::new(db),
            _semaphore: Arc::new(Semaphore::new(max_concurrent)),
        })
    }

    async fn validate_environment(db_path: &PathBuf) -> Result<()> {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).await?;
            let metadata = fs::metadata(parent).await?;
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(parent, perms).await?;
        }

        Ok(())
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        tokio::task::spawn_blocking(move || -> Result<()> {
            db.insert(key, value)
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("Failed to insert into sled: {}", e))
        }).await?
    }

    async fn get(&self, key: &[u8]) -> Result<Option<IVec>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || -> Result<Option<IVec>> {
            db.get(key).map_err(|e| anyhow::anyhow!("Failed to get from sled: {}", e))
        }).await?
    }

    async fn remove(&self, key: &[u8]) -> Result<Option<IVec>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || -> Result<Option<IVec>> {
            db.remove(key).map_err(|e| anyhow::anyhow!("Failed to remove from sled: {}", e))
        }).await?
    }

    async fn iter_all(&self) -> Result<Vec<(IVec, IVec)>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> Result<Vec<(IVec, IVec)>> {
            db.iter().map(|result| result.map_err(|e| anyhow::anyhow!("Failed to iterate sled: {}", e))).collect()
        }).await?
    }
}

#[derive(Clone)]
pub struct DaemonRegistry {
    inner: Arc<NonBlockingDaemonRegistry>,
}

impl DaemonRegistry {
    pub async fn new() -> Result<Self> {
        Ok(DaemonRegistry {
            inner: Arc::new(NonBlockingDaemonRegistry::new().await?),
        })
    }
}

pub struct AsyncRegistryWrapper {
    registry: DaemonRegistry,
}

impl AsyncRegistryWrapper {
    pub async fn new() -> Result<Self> {
        Ok(AsyncRegistryWrapper {
            registry: DaemonRegistry::new().await?,
        })
    }

    pub async fn register_daemon(&self, metadata: DaemonMetadata) -> Result<()> {
        self.registry.inner.register_daemon(metadata).await
    }

    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.registry.inner.get_daemon_metadata(port).await
    }

    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        self.registry.inner.unregister_daemon(port).await
    }

    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DaemonMetadata>> {
        self.registry.inner.get_all_daemon_metadata().await
    }

    pub async fn clear_all_daemons(&self) -> Result<()> {
        self.registry.inner.clear_all_daemons().await
    }

    pub async fn find_daemon_by_port(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.registry.inner.get_daemon_metadata(port).await
    }

    pub async fn remove_daemon_by_type(&self, service_type: &str, port: u16) -> Result<Option<DaemonMetadata>> {
        self.registry.inner.remove_daemon_by_type(service_type, port).await
    }

    pub async fn close(&self) -> Result<()> {
        self.registry.inner.close().await
    }

    pub async fn health_check(&self) -> Result<bool> {
        self.registry.inner.health_check().await
    }
}

#[derive(Debug)]
struct RegistryConfig {
    is_fallback_mode: bool,
    fallback_file: PathBuf,
    db_path: PathBuf,
    max_concurrent_ops: usize,
}

#[derive(Clone)]
struct NonBlockingDaemonRegistry {
    memory_store: Arc<RwLock<HashMap<u16, DaemonMetadata>>>,
    storage: Arc<RwLock<Option<ImprovedSledPool>>>,
    config: Arc<RegistryConfig>,
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

impl NonBlockingDaemonRegistry {
    pub async fn new() -> Result<Self> {
        let config = Arc::new(RegistryConfig {
            is_fallback_mode: Self::should_use_fallback_mode(),
            fallback_file: Self::get_fallback_file_path(),
            db_path: Self::get_db_path(),
            max_concurrent_ops: 10,
        });

        let memory_store = Arc::new(RwLock::new(HashMap::new()));
        let storage = Arc::new(RwLock::new(None));

        let registry = NonBlockingDaemonRegistry {
            memory_store,
            storage,
            config: config.clone(),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
        };

        if !config.is_fallback_mode {
            registry.initialize_storage_background().await;
        }

        registry.load_initial_data().await?;

        Ok(registry)
    }

    fn should_use_fallback_mode() -> bool {
        std::env::args().any(|arg| {
            matches!(arg.as_str(), "status" | "stop" | "list" | "--help" | "-h")
        })
    }

    fn get_fallback_file_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(".graphdb")
            .join("daemon_registry_fallback.json")
    }

    fn get_db_path() -> PathBuf {
        dirs::home_dir()
            .map(|home| home.join(".graphdb").join("daemon_registry_db"))
            .unwrap_or_else(|| PathBuf::from("/tmp/graphdb_registry"))
    }

    async fn initialize_storage_background(&self) {
        let storage = self.storage.clone();
        let db_path = self.config.db_path.clone();
        let max_concurrent = self.config.max_concurrent_ops;

        let task = tokio::spawn(async move {
            match ImprovedSledPool::new(db_path, max_concurrent).await {
                Ok(pool) => {
                    let mut storage_guard = storage.write().await;
                    *storage_guard = Some(pool);
                    info!("Storage backend initialized successfully");
                }
                Err(e) => {
                    warn!(
                        "Failed to initialize storage backend: {}. Operating in memory-only mode",
                        e
                    );
                }
            }
        });

        if let Err(e) = task.await {
            warn!("Storage initialization task failed: {}", e);
        } else {
            let mut tasks = self.background_tasks.write().await;
        }
    }

    async fn load_initial_data(&self) -> Result<()> {
        if let Ok(data) = self.load_from_fallback().await {
            let mut memory = self.memory_store.write().await;
            for metadata in data {
                if NonBlockingDaemonRegistry::is_pid_running(metadata.pid).await.unwrap_or(false) {
                    memory.insert(metadata.port, metadata);
                }
            }
            info!("Loaded initial data from fallback file");
        }

        if !self.config.is_fallback_mode {
            self.schedule_storage_sync().await;
        }

        Ok(())
    }

    async fn schedule_storage_sync(&self) {
        let storage = self.storage.clone();

        let task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                info!("Background storage sync completed");
            }
        });

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    async fn validate_process_fast(&self, pid: u32, _port: u16) -> Result<bool> {
        let validation_result = tokio::spawn(async move {
            NonBlockingDaemonRegistry::is_pid_running(pid).await
        }).await?;

        validation_result
    }

    async fn is_pid_running(pid: u32) -> Result<bool> {
        tokio::task::spawn_blocking(move || {
            let mut sys = System::new();
            let sysinfo_pid = Pid::from_u32(pid);
            sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sysinfo_pid]), false);
            Ok(sys.process(sysinfo_pid).is_some())
        }).await?
    }

    pub async fn register_daemon(&self, metadata: DaemonMetadata) -> Result<()> {
        let is_valid = self.validate_process_fast(metadata.pid, metadata.port).await?;
        if !is_valid {
            return Err(anyhow::anyhow!("Process validation failed for PID {}", metadata.pid));
        }

        self.clean_stale_daemons().await?;

        // First, clear any existing entries for this port to prevent stale data.
        let mut memory = self.memory_store.write().await;
        memory.remove(&metadata.port);
        memory.insert(metadata.port, metadata.clone());
        drop(memory);

        // Now, update the Sled database synchronously to ensure persistence.
        let storage = self.storage.clone();
        let metadata_clone = metadata.clone();
        
        // This is a crucial change: we await the persistence task.
        let task = tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                let key = metadata_clone.port.to_string().into_bytes();
                let encoded = encode_to_vec(&metadata_clone, config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to encode metadata for port {}: {}", metadata_clone.port, e))?;
                if let Err(e) = pool.insert(&key, &encoded).await {
                    warn!("Failed to insert into sled for port {}: {}", metadata_clone.port, e);
                }
            }
            Ok::<_, anyhow::Error>(())
        });
        task.await??;

        // Also update the fallback file
        let memory = self.memory_store.read().await;
        let all_metadata: Vec<_> = memory.values().cloned().collect();
        drop(memory);
        Self::save_fallback_file(&self.config.fallback_file, &all_metadata).await?;

        info!("Registered daemon: {} on port {}", metadata.service_type, metadata.port);
        Ok(())
    }
    
    // This function now reloads from Sled to guarantee fresh data.
    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.clean_stale_daemons().await?;

        let memory = self.memory_store.read().await;
        if let Some(metadata) = memory.get(&port) {
            let metadata_clone = metadata.clone();
            drop(memory);

            if self.validate_process_fast(metadata_clone.pid, port).await.unwrap_or(false) {
                return Ok(Some(metadata_clone));
            }
        }
        
        self.discover_daemon(port).await
    }
    
    async fn discover_daemon(&self, _port: u16) -> Result<Option<DaemonMetadata>> {
        Ok(None)
    }
    
    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DaemonMetadata>> {
        self.clean_stale_daemons().await?;
        
        // CRUCIAL CHANGE: Re-sync with the Sled database on every call.
        let storage_guard = self.storage.read().await;
        if let Some(pool) = &*storage_guard {
            let mut memory = self.memory_store.write().await;
            memory.clear();
            
            let all_sled_entries = pool.iter_all().await?;
            for (_, encoded_value) in all_sled_entries {
                let metadata: DaemonMetadata = decode_from_slice(&encoded_value, config::standard())?.0;
                memory.insert(metadata.port, metadata);
            }
        }
        drop(storage_guard);
        
        let memory = self.memory_store.read().await;
        let all_metadata: Vec<_> = memory.values().cloned().collect();
        drop(memory);
        
        let valid_metadata = tokio::spawn(async move {
            let mut valid = Vec::new();
            for metadata in all_metadata {
                if NonBlockingDaemonRegistry::is_pid_running(metadata.pid).await.unwrap_or(false) {
                    valid.push(metadata);
                }
            }
            valid
        }).await?;
        
        Ok(valid_metadata)
    }
    
    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        let metadata = {
            let mut memory = self.memory_store.write().await;
            memory.remove(&port)
        };
        
        if let Some(metadata) = metadata {
            self.cleanup_daemon_background(metadata).await;
            info!("Unregistered daemon on port {}", port);
        }
        
        Ok(())
    }
    
    async fn cleanup_daemon_background(&self, metadata: DaemonMetadata) {
        let storage = self.storage.clone();
        let fallback_file = self.config.fallback_file.clone();
        let memory_store = self.memory_store.clone();
        
        let task = tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                let key = metadata.port.to_string().into_bytes();
                let _ = pool.remove(&key).await;
            }
            drop(storage_guard);
            
            let memory = memory_store.read().await;
            let all_metadata: Vec<_> = memory.values().cloned().collect();
            drop(memory);
            
            let _ = Self::save_fallback_file(&fallback_file, &all_metadata).await;
            
            let pid_files = vec![
                format!("/tmp/graphdb-daemon-{}.pid", metadata.port),
                format!("/tmp/graphdb-rest-{}.pid", metadata.port),
                format!("/tmp/graphdb-storage-{}.pid", metadata.port),
            ];
            
            for pid_file in pid_files {
                let _ = fs::remove_file(&pid_file).await;
            }
        });
        
        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }
    
    async fn terminate_process(pid: u32) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            let mut system = System::new();
            let sysinfo_pid = Pid::from_u32(pid);
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[sysinfo_pid]), true);
            
            if let Some(process) = system.process(sysinfo_pid) {
                let _ = process.kill_with(sysinfo::Signal::Term);
            }
        }).await?;
        
        Ok(())
    }
    
    pub async fn clear_all_daemons(&self) -> Result<()> {
        let metadata_list = {
            let mut memory = self.memory_store.write().await;
            let list: Vec<_> = memory.values().cloned().collect();
            memory.clear();
            list
        };
        
        self.cleanup_all_daemons_background(metadata_list).await;
        
        info!("Cleared all daemons from registry");
        Ok(())
    }
    
    async fn cleanup_all_daemons_background(&self, metadata_list: Vec<DaemonMetadata>) {
        let storage = self.storage.clone();
        let fallback_file = self.config.fallback_file.clone();
        
        let task = tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(_pool) = &*storage_guard {
                debug!("Clearing storage backend");
            }
            drop(storage_guard);
            
            for metadata in metadata_list {
                let pid_files = vec![
                    format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, metadata.port),
                    format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, metadata.port),
                    format!("/tmp/graphdb-storage-{}.pid", metadata.port),
                ];
                
                for pid_file in pid_files {
                    let _ = fs::remove_file(&pid_file).await;
                }
                
                let _ = Self::terminate_process(metadata.pid).await;
            }
            
            let _ = Self::save_fallback_file(&fallback_file, &[]).await;
        });
        
        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    pub async fn remove_daemon_by_type(&self, service_type: &str, port: u16) -> Result<Option<DaemonMetadata>> {
        let metadata = {
            let mut memory = self.memory_store.write().await;
            if let Some(metadata) = memory.get(&port) {
                if metadata.service_type == service_type {
                    memory.remove(&port)
                } else {
                    None
                }
            } else {
                None
            }
        };
        
        if let Some(ref metadata_ref) = metadata {
            self.cleanup_daemon_background(metadata_ref.clone()).await;
            info!("Removed {} daemon on port {}", service_type, port);
        }
        
        Ok(metadata)
    }

    pub async fn clean_stale_daemons(&self) -> Result<()> {
        let mut memory = self.memory_store.write().await;
        let mut to_remove = Vec::new();
        for (port, metadata) in memory.iter() {
            if !self.validate_process_fast(metadata.pid, *port).await.unwrap_or(false) {
                to_remove.push(*port);
            }
        }
        
        for port in to_remove {
            if let Some(metadata) = memory.remove(&port) {
                self.cleanup_daemon_background(metadata).await;
                info!("Removed stale daemon entry for port {}", port);
            }
        }
        
        Ok(())
    }

    pub async fn health_check(&self) -> Result<bool> {
        let _memory = self.memory_store.read().await;
        
        let storage_guard = self.storage.read().await;
        if let Some(pool) = &*storage_guard {
            let test_key = b"__health_check__";
            let test_value = b"ok";
            if pool.insert(test_key, test_value).await.is_ok() {
                let _ = pool.remove(test_key).await;
                return Ok(true);
            }
        }
        
        Ok(true)
    }

    async fn load_from_fallback(&self) -> Result<Vec<DaemonMetadata>> {
        if !self.config.fallback_file.exists() {
            return Ok(Vec::new());
        }
        
        let data = fs::read_to_string(&self.config.fallback_file).await?;
        let metadata_list: Vec<DaemonMetadata> = serde_json::from_str(&data)?;
        Ok(metadata_list)
    }
    
    async fn save_fallback_file(file_path: &PathBuf, metadata_list: &[DaemonMetadata]) -> Result<()> {
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
            let metadata = fs::metadata(parent).await?;
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(parent, perms).await?;
        }
        
        let data = serde_json::to_string_pretty(metadata_list)?;
        
        let temp_file = file_path.with_extension("tmp");
        fs::write(&temp_file, data).await?;
        fs::rename(&temp_file, file_path).await?;
        
        Ok(())
    }
    
    pub async fn close(&self) -> Result<()> {
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            task.abort();
        }
        
        let memory = self.memory_store.read().await;
        let all_metadata: Vec<_> = memory.values().cloned().collect();
        drop(memory);
        
        Self::save_fallback_file(&self.config.fallback_file, &all_metadata).await?;
        
        info!("Registry closed gracefully");
        Ok(())
    }
}

pub struct DaemonRegistryWrapper {
    inner: OnceCell<AsyncRegistryWrapper>,
}

impl DaemonRegistryWrapper {
    pub const fn new() -> Self {
        DaemonRegistryWrapper {
            inner: OnceCell::const_new(),
        }
    }
    
    pub async fn get(&self) -> &AsyncRegistryWrapper {
        self.inner
            .get_or_init(|| async {
                AsyncRegistryWrapper::new().await.unwrap_or_else(|e| {
                    error!("Failed to initialize registry: {}", e);
                    panic!("Cannot initialize registry")
                })
            })
            .await
    }
    
    pub async fn register_daemon(&self, metadata: DaemonMetadata) -> Result<()> {
        self.get().await.register_daemon(metadata).await
    }
    
    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.get().await.get_daemon_metadata(port).await
    }
    
    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        self.get().await.unregister_daemon(port).await
    }
    
    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DaemonMetadata>> {
        self.get().await.get_all_daemon_metadata().await
    }
    
    pub async fn clear_all_daemons(&self) -> Result<()> {
        self.get().await.clear_all_daemons().await
    }
    
    pub async fn find_daemon_by_port(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.get().await.find_daemon_by_port(port).await
    }
    
    pub async fn remove_daemon_by_type(&self, service_type: &str, port: u16) -> Result<Option<DaemonMetadata>> {
        self.get().await.remove_daemon_by_type(service_type, port).await
    }
    
    pub async fn close(&self) -> Result<()> {
        self.get().await.close().await
    }
    
    pub async fn health_check(&self) -> Result<bool> {
        self.get().await.health_check().await
    }

    pub async fn debug_database_state(&self) -> Result<String> {
        let mut status = String::new();
        let db_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);
        status.push_str(&format!("Database path: {:?}\n", db_path));
        
        if db_path.exists() {
            status.push_str("Database directory exists\n");
        } else {
            status.push_str("Database directory does not exist\n");
        }
        
        status.push_str("Improved non-blocking registry active\n");
        Ok(status)
    }
}

pub static GLOBAL_DAEMON_REGISTRY: DaemonRegistryWrapper = DaemonRegistryWrapper::new();

pub async fn emergency_cleanup_daemon_registry() -> Result<()> {
    let db_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);
    emergency_cleanup_daemon_registry_internal(&db_path).await
}

async fn emergency_cleanup_daemon_registry_internal(db_path: &PathBuf) -> Result<()> {
    info!("Starting emergency cleanup of daemon registry at {:?}", db_path);

    if db_path.exists() {
        let db_path_clone = db_path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            std::fs::remove_dir_all(&db_path_clone)?;
            info!("Removed database directory {:?}", db_path_clone);
            std::fs::create_dir_all(&db_path_clone)?;
            let metadata = std::fs::metadata(&db_path_clone)?;
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&db_path_clone, perms)?;
            info!("Recreated clean database directory at {:?}", db_path_clone);
            Ok(())
        }).await??;
    }
    info!("Finished emergency cleanup of daemon registry");
    Ok(())
}


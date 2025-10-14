use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore, OnceCell, Mutex as TokioMutex };
use tokio::time::{Instant, Duration as TokioDuration};
use bincode::{config, encode_to_vec, decode_from_slice};
use serde::{Serialize, Deserialize};
use log::{info, warn, error, debug};
use anyhow::Result;
use sled::{Db as SledDB, IVec, Config};
use sysinfo::{Pid, System, ProcessRefreshKind};
use rocksdb::{DB as RocksDB, ColumnFamily as RocksDBColumnFamily};
use std::os::unix::fs::PermissionsExt;
use tokio::fs;
use std::fs as std_fs;
use std::io;

use crate::config::{ StorageConfig, RocksDBWithPath };
use crate::daemon_config::{
    DAEMON_PID_FILE_NAME_PREFIX,
    REST_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_NAME_PREFIX,
};

pub type RocksDBDaemonInstanceType = TokioMutex<RocksDBWithPath>;
// Serializable version of DBDaemonMetadata without DB instances
#[derive(Serialize, Deserialize, Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct SerializableDBDaemonMetadata {
    pub port: u16,
    pub pid: u32,
    pub ip_address: String,
    pub data_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub engine_type: Option<String>,
    pub last_seen_nanos: i64,
}

impl From<DBDaemonMetadata> for SerializableDBDaemonMetadata {
    fn from(metadata: DBDaemonMetadata) -> Self {
        SerializableDBDaemonMetadata {
            port: metadata.port,
            pid: metadata.pid,
            ip_address: metadata.ip_address,
            data_dir: metadata.data_dir,
            config_path: metadata.config_path,
            engine_type: metadata.engine_type,
            last_seen_nanos: metadata.last_seen_nanos,
        }
    }
}

impl From<SerializableDBDaemonMetadata> for DBDaemonMetadata {
    fn from(ser: SerializableDBDaemonMetadata) -> Self {
        DBDaemonMetadata {
            port: ser.port,
            pid: ser.pid,
            ip_address: ser.ip_address,
            data_dir: ser.data_dir,
            config_path: ser.config_path,
            engine_type: ser.engine_type,
            last_seen_nanos: ser.last_seen_nanos,
            sled_db_instance: None,
            rocksdb_db_instance: None,
            sled_vertices: None,
            sled_edges: None,
            sled_kv_pairs: None,
            rocksdb_vertices: None,
            rocksdb_edges: None,
            rocksdb_kv_pairs: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DBDaemonMetadata {
    pub port: u16,
    pub pid: u32,
    pub ip_address: String,
    pub data_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub engine_type: Option<String>,
    pub last_seen_nanos: i64,
    #[serde(skip)]
    pub sled_db_instance: Option<Arc<SledDB>>,
    #[serde(skip)]
    pub rocksdb_db_instance: Option<Arc<RocksDB>>,
    #[serde(skip)]
    pub sled_vertices: Option<sled::Tree>,
    #[serde(skip)]
    pub sled_edges: Option<sled::Tree>,
    #[serde(skip)]
    pub sled_kv_pairs: Option<sled::Tree>,
    #[serde(skip)]
    pub rocksdb_vertices: Option<RocksDBColumnFamily>,
    #[serde(skip)]
    pub rocksdb_edges: Option<RocksDBColumnFamily>,
    #[serde(skip)]
    pub rocksdb_kv_pairs: Option<RocksDBColumnFamily>,
}

impl std::fmt::Debug for DBDaemonMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DBDaemonMetadata")
            .field("port", &self.port)
            .field("pid", &self.pid)
            .field("ip_address", &self.ip_address)
            .field("data_dir", &self.data_dir)
            .field("config_path", &self.config_path)
            .field("engine_type", &self.engine_type)
            .field("last_seen_nanos", &self.last_seen_nanos)
            .field("sled_db_instance", &self.sled_db_instance.is_some())
            .field("rocksdb_db_instance", &self.rocksdb_db_instance.is_some())
            .field("sled_vertices", &self.sled_vertices.is_some())
            .field("sled_edges", &self.sled_edges.is_some())
            .field("sled_kv_pairs", &self.sled_kv_pairs.is_some())
            .field("rocksdb_vertices", &self.rocksdb_vertices.is_some())
            .field("rocksdb_edges", &self.rocksdb_edges.is_some())
            .field("rocksdb_kv_pairs", &self.rocksdb_kv_pairs.is_some())
            .finish()
    }
}

impl Clone for DBDaemonMetadata {
    fn clone(&self) -> Self {
        DBDaemonMetadata {
            port: self.port,
            pid: self.pid,
            ip_address: self.ip_address.clone(),
            data_dir: self.data_dir.clone(),
            config_path: self.config_path.clone(),
            engine_type: self.engine_type.clone(),
            last_seen_nanos: self.last_seen_nanos,
            sled_db_instance: self.sled_db_instance.clone(),
            rocksdb_db_instance: self.rocksdb_db_instance.clone(),
            sled_vertices: self.sled_vertices.clone(),
            sled_edges: self.sled_edges.clone(),
            sled_kv_pairs: self.sled_kv_pairs.clone(),
            rocksdb_vertices: None,
            rocksdb_edges: None,
            rocksdb_kv_pairs: None,
        }
    }
}

impl DBDaemonMetadata {
    pub fn merge_non_empty(&mut self, update: &DBDaemonMetadata) {
        if update.port != 0 {
            self.port = update.port;
        }
        if update.pid != 0 {
            self.pid = update.pid;
        }
        if !update.ip_address.is_empty() {
            self.ip_address = update.ip_address.clone();
        }
        if update.data_dir.is_some() {
            self.data_dir = update.data_dir.clone();
        }
        if update.config_path.is_some() {
            self.config_path = update.config_path.clone();
        }
        if update.engine_type.is_some() {
            self.engine_type = update.engine_type.clone();
        }
        if update.last_seen_nanos != 0 {
            self.last_seen_nanos = update.last_seen_nanos;
        }
    }
}

#[derive(Clone)]
struct ImprovedSledPool {
    db: Arc<SledDB>,
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
        })
        .await??;

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
            db.insert(key, value)?;
            Ok(())
        })
        .await?
    }

    async fn get(&self, key: &[u8]) -> Result<Option<IVec>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || -> Result<Option<IVec>> {
            db.get(key).map_err(|e| anyhow::anyhow!("Failed to get from sled: {}", e))
        })
        .await?
    }

    async fn remove(&self, key: &[u8]) -> Result<Option<IVec>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || -> Result<Option<IVec>> {
            db.remove(key).map_err(|e| anyhow::anyhow!("Failed to remove from sled: {}", e))
        })
        .await?
    }

    async fn iter_all(&self) -> Result<Vec<(IVec, IVec)>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> Result<Vec<(IVec, IVec)>> {
            let items: Vec<_> = db
                .iter()
                .map(|result| result.map_err(|e| anyhow::anyhow!("Failed to iterate sled: {}", e)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(items)
        })
        .await?
    }
}

#[derive(Debug)]
struct RegistryConfig {
    is_fallback_mode: bool,
    fallback_file: PathBuf,
    db_path: PathBuf,
    max_concurrent_ops: usize,
}

pub struct DBDaemonRegistry {
    memory_store: Arc<RwLock<HashMap<u16, DBDaemonMetadata>>>,
    storage: Arc<RwLock<Option<ImprovedSledPool>>>,
    config: Arc<RegistryConfig>,
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    stale_threshold_nanos: i64,
}

impl DBDaemonRegistry {
    pub async fn new(storage_config: Arc<StorageConfig>) -> Result<Self> {
        let config = Arc::new(RegistryConfig {
            is_fallback_mode: Self::should_use_fallback_mode(),
            fallback_file: Self::get_fallback_file_path(),
            db_path: Self::get_db_path(),
            max_concurrent_ops: 10,
        });

        let registry = DBDaemonRegistry {
            memory_store: Arc::new(RwLock::new(HashMap::new())),
            storage: Arc::new(RwLock::new(None)),
            config,
            background_tasks: Arc::new(RwLock::new(Vec::new())),
            stale_threshold_nanos: 300_000_000_000i64,
        };

        if !registry.config.is_fallback_mode {
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
            .join("db_daemon_registry_fallback.json")
    }

    fn get_db_path() -> PathBuf {
        dirs::home_dir()
            .map(|home| home.join(".graphdb").join("db_daemon_registry_db"))
            .unwrap_or_else(|| PathBuf::from("/tmp/graphdb_db_registry"))
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

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    async fn load_initial_data(&self) -> Result<()> {
        if let Ok(data) = self.load_from_fallback().await {
            let mut memory = self.memory_store.write().await;
            for metadata in data {
                if Self::is_pid_running(metadata.pid).await.unwrap_or(false) {
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
        let memory_store = self.memory_store.clone();
        let fallback_file = self.config.fallback_file.clone();

        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(TokioDuration::from_millis(100)).await;

                let memory = memory_store.read().await;
                let all_metadata: Vec<_> = memory.values().cloned().collect();
                drop(memory);

                let storage_guard = storage.read().await;
                if let Some(pool) = &*storage_guard {
                    info!("Performing background storage sync");
                    for metadata in &all_metadata {
                        let key = metadata.port.to_string().into_bytes();
                        let serializable = SerializableDBDaemonMetadata::from(metadata.clone());
                        match encode_to_vec(&serializable, config::standard()) {
                            Ok(encoded) => {
                                if let Err(e) = pool.insert(&key, &encoded).await {
                                    warn!("Failed to sync daemon to sled for port {}: {}", metadata.port, e);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to encode metadata for port {}: {}", metadata.port, e);
                            }
                        }
                    }
                }
                drop(storage_guard);

                let _ = Self::save_fallback_file(&fallback_file, &all_metadata).await;

                tokio::time::sleep(TokioDuration::from_secs(5)).await;
            }
        });

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    async fn validate_process_fast(&self, pid: u32, _port: u16) -> Result<bool> {
        Self::is_pid_running(pid).await
    }

    pub async fn is_pid_running(pid: u32) -> Result<bool> {
        tokio::task::spawn_blocking(move || {
            let mut sys = System::new();
            let sysinfo_pid = Pid::from_u32(pid);
            sys.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[sysinfo_pid]),
                false,
                ProcessRefreshKind::everything(),
            );
            Ok(sys.process(sysinfo_pid).is_some())
        })
        .await?
    }

    pub async fn clean_stale_db_daemons(&self) -> Result<()> {
        let now_nanos = Instant::now().elapsed().as_nanos() as i64;
        let stale_limit = now_nanos - self.stale_threshold_nanos;

        let mut memory = self.memory_store.write().await;
        let storage_guard = self.storage.read().await;

        let mut ports_to_remove = Vec::new();

        for (port, metadata) in memory.iter() {
            let is_stale_time = metadata.last_seen_nanos < stale_limit;
            let is_pid_running = Self::is_pid_running(metadata.pid).await.unwrap_or(false);

            if is_stale_time || !is_pid_running {
                ports_to_remove.push(*port);
            }
        }

        let mut removed_count = 0;
        for port in ports_to_remove {
            if let Some(metadata) = memory.remove(&port) {
                self.cleanup_daemon_background(metadata).await;
                info!("Removed stale DB daemon entry for port {}", port);
                removed_count += 1;
            }
        }

        if removed_count > 0 {
            info!("Cleared {} stale DB daemons from registry", removed_count);
        } else {
            info!("No stale DB daemons found to clear");
        }

        Ok(())
    }

    async fn cleanup_daemon_background(&self, metadata: DBDaemonMetadata) {
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
                format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, metadata.port),
                format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, metadata.port),
                format!("/tmp/{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, metadata.port),
            ];

            for pid_file in pid_files {
                let _ = fs::remove_file(&pid_file).await;
            }
        });

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    async fn terminate_process(pid: u32) -> Result<()> {
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut system = System::new();
            let sysinfo_pid = Pid::from_u32(pid);
            system.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::All,
                false,
                ProcessRefreshKind::everything(),
            );
            if let Some(process) = system.process(sysinfo_pid) {
                let _ = process.kill();
            }
            Ok(())
        })
        .await?
    }

    pub async fn register_db_daemon(&self, metadata: DBDaemonMetadata) -> Result<()> {
        let is_valid = self.validate_process_fast(metadata.pid, metadata.port).await?;
        if !is_valid {
            return Err(anyhow::anyhow!("Process validation failed for PID {}", metadata.pid));
        }

        self.clean_stale_db_daemons().await?;

        let mut memory = self.memory_store.write().await;
        memory.remove(&metadata.port); // Clear existing entry
        memory.insert(metadata.port, metadata.clone());
        drop(memory);

        let storage = self.storage.clone();
        let metadata_clone = metadata.clone();

        let task = tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                let key = metadata_clone.port.to_string().into_bytes();
                let serializable = SerializableDBDaemonMetadata::from(metadata_clone);
                let encoded = encode_to_vec(&serializable, config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to encode metadata for port {}: {}", serializable.port, e))?;
                if let Err(e) = pool.insert(&key, &encoded).await {
                    warn!("Failed to insert into sled for port {}: {}", serializable.port, e);
                }
            }
            Ok::<_, anyhow::Error>(())
        });
        task.await??;

        let memory = self.memory_store.read().await;
        let all_metadata: Vec<_> = memory.values().cloned().collect();
        drop(memory);
        Self::save_fallback_file(&self.config.fallback_file, &all_metadata).await?;

        info!("Registered DB daemon: {:?} on port {}", metadata.engine_type, metadata.port);
        Ok(())
    }

    pub async fn update_db_daemon_metadata(&self, update: DBDaemonMetadata) -> Result<Option<DBDaemonMetadata>> {
        let port = update.port;
        let mut memory = self.memory_store.write().await;

        if let Some(existing_metadata) = memory.get_mut(&port) {
            let original = existing_metadata.clone();
            existing_metadata.merge_non_empty(&update);
            let updated_metadata = existing_metadata.clone();
            drop(memory);

            let storage = self.storage.clone();
            let metadata_clone = updated_metadata.clone();

            let task = tokio::spawn(async move {
                let storage_guard = storage.read().await;
                if let Some(pool) = &*storage_guard {
                    let key = metadata_clone.port.to_string().into_bytes();
                    let serializable = SerializableDBDaemonMetadata::from(metadata_clone);
                    let encoded = encode_to_vec(&serializable, config::standard())
                        .map_err(|e| anyhow::anyhow!("Failed to encode updated metadata for port {}: {}", serializable.port, e))?;
                    if let Err(e) = pool.insert(&key, &encoded).await {
                        warn!("Failed to update sled for port {}: {}", serializable.port, e);
                    }
                }
                Ok::<_, anyhow::Error>(())
            });
            let _ = task.await;

            let memory = self.memory_store.read().await;
            let all_metadata: Vec<_> = memory.values().cloned().collect();
            drop(memory);
            let _ = Self::save_fallback_file(&self.config.fallback_file, &all_metadata).await;

            info!("Updated DB daemon metadata for port {}", port);
            Ok(Some(updated_metadata))
        } else {
            drop(memory);
            warn!("Attempted to update non-existent DB daemon on port {}", port);
            Ok(None)
        }
    }

    pub async fn get_all_db_daemon_metadata(&self) -> Result<Vec<DBDaemonMetadata>> {
        self.clean_stale_db_daemons().await?;

        let storage_guard = self.storage.read().await;
        if let Some(pool) = &*storage_guard {
            let mut memory = self.memory_store.write().await;
            memory.clear();

            let all_sled_entries = pool.iter_all().await?;
            for (key, encoded_value) in all_sled_entries {
                match decode_from_slice::<SerializableDBDaemonMetadata, _>(&encoded_value, config::standard()) {
                    Ok((ser, _)) => {
                        let metadata = DBDaemonMetadata::from(ser);
                        memory.insert(metadata.port, metadata);
                    }
                    Err(e) => {
                        error!("Failed to decode SerializableDBDaemonMetadata from Sled entry: {}", e);
                        continue;
                    }
                }
            }
        }

        let memory = self.memory_store.read().await;
        let all_metadata: Vec<_> = memory.values().cloned().collect();
        drop(memory);

        let valid_metadata = tokio::spawn(async move {
            let mut valid = Vec::new();
            for metadata in all_metadata {
                if Self::is_pid_running(metadata.pid).await.unwrap_or(false) {
                    valid.push(metadata);
                }
            }
            valid
        })
        .await?;

        Ok(valid_metadata)
    }

    pub async fn get_db_daemon_metadata_by_port(&self, port: u16) -> Result<Option<DBDaemonMetadata>> {
        self.clean_stale_db_daemons().await?;

        let memory = self.memory_store.read().await;
        if let Some(metadata) = memory.get(&port) {
            let metadata_clone = metadata.clone();
            drop(memory);

            if Self::is_pid_running(metadata_clone.pid).await.unwrap_or(false) {
                return Ok(Some(metadata_clone));
            }
        }

        self.discover_db_daemon_from_sled(port).await
    }

    pub async fn get_db_daemon_metadata_by_pid(&self, pid: u32) -> Result<Option<DBDaemonMetadata>> {
        self.clean_stale_db_daemons().await?;

        let memory = self.memory_store.read().await;
        let result = memory.values().find(|metadata| metadata.pid == pid).cloned();
        Ok(result)
    }

    pub async fn get_db_daemon_metadata_by_engine_type(&self, engine_type: &str) -> Result<Vec<DBDaemonMetadata>> {
        self.clean_stale_db_daemons().await?;

        let engine_type_lower = engine_type.to_lowercase();
        let memory = self.memory_store.read().await;

        let results: Vec<DBDaemonMetadata> = memory
            .values()
            .filter(|metadata| {
                metadata
                    .engine_type
                    .as_ref()
                    .map(|et| et.to_lowercase() == engine_type_lower)
                    .unwrap_or(false)
            })
            .cloned()
            .collect();

        Ok(results)
    }

    async fn discover_db_daemon_from_sled(&self, port: u16) -> Result<Option<DBDaemonMetadata>> {
        debug!("Attempting to discover DB daemon from persistent store on port {}", port);

        let storage_guard = self.storage.read().await;
        if let Some(pool) = &*storage_guard {
            let key = port.to_string().into_bytes();
            if let Some(encoded_value) = pool.get(&key).await? {
                match decode_from_slice::<SerializableDBDaemonMetadata, _>(&encoded_value, config::standard()) {
                    Ok((ser, _)) => {
                        let metadata = DBDaemonMetadata::from(ser);
                        if Self::is_pid_running(metadata.pid).await.unwrap_or(false) {
                            let mut memory = self.memory_store.write().await;
                            memory.insert(port, metadata.clone());
                            return Ok(Some(metadata));
                        } else {
                            let _ = pool.remove(&key).await;
                            warn!("Discovered stale DB daemon in Sled on port {} (PID {}), removed.", port, metadata.pid);
                        }
                    }
                    Err(e) => {
                        error!("Failed to decode discovered SerializableDBDaemonMetadata for port {}: {}", port, e);
                    }
                }
            }
        }

        Ok(None)
    }

    pub async fn register_daemon(&self, metadata: DBDaemonMetadata) -> Result<()> {
        self.register_db_daemon(metadata).await
    }

    pub async fn update_daemon_metadata(&self, update: DBDaemonMetadata) -> Result<Option<DBDaemonMetadata>> {
        self.update_db_daemon_metadata(update).await
    }

    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DBDaemonMetadata>> {
        self.get_db_daemon_metadata_by_port(port).await
    }

    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        let metadata = {
            let mut memory = self.memory_store.write().await;
            memory.remove(&port)
        };

        if let Some(metadata) = metadata {
            self.cleanup_daemon_background(metadata).await;
            info!("Unregistered DB daemon on port {}", port);
        }

        Ok(())
    }

    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DBDaemonMetadata>> {
        self.get_all_db_daemon_metadata().await
    }

    pub async fn clear_stale_daemons(&self) -> Result<()> {
        self.clean_stale_db_daemons().await
    }

    pub async fn clear_all_daemons(&self) -> Result<()> {
        let metadata_list = {
            let mut memory = self.memory_store.write().await;
            let list: Vec<_> = memory.values().cloned().collect();
            memory.clear();
            list
        };

        self.cleanup_all_daemons_background(metadata_list).await;

        info!("Cleared all DB daemons from registry");
        Ok(())
    }

    async fn cleanup_all_daemons_background(&self, metadata_list: Vec<DBDaemonMetadata>) {
        let storage = self.storage.clone();
        let fallback_file = self.config.fallback_file.clone();

        let task = tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                debug!("Clearing storage backend");
                match pool.iter_all().await {
                    Ok(entries) => {
                        for (key, _) in entries {
                            let _ = pool.remove(&key).await;
                        }
                    }
                    Err(e) => warn!("Failed to iterate and clear sled database: {}", e),
                }
            }
            drop(storage_guard);

            for metadata in metadata_list {
                let pid_files = vec![
                    format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, metadata.port),
                    format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, metadata.port),
                    format!("/tmp/{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, metadata.port),
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

    pub async fn find_daemon_by_port(&self, port: u16) -> Result<Option<DBDaemonMetadata>> {
        self.get_db_daemon_metadata_by_port(port).await
    }

    pub async fn remove_daemon_by_type(&self, service_type: &str, port: u16) -> Result<Option<DBDaemonMetadata>> {
        let metadata = {
            let mut memory = self.memory_store.write().await;
            if let Some(metadata) = memory.get(&port) {
                if metadata
                    .engine_type
                    .as_deref()
                    .map(|et| et.eq_ignore_ascii_case(service_type))
                    .unwrap_or(false)
                {
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
            info!("Removed {} DB daemon on port {}", service_type, port);
        }

        Ok(metadata)
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

        let mut storage = self.storage.write().await;
        if let Some(pool) = storage.take() {
            info!("Attempting to flush and close Sled DB for registry.");
            let db = pool.db.clone();
            tokio::task::spawn_blocking(move || db.flush()).await??;
            info!("Sled DB for registry closed.");
        }

        info!("DB Registry closed gracefully");
        Ok(())
    }

    pub async fn health_check(&self) -> Result<bool> {
        self.clean_stale_db_daemons().await?;

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

    async fn load_from_fallback(&self) -> Result<Vec<DBDaemonMetadata>> {
        if !self.config.fallback_file.exists() {
            return Ok(Vec::new());
        }

        let data = fs::read_to_string(&self.config.fallback_file).await?;
        let metadata_list: Vec<DBDaemonMetadata> = serde_json::from_str(&data)?;
        Ok(metadata_list)
    }

    async fn save_fallback_file(file_path: &PathBuf, metadata_list: &[DBDaemonMetadata]) -> Result<()> {
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

    pub async fn debug_database_state(&self) -> Result<String> {
        let memory = self.memory_store.read().await;
        let storage_guard = self.storage.read().await;

        let mut debug_info = format!("--- DBDaemonRegistry State ---\n");
        debug_info.push_str(&format!("In-Memory Daemons: {} active\n", memory.len()));

        for (port, metadata) in memory.iter() {
            debug_info.push_str(&format!(
                "  - Port {}: PID={}, Engine={:?}, Last Seen={}\n",
                port,
                metadata.pid,
                metadata.engine_type.as_deref().unwrap_or("N/A"),
                metadata.last_seen_nanos
            ));
        }

        if let Some(pool) = &*storage_guard {
            let sled_count = pool.db.len();
            debug_info.push_str(&format!("Persistent Sled Entries: {} total\n", sled_count));

            debug_info.push_str("Persistent Sled Ports (first 5): ");
            let mut keys_listed = 0;
            let mut key_list = String::new();
            for result in pool.db.iter().keys() {
                if keys_listed >= 5 {
                    key_list.push_str("... (truncated)");
                    break;
                }
                match result {
                    Ok(key) => {
                        if let Ok(port_str) = std::str::from_utf8(&key) {
                            key_list.push_str(&format!("{}, ", port_str));
                        } else {
                            key_list.push_str(" (non-UTF8 key), ");
                        }
                    }
                    Err(_) => key_list.push_str(" (key error), "),
                }
                keys_listed += 1;
            }

            if keys_listed == 0 {
                debug_info.push_str("(none)\n");
            } else {
                debug_info.push_str(&key_list.trim_end_matches(", "));
                debug_info.push('\n');
            }
        } else {
            debug_info.push_str("Persistent Storage: NOT INITIALIZED\n");
        }

        Ok(debug_info)
    }
}

pub struct DBDaemonRegistryWrapper {
    cell: OnceCell<Arc<DBDaemonRegistry>>,
}

impl DBDaemonRegistryWrapper {
    pub const fn new() -> Self {
        DBDaemonRegistryWrapper {
            cell: OnceCell::const_new(),
        }
    }

    pub async fn get_or_init_instance(&self, config: Arc<StorageConfig>) -> Result<Arc<DBDaemonRegistry>> {
        let registry = self
            .cell
            .get_or_init(|| async {
                info!("Initializing GLOBAL_DB_DAEMON_REGISTRY...");
                match DBDaemonRegistry::new(config.clone()).await {
                    Ok(registry) => Arc::new(registry),
                    Err(e) => {
                        error!("Failed to initialize DBDaemonRegistry: {}", e);
                        panic!("Cannot initialize DBDaemonRegistry: {}", e)
                    }
                }
            })
            .await;
        Ok(Arc::clone(registry))
    }

    pub async fn get_registry(&self) -> Arc<DBDaemonRegistry> {
        Arc::clone(
            self.cell
                .get_or_init(|| async {
                    warn!("get_registry called without prior get_or_init_instance - using default config");
                    let default_config = Arc::new(StorageConfig::default());
                    match DBDaemonRegistry::new(default_config).await {
                        Ok(registry) => Arc::new(registry),
                        Err(e) => {
                            error!("Failed to initialize DBDaemonRegistry: {}", e);
                            panic!("Cannot initialize DBDaemonRegistry: {}", e)
                        }
                    }
                })
                .await,
        )
    }

    pub async fn get(&self) -> Arc<DBDaemonRegistry> {
        Arc::clone(
            self.cell
                .get_or_init(|| async {
                    warn!("get_registry called without prior get_or_init_instance - using default config");
                    let default_config = Arc::new(StorageConfig::default());
                    match DBDaemonRegistry::new(default_config).await {
                        Ok(registry) => Arc::new(registry),
                        Err(e) => {
                            error!("Failed to initialize DBDaemonRegistry: {}", e);
                            panic!("Cannot initialize DBDaemonRegistry: {}", e)
                        }
                    }
                })
                .await,
        )
    }

    pub async fn register_daemon(&self, metadata: DBDaemonMetadata) -> Result<()> {
        self.get_registry().await.register_daemon(metadata).await
    }

    pub async fn update_daemon_metadata(&self, update: DBDaemonMetadata) -> Result<Option<DBDaemonMetadata>> {
        self.get_registry().await.update_daemon_metadata(update).await
    }

    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DBDaemonMetadata>> {
        self.get_registry().await.get_daemon_metadata(port).await
    }

    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        self.get_registry().await.unregister_daemon(port).await
    }

    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DBDaemonMetadata>> {
        self.get_registry().await.get_all_daemon_metadata().await
    }

    pub async fn clear_stale_daemons(&self) -> Result<()> {
        self.get_registry().await.clear_stale_daemons().await
    }

    pub async fn clear_all_daemons(&self) -> Result<()> {
        self.get_registry().await.clear_all_daemons().await
    }

    pub async fn find_daemon_by_port(&self, port: u16) -> Result<Option<DBDaemonMetadata>> {
        self.get_registry().await.find_daemon_by_port(port).await
    }

    pub async fn remove_daemon_by_type(&self, service_type: &str, port: u16) -> Result<Option<DBDaemonMetadata>> {
        self.get_registry().await.remove_daemon_by_type(service_type, port).await
    }

    pub async fn close(&self) -> Result<()> {
        self.get_registry().await.close().await
    }

    pub async fn health_check(&self) -> Result<bool> {
        self.get_registry().await.health_check().await
    }

    pub async fn debug_database_state(&self) -> Result<String> {
        self.get_registry().await.debug_database_state().await
    }
}

pub static GLOBAL_DB_DAEMON_REGISTRY: DBDaemonRegistryWrapper = DBDaemonRegistryWrapper::new();

use anyhow::{anyhow, Context, Result};
use serde::{Serialize, Deserialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use tokio::sync::Mutex as TokioMutex;
use std::collections::HashMap;
use log::{info, warn, error, debug};
use chrono::{DateTime, Utc};
use sysinfo::{Pid, System};
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode};
use tokio::process::Command;
use std::time::Duration;
use tokio::fs::{self as async_fs, File};
use std::os::unix::fs::PermissionsExt;
use sled::{Db, IVec, Config};
use glob::glob;
use std::ffi::{OsStr, OsString};
use std::borrow::Cow;

use crate::daemon_config::{
    DAEMON_REGISTRY_DB_PATH,
    DAEMON_PID_FILE_NAME_PREFIX,
    REST_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_NAME_PREFIX,
};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Encode, Decode)]
pub struct DaemonMetadata {
    pub service_type: String, // "main", "rest", "storage"
    pub port: u16,
    pub pid: u32,
    pub ip_address: String, // For now, always "127.0.0.1"
    pub data_dir: Option<PathBuf>,
    pub config_path: Option<PathBuf>,
    pub engine_type: Option<String>, // e.g., "sled", "rocksdb" for storage daemons
    pub last_seen_nanos: i64, // Timestamp in nanoseconds since epoch
}

#[derive(Clone)]
pub struct DaemonRegistry {
    db: Arc<TokioMutex<Option<Db>>>,
    memory_store: Arc<TokioMutex<HashMap<u16, DaemonMetadata>>>,
    is_fallback_mode: bool,
    fallback_file: PathBuf,
    fallback_loaded: Arc<TokioMutex<bool>>,
}

impl DaemonRegistry {
    pub fn new() -> Result<Self> {
        let default_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);
        let home_path = dirs::home_dir()
            .map(|home| home.join(".graphdb").join("daemon_registry_db"))
            .unwrap_or(default_path.clone());

        match Self::new_with_path(home_path.clone()) {
            Ok(registry) => {
                info!("Successfully initialized registry with home path {:?}", home_path);
                Ok(registry)
            }
            Err(e) => {
                warn!("Failed to initialize with home path {:?}: {}. Trying default path {:?}", home_path, e, default_path);
                Self::new_with_path(default_path)
            }
        }
    }

    pub fn new_with_path(db_path: PathBuf) -> Result<Self> {
        let memory_store = Arc::new(TokioMutex::new(HashMap::new()));
        let fallback_file = db_path
            .parent()
            .unwrap_or(&Path::new("/tmp"))
            .join("daemon_registry_fallback.json");

        // Skip sled for CLI operations requiring fast response
        if std::env::args().any(|arg| arg == "status" || arg == "stop" || arg == "list") {
            info!("CLI operation detected, using in-memory fallback mode with file {:?}", fallback_file);
            return Ok(DaemonRegistry {
                db: Arc::new(TokioMutex::new(None)),
                memory_store,
                is_fallback_mode: true,
                fallback_file,
                fallback_loaded: Arc::new(TokioMutex::new(false)),
            });
        }

        // Ensure directory is created with correct permissions
        if let Some(parent) = db_path.parent() {
            debug!("Attempting to create parent directory {:?}", parent);
            if let Err(e) = std::fs::create_dir_all(parent) {
                error!("Failed to create parent directory {:?}: {}", parent, e);
                return Ok(DaemonRegistry {
                    db: Arc::new(TokioMutex::new(None)),
                    memory_store,
                    is_fallback_mode: true,
                    fallback_file,
                    fallback_loaded: Arc::new(TokioMutex::new(false)),
                });
            }
            if let Ok(metadata) = std::fs::metadata(parent) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o755);
                if let Err(e) = std::fs::set_permissions(parent, perms) {
                    warn!("Failed to set permissions for {:?}: {}", parent, e);
                }
            }
            info!("Created parent directory {:?}", parent);
        }

        // Create database directory
        debug!("Attempting to create database directory {:?}", db_path);
        if let Err(e) = std::fs::create_dir_all(&db_path) {
            error!("Failed to create database directory {:?}: {}", db_path, e);
            return Ok(DaemonRegistry {
                db: Arc::new(TokioMutex::new(None)),
                memory_store,
                is_fallback_mode: true,
                fallback_file,
                fallback_loaded: Arc::new(TokioMutex::new(false)),
            });
        }
        if let Ok(metadata) = std::fs::metadata(&db_path) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            if let Err(e) = std::fs::set_permissions(&db_path, perms) {
                warn!("Failed to set permissions for {:?}: {}", db_path, e);
            }
        }
        info!("Created database directory {:?}", db_path);

        // Clean up stale lock file
        let lock_path = db_path.join("db.lck");
        if let Err(e) = Self::cleanup_stale_locks(&db_path, &lock_path) {
            warn!("Failed to cleanup stale locks: {}", e);
        }

        // Try opening database with retries
        let max_retries = 3;
        for attempt in 1..=max_retries {
            match Self::try_open_database(&db_path) {
                Ok(db) => {
                    return Ok(DaemonRegistry {
                        db: Arc::new(TokioMutex::new(Some(db))),
                        memory_store,
                        is_fallback_mode: false,
                        fallback_file,
                        fallback_loaded: Arc::new(TokioMutex::new(false)),
                    });
                }
                Err(e) => {
                    warn!("Failed to open database at {:?} (attempt {}/{}): {}", db_path, attempt, max_retries, e);
                    if attempt < max_retries {
                        std::thread::sleep(Duration::from_millis(200));
                    }
                }
            }
        }

        error!("Could not open sled database, using in-memory fallback with file {:?}", fallback_file);
        Ok(DaemonRegistry {
            db: Arc::new(TokioMutex::new(None)),
            memory_store,
            is_fallback_mode: true,
            fallback_file,
            fallback_loaded: Arc::new(TokioMutex::new(false)),
        })
    }

    fn cleanup_stale_locks(_db_path: &PathBuf, lock_path: &PathBuf) -> Result<()> {
        if !lock_path.exists() {
            return Ok(());
        }

        let mut system = System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
        let has_graphdb_processes = system
            .processes()
            .values()
            .any(|proc| proc.name().to_string_lossy().contains("graphdb"));

        if !has_graphdb_processes {
            info!("No GraphDB processes found, removing stale lock file {:?}", lock_path);
            std::fs::remove_file(lock_path)?;
        } else {
            debug!("GraphDB processes found, keeping lock file {:?}", lock_path);
        }
        Ok(())
    }

    fn try_open_database(db_path: &PathBuf) -> Result<Db> {
        Config::new()
            .path(db_path)
            .cache_capacity(8 * 1024 * 1024)
            .flush_every_ms(Some(5000))
            .use_compression(true)
            .open()
            .context(format!("Failed to open sled database at {:?}", db_path))
    }

    async fn save_fallback_file(&self, metadata_list: &[DaemonMetadata]) -> Result<()> {
        let data = serde_json::to_string(metadata_list)
            .map_err(|e| anyhow!("Failed to serialize fallback file: {}", e))?;
        let temp_file = self.fallback_file.with_extension("json.tmp");
        async_fs::write(&temp_file, &data).await
            .map_err(|e| anyhow!("Failed to write temp fallback file {:?}: {}", temp_file, e))?;
        async_fs::rename(&temp_file, &self.fallback_file).await
            .map_err(|e| anyhow!("Failed to rename temp file to {:?}: {}", self.fallback_file, e))?;
        if let Ok(metadata) = async_fs::metadata(&self.fallback_file).await {
            let mut perms = metadata.permissions();
            perms.set_mode(0o644);
            let _ = async_fs::set_permissions(&self.fallback_file, perms).await;
        }
        info!("Saved {} daemons to fallback file {:?}", metadata_list.len(), self.fallback_file);
        Ok(())
    }

    fn determine_service_type(proc_name: &str, cmdline: &str) -> Option<&'static str> {
        if cmdline.contains("--internal-main-run") || proc_name.contains("graphdb-c") && !cmdline.contains("rest") && !cmdline.contains("storage") {
            Some("main")
        } else if cmdline.contains("--internal-rest-run") || proc_name.contains("rest") {
            Some("rest")
        } else if cmdline.contains("--internal-storage-run") || proc_name.contains("storage") {
            Some("storage")
        } else {
            None
        }
    }
}

pub struct AsyncRegistryWrapper {
    registry: DaemonRegistry,
}

impl AsyncRegistryWrapper {
    pub fn new() -> Self {
        AsyncRegistryWrapper {
            registry: DaemonRegistry::new().expect("Failed to initialize DaemonRegistry"),
        }
    }

    pub async fn with_db<R>(&self, f: impl FnOnce(Option<&Db>) -> R) -> R {
        let db_guard = self.registry.db.lock().await;
        f(db_guard.as_ref())
    }

    pub async fn with_memory_store<R>(&self, f: impl FnOnce(&mut HashMap<u16, DaemonMetadata>) -> R) -> R {
        let mut memory_guard = self.registry.memory_store.lock().await;
        f(&mut memory_guard)
    }

    pub async fn with_memory_store_read<R>(&self, f: impl FnOnce(&HashMap<u16, DaemonMetadata>) -> R) -> R {
        let memory_guard = self.registry.memory_store.lock().await;
        f(&memory_guard)
    }

    pub async fn with_fallback_loaded<R>(&self, f: impl FnOnce(&mut bool) -> R) -> R {
        let mut fallback_guard = self.registry.fallback_loaded.lock().await;
        f(&mut fallback_guard)
    }

    pub async fn with_fallback_loaded_read<R>(&self, f: impl FnOnce(bool) -> R) -> R {
        let fallback_guard = self.registry.fallback_loaded.lock().await;
        f(*fallback_guard)
    }

    pub async fn with_memory_and_db<R>(&self, f: impl FnOnce(&mut HashMap<u16, DaemonMetadata>, Option<&Db>) -> R) -> R {
        let mut memory_guard = self.registry.memory_store.lock().await;
        let db_guard = self.registry.db.lock().await;
        f(&mut memory_guard, db_guard.as_ref())
    }

    pub async fn with_all_locks<R>(&self, f: impl FnOnce(&mut HashMap<u16, DaemonMetadata>, Option<&Db>, &mut bool) -> R) -> R {
        let mut memory_guard = self.registry.memory_store.lock().await;
        let db_guard = self.registry.db.lock().await;
        let mut fallback_guard = self.registry.fallback_loaded.lock().await;
        f(&mut memory_guard, db_guard.as_ref(), &mut fallback_guard)
    }

    pub fn is_fallback_mode(&self) -> bool {
        self.registry.is_fallback_mode
    }

    pub fn fallback_file(&self) -> &PathBuf {
        &self.registry.fallback_file
    }

    pub async fn load_fallback_file(&self) -> Result<()> {
        let already_loaded = self.with_fallback_loaded_read(|loaded| loaded).await;
        if already_loaded {
            debug!("Fallback file already loaded for {:?}", self.registry.fallback_file);
            return Ok(());
        }

        if !self.registry.fallback_file.exists() {
            self.with_fallback_loaded(|fallback_loaded| {
                *fallback_loaded = true;
            }).await;
            return Ok(());
        }

        let data = async_fs::read_to_string(&self.registry.fallback_file).await
            .map_err(|e| anyhow!("Failed to read fallback file {:?}: {}", self.registry.fallback_file, e))?;
        let metadata_list: Vec<DaemonMetadata> = serde_json::from_str(&data)
            .map_err(|e| anyhow!("Failed to parse fallback file: {}", e))?;

        let system = System::new_all();
        self.with_memory_store(|memory_store| {
            for metadata in metadata_list {
                if system.process(Pid::from_u32(metadata.pid)).is_some() {
                    memory_store.insert(metadata.port, metadata.clone());
                    info!("Loaded daemon from fallback file: {} on port {}", metadata.service_type, metadata.port);
                }
            }
        }).await;

        self.with_fallback_loaded(|fallback_loaded| {
            *fallback_loaded = true;
        }).await;

        Ok(())
    }

    pub async fn migrate_to_memory_if_needed(&self) -> Result<()> {
        if self.is_fallback_mode() {
            self.load_fallback_file().await?;
            return Ok(());
        }

        let memory_empty = self.with_memory_store_read(|memory_store| memory_store.is_empty()).await;
        if !memory_empty {
            return Ok(());
        }

        self.with_all_locks(|memory_store, db, fallback_loaded| {
            if let Some(db) = db {
                let mut count = 0;
                let mut daemons_from_db = Vec::new(); // Collect daemons here
                for item_result in db.iter() {
                    if let Ok((key, ivec)) = item_result {
                        let key: Vec<u8> = key.to_vec();
                        let ivec: Vec<u8> = ivec.to_vec();
                        // Assuming bincode::decode_from_slice and bincode::config::standard are available
                        match bincode::decode_from_slice::<DaemonMetadata, _>(&ivec, bincode::config::standard()) {
                            Ok((metadata, _)) => {
                                if let Ok(port) = u16::from_str_radix(&String::from_utf8_lossy(&key), 10) {
                                    memory_store.insert(port, metadata.clone()); // Insert into memory_store
                                    daemons_from_db.push(metadata); // Also add to a separate Vec for saving
                                    count += 1;
                                }
                            }
                            Err(e) => warn!("Skipping corrupted entry during migration: {}", e),
                        }
                    }
                }
                if count > 0 {
                    let registry = self.registry.clone();
                    // FIX: Move the owned `daemons_from_db` into the spawned task
                    tokio::spawn(async move {
                        if let Err(e) = registry.save_fallback_file(&daemons_from_db).await {
                            warn!("Failed to save fallback file: {}", e);
                        }
                    });
                }
                *fallback_loaded = true;
                info!("Migrated {} entries to memory store", count);
            }
        }).await;

        Ok(())
    }
    
    pub async fn fallback_discover_daemons(&self) -> Result<Vec<DaemonMetadata>> {
        let mut metadata_list = Vec::new();
        let system = System::new_all();

        // First, check PID files in /tmp for known daemons
        for prefix in [
            DAEMON_PID_FILE_NAME_PREFIX,
            REST_PID_FILE_NAME_PREFIX,
            STORAGE_PID_FILE_NAME_PREFIX,
            "graphdb-daemon-",
            "graphdb-cli-",
        ].iter() {
            let pattern = format!("/tmp/{}*.pid", prefix);
            if let Ok(entries) = glob(&pattern) {
                for entry in entries.flatten() {
                    if let Ok(pid_str) = async_fs::read_to_string(&entry).await {
                        if let Ok(pid) = pid_str.trim().parse::<u32>() {
                            let port_str = entry.file_name()
                                .and_then(|n| n.to_str())
                                .and_then(|n| n.strip_prefix(prefix).and_then(|s| s.strip_suffix(".pid")))
                                .and_then(|s| s.parse::<u16>().ok());
                            if let Some(port) = port_str {
                                if let Some(process) = system.process(Pid::from_u32(pid)) {
                                    let cmdline = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect::<Vec<_>>().join(" ");
                                    if let Some(service_type) = DaemonRegistry::determine_service_type(&process.name().to_string_lossy(), &cmdline) {
                                        let metadata = DaemonMetadata {
                                            service_type: service_type.to_string(),
                                            port,
                                            pid,
                                            ip_address: "127.0.0.1".to_string(),
                                            data_dir: if service_type == "storage" {
                                                Some(PathBuf::from("/opt/graphdb/storage_data"))
                                            } else {
                                                None
                                            },
                                            config_path: None,
                                            engine_type: if service_type == "storage" {
                                                Some("sled".to_string())
                                            } else {
                                                None
                                            },
                                            last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                                        };
                                        metadata_list.push(metadata);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Fallback to lsof for additional discovery
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            Command::new("lsof")
                .args(&["-iTCP", "-sTCP:LISTEN", "-a", "-n", "-P", "-c", "graphdb"])
                .output(),
        ).await;

        if let Ok(Ok(output)) = output {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    if !line.contains("graphdb") {
                        continue;
                    }
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() < 9 {
                        continue;
                    }
                    if let (Ok(pid), Some(port)) = (
                        parts[1].parse::<u32>(),
                        parts[8].split(':').last().and_then(|p| p.parse::<u16>().ok()),
                    ) {
                        // Skip if already found via PID file
                        if metadata_list.iter().any(|m| m.pid == pid || m.port == port) {
                            continue;
                        }
                        let cmdline_output = Command::new("ps")
                            .args(&["-p", &pid.to_string(), "-o", "args="])
                            .output()
                            .await;
                        let cmdline = cmdline_output
                            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
                            .unwrap_or_default();
                        if let Some(service_type) = DaemonRegistry::determine_service_type(&parts[0], &cmdline) {
                            let metadata = DaemonMetadata {
                                service_type: service_type.to_string(),
                                port,
                                pid,
                                ip_address: "127.0.0.1".to_string(),
                                data_dir: if service_type == "storage" {
                                    Some(PathBuf::from("/opt/graphdb/storage_data"))
                                } else {
                                    None
                                },
                                config_path: None,
                                engine_type: if service_type == "storage" {
                                    Some("sled".to_string())
                                } else {
                                    None
                                },
                                last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                            };
                            metadata_list.push(metadata);
                        }
                    }
                }
            }
        } else if let Ok(Err(e)) = output {
            warn!("lsof command failed: {}", e);
        } else {
            warn!("lsof command timed out");
        }

        if self.is_fallback_mode() {
            self.with_memory_store(|memory_store| {
                for metadata in &metadata_list {
                    memory_store.insert(metadata.port, metadata.clone());
                }
            }).await;
            self.registry.save_fallback_file(&metadata_list).await?;
            self.with_fallback_loaded(|fallback_loaded| *fallback_loaded = true).await;
        }

        Ok(metadata_list)
    }

    pub async fn register_daemon(&self, metadata: DaemonMetadata) -> Result<()> {
        let system = System::new_all();
        if system.process(Pid::from_u32(metadata.pid)).is_none() {
            return Err(anyhow!("Process with PID {} is not running", metadata.pid));
        }

        let pid_file = match metadata.service_type.as_str() {
            "main" => format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, metadata.port),
            "rest" => format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, metadata.port),
            "storage" => format!("/tmp/{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, metadata.port),
            _ => format!("/tmp/graphdb-cli-{}.pid", metadata.port),
        };

        if let Err(e) = async_fs::write(&pid_file, metadata.pid.to_string()).await {
            warn!("Failed to create PID file {}: {}", pid_file, e);
        } else {
            if let Ok(metadata_file) = async_fs::metadata(&pid_file).await {
                let mut perms = metadata_file.permissions();
                perms.set_mode(0o644);
                let _ = async_fs::set_permissions(&pid_file, perms).await;
            }
            info!("Created PID file {} with PID {}", pid_file, metadata.pid);
        }

        self.with_memory_and_db(|memory_store, db| {
            memory_store.insert(metadata.port, metadata.clone());
            let metadata_list = memory_store.values().cloned().collect::<Vec<_>>();
            let registry = self.registry.clone();
            tokio::spawn(async move {
                if let Err(e) = registry.save_fallback_file(&metadata_list).await {
                    warn!("Failed to save fallback file: {}", e);
                }
            });
            info!("Registered daemon in memory: {} on port {} with PID {}", metadata.service_type, metadata.port, metadata.pid);

            if let Some(db) = db {
                let key = metadata.port.to_string().into_bytes();
                match encode_to_vec(&metadata, config::standard()) {
                    Ok(encoded) => {
                        if let Err(e) = db.insert(&key, encoded) {
                            warn!("Failed to persist daemon to sled: {}", e);
                        } else {
                            let db_clone = db.clone();
                            tokio::spawn(async move {
                                if let Err(e) = db_clone.flush_async().await {
                                    warn!("Failed to flush registry: {}", e);
                                }
                            });
                            info!("Persisted daemon to sled: {} on port {}", metadata.service_type, metadata.port);
                        }
                    }
                    Err(e) => warn!("Failed to encode daemon metadata: {}", e),
                }
            }
        }).await;

        Ok(())
    }

    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        let metadata = self.with_memory_store(|memory_store| memory_store.remove(&port)).await;

        if let Some(metadata) = &metadata {
            info!("Unregistered daemon from memory: {} on port {} with PID {}", metadata.service_type, port, metadata.pid);
            let pid_files = vec![
                format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, port),
                format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, port),
                format!("/tmp/{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, port),
                format!("/tmp/graphdb-daemon-{}.pid", port),
                format!("/tmp/graphdb-cli-{}.pid", port),
            ];
            for pid_file in pid_files {
                if PathBuf::from(&pid_file).exists() {
                    if let Err(e) = async_fs::remove_file(&pid_file).await {
                        warn!("Failed to remove PID file {}: {}", pid_file, e);
                    } else {
                        info!("Removed PID file {}", pid_file);
                    }
                }
            }
            self.with_memory_store(|memory_store| {
                let metadata_list = memory_store.values().cloned().collect::<Vec<_>>();
                let registry = self.registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = registry.save_fallback_file(&metadata_list).await {
                        warn!("Failed to save fallback file: {}", e);
                    }
                });
            }).await;
        }

        if !self.is_fallback_mode() {
            self.with_db(|db| {
                if let Some(db) = db {
                    let key = port.to_string().into_bytes();
                    match db.remove(&key) {
                        Ok(Some(_)) => {
                            let db_clone = db.clone();
                            tokio::spawn(async move {
                                if let Err(e) = db_clone.flush_async().await {
                                    warn!("Failed to flush registry: {}", e);
                                }
                            });
                            info!("Unregistered daemon from sled on port {}", port);
                        }
                        Ok(None) => debug!("Daemon on port {} was not in sled database", port),
                        Err(e) => warn!("Failed to remove daemon from sled: {}", e),
                    }
                }
            }).await;
        }

        if let Some(metadata) = metadata {
            let system = System::new_all();
            if let Some(process) = system.process(Pid::from_u32(metadata.pid)) {
                if process.kill_with(sysinfo::Signal::Term).unwrap_or(false) {
                    info!("Sent SIGTERM to process {} for port {}", metadata.pid, port);
                } else {
                    warn!("Failed to send SIGTERM to process {} for port {}", metadata.pid, port);
                }
            }
        }

        Ok(())
    }

    pub async fn remove_daemon_by_type(&self, service_type: &str, port: u16) -> Result<Option<DaemonMetadata>> {
        let metadata = self.with_memory_store(|memory_store| {
            if let Some(metadata) = memory_store.get(&port) {
                if metadata.service_type == service_type {
                    memory_store.remove(&port)
                } else {
                    None
                }
            } else {
                None
            }
        }).await;

        if let Some(metadata) = &metadata {
            info!("Unregistered {} daemon from memory on port {} with PID {}", metadata.service_type, port, metadata.pid);
            let pid_files = vec![
                format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, port),
                format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, port),
                format!("/tmp/{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, port),
                format!("/tmp/graphdb-daemon-{}.pid", port),
                format!("/tmp/graphdb-cli-{}.pid", port),
            ];
            for pid_file in pid_files {
                if PathBuf::from(&pid_file).exists() {
                    if let Err(e) = async_fs::remove_file(&pid_file).await {
                        warn!("Failed to remove PID file {}: {}", pid_file, e);
                    } else {
                        info!("Removed PID file {}", pid_file);
                    }
                }
            }
            self.with_memory_store(|memory_store| {
                let metadata_list = memory_store.values().cloned().collect::<Vec<_>>();
                let registry = self.registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = registry.save_fallback_file(&metadata_list).await {
                        warn!("Failed to save fallback file: {}", e);
                    }
                });
            }).await;
        }

        if !self.is_fallback_mode() {
            self.with_db(|db| -> Result<()> {
                if let Some(db) = db {
                    let key = port.to_string().into_bytes();
                    match db.get(&key) {
                        Ok(Some(ivec)) => {
                            match decode_from_slice::<DaemonMetadata, _>(&ivec, config::standard()) {
                                Ok((stored_metadata, _)) => {
                                    if stored_metadata.service_type == service_type {
                                        match db.remove(&key) {
                                            Ok(_) => {
                                                let db_clone = db.clone();
                                                tokio::spawn(async move {
                                                    if let Err(e) = db_clone.flush_async().await {
                                                        warn!("Failed to flush registry: {}", e);
                                                    }
                                                });
                                                info!("Unregistered {} daemon from sled on port {}", service_type, port);
                                            }
                                            Err(e) => {
                                                warn!("Failed to remove {} daemon from sled on port {}: {}", service_type, port, e);
                                                return Err(anyhow!("Failed to remove {} daemon from sled: {}", service_type, e));
                                            }
                                        }
                                    } else {
                                        debug!("No {} daemon found in sled on port {}", service_type, port);
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to decode daemon metadata for port {}: {}", port, e);
                                    return Err(anyhow!("Failed to decode daemon metadata: {}", e));
                                }
                            }
                        }
                        Ok(None) => {
                            debug!("No daemon found in sled on port {}", port);
                        }
                        Err(e) => {
                            warn!("Failed to access sled database for port {}: {}", port, e);
                            return Err(anyhow!("Failed to access sled database: {}", e));
                        }
                    }
                }
                Ok(())
            }).await?;
        }

        Ok(metadata)
    }

    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.load_fallback_file().await?;
        self.migrate_to_memory_if_needed().await?;

        let metadata = self.with_memory_store_read(|memory_store| {
            if let Some(metadata) = memory_store.get(&port) {
                let system = System::new_all();
                if system.process(Pid::from_u32(metadata.pid)).is_some() {
                    return Some(metadata.clone());
                }
            }
            None
        }).await;

        if metadata.is_some() {
            return Ok(metadata);
        }

        let daemons = self.fallback_discover_daemons().await?;
        let daemon = daemons.into_iter().find(|m| m.port == port);
        if let Some(ref metadata) = daemon {
            self.with_memory_store(|memory_store| {
                memory_store.insert(port, metadata.clone());
                let metadata_list = memory_store.values().cloned().collect::<Vec<_>>();
                let registry = self.registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = registry.save_fallback_file(&metadata_list).await {
                        warn!("Failed to save fallback file: {}", e);
                    }
                });
            }).await;
        }
        Ok(daemon)
    }

    pub async fn find_daemon_by_port(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.get_daemon_metadata(port).await
    }

    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DaemonMetadata>> {
        self.load_fallback_file().await?;
        self.migrate_to_memory_if_needed().await?;

        let mut all_metadata = self.with_memory_store_read(|memory_store| {
            let mut valid_metadata = Vec::new();
            let system = System::new_all();
            for metadata in memory_store.values() {
                if system.process(Pid::from_u32(metadata.pid)).is_some() {
                    valid_metadata.push(metadata.clone());
                }
            }
            valid_metadata
        }).await;

        if all_metadata.is_empty() {
            all_metadata = self.fallback_discover_daemons().await?;
            self.with_memory_store(|memory_store| {
                memory_store.clear();
                for metadata in &all_metadata {
                    memory_store.insert(metadata.port, metadata.clone());
                }
                let metadata_list = memory_store.values().cloned().collect::<Vec<_>>();
                let registry = self.registry.clone();
                tokio::spawn(async move {
                    if let Err(e) = registry.save_fallback_file(&metadata_list).await {
                        warn!("Failed to save fallback file: {}", e);
                    }
                });
            }).await;
            self.with_fallback_loaded(|fallback_loaded| *fallback_loaded = true).await;
        }

        let known_ports: Vec<u16> = all_metadata.iter().map(|m| m.port).collect();
        for port in known_ports {
            for prefix in [
                DAEMON_PID_FILE_NAME_PREFIX,
                REST_PID_FILE_NAME_PREFIX,
                STORAGE_PID_FILE_NAME_PREFIX,
                "graphdb-daemon-",
                "graphdb-cli-",
            ].iter() {
                let pid_file = format!("/tmp/{}{}.pid", prefix, port);
                if let Ok(pid_str) = async_fs::read_to_string(&pid_file).await {
                    if let Ok(pid) = pid_str.trim().parse::<u32>() {
                        let system = System::new_all();
                        if let Some(process) = system.process(Pid::from_u32(pid)) {
                            let cmdline = process.cmd().iter().map(|s| s.to_string_lossy().into_owned()).collect::<Vec<_>>().join(" ");
                            if let Some(service_type) = DaemonRegistry::determine_service_type(&process.name().to_string_lossy(), &cmdline) {
                                if !all_metadata.iter().any(|m| m.port == port && m.service_type == service_type) {
                                    let metadata = DaemonMetadata {
                                        service_type: service_type.to_string(),
                                        port,
                                        pid,
                                        ip_address: "127.0.0.1".to_string(),
                                        data_dir: if service_type == "storage" {
                                            Some(PathBuf::from("/opt/graphdb/storage_data"))
                                        } else {
                                            None
                                        },
                                        config_path: None,
                                        engine_type: if service_type == "storage" {
                                            Some("sled".to_string())
                                        } else {
                                            None
                                        },
                                        last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                                    };
                                    all_metadata.push(metadata);
                                }
                            }
                        }
                    }
                }
            }
        }

        info!("Returning {} daemon metadata entries", all_metadata.len());
        Ok(all_metadata)
    }

    pub async fn clear_all_daemons(&self) -> Result<()> {
        self.load_fallback_file().await?;
        let metadata_list = self.with_memory_store(|memory_store| {
            let list: Vec<_> = memory_store.values().cloned().collect();
            memory_store.clear();
            list
        }).await;

        for metadata in metadata_list {
            let pid_files = vec![
                format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, metadata.port),
                format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, metadata.port),
                format!("/tmp/{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, metadata.port),
                format!("/tmp/graphdb-daemon-{}.pid", metadata.port),
                format!("/tmp/graphdb-cli-{}.pid", metadata.port),
            ];
            for pid_file in pid_files {
                if PathBuf::from(&pid_file).exists() {
                    if let Err(e) = async_fs::remove_file(&pid_file).await {
                        warn!("Failed to remove PID file {}: {}", pid_file, e);
                    } else {
                        info!("Removed PID file {}", pid_file);
                    }
                }
            }
            let system = System::new_all();
            if let Some(process) = system.process(Pid::from_u32(metadata.pid)) {
                if process.kill_with(sysinfo::Signal::Term).unwrap_or(false) {
                    info!("Sent SIGTERM to process {} for port {}", metadata.pid, metadata.port);
                } else {
                    warn!("Failed to send SIGTERM to process {} for port {}", metadata.pid, metadata.port);
                }
            }
        }
        self.with_memory_store(|memory_store| {
            let metadata_list = memory_store.values().cloned().collect::<Vec<_>>();
            let registry = self.registry.clone();
            tokio::spawn(async move {
                if let Err(e) = registry.save_fallback_file(&metadata_list).await {
                    warn!("Failed to save fallback file: {}", e);
                }
            });
        }).await;

        if !self.is_fallback_mode() {
            self.with_db(|db| {
                if let Some(db) = db {
                    if let Err(e) = db.clear() {
                        warn!("Failed to clear sled database: {}", e);
                    } else {
                        let db_clone = db.clone();
                        tokio::spawn(async move {
                            if let Err(e) = db_clone.flush_async().await {
                                warn!("Failed to flush registry: {}", e);
                            }
                        });
                        info!("Cleared sled database");
                    }
                }
            }).await;
        }
        self.with_fallback_loaded(|fallback_loaded| *fallback_loaded = false).await;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        self.with_all_locks(|memory_store, db, fallback_loaded| {
            if let Some(database) = db {
                let db_clone = database.clone();
                tokio::spawn(async move {
                    if let Err(e) = db_clone.flush_async().await {
                        warn!("Failed to flush database before close: {}", e);
                    }
                });
                info!("Closed sled database");
            }
            let metadata_list = memory_store.values().cloned().collect::<Vec<_>>();
            let registry = self.registry.clone();
            tokio::spawn(async move {
                if let Err(e) = registry.save_fallback_file(&metadata_list).await {
                    warn!("Failed to save fallback file: {}", e);
                }
            });
            memory_store.clear();
            *fallback_loaded = false;
            info!("Cleared memory store on close");
        }).await;
        Ok(())
    }

    pub async fn health_check(&self) -> Result<bool> {
        if self.is_fallback_mode() {
            self.with_memory_store_read(|_memory_store| {
                debug!("Health check passed: in fallback mode");
                true
            }).await;
            Ok(true)
        } else {
            let result = self.with_db(|db| {
                match db {
                    Some(db) => {
                        let test_key = b"__health_check__";
                        let test_value = b"ok";
                        match db.insert(test_key, test_value) {
                            Ok(_) => {
                                let _ = db.remove(test_key);
                                debug!("Health check passed: sled database is writable");
                                true
                            }
                            Err(e) => {
                                warn!("Database health check failed: {}", e);
                                false
                            }
                        }
                    }
                    None => {
                        debug!("Health check failed: no sled database");
                        false
                    }
                }
            }).await;
            Ok(result)
        }
    }

    pub async fn set_fallback_loaded(&self, loaded: bool) -> Result<()> {
        self.with_fallback_loaded(|fallback_loaded| {
            *fallback_loaded = loaded;
        }).await;
        Ok(())
    }
}

pub static GLOBAL_DAEMON_REGISTRY: LazyLock<AsyncRegistryWrapper> = LazyLock::new(|| {
    info!("Initializing GLOBAL_DAEMON_REGISTRY with AsyncRegistryWrapper");
    AsyncRegistryWrapper::new()
});

pub async fn emergency_cleanup_daemon_registry() -> Result<()> {
    let db_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);

    GLOBAL_DAEMON_REGISTRY.close().await?;

    let lock_path = db_path.join("db.lck");
    if lock_path.exists() {
        async_fs::remove_file(&lock_path).await?;
        info!("Removed lock file {:?}", lock_path);
    }
    if db_path.exists() {
        async_fs::remove_dir_all(&db_path).await?;
        info!("Removed database directory {:?}", db_path);

        async_fs::create_dir_all(&db_path)
            .await
            .with_context(|| format!("Failed to recreate database directory {:?}", db_path))?;
        if let Ok(metadata) = async_fs::metadata(&db_path).await {
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            let _ = async_fs::set_permissions(&db_path, perms).await;
        }
        info!("Recreated clean database directory at {:?}", db_path);
    }
    GLOBAL_DAEMON_REGISTRY.set_fallback_loaded(false).await?;
    Ok(())
}
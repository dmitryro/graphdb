use anyhow::{Result, anyhow, Context};
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
use std::fs::{self, OpenOptions};
use std::os::unix::fs::PermissionsExt;
use sled::{Db, IVec, Config};

use crate::daemon_config::{
    DAEMON_REGISTRY_DB_PATH,
    DAEMON_PID_FILE_NAME_PREFIX,
    REST_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_NAME_PREFIX
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
        let fallback_file = db_path.parent().unwrap_or(&Path::new("/tmp")).join("daemon_registry_fallback.json");

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
            if let Err(e) = fs::create_dir_all(parent) {
                error!("Failed to create parent directory {:?}: {}", parent, e);
                return Ok(DaemonRegistry {
                    db: Arc::new(TokioMutex::new(None)),
                    memory_store,
                    is_fallback_mode: true,
                    fallback_file,
                    fallback_loaded: Arc::new(TokioMutex::new(false)),
                });
            }
            if let Ok(metadata) = fs::metadata(parent) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o755);
                if let Err(e) = fs::set_permissions(parent, perms) {
                    warn!("Failed to set permissions for {:?}: {}", parent, e);
                }
            }
            info!("Created parent directory {:?}", parent);
        }

        // Create database directory
        debug!("Attempting to create database directory {:?}", db_path);
        if let Err(e) = fs::create_dir_all(&db_path) {
            error!("Failed to create database directory {:?}: {}", db_path, e);
            return Ok(DaemonRegistry {
                db: Arc::new(TokioMutex::new(None)),
                memory_store,
                is_fallback_mode: true,
                fallback_file,
                fallback_loaded: Arc::new(TokioMutex::new(false)),
            });
        }
        if let Ok(metadata) = fs::metadata(&db_path) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            if let Err(e) = fs::set_permissions(&db_path, perms) {
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

    pub async fn load_fallback_file(&self) -> Result<()> {
        let mut fallback_loaded = self.fallback_loaded.lock().await;
        if *fallback_loaded {
            debug!("Fallback file already loaded for {:?}", self.fallback_file);
            return Ok(());
        }
        if self.fallback_file.exists() {
            let data = fs::read_to_string(&self.fallback_file)?;
            let metadata_list: Vec<DaemonMetadata> = serde_json::from_str(&data)
                .map_err(|e| anyhow!("Failed to parse fallback file: {}", e))?;
            let mut memory_store = self.memory_store.lock().await;
            for metadata in metadata_list {
                let mut system = System::new();
                system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(metadata.pid)]), true);
                if system.process(Pid::from_u32(metadata.pid)).is_some() {
                    memory_store.insert(metadata.port, metadata.clone());
                    info!("Loaded daemon from fallback file: {} on port {}", metadata.service_type, metadata.port);
                }
            }
            *fallback_loaded = true;
        }
        Ok(())
    }

    fn save_fallback_file(&self, metadata_list: &[DaemonMetadata]) -> Result<()> {
        let data = serde_json::to_string(metadata_list)
            .map_err(|e| anyhow!("Failed to serialize fallback file: {}", e))?;
        if let Err(e) = fs::write(&self.fallback_file, data) {
            warn!("Failed to write fallback file {:?}: {}", self.fallback_file, e);
        } else {
            if let Ok(metadata) = fs::metadata(&self.fallback_file) {
                let mut perms = metadata.permissions();
                perms.set_mode(0o644);
                let _ = fs::set_permissions(&self.fallback_file, perms);
            }
            info!("Saved {} daemons to fallback file {:?}", metadata_list.len(), self.fallback_file);
        }
        Ok(())
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
            fs::remove_file(lock_path)?;
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

    pub async fn migrate_to_memory_if_needed(&self) -> Result<()> {
        if self.is_fallback_mode {
            self.load_fallback_file().await?;
            return Ok(());
        }

        let memory_store = self.memory_store.lock().await;
        if !memory_store.is_empty() {
            return Ok(());
        }
        drop(memory_store);

        let db = self.db.lock().await;
        if let Some(db) = db.as_ref() {
            let mut memory_store = self.memory_store.lock().await;
            let mut count = 0;
            for item_result in db.iter() {
                let (key, ivec) = item_result?;
                let key: Vec<u8> = key.to_vec();
                let ivec: Vec<u8> = ivec.to_vec();
                match decode_from_slice::<DaemonMetadata, _>(&ivec, config::standard()) {
                    Ok((metadata, _)) => {
                        memory_store.insert(u16::from_str_radix(&String::from_utf8_lossy(&key), 10)?, metadata);
                        count += 1;
                    }
                    Err(e) => warn!("Skipping corrupted entry during migration: {}", e),
                }
                if count % 10 == 0 {
                    tokio::task::yield_now().await;
                }
            }
            if count > 0 {
                self.save_fallback_file(&memory_store.values().cloned().collect::<Vec<_>>())?;
            }
            let mut fallback_loaded = self.fallback_loaded.lock().await;
            *fallback_loaded = true;
            info!("Migrated {} entries to memory store", count);
        }
        Ok(())
    }

    pub async fn fallback_discover_daemons(&self) -> Result<Vec<DaemonMetadata>> {
        let mut metadata_list = Vec::new();
        
        let output = tokio::time::timeout(
            Duration::from_secs(5),
            Command::new("lsof")
                .args(&["-iTCP", "-sTCP:LISTEN", "-a", "-n", "-P", "-c", "graphdb"])
                .output()
        ).await;

        let output = match output {
            Ok(Ok(output)) => output,
            Ok(Err(e)) => {
                warn!("lsof command failed: {}", e);
                return Ok(metadata_list);
            }
            Err(_) => {
                warn!("lsof command timed out");
                return Ok(metadata_list);
            }
        };

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
                if let (Ok(pid), Some(port)) = (parts[1].parse::<u32>(), parts[8].split(':').last().and_then(|p| p.parse::<u16>().ok())) {
                    let cmdline_output = Command::new("ps")
                        .args(&["-p", &pid.to_string(), "-o", "args="])
                        .output()
                        .await;
                    let cmdline = cmdline_output
                        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
                        .unwrap_or_default();
                    let service_type = Self::determine_service_type(&parts[0], &cmdline);
                    if let Some(service_type) = service_type {
                        let metadata = DaemonMetadata {
                            service_type: service_type.to_string(),
                            port,
                            pid,
                            ip_address: "127.0.0.1".to_string(),
                            data_dir: if service_type == "storage" { Some(PathBuf::from("/opt/graphdb/storage_data")) } else { None },
                            config_path: None,
                            engine_type: if service_type == "storage" { Some("sled".to_string()) } else { None },
                            last_seen_nanos: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                        };
                        metadata_list.push(metadata);
                    }
                }
            }
        }

        if self.is_fallback_mode {
            let mut memory_store = self.memory_store.lock().await;
            for metadata in &metadata_list {
                memory_store.insert(metadata.port, metadata.clone());
            }
            self.save_fallback_file(&metadata_list)?;
            let mut fallback_loaded = self.fallback_loaded.lock().await;
            *fallback_loaded = true;
        }

        Ok(metadata_list)
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

    pub async fn register_daemon(&self, metadata: DaemonMetadata) -> Result<()> {
        let mut system = System::new();
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(metadata.pid)]), true);
        if system.process(Pid::from_u32(metadata.pid)).is_none() {
            return Err(anyhow!("Process with PID {} is not running", metadata.pid));
        }

        let pid_file = match metadata.service_type.as_str() {
            "main" => format!("/tmp/{}{}.pid", DAEMON_PID_FILE_NAME_PREFIX, metadata.port),
            "rest" => format!("/tmp/{}{}.pid", REST_PID_FILE_NAME_PREFIX, metadata.port),
            "storage" => format!("/tmp/{}{}.pid", STORAGE_PID_FILE_NAME_PREFIX, metadata.port),
            _ => format!("/tmp/graphdb-cli-{}.pid", metadata.port),
        };
        
        if let Err(e) = fs::write(&pid_file, metadata.pid.to_string()) {
            warn!("Failed to create PID file {}: {}", pid_file, e);
        } else {
            if let Ok(metadata_file) = fs::metadata(&pid_file) {
                let mut perms = metadata_file.permissions();
                perms.set_mode(0o644);
                let _ = fs::set_permissions(&pid_file, perms);
            }
            info!("Created PID file {} with PID {}", pid_file, metadata.pid);
        }

        {
            let mut memory_store = self.memory_store.lock().await;
            memory_store.insert(metadata.port, metadata.clone());
            self.save_fallback_file(&memory_store.values().cloned().collect::<Vec<_>>())?;
            info!("Registered daemon in memory: {} on port {} with PID {}", metadata.service_type, metadata.port, metadata.pid);
        }

        if !self.is_fallback_mode {
            let db = self.db.lock().await;
            if let Some(db) = db.as_ref() {
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
        }
        Ok(())
    }

    pub async fn unregister_daemon(&self, port: u16) -> Result<()> {
        let metadata = {
            let mut memory_store = self.memory_store.lock().await;
            memory_store.remove(&port)
        };
        
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
                    if let Err(e) = fs::remove_file(&pid_file) {
                        warn!("Failed to remove PID file {}: {}", pid_file, e);
                    } else {
                        info!("Removed PID file {}", pid_file);
                    }
                }
            }
            let mut memory_store = self.memory_store.lock().await;
            self.save_fallback_file(&memory_store.values().cloned().collect::<Vec<_>>())?;
        }
        
        if !self.is_fallback_mode {
            let db = self.db.lock().await;
            if let Some(db) = db.as_ref() {
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
        }
        
        if let Some(metadata) = metadata {
            let mut system = System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(metadata.pid)]), true);
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

    pub async fn get_daemon_metadata(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.load_fallback_file().await?;
        let _ = self.migrate_to_memory_if_needed().await;

        {
            let memory_store = self.memory_store.lock().await;
            if let Some(metadata) = memory_store.get(&port) {
                let mut system = System::new();
                system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(metadata.pid)]), true);
                if system.process(Pid::from_u32(metadata.pid)).is_some() {
                    return Ok(Some(metadata.clone()));
                }
            }
        }

        let daemons = self.fallback_discover_daemons().await?;
        let daemon = daemons.into_iter().find(|m| m.port == port);
        if let Some(ref metadata) = daemon {
            let mut memory_store = self.memory_store.lock().await;
            memory_store.insert(port, metadata.clone());
            self.save_fallback_file(&memory_store.values().cloned().collect::<Vec<_>>())?;
        }
        Ok(daemon)
    }

    pub async fn find_daemon_by_port(&self, port: u16) -> Result<Option<DaemonMetadata>> {
        self.get_daemon_metadata(port).await
    }

    pub async fn get_all_daemon_metadata(&self) -> Result<Vec<DaemonMetadata>> {
        self.load_fallback_file().await?;
        let _ = self.migrate_to_memory_if_needed().await;

        let mut all_metadata = {
            let memory_store = self.memory_store.lock().await;
            let mut valid_metadata = Vec::new();
            let mut system = System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
            for metadata in memory_store.values() {
                if system.process(Pid::from_u32(metadata.pid)).is_some() {
                    valid_metadata.push(metadata.clone());
                }
            }
            valid_metadata
        };

        if all_metadata.is_empty() {
            all_metadata = self.fallback_discover_daemons().await?;
            let mut memory_store = self.memory_store.lock().await;
            memory_store.clear();
            for metadata in &all_metadata {
                memory_store.insert(metadata.port, metadata.clone());
            }
            self.save_fallback_file(&all_metadata)?;
            let mut fallback_loaded = self.fallback_loaded.lock().await;
            *fallback_loaded = true;
        }

        let known_ports = all_metadata.iter().map(|m| m.port).collect::<Vec<_>>();
        for port in known_ports {
            for prefix in [DAEMON_PID_FILE_NAME_PREFIX, REST_PID_FILE_NAME_PREFIX, STORAGE_PID_FILE_NAME_PREFIX, "graphdb-daemon-", "graphdb-cli-"].iter() {
                let pid_file = format!("/tmp/{}{}.pid", prefix, port);
                if let Ok(pid_str) = fs::read_to_string(&pid_file) {
                    if let Ok(pid) = pid_str.trim().parse::<u32>() {
                        let mut system = System::new();
                        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true);
                        if system.process(Pid::from_u32(pid)).is_some() {
                            let cmdline_output = Command::new("ps")
                                .args(&["-p", &pid.to_string(), "-o", "args="])
                                .output()
                                .await;
                            let cmdline = cmdline_output
                                .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
                                .unwrap_or_default();
                            if let Some(service_type) = Self::determine_service_type("graphdb", &cmdline) {
                                if !all_metadata.iter().any(|m| m.port == port && m.service_type == service_type) {
                                    let metadata = DaemonMetadata {
                                        service_type: service_type.to_string(),
                                        port,
                                        pid,
                                        ip_address: "127.0.0.1".to_string(),
                                        data_dir: if service_type == "storage" { Some(PathBuf::from("/opt/graphdb/storage_data")) } else { None },
                                        config_path: None,
                                        engine_type: if service_type == "storage" { Some("sled".to_string()) } else { None },
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
        let metadata_list = {
            let mut memory_store = self.memory_store.lock().await;
            let list: Vec<_> = memory_store.values().cloned().collect();
            memory_store.clear();
            list
        };

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
                    if let Err(e) = fs::remove_file(&pid_file) {
                        warn!("Failed to remove PID file {}: {}", pid_file, e);
                    } else {
                        info!("Removed PID file {}", pid_file);
                    }
                }
            }
            let mut system = System::new();
            system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(metadata.pid)]), true);
            if let Some(process) = system.process(Pid::from_u32(metadata.pid)) {
                if process.kill_with(sysinfo::Signal::Term).unwrap_or(false) {
                    info!("Sent SIGTERM to process {} for port {}", metadata.pid, metadata.port);
                } else {
                    warn!("Failed to send SIGTERM to process {} for port {}", metadata.pid, metadata.port);
                }
            }
        }
        self.save_fallback_file(&[])?;
        info!("Cleared memory store");

        if !self.is_fallback_mode {
            let mut db = self.db.lock().await;
            if let Some(db) = db.as_ref() {
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
        }
        let mut fallback_loaded = self.fallback_loaded.lock().await;
        *fallback_loaded = false;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let mut db = self.db.lock().await;
        if let Some(database) = db.take() {
            if let Err(e) = database.flush_async().await {
                warn!("Failed to flush database before close: {}", e);
            }
            info!("Closed sled database");
        }
        let mut memory_store = self.memory_store.lock().await;
        self.save_fallback_file(&memory_store.values().cloned().collect::<Vec<_>>())?;
        memory_store.clear();
        let mut fallback_loaded = self.fallback_loaded.lock().await;
        *fallback_loaded = false;
        info!("Cleared memory store on close");
        Ok(())
    }

    pub async fn health_check(&self) -> Result<bool> {
        if self.is_fallback_mode {
            let _memory_store = self.memory_store.lock().await;
            debug!("Health check passed: in fallback mode");
            Ok(true)
        } else {
            let db = self.db.lock().await;
            match db.as_ref() {
                Some(db) => {
                    let test_key = b"__health_check__";
                    let test_value = b"ok";
                    match db.insert(test_key, test_value) {
                        Ok(_) => {
                            let _ = db.remove(test_key);
                            debug!("Health check passed: sled database is writable");
                            Ok(true)
                        }
                        Err(e) => {
                            warn!("Database health check failed: {}", e);
                            Ok(false)
                        }
                    }
                }
                None => {
                    debug!("Health check failed: no sled database");
                    Ok(false)
                }
            }
        }
    }
}

pub static GLOBAL_DAEMON_REGISTRY: LazyLock<DaemonRegistry> = LazyLock::new(|| {
    match DaemonRegistry::new() {
        Ok(registry) => {
            info!("Successfully initialized GLOBAL_DAEMON_REGISTRY");
            registry
        }
        Err(e) => {
            error!("Failed to initialize GLOBAL_DAEMON_REGISTRY: {}. Using fallback mode.", e);
            DaemonRegistry {
                db: Arc::new(TokioMutex::new(None)),
                memory_store: Arc::new(TokioMutex::new(HashMap::new())),
                is_fallback_mode: true,
                fallback_file: PathBuf::from("/tmp/daemon_registry_fallback.json"),
                fallback_loaded: Arc::new(TokioMutex::new(false)),
            }
        }
    }
});

pub async fn emergency_cleanup_daemon_registry() -> Result<()> {
    let db_path = PathBuf::from(DAEMON_REGISTRY_DB_PATH);
    
    if let Err(e) = GLOBAL_DAEMON_REGISTRY.close().await {
        warn!("Failed to close registry during cleanup: {}", e);
    }

    let lock_path = db_path.join("db.lck");
    if lock_path.exists() {
        fs::remove_file(&lock_path)?;
        info!("Removed lock file {:?}", lock_path);
    }
    if db_path.exists() {
        fs::remove_dir_all(&db_path)?;
        info!("Removed database directory {:?}", db_path);
        
        fs::create_dir_all(&db_path)
            .with_context(|| format!("Failed to recreate database directory {:?}", db_path))?;
        if let Ok(metadata) = fs::metadata(&db_path) {
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            let _ = fs::set_permissions(&db_path, perms);
        }
        info!("Recreated clean database directory at {:?}", db_path);
    }
    let mut fallback_loaded = GLOBAL_DAEMON_REGISTRY.fallback_loaded.lock().await;
    *fallback_loaded = false;
    Ok(())
}
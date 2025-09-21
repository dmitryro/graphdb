use async_trait::async_trait;
use std::fmt;
use std::fs;
use std::ffi::OsStr;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;
use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value, from_value};
use tokio::sync::Mutex as TokioMutex;
use crate::config::config_impl_cli::*;
use crate::config::config_defaults::*;
use crate::config::config_structs::*;
use crate::config::config_constants::*;
use openraft::{Entry, RaftStorage, StorageError};
use crate::config::config_helpers::{load_engine_specific_config, hashmap_to_engine_specific_config};
use models::errors::GraphError;
use crate::storage_engine::storage_engine::StorageEngineManager;
use rocksdb::{
    DB,
    WriteBatch,
    BoundColumnFamily,
    ColumnFamilyDescriptor,
    Options,
    ColumnFamily,
    IteratorMode,
    WriteOptions,
};

// Helper function to normalize the engine path
fn normalize_engine_path(mut path: PathBuf, engine_type_str: &str) -> PathBuf {
    let engine_low = engine_type_str.to_lowercase();

    if let Some(last) = path.file_name() {
        if let Some(last_str) = last.to_str() {
            if last_str == engine_low {
                let _ = path.pop();
            }
        }
    }

    let path_str = path.to_str().unwrap_or("").to_lowercase();
    let ends_with_engine = path_str.ends_with(&format!("/{}", engine_low))
        || path_str.ends_with(&engine_low)
        || path.components().last().map(|c| c.as_os_str() == OsStr::new(&engine_low)).unwrap_or(false);

    if !ends_with_engine {
        path.push(&engine_low);
    }

    path
}

pub struct Storage {
    db: Arc<DB>,
}

impl Storage {
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;
        Ok(Self { db: Arc::new(db) })
    }

    pub fn cleanup_cf(&self, cf: &ColumnFamily) -> anyhow::Result<()> {
        let db = &*self.db;
        let mut batch = WriteBatch::default();
        let iter = db.iterator_cf(cf, IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| anyhow!("Iterator error: {}", e))?;
            batch.delete_cf(cf, &key);
        }

        let write_opts = WriteOptions::default();
        db.write(batch)
            .map_err(|e| anyhow!("rocksdb write error: {}", e))?;
        db.flush()
            .map_err(|e| anyhow!("rocksdb flush error: {}", e))?;

        Ok(())
    }
}

// Main wrapper struct for top-level `storage` key
#[derive(Debug, Serialize, Deserialize, Clone)]
struct StorageConfigWrapper {
    storage: StorageConfig,
}

impl Default for StorageConfigInner {
    fn default() -> Self {
        StorageConfigInner {
            path: None,
            host: None,
            port: None,
            username: None,
            password: None,
            database: None,
            pd_endpoints: None,
            cache_capacity: Some(1024 * 1024 * 1024),
            use_compression: true,
            temporary: false,
            use_raft_for_scale: false,
        }
    }
}

impl Default for RocksDBConfig {
    fn default() -> Self {
        RocksDBConfig {
            storage_engine_type: StorageEngineType::RocksDB,
            path: PathBuf::from("/opt/graphdb/storage_data/rocksdb"),
            host: Some(String::from("127.0.0.1")),
            port: Some(8049),
            cache_capacity: None,
            temporary: false,
            use_compression: true,
            use_raft_for_scale: false,
            max_background_jobs: Some(1000),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        let default_engine = StorageEngineType::Sled;
        StorageConfig {
            config_root_directory: Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            data_directory: Some(PathBuf::from(DEFAULT_DATA_DIRECTORY)),
            log_directory: Some(PathBuf::from(DEFAULT_LOG_DIRECTORY)),
            default_port: DEFAULT_STORAGE_PORT,
            cluster_range: DEFAULT_STORAGE_PORT.to_string(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: default_engine,
            engine_specific_config: Some(create_default_selected_storage_config(&default_engine)),
            max_open_files: 100,
        }
    }
}

impl std::fmt::Display for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "== Storage Configuration ==")?;
        writeln!(f, "  Storage Engine Type: {}", self.storage_engine_type)?;
        writeln!(f, "  Config Root Directory: {}",
            self.config_root_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "Not specified".to_string())
        )?;
        writeln!(f, "  Data Directory: {}",
            self.data_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "Not specified".to_string())
        )?;
        writeln!(f, "  Log Directory: {}",
            self.log_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "Not specified".to_string())
        )?;
        writeln!(f, "  Default Port: {}", self.default_port)?;
        writeln!(f, "  Cluster Range: {}", self.cluster_range)?;
        writeln!(f, "  Max Disk Space (GB): {}", self.max_disk_space_gb)?;
        writeln!(f, "  Min Disk Space (GB): {}", self.min_disk_space_gb)?;
        writeln!(f, "  Use Raft for Scale: {}", self.use_raft_for_scale)?;
        writeln!(f, "  Max Open Files: {}", self.max_open_files)?;

        if let Some(engine_config) = &self.engine_specific_config {
            writeln!(f, "\n  == Engine-Specific Configuration ==")?;
            writeln!(f, "    Engine Type: {}", engine_config.storage_engine_type)?;
            writeln!(f, "    Path: {}",
                engine_config.storage.path.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "Not specified".to_string())
            )?;
            writeln!(f, "    Host: {}",
                engine_config.storage.host.as_deref().unwrap_or("Not specified")
            )?;
            writeln!(f, "    Port: {}",
                engine_config.storage.port.map(|p| p.to_string()).unwrap_or_else(|| "Not specified".to_string())
            )?;
            writeln!(f, "    Username: (Omitted for security)")?;
            writeln!(f, "    Password: (Omitted for security)")?;
            writeln!(f, "    Database: (Omitted for security)")?;
        } else {
            writeln!(f, "\n  Engine-Specific Configuration: None")?;
        }

        Ok(())
    }
}

impl StorageConfig {
    pub async fn load(path: &Path) -> Result<StorageConfig> {
        if !path.exists() {
            info!("Config file not found at {:?}", path);
            let default_config = StorageConfig::default();
            default_config.save().await.context("Failed to save default config.")?;
            return Ok(default_config);
        }

        let config_content = tokio::fs::read_to_string(path)
            .await
            .context(format!("Failed to read storage config file: {}", path.display()))?;
        let wrapper: StorageConfigWrapper = serde_yaml::from_str(&config_content)
            .context(format!("Failed to parse YAML as StorageConfigWrapper from {:?}", path.display()))
            .map_err(|e| {
                error!("Deserialization error: {:?}", e);
                e
            })?;
        let mut config = wrapper.storage;

        let engine_specific_config_path = match config.storage_engine_type.to_string().to_lowercase().as_str() {
            "sled" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED),
            "rocksdb" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB),
            "tikv" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV),
            "mysql" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL),
            "postgres" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES),
            "redis" => PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS),
            _ => path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(format!("storage_config_{}.yaml", config.storage_engine_type.to_string().to_lowercase())),
        };

        if engine_specific_config_path.exists() {
            info!("Loading engine-specific config from {:?}", engine_specific_config_path);
            match SelectedStorageConfig::load_from_yaml(&engine_specific_config_path) {
                Ok(mut engine_config) => {
                    // Propagate top-level use_raft_for_scale to engine-specific config
                    engine_config.storage.use_raft_for_scale = config.use_raft_for_scale;
                    config.engine_specific_config = Some(engine_config);
                    if let Some(port) = config.engine_specific_config.as_ref().and_then(|c| c.storage.port) {
                        if port != config.default_port {
                            info!("Overriding default_port {} with engine-specific port {} from {:?}",
                                config.default_port, port, engine_specific_config_path);
                            config.default_port = port;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to load engine-specific config from {:?}: {}. Using default.", engine_specific_config_path, e);
                    let mut engine_config = create_default_selected_storage_config(&config.storage_engine_type);
                    engine_config.storage.use_raft_for_scale = config.use_raft_for_scale;
                    config.engine_specific_config = Some(engine_config);
                }
            }
        } else {
            info!("Engine-specific config file not found at {:?}, using default for {:?}", engine_specific_config_path, config.storage_engine_type);
            let mut engine_config = create_default_selected_storage_config(&config.storage_engine_type);
            engine_config.storage.use_raft_for_scale = config.use_raft_for_scale;
            config.engine_specific_config = Some(engine_config);
        }

        if let Some(engine_config) = config.engine_specific_config.clone() {
            if config.storage_engine_type != engine_config.storage_engine_type {
                info!(
                    "Top-level storage_engine_type ({:?}) does not match engine_specific_config ({:?}). Synchronizing to the engine-specific type.",
                    config.storage_engine_type, engine_config.storage_engine_type
                );
                config.storage_engine_type = engine_config.storage_engine_type;
            }

            let engine_path_name = config.storage_engine_type.to_string().to_lowercase();
            let data_dir_path = config.data_directory.as_ref().map_or(
                PathBuf::from(DEFAULT_DATA_DIRECTORY),
                |p| p.clone()
            );

            let engine_data_path = if data_dir_path.ends_with(&engine_path_name) {
                data_dir_path
            } else {
                data_dir_path.join(&engine_path_name)
            };

            if engine_config.storage.path.is_none() || engine_config.storage.path != Some(engine_data_path.clone()) {
                info!(
                    "Engine-specific path was not set or mismatched. Setting path to: {:?}", engine_data_path
                );
                let mut updated_engine_config = engine_config.clone();
                updated_engine_config.storage.path = Some(engine_data_path);
                config.engine_specific_config = Some(updated_engine_config);
            }
        } else {
            info!("'engine_specific_config' was missing, setting to default for engine: {:?}", config.storage_engine_type);
            let mut engine_config = create_default_selected_storage_config(&config.storage_engine_type);
            engine_config.storage.use_raft_for_scale = config.use_raft_for_scale;
            config.engine_specific_config = Some(engine_config);
        }

        let data_dir = config.data_directory.as_ref().ok_or_else(|| anyhow!("Data directory is not specified in configuration"))?;
        tokio::fs::create_dir_all(data_dir)
            .await
            .context(format!("Failed to create data directory {:?}", data_dir))?;
        info!("Ensured data directory exists: {:?}", data_dir);

        let log_dir = config.log_directory.as_ref().ok_or_else(|| anyhow!("Log directory is not specified in configuration"))?;
        tokio::fs::create_dir_all(log_dir)
            .await
            .context(format!("Failed to create log directory {:?}", log_dir))?;
        info!("Ensured log directory exists: {:?}", log_dir);

        let validated_config = config.validate().context("Failed to validate storage configuration")?;
        info!("Successfully loaded and validated storage configuration from {:?}", path);
        debug!(
            "Final validated config: default_port={}, cluster_range={}, data_directory={:?}, log_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}",
            validated_config.default_port,
            validated_config.cluster_range,
            validated_config.data_directory,
            validated_config.log_directory,
            validated_config.storage_engine_type,
            validated_config.engine_specific_config,
        );

        Ok(validated_config)
    }

    pub async fn save(&self) -> Result<(), anyhow::Error> {
        let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        debug!("Saving configuration to {:?}", config_path);

        let engine_type_str = self.storage_engine_type.to_string().to_lowercase();

        let (engine_path, engine_host, engine_port, engine_username, engine_password, engine_pd_endpoints) = match self.engine_specific_config.as_ref() {
            Some(es) => {
                let base_path = es.storage.path.clone().unwrap_or_else(|| {
                    self.data_directory.as_ref().cloned().unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY))
                });
                let normalized_path = normalize_engine_path(base_path, &engine_type_str);
                (
                    normalized_path,
                    es.storage.host.clone().unwrap_or_else(|| "127.0.0.1".to_string()),
                    es.storage.port.unwrap_or(self.default_port),
                    es.storage.username.clone().unwrap_or_default(),
                    es.storage.password.clone().unwrap_or_default(),
                    es.storage.pd_endpoints.clone().unwrap_or_default(),
                )
            },
            None => {
                let base_path = self.data_directory.as_ref().cloned().unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY));
                let normalized_path = normalize_engine_path(base_path, &engine_type_str);
                (
                    normalized_path,
                    "127.0.0.1".to_string(),
                    self.default_port,
                    String::new(),
                    String::new(),
                    String::new(),
                )
            },
        };

        let data_directory_str = self.data_directory.as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| DEFAULT_DATA_DIRECTORY.to_string());

        let yaml_string = format!(
            r#"storage:
    config_root_directory: "{}"
    data_directory: "{}"
    log_directory: "{}"
    default_port: {}
    cluster_range: "{}"
    max_disk_space_gb: {}
    min_disk_space_gb: {}
    use_raft_for_scale: {}
    storage_engine_type: "{}"
    engine_specific_config:
      storage_engine_type: "{}"
      path: "{}"
      host: "{}"
      port: {}
      username: "{}"
      password: "{}"
      pd_endpoints: "{}"
      max_background_jobs: {}
      temporary: {}
      use_compression: {}
    max_open_files: {}
  "#,
            self.config_root_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
            data_directory_str,
            self.log_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
            self.default_port,
            self.cluster_range,
            self.max_disk_space_gb,
            self.min_disk_space_gb,
            self.use_raft_for_scale,
            self.storage_engine_type.to_string().to_lowercase(),
            self.storage_engine_type.to_string().to_lowercase(),
            engine_path.display(),
            engine_host,
            engine_port,
            engine_username,
            engine_password,
            engine_pd_endpoints,
            self.engine_specific_config.as_ref().and_then(|es| es.storage.cache_capacity).unwrap_or(0),
            self.engine_specific_config.as_ref().map_or(false, |es| es.storage.use_compression),
            self.engine_specific_config.as_ref().map_or(false, |es| es.storage.use_compression),
            self.max_open_files
        );

        tokio::fs::create_dir_all(config_path.parent().unwrap())
            .await
            .context(format!("Failed to create parent directories for {}", config_path.display()))?;

        tokio::fs::write(&config_path, yaml_string.as_bytes())
            .await
            .context(format!("Failed to write StorageConfig to file: {}", config_path.display()))?;

        info!("Saved storage configuration to {:?}", config_path);

        let written_content = tokio::fs::read_to_string(&config_path)
            .await
            .context(format!("Failed to read back storage config file: {}", config_path.display()))?;
        if written_content != yaml_string {
            error!("Written config does not match expected content at {:?}", config_path);
            return Err(anyhow!("Written config verification failed"));
        }
        debug!("Verified written content:\n{}", written_content);

        Ok(())
    }

    pub fn validate(self) -> Result<Self, GraphError> {
        let available_engines = StorageEngineManager::available_engines();
        if !available_engines.contains(&self.storage_engine_type) {
            return Err(GraphError::InvalidStorageEngine(format!(
                "Storage engine {:?} is not enabled. Available engines: {:?}", self.storage_engine_type, available_engines
            )));
        }
        if let Some(engine_config) = &self.engine_specific_config {
            if engine_config.storage_engine_type != self.storage_engine_type {
                return Err(GraphError::ConfigurationError(
                    "engine_specific_config.storage_engine_type must match storage_engine_type".to_string()
                ));
            }
        }
        if self.default_port == 0 {
            return Err(GraphError::ConfigurationError("default_port must be non-zero".to_string()));
        }
        if self.cluster_range.is_empty() {
            return Err(GraphError::ConfigurationError("cluster_range must be non-empty".to_string()));
        }
        Ok(self)
    }
}

impl From<RawStorageConfig> for StorageConfig {
    fn from(raw: RawStorageConfig) -> Self {
        let storage_engine_type = raw.storage_engine_type.unwrap_or(StorageEngineType::Sled);

        let engine_specific_config = raw.engine_specific_config.map(|config_map| {
            let mut storage_config_inner = StorageConfigInner {
                path: None,
                host: None,
                port: None,
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
                cache_capacity: None,
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
            };

            if let Some(path) = config_map.get("path").and_then(|v| v.as_str()) {
                let engine_path_name = storage_engine_type.to_string().to_lowercase();
                let data_dir_path = PathBuf::from(DEFAULT_DATA_DIRECTORY);
                let engine_data_path = data_dir_path.join(&engine_path_name);
                storage_config_inner.path = Some(engine_data_path);
            } else {
                storage_config_inner.path = Some(PathBuf::from(format!(
                    "{}/{}",
                    DEFAULT_DATA_DIRECTORY,
                    storage_engine_type.to_string().to_lowercase()
                )));
            }
            if let Some(host) = config_map.get("host").and_then(|v| v.as_str()) {
                storage_config_inner.host = Some(host.to_string());
            }
            if let Some(port) = config_map.get("port").and_then(|v| v.as_u64()) {
                storage_config_inner.port = Some(port as u16);
            }
            if let Some(username) = config_map.get("username").and_then(|v| v.as_str()) {
                storage_config_inner.username = Some(username.to_string());
            }
            if let Some(password) = config_map.get("password").and_then(|v| v.as_str()) {
                storage_config_inner.password = Some(password.to_string());
            }
            if let Some(database) = config_map.get("database").and_then(|v| v.as_str()) {
                storage_config_inner.database = Some(database.to_string());
            }
            if let Some(pd_endpoints) = config_map.get("pd_endpoints").and_then(|v| v.as_str()) {
                storage_config_inner.pd_endpoints = Some(pd_endpoints.to_string());
            }

            SelectedStorageConfig {
                storage_engine_type,
                storage: storage_config_inner,
            }
        }).or_else(|| Some(create_default_selected_storage_config(&storage_engine_type)));

        StorageConfig {
            config_root_directory: raw.config_root_directory,
            data_directory: raw.data_directory,
            log_directory: raw.log_directory,
            default_port: raw.default_port.unwrap_or(DEFAULT_STORAGE_PORT),
            cluster_range: raw.cluster_range.unwrap_or_else(|| DEFAULT_STORAGE_PORT.to_string()),
            max_disk_space_gb: raw.max_disk_space_gb.unwrap_or(1000),
            min_disk_space_gb: raw.min_disk_space_gb.unwrap_or(10),
            use_raft_for_scale: raw.use_raft_for_scale.unwrap_or(true),
            storage_engine_type,
            engine_specific_config,
            max_open_files: raw.max_open_files.unwrap_or(100),
        }
    }
}

impl Default for SelectedStorageConfig {
    fn default() -> Self {
        create_default_selected_storage_config(&StorageEngineType::Sled)
    }
}

impl SelectedStorageConfig {
    pub fn load_from_yaml<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file {:?}", path))?;
        debug!("Raw YAML content from {:?}:\n{}", path, content);

        let cleaned_content = content
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.is_empty() && !trimmed.starts_with('#')
            })
            .collect::<Vec<&str>>()
            .join("\n");

        match serde_yaml::from_str::<StorageConfigWrapper>(&cleaned_content) {
            Ok(wrapper) => {
                if let Some(engine_config) = wrapper.storage.engine_specific_config {
                    info!("Successfully extracted engine-specific config from main config file {:?}", path);
                    debug!("Parsed config: {:?}", engine_config);
                    return Ok(engine_config);
                } else {
                    debug!("No engine_specific_config found in main config file, trying as standalone engine config");
                }
            }
            Err(e) => {
                debug!("Failed to parse as main config file from {:?}: {}", path, e);
            }
        }

        #[derive(Deserialize)]
        struct EngineSpecificWrapper {
            storage: SelectedStorageConfig,
        }

        match serde_yaml::from_str::<EngineSpecificWrapper>(&cleaned_content) {
            Ok(wrapper) => {
                info!("Successfully parsed engine-specific config with storage wrapper from {:?}", path);
                debug!("Parsed config: {:?}", wrapper.storage);
                return Ok(wrapper.storage);
            }
            Err(e) => {
                debug!("Failed to parse as engine-specific config with storage wrapper from {:?}: {}", path, e);
            }
        }

        match serde_yaml::from_str::<SelectedStorageConfigWrapper>(&cleaned_content) {
            Ok(wrapper) => {
                info!("Successfully parsed standalone engine-specific config from {:?}", path);
                debug!("Parsed config: {:?}", wrapper.storage);
                return Ok(wrapper.storage);
            }
            Err(e) => {
                debug!("Failed to parse as standalone engine config from {:?}: {}", path, e);
            }
        }

        let mut in_storage_block = false;
        let mut in_engine_config_block = false;
        let mut storage_lines: Vec<String> = Vec::new();

        for raw_line in cleaned_content.lines() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }

            if !in_storage_block {
                if line.starts_with("storage:") {
                    in_storage_block = true;
                    continue;
                }
            } else if !in_engine_config_block {
                if line.starts_with("engine_specific_config:") {
                    in_engine_config_block = true;
                    continue;
                }
                continue;
            } else {
                if (!raw_line.starts_with(' ') && !raw_line.starts_with('\t')) ||
                   (line.ends_with(':') && !line.contains(' ')) {
                    break;
                }
                storage_lines.push(line.to_string());
            }
        }

        if storage_lines.is_empty() {
            for raw_line in cleaned_content.lines() {
                let line = raw_line.trim();
                if line.is_empty() {
                    continue;
                }
                if !in_storage_block {
                    if line.starts_with("storage:") {
                        in_storage_block = true;
                        continue;
                    }
                } else {
                    if !raw_line.starts_with(' ') && !raw_line.starts_with('\t') {
                        break;
                    }
                    storage_lines.push(line.to_string());
                }
            }
        }

        let storage_block = storage_lines.join("\n");

        let extract_simple = |key: &str| -> Option<String> {
            for line in storage_block.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if trimmed.starts_with(&format!("{}:", key)) {
                    if let Some(value) = trimmed.splitn(2, ':').nth(1) {
                        let cleaned = value.trim().trim_matches('"').to_string();
                        if !cleaned.is_empty() {
                            return Some(cleaned);
                        }
                    }
                }
            }
            None
        };

        let storage_engine_type = extract_simple("storage_engine_type")
            .map(|et_str| {
                match <StorageEngineType as std::str::FromStr>::from_str(&et_str) {
                    Ok(t) => t,
                    Err(_) => {
                        debug!("Failed to parse storage_engine_type '{}', inferring from filename", et_str);
                        match path.file_name().and_then(|s| s.to_str()) {
                            Some(fname) if fname.to_lowercase().contains("rocks") => StorageEngineType::RocksDB,
                            Some(fname) if fname.to_lowercase().contains("tikv") => StorageEngineType::TiKV,
                            Some(fname) if fname.to_lowercase().contains("mysql") => StorageEngineType::MySQL,
                            Some(fname) if fname.to_lowercase().contains("postgres") || fname.to_lowercase().contains("postgresql") => StorageEngineType::PostgreSQL,
                            Some(fname) if fname.to_lowercase().contains("redis") => StorageEngineType::Redis,
                            Some(fname) if fname.to_lowercase().contains("sled") => StorageEngineType::Sled,
                            _ => StorageEngineType::Sled,
                        }
                    }
                }
            })
            .unwrap_or_else(|| {
                match path.file_name().and_then(|s| s.to_str()) {
                    Some(fname) if fname.to_lowercase().contains("rocks") => StorageEngineType::RocksDB,
                    Some(fname) if fname.to_lowercase().contains("tikv") => StorageEngineType::TiKV,
                    Some(fname) if fname.to_lowercase().contains("mysql") => StorageEngineType::MySQL,
                    Some(fname) if fname.to_lowercase().contains("postgres") || fname.to_lowercase().contains("postgresql") => StorageEngineType::PostgreSQL,
                    Some(fname) if fname.to_lowercase().contains("redis") => StorageEngineType::Redis,
                    Some(fname) if fname.to_lowercase().contains("sled") => StorageEngineType::Sled,
                    _ => StorageEngineType::Sled,
                }
            });

        let cfg = SelectedStorageConfig {
            storage_engine_type,
            storage: StorageConfigInner {
                path: extract_simple("path").map(PathBuf::from),
                host: extract_simple("host"),
                port: extract_simple("port").and_then(|s| s.parse::<u16>().ok()),
                username: extract_simple("username"),
                password: extract_simple("password"),
                database: extract_simple("database"),
                pd_endpoints: extract_simple("pd_endpoints"),
                cache_capacity: Some(1024 * 1024 * 1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
            },
        };

        info!("Using fallback parser to build SelectedStorageConfig from {:?}", path);
        debug!("Fallback parsed config: {:?}", cfg);
        Ok(cfg)
    }
}

fn is_engine_specific_config_complete(config: &Option<SelectedStorageConfig>) -> bool {
    if let Some(engine_config) = config {
        engine_config.storage.path.is_some() || engine_config.storage.host.is_some()
    } else {
        false
    }
}

fn create_default_selected_storage_config(engine_type: &StorageEngineType) -> SelectedStorageConfig {
    let engine_path_name = engine_type.to_string().to_lowercase();
    let engine_data_path = PathBuf::from(DEFAULT_DATA_DIRECTORY).join(&engine_path_name);

    SelectedStorageConfig {
        storage_engine_type: *engine_type,
        storage: StorageConfigInner {
            path: Some(engine_data_path),
            host: Some("127.0.0.1".to_string()),
            port: Some(DEFAULT_STORAGE_PORT),
            username: None,
            password: None,
            database: None,
            pd_endpoints: None,
            cache_capacity: None,
            use_compression: true,
            temporary: false,
            use_raft_for_scale: false,
        },
    }
}

impl RocksDBRaftStorage {
    /// Creates a new instance of RocksDBRaftStorage.
    pub fn new(
        db: Arc<DB>,
        log_cf: Arc<BoundColumnFamily<'static>>,
        state_cf: Arc<BoundColumnFamily<'static>>,
        snapshot_cf: Arc<BoundColumnFamily<'static>>,
        snapshot_meta_cf: Arc<BoundColumnFamily<'static>>,
        client: Arc<RocksDBClient>,
        config: StorageConfig,
    ) -> Self {
        Self {
            db,
            state_cf_name: "state".to_string(),
            log_cf_name: "log".to_string(),
            state_cf,
            log_cf,
            snapshot_cf,
            snapshot_cf_name: "snapshot".to_string(),
            snapshot_meta_cf_name: "snapshot_meta".to_string(),
            snapshot_meta_cf,
            config,
            client: Some(client),
        }
    }

    /// Retrieves a value for a given key from the log column family.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        let db = self.db.clone();
        let log_cf = self.log_cf.clone();
        let key = key.to_vec(); // Clone to owned Vec<u8> for 'static lifetime

        task::spawn_blocking(move || {
            debug!("Getting key {:?}", key);
            db.get_cf(&log_cf, &key)
                .map_err(|e| GraphError::StorageError(e.to_string()))
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to spawn blocking task: {}", e)))?
    }

    /// Sets a value for a given key in the log column family.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<(), GraphError> {
        let db = self.db.clone();
        let log_cf = self.log_cf.clone();
        let key = key.to_vec(); // Clone to owned Vec<u8> for 'static lifetime
        let value = value.to_vec(); // Clone to owned Vec<u8> for 'static lifetime

        task::spawn_blocking(move || {
            let mut write_opts = WriteOptions::default();
            write_opts.set_sync(true);
            debug!("Setting key {:?} with value {:?}", key, value);
            db.put_cf_opt(&log_cf, &key, &value, &write_opts)
                .map_err(|e| GraphError::StorageError(e.to_string()))
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to spawn blocking task: {}", e)))?
    }

    /// Deletes a key-value pair from the log column family.
    pub async fn delete(&self, key: &[u8]) -> Result<(), GraphError> {
        let db = self.db.clone();
        let log_cf = self.log_cf.clone();
        let key = key.to_vec(); // Clone to owned Vec<u8> for 'static lifetime

        task::spawn_blocking(move || {
            let mut write_opts = WriteOptions::default();
            write_opts.set_sync(true);
            debug!("Deleting key {:?}", key);
            db.delete_cf_opt(&log_cf, &key, &write_opts)
                .map_err(|e| GraphError::StorageError(e.to_string()))
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to spawn blocking task: {}", e)))?
    }

    /// Flushes all pending writes to disk.
    pub async fn flush(&self) -> Result<(), GraphError> {
        let db = self.db.clone();

        task::spawn_blocking(move || {
            debug!("Flushing WAL");
            db.flush_wal(true)
                .map_err(|e| GraphError::StorageError(e.to_string()))
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to spawn blocking task: {}", e)))?
    }

    /// Closes the underlying RocksDB client.
    pub async fn close(&mut self) -> Result<(), GraphError> {
        if let Some(client_arc) = self.client.take() {
            debug!("Closing RocksDB client");
            // The unwrap is safe here because `take()` guarantees we have a value.
            let mut client = Arc::try_unwrap(client_arc)
                .map_err(|_| GraphError::Storage("Could not unwrap Arc to close client".to_string()))?;
            client.close().await
        } else {
            debug!("No RocksDB client to close");
            Ok(())
        }
    }
}

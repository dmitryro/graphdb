use std::fs;
use std::fs::File;
use std::io::Write; // <-- this is the missing import
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::collections::{HashMap};
use std::fmt;
use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn, trace};
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer, Deserializer};
use serde::de::{self, MapAccess, Visitor};
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
use crate::cli::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction, 
                           ReloadAction, RestCliCommand, StatusAction, StorageAction, ShowAction, ShowArgs,
                           StartAction, StopAction, StopArgs, DaemonCliCommand, UseAction, SaveAction};
pub use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig, 
                                      SelectedStorageConfig as LibSelectedStorageConfig,
                                      StorageConfigInner as LibStorageConfigInner};
use lib::query_exec_engine::query_exec_engine::{QueryExecEngine};
pub use lib::storage_engine::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
pub use lib::storage_engine::config::{EngineTypeOnly, RocksdbConfig, SledConfig, TikvConfig, MySQLConfig, 
                                      RedisConfig, PostgreSQLConfig};
pub use models::errors::GraphError;
pub use crate::cli::config_structs::*;
pub use crate::cli::config_constants::*;
pub use crate::cli::config_defaults::*;
pub use crate::cli::config_helpers::*;
pub use crate::cli::serializers::*;


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
        }
    }
}

// Updated StorageConfig default implementation
impl Default for StorageConfig {
    fn default() -> Self {
        let default_engine = StorageEngineType::Sled; // Use Sled as default
        StorageConfig {
            config_root_directory: Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            data_directory: default_data_directory(),
            log_directory: default_log_directory(),
            default_port: 8049, // Use 8049 as default to match YAML
            cluster_range: "8049".to_string(), // Use 8049 as default to match YAML
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: default_engine,
            engine_specific_config: create_default_engine_specific_config(&default_engine),
            max_open_files: 100,
        }
    }
}

impl fmt::Display for StorageConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

        // Handle the optional nested config
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
            
            // Omit sensitive fields for security
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
    pub fn load(path: &Path) -> Result<StorageConfig> {
        // 1. Check if the configuration file exists. If not, create and save a default one.
        if !path.exists() {
            info!("Config file not found at {:?}, creating default configuration.", path);
            let default_config = StorageConfig::default();
            default_config.save().context("Failed to save default config.")?;
            return Ok(default_config);
        }

        // 2. Read the file content.
        let config_content = fs::read_to_string(path)
            .context(format!("Failed to read storage config file: {}", path.display()))?;
        debug!("Raw YAML content from {:?}:\n{}", path, config_content);

        // 3. Deserialize the configuration.
        let mut config: StorageConfig = match serde_yaml::from_str::<StorageConfigWrapper>(&config_content) {
            Ok(wrapper) => {
                debug!("Successfully deserialized as StorageConfigWrapper: {:?}", wrapper);
                wrapper.storage
            }
            Err(e) => {
                error!("Failed to parse YAML as StorageConfigWrapper: {}. Raw content:\n{}", e, config_content);
                return Err(anyhow!("Failed to parse YAML as StorageConfigWrapper: {}", e));
            }
        };

        debug!("Raw deserialized config: {:?}", config);

        // 4. Synchronize storage_engine_type with engine_specific_config.
        if let Some(engine_config) = config.engine_specific_config.clone() {
            if config.storage_engine_type != engine_config.storage_engine_type {
                info!(
                    "Top-level storage_engine_type ({:?}) does not match engine_specific_config ({:?}). Synchronizing to the engine-specific type.",
                    config.storage_engine_type, engine_config.storage_engine_type
                );
                config.storage_engine_type = engine_config.storage_engine_type;
            }
            // Ensure the path in the engine-specific config is correctly set relative to the data directory.
            let engine_path_name = config.storage_engine_type.to_string().to_lowercase();
            let data_dir_path = config.data_directory.as_ref().map_or(
                PathBuf::from(DEFAULT_DATA_DIRECTORY),
                |p| p.clone()
            );
            
            let engine_data_path = data_dir_path.join(engine_path_name);
            
            if engine_config.storage.path.is_none() || engine_config.storage.path != Some(engine_data_path.clone()) {
                info!(
                    "Engine-specific path was not set or mismatched. Setting path to: {:?}",
                    engine_data_path
                );
                
                let mut updated_engine_config = engine_config.clone();
                updated_engine_config.storage.path = Some(engine_data_path);
                updated_engine_config.storage.port = Some(config.default_port); // Ensure port consistency
                config.engine_specific_config = Some(updated_engine_config);
            }
        } else {
            info!("'engine_specific_config' was missing, setting to default for engine: {:?}", config.storage_engine_type);
            config.engine_specific_config = create_default_engine_specific_config(&config.storage_engine_type);
        }

        // 5. Ensure required directories exist.
        if let Some(ref data_dir) = config.data_directory {
            fs::create_dir_all(data_dir)
                .context(format!("Failed to create data directory {:?}", data_dir))?;
            info!("Ensured data directory exists: {:?}", data_dir);
        }
        if let Some(ref log_dir) = config.log_directory {
            fs::create_dir_all(log_dir)
                .context(format!("Failed to create log directory {:?}", log_dir))?;
            info!("Ensured log directory exists: {:?}", log_dir);
        }

        // 6. Final validation.
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

    pub fn save(&self) -> Result<(), anyhow::Error> {
        let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        debug!("Saving configuration to {:?}", config_path);

        let (engine_path, engine_host, engine_port, engine_username, engine_password, engine_pd_endpoints) = match self.engine_specific_config.as_ref() {
            Some(es) => (
                es.storage.path.clone().unwrap_or_else(|| PathBuf::from(format!(
                    "{}/{}",
                    self.data_directory.as_ref().map_or(DEFAULT_DATA_DIRECTORY.to_string(), |p| p.to_string_lossy().to_string()),
                    self.storage_engine_type.to_string().to_lowercase()
                ))),
                es.storage.host.clone().unwrap_or_else(|| "127.0.0.1".to_string()),
                es.storage.port.unwrap_or(self.default_port),
                es.storage.username.clone(),
                es.storage.password.clone(),
                es.storage.pd_endpoints.clone(),
            ),
            None => (
                PathBuf::from(format!(
                    "{}/{}",
                    self.data_directory.as_ref().map_or(DEFAULT_DATA_DIRECTORY.to_string(), |p| p.to_string_lossy().to_string()),
                    self.storage_engine_type.to_string().to_lowercase()
                )),
                "127.0.0.1".to_string(),
                self.default_port,
                None,
                None,
                None,
            ),
        };

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
        path: "{}"
        host: "{}"
        port: {}
        storage_engine_type: "{}"
        username: "{}"
        password: "{}"
        pd_endpoints: "{}"
      max_open_files: {}
    "#,
            self.config_root_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "".to_string()),
            self.data_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "".to_string()),
            self.log_directory.as_ref().map(|p| p.display().to_string()).unwrap_or_else(|| "".to_string()),
            self.default_port,
            self.cluster_range,
            self.max_disk_space_gb,
            self.min_disk_space_gb,
            self.use_raft_for_scale,
            self.storage_engine_type.to_string().to_lowercase(),
            engine_path.display(),
            engine_host,
            engine_port,
            self.storage_engine_type.to_string().to_lowercase(),
            engine_username.unwrap_or_default(),
            engine_password.unwrap_or_default(),
            engine_pd_endpoints.unwrap_or_default(),
            self.max_open_files
        );

        fs::create_dir_all(config_path.parent().unwrap())
            .context(format!("Failed to create parent directories for {}", config_path.display()))?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&config_path)
            .context(format!("Failed to open config file for writing: {}", config_path.display()))?;
        file.write_all(yaml_string.as_bytes())
            .context(format!("Failed to write StorageConfig to file: {}", config_path.display()))?;
        file.flush()
            .context(format!("Failed to flush config file: {}", config_path.display()))?;
        drop(file); // Ensure the file is closed
        info!("Saved storage configuration to {:?}", config_path);

        // Verify the written content
        let written_content = fs::read_to_string(&config_path)
            .context(format!("Failed to read back storage config file: {}", config_path.display()))?;
        if written_content != yaml_string {
            error!("Written config does not match expected content at {:?}", config_path);
            return Err(anyhow!("Written config verification failed"));
        }
        debug!("Verified written content:\n{}", written_content);

        Ok(())
    }

    pub fn default() -> Self {
        Self {
            config_root_directory: Some(PathBuf::from("./storage_daemon_server")),
            data_directory: default_data_directory(),
            log_directory: default_log_directory(),
            default_port: default_default_port(),
            cluster_range: default_cluster_range(),
            max_disk_space_gb: default_max_disk_space_gb(),
            min_disk_space_gb: default_min_disk_space_gb(),
            use_raft_for_scale: default_use_raft_for_scale(),
            storage_engine_type: default_storage_engine_type(),
            engine_specific_config: default_engine_specific_config(),
            max_open_files: default_max_open_files(),
        }
    }

    pub fn validate(self) -> Result<Self, GraphError> {
        let available_engines = StorageEngineManager::available_engines();
        if !available_engines.contains(&self.storage_engine_type) {
            println!("DEBUG: Invalid engine type: {:?}", self.storage_engine_type);
            return Err(GraphError::InvalidStorageEngine(format!(
                "Storage engine {:?} is not enabled. Available engines: {:?}", 
                self.storage_engine_type, available_engines
            )));
        }
        if let Some(engine_config) = &self.engine_specific_config {
            if engine_config.storage_engine_type != self.storage_engine_type {
                println!("DEBUG: Mismatched engine types: config={:?}, specific={:?}", 
                         self.storage_engine_type, engine_config.storage_engine_type);
                return Err(GraphError::ConfigurationError(
                    "engine_specific_config.storage_engine_type must match storage_engine_type".to_string()
                ));
            }
        }
        // Ensure port and cluster_range are not overridden
        if self.default_port == 0 {
            println!("DEBUG: Invalid default_port: {}", self.default_port);
            return Err(GraphError::ConfigurationError("default_port must be non-zero".to_string()));
        }
        if self.cluster_range.is_empty() {
            println!("DEBUG: Invalid cluster_range: {}", self.cluster_range);
            return Err(GraphError::ConfigurationError("cluster_range must be non-empty".to_string()));
        }
        Ok(self)
    }
}

/// Implements the `From` trait to allow easy conversion from the library's
/// `StorageConfig` to the CLI's `StorageConfig`. This is the core fix for the
/// type mismatch error.
impl From<LibStorageConfig> for StorageConfig {
    fn from(item: LibStorageConfig) -> Self {
        let engine_specific_config = item.engine_specific_config.map(|config_map| {
            let mut storage_config_inner = StorageConfigInner {
                path: None,
                host: None,
                port: None,
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            };

            if let Some(path) = config_map.get("path").and_then(|v| v.as_str()) {
                storage_config_inner.path = Some(PathBuf::from(path));
            } else {
                // Set default path based on engine type
                storage_config_inner.path = Some(PathBuf::from(format!(
                    "{}/{}",
                    DEFAULT_DATA_DIRECTORY,
                    item.storage_engine_type.to_string().to_lowercase()
                )));
            }
            if let Some(host) = config_map.get("host").and_then(|v| v.as_str()) {
                storage_config_inner.host = Some(host.to_string());
            } else {
                storage_config_inner.host = Some("127.0.0.1".to_string());
            }
            if let Some(port) = config_map.get("port").and_then(|v| v.as_u64()) {
                storage_config_inner.port = Some(port as u16);
            } else {
                storage_config_inner.port = Some(DEFAULT_STORAGE_PORT);
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

            SelectedStorageConfig {
                storage_engine_type: item.storage_engine_type,
                storage: storage_config_inner,
            }
        }).unwrap_or_else(|| create_default_engine_specific_config(&item.storage_engine_type).unwrap());

        StorageConfig {
            config_root_directory: Some(item.config_root_directory),
            data_directory: Some(item.data_directory),
            log_directory: Some(PathBuf::from(item.log_directory)),
            default_port: item.default_port,
            cluster_range: item.cluster_range,
            max_disk_space_gb: item.max_disk_space_gb,
            min_disk_space_gb: item.min_disk_space_gb,
            use_raft_for_scale: item.use_raft_for_scale,
            storage_engine_type: item.storage_engine_type,
            engine_specific_config: Some(engine_specific_config),
            max_open_files: item.max_open_files.unwrap_or(100) as u64,
        }
    }
}

impl From<StorageConfig> for LibStorageConfig {
    fn from(cli_config: StorageConfig) -> Self {
        LibStorageConfig {
            storage_engine_type: cli_config.storage_engine_type,
            config_root_directory: cli_config.config_root_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            data_directory: cli_config.data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
            log_directory: cli_config.log_directory
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|| DEFAULT_LOG_DIRECTORY.to_string()),
            default_port: cli_config.default_port,
            cluster_range: cli_config.cluster_range,
            max_disk_space_gb: cli_config.max_disk_space_gb,
            min_disk_space_gb: cli_config.min_disk_space_gb,
            use_raft_for_scale: cli_config.use_raft_for_scale,
            max_open_files: Some(
                cli_config.max_open_files.min(i32::MAX as u64) as i32
            ),
            engine_specific_config: cli_config.engine_specific_config.map(|esc| {
                let mut config_map = HashMap::new();
                
                // Convert storage_engine_type to string
                config_map.insert(
                    "storage_engine_type".to_string(), 
                    Value::String(esc.storage_engine_type.to_string())
                );
                
                // Convert storage config fields
                if let Some(path) = esc.storage.path {
                    config_map.insert(
                        "path".to_string(), 
                        Value::String(path.to_string_lossy().to_string())
                    );
                }
                
                if let Some(host) = esc.storage.host {
                    config_map.insert("host".to_string(), Value::String(host));
                }
                
                if let Some(port) = esc.storage.port {
                    config_map.insert("port".to_string(), Value::Number(port.into()));
                }
                
                if let Some(database) = esc.storage.database {
                    config_map.insert("database".to_string(), Value::String(database));
                }
                
                if let Some(username) = esc.storage.username {
                    config_map.insert("username".to_string(), Value::String(username));
                }
                
                if let Some(password) = esc.storage.password {
                    config_map.insert("password".to_string(), Value::String(password));
                }
                
                config_map
            }),
            connection_string: None, // Set to None as StorageConfig does not provide this
        }
    }
}

// The `From` implementation that converts the `RawStorageConfig` to `StorageConfig`
// Fixed From<RawStorageConfig> implementation with proper synchronization
impl From<RawStorageConfig> for StorageConfig {
    fn from(raw: RawStorageConfig) -> Self {
        // First, determine the storage engine type from the raw config or default
        let mut storage_engine_type = raw.storage_engine_type.unwrap_or_else(default_storage_engine_type);
        
        let engine_specific_config = if let Some(raw_config_map) = raw.engine_specific_config {
            let mut storage_config_inner = StorageConfigInner {
                path: None,
                host: None,
                port: None,
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            };
            
            // Extract values from the HashMap and set them on StorageConfigInner
            if let Some(path) = raw_config_map.get("path") {
                if let Some(s) = path.as_str() {
                    storage_config_inner.path = Some(PathBuf::from(s));
                }
            }
            if let Some(host) = raw_config_map.get("host") {
                if let Some(s) = host.as_str() {
                    storage_config_inner.host = Some(s.to_string());
                }
            }
            if let Some(port) = raw_config_map.get("port") {
                if let Some(p) = port.as_u64() {
                    storage_config_inner.port = Some(p as u16);
                }
            }
            if let Some(username) = raw_config_map.get("username") {
                if let Some(s) = username.as_str() {
                    storage_config_inner.username = Some(s.to_string());
                }
            }
            if let Some(password) = raw_config_map.get("password") {
                if let Some(s) = password.as_str() {
                    storage_config_inner.password = Some(s.to_string());
                }
            }
            if let Some(database) = raw_config_map.get("database") {
                if let Some(s) = database.as_str() {
                    storage_config_inner.database = Some(s.to_string());
                }
            }

            let selected_config = SelectedStorageConfig {
                storage_engine_type,
                storage: storage_config_inner,
            };

            Some(selected_config)
        } else {
            None
        };

        // CRITICAL FIX: If engine_specific_config exists and has a different engine type,
        // use that as the authoritative source
        if let Some(ref selected_config) = engine_specific_config {
            storage_engine_type = selected_config.storage_engine_type;
            eprintln!("DEBUG: Synchronized storage_engine_type to {:?} from engine_specific_config", storage_engine_type);
        }

        StorageConfig {
            config_root_directory: raw.config_root_directory,
            data_directory: raw.data_directory,
            log_directory: raw.log_directory,
            default_port: raw.default_port.unwrap_or_else(default_default_port),
            cluster_range: raw.cluster_range.unwrap_or_else(default_cluster_range),
            max_disk_space_gb: raw.max_disk_space_gb.unwrap_or_else(default_max_disk_space_gb),
            min_disk_space_gb: raw.min_disk_space_gb.unwrap_or_else(default_min_disk_space_gb),
            use_raft_for_scale: raw.use_raft_for_scale.unwrap_or_else(default_use_raft_for_scale),
            storage_engine_type, // Use the synchronized value
            engine_specific_config,
            max_open_files: raw.max_open_files.unwrap_or_else(default_max_open_files),
        }
    }
}

// This `impl` block provides a default instance of `SelectedStorageConfig`.
// It's useful for scenarios where no configuration is provided, ensuring
// the application can start with a sensible default.
impl Default for SelectedStorageConfig {
    fn default() -> Self {
        Self {
            storage_engine_type: StorageEngineType::Sled, // Use Sled as default
            storage: StorageConfigInner {
                path: Some(PathBuf::from(format!("{}/sled", DEFAULT_DATA_DIRECTORY))),
                port: Some(8049),
                host: Some("127.0.0.1".to_string()),
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            },
        }
    }
}

impl SelectedStorageConfig {
    pub fn load_from_yaml<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let message = format!("AND THIS WILL FAIL - SEE {}", path_str);
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file {:?}", path.as_ref()))?;
        serde_yaml2::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", path.as_ref()))
    }
}

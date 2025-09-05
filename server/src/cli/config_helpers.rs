use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn, trace};
use std::collections::{HashMap};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::env;
use std::fs;
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer, Deserializer};
use serde::de::{self, MapAccess, Visitor};
use serde_yaml2 as serde_yaml;
use serde_json::{self, Map, Value, value::Number};
use crate::cli::config_structs::*;
use crate::cli::config_constants::{DEFAULT_REST_CONFIG_PATH_RELATIVE, DEFAULT_CONFIG_ROOT_DIRECTORY_STR };
use super::daemon_management::{is_port_in_cluster_range, is_valid_cluster_range, parse_cluster_range}; // Import the correct helper function
pub use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig, 
                                      SelectedStorageConfig as LibSelectedStorageConfig,
                                      StorageConfigInner as LibStorageConfigInner};
use lib::query_exec_engine::query_exec_engine::{QueryExecEngine};


pub fn deserialize_engine_config<'de, D>(deserializer: D) -> Result<Option<SelectedStorageConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    // The YAML data for this field comes in as a HashMap
    let raw_config: Option<HashMap<String, Value>> = Option::deserialize(deserializer)?;

    if let Some(raw_map) = raw_config {
        // Use fully qualified method calls to avoid import issues
        let engine_type_value = raw_map.get("storage_engine_type")
            .ok_or_else(|| serde::de::Error::missing_field("storage_engine_type"))?;

        let storage_engine_type = StorageEngineType::deserialize(engine_type_value.clone())
            .map_err(serde::de::Error::custom)?;

        let mut storage_config_inner = StorageConfigInner {
            path: None,
            host: None,
            port: None,
            username: None,
            password: None,
            database: None,
            pd_endpoints: None,
        };

        if let Some(path_val) = raw_map.get("path") {
            if let Some(path_str) = path_val.as_str() {
                storage_config_inner.path = Some(PathBuf::from(path_str));
            }
        }
        if let Some(host_val) = raw_map.get("host") {
            if let Some(host_str) = host_val.as_str() {
                storage_config_inner.host = Some(host_str.to_string());
            }
        }
        if let Some(port_val) = raw_map.get("port") {
            if let Some(port_num) = port_val.as_u64() {
                storage_config_inner.port = Some(port_num as u16);
            }
        }
        if let Some(username_val) = raw_map.get("username") {
            if let Some(username_str) = username_val.as_str() {
                storage_config_inner.username = Some(username_str.to_string());
            }
        }
        if let Some(password_val) = raw_map.get("password") {
            if let Some(password_str) = password_val.as_str() {
                storage_config_inner.password = Some(password_str.to_string());
            }
        }
        if let Some(database_val) = raw_map.get("database") {
            if let Some(database_str) = database_val.as_str() {
                storage_config_inner.database = Some(database_str.to_string());
            }
        }

        // The fix: Initialize the struct with the now-available storage_engine_type.
        Ok(Some(SelectedStorageConfig {
            storage_engine_type,
            storage: storage_config_inner,
        }))
    } else {
        Ok(None)
    }
}

pub fn load_rest_config(config_file_path: Option<&str>) -> Result<RestApiConfig> {
    let default_config_path = PathBuf::from(DEFAULT_REST_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load REST API config from {:?}", path_to_use);

    if path_to_use.exists() {
        match fs::canonicalize(&path_to_use) {
            Ok(canonical_path) => {
                let config_content = fs::read_to_string(&canonical_path)
                    .context(format!("Failed to read REST API config file: {}", canonical_path.display()))?;
                debug!("REST API config content: {}", config_content);
                let wrapper: RestApiConfigWrapper = serde_yaml::from_str(&config_content)
                    .map_err(|e| {
                        error!("YAML parsing error for REST API at {:?}: {:?}", canonical_path, e);
                        if let Ok(partial) = serde_yaml::from_str::<Value>(&config_content) {
                            error!("Partial YAML parse: {:?}", partial);
                        }
                        anyhow!("Failed to parse REST API config YAML: {}", canonical_path.display())
                    })?;
                info!("Loaded REST API config: {:?}", wrapper.rest_api);
                Ok(wrapper.rest_api)
            }
            Err(e) => {
                warn!("Failed to canonicalize REST API config path {:?}", path_to_use);
                warn!("Config file not found at {}. Using default REST API config.", path_to_use.display());
                Ok(RestApiConfig::default())
            }
        }
    } else {
        warn!("Config file not found at {}. Using default REST API config.", path_to_use.display());
        Ok(RestApiConfig::default())
    }
}

pub fn save_rest_config(config: &RestApiConfig) -> Result<()> {
    let config_path = PathBuf::from(DEFAULT_REST_CONFIG_PATH_RELATIVE);

    let wrapper = RestApiConfigWrapper {
        config_root_directory: DEFAULT_CONFIG_ROOT_DIRECTORY_STR.to_string(),
        rest_api: config.clone(),
    };

    let yaml_string = serde_yaml::to_string(&wrapper)
        .context("Failed to serialize RestApiConfig to YAML")?;

    fs::create_dir_all(config_path.parent().unwrap())
        .context(format!("Failed to create parent directories for {}", config_path.display()))?;

    fs::write(&config_path, yaml_string)
        .context(format!("Failed to write RestApiConfig to file: {}", config_path.display()))?;

    Ok(())
}

// Helper function to convert StorageEngineType to String
pub fn daemon_api_storage_engine_type_to_string(engine_type: &StorageEngineType) -> String {
    match engine_type {
        StorageEngineType::Hybrid => "hybrid".to_string(),
        StorageEngineType::Sled => "sled".to_string(),
        StorageEngineType::RocksDB => "rocksdb".to_string(),
        StorageEngineType::TiKV => "tikv".to_string(),
        StorageEngineType::InMemory => "inmemory".to_string(),
        StorageEngineType::Redis => "redis".to_string(),
        StorageEngineType::PostgreSQL => "postgresql".to_string(),
        StorageEngineType::MySQL => "mysql".to_string(),
    }
}

// Helper function to map StorageEngineType to YAML file path
pub fn get_engine_config_path(engine_type: &StorageEngineType) -> Option<PathBuf> {
    match engine_type {
        StorageEngineType::Hybrid => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_HYBRID)),
        StorageEngineType::RocksDB => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB)),
        StorageEngineType::Sled => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED)),
        StorageEngineType::TiKV => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_TIKV)),
        StorageEngineType::PostgreSQL => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES)),
        StorageEngineType::MySQL => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL)),
        StorageEngineType::Redis => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS)),
        StorageEngineType::InMemory => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)),
        _ => None,
    }
}

// Function to create default engine-specific configuration
pub fn create_default_selected_storage_config(engine_type: &StorageEngineType) -> SelectedStorageConfig {
    let engine_path_name = engine_type.to_string().to_lowercase();
    let default_path = PathBuf::from(DEFAULT_DATA_DIRECTORY).join(&engine_path_name);

    let storage_config_inner = StorageConfigInner {
        path: Some(default_path),
        host: Some("127.0.0.1".to_string()),
        port: Some(DEFAULT_STORAGE_PORT), // Use 8049 from config_constants.rs for all engines
        username: None,
        password: None,
        database: None,
        pd_endpoints: if *engine_type == StorageEngineType::TiKV {
            Some("127.0.0.1:2379".to_string())
        } else {
            None
        },
    };

    SelectedStorageConfig {
        storage_engine_type: engine_type.clone(),
        storage: storage_config_inner,
    }
}

// --- Config Loading Functions ---
pub fn load_main_daemon_config(config_file_path: Option<&str>) -> Result<MainDaemonConfig> {
    let default_config_path = PathBuf::from(DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load Main Daemon config from {:?}", path_to_use);

    if path_to_use.exists() {
        match fs::canonicalize(&path_to_use) {
            Ok(canonical_path) => {
                let config_content = fs::read_to_string(&canonical_path)
                    .context(format!("Failed to read main daemon config file: {}", canonical_path.display()))?;
                debug!("Main Daemon config content: {}", config_content);
                let wrapper: MainConfigWrapper = serde_yaml::from_str(&config_content)
                    .map_err(|e| {
                        error!("YAML parsing error for Main Daemon at {:?}: {:?}", canonical_path, e);
                        if let Ok(partial) = serde_yaml::from_str::<Value>(&config_content) {
                            error!("Partial YAML parse: {:?}", partial);
                        }
                        anyhow!("Failed to parse main daemon config YAML: {}", canonical_path.display())
                    })?;
                info!("Successfully loaded Main Daemon config: {:?}", wrapper.main_daemon);
                Ok(wrapper.main_daemon)
            }
            Err(e) => {
                warn!("Failed to canonicalize Main Daemon config path {:?}", path_to_use);
                warn!("Config file not found at {}. Using default Main Daemon config.", path_to_use.display());
                Ok(MainDaemonConfig::default())
            }
        }
    } else {
        warn!("Config file not found at {}. Using default Main Daemon config.", path_to_use.display());
        Ok(MainDaemonConfig::default())
    }
}

pub fn hashmap_to_engine_specific_config(
    engine_type: StorageEngineType,
    map: HashMap<String, Value>,
) -> Result<SelectedStorageConfig, GraphError> {
    let path = map
        .get("path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from);
    
    let host = map
        .get("host")
        .and_then(|v| v.as_str())
        .map(String::from);
    
    let port = map
        .get("port")
        .and_then(|v| v.as_u64())
        .map(|p| p as u16);
    
    let username = map
        .get("username")
        .and_then(|v| v.as_str())
        .map(String::from);
    
    let password = map
        .get("password")
        .and_then(|v| v.as_str())
        .map(String::from);
    
    let database = map
        .get("database")
        .and_then(|v| v.as_str())
        .map(String::from);

    let pd_endpoints = map
        .get("pd_endpoints")
        .and_then(|v| v.as_str())
        .map(String::from);

    Ok(SelectedStorageConfig {
        storage_engine_type: engine_type,
        storage: StorageConfigInner {
            path,
            host,
            port,
            username,
            password,
            database,
            pd_endpoints,
        },
    })
}

// Loads engine-specific configuration
pub fn load_engine_specific_config(
    engine_type: StorageEngineType,
    base_path: &Path,
) -> Result<HashMap<String, Value>, GraphError> {
    let engine_config_path = base_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(format!("storage_config_{}.yaml", engine_type.to_string().to_lowercase()));
    info!(
        "Attempting to load engine-specific config for {:?} from {:?}",
        engine_type, engine_config_path
    );
    trace!("Constructed engine config path: {:?}", engine_config_path);

    let mut engine_specific_config: HashMap<String, Value> = HashMap::new();

    if engine_config_path.exists() {
        let content = fs::read_to_string(&engine_config_path).map_err(|e| {
            error!(
                "Failed to read engine-specific YAML file at {:?}: {}",
                engine_config_path, e
            );
            GraphError::Io(e)
        })?;
        debug!(
            "Raw engine-specific YAML content from {:?}:\n{}",
            engine_config_path, content
        );
        trace!("Successfully read content from engine config file.");

        let config: EngineConfig = serde_yaml::from_str(&content).map_err(|e| {
            error!(
                "Failed to deserialize engine-specific YAML from {:?}: {}",
                engine_config_path, e
            );
            GraphError::SerializationError(format!(
                "Failed to deserialize engine-specific YAML: {}",
                e
            ))
        })?;

        let mut storage_map = config.storage;

        if let Some(storage_engine_type_value) = storage_map.remove("storage_engine_type") {
            let storage_engine_type_str =
                storage_engine_type_value
                    .as_str()
                    .ok_or_else(|| GraphError::SerializationError(
                        "storage_engine_type value is not a string".to_string()
                    ))?;
            let storage_engine_type = StorageEngineType::from_str(storage_engine_type_str)
                .map_err(|e| GraphError::SerializationError(format!("Invalid storage_engine_type: {}", e)))?;
            engine_specific_config.insert("storage_engine_type".to_string(), Value::String(storage_engine_type.to_string().to_lowercase()));
        }

        for (key, value) in storage_map {
            engine_specific_config.insert(key, value);
        }

        debug!(
            "Deserialized engine-specific config: {:?}",
            engine_specific_config
        );
        trace!("Successfully deserialized engine-specific config.");
    } else {
        warn!(
            "Engine-specific config file not found at {:?}. Using defaults.",
            engine_config_path
        );
        trace!("Engine config file does not exist. Proceeding with default values.");
    }

    if matches!(engine_type, StorageEngineType::RocksDB | StorageEngineType::Sled) {
        if !engine_specific_config.contains_key("path") || engine_specific_config.get("path").and_then(|v| v.as_str()).unwrap_or("").is_empty() {
            let default_path = format!("{}/{}", "/opt/graphdb/storage_data", engine_type.to_string().to_lowercase());
            warn!("No valid 'path' in engine-specific config for {:?}, using default: {:?}", engine_type, default_path);
            engine_specific_config.insert("path".to_string(), Value::String(default_path));
        }
        if !engine_specific_config.contains_key("host") {
            debug!("No 'host' in engine-specific config for {:?}, using default: 127.0.0.1", engine_type);
            engine_specific_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
        }
        // Do not set a default port to preserve the port from the YAML file
    }
    if matches!(engine_type, StorageEngineType::TiKV) {
        if !engine_specific_config.contains_key("pd_endpoints") || engine_specific_config.get("pd_endpoints").and_then(|v| v.as_str()).unwrap_or("").is_empty() {
            let default_pd_endpoints = "127.0.0.1:2379";
            warn!("No valid 'pd_endpoints' in engine-specific config for TiKV, using default: {:?}", default_pd_endpoints);
            engine_specific_config.insert("pd_endpoints".to_string(), Value::String(default_pd_endpoints.to_string()));
        }
        if !engine_specific_config.contains_key("host") {
            debug!("No 'host' in engine-specific config for TiKV, using default: 127.0.0.1");
            engine_specific_config.insert("host".to_string(), Value::String("127.0.0.1".to_string()));
        }
        if !engine_specific_config.contains_key("port") {
            let default_port = 2379;
            debug!("No 'port' in engine-specific config for TiKV, using default: {}", default_port);
            engine_specific_config.insert("port".to_string(), Value::Number(default_port.into()));
        }
    }

    let storage_type_value = serde_json::to_value(engine_type.to_string().to_lowercase())
        .map_err(|e| GraphError::ConfigurationError(format!("Failed to serialize StorageEngineType: {}", e)))?;
    engine_specific_config.insert("storage_engine_type".to_string(), storage_type_value);
    trace!(
        "Final engine-specific config for {:?}: {:?}",
        engine_type,
        engine_specific_config
    );
    Ok(engine_specific_config)
}

pub fn load_cli_config() -> Result<CliConfigToml> {
    let default_config_path = PathBuf::from("/opt/graphdb/config.toml");
    let project_root = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."));
    let project_config_path = project_root.join("server/src/cli/config.toml");

    let config_path = if default_config_path.exists() {
        default_config_path
    } else if project_config_path.exists() {
        project_config_path
    } else {
        warn!("No CLI config file found at {} or {}. Falling back to default CLI config.", 
            default_config_path.display(), project_config_path.display());
        return Ok(CliConfigToml::default());
    };

    info!("Loading CLI config from {:?}", config_path);
    let config_content = fs::read_to_string(&config_path)
        .context(format!("Failed to read CLI config file: {}", config_path.display()))?;
    debug!("CLI config content: {}", config_content);

    let config: CliConfigToml = toml::from_str(&config_content)
        .map_err(|e| {
            error!("TOML parsing error for CLI config at {:?}: {:?}", config_path, e);
            if let Ok(partial) = toml::from_str::<Value>(&config_content) {
                error!("Partial TOML parse: {:?}", partial);
            }
            anyhow!("Failed to parse CLI config TOML: {}", config_path.display())
        })?;

    // If storage config is missing or incomplete, fall back to YAML
    if config.storage.storage_engine_type.is_none() {
        warn!("Storage engine type missing in TOML config, attempting to load from YAML");
        let storage_config = load_storage_config_from_yaml(None)
            .map_err(|e| anyhow!("Failed to load storage config from YAML: {}", e))?;
        let mut new_config = config.clone();
        new_config.storage = CliTomlStorageConfig {
            port: Some(storage_config.default_port),
            default_port: Some(storage_config.default_port),
            cluster_range: Some(storage_config.cluster_range),
            data_directory: storage_config.data_directory.map(|p| p.to_string_lossy().into_owned()),
            config_root_directory: storage_config.config_root_directory,
            log_directory: storage_config.log_directory.map(|p| p.to_string_lossy().into_owned()),
            max_disk_space_gb: Some(storage_config.max_disk_space_gb),
            min_disk_space_gb: Some(storage_config.min_disk_space_gb),
            use_raft_for_scale: Some(storage_config.use_raft_for_scale),
            storage_engine_type: Some(storage_config.storage_engine_type),
            max_open_files: Some(storage_config.max_open_files),
            config_file: None,
        };
        info!("Merged storage config from YAML: {:?}", new_config.storage);
        return Ok(new_config);
    }

    info!("Successfully loaded CLI config: {:?}", config);
    Ok(config)
}

// --- server/src/cli/config.rs: Revised load_storage_config_from_yaml function and new helper functions ---

// FIX: Re-instantiated the `create_default_engine_specific_config` function based on the compiler errors.
// It now correctly creates a `SelectedStorageConfig` which contains an `EngineSpecificConfig`.
// FIX: This function has been corrected to use the new struct definition.
// The previous errors were because it tried to return an enum variant which no longer exists.
// Now it correctly creates and returns an Option<SelectedStorageConfig>.
pub fn create_default_engine_specific_config(engine_type: &StorageEngineType) -> Option<SelectedStorageConfig> {
    let data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
    let engine_path = data_dir.join(engine_type.to_string().to_lowercase());
    
    match engine_type {
        StorageEngineType::Hybrid => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::Hybrid,
            storage: StorageConfigInner {
                path: Some(engine_path),
                host: Some("127.0.0.1".to_string()),
                port: Some(8049),
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            }
        }),
        StorageEngineType::RocksDB => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::RocksDB,
            storage: StorageConfigInner {
                path: Some(engine_path),
                host: Some("127.0.0.1".to_string()),
                port: Some(8049),
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            }
        }),
        StorageEngineType::Sled => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::Sled,
            storage: StorageConfigInner {
                path: Some(engine_path),
                host: Some("127.0.0.1".to_string()),
                port: Some(8049),
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            }
        }),
        StorageEngineType::TiKV => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::TiKV,
            storage: StorageConfigInner {
                path: Some(engine_path),
                host: Some("127.0.0.1".to_string()),
                port: Some(20160),
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            }
        }),
        StorageEngineType::PostgreSQL => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::PostgreSQL,
            storage: StorageConfigInner {
                path: Some(engine_path),
                host: Some("127.0.0.1".to_string()),
                port: Some(5432),
                username: Some("postgres".to_string()),
                password: Some("password".to_string()),
                database: Some("graphdb".to_string()),
                pd_endpoints: None,
            }
        }),
        StorageEngineType::MySQL => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::MySQL,
            storage: StorageConfigInner {
                path: Some(engine_path),
                host: Some("127.0.0.1".to_string()),
                port: Some(3306),
                username: Some("root".to_string()),
                password: Some("password".to_string()),
                database: Some("graphdb".to_string()),
                pd_endpoints: None,
            }
        }),
        StorageEngineType::Redis => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::Redis,
            storage: StorageConfigInner {
                path: Some(engine_path),
                host: Some("127.0.0.1".to_string()),
                port: Some(6379),
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            }
        }),
        StorageEngineType::InMemory => Some(SelectedStorageConfig {
            storage_engine_type: StorageEngineType::InMemory,
            storage: StorageConfigInner {
                path: None,
                host: None,
                port: None,
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
            }
        }),
    }
}

pub fn load_daemon_config(config_file_path: Option<&str>) -> Result<DaemonYamlConfig> {
    let default_config_path = PathBuf::from(DEFAULT_DAEMON_CONFIG_PATH_RELATIVE);

    let path_to_use = config_file_path
        .map(PathBuf::from)
        .unwrap_or(default_config_path);

    info!("Attempting to load Daemon config from {:?}", path_to_use);

    if path_to_use.exists() {
        match fs::canonicalize(&path_to_use) {
            Ok(canonical_path) => {
                let config_content = fs::read_to_string(&canonical_path)
                    .context(format!("Failed to read daemon config file: {}", canonical_path.display()))?;
                debug!("Daemon config content: {}", config_content);
                let config: DaemonYamlConfig = serde_yaml::from_str(&config_content)
                    .map_err(|e| {
                        error!("YAML parsing error for Daemon at {:?}: {:?}", canonical_path, e);
                        if let Ok(partial) = serde_yaml::from_str::<Value>(&config_content) {
                            error!("Partial YAML parse: {:?}", partial);
                        }
                        anyhow!("Failed to parse daemon config YAML: {}", canonical_path.display())
                    })?;
                info!("Loaded Daemon config: {:?}", config);
                Ok(config)
            }
            Err(e) => {
                warn!("Failed to canonicalize Daemon config path {:?}", path_to_use);
                warn!("Config file not found at {}. Using default Daemon config.", path_to_use.display());
                Ok(DaemonYamlConfig::default())
            }
        }
    } else {
        warn!("Config file not found at {}. Using default Daemon config.", path_to_use.display());
        Ok(DaemonYamlConfig::default())
    }
}

pub fn save_daemon_config(config: &DaemonYamlConfig) -> Result<()> {
    let config_path = PathBuf::from(DEFAULT_DAEMON_CONFIG_PATH_RELATIVE);

    let yaml_string = serde_yaml::to_string(config)
        .context("Failed to serialize DaemonYamlConfig to YAML")?;

    fs::create_dir_all(config_path.parent().unwrap())
        .context(format!("Failed to create parent directories for {}", config_path.display()))?;

    fs::write(&config_path, yaml_string)
        .context(format!("Failed to write DaemonYamlConfig to file: {}", config_path.display()))?;

    Ok(())
}

pub fn get_daemon_cluster_range() -> String {
    load_daemon_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| DaemonYamlConfig::default().cluster_range)
}

pub fn get_rest_cluster_range() -> String {
    load_rest_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| RestApiConfig::default().cluster_range)
}

// FIX: New helper function to validate and populate missing fields after deserialization.
// This ensures that `config_root_directory` and `engine_specific_config` are never `None`
pub fn ensure_storage_config_is_valid(mut config: StorageConfig) -> StorageConfig {
    // Ensure config_root_directory is set
    if config.config_root_directory.is_none() {
        warn!("'config_root_directory' was missing or invalid, setting to default: {}", DEFAULT_CONFIG_ROOT_DIRECTORY_STR);
        config.config_root_directory = Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR));
    }

    // Ensure data_directory is set
    if config.data_directory.as_ref().map_or(true, |p| p.as_os_str().is_empty()) {
        warn!("No data_directory specified in config, applying default: {:?}", default_data_directory());
        config.data_directory = default_data_directory();
    }

    // Ensure log_directory is set
    if config.log_directory.as_ref().map_or(true, |p| p.as_os_str().is_empty()) {
        warn!("No log_directory specified in config, using default: {:?}", default_log_directory());
        config.log_directory = default_log_directory();
    }

    // Validate cluster range
    if let Err(e) = parse_cluster_range(&config.cluster_range) {
        warn!("Invalid cluster range in config: {}. Using default: {}", e, default_cluster_range());
        config.cluster_range = default_cluster_range();
    }

    // Validate port range
    if config.default_port < 1024 || config.default_port > 65535 {
        warn!("Invalid port number: {}. Using default: {}", config.default_port, storage_config_serde::default_default_port());
        config.default_port = storage_config_serde::default_default_port();
    }

    // Ensure engine_specific_config is consistent with storage_engine_type
    if config.engine_specific_config.is_none() {
        info!("'engine_specific_config' was missing, setting to default for engine: {:?}", config.storage_engine_type);
        config.engine_specific_config = create_default_engine_specific_config(&config.storage_engine_type);
    } else if let Some(engine_config) = &config.engine_specific_config {
        if config.storage_engine_type != engine_config.storage_engine_type {
            warn!(
                "Top-level storage_engine_type ({:?}) does not match engine_specific_config ({:?}). Updating engine_specific_config.",
                config.storage_engine_type, engine_config.storage_engine_type
            );
            let mut storage = engine_config.storage.clone();
            storage.path = Some(PathBuf::from(format!(
                "{}/{}",
                config.data_directory.as_ref().map_or(DEFAULT_DATA_DIRECTORY.to_string(), |p| p.to_string_lossy().to_string()),
                config.storage_engine_type.to_string().to_lowercase()
            )));
            storage.port = Some(config.default_port); // Ensure port consistency
            config.engine_specific_config = Some(SelectedStorageConfig {
                storage_engine_type: config.storage_engine_type,
                storage,
            });
        }
    }

    // Create directories if they don't exist
    if let Some(ref data_dir) = config.data_directory {
        if let Err(e) = fs::create_dir_all(data_dir) {
            error!("Failed to create data directory {:?}: {}", data_dir, e);
        } else {
            info!("Ensured data directory exists: {:?}", data_dir);
        }
    }

    if let Some(ref log_dir) = config.log_directory {
        if let Err(e) = fs::create_dir_all(log_dir) {
            error!("Failed to create log directory {:?}: {}", log_dir, e);
        } else {
            info!("Ensured log directory exists: {:?}", log_dir);
        }
    }

    // Log the final validated config
    info!(
        "Validated storage config: default_port={}, cluster_range={}, data_directory={:?}, log_directory={:?}, config_root_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        config.default_port,
        config.cluster_range,
        config.data_directory,
        config.log_directory,
        config.config_root_directory,
        config.storage_engine_type,
        config.engine_specific_config,
        config.max_disk_space_gb,
        config.min_disk_space_gb,
        config.use_raft_for_scale
    );
    println!(
        "[DEBUG] => Validated config: default_port={}, cluster_range={}, data_directory={:?}, log_directory={:?}, config_root_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        config.default_port,
        config.cluster_range,
        config.data_directory,
        config.log_directory,
        config.config_root_directory,
        config.storage_engine_type,
        config.engine_specific_config,
        config.max_disk_space_gb,
        config.min_disk_space_gb,
        config.use_raft_for_scale
    );

    config
}

pub fn load_storage_config_from_yaml(config_file_path: Option<PathBuf>) -> Result<StorageConfig, GraphError> {
    // Determine the path to use for loading the main configuration file
    let main_config_path = config_file_path.unwrap_or_else(|| {
        let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        if project_config_path.exists() {
            debug!("Using project config path: {:?}", project_config_path);
            project_config_path
        } else {
            let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
            debug!("Using default config path: {:?}", default_config_path);
            default_config_path
        }
    });

    // Log the file being read
    debug!("Reading configuration from file: {:?}", main_config_path);

    // Use StorageConfig::load to handle the loading and validation
    let config = StorageConfig::load(&main_config_path)?;

    // Log the final validated configuration
    debug!("Final validated configuration: {:?}", config);
    Ok(config)
}

// Fixed helper function for string paths
pub fn load_storage_config_str(config_file_path: Option<&str>) -> Result<StorageConfig, anyhow::Error> {
    let path = config_file_path.map(PathBuf::from);
    load_storage_config_from_yaml(path)
        .map_err(|e| anyhow::anyhow!("Failed to load storage config: {}", e))
}

// Helper function to validate cluster range
pub fn validate_cluster_range(range: &str, port: u16) -> bool {
    if range.contains('-') {
        let parts: Vec<&str> = range.split('-').collect();
        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                return port >= start && port <= end;
            }
        }
    } else if let Ok(single_port) = range.parse::<u16>() {
        return port == single_port;
    }
    false
}

pub async fn handle_exec_command(_engine: Arc<QueryExecEngine>, command: String) -> Result<()> {
    // This is a placeholder function to show the fix for the unused `engine` variable.
    // Replace this with your actual implementation.
    println!("Executing command: {}", command);
    Ok(())
}

pub fn map_cli_to_lib_storage_config(cli_config: StorageConfig) -> LibStorageConfig {
    let engine_specific_config = cli_config.engine_specific_config.map(|esc| {
        let mut map = HashMap::new();
        map.insert(
            "storage_engine_type".to_string(),
            Value::String(esc.storage_engine_type.to_string().to_lowercase()),
        );
        map.insert(
            "path".to_string(),
            Value::String(
                esc.storage.path
                    .map_or("".to_string(), |p| p.to_string_lossy().to_string()),
            ),
        );
        map.insert(
            "host".to_string(),
            Value::String(esc.storage.host.unwrap_or_default()),
        );
        map.insert(
            "port".to_string(),
            Value::Number(esc.storage.port.unwrap_or(0).into()),
        );
        map.insert(
            "database".to_string(),
            Value::String(esc.storage.database.unwrap_or_default()),
        );
        map.insert(
            "username".to_string(),
            Value::String(esc.storage.username.unwrap_or_default()),
        );
        map.insert(
            "password".to_string(),
            Value::String(esc.storage.password.unwrap_or_default()),
        );
        map
    });

    LibStorageConfig {
        storage_engine_type: cli_config.storage_engine_type,
        config_root_directory: cli_config.config_root_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
        data_directory: cli_config.data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
        log_directory: cli_config.log_directory.map_or_else(|| DEFAULT_LOG_DIRECTORY.to_string(), |p| p.to_string_lossy().to_string()),
        default_port: cli_config.default_port,
        cluster_range: cli_config.cluster_range,
        max_disk_space_gb: cli_config.max_disk_space_gb,
        min_disk_space_gb: cli_config.min_disk_space_gb,
        use_raft_for_scale: cli_config.use_raft_for_scale,
        max_open_files: Some(cli_config.max_open_files as i32), // Assuming lib::StorageConfig uses Option<i32>
        engine_specific_config,
        connection_string: None, // Set to None as StorageConfig does not provide this
    }
}


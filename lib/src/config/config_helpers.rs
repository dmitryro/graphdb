use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, warn, trace};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::env;
use std::fs;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_yaml2 as serde_yaml;
use serde_json::{self, Map, Value};
use futures::FutureExt;
pub use crate::config::config_defaults::*;
pub use crate::config::config_structs::*;
pub use crate::config::config_constants::*;
pub use crate::config::config_serializers::*;
use crate::daemon_utils::{is_port_in_cluster_range, is_valid_cluster_range, parse_cluster_range};
use crate::query_exec_engine::query_exec_engine::{QueryExecEngine};
use models::errors::{GraphError, GraphResult};

pub fn deserialize_engine_config<'de, D>(deserializer: D) -> Result<Option<SelectedStorageConfig>, D::Error>
where
    D: serde::de::Deserializer<'de>, // Fixed: Use serde::de::Deserializer
{
    let raw_config: Option<HashMap<String, Value>> = Option::deserialize(deserializer)?;

    if let Some(raw_map) = raw_config {
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
            cache_capacity: Some(1024*1024*1024),
            use_compression: true,
            temporary: false,
            use_raft_for_scale: false,
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

pub fn create_default_selected_storage_config(engine_type: &StorageEngineType) -> SelectedStorageConfig {
    let engine_path_name = engine_type.to_string().to_lowercase();
    let default_path = PathBuf::from(DEFAULT_DATA_DIRECTORY).join(&engine_path_name);

    let storage_config_inner = StorageConfigInner {
        path: Some(default_path),
        host: Some("127.0.0.1".to_string()),
        port: Some(DEFAULT_STORAGE_PORT),
        username: None,
        password: None,
        database: None,
        pd_endpoints: if *engine_type == StorageEngineType::TiKV {
            Some("127.0.0.1:2379".to_string())
        } else {
            None
        },
        cache_capacity: Some(1024*1024*1024),
        use_compression: true,
        temporary: false, 
        use_raft_for_scale: false,
    };

    SelectedStorageConfig {
        storage_engine_type: engine_type.clone(),
        storage: storage_config_inner,
    }
}

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

    let cache_capacity = map
        .get("cache_capacity")
        .and_then(|v| v.as_u64());

    // Fix 1: The `use_compression` value from the map should be parsed as a boolean, not a u64.
    // Fix 2: Provide a default value (`false`) if the key is not present or the value is not a bool.
    let use_compression = map
        .get("use_compression")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let temporary = map
        .get("temporary")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let use_raft_for_scale = map
        .get("use_raft_for_scale")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

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
            cache_capacity,
            use_compression,
            temporary,
            use_raft_for_scale,
        },
    })
}

pub fn load_engine_specific_config(
    engine_type: StorageEngineType,
    base_path: &Path,
) -> Result<SelectedStorageConfig, GraphError> {
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
            GraphError::Io(e.to_string())
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

        let storage_map = config.storage;

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
    hashmap_to_engine_specific_config(engine_type, engine_specific_config)
}

pub async fn load_cli_config() -> Result<CliConfigToml> {
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

    if config.storage.storage_engine_type.is_none() {
        warn!("Storage engine type missing in TOML config, attempting to load from YAML");
        let storage_config = load_storage_config_from_yaml(None)
            .await
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
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
                cache_capacity: Some(1024*1024*1024),
                use_compression: true,
                temporary: false,
                use_raft_for_scale: false,
            }
        }),
    }
}

pub async fn load_daemon_config(config_file_path: Option<&str>) -> Result<DaemonConfig> {
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
                let config: DaemonConfig = serde_yaml::from_str(&config_content)
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
                Ok(DaemonConfig {
                    port: None,
                    process_name: "graphdb".to_string(),
                    user: "graphdb".to_string(),
                    group: "graphdb".to_string(),
                    default_port: 8049,
                    cluster_range: String::from("8000-9000"),
                    max_connections: 300,
                    max_open_files: 100,
                    use_raft_for_scale: false,
                    log_level: "DEBUG".to_string(),
                    log_directory :Some("/opt/graphdb/logs".to_string()),
                })
            }
        }
    } else {
        warn!("Config file not found at {}. Using default Daemon config.", path_to_use.display());
        Ok(DaemonConfig {
            port: None,
            process_name: "graphdb".to_string(),
            user: "graphdb".to_string(),
            group: "graphdb".to_string(),
            default_port: 8049,
            cluster_range: String::from("8000-9000"),
            max_connections: 300,
            max_open_files: 100,
            use_raft_for_scale: false,
            log_level: "DEBUG".to_string(),
            log_directory: Some("/opt/graphdb/logs".to_string()),
        })
    }
}

pub fn save_daemon_config(config: &DaemonConfig) -> Result<()> {
    let config_path = PathBuf::from(DEFAULT_DAEMON_CONFIG_PATH_RELATIVE);

    let yaml_string = serde_yaml::to_string(config)
        .context("Failed to serialize DaemonConfig to YAML")?;

    fs::create_dir_all(config_path.parent().unwrap())
        .context(format!("Failed to create parent directories for {}", config_path.display()))?;

    fs::write(&config_path, yaml_string)
        .context(format!("Failed to write DaemonConfig to file: {}", config_path.display()))?;

    Ok(())
}

/// Returns the cluster range for the daemon configuration.
pub async fn get_daemon_cluster_range() -> String {
    load_daemon_config(None)
        .await
        .map(|cfg: DaemonConfig| cfg.cluster_range)
        .unwrap_or_else(|_e| "8080-8082".to_string())
}

pub fn get_rest_cluster_range() -> String {
    load_rest_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| RestApiConfig::default().cluster_range)
}

pub fn ensure_storage_config_is_valid(mut config: StorageConfig) -> StorageConfig {
    if config.config_root_directory.is_none() {
        warn!("'config_root_directory' was missing or invalid, setting to default: {}", DEFAULT_CONFIG_ROOT_DIRECTORY_STR);
        config.config_root_directory = Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR));
    }

    if config.data_directory.as_ref().map_or(true, |p| p.as_os_str().is_empty()) {
        warn!("No data_directory specified in config, applying default: {:?}", default_data_directory());
        config.data_directory = default_data_directory();
    }

    if config.log_directory.as_ref().map_or(true, |p| p.as_os_str().is_empty()) {
        warn!("No log_directory specified in config, using default: {:?}", default_log_directory());
        config.log_directory = default_log_directory();
    }

    if let Err(e) = parse_cluster_range(&config.cluster_range) {
        warn!("Invalid cluster range in config: {}. Using default: {}", e, default_cluster_range());
        config.cluster_range = default_cluster_range();
    }

    if config.default_port < 1024 || config.default_port > 65535 {
        warn!("Invalid port number: {}. Using default: {}", config.default_port, storage_config_serde::default_default_port());
        config.default_port = storage_config_serde::default_default_port();
    }

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
            storage.port = Some(config.default_port);
            config.engine_specific_config = Some(SelectedStorageConfig {
                storage_engine_type: config.storage_engine_type,
                storage,
            });
        }
    }

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

pub async fn load_storage_config_from_yaml(config_file_path: Option<PathBuf>) -> Result<StorageConfig, GraphError> {
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

    debug!("Reading configuration from file: {:?}", main_config_path);

    let config = StorageConfig::load(&main_config_path).await?;

    debug!("Final validated configuration: {:?}", config);
    Ok(config)
}

pub async fn load_storage_config_str(config_file_path: Option<&str>) -> Result<StorageConfig, anyhow::Error> {
    let path = config_file_path.map(PathBuf::from);
    load_storage_config_from_yaml(path)
        .await
        .map_err(|e| anyhow!("Failed to load storage config: {}", e))
}

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
    println!("Executing command: {}", command);
    Ok(())
}

pub fn available_engines() -> Vec<StorageEngineType> {
    let mut engines = vec![StorageEngineType::Sled];
    #[cfg(feature = "rocksdb")]
    engines.push(StorageEngineType::RocksDB);
    #[cfg(feature = "tikv")]
    engines.push(StorageEngineType::TiKV);
    #[cfg(any(feature = "sled", feature = "rocksdb", feature = "tikv"))]
    engines.push(StorageEngineType::Hybrid);
    #[cfg(feature = "redis")]
    engines.push(StorageEngineType::Redis);
    #[cfg(feature = "postgresql")]
    engines.push(StorageEngineType::PostgreSQL);
    #[cfg(feature = "mysql")]
    engines.push(StorageEngineType::MySQL);
    #[cfg(feature = "inmemory")]
    engines.push(StorageEngineType::InMemory);
    debug!("Available storage engines: {:?}", engines);
    engines
}

pub fn is_engine_specific_config_complete(config: &Option<SelectedStorageConfig>) -> bool {
    if let Some(engine_config) = config {
        engine_config.storage.path.is_some() || engine_config.storage.host.is_some()
    } else {
        false
    }
}

pub fn format_engine_config(storage_config: &StorageConfig) -> Vec<String> {
    trace!("Formatting engine config for {:?}", storage_config);
    let mut config_lines = Vec::new();
    
    config_lines.push(format!("Engine: {}", storage_config.storage_engine_type));
    trace!("Added engine type: {}", storage_config.storage_engine_type);
    
    if let Some(ref engine_config_map) = storage_config.engine_specific_config {
        config_lines.push("  Engine-Specific Configuration:".to_string());
        trace!("Processing engine_specific_config: {:?}", engine_config_map);
        if let Some(path) = &engine_config_map.storage.path {
            config_lines.push(format!("    path: {:?}", path));
        }
        if let Some(host) = &engine_config_map.storage.host {
            config_lines.push(format!("    host: {}", host));
        }
        if let Some(port) = engine_config_map.storage.port {
            config_lines.push(format!("    port: {}", port));
        }
        if let Some(username) = &engine_config_map.storage.username {
            config_lines.push(format!("    username: {}", username));
        }
        if let Some(password) = &engine_config_map.storage.password {
            config_lines.push(format!("    password: {}", password));
        }
        if let Some(database) = &engine_config_map.storage.database {
            config_lines.push(format!("    database: {}", database));
        }
        if let Some(pd_endpoints) = &engine_config_map.storage.pd_endpoints {
            config_lines.push(format!("    pd_endpoints: {}", pd_endpoints));
        }
    } else {
        config_lines.push("  Config: Using default configuration".to_string());
        trace!("No engine_specific_config, using default configuration");
    }
    
    config_lines.push(format!("  Max Open Files: {}", storage_config.max_open_files));
    trace!("Added max_open_files: {}", storage_config.max_open_files);
    
    config_lines.push(format!("  Max Disk Space: {} GB", storage_config.max_disk_space_gb));
    trace!("Added max_disk_space_gb: {}", storage_config.max_disk_space_gb);
    config_lines.push(format!("  Min Disk Space: {} GB", storage_config.min_disk_space_gb));
    trace!("Added min_disk_space_gb: {}", storage_config.min_disk_space_gb);
    config_lines.push(format!("  Use Raft: {}", storage_config.use_raft_for_scale));
    trace!("Added use_raft_for_scale: {}", storage_config.use_raft_for_scale);
    
    trace!("Returning config lines: {:?}", config_lines);
    config_lines
}

pub fn create_default_yaml_config(yaml_path: &PathBuf, engine_type: StorageEngineType) -> Result<(), GraphError> {
    info!("Creating default YAML config at {:?}", yaml_path);
    trace!("Creating default config for engine_type: {:?}", engine_type);
    
    let mut config = StorageConfig::default();
    config.storage_engine_type = engine_type;
    trace!("Set storage_engine_type to: {:?}", engine_type);
    
    let engine_path = match engine_type {
        StorageEngineType::Sled => format!("{}/sled", DEFAULT_DATA_DIRECTORY),
        StorageEngineType::RocksDB => format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY),
        StorageEngineType::Redis => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::PostgreSQL => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::MySQL => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::InMemory => DEFAULT_DATA_DIRECTORY.to_string(),
        StorageEngineType::TiKV => format!("{}/tikv", DEFAULT_DATA_DIRECTORY),
        StorageEngineType::Hybrid => format!("{}/hybrid", DEFAULT_DATA_DIRECTORY),
    };
    
    let engine_port = match engine_type {
        StorageEngineType::Redis => 6379,
        StorageEngineType::PostgreSQL => 5432,
        StorageEngineType::MySQL => 3306,
        _ => config.default_port,
    };
    
    trace!("Setting engine_path: {}, engine_port: {}", engine_path, engine_port);
    
    config.engine_specific_config = Some(SelectedStorageConfig {
        storage_engine_type: engine_type,
        storage: StorageConfigInner {
            path: Some(PathBuf::from(engine_path)),
            host: Some("127.0.0.1".to_string()),
            port: Some(engine_port),
            username: None,
            password: None,
            database: None,
            pd_endpoints: None,
            cache_capacity: Some(1024*1024*1024),
            use_compression: true,
            temporary: false,
            use_raft_for_scale: false,
        }
    });
    
    trace!("Set engine_specific_config: {:?}", config.engine_specific_config);
    config.data_directory = Some(PathBuf::from(DEFAULT_DATA_DIRECTORY));
    trace!("Set data_directory: {:?}", config.data_directory);
    config.default_port = engine_port;
    trace!("Set default_port: {}", config.default_port);
    config.cluster_range = engine_port.to_string();
    trace!("Set cluster_range: {}", config.cluster_range);
    
    let wrapper = StorageConfigWrapper { storage: config };
    trace!("Created StorageConfigWrapper: {:?}", wrapper);
    
    let yaml_content = serde_yaml::to_string(&wrapper)
        .map_err(|e| {
            error!("Failed to serialize default config: {}", e);
            trace!("Serialization error: {:?}", e);
            GraphError::SerializationError(format!("Failed to serialize default config: {}", e))
        })?;
    
    debug!("Generated YAML content:\n{}", yaml_content);
    
    if let Some(parent) = yaml_path.parent() {
        trace!("Creating parent directories: {:?}", parent);
        fs::create_dir_all(parent)
            .map_err(|e| {
                error!("Failed to create parent directories for {:?}: {}", yaml_path, e);
                trace!("Directory creation error: {:?}", e);
                GraphError::Io(e.to_string())
            })?;
    }
    
    fs::write(yaml_path, yaml_content)
        .map_err(|e| {
            error!("Failed to write YAML config to {:?}: {}", yaml_path, e);
            trace!("Write error: {:?}", e);
            GraphError::Io(e.to_string())
        })?;
    
    info!("Created default config file at {:?}", yaml_path);
    trace!("Default config file written successfully");
    Ok(())
}

/// Creates a default YAML configuration file
pub async fn create_default_storage_yaml_config(yaml_path: &PathBuf, engine_type: StorageEngineType) -> GraphResult<()> {
    info!("Creating default YAML config at {:?}", yaml_path);
    let config = StorageConfig {
        storage_engine_type: engine_type,
        config_root_directory: Some(PathBuf::from("./storage_daemon_server")),
        data_directory: Some(PathBuf::from(DEFAULT_DATA_DIRECTORY)),
        log_directory: Some(PathBuf::from(DEFAULT_LOG_DIRECTORY)),
        default_port: DEFAULT_STORAGE_PORT,
        cluster_range: DEFAULT_STORAGE_PORT.to_string(),
        max_disk_space_gb: 1000,
        min_disk_space_gb: 10,
        use_raft_for_scale: true,
        max_open_files: 100u64,
        engine_specific_config: Some(SelectedStorageConfig {
            storage_engine_type: engine_type,
            storage: StorageConfigInner {
                path: Some(PathBuf::from(format!("{}/{}", DEFAULT_DATA_DIRECTORY, engine_type.to_string().to_lowercase()))),
                host: Some("127.0.0.1".to_string()),
                port: Some(DEFAULT_STORAGE_PORT),
                username: None,
                password: None,
                database: None,
                pd_endpoints: None,
                cache_capacity: Some(1024 * 1024 * 1024),
                use_compression: false,
                temporary: false,
                use_raft_for_scale: false,
            },
        }),
    };

    config.save().await
        .map_err(|e| {
            error!("Failed to save default YAML config to {:?}: {}", yaml_path, e);
            // This is the corrected line. It converts the error `e` into a String.
            GraphError::Io(e.to_string())
        })?;

    info!("Default YAML config created at {:?}", yaml_path);
    Ok(())
}

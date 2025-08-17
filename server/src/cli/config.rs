// server/src/cli/config.rs
// ADDED: 2025-08-08 - Added `enable_plugins` field to `CliConfigToml`. Implemented `save` method for `CliConfigToml` to serialize to TOML file. Replaced `std::fs` with `fs` and ensured consistent use of `serde_yaml2`.
// UPDATED: 2025-08-08 - Changed `load_cli_config` to use `/opt/graphdb/config.toml` instead of `config.toml` in `CARGO_MANIFEST_DIR`.
// UPDATED: 2025-08-08 - Updated `load_cli_config` to load from `server/src/cli/config.toml` using `CARGO_MANIFEST_DIR` and save to `/opt/graphdb/config.toml`.
// FIXED: 2025-08-08 - Corrected typo in `DEFAULT_STORAGE_CONFIG_PATH_POSTGRES` from `posgres` to `postgres`.
// ADDED: 2025-08-08 - Added `get_engine_config_path` to map `StorageEngineType` to YAML file paths.
// ADDED: 2025-08-08 - Added `load_engine_specific_config` to load engine-specific configurations using `SelectedStorageConfig`.
// UPDATED: 2025-08-09 - Restored `engine_specific_config: Option<SelectedStorageConfig>` in `StorageConfig` as a structured sub-object.
// FIXED: 2025-08-09 - Replaced `StorageConfigInner` with `StorageEngineConfig` for clarity and removed redundant `storage_engine_type`.
// FIXED: 2025-08-09 - Updated `SelectedStorageConfig::default` to align with `StorageConfig::default`.
// FIXED: 2025-08-09 - Ensured `load_engine_specific_config` loads engine-specific YAML files correctly.
// FIXED: 2025-08-09 - Added `Redis` branch to `get_engine_config_path` to handle `StorageEngineType::Redis`.
// UPDATED: 2025-08-08 - Reverted `StorageEngineConfig` back to `StorageConfigInner` per user request, maintaining same fields and serialization.
// ADDED: 2025-08-09 - Added `UseStorage` subcommand to `Commands` for switching storage engines via `StorageEngineManager`.
// UPDATED: 2025-08-09 - Added `execute` method to `CliConfig` to handle `UseStorage` command with session-based or permanent engine switching.
// FIXED: 2025-08-10 - Corrected `Commands::UseStorage` to `Commands::Use(UseAction::Storage)` in match arms to fix E0599 errors.
// FIXED: 2025-08-09 - Added `permanent` boolean field to `UseAction::Storage` and `CommandType::UseStorage` to resolve `E0026` error.
// UPDATED: 2025-08-09 - Modified `CliConfig::execute` to respect `--permanent` flag in both interactive and non-interactive modes, allowing non-permanent switches in non-interactive mode.
// ADDED: 2025-08-09 - Added `Save` command handling in `execute` for `SaveAction::Storage` and `SaveAction::Configuration` to save storage and config changes.
// UPDATED: 2025-08-09 - Modified `UseAction::Storage` to save engine changes when `--permanent` is true, equivalent to `use storage` followed by `save storage`.
// FIXED: 2025-08-09 - Restored truncated portion of `config.rs` from `load_rest_config` to end of file.
// FIXED: 2025-08-09 - Corrected file truncation and removed test module per user request, ensuring full file content from start.
// FIXED: 2025-08-09 - Corrected `SaveConfiguration` to `SaveConfig` in `CliConfig::load` to fix E0599 error.
// ADDED: 2025-08-11 - Added custom deserializer for `StorageEngineType` to handle `RocksDB`, `PostgreSQL`, and `MySQL` in TOML.
// ADDED: 2025-08-11 - Modified `load_cli_config` to fall back to `load_storage_config_from_yaml` for storage config if TOML parsing fails.
// FIXED: 2025-08-11 - Added `option_storage_engine_type_serde` module to handle `Option<StorageEngineType>` in `CliTomlStorageConfig` to fix E0308 errors.
// FIXED: 2025-08-11 - Fixed type mismatches for `data_directory` field in `StorageConfig` struct - changed from `Option<PathBuf>` to `PathBuf` and updated related serialization and method calls.
// FIXED: 2025-08-12 - Updated `daemon_api_storage_engine_type_to_string` to return lowercase strings for consistency with YAML serialization.
// FIXED: 2025-08-12 - Ensured `CliConfig::execute` correctly updates and saves storage engine type for `UseAction::Storage`.
// UPDATED: 2025-08-13 - Removed complex custom deserializer for `StorageConfig` and replaced with standard serde derive.
// UPDATED: 2025-08-13 - Updated `load_storage_config_from_yaml` to include directory creation and cluster range validation.
// FIXED: 2025-08-13 - Corrected `load_engine_specific_config` to use proper `config_root` path.
// UPDATED: 2025-08-13 - Updated default paths to use relative paths (`./storage_daemon_server`).

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize, Serializer, Deserializer};
use serde_json::Value;
use serde_yaml2 as serde_yaml;
use std::collections::{HashMap};
use std::env;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use dirs;
use toml;
use lib::storage_engine::config::StorageConfig as EngineStorageConfig;
use crate::cli::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction, 
                           ReloadAction, RestCliCommand, StatusAction, StorageAction, 
                           StartAction, StopAction, StopArgs, DaemonCliCommand, UseAction, SaveAction};
use super::daemon_management::{is_port_in_cluster_range, is_valid_cluster_range, parse_cluster_range}; // Import the correct helper function
pub use lib::storage_engine::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
pub use lib::storage_engine::config::{EngineTypeOnly, RocksdbConfig, SledConfig};
pub use models::errors::GraphError;
pub use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig};
pub use crate::cli::serializers::{string_or_u16, string_or_u16_non_option};
use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};


// --- Custom PathBuf Serialization Module ---
// This module is responsible for serializing a non-optional PathBuf.
mod path_buf_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    // The serialize function now correctly accepts a reference to a PathBuf.
    pub fn serialize<S>(path: &PathBuf, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&path.to_string_lossy())
    }

    // The deserialize function now correctly returns a PathBuf.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        Ok(PathBuf::from(s))
    }
}

// --- Custom Option<PathBuf> Serialization Module ---
// This module is responsible for handling the Option wrapper.
pub mod option_path_buf_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(opt_path: &Option<PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match opt_path {
            // The `path` is a `&PathBuf`, which is now the expected type for path_buf_serde::serialize.
            Some(path) => path_buf_serde::serialize(path, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_s = Option::<String>::deserialize(deserializer)?;
        Ok(opt_s.map(PathBuf::from))
    }
}

// --- Custom Deserializer for StorageConfig to Handle Optional Fields ---
mod storage_config_serde {
    use super::*;
    use serde::{Deserialize, Deserializer};

    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    struct StorageConfigRaw {
        #[serde(with = "option_path_buf_serde", default)]
        config_root_directory: Option<PathBuf>,
        #[serde(with = "option_path_buf_serde", default)]
        data_directory: Option<PathBuf>,
        #[serde(with = "option_path_buf_serde", default)]
        log_directory: Option<PathBuf>,
        #[serde(default = "default_default_port")]
        default_port: u16,
        #[serde(with = "string_or_u16_non_option", default = "default_cluster_range")]
        cluster_range: String,
        #[serde(default = "default_max_disk_space_gb")]
        max_disk_space_gb: u64,
        #[serde(default = "default_min_disk_space_gb")]
        min_disk_space_gb: u64,
        #[serde(default = "default_use_raft_for_scale")]
        use_raft_for_scale: bool,
        #[serde(with = "storage_engine_type_serde", default = "default_storage_engine_type")]
        storage_engine_type: StorageEngineType,
        #[serde(default)]
        engine_specific_config: Option<SelectedStorageConfig>,
        #[serde(default = "default_max_open_files")]
        max_open_files: u64,
    }

    fn default_default_port() -> u16 { 8083 }
    fn default_cluster_range() -> String { "8083-8087".to_string() }
    fn default_max_disk_space_gb() -> u64 { 1000 }
    fn default_min_disk_space_gb() -> u64 { 10 }
    fn default_use_raft_for_scale() -> bool { true }
    fn default_storage_engine_type() -> StorageEngineType { StorageEngineType::Sled }
    fn default_max_open_files() -> u64 { 100 }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StorageConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = StorageConfigRaw::deserialize(deserializer)?;
        Ok(StorageConfig {
            config_root_directory: raw.config_root_directory,
            data_directory: raw.data_directory,
            log_directory: raw.log_directory,
            default_port: raw.default_port,
            cluster_range: raw.cluster_range,
            max_disk_space_gb: raw.max_disk_space_gb,
            min_disk_space_gb: raw.min_disk_space_gb,
            use_raft_for_scale: raw.use_raft_for_scale,
            storage_engine_type: raw.storage_engine_type,
            engine_specific_config: raw.engine_specific_config,
            max_open_files: raw.max_open_files,
        })
    }
}

// --- Custom StorageEngineType Serialization Module ---
mod storage_engine_type_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(engine_type: &StorageEngineType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&engine_type.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StorageEngineType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // Map common user inputs to correct variants
        let engine_type = match s.as_str() {
            "RocksDB" | "rocksdb" => StorageEngineType::RocksDB,
            "sled" => StorageEngineType::Sled,
            "in_memory" => StorageEngineType::InMemory,
            "redis" => StorageEngineType::Redis,
            "PostgreSQL" | "postgresql" => StorageEngineType::PostgreSQL,
            "MySQL" | "mysql" => StorageEngineType::MySQL,
            "rocks_d_b" => StorageEngineType::RocksDB,
            "postgre_s_q_l" => StorageEngineType::PostgreSQL,
            "my_s_q_l" => StorageEngineType::MySQL,
            _ => return Err(serde::de::Error::custom(format!(
                "unknown variant `{}`, expected one of `sled`, `rocks_d_b`, `in_memory`, `redis`, `postgre_s_q_l`, `my_s_q_l`",
                s
            ))),
        };
        Ok(engine_type)
    }

    #[derive(Deserialize)]
    struct StorageEngineTypeWrapper {
        name: String,
    }

    pub mod mapping {
        use super::*;
        pub fn deserialize<'de, D>(deserializer: D) -> Result<StorageEngineType, D::Error>
        where
            D: Deserializer<'de>,
        {
            let wrapper = StorageEngineTypeWrapper::deserialize(deserializer)?;
            StorageEngineType::from_str(&wrapper.name).map_err(serde::de::Error::custom)
        }
    }
}

// --- Custom Option<StorageEngineType> Serialization Module ---
mod option_storage_engine_type_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(opt_engine_type: &Option<StorageEngineType>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match opt_engine_type {
            Some(engine_type) => storage_engine_type_serde::serialize(engine_type, serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<StorageEngineType>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_s = Option::<String>::deserialize(deserializer)?;
        match opt_s {
            Some(s) => {
                let engine_type = match s.as_str() {
                    "RocksDB" | "rocksdb" => StorageEngineType::RocksDB,
                    "sled" => StorageEngineType::Sled,
                    "in_memory" => StorageEngineType::InMemory,
                    "redis" => StorageEngineType::Redis,
                    "PostgreSQL" | "postgresql" => StorageEngineType::PostgreSQL,
                    "MySQL" | "mysql" => StorageEngineType::MySQL,
                    "rocks_d_b" => StorageEngineType::RocksDB,
                    "postgre_s_q_l" => StorageEngineType::PostgreSQL,
                    "my_s_q_l" => StorageEngineType::MySQL,
                    _ => return Err(serde::de::Error::custom(format!(
                        "unknown variant `{}`, expected one of `sled`, `rocks_d_b`, `in_memory`, `redis`, `postgre_s_q_l`, `my_s_q_l`",
                        s
                    ))),
                };
                Ok(Some(engine_type))
            }
            None => Ok(None),
        }
    }
}

// --- Constants ---
pub const DAEMON_REGISTRY_DB_PATH: &str = "./daemon_registry_db";
pub const DEFAULT_DAEMON_PORT: u16 = 9001;
pub const DEFAULT_REST_API_PORT: u16 = 8081;
pub const DEFAULT_STORAGE_PORT: u16 = 8049;
pub const MAX_CLUSTER_SIZE: usize = 100;
pub const DEFAULT_MAIN_PORT: u16 = 9001;
pub const DEFAULT_CLUSTER_RANGE: &str = "9001-9005";
pub const CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS: u16 = 8085;
pub const DEFAULT_DATA_DIRECTORY: &str = "/opt/graphdb/storage_data";
pub const DEFAULT_LOG_DIRECTORY: &str = "/opt/graphdb/logs";
pub const DEFAULT_STORAGE_CONFIG_PATH: &str = "./storage_daemon_server/storage_config.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/storage_config.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB: &str = "./storage_daemon_server/storage_config_rocksdb.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_SLED: &str = "./storage_daemon_server/storage_config_sled.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_POSTGRES: &str = "./storage_daemon_server/storage_config_postgres.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_MYSQL: &str = "./storage_daemon_server/storage_config_mysql.yaml";
pub const DEFAULT_STORAGE_CONFIG_PATH_REDIS: &str = "./storage_daemon_server/storage_config_redis.yaml";
pub const DEFAULT_REST_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/rest_api_config.yaml";
pub const DEFAULT_DAEMON_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/daemon_config.yaml";
pub const DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/main_app_config.yaml";
pub const PID_FILE_DIR: &str = "./storage_daemon_server/pids";
pub const DAEMON_PID_FILE_NAME_PREFIX: &str = "graphdb-daemon-";
pub const REST_PID_FILE_NAME_PREFIX: &str = "graphdb-rest-";
pub const STORAGE_PID_FILE_NAME_PREFIX: &str = "graphdb-storage-";
pub const EXECUTABLE_NAME: &str = "graphdb-server";
pub const DEFAULT_CONFIG_ROOT_DIRECTORY_STR: &str = "./storage_daemon_server";
pub const DEFAULT_HTTP_TIMEOUT_SECONDS: u64 = 30;
pub const DEFAULT_HTTP_RETRIES: u32 = 3;
pub const DEFAULT_GRPC_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_GRPC_RETRIES: u32 = 5;
pub const DAEMON_NAME_STORAGE_DAEMON: &str = "storage";
pub fn default_config_root_directory() -> PathBuf {
    PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
}

// This is the custom deserializer function that will handle the conversion.
fn deserialize_engine_config<'de, D>(deserializer: D) -> Result<Option<SelectedStorageConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    // The YAML data for this field comes in as a HashMap
    let raw_config: Option<HashMap<String, Value>> = Option::deserialize(deserializer)?;

    if let Some(raw_map) = raw_config {
        let mut storage_config_inner = StorageConfigInner {
            path: None,
            host: None,
            port: None,
            username: None,
            password: None,
            database: None,
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

        Ok(Some(SelectedStorageConfig { storage: storage_config_inner }))
    } else {
        Ok(None)
    }
}

// A temporary struct to deserialize the raw YAML content, which is flexible.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct RawStorageConfig {
    pub config_root_directory: Option<PathBuf>,
    pub data_directory: Option<PathBuf>,
    pub log_directory: Option<PathBuf>,
    pub default_port: Option<u16>,
    pub cluster_range: Option<String>,
    pub max_disk_space_gb: Option<u64>,
    pub min_disk_space_gb: Option<u64>,
    pub use_raft_for_scale: Option<bool>,
    pub storage_engine_type: Option<StorageEngineType>,
    pub engine_specific_config: Option<HashMap<String, serde_json::Value>>,
    pub max_open_files: Option<u64>,
}


// The `From` implementation that converts the `RawStorageConfig` to `StorageConfig`
impl From<RawStorageConfig> for StorageConfig {
    fn from(raw: RawStorageConfig) -> Self {
        let storage_engine_type = raw.storage_engine_type.unwrap_or_else(default_storage_engine_type);
        
        let engine_specific_config = if let Some(raw_config_map) = raw.engine_specific_config {
            let mut storage_config_inner = StorageConfigInner {
                path: None,
                host: None,
                port: None,
                username: None,
                password: None,
                database: None,
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

            Some(SelectedStorageConfig {
                storage: storage_config_inner,
            })
        } else {
            None
        };

        StorageConfig {
            config_root_directory: raw.config_root_directory,
            data_directory: raw.data_directory,
            log_directory: raw.log_directory,
            default_port: raw.default_port.unwrap_or_else(default_default_port),
            cluster_range: raw.cluster_range.unwrap_or_else(default_cluster_range),
            max_disk_space_gb: raw.max_disk_space_gb.unwrap_or_else(default_max_disk_space_gb),
            min_disk_space_gb: raw.min_disk_space_gb.unwrap_or_else(default_min_disk_space_gb),
            use_raft_for_scale: raw.use_raft_for_scale.unwrap_or_else(default_use_raft_for_scale),
            storage_engine_type,
            engine_specific_config,
            max_open_files: raw.max_open_files.unwrap_or_else(default_max_open_files),
        }
    }
}

/// This struct represents the inner part of a storage configuration, allowing
/// optional paths and other connection details.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfigInner {
    // FIX: Changed from `path_buf_serde` to `option_path_buf_serde`
    // This resolves the mismatched types error when serializing/deserializing
    // an Option<PathBuf>. The `default` attribute will now also work correctly.
    #[serde(with = "option_path_buf_serde", default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedStorageConfig {
    pub storage: StorageConfigInner,
}

impl SelectedStorageConfig {
    pub fn load_from_yaml<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file {:?}", path.as_ref()))?;
        serde_yaml2::from_str(&content)
            .with_context(|| format!("Failed to parse YAML from {:?}", path.as_ref()))
    }
}

impl Default for SelectedStorageConfig {
    fn default() -> Self {
        SelectedStorageConfig {
            storage: StorageConfigInner {
                path: Some(PathBuf::from(DEFAULT_DATA_DIRECTORY)),
                host: Some("127.0.0.1".to_string()),
                port: Some(DEFAULT_STORAGE_PORT),
                username: None,
                password: None,
                database: None,
            },
        }
    }
}

// --- YAML Configuration Structs ---
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MainDaemonConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

impl Default for MainDaemonConfig {
    fn default() -> Self {
        MainDaemonConfig {
            data_directory: format!("{}/daemon_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            log_directory: format!("{}/daemon", DEFAULT_LOG_DIRECTORY),
            default_port: DEFAULT_MAIN_PORT,
            cluster_range: DEFAULT_CLUSTER_RANGE.to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct MainConfigWrapper {
    main_daemon: MainDaemonConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestApiConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

impl Default for RestApiConfig {
    fn default() -> Self {
        RestApiConfig {
            data_directory: format!("{}/rest_api_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            log_directory: format!("{}/daemon", DEFAULT_LOG_DIRECTORY),
            default_port: DEFAULT_REST_API_PORT,
            cluster_range: format!("{}", DEFAULT_REST_API_PORT),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct RestApiConfigWrapper {
    config_root_directory: String,
    rest_api: RestApiConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaemonYamlConfig {
    pub config_root_directory: String,
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_connections: u32,
    pub max_open_files: u64,
    pub use_raft_for_scale: bool,
    pub log_level: String,
}

impl Default for DaemonYamlConfig {
    fn default() -> Self {
        DaemonYamlConfig {
            config_root_directory: default_config_root_directory().to_string_lossy().into_owned(),
            data_directory: format!("{}/daemon_data", DEFAULT_CONFIG_ROOT_DIRECTORY_STR),
            log_directory: format!("{}/daemon", DEFAULT_LOG_DIRECTORY),
            default_port: DEFAULT_DAEMON_PORT,
            cluster_range: format!("{}", DEFAULT_DAEMON_PORT),
            max_connections: 1000,
            max_open_files: 1024,
            use_raft_for_scale: true,
            log_level: "info".to_string(),
        }
    }
}

/// The top-level `[storage]` configuration struct, which can be loaded from TOML.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct StorageConfig {
    // This one was already correct
    #[serde(with = "option_path_buf_serde", default)]
    pub config_root_directory: Option<PathBuf>,
    
    // FIX: Changed from `path_buf_serde` to `option_path_buf_serde`
    // The `default` function `default_data_directory` now returns `Option<PathBuf>`.
    #[serde(with = "option_path_buf_serde", default = "default_data_directory")]
    pub data_directory: Option<PathBuf>,
    
    // FIX: Changed from `path_buf_serde` to `option_path_buf_serde`
    // The `default` function `default_log_directory` now returns `Option<PathBuf>`.
    #[serde(with = "option_path_buf_serde", default = "default_log_directory")]
    pub log_directory: Option<PathBuf>,
    
    #[serde(default = "default_default_port")]
    pub default_port: u16,
    #[serde(default = "default_cluster_range")]
    pub cluster_range: String,
    #[serde(default = "default_max_disk_space_gb")]
    pub max_disk_space_gb: u64,
    #[serde(default = "default_min_disk_space_gb")]
    pub min_disk_space_gb: u64,
    #[serde(default = "default_use_raft_for_scale")]
    pub use_raft_for_scale: bool,
    #[serde(default = "default_storage_engine_type")]
    pub storage_engine_type: StorageEngineType,
    #[serde(default = "default_engine_specific_config")]
    pub engine_specific_config: Option<SelectedStorageConfig>,
    #[serde(default = "default_max_open_files")]
    pub max_open_files: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

fn default_config_root_directory_option() -> Option<PathBuf> {
    Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR))
}
fn default_data_directory() -> Option<PathBuf> {
    Some(PathBuf::from(DEFAULT_DATA_DIRECTORY))
}
fn default_log_directory() -> Option<PathBuf> {
    Some(PathBuf::from(DEFAULT_LOG_DIRECTORY))
}
fn default_default_port() -> u16 { 8083 }
fn default_cluster_range() -> String { "8083".to_string() }
fn default_max_disk_space_gb() -> u64 { 1000 }
fn default_min_disk_space_gb() -> u64 { 10 }
fn default_use_raft_for_scale() -> bool { true }
fn default_storage_engine_type() -> StorageEngineType { StorageEngineType::Sled }
fn default_engine_specific_config() -> Option<SelectedStorageConfig> {
    Some(SelectedStorageConfig {
        storage: StorageConfigInner {
            path: Some(PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY))),
            host: Some("127.0.0.1".to_string()),
            port: Some(DEFAULT_STORAGE_PORT),
            username: None,
            password: None,
            database: None,
        },
    })
}
fn default_max_open_files() -> u64 { 100 }

#[derive(Debug, Deserialize, Serialize)]
struct EngineSpecificConfigWrapper {
    storage: EngineSpecificConfig,
}

#[derive(Debug, Deserialize, Serialize)]
struct EngineSpecificConfig {
    path: PathBuf,
    host: String,
    port: u16,
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
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            config_root_directory: Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            data_directory: default_data_directory(),
            log_directory: default_log_directory(),
            default_port: 8083,
            cluster_range: "8083-8087".to_string(),
            max_disk_space_gb: 1000,
            min_disk_space_gb: 10,
            use_raft_for_scale: true,
            storage_engine_type: StorageEngineType::RocksDB,
            engine_specific_config: Some(SelectedStorageConfig {
                storage: StorageConfigInner {
                    path: Some(PathBuf::from(&format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY))),
                    host: Some("127.0.0.1".to_string()),
                    port: Some(8083),
                    username: None,
                    password: None,
                    database: None,
                },
            }),
            max_open_files: 100,
        }
    }
}

impl StorageConfig {
    /// Saves the current StorageConfig to the main `storage_config.yaml` file.
    /// This function does NOT touch or modify any engine-specific YAML files.
    pub fn save(&self) -> Result<()> {
        let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
        let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
        let config_path = if project_config_path.exists() {
            project_config_path
        } else {
            default_config_path
        };
        
        // Load the engine-specific settings that will be included in the final saved YAML.
        let (engine_path, engine_host, engine_port) = match self.engine_specific_config.as_ref() {
            Some(es) => {
                let p = es.storage.path.clone().unwrap_or_else(|| {
                    PathBuf::from(format!("{}/{}", DEFAULT_DATA_DIRECTORY, self.storage_engine_type.to_string().to_lowercase()))
                });
                let h = es.storage.host.clone().unwrap_or_else(|| "127.0.0.1".to_string());
                let pt = es.storage.port.unwrap_or(self.default_port);
                (p, h, pt)
            },
            None => {
                let default_path = PathBuf::from(format!("{}/{}", DEFAULT_DATA_DIRECTORY, self.storage_engine_type.to_string().to_lowercase()));
                let default_host = "127.0.0.1".to_string();
                let default_port = self.default_port;
                (default_path, default_host, default_port)
            }
        };

        // Create a properly formatted YAML string manually for better control
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
  max_open_files: {}
"#,
            self.config_root_directory.as_ref().unwrap_or(&PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)).display(),
            self.data_directory.as_ref().unwrap_or(&engine_path).display(),
            self.log_directory.as_ref().unwrap_or(&PathBuf::from(DEFAULT_LOG_DIRECTORY)).display(),
            self.default_port,
            self.cluster_range,
            self.max_disk_space_gb,
            self.min_disk_space_gb,
            self.use_raft_for_scale,
            daemon_api_storage_engine_type_to_string(&self.storage_engine_type),
            engine_path.display(),
            engine_host,
            engine_port,
            self.max_open_files
        );

        fs::create_dir_all(config_path.parent().unwrap())
            .context(format!("Failed to create parent directories for {}", config_path.display()))?;
        fs::write(&config_path, yaml_string)
            .context(format!("Failed to write StorageConfig to file: {}", config_path.display()))?;
        info!("Saved storage configuration to {:?}", config_path);

        // Verify the written file
        let written_content = fs::read_to_string(&config_path)
            .context(format!("Failed to read back storage config file: {}", config_path.display()))?;
        match serde_yaml::from_str::<StorageConfigWrapper>(&written_content) {
            Ok(_) => info!("Successfully verified written storage config at {:?}", config_path),
            Err(e) => warn!("Failed to verify written storage config at {:?}: {}", config_path, e),
        };
        Ok(())
    }
}

impl From<&CliTomlStorageConfig> for EngineStorageConfig {
    fn from(cli: &CliTomlStorageConfig) -> Self {
        EngineStorageConfig {
            storage_engine_type: cli.storage_engine_type.unwrap_or(StorageEngineType::Sled),
            data_directory: cli.data_directory
                .clone()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)),
            connection_string: None,
            max_open_files: cli.max_open_files.map(|v| v as i32),
            engine_specific_config: None,
            default_port: cli.default_port.unwrap_or(DEFAULT_STORAGE_PORT),
            log_directory: cli.log_directory
                .clone()
                .unwrap_or_else(|| DEFAULT_LOG_DIRECTORY.into()),
            config_root_directory: cli.config_root_directory
                .clone()
                .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
            cluster_range: cli.cluster_range
                .clone()
                .unwrap_or("8083-8087".to_string()),
            use_raft_for_scale: cli.use_raft_for_scale.unwrap_or(true),
            max_disk_space_gb: cli.max_disk_space_gb.unwrap_or(1000),
            min_disk_space_gb: cli.min_disk_space_gb.unwrap_or(10),
        }
    }
}

/// Implements the `From` trait to allow easy conversion from the library's
/// `StorageConfig` to the CLI's `StorageConfig`. This is the core fix for the
/// type mismatch error.
impl From<LibStorageConfig> for StorageConfig {
    /// Converts a `LibStorageConfig` instance into a `StorageConfig` instance.
    fn from(item: LibStorageConfig) -> Self {
        // Here, we manually map each field from the source (`item`) to the
        // destination (`Self`). We handle the type conversions as needed.
        StorageConfig {
            // `PathBuf` to `Option<PathBuf>` is a straightforward wrap in `Some`.
            config_root_directory: Some(item.config_root_directory),
            data_directory: Some(item.data_directory),
            // `String` to `Option<PathBuf>` requires creating a `PathBuf` first.
            log_directory: Some(PathBuf::from(item.log_directory)),
            // These fields have the same type, so they are a direct copy.
            default_port: item.default_port,
            cluster_range: item.cluster_range,
            max_disk_space_gb: item.max_disk_space_gb,
            min_disk_space_gb: item.min_disk_space_gb,
            use_raft_for_scale: item.use_raft_for_scale,
            storage_engine_type: item.storage_engine_type,
            // The `engine_specific_config` field cannot be converted directly
            // because the types are different (`Option<HashMap<String, Value>>` vs.
            // `Option<SelectedStorageConfig>`). A custom `From` implementation
            // for `SelectedStorageConfig` would be needed, likely involving a
            // `match` on `item.storage_engine_type`. For now, we will return `None`.
            engine_specific_config: None,
            // `Option<i32>` to `u64` requires handling the `Option` and casting.
            // `unwrap_or_default()` is a safe way to handle a `None` value by
            // using the default value for the destination type (`0` for `u64`).
            max_open_files: item.max_open_files.map(|val| val as u64).unwrap_or_default(),
        }
    }
}

// Helper function to convert StorageEngineType to String
pub fn daemon_api_storage_engine_type_to_string(engine_type: &StorageEngineType) -> String {
    match engine_type {
        StorageEngineType::Sled => "sled".to_string(),
        StorageEngineType::RocksDB => "rocksdb".to_string(),
        StorageEngineType::InMemory => "inmemory".to_string(),
        StorageEngineType::Redis => "redis".to_string(),
        StorageEngineType::PostgreSQL => "postgresql".to_string(),
        StorageEngineType::MySQL => "mysql".to_string(),
    }
}

// Helper function to map StorageEngineType to YAML file path
pub fn get_engine_config_path(engine_type: &StorageEngineType) -> Option<PathBuf> {
    match engine_type {
        StorageEngineType::RocksDB => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_ROCKSDB)),
        StorageEngineType::Sled => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_SLED)),
        StorageEngineType::PostgreSQL => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_POSTGRES)),
        StorageEngineType::MySQL => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_MYSQL)),
        StorageEngineType::Redis => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_REDIS)),
        StorageEngineType::InMemory => Some(PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH)),
        _ => None,
    }
}

// --- CLI Configuration ---
#[derive(Debug, Parser, Clone)]
#[clap(name = "GraphDB CLI", about = "Command line interface for GraphDB")]
pub struct CliConfig {
    #[clap(subcommand)]
    pub command: Commands,

    #[clap(
        long,
        env = "GRAPHDB_HTTP_TIMEOUT_SECONDS",
        default_value_t = DEFAULT_HTTP_TIMEOUT_SECONDS,
        help = "HTTP request timeout in seconds"
    )]
    pub http_timeout_seconds: u64,

    #[clap(
        long,
        env = "GRAPHDB_HTTP_RETRIES",
        default_value_t = DEFAULT_HTTP_RETRIES,
        help = "Number of HTTP request retries"
    )]
    pub http_retries: u32,

    #[clap(
        long,
        env = "GRAPHDB_GRPC_TIMEOUT_SECONDS",
        default_value_t = DEFAULT_GRPC_TIMEOUT_SECONDS,
        help = "gRPC request timeout in seconds"
    )]
    pub grpc_timeout_seconds: u64,

    #[clap(
        long,
        env = "GRAPHDB_GRPC_RETRIES",
        default_value_t = DEFAULT_GRPC_RETRIES,
        help = "Number of gRPC request retries"
    )]
    pub grpc_retries: u32,

    #[clap(long, help = "Port for the main GraphDB Daemon")]
    pub daemon_port: Option<u16>,

    #[clap(long, help = "Cluster range for the main GraphDB Daemon (e.g., '9001-9005')")]
    pub daemon_cluster: Option<String>,

    #[clap(long, help = "Port for the REST API")]
    pub rest_port: Option<u16>,

    #[clap(long, help = "Cluster range for the REST API")]
    pub rest_cluster: Option<String>,

    #[clap(long, help = "Port for the Storage Daemon (synonym for --port in storage commands)")]
    pub storage_port: Option<u16>,

    #[clap(long, help = "Cluster range for the Storage Daemon (synonym for --cluster in storage commands)")]
    pub storage_cluster: Option<String>,

    #[clap(long, help = "Path to the storage daemon configuration YAML")]
    pub storage_config_path: Option<PathBuf>,

    #[clap(long, help = "Path to the REST API configuration YAML")]
    pub rest_api_config_path: Option<PathBuf>,

    #[clap(long, help = "Path to the main daemon configuration YAML")]
    pub main_daemon_config_path: Option<PathBuf>,

    #[clap(long, help = "Root directory for configurations, if not using default paths")]
    pub config_root_directory: Option<PathBuf>,

    #[clap(long, short = 'c', help = "Run CLI in interactive mode")]
    pub cli: bool,
}

impl CliConfig {
    pub async fn execute(&self, manager: &mut StorageEngineManager) -> Result<(), GraphError> {
        match &self.command {
            Commands::Use(UseAction::Storage { engine, permanent }) => {
                let is_permanent = *permanent;
                info!(
                    "Switching to storage engine: {:?}, permanent: {} (interactive mode: {})",
                    engine, is_permanent, self.cli
                );
                // Validate engine type against available engines
                let available_engines = StorageEngineManager::available_engines();
                if !available_engines.contains(engine) {
                    return Err(GraphError::InvalidStorageEngine(format!(
                        "Storage engine {:?} is not enabled. Available engines: {:?}", engine, available_engines
                    )));
                }
                // Update StorageEngineManager
                manager.use_storage(*engine, is_permanent).await?;
                if is_permanent {
                    let mut storage_config = load_storage_config_from_yaml(None)
                        .map_err(|e| GraphError::ConfigurationError(format!("Failed to load storage config: {}", e)))?;
                    storage_config.storage_engine_type = *engine;
                    storage_config.save()
                        .map_err(|e| GraphError::ConfigurationError(format!("Failed to save storage config: {}", e)))?;
                    info!("Saved storage engine change to {:?}", DEFAULT_STORAGE_CONFIG_PATH);
                }
                println!(
                    "Switched to storage engine: {}{}",
                    daemon_api_storage_engine_type_to_string(engine),
                    if is_permanent { " (persisted)" } else { " (non-persisted)" }
                );
                Ok(())
            }
            Commands::Save(SaveAction::Storage) => {
                let storage_config = load_storage_config_from_yaml(None)
                    .map_err(|e| GraphError::ConfigurationError(format!("Failed to load storage config: {}", e)))?;
                storage_config.save()
                    .map_err(|e| GraphError::ConfigurationError(format!("Failed to save storage config: {}", e)))?;
                println!("Saved storage configuration to {:?}", DEFAULT_STORAGE_CONFIG_PATH);
                Ok(())
            }
            Commands::Save(SaveAction::Configuration) => {
                let cli_config = load_cli_config()
                    .map_err(|e| GraphError::ConfigurationError(format!("Failed to load CLI config: {}", e)))?;
                cli_config.save()
                    .map_err(|e| GraphError::ConfigurationError(format!("Failed to save CLI config: {}", e)))?;
                println!("Saved CLI configuration to {:?}", PathBuf::from("/opt/graphdb/config.toml"));
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn load(interactive_command: Option<CommandType>) -> Result<Self> {
        let mut args = if let Some(cmd) = interactive_command {
            let mut config = CliConfig {
                command: Commands::Exit,
                http_timeout_seconds: DEFAULT_HTTP_TIMEOUT_SECONDS,
                http_retries: DEFAULT_HTTP_RETRIES,
                grpc_timeout_seconds: DEFAULT_GRPC_TIMEOUT_SECONDS,
                grpc_retries: DEFAULT_GRPC_RETRIES,
                daemon_port: None,
                daemon_cluster: None,
                rest_port: None,
                rest_cluster: None,
                storage_port: None,
                storage_cluster: None,
                storage_config_path: None,
                rest_api_config_path: None,
                main_daemon_config_path: None,
                config_root_directory: None,
                cli: true,
            };
            match cmd {
                CommandType::StartStorage { port, config_file, cluster, storage_port, storage_cluster } => {
                    config.command = Commands::Storage(StorageAction::Start {
                        port: storage_port.or(port),
                        config_file: config_file.clone(),
                        cluster: storage_cluster.clone().or(cluster.clone()),
                        storage_port,
                        storage_cluster: storage_cluster.clone(),
                    });
                    config.storage_port = storage_port.or(port);
                    config.storage_cluster = storage_cluster.or(cluster);
                    config.storage_config_path = config_file;
                }
                CommandType::UseStorage { engine, permanent } => {
                    config.command = Commands::Use(UseAction::Storage { engine, permanent });
                }
                CommandType::SaveStorage => {
                    config.command = Commands::Save(SaveAction::Storage);
                }
                CommandType::SaveConfig => {
                    config.command = Commands::Save(SaveAction::Configuration);
                }
                CommandType::Exit => {
                    config.command = Commands::Exit;
                }
                _ => {
                    return Err(anyhow!("Unsupported interactive command: {:?}", cmd));
                }
            }
            config
        } else {
            match <Self as clap::Parser>::try_parse() {
                Ok(args) => args,
                Err(e) => {
                    error!("Failed to parse CLI arguments: {:?}", e);
                    return Err(anyhow!("CLI parsing error: {}", e));
                }
            }
        };
        let config_root = args
            .config_root_directory
            .clone()
            .unwrap_or_else(|| default_config_root_directory());

        debug!("Resolved config root directory: {:?}", config_root);
        let global_config_path = config_root.join("graphdb_config.yaml");
        let mut global_yaml_config: Option<GlobalYamlConfig> = None;
        if global_config_path.exists() {
            match fs::read_to_string(&global_config_path) {
                Ok(content) => match serde_yaml2::from_str(&content) {
                    Ok(config) => {
                        global_yaml_config = Some(config);
                    }
                    Err(e) => warn!("Failed to parse global config YAML {:?}: {}", global_config_path, e),
                },
                Err(e) => warn!("Failed to read global config file {:?}: {}", global_config_path, e),
            }
        }
        let storage_yaml_config = load_storage_config_from_yaml(args.storage_config_path.clone())
            .map_err(|e| anyhow!("Failed to load storage config: {}", e))
            .unwrap_or_else(|e| {
                warn!("Failed to load storage config, using default: {}", e);
                StorageConfig::default()
            });

        let rest_api_yaml_config = Self::load_specific_yaml(
            args.rest_api_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.rest_api.clone()),
            &PathBuf::from(DEFAULT_REST_CONFIG_PATH_RELATIVE),
            "rest_api_config.yaml",
        );

        let main_daemon_yaml_config = Self::load_specific_yaml(
            args.main_daemon_config_path.as_ref(),
            global_yaml_config.as_ref().and_then(|g| g.main_daemon.clone()),
            &PathBuf::from(DEFAULT_MAIN_APP_CONFIG_PATH_RELATIVE),
            "main_app_config.yaml",
        );

        args.storage_config_path = Some(
            args.storage_config_path
                .unwrap_or_else(|| PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE))
        );

        if args.daemon_port.is_none() {
            args.daemon_port = match &args.command {
                Commands::Start { action: Some(StartAction::Daemon { port, daemon_port, .. }), .. } => daemon_port.or(*port),
                Commands::Start { action: Some(StartAction::All { daemon_port, port, .. }), .. } => daemon_port.or(*port),
                Commands::Daemon(DaemonCliCommand::Start { port, daemon_port, .. }) => daemon_port.or(*port),
                Commands::Stop(StopArgs { action: Some(StopAction::Daemon { port }), .. }) => *port,
                Commands::Daemon(DaemonCliCommand::Stop { port, .. }) => *port,
                Commands::Status(StatusArgs { action: Some(StatusAction::Daemon { port, .. }), .. }) => *port,
                Commands::Daemon(DaemonCliCommand::Status { port, .. }) => *port,
                Commands::Reload(ReloadArgs { action: Some(ReloadAction::Daemon { port }), .. }) => *port,
                Commands::Restart(RestartArgs { action: RestartAction::Daemon { port, daemon_port, .. } }) => daemon_port.or(*port),
                Commands::Restart(RestartArgs { action: RestartAction::All { port, daemon_port, .. } }) => daemon_port.or(*port),
                _ => main_daemon_yaml_config.as_ref().map(|c| c.default_port),
            };
        }

        if args.daemon_cluster.is_none() {
            args.daemon_cluster = match &args.command {
                Commands::Start { action: Some(StartAction::Daemon { cluster, daemon_cluster, .. }), .. } => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Start { action: Some(StartAction::All { daemon_cluster, cluster, .. }), .. } => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Daemon(DaemonCliCommand::Start { cluster, daemon_cluster, .. }) => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Status(StatusArgs { action: Some(StatusAction::Daemon { cluster, .. }), .. }) => cluster.clone(),
                Commands::Daemon(DaemonCliCommand::Status { cluster, .. }) => cluster.clone(),
                Commands::Restart(RestartArgs { action: RestartAction::Daemon { cluster, daemon_cluster, .. } }) => daemon_cluster.clone().or_else(|| cluster.clone()),
                Commands::Restart(RestartArgs { action: RestartAction::All { cluster, daemon_cluster, .. } }) => daemon_cluster.clone().or_else(|| cluster.clone()),
                _ => main_daemon_yaml_config.as_ref().map(|c| c.cluster_range.clone()),
            };
        }

        if args.rest_port.is_none() {
            args.rest_port = match &args.command {
                Commands::Start { action: Some(StartAction::Rest { port, rest_port, .. }), .. } => rest_port.or(*port),
                Commands::Start { action: Some(StartAction::All { rest_port, listen_port, .. }), .. } => rest_port.or(*listen_port),
                Commands::Rest(RestCliCommand::Start { port, rest_port, .. }) => rest_port.or(*port),
                Commands::Stop(StopArgs { action: Some(StopAction::Rest { port, .. }), .. }) => *port,
                Commands::Rest(RestCliCommand::Stop { port, .. }) => *port,
                Commands::Status(StatusArgs { action: Some(StatusAction::Rest { port, .. }), .. }) => *port,
                Commands::Rest(RestCliCommand::Status { port, .. }) => *port,
                Commands::Restart(RestartArgs { action: RestartAction::Rest { port, rest_port, .. } }) => rest_port.or(*port),
                Commands::Restart(RestartArgs { action: RestartAction::All { listen_port, rest_port, .. } }) => rest_port.or(*listen_port),
                _ => rest_api_yaml_config.as_ref().map(|c| c.default_port),
            };
        }

        if args.rest_cluster.is_none() {
            args.rest_cluster = match &args.command {
                Commands::Start { action: Some(StartAction::Rest { cluster, rest_cluster, .. }), .. } => rest_cluster.clone().or_else(|| cluster.clone()),
                Commands::Start { action: Some(StartAction::All { rest_cluster, .. }), .. } => rest_cluster.clone(),
                Commands::Rest(RestCliCommand::Start { cluster, rest_cluster, .. }) => rest_cluster.clone().or_else(|| cluster.clone()),
                Commands::Status(StatusArgs { action: Some(StatusAction::Rest { cluster, .. }), .. }) => cluster.clone(),
                Commands::Rest(RestCliCommand::Status { cluster, .. }) => cluster.clone(),
                Commands::Restart(RestartArgs { action: RestartAction::Rest { cluster, rest_cluster, .. } }) => rest_cluster.clone().or_else(|| cluster.clone()),
                Commands::Restart(RestartArgs { action: RestartAction::All { rest_cluster, .. } }) => rest_cluster.clone(),
                _ => Some(args.rest_port.unwrap_or(DEFAULT_REST_API_PORT).to_string()),
            };
        }

        if args.storage_port.is_none() {
            args.storage_port = match &args.command {
                Commands::Start { action: Some(StartAction::Storage { port, storage_port, .. }), .. } => storage_port.or(*port),
                Commands::Start { action: Some(StartAction::All { storage_port, .. }), .. } => *storage_port,
                Commands::Storage(StorageAction::Start { port, storage_port, .. }) => storage_port.or(*port),
                Commands::Stop(StopArgs { action: Some(StopAction::Storage { port }), .. }) => *port,
                Commands::Storage(StorageAction::Stop { port, .. }) => *port,
                Commands::Status(StatusArgs { action: Some(StatusAction::Storage { port, .. }), .. }) => *port,
                Commands::Storage(StorageAction::Status { port, .. }) => *port,
                Commands::Restart(RestartArgs { action: RestartAction::Storage { port, storage_port, .. } }) => storage_port.or(*port),
                Commands::Restart(RestartArgs { action: RestartAction::All { storage_port, .. } }) => *storage_port,
                Commands::Use(UseAction::Storage { .. }) => Some(DEFAULT_STORAGE_PORT),
                _ => Some(load_storage_config_from_yaml(None).map(|c| c.default_port).unwrap_or(DEFAULT_STORAGE_PORT)),
            };
        }

        if args.storage_cluster.is_none() {
            args.storage_cluster = match &args.command {
                Commands::Start { action: Some(StartAction::Storage { cluster, storage_cluster, .. }), .. } => storage_cluster.clone().or_else(|| cluster.clone()),
                Commands::Start { action: Some(StartAction::All { storage_cluster, .. }), .. } => storage_cluster.clone(),
                Commands::Storage(StorageAction::Start { cluster, storage_cluster, .. }) => storage_cluster.clone().or_else(|| cluster.clone()),
                Commands::Status(StatusArgs { action: Some(StatusAction::Storage { cluster, .. }), .. }) => cluster.clone(),
                Commands::Storage(StorageAction::Status { cluster, .. }) => cluster.clone(),
                Commands::Restart(RestartArgs { action: RestartAction::Storage { cluster, storage_cluster, .. } }) => storage_cluster.clone().or_else(|| cluster.clone()),
                Commands::Restart(RestartArgs { action: RestartAction::All { storage_cluster, .. } }) => storage_cluster.clone(),
                Commands::Use(UseAction::Storage { .. }) => Some("8083-8087".to_string()),
                _ => Some(load_storage_config_from_yaml(None).map(|c| c.cluster_range).unwrap_or_else(|_| StorageConfig::default().cluster_range)),
            };
        }

        println!("SUCCESS: CLI config loaded.");
        info!("SUCCESS: Determined config path: {:?}", args.storage_config_path);
        info!("SUCCESS: Storage config loaded: {:?}", load_storage_config_from_yaml(None));
        Ok(args)
    }

    pub fn load_specific_yaml<T>(
        cli_path: Option<&PathBuf>,
        global_embed: Option<T>,
        default_path: &Path,
        config_name_for_log: &str,
    ) -> Option<T>
    where
        T: for<'de> Deserialize<'de> + Clone + std::fmt::Debug,
    {
        if let Some(path) = cli_path {
            match fs::canonicalize(path) {
                Ok(canonical_path) => match fs::read_to_string(&canonical_path) {
                    Ok(content) => match serde_yaml2::from_str(&content) {
                        Ok(config) => {
                            return Some(config);
                        }
                        Err(e) => {
                            error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, canonical_path, e);
                            warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, canonical_path, e);
                        }
                    },
                    Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, canonical_path, e),
                },
                Err(e) => warn!("Failed to canonicalize {} path {:?}: {}", config_name_for_log, path, e),
            }
        }

        if let Some(embed) = global_embed {
            debug!("Using {} config embedded in global config.", config_name_for_log);
            return Some(embed);
        }

        if default_path.exists() {
            match fs::canonicalize(default_path) {
                Ok(canonical_path) => match fs::read_to_string(&canonical_path) {
                    Ok(content) => match serde_yaml2::from_str(&content) {
                        Ok(config) => {
                            return Some(config);
                        }
                        Err(e) => {
                            error!("Detailed YAML parsing error for {} at {:?}: {:?}", config_name_for_log, canonical_path, e);
                            warn!("Failed to parse {} YAML from {:?}: {}", config_name_for_log, canonical_path, e);
                        }
                    },
                    Err(e) => warn!("Failed to read {} file from {:?}: {}", config_name_for_log, canonical_path, e),
                },
                Err(e) => warn!("Failed to canonicalize {} path {:?}: {}", config_name_for_log, default_path, e),
            }
        }

        debug!("No {} config found via CLI, global, or default paths.", config_name_for_log);
        None
    }

    pub fn get_daemon_address(&self) -> Result<SocketAddr> {
        let ip_str = "127.0.0.1";
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_daemon_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_daemon_port(&self) -> Result<u16> {
        self.daemon_port
            .with_context(|| "Daemon port not specified and no default found.")
    }

    pub fn get_daemon_cluster_range(&self) -> Result<String> {
        self.daemon_cluster
            .clone()
            .with_context(|| "Daemon cluster range not specified and no default found.")
    }

    pub fn get_rest_api_port(&self) -> Result<u16> {
        self.rest_port
            .with_context(|| "REST API port not specified and no default found.")
    }

    pub fn get_rest_api_cluster_range(&self) -> Result<String> {
        self.rest_cluster
            .clone()
            .with_context(|| "REST API cluster range not specified and no default found.")
    }

    pub fn get_storage_daemon_address(&self) -> Result<SocketAddr> {
        let ip_str = "127.0.0.1";
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_storage_daemon_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_storage_daemon_port(&self) -> Result<u16> {
        self.storage_port
            .with_context(|| "Storage Daemon port not specified and no default found.")
    }

    pub fn get_storage_daemon_cluster_range(&self) -> Result<String> {
        self.storage_cluster
            .clone()
            .with_context(|| "Storage Daemon cluster range not specified and no default found.")
    }

    pub fn get_rest_api_address(&self) -> Result<SocketAddr> {
        let ip_str = "127.0.0.1";
        let ip: IpAddr = ip_str.parse().with_context(|| format!("Invalid IP address: {}", ip_str))?;
        let port = self.get_rest_api_port()?;
        Ok(SocketAddr::new(ip, port))
    }

    pub fn get_data_directory(&self) -> Result<String> {
        match &self.command {
            Commands::Start { action: Some(start_action), .. } => match start_action {
                StartAction::Daemon { .. } => Ok(MainDaemonConfig::default().data_directory),
                StartAction::Rest { .. } => Ok(RestApiConfig::default().data_directory),
                StartAction::Storage { .. } => Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned()),
                _ => Err(anyhow!("Not in a start context to retrieve data directory.")),
            },
            Commands::Rest(RestCliCommand::Start { .. }) => {
                Ok(RestApiConfig::default().data_directory)
            }
            Commands::Storage(StorageAction::Start { .. }) => {
                Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned())
            }
            Commands::Daemon(DaemonCliCommand::Start { .. }) => {
                Ok(MainDaemonConfig::default().data_directory)
            }
            Commands::Use(UseAction::Storage { .. }) => {
                Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned())
            }
            Commands::Save(SaveAction::Storage) => {
                Ok(StorageConfig::default().data_directory.unwrap_or_else(|| PathBuf::from(DEFAULT_DATA_DIRECTORY)).to_string_lossy().into_owned())
            }
            Commands::Save(SaveAction::Configuration) => {
                Ok(default_config_root_directory().to_string_lossy().into_owned())
            }
            _ => Err(anyhow!("Not in a start/cli context to retrieve data directory.")),
        }
    }

    pub fn get_log_directory(&self) -> Result<String> {
        match &self.command {
            Commands::Start { action: Some(start_action), .. } => match start_action {
                StartAction::Daemon { .. } => Ok(MainDaemonConfig::default().log_directory),
                StartAction::Rest { .. } => Ok(RestApiConfig::default().log_directory),
                StartAction::Storage { .. } => {
                    StorageConfig::default().log_directory
                        .map(|path| path.to_string_lossy().to_string())
                        .ok_or_else(|| anyhow!("No log directory configured for storage"))
                },
                _ => Err(anyhow!("Not in a start context to retrieve log directory.")),
            },
            Commands::Rest(RestCliCommand::Start { .. }) => {
                Ok(RestApiConfig::default().log_directory)
            }
            Commands::Storage(StorageAction::Start { .. }) => {
                StorageConfig::default().log_directory
                    .map(|path| path.to_string_lossy().to_string())
                    .ok_or_else(|| anyhow!("No log directory configured for storage"))
            }
            Commands::Daemon(DaemonCliCommand::Start { .. }) => {
                Ok(MainDaemonConfig::default().log_directory)
            }
            Commands::Use(UseAction::Storage { .. }) => {
                StorageConfig::default().log_directory
                    .map(|path| path.to_string_lossy().to_string())
                    .ok_or_else(|| anyhow!("No log directory configured for storage"))
            }
            Commands::Save(SaveAction::Storage) => {
                StorageConfig::default().log_directory
                    .map(|path| path.to_string_lossy().to_string())
                    .ok_or_else(|| anyhow!("No log directory configured for storage"))
            }
            Commands::Save(SaveAction::Configuration) => {
                Ok(DEFAULT_LOG_DIRECTORY.to_string())
            }
            _ => Err(anyhow!("Not in a start/cli context to retrieve log directory.")),
        }
    }

    pub fn get_storage_max_disk_space_gb(&self) -> Result<u64> {
        Ok(StorageConfig::default().max_disk_space_gb)
    }

    pub fn get_storage_min_disk_space_gb(&self) -> Result<u64> {
        Ok(StorageConfig::default().min_disk_space_gb)
    }

    pub fn get_storage_use_raft_for_scale(&self) -> Result<bool> {
        Ok(StorageConfig::default().use_raft_for_scale)
    }

    pub fn get_storage_engine_type(&self) -> Result<String> {
        Ok(daemon_api_storage_engine_type_to_string(&StorageConfig::default().storage_engine_type))
    }

    pub fn get_http_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.http_timeout_seconds)
    }

    pub fn get_http_retries(&self) -> u32 {
        self.http_retries
    }

    pub fn get_grpc_timeout_duration(&self) -> Duration {
        Duration::from_secs(self.grpc_timeout_seconds)
    }

    pub fn get_grpc_retries(&self) -> u32 {
        self.grpc_retries
    }

    pub fn get_cli_internal_storage_engine(&self) -> Result<StorageEngineType> {
        Ok(StorageConfig::default().storage_engine_type)
    }

    pub fn get_cli_daemon_storage_engine(&self) -> Result<StorageEngineType> {
        Ok(StorageConfig::default().storage_engine_type)
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
struct GlobalYamlConfig {
    #[serde(default = "default_config_root_directory_option")]
    #[serde(with = "option_path_buf_serde")]
    config_root_directory: Option<PathBuf>,
    storage: Option<StorageConfig>,
    rest_api: Option<RestApiConfig>,
    main_daemon: Option<MainDaemonConfig>,
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

pub fn get_default_rest_port_from_config() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
}

pub fn get_rest_cluster_range() -> String {
    load_rest_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| RestApiConfig::default().cluster_range)
}

// --- Updated `load_storage_config_from_yaml` function ---
pub fn load_storage_config_from_yaml(config_file_path: Option<PathBuf>) -> Result<StorageConfig> {
    let default_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH);
    let project_config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    let path_to_use = config_file_path
        .or_else(|| project_config_path.exists().then(|| project_config_path))
        .unwrap_or(default_config_path);
    info!("Loading storage config from {:?}", path_to_use);

    let mut config = if path_to_use.exists() {
        let config_content = fs::read_to_string(&path_to_use)
            .context(format!("Failed to read storage config file: {}", path_to_use.display()))?;
        debug!("Raw YAML content for storage_config.yaml: {}", config_content);

        // Try StorageConfigWrapper first
        match serde_yaml2::from_str::<StorageConfigWrapper>(&config_content) {
            Ok(wrapper) => {
                info!("Successfully parsed config with 'storage' key.");
                wrapper.storage
            }
            Err(e1) => {
                warn!("Failed to parse as StorageConfigWrapper: {}", e1);
                // Try direct StorageConfig
                match serde_yaml2::from_str::<StorageConfig>(&config_content) {
                    Ok(config) => {
                        info!("Successfully parsed config directly as StorageConfig.");
                        config
                    }
                    Err(e2) => {
                        error!("Failed to parse storage config YAML at {}: {}", path_to_use.display(), e2);
                        // Log partial parse result using serde_json::Value
                        if let Ok(json_value) = serde_yaml2::from_str::<serde_json::Value>(&config_content) {
                            error!("Partial YAML parse result: {:?}", json_value);
                        } else {
                            error!("Failed to parse YAML content as JSON for partial logging");
                        }
                        return Err(anyhow!("Serialization error: {}", e2));
                    }
                }
            }
        }
    } else {
        info!("Config file not found at {}. Using default storage config.", path_to_use.display());
        StorageConfig::default()
    };

    // Ensure data_directory is set to default if None
    if config.data_directory.is_none() {
        warn!("No data_directory specified in config, applying default: {:?}", default_data_directory());
        config.data_directory = default_data_directory();
    }

    // Validate cluster range
    if !is_port_in_cluster_range(config.default_port, &config.cluster_range) {
        warn!("Port {} is outside cluster range {}", config.default_port, config.cluster_range);
        return Err(anyhow!("Port {} is outside cluster range {}", config.default_port, config.cluster_range));
    }

    // Ensure directories exist
    if let Some(data_dir) = &config.data_directory {
        fs::create_dir_all(data_dir)
            .context(format!("Failed to create data directory {:?}", data_dir))?;
        info!("Ensured data directory exists: {:?}", data_dir);
    }
    if let Some(log_dir) = &config.log_directory {
        fs::create_dir_all(log_dir)
            .context(format!("Failed to create log directory {:?}", log_dir))?;
        info!("Ensured log directory exists: {:?}", log_dir);
    } else {
        warn!("No log_directory specified in config, using default: {:?}", default_log_directory());
        config.log_directory = default_log_directory();
    }

    info!(
        "Loaded storage config: default_port={}, cluster_range={}, data_directory={:?}, storage_engine_type={:?}, engine_specific_config={:?}, log_directory={:?}, max_disk_space_gb={}, min_disk_space_gb={}, use_raft_for_scale={}",
        config.default_port,
        config.cluster_range,
        config.data_directory,
        config.storage_engine_type,
        config.engine_specific_config,
        config.log_directory,
        config.max_disk_space_gb,
        config.min_disk_space_gb,
        config.use_raft_for_scale
    );
    Ok(config)
}

// Saves the StorageConfig to a YAML file
pub fn save_storage_config_to_yaml(config: &StorageConfig, path: &Path) -> Result<()> {
    let wrapper = StorageConfigWrapper { storage: config.clone() };
    
    let yaml_string = serde_yaml::to_string(&wrapper)
        .context("Failed to serialize StorageConfig to YAML")?;

    fs::create_dir_all(path.parent().unwrap())
        .context(format!("Failed to create parent directories for {}", path.display()))?;

    fs::write(path, yaml_string)
        .context(format!("Failed to write storage config to file: {}", path.display()))?;

    Ok(())
}

pub fn load_storage_config_str(config_file_path: Option<&str>) -> Result<StorageConfig> {
    let path = config_file_path.map(PathBuf::from);
    load_storage_config_from_yaml(path)
}

pub fn get_default_storage_port_from_config_or_cli_default() -> u16 {
    load_storage_config_from_yaml(None)
        .map(|config| config.default_port)
        .unwrap_or(DEFAULT_STORAGE_PORT)
}

pub fn get_storage_cluster_range() -> String {
    load_storage_config_from_yaml(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| StorageConfig::default().cluster_range)
}

pub fn get_default_rest_port() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
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

pub fn get_default_daemon_port() -> u16 {
    load_daemon_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_DAEMON_PORT)
}

pub fn get_daemon_cluster_range() -> String {
    load_daemon_config(None)
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| DaemonYamlConfig::default().cluster_range)
}

// --- TOML CLI Config ---
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AppConfig {
    pub version: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            version: "0.1.0".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeploymentConfig {
    pub config_root_directory: PathBuf,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        DeploymentConfig {
            config_root_directory: default_config_root_directory(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LogConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PathsConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct SecurityConfig {}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            port: Some(DEFAULT_MAIN_PORT),
            host: Some("127.0.0.1".to_string()),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestConfig {
    pub port: u16,
    pub host: String,
}

impl Default for RestConfig {
    fn default() -> Self {
        RestConfig {
            port: DEFAULT_REST_API_PORT,
            host: "127.0.0.1".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DaemonConfig {
    pub port: Option<u16>,
    pub process_name: String,
    pub user: String,
    pub group: String,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        DaemonConfig {
            port: Some(DEFAULT_DAEMON_PORT),
            process_name: EXECUTABLE_NAME.to_string(),
            user: "graphdb".to_string(),
            group: "graphdb".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "snake_case")]
pub struct CliTomlStorageConfig {
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub default_port: Option<u16>,
    #[serde(with = "string_or_u16", default)]
    pub cluster_range: Option<String>,
    #[serde(default)]
    pub data_directory: Option<String>,
    #[serde(default)]
    pub config_root_directory: Option<PathBuf>,
    #[serde(default)]
    pub log_directory: Option<String>,
    #[serde(default)]
    pub max_disk_space_gb: Option<u64>,
    #[serde(default)]
    pub min_disk_space_gb: Option<u64>,
    #[serde(default)]
    pub use_raft_for_scale: Option<bool>,
    #[serde(with = "option_storage_engine_type_serde", default)]
    pub storage_engine_type: Option<StorageEngineType>,
    #[serde(default)]
    pub max_open_files: Option<u64>,
    #[serde(default)]
    pub config_file: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub struct CliConfigToml {
    pub app: AppConfig,
    pub server: ServerConfig,
    pub rest: RestConfig,
    pub daemon: DaemonConfig,
    pub storage: CliTomlStorageConfig,
    pub deployment: DeploymentConfig,
    pub log: LogConfig,
    pub paths: PathsConfig,
    pub security: SecurityConfig,
    #[serde(default)]
    pub enable_plugins: bool,
}

impl Default for CliConfigToml {
    fn default() -> Self {
        CliConfigToml {
            app: AppConfig::default(),
            server: ServerConfig::default(),
            rest: RestConfig::default(),
            daemon: DaemonConfig::default(),
            storage: CliTomlStorageConfig::default(),
            deployment: DeploymentConfig::default(),
            log: LogConfig::default(),
            paths: PathsConfig::default(),
            security: SecurityConfig::default(),
            enable_plugins: false,
        }
    }
}

impl CliConfigToml {
    pub fn save(&self) -> Result<()> {
        let config_path = PathBuf::from("/opt/graphdb/config.toml");
        let toml_string = toml::to_string(self)
            .context("Failed to serialize CliConfigToml to TOML")?;

        fs::create_dir_all(config_path.parent().unwrap())
            .context(format!("Failed to create parent directories for {}", config_path.display()))?;

        fs::write(&config_path, toml_string)
            .context(format!("Failed to write CliConfigToml to file: {}", config_path.display()))?;

        info!("Saved CLI configuration to {:?}", config_path);
        Ok(())
    }
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

// Loads engine-specific configuration
pub fn load_engine_specific_config(engine_type: &StorageEngineType) -> Result<SelectedStorageConfig> {
    let config_root = PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR);
    let config_path = get_engine_config_path(engine_type)
        .ok_or_else(|| anyhow!("No config path defined for engine {:?}", engine_type))?;

    let full_path = config_root.join(&config_path);
    info!("Loading engine-specific config for {:?} from {:?}", engine_type, full_path);

    if full_path.exists() {
        let config = SelectedStorageConfig::load_from_yaml(&full_path)
            .with_context(|| format!("Failed to load engine-specific config for {:?}", engine_type))?;
        info!("Loaded engine-specific config: {:?}", config);
        Ok(config)
    } else {
        warn!("Engine-specific config file not found at {:?}", full_path);
        match engine_type {
            StorageEngineType::Sled => Ok(SelectedStorageConfig {
                storage: StorageConfigInner {
                    path: Some(PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY))),
                    host: Some("127.0.0.1".to_string()),
                    port: Some(DEFAULT_STORAGE_PORT),
                    username: None,
                    password: None,
                    database: None,
                },
            }),
            StorageEngineType::RocksDB => Ok(SelectedStorageConfig {
                storage: StorageConfigInner {
                    path: Some(PathBuf::from(&format!("{}/rocksdb", DEFAULT_DATA_DIRECTORY))),
                    host: Some("127.0.0.1".to_string()),
                    port: Some(DEFAULT_STORAGE_PORT),
                    username: None,
                    password: None,
                    database: None,
                },
            }),
            StorageEngineType::InMemory => Ok(SelectedStorageConfig {
                storage: StorageConfigInner {
                    path: None,
                    host: Some("127.0.0.1".to_string()),
                    port: Some(DEFAULT_STORAGE_PORT),
                    username: None,
                    password: None,
                    database: None,
                },
            }),
            StorageEngineType::Redis => Ok(SelectedStorageConfig {
                storage: StorageConfigInner {
                    path: None,
                    host: Some("127.0.0.1".to_string()),
                    port: Some(6379),
                    username: None,
                    password: None,
                    database: Some("0".to_string()),
                },
            }),
            StorageEngineType::PostgreSQL => Ok(SelectedStorageConfig {
                storage: StorageConfigInner {
                    path: None,
                    host: Some("127.0.0.1".to_string()),
                    port: Some(5432),
                    username: Some("postgres".to_string()),
                    password: Some("password".to_string()),
                    database: Some("graphdb".to_string()),
                },
            }),
            StorageEngineType::MySQL => Ok(SelectedStorageConfig {
                storage: StorageConfigInner {
                    path: None,
                    host: Some("127.0.0.1".to_string()),
                    port: Some(3306),
                    username: Some("root".to_string()),
                    password: Some("password".to_string()),
                    database: Some("graphdb".to_string()),
                },
            }),
        }
    }
}

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
use log::{debug, error, info, warn, trace};
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer, Deserializer};
use serde::de::{self, MapAccess, Visitor};
use serde_yaml2 as serde_yaml;
use serde_json::{Map, Value};
use std::io::Write;
use std::sync::Arc;
use std::collections::{HashMap};
use std::fmt;
use std::env;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use dirs;
use toml;
use lib::storage_engine::config::StorageConfig as EngineStorageConfig;
use crate::cli::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction, 
                           ReloadAction, RestCliCommand, StatusAction, StorageAction, ShowAction, ShowArgs,
                           StartAction, StopAction, StopArgs, DaemonCliCommand, UseAction, SaveAction};
use super::daemon_management::{is_port_in_cluster_range, is_valid_cluster_range, parse_cluster_range}; // Import the correct helper function
pub use lib::storage_engine::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
pub use lib::storage_engine::config::{EngineTypeOnly, RocksdbConfig, SledConfig, TikvConfig, MySQLConfig, 
                                      RedisConfig, PostgreSQLConfig};
pub use models::errors::GraphError;
pub use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig, 
                                      SelectedStorageConfig as LibSelectedStorageConfig,
                                      StorageConfigInner as LibStorageConfigInner};
pub use crate::cli::config_structs::*;
pub use crate::cli::config_constants::*;
pub use crate::cli::config_defaults::*;
pub use crate::cli::config_helpers::*;
pub use crate::cli::serializers::*;
pub use crate:: cli::config_impl_cli::*;
pub use crate:: cli::config_impl_storage::*;

use lib::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};

// This is the custom deserializer function that will handle the conversion.
use serde::de::Error; // Add this import at the top of your file


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


impl Default for AppConfig {
    fn default() -> Self {
        AppConfig {
            version: "0.1.0".to_string(),
        }
    }
}


impl Default for DeploymentConfig {
    fn default() -> Self {
        DeploymentConfig {
            config_root_directory: default_config_root_directory(),
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            port: Some(DEFAULT_MAIN_PORT),
            host: Some("127.0.0.1".to_string()),
        }
    }
}

impl Default for RestConfig {
    fn default() -> Self {
        RestConfig {
            port: DEFAULT_REST_API_PORT,
            host: "127.0.0.1".to_string(),
        }
    }
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





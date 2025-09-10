use std::path::{Path, PathBuf};
pub use crate::config::config_constants::*;
pub use crate::config::config_structs::{StorageConfigInner, SelectedStorageConfig, StorageConfig};
pub use crate::config::config_helpers::{load_rest_config, load_storage_config_from_yaml, load_daemon_config};
pub use crate::config::{StorageEngineType};

pub  fn default_config_root_directory_option() -> Option<PathBuf> {
    Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR))
}
pub fn default_data_directory() -> Option<PathBuf> {
    Some(PathBuf::from(DEFAULT_DATA_DIRECTORY))
}
pub fn default_log_directory() -> Option<PathBuf> {
    Some(PathBuf::from(DEFAULT_LOG_DIRECTORY))
}
pub fn default_default_port() -> u16 { 8083 }
pub fn default_cluster_range() -> String { "8083".to_string() }
pub fn default_max_disk_space_gb() -> u64 { 1000 }
pub fn default_min_disk_space_gb() -> u64 { 10 }
pub fn default_use_raft_for_scale() -> bool { true }
pub fn default_storage_engine_type() -> StorageEngineType { StorageEngineType::Sled }
// The fix is to explicitly add the storage_engine_type to the initializer.
pub fn default_engine_specific_config() -> Option<SelectedStorageConfig> {
    Some(SelectedStorageConfig {
        storage_engine_type: StorageEngineType::Sled, // Use Sled as default
        storage: StorageConfigInner {
            path: Some(PathBuf::from(&format!("{}/sled", DEFAULT_DATA_DIRECTORY))),
            host: Some("127.0.0.1".to_string()),
            port: Some(DEFAULT_STORAGE_PORT),
            username: None,
            password: None,
            database: None,
            pd_endpoints: None,
            cache_capacity: Some(1024*1024*1024),
            use_compression: true,
        },
    })
}

pub fn default_max_open_files() -> u64 { 100 }

// Helper function to get the default RocksDB path.
// This function must exist to be called in the `default()` implementation.
pub fn default_rocksdb_path() -> PathBuf {
    let mut path = PathBuf::new();
    // Use a platform-appropriate path for data storage.
    // For demonstration, we'll use a simple, predictable path.
    // In a production app, you might use directories provided by `dirs-rs` or a similar crate.
    path.push("data_storage");
    path.push("rocksdb");
    path
}

// Helper function to get the default RocksDB port.
// This function must exist to be called in the `default()` implementation.
pub fn default_rocksdb_port() -> u16 {
    8049
}

pub fn default_path() -> Option<PathBuf> {
    Some(PathBuf::from(DEFAULT_DATA_DIRECTORY))
}

pub fn default_host() -> Option<String> {
    Some("127.0.0.1".to_string())
}

pub fn default_port() -> Option<u16> {
    Some(8049)
}

pub fn default_config_root_directory() -> PathBuf {
    PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)
}


pub fn get_default_rest_port_from_config() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
}

pub async fn get_default_daemon_port() -> u16 {
    load_daemon_config(None).await
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_DAEMON_PORT)
}

pub async fn get_default_storage_port_from_config_or_cli_default() -> u16 {
    load_storage_config_from_yaml(None).await
        .map(|config| config.default_port)
        .unwrap_or(DEFAULT_STORAGE_PORT)
}

pub async fn get_storage_cluster_range() -> String {
    load_storage_config_from_yaml(None).await
        .map(|cfg| cfg.cluster_range)
        .unwrap_or_else(|_| StorageConfig::default().cluster_range)
}

pub fn get_default_rest_port() -> u16 {
    load_rest_config(None)
        .map(|cfg| cfg.default_port)
        .unwrap_or(DEFAULT_REST_API_PORT)
}

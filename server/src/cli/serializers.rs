// server/src/cli/serializers.rs
use std::path::{Path, PathBuf};
use serde::{self, de::DeserializeOwned, Deserialize, Serialize, Serializer, Deserializer, de::Error};
use serde::de::{self, MapAccess, Visitor};
pub use lib::storage_engine::config::{StorageEngineType, StorageConfig as LibStorageConfig, 
                                      SelectedStorageConfig as LibSelectedStorageConfig,
                                      StorageConfigInner as LibStorageConfigInner};
use crate::cli::config_structs::*;
use crate::cli::config_constants::*;

pub mod string_or_u16 {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrU16 {
            String(String),
            U16(u16),
        }

        let optional_value: Option<StringOrU16> = Option::deserialize(deserializer)?;

        match optional_value {
            Some(StringOrU16::String(s)) => Ok(Some(s)),
            Some(StringOrU16::U16(u)) => Ok(Some(u.to_string())),
            None => Ok(None),
        }
    }

    pub fn serialize<S>(s: &Option<String>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match s {
            Some(value) => serializer.serialize_some(value),
            None => serializer.serialize_none(),
        }
    }
}

pub mod string_or_u16_non_option {
    use super::*;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrU16 {
            String(String),
            U16(u16),
        }

        let value: StringOrU16 = Deserialize::deserialize(deserializer)?;

        match value {
            StringOrU16::String(s) => Ok(s),
            StringOrU16::U16(u) => Ok(u.to_string()),
        }
    }

    pub fn serialize<S>(s: &String, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(s.as_str())
    }
}

// --- Custom Option<StorageEngineType> Serialization Module ---
pub mod option_storage_engine_type_serde {
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
                    "TiKV" | "tikv" => StorageEngineType::TiKV,
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


// --- Custom Option<PathBuf> Serialization Module ---
// This module is responsible for handling the Option wrapper.
pub mod option_path_buf_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::path::PathBuf;

    pub fn serialize<S>(path: &Option<PathBuf>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match path {
            Some(p) => serializer.serialize_str(&p.to_string_lossy()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<PathBuf>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: Option<String> = Option::deserialize(deserializer)?;
        Ok(s.map(PathBuf::from))
    }
}


// --- Custom StorageEngineType Serialization Module ---
// Fixed storage_engine_type_serde deserializer
pub mod storage_engine_type_serde {
    use super::*;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::str::FromStr;

    pub fn serialize<S>(engine_type: &StorageEngineType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&engine_type.to_string().to_lowercase())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<StorageEngineType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let engine_type = match s.to_lowercase().as_str() {
            "rocksdb" => StorageEngineType::RocksDB,
            "sled" => StorageEngineType::Sled,
            "tikv" => StorageEngineType::TiKV,
            "in_memory" | "inmemory" => StorageEngineType::InMemory,
            "redis" => StorageEngineType::Redis,
            "postgresql" => StorageEngineType::PostgreSQL,
            "mysql" => StorageEngineType::MySQL,
            _ => {
                eprintln!("DEBUG: Failed to deserialize StorageEngineType from: '{}'", s);
                return Err(serde::de::Error::custom(format!(
                    "unknown variant `{}`, expected one of `sled`, `rocksdb`, `tikv`, `in_memory`, `redis`, `postgresql`, `mysql`",
                    s
                )));
            },
        };
        Ok(engine_type)
    }
}

// --- Custom PathBuf Serialization Module ---
// This module is responsible for serializing a non-optional PathBuf.
pub mod path_buf_serde {
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

// --- Custom Deserializer for StorageConfig to Handle Optional Fields ---
pub mod storage_config_serde {
    use super::*;
    use serde::de::{self, MapAccess, Visitor};
    use std::fmt;

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct StorageConfigRaw {
        #[serde(with = "option_path_buf_serde", default)]
        pub config_root_directory: Option<PathBuf>,
        #[serde(with = "option_path_buf_serde", default)]
        pub data_directory: Option<PathBuf>,
        #[serde(with = "option_path_buf_serde", default)]
        pub log_directory: Option<PathBuf>,
        #[serde(default = "default_default_port")]
        pub default_port: u16,
        #[serde(with = "string_or_u16_non_option", default = "default_cluster_range")]
        pub cluster_range: String,
        #[serde(default = "default_max_disk_space_gb")]
        pub max_disk_space_gb: u64,
        #[serde(default = "default_min_disk_space_gb")]
        pub min_disk_space_gb: u64,
        #[serde(default = "default_use_raft_for_scale")]
        pub use_raft_for_scale: bool,
        #[serde(default = "default_storage_engine_type")]
        #[serde(deserialize_with = "deserialize_storage_engine_type")]
        pub storage_engine_type: StorageEngineType,
        #[serde(default)]
        pub engine_specific_config: Option<SelectedStorageConfig>,
        #[serde(default = "default_max_open_files")]
        pub max_open_files: u64,
    }

    pub fn default_default_port() -> u16 { 8083 }
    pub fn default_cluster_range() -> String { "8083-8087".to_string() }
    pub fn default_max_disk_space_gb() -> u64 { 1000 }
    pub fn default_min_disk_space_gb() -> u64 { 10 }
    pub fn default_use_raft_for_scale() -> bool { true }
    pub fn default_storage_engine_type() -> StorageEngineType { StorageEngineType::Sled }
    pub fn default_max_open_files() -> u64 { 100 }

    // Custom deserializer for storage_engine_type to handle both snake_case and kebab-case
    pub fn deserialize_storage_engine_type<'de, D>(deserializer: D) -> Result<StorageEngineType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.to_lowercase().as_str() {
            "sled" => Ok(StorageEngineType::Sled),
            "rocksdb" | "rocks_db" => Ok(StorageEngineType::RocksDB),
            "postgresql" | "postgres_sql" => Ok(StorageEngineType::PostgreSQL),
            "tikv" | "ti_kv" => Ok(StorageEngineType::TiKV),
            "redis" => Ok(StorageEngineType::Redis),
            "mysql" | "my_sql" => Ok(StorageEngineType::MySQL),
            _ => Err(de::Error::unknown_variant(&s, &["sled", "rocksdb", "rocks_db", "postgresql", "postgres_sql", "tikv", "ti_kv", "redis", "mysql", "my_sql"])),
        }
    }

    // Custom deserializer to handle both snake_case and kebab-case for all fields
    pub fn deserialize<'de, D>(deserializer: D) -> Result<StorageConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StorageConfigVisitor;

        impl<'de> Visitor<'de> for StorageConfigVisitor {
            type Value = StorageConfig;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a storage configuration")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut config = StorageConfigRaw {
                    config_root_directory: None,
                    data_directory: None,
                    log_directory: None,
                    default_port: default_default_port(),
                    cluster_range: default_cluster_range(),
                    max_disk_space_gb: default_max_disk_space_gb(),
                    min_disk_space_gb: default_min_disk_space_gb(),
                    use_raft_for_scale: default_use_raft_for_scale(),
                    storage_engine_type: default_storage_engine_type(),
                    engine_specific_config: None,
                    max_open_files: default_max_open_files(),
                };

                let fields = [
                    ("config_root_directory", "config-root-directory"),
                    ("data_directory", "data-directory"),
                    ("log_directory", "log-directory"),
                    ("default_port", "default-port"),
                    ("cluster_range", "cluster-range"),
                    ("max_disk_space_gb", "max-disk-space-gb"),
                    ("min_disk_space_gb", "min-disk-space-gb"),
                    ("use_raft_for_scale", "use-raft-for-scale"),
                    ("storage_engine_type", "storage-engine-type"),
                    ("engine_specific_config", "engine-specific-config"),
                    ("max_open_files", "max-open-files"),
                ];

                while let Some(key) = map.next_key::<String>()? {
                    let key = key.as_str();
                    let field = fields.iter().find(|&&(snake, kebab)| key == snake || key == kebab);
                    match field {
                        Some((field, _)) => match *field {
                            "config_root_directory" => config.config_root_directory = map.next_value()?,
                            "data_directory" => config.data_directory = map.next_value()?,
                            "log_directory" => config.log_directory = map.next_value()?,
                            "default_port" => config.default_port = map.next_value()?,
                            "cluster_range" => config.cluster_range = map.next_value()?,
                            "max_disk_space_gb" => config.max_disk_space_gb = map.next_value()?,
                            "min_disk_space_gb" => config.min_disk_space_gb = map.next_value()?,
                            "use_raft_for_scale" => config.use_raft_for_scale = map.next_value()?,
                            "storage_engine_type" => config.storage_engine_type = map.next_value::<StorageEngineType>()?,
                            "engine_specific_config" => config.engine_specific_config = map.next_value()?,
                            "max_open_files" => config.max_open_files = map.next_value()?,
                            _ => return Err(de::Error::unknown_field(key, &["config_root_directory", "data_directory", "log_directory", "default_port", "cluster_range", "max_disk_space_gb", "min_disk_space_gb", "use_raft_for_scale", "storage_engine_type", "engine_specific_config", "max_open_files"])),
                        },
                        None => return Err(de::Error::unknown_field(key, &["config_root_directory", "data_directory", "log_directory", "default_port", "cluster_range", "max_disk_space_gb", "min_disk_space_gb", "use_raft_for_scale", "storage_engine_type", "engine_specific_config", "max_open_files"])),
                    }
                }

                Ok(StorageConfig {
                    config_root_directory: config.config_root_directory,
                    data_directory: config.data_directory,
                    log_directory: config.log_directory,
                    default_port: config.default_port,
                    cluster_range: config.cluster_range,
                    max_disk_space_gb: config.max_disk_space_gb,
                    min_disk_space_gb: config.min_disk_space_gb,
                    use_raft_for_scale: config.use_raft_for_scale,
                    storage_engine_type: config.storage_engine_type,
                    engine_specific_config: config.engine_specific_config,
                    max_open_files: config.max_open_files,
                })
            }
        }

        deserializer.deserialize_map(StorageConfigVisitor)
    }
}

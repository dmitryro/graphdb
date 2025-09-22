use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use derivative::Derivative;
use tokio::sync::{Mutex as TokioMutex, RwLock};
use tokio::time::Duration;
use clap::{Args, Parser, Subcommand};
use uuid::Uuid;
use log::{debug, error, info, warn, trace};
use std::path::{Path, PathBuf};
use std::collections::{HashMap};
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer, Deserializer};
use serde::de::{self, MapAccess, Visitor};
use serde_yaml2 as serde_yaml;
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::{Map, Value};
use crate::config::config_defaults::*;
use crate::config::config_constants::*;
use crate::config::config_serializers::*;
use crate::query_exec_engine::query_exec_engine::{QueryExecEngine};
use crate::commands::{Commands, CommandType, StatusArgs, RestartArgs, ReloadArgs, RestartAction,
                      ReloadAction, RestCliCommand, StatusAction, StorageAction, ShowAction, ShowArgs,
                      StartAction, StopAction, StopArgs, DaemonCliCommand, UseAction, SaveAction};
use crate::daemon_utils::{is_port_in_cluster_range, is_valid_cluster_range, parse_cluster_range};
pub use crate::storage_engine::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
use models::{Vertex, Edge, Identifier, identifiers::SerializableUuid};
use models::errors::{GraphError, GraphResult};
use openraft_memstore::MemStore;
use openraft::{
    self, BasicNode, Entry, LogId, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot,
    SnapshotMeta, RaftTypeConfig, StoredMembership, Vote,
};
use rocksdb::{BoundColumnFamily, ColumnFamily, ColumnFamilyDescriptor, DB, Options, WriteOptions};

#[cfg(feature = "with-openraft-sled")]
use openraft_sled::SledRaftStorage;
use sled::{Config, Db, Tree};
use crate::daemon_registry::DaemonMetadata;

// Raft type configuration
#[derive(Clone, Debug, Default, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Copy)]
pub struct TypeConfig;

/// The type for a Raft node's unique ID.
pub type NodeIdType = u64;

// The NodeId must be `u64` as per the `openraft` crate's definition.

// Define Raft application's request and response types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AppResponse {
    SetResponse(String),
    DeleteResponse,
}

impl RaftTypeConfig for TypeConfig {
    type D = AppRequest;
    type R = AppResponse;
    type NodeId = NodeIdType;
    type Node = NodeIdType; // Simplified to use NodeIdType directly
    type Entry = openraft::Entry<Self>;
    type SnapshotData = std::io::Cursor<Vec<u8>>;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::raft::responder::OneshotResponder<Self>;
}

/*
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppResponse {
    pub message: String,
}*/

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftResponse {
    SetResponse(String),
    DeleteResponse,
}

/// Represents a plan for a graph query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlan {
    pub query: String,
}

/// Represents the result of an executed graph query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueryResult {
    Success(String),
    Null,
}

/// Replication strategy for data operations
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ReplicationStrategy {
    AllNodes,
    NNodes(usize),
    Raft,
}

/// Health status of a daemon node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHealth {
    pub port: u16,
    pub is_healthy: bool,
    pub last_check: SystemTime,
    pub response_time_ms: u64,
    pub error_count: u32,
}
/// Load balancer for routing requests across healthy nodes
#[derive(Derivative, Clone, Serialize, Deserialize)]
#[derivative(Debug, Default)]
pub struct LoadBalancer {
    #[serde(skip)]
    #[derivative(Debug = "ignore")]
    pub nodes: Arc<RwLock<HashMap<u16, NodeHealth>>>,

    #[serde(skip)]
    #[derivative(Debug = "ignore")]
    pub current_index: Arc<TokioMutex<usize>>,

    pub replication_factor: usize,

    #[serde(skip)]
    #[derivative(Debug = "ignore")]
    pub healthy_nodes: Arc<RwLock<VecDeque<NodeHealth>>>,

    #[serde(skip)]
    #[derivative(Debug = "ignore")]
    pub health_check_config: HealthCheckConfig,
}


#[derive(Clone)]
pub struct HealthCheckConfig {
    pub interval: Duration,
    pub connect_timeout: Duration,
    pub response_buffer_size: usize,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(2),
            response_buffer_size: 1024,
        }
    }
}

/// Storage engine types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StorageEngineType {
    Hybrid,
    Sled,
    RocksDB,
    TiKV,
    InMemory,
    Redis,
    PostgreSQL,
    MySQL,
}

pub struct MemStoreForTypeConfig {
    pub inner: Arc<MemStore>,
}

/// Engine configuration from YAML
#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    pub storage: HashMap<String, Value>,
}

/// Struct to hold sled::Db and its path
#[derive(Debug)]
pub struct SledDbWithPath {
    pub db: Arc<sled::Db>,
    pub path: PathBuf,
}

/// Sled daemon configuration
#[derive(Debug, Clone)]
pub struct SledDaemon {
    pub port: u16,
    pub db_path: PathBuf,
    pub db: Arc<Db>,
    pub vertices: Tree,
    pub edges: Tree,
    pub kv_pairs: Tree,
    pub running: Arc<TokioMutex<bool>>,
    #[cfg(feature = "with-openraft-sled")]
    pub raft_storage: Arc<openraft_sled::SledRaftStorage<TypeConfig>>,
    #[cfg(feature = "with-openraft-sled")]
    pub node_id: u64,
}

/// Sled daemon pool
#[derive(Debug, Default, Clone)]
pub struct SledDaemonPool {
    pub daemons: HashMap<u16, Arc<SledDaemon>>,
    pub registry: Arc<RwLock<HashMap<u16, DaemonMetadata>>>,
    pub initialized: Arc<RwLock<bool>>,
    pub load_balancer: Arc<LoadBalancer>,
    pub use_raft_for_scale: bool, // Changed from use_raft
}

/// Sled storage configuration
#[derive(Debug, Clone)]
pub struct SledStorage {
    pub pool: Arc<TokioMutex<SledDaemonPool>>,
}

/// Struct to hold rocksdb::DB and its path
#[derive(Debug)]
pub struct RocksDbWithPath {
    pub db: Arc<DB>,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    SetKey { key: Vec<u8>, value: Vec<u8> },
    DeleteKey { key: Vec<u8> },
    CreateVertex(Vertex),
    UpdateVertex(Vertex),
    DeleteVertex(Uuid),
    CreateEdge(Edge),
    UpdateEdge(Edge),
    Set { key: String, value: String, cf: String },
    Insert { key: String, value: String, cf: String },
    Remove { key: String, cf: String },
    Delete { key: String, cf: String },
    DeleteEdge { outbound_id: Uuid, edge_type: Identifier, inbound_id: Uuid },
}


// The AppRequest enum wraps your RaftCommand. OpenRaft is generic over this
// type, allowing it to handle your specific application commands.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AppRequest {
    // The Command variant holds the actual application logic to be executed.
    Command(RaftCommand),
}

// Add an enum to track the client mode
#[derive(Debug, Clone)]
pub enum RocksDBClientMode {
    Direct,    // Direct database access (original behavior)
    ZMQ(u16),  // ZMQ communication with daemon on specified port
}

#[derive(Debug, Clone)]
pub enum SledClientMode {
    Direct,
    ZMQ(u16),
}

/// RocksDB Client
#[derive(Debug, Clone)]
pub struct RocksDBClient {
    pub inner: Arc<TokioMutex<Arc<DB>>>,
    pub db_path: PathBuf,
    pub is_running: bool,
    pub mode: Option<RocksDBClientMode>,
}


#[derive(Clone, Debug)]
pub struct SledClient {
    pub inner: Arc<TokioMutex<Arc<Db>>>,
    pub db_path: PathBuf,
    pub is_running: Arc<TokioMutex<bool>>,  // Changed from bool
    pub mode: Option<SledClientMode>,
}


/// RocksDB Raft storage
#[derive(Clone)]
pub struct RocksDBRaftStorage {
    pub db: Arc<DB>,
    pub state_cf_name: String,
    pub log_cf_name: String,
    pub snapshot_cf_name: String,
    pub snapshot_meta_cf_name: String,
    pub state_cf: Arc<BoundColumnFamily<'static>>,
    pub log_cf: Arc<BoundColumnFamily<'static>>,
    pub snapshot_cf: Arc<BoundColumnFamily<'static>>,
    pub snapshot_meta_cf: Arc<BoundColumnFamily<'static>>,
    pub config: StorageConfig,
    pub client: Option<Arc<RocksDBClient>>,
}

/// RocksDB daemon configuration
#[derive(Debug, Clone)]
pub struct RocksDBDaemon {
    pub port: u16,
    pub db_path: PathBuf,
    pub db: Arc<DB>,
    pub running: Arc<TokioMutex<bool>>,
    #[cfg(feature = "with-openraft-rocksdb")]
    pub raft_storage: Arc<RocksDBRaftStorage>,
    #[cfg(feature = "with-openraft-rocksdb")]
    pub node_id: u64,
}

/// RocksDB daemon pool
#[derive(Debug, Default, Clone)]
pub struct RocksDBDaemonPool {
    pub daemons: HashMap<u16, Arc<RocksDBDaemon>>,
    pub registry: Arc<RwLock<HashMap<u16, DaemonMetadata>>>,
    pub initialized: Arc<RwLock<bool>>,
    pub load_balancer: Arc<LoadBalancer>,
    pub use_raft_for_scale: bool, // Changed from use_raft
}

/// RocksDB storage configuration
#[derive(Debug, Clone)]
pub struct RocksDBStorage {
    pub use_raft_for_scale: bool,
    pub pool: Arc<TokioMutex<RocksDBDaemonPool>>,
    #[cfg(feature = "with-openraft-rocksdb")]
    pub raft: Option<openraft::Raft<TypeConfig>>,
}

/// Engine type configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EngineTypeOnly {
    pub storage_engine_type: StorageEngineType,
}

/// Hybrid storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

/// RocksDB configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDBConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub temporary: bool,
    pub use_compression: bool,
    pub use_raft_for_scale: bool,
    pub cache_capacity: Option<u64>,
    pub max_background_jobs: Option<u16>,
}

/// Sled configuration
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct SledConfig {
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub temporary: bool,
    pub use_compression: bool,
    pub cache_capacity: Option<u64>,
    pub storage_engine_type: StorageEngineType,
}

impl Default for SledConfig {
    fn default() -> Self {
        SledConfig {
            host: Some(String::from("127.0.0.1")),
            port: Some(8049),
            path: PathBuf::from("/opt/graphdb/storage_data/sled"),
            temporary: false,
            use_compression: true,
            cache_capacity: None,
            storage_engine_type: StorageEngineType::Sled,
        }
    }
}

/// TiKV configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TikvConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub pd_endpoints: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Redis configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

/// MySQL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySQLConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

/// PostgreSQL configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgreSQLConfig {
    pub storage_engine_type: StorageEngineType,
    pub path: PathBuf,
    pub host: Option<String>,
    pub port: Option<u16>,
}

/// Raw storage configuration from YAML
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

/// Inner storage configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageConfigInner {
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
    #[serde(default)]
    pub pd_endpoints: Option<String>,
    #[serde(default)]
    pub use_compression: bool,
    #[serde(default)]
    pub cache_capacity: Option<u64>,
    #[serde(default = "default_temporary")]
    pub temporary: bool,
    #[serde(default = "default_use_raft_for_scale")]
    pub use_raft_for_scale: bool,
}

fn default_use_raft_for_scale() -> bool {
    false // Matches StorageConfig default
}

fn default_temporary() -> bool {
    false
}

/// Selected storage configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SelectedStorageConfig {
    #[serde(with = "storage_engine_type_serde")]
    pub storage_engine_type: StorageEngineType,
    #[serde(flatten)]
    pub storage: StorageConfigInner,
}

/// Wrapper for selected storage configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SelectedStorageConfigWrapper {
    pub storage: SelectedStorageConfig,
}

/// Main daemon configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MainDaemonConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

/// Wrapper for main daemon configuration
#[derive(Debug, Deserialize)]
pub struct MainConfigWrapper {
    pub main_daemon: MainDaemonConfig,
}

/// REST API configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RestApiConfig {
    pub data_directory: String,
    pub log_directory: String,
    pub default_port: u16,
    pub cluster_range: String,
}

/// Wrapper for REST API configuration
#[derive(Debug, Deserialize, Serialize)]
pub struct RestApiConfigWrapper {
    pub config_root_directory: String,
    pub rest_api: RestApiConfig,
}

/// Daemon YAML configuration
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

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(with = "option_path_buf_serde", default, alias = "config-root-directory")]
    pub config_root_directory: Option<PathBuf>,
    #[serde(with = "option_path_buf_serde", default = "default_data_directory", alias = "data-directory")]
    pub data_directory: Option<PathBuf>,
    #[serde(with = "option_path_buf_serde", default = "default_log_directory", alias = "log-directory")]
    pub log_directory: Option<PathBuf>,
    #[serde(default = "default_default_port", alias = "default-port")]
    pub default_port: u16,
    #[serde(with = "string_or_u16_non_option", default = "default_cluster_range", alias = "cluster-range")]
    pub cluster_range: String,
    #[serde(default = "default_max_disk_space_gb", alias = "max-disk-space-gb")]
    pub max_disk_space_gb: u64,
    #[serde(default = "default_min_disk_space_gb", alias = "min-disk-space-gb")]
    pub min_disk_space_gb: u64,
    #[serde(default = "default_use_raft_for_scale", alias = "use-raft-for-scale")]
    pub use_raft_for_scale: bool,
    #[serde(with = "storage_engine_type_serde", default = "default_storage_engine_type", alias = "storage-engine-type")]
    pub storage_engine_type: StorageEngineType,
    #[serde(default = "default_engine_specific_config")]
    pub engine_specific_config: Option<SelectedStorageConfig>,
    #[serde(default = "default_max_open_files", alias = "max-open-files")]
    pub max_open_files: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfigWrapper {
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineSpecificConfig {
    pub storage_engine_type: StorageEngineType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pd_endpoints: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EngineSpecificConfigWrapper {
    storage: EngineSpecificConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TiKVConfigWrapper {
    pub storage: SpecificEngineFileConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SpecificEngineFileConfig {
    #[serde(with = "storage_engine_type_serde", alias = "storage-engine-type")]
    pub storage_engine_type: StorageEngineType,
    #[serde(with = "option_path_buf_serde", default, alias = "path")]
    pub path: Option<PathBuf>,
    #[serde(default, alias = "host")]
    pub host: Option<String>,
    #[serde(default, alias = "port")]
    pub port: Option<u16>,
    #[serde(default, alias = "database")]
    pub database: Option<String>,
    #[serde(default, alias = "username")]
    pub username: Option<String>,
    #[serde(default, alias = "password")]
    pub password: Option<String>,
    #[serde(default, alias = "pd_endpoints")]
    pub pd_endpoints: Option<String>,
}

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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GlobalYamlConfig {
    pub storage: Option<StorageYamlConfig>,
    pub rest_api: Option<RestApiYamlConfig>,
    pub main_daemon: Option<MainDaemonYamlConfig>,
    pub config_root_directory: Option<PathBuf>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageYamlConfig {
    pub default_port: u16,
    pub cluster_range: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RestApiYamlConfig {
    pub default_port: u16,
    pub cluster_range: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MainDaemonYamlConfig {
    pub default_port: u16,
    pub cluster_range: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub version: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        trace!("Creating default AppConfig");
        AppConfig { version: Some("0.1.0".to_string()) }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct DeploymentConfig {
    #[serde(rename = "config-root-directory")]
    pub config_root_directory: Option<PathBuf>,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        trace!("Creating default DeploymentConfig");
        DeploymentConfig {
            config_root_directory: Some(PathBuf::from(DEFAULT_CONFIG_ROOT_DIRECTORY_STR)),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct LogConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct PathsConfig {}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
pub struct SecurityConfig {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
    pub max_connections: u32,
}

impl Default for ServerConfig {
    fn default() -> Self {
        trace!("Creating default ServerConfig");
        ServerConfig {
            port: Some(DEFAULT_STORAGE_PORT),
            host: Some("127.0.0.1".to_string()),
            max_connections: 100,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RestConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
}

impl Default for RestConfig {
    fn default() -> Self {
        trace!("Creating default RestConfig");
        RestConfig {
            port: Some(DEFAULT_REST_API_PORT),
            host: Some("127.0.0.1".to_string()),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DaemonConfig {
    pub port: Option<u16>,
    pub process_name: String,
    pub user: String,
    pub group: String,
    pub default_port: u16,
    pub cluster_range: String,
    pub max_connections: u32,
    pub max_open_files: u64,
    pub use_raft_for_scale: bool,
    pub log_level: String,
    #[serde(default)]
    pub log_directory: Option<String>,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        trace!("Creating default DaemonConfig");
        DaemonConfig {
            port: None,
            process_name: "graphdb".to_string(),
            user: "graphdb".to_string(),
            group: "graphdb".to_string(),
            default_port: 8049,
            cluster_range: "8000-9000".to_string(),
            max_connections: 300,
            max_open_files: 100,
            use_raft_for_scale: false,
            log_level: "DEBUG".to_string(),
            log_directory: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
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

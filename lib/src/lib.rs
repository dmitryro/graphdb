// lib/src/lib.rs
// Updated: 2025-07-04 - Refactored to use new storage engine names and removed obsolete imports.

#![cfg_attr(feature = "bench-suite", feature(test))]

pub mod query_parser;
pub mod engine;
pub mod graph_evolution;
pub mod network_interfaces;
pub mod plugin_system;
pub mod database;
pub mod memory; // Now contains InMemoryGraphStorage
pub mod errors;
pub mod query_exec_engine;
pub mod storage_engine; // This declares the directory `storage_engine`
pub mod transact_indexing;
pub mod indexing_caching;
pub mod daemon;
pub mod util;
pub use config::StorageEngineType;
pub mod config;
pub mod commands;

// Now, import directly from the 'models' crate.
pub use models::{Edge, Identifier, Json, Vertex}; // Added Vertex here for convenience

// Fix the imports by specifying the correct sub-modules
pub use models::bulk_insert::BulkInsertItem;
pub use models::queries::EdgeDirection;
pub use models::properties::{EdgeProperties, NamedProperty, PropertyValue, VertexProperties};
pub use models::queries::{Query, QueryOutputValue};
pub use models::medical::{Login, User};
pub use commands::*;
pub use daemon::*;

#[cfg(feature = "bench-suite")]
extern crate test;

#[cfg(feature = "test-suite")]
#[macro_use]
pub mod tests;

#[cfg(feature = "bench-suite")]
#[macro_use]
pub mod benches;

// Explicit re-exports
pub use crate::indexing_caching::{index_node, cache_node_state, get_cached_node_state};
pub use crate::database::*; // Re-exports the main Database struct
pub use crate::errors::*;
pub use crate::memory::InMemoryGraphStorage; // Re-export the new in-memory storage

pub use daemon_registry::{
    GLOBAL_DAEMON_REGISTRY,
    DaemonRegistry, DaemonMetadata,
};
pub use crate::daemon_config::{
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
    DEFAULT_DAEMON_PORT,
    DEFAULT_REST_API_PORT,
    DAEMON_REGISTRY_DB_PATH,
    DAEMON_PID_FILE_NAME_PREFIX,
    REST_PID_FILE_NAME_PREFIX,
    STORAGE_PID_FILE_NAME_PREFIX
};

pub use crate::daemon_utils::{
    find_pid_by_port,
    stop_process,
};
// Re-export from storage_engine/mod.rs (assuming it exists and re-exports these)
pub use crate::storage_engine::{StorageEngine, GraphStorageEngine, SledStorage, log_lock_file_diagnostics};
#[cfg(feature = "with-rocksdb")]
pub use crate::storage_engine::rocksdb_storage::RocksdbStorage; // Re-export the new RocksDB storage

// Do NOT glob-import engine and models together to avoid ambiguity
// Instead, re-export them under namespaces
pub mod api {
    pub use crate::engine::{
        Graph,
        Edge as EngineEdge,
        Vertex as EngineVertex,
        properties::PropertyValue as EnginePropertyValue
    };
    pub use models::{
        Edge as ModelEdge,
        Vertex as ModelVertex,
        Identifier,
        Json
    };
}

// The 'sled' module and 'SledDatastore' alias are likely obsolete if SledStorage
// is directly used via `storage_engine`. Remove if not needed.
// #[cfg(feature = "sled-datastore")]
// pub mod sled; // This declares the 'sled' module, referring to src/sled/managers.rs (implicitly)
// #[cfg(feature = "sled-datastore")]
// pub use crate::sled::managers::SledManager as SledDatastore;

// Type alias for CurrentDatastore, now using GraphStorageEngine implementations
#[cfg(feature = "with-rocksdb")]
pub type CurrentGraphStorage = RocksdbStorage; // If RocksDB is enabled, use it
#[cfg(not(feature = "with-rocksdb"))]
pub type CurrentGraphStorage = InMemoryGraphStorage; // Otherwise, default to in-memory (or Sled if preferred)

// You might also want a default for when no feature is enabled, e.g.:
// pub type CurrentGraphStorage = SledStorage; // Assuming Sled is always available


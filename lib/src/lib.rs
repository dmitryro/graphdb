// lib/src/lib.rs
// Updated: 2025-06-30 - Corrected pub mod declarations based on actual file existence.

#![cfg_attr(feature = "bench-suite", feature(test))]

pub mod query_parser;
pub mod engine;
pub mod graph_evolution;
pub mod network_interfaces;
pub mod plugin_system;
pub mod database;
pub mod memory;
pub mod models;
pub mod errors;
pub mod query_exec_engine;
pub mod storage_engine; // This declares the directory `storage_engine`
pub mod transact_indexing;
pub mod indexing_caching;
pub mod util;

// REMOVED: pub mod security; // This is a separate crate, not a module within lib/src/
// REMOVED: pub mod startup;
// REMOVED: pub mod schema;
// REMOVED: pub mod data_processing;
// REMOVED: pub mod api; // This is defined later inline as `pub mod api { ... }`
// REMOVED: pub mod telemetry;
// REMOVED: pub mod constants;


pub use crate::models::{Edge, Identifier, Json};

#[cfg(feature = "with_rocksdb")]
mod rocksdb_storage; // Assumes rocksdb_storage.rs is directly in lib/src/

#[cfg(feature = "with_sled")]
// use storage_engine::sled_storage; // Removed as it's redundant and causes "unused import"

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
pub use crate::database::*;
pub use crate::errors::*;
pub use crate::memory::*;
pub use crate::engine::properties::PropertyValue;

// Re-export User and Login from medical models for convenience
pub use crate::models::medical::{User, Login};
// Re-export from storage_engine/mod.rs
pub use crate::storage_engine::{SledUserStorage, UserStorageEngine, open_sled_db, StorageEngine, GraphStorageEngine};


// Do NOT glob-import engine and models together to avoid ambiguity
// Instead, re-export them under namespaces
pub mod api { // This is the correct, inline definition of the 'api' module
    pub use crate::engine::{
        Graph,
        Edge as EngineEdge,
        Vertex as EngineVertex,
        properties::PropertyValue
    };
    pub use crate::models::{
        Edge as ModelEdge,
        Vertex as ModelVertex,
        Identifier,
        Json
    };
}

#[cfg(feature = "rocksdb-datastore")]
mod rdb;

#[cfg(feature = "rocksdb-datastore")]
pub use crate::rdb::RocksdbDatastore;

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
pub mod storage_engine;
pub mod transact_indexing;
pub mod indexing_caching;
pub mod util;
pub use crate::models::{Edge, Identifier, Json};

#[cfg(feature = "with_rocksdb")]
mod rocksdb_storage;

#[cfg(feature = "with_sled")]
mod sled_storage;

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

// Do NOT glob-import engine and models together to avoid ambiguity
// Instead, re-export them under namespaces
pub mod api {
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


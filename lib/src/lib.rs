#![cfg_attr(feature = "bench-suite", feature(test))]

pub mod query_parser; // Ensure this line exists
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
pub mod indexing_caching; // Import the module
pub use crate::indexing_caching::{index_node, cache_node_state, get_cached_node_state};
pub mod util;


#[cfg(feature = "bench-suite")]
extern crate test;

#[cfg(feature = "test-suite")]
#[macro_use]
pub mod tests;
#[cfg(feature = "bench-suite")]
#[macro_use]
pub mod benches;

pub use crate::memory::*;
pub use crate::models::*;
pub use crate::database::*;
pub use crate::errors::*;


#[cfg(feature = "rocksdb-datastore")]
mod rdb;

#[cfg(feature = "rocksdb-datastore")]
pub use crate::rdb::RocksdbDatastore;

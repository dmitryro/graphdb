// lib/src/lib.rs
// Updated: 2025-07-02 - Refactored to use the new 'models' crate for shared types.
// Corrected unresolved imports for SledUserStorage and UserStorageEngine.

#![cfg_attr(feature = "bench-suite", feature(test))]

pub mod query_parser;
pub mod engine;
pub mod graph_evolution;
pub mod network_interfaces;
pub mod plugin_system;
pub mod database;
pub mod memory;
// REMOVED: pub mod models; // Models are now in a separate crate!
pub mod errors;
pub mod query_exec_engine;
pub mod storage_engine; // This declares the directory `storage_engine`
pub mod transact_indexing;
pub mod indexing_caching;
pub mod util;

// Now, import directly from the 'models' crate.
// Ensure these are correctly named based on what `models/src/lib.rs` re-exports.
// For instance, if models::edges::Edge is simply re-exported as models::Edge, then `models::Edge` is correct.
pub use models::{Edge, Identifier, Json};

// Fix the imports by specifying the correct sub-modules
pub use models::bulk_insert::BulkInsertItem;
pub use models::queries::EdgeDirection; // CORRECTED PATH (as per previous fix)
pub use models::properties::{EdgeProperties, NamedProperty, PropertyValue, VertexProperties}; // Keep this one for PropertyValue
pub use models::queries::{Query, QueryOutputValue};
pub use models::medical::{Login, User};

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
// REMOVED: pub use models::PropertyValue; // <-- REMOVED THIS DUPLICATE LINE

// Re-export from storage_engine/mod.rs
// Removed SledUserStorage and UserStorageEngine as their definitions/re-exports are not available.
// If these are needed, ensure they are correctly defined and re-exported in storage_engine/mod.rs
// and storage_engine/user_storage.rs
pub use crate::storage_engine::{open_sled_db, StorageEngine, GraphStorageEngine};


// Do NOT glob-import engine and models together to avoid ambiguity
// Instead, re-export them under namespaces
pub mod api {
    pub use crate::engine::{
        Graph,
        Edge as EngineEdge,
        Vertex as EngineVertex,
        // CHECK THIS: If `engine::properties::PropertyValue` is distinct from `models::properties::PropertyValue`
        // and needs to be exposed here. If not, consider removing or aliasing appropriately.
        properties::PropertyValue as EnginePropertyValue // Aliased to avoid conflict if both exist
    };
    // Now, import ModelEdge, ModelVertex, Identifier, Json from the new `models` crate
    pub use models::{ // Already imported at the top, just re-exporting under `api`
        Edge as ModelEdge,
        Vertex as ModelVertex,
        Identifier,
        Json
    };
}

#[cfg(feature = "sled-datastore")]
pub mod sled; // This declares the 'sled' module, referring to src/sled/managers.rs (implicitly)

#[cfg(feature = "sled-datastore")]
// Assuming your main Sled manager struct is named SledManager
// and you want to re-export it as 'SledDatastore' for consistency with the previous name
pub use crate::sled::managers::SledManager as SledDatastore;

// If you had a mechanism to choose between datastores based on feature,
// you might then have a type alias or a trait implementation somewhere that uses it:

// #[cfg(feature = "rocksdb-datastore")]
// pub type CurrentDatastore = RocksdbDatastore;

#[cfg(feature = "sled-datastore")]
pub type CurrentDatastore = SledDatastore;


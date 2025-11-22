use std::sync::Arc;
use anyhow::Result;
use serde_json::Value;

// Required for the concrete database handles in EngineHandles
use sled::Db;
use rocksdb::DB; 

// CRITICAL IMPORT: Use the authoritative StorageEngineType definition
use crate::config::config_structs::StorageEngineType;

// --- Module Declarations ---
pub mod tantivy_engine; // The concrete Tantivy implementation
pub mod service;        // The public API (singleton and delegation)
// Export the public types and traits
pub mod backend;
pub use backend::{IndexingBackend, IndexingError, Document, IndexResult};

pub use tantivy_engine::{ TantivyIndexingEngine };

// -----------------------------------------------------------------
// ---------------------- FOUNDATION TYPES -------------------------
// -----------------------------------------------------------------

/// Core interface for managing and searching the Tantivy index.
/// All concrete indexing engines (like TantivyIndexingEngine) must implement this trait,
/// ensuring the public IndexingService remains storage-engine-agnostic.
/*
pub trait IndexingBackend: Send + Sync {
    /// Returns the type of storage engine this backend is associated with.
    fn engine_type(&self) -> StorageEngineType;
    
    // --- Standard Indexing ---
    fn create_index(&self, label: &str, property: &str) -> Result<Value>;
    fn drop_index(&self, label: &str, property: &str) -> Result<Value>;
    
    // --- Full-Text Indexing ---
    fn create_fulltext_index(
        &self,
        name: &str,
        labels: &[&str],
        properties: &[&str],
    ) -> Result<Value>;
    fn drop_fulltext_index(&self, name: &str) -> Result<Value>;
    fn fulltext_search(&self, query: &str, limit: usize) -> Result<Value>;

    // --- Maintenance & Stats ---
    fn rebuild_indexes(&self) -> Result<Value>;
    fn index_stats(&self) -> Result<Value>;
}
*/
// -----------------------------------------------------------------
// ---------------------- PUBLIC RE-EXPORTS ------------------------
// -----------------------------------------------------------------

/// Re-export the core public API struct and functions from the service module.
pub use service::{
    IndexingService, 
    init_indexing_service, 
    indexing_service,
    EngineHandles,
};


use std::sync::Arc;
use anyhow::{Result, Context, anyhow}; // Added `anyhow` for the macro to map errors
use serde_json::Value;
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use sled::Db;
use rocksdb::DB;

// --- CRITICAL IMPORTS ---
use crate::config::config_structs::StorageEngineType;
use crate::indexing::tantivy_engine::{
    TantivyIndexingEngine,
    // Import and rename the concrete Tantivy engine types for mapping/conversion
    EngineHandles as TantivyEngineHandles,
};
use crate::indexing::IndexingBackend; // The trait
use crate::indexing::backend::IndexingError; // Needed for error mapping in public methods

// --- DATABASE HANDLES ---
/// Abstraction to hold the actual Arc-wrapped database handles
/// specific to the storage engine being initialized.
pub enum EngineHandles {
    Sled(Arc<Db>),
    RocksDB(Arc<DB>),
    TiKV(String), // Placeholder
    Redis(String), // Placeholder
    None, // For engines like InMemory where no persistent metadata is needed
}

// --- SERVICE DEFINITIONS ---

pub type Service = Arc<TokioMutex<IndexingService>>; // Made type public for use in SledDaemon

// Global, lazily initialized singleton for thread-safe access to the indexing logic.
static SINGLETON: OnceCell<Service> = OnceCell::const_new();

/// The core public API for all indexing operations.
/// It holds the concrete Tantivy implementation behind the IndexingBackend trait.
#[derive(Clone)]
pub struct IndexingService {
    backend: Arc<dyn IndexingBackend>,
}

impl IndexingService {
    pub fn new(backend: Arc<dyn IndexingBackend>) -> Self {
        Self { backend }
    }

    /* ----------------------------------------------------------------- */
    /* ---------------------- PUBLIC API METHODS ----------------------- */
    /* ----------------------------------------------------------------- */
    
    /// Creates a standard index (for equality/constraint checks).
    /// Delegates directly to the underlying engine.
    pub async fn create_index(&self, label: &str, property: &str) -> Result<Value> {
        // FIX: Must await the future returned by the backend call, then map the result
        self.backend.create_index(label, property).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    /// Drops a standard index.
    pub async fn drop_index(&self, label: &str, property: &str) -> Result<Value> {
        // FIX: Must await and map the result
        self.backend.drop_index(label, property).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    /// Lists all active standard and full-text indexes.
    pub async fn list_indexes(&self) -> Result<Value> {
        // NOTE: If list_indexes is not on the trait, index_stats is used as a proxy.
        // Assume the trait is corrected or we use the proxy:
        // FIX: Must await and map the result
        self.backend.index_stats().await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }
    
    /// Creates a full-text search index using Tantivy.
    /// The labels and properties define what data gets funnelled into the index.
    pub async fn create_fulltext_index(
        &self,
        name: &str,
        labels: &[&str],
        properties: &[&str],
    ) -> Result<Value> {
        // FIX: Must await and map the result
        self.backend.create_fulltext_index(name, labels, properties).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    /// Drops a full-text search index.
    pub async fn drop_fulltext_index(&self, name: &str) -> Result<Value> {
        // FIX: Must await and map the result
        self.backend.drop_fulltext_index(name).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    /// Executes a full-text search query.
    pub async fn fulltext_search(&self, query: &str, limit: usize) -> Result<Value> {
        // FIX: Must await and map the result
        self.backend.fulltext_search(query, limit).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    /// Rebuilds all existing indexes (usually a maintenance task).
    pub async fn rebuild_indexes(&self) -> Result<Value> {
        // FIX: Must await and map the result
        self.backend.rebuild_indexes().await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    /// Provides statistics about the indexing backend (e.g., doc count, segment count).
    pub async fn index_stats(&self) -> Result<Value> {
        // FIX: Must await and map the result
        self.backend.index_stats().await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }
}

/* ----------------------------------------------------------------- */
/* ----------------------- PUBLIC HELPERS -------------------------- */
/* ----------------------------------------------------------------- */

/// Initializes the global IndexingService with the specified engine type and database handles.
///
/// This is called once by the storage daemon pool (Sled, RocksDB, etc.) during startup.
/// It returns the initialized service or panics on failure.
pub async fn init_indexing_service(
    engine_type: StorageEngineType,
    handles: EngineHandles,
) -> Result<Service> {
    // Attempt to set the singleton. If it's already set (Ok), return the existing one.
    // If it's not set (Err), run the initialization closure.
    let service = SINGLETON.get_or_try_init(|| async {
        
        // FIX: The erroneous conversion from StorageEngineType to the non-existent 
        // TantivyStorageEngineType is removed. We use the input `engine_type` directly,
        // relying on the fact that TantivyIndexingEngine expects the unified StorageEngineType.
        let backend_engine_type = engine_type;
        
        // Explicitly convert the input EngineHandles to the Tantivy engine's expected type
        let backend_handles = match handles {
            EngineHandles::Sled(h) => TantivyEngineHandles::Sled(h),
            EngineHandles::RocksDB(h) => TantivyEngineHandles::RocksDB(h),
            EngineHandles::TiKV(s) => TantivyEngineHandles::TiKV(s),
            EngineHandles::Redis(s) => TantivyEngineHandles::Redis(s),
            EngineHandles::None => TantivyEngineHandles::None,
        };

        // Instantiate the concrete TantivyIndexingEngine, connecting it to the DB handles
        let backend = TantivyIndexingEngine::new(backend_engine_type, backend_handles)
            .context("Failed to initialize TantivyIndexingEngine")?;
        
        // Wrap the backend in an Arc<dyn IndexingBackend> and then in the service wrapper
        Ok(Arc::new(TokioMutex::new(IndexingService::new(Arc::new(backend)))))
    })
    .await
    // FIX: This map_err is fine, but we ensure `anyhow` is imported for the macro
    .map_err(|e: anyhow::Error| anyhow::anyhow!("Indexing service failed initialization: {}", e))?;
        
    Ok(service.clone())
}

/// Provides cheap, thread-safe access to the already-built IndexingService.
/// Panics if called before `init_indexing_service` has completed.
pub fn indexing_service() -> Service {
    SINGLETON
        .get()
        .expect("IndexingService not initialised – call init_indexing_service first")
        .clone()
}
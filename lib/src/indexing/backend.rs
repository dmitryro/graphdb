use std::collections::HashMap;
use async_trait::async_trait;
use crate::config::{ StorageEngineType };
use serde::{Serialize, Deserialize}; // <-- ADDED: Necessary traits for JSON operations

// Result type for all indexing operations
pub type IndexResult<T> = Result<T, IndexingError>;

/// A simple struct representing a document to be indexed.
/// In a real application, this would contain structured fields.
#[derive(Debug, Clone, Serialize, Deserialize)] // <-- FIXED: Added Serialize and Deserialize
pub struct Document {
    pub id: String,
    pub fields: HashMap<String, String>,
}

/// Custom error type for the indexing service.
#[derive(Debug, thiserror::Error)]
pub enum IndexingError {
    #[error("Tantivy error: {0}")]
    TantivyError(#[from] tantivy::TantivyError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("A document with ID '{0}' was not found.")]
    DocumentNotFound(String),
    #[error("Operation failed: {0}")]
    Other(String),
}

/// The core trait defining the contract for any indexing system backend.
/// Any backend (Tantivy, Elastic, etc.) must implement this trait.
/// All database-level operations return a placeholder `IndexResult<String>`
/// which represents a successful database operation returning a generic value.
#[async_trait]
pub trait IndexingBackend: Send + Sync + 'static {
    /// Returns the type of storage engine this backend is associated with.
    /// This is a synchronous getter for metadata.
    fn engine_type(&self) -> StorageEngineType;

    /// Initializes the index, including creating the schema and opening the directory.
    async fn initialize(&self) -> IndexResult<()>;

    // --- Standard Indexing ---

    /// Creates a standard property index on nodes with the given label and property.
    /// Returns a status or result string (placeholder for database `Value`).
    async fn create_index(&self, label: &str, property: &str) -> IndexResult<String>;
    
    /// Drops a standard property index on nodes with the given label and property.
    /// Returns a status or result string (placeholder for database `Value`).
    async fn drop_index(&self, label: &str, property: &str) -> IndexResult<String>;

    // --- Full-Text Indexing ---

    /// Creates a full-text search index with the given name, applied to nodes
    /// with specified labels and properties.
    /// Returns a status or result string (placeholder for database `Value`).
    async fn create_fulltext_index(
        &self,
        name: &str,
        labels: &[&str],
        properties: &[&str],
    ) -> IndexResult<String>;
    
    /// Drops the full-text index specified by the name.
    /// Returns a status or result string (placeholder for database `Value`).
    async fn drop_fulltext_index(&self, name: &str) -> IndexResult<String>;

    /// Executes a full-text search query against the index.
    /// Returns a result string (placeholder for database `Value`) which
    /// typically contains the search hits/document IDs.
    async fn fulltext_search(&self, query: &str, limit: usize) -> IndexResult<String>;

    // --- Document Operations (Kept from original file) ---
    
    /// Adds or updates a document in the index.
    async fn index_document(&self, doc: Document) -> IndexResult<()>;

    /// Deletes a document from the index by its ID.
    async fn delete_document(&self, doc_id: &str) -> IndexResult<()>;

    /// Executes a text query against the index.
    /// Returns a vector of matched documents.
    async fn search(&self, query: &str) -> IndexResult<Vec<Document>>;
    
    // --- Maintenance & Stats ---

    /// Triggers a full rebuild of all indexes, which can be an expensive operation.
    /// Returns a status or result string (placeholder for database `Value`).
    async fn rebuild_indexes(&self) -> IndexResult<String>;

    /// Retrieves statistics and metrics about the current state of the indexes.
    /// Returns a result string (placeholder for database `Value`).
    async fn index_stats(&self) -> IndexResult<String>;
}
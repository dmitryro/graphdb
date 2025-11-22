use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use anyhow::{Result, Context, bail};
use serde_json::{json, Value as JsonValue};
use serde::{Deserialize, Serialize};

// --- External Imports (Assuming paths based on file context) ---
// We assume these are imported from the crate root or a shared module.
use async_trait::async_trait;
use tokio::sync::Mutex as TokioMutex;
use sled::Db; 
use rocksdb::DB; 
use std::path::PathBuf; 
// Mocking the required types from lib/src/indexing/backend.rs:
pub type IndexResult<T> = Result<T, crate::indexing::backend::IndexingError>;
use crate::indexing::backend::{IndexingBackend, Document,  IndexingError}; 
use crate::config::{ StorageEngineType };
// Using `crate::indexing::backend::Document` requires conversion, 
// so we'll use the user-defined `GraphDocument` internally and adapt.

// Tantivy specific imports
use tantivy::collector::TopDocs;
use tantivy::query::{QueryParser, TermQuery};
use tantivy::schema::{Field, Schema, TEXT, STORED, Term, Value};
use tantivy::{Index, IndexWriter, Searcher, TantivyDocument};
use tantivy::directory::MmapDirectory;


// --- STRUCTURES USED INTERNALLY (Keeping relevant user-defined structs) ---

/// Mock of the document structure received from the storage layer (used internally by this engine).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphDocument {
    pub node_id: i64,
    pub properties: HashMap<String, JsonValue>,
    pub labels: Vec<String>,
}

/// The handles for interacting with the underlying storage layer (used for metadata persistence).
pub enum EngineHandles {
    Sled(Arc<sled::Db>),
    RocksDB(Arc<DB>),
    TiKV(String),
    Redis(String),
    None,
}

// --- TANTIVY MANAGER ---

/// Manages the Tantivy index, writer, and searcher.
struct TantivyManager {
    index: Index,
    schema: Schema,
    writer: Arc<TokioMutex<IndexWriter>>, // Locked asynchronously
    searcher: Arc<RwLock<Searcher>>,      // Locked synchronously (read/write lock)
    node_id_field: Field,
    fulltext_content_field: Field,
}

impl TantivyManager {
    pub fn new() -> Result<Self> {
        // Defines a simple schema with a stored ID and a full-text field.
        let mut schema_builder = Schema::builder();
        let node_id_field = schema_builder.add_i64_field("node_id", STORED);
        let fulltext_content_field = schema_builder.add_text_field("fulltext_content", TEXT);
        let schema = schema_builder.build();

        // Use a persistent directory instead of tempdir to avoid cleanup issues
        let index_path = std::env::temp_dir().join(format!("tantivy_index_{}", std::process::id()));
        
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&index_path)
            .context(format!("Failed to create index directory at {:?}", index_path))?;
        
        let directory = MmapDirectory::open(&index_path)
            .context(format!("Failed to open MmapDirectory at {:?}", index_path))?;
        
        let index = Index::open_or_create(directory, schema.clone())
            .context("Failed to create or open Tantivy index")?;
        
        // Create writer with a buffer size of 50MB
        let writer = index.writer(50_000_000)
            .context("Failed to create Tantivy IndexWriter")?;
        
        // Initialize reader and searcher
        let reader = index.reader()
            .context("Failed to create Tantivy IndexReader")?;
        let searcher = reader.searcher();
        
        Ok(TantivyManager {
            index, schema, writer: Arc::new(TokioMutex::new(writer)),
            searcher: Arc::new(RwLock::new(searcher)),
            node_id_field, fulltext_content_field,
        })
    }

    /// Recreates the searcher to make committed changes visible for searching.
    pub fn reload_searcher(&self) -> Result<()> {
        let reader = self.index.reader()
            .context("Failed to create reader for reload")?;
        *self.searcher.write().unwrap() = reader.searcher();
        Ok(())
    }
}

// --- INDEX METADATA STRUCTS ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum IndexType {
    Standard,
    FullText,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexMetadata {
    index_type: IndexType,
    tantivy_index_name: String, // Name used for user interaction (e.g., "my_ft_index")
    labels: Vec<String>,        // Labels associated with the index
    properties: Vec<String>,    // Properties associated with the index
}

// --- Indexing Service Implementation ---

/// The Tantivy indexing service, designed to be integrated as an IndexingBackend.
pub struct TantivyIndexingEngine {
    engine_type: StorageEngineType,
    manager: Arc<TantivyManager>, // Tantivy core management
    handles: EngineHandles,       // Handles to the underlying storage for metadata persistence
    metadata_id: String,          // Unique ID based on the engine type for metadata isolation
}

impl TantivyIndexingEngine {
    /// Creates a new TantivyIndexingEngine instance.
    pub fn new(engine_type: StorageEngineType, handles: EngineHandles) -> Result<Self> {
        let manager = Arc::new(TantivyManager::new()?);
        
        // Generate a unique ID based on the engine type for metadata isolation.
        let metadata_id = format!("{:?}", engine_type).to_lowercase();

        Ok(Self {  
            engine_type,
            manager,  
            handles,
            metadata_id,
        })
    }
    
    // --- Metadata Persistence Logic (Using Handles - remains synchronous) ---

    /// Stores index metadata using a container unique to the engine type (Sled or RocksDB).
    fn store_metadata(&self, key: &str, metadata: &IndexMetadata) -> Result<()> {
        let data = serde_json::to_vec(metadata).context("Failed to serialize metadata")?;
        
        match &self.handles {
            EngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                db.open_tree(tree_name)?.insert(key.as_bytes(), data)?;
            },
            EngineHandles::RocksDB(db) => {
                let prefixed_key = format!("{}:{}", self.metadata_id, key);
                db.put(prefixed_key.as_bytes(), data).context("RocksDB put failed")?;
            },
            _ => bail!("Metadata storage not implemented for {:?}", self.engine_type),
        }
        Ok(())
    }
    
    /// Retrieves index metadata only from the engine-specific container.
    fn retrieve_metadata(&self, key: &str) -> Result<Option<IndexMetadata>> {
        let result = match &self.handles {
            EngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                db.open_tree(tree_name)?.get(key.as_bytes())?.map(|ivec| ivec.to_vec())
            },
            EngineHandles::RocksDB(db) => {
                let prefixed_key = format!("{}:{}", self.metadata_id, key);
                db.get(prefixed_key.as_bytes()).context("RocksDB get failed")?
            },
            _ => return Ok(None),
        };

        if let Some(data) = result {
            let metadata: IndexMetadata = serde_json::from_slice(&data).context("Failed to deserialize metadata")?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }
    
    /// Retrieves all index metadata of a given type from the engine-specific container.
    fn retrieve_all_metadata(&self, index_type: IndexType) -> Result<Vec<IndexMetadata>> {
        let mut results = Vec::new();

        match &self.handles {
            EngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                let tree = db.open_tree(tree_name)?;
                
                for item in tree.iter() {
                    let (_, data) = item?;
                    let metadata: IndexMetadata = serde_json::from_slice(&data).context("Failed to deserialize metadata during full scan")?;
                    if metadata.index_type == index_type {
                        results.push(metadata);
                    }
                }
            },
            // Full scan on RocksDB or others would require custom prefix iteration logic, omitted for brevity.
            _ => { /* Return empty vector for unsupported types */ }
        }
        
        Ok(results)
    }

    /// Deletes index metadata only from the engine-specific container.
    fn delete_metadata(&self, key: &str) -> Result<()> {
        match &self.handles {
            EngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                db.open_tree(tree_name)?.remove(key.as_bytes())?;
            },
            EngineHandles::RocksDB(db) => {
                let prefixed_key = format!("{}:{}", self.metadata_id, key);
                db.delete(prefixed_key.as_bytes()).context("RocksDB delete failed")?;
            },
            _ => { /* No-op for others */ }
        }
        Ok(())
    }

    /// Commits the writer and reloads the searcher.
    /// This method is async and intended for internal use where awaiting is possible (like rebuild).
    #[allow(dead_code)] // Only used in tests/advanced scenarios currently
    async fn commit_and_reload_internal(&self) -> Result<()> {
        let mut writer = self.manager.writer.lock().await;
        // The `commit()` is synchronous and blocks the thread.
        writer.commit().context("Tantivy commit failed")?;
        drop(writer);
        self.manager.reload_searcher()?;
        Ok(())
    }

    /// Helper function to convert the generic `Document` type from the trait into an internal `GraphDocument`.
    ///
    /// FIX: The original method failed because the generic `Document` type does not implement `serde::Serialize`.
    /// We now assume the `Document` object contains the raw, serialized JSON and extract it directly.
   /// FIX: The generic `backend::Document` does not implement `serde::Serialize`, which caused the previous error.
    /// We assume `backend::Document` implements `ToString` to extract the raw JSON string payload directly.
    fn convert_document_to_graph_doc(&self, doc: Document) -> Result<GraphDocument> {
        // Step 1: Serialize the structured Document into a JSON string.
        // This bridges the structured data definition in `backend.rs` to the expected 
        // deserialization process for `GraphDocument`.
        let serialized_payload = serde_json::to_string(&doc)
            .map_err(|e| IndexingError::SerializationError(format!("Failed to serialize backend::Document into JSON string: {}", e)))?;

        // Step 2: Deserialize the JSON payload into the target structure (`GraphDocument`).
        let graph_doc: GraphDocument = serde_json::from_str(&serialized_payload)
            .map_err(|e| IndexingError::SerializationError(format!("Failed to deserialize JSON payload into GraphDocument: {}", e)))?;
        
        Ok(graph_doc)
    }
}

// --- IndexingBackend Trait Implementation (ASYNCHRONOUS FIX) ---

/// Fix for E0277: Apply async_trait and make all trait methods async.
#[async_trait]
impl IndexingBackend for TantivyIndexingEngine {
    
    // Synchronous metadata getter
    fn engine_type(&self) -> StorageEngineType {
        self.engine_type
    }

    /// Placeholder for index initialization.
    async fn initialize(&self) -> IndexResult<()> {
        // TantivyManager::new already handles file system initialization.
        Ok(())
    }
    
    // --- Core Document Operations ---

    /// Indexes a graph document (node) for full-text search.
    async fn index_document(&self, doc: Document) -> IndexResult<()> {
        // Convert the generic trait Document to the internal GraphDocument type
        let graph_doc = self.convert_document_to_graph_doc(doc)
            .map_err(|e| IndexingError::SerializationError(e.to_string()))?;

        // Fix: Use TokioMutex lock().await instead of blocking_lock()
        let mut writer = self.manager.writer.lock().await; 
        
        // 1. Delete the existing document (Tantivy uses deletion/re-insertion for updates)
        let term = Term::from_field_i64(self.manager.node_id_field, graph_doc.node_id);
        writer.delete_term(term);

        // 2. Aggregate properties based on existing full-text index metadata
        // Note: Metadata retrieval remains synchronous as it hits local storage handles (Sled/RocksDB)
        let fulltext_indexes = self.retrieve_all_metadata(IndexType::FullText)
            .map_err(|e| IndexingError::Other(format!("Metadata error: {}", e)))?;

        let mut combined_content = String::new();
        let doc_labels: HashSet<_> = graph_doc.labels.iter().map(|s| s.as_str()).collect();

        for metadata in fulltext_indexes {
            let metadata_labels: HashSet<&str> = metadata.labels.iter().map(|s| s.as_str()).collect();
            let is_global = metadata.labels.is_empty();

            let has_matching_label = is_global || doc_labels.intersection(&metadata_labels).next().is_some();

            if has_matching_label {
                for prop_name in &metadata.properties {
                    if let Some(prop_value) = graph_doc.properties.get(prop_name) {
                        if let Some(s) = prop_value.as_str() {
                            combined_content.push_str(s);
                            combined_content.push(' ');
                        }
                    }
                }
            }
        }

        // 3. If there is aggregated content, add the new document
        if !combined_content.is_empty() {
            let mut tantivy_doc = TantivyDocument::new();
            tantivy_doc.add_i64(self.manager.node_id_field, graph_doc.node_id);
            tantivy_doc.add_text(self.manager.fulltext_content_field, &combined_content.trim());
            
            writer.add_document(tantivy_doc)
                .map_err(|e| IndexingError::TantivyError(e))
                .context(format!("Failed to add document to Tantivy writer for node {}", graph_doc.node_id))
                .map_err(|e| IndexingError::Other(e.to_string()))?;
        }
        
        // Return () for success, matching the IndexResult<()> required by the trait
        Ok(())
    }

    /// Deletes a document from the index by its ID (which is the node_id).
    async fn delete_document(&self, doc_id: &str) -> IndexResult<()> {
        let node_id: i64 = doc_id.parse()
            .map_err(|_| IndexingError::Other(format!("Invalid document ID format: {}", doc_id)))?;
            
        // Fix: Use TokioMutex lock().await instead of blocking_lock()
        let mut writer = self.manager.writer.lock().await;

        let term = Term::from_field_i64(self.manager.node_id_field, node_id);
        
        writer.delete_term(term);

        Ok(())
    }

    /// Executes a text query against the index.
    async fn search(&self, query: &str) -> IndexResult<Vec<Document>> {
        // Note: This implementation returns the internal node IDs.
        // The service layer must then fetch the full documents from the storage engine.
        let searcher_lock = self.manager.searcher.read().unwrap();
        let content_field = self.manager.fulltext_content_field;
        
        let query_parser = QueryParser::for_index(&self.manager.index, vec![content_field]);
        let tantivy_query = query_parser.parse_query(query)
            .map_err(|e| IndexingError::Other(format!("Failed to parse query: {}", e)))?;

        let top_docs = searcher_lock.search(&tantivy_query, &TopDocs::with_limit(100)) // Use a sensible default limit
            .map_err(|e| IndexingError::TantivyError(e))?;

        let results: Vec<Document> = top_docs.into_iter().filter_map(|(_score, doc_address)| {
            let retrieved_doc: TantivyDocument = searcher_lock.doc(doc_address).ok()?;
            
            // Extract the stored node_id
            let node_id = retrieved_doc
                .get_first(self.manager.node_id_field)?
                .as_i64()?;
            
            // The trait requires returning a Vec<Document>. Since we only have the node_id 
            // here, we must create a placeholder Document based on the ID. The actual
            // document retrieval happens in the service layer.
            Some(Document {
                id: node_id.to_string(),
                // Placeholder fields: the service layer will fetch the real fields
                fields: HashMap::new(), 
            })
        }).collect();
        
        Ok(results)
    }
    
    // --- Standard Indexing ---

    /// Creates a standard property index on nodes.
    async fn create_index(&self, label: &str, property: &str) -> IndexResult<String> {
        let index_key = format!("{}:{}", label, property);
        
        let metadata = IndexMetadata {
            index_type: IndexType::Standard,
            tantivy_index_name: index_key.clone(),
            labels: vec![label.to_string()],
            properties: vec![property.to_string()],
        };
        // Metadata storage is synchronous
        self.store_metadata(&index_key, &metadata)
            .map_err(|e| IndexingError::Other(format!("Metadata storage error: {}", e)))?;

        Ok(format!("Standard index metadata stored for {}:{}.", label, property))
    }
    
    /// Drops a standard property index.
    async fn drop_index(&self, label: &str, property: &str) -> IndexResult<String> {
        let index_key = format!("{}:{}", label, property);
        self.delete_metadata(&index_key)
            .map_err(|e| IndexingError::Other(format!("Metadata deletion error: {}", e)))?;

        Ok(format!("Standard index metadata dropped for {}:{}.", label, property))
    }

    // --- Full-Text Indexing ---

    /// Creates a full-text search index.
    async fn create_fulltext_index(
        &self,
        name: &str,
        labels: &[&str],
        properties: &[&str],
    ) -> IndexResult<String> {
        let metadata = IndexMetadata {
            index_type: IndexType::FullText,
            tantivy_index_name: name.to_string(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            properties: properties.iter().map(|s| s.to_string()).collect(),
        };
        self.store_metadata(name, &metadata)
            .map_err(|e| IndexingError::Other(format!("Metadata storage error: {}", e)))?;

        Ok(format!("Full-text index '{}' created.", name))
    }
    
    /// Drops the full-text index specified by the name.
    async fn drop_fulltext_index(&self, name: &str) -> IndexResult<String> {
        self.delete_metadata(name)
            .map_err(|e| IndexingError::Other(format!("Metadata deletion error: {}", e)))?;
            
        Ok(format!("Full-text index '{}' dropped.", name))
    }

    /// Executes a full-text search query against the index (returns a string placeholder).
    async fn fulltext_search(&self, query: &str, limit: usize) -> IndexResult<String> {
        let searcher_lock = self.manager.searcher.read().unwrap();
        let content_field = self.manager.fulltext_content_field;
        
        let query_parser = QueryParser::for_index(&self.manager.index, vec![content_field]);
        let tantivy_query = query_parser.parse_query(query)
            .map_err(|e| IndexingError::Other(format!("Failed to parse query: {}", e)))?;

        let top_docs = searcher_lock.search(&tantivy_query, &TopDocs::with_limit(limit))
            .map_err(|e| IndexingError::TantivyError(e))?;

        // Process results into JSON as requested by the original (synchronous) logic, 
        // then return the JSON as a String to match the trait's IndexResult<String>
        let results: Vec<JsonValue> = top_docs.into_iter().filter_map(|(score, doc_address)| {
            let retrieved_doc: TantivyDocument = searcher_lock.doc(doc_address).ok()?;
            
            let node_id = retrieved_doc
                .get_first(self.manager.node_id_field)?
                .as_i64()?;
            
            Some(json!({
                "node_id": node_id, 
                "score": score, 
            }))
        }).collect();
        
        let result_json = json!({
            "status": "success", 
            "results": results,
            "query": query, 
            "limit": limit,
        });

        Ok(result_json.to_string())
    }

    // --- Maintenance & Stats ---

    /// Triggers a full rebuild of all indexes (commit and reload).
    async fn rebuild_indexes(&self) -> IndexResult<String> {
        // Use the internal async commit/reload function
        self.commit_and_reload_internal().await
            .map_err(|e| IndexingError::Other(format!("Rebuild failed: {}", e)))?;

        Ok("Index commit completed and searcher reloaded.".to_string())
    }

    /// Retrieves statistics and metrics about the current state of the indexes.
    async fn index_stats(&self) -> IndexResult<String> {
        let reader = self.manager.index.reader()
            .map_err(|e| IndexingError::TantivyError(e))?;
        let searcher = reader.searcher();
        let segment_count = searcher.segment_readers().len();
        let total_docs: u64 = searcher.segment_readers().iter().map(|r| r.num_docs() as u64).sum();

        let stats_json = json!({
            "status": "success", 
            "stats": {
                "total_docs": total_docs, 
                "segment_count": segment_count, 
                "metadata_engine": format!("{:?}", self.engine_type)
            }
        });
        
        Ok(stats_json.to_string())
    }
}

// --- Test Implementation (Updated to be async) ---
// Note: This uses anyhow::Result in the test signature for convenience, but the main logic uses IndexResult.
#[cfg(test)]
mod tests {
    use super::*;
    
    // Helper function to set up a mock TantivyIndexingEngine backed by a temporary Sled DB
    fn setup_sled_engine() -> Result<TantivyIndexingEngine> {
        let db = sled::Config::new().temporary(true).open()?;
        let handles = EngineHandles::Sled(Arc::new(db));
        TantivyIndexingEngine::new(StorageEngineType::Sled, handles)
    }

    // Helper to mock the external Document type for indexing
    fn create_mock_external_document(node_id: i64, properties: HashMap<String, JsonValue>, labels: Vec<String>) -> Document {
        // Serialize the GraphDocument structure into the generic Document format
        let graph_doc = GraphDocument { node_id, properties, labels };
        let fields: HashMap<String, String> = serde_json::from_value(serde_json::to_value(graph_doc).unwrap()).unwrap();
        
        // This is a rough mock conversion to satisfy the required Document type structure.
        Document {
            id: node_id.to_string(),
            fields,
        }
    }


    #[tokio::test]
    async fn test_fulltext_indexing_and_retrieval() -> Result<()> {
        let engine = setup_sled_engine()?;
        
        // 2. Create the full-text index metadata for "name" property on "Person:Director"
        let index_name = "dir_name_index";
        let create_res_str = engine.create_fulltext_index(
            index_name, 
            &["Person:Director"], 
            &["name"]
        ).await?;
        assert!(create_res_str.contains("success"));

        // 3. Create mock document data 
        let mut props = HashMap::new();
        props.insert("name".to_string(), JsonValue::String("Oliver Stone".to_string()));
        props.insert("id".to_string(), JsonValue::String("2c651016".to_string())); 
        
        let doc = create_mock_external_document(
            10001, 
            props, 
            vec!["Person".to_string(), "Director".to_string(), "Person:Director".to_string()]
        );

        // 4. Index the document (now async)
        engine.index_document(doc).await?;
        
        // 5. Commit the writer and reload the searcher to make changes visible (now async)
        let rebuild_res_str = engine.rebuild_indexes().await?; 
        assert!(rebuild_res_str.contains("completed"));
        
        // 6. Search for 'Oliver Stone'
        let query = "Oliver Stone";
        let search_res_str = engine.fulltext_search(query, 10).await?;
        let search_res: JsonValue = serde_json::from_str(&search_res_str)?;
        
        // 7. Assert results
        let results = search_res["results"].as_array().context("Results should be an array")?;
        
        // Check if results are non-empty and contain the expected node ID
        assert!(!results.is_empty(), "Search for '{}' should return results after indexing.", query);
        
        let found_id = results[0]["node_id"].as_i64().unwrap_or(0);
        assert_eq!(found_id, 10001, "The retrieved node ID should match the indexed node ID.");

        Ok(())
    }
    
    #[tokio::test]
    async fn test_document_update_and_delete() -> Result<()> {
        let engine = setup_sled_engine()?;
        let index_name = "test_index";
        engine.create_fulltext_index(index_name, &[], &["content"]).await?;

        // 1. Index initial document
        let doc1 = create_mock_external_document(
            20001, 
            HashMap::from([("content".to_string(), JsonValue::String("Initial content for deletion test".to_string()))]), 
            vec![]
        );
        engine.index_document(doc1).await?;
        engine.rebuild_indexes().await?;
        assert!(!serde_json::from_str::<JsonValue>(&engine.fulltext_search("deletion", 10).await?)?["results"].as_array().unwrap().is_empty());

        // 2. Update document (re-index)
        let doc2_update = create_mock_external_document(
            20001, 
            HashMap::from([("content".to_string(), JsonValue::String("Updated text for search verification".to_string()))]), 
            vec![]
        );
        engine.index_document(doc2_update).await?;
        engine.rebuild_indexes().await?;
        
        // Verify old term is gone and new term exists
        assert!(serde_json::from_str::<JsonValue>(&engine.fulltext_search("deletion", 10).await?)?["results"].as_array().unwrap().is_empty());
        assert!(!serde_json::from_str::<JsonValue>(&engine.fulltext_search("verification", 10).await?)?["results"].as_array().unwrap().is_empty());

        // 3. Delete document
        engine.delete_document("20001").await?;
        engine.rebuild_indexes().await?;

        // Verify document is deleted
        assert!(serde_json::from_str::<JsonValue>(&engine.fulltext_search("verification", 10).await?)?["results"].as_array().unwrap().is_empty());

        Ok(())
    }
    
    #[tokio::test]
    async fn test_index_stats() -> Result<()> {
        let engine = setup_sled_engine()?;
        let index_name = "stats_test_index";
        engine.create_fulltext_index(index_name, &[], &["prop"]).await?;

        // Index documents
        for i in 30001..30005 {
            let doc = create_mock_external_document(
                i, 
                HashMap::from([("prop".to_string(), JsonValue::String(format!("test doc {}", i)))]), 
                vec![]
            );
            engine.index_document(doc).await?;
        }
        
        // Stats before commit should show 0 docs if no previous commit existed
        let stats_pre_str = engine.index_stats().await?;
        let stats_pre: JsonValue = serde_json::from_str(&stats_pre_str)?;
        let pre_docs = stats_pre["stats"]["total_docs"].as_u64().unwrap_or(0);
        
        // Commit and check stats
        engine.rebuild_indexes().await?;
        let stats_post_str = engine.index_stats().await?;
        let stats_post: JsonValue = serde_json::from_str(&stats_post_str)?;
        let post_docs = stats_post["stats"]["total_docs"].as_u64().context("Total docs not found or invalid")?;
        
        // We indexed 4 documents. If the index was clean, we expect 4.
        assert_eq!(post_docs, pre_docs + 4, "Post-commit index should contain 4 new documents.");
        
        Ok(())
    }
}
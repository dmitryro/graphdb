// indexing_service/src/fulltext.rs
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::*;
//use tantivy::schema::Value as TantivyValue; // alias to avoid confusion
use tantivy::{Index, IndexWriter, ReloadPolicy}; 
use tantivy::doc;
use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

/// Full-text index wrapper using Tantivy
pub struct FullTextIndex {
    index: Index,
    writer: Arc<TokioMutex<IndexWriter>>,
    schema: Schema,
    text_field: Field,
    id_field: Field,
}

impl FullTextIndex {
    /// Create or open a Tantivy index at `path`
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Define schema
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("content", TEXT | STORED);
        let id_field = schema_builder.add_text_field("id", STRING | STORED);
        let schema = schema_builder.build();

        // Open or create index
        let index = if path.as_ref().exists() {
            Index::open_in_dir(&path)?
        } else {
            Index::create_in_dir(&path, schema.clone())?
        };

        let writer = index.writer(50_000_000)?; // 50MB buffer

        Ok(Self {
            index,
            writer: Arc::new(TokioMutex::new(writer)),
            schema,
            text_field,
            id_field,
        })
    }

    /// Index a document by id and text content
    pub async fn index_document(&self, doc_id: &str, content: &str) -> Result<()> {
        let mut writer = self.writer.lock().await;
        
        writer.add_document(doc!(
            self.text_field => content,
            self.id_field => doc_id
        ))?;
        writer.commit()?;
        
        Ok(())
    }

    /// Search indexed documents
    pub fn search(&self, query_str: &str, top: usize) -> Result<Vec<(String, String)>> {
        let reader = self.index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        
        let searcher = reader.searcher();
        let query_parser = QueryParser::for_index(&self.index, vec![self.text_field]);
        let query = query_parser.parse_query(query_str)?;
        
        let top_docs = searcher.search(&query, &TopDocs::with_limit(top))?;
        
        let mut results = Vec::new();
        for (_score, doc_address) in top_docs {
            // FIX: Use tantivy::Document instead of just Document
            let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            let content = retrieved_doc
                .get_first(self.text_field)
                .and_then(|v| v.as_str())  // <-- now compiles
                .unwrap_or_default()
                .to_string();

            let id = retrieved_doc
                .get_first(self.id_field)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            results.push((id, content));
        }
        
        Ok(results)
    }

    /// Commit pending documents (optional if using auto-commit)
    pub async fn commit(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        writer.commit()?;
        Ok(())
    }

    /// Delete documents by ID
    pub async fn delete_document(&self, doc_id: &str) -> Result<()> {
        let mut writer = self.writer.lock().await;
        let term = Term::from_field_text(self.id_field, doc_id);
        writer.delete_term(term);
        writer.commit()?;
        Ok(())
    }

    /// Update a document (delete old and add new)
    pub async fn update_document(&self, doc_id: &str, content: &str) -> Result<()> {
        self.delete_document(doc_id).await?;
        self.index_document(doc_id, content).await?;
        Ok(())
    }
}

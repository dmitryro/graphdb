use crate::storage_engine::{GraphStorageEngine};
use models::errors::GraphError;
use caching::Cache;
use models::Vertex;
use anyhow::Result;
use dashmap::DashMap;
use async_trait::async_trait;
use std::sync::Arc;

use crate::fulltext::FullTextIndex;

#[async_trait]
pub trait IndexingServiceTrait: Send + Sync {
    async fn index_node(&self, vertex: &Vertex) -> Result<(), GraphError>;
    async fn search_text(&self, query: &str, top: usize) -> Result<Vec<String>, GraphError>;
}

pub struct IndexingService {
    kv_index: DashMap<String, String>,
    storage: Arc<dyn GraphStorageEngine>,
    cache: Cache,
    ft_index: Arc<FullTextIndex>,
}

#[async_trait]
impl IndexingServiceTrait for IndexingService {
    async fn index_node(&self, vertex: &Vertex) -> Result<(), GraphError> {
        if let Some(patient_id) = vertex.properties.get("patient_id").and_then(|pv| pv.as_str()) {
            self.kv_index.insert(patient_id.to_string(), vertex.id.to_string());
            self.cache
                .insert(vertex.id.to_string(), serde_json::to_value(vertex)?)
                .await?;

            let doc_id   = vertex.id.to_string();
            let text_content = vertex.properties
                                      .get("notes")
                                      .and_then(|pv| pv.as_str())
                                      .unwrap_or_default();

            self.ft_index.index_document(&doc_id, text_content).await?;
        }
        Ok(())
    }

    async fn search_text(&self, query: &str, top: usize) -> Result<Vec<String>, GraphError> {
        Ok(self.ft_index
               .search(query, top)?
               .into_iter()
               .map(|(id, _txt)| id)
               .collect())
    }
}

impl IndexingService {
    pub fn new(
        storage: Arc<dyn GraphStorageEngine>,
        cache: Cache,
        ft_index: Arc<FullTextIndex>,
    ) -> Self {
        IndexingService {
            kv_index: DashMap::new(),
            storage,
            cache,
            ft_index,
        }
    }
}

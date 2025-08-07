use lib::storage_engine::GraphStorageEngine;
use lib::errors::GraphError;
use caching::Cache;
use models::Vertex;
use anyhow::Result;
use dashmap::DashMap;
use async_trait::async_trait;
use std::sync::Arc;

#[async_trait]
pub trait IndexingServiceTrait: Send + Sync {
    async fn index_node(&self, vertex: &Vertex) -> Result<(), GraphError>;
}

pub struct IndexingService {
    kv_index: DashMap<String, String>,
    storage: Arc<dyn GraphStorageEngine>,
    cache: Cache,
}

#[async_trait]
impl IndexingServiceTrait for IndexingService {
    async fn index_node(&self, vertex: &Vertex) -> Result<(), GraphError> {
        if let Some(patient_id) = vertex.properties.get("patient_id").and_then(|v| v.as_str()) {
            self.kv_index.insert(patient_id.to_string(), vertex.id.to_string());
            self.cache.insert(vertex.id.to_string(), serde_json::to_value(vertex)?).await?;
        }
        Ok(())
    }
}

impl IndexingService {
    pub fn new(storage: Arc<dyn GraphStorageEngine>, cache: Cache) -> Self {
        IndexingService {
            kv_index: DashMap::new(),
            storage,
            cache,
        }
    }
}

use anyhow::Result;
use lib::storage::{GraphStorage, SledStorage};
use lib::types::Node;
use caching::Cache;
use caching_service::CachingService;
use logging_service::LogService;
use indexing_service::IndexingService;
use async_trait::async_trait;

#[async_trait]
pub trait OrchestratorTrait: Send + Sync {
    async fn store_node(&self, node: &Node) -> Result<()>;
    async fn get_node(&self, id: &str) -> Result<Option<Node>>;
}

pub struct Orchestrator {
    storage: Box<dyn GraphStorage>,
    cache: Cache,
    caching_service: CachingService,
    logging_service: LogService,
    indexing_service: IndexingService,
}

impl Orchestrator {
    pub async fn new(
        node_id: u64,
        peers: Vec<u64>,
        storage: Box<dyn GraphStorage>,
        cache: Cache,
        brokers: &str,
        group_id: &str,
        topic: &str,
    ) -> Result<Self> {
        let storage = Box::new(lib::storage_engine::DistributedStorage::new(node_id, peers, storage, cache.clone()).await?);
        let caching_service = CachingService::new(brokers, group_id, topic, storage.clone()).await?;
        let logging_service = LogService::new(storage.clone(), cache.clone())?;
        let indexing_service = IndexingService::new(storage.clone(), cache.clone())?;
        Ok(Orchestrator {
            storage,
            cache,
            caching_service,
            logging_service,
            indexing_service,
        })
    }
}

#[async_trait]
impl OrchestratorTrait for Orchestrator {
    async fn store_node(&self, node: &Node) -> Result<()> {
        self.storage.store_node(node).await?;
        self.logging_service.log_query(&format!("store node {}", node.id), "system", "storage").await?;
        self.indexing_service.index_node(node).await?;
        self.caching_service.get_node(&node.id).await?; // Ensure cache sync
        Ok(())
    }

    async fn get_node(&self, id: &str) -> Result<Option<Node>> {
        let node = self.caching_service.get_node(id).await?;
        if node.is_some() {
            self.logging_service.log_query(&format!("get node {}", id), "system", "storage").await?;
        }
        Ok(node)
    }
}

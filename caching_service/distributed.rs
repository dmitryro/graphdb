use lib::storage_engine::GraphStorageEngine;
use lib::notifications::Notification;
use lib::errors::GraphError;
use caching::Cache;
use models::Vertex;
use anyhow::Result;
use rdkafka::consumer::{Consumer, StreamConsumer};
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub trait CachingServiceTrait: Send + Sync {
    async fn run(&self) -> Result<(), GraphError>;
    async fn get_node(&self, id: &str) -> Result<Option<Vertex>, GraphError>;
}

pub struct CachingService {
    cache: Cache,
    storage: Arc<dyn GraphStorageEngine>,
    consumer: StreamConsumer,
    #[cfg(feature = "enterprise")]
    hit_counter: prometheus::Counter,
}

#[async_trait]
impl CachingServiceTrait for CachingService {
    async fn run(&self) -> Result<(), GraphError> {
        loop {
            let message = self.consumer.recv().await?;
            if let Some(payload) = message.payload() {
                let notification: Notification = serde_json::from_slice(payload)?;
                if notification.event_type == "node_updated" {
                    let node_id = notification.payload["id"].as_str().unwrap_or("");
                    self.cache.invalidate(node_id).await;
                }
            }
        }
    }

    async fn get_node(&self, id: &str) -> Result<Option<Vertex>, GraphError> {
        if let Some(value) = self.cache.get(id).await {
            #[cfg(feature = "enterprise")]
            self.hit_counter.inc();
            return Ok(serde_json::from_value(value)?);
        }
        let uuid = Uuid::parse_str(id).map_err(|e| GraphError::InvalidData(e.to_string()))?;
        if let Some(vertex) = self.storage.get_vertex(&uuid).await? {
            self.cache.insert(id.to_string(), serde_json::to_value(&vertex)?).await?;
            Ok(Some(vertex))
        } else {
            Ok(None)
        }
    }
}

impl CachingService {
    pub async fn new(brokers: &str, group_id: &str, topic: &str, storage: Arc<dyn GraphStorageEngine>) -> Result<Self, GraphError> {
        let cache = Cache::new(10_000);
        let consumer = rdkafka::config::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .create()?;
        consumer.subscribe(&[topic])?;
        #[cfg(feature = "enterprise")]
        let hit_counter = prometheus::Counter::new("cache_hits", "Number of cache hits")?;
        Ok(CachingService {
            cache,
            storage,
            consumer,
            #[cfg(feature = "enterprise")]
            hit_counter,
        })
    }
}

use serde::{Deserialize, Serialize};
use anyhow::Result;
use moka::future::Cache as MokaCache;

#[derive(Clone)]
pub struct Cache {
    inner: MokaCache<String, serde_json::Value>,
}

impl Cache {
    pub fn new(capacity: u64) -> Self {
        Cache {
            inner: MokaCache::new(capacity),
        }
    }

    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.inner.get(key).await
    }

    pub async fn insert(&self, key: String, value: serde_json::Value) -> Result<()> {
        self.inner.insert(key, value).await;
        Ok(())
    }

    pub async fn invalidate(&self, key: &str) {
        self.inner.invalidate(key).await;
    }
}

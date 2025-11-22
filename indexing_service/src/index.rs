// indexing_service/src/index.rs
use std::sync::Arc;
use anyhow::Result;
use serde_json::{json, Value};
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use crate::adapters::send_zmq_command;

type Service = Arc<TokioMutex<IndexingService>>;

static SINGLETON: OnceCell<Service> = OnceCell::const_new();

/* ---------- public helpers ---------- */

/// Called **once** when the storage engine starts.
/// `port` is the daemon port that the ZMQ adapter will talk to.
pub async fn init_indexing_service(port: u16) -> Result<()> {
    SINGLETON
        .get_or_init(|| async { Arc::new(TokioMutex::new(IndexingService::new(port))) })
        .await;
    Ok(())
}

/// Cheap clone of the already-built service.
/// Panics if called before `init_indexing_service`.
pub fn indexing_service() -> Service {
    SINGLETON
        .get()
        .expect("IndexingService not initialised – start storage first")
        .clone()
}

/* ---------- the service itself ---------- */

#[derive(Clone)]
pub struct IndexingService {
    daemon_port: u16,
}

impl IndexingService {
    pub fn new(daemon_port: u16) -> Self {
        Self { daemon_port }
    }

    /* all business methods stay identical */

    pub async fn create_index(&self, label: &str, property: &str) -> Result<Value> {
        let payload = json!({
            "command": "index_create",
            "label": label,
            "property": property
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    pub async fn drop_index(&self, label: &str, property: &str) -> Result<Value> {
        let payload = json!({
            "command": "index_drop",
            "label": label,
            "property": property
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    pub async fn list_indexes(&self) -> Result<Value> {
        let payload = json!({"command": "index_list"});
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    pub async fn create_fulltext_index(
        &self,
        name: &str,
        labels: &[&str],
        properties: &[&str],
    ) -> Result<Value> {
        let payload = json!({
            "command": "fulltext_create",
            "name": name,
            "labels": labels,
            "properties": properties
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    pub async fn drop_fulltext_index(&self, name: &str) -> Result<Value> {
        let payload = json!({
            "command": "fulltext_drop",
            "name": name
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    pub async fn fulltext_search(&self, query: &str, limit: usize) -> Result<Value> {
        let payload = json!({
            "command": "fulltext_search",
            "query": query,
            "limit": limit
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    pub async fn rebuild_indexes(&self) -> Result<Value> {
        let payload = json!({"command": "fulltext_rebuild"});
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    pub async fn index_stats(&self) -> Result<Value> {
        let payload = json!({"command": "index_stats"});
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }
}
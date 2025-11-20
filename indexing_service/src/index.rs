// indexing_service/src/index.rs
use anyhow::Result;
use serde_json::{json, Value};
use crate::adapters::send_zmq_command;

#[derive(Clone)]
pub struct IndexingService {
    daemon_port: u16,
}

impl IndexingService {
    pub fn new(daemon_port: u16) -> Self {
        Self { daemon_port }
    }

    /// index create Person name
    pub async fn create_index(&self, label: &str, property: &str) -> Result<Value> {
        let payload = json!({
            "command": "index_create",
            "label": label,
            "property": property
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    /// index drop Person name
    pub async fn drop_index(&self, label: &str, property: &str) -> Result<Value> {
        let payload = json!({
            "command": "index_drop",
            "label": label,
            "property": property
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    /// index list
    pub async fn list_indexes(&self) -> Result<Value> {
        let payload = json!({
            "command": "index_list"
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    /// index create-fulltext notesIndex Note content Document text
    pub async fn create_fulltext_index(&self, name: &str, labels: &[&str], properties: &[&str]) -> Result<Value> {
        let payload = json!({
            "command": "fulltext_create",
            "name": name,
            "labels": labels,
            "properties": properties
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    /// index drop-fulltext notesIndex
    pub async fn drop_fulltext_index(&self, name: &str) -> Result<Value> {
        let payload = json!({
            "command": "fulltext_drop",
            "name": name
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    /// index search "Oliver Stone" [top N]
    pub async fn fulltext_search(&self, query: &str, limit: usize) -> Result<Value> {
        let payload = json!({
            "command": "fulltext_search",
            "query": query,
            "limit": limit
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    /// index rebuild
    pub async fn rebuild_indexes(&self) -> Result<Value> {
        let payload = json!({
            "command": "fulltext_rebuild"
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }

    /// index stats (optional)
    pub async fn index_stats(&self) -> Result<Value> {
        let payload = json!({
            "command": "index_stats"
        });
        send_zmq_command(self.daemon_port, &payload.to_string()).await
    }
}
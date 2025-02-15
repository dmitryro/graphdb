use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

/// Represents a node in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub id: String,
    pub label: String,
    pub properties: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// Represents an edge in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    pub id: String,
    pub source: String,
    pub target: String,
    pub label: String,
    pub properties: serde_json::Value,
    pub created_at: DateTime<Utc>,
}


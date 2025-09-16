use std::collections::HashMap;
use serde::{Deserialize, Serialize};
pub use crate::config::{ StorageEngineType };
/// Type alias for a node's unique identifier.
pub type NodeId = String;
/// Type alias for an edge's unique identifier.
pub type EdgeId = String;
/// Type alias for a collection of key-value string properties.
pub type Properties = HashMap<String, String>;
/// Type alias for a collection of key-value string metadata.
pub type Metadata = HashMap<String, String>;

/// Enum representing the different types of storage engines.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EngineType {
    Sled,
    ZeroMq,
    InMemory,
}

/// A struct that describes the capabilities of the storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCapabilities {
    /// If the engine supports transactions.
    pub transactions: bool,
    /// If the engine supports concurrent access from multiple clients.
    pub concurrency: bool,
    /// If the engine supports full-text search on node/edge properties.
    pub full_text_search: bool,
    /// The max number of nodes the engine can hold.
    pub max_nodes: Option<u64>,
    /// The max number of edges the engine can hold.
    pub max_edges: Option<u64>,
}

/// A struct that provides runtime information about the storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineInfo {
    /// The type of the running engine.
    pub engine_type: EngineType,
    /// The version of the engine.
    pub version: String,
    /// The current number of nodes in the graph.
    pub node_count: u64,
    /// The current number of edges in the graph.
    pub edge_count: u64,
}


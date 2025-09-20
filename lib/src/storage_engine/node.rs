use serde::{Deserialize, Serialize};

pub use super::types::{NodeId, Properties, Metadata};

/// Represents a node in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// The unique identifier for the node.
    pub id: NodeId,
    /// A map of key-value string pairs for node properties.
    pub properties: Properties,
    /// A map of key-value string pairs for metadata (e.g., timestamps).
    pub metadata: Metadata,
}


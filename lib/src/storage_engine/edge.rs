use serde::{Deserialize, Serialize};

pub use super::{node::NodeId, types::{EdgeId, Properties, Metadata}};

/// Represents a directed edge in the graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    /// The unique identifier for the edge.
    pub id: EdgeId,
    /// The ID of the source node.
    pub source: NodeId,
    /// The ID of the target node.
    pub target: NodeId,
    /// A map of key-value string pairs for edge properties.
    pub properties: Properties,
    /// A map of key-value string pairs for metadata (e.g., timestamps).
    pub metadata: Metadata,
}


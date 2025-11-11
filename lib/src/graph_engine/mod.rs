// lib/src/graph_engine/mod.rs
pub mod graph;
pub mod traversal;
pub mod medical;
pub mod pattern_match;
pub mod adapters;   // new

// Re-export the *exact* types that the rest of the engine uses
pub use models::properties;
pub use graph::Graph;
pub use models::vertices::Vertex;
pub use models::edges::Edge;
pub use models::properties::PropertyValue;
pub use models::identifiers::Identifier;
pub use models::identifiers::SerializableUuid;

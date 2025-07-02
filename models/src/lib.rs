// models/src/lib.rs

// Declare all top-level modules within the 'models' crate
pub mod bulk_insert;
pub mod edges;
pub mod identifiers;
pub mod json;
pub mod properties;
pub mod queries;
pub mod to_vertex; // This was added/confirmed in the last step
pub mod vertices;
pub mod errors;
pub mod util;

// Declare the 'medical' sub-module
pub mod medical;

// Re-export common core types for convenience when other crates use 'models::*'
// Ensure these types are actually `pub` in their respective modules.

// --- IMPORTANT CORRECTION HERE ---
// Based on the error "no `Node` in `vertices`", we must remove `Node` from this re-export.
// If `Node` is intended to exist, you need to define it in `models/src/vertices.rs`
// and make it public. For now, to fix the compilation, it's removed.
pub use vertices::Vertex; // Changed from `pub use vertices::{Node, Vertex};`
// ---------------------------------

pub use edges::Edge;
pub use properties::{PropertyValue, PropertyMap};
pub use identifiers::Identifier;
pub use json::Json;
pub use to_vertex::ToVertex; // Trait re-export - confirmed this is correct now

// Re-export User and Login from medical models if

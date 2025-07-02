use crate::{edges::Edge, identifiers::Identifier, json::Json, vertices::Vertex};
use uuid::Uuid; // Already covered in previous fix.

/// An item to insert, as part of a bulk insert request.
#[derive(Clone, Debug, PartialEq)]
pub enum BulkInsertItem {
    /// A vertex to insert.
    Vertex(Vertex),
    /// An edge to insert.
    Edge(Edge),
    /// A vertex property to insert.
    VertexProperty(Uuid, Identifier, Json),
    /// An edge property to insert.
    EdgeProperty(Edge, Identifier, Json),
}

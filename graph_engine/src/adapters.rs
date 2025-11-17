// lib/src/graph_engine/adapters.rs
use models::edges::Edge as ModelEdge;
use models::vertices::Vertex as ModelVertex;
use models::properties::PropertyValue;
use models::identifiers::SerializableUuid;
use uuid::Uuid;
use std::collections::{BTreeMap, HashMap};

/// Convert a **model** vertex (from `models`) into the **engine’s in-memory** representation.
///
/// The engine uses `HashMap` for vertex properties.
pub fn vertex_to_engine(v: &ModelVertex) -> ModelVertex {
    ModelVertex {
        id: v.id, // already SerializableUuid
        label: v.label.clone(),
        properties: v.properties.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>(), // ✅ Use HashMap
    }
}

/// Convert engine vertex back to model (for WAL/snapshot).
///
/// Converts:
/// - `HashMap → HashMap` (same type, no conversion needed)
pub fn vertex_from_engine(v: &ModelVertex) -> ModelVertex {
    ModelVertex {
        id: v.id, // already SerializableUuid
        label: v.label.clone(),
        properties: v.properties.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<HashMap<_, _>>(), // ✅ Use HashMap
    }
}

/// Convert a **model** edge into the **engine’s in-memory** representation.
///
/// Engine uses `BTreeMap` for edge properties.
pub fn edge_to_engine(e: &ModelEdge) -> ModelEdge {
    ModelEdge {
        id: e.id,
        outbound_id: e.outbound_id,
        edge_type: e.edge_type.clone(),
        inbound_id: e.inbound_id,
        label: e.label.clone(),
        properties: e.properties.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>(), // ✅ Use BTreeMap
    }
}

/// Convert engine edge back to model (for WAL/snapshot).
pub fn edge_from_engine(e: &ModelEdge) -> ModelEdge {
    ModelEdge {
        id: e.id,
        outbound_id: e.outbound_id,
        edge_type: e.edge_type.clone(),
        inbound_id: e.inbound_id,
        label: e.label.clone(),
        properties: e.properties.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<BTreeMap<_, _>>(), // ✅ Use BTreeMap
    }
}

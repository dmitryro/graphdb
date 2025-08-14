// lib/src/storage_engine/storage_utils.rs
// Created: 2025-07-04 - Added serialization utilities for Sled storage
// Fixed: 2025-07-04 - Corrected create_edge_key to use util::build for Identifier
// Updated: 2025-08-13 - Changed create_edge_key to accept SerializableUuid and return GraphResult<Vec<u8>>
// Fixed: 2025-08-14 - Corrected unresolved import of `Component` from `util`.
// Fixed: 2025-08-14 - Corrected the return value of `create_edge_key` to not use `.map_err` as `util_build_key` is infallible.

use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use models::identifiers::SerializableUuid;
use models::util::{build as util_build_key, Component as UtilComponent};
use bincode::{encode_to_vec, decode_from_slice, config};
use uuid::Uuid;

/// Helper to serialize a Vertex to bytes using bincode.
pub fn serialize_vertex(vertex: &Vertex) -> GraphResult<Vec<u8>> {
    encode_to_vec(vertex, config::standard())
        .map_err(|e| GraphError::SerializationError(e.to_string()))
}

/// Helper to deserialize bytes to a Vertex using bincode.
pub fn deserialize_vertex(bytes: &[u8]) -> GraphResult<Vertex> {
    decode_from_slice(bytes, config::standard())
        .map(|(val, _)| val)
        .map_err(|e| GraphError::DeserializationError(e.to_string()))
}

/// Helper to serialize an Edge to bytes using bincode.
pub fn serialize_edge(edge: &Edge) -> GraphResult<Vec<u8>> {
    encode_to_vec(edge, config::standard())
        .map_err(|e| GraphError::SerializationError(e.to_string()))
}

/// Helper to deserialize bytes to an Edge using bincode.
pub fn deserialize_edge(bytes: &[u8]) -> GraphResult<Edge> {
    decode_from_slice(bytes, config::standard())
        .map(|(val, _)| val)
        .map_err(|e| GraphError::DeserializationError(e.to_string()))
}

/// Helper to create a consistent byte key for an edge from its components.
/// This key is used for storage in key-value stores.
/// It uses the `util::build` function to ensure correct Identifier serialization.
pub fn create_edge_key(outbound_id: &SerializableUuid, edge_type: &Identifier, inbound_id: &SerializableUuid) -> Result<Vec<u8>, GraphError> {
    Ok(util_build_key(&[
        UtilComponent::Uuid(outbound_id.0),
        UtilComponent::Identifier(edge_type.clone()),
        UtilComponent::Uuid(inbound_id.0),
    ]))
}
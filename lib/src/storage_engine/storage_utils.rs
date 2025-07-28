// lib/src/storage_engine/storage_utils.rs
// Fixed: 2025-07-04 - Corrected create_edge_key to use util::build for Identifier.

use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use uuid::Uuid;
// Corrected bincode imports for version 2.x API
use bincode::{encode_to_vec, decode_from_slice, config};

// Import util for key building
use crate::util::{build as util_build_key, Component as UtilComponent};


/// Helper to serialize a Vertex to bytes using bincode.
pub fn serialize_vertex(vertex: &Vertex) -> GraphResult<Vec<u8>> {
    encode_to_vec(vertex, config::standard())
        .map_err(|e| GraphError::SerializationError(e.to_string()))
}

/// Helper to deserialize bytes to a Vertex using bincode.
pub fn deserialize_vertex(bytes: &[u8]) -> GraphResult<Vertex> {
    decode_from_slice(bytes, config::standard())
        .map(|(val, _)| val) // decode_from_slice returns (value, bytes_read)
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
        .map(|(val, _)| val) // decode_from_slice returns (value, bytes_read)
        .map_err(|e| GraphError::DeserializationError(e.to_string()))
}

/// Helper to create a consistent byte key for an edge from its components.
/// This key is used for storage in key-value stores.
/// It uses the `util::build` function to ensure correct Identifier serialization.
pub fn create_edge_key(outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Vec<u8> {
    util_build_key(&[
        UtilComponent::Uuid(*outbound_id),
        UtilComponent::Identifier(edge_type.clone()), // Identifier needs to be cloned for Component enum
        UtilComponent::Uuid(*inbound_id),
    ])
}


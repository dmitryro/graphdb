// lib/src/storage_engine/storage_utils.rs
// Fixed: 2025-07-02 - Corrected bincode serialization/deserialization function calls.

use models::{Edge, Identifier, Vertex};
use models::errors::{GraphError, GraphResult};
use uuid::Uuid;
// Corrected bincode imports for version 2.x API
use bincode::{encode_to_vec, decode_from_slice, config};


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
pub fn create_edge_key(outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Vec<u8> {
    // A simple concatenation for the key.
    // In a real system, you might want a more robust keying scheme that
    // ensures lexicographical ordering for range scans and avoids collisions.
    // For example, using a fixed-size prefix for type, then UUIDs.
    let mut key = Vec::new();
    key.extend_from_slice(outbound_id.as_bytes());
    key.extend_from_slice(edge_type.as_bytes()); // Identifier's as_bytes() might need careful handling if it's not fixed length or null-terminated
    key.extend_from_slice(inbound_id.as_bytes());
    key
}


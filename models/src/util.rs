//! Utility functions. These are public because they may be useful for crates
//! that implement Datastore.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read, Result as IoResult, Write};
use std::str;

use crate::errors::{GraphResult, ValidationError, ValidationResult};
use crate::{
    edges::Edge,
    identifiers::{Identifier, SerializableInternString, SerializableUuid},
    json::Json,
    properties::{EdgeProperties, VertexProperties},
    queries::QueryOutputValue,
    vertices::Vertex,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use once_cell::sync::Lazy;
use uuid::{Context, Timestamp, Uuid};

const NODE_ID: [u8; 6] = [0, 0, 0, 0, 0, 0];

static CONTEXT: Lazy<Context> = Lazy::new(|| Context::new(0));

/// A byte-serializable value, frequently employed in the keys of key/value store.
pub enum Component<'a> {
    /// A UUID.
    Uuid(Uuid),
    /// A fixed length string.
    FixedLengthString(&'a str),
    /// An identifier.
    Identifier(Identifier),
    /// A JSON value.
    Json(&'a Json),
}

impl Component<'_> {
    /// Gets the length of the component. This isn’t called `len` to avoid a clippy warning.
    pub fn byte_len(&self) -> usize {
        match *self {
            Component::Uuid(_) => 16,
            Component::FixedLengthString(s) => s.len(),
            Component::Identifier(ref t) => t.0.0.len() + 1,
            Component::Json(_) => 8,
        }
    }

    /// Writes a component into a cursor of bytes.
    pub fn write(&self, cursor: &mut Cursor<Vec<u8>>) -> IoResult<()> {
        match *self {
            Component::Uuid(uuid) => cursor.write_all(uuid.as_bytes()),
            Component::FixedLengthString(s) => cursor.write_all(s.as_bytes()),
            Component::Identifier(ref i) => {
                cursor.write_all(&[i.0.0.len() as u8])?;
                cursor.write_all(i.0.0.as_bytes())
            }
            Component::Json(json) => {
                let mut hasher = DefaultHasher::new();
                json.hash(&mut hasher);
                let hash = hasher.finish();
                cursor.write_u64::<BigEndian>(hash)
            }
        }
    }
}

/// Serializes component(s) into bytes.
///
/// # Arguments
/// * `components`: The components to serialize to bytes.
pub fn build(components: &[Component]) -> Vec<u8> {
    let len = components.iter().fold(0, |len, component| len + component.byte_len());
    let mut cursor: Cursor<Vec<u8>> = Cursor::new(Vec::with_capacity(len));
    for component in components {
        component
            .write(&mut cursor)
            .expect("failed to write bytes to in-memory buffer");
    }

    cursor.into_inner()
}

/// Reads a UUID from bytes.
pub fn read_uuid<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<Uuid> {
    let mut buf: [u8; 16] = [0; 16];
    cursor.read_exact(&mut buf)?;
    let uuid = Uuid::from_slice(&buf).map_err(Into::<crate::errors::GraphError>::into)?;
    Ok(uuid)
}

/// Reads an identifier from bytes.
///
/// # Safety
/// This is used for reading in datastores that already checked the validity
/// of the data at write-time. Re-validation is skipped in the interest of performance.
#[allow(unsafe_code)]
pub unsafe fn read_identifier<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<Identifier> {
    let t_len = {
        let mut buf: [u8; 1] = [0; 1];
        cursor.read_exact(&mut buf)?;
        buf[0] as usize
    };

    let mut buf = vec![0u8; t_len];
    cursor.read_exact(&mut buf)?;
    let s = unsafe { str::from_utf8_unchecked(&buf) }.to_string();
    unsafe { Ok(Identifier::new_unchecked(s)) }
}

/// Reads a fixed-length string from bytes.
pub fn read_fixed_length_string<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<String> {
    let mut buf = String::new();
    cursor.read_to_string(&mut buf)?;
    Ok(buf)
}

/// Reads a `u64` from bytes.
pub fn read_u64<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<u64> {
    let i = cursor.read_u64::<BigEndian>()?;
    Ok(i)
}

/// Generates a UUID v1. This utility method uses a shared context and node ID
/// to help ensure generated UUIDs are unique.
pub fn generate_uuid_v1() -> Uuid {
    let ts = Timestamp::now(&*CONTEXT);
    Uuid::new_v1(ts, &NODE_ID)
}

/// Gets the next UUID that would occur after the given one.
///
/// # Errors
/// Returns a `ValidationError` if the input UUID is the greatest possible value
/// (i.e., FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF)
pub fn next_uuid(uuid: Uuid) -> ValidationResult<Uuid> {
    let mut bytes = *uuid.as_bytes();

    for i in (0..16).rev() {
        if bytes[i] < 255 {
            bytes[i] += 1;
            return Ok(Uuid::from_slice(&bytes[..]).unwrap());
        } else {
            bytes[i] = 0;
        }
    }

    Err(ValidationError::CannotIncrementUuid)
}

/// Extracts vertices from the last query output value, or `None`.
pub fn extract_vertices(mut output: Vec<QueryOutputValue>) -> Option<Vec<Vertex>> {
    if let Some(QueryOutputValue::Vertices(vertices)) = output.pop() {
        Some(vertices)
    } else {
        None
    }
}

/// Extracts edges from the last query output value, or `None`.
pub fn extract_edges(mut output: Vec<QueryOutputValue>) -> Option<Vec<Edge>> {
    if let Some(QueryOutputValue::Edges(edges)) = output.pop() {
        Some(edges)
    } else {
        None
    }
}

/// Extracts a count from the last query output value, or `None`.
pub fn extract_count(mut output: Vec<QueryOutputValue>) -> Option<u64> {
    if let Some(QueryOutputValue::Count(count)) = output.pop() {
        Some(count)
    } else {
        None
    }
}

/// Extracts vertex properties from the last query output value, or `None`.
pub fn extract_vertex_properties(mut output: Vec<QueryOutputValue>) -> Option<Vec<VertexProperties>> {
    if let Some(QueryOutputValue::VertexProperties(props)) = output.pop() {
        Some(props)
    } else {
        None
    }
}

/// Extracts edge properties from the last query output value, or `None`.
pub fn extract_edge_properties(mut output: Vec<QueryOutputValue>) -> Option<Vec<EdgeProperties>> {
    if let Some(QueryOutputValue::EdgeProperties(props)) = output.pop() {
        Some(props)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{
        extract_count, extract_edge_properties, extract_edges, extract_vertex_properties, extract_vertices,
        generate_uuid_v1, next_uuid,
    };
    use crate::{
        identifiers::Identifier,
        json::ijson,
        edges::Edge,
        vertices::Vertex,
        properties::PropertyMap, // PropertyMap is used in tests, so keep it.
        queries::QueryOutputValue,
    };
    use core::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn should_generate_new_uuid_v1() {
        let first = generate_uuid_v1();
        let second = generate_uuid_v1();
        assert_ne!(first, second);
    }

    #[test]
    fn should_generate_next_uuid() {
        let result = next_uuid(Uuid::from_str("16151dea-a538-4bf1-9559-851e256cf139").unwrap());
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Uuid::from_str("16151dea-a538-4bf1-9559-851e256cf13a").unwrap()
        );

        let from_uuid = Uuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();
        assert!(next_uuid(from_uuid).is_err());
    }

    #[test]
    fn should_not_extract_vertices_on_empty() {
        assert_eq!(extract_vertices(vec![]), None);
    }

    #[test]
    fn should_extract_vertices() {
        let dummy_vertex = Vertex::new(PropertyMap::new());
        let output = vec![QueryOutputValue::Vertices(vec![dummy_vertex.clone()])];
        assert_eq!(extract_vertices(output).unwrap().len(), 1);
    }

    #[test]
    fn should_not_extract_edges_on_empty() {
        assert_eq!(extract_edges(vec![]), None);
    }

    #[test]
    fn should_not_extract_count_on_empty() {
        assert_eq!(extract_count(vec![]), None);
    }

    #[test]
    fn should_not_extract_vertex_properties_on_empty() {
        assert_eq!(extract_vertex_properties(vec![]), None);
    }

    #[test]
    fn should_not_extract_edge_properties_on_empty() {
        assert_eq!(extract_edge_properties(vec![]), None);
    }
}

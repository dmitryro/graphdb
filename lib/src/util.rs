// lib/src/util.rs
// Updated: 2025-07-04 - next_uuid now returns GraphResult and handles ValidationError.
// Fixed: 2025-07-04 - Corrected import for GraphResult.
// Fixed: 2025-08-08 - Added unsafe block for Identifier::new_unchecked to resolve E0133 warning.
// Fixed: 2025-08-13 - Simplified Component enum, removed lifetime, and added error handling to build.

use std::io::{Cursor, Read, Write};
use uuid::Uuid;
use models::errors::{GraphError, GraphResult, ValidationError};
use models::identifiers::{Identifier, SerializableInternString};
use models::json::Json;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

#[derive(Debug)]
pub enum UtilComponent {
    Uuid(Uuid),
    Identifier(Identifier),
    Json(Json),
}

// Helper for hashing Json (consistent with storage_daemon_server)
pub fn calculate_json_hash(json: &Json) -> u64 {
    let mut hasher = DefaultHasher::new();
    let serialized = serde_json::to_string(json).expect("Failed to serialize Json for hashing");
    serialized.hash(&mut hasher);
    hasher.finish()
}

pub fn build(components: &[UtilComponent]) -> GraphResult<Vec<u8>> {
    let mut vec = Vec::new();
    for component in components {
        match component {
            UtilComponent::Uuid(id) => vec.write_all(id.as_bytes())
                .map_err(|e| GraphError::SerializationError(e.to_string()))?,
            UtilComponent::Identifier(identifier) => {
                let s_bytes = identifier.0.0.as_bytes();
                if s_bytes.len() > u8::MAX as usize {
                    return Err(GraphError::Validation(ValidationError::InvalidIdentifierLength));
                }
                vec.write_u8(s_bytes.len() as u8)
                    .map_err(|e| GraphError::SerializationError(e.to_string()))?;
                vec.write_all(s_bytes)
                    .map_err(|e| GraphError::SerializationError(e.to_string()))?;
            },
            UtilComponent::Json(json_value) => {
                let hash_value = calculate_json_hash(json_value);
                vec.write_u64::<BigEndian>(hash_value)
                    .map_err(|e| GraphError::SerializationError(e.to_string()))?;
            },
        }
    }
    Ok(vec)
}

#[allow(unsafe_code)]
pub unsafe fn read_uuid<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<Uuid> {
    // Assumes input was created by build() with valid Uuid bytes
    let mut bytes = [0u8; 16];
    cursor.read_exact(&mut bytes)?;
    Ok(Uuid::from_bytes(bytes))
}

#[allow(unsafe_code)]
pub unsafe fn read_identifier<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<Identifier> {
    // Assumes input was created by build() with valid length-prefixed UTF-8 bytes
    let t_len = {
        let mut buf: [u8; 1] = [0; 1];
        cursor.read_exact(&mut buf)?;
        buf[0] as usize
    };

    let mut buf = vec![0u8; t_len];
    cursor.read_exact(&mut buf)?;
    let s = String::from_utf8(buf)
        .map_err(|e| GraphError::DeserializationError(e.to_string()))?;
    Ok(Identifier::new(s)?)
}

pub fn read_u64<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<u64> {
    Ok(cursor.read_u64::<BigEndian>()?)
}

pub fn next_uuid(uuid: Uuid) -> GraphResult<Uuid> {
    let mut bytes = *uuid.as_bytes();
    for i in (0..16).rev() {
        if bytes[i] == 255 {
            bytes[i] = 0;
        } else {
            bytes[i] += 1;
            return Ok(Uuid::from_bytes(bytes));
        }
    }
    Err(GraphError::Validation(ValidationError::CannotIncrementUuid))
}
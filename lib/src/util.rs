// lib/src/util.rs
// Updated: 2025-07-04 - next_uuid now returns GraphResult and handles ValidationError.
// Fixed: 2025-07-04 - Corrected import for GraphResult.

use std::io::{Cursor, Read, Write};
use uuid::Uuid;
use models::errors::GraphResult; // Corrected: Import GraphResult directly
use models::identifiers::{Identifier, SerializableInternString};
use models::json::Json;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use models::errors::{GraphError, ValidationError}; // Import GraphError and ValidationError

#[derive(Debug)]
pub enum Component<'a> {
    Uuid(Uuid),
    // Aligned with models/src/util.rs: Identifier is length-prefixed
    Identifier(Identifier), 
    Json(&'a Json),
}

// Helper for hashing Json (consistent with storage_daemon_server)
pub fn calculate_json_hash(json: &Json) -> u64 {
    let mut hasher = DefaultHasher::new();
    let serialized = serde_json::to_string(json).expect("Failed to serialize Json for hashing");
    serialized.hash(&mut hasher);
    hasher.finish()
}

pub fn build(components: &[Component<'_>]) -> Vec<u8> {
    let mut vec = Vec::new();
    for component in components {
        match component {
            Component::Uuid(id) => vec.write_all(id.as_bytes()).unwrap(),
            Component::Identifier(identifier) => {
                // FIX: Aligned with models/src/util.rs for length-prefixed Identifier
                // Write length byte, then the string bytes
                let s_bytes = identifier.0 .0.as_bytes();
                vec.write_u8(s_bytes.len() as u8).unwrap(); // Length byte
                vec.write_all(s_bytes).unwrap(); // String bytes
            },
            Component::Json(json_value) => {
                let hash_value = calculate_json_hash(json_value);
                vec.write_u64::<BigEndian>(hash_value).unwrap();
            },
        }
    }
    vec
}

#[allow(unsafe_code)]
pub unsafe fn read_uuid<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<Uuid> { // Changed Result to GraphResult
    let mut bytes = [0u8; 16];
    cursor.read_exact(&mut bytes)?;
    Ok(Uuid::from_bytes(bytes))
}

#[allow(unsafe_code)]
pub unsafe fn read_identifier<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<Identifier> { // Changed Result to GraphResult
    // Aligned with models/src/util.rs for length-prefixed Identifier
    let t_len = {
        let mut buf: [u8; 1] = [0; 1];
        cursor.read_exact(&mut buf)?;
        buf[0] as usize
    };

    let mut buf = vec![0u8; t_len];
    cursor.read_exact(&mut buf)?;
    let s = unsafe { String::from_utf8_unchecked(buf) }; // Use from_utf8_unchecked for performance
    Ok(Identifier::new_unchecked(s))
}

pub fn read_u64<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> GraphResult<u64> { // Changed Result to GraphResult
    Ok(cursor.read_u64::<BigEndian>()?)
}

// FIX: next_uuid now returns GraphResult and handles ValidationError
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
    // If all bytes are 255, it means we cannot increment
    Err(GraphError::Validation(ValidationError::CannotIncrementUuid))
}


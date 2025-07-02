use std::io::{Cursor, Read, Write};
use uuid::Uuid;
use crate::errors::Result;
use models::identifiers::{Identifier, SerializableInternString};
use models::json::Json;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

#[derive(Debug)]
pub enum Component<'a> {
    Uuid(Uuid),
    Identifier(Identifier), // For generic IDs like NodeKind, PropertyName, EdgeType
    Json(&'a Json),         // For property values (JSON)
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
                // FIX: identifier.0 is SerializableInternString, get .0 (String) then as_bytes()
                vec.write_all(identifier.0 .0.as_bytes()).unwrap();
                vec.write_u8(0x00).unwrap(); // Null terminator for Identifier strings
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
pub unsafe fn read_uuid<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Uuid> {
    let mut bytes = [0u8; 16];
    cursor.read_exact(&mut bytes)?;
    Ok(Uuid::from_bytes(bytes))
}

#[allow(unsafe_code)]
pub unsafe fn read_identifier<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Identifier> {
    let mut bytes = Vec::new();
    loop {
        let byte = cursor.read_u8()?;
        if byte == 0x00 {
            break;
        }
        bytes.push(byte);
    }
    let s = String::from_utf8(bytes)?;
    Ok(Identifier::new_unchecked(s))
}

pub fn read_u64<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<u64> {
    Ok(cursor.read_u64::<BigEndian>()?)
}

// FIX: Removed 'mut' from uuid parameter to clear the warning
pub fn next_uuid(uuid: Uuid) -> Uuid {
    let mut bytes = *uuid.as_bytes();
    for i in (0..16).rev() {
        if bytes[i] == 255 {
            bytes[i] = 0;
        } else {
            bytes[i] += 1;
            break;
        }
    }
    Uuid::from_bytes(bytes)
}

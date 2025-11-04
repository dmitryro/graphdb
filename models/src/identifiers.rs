use core::{hash::Hash, ops::Deref};
use std::{cmp::Ordering, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode, BorrowDecode};
use internment::Intern;
use uuid::Uuid;

use crate::errors::{ValidationError, ValidationResult};

#[derive(Clone, Debug, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SerializableUuid(pub Uuid);



impl SerializableUuid {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl FromStr for SerializableUuid {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::from_str(s).map(SerializableUuid)
    }
}

impl fmt::Display for SerializableUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Encode for SerializableUuid {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.as_bytes().encode(encoder)
    }
}

impl<CTX> Decode<CTX> for SerializableUuid {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        let arr: [u8; 16] = Decode::decode(decoder)?;
        Ok(SerializableUuid(Uuid::from_bytes(arr)))
    }
}

impl<'de, CTX> BorrowDecode<'de, CTX> for SerializableUuid {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        let arr: [u8; 16] = BorrowDecode::borrow_decode(decoder)?;
        Ok(SerializableUuid(Uuid::from_bytes(arr)))
    }
}

impl From<Uuid> for SerializableUuid {
    fn from(uuid: Uuid) -> Self {
        SerializableUuid(uuid)
    }
}

impl From<SerializableUuid> for Uuid {
    fn from(s_uuid: SerializableUuid) -> Self {
        s_uuid.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SerializableInternString(pub Intern<String>);

impl Encode for SerializableInternString {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.as_ref().encode(encoder)
    }
}

impl<CTX> Decode<CTX> for SerializableInternString {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        let s: String = Decode::decode(decoder)?;
        Ok(SerializableInternString(Intern::new(s)))
    }
}

impl<'de, CTX> BorrowDecode<'de, CTX> for SerializableInternString {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        let s: String = BorrowDecode::borrow_decode(decoder)?;
        Ok(SerializableInternString(Intern::new(s)))
    }
}

impl From<Intern<String>> for SerializableInternString {
    fn from(intern_str: Intern<String>) -> Self {
        SerializableInternString(intern_str)
    }
}

impl From<SerializableInternString> for Intern<String> {
    fn from(s_intern_str: SerializableInternString) -> Self {
        s_intern_str.0
    }
}

impl AsRef<str> for SerializableInternString {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl Deref for SerializableInternString {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl fmt::Display for SerializableInternString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize, Encode)]
pub struct Identifier(pub SerializableInternString);

impl<CTX> Decode<CTX> for Identifier {
    fn decode<D: bincode::de::Decoder>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self(SerializableInternString::decode(decoder)?))
    }
}

impl<'de, CTX> BorrowDecode<'de, CTX> for Identifier {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        Ok(Self(SerializableInternString::borrow_decode(decoder)?))
    }
}

impl Identifier {
    pub fn new(value: String) -> ValidationResult<Self> {
        if value.is_empty() || value.len() > u8::MAX as usize {
            return Err(ValidationError::InvalidIdentifierLength);
        }
        Ok(Self(SerializableInternString(Intern::new(value))))
    }

    #[allow(unsafe_code)]
    pub unsafe fn new_unchecked(value: String) -> Self {
        Self(SerializableInternString(Intern::new(value)))
    }
}

impl AsRef<str> for Identifier {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl Deref for Identifier {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl FromStr for Identifier {
    type Err = ValidationError;
    fn from_str(s: &str) -> ValidationResult<Self> {
        Self::new(s.to_string())
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialOrd for Identifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl Ord for Identifier {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{Identifier, ValidationError};
    use core::str::FromStr;

    #[test]
    fn should_not_create_empty_identifier() {
        let identifier = Identifier::new("".to_string());
        assert!(identifier.is_err());
        assert_eq!(identifier.unwrap_err(), ValidationError::InvalidIdentifierLength);
    }

    #[test]
    fn should_not_create_too_long_identifier() {
        let identifier = Identifier::new("a".repeat(256));
        assert!(identifier.is_err());
        assert_eq!(identifier.unwrap_err(), ValidationError::InvalidIdentifierLength);
    }

    #[test]
    fn should_create_identifier() {
        let identifier = Identifier::new("test".to_string());
        assert!(identifier.is_ok());
        assert_eq!(identifier.unwrap().as_ref(), "test");
    }

    #[test]
    fn should_convert_identifier_from_str() {
        let identifier = Identifier::from_str("test");
        assert!(identifier.is_ok());
        assert_eq!(identifier.unwrap().as_ref(), "test");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct VertexId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct EdgeId(pub u64);

impl VertexId {
    pub fn new(id: u64) -> Self {
        VertexId(id)
    }
}

impl EdgeId {
    pub fn new(id: u64) -> Self {
        EdgeId(id)
    }
}

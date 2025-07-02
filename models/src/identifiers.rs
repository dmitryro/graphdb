// models/src/identifiers.rs

use core::{hash::Hash, ops::Deref};
use std::{cmp::Ordering, fmt, str::FromStr};

use serde::{Deserialize, Serialize};

use internment::Intern;

use crate::errors::{ValidationError, ValidationResult};
// FIX: Removed unused import `util`
// use crate::util; // This line was removed

/// An identifier. Identifiers are fixed-length strings (255 bytes max) that
/// uniquely identify a schema object, such as a vertex label or property name.
/// The fixed length enables faster comparisons.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Identifier(pub Intern<String>);

impl Identifier {
    /// Creates a new identifier.
    ///
    /// # Arguments
    /// * `value`: The identifier value. The value must be between 1 and 255
    /// bytes in length (inclusive).
    ///
    /// # Errors
    /// Returns a `ValidationError` if the `value` is not between 1 and 255
    /// bytes in length (inclusive).
    pub fn new(value: String) -> ValidationResult<Self> {
        if value.is_empty() || value.len() > u8::MAX as usize {
            return Err(ValidationError::InvalidIdentifierLength);
        }

        Ok(Self(Intern::new(value)))
    }

    /// Creates a new identifier without validation.
    ///
    /// # Arguments
    /// * `value`: The identifier value.
    ///
    /// # Safety
    /// The caller must ensure that the `value` is between 1 and 255 bytes in
    /// length (inclusive).
    #[allow(unsafe_code)]
    pub unsafe fn new_unchecked(value: String) -> Self {
        Self(Intern::new(value))
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

impl From<Identifier> for String {
    fn from(value: Identifier) -> Self {
        value.0.to_string()
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
    use super::Identifier;
    use crate::errors::ValidationError;
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
        assert_eq!(identifier.unwrap().0.as_ref(), "test");
    }

    #[test]
    fn should_convert_identifier_from_str() {
        let identifier = Identifier::from_str("test");
        assert!(identifier.is_ok());
        assert_eq!(identifier.unwrap().0.as_ref(), "test");
    }
}

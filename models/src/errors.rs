use std::io;
pub use thiserror::Error; // Make the derive macro visible, though usually it's just `use thiserror::Error` for the derive.
                          // The `#[derive(Error)]` is what uses it.
                          // It's more about the enum itself being `pub`.
use uuid;
use bincode::error::{DecodeError, EncodeError}; // bincode needs to be in models/Cargo.toml

use crate::{identifiers::Identifier, properties::PropertyMap, PropertyValue};

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("entity with identifier {0} was not found")]
    NotFound(Identifier),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Validation(#[from] ValidationError),
    #[cfg(feature = "rocksdb-errors")]
    #[error(transparent)]
    Rocksdb(#[from] rocksdb::Error),
    #[cfg(feature = "sled-errors")]
    #[error(transparent)]
    Sled(#[from] sled::Error),
    #[cfg(feature = "bincode-errors")]
    #[error(transparent)]
    BincodeDecode(#[from] DecodeError),
    #[cfg(feature = "bincode-errors")]
    #[error(transparent)]
    BincodeEncode(#[from] EncodeError),
    #[error("UUID parsing or generation error: {0}")]
    Uuid(#[from] uuid::Error),
    #[error("An unknown error occurred.")]
    Unknown,
    // Add a conversion for bcrypt::BcryptError if you want it to propagate into GraphError
    #[error("Authentication error: {0}")]
    Auth(String), // Add a general authentication error
}

/// A validation error.
#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
    // ... (rest of your ValidationError enum)
    /// An invalid value was provided where a specific value or format was expected.
    #[error("invalid value provided")]
    InvalidValue,
    /// An inner query produced an invalid or unexpected output type.
    #[error("inner query produced an invalid output type")]
    InnerQuery,
    /// An identifier is invalid (e.g., malformed string).
    #[error("identifier '{0}' is invalid")]
    InvalidIdentifier(String),
    /// An identifier has an invalid length.
    #[error("identifier has invalid length")]
    InvalidIdentifierLength,
    /// Cannot increment UUID because it is already at its maximum value.
    #[error("cannot increment UUID because it is already at its maximum value")]
    CannotIncrementUuid,
    /// A property with the given name was not found.
    #[error("property with name {0} not found")]
    PropertyNotFound(Identifier),
    /// The property has an unexpected type.
    #[error("property has unexpected type, expected {0}, found {1}")]
    PropertyTypeMismatch(String, String),
    /// A required property was not found.
    #[error("required property with name {0} not found")]
    RequiredPropertyNotFound(Identifier),
    /// A required property has an unexpected type.
    #[error("required property {0} has unexpected type, expected {1}, found {2}")]
    RequiredPropertyTypeMismatch(Identifier, String, String),
    /// A vertex with the given label already exists.
    #[error("vertex with label {0} already exists")]
    VertexAlreadyExists(Identifier),
    /// An edge with the given label already exists.
    #[error("edge with label {0} already exists")]
    EdgeAlreadyExists(Identifier),
    /// A property value is missing for the given property name.
    #[error("missing property value for {0}")]
    MissingPropertyValue(Identifier),
    /// An invalid value for a property was provided.
    #[error("invalid value for property {0}")]
    InvalidPropertyValue(Identifier),
    /// A property is read-only and cannot be changed.
    #[error("property {0} is read-only and cannot be changed")]
    ReadOnlyProperty(Identifier),
    /// Password hashing failed.
    #[error("password hashing failed")]
    PasswordHashingFailed,
    /// Password verification failed.
    #[error("password verification failed")]
    PasswordVerificationFailed,
    /// An unexpected property was found.
    #[error("unexpected property with name {0}")]
    UnexpectedProperty(Identifier),
    /// An invalid date format was provided.
    #[error("invalid date format: {0}")]
    InvalidDateFormat(String),
}

/// A type alias for a `Result` that returns a `GraphError` on failure.
pub type GraphResult<T> = Result<T, GraphError>;

/// A type alias for a `Result` that returns a `ValidationError` on failure.
pub type ValidationResult<T> = Result<T, ValidationError>;

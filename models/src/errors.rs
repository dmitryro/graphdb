// models/src/errors.rs

use std::io;
use std::string::FromUtf8Error;
pub use thiserror::Error;
use uuid;
use uuid::Error as UuidError;
use bincode::error::{DecodeError, EncodeError};
use anyhow::Error as AnyhowError;
use serde_json::Error as SerdeJsonError;
use rmp_serde::encode::Error as RmpEncodeError;
use rmp_serde::decode::Error as RmpDecodeError;
use zmq::Error as ZmqError;
use tokio::task::JoinError;

use crate::{identifiers::Identifier, properties::PropertyMap, PropertyValue};

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("Storage error: {0}")]
    StorageError(String), // General storage operation error
    #[error("Serialization error: {0}")]
    SerializationError(String), // Error during data serialization
    #[error("Deserialization error: {0}")]
    DeserializationError(String), // Error during data deserialization
    #[error("Invalid query: {0}")]
    QueryError(String), // Error during query parsing or execution logic
    #[error("Database connection error: {0}")]
    ConnectionError(String), // Error connecting to the database
    #[error("Transaction error: {0}")]
    TransactionError(String), // Error specific to transaction management
    #[error("Configuration error: {0}")]
    ConfigError(String), // Error with configuration loading or validation

    #[error("Failed to acquire lock: {0}")] // Added LockError
    LockError(String),
    #[error("Feature not implemented: {0}")] // Added NotImplemented
    NotImplemented(String),
    #[error("Entity already exists: {0}")] // Added AlreadyExists
    AlreadyExists(String),
    #[error("Invalid data provided: {0}")] // Added InvalidData
    InvalidData(String),
    #[error("An internal error occurred: {0}")] // Added InternalError
    InternalError(String),

    #[error("entity with identifier {0} was not found")]
    NotFound(Identifier),
    #[error(transparent)]
    Io(#[from] io::Error), // Correctly defined Io variant
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
    Uuid(#[from] UuidError),
    #[error("An unknown error occurred.")]
    Unknown,
    #[error("Authentication error: {0}")]
    Auth(String), // General authentication error

    #[error("Invalid storage engine: {0}")]
    InvalidStorageEngine(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Network error: {0}")]
    NetworkError(String),
}

// Implement From for serde_json::Error to convert into GraphError variants.
impl From<serde_json::Error> for GraphError {
    fn from(err: serde_json::Error) -> Self {
        GraphError::SerializationError(format!("JSON processing error: {}", err))
    }
}

// Added: Implement From for rmp_serde::encode::Error
impl From<rmp_serde::encode::Error> for GraphError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        GraphError::SerializationError(format!("MessagePack encode error: {}", err))
    }
}

// Added: Implement From for rmp_serde::decode::Error
impl From<rmp_serde::decode::Error> for GraphError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        GraphError::DeserializationError(format!("MessagePack decode error: {}", err))
    }
}

// Added: Implement From for anyhow::Error
impl From<AnyhowError> for GraphError {
    fn from(err: AnyhowError) -> Self {
        GraphError::StorageError(format!("Underlying storage operation failed: {}", err))
    }
}

// NEW: Implement From for zmq::Error
impl From<ZmqError> for GraphError {
    fn from(err: ZmqError) -> Self {
        GraphError::NetworkError(format!("ZeroMQ error: {}", err))
    }
}

// THIS IS THE MISSING IMPLEMENTATION THAT THE COMPILER IS ASKING FOR.
impl From<JoinError> for GraphError {
    fn from(err: JoinError) -> Self {
        GraphError::InternalError(format!("Task failed to join: {:?}", err))
    }
}

/// A validation error.
#[derive(Debug, Error, PartialEq)]
pub enum ValidationError {
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

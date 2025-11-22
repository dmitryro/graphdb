use std::io;
use std::string::FromUtf8Error;
pub use thiserror::Error;
use uuid::Error as UuidError;
use bincode::error::{DecodeError, EncodeError};
use anyhow::Error as AnyhowError;
use serde_json::Error as SerdeJsonError;
use serde::{Serialize, Deserialize};
use rmp_serde::encode::Error as RmpEncodeError;
use rmp_serde::decode::Error as RmpDecodeError;
use zmq::Error as ZmqError;
use tokio::task::JoinError;
use std::time::SystemTimeError;
use crate::{identifiers::Identifier, properties::PropertyMap, PropertyValue};

#[derive(Debug, Serialize, Deserialize, Error, Clone)] // <--- ADDED CLONE
pub enum GraphError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("IO error: {0}")]
    IoError(String),
    #[error("Storage error: {0}")]
    StorageError(String), // General storage operation error
    #[error("ZMQ error: {0}")]
    ZmqError(String), // General ZMQ error
    #[error("Invalid Request: {0}")]
    InvalidRequest(String),
    #[error("Timeout error: {0}")]
    TimeoutError(String),
    #[error("Daemon error: {0}")]
    DaemonError(String),
    #[error("Daemon start error: {0}")]
    DaemonStartError(String),
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
    ConfigurationError(String), // Error with configuration loading or validation
    #[error("Failed to acquire lock: {0}")]
    LockError(String),
    #[error("System failure: {0}")]
    SystemError(String),
    #[error("Feature not implemented: {0}")]
    NotImplemented(String),
    #[error("Entity already exists: {0}")]
    AlreadyExists(String),
    #[error("Invalid data provided: {0}")]
    InvalidData(String),
    #[error("An internal error occurred: {0}")]
    InternalError(String),
    #[error("entity with identifier {0} was not found")]
    NotFound(Identifier),
    #[error("Validation error: {0}")]
    Validation(ValidationError),
    #[cfg(feature = "rocksdb-errors")]
    #[error("RocksDB error: {0}")]
    Rocksdb(String),
    #[cfg(feature = "sled-errors")]
    #[error("Sled error: {0}")]
    Sled(String),
    #[cfg(feature = "bincode-errors")]
    #[error("Bincode decode error: {0}")]
    BincodeDecode(String),
    #[cfg(feature = "bincode-errors")]
    #[error("Bincode encode error: {0}")]
    BincodeEncode(String),
    #[error("UUID parsing or generation error: {0}")]
    Uuid(String),
    #[error("An unknown error occurred.")]
    Unknown,
    #[error("Authentication error: {0}")]
    Auth(String), // General authentication error
    #[error("Invalid storage engine: {0}")]
    InvalidStorageEngine(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Storage error: {0}")]
    Storage(String),
}

// Implement the From trait for &str
impl From<&str> for GraphError {
    fn from(error: &str) -> Self {
        GraphError::InvalidRequest(error.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for GraphError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        GraphError::TimeoutError("operation timed out".into())
    }
}

// Implement From for rmp_serde::encode::Error
impl From<RmpEncodeError> for GraphError {
    fn from(err: RmpEncodeError) -> Self {
        GraphError::SerializationError(format!("MessagePack encode error: {}", err))
    }
}

// Implement From for rmp_serde::decode::Error
impl From<RmpDecodeError> for GraphError {
    fn from(err: RmpDecodeError) -> Self {
        GraphError::DeserializationError(format!("MessagePack decode error: {}", err))
    }
}

// Implement From for serde_json::Error
impl From<SerdeJsonError> for GraphError {
    fn from(err: SerdeJsonError) -> Self {
        GraphError::Serialization(format!("JSON serialization error: {}", err))
    }
}

// Implement From for anyhow::Error
impl From<AnyhowError> for GraphError {
    fn from(err: AnyhowError) -> Self {
        GraphError::StorageError(format!("Underlying storage operation failed: {}", err))
    }
}

// Implement From for zmq::Error
impl From<ZmqError> for GraphError {
    fn from(err: ZmqError) -> Self {
        GraphError::NetworkError(format!("ZeroMQ error: {}", err))
    }
}

impl From<SystemTimeError> for GraphError {
    fn from(err: SystemTimeError) -> Self {
        GraphError::StorageError(format!("System time error: {}", err))
    }
}

// Implement From for JoinError
impl From<JoinError> for GraphError {
    fn from(err: JoinError) -> Self {
        GraphError::InternalError(format!("Task failed to join: {}", err))
    }
}

// Implement From for io::Error
impl From<io::Error> for GraphError {
    fn from(err: io::Error) -> Self {
        GraphError::Io(format!("IO error: {}", err))
    }
}

// Implement From for UuidError
impl From<UuidError> for GraphError {
    fn from(err: UuidError) -> Self {
        GraphError::Uuid(format!("UUID error: {}", err))
    }
}

// Implement From for ValidationError
impl From<ValidationError> for GraphError {
    fn from(err: ValidationError) -> Self {
        GraphError::Validation(err)
    }
}

// Implement From for rocksdb::Error
#[cfg(feature = "rocksdb-errors")]
impl From<rocksdb::Error> for GraphError {
    fn from(err: rocksdb::Error) -> Self {
        GraphError::Rocksdb(format!("RocksDB error: {}", err))
    }
}

// Implement From for sled::Error
#[cfg(feature = "sled-errors")]
impl From<sled::Error> for GraphError {
    fn from(err: sled::Error) -> Self {
        GraphError::Sled(format!("Sled error: {}", err))
    }
}

// Implement From for bincode::error::DecodeError
#[cfg(feature = "bincode-errors")]
impl From<DecodeError> for GraphError {
    fn from(err: DecodeError) -> Self {
        GraphError::BincodeDecode(format!("Bincode decode error: {}", err))
    }
}

// Implement From for bincode::error::EncodeError
#[cfg(feature = "bincode-errors")]
impl From<EncodeError> for GraphError {
    fn from(err: EncodeError) -> Self {
        GraphError::BincodeEncode(format!("Bincode encode error: {}", err))
    }
}

#[derive(Debug, Serialize, Deserialize, Error, PartialEq, Clone)] // <--- ADDED CLONE
pub enum ValidationError {
    #[error("invalid value provided")]
    InvalidValue,
    #[error("inner query produced an invalid output type")]
    InnerQuery,
    #[error("identifier '{0}' is invalid")]
    InvalidIdentifier(String),
    #[error("identifier has invalid length")]
    InvalidIdentifierLength,
    #[error("cannot increment UUID because it is already at its maximum value")]
    CannotIncrementUuid,
    #[error("property with name {0} not found")]
    PropertyNotFound(Identifier),
    #[error("property has unexpected type, expected {0}, found {1}")]
    PropertyTypeMismatch(String, String),
    #[error("required property with name {0} not found")]
    RequiredPropertyNotFound(Identifier),
    #[error("required property {0} has unexpected type, expected {1}, found {2}")]
    RequiredPropertyTypeMismatch(Identifier, String, String),
    #[error("vertex with label {0} already exists")]
    VertexAlreadyExists(Identifier),
    #[error("edge with label {0} already exists")]
    EdgeAlreadyExists(Identifier),
    #[error("missing property value for {0}")]
    MissingPropertyValue(Identifier),
    #[error("invalid value for property {0}")]
    InvalidPropertyValue(Identifier),
    #[error("property {0} is read-only and cannot be changed")]
    ReadOnlyProperty(Identifier),
    #[error("password hashing failed")]
    PasswordHashingFailed,
    #[error("password verification failed")]
    PasswordVerificationFailed,
    #[error("unexpected property with name {0}")]
    UnexpectedProperty(Identifier),
    #[error("invalid date format: {0}")]
    InvalidDateFormat(String),
}

/// A type alias for a `Result` that returns a `GraphError` on failure.
pub type GraphResult<T> = Result<T, GraphError>;

/// A type alias for a `Result` that returns a `ValidationError` on failure.
pub type ValidationResult<T> = Result<T, ValidationError>;


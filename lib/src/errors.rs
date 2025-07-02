// lib/src/errors.rs

use std::error::Error as StdError;
use std::fmt;
use thiserror::Error;

use bincode::error::{DecodeError, EncodeError};
use bcrypt::BcryptError;
use uuid::Error as UuidError;
use serde_json::Error as SerdeJsonError;
use std::string::FromUtf8Error;
use rmp_serde::encode::Error as RmpEncodeError; // NEW: Add this import


#[derive(Debug, Error)]
pub enum GraphError {
    #[error("Database operation failed: {0}")]
    DatabaseError(String),

    #[error("Serialization/Deserialization error: {0}")]
    SerializationError(String),

    #[error("Invalid input or data: {0}")]
    InvalidData(String),

    #[error("Not Found: {0}")]
    NotFound(String),

    #[error("Already Exists: {0}")]
    AlreadyExists(String),

    #[error("Authentication failed: {0}")]
    AuthenticationError(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Internal server error: {0}")]
    InternalError(String),

    #[error("Feature not implemented: {0}")]
    NotImplemented(String),

    #[error("Lock acquisition failed: {0}")]
    LockError(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("GraphQL error: {0}")]
    GraphQLError(String),

    #[error("File I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] DecodeError),
    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] EncodeError),

    #[error("Password hashing error: {0}")]
    PasswordHashingError(#[from] BcryptError),

    #[error("Invalid password hash: {0}")]
    InvalidPasswordHash(String),

    #[error("UUID error: {0}")]
    UuidError(#[from] UuidError),

    #[error("JSON serialization/deserialization error: {0}")]
    JsonError(#[from] SerdeJsonError),

    #[error("UTF-8 conversion error: {0}")]
    FromUtf8(#[from] FromUtf8Error),

    // NEW: This variant handles rmp_serde::encode::Error
    #[error("MessagePack encoding error: {0}")]
    RmpSerdeEncode(#[from] RmpEncodeError),

    #[error("Custom error: {0}")]
    Custom(String),
}

pub type Result<T> = std::result::Result<T, GraphError>;

impl From<sled::Error> for GraphError {
    fn from(err: sled::Error) -> Self {
        GraphError::DatabaseError(err.to_string())
    }
}

impl From<tokio::task::JoinError> for GraphError {
    fn from(err: tokio::task::JoinError) -> Self {
        GraphError::InternalError(format!("Async task join error: {}", err))
    }
}

impl From<anyhow::Error> for GraphError {
    fn from(err: anyhow::Error) -> Self {
        GraphError::InternalError(format!("An internal error occurred: {}", err))
    }
}

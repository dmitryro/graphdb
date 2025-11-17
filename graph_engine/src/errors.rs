// lib/src/errors.rs

use std::fmt;

#[derive(Debug)]
pub enum GraphError {
    /// Vertex with specified ID not found
    VertexNotFound,

    /// Edge with specified ID not found
    EdgeNotFound,

    /// Invalid query string or format
    InvalidQuery(String),

    /// Generic storage-related error
    StorageError(String),

    /// Invalid identifier format
    InvalidIdentifier(String),

    /// Other unspecified errors
    Unknown(String),
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphError::VertexNotFound => write!(f, "Vertex not found"),
            GraphError::EdgeNotFound => write!(f, "Edge not found"),
            GraphError::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            GraphError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            GraphError::InvalidIdentifier(msg) => write!(f, "Invalid identifier: {}", msg),
            GraphError::Unknown(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for GraphError {}


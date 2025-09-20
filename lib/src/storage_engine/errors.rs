use std::fmt;

/// A custom error type that encapsulates various errors that can occur within the
/// storage engine client.
///
/// It categorizes errors into three types for structured error handling:
/// - `ConnectionError`: For issues related to network communication (e.g., ZeroMQ failures).
/// - `EngineError`: For errors returned by the remote storage engine itself.
/// - `InternalError`: For unexpected client-side logic errors.
#[derive(Debug)]
pub enum StorageEngineError {
    ConnectionError(String),
    InternalError(String),
    EngineError(String),
}

// The `Display` trait allows us to print the error in a user-friendly way.
impl fmt::Display for StorageEngineError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StorageEngineError::ConnectionError(msg) => write!(f, "Connection Error: {}", msg),
            StorageEngineError::InternalError(msg) => write!(f, "Internal Error: {}", msg),
            StorageEngineError::EngineError(msg) => write!(f, "Engine Error: {}", msg),
        }
    }
}

// The `Error` trait is a marker trait that signifies a type is a Rust error type.
// It requires `Debug` and `Display`, which we have already implemented.
impl std::error::Error for StorageEngineError {}

// This `From` implementation allows us to easily convert `anyhow::Error` into our
// custom `StorageEngineError`. This is crucial for handling the `.context()` calls
// from the `anyhow` crate in your ZmqClient.
impl From<anyhow::Error> for StorageEngineError {
    fn from(e: anyhow::Error) -> Self {
        StorageEngineError::InternalError(format!("{:?}", e))
    }
}


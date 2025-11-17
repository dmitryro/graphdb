// indexing_service/src/errors.rs

use thiserror::Error;

#[derive(Error, Debug)]
pub enum IndexingError {
    #[error("Full-text indexing error: {0}")]
    FullTextError(String),

    #[error("KV indexing error: {0}")]
    KvError(String),

    #[error("Cache error: {0}")]
    CacheError(String),
}

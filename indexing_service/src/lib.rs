// indexing_service/src/lib.rs
pub mod index;
pub mod fulltext;
pub mod query;
pub mod adapters;
pub mod errors;

pub use index::*;
pub use fulltext::*;
pub use query::*;
pub use adapters::*;
pub use errors::*;

// Re-export the main service
pub use index::IndexingService;
pub use index::indexing_service;
pub use index::init_indexing_service;

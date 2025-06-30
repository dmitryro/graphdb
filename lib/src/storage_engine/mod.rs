// lib/src/storage_engine/mod.rs
// Updated: 2025-06-30 - Defined the general `StorageEngine` trait and made it public.

pub mod user_storage;
pub mod sled_storage;

// --- ADDED: Definition of the general StorageEngine trait ---
pub trait StorageEngine: Send + Sync {
    // Add common methods here that all storage engines should implement, e.g.:
    // async fn initialize(&self) -> Result<()>;
    // async fn shutdown(&self) -> Result<()>;
    // fn get_name(&self) -> &'static str;
    // For now, it can be an empty trait if it's just a marker or a base for other traits.
}


pub use user_storage::{SledUserStorage, UserStorageEngine};
pub use sled_storage::{GraphStorageEngine, SledGraphStorage, open_sled_db};

// `StorageEngine` is now directly defined in this module,
// so `lib/src/lib.rs` can import it directly from `crate::storage_engine`.

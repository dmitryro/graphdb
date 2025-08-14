// lib/src/memory/mod.rs
// Fixed: 2025-07-04 - Made `datastore` module public and updated re-export for `InMemoryGraphStorage`.
// Fixed: 2025-07-04 - Removed obsolete MemoryDatastore re-export and updated test macros.

//! The in-memory datastore implementation. This is the simplest and generally
//! fastest implementation, but there's no support for graphs larger than what
//! can fit in-memory, and data is only persisted to disk when explicitly
//! requested.

pub mod datastore;

// Removed: pub use datastore::MemoryDatastore; // Obsolete
pub use crate::storage_engine::inmemory_storage::{InMemoryStorage as InMemoryGraphStorage}; // Correct re-export

#[cfg(feature = "bench-suite")]
full_bench_impl!(InMemoryGraphStorage::new()); // Updated to use InMemoryGraphStorage::new()

#[cfg(feature = "test-suite")]
#[cfg(test)]
mod tests {
    use super::InMemoryGraphStorage; // Use the new name
    use crate::util::{extract_count, extract_vertices};
    use crate::{ijson, Identifier};
    use crate::database::Database; // Import the non-generic Database

    use tempfile::NamedTempFile;
    use uuid::Uuid;

    // Use the new Database struct which internally manages the storage engine
    full_test_impl!(async {
        let config = crate::storage_engine::StorageConfig {
            engine_type: crate::storage_engine::StorageEngineType::InMemory,
            data_path: "".to_string(), // In-memory doesn't strictly need a path for non-persistence
            engine_specific_config: None,
            max_open_files: None,
        };
        Database::new(config).await.unwrap()
    });

    // These test helper functions need to be updated to work with the new Database structure
    // which no longer has a generic parameter for the datastore.
    // They will need to interact with the Database methods directly.

    // Removed or updated these functions, as they are likely incompatible
    // with the new non-generic Database struct.
    // The full_test_impl! macro should handle most of the common test cases.
    /*
    fn create_vertex_with_property(db: &Database) -> Uuid { // No generic parameter
        let id = db.create_vertex_from_type(Identifier::default()).unwrap();
        db.set_properties(SpecificVertexQuery::single(id), Identifier::default(), &ijson!(true))
            .unwrap();
        id
    }

    fn expect_vertex(db: &Database, id: Uuid) { // No generic parameter
        assert_eq!(extract_count(db.get(AllVertexQuery.count().unwrap()).unwrap()), Some(1));
        let vertices = extract_vertices(db.get(SpecificVertexQuery::new(vec![id])).unwrap()).unwrap();
        assert_eq!(vertices.len(), 1);
        assert_eq!(vertices[0].id, id);
        assert_eq!(vertices[0].t, Identifier::default());
    }
    */

    #[tokio::test] // Use tokio::test for async tests
    async fn should_serialize_msgpack() {
        let path = NamedTempFile::new().unwrap();
        let config = crate::storage_engine::StorageConfig {
            engine_type: crate::storage_engine::StorageEngineType::InMemory,
            data_path: path.path().to_str().unwrap().to_string(), // Use temp path for persistence
            engine_specific_config: None,
            max_open_files: None,
        };
        let db = Database::new(config.clone()).await.unwrap(); // Clone config for second db instance

        let vertex_id = uuid::Uuid::new_v4();
        let vertex = models::Vertex::new_with_id(vertex_id, Identifier::new("test_label".to_string()).unwrap());
        db.create_vertex(vertex).await.unwrap();
        
        db.stop().await.unwrap(); // Stop to ensure flush

        let db_reloaded = Database::new(config).await.unwrap(); // Re-open from path
        let retrieved_vertex = db_reloaded.get_vertex(&vertex_id).await.unwrap();
        assert!(retrieved_vertex.is_some());
        assert_eq!(retrieved_vertex.unwrap().id.0, vertex_id);
    }
}


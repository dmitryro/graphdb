// lib/src/database.rs

use uuid::Uuid; // Uuid is used in the Transaction trait

// Core graph model types
use models::{Edge, Identifier, Json, Vertex};

// More specific model types, assuming their locations within the 'models' crate
// Please adjust these paths if your 'models' crate has a different internal structure.
use models::bulk_insert::BulkInsertItem;
// FIX: Changed from models::edges::EdgeDirection to models::queries::EdgeDirection
use models::queries::EdgeDirection; // CORRECTED PATH
use models::properties::{EdgeProperties, NamedProperty, PropertyValue, VertexProperties};
use models::queries::{Query, QueryOutputValue};

// Application-specific models, if they are used within database.rs or re-exported from here
use models::medical::{Login, User}; // Assuming these are in models::medical

// Import your custom error type
use crate::errors::Result;

// Define a type alias for a dynamic iterator, commonly used for trait objects
// and allowing different iterator implementations to be returned.
pub type DynIter<'a, T> = Box<dyn Iterator<Item = Result<T>> + 'a>;


/// Defines the core functionality required for a datastore.
///
/// Any type implementing this trait can serve as the backend for the `Database`.
pub trait Datastore {
    /// The associated transaction type for this datastore.
    ///
    /// This associated type allows the `Datastore` trait to define methods
    /// that return a specific transaction type, which in turn implements
    /// the `Transaction` trait. The `'a` lifetime parameter indicates that
    /// the transaction's lifetime is tied to the lifetime of the `Datastore`
    /// instance from which it was created.
    type Transaction<'a>
    where
        Self: 'a; // Self must outlive 'a, or 'a is tied to Self's lifetime

    /// Begins a new transaction for this datastore.
    ///
    /// The returned transaction can be used for both read and write operations.
    fn transaction(&'_ self) -> Self::Transaction<'_>;
}


/// Defines the operations that can be performed within a database transaction.
///
/// This trait outlines the read and write methods for interacting with the graph data.
pub trait Transaction<'a>: Sized {
    // Graph Read Operations

    /// Returns the total number of vertices in the database.
    fn vertex_count(&self) -> u64;

    /// Returns an iterator over all vertices in the database.
    fn all_vertices(&'a self) -> Result<DynIter<'a, Vertex>>;

    /// Returns an iterator over vertices starting from a given offset UUID.
    /// Useful for pagination or resuming iteration.
    fn range_vertices(&'a self, offset: Uuid) -> Result<DynIter<'a, Vertex>>;

    /// Returns an iterator over specific vertices identified by their UUIDs.
    fn specific_vertices(&'a self, ids: Vec<Uuid>) -> Result<DynIter<'a, Vertex>>;

    /// Returns an optional iterator of vertex UUIDs that have a property with the given name.
    /// Returns `None` if the property is not indexed.
    fn vertex_ids_with_property(&'a self, name: Identifier) -> Result<Option<DynIter<'a, Uuid>>>;

    /// Returns an optional iterator of vertex UUIDs that have a property with the given name
    /// and a specific JSON value. Returns `None` if the property is not indexed.
    fn vertex_ids_with_property_value(&'a self, name: Identifier, value: &Json) -> Result<Option<DynIter<'a, Uuid>>>;

    /// Returns the total number of edges in the database.
    fn edge_count(&self) -> u64;

    /// Returns an iterator over all edges in the database.
    fn all_edges(&'a self) -> Result<DynIter<'a, Edge>>;

    /// Returns an iterator over edges starting from a given offset Edge.
    /// Useful for pagination or resuming iteration.
    fn range_edges(&'a self, offset: Edge) -> Result<DynIter<'a, Edge>>;

    /// Returns an iterator over reversed edges (inbound to outbound) starting from a given offset Edge.
    fn range_reversed_edges(&'a self, offset: Edge) -> Result<DynIter<'a, Edge>>;

    /// Returns an iterator over specific edges.
    fn specific_edges(&'a self, edges: Vec<Edge>) -> Result<DynIter<'a, Edge>>;

    /// Returns an optional iterator of edges that have a property with the given name.
    /// Returns `None` if the property is not indexed.
    fn edges_with_property(&'a self, name: Identifier) -> Result<Option<DynIter<'a, Edge>>>;

    /// Returns an optional iterator of edges that have a property with the given name
    /// and a specific JSON value. Returns `None` if the property is not indexed.
    fn edges_with_property_value(&'a self, name: Identifier, value: &Json) -> Result<Option<DynIter<'a, Edge>>>;

    /// Retrieves the value of a specific property for a given vertex.
    fn vertex_property(&self, vertex: &Vertex, name: Identifier) -> Result<Option<Json>>;

    /// Returns an iterator over all properties (name and value) for a given vertex.
    fn all_vertex_properties_for_vertex(&'a self, vertex: &Vertex) -> Result<DynIter<'a, (Identifier, Json)>>;

    /// Retrieves the value of a specific property for a given edge.
    fn edge_property(&self, edge: &Edge, name: Identifier) -> Result<Option<Json>>;

    /// Returns an iterator over all properties (name and value) for a given edge.
    fn all_edge_properties_for_edge(&'a self, edge: &Edge) -> Result<DynIter<'a, (Identifier, Json)>>;

    // Graph Write Operations

    /// Deletes a collection of vertices and all their associated edges and properties.
    fn delete_vertices(&mut self, vertices: Vec<Vertex>) -> Result<()>;

    /// Deletes a collection of edges and their associated properties.
    fn delete_edges(&mut self, edges: Vec<Edge>) -> Result<()>;

    /// Deletes specific properties from vertices.
    fn delete_vertex_properties(&mut self, props: Vec<(Uuid, Identifier)>) -> Result<()>;

    /// Deletes specific properties from edges.
    fn delete_edge_properties(&mut self, props: Vec<(Edge, Identifier)>) -> Result<()>;

    /// Synchronizes the datastore's internal state with its persistent storage.
    /// This might involve flushing buffers or performing compaction.
    fn sync(&self) -> Result<()>;

    /// Creates a new vertex in the database. Returns `true` if created, `false` if it already existed.
    fn create_vertex(&mut self, vertex: &Vertex) -> Result<bool>;

    /// Creates a new edge in the database. Returns `true` if created, `false` if it already existed
    /// or if the incident vertices do not exist.
    fn create_edge(&mut self, edge: &Edge) -> Result<bool>;

    /// Performs a bulk insert of various graph items (vertices, edges, properties).
    /// This method is typically optimized for performance by batching operations.
    fn bulk_insert(&mut self, items: Vec<BulkInsertItem>) -> Result<()>;

    /// Indexes a property by its name. After indexing, queries on this property
    /// will be more efficient. This operation may take time for existing data.
    fn index_property(&mut self, name: Identifier) -> Result<()>;

    /// Sets (creates or updates) a specific property for a collection of vertices.
    fn set_vertex_properties(&mut self, vertices: Vec<Uuid>, name: Identifier, value: &Json) -> Result<()>;

    /// Sets (creates or updates) a specific property for a collection of edges.
    fn set_edge_properties(&mut self, edges: Vec<Edge>, name: Identifier, value: &Json) -> Result<()>;
}


/// A graph database wrapper that provides a simplified interface for
/// interacting with an underlying `Datastore`.
///
/// This struct manages the datastore instance and provides convenient methods
/// to obtain read and write transactions.
#[derive(Debug)]
pub struct Database<D: Datastore> {
    datastore: D,
}

impl<D: Datastore> Database<D> {
    /// Creates a new database instance.
    ///
    /// # Arguments
    /// * `datastore`: The underlying datastore implementation that will store the data.
    pub fn new(datastore: D) -> Self {
        Self { datastore }
    }

    /// Gets a read transaction for performing read operations on the database.
    ///
    /// The transaction's lifetime is tied to the `Database` instance.
    pub fn read_txn(&self) -> D::Transaction<'_> {
        self.datastore.transaction()
    }

    /// Gets a write transaction for performing write operations on the database.
    ///
    /// The transaction is typically committed automatically when it goes out of scope (dropped),
    /// or it might provide explicit commit/rollback methods depending on the `Transaction`
    /// trait implementation.
    pub fn write_txn(&mut self) -> D::Transaction<'_> {
        self.datastore.transaction()
    }
}

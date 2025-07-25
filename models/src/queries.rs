// models/src/queries.rs
use std::str::FromStr;
use crate::{errors, identifiers::Identifier, json::Json};
use uuid::Uuid;

// Assuming these imports are correct based on your project structure.
// These types need to implement `Eq` and `PartialEq` for QueryOutputValue to derive them.
use crate::properties::{VertexProperties, EdgeProperties};
use crate::edges::Edge;
use crate::vertices::Vertex;

// --- DEFINITION OF QueryOutputValue ---
// This enum MUST have all its contained types implement `Eq` and `PartialEq`
// for QueryOutputValue to derive these traits.
#[derive(PartialEq, Clone, Debug)]
pub enum QueryOutputValue {
    Vertices(Vec<Vertex>),
    Edges(Vec<Edge>),
    Count(u64),
    // For these to derive Eq/PartialEq, VertexProperties and EdgeProperties
    // themselves MUST implement Eq/PartialEq. This fix is in models/src/properties.rs
    VertexProperties(Vec<VertexProperties>),
    EdgeProperties(Vec<EdgeProperties>),
    // Add other variants as needed by your application
}
// --- END DEFINITION OF QueryOutputValue ---


macro_rules! into_query {
    ($name:ident, $variant:ident) => {
        // we don't want to impl From since the reverse operation isn't allowed
        #[allow(clippy::from_over_into)]
        impl Into<Query> for $name {
            fn into(self) -> Query {
                Query::$variant(self)
            }
        }
    };
}

macro_rules! nestable_query {
    ($name:ident, $variant:ident) => {
        impl QueryExt for $name {}
        impl CountQueryExt for $name {}
        into_query!($name, $variant);
    };
}

/// Specifies what kind of items should be piped from one type of query to
/// another.
///
/// Edge and vertex queries can build off of one another via pipes - e.g. you
/// can get the outbound edges of a set of vertices by piping from a vertex
/// query to an edge query. `EdgeDirection`s are used to specify which
/// end of things you want to pipe - either the outbound items or the inbound
/// items.
#[derive(Eq, PartialEq, Clone, Debug, Hash, Copy)]
pub enum EdgeDirection {
    /// Outbound direction.
    Outbound,
    /// Inbound direction.
    Inbound,
}

impl FromStr for EdgeDirection {
    type Err = errors::ValidationError;

    fn from_str(s: &str) -> Result<EdgeDirection, Self::Err> {
        match s {
            "outbound" => Ok(EdgeDirection::Outbound),
            "inbound" => Ok(EdgeDirection::Inbound),
            _ => Err(errors::ValidationError::InvalidValue),
        }
    }
}

impl From<EdgeDirection> for String {
    fn from(d: EdgeDirection) -> Self {
        match d {
            EdgeDirection::Outbound => "outbound".to_string(),
            EdgeDirection::Inbound => "inbound".to_string(),
        }
    }
}

/// A query to get a set of values from the database.
#[derive(Eq, PartialEq, Clone, Debug)]
pub enum Query {
    /// Gets all vertices.
    AllVertex,
    /// Gets a range of vertices.
    RangeVertex(RangeVertexQuery),
    /// Gets a specific set of vertices.
    SpecificVertex(SpecificVertexQuery),
    /// Gets vertices with or without a given property.
    VertexWithPropertyPresence(VertexWithPropertyPresenceQuery),
    /// Gets vertices with a property equal to a given value.
    VertexWithPropertyValue(VertexWithPropertyValueQuery),

    /// Gets all edges.
    AllEdge,
    /// Gets a specific set of edges.
    SpecificEdge(SpecificEdgeQuery),
    /// Gets edges with or without a given property.
    EdgeWithPropertyPresence(EdgeWithPropertyPresenceQuery),
    /// Gets edges with a property equal to a given value.
    EdgeWithPropertyValue(EdgeWithPropertyValueQuery),

    /// Gets the vertices associated with edges, or edges associated with
    /// vertices.
    Pipe(PipeQuery),
    /// Returns the properties associated with a vertex or edge.
    PipeProperty(PipePropertyQuery),
    /// Gets vertices or edges with or without a property.
    PipeWithPropertyPresence(PipeWithPropertyPresenceQuery),
    /// Gets vertices or edges with a property equal to a given value.
    PipeWithPropertyValue(PipeWithPropertyValueQuery),

    /// Includes the results of a query in output.
    Include(IncludeQuery),
    /// Counts the number of items returned from a query.
    Count(CountQuery),
}

impl Query {
    /// Determines the number of output values the query will produce without
    /// running it, so we can allocate a `Vec` with the correct capacity
    /// ahead-of-time.
    pub(crate) fn output_len(&self) -> usize {
        match self {
            Query::AllVertex
            | Query::RangeVertex(_)
            | Query::SpecificVertex(_)
            | Query::VertexWithPropertyPresence(_)
            | Query::VertexWithPropertyValue(_)
            | Query::AllEdge
            | Query::SpecificEdge(_)
            | Query::EdgeWithPropertyPresence(_)
            | Query::EdgeWithPropertyValue(_)
            | Query::Count(_) => 1,
            Query::Pipe(q) => q.inner.output_len(),
            Query::PipeProperty(q) => q.inner.output_len(),
            Query::PipeWithPropertyPresence(q) => q.inner.output_len(),
            Query::PipeWithPropertyValue(q) => q.inner.output_len(),
            Query::Include(q) => 1 + q.inner.output_len(),
        }
    }

    /// Determines the `QueryOutputValue` variant that will be produced
    /// without running the query, which can help validate the query
    /// ahead-of-time.
    pub(crate) fn output_type(&self) -> errors::ValidationResult<QueryOutputValue> {
        match self {
            Query::AllVertex
            | Query::RangeVertex(_)
            | Query::SpecificVertex(_)
            | Query::VertexWithPropertyPresence(_)
            | Query::VertexWithPropertyValue(_) => Ok(QueryOutputValue::Vertices(Vec::default())),
            Query::AllEdge
            | Query::SpecificEdge(_)
            | Query::EdgeWithPropertyPresence(_)
            | Query::EdgeWithPropertyValue(_) => Ok(QueryOutputValue::Edges(Vec::default())),
            Query::Count(_) => Ok(QueryOutputValue::Count(0)),
            Query::Pipe(q) => q.inner.output_type(),
            Query::PipeProperty(q) => match q.inner.output_type()? {
                QueryOutputValue::Vertices(_) => Ok(QueryOutputValue::VertexProperties(Vec::default())),
                QueryOutputValue::Edges(_) => Ok(QueryOutputValue::EdgeProperties(Vec::default())),
                _ => Err(errors::ValidationError::InnerQuery),
            },
            Query::PipeWithPropertyPresence(q) => q.inner.output_type(),
            Query::PipeWithPropertyValue(q) => q.inner.output_type(),
            Query::Include(q) => q.inner.output_type(),
        }
    }
}

/// Extension trait containing common functions for all query structs.
pub trait QueryExt: Into<Query> {
    /// Gets the outbound vertices or edges associated with this query.
    fn outbound(self) -> errors::ValidationResult<PipeQuery> {
        PipeQuery::new(Box::new(self.into()), EdgeDirection::Outbound)
    }

    /// Gets the inbound vertices or edges associated with this query.
    fn inbound(self) -> errors::ValidationResult<PipeQuery> {
        PipeQuery::new(Box::new(self.into()), EdgeDirection::Inbound)
    }

    /// Gets values with a property.
    ///
    /// # Arguments
    /// * `name`: The name of the property.
    fn with_property<T: Into<Identifier>>(self, name: T) -> errors::ValidationResult<PipeWithPropertyPresenceQuery> {
        PipeWithPropertyPresenceQuery::new(Box::new(self.into()), name, true)
    }

    /// Gets values without a property.
    ///
    /// # Arguments
    /// * `name`: The name of the property.
    fn without_property<T: Into<Identifier>>(self, name: T) -> errors::ValidationResult<PipeWithPropertyPresenceQuery> {
        PipeWithPropertyPresenceQuery::new(Box::new(self.into()), name, false)
    }

    /// Gets values with a property equal to a given value.
    ///
    /// # Arguments
    /// * `name`: The name of the property.
    /// * `value`: The value of the property.
    fn with_property_equal_to<T: Into<Identifier>>(
        self,
        name: T,
        value: Json,
    ) -> errors::ValidationResult<PipeWithPropertyValueQuery> {
        PipeWithPropertyValueQuery::new(Box::new(self.into()), name, value, true)
    }

    /// Gets values with a property not equal to a given value.
    ///
    /// # Arguments
    /// * `name`: The name of the property.
    /// * `value`: The value of the property.
    fn with_property_not_equal_to<T: Into<Identifier>>(
        self,
        name: T,
        value: Json,
    ) -> errors::ValidationResult<PipeWithPropertyValueQuery> {
        PipeWithPropertyValueQuery::new(Box::new(self.into()), name, value, false)
    }

    /// Gets the properties associated with the query results.
    fn properties(self) -> errors::ValidationResult<PipePropertyQuery> {
        PipePropertyQuery::new(Box::new(self.into()))
    }

    /// Include this query's output, even if it is an intermediate result.
    fn include(self) -> IncludeQuery {
        IncludeQuery::new(Box::new(self.into()))
    }
}

pub trait CountQueryExt: Into<Query> {
    /// Gets the count from this query.
    fn count(self) -> errors::ValidationResult<CountQuery> {
        CountQuery::new(Box::new(self.into()))
    }
}

// --- Query Structs (Moved to be defined before usage in QueryExt) ---

/// Gets all vertices.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct AllVertexQuery;

impl QueryExt for AllVertexQuery {}
impl CountQueryExt for AllVertexQuery {}

// we don't want to impl From since the reverse operation isn't allowed
#[allow(clippy::from_over_into)]
impl Into<Query> for AllVertexQuery {
    fn into(self) -> Query {
        Query::AllVertex
    }
}

/// Gets a range of vertices.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct RangeVertexQuery {
    /// Limits the number of vertices to get.
    pub limit: u32,

    /// Filters the type of vertices returned.
    pub t: Option<Identifier>,

    /// Sets the lowest vertex ID to return.
    pub start_id: Option<Uuid>,
}

nestable_query!(RangeVertexQuery, RangeVertex);

impl Default for RangeVertexQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl RangeVertexQuery {
    /// Creates a new vertex range query.
    pub fn new() -> Self {
        Self {
            limit: u32::MAX,
            t: None,
            start_id: None,
        }
    }

    /// Sets the limit.
    ///
    /// # Arguments
    /// * `limit`: Limits the number of returned results.
    pub fn limit(self, limit: u32) -> Self {
        Self {
            limit,
            t: self.t,
            start_id: self.start_id,
        }
    }

    /// Filter the type of vertices returned.
    ///
    /// # Arguments
    /// * `t`: Sets the type filter.
    pub fn t(self, t: Identifier) -> Self {
        Self {
            limit: self.limit,
            t: Some(t),
            start_id: self.start_id,
        }
    }

    /// Sets the lowest vertex ID to return.
    ///
    /// # Arguments
    /// * `start_id`: The lowest vertex ID to return.
    pub fn start_id(self, start_id: Uuid) -> Self {
        Self {
            limit: self.limit,
            t: self.t,
            start_id: Some(start_id),
        }
    }
}

/// Gets a specific set of vertices.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct SpecificVertexQuery {
    /// The IDs of the vertices to get.
    pub ids: Vec<Uuid>,
}

nestable_query!(SpecificVertexQuery, SpecificVertex);

impl SpecificVertexQuery {
    /// Creates a new vertex query for getting a list of vertices by their
    /// IDs.
    ///
    /// Arguments
    /// * `ids`: The IDs of the vertices to get.
    pub fn new(ids: Vec<Uuid>) -> Self {
        Self { ids }
    }

    /// Creates a new vertex query for getting a single vertex.
    ///
    /// Arguments
    /// * `id`: The ID of the vertex to get.
    pub fn single(id: Uuid) -> Self {
        Self { ids: vec![id] }
    }
}

/// Gets vertices with or without a given property.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct VertexWithPropertyPresenceQuery {
    /// The name of the property.
    pub name: Identifier,
}

nestable_query!(VertexWithPropertyPresenceQuery, VertexWithPropertyPresence);

impl VertexWithPropertyPresenceQuery {
    /// Creates a new vertex with property presence query.
    ///
    /// # Arguments
    /// * `name`: The property name.
    pub fn new<T: Into<Identifier>>(name: T) -> Self {
        Self { name: name.into() }
    }
}

/// Gets vertices with a property equal to a given value.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct VertexWithPropertyValueQuery {
    /// The name of the property.
    pub name: Identifier,
    /// The value of the property.
    pub value: Json,
}

nestable_query!(VertexWithPropertyValueQuery, VertexWithPropertyValue);

impl VertexWithPropertyValueQuery {
    /// Creates a new vertex with property value query.
    ///
    /// # Arguments
    /// * `name`: The property name.
    /// * `value`: The property value.
    pub fn new<T: Into<Identifier>>(name: T, value: Json) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// Gets all edges.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct AllEdgeQuery;

impl QueryExt for AllEdgeQuery {}
impl CountQueryExt for AllEdgeQuery {}

// we don't want to impl From since the reverse operation isn't allowed
#[allow(clippy::from_over_into)]
impl Into<Query> for AllEdgeQuery {
    fn into(self) -> Query {
        Query::AllEdge
    }
}

/// Gets a specific set of edges.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct SpecificEdgeQuery {
    /// The edges to get.
    pub edges: Vec<Edge>,
}

nestable_query!(SpecificEdgeQuery, SpecificEdge);

impl SpecificEdgeQuery {
    /// Creates a new edge query for getting a list of specific edges.
    ///
    /// Arguments
    /// * `edges`: The edges to get.
    pub fn new(edges: Vec<Edge>) -> Self {
        Self { edges }
    }

    /// Creates a new edge query for getting a single edge.
    ///
    /// Arguments
    /// * `edge`: The edge to get.
    pub fn single(edge: Edge) -> Self {
        Self { edges: vec![edge] }
    }
}

/// Gets edges with or without a given property.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct EdgeWithPropertyPresenceQuery {
    /// The name of the property.
    pub name: Identifier,
}

nestable_query!(EdgeWithPropertyPresenceQuery, EdgeWithPropertyPresence);

impl EdgeWithPropertyPresenceQuery {
    /// Creates a new edge with property presence query.
    ///
    /// # Arguments
    /// * `name`: The property name.
    pub fn new<T: Into<Identifier>>(name: T) -> Self {
        Self { name: name.into() }
    }
}

/// Gets edges with a property equal to a given value.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct EdgeWithPropertyValueQuery {
    /// The name of the property.
    pub name: Identifier,
    /// The value of the property.
    pub value: Json,
}

nestable_query!(EdgeWithPropertyValueQuery, EdgeWithPropertyValue);

impl EdgeWithPropertyValueQuery {
    /// Creates a new edge with property value query.
    ///
    /// # Arguments
    /// * `name`: The property name.
    /// * `value`: The property value.
    pub fn new<T: Into<Identifier>>(name: T, value: Json) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

// --- Pipe and other complex queries ---
// These need to be defined before QueryExt if they are being used within its methods
// as constructors. Placing them here ensures they are declared before they are called.

/// Gets the vertices associated with edges, or edges associated with vertices.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct PipeQuery {
    /// The inner query.
    pub inner: Box<Query>,
    /// The direction of the pipe.
    pub direction: EdgeDirection,
}

into_query!(PipeQuery, Pipe);

impl PipeQuery {
    /// Creates a new pipe query.
    ///
    /// # Arguments
    /// * `inner`: The inner query.
    /// * `direction`: The direction of the pipe.
    pub fn new(inner: Box<Query>, direction: EdgeDirection) -> errors::ValidationResult<Self> {
        let output_type = inner.output_type()?;
        match (output_type, direction) {
            (QueryOutputValue::Vertices(_), _) => Ok(Self { inner, direction }),
            (QueryOutputValue::Edges(_), _) => Ok(Self { inner, direction }),
            _ => Err(errors::ValidationError::InnerQuery),
        }
    }
}

/// Returns the properties associated with a vertex or edge.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct PipePropertyQuery {
    /// The inner query.
    pub inner: Box<Query>,
}

into_query!(PipePropertyQuery, PipeProperty);

impl PipePropertyQuery {
    /// Creates a new pipe property query.
    ///
    /// # Arguments
    /// * `inner`: The inner query.
    pub fn new(inner: Box<Query>) -> errors::ValidationResult<Self> {
        let output_type = inner.output_type()?;
        match output_type {
            QueryOutputValue::Vertices(_) => Ok(Self { inner }),
            QueryOutputValue::Edges(_) => Ok(Self { inner }),
            _ => Err(errors::ValidationError::InnerQuery),
        }
    }
}

/// Gets vertices or edges with or without a property.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct PipeWithPropertyPresenceQuery {
    /// The inner query.
    pub inner: Box<Query>,
    /// The name of the property.
    pub name: Identifier,
    /// Whether the property should be present.
    pub present: bool,
}

into_query!(PipeWithPropertyPresenceQuery, PipeWithPropertyPresence);

impl PipeWithPropertyPresenceQuery {
    /// Creates a new pipe with property presence query.
    ///
    /// # Arguments
    /// * `inner`: The inner query.
    /// * `name`: The name of the property.
    /// * `present`: Whether the property should be present.
    pub fn new<T: Into<Identifier>>(
        inner: Box<Query>,
        name: T,
        present: bool,
    ) -> errors::ValidationResult<Self> {
        let output_type = inner.output_type()?;
        match output_type {
            QueryOutputValue::Vertices(_) => Ok(Self {
                inner,
                name: name.into(),
                present,
            }),
            QueryOutputValue::Edges(_) => Ok(Self {
                inner,
                name: name.into(),
                present,
            }),
            _ => Err(errors::ValidationError::InnerQuery),
        }
    }
}

/// Gets vertices or edges with a property equal to a given value.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct PipeWithPropertyValueQuery {
    /// The inner query.
    pub inner: Box<Query>,
    /// The name of the property.
    pub name: Identifier,
    /// The value of the property.
    pub value: Json,
    /// Whether the property value should be equal.
    pub equal: bool,
}

into_query!(PipeWithPropertyValueQuery, PipeWithPropertyValue);

impl PipeWithPropertyValueQuery {
    /// Creates a new pipe with property value query.
    ///
    /// # Arguments
    /// * `inner`: The inner query.
    /// * `name`: The name of the property.
    /// * `value`: The property value.
    /// * `equal`: Whether the property value should be equal.
    pub fn new<T: Into<Identifier>>(
        inner: Box<Query>,
        name: T,
        value: Json,
        equal: bool,
    ) -> errors::ValidationResult<Self> {
        let output_type = inner.output_type()?;
        match output_type {
            QueryOutputValue::Vertices(_) => Ok(Self {
                inner,
                name: name.into(),
                value,
                equal,
            }),
            QueryOutputValue::Edges(_) => Ok(Self {
                inner,
                name: name.into(),
                value,
                equal,
            }),
            _ => Err(errors::ValidationError::InnerQuery),
        }
    }
}

/// Includes the results of a query in output.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct IncludeQuery {
    /// The inner query.
    pub inner: Box<Query>,
}

into_query!(IncludeQuery, Include);

impl IncludeQuery {
    /// Creates a new include query.
    ///
    /// # Arguments
    /// * `inner`: The inner query.
    pub fn new(inner: Box<Query>) -> Self {
        Self { inner }
    }
}

/// Counts the number of items returned from a query.
#[derive(Eq, PartialEq, Clone, Debug)]
pub struct CountQuery {
    /// The inner query.
    pub inner: Box<Query>,
}

into_query!(CountQuery, Count);

impl CountQuery {
    /// Creates a new count query.
    ///
    /// # Arguments
    /// * `inner`: The inner query.
    pub fn new(inner: Box<Query>) -> errors::ValidationResult<Self> {
        // Count can operate on any query that produces a collection of items
        let _ = inner.output_type()?; // Ensure inner query produces a valid output type
        Ok(Self { inner })
    }
}

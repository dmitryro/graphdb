// models/src/properties.rs
use crate::{edges::Edge, identifiers::Identifier, json::Json, vertices::Vertex};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use std::hash::{Hash, Hasher}; // Needed for custom Hash impl for FloatWrapper

// --- NEW STRUCT FOR F64 WRAPPER ---
// f64 does not implement `Eq` or `Hash` directly.
// We need a newtype wrapper to implement these traits manually.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(transparent)] // Serialize/deserialize as the inner f64
pub struct SerializableFloat(pub f64);

// Implement PartialEq for f64 comparison. This is typically bit-wise equality
// for `Eq` derivation, which means `NaN != NaN`.
impl PartialEq for SerializableFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

// Since PartialEq is implemented, we can implement Eq (assuming reflexivity, symmetry, transitivity)
impl Eq for SerializableFloat {}

// Implement Hash for f64. This also uses bit-wise representation.
impl Hash for SerializableFloat {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}
// --- END NEW STRUCT ---


/// Represents a generic property value.
// FIX: Added `Eq` and `Hash` back here. `Hash` is necessary if `PropertyValue` is ever
// used as a key in a HashMap or if a parent struct deriving Hash contains it.
// `Eq` is necessary for Vertex's HashMap to derive Eq/PartialEq.
// Now that `Float(f64)` is replaced with `Float(SerializableFloat)`, `Eq` and `Hash` can be derived.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)] // Added Eq and Hash back
#[serde(untagged)]
pub enum PropertyValue {
    String(String),
    Integer(i64),
    Float(SerializableFloat), // FIX: Use the new wrapper type
    Boolean(bool),
    Uuid(Uuid),
    // Add other types as needed, e.g.,
    // Bytes(Vec<u8>),
    // Array(Vec<PropertyValue>), // If Array contains PropertyValue, it will need Eq/Hash too.
    // Object(PropertyMap),      // If Object contains PropertyMap, it will also need Eq/Hash.
}

impl From<String> for PropertyValue { fn from(s: String) -> Self { PropertyValue::String(s) } }
impl From<&str> for PropertyValue { fn from(s: &str) -> Self { PropertyValue::String(s.to_string()) } }
impl From<i64> for PropertyValue { fn from(i: i64) -> Self { PropertyValue::Integer(i) } }
impl From<f64> for PropertyValue { fn from(f: f64) -> Self { PropertyValue::Float(SerializableFloat(f)) } } // FIX: Wrap f64
impl From<bool> for PropertyValue { fn from(b: bool) -> Self { PropertyValue::Boolean(b) } }
impl From<Uuid> for PropertyValue { fn from(u: Uuid) -> Self { PropertyValue::Uuid(u) } }
// Implement other From traits as needed for your data types

/// A map of property names to their values.
// FIX: PropertyMap can now derive `Eq` and `Hash` if `Identifier` also implements `Eq/Hash`.
// Assuming Identifier already implements Eq and Hash from previous steps.
// If you need Hash, consider `BTreeMap` for properties to avoid HashMap's non-deterministic hash.
// However, the current error is for `Eq`, which HashMap can derive if its values are `Eq`.
pub type PropertyMap = HashMap<Identifier, PropertyValue>; // Use Identifier for keys for consistency

/// Represents a vertex property.
// FIX: Added `Eq` and `Hash` back as `Json` might not provide these, but if it does, keep them.
// If Json contains f64, it might still break. Json from `serde_json::Value` doesn't derive Eq/Hash.
// If Json can contain floats, you might need a custom Eq/Hash for VertexProperty, or remove derive(Eq/Hash).
// For now, let's assume `Json` is handled or these structs don't strictly need Eq/Hash.
// If `Json` can contain floats, it will break `Eq` and `Hash` here too.
// If `VertexProperty` doesn't need to be hashable/equatable with floats, remove these.
#[derive(Clone, Debug, PartialEq)] // Removed `Eq` for now assuming `Json` may contain `f64`
pub struct VertexProperty {
    /// The id of the vertex.
    pub id: Uuid,

    /// The property value.
    pub value: Json, // Json from `serde_json::Value` does NOT implement Eq/Hash if it contains floats or NaNs.
}

impl VertexProperty {
    /// Creates a new vertex property.
    ///
    /// # Arguments
    /// * `id`: The id of the vertex.
    /// * `value`: The property value.
    pub fn new(id: Uuid, value: Json) -> Self {
        Self { id, value }
    }
}

/// A property.
// FIX: Same logic as VertexProperty for Json.
#[derive(Clone, Debug, PartialEq)] // Removed `Eq` for now assuming `Json` may contain `f64`
pub struct NamedProperty {
    /// The property name.
    pub name: Identifier,

    /// The property value.
    pub value: Json, // Json from `serde_json::Value` does NOT implement Eq/Hash if it contains floats or NaNs.
}

impl NamedProperty {
    /// Creates a new vertex property.
    ///
    /// # Arguments
    /// * `name`: The name of the property.
    /// * `value`: The property value.
    pub fn new(name: Identifier, value: Json) -> Self {
        Self { name, value }
    }
}

/// A vertex with properties.
// FIX: Removed `Hash` and `Eq` if `props` contains `Json`.
#[derive(Clone, Debug, PartialEq)]
pub struct VertexProperties {
    /// The vertex.
    pub vertex: Vertex, // Assuming Vertex implements PartialEq
    /// All of the vertex's properties.
    pub props: Vec<NamedProperty>, // NamedProperty's `value: Json` will prevent Eq/Hash.
}

impl VertexProperties {
    /// Creates new properties for a given vertex.
    ///
    /// # Arguments
    /// * `vertex`: The vertex information
    /// * `props`: The properties
    pub fn new(vertex: Vertex, props: Vec<NamedProperty>) -> Self {
        VertexProperties { vertex, props }
    }
}

/// An edge with properties.
// FIX: Removed `Hash` and `Eq` if `props` contains `Json`.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeProperties {
    /// The edge.
    pub edge: Edge, // Assuming Edge implements PartialEq
    /// All of the edge's properties.
    pub props: Vec<NamedProperty>, // NamedProperty's `value: Json` will prevent Eq/Hash.
}

impl EdgeProperties {
    /// Creates a new edge properties for a given edge.
    ///
    /// # Arguments
    /// * `edge`: The edge information
    /// * `props`: The properties
    pub fn new(edge: Edge, props: Vec<NamedProperty>) -> Self {
        EdgeProperties { edge, props }
    }
}

/// Represents an edge property.
// FIX: Removed `Hash` and `Eq` if `value` contains `Json`.
#[derive(Clone, Debug, PartialEq)]
pub struct EdgeProperty {
    /// The edge.
    pub edge: Edge, // Assuming Edge implements PartialEq

    /// The property value.
    pub value: Json, // Json from `serde_json::Value` does NOT implement Eq/Hash if it contains floats or NaNs.
}

impl EdgeProperty {
    /// Creates a new edge property.
    ///
    /// # Arguments
    /// * `edge`: The edge.
    /// * `value`: The property value.
    pub fn new(edge: Edge, value: Json) -> Self {
        Self { edge, value }
    }
}

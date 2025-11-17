// models/src/properties.rs
use crate::{edges::Edge, identifiers::Identifier, json::Json, vertices::Vertex};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use bincode::{Encode, Decode};

/// --- NEW STRUCT FOR F64 WRAPPER ---
/// f64 does not implement `Eq` or `Hash` directly.
/// We need a newtype wrapper to implement these traits manually.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Encode, Decode)]
#[serde(transparent)]
pub struct SerializableFloat(pub f64);

impl PartialEq for SerializableFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for SerializableFloat {}

impl PartialOrd for SerializableFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SerializableFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_bits().cmp(&other.0.to_bits())
    }
}
impl std::hash::Hash for SerializableFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}
/// --- END NEW STRUCT ---

/// Represents a generic property value.
///
/// NOTE: Only types supported by bincode derive Encode/Decode.
/// `Uuid` is supported via SerializableUuid.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode)]
#[serde(untagged)]
pub enum PropertyValue {
    Boolean(bool),
    Integer(i64),
    Float(SerializableFloat),
    String(String),
    Uuid(crate::identifiers::SerializableUuid),
}

impl PropertyValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self { PropertyValue::String(s) }
}
impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self { PropertyValue::String(s.to_string()) }
}
impl From<i64> for PropertyValue {
    fn from(i: i64) -> Self { PropertyValue::Integer(i) }
}
impl From<f64> for PropertyValue {
    fn from(f: f64) -> Self { PropertyValue::Float(SerializableFloat(f)) }
}
impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self { PropertyValue::Boolean(b) }
}
impl From<Uuid> for PropertyValue {
    fn from(u: Uuid) -> Self { PropertyValue::Uuid(u.into()) }
}
impl From<crate::identifiers::SerializableUuid> for PropertyValue {
    fn from(u: crate::identifiers::SerializableUuid) -> Self { PropertyValue::Uuid(u) }
}

/// A map of property names to their values.
pub type PropertyMap = HashMap<Identifier, PropertyValue>;

/// Represents a vertex property.
/// NOT bincode-Encode/Decode as Json (serde_json::Value) is not bincode compatible.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VertexProperty {
    pub id: Uuid,
    pub value: Json,
}
impl VertexProperty {
    pub fn new(id: Uuid, value: Json) -> Self { Self { id, value } }
}

/// A named property, for both vertices and edges.
/// NOT bincode-Encode/Decode due to Json.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamedProperty {
    pub name: Identifier,
    pub value: Json,
}
impl NamedProperty {
    pub fn new(name: Identifier, value: Json) -> Self { Self { name, value } }
}

/// A vertex with properties.
/// NOT bincode-Encode/Decode due to Json in props.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VertexProperties {
    pub vertex: Vertex,
    pub props: Vec<NamedProperty>,
}
impl VertexProperties {
    pub fn new(vertex: Vertex, props: Vec<NamedProperty>) -> Self { VertexProperties { vertex, props } }
}

/// An edge with properties.
/// NOT bincode-Encode/Decode due to Json in props.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgeProperties {
    pub edge: Edge,
    pub props: Vec<NamedProperty>,
}
impl EdgeProperties {
    pub fn new(edge: Edge, props: Vec<NamedProperty>) -> Self { EdgeProperties { edge, props } }
}

/// Represents an edge property.
/// NOT bincode-Encode/Decode due to Json.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgeProperty {
    pub edge: Edge,
    pub value: Json,
}
impl EdgeProperty {
    pub fn new(edge: Edge, value: Json) -> Self { Self { edge, value } }
}

/// Represents a vertex property update event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VertexPropertyUpdate {
    pub id: Uuid,
    pub property: Identifier,
    pub value: Option<Json>,
}
impl VertexPropertyUpdate {
    pub fn new(id: Uuid, property: Identifier, value: Option<Json>) -> Self {
        Self { id, property, value }
    }
}

/// Represents an edge property update event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgePropertyUpdate {
    pub edge: Edge,
    pub property: Identifier,
    pub value: Option<Json>,
}
impl EdgePropertyUpdate {
    pub fn new(edge: Edge, property: Identifier, value: Option<Json>) -> Self {
        Self { edge, property, value }
    }
}

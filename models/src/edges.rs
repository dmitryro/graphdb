// models/src/edges.rs
use crate::identifiers::{Identifier, SerializableUuid};
use crate::properties::PropertyValue;
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode};
use std::collections::BTreeMap;
use uuid::Uuid;

/// A directed, typed edge connecting two vertices.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize, Encode, Decode)]
pub struct Edge {
    /// Auto-generated unique ID for the edge.
    pub id: SerializableUuid,

    /// Source vertex.
    pub outbound_id: SerializableUuid,

    /// Edge type (e.g., "FOLLOWS", "HAS_DIAGNOSIS").
    pub edge_type: Identifier,

    /// Target vertex.
    pub inbound_id: SerializableUuid,

    /// Human-readable label (optional, defaults to `t` as string).
    pub label: String,

    /// Edge properties (ordered for deterministic serialization).
    pub properties: BTreeMap<String, PropertyValue>,
}

impl Edge {
    /// Create a new edge with an auto-generated `id`.
    ///
    /// # Arguments
    /// * `outbound_id` – source vertex
    /// * `edge_type`           – edge type (`Identifier`)
    /// * `inbound_id`  – target vertex
    ///
    /// The `label` defaults to the string representation of `edge_type`.
    pub fn new(
        outbound_id: impl Into<SerializableUuid>,
        edge_type: Identifier,
        inbound_id: impl Into<SerializableUuid>,
    ) -> Self {
        let outbound_id = outbound_id.into();
        let inbound_id = inbound_id.into();
        let label = edge_type.to_string();

        Self {
            id: SerializableUuid(Uuid::new_v4()),
            outbound_id,
            edge_type,
            inbound_id,
            label,
            properties: BTreeMap::new(),
        }
    }

    /// Add or update a property using a builder pattern.
    ///
    /// ```rust
    /// let edge = Edge::new(alice, Identifier::new("KNOWS")?, bob)
    ///     .with_property("since", 2020i64.into())
    ///     .with_property("strength", 0.8f64.into());
    /// ```
    pub fn with_property(mut self, key: impl Into<String>, value: PropertyValue) -> Self {
        self.properties.insert(key.into(), value);
        self
    }

    /// Reverse the direction of the edge.
    ///
    /// Preserves `id`, `t`, `label`, and `properties`.
    pub fn reversed(&self) -> Self {
        Self {
            id: self.id,
            outbound_id: self.inbound_id,
            edge_type: self.edge_type.clone(),
            inbound_id: self.outbound_id,
            label: self.label.clone(),
            properties: self.properties.clone(),
        }
    }
}

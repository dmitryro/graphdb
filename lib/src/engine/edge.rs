use std::collections::BTreeMap;
use uuid::Uuid;
use crate::engine::properties::PropertyValue;
use crate::models::identifiers::Identifier; // Ensure this is imported

/// A directed, typed edge connecting two vertices.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Edge {
    pub id: Uuid,
    pub from: Uuid,
    pub to: Uuid,
    pub label: String,
    pub t: Identifier,
    pub properties: BTreeMap<String, PropertyValue>,
}

impl Edge {
    pub fn new(from: Uuid, to: Uuid, label: impl Into<String>, t: Identifier) -> Self {
        Edge {
            id: Uuid::new_v4(),
            from,
            to,
            label: label.into(),
            t,
            properties: BTreeMap::new(),
        }
    }

    pub fn with_property(mut self, key: impl Into<String>, value: PropertyValue) -> Self {
        self.properties.insert(key.into(), value);
        self
    }
}


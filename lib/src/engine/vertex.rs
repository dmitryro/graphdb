// lib/src/engine/vertex.rs
use uuid::Uuid;
use std::collections::BTreeMap;
// FIX: Change to use PropertyValue from the 'models' crate
use models::properties::PropertyValue;

/// A vertex represents an entity in the graph (e.g., a patient, medication, doctor).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Vertex {
    pub id: Uuid,
    pub label: String,  // e.g., "Patient", "Doctor", "Medication"
    pub properties: BTreeMap<String, PropertyValue>, // This type now matches models::properties::PropertyValue
}

impl Vertex {
    /// Create a new vertex with the given label and auto-generated UUID.
    pub fn new(label: impl Into<String>) -> Self {
        Vertex {
            id: Uuid::new_v4(),
            label: label.into(),
            properties: BTreeMap::new(),
        }
    }

    /// Create a new vertex with an explicit UUID (used in deserialization or imports).
    pub fn with_id(id: Uuid, label: impl Into<String>) -> Self {
        Vertex {
            id,
            label: label.into(),
            properties: BTreeMap::new(),
        }
    }

    /// Add or update a property on this vertex.
    pub fn with_property(mut self, key: impl Into<String>, value: PropertyValue) -> Self {
        self.properties.insert(key.into(), value);
        self
    }

    /// Retrieve a property by key.
    pub fn get_property(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }

    /// Remove a property by key.
    pub fn remove_property(&mut self, key: &str) -> Option<PropertyValue> {
        self.properties.remove(key)
    }
}

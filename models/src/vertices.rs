// models/src/vertices.rs
use core::hash::Hash;
use std::{cmp::Ordering, collections::HashMap}; // Assuming HashMap for properties

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    identifiers::Identifier,
    // FIX: Removed 'Property' as it doesn't exist in properties.rs
    properties::PropertyValue,
    errors::GraphError,
};

/// A vertex.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Vertex {
    /// The id of the vertex.
    pub id: Uuid,

    /// The label of the vertex (e.g., "Person", "Address").
    pub label: Identifier,

    /// The properties of the vertex.
    pub properties: HashMap<String, PropertyValue>,
}

impl Vertex {
    /// Creates a new vertex.
    pub fn new(label: Identifier) -> Self {
        Vertex {
            id: Uuid::new_v4(),
            label,
            properties: HashMap::new(),
        }
    }

    /// Creates a new vertex with a specified ID.
    pub fn new_with_id(id: Uuid, label: Identifier) -> Self {
        Vertex {
            id,
            label,
            properties: HashMap::new(),
        }
    }

    /// Returns a reference to the vertex's label.
    pub fn label(&self) -> &Identifier {
        &self.label
    }

    /// Returns a reference to the vertex's ID.
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Adds a property to the vertex.
    ///
    /// # Arguments
    /// * `key`: The key of the property.
    /// * `value`: The value of the property.
    pub fn add_property(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), PropertyValue::String(value.to_string()));
    }

    /// Gets a property value by key.
    ///
    /// Returns `Some(&str)` if the property exists and is a string, otherwise `None`.
    pub fn get_property(&self, key: &str) -> Option<&str> {
        self.properties.get(key)
            .and_then(|prop_val| {
                match prop_val {
                    PropertyValue::String(s) => Some(s.as_str()),
                    _ => None,
                }
            })
    }
}

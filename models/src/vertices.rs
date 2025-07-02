use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode};

use crate::{
    identifiers::{Identifier, SerializableUuid},
    properties::PropertyValue,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct Vertex {
    pub id: SerializableUuid,
    pub label: Identifier,
    pub properties: HashMap<String, PropertyValue>,
}

impl Vertex {
    pub fn new(label: Identifier) -> Self {
        use uuid::Uuid;
        Vertex {
            id: SerializableUuid(Uuid::new_v4()),
            label,
            properties: HashMap::new(),
        }
    }

    pub fn new_with_id(id: impl Into<SerializableUuid>, label: Identifier) -> Self {
        Vertex {
            id: id.into(),
            label,
            properties: HashMap::new(),
        }
    }

    pub fn label(&self) -> &Identifier {
        &self.label
    }

    pub fn id(&self) -> &SerializableUuid {
        &self.id
    }

    pub fn add_property(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), PropertyValue::String(value.to_string()));
    }

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

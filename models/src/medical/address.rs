use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::{
    edges::Edge, // Keep if edges are used for relationships involving Address
    identifiers::Identifier,
    properties::{EdgeProperties, PropertyMap, VertexProperties}, // PropertyMap needed for Vertex struct
    vertices::Vertex, // Import Vertex
};
use chrono::{DateTime, Utc}; // Keep DateTime, Utc as they are good general utilities for timestamps.

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Address {
    pub id: Uuid,
    pub address_line1: String,
    pub address_line2: Option<String>,
    pub city: String,
    pub state_province: Identifier, // This will be a vertex (StateProvince)
    pub postal_code: String,
    pub country: String,
}

impl Address {
    pub fn new(
        address_line1: String,
        address_line2: Option<String>,
        city: String,
        state_province: Identifier,
        postal_code: String,
        country: String,
    ) -> Self {
        Address {
            id: Uuid::new_v4(), // Generate a new UUID for the Address
            address_line1,
            address_line2,
            city,
            state_province,
            postal_code,
            country,
        }
    }

    pub fn to_vertex(&self) -> Vertex {
        // Identifier::new expects String, which is already correctly handled here.
        let id_type = Identifier::new("Address".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        // Vertex::add_property expects `&str` for both key and value.
        // Use `.to_string()` on Uuid and Identifier, then `.as_str()` or `&` directly.
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("address_line1", &self.address_line1);
        if let Some(ref addr2) = self.address_line2 {
            vertex.add_property("address_line2", addr2); // `&String` dereferences to `&str`
        }
        vertex.add_property("city", &self.city);
        vertex.add_property("state_province_id", self.state_province.as_ref()); // Identifier already derefs to &str via AsRef<str>
        vertex.add_property("postal_code", &self.postal_code);
        vertex.add_property("country", &self.country);
        vertex
    }

    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        // The `label()` method on Vertex returns `&Identifier`. `as_ref()` on `&Identifier` gives `&str`.
        if vertex.label().as_ref() != "Address" {
            return None;
        }

        // The id field is of type SerializableUuid, convert to Uuid.
        let id = vertex.id.clone().into();

        // get_property returns Option<&str>.
        let address_line1 = vertex.get_property("address_line1")?.to_string();
        let address_line2 = vertex.get_property("address_line2").map(|s| s.to_string());
        let city = vertex.get_property("city")?.to_string();

        let state_province_str = vertex.get_property("state_province_id")?.to_string();
        let state_province = Identifier::new(state_province_str).ok()?;

        let postal_code = vertex.get_property("postal_code")?.to_string();
        let country = vertex.get_property("country")?.to_string();

        Some(Address {
            id,
            address_line1,
            address_line2,
            city,
            state_province,
            postal_code,
            country,
        })
    }
}

// lib/src/models/medical/address.rs
use crate::models::{Identifier, ToVertex, Vertex};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Address {
    pub id: i32,
    pub address1: String,
    pub address2: String,
    pub floor: Option<String>,
    pub room: Option<String>,
    pub apartment: Option<String>,
    pub address_type: String,
    pub city: String,
    pub state_province_id: i32,
    pub postal_code: Option<String>,
    pub county: Option<String>,
    pub region: Option<String>,
    pub country: String,
    // You may want to add a StateProvince struct and include it here optionally,
    // but for now just keep the ID as in your original C# model
}

impl ToVertex for Address {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("Address").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("address1", &self.address1);
        vertex.add_property("address2", &self.address2);
        if let Some(ref floor) = self.floor {
            vertex.add_property("floor", floor);
        }
        if let Some(ref room) = self.room {
            vertex.add_property("room", room);
        }
        if let Some(ref apartment) = self.apartment {
            vertex.add_property("apartment", apartment);
        }
        vertex.add_property("address_type", &self.address_type);
        vertex.add_property("city", &self.city);
        vertex.add_property("state_province_id", &self.state_province_id.to_string());
        if let Some(ref postal_code) = self.postal_code {
            vertex.add_property("postal_code", postal_code);
        }
        if let Some(ref county) = self.county {
            vertex.add_property("county", county);
        }
        if let Some(ref region) = self.region {
            vertex.add_property("region", region);
        }
        vertex.add_property("country", &self.country);

        vertex
    }
}


// lib/src/models/medical/state_province.rs
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct StateProvince {
    pub id: i32,
    pub name: String,
    pub code: String,
    pub country: Option<String>,
}

impl ToVertex for StateProvince {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("StateProvince").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("name", &self.name);
        vertex.add_property("code", &self.code);

        if let Some(ref country) = self.country {
            vertex.add_property("country", country);
        }

        vertex
    }
}


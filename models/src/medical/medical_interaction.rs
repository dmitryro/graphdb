use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MedicalInteraction {
    pub id: i32,
    pub primary_medication_id: i32,
    pub secondary_medication_id: i32,
}

impl ToVertex for MedicalInteraction {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("MedicalInteraction".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("primary_medication_id", &self.primary_medication_id.to_string());
        vertex.add_property("secondary_medication_id", &self.secondary_medication_id.to_string());

        vertex
    }
}


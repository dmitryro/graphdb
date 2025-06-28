use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MedicalInteractionSecondary {
    pub id: i32,
    pub primary_medication_id: i32,
    pub secondary_medication_id: i32,
    pub severity: String,
    pub description: Option<String>,
}

impl ToVertex for MedicalInteractionSecondary {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("MedicalInteractionSecondary").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("primary_medication_id", &self.primary_medication_id.to_string());
        vertex.add_property("secondary_medication_id", &self.secondary_medication_id.to_string());
        vertex.add_property("severity", &self.severity);
        if let Some(ref desc) = self.description {
            vertex.add_property("description", desc);
        }

        vertex
    }
}


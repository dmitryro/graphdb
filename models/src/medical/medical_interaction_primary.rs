use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MedicalInteractionPrimary {
    pub id: i32,
    pub medication_id: i32,
    pub interaction_name: String,
    pub interaction_class: String,
    pub description: Option<String>,
}

impl ToVertex for MedicalInteractionPrimary {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("MedicalInteractionPrimary".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("medication_id", &self.medication_id.to_string());
        vertex.add_property("interaction_name", &self.interaction_name);
        vertex.add_property("interaction_class", &self.interaction_class);
        if let Some(ref desc) = self.description {
            vertex.add_property("description", desc);
        }

        vertex
    }
}


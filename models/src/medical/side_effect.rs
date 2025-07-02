// lib/src/models/medical/side_effect.rs
use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct SideEffect {
    pub id: i32,
    pub medication_id: i32,
    pub description: String,
    pub severity: String,
    pub onset: Option<String>,
    pub duration: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for SideEffect {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("SideEffect".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("medication_id", &self.medication_id.to_string());
        vertex.add_property("description", &self.description);
        vertex.add_property("severity", &self.severity);

        if let Some(ref onset) = self.onset {
            vertex.add_property("onset", onset);
        }

        if let Some(ref duration) = self.duration {
            vertex.add_property("duration", duration);
        }

        vertex.add_property("created_at", &self.created_at.to_rfc3339());
        vertex.add_property("updated_at", &self.updated_at.to_rfc3339());

        vertex
    }
}


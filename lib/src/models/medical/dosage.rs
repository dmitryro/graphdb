use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Dosage {
    pub id: i32,
    pub medication_id: i32,
    pub dosage_amount: String,
    pub dosage_frequency: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl ToVertex for Dosage {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Dosage").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("medication_id", &self.medication_id.to_string());
        vertex.add_property("dosage_amount", &self.dosage_amount);
        vertex.add_property("dosage_frequency", &self.dosage_frequency);
        vertex.add_property("created_at", &self.created_at.to_rfc3339());

        if let Some(ref updated) = self.updated_at {
            vertex.add_property("updated_at", &updated.to_rfc3339());
        }

        vertex
    }
}


// lib/src/models/medical/social_determinant.rs
use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct SocialDeterminant {
    pub id: i32,
    pub patient_id: i32,
    pub factor_type: String,
    pub details: Option<String>,
    pub recorded_by: Option<i32>,
    pub recorded_at: DateTime<Utc>,
}

impl ToVertex for SocialDeterminant {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("SocialDeterminant".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("factor_type", &self.factor_type);

        if let Some(ref details) = self.details {
            vertex.add_property("details", details);
        }

        if let Some(ref recorded_by) = self.recorded_by {
            vertex.add_property("recorded_by", &recorded_by.to_string());
        }

        vertex.add_property("recorded_at", &self.recorded_at.to_rfc3339());

        vertex
    }
}


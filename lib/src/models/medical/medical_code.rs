// medical_code.rs
use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct MedicalCode {
    pub id: i32,
    pub code: String,
    pub description: String,
    pub code_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for MedicalCode {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("MedicalCode").unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("code", &self.code);
        v.add_property("description", &self.description);
        v.add_property("code_type", &self.code_type);
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}


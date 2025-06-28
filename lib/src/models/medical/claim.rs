use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Claim {
    pub id: i32,
    pub patient_id: i32,
    pub insurance_id: i32,
    pub date_of_service: DateTime<Utc>,
    pub amount_billed: f32,
    pub amount_covered: f32,
    pub status: String,
}

impl ToVertex for Claim {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("Claim").expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("insurance_id", &self.insurance_id.to_string());
        vertex.add_property("date_of_service", &self.date_of_service.to_rfc3339());
        vertex.add_property("amount_billed", &self.amount_billed.to_string());
        vertex.add_property("amount_covered", &self.amount_covered.to_string());
        vertex.add_property("status", &self.status);

        vertex
    }
}


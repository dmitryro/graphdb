// lib/src/models/medical/pharmacy_integration.rs
use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct PharmacyIntegration {
    pub id: i32,
    pub pharmacy_id: i32,
    pub prescription_id: i32,
    pub status: String,
    pub fulfillment_date: Option<DateTime<Utc>>,
}

impl ToVertex for PharmacyIntegration {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("PharmacyIntegration").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("pharmacy_id", &self.pharmacy_id.to_string());
        vertex.add_property("prescription_id", &self.prescription_id.to_string());
        vertex.add_property("status", &self.status);
        if let Some(ref v) = self.fulfillment_date {
            vertex.add_property("fulfillment_date", &v.to_rfc3339());
        }

        vertex
    }
}

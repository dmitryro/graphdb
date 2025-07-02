// medical_record.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct MedicalRecord {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub record_type: Option<String>,
    pub record_data: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for MedicalRecord {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("MedicalRecord".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("doctor_id", &self.doctor_id.to_string());
        if let Some(ref val) = self.record_type {
            v.add_property("record_type", val);
        }
        if let Some(ref val) = self.record_data {
            v.add_property("record_data", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}


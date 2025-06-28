// lib/src/models/medical/prescription.rs
use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Prescription {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub medication_name: String,
    pub dose: String,
    pub frequency: String,
    pub start_date: DateTime<Utc>,
    pub end_date: Option<DateTime<Utc>>,
}

impl ToVertex for Prescription {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Prescription").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("doctor_id", &self.doctor_id.to_string());
        vertex.add_property("medication_name", &self.medication_name);
        vertex.add_property("dose", &self.dose);
        vertex.add_property("frequency", &self.frequency);
        vertex.add_property("start_date", &self.start_date.to_rfc3339());
        if let Some(ref v) = self.end_date {
            vertex.add_property("end_date", &v.to_rfc3339());
        }

        vertex
    }
}


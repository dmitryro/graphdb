use chrono::NaiveDate;
use crate::models::{identifiers::Identifier, Vertex, ToVertex};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Diagnosis {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub code_id: i32,
    pub description: String,
    pub date: NaiveDate,
}

impl ToVertex for Diagnosis {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Diagnosis").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("doctor_id", &self.doctor_id.to_string());
        vertex.add_property("code_id", &self.code_id.to_string());
        vertex.add_property("description", &self.description);
        vertex.add_property("date", &self.date.to_string());

        vertex
    }
}


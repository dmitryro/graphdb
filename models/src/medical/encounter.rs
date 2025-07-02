use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Encounter {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub encounter_type: String,
    pub date: DateTime<Utc>,
    pub notes: Option<String>,
}

impl ToVertex for Encounter {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Encounter".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("doctor_id", &self.doctor_id.to_string());
        vertex.add_property("encounter_type", &self.encounter_type);
        vertex.add_property("date", &self.date.to_rfc3339());

        if let Some(ref n) = self.notes {
            vertex.add_property("notes", n);
        }

        vertex
    }
}


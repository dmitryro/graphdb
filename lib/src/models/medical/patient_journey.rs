use crate::models::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct PatientJourney {
    pub id: i32,
    pub patient_id: i32,
    pub encounter_id: i32,
    pub diagnosis_id: i32,
    pub prescription_id: Option<i32>,
    pub vitals_id: Option<i32>,
    pub timestamp: DateTime<Utc>,
}

impl ToVertex for PatientJourney {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("PatientJourney").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("encounter_id", &self.encounter_id.to_string());
        vertex.add_property("diagnosis_id", &self.diagnosis_id.to_string());
        if let Some(p) = self.prescription_id { vertex.add_property("prescription_id", &p.to_string()); }
        if let Some(v) = self.vitals_id { vertex.add_property("vitals_id", &v.to_string()); }
        vertex.add_property("timestamp", &self.timestamp.to_rfc3339());

        vertex
    }
}


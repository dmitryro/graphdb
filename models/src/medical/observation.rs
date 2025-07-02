// models/src/medical/observation.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Observation {
    pub id: i32,
    pub encounter_id: i32, // Links to an existing Encounter
    pub patient_id: i32, // Links to an existing Patient
    pub observation_type: String, // e.g., "GCS", "Pupil Response", "Wound Assessment", "Pain Location"
    pub value: String, // The actual observation value (e.g., "E4V5M6" for GCS, "Reactive" for pupils)
    pub unit: Option<String>, // Unit of measurement if applicable (e.g., "mm", "cm", "score")
    pub observed_at: DateTime<Utc>,
    pub observed_by_user_id: i32, // Links to an existing User (Nurse, Doctor, etc.)
}

impl ToVertex for Observation {
    fn to_vertex(&self) -> Vertex {
        // FIX: Convert "&str" to String using .to_string()
        let mut v = Vertex::new(Identifier::new("Observation".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("observation_type", &self.observation_type);
        v.add_property("value", &self.value);
        if let Some(ref val) = self.unit {
            v.add_property("unit", val);
        }
        v.add_property("observed_at", &self.observed_at.to_rfc3339());
        v.add_property("observed_by_user_id", &self.observed_by_user_id.to_string());
        v
    }
}

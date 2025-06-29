// lib/src/models/medical/disposition.rs
use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Disposition {
    pub id: i32,
    pub encounter_id: i32, // Links to an existing Encounter
    pub patient_id: i32, // Links to an existing Patient
    pub disposition_type: String, // e.g., "Discharged Home", "Admitted to Inpatient", "Transfer to Other Facility", "Left Against Medical Advice", "Expired"
    pub admitting_service: Option<String>, // e.g., "Internal Medicine", "Cardiology" (if admitted)
    pub admitting_doctor_id: Option<i32>, // Links to an existing Doctor (if admitted)
    pub transfer_facility_id: Option<i32>, // Links to a Hospital or Partner model (if transferred)
    pub discharge_instructions: Option<String>,
    pub disposed_at: DateTime<Utc>,
}

impl ToVertex for Disposition {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Disposition").unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("disposition_type", &self.disposition_type);
        if let Some(ref val) = self.admitting_service {
            v.add_property("admitting_service", val);
        }
        if let Some(ref val) = self.admitting_doctor_id {
            v.add_property("admitting_doctor_id", &val.to_string());
        }
        if let Some(ref val) = self.transfer_facility_id {
            v.add_property("transfer_facility_id", &val.to_string());
        }
        if let Some(ref val) = self.discharge_instructions {
            v.add_property("discharge_instructions", val);
        }
        v.add_property("disposed_at", &self.disposed_at.to_rfc3339());
        v
    }
}

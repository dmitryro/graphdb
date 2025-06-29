// lib/src/models/medical/facility_unit.rs
use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct FacilityUnit {
    pub id: i32,
    pub department_id: i32, // Links to an existing Department
    pub name: String,
    pub unit_type: String, // e.g., "Patient Care Unit", "Procedure Room", "Lab", "ICU", "ED Triage", "Operating Room"
    pub total_beds: Option<i32>,
    pub current_occupancy: Option<i32>, // Useful for real-time bed management in ED
    pub phone: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for FacilityUnit {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("FacilityUnit").unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("department_id", &self.department_id.to_string());
        v.add_property("name", &self.name);
        v.add_property("unit_type", &self.unit_type);
        if let Some(ref val) = self.total_beds {
            v.add_property("total_beds", &val.to_string());
        }
        if let Some(ref val) = self.current_occupancy {
            v.add_property("current_occupancy", &val.to_string());
        }
        if let Some(ref val) = self.phone {
            v.add_property("phone", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

// models/src/medical/staff_assignment.rs
use chrono::{DateTime, Utc};
// FIX: Changed `crate::models::{Vertex, ToVertex, identifiers::Identifier}`
// to `crate::{Vertex, ToVertex, identifiers::Identifier}`
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct StaffAssignment {
    pub id: i32,
    pub user_id: i32, // Links to an existing User (can be Doctor, Nurse, Admin, etc.)
    pub hospital_id: i32, // Links to an existing Hospital
    pub department_id: Option<i32>, // Links to an existing Department (optional, as user might be hospital-wide admin)
    pub facility_unit_id: Option<i32>, // Links to an existing FacilityUnit (optional, for specific unit assignments)
    pub assigned_role_id: i32, // Links to an existing Role model
    pub start_date: DateTime<Utc>,
    pub end_date: Option<DateTime<Utc>>, // Null if current active assignment
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for StaffAssignment {
    fn to_vertex(&self) -> Vertex {
        // FIX: Convert "&str" to String using .to_string()
        let mut v = Vertex::new(Identifier::new("StaffAssignment".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("user_id", &self.user_id.to_string());
        v.add_property("hospital_id", &self.hospital_id.to_string());
        if let Some(ref val) = self.department_id {
            v.add_property("department_id", &val.to_string());
        }
        if let Some(ref val) = self.facility_unit_id {
            v.add_property("facility_unit_id", &val.to_string());
        }
        v.add_property("assigned_role_id", &self.assigned_role_id.to_string());
        v.add_property("start_date", &self.start_date.to_rfc3339());
        if let Some(ref val) = self.end_date {
            v.add_property("end_date", &val.to_rfc3339());
        }
        v.add_property("is_active", &self.is_active.to_string());
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

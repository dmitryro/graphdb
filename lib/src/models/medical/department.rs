// lib/src/models/medical/department.rs
use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Department {
    pub id: i32,
    pub hospital_id: i32, // Links to an existing Hospital
    pub name: String,
    pub department_type: String, // e.g., "Clinical", "Administrative", "Ancillary", "Emergency"
    pub head_of_department_user_id: Option<i32>, // Links to an existing User
    pub phone: Option<String>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Department {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Department").unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("hospital_id", &self.hospital_id.to_string());
        v.add_property("name", &self.name);
        v.add_property("department_type", &self.department_type);
        if let Some(ref val) = self.head_of_department_user_id {
            v.add_property("head_of_department_user_id", &val.to_string());
        }
        if let Some(ref val) = self.phone {
            v.add_property("phone", val);
        }
        if let Some(ref val) = self.description {
            v.add_property("description", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

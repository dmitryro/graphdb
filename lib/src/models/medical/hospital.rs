// lib/src/models/medical/hospital.rs
use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Hospital {
    pub id: i32,
    pub name: String,
    pub address_id: i32, // Links to an existing Address model
    pub phone: Option<String>,
    pub website: Option<String>,
    pub admin_contact_user_id: Option<i32>, // Links to an existing User for primary hospital admin
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Hospital {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Hospital").unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("name", &self.name);
        v.add_property("address_id", &self.address_id.to_string());
        if let Some(ref val) = self.phone {
            v.add_property("phone", val);
        }
        if let Some(ref val) = self.website {
            v.add_property("website", val);
        }
        if let Some(ref val) = self.admin_contact_user_id {
            v.add_property("admin_contact_user_id", &val.to_string());
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

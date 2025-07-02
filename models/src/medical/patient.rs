use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Patient {
    pub id: i32,
    pub user_id: Option<i32>, // Links to an existing User (if patient is also a user)
    pub first_name: String,
    pub last_name: String,
    pub date_of_birth: DateTime<Utc>,
    pub gender: String, // e.g., "Male", "Female", "Other"
    pub address: Option<String>,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Patient {
    fn to_vertex(&self) -> Vertex {
        // FIX: Convert "&str" to String using .to_string()
        let id_type = Identifier::new("Patient".to_string()).expect("Invalid Identifier");
        let mut v = Vertex::new(id_type);
        v.add_property("id", &self.id.to_string());
        if let Some(ref val) = self.user_id {
            v.add_property("user_id", &val.to_string());
        }
        v.add_property("first_name", &self.first_name);
        v.add_property("last_name", &self.last_name);
        v.add_property("date_of_birth", &self.date_of_birth.to_rfc3339());
        v.add_property("gender", &self.gender);
        if let Some(ref val) = self.address {
            v.add_property("address", val);
        }
        if let Some(ref val) = self.phone {
            v.add_property("phone", val);
        }
        if let Some(ref val) = self.email {
            v.add_property("email", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

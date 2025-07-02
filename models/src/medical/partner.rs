// models/src/medical/partner.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Partner {
    pub id: i32,
    pub partner_type: String, // e.g., "Pharmacy", "Laboratory", "Medical Supplier", "Insurance"
    pub name: String,
    pub contact_person_user_id: Option<i32>, // Links to an existing User
    pub phone: Option<String>,
    pub email: Option<String>,
    pub address: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Partner {
    fn to_vertex(&self) -> Vertex {
        // FIX: Convert "&str" to String using .to_string()
        let mut vertex = Vertex::new(Identifier::new("Partner".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("partner_type", &self.partner_type);
        vertex.add_property("name", &self.name);
        if let Some(ref val) = self.contact_person_user_id {
            vertex.add_property("contact_person_user_id", &val.to_string());
        }
        if let Some(ref val) = self.phone {
            vertex.add_property("phone", val);
        }
        if let Some(ref val) = self.email {
            vertex.add_property("email", val);
        }
        if let Some(ref val) = self.address {
            vertex.add_property("address", val);
        }
        vertex.add_property("created_at", &self.created_at.to_rfc3339());
        vertex.add_property("updated_at", &self.updated_at.to_rfc3339());
        vertex
    }
}

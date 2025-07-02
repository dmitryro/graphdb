// lib/src/models/medical/registration.rs
use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Registration {
    pub id: u32,
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password: String,
    pub phone: String,
    pub role_id: u32,
    pub created_at: DateTime<Utc>,
}

impl ToVertex for Registration {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Registration".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("first", &self.first);
        vertex.add_property("last", &self.last);
        vertex.add_property("username", &self.username);
        vertex.add_property("email", &self.email);
        vertex.add_property("password", &self.password);
        vertex.add_property("phone", &self.phone);
        vertex.add_property("role_id", &self.role_id.to_string());
        vertex.add_property("created_at", &self.created_at.to_rfc3339());

        vertex
    }
}


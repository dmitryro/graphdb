// user.rs
use chrono::{DateTime, Utc};
use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct User {
    pub id: u32,
    pub first: String,
    pub last: String,
    pub username: String,
    pub email: String,
    pub password: String,
    pub phone: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub role_id: u32,
}

impl ToVertex for User {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("User").unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("first", &self.first);
        v.add_property("last", &self.last);
        v.add_property("username", &self.username);
        v.add_property("email", &self.email);
        v.add_property("password", &self.password);
        if let Some(ref phone) = self.phone {
            v.add_property("phone", phone);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v.add_property("role_id", &self.role_id.to_string());
        v
    }
}

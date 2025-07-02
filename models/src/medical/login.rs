// login.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Login {
    pub id: u32,
    pub username: String,
    pub password: String,
    pub login_time: DateTime<Utc>,
}

impl ToVertex for Login {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Login".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("username", &self.username);
        v.add_property("password", &self.password);
        v.add_property("login_time", &self.login_time.to_rfc3339());
        v
    }
}

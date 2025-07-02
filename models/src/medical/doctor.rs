use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Doctor {
    pub id: i32,
    pub first_name: String,
    pub last_name: String,
    pub phone: String,
    pub email: String,
    pub specialization: String,
    pub license_number: String,
}

impl ToVertex for Doctor {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Doctor".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("first_name", &self.first_name);
        vertex.add_property("last_name", &self.last_name);
        vertex.add_property("phone", &self.phone);
        vertex.add_property("email", &self.email);
        vertex.add_property("specialization", &self.specialization);
        vertex.add_property("license_number", &self.license_number);

        vertex
    }
}


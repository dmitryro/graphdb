use crate::models::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Partner {
    pub id: i32,
    pub name: String,
    pub contact_info: String,
    pub partnership_type: String,
}

impl ToVertex for Partner {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Partner").unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("name", &self.name);
        vertex.add_property("contact_info", &self.contact_info);
        vertex.add_property("partnership_type", &self.partnership_type);

        vertex
    }
}


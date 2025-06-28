use crate::{util::generate_uuid_v1, Identifier};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

/// A vertex.
///
/// Vertices are how you would represent nouns in the datastore. An example
/// might be a user, or a movie. All vertices have a unique ID and a type.
#[derive(Clone, Debug)]
pub struct Vertex {
    /// The id of the vertex.
    pub id: Uuid,

    /// The type of the vertex.
    pub t: Identifier,

    /// Properties associated with this vertex.
    pub properties: HashMap<String, String>,
}

impl Vertex {
    /// Creates a new vertex with an ID generated via UUIDv1. These vertex IDs
    /// are trivially guessable and consequently less secure, but index
    /// better. This method is suggested unless you need vertex IDs to not be
    /// trivially guessable.
    ///
    /// # Arguments
    ///
    /// * `t`: The type of the vertex.
    pub fn new(t: Identifier) -> Self {
        Self::with_id(generate_uuid_v1(), t)
    }

    /// Creates a new vertex with a specified id.
    ///
    /// # Arguments
    ///
    /// * `id`: The id of the vertex.
    /// * `t`: The type of the vertex.
    pub fn with_id(id: Uuid, t: Identifier) -> Self {
        Vertex {
            id,
            t,
            properties: HashMap::new(),
        }
    }

    /// Add a property (mutable).
    pub fn add_property(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), value.to_string());
    }

    /// Builder-style method to add a property (returns Self for chaining).
    pub fn with_property(mut self, key: &str, value: &str) -> Self {
        self.properties.insert(key.to_string(), value.to_string());
        self
    }
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Vertex) -> bool {
        self.id == other.id
    }
}

impl Hash for Vertex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Eq for Vertex {}

#[cfg(test)]
mod tests {
    use super::Vertex;
    use crate::Identifier;
    use std::collections::HashSet;
    use uuid::Uuid;

    #[test]
    fn should_hash() {
        assert_eq!(
            HashSet::from([Vertex::with_id(Uuid::default(), Identifier::new("foo").unwrap())]),
            HashSet::from([Vertex::with_id(Uuid::default(), Identifier::new("foo").unwrap())])
        );
    }

    #[test]
    fn test_add_and_with_property() {
        let id_type = Identifier::new("TestVertex").unwrap();
        let mut v = Vertex::new(id_type.clone());
        v.add_property("key1", "value1");
        assert_eq!(v.properties.get("key1").map(String::as_str), Some("value1"));

        let v2 = Vertex::new(id_type).with_property("key2", "value2");
        assert_eq!(v2.properties.get("key2").map(String::as_str), Some("value2"));
    }
}


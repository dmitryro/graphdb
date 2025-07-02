// lib/src/models/to_vertex.rs

use super::vertices::Vertex;

pub trait ToVertex {
    fn to_vertex(&self) -> Vertex;
}

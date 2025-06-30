use crate::engine::{vertex::Vertex, edge::Edge};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub struct Graph {
    pub vertices: HashMap<Uuid, Vertex>,
    pub edges: HashMap<Uuid, Edge>,

    // For fast traversal (adjacency list)
    pub adjacency_list: HashMap<Uuid, HashSet<Uuid>>, // from vertex id -> set of edge ids
    pub inbound_list: HashMap<Uuid, HashSet<Uuid>>,   // to vertex id -> set of edge ids
}

impl Graph {
    pub fn new() -> Self {
        Graph {
            vertices: HashMap::new(),
            edges: HashMap::new(),
            adjacency_list: HashMap::new(),
            inbound_list: HashMap::new(),
        }
    }

    pub fn add_vertex(&mut self, vertex: Vertex) {
        self.vertices.insert(vertex.id, vertex);
    }

    pub fn add_edge(&mut self, edge: Edge) {
        let from = edge.from;
        let to = edge.to;
        let edge_id = edge.id;

        self.edges.insert(edge.id, edge);
        self.adjacency_list.entry(from).or_default().insert(edge_id);
        self.inbound_list.entry(to).or_default().insert(edge_id);
    }

    pub fn get_vertex(&self, id: &Uuid) -> Option<&Vertex> {
        self.vertices.get(id)
    }

    pub fn get_edges_from(&self, vertex_id: &Uuid) -> Option<&HashSet<Uuid>> {
        self.adjacency_list.get(vertex_id)
    }

    pub fn get_edges_to(&self, vertex_id: &Uuid) -> Option<&HashSet<Uuid>> {
        self.inbound_list.get(vertex_id)
    }

    // More graph methods: remove_vertex, remove_edge, traversal, queries ...
}


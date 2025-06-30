// lib/src/engine/pattern_match.rs

use crate::models::vertices::Vertex;
use crate::engine::Edge; 
use crate::models::identifiers::Identifier;
use std::collections::HashMap;

pub enum Pattern {
    VertexType(String),
    EdgeType(String),
    PropertyEquals(String, String),
    And(Box<Pattern>, Box<Pattern>),
    Or(Box<Pattern>, Box<Pattern>),
    Not(Box<Pattern>),
}

impl Pattern {
    pub fn matches_vertex(&self, vertex: &Vertex) -> bool {
        match self {
            Pattern::VertexType(t) => vertex.t.to_string() == *t,
            Pattern::PropertyEquals(k, v) => {
                vertex.properties.get(k).map_or(false, |val| val == v)
            }
            Pattern::And(left, right) => {
                left.matches_vertex(vertex) && right.matches_vertex(vertex)
            }
            Pattern::Or(left, right) => {
                left.matches_vertex(vertex) || right.matches_vertex(vertex)
            }
            Pattern::Not(inner) => !inner.matches_vertex(vertex),
            _ => false,
        }
    }

    pub fn matches_edge(&self, edge: &Edge) -> bool {
        match self {
            Pattern::EdgeType(t) => edge.t.to_string() == *t,
            Pattern::PropertyEquals(k, v) => {
                edge.properties.get(k).map_or(false, |val| val.to_string() == *v)
            }
            Pattern::And(left, right) => {
                left.matches_edge(edge) && right.matches_edge(edge)
            }
            Pattern::Or(left, right) => {
                left.matches_edge(edge) || right.matches_edge(edge)
            }
            Pattern::Not(inner) => !inner.matches_edge(edge),
            _ => false,
        }
    }
}


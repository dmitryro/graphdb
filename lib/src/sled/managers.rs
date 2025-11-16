// lib/src/sled/managers.rs
use std::io::{Cursor, Read};
use sled::{Db, Tree, IVec, Batch};
use std::collections::HashMap;
use bincode::{
    config::{self, Configuration, BigEndian, Fixint}, // Import BigEndian and Fixint
    serde::{decode_from_slice, encode_to_vec},
};

// Import from models crate
use models::errors::GraphResult as ModelsResult; // Alias to prevent conflict
use crate::errors::{GraphError, Result}; // Your lib's error type
use models::identifiers::{Identifier, SerializableUuid};
use models::json::Json;
use models::edges::Edge;
use models::vertices::Vertex;
use models::util; // Assuming util is a module in models

// Struct to hold results from Sled iteration
#[derive(Debug)]
pub struct SledReadResult {
    pub key: IVec,
    pub value: IVec,
}

// Updated bincode_config for new bincode API
fn bincode_config() -> Configuration<BigEndian, Fixint> {
    config::standard()
        .with_big_endian()
        .with_fixed_int_encoding()
}

fn take_with_prefix_sled(iterator: sled::Iter, prefix: Vec<u8>) -> impl Iterator<Item = SledReadResult> {
    iterator
        .filter_map(move |item| {
            let (key_ivec, value_ivec) = item.ok()?;
            if key_ivec.starts_with(&prefix) {
                Some((key_ivec, value_ivec))
            } else {
                None
            }
        })
        .map(|(key, value)| SledReadResult { key, value })
}

pub struct SledManager {
    tree: Tree,
    bincode_conf: Configuration<BigEndian, Fixint>,
}

impl SledManager {
    pub fn new(db: &Db, tree_name: &str) -> Result<Self> {
        let tree = db.open_tree(tree_name)?;
        Ok(Self {
            tree,
            bincode_conf: bincode_config(),
        })
    }

    // --- Vertex Operations ---
    pub fn add_vertex(&self, vertex: &Vertex) -> Result<IVec> {
        let key = util::build(&[
            util::Component::Identifier(vertex.label().clone()),
            util::Component::Uuid(vertex.id.into()),
        ]);
        let value_json = serde_json::to_vec(&vertex)?;

        let mut batch = sled::Batch::default();
        batch.insert(IVec::from(key), value_json.as_slice());
        self.tree.apply_batch(batch)?;
        Ok(IVec::from(util::build(&[
            util::Component::Identifier(vertex.label().clone()),
            util::Component::Uuid(vertex.id.into()),
        ])))
    }

    pub fn get_vertex(&self, vertex_type: &Identifier, id: &uuid::Uuid) -> Result<Option<Vertex>> {
        let key = util::build(&[
            util::Component::Identifier(vertex_type.clone()),
            util::Component::Uuid(*id),
        ]);
        let result = self.tree.get(&key)?;
        match result {
            Some(ivec) => {
                let vertex: Vertex = serde_json::from_slice(&ivec)?;
                Ok(Some(vertex))
            }
            None => Ok(None),
        }
    }

    pub fn update_vertex(&self, vertex: &Vertex) -> Result<IVec> {
        let key = util::build(&[
            util::Component::Identifier(vertex.label().clone()),
            util::Component::Uuid(vertex.id.into()),
        ]);
        let value_json = serde_json::to_vec(&vertex)?;

        let mut batch = sled::Batch::default();
        batch.insert(IVec::from(key), value_json.as_slice());
        self.tree.apply_batch(batch)?;
        Ok(IVec::from(util::build(&[
            util::Component::Identifier(vertex.label().clone()),
            util::Component::Uuid(vertex.id.into()),
        ])))
    }

    pub fn delete_vertex(&self, vertex_type: &Identifier, id: &uuid::Uuid) -> Result<()> {
        let key = util::build(&[
            util::Component::Identifier(vertex_type.clone()),
            util::Component::Uuid(*id),
        ]);
        let mut batch = sled::Batch::default();
        batch.remove(IVec::from(key));
        self.tree.apply_batch(batch)?;
        Ok(())
    }

    // --- Edge Operations ---
    pub fn add_edge(&self, edge: &Edge) -> Result<IVec> {
        let key = util::build(&[
            util::Component::Identifier(edge.edge_type.clone()),
            util::Component::Uuid(edge.outbound_id.into()),
            util::Component::Uuid(edge.inbound_id.into()),
        ]);
        let value_json = serde_json::to_vec(&edge)?;

        let mut batch = sled::Batch::default();
        batch.insert(IVec::from(key), value_json.as_slice());
        self.tree.apply_batch(batch)?;
        Ok(IVec::from(util::build(&[
            util::Component::Identifier(edge.edge_type.clone()),
            util::Component::Uuid(edge.outbound_id.into()),
            util::Component::Uuid(edge.inbound_id.into()),
        ])))
    }

    pub fn get_edge(&self, edge_type: &Identifier, outbound_id: &uuid::Uuid, inbound_id: &uuid::Uuid) -> Result<Option<Edge>> {
        let key = util::build(&[
            util::Component::Identifier(edge_type.clone()),
            util::Component::Uuid(*outbound_id),
            util::Component::Uuid(*inbound_id),
        ]);
        let result = self.tree.get(&key)?;
        match result {
            Some(ivec) => {
                let edge: Edge = serde_json::from_slice(&ivec)?;
                Ok(Some(edge))
            }
            None => Ok(None),
        }
    }

    pub fn update_edge(&self, edge: &Edge) -> Result<IVec> {
        let key = util::build(&[
            util::Component::Identifier(edge.edge_type.clone()),
            util::Component::Uuid(edge.outbound_id.into()),
            util::Component::Uuid(edge.inbound_id.into()),
        ]);
        let value_json = serde_json::to_vec(&edge)?;

        let mut batch = sled::Batch::default();
        batch.insert(IVec::from(key), value_json.as_slice());
        self.tree.apply_batch(batch)?;
        Ok(IVec::from(util::build(&[
            util::Component::Identifier(edge.edge_type.clone()),
            util::Component::Uuid(edge.outbound_id.into()),
            util::Component::Uuid(edge.inbound_id.into()),
        ])))
    }

    pub fn delete_edge(&self, edge_type: &Identifier, outbound_id: &uuid::Uuid, inbound_id: &uuid::Uuid) -> Result<()> {
        let key = util::build(&[
            util::Component::Identifier(edge_type.clone()),
            util::Component::Uuid(*outbound_id),
            util::Component::Uuid(*inbound_id),
        ]);
        let mut batch = sled::Batch::default();
        batch.remove(IVec::from(key));
        self.tree.apply_batch(batch)?;
        Ok(())
    }

    pub fn get_vertices_by_type(&self, vertex_type: &Identifier) -> Result<Vec<Vertex>> {
        let prefix = util::build(&[util::Component::Identifier(vertex_type.clone())]);
        let iter = take_with_prefix_sled(self.tree.scan_prefix(&prefix), prefix);

        let mut vertices = Vec::new();
        for result in iter {
            let sled_result = result;
            let vertex: Vertex = serde_json::from_slice(&sled_result.value)?;
            vertices.push(vertex);
        }
        Ok(vertices)
    }

    pub fn get_edges_by_type(&self, edge_type: &Identifier) -> Result<Vec<Edge>> {
        let prefix = util::build(&[util::Component::Identifier(edge_type.clone())]);
        let iter = take_with_prefix_sled(self.tree.scan_prefix(&prefix), prefix);

        let mut edges = Vec::new();
        for result in iter {
            let sled_result = result;
            let edge: Edge = serde_json::from_slice(&sled_result.value)?;
            edges.push(edge);
        }
        Ok(edges)
    }
}

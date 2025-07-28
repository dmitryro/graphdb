use crate::identifiers::{Identifier, SerializableUuid};
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize, Encode, Decode)]
pub struct Edge {
    pub outbound_id: SerializableUuid,
    pub t: Identifier,
    pub inbound_id: SerializableUuid,
}

impl Edge {
    pub fn new(outbound_id: impl Into<SerializableUuid>, t: Identifier, inbound_id: impl Into<SerializableUuid>) -> Edge {
        Edge {
            outbound_id: outbound_id.into(),
            t,
            inbound_id: inbound_id.into(),
        }
    }

    pub fn reversed(&self) -> Edge {
        Edge::new(self.inbound_id, self.t.clone(), self.outbound_id)
    }
}

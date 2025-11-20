// indexing_service/src/adapters.rs
use anyhow::Result;
use serde_json::{ Value, json };
use zmq::{Context, SocketType};

pub async fn send_zmq_command(port: u16, command: &str) -> Result<Value> {
    let context = Context::new();
    let socket = context.socket(SocketType::REQ)?;
    socket.set_rcvtimeo(5000)?;
    socket.set_sndtimeo(5000)?;
    let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);
    socket.connect(&endpoint)?;

    let payload = json!({ "command": "cypher", "query": command });
    socket.send(serde_json::to_vec(&payload)?.as_slice(), 0)?;

    let mut msg = zmq::Message::new();
    socket.recv(&mut msg, 0)?;
    let response: Value = serde_json::from_slice(msg.as_ref())?;
    Ok(response)
}

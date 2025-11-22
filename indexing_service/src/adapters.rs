// indexing_service/src/adapters.rs (CORRECTED)
use anyhow::{Result, Context as AnyhowContext};
use serde_json::{ Value, json };
use zmq::{Context, SocketType};
use tokio::task;

pub async fn send_zmq_command(port: u16, command: &str) -> Result<Value> {
    let endpoint = format!("ipc:///tmp/graphdb-{}.ipc", port);

    // 1. Deserialize the command string (&str) back into a JSON Value object.
    // This maintains the original structure (e.g., {"command": "index_create", ...})
    let payload: Value = serde_json::from_str(command)
        .with_context(|| format!("Failed to parse command string into JSON: {}", command))?;

    // The move closure captures the owned `payload` and `endpoint`.
    let response = task::spawn_blocking(move || -> Result<Value> {
        let context = Context::new();
        let socket = context.socket(SocketType::REQ)
            .context("Failed to create ZMQ socket")?;
        
        // Timeout configuration
        socket.set_rcvtimeo(5000)
            .context("Failed to set ZMQ receive timeout")?;
        socket.set_sndtimeo(5000)
            .context("Failed to set ZMQ send timeout")?;
        
        socket.connect(&endpoint)
            .with_context(|| format!("Failed to connect ZMQ socket to {}", endpoint))?;

        // 2. Serialize and send the fully-formed original JSON payload directly.
        let payload_vec = serde_json::to_vec(&payload)
            .context("Failed to serialize ZMQ payload")?;
            
        socket.send(payload_vec.as_slice(), 0)
            .context("Failed to send ZMQ message")?;

        let mut msg = zmq::Message::new();
        socket.recv(&mut msg, 0)
            .context("Failed to receive ZMQ response")?;
            
        let response: Value = serde_json::from_slice(msg.as_ref())
            .context("Failed to deserialize ZMQ response")?;
            
        Ok(response)
    })
    .await.with_context(|| "ZMQ blocking task failed to execute")??; 

    Ok(response)
}
use std::sync::Arc;
use std::time::Duration;
use anyhow::{Result, anyhow, Context};
use serde::{Deserialize, Serialize};
use log::{info, error};

use crate::storage_engine::{
    errors::{StorageEngineError},
    node::{Node, NodeId},
    edge::{Edge, EdgeId},
    types::{EngineInfo, StorageCapabilities},
};

// This is a simple request/response protocol for the ZeroMQ client and server.
// The client serializes a Request and sends it. The server processes it and sends back a Response.
#[derive(Serialize, Deserialize)]
enum Request {
    // Add a new variant for a simple health check.
    Ping,
    GetNode(NodeId),
    AddNode(Node),
    UpdateNode(Node),
    DeleteNode(NodeId),
    GetEdge(EdgeId),
    AddEdge(Edge),
    UpdateEdge(Edge),
    DeleteEdge(EdgeId),
    GetEngineInfo,
    GetCapabilities,
    Shutdown,
}

#[derive(Serialize, Deserialize)]
enum Response {
    Ok,
    Node(Option<Node>),
    Edge(Option<Edge>),
    EngineInfo(EngineInfo),
    StorageCapabilities(StorageCapabilities),
    Error(String),
}

/// A client for a ZeroMQ-based graph storage engine.
/// It sends requests and receives responses over a network socket.
// The `zmq::Socket` type does not implement `Debug`, `Serialize`, or `Deserialize`,
// so we must remove the derive macros for those traits.
pub struct ZmqClient {
    socket: zmq::Socket,
}

impl ZmqClient {
    /// Creates a new `ZmqClient` and connects to the specified address.
    pub async fn new(ip_address: &str, port: u16) -> Result<Arc<Self>> {
        let context = zmq::Context::new();
        let socket = context.socket(zmq::REQ).context("Failed to create ZeroMQ socket")?;
        let endpoint = format!("tcp://{}:{}", ip_address, port);

        info!("ZmqClient: Attempting to connect to ZeroMQ endpoint: {}", endpoint);
        socket.connect(&endpoint).context("Failed to connect to ZeroMQ endpoint")?;
        info!("ZmqClient: Successfully connected to endpoint: {}", endpoint);

        Ok(Arc::new(Self { socket }))
    }

    /// Sends a request to the server and receives a response.
    /// This is a private helper method to centralize the communication logic.
    async fn send_and_receive(&self, request: Request) -> Result<Response> {
        // Serialize the request into a message
        let request_bytes = serde_json::to_vec(&request)
            .context("Failed to serialize request")?;

        // Send the request
        self.socket.send(&request_bytes, 0).context("Failed to send ZeroMQ message")?;

        // Wait for and receive the response
        let mut msg = zmq::Message::new();
        self.socket.recv(&mut msg, 0).context("Failed to receive ZeroMQ message")?;
        
        // Deserialize the response message
        let response: Response = serde_json::from_slice(&msg)
            .context("Failed to deserialize ZeroMQ response")?;
        
        Ok(response)
    }

    /// Performs a simple health check by sending a Ping request.
    /// This method is lightweight and does not perform any graph operations.
    pub async fn ping(&self) -> Result<(), StorageEngineError> {
        let request = Request::Ping;
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => {
                info!("ZmqClient: Ping successful.");
                Ok(())
            },
            Ok(Response::Error(msg)) => {
                error!("ZmqClient: Ping failed with server error: {}", msg);
                Err(StorageEngineError::EngineError(msg))
            },
            Ok(_) => {
                error!("ZmqClient: Ping received an unexpected response type.");
                Err(StorageEngineError::InternalError("Unexpected response type".to_string()))
            },
            Err(_e) => { // Fixed: Renamed `e` to `_e` to suppress the unused variable warning.
                error!("ZmqClient: Ping failed due to connection error: {}", _e);
                Err(StorageEngineError::ConnectionError(_e.to_string()))
            },
        }
    }

    pub async fn get_node(&self, node_id: NodeId) -> Result<Option<Node>, StorageEngineError> {
        let request = Request::GetNode(node_id);
        match self.send_and_receive(request).await {
            Ok(Response::Node(node)) => Ok(node),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())), // Fixed here too
        }
    }

    pub async fn add_node(&self, node: Node) -> Result<(), StorageEngineError> {
        let request = Request::AddNode(node);
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => Ok(()),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())), // And here
        }
    }

    pub async fn update_node(&self, node: Node) -> Result<(), StorageEngineError> {
        let request = Request::UpdateNode(node);
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => Ok(()),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }

    pub async fn delete_node(&self, node_id: NodeId) -> Result<(), StorageEngineError> {
        let request = Request::DeleteNode(node_id);
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => Ok(()),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }

    pub async fn get_edge(&self, edge_id: EdgeId) -> Result<Option<Edge>, StorageEngineError> {
        let request = Request::GetEdge(edge_id);
        match self.send_and_receive(request).await {
            Ok(Response::Edge(edge)) => Ok(edge),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }
    
    pub async fn add_edge(&self, edge: Edge) -> Result<(), StorageEngineError> {
        let request = Request::AddEdge(edge);
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => Ok(()),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }

    pub async fn update_edge(&self, edge: Edge) -> Result<(), StorageEngineError> {
        let request = Request::UpdateEdge(edge);
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => Ok(()),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }

    pub async fn delete_edge(&self, edge_id: EdgeId) -> Result<(), StorageEngineError> {
        let request = Request::DeleteEdge(edge_id);
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => Ok(()),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }

    pub async fn get_engine_info(&self) -> Result<EngineInfo, StorageEngineError> {
        let request = Request::GetEngineInfo;
        match self.send_and_receive(request).await {
            Ok(Response::EngineInfo(info)) => Ok(info),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }

    pub async fn get_capabilities(&self) -> Result<StorageCapabilities, StorageEngineError> {
        let request = Request::GetCapabilities;
        match self.send_and_receive(request).await {
            Ok(Response::StorageCapabilities(caps)) => Ok(caps),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }

    pub async fn shutdown(&self) -> Result<(), StorageEngineError> {
        let request = Request::Shutdown;
        match self.send_and_receive(request).await {
            Ok(Response::Ok) => Ok(()),
            Ok(Response::Error(msg)) => Err(StorageEngineError::EngineError(msg)),
            Ok(_) => Err(StorageEngineError::InternalError("Unexpected response type".to_string())),
            Err(_e) => Err(StorageEngineError::ConnectionError(_e.to_string())),
        }
    }
}

use axum::{
    extract::{Path, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use tokio::net::TcpListener;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tokio::time::{Duration};
use tower_http::cors::{Any, CorsLayer};
use thiserror::Error;
use daemon_api::{
    start_daemon, stop_daemon_api_call, stop_port_daemon,
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
};
use daemon_api::find_running_storage_daemon_port;
use lib::query_parser::{parse_query_from_string, QueryType};
use anyhow::Context;
use anyhow::Error as AnyhowError;

use daemon_api::help_generator::{generate_full_help, generate_help_for_path};

mod config;
use crate::config::{load_rest_api_config, load_storage_config, StorageConfig};

// Define the REST API error enum
#[derive(Debug, Error)]
pub enum RestApiError {
    #[error("Daemon API error: {0}")]
    DaemonApi(#[from] daemon_api::DaemonError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("JSON serialization/deserialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Storage daemon error: {0}")]
    StorageDaemon(String),
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Anyhow error: {0}")]
    Anyhow(#[from] AnyhowError),
    #[error("General error: {0}")]
    GeneralError(String),
}

// Implement IntoResponse for RestApiError to convert it into an HTTP response
impl IntoResponse for RestApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            RestApiError::DaemonApi(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Daemon API error: {}", e)),
            RestApiError::Io(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("IO error: {}", e)),
            RestApiError::Reqwest(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Request error: {}", e)),
            RestApiError::SerdeJson(e) => (StatusCode::BAD_REQUEST, format!("JSON error: {}", e)),
            RestApiError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg),
            RestApiError::StorageDaemon(msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Storage daemon error: {}", msg)),
            RestApiError::Config(msg) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Configuration error: {}", msg)),
            RestApiError::Anyhow(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("Internal error: {}", e)),
            RestApiError::GeneralError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = Json(json!({
            "status": "error",
            "message": error_message,
        }));

        (status, body).into_response()
    }
}

// Shared state for the Axum application
#[derive(Clone)]
struct AppState {
    daemon_handles: Arc<Mutex<HashMap<u16, tokio::task::JoinHandle<Result<(), daemon_api::DaemonError>>>>>,
    rest_api_shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    rest_api_port: Arc<Mutex<u16>>,
    storage_config: Arc<StorageConfig>,
}

#[derive(Debug, Deserialize)]
struct StartDaemonRequest {
    port: Option<u16>,
    cluster: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StopDaemonRequest {
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RegisterUserRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AuthenticateRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GraphQueryRequest {
    pub query: String,
    pub persist: Option<bool>,
}

// Handler for the /api/v1/daemon/start endpoint
async fn start_daemon_handler(
    State(state): State<AppState>,
    Json(payload): Json<StartDaemonRequest>,
) -> Result<Json<Value>, RestApiError> {
    let port = payload.port.unwrap_or(8080); // Default daemon port
    let cluster_range = payload.cluster;

    let mut handles = state.daemon_handles.lock().await;

    if handles.contains_key(&port) {
        return Ok(Json(json!({
            "status": "error",
            "message": format!("Daemon on port {} is already running (managed by this API).", port)
        })));
    }

    // Get the current REST API port to skip it when starting daemons
    let current_rest_port = *state.rest_api_port.lock().await;
    let skip_ports = vec![current_rest_port];

    println!("Attempting to start daemon on port {}...", port);

    let daemon_join_handle = tokio::spawn(async move {
        start_daemon(Some(port), cluster_range, skip_ports, "rest").await
    });

    handles.insert(port, daemon_join_handle);

    Ok(Json(json!({
        "status": "success",
        "message": format!("Daemon on port {} started successfully (async).", port)
    })))
}

// Handler for the /api/v1/daemon/stop endpoint
async fn stop_daemon_handler(
    State(state): State<AppState>,
    Json(payload): Json<StopDaemonRequest>,
) -> Result<Json<Value>, RestApiError> {
    if let Some(port) = payload.port {
        let mut handles = state.daemon_handles.lock().await;
        if let Some(handle) = handles.remove(&port) {
            println!("Attempting to stop daemon on port {}...", port);
            handle.abort();
            stop_port_daemon(port, "rest").await?;
            Ok(Json(json!({
                "status": "success",
                "message": format!("Daemon on port {} stopped.", port)
            })))
        } else {
            match stop_port_daemon(port, "rest").await {
                Ok(_) => Ok(Json(json!({
                    "status": "success",
                    "message": format!("Daemon on port {} stopped (externally).", port)
                }))),
                Err(e) => Err(RestApiError::DaemonApi(e)),
            }
        }
    } else {
        let mut handles = state.daemon_handles.lock().await;
        let ports_to_stop: Vec<u16> = handles.keys().cloned().collect();
        for port in ports_to_stop {
            if let Some(handle) = handles.remove(&port) {
                handle.abort();
                println!("Aborted task for daemon on port {}.", port);
            }
            if let Err(e) = stop_port_daemon(port, "rest").await {
                eprintln!("Failed to stop daemon on port {}: {:?}", port, e);
            }
        }
        match stop_daemon_api_call().await {
            Ok(_) => Ok(Json(json!({
                "status": "success",
                "message": "All daemons stopped."
            }))),
            Err(e) => Err(RestApiError::Anyhow(e)),
        }
    }
}

// Handler for the /api/v1/daemon/list endpoint
async fn list_daemons_handler(
    State(state): State<AppState>,
) -> Result<Json<Value>, RestApiError> {
    let handles = state.daemon_handles.lock().await;
    let running_ports: Vec<u16> = handles.keys().cloned().collect();
    Ok(Json(json!({
        "status": "success",
        "daemons": running_ports
    })))
}

// Handler for the /api/v1/shutdown endpoint
async fn shutdown_handler(
    State(state): State<AppState>,
) -> Result<Json<Value>, RestApiError> {
    let mut tx_guard = state.rest_api_shutdown_tx.lock().await;
    if let Some(tx) = tx_guard.take() {
        let _ = tx.send(());
        Ok(Json(json!({
            "status": "success",
            "message": "Shutting down REST API server."
        })))
    } else {
        Err(RestApiError::GeneralError("Shutdown signal already sent or not available.".to_string()))
    }
}

// Handler for the /api/v1/health endpoint
async fn health_check_handler() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "status": "ok", "message": "REST API is healthy" })))
}

// Handler for the /api/v1/version endpoint
async fn version_handler() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "version": "0.1.0-alpha", "api_level": 1 })))
}

// Handler for /api/v1/register
async fn register_user_handler(
    Json(payload): Json<RegisterUserRequest>,
) -> (StatusCode, Json<Value>) {
    println!("Registering user: {}", payload.username);
    (StatusCode::OK, Json(json!({
        "status": "success",
        "message": format!("User '{}' registered successfully (mock).", payload.username)
    })))
}

// Handler for /api/v1/auth
async fn authenticate_handler(
    Json(payload): Json<AuthenticateRequest>,
) -> (StatusCode, Json<Value>) {
    println!("Authenticating user: {}", payload.username);
    if payload.username == "testuser" && payload.password == "testpass" {
        (StatusCode::OK, Json(json!({
            "status": "success",
            "message": "Authentication successful (mock).",
            "token": "mock-jwt-token-12345"
        })))
    } else {
        (StatusCode::UNAUTHORIZED, Json(json!({
            "status": "error",
            "message": "Invalid credentials (mock)."
        })))
    }
}

// Handler for /api/v1/query
async fn query_handler(
    Json(payload): Json<GraphQueryRequest>,
) -> Result<Json<Value>, RestApiError> {
    println!("Received query: '{}', persist: {}", payload.query, payload.persist.unwrap_or(false));

    match parse_query_from_string(&payload.query) {
        Ok(query_type) => {
            let response_message = match query_type {
                QueryType::Cypher => "Cypher query processed (mock).",
                QueryType::SQL => "SQL query processed (mock).",
                QueryType::GraphQL => "GraphQL query processed (mock).",
            };
            Ok(Json(json!({
                "status": "success",
                "message": response_message,
                "query_type": format!("{:?}", query_type),
                "results": []
            })))
        },
        Err(e) => {
            Err(RestApiError::InvalidInput(format!("Invalid query format: {}", e)))
        }
    }
}

// Handler for the /api/v1/help endpoint
async fn get_full_help_rest() -> Json<serde_json::Value> {
    let help_text = generate_full_help();
    Json(serde_json::json!({"response": help_text}))
}

// Handler for the /api/v1/help/{path} endpoint
async fn get_filtered_help_rest(Path(path): Path<String>) -> Json<serde_json::Value> {
    let command_path: Vec<String> = path.split('/').map(|s| s.to_string()).collect();
    let help_text = generate_help_for_path(&command_path);
    Json(serde_json::json!({"response": help_text}))
}

// Helper functions for status endpoints
async fn get_rest_api_status_string() -> String {
    let rest_port = load_rest_api_config().map(|c| c.port).unwrap_or(8082);
    let rest_health_url = format!("http://127.0.0.1:{}/api/v1/health", rest_port);
    let rest_version_url = format!("http://127.0.0.1:{}/api/v1/version", rest_port);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build().expect("Failed to build reqwest client");

    let mut status_msg = format!("REST API on port {}: ", rest_port);
    match client.get(&rest_health_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            status_msg.push_str("Running. ");
            let version_info = client.get(&rest_version_url).send().await;
            match version_info {
                Ok(v_resp) if v_resp.status().is_success() => {
                    let v_json: serde_json::Value = v_resp.json().await.unwrap_or_default();
                    let version = v_json["version"].as_str().unwrap_or("N/A");
                    status_msg.push_str(&format!("Version: {}.", version));
                },
                _ => status_msg.push_str("Version: N/A."),
            }
        },
        _ => status_msg.push_str("Down."),
    }
    status_msg
}

async fn get_daemon_status_string(port_arg: Option<u16>) -> String {
    let mut status_msg = String::new();
    if let Some(port) = port_arg {
        let output = tokio::process::Command::new("lsof")
            .arg("-i")
            .arg(format!(":{}", port))
            .arg("-t")
            .output()
            .await;

        if let Ok(output) = output {
            if !output.stdout.is_empty() {
                status_msg.push_str(&format!("GraphDB Daemon on port {}: Running.", port));
            } else {
                status_msg.push_str(&format!("GraphDB Daemon on port {}: Down.", port));
            }
        } else {
            status_msg.push_str(&format!("GraphDB Daemon on port {}: Status check failed.", port));
        }
    } else {
        let common_daemon_ports = [8080, 8081, 9001, 9002, 9003, 9004, 9005];
        let mut running_ports = Vec::new();
        for &port in &common_daemon_ports {
            let output = tokio::process::Command::new("lsof")
                .arg("-i")
                .arg(format!(":{}", port))
                .arg("-t")
                .output()
                .await;
            if let Ok(output) = output {
                if !output.stdout.is_empty() {
                    running_ports.push(port.to_string());
                }
            }
        }
        if !running_ports.is_empty() {
            status_msg.push_str(&format!("GraphDB Daemons running on: {}.", running_ports.join(", ")));
        } else {
            status_msg.push_str("No GraphDB Daemons found on common ports.");
        }
    }
    status_msg
}

async fn get_storage_daemon_status_string(port_arg: Option<u16>) -> String {
    let mut status_msg = String::new();
    let port_to_check = if let Some(p) = port_arg {
        p
    } else {
        find_running_storage_daemon_port().await.unwrap_or(CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS)
    };

    let storage_config = load_storage_config(None)
        .unwrap_or_else(|e| {
            eprintln!("Warning: Could not load storage config for status check: {}. Using defaults.", e);
            StorageConfig {
                data_directory: "/tmp/graphdb_data".to_string(),
                log_directory: "/var/log/graphdb".to_string(),
                default_port: CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
                cluster_range: "9000-9002".to_string(),
                max_disk_space_gb: 1000,
                min_disk_space_gb: 10,
                use_raft_for_scale: true,
                storage_engine_type: "sled".to_string(),
            }
        });

    let output = tokio::process::Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port_to_check))
        .arg("-t")
        .output()
        .await;

    if let Ok(output) = output {
        if !output.stdout.is_empty() {
            status_msg.push_str(&format!("Storage Daemon on port {}: Running (Type: {}).", port_to_check, storage_config.storage_engine_type));
        } else {
            status_msg.push_str(&format!("Storage Daemon on port {}: Down (Type: {}).", port_to_check, storage_config.storage_engine_type));
        }
    } else {
        status_msg.push_str(&format!("Storage Daemon on port {}: Status check failed.", port_to_check));
    }
    status_msg
}

async fn get_full_status_summary_string() -> String {
    let mut summary = String::from("--- GraphDB System Status Summary ---\n");
    summary.push_str(&format!("{}\n", get_rest_api_status_string().await));
    summary.push_str(&format!("{}\n", get_daemon_status_string(None).await));
    summary.push_str(&format!("{}\n", get_storage_daemon_status_string(None).await));
    summary
}

// Handler for /api/v1/status/*path
async fn get_status_handler(Path(path): Path<String>) -> impl IntoResponse {
    // Split the path into segments and filter out empty ones
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    
    match segments.as_slice() {
        [] => {
            let status_text = get_full_status_summary_string().await;
            (StatusCode::OK, status_text)
        }
        ["rest"] => {
            let status_text = get_rest_api_status_string().await;
            (StatusCode::OK, status_text)
        }
        ["daemon"] => {
            let status_text = get_daemon_status_string(None).await;
            (StatusCode::OK, status_text)
        }
        ["daemon", port_str] => match port_str.parse::<u16>() {
            Ok(port) => {
                let status_text = get_daemon_status_string(Some(port)).await;
                (StatusCode::OK, status_text)
            }
            Err(_) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid port number: {}", port_str),
            ),
        },
        ["storage"] => {
            let status_text = get_storage_daemon_status_string(None).await;
            (StatusCode::OK, status_text)
        }
        ["storage", port_str] => match port_str.parse::<u16>() {
            Ok(port) => {
                let status_text = get_storage_daemon_status_string(Some(port)).await;
                (StatusCode::OK, status_text)
            }
            Err(_) => (
                StatusCode::BAD_REQUEST,
                format!("Invalid port number: {}", port_str),
            ),
        },
        _ => (
            StatusCode::NOT_FOUND,
            "Unknown status path".to_string(),
        ),
    }
}

// Main function to start the REST API server
pub async fn start_server(
    port: u16,
    shutdown_rx: oneshot::Receiver<()>,
    storage_data_directory: String,
) -> Result<(), AnyhowError> {
    let rest_api_config = load_rest_api_config()
        .context("Failed to load REST API configuration")?;

    let storage_config = load_storage_config(None)
        .context("Failed to load storage configuration for REST API")?;

    let app_state = AppState {
        daemon_handles: Arc::new(Mutex::new(HashMap::new())),
        rest_api_shutdown_tx: Arc::new(Mutex::new(None)),
        rest_api_port: Arc::new(Mutex::new(port)),
        storage_config: Arc::new(storage_config),
    };

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any);

    let app = Router::new()
        .route("/api/v1/daemon/start", post(start_daemon_handler))
        .route("/api/v1/daemon/stop", post(stop_daemon_handler))
        .route("/api/v1/daemon/list", get(list_daemons_handler))
        .route("/api/v1/shutdown", get(shutdown_handler))
        .route("/api/v1/health", get(health_check_handler))
        .route("/api/v1/version", get(version_handler))
        .route("/api/v1/register", post(register_user_handler))
        .route("/api/v1/auth", post(authenticate_handler))
        .route("/api/v1/query", post(query_handler))
        .route("/api/v1/status/*path", get(get_status_handler))
        .route("/api/v1/help", get(get_full_help_rest))
        .route("/api/v1/help/*path", get(get_filtered_help_rest))
        .with_state(app_state.clone())
        .layer(cors);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("REST API server listening on {}", addr);

    let (tx, rx_internal) = oneshot::channel();
    *app_state.rest_api_shutdown_tx.lock().await = Some(tx);

    let combined_shutdown_signal = async {
        tokio::select! {
            _ = shutdown_rx => {
                println!("Received external shutdown signal.");
            }
            _ = rx_internal => {
                println!("Received internal shutdown signal.");
            }
        }
    };

    let listener = TcpListener::bind(&addr)
        .await
        .context(format!("Failed to bind to address: {}", addr))?;

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(combined_shutdown_signal)
        .await
        .context("REST API server failed to start or run")?;

    println!("REST API server stopped.");
    Ok(())
}
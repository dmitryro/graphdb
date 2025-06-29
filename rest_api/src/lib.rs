use hyper::{Body, Request, Response, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use serde_json::json;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::fs::OpenOptions;
use std::io::Write;
use daemon_api::{start_daemon, stop_daemon};
use graphdb_lib::query_parser::{parse_query_from_string, QueryType}; // <-- FIXED

fn log_rest_event(message: &str) {
    if let Ok(mut log) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/tmp/graphdb-rest.log") {
        let _ = writeln!(log, "{message}");
    }
}

async fn handle_request(
    req: Request<Body>,
    shutdown_flag: Arc<Mutex<bool>>,
    rest_api_port: u16,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    let method = req.method();

    match (method, path) {
        // Health endpoint
        (&hyper::Method::GET, "/api/v1/health") => {
            let response = json!({
                "status": "ok",
                "message": "GraphDB REST API is healthy"
            });
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }

        // Version endpoint (replace with real version if needed)
        (&hyper::Method::GET, "/api/v1/version") => {
            let response = json!({
                "version": env!("CARGO_PKG_VERSION")
            });
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }

        // POST to run a Cypher/SQL/GraphQL query and return parsed type
        (&hyper::Method::POST, "/api/v1/query") => {
            let whole_body = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
            let input: serde_json::Value = serde_json::from_slice(&whole_body).unwrap_or_default();
            let query = input.get("query").and_then(|q| q.as_str()).unwrap_or("").to_string();
            let mut response = json!({
                "status": "error",
                "message": "No query provided"
            });

            if !query.is_empty() {
                match parse_query_from_string(&query) {
                    Ok(query_type) => {
                        let message = match query_type {
                            QueryType::Cypher => format!("Cypher query detected: {}", query),
                            QueryType::SQL => format!("SQL query detected: {}", query),
                            QueryType::GraphQL => format!("GraphQL query detected: {}", query),
                        };
                        response = json!({
                            "status": "success",
                            "message": message
                        });
                    }
                    Err(e) => {
                        response = json!({
                            "status": "error",
                            "message": format!("Failed to parse query: {}", e)
                        });
                    }
                }
            }

            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }

        // Start daemon on a port (always skip the REST API port)
        (&hyper::Method::POST, path) if path.starts_with("/api/v1/start/port/") => {
            let port_str = path.strip_prefix("/api/v1/start/port/").unwrap();
            match port_str.parse::<u16>() {
                Ok(port) => {
                    if port == rest_api_port {
                        let response = json!({
                            "status": "error",
                            "message": format!("Port {} is reserved for the REST API", port)
                        });
                        log_rest_event(&format!("REST API: Attempt to start daemon on REST API port {port} rejected"));
                        return Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(serde_json::to_string(&response).unwrap()))
                            .unwrap());
                    }
                    log_rest_event(&format!("REST API: Received request to start daemon on port {port}"));
                    let skip_ports = vec![rest_api_port];
                    let result = tokio::task::spawn_blocking(move || {
                        start_daemon(Some(port), None, skip_ports)
                    }).await.unwrap();
                    match result {
                        Ok(()) => {
                            let response = json!({
                                "status": "success",
                                "message": format!("Daemon started on port {}", port)
                            });
                            log_rest_event(&format!("REST API: Daemon started on port {port} (success)"));
                            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                        }
                        Err(e) => {
                            let response = json!({
                                "status": "error",
                                "message": format!("Failed to start daemon on port {}: {:?}", port, e)
                            });
                            log_rest_event(&format!("REST API: Failed to start daemon on port {port}: {e:?}"));
                            Ok(Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                .unwrap())
                        }
                    }
                }
                Err(_) => {
                    let response = json!({
                        "status": "error",
                        "message": "Invalid port number"
                    });
                    log_rest_event("REST API: Invalid port number requested");
                    Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::from(serde_json::to_string(&response).unwrap()))
                        .unwrap())
                }
            }
        }
        // Start a cluster of daemons (always skip the REST API port)
        (&hyper::Method::POST, path) if path.starts_with("/api/v1/start/cluster/") => {
            let range_str = path.strip_prefix("/api/v1/start/cluster/").unwrap();
            let parts: Vec<&str> = range_str.split('-').collect();
            if parts.len() == 2 {
                match (parts[0].parse::<u16>(), parts[1].parse::<u16>()) {
                    (Ok(start_port), Ok(end_port)) if start_port <= end_port && end_port - start_port <= 10 => {
                        if (start_port..=end_port).contains(&rest_api_port) {
                            let response = json!({
                                "status": "error",
                                "message": format!("Cluster range {}-{} overlaps REST API port {}", start_port, end_port, rest_api_port)
                            });
                            log_rest_event(&format!("REST API: Attempt to start cluster overlapping REST API port {rest_api_port} rejected"));
                            return Ok(Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(Body::from(serde_json::to_string(&response).unwrap()))
                                .unwrap());
                        }
                        log_rest_event(&format!("REST API: Received request to start cluster {}-{}", start_port, end_port));
                        let cluster = Some(format!("{}-{}", start_port, end_port));
                        let skip_ports = vec![rest_api_port];
                        let result = tokio::task::spawn_blocking(move || {
                            start_daemon(None, cluster, skip_ports)
                        }).await.unwrap();
                        match result {
                            Ok(()) => {
                                let response = json!({
                                    "status": "success",
                                    "message": format!("Daemon cluster started on ports {}-{}", start_port, end_port)
                                });
                                log_rest_event(&format!("REST API: Cluster started on ports {}-{} (success)", start_port, end_port));
                                Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
                            }
                            Err(e) => {
                                let response = json!({
                                    "status": "error",
                                    "message": format!("Failed to start daemon cluster on ports {}-{}: {:?}", start_port, end_port, e)
                                });
                                log_rest_event(&format!("REST API: Failed to start cluster on ports {}-{}: {e:?}", start_port, end_port));
                                Ok(Response::builder()
                                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                                    .unwrap())
                            }
                        }
                    }
                    _ => {
                        let response = json!({
                            "status": "error",
                            "message": "Invalid port range or range exceeds 10 ports"
                        });
                        log_rest_event("REST API: Invalid cluster range requested or range exceeds 10 ports");
                        Ok(Response::builder()
                            .status(StatusCode::BAD_REQUEST)
                            .body(Body::from(serde_json::to_string(&response).unwrap()))
                            .unwrap())
                    }
                }
            } else {
                let response = json!({
                    "status": "error",
                    "message": "Invalid cluster range format. Use start-end (e.g., 9001-9005)"
                });
                log_rest_event("REST API: Invalid cluster range format");
                Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(serde_json::to_string(&response).unwrap()))
                    .unwrap())
            }
        }
        // Stop all daemons
        (&hyper::Method::POST, "/api/v1/stop") => {
            log_rest_event("REST API: Received request to stop all daemons");
            stop_daemon();
            let response = json!({
                "status": "success",
                "message": "All daemons and REST API server stopped"
            });
            *shutdown_flag.lock().unwrap() = true;
            log_rest_event("REST API: All daemons and REST API server stopped (success)");
            Ok(Response::new(Body::from(serde_json::to_string(&response).unwrap())))
        }
        // Catch-all for unknown endpoints
        _ => {
            let response = json!({
                "status": "error",
                "message": "Endpoint not found or method not allowed"
            });
            log_rest_event(&format!("REST API: Unknown endpoint or method: {} {}", method, path));
            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(serde_json::to_string(&response).unwrap()))
                .unwrap())
        }
    }
}

/// Launch the REST API server on the specified port, with graceful shutdown.
/// Call this from your CLI or main daemon entry point.
pub async fn start_server(
    listen_port: u16,
    shutdown_rx: tokio::sync::oneshot::Receiver<()>
) -> Result<(), hyper::Error> {
    let addr = SocketAddr::from(([127, 0, 0, 1], listen_port));
    let shutdown_flag = Arc::new(Mutex::new(false));
    let shutdown_flag_for_service = Arc::clone(&shutdown_flag);
    let make_service = make_service_fn(move |_conn| {
        let shutdown_flag = Arc::clone(&shutdown_flag_for_service);
        let rest_api_port = listen_port;
        async move {
            Ok::<_, Infallible>(service_fn(move |req| handle_request(req, Arc::clone(&shutdown_flag), rest_api_port)))
        }
    });
    let server = Server::bind(&addr)
        .serve(make_service)
        .with_graceful_shutdown(async {
            tokio::select! {
                _ = shutdown_rx => {},
                _ = async {
                    while !*shutdown_flag.lock().unwrap() {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                } => {},
            }
        });
    println!("REST API server running on http://{}", addr);
    log_rest_event(&format!("REST API server running on http://{}", addr));
    server.await
}

use std::net::SocketAddr;
use tokio::signal;
use graphdb_rest_api::start_server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set the listening address and port for the REST API server
    let port: u16 = 8082;
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("GraphDB REST API server starting on http://{}", addr);

    // Set up a shutdown signal
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Spawn a task to listen for Ctrl+C and signal shutdown
    tokio::spawn(async move {
        if signal::ctrl_c().await.is_ok() {
            let _ = shutdown_tx.send(());
        }
    });

    // Start the REST API server and await its completion or shutdown
    start_server(port, shutdown_rx).await?;

    println!("GraphDB REST API server stopped.");
    Ok(())
}

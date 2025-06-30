use rest_api::start_server;
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    env_logger::init(); // Initialize logger

    let listen_port = 8082;
    println!("GraphDB REST API server starting on http://127.0.0.1:{}", listen_port);

    // Channel for graceful shutdown. Not strictly necessary if daemon_api::stop_daemon
    // also handles server process termination, but good practice for Hyper.
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Spawn a task to listen for shutdown signals (e.g., from an API endpoint)
    // This allows the server to shut down cleanly if the stop_daemon() is called.
    // However, the stop_daemon in lib.rs already sets a flag for Hyper's graceful_shutdown,
    // so this oneshot is mainly for external shutdown signals.
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
        eprintln!("\nCtrl-C received, shutting down server...");
        let _ = shutdown_tx.send(());
    });

    start_server(listen_port, shutdown_rx).await
}

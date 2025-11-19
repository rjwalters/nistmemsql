use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

mod config;
mod connection;
mod protocol;
mod session;

use config::Config;
use connection::ConnectionHandler;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Load configuration
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("Warning: Could not load config file: {}", e);
        eprintln!("Using default configuration");
        Config::default()
    });

    info!("Starting VibeSQL Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Configuration:");
    info!("  Host: {}", config.server.host);
    info!("  Port: {}", config.server.port);
    info!("  Max connections: {}", config.server.max_connections);
    info!("  SSL enabled: {}", config.server.ssl_enabled);

    // Bind to address
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .expect("Invalid server address");

    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);

    // Share configuration across handlers
    let config = Arc::new(config);

    loop {
        // Accept new connections
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!("New connection from {}", peer_addr);

                let config = Arc::clone(&config);

                // Spawn a new task for each connection
                tokio::spawn(async move {
                    let mut handler = ConnectionHandler::new(stream, peer_addr, config);
                    if let Err(e) = handler.handle().await {
                        error!("Connection error from {}: {}", peer_addr, e);
                    }
                    info!("Connection closed: {}", peer_addr);
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}

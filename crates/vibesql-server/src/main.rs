use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

mod config;
mod connection;
mod observability;
mod protocol;
mod session;

use config::Config;
use connection::ConnectionHandler;
use observability::ObservabilityProvider;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration first (needed for observability setup)
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("Warning: Could not load config file: {}", e);
        eprintln!("Using default configuration");
        Config::default()
    });

    // Initialize observability (this sets up tracing subscriber if configured)
    let observability = ObservabilityProvider::init(&config.observability)?;

    // Initialize basic tracing if observability didn't set it up
    if !config.observability.enabled || !config.observability.logs.bridge_tracing {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| {
                        tracing_subscriber::EnvFilter::new(config.logging.level.to_lowercase())
                    }),
            )
            .try_init()
            .ok(); // Ignore error if already initialized
    }

    info!("Starting VibeSQL Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Configuration:");
    info!("  Host: {}", config.server.host);
    info!("  Port: {}", config.server.port);
    info!("  Max connections: {}", config.server.max_connections);
    info!("  SSL enabled: {}", config.server.ssl_enabled);
    info!("  Observability enabled: {}", config.observability.enabled);

    // Bind to address
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .expect("Invalid server address");

    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);

    // Share configuration and observability across handlers
    let config = Arc::new(config);
    let observability = Arc::new(observability);

    loop {
        // Accept new connections
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!("New connection from {}", peer_addr);

                let config = Arc::clone(&config);
                let observability = Arc::clone(&observability);

                // Record connection metric
                if let Some(metrics) = observability.metrics() {
                    metrics.record_connection();
                }

                // Spawn a new task for each connection
                tokio::spawn(async move {
                    let mut handler =
                        ConnectionHandler::new(stream, peer_addr, config, observability);
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

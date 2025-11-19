//! OpenTelemetry observability module for vibesql-server
//!
//! This module provides comprehensive observability through OpenTelemetry,
//! including metrics, distributed tracing, and structured logging.
//!
//! # Features
//!
//! - **Metrics**: Connection counts, query latency, error rates
//! - **Traces**: Distributed tracing through query execution pipeline
//! - **Logs**: Structured logs correlated with traces
//!
//! # Configuration
//!
//! Observability is configured through the `[observability]` section in
//! `vibesql-server.toml`. See `config.rs` for available options.
//!
//! # Example
//!
//! ```no_run
//! use vibesql_server::observability::{ObservabilityConfig, ObservabilityProvider};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = ObservabilityConfig::default();
//! let provider = ObservabilityProvider::init(&config)?;
//!
//! // Use metrics
//! if let Some(metrics) = provider.metrics() {
//!     metrics.record_connection();
//! }
//!
//! // Shutdown cleanly
//! provider.shutdown()?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod metrics;
pub mod provider;

pub use config::ObservabilityConfig;
#[allow(unused_imports)] // Re-exported for public API
pub use metrics::ServerMetrics;
pub use provider::ObservabilityProvider;

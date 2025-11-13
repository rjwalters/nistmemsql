// ============================================================================
// Database Module
// ============================================================================

mod core;
mod lifecycle;
mod metadata;
mod operations;
mod resource_tracker;

pub mod indexes;
pub mod transactions;

#[cfg(test)]
mod tests;

pub use core::{Database, ExportedSpatialIndexMetadata as SpatialIndexMetadata};
pub use operations::SpatialIndexMetadata as OperationsSpatialIndexMetadata;

pub use indexes::{IndexData, IndexManager, IndexMetadata};
pub use resource_tracker::{IndexBackend, IndexStats, ResourceTracker};
pub use transactions::{Savepoint, TransactionChange, TransactionManager, TransactionState};

/// Configuration for database resource budgets
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Maximum memory for indexes and buffer pools (bytes)
    pub memory_budget: usize,

    /// Maximum disk space for database files (bytes)
    pub disk_budget: usize,

    /// Policy for handling memory budget violations
    pub spill_policy: SpillPolicy,
}

/// Policy for what to do when memory budget is exceeded
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillPolicy {
    /// Reject CREATE INDEX if it would exceed budget
    Reject,

    /// Automatically spill cold indexes from memory to disk
    SpillToDisk,

    /// Best effort - try to allocate, graceful degradation
    BestEffort,
}

impl DatabaseConfig {
    /// Default configuration for browser/WASM environments
    /// - 512MB memory budget (conservative for browsers)
    /// - 2GB disk budget (typical OPFS quota)
    /// - SpillToDisk policy (automatic eviction)
    pub fn browser_default() -> Self {
        DatabaseConfig {
            memory_budget: 512 * 1024 * 1024,  // 512MB
            disk_budget: 2 * 1024 * 1024 * 1024,  // 2GB
            spill_policy: SpillPolicy::SpillToDisk,
        }
    }

    /// Default configuration for server environments
    /// - 16GB memory budget (abundant server RAM)
    /// - 1TB disk budget (generous server storage)
    /// - BestEffort policy (prefer memory, fall back to disk)
    pub fn server_default() -> Self {
        DatabaseConfig {
            memory_budget: 16 * 1024 * 1024 * 1024,  // 16GB
            disk_budget: 1024 * 1024 * 1024 * 1024,  // 1TB
            spill_policy: SpillPolicy::BestEffort,
        }
    }

    /// Minimal configuration for testing
    /// - 10MB memory budget (force eviction quickly)
    /// - 100MB disk budget
    /// - SpillToDisk policy
    pub fn test_default() -> Self {
        DatabaseConfig {
            memory_budget: 10 * 1024 * 1024,  // 10MB
            disk_budget: 100 * 1024 * 1024,  // 100MB
            spill_policy: SpillPolicy::SpillToDisk,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        // Default to server configuration (most permissive)
        Self::server_default()
    }
}

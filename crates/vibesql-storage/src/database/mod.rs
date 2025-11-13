// ============================================================================
// Database Module
// ============================================================================

mod core;
mod lifecycle;
mod metadata;
mod operations;
pub mod indexes;
pub mod transactions;

pub use core::{Database, ExportedSpatialIndexMetadata as SpatialIndexMetadata};
pub use operations::SpatialIndexMetadata as OperationsSpatialIndexMetadata;

pub use indexes::{IndexData, IndexManager, IndexMetadata};
pub use transactions::{Savepoint, TransactionChange, TransactionManager, TransactionState};

// ============================================================================
// Index Management Module - User-defined index operations
// ============================================================================
//
// This module has been refactored into focused submodules for better
// maintainability and code organization:
//
// - index_metadata: Types and helpers for index definitions
// - index_operations: Query methods on IndexData (range_scan, multi_lookup, etc.)
// - index_maintenance: CRUD operations (create, drop, rebuild, update)
// - index_manager: Core IndexManager coordination and queries

mod index_metadata;
mod index_operations;
mod index_maintenance;
mod index_manager;

// Re-export public API
pub use index_metadata::{IndexData, IndexMetadata};
pub use index_manager::IndexManager;

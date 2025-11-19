//! Index optimization strategies for SELECT queries
//!
//! This module provides optimizations that leverage database indexes for:
//! - WHERE clause filtering (Spatial indexes)
//! - ORDER BY sorting (handled at scan level in scan/index_scan.rs)
//!
//! These optimizations can significantly improve query performance by reducing
//! the amount of data that needs to be processed.
//!
//! NOTE: B-tree index optimization has been moved to the scan level (scan/index_scan.rs)
//! to avoid row-index mismatch problems when predicate pushdown is enabled.
//! This module now only contains spatial index optimization as a special case.

#[cfg(feature = "spatial")]
mod spatial;

#[cfg(feature = "spatial")]
pub(in crate::select::executor) use spatial::try_spatial_index_optimization;

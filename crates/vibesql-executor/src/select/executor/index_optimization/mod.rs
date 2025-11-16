//! Index optimization strategies for SELECT queries
//!
//! This module provides optimizations that leverage database indexes for:
//! - WHERE clause filtering (B-tree indexes)
//! - WHERE clause filtering (Spatial indexes)
//! - ORDER BY sorting (now handled at scan level in scan/index_scan.rs)
//!
//! These optimizations can significantly improve query performance by reducing
//! the amount of data that needs to be processed.

mod where_filter;
mod spatial;

pub(in crate::select::executor) use where_filter::{try_index_for_in_clause, requires_predicate_pushdown_disable};
pub(in crate::select::executor) use spatial::try_spatial_index_optimization;

//! GROUP BY operations and aggregate function evaluation
//!
//! This module provides:
//! - Aggregate function accumulators (COUNT, SUM, AVG, MIN, MAX)
//! - Hash-based grouping implementation
//! - SQL value comparison and arithmetic helpers

mod aggregates;
mod hash;

// Re-export public API
pub(super) use aggregates::{AggregateAccumulator, compare_sql_values};
pub(super) use hash::group_rows;

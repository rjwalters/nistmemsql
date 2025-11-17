//! Table and column statistics for query optimization
//!
//! This module provides statistics collection and estimation for:
//! - Table cardinality (row counts)
//! - Column cardinality (distinct values)
//! - Value distributions (min/max, most common values)
//! - Selectivity estimation for predicates
//!
//! Statistics are used by the query optimizer for:
//! - JOIN order selection
//! - Index selection
//! - Predicate pushdown prioritization
//! - Cost estimation

mod table;
mod column;

pub use table::TableStatistics;
pub use column::ColumnStatistics;

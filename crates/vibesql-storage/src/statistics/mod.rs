//! Table and column statistics for query optimization
//!
//! This module provides statistics collection and estimation for:
//! - Table cardinality (row counts)
//! - Column cardinality (distinct values)
//! - Value distributions (min/max, most common values)
//! - Selectivity estimation for predicates
//! - Cost estimation for access methods
//!
//! Statistics are used by the query optimizer for:
//! - JOIN order selection
//! - Index selection (cost-based)
//! - Predicate pushdown prioritization
//! - Cost estimation

mod table;
mod column;
mod cost;

pub use table::TableStatistics;
pub use column::ColumnStatistics;
pub use cost::{CostEstimator, AccessMethod};

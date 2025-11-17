//! Table and column statistics for query optimization
//!
//! This module provides statistics collection and estimation for:
//! - Table cardinality (row counts)
//! - Column cardinality (distinct values)
//! - Value distributions (min/max, most common values)
//! - Selectivity estimation for predicates
//! - Cost estimation for access methods
//! - Histogram-based selectivity (Phase 5.1)
//! - Sampling for large tables (Phase 5.2)
//!
//! Statistics are used by the query optimizer for:
//! - JOIN order selection
//! - Index selection (cost-based)
//! - Predicate pushdown prioritization
//! - Cost estimation

mod table;
mod column;
mod cost;
mod sampling;
mod histogram;

pub use table::TableStatistics;
pub use column::ColumnStatistics;
pub use cost::{CostEstimator, AccessMethod};
pub use sampling::{SamplingConfig, SampleSize, SamplingMethod, SampleMetadata};
pub use histogram::{Histogram, HistogramBucket, BucketStrategy};

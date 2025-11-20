//! Generic monomorphic execution patterns
//!
//! This module provides generic monomorphic execution plans that match on query
//! characteristics (structure, aggregation type, etc.) rather than specific table
//! or column names.
//!
//! Unlike TPC-H-specific plans which hardcode table names and column indices,
//! these generic patterns:
//! - Match any table with compatible schema
//! - Extract filter predicates dynamically from WHERE clause
//! - Resolve column indices from schema at plan creation time
//! - Still use unchecked accessors for maximum performance
//!
//! This provides the same performance benefits (~230ns/row improvement) while
//! working for any user query with similar structure.
//!
//! ## Module Organization
//!
//! The generic execution module is organized into three main submodules:
//!
//! - [`filter`]: Filter predicate extraction and evaluation infrastructure
//! - [`aggregation`]: Generic single-table aggregation plans (SUM, COUNT without GROUP BY)
//! - [`group_by`]: Generic GROUP BY aggregation plans with multiple aggregate functions

// Submodules
pub mod aggregation;
pub mod filter;
pub mod group_by;

// Re-export commonly used types for backwards compatibility
pub use aggregation::{
    AggregationSpec, GenericFilteredAggregationMatcher, GenericFilteredAggregationPlan,
};
pub use filter::{extract_filters, optimize_date_ranges, ComparisonOp, FilterPredicate};
pub use group_by::{GenericGroupedAggregationPlan, GroupAggregateSpec, GroupByColumnSpec};

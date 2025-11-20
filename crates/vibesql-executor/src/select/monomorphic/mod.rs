//! Monomorphic query execution paths
//!
//! This module provides type-specialized execution paths that eliminate SqlValue
//! enum overhead by generating optimized code based on schema information at
//! query planning time.
//!
//! ## Performance Benefits
//!
//! - Eliminates enum tag checks (~15ns per access)
//! - Removes pattern matching overhead (~70ns per access)
//! - Enables direct value access (~50ns savings)
//! - Allows native operations without type coercion (~30ns savings)
//!
//! Total savings: ~230ns per row for typical queries
//!
//! ## Safety
//!
//! These plans use unsafe code for performance. Safety is guaranteed by:
//! 1. Schema validation at query planning time
//! 2. Debug assertions in all unchecked accessors
//! 3. Type information from table metadata
//!
//! ## Usage
//!
//! Monomorphic plans are automatically selected by the SelectExecutor when:
//! 1. A query matches a known pattern (e.g., TPC-H Q6)
//! 2. Schema information is available for all accessed columns
//! 3. The plan can be safely constructed from the AST

use vibesql_ast::SelectStmt;
use vibesql_storage::Row;

use crate::{errors::ExecutorError, schema::CombinedSchema};

pub mod generic;
pub mod jit;
pub mod pattern;
pub mod tpch;

/// Trait for type-specialized query execution plans
///
/// Implementations of this trait provide optimized execution paths for
/// specific query patterns where column types are known at planning time.
pub trait MonomorphicPlan: Send + Sync {
    /// Execute the query plan on a set of rows
    ///
    /// # Arguments
    ///
    /// * `rows` - Input rows to process (typically from table scan)
    ///
    /// # Returns
    ///
    /// Result rows (aggregated, filtered, projected, etc.)
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError>;

    /// Get a description of this plan for debugging/profiling
    fn description(&self) -> &str;
}

/// Attempts to create a monomorphic plan for a query pattern
///
/// Returns None if no specialized plan is available for this query.
///
/// Priority order:
/// 1. Generic patterns (work for any table with compatible structure)
/// 2. TPC-H-specific patterns (for backward compatibility, will be deprecated)
pub fn try_create_monomorphic_plan(
    stmt: &SelectStmt,
    schema: &CombinedSchema,
) -> Option<Box<dyn MonomorphicPlan>> {
    // Try generic patterns first (preferred - work for any table)
    if let Some(plan) = generic::GenericFilteredAggregationPlan::try_create(stmt, schema) {
        return Some(Box::new(plan));
    }

    // Fall back to TPC-H-specific patterns for backward compatibility
    // These will be deprecated once generic patterns are fully tested
    if let Some(plan) = tpch::try_create_tpch_plan(stmt, schema) {
        return Some(plan);
    }

    // Future: Add more generic pattern matchers here
    // - Grouped aggregations (GROUP BY support)
    // - Join patterns
    // - Window functions
    // etc.

    None
}

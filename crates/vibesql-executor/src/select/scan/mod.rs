//! FROM clause scanning logic
//!
//! Handles execution of FROM clauses including:
//! - Table scans (regular tables and CTEs)
//! - JOIN operations (delegates to join module)
//! - Derived tables (subqueries)
//! - Predicate pushdown for WHERE clause optimization
//! - Join order optimization (experimental, opt-in via JOIN_REORDER_ENABLED)

use std::collections::HashMap;

use super::{cte::CteResult, join::FromResult};
use crate::errors::ExecutorError;

// Strategy modules
mod derived;
mod join_scan;
mod predicates;
mod reorder;
mod table;

/// Execute a FROM clause (table, join, or subquery) and return combined schema and rows
///
/// This function handles all types of FROM clauses:
/// - Simple table references (with optional alias)
/// - CTEs (Common Table Expressions)
/// - JOIN operations (INNER, LEFT, RIGHT, FULL)
/// - Derived tables (subqueries with alias)
///
/// The WHERE clause is passed for predicate pushdown optimization:
/// - Table-local predicates are applied during table scan
/// - Equijoin predicates can be pushed into join operations
/// - Complex predicates remain in post-join WHERE
///
/// NEW: Join reordering optimization (opt-in via JOIN_REORDER_ENABLED):
/// - For multi-table joins (3+ tables), analyzes join conditions
/// - Uses cost-based search to find optimal join order
/// - Minimizes intermediate result sizes
pub(super) fn execute_from_clause<F>(
    from: &vibesql_ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<Vec<vibesql_storage::Row>, ExecutorError> + Copy,
{
    // Check if this is a multi-table join that could benefit from reordering
    if matches!(from, vibesql_ast::FromClause::Join { .. }) {
        let table_count = reorder::count_tables_in_from(from);
        // Only apply reordering if:
        // 1. Environment variable is set (opt-in)
        // 2. We have 3+ tables
        // 3. All joins are INNER or CROSS (safe to reorder)
        if reorder::should_apply_join_reordering(table_count) && reorder::all_joins_are_inner_or_cross(from) {
            // Apply join reordering optimization
            return reorder::execute_with_join_reordering(from, cte_results, database, where_clause, execute_subquery);
        }
    }

    // Fall back to standard execution (recursive left-deep joins)
    match from {
        vibesql_ast::FromClause::Table { name, alias } => {
            table::execute_table_scan(name, alias.as_ref(), cte_results, database, where_clause)
        }
        vibesql_ast::FromClause::Join { left, right, join_type, condition } => join_scan::execute_join(
            left,
            right,
            join_type,
            condition,
            cte_results,
            database,
            where_clause,
            execute_subquery,
        ),
        vibesql_ast::FromClause::Subquery { query, alias } => {
            derived::execute_derived_table(query, alias, execute_subquery)
        }
    }
}

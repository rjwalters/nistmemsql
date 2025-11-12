//! JOIN execution logic for FROM clause scanning
//!
//! Handles execution of JOIN operations within FROM clauses by:
//! - Recursively executing left and right sides
//! - Extracting equijoin predicates from WHERE clause
//! - Delegating to nested loop join implementation

use std::collections::HashMap;

use crate::{
    errors::ExecutorError, optimizer::decompose_where_clause, select::cte::CteResult,
};

/// Execute a JOIN operation
pub(crate) fn execute_join<F>(
    left: &vibesql_ast::FromClause,
    right: &vibesql_ast::FromClause,
    join_type: &vibesql_ast::JoinType,
    condition: &Option<vibesql_ast::Expression>,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    execute_subquery: F,
) -> Result<super::FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<Vec<vibesql_storage::Row>, ExecutorError> + Copy,
{
    // Execute left and right sides with WHERE clause for predicate pushdown
    let left_result = super::execute_from_clause(left, cte_results, database, where_clause, execute_subquery)?;
    let right_result = super::execute_from_clause(right, cte_results, database, where_clause, execute_subquery)?;

    // If we have a WHERE clause, decompose it using the combined schema
    let equijoin_predicates = if let Some(where_expr) = where_clause {
        // Build combined schema for WHERE clause analysis
        let mut combined_schema = left_result.schema.clone();
        for (table_name, table_schema) in &right_result.schema.table_schemas {
            combined_schema.table_schemas.insert(table_name.clone(), table_schema.clone());
        }

        // Decompose WHERE clause with full schema
        let decomposition = decompose_where_clause(Some(where_expr), &combined_schema)
            .map_err(|e| ExecutorError::InvalidWhereClause(e))?;

        // Extract equijoin conditions that apply to this join
        let left_schema_tables: std::collections::HashSet<_> =
            left_result.schema.table_schemas.keys().cloned().collect();
        let right_schema_tables: std::collections::HashSet<_> =
            right_result.schema.table_schemas.keys().cloned().collect();

        decomposition
            .equijoin_conditions
            .into_iter()
            .filter_map(|(left_table, _left_col, right_table, _right_col, expr)| {
                // Check if this equijoin connects tables from left and right
                let left_in_left = left_schema_tables.contains(&left_table);
                let right_in_right = right_schema_tables.contains(&right_table);
                let right_in_left = left_schema_tables.contains(&right_table);
                let left_in_right = right_schema_tables.contains(&left_table);

                if (left_in_left && right_in_right) || (right_in_left && left_in_right) {
                    Some(expr)
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    // Perform nested loop join with equijoin predicates from WHERE clause
    use crate::select::join::nested_loop_join;
    let result = nested_loop_join(
        left_result,
        right_result,
        join_type,
        condition,
        database,
        &equijoin_predicates,
    )?;
    Ok(result)
}

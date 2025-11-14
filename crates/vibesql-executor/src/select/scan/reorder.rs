//! Join reordering optimization (experimental)
//!
//! Provides cost-based join reordering for multi-table queries:
//! - Analyzes join conditions and WHERE predicates
//! - Uses greedy search to find optimal join order
//! - Minimizes intermediate result sizes
//!
//! This optimization is opt-in via JOIN_REORDER_ENABLED environment variable.

use std::collections::{HashMap, HashSet};

use super::{derived::execute_derived_table, table::execute_table_scan};
use crate::{
    errors::ExecutorError,
    select::{
        cte::CteResult,
        join::{nested_loop_join, JoinOrderAnalyzer, JoinOrderSearch},
        SelectResult,
    },
};

/// Check if join reordering optimization should be applied
///
/// Controlled by JOIN_REORDER_ENABLED environment variable (opt-in for safety)
pub(crate) fn should_apply_join_reordering(table_count: usize) -> bool {
    // Must have at least 3 tables for reordering to be beneficial
    if table_count < 3 {
        return false;
    }

    // Check environment variable (opt-in)
    std::env::var("JOIN_REORDER_ENABLED").is_ok()
}

/// Count the number of tables in a FROM clause (including nested joins)
pub(crate) fn count_tables_in_from(from: &vibesql_ast::FromClause) -> usize {
    match from {
        vibesql_ast::FromClause::Table { .. } => 1,
        vibesql_ast::FromClause::Subquery { .. } => 1,
        vibesql_ast::FromClause::Join { left, right, .. } => {
            count_tables_in_from(left) + count_tables_in_from(right)
        }
    }
}

/// Check if all joins in the tree are CROSS or INNER joins
///
/// Join reordering is only safe for CROSS and INNER joins. LEFT/RIGHT/FULL OUTER
/// joins have specific semantics that must be preserved.
pub(crate) fn all_joins_are_inner_or_cross(from: &vibesql_ast::FromClause) -> bool {
    match from {
        vibesql_ast::FromClause::Table { .. } | vibesql_ast::FromClause::Subquery { .. } => true,
        vibesql_ast::FromClause::Join { left, right, join_type, .. } => {
            let is_inner_or_cross = matches!(join_type, vibesql_ast::JoinType::Inner | vibesql_ast::JoinType::Cross);
            is_inner_or_cross
                && all_joins_are_inner_or_cross(left)
                && all_joins_are_inner_or_cross(right)
        }
    }
}

/// Information about a table extracted from a FROM clause
#[derive(Debug, Clone)]
struct TableRef {
    name: String,
    alias: Option<String>,
    #[allow(dead_code)]
    is_cte: bool,
    is_subquery: bool,
    subquery: Option<Box<vibesql_ast::SelectStmt>>,
}

/// Flatten a nested join tree into a list of table references
fn flatten_join_tree(from: &vibesql_ast::FromClause, tables: &mut Vec<TableRef>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias } => {
            tables.push(TableRef {
                name: name.clone(),
                alias: alias.clone(),
                is_cte: false,
                is_subquery: false,
                subquery: None,
            });
        }
        vibesql_ast::FromClause::Subquery { query, alias } => {
            tables.push(TableRef {
                name: alias.clone(),
                alias: Some(alias.clone()),
                is_cte: false,
                is_subquery: true,
                subquery: Some(query.clone()),
            });
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            flatten_join_tree(left, tables);
            flatten_join_tree(right, tables);
        }
    }
}

/// Extract all join conditions and WHERE predicates from a FROM clause
fn extract_all_conditions(from: &vibesql_ast::FromClause, conditions: &mut Vec<vibesql_ast::Expression>) {
    match from {
        vibesql_ast::FromClause::Table { .. } | vibesql_ast::FromClause::Subquery { .. } => {
            // No conditions in simple table refs
        }
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            // Add this join's condition
            if let Some(cond) = condition {
                conditions.push(cond.clone());
            }
            // Recurse into nested joins
            extract_all_conditions(left, conditions);
            extract_all_conditions(right, conditions);
        }
    }
}

/// Apply join reordering optimization to a multi-table join
///
/// This function:
/// 1. Flattens the join tree to extract all tables
/// 2. Analyzes join conditions and WHERE predicates
/// 3. Uses cost-based search to find optimal join order
/// 4. Builds and executes joins in the optimal order
pub(crate) fn execute_with_join_reordering<F>(
    from: &vibesql_ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    execute_subquery: F,
) -> Result<super::FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<SelectResult, ExecutorError> + Copy,
{
    // Step 1: Flatten join tree to extract all tables
    let mut table_refs = Vec::new();
    flatten_join_tree(from, &mut table_refs);

    // Step 2: Extract all join conditions
    let mut join_conditions = Vec::new();
    extract_all_conditions(from, &mut join_conditions);

    // Step 3: Build analyzer with table names
    let table_names: Vec<String> =
        table_refs.iter().map(|t| t.alias.clone().unwrap_or_else(|| t.name.clone())).collect();

    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(table_names.clone());

    // Combine table names into a set for predicate analysis
    let table_set: HashSet<String> = table_names.iter().cloned().collect();

    // Step 4: Analyze join conditions to extract edges
    for condition in &join_conditions {
        analyzer.analyze_predicate(condition, &table_set);
    }

    // Step 5: Analyze WHERE clause predicates if available
    if let Some(where_expr) = where_clause {
        analyzer.analyze_predicate(where_expr, &table_set);
    }

    // Step 6: Use search to find optimal join order
    let search = JoinOrderSearch::from_analyzer(&analyzer);
    let optimal_order = search.find_optimal_order();

    // Log the reordering decision (optional, for debugging)
    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
        eprintln!("[JOIN_REORDER] Original order: {:?}", table_names);
        eprintln!("[JOIN_REORDER] Optimal order:  {:?}", optimal_order);
    }

    // Step 7: Build a map from table name to TableRef for easy lookup
    let table_map: HashMap<String, TableRef> = table_refs
        .into_iter()
        .map(|t| {
            let key = t.alias.clone().unwrap_or_else(|| t.name.clone());
            (key, t)
        })
        .collect();

    // Step 8: Execute tables in optimal order, joining them sequentially
    let mut result: Option<super::FromResult> = None;

    for table_name in &optimal_order {
        let table_ref = table_map.get(table_name).ok_or_else(|| {
            ExecutorError::UnsupportedFeature(format!("Table not found in map: {}", table_name))
        })?;

        // Execute this table
        let table_result = if table_ref.is_subquery {
            if let Some(subquery) = &table_ref.subquery {
                execute_derived_table(subquery, table_name, execute_subquery)?
            } else {
                return Err(ExecutorError::UnsupportedFeature(
                    "Subquery reference missing query".to_string(),
                ));
            }
        } else {
            execute_table_scan(&table_ref.name, table_ref.alias.as_ref(), cte_results, database, where_clause)?
        };

        // Join with previous result (if any)
        if let Some(prev_result) = result {
            // Find join conditions that connect prev tables with this table
            // For now, use cross join and let conditions filter
            // TODO: Extract specific join conditions for this table pair
            result = Some(nested_loop_join(
                prev_result,
                table_result,
                &vibesql_ast::JoinType::Cross, // Use cross join, filtered by conditions
                &None,
                false, // Not a NATURAL JOIN
                database,
                &join_conditions, // Pass all conditions as equijoin filters
            )?);
        } else {
            result = Some(table_result);
        }
    }

    result.ok_or_else(|| ExecutorError::UnsupportedFeature("No tables in join".to_string()))
}

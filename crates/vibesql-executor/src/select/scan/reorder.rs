//! Join reordering optimization
//!
//! Provides cost-based join reordering for multi-table queries:
//! - Analyzes join conditions and WHERE predicates
//! - Uses exhaustive search with pruning to find optimal join order
//! - Minimizes intermediate result sizes
//!
//! This optimization is enabled by default for 3+ table INNER/CROSS joins.
//! Can be disabled via JOIN_REORDER_DISABLED environment variable.

use std::collections::{HashMap, HashSet};

use super::{derived::execute_derived_table, table::execute_table_scan, FromResult};
use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::{
        cte::CteResult,
        join::{nested_loop_join, JoinOrderAnalyzer, JoinOrderSearch},
        SelectResult,
    },
};

/// Check if join reordering optimization should be applied
///
/// Enabled by default for 3+ table joins. Can be disabled via JOIN_REORDER_DISABLED env var.
pub(crate) fn should_apply_join_reordering(table_count: usize) -> bool {
    // Must have at least 3 tables for reordering to be beneficial
    if table_count < 3 {
        return false;
    }

    // Allow opt-out via environment variable if needed
    std::env::var("JOIN_REORDER_DISABLED").is_err()
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

/// Check if all joins in the tree are CROSS joins (comma-list syntax)
///
/// Join reordering changes column ordering, so we only apply it to implicit CROSS joins
/// from comma-list syntax (FROM t1, t2, t3). Explicit INNER/LEFT/RIGHT joins must
/// preserve their declared ordering.
pub(crate) fn all_joins_are_cross(from: &vibesql_ast::FromClause) -> bool {
    match from {
        vibesql_ast::FromClause::Table { .. } | vibesql_ast::FromClause::Subquery { .. } => true,
        vibesql_ast::FromClause::Join { left, right, join_type, .. } => {
            matches!(join_type, vibesql_ast::JoinType::Cross)
                && all_joins_are_cross(left)
                && all_joins_are_cross(right)
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
/// 5. Restores original column ordering to preserve query semantics
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

    // Step 3: Build analyzer with table names (preserving original order)
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

    // Step 6: Use search to find optimal join order (with real statistics)
    let search = JoinOrderSearch::from_analyzer(&analyzer, database);
    let optimal_order = search.find_optimal_order();

    // Log the reordering decision (optional, for debugging)
    if std::env::var("JOIN_REORDER_VERBOSE").is_ok() {
        eprintln!("[JOIN_REORDER] Original order: {:?}", table_names);
        eprintln!("[JOIN_REORDER] Optimal order:  {:?}", optimal_order);
    }

    // Step 7: Build a map from table name to TableRef for easy lookup
    // IMPORTANT: Normalize keys to lowercase to match analyzer's normalization
    let table_map: HashMap<String, TableRef> = table_refs
        .into_iter()
        .map(|t| {
            let key = t.alias.clone().unwrap_or_else(|| t.name.clone()).to_lowercase();
            (key, t)
        })
        .collect();

    // Step 8: Track column count per table for later column reordering
    let mut table_column_counts: HashMap<String, usize> = HashMap::new();

    // Step 9: Execute tables in optimal order, joining them sequentially
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
            execute_table_scan(&table_ref.name, table_ref.alias.as_ref(), cte_results, database, where_clause, None)?
        };

        // Record the column count for this table (using table_schemas to get column info)
        let col_count = if let Some((_, schema)) = table_result.schema.table_schemas.get(table_name) {
            schema.columns.len()
        } else {
            table_result.schema.total_columns
        };
        table_column_counts.insert(table_name.clone(), col_count);

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

    let result = result.ok_or_else(|| ExecutorError::UnsupportedFeature("No tables in join".to_string()))?;

    // Step 10: Restore original column ordering if needed
    // Build column permutation: map from current position to target position
    let column_permutation = build_column_permutation(&table_names, &optimal_order, &table_column_counts);

    // Reorder rows according to the permutation
    let rows = result.data.into_rows();
    let reordered_rows: Vec<vibesql_storage::Row> = rows
        .into_iter()
        .map(|row| {
            let mut new_values = Vec::with_capacity(row.values.len());
            for &idx in &column_permutation {
                new_values.push(row.values[idx].clone());
            }
            vibesql_storage::Row::new(new_values)
        })
        .collect();

    // Build a new combined schema with tables in original order
    let new_schema = build_reordered_schema(&result.schema, &table_names, &optimal_order);

    // Return result with reordered data and schema
    Ok(FromResult::from_rows(new_schema, reordered_rows))
}

/// Build a reordered combined schema with tables in original order
///
/// Takes the current schema (with tables in optimal order) and reconstructs it
/// with tables in the original FROM clause order.
fn build_reordered_schema(
    current_schema: &CombinedSchema,
    original_order: &[String],
    _optimal_order: &[String],
) -> CombinedSchema {
    let mut new_table_schemas = HashMap::new();
    let mut current_position = 0;

    // Walk through original order and rebuild schema with correct positions
    for table_name in original_order {
        let table_lower = table_name.to_lowercase();

        // Find this table's schema in the current (optimally ordered) schema
        // Try exact match first, then case-insensitive
        let table_schema = current_schema
            .table_schemas
            .get(table_name)
            .or_else(|| {
                current_schema.table_schemas.iter().find_map(|(k, v): (&String, &(usize, vibesql_catalog::TableSchema))| {
                    if k.to_lowercase() == table_lower {
                        Some(v)
                    } else {
                        None
                    }
                })
            })
            .map(|(_, schema): &(usize, vibesql_catalog::TableSchema)| schema.clone());

        if let Some(schema) = table_schema {
            let col_count = schema.columns.len();
            new_table_schemas.insert(table_name.clone(), (current_position, schema));
            current_position += col_count;
        }
    }

    CombinedSchema { table_schemas: new_table_schemas, total_columns: current_position }
}

/// Build a column permutation to restore original table ordering
///
/// Given:
/// - Original table order: [tab0, tab2, tab1]
/// - Optimal execution order: [tab1, tab0, tab2]
/// - Column counts: {tab0: 3, tab1: 3, tab2: 3}
///
/// Returns permutation mapping current positions to original positions:
/// - Current: [tab1.col0, tab1.col1, tab1.col2, tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2]
/// - Target:  [tab0.col0, tab0.col1, tab0.col2, tab2.col0, tab2.col1, tab2.col2, tab1.col0, tab1.col1, tab1.col2]
/// - Permutation: [3, 4, 5, 6, 7, 8, 0, 1, 2]
fn build_column_permutation(
    original_order: &[String],
    optimal_order: &[String],
    column_counts: &HashMap<String, usize>,
) -> Vec<usize> {
    // Build position map: table name -> starting column index in optimal order
    let mut optimal_positions: HashMap<String, usize> = HashMap::new();
    let mut current_position = 0;
    for table in optimal_order {
        optimal_positions.insert(table.clone(), current_position);
        current_position += column_counts.get(table).unwrap_or(&0);
    }

    // Build permutation by walking through original order
    let mut permutation = Vec::new();
    for table in original_order {
        let table_lower = table.to_lowercase();
        let start_pos = optimal_positions.get(&table_lower).unwrap_or(&0);
        let col_count = column_counts.get(&table_lower).unwrap_or(&0);

        // Add all column indices for this table
        for i in 0..*col_count {
            permutation.push(start_pos + i);
        }
    }

    permutation
}

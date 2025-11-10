//! FROM clause scanning logic
//!
//! Handles execution of FROM clauses including:
//! - Table scans (regular tables and CTEs)
//! - JOIN operations (delegates to join module)
//! - Derived tables (subqueries)
//! - Predicate pushdown for WHERE clause optimization
//! - Join order optimization (experimental, opt-in via JOIN_REORDER_ENABLED)

use std::collections::{HashMap, HashSet};

use super::{
    cte::CteResult,
    join::{nested_loop_join, FromResult, JoinOrderAnalyzer, JoinOrderSearch},
};
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator,
    optimizer::decompose_where_clause, privilege_checker::PrivilegeChecker, schema::CombinedSchema,
};

/// Check if join reordering optimization should be applied
///
/// Controlled by JOIN_REORDER_ENABLED environment variable (opt-in for safety)
fn should_apply_join_reordering(table_count: usize) -> bool {
    // Must have at least 3 tables for reordering to be beneficial
    if table_count < 3 {
        return false;
    }

    // Check environment variable (opt-in)
    std::env::var("JOIN_REORDER_ENABLED").is_ok()
}

/// Count the number of tables in a FROM clause (including nested joins)
fn count_tables_in_from(from: &ast::FromClause) -> usize {
    match from {
        ast::FromClause::Table { .. } => 1,
        ast::FromClause::Subquery { .. } => 1,
        ast::FromClause::Join { left, right, .. } => {
            count_tables_in_from(left) + count_tables_in_from(right)
        }
    }
}

/// Check if all joins in the tree are CROSS or INNER joins
///
/// Join reordering is only safe for CROSS and INNER joins. LEFT/RIGHT/FULL OUTER
/// joins have specific semantics that must be preserved.
fn all_joins_are_inner_or_cross(from: &ast::FromClause) -> bool {
    match from {
        ast::FromClause::Table { .. } | ast::FromClause::Subquery { .. } => true,
        ast::FromClause::Join { left, right, join_type, .. } => {
            let is_inner_or_cross = matches!(join_type, ast::JoinType::Inner | ast::JoinType::Cross);
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
    is_cte: bool,
    is_subquery: bool,
    subquery: Option<Box<ast::SelectStmt>>,
}

/// Flatten a nested join tree into a list of table references
fn flatten_join_tree(from: &ast::FromClause, tables: &mut Vec<TableRef>) {
    match from {
        ast::FromClause::Table { name, alias } => {
            tables.push(TableRef {
                name: name.clone(),
                alias: alias.clone(),
                is_cte: false,
                is_subquery: false,
                subquery: None,
            });
        }
        ast::FromClause::Subquery { query, alias } => {
            tables.push(TableRef {
                name: alias.clone(),
                alias: Some(alias.clone()),
                is_cte: false,
                is_subquery: true,
                subquery: Some(query.clone()),
            });
        }
        ast::FromClause::Join { left, right, .. } => {
            flatten_join_tree(left, tables);
            flatten_join_tree(right, tables);
        }
    }
}

/// Extract all join conditions and WHERE predicates from a FROM clause
fn extract_all_conditions(from: &ast::FromClause, conditions: &mut Vec<ast::Expression>) {
    match from {
        ast::FromClause::Table { .. } | ast::FromClause::Subquery { .. } => {
            // No conditions in simple table refs
        }
        ast::FromClause::Join { left, right, condition, .. } => {
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
fn execute_with_join_reordering<F>(
    from: &ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
    where_clause: Option<&ast::Expression>,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> + Copy,
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
    let mut result: Option<FromResult> = None;

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
                &ast::JoinType::Cross, // Use cross join, filtered by conditions
                &None,
                database,
                &join_conditions, // Pass all conditions as equijoin filters
            )?);
        } else {
            result = Some(table_result);
        }
    }

    result.ok_or_else(|| ExecutorError::UnsupportedFeature("No tables in join".to_string()))
}

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
    from: &ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
    where_clause: Option<&ast::Expression>,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> + Copy,
{
    // Check if this is a multi-table join that could benefit from reordering
    if matches!(from, ast::FromClause::Join { .. }) {
        let table_count = count_tables_in_from(from);
        // Only apply reordering if:
        // 1. Environment variable is set (opt-in)
        // 2. We have 3+ tables
        // 3. All joins are INNER or CROSS (safe to reorder)
        if should_apply_join_reordering(table_count) && all_joins_are_inner_or_cross(from) {
            // Apply join reordering optimization
            return execute_with_join_reordering(from, cte_results, database, where_clause, execute_subquery);
        }
    }

    // Fall back to standard execution (recursive left-deep joins)
    match from {
        ast::FromClause::Table { name, alias } => {
            execute_table_scan(name, alias.as_ref(), cte_results, database, where_clause)
        }
        ast::FromClause::Join { left, right, join_type, condition } => execute_join(
            left,
            right,
            join_type,
            condition,
            cte_results,
            database,
            where_clause,
            execute_subquery,
        ),
        ast::FromClause::Subquery { query, alias } => {
            execute_derived_table(query, alias, execute_subquery)
        }
    }
}

/// Execute a table scan (handles CTEs, views, and regular tables)
fn execute_table_scan(
    table_name: &str,
    alias: Option<&String>,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
    where_clause: Option<&ast::Expression>,
) -> Result<FromResult, ExecutorError> {
    // Check if table is a CTE first
    if let Some((cte_schema, cte_rows)) = cte_results.get(table_name) {
        // Use CTE result
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let schema = CombinedSchema::from_table(effective_name, cte_schema.clone());
        let mut rows = cte_rows.clone();

        // Apply table-local predicates from WHERE clause
        if let Some(where_expr) = where_clause {
            rows = apply_table_local_predicates(
                rows,
                schema.clone(),
                where_expr,
                table_name,
                database,
            )?;
        }

        return Ok(FromResult { schema, rows });
    }

    // Check if it's a view
    if let Some(view) = database.catalog.get_view(table_name) {
        // Check SELECT privilege on the view
        PrivilegeChecker::check_select(database, table_name)?;

        // Execute the view's query to get the result
        // We need to execute the entire SELECT statement, not just the FROM clause
        use super::SelectExecutor;
        let executor = SelectExecutor::new(database);

        // Get both rows and column metadata
        let select_result = executor.execute_with_columns(&view.query)?;

        // Build a schema from the column names
        // Since views can have arbitrary SELECT expressions, we derive column types from the first
        // row
        let columns = if !select_result.rows.is_empty() {
            let first_row = &select_result.rows[0];
            select_result
                .columns
                .iter()
                .zip(&first_row.values)
                .map(|(name, value)| {
                    catalog::ColumnSchema {
                        name: name.clone(),
                        data_type: value.get_type(),
                        nullable: true, // Views return nullable columns by default
                        default_value: None,
                    }
                })
                .collect()
        } else {
            // For empty views, create columns without specific types
            // This is a limitation but views with no rows are edge cases
            vec![]
        };

        let view_schema = catalog::TableSchema::new(table_name.to_string(), columns);
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let schema = CombinedSchema::from_table(effective_name, view_schema);
        let mut rows = select_result.rows;

        // Apply table-local predicates from WHERE clause
        if let Some(where_expr) = where_clause {
            rows = apply_table_local_predicates(
                rows,
                schema.clone(),
                where_expr,
                table_name,
                database,
            )?;
        }

        return Ok(FromResult { schema, rows });
    }

    // Check SELECT privilege on the table
    PrivilegeChecker::check_select(database, table_name)?;

    // Use database table
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());
    let mut rows = table.scan().to_vec();

    // Apply table-local predicates from WHERE clause
    if let Some(where_expr) = where_clause {
        rows =
            apply_table_local_predicates(rows, schema.clone(), where_expr, table_name, database)?;
    }

    Ok(FromResult { schema, rows })
}

/// Execute a JOIN operation
fn execute_join<F>(
    left: &ast::FromClause,
    right: &ast::FromClause,
    join_type: &ast::JoinType,
    condition: &Option<ast::Expression>,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
    where_clause: Option<&ast::Expression>,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> + Copy,
{
    // Execute left and right sides first to get their schemas
    let left_result = execute_from_clause(left, cte_results, database, None, execute_subquery)?;
    let right_result = execute_from_clause(right, cte_results, database, None, execute_subquery)?;

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

/// Execute a derived table (subquery with alias)
fn execute_derived_table<F>(
    query: &ast::SelectStmt,
    alias: &str,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError>,
{
    // Execute subquery to get rows
    let rows = execute_subquery(query)?;

    // Derive schema from SELECT list
    let mut column_names = Vec::new();
    let mut column_types = Vec::new();

    let mut col_index = 0;
    for item in &query.select_list {
        match item {
            ast::SelectItem::Wildcard { .. } | ast::SelectItem::QualifiedWildcard { .. } => {
                // For SELECT * or SELECT table.*, expand to all columns from the result rows
                // Since we executed the subquery, the rows tell us how many columns there are
                if let Some(first_row) = rows.first() {
                    for (j, value) in first_row.values.iter().enumerate() {
                        let col_name = format!("column{}", col_index + j + 1);
                        column_names.push(col_name);
                        column_types.push(value.get_type());
                    }
                    col_index += first_row.values.len();
                } else {
                    // No rows, no columns from wildcard
                }
            }
            ast::SelectItem::Expression { expr: _, alias: col_alias } => {
                // Use alias if provided, otherwise generate column name
                let col_name = if let Some(a) = col_alias {
                    a.clone()
                } else {
                    format!("column{}", col_index + 1)
                };
                column_names.push(col_name);

                // Infer type from first row if available
                let col_type = if let Some(first_row) = rows.first() {
                    if col_index < first_row.values.len() {
                        first_row.values[col_index].get_type()
                    } else {
                        types::DataType::Null
                    }
                } else {
                    types::DataType::Null
                };
                column_types.push(col_type);
                col_index += 1;
            }
        }
    }

    // Create schema with table alias
    let schema = CombinedSchema::from_derived_table(alias.to_string(), column_names, column_types);

    Ok(FromResult { schema, rows })
}

/// Apply table-local predicates from WHERE clause during table scan
///
/// This function implements predicate pushdown by filtering rows early,
/// before they contribute to larger Cartesian products in JOINs.
fn apply_table_local_predicates(
    rows: Vec<storage::Row>,
    schema: CombinedSchema,
    where_clause: &ast::Expression,
    table_name: &str,
    database: &storage::Database,
) -> Result<Vec<storage::Row>, ExecutorError> {
    // Decompose WHERE clause using branch-specific API with schema
    let decomposition = decompose_where_clause(Some(where_clause), &schema)
        .map_err(|e| ExecutorError::InvalidWhereClause(e))?;

    // Extract predicates that can be applied to this table
    let table_local_preds: Option<&Vec<ast::Expression>> =
        decomposition.table_local_predicates.get(table_name);

    // If there are table-local predicates, apply them
    if let Some(preds) = table_local_preds {
        if !preds.is_empty() {
            // Combine predicates with AND
            let combined_where = combine_predicates_with_and(preds.clone());

            // Create evaluator for filtering
            let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

            // Apply filtering to rows directly (without executor for timeout checking)
            let mut filtered_rows = Vec::new();
            for row in rows {
                let include_row = match evaluator.eval(&combined_where, &row)? {
                    types::SqlValue::Boolean(true) => true,
                    types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
                    // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
                    types::SqlValue::Integer(0) => false,
                    types::SqlValue::Integer(_) => true,
                    types::SqlValue::Smallint(0) => false,
                    types::SqlValue::Smallint(_) => true,
                    types::SqlValue::Bigint(0) => false,
                    types::SqlValue::Bigint(_) => true,
                    types::SqlValue::Float(f) if f == 0.0 => false,
                    types::SqlValue::Float(_) => true,
                    types::SqlValue::Real(f) if f == 0.0 => false,
                    types::SqlValue::Real(_) => true,
                    types::SqlValue::Double(f) if f == 0.0 => false,
                    types::SqlValue::Double(_) => true,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "WHERE clause must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                };

                if include_row {
                    filtered_rows.push(row);
                }
            }
            return Ok(filtered_rows);
        }
    }

    // No table-local predicates - return rows as-is
    Ok(rows)
}

/// Helper function to combine predicates with AND operator
fn combine_predicates_with_and(mut predicates: Vec<ast::Expression>) -> ast::Expression {
    if predicates.is_empty() {
        // This shouldn't happen, but default to TRUE
        ast::Expression::Literal(types::SqlValue::Boolean(true))
    } else if predicates.len() == 1 {
        predicates.pop().unwrap()
    } else {
        let mut result = predicates.remove(0);
        for predicate in predicates {
            result = ast::Expression::BinaryOp {
                op: ast::BinaryOperator::And,
                left: Box::new(result),
                right: Box::new(predicate),
            };
        }
        result
    }
}

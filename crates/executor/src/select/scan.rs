//! FROM clause scanning logic
//!
//! Handles execution of FROM clauses including:
//! - Table scans (regular tables and CTEs)
//! - JOIN operations (delegates to join module)
//! - Derived tables (subqueries)
//! - Predicate pushdown for WHERE clause optimization

use std::collections::HashMap;

use crate::errors::ExecutorError;
use crate::privilege_checker::PrivilegeChecker;
use crate::schema::CombinedSchema;
use crate::evaluator::CombinedExpressionEvaluator;
use crate::optimizer::{decompose_where_predicates, get_table_local_predicates, get_equijoin_predicates, combine_with_and, get_predicates_for_tables};

use super::cte::CteResult;
use super::join::{nested_loop_join, FromResult};

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
    match from {
        ast::FromClause::Table { name, alias } => {
            execute_table_scan(name, alias.as_ref(), cte_results, database, where_clause)
        }
        ast::FromClause::Join { left, right, join_type, condition } => {
            execute_join(left, right, join_type, condition, cte_results, database, where_clause, execute_subquery)
        }
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
            rows = apply_table_local_predicates(rows, schema.clone(), where_expr, table_name, database)?;
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
        // Since views can have arbitrary SELECT expressions, we derive column types from the first row
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
            rows = apply_table_local_predicates(rows, schema.clone(), where_expr, table_name, database)?;
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
        rows = apply_table_local_predicates(rows, schema.clone(), where_expr, table_name, database)?;
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
    // Decompose WHERE clause once for filtering by branch
    let predicates = where_clause.map(decompose_where_predicates);
    
    // Get table names for each branch to filter predicates
    let (left_tables, right_tables) = get_branch_tables(left, right, cte_results, database)?;
    
    // Filter WHERE clause for each branch - only pass relevant predicates
    let left_where = if let Some(ref preds) = predicates {
        let filtered_preds = get_predicates_for_tables(preds, &left_tables);
        combine_with_and(filtered_preds)
    } else {
        None
    };
    
    let right_where = if let Some(ref preds) = predicates {
        let filtered_preds = get_predicates_for_tables(preds, &right_tables);
        combine_with_and(filtered_preds)
    } else {
        None
    };
    
    // Execute left and right sides with filtered WHERE clauses
    let left_result = execute_from_clause(left, cte_results, database, left_where.as_ref(), execute_subquery)?;
    let right_result = execute_from_clause(right, cte_results, database, right_where.as_ref(), execute_subquery)?;

    // Extract equijoin predicates from WHERE clause that apply to this join
    let equijoin_predicates = if let Some(ref preds) = predicates {
        let all_equijoins = get_equijoin_predicates(preds);
        
        // Filter to only equijoins that reference tables in left and right schemas
        let left_schema_tables: std::collections::HashSet<_> = left_result.schema.table_schemas.keys().cloned().collect();
        let right_schema_tables: std::collections::HashSet<_> = right_result.schema.table_schemas.keys().cloned().collect();
        
        all_equijoins.into_iter().filter(|eq_pred| {
            // Check if this equijoin references tables in both left and right
            matches_join_condition(&eq_pred, &left_schema_tables, &right_schema_tables)
        }).collect()
    } else {
        Vec::new()
    };

    // Perform nested loop join with equijoin predicates from WHERE clause
    let result = nested_loop_join(left_result, right_result, join_type, condition, database, &equijoin_predicates)?;
    Ok(result)
}

/// Determine what tables are in the left and right branches of a join
fn get_branch_tables(
    left: &ast::FromClause,
    right: &ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
) -> Result<(std::collections::HashSet<String>, std::collections::HashSet<String>), ExecutorError> {
    let mut left_tables = std::collections::HashSet::new();
    let mut right_tables = std::collections::HashSet::new();
    
    collect_table_names(left, &mut left_tables, cte_results, database);
    collect_table_names(right, &mut right_tables, cte_results, database);
    
    Ok((left_tables, right_tables))
}

/// Recursively collect all table names referenced in a FROM clause
fn collect_table_names(
    from: &ast::FromClause,
    tables: &mut std::collections::HashSet<String>,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
) {
    match from {
        ast::FromClause::Table { name, alias } => {
            let effective_name = alias.as_ref().cloned().unwrap_or_else(|| name.clone());
            tables.insert(effective_name);
        }
        ast::FromClause::Join { left, right, .. } => {
            collect_table_names(left, tables, cte_results, database);
            collect_table_names(right, tables, cte_results, database);
        }
        ast::FromClause::Subquery { alias, .. } => {
            tables.insert(alias.clone());
        }
    }
}

/// Check if an equijoin expression references tables from both left and right schema groups
fn matches_join_condition(
    expr: &ast::Expression,
    left_tables: &std::collections::HashSet<String>,
    right_tables: &std::collections::HashSet<String>,
) -> bool {
    let mut left_refs = std::collections::HashSet::new();
    let mut right_refs = std::collections::HashSet::new();
    
    extract_referenced_tables(expr, &mut left_refs, &mut right_refs, left_tables, right_tables);
    
    !left_refs.is_empty() && !right_refs.is_empty()
}

/// Extract table references from an expression, categorizing by left/right tables
fn extract_referenced_tables(
    expr: &ast::Expression,
    left_refs: &mut std::collections::HashSet<String>,
    right_refs: &mut std::collections::HashSet<String>,
    left_tables: &std::collections::HashSet<String>,
    right_tables: &std::collections::HashSet<String>,
) {
    match expr {
        ast::Expression::ColumnRef { table: Some(table), .. } => {
            if left_tables.contains(table) {
                left_refs.insert(table.clone());
            } else if right_tables.contains(table) {
                right_refs.insert(table.clone());
            }
        }
        ast::Expression::BinaryOp { left, right, .. } => {
            extract_referenced_tables(left, left_refs, right_refs, left_tables, right_tables);
            extract_referenced_tables(right, left_refs, right_refs, left_tables, right_tables);
        }
        ast::Expression::UnaryOp { expr: inner, .. } => {
            extract_referenced_tables(inner, left_refs, right_refs, left_tables, right_tables);
        }
        _ => {}
    }
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
    // Decompose WHERE clause into pushdown-friendly predicates
    let predicates = decompose_where_predicates(where_clause);
    
    // Extract predicates that can be applied to this table
    let table_local_preds = get_table_local_predicates(&predicates, table_name);
    
    // If there are table-local predicates, apply them
    if !table_local_preds.is_empty() {
        if let Some(combined_where) = combine_with_and(table_local_preds) {
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

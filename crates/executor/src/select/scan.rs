//! FROM clause scanning logic
//!
//! Handles execution of FROM clauses including:
//! - Table scans (regular tables and CTEs)
//! - JOIN operations (delegates to join module)
//! - Derived tables (subqueries)

use std::collections::HashMap;

use crate::errors::ExecutorError;
use crate::privilege_checker::PrivilegeChecker;
use crate::schema::CombinedSchema;

use super::cte::CteResult;
use super::join::{nested_loop_join, FromResult};

/// Execute a FROM clause (table, join, or subquery) and return combined schema and rows
///
/// This function handles all types of FROM clauses:
/// - Simple table references (with optional alias)
/// - CTEs (Common Table Expressions)
/// - JOIN operations (INNER, LEFT, RIGHT, FULL)
/// - Derived tables (subqueries with alias)
pub(super) fn execute_from_clause<F>(
    from: &ast::FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> + Copy,
{
    eprintln!(
        "DEBUG FROM CLAUSE: {:?}",
        match from {
            ast::FromClause::Table { name, alias } => format!("Table({:?}, {:?})", name, alias),
            ast::FromClause::Join { join_type, .. } => format!("Join({:?})", join_type),
            ast::FromClause::Subquery { alias, .. } => format!("Subquery({:?})", alias),
        }
    );

    match from {
        ast::FromClause::Table { name, alias } => {
            execute_table_scan(name, alias.as_ref(), cte_results, database)
        }
        ast::FromClause::Join { left, right, join_type, condition } => {
            execute_join(left, right, join_type, condition, cte_results, database, execute_subquery)
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
) -> Result<FromResult, ExecutorError> {
    // Check if table is a CTE first
    if let Some((cte_schema, cte_rows)) = cte_results.get(table_name) {
        // Use CTE result
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        eprintln!(
            "DEBUG SCAN CTE: table_name={}, alias={:?}, effective_name={}",
            table_name, alias, effective_name
        );
        let schema = CombinedSchema::from_table(effective_name, cte_schema.clone());
        let rows = cte_rows.clone();
        return Ok(FromResult { schema, rows });
    }

    // Check if it's a view
    if let Some(view) = database.catalog.get_view(table_name) {
        eprintln!("DEBUG SCAN VIEW: table_name={}, alias={:?}", table_name, alias);

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
            select_result.columns.iter().zip(&first_row.values).map(|(name, value)| {
                catalog::ColumnSchema {
                    name: name.clone(),
                    data_type: value.get_type(),
                    nullable: true, // Views return nullable columns by default
                    default_value: None,
                }
            }).collect()
        } else {
            // For empty views, create columns without specific types
            // This is a limitation but views with no rows are edge cases
            vec![]
        };

        let view_schema = catalog::TableSchema::new(table_name.to_string(), columns);
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let schema = CombinedSchema::from_table(effective_name, view_schema);

        return Ok(FromResult { schema, rows: select_result.rows });
    }

    // Check SELECT privilege on the table
    PrivilegeChecker::check_select(database, table_name)?;

    // Use database table
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    eprintln!(
        "DEBUG SCAN TABLE: table_name={}, alias={:?}, effective_name={}",
        table_name, alias, effective_name
    );
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());
    let rows = table.scan().to_vec();

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
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&ast::SelectStmt) -> Result<Vec<storage::Row>, ExecutorError> + Copy,
{
    eprintln!("DEBUG JOIN: join_type={:?}", join_type);

    // Execute left and right sides recursively
    let left_result = execute_from_clause(left, cte_results, database, execute_subquery)?;
    eprintln!(
        "DEBUG JOIN: left schema keys={:?}",
        left_result.schema.table_schemas.keys().collect::<Vec<_>>()
    );

    let right_result = execute_from_clause(right, cte_results, database, execute_subquery)?;
    eprintln!(
        "DEBUG JOIN: right schema keys={:?}",
        right_result.schema.table_schemas.keys().collect::<Vec<_>>()
    );

    // Perform nested loop join
    let result = nested_loop_join(left_result, right_result, join_type, condition, database)?;
    eprintln!(
        "DEBUG JOIN: result schema keys={:?}",
        result.schema.table_schemas.keys().collect::<Vec<_>>()
    );
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
            ast::SelectItem::Wildcard | ast::SelectItem::QualifiedWildcard { .. } => {
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

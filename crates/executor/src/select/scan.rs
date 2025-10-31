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

/// Execute a table scan (handles both regular tables and CTEs)
fn execute_table_scan(
    table_name: &str,
    alias: Option<&String>,
    cte_results: &HashMap<String, CteResult>,
    database: &storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Check if table is a CTE first, then check database
    if let Some((cte_schema, cte_rows)) = cte_results.get(table_name) {
        // Use CTE result
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        eprintln!(
            "DEBUG SCAN CTE: table_name={}, alias={:?}, effective_name={}",
            table_name, alias, effective_name
        );
        let schema = CombinedSchema::from_table(effective_name, cte_schema.clone());
        let rows = cte_rows.clone();
        Ok(FromResult { schema, rows })
    } else {
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

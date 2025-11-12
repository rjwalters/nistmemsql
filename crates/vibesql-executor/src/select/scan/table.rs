//! Table scanning logic
//!
//! Handles execution of simple table scans including:
//! - Regular database tables
//! - CTEs (Common Table Expressions)
//! - Views
//! - Predicate pushdown optimization

use std::collections::HashMap;

use super::predicates::apply_table_local_predicates;
use crate::{
    errors::ExecutorError, optimizer::decompose_where_clause, privilege_checker::PrivilegeChecker,
    schema::CombinedSchema, select::cte::CteResult,
};

/// Execute a table scan (handles CTEs, views, and regular tables)
pub(crate) fn execute_table_scan(
    table_name: &str,
    alias: Option<&String>,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
) -> Result<super::FromResult, ExecutorError> {
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

        return Ok(super::FromResult::from_rows(schema, rows));
    }

    // Check if it's a view
    if let Some(view) = database.catalog.get_view(table_name) {
        // Check SELECT privilege on the view
        PrivilegeChecker::check_select(database, table_name)?;

        // Execute the view's query to get the result
        // We need to execute the entire SELECT statement, not just the FROM clause
        use crate::select::SelectExecutor;
        let executor = SelectExecutor::new(database);

        // Get both rows and column metadata
        let select_result = executor.execute_with_columns(&view.query)?;

        // Build a schema from the column names
        // Apply view's explicit column aliases if provided
        let column_names = if let Some(ref view_columns) = view.columns {
            // Use view's explicit column names
            view_columns.clone()
        } else {
            // Use column names from the SELECT statement
            select_result.columns.clone()
        };

        // Since views can have arbitrary SELECT expressions, we derive column types from the first row
        let columns = if !select_result.rows.is_empty() {
            let first_row = &select_result.rows[0];
            column_names
                .iter()
                .zip(&first_row.values)
                .map(|(name, value)| {
                    vibesql_catalog::ColumnSchema {
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
            column_names
                .into_iter()
                .map(|name| vibesql_catalog::ColumnSchema {
                    name,
                    data_type: vibesql_types::DataType::Varchar { max_length: None },
                    nullable: true,
                    default_value: None,
                })
                .collect()
        };

        let view_schema = vibesql_catalog::TableSchema::new(table_name.to_string(), columns);
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

        return Ok(super::FromResult::from_rows(schema, rows));
    }

    // Check SELECT privilege on the table
    PrivilegeChecker::check_select(database, table_name)?;

    // Check if we should use an index scan
    if let Some(index_name) = super::index_scan::should_use_index_scan(table_name, where_clause, database) {
        // Use index scan for potentially better performance
        return super::index_scan::execute_index_scan(table_name, &index_name, alias, where_clause, database);
    }

    // Use database table (fall back to table scan)
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());
    let rows = table.scan().to_vec();

    // Check if we need to apply table-local predicates
    if let Some(where_expr) = where_clause {
        // Check if there are actually table-local predicates for this table
        let decomposition = decompose_where_clause(Some(where_expr), &schema)
            .map_err(ExecutorError::InvalidWhereClause)?;

        if let Some(preds) = decomposition.table_local_predicates.get(table_name) {
            if !preds.is_empty() {
                // Have table-local predicates: materialize and filter
                let filtered_rows = apply_table_local_predicates(
                    rows,
                    schema.clone(),
                    where_expr,
                    table_name,
                    database,
                )?;
                return Ok(super::FromResult::from_rows(schema, filtered_rows));
            }
        }
    }

    // No table-local predicates or no WHERE clause: return iterator for lazy evaluation
    use crate::select::from_iterator::FromIterator;
    Ok(super::FromResult::from_iterator(schema, FromIterator::from_table_scan(rows)))
}

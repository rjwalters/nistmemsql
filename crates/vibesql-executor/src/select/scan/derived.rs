//! Derived table (subquery) scanning logic
//!
//! Handles execution of subqueries in FROM clauses (derived tables)
//! by executing the subquery and wrapping it with an alias.

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Execute a derived table (subquery with alias)
pub(crate) fn execute_derived_table<F>(
    query: &vibesql_ast::SelectStmt,
    alias: &str,
    execute_subquery: F,
) -> Result<super::FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
{
    // Execute subquery to get rows
    let rows = execute_subquery(query)?;

    // Derive schema from SELECT list
    let mut column_names = Vec::new();
    let mut column_types = Vec::new();

    let mut col_index = 0;
    for item in &query.select_list {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. } | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
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
            vibesql_ast::SelectItem::Expression { expr: _, alias: col_alias } => {
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
                        vibesql_types::DataType::Null
                    }
                } else {
                    vibesql_types::DataType::Null
                };
                column_types.push(col_type);
                col_index += 1;
            }
        }
    }

    // Create schema with table alias
    let schema = CombinedSchema::from_derived_table(alias.to_string(), column_names, column_types);

    Ok(super::FromResult::from_rows(schema, rows))
}

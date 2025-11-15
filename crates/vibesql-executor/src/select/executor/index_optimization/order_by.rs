//! Index-based ORDER BY optimization
//!
//! Enhanced to support:
//! - Multi-column ORDER BY
//! - Reverse index traversal (ASC index used for DESC ordering)
//! - Mixed ASC/DESC directions when index supports them

use vibesql_storage::database::Database;
use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::{grouping::compare_sql_values, order::RowWithSortKeys},
};

/// Try to use an index for ORDER BY optimization
/// Returns ordered rows if an index can be used, None otherwise
pub(in crate::select::executor) fn try_index_based_ordering(
    _database: &Database,
    _rows: &[RowWithSortKeys],
    _order_by: &[vibesql_ast::OrderByItem],
    _schema: &CombinedSchema,
    _from_clause: &Option<vibesql_ast::FromClause>,
    _select_list: &[vibesql_ast::SelectItem],
) -> Result<Option<Vec<RowWithSortKeys>>, ExecutorError> {
    // TEMPORARILY DISABLED: Index-based ORDER BY optimization has bugs that cause
    // rows to be dropped and incorrect ordering. Falling back to regular sorting.
    // See issue #1744
    Ok(None)
}

/// Find an index that can be used for multi-column ordering
///
/// Returns (index_name, needs_reverse) where:
/// - index_name: The name of the index to use (or None if no suitable index found)
/// - needs_reverse: True if the index traversal should be reversed
pub(in crate::select::executor) fn find_index_for_multi_column_ordering(
    database: &Database,
    table_name: &str,
    column_names: &[String],
    directions: &[vibesql_ast::OrderDirection],
) -> Result<Option<(String, bool)>, ExecutorError> {
    use vibesql_ast::OrderDirection;

    // Look through all indexes to find one that matches
    let all_indexes = database.list_indexes();
    for index_name in all_indexes {
        if let Some(metadata) = database.get_index(&index_name) {
            // Check if this index is on the correct table
            if metadata.table_name != table_name {
                continue;
            }

            // Check if the index covers all ORDER BY columns in the same order
            if metadata.columns.len() < column_names.len() {
                continue; // Index doesn't have enough columns
            }

            // Check if the first N columns of the index match our ORDER BY columns
            let mut columns_match = true;
            for (i, col_name) in column_names.iter().enumerate() {
                if metadata.columns[i].column_name != *col_name {
                    columns_match = false;
                    break;
                }
            }

            if !columns_match {
                continue;
            }

            // Now check if the directions match (either exactly or all reversed)
            // Case 1: All directions match exactly
            let mut exact_match = true;
            for (i, dir) in directions.iter().enumerate() {
                if metadata.columns[i].direction != *dir {
                    exact_match = false;
                    break;
                }
            }

            if exact_match {
                return Ok(Some((index_name, false))); // Use index as-is
            }

            // Case 2: All directions are opposite (can reverse traversal)
            let mut all_opposite = true;
            for (i, dir) in directions.iter().enumerate() {
                let expected_opposite = match dir {
                    OrderDirection::Asc => OrderDirection::Desc,
                    OrderDirection::Desc => OrderDirection::Asc,
                };
                if metadata.columns[i].direction != expected_opposite {
                    all_opposite = false;
                    break;
                }
            }

            if all_opposite {
                return Ok(Some((index_name, true))); // Use index with reversal
            }

            // Index columns match but directions don't match the two patterns we support
            // Continue looking for a better index
        }
    }

    // Check if this is a primary key (implicit ASC index)
    if directions.len() == 1 && directions[0] == OrderDirection::Asc {
        if let Some(table) = database.get_table(&format!("public.{}", table_name)) {
            if let Some(pk_columns) = &table.schema.primary_key {
                if pk_columns.len() == 1 && pk_columns[0] == column_names[0] {
                    // Return a special name for primary key index
                    return Ok(Some((format!("__pk_{}", table_name), false)));
                }
            }
        }
    } else if directions.len() == 1 && directions[0] == OrderDirection::Desc {
        // Can use primary key index with reversal for single-column DESC
        if let Some(table) = database.get_table(&format!("public.{}", table_name)) {
            if let Some(pk_columns) = &table.schema.primary_key {
                if pk_columns.len() == 1 && pk_columns[0] == column_names[0] {
                    return Ok(Some((format!("__pk_{}", table_name), true)));
                }
            }
        }
    }

    Ok(None)
}

/// Resolve ORDER BY expression to handle positional references and aliases
///
/// Converts:
/// - ORDER BY 1 -> ORDER BY <first column in SELECT list>
/// - ORDER BY alias -> ORDER BY <expression aliased in SELECT list>
/// - Otherwise returns the original expression
fn resolve_order_by_expression<'a>(
    order_expr: &'a vibesql_ast::Expression,
    select_list: &'a [vibesql_ast::SelectItem],
) -> Result<&'a vibesql_ast::Expression, ExecutorError> {
    // Handle positional reference (ORDER BY 1, ORDER BY 2, etc.)
    if let vibesql_ast::Expression::Literal(SqlValue::Integer(pos)) = order_expr {
        // Convert to 0-indexed
        let index = (*pos as usize).checked_sub(1).ok_or_else(|| ExecutorError::Other(
            format!("Invalid ORDER BY position: {} (must be >= 1)", pos)
        ))?;

        // Check if position is within SELECT list bounds
        if index >= select_list.len() {
            return Err(ExecutorError::Other(format!(
                "ORDER BY position {} is out of range (SELECT list has {} columns)",
                pos,
                select_list.len()
            )));
        }

        // Get the expression at this position in the SELECT list
        return match &select_list[index] {
            vibesql_ast::SelectItem::Expression { expr, .. } => Ok(expr),
            vibesql_ast::SelectItem::Wildcard { .. } | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                Err(ExecutorError::Other(
                    format!("Cannot use ORDER BY position {} with wildcard in SELECT list", pos)
                ))
            }
        };
    }

    // Handle alias reference (ORDER BY alias_name)
    if let vibesql_ast::Expression::ColumnRef { table: None, column } = order_expr {
        // Search for matching alias in SELECT list
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, alias: Some(alias_name) } = item {
                if alias_name == column {
                    // Found matching alias, use the SELECT list expression
                    return Ok(expr);
                }
            }
        }
    }

    // Not a positional reference or alias - return original expression
    Ok(order_expr)
}

//! Index-based ORDER BY optimization
//!
//! Enhanced to support:
//! - Multi-column ORDER BY
//! - Reverse index traversal (ASC index used for DESC ordering)
//! - Mixed ASC/DESC directions when index supports them

use std::collections::HashMap;

use vibesql_storage::database::{Database, IndexData};
use vibesql_types::SqlValue;

use crate::{
    errors::ExecutorError,
    schema::CombinedSchema,
    select::{grouping::compare_sql_values, order::RowWithSortKeys},
};

/// Try to use an index for ORDER BY optimization
/// Returns ordered rows if an index can be used, None otherwise
pub(in crate::select::executor) fn try_index_based_ordering(
    database: &Database,
    rows: &[RowWithSortKeys],
    order_by: &[vibesql_ast::OrderByItem],
    schema: &CombinedSchema,
    _from_clause: &Option<vibesql_ast::FromClause>,
) -> Result<Option<Vec<RowWithSortKeys>>, ExecutorError> {
    // Empty ORDER BY - nothing to optimize
    if order_by.is_empty() {
        return Ok(None);
    }

    // Extract column names and directions from ORDER BY
    let mut order_columns = Vec::new();
    let mut order_directions = Vec::new();

    for order_item in order_by {
        // Check if ORDER BY is on a simple column reference
        let column_name = match &order_item.expr {
            vibesql_ast::Expression::ColumnRef { table: None, column } => column,
            _ => return Ok(None), // Complex expressions can't use index
        };
        order_columns.push(column_name.clone());
        order_directions.push(order_item.direction.clone());
    }

    // Find the table that has the first ORDER BY column
    let first_column = &order_columns[0];
    let mut found_table = None;
    for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
        if table_schema.get_column_index(first_column).is_some() {
            found_table = Some(table_name.clone());
            break;
        }
    }

    let table_name = match found_table {
        Some(name) => name,
        None => return Ok(None),
    };

    // Find an index that can be used for this ORDER BY
    let result =
        find_index_for_multi_column_ordering(database, &table_name, &order_columns, &order_directions)?;
    let (index_name, needs_reverse) = match result {
        Some(r) => r,
        None => return Ok(None),
    };

    // Get the index data
    let index_data = if index_name.starts_with("__pk_") {
        // Primary key index
        let table_name = &index_name[5..]; // Remove "__pk_" prefix
        let qualified_table_name = format!("public.{}", table_name);
        if let Some(table) = database.get_table(&qualified_table_name) {
            if let Some(pk_index) = table.primary_key_index() {
                // Convert to IndexData format (HashMap)
                let data: HashMap<Vec<SqlValue>, Vec<usize>> =
                    pk_index.iter().map(|(key, &row_idx)| (key.clone(), vec![row_idx])).collect();
                IndexData { data }
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    } else {
        match database.get_index_data(&index_name) {
            Some(data) => data.clone(),
            None => return Ok(None),
        }
    };

    // Extract ORDER BY column values from filtered rows
    // Build a map: ORDER BY value(s) -> Vec<row position in filtered set>
    let mut value_to_row_positions: HashMap<Vec<SqlValue>, Vec<usize>> = HashMap::new();

    for (row_idx, (row, _)) in rows.iter().enumerate() {
        // Extract the ORDER BY column values from this row
        let mut order_values = Vec::new();
        for col_name in &order_columns {
            // Get column value from the row
            // Find which table this row belongs to
            let mut found_value = None;
            for (_tbl_name, (start_idx, tbl_schema)) in &schema.table_schemas {
                if let Some(col_idx) = tbl_schema.get_column_index(col_name) {
                    let global_col_idx = start_idx + col_idx;
                    if global_col_idx < row.len() {
                        found_value = Some(row.values[global_col_idx].clone());
                        break;
                    }
                }
            }

            if let Some(value) = found_value {
                order_values.push(value);
            } else {
                // Column not found in row, can't use index
                return Ok(None);
            }
        }

        value_to_row_positions.entry(order_values).or_insert_with(Vec::new).push(row_idx);
    }

    // Convert index HashMap to Vec and sort for consistent ordering
    let mut data_vec: Vec<(Vec<SqlValue>, Vec<usize>)> =
        index_data.data.iter().map(|(k, v): (&Vec<SqlValue>, &Vec<usize>)| (k.clone(), v.clone())).collect();

    // Sort by key
    data_vec.sort_by(|(a, _): &(Vec<SqlValue>, Vec<usize>), (b, _): &(Vec<SqlValue>, Vec<usize>)| {
        for (val_a, val_b) in a.iter().zip(b.iter()) {
            match compare_sql_values(val_a, val_b) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }
        std::cmp::Ordering::Equal
    });

    // Reverse if needed (when using ASC index for DESC ordering)
    if needs_reverse {
        data_vec.reverse();
    }

    // Build ordered rows by traversing index and looking up filtered rows
    let mut ordered_rows: Vec<RowWithSortKeys> = Vec::new();
    for (index_key, _) in data_vec {
        // Check if we have any filtered rows with this index key value
        if let Some(row_positions) = value_to_row_positions.get(&index_key) {
            // Add all rows with this key value (handles duplicates)
            for &row_pos in row_positions {
                ordered_rows.push(rows[row_pos].clone());
            }
        }
    }

    Ok(Some(ordered_rows))
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

/// Legacy function kept for backward compatibility
/// Calls the new multi-column function with a single column
pub(in crate::select::executor) fn find_index_for_ordering(
    database: &Database,
    table_name: &str,
    column_name: &str,
    direction: vibesql_ast::OrderDirection,
) -> Result<Option<String>, ExecutorError> {
    let result = find_index_for_multi_column_ordering(
        database,
        table_name,
        &[column_name.to_string()],
        &[direction],
    )?;

    Ok(result.map(|(name, _)| name))
}

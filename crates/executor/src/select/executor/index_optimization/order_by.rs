//! Index-based ORDER BY optimization

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use crate::select::order::RowWithSortKeys;
use crate::select::grouping::compare_sql_values;
use storage::database::{Database, IndexData};
use types::SqlValue;
use std::collections::HashMap;

/// Try to use an index for ORDER BY optimization
/// Returns ordered rows if an index can be used, None otherwise
pub(in crate::select::executor) fn try_index_based_ordering(
    database: &Database,
    rows: &[RowWithSortKeys],
    order_by: &[ast::OrderByItem],
    schema: &CombinedSchema,
    _from_clause: &Option<ast::FromClause>,
) -> Result<Option<Vec<RowWithSortKeys>>, ExecutorError> {
    // For now, only support single-column ORDER BY
    if order_by.len() != 1 {
        return Ok(None);
    }

    let order_item = &order_by[0];

    // Check if ORDER BY is on a simple column reference
    let column_name = match &order_item.expr {
        ast::Expression::ColumnRef { table: None, column } => column,
        _ => return Ok(None), // Complex expressions can't use index
    };

    // For now, assume single table and try to find any table that has this column
    let mut found_table = None;
    for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
        if table_schema.get_column_index(column_name).is_some() {
            found_table = Some(table_name.clone());
            break;
        }
    }

    let table_name = match found_table {
        Some(name) => name,
        None => return Ok(None),
    };

    // Find an index on this table and column
    let index_name = find_index_for_ordering(database, &table_name, column_name, order_item.direction.clone())?;
    if index_name.is_none() {
        return Ok(None);
    }
    let index_name = index_name.unwrap();

    // Get the index data
    let index_data = if index_name.starts_with("__pk_") {
        // Primary key index
        let table_name = &index_name[5..]; // Remove "__pk_" prefix
        let qualified_table_name = format!("public.{}", table_name);
        if let Some(table) = database.get_table(&qualified_table_name) {
            if let Some(pk_index) = table.primary_key_index() {
                // Convert to IndexData format (HashMap)
                let data: HashMap<Vec<SqlValue>, Vec<usize>> = pk_index
                    .iter()
                    .map(|(key, &row_idx)| (key.clone(), vec![row_idx]))
                    .collect();
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

    // For this proof of concept, only use index when we have all rows from the table
    // Check by getting the table and comparing row counts
    let table_row_count = match database.get_table(&table_name) {
        Some(table) => table.row_count(),
        None => return Ok(None),
    };

    if rows.len() != table_row_count {
        // WHERE filtering was applied, can't use index easily
        return Ok(None);
    }

    // All rows are included, we can use the index directly
    // Convert HashMap to Vec and sort for consistent ordering
    let mut data_vec: Vec<(Vec<SqlValue>, Vec<usize>)> = index_data.data.iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Sort by key
    data_vec.sort_by(|(a, _), (b, _)| {
        for (val_a, val_b) in a.iter().zip(b.iter()) {
            match compare_sql_values(val_a, val_b) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            }
        }
        std::cmp::Ordering::Equal
    });

    // Reverse if DESC
    if order_item.direction == ast::OrderDirection::Desc {
        data_vec.reverse();
    }

    // Build ordered rows
    let mut ordered_rows = Vec::new();
    for (_, indices) in data_vec {
        for &row_idx in &indices {
            if row_idx < rows.len() {
                ordered_rows.push(rows[row_idx].clone());
            }
        }
    }

    Ok(Some(ordered_rows))
}

/// Find an index that can be used for ordering by the given column
pub(in crate::select::executor) fn find_index_for_ordering(
    database: &Database,
    table_name: &str,
    column_name: &str,
    direction: ast::OrderDirection,
) -> Result<Option<String>, ExecutorError> {
    // For now, look through all indexes (this is inefficient but works for the proof of concept)
    // In a real implementation, we'd have better index lookup
    let all_indexes = database.list_indexes();
    for index_name in all_indexes {
        if let Some(metadata) = database.get_index(&index_name) {
            if metadata.table_name == table_name
                && metadata.columns.len() == 1
                && metadata.columns[0].column_name == column_name
                && metadata.columns[0].direction == direction {
                return Ok(Some(index_name));
            }
        }
    }

    // Check if this is a primary key column (implicit ASC index)
    if direction == ast::OrderDirection::Asc {
        if let Some(table) = database.get_table(&format!("public.{}", table_name)) {
            if let Some(pk_columns) = &table.schema.primary_key {
                if pk_columns.len() == 1 && pk_columns[0] == column_name {
                    // Return a special name for primary key index
                    return Ok(Some(format!("__pk_{}", table_name)));
                }
            }
        }
    }

    Ok(None)
}

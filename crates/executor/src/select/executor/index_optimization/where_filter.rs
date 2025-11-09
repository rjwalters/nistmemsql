//! Index-based WHERE clause filtering optimization

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use storage::database::Database;

/// Try to use indexes for WHERE clause filtering
/// Returns Some(rows) if index optimization was applied, None if not applicable
pub(in crate::select::executor) fn try_index_based_where_filtering(
    database: &Database,
    where_expr: Option<&ast::Expression>,
    all_rows: &[storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
    let where_expr = match where_expr {
        Some(expr) => expr,
        None => return Ok(None), // No WHERE clause
    };

    // Try to match different predicate patterns
    match where_expr {
        // AND expressions (for BETWEEN pattern) - check first before binary op
        ast::Expression::BinaryOp { op: ast::BinaryOperator::And, left, right } => {
            try_index_for_and_expr(database, left, right, all_rows, schema)
        }
        // Simple binary operations: column OP value
        ast::Expression::BinaryOp { left, op, right } => {
            try_index_for_binary_op(database, left, op, right, all_rows, schema)
        }
        // IN expressions: column IN (val1, val2, ...)
        ast::Expression::InList { expr, values, negated: false } => {
            try_index_for_in_expr(database, expr, values, all_rows, schema)
        }
        // Other expressions not supported for index optimization
        _ => Ok(None),
    }
}

/// Try to use index for binary operation predicates (=, <, >, <=, >=)
pub(in crate::select::executor) fn try_index_for_binary_op(
    database: &Database,
    left: &ast::Expression,
    op: &ast::BinaryOperator,
    right: &ast::Expression,
    all_rows: &[storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
    // Extract column and value
    let (table_name, column_name, value) = match (left, right) {
        (ast::Expression::ColumnRef { table: None, column }, ast::Expression::Literal(val)) => {
            // Find which table this column belongs to
            let mut found_table = None;
            for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
                if _table_schema.get_column_index(column).is_some() {
                    found_table = Some(table.clone());
                    break;
                }
            }
            match found_table {
                Some(table) => (table, column.clone(), val.clone()),
                None => return Ok(None), // Column not found
            }
        }
        _ => return Ok(None), // Not a simple column OP literal
    };

    // Find an index on this table and column
    let index_name = find_index_for_where(database, &table_name, &column_name)?;
    if index_name.is_none() {
        return Ok(None);
    }
    let index_name = index_name.unwrap();

    // Get the index data
    let index_data = match database.get_index_data(&index_name) {
        Some(data) => data,
        None => return Ok(None),
    };

    // Get matching row indices based on operator
    let matching_row_indices = match op {
        ast::BinaryOperator::Equal => {
            // Equality: exact lookup
            let search_key = vec![value];
            index_data.data.get(&search_key).cloned().unwrap_or_else(Vec::new)
        }
        ast::BinaryOperator::GreaterThan => {
            // col > value: use range_scan(Some(value), None, false, false)
            index_data.range_scan(Some(&value), None, false, false)
        }
        ast::BinaryOperator::LessThan => {
            // col < value: use range_scan(None, Some(value), false, false)
            index_data.range_scan(None, Some(&value), false, false)
        }
        ast::BinaryOperator::GreaterThanOrEqual => {
            // col >= value: use range_scan(Some(value), None, true, false)
            index_data.range_scan(Some(&value), None, true, false)
        }
        ast::BinaryOperator::LessThanOrEqual => {
            // col <= value: use range_scan(None, Some(value), false, true)
            index_data.range_scan(None, Some(&value), false, true)
        }
        _ => return Ok(None), // Operator not supported for index optimization
    };

    // Convert row indices to actual rows
    let result_rows = matching_row_indices
        .iter()
        .filter_map(|&row_idx| {
            if row_idx < all_rows.len() {
                Some(all_rows[row_idx].clone())
            } else {
                None
            }
        })
        .collect();

    Ok(Some(result_rows))
}

/// Try to use index for AND expressions (detecting BETWEEN pattern)
pub(in crate::select::executor) fn try_index_for_and_expr(
    database: &Database,
    left: &ast::Expression,
    right: &ast::Expression,
    all_rows: &[storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
    // Try to detect BETWEEN pattern: (col >= start) AND (col <= end)
    // or variations like (col > start) AND (col < end)

    let (col_name, start_val, start_inclusive, end_val, end_inclusive) =
        match (left, right) {
            (
                ast::Expression::BinaryOp {
                    left: left_col,
                    op: left_op,
                    right: left_val
                },
                ast::Expression::BinaryOp {
                    left: right_col,
                    op: right_op,
                    right: right_val
                }
            ) => {
                // Both sides are binary operations
                // Check if both refer to the same column
                let (left_col_name, _right_col_name) = match (left_col.as_ref(), right_col.as_ref()) {
                    (
                        ast::Expression::ColumnRef { table: None, column: lc },
                        ast::Expression::ColumnRef { table: None, column: rc }
                    ) if lc == rc => (lc, rc),
                    _ => return Ok(None), // Not the same column
                };

                // Extract values
                let (left_lit, right_lit) = match (left_val.as_ref(), right_val.as_ref()) {
                    (ast::Expression::Literal(lv), ast::Expression::Literal(rv)) => (lv, rv),
                    _ => return Ok(None), // Not literals
                };

                // Determine the bounds based on operators
                // left is lower bound operation (>= or >)
                // right is upper bound operation (<= or <)
                match (left_op, right_op) {
                    (ast::BinaryOperator::GreaterThanOrEqual, ast::BinaryOperator::LessThanOrEqual) => {
                        (left_col_name.clone(), left_lit.clone(), true, right_lit.clone(), true)
                    }
                    (ast::BinaryOperator::GreaterThanOrEqual, ast::BinaryOperator::LessThan) => {
                        (left_col_name.clone(), left_lit.clone(), true, right_lit.clone(), false)
                    }
                    (ast::BinaryOperator::GreaterThan, ast::BinaryOperator::LessThanOrEqual) => {
                        (left_col_name.clone(), left_lit.clone(), false, right_lit.clone(), true)
                    }
                    (ast::BinaryOperator::GreaterThan, ast::BinaryOperator::LessThan) => {
                        (left_col_name.clone(), left_lit.clone(), false, right_lit.clone(), false)
                    }
                    _ => return Ok(None), // Not a BETWEEN-like pattern
                }
            }
            _ => return Ok(None), // Not a BETWEEN-like pattern
        };

    // Find which table this column belongs to
    let mut found_table = None;
    for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
        if _table_schema.get_column_index(&col_name).is_some() {
            found_table = Some(table.clone());
            break;
        }
    }
    let table_name = match found_table {
        Some(table) => table,
        None => return Ok(None), // Column not found
    };

    // Find an index on this table and column
    let index_name = find_index_for_where(database, &table_name, &col_name)?;
    if index_name.is_none() {
        return Ok(None);
    }
    let index_name = index_name.unwrap();

    // Get the index data
    let index_data = match database.get_index_data(&index_name) {
        Some(data) => data,
        None => return Ok(None),
    };

    // Use range_scan with both bounds
    let matching_row_indices = index_data.range_scan(
        Some(&start_val),
        Some(&end_val),
        start_inclusive,
        end_inclusive,
    );

    // Convert row indices to actual rows
    let result_rows = matching_row_indices
        .iter()
        .filter_map(|&row_idx| {
            if row_idx < all_rows.len() {
                Some(all_rows[row_idx].clone())
            } else {
                None
            }
        })
        .collect();

    Ok(Some(result_rows))
}

/// Try to use index for IN expressions
pub(in crate::select::executor) fn try_index_for_in_expr(
    database: &Database,
    expr: &ast::Expression,
    values: &[ast::Expression],
    all_rows: &[storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<storage::Row>>, ExecutorError> {
    // Extract column name
    let column_name = match expr {
        ast::Expression::ColumnRef { table: None, column } => column,
        _ => return Ok(None), // Not a simple column reference
    };

    // Find which table this column belongs to
    let mut found_table = None;
    for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
        if _table_schema.get_column_index(column_name).is_some() {
            found_table = Some(table.clone());
            break;
        }
    }
    let table_name = match found_table {
        Some(table) => table,
        None => return Ok(None), // Column not found
    };

    // Extract literal values
    let mut literal_values = Vec::new();
    for val_expr in values {
        if let ast::Expression::Literal(val) = val_expr {
            literal_values.push(val.clone());
        } else {
            return Ok(None); // Not all values are literals
        }
    }

    // Find an index on this table and column
    let index_name = find_index_for_where(database, &table_name, column_name)?;
    if index_name.is_none() {
        return Ok(None);
    }
    let index_name = index_name.unwrap();

    // Get the index data
    let index_data = match database.get_index_data(&index_name) {
        Some(data) => data,
        None => return Ok(None),
    };

    // Use multi_lookup for IN predicate
    let matching_row_indices = index_data.multi_lookup(&literal_values);

    // Convert row indices to actual rows
    let result_rows = matching_row_indices
        .iter()
        .filter_map(|&row_idx| {
            if row_idx < all_rows.len() {
                Some(all_rows[row_idx].clone())
            } else {
                None
            }
        })
        .collect();

    Ok(Some(result_rows))
}

/// Find an index that can be used for WHERE clause filtering
pub(in crate::select::executor) fn find_index_for_where(
    database: &Database,
    table_name: &str,
    column_name: &str,
) -> Result<Option<String>, ExecutorError> {
    // Look through all indexes for one on this table and column
    let all_indexes = database.list_indexes();
    for index_name in all_indexes {
        if let Some(metadata) = database.get_index(&index_name) {
            if metadata.table_name == table_name
                && metadata.columns.len() == 1
                && metadata.columns[0].column_name == column_name {
                return Ok(Some(index_name));
            }
        }
    }
    Ok(None)
}

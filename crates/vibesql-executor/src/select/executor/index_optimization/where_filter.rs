//! Index-based WHERE clause filtering optimization

use vibesql_storage::database::Database;

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Try to use indexes for WHERE clause filtering
/// Returns Some(rows) if index optimization was applied, None if not applicable
pub(in crate::select::executor) fn try_index_based_where_filtering(
    database: &Database,
    where_expr: Option<&vibesql_ast::Expression>,
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    let where_expr = match where_expr {
        Some(expr) => expr,
        None => return Ok(None), // No WHERE clause
    };

    // Try to match different predicate patterns
    match where_expr {
        // AND expressions (for BETWEEN pattern) - check first before binary op
        vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::And, left, right } => {
            try_index_for_and_expr(database, left, right, all_rows, schema)
        }
        // Simple binary operations: column OP value
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            try_index_for_binary_op(database, left, op, right, all_rows, schema)
        }
        // IN expressions: column IN (val1, val2, ...)
        vibesql_ast::Expression::InList { expr, values, negated: false } => {
            try_index_for_in_expr(database, expr, values, all_rows, schema)
        }
        // Other expressions not supported for index optimization
        _ => Ok(None),
    }
}

/// Try to use indexes specifically for IN clause expressions
/// This is a focused re-enablement of IN clause optimization (issue #1764)
/// while keeping other WHERE filtering disabled due to bugs in #1744
pub(in crate::select::executor) fn try_index_for_in_clause(
    database: &Database,
    where_expr: Option<&vibesql_ast::Expression>,
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    let where_expr = match where_expr {
        Some(expr) => expr,
        None => return Ok(None),
    };

    match where_expr {
        // Simple IN clause: col IN (...)
        vibesql_ast::Expression::InList { expr, values, negated: false } => {
            try_index_for_in_expr(database, expr, values, all_rows, schema)
        }
        // IN clause combined with AND: col IN (...) AND comparison
        vibesql_ast::Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
            // Try left as IN, right as comparison
            if let Some(rows) = try_index_for_in_and_comparison(database, left, right, all_rows, schema)? {
                return Ok(Some(rows));
            }
            // Try right as IN, left as comparison
            if let Some(rows) = try_index_for_in_and_comparison(database, right, left, all_rows, schema)? {
                return Ok(Some(rows));
            }
            Ok(None)
        }
        // Not an IN clause pattern we can optimize
        _ => Ok(None),
    }
}

/// Try to use index for binary operation predicates (=, <, >, <=, >=)
pub(in crate::select::executor) fn try_index_for_binary_op(
    database: &Database,
    left: &vibesql_ast::Expression,
    op: &vibesql_ast::BinaryOperator,
    right: &vibesql_ast::Expression,
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    // Extract column, value, and normalized operator
    // Handle both "column OP literal" and "literal OP column" (commutative property)
    let (table_name, column_name, value, normalized_op) = match (left, right) {
        // Case 1: column OP literal (e.g., col0 = 5)
        (vibesql_ast::Expression::ColumnRef { table: None, column }, vibesql_ast::Expression::Literal(val)) => {
            // Find which table this column belongs to
            let mut found_table = None;
            for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
                if _table_schema.get_column_index(column).is_some() {
                    found_table = Some(table.clone());
                    break;
                }
            }
            match found_table {
                Some(table) => (table, column.clone(), val.clone(), *op),
                None => return Ok(None), // Column not found
            }
        }
        // Case 2: literal OP column (e.g., 5 = col0)
        // Flip the operator to normalize: literal < column → column > literal
        (vibesql_ast::Expression::Literal(val), vibesql_ast::Expression::ColumnRef { table: None, column }) => {
            // Find which table this column belongs to
            let mut found_table = None;
            for (table, (_start_idx, _table_schema)) in &schema.table_schemas {
                if _table_schema.get_column_index(column).is_some() {
                    found_table = Some(table.clone());
                    break;
                }
            }
            // Flip the operator for commutative handling
            let flipped_op = match op {
                vibesql_ast::BinaryOperator::Equal => vibesql_ast::BinaryOperator::Equal,
                vibesql_ast::BinaryOperator::LessThan => vibesql_ast::BinaryOperator::GreaterThan,
                vibesql_ast::BinaryOperator::GreaterThan => vibesql_ast::BinaryOperator::LessThan,
                vibesql_ast::BinaryOperator::LessThanOrEqual => vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                vibesql_ast::BinaryOperator::GreaterThanOrEqual => vibesql_ast::BinaryOperator::LessThanOrEqual,
                _ => return Ok(None), // Operator not supported for flipping
            };
            match found_table {
                Some(table) => (table, column.clone(), val.clone(), flipped_op),
                None => return Ok(None), // Column not found
            }
        }
        _ => return Ok(None), // Not a simple column OP literal or literal OP column
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

    // Get matching row indices based on normalized operator
    let matching_row_indices = match normalized_op {
        vibesql_ast::BinaryOperator::Equal => {
            // Equality: exact lookup
            let search_key = vec![value];
            index_data.get(&search_key).cloned().unwrap_or_else(Vec::new)
        }
        vibesql_ast::BinaryOperator::GreaterThan => {
            // col > value: use range_scan(Some(value), None, false, false)
            index_data.range_scan(Some(&value), None, false, false)
        }
        vibesql_ast::BinaryOperator::LessThan => {
            // col < value: use range_scan(None, Some(value), false, false)
            index_data.range_scan(None, Some(&value), false, false)
        }
        vibesql_ast::BinaryOperator::GreaterThanOrEqual => {
            // col >= value: use range_scan(Some(value), None, true, false)
            index_data.range_scan(Some(&value), None, true, false)
        }
        vibesql_ast::BinaryOperator::LessThanOrEqual => {
            // col <= value: use range_scan(None, Some(value), false, true)
            index_data.range_scan(None, Some(&value), false, true)
        }
        _ => return Ok(None), // Operator not supported for index optimization
    };

    // Convert row indices to actual rows
    let result_rows =
        matching_row_indices
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

/// Try to handle IN clause combined with a comparison (e.g., col1 IN (...) AND col2 > value)
/// Returns Some(rows) if optimization was applied, None if not applicable
pub(in crate::select::executor) fn try_index_for_in_and_comparison(
    database: &Database,
    in_expr: &vibesql_ast::Expression,
    comparison_expr: &vibesql_ast::Expression,
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    // Check if first expression is an IN list
    let (in_column, in_values) = match in_expr {
        vibesql_ast::Expression::InList { expr, values, negated: false } => {
            match expr.as_ref() {
                vibesql_ast::Expression::ColumnRef { table: None, column } => (column, values),
                _ => return Ok(None), // Not a simple column reference
            }
        }
        _ => return Ok(None), // Not an IN list
    };

    // Check if second expression is a binary comparison
    let comparison_can_use_index = match comparison_expr {
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            // Check if this is a simple column comparison that could use an index
            matches!(
                op,
                vibesql_ast::BinaryOperator::Equal
                    | vibesql_ast::BinaryOperator::GreaterThan
                    | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                    | vibesql_ast::BinaryOperator::LessThan
                    | vibesql_ast::BinaryOperator::LessThanOrEqual
            ) && (matches!(left.as_ref(), vibesql_ast::Expression::ColumnRef { .. })
                || matches!(right.as_ref(), vibesql_ast::Expression::ColumnRef { .. }))
        }
        _ => false,
    };

    if !comparison_can_use_index {
        return Ok(None);
    }

    // Strategy: Use index for IN clause to get candidate rows, then post-filter with comparison
    // This is correct and often faster than a full table scan

    // Try to get rows using the IN clause index
    let candidate_rows = try_index_for_in_expr(database, in_expr, in_values, all_rows, schema)?;

    let candidate_rows = match candidate_rows {
        Some(rows) => rows,
        None => {
            // IN clause couldn't use index, try the comparison instead
            // and post-filter with IN clause (slower but correct)
            return try_comparison_then_filter_in(
                database,
                comparison_expr,
                in_column,
                in_values,
                all_rows,
                schema,
            );
        }
    };

    // Now filter the candidate rows by the comparison expression
    // For simple comparisons, we can evaluate them directly
    let result_rows = filter_rows_by_comparison(candidate_rows, comparison_expr, schema)?;

    Ok(Some(result_rows))
}

/// Try using index for comparison, then filter by IN clause
fn try_comparison_then_filter_in(
    database: &Database,
    comparison_expr: &vibesql_ast::Expression,
    in_column: &str,
    in_values: &[vibesql_ast::Expression],
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    // Try to use index for the comparison
    let (left, op, right) = match comparison_expr {
        vibesql_ast::Expression::BinaryOp { left, op, right } => (left.as_ref(), op, right.as_ref()),
        _ => return Ok(None),
    };

    let candidate_rows = try_index_for_binary_op(database, left, op, right, all_rows, schema)?;

    let candidate_rows = match candidate_rows {
        Some(rows) => rows,
        None => return Ok(None), // Can't optimize either side
    };

    // Extract literal values from IN list
    let mut literal_values = Vec::new();
    for val_expr in in_values {
        if let vibesql_ast::Expression::Literal(val) = val_expr {
            literal_values.push(val.clone());
        } else {
            // Can't optimize if IN list contains non-literals
            return Ok(None);
        }
    }

    // Find which table/column the IN clause references
    let mut in_column_schema_idx = None;
    for (_table_name, (start_idx, table_schema)) in &schema.table_schemas {
        if let Some(col_idx) = table_schema.get_column_index(in_column) {
            in_column_schema_idx = Some(start_idx + col_idx);
            break;
        }
    }

    let in_column_idx = match in_column_schema_idx {
        Some(idx) => idx,
        None => return Ok(None), // Column not found
    };

    // Filter candidate rows by IN clause
    use std::collections::HashSet;
    let value_set: HashSet<_> = literal_values.into_iter().collect();

    let result_rows = candidate_rows
        .into_iter()
        .filter(|row| {
            if let Some(col_value) = row.get(in_column_idx) {
                value_set.contains(col_value)
            } else {
                false
            }
        })
        .collect();

    Ok(Some(result_rows))
}

/// Filter rows by a simple binary comparison
/// For simple cases like col > value, we can evaluate without a full evaluator
fn filter_rows_by_comparison(
    rows: Vec<vibesql_storage::Row>,
    comparison_expr: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Extract comparison details
    let (column_name, op, literal_value) = match comparison_expr {
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            // Case 1: column OP literal
            if let (vibesql_ast::Expression::ColumnRef { table: None, column }, vibesql_ast::Expression::Literal(val)) = (left.as_ref(), right.as_ref()) {
                (column, *op, val.clone())
            }
            // Case 2: literal OP column (need to flip operator)
            else if let (vibesql_ast::Expression::Literal(val), vibesql_ast::Expression::ColumnRef { table: None, column }) = (left.as_ref(), right.as_ref()) {
                let flipped_op = match op {
                    vibesql_ast::BinaryOperator::Equal => vibesql_ast::BinaryOperator::Equal,
                    vibesql_ast::BinaryOperator::LessThan => vibesql_ast::BinaryOperator::GreaterThan,
                    vibesql_ast::BinaryOperator::GreaterThan => vibesql_ast::BinaryOperator::LessThan,
                    vibesql_ast::BinaryOperator::LessThanOrEqual => vibesql_ast::BinaryOperator::GreaterThanOrEqual,
                    vibesql_ast::BinaryOperator::GreaterThanOrEqual => vibesql_ast::BinaryOperator::LessThanOrEqual,
                    _ => return Err(ExecutorError::UnsupportedExpression("Unsupported operator for index optimization".to_string())),
                };
                (column, flipped_op, val.clone())
            } else {
                return Err(ExecutorError::UnsupportedExpression("Complex comparison not supported for index optimization".to_string()));
            }
        }
        _ => return Err(ExecutorError::UnsupportedExpression("Not a binary comparison".to_string())),
    };

    // Find column index in schema
    let mut column_idx = None;
    for (_table_name, (start_idx, table_schema)) in &schema.table_schemas {
        if let Some(col_idx) = table_schema.get_column_index(column_name) {
            column_idx = Some(start_idx + col_idx);
            break;
        }
    }

    let column_idx = match column_idx {
        Some(idx) => idx,
        None => return Err(ExecutorError::UnsupportedExpression(format!("Column {} not found", column_name))),
    };

    // Filter rows based on the comparison
    let result_rows = rows
        .into_iter()
        .filter(|row| {
            if let Some(row_value) = row.get(column_idx) {
                match op {
                    vibesql_ast::BinaryOperator::Equal => row_value == &literal_value,
                    vibesql_ast::BinaryOperator::GreaterThan => row_value > &literal_value,
                    vibesql_ast::BinaryOperator::GreaterThanOrEqual => row_value >= &literal_value,
                    vibesql_ast::BinaryOperator::LessThan => row_value < &literal_value,
                    vibesql_ast::BinaryOperator::LessThanOrEqual => row_value <= &literal_value,
                    _ => false,
                }
            } else {
                false
            }
        })
        .collect();

    Ok(result_rows)
}

/// Try to use index for AND expressions (detecting BETWEEN pattern or IN + comparison)
pub(in crate::select::executor) fn try_index_for_and_expr(
    database: &Database,
    left: &vibesql_ast::Expression,
    right: &vibesql_ast::Expression,
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    // First, try to handle IN + comparison pattern
    // Pattern: col1 IN (...) AND col2 > value (or any comparison)
    if let Some(rows) = try_index_for_in_and_comparison(database, left, right, all_rows, schema)? {
        return Ok(Some(rows));
    }

    // Try the reverse: comparison AND IN
    if let Some(rows) = try_index_for_in_and_comparison(database, right, left, all_rows, schema)? {
        return Ok(Some(rows));
    }

    // Fall back to BETWEEN pattern detection
    // Try to detect BETWEEN pattern: (col >= start) AND (col <= end)
    // or variations like (col > start) AND (col < end)

    let (col_name, start_val, start_inclusive, end_val, end_inclusive) = match (left, right) {
        (
            vibesql_ast::Expression::BinaryOp { left: left_col, op: left_op, right: left_val },
            vibesql_ast::Expression::BinaryOp { left: right_col, op: right_op, right: right_val },
        ) => {
            // Both sides are binary operations
            // Check if both refer to the same column
            let (left_col_name, _right_col_name) = match (left_col.as_ref(), right_col.as_ref()) {
                (
                    vibesql_ast::Expression::ColumnRef { table: None, column: lc },
                    vibesql_ast::Expression::ColumnRef { table: None, column: rc },
                ) if lc == rc => (lc, rc),
                _ => return Ok(None), // Not the same column
            };

            // Extract values
            let (left_lit, right_lit) = match (left_val.as_ref(), right_val.as_ref()) {
                (vibesql_ast::Expression::Literal(lv), vibesql_ast::Expression::Literal(rv)) => (lv, rv),
                _ => return Ok(None), // Not literals
            };

            // Determine the bounds based on operators
            // left is lower bound operation (>= or >)
            // right is upper bound operation (<= or <)
            match (left_op, right_op) {
                (vibesql_ast::BinaryOperator::GreaterThanOrEqual, vibesql_ast::BinaryOperator::LessThanOrEqual) => {
                    (left_col_name.clone(), left_lit.clone(), true, right_lit.clone(), true)
                }
                (vibesql_ast::BinaryOperator::GreaterThanOrEqual, vibesql_ast::BinaryOperator::LessThan) => {
                    (left_col_name.clone(), left_lit.clone(), true, right_lit.clone(), false)
                }
                (vibesql_ast::BinaryOperator::GreaterThan, vibesql_ast::BinaryOperator::LessThanOrEqual) => {
                    (left_col_name.clone(), left_lit.clone(), false, right_lit.clone(), true)
                }
                (vibesql_ast::BinaryOperator::GreaterThan, vibesql_ast::BinaryOperator::LessThan) => {
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
    let matching_row_indices =
        index_data.range_scan(Some(&start_val), Some(&end_val), start_inclusive, end_inclusive);

    // Convert row indices to actual rows
    let result_rows =
        matching_row_indices
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
    expr: &vibesql_ast::Expression,
    values: &[vibesql_ast::Expression],
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    // Extract column name
    let column_name = match expr {
        vibesql_ast::Expression::ColumnRef { table: None, column } => column,
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
        if let vibesql_ast::Expression::Literal(val) = val_expr {
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
    let result_rows =
        matching_row_indices
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
                && metadata.columns[0].column_name == column_name
            {
                return Ok(Some(index_name));
            }
        }
    }
    Ok(None)
}

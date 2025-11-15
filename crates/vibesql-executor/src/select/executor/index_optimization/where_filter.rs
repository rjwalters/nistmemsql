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

    // Strategy: Use index for IN clause to get candidate rows, then post-filter with comparison
    // This is correct and often faster than a full table scan
    // Works for both simple comparisons AND complex expressions (OR, AND, BETWEEN, etc.)

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
    // Use the full evaluator to handle any expression type (simple or complex)
    use crate::evaluator::CombinedExpressionEvaluator;

    let evaluator = CombinedExpressionEvaluator::with_database(schema, database);
    let mut filtered_rows = Vec::new();

    for row in candidate_rows {
        // Clear CSE cache before evaluating each row
        evaluator.clear_cse_cache();

        let include_row = match evaluator.eval(comparison_expr, &row)? {
            vibesql_types::SqlValue::Boolean(true) => true,
            vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            vibesql_types::SqlValue::Integer(0) => false,
            vibesql_types::SqlValue::Integer(_) => true,
            vibesql_types::SqlValue::Smallint(0) => false,
            vibesql_types::SqlValue::Smallint(_) => true,
            vibesql_types::SqlValue::Bigint(0) => false,
            vibesql_types::SqlValue::Bigint(_) => true,
            vibesql_types::SqlValue::Float(0.0) => false,
            vibesql_types::SqlValue::Float(_) => true,
            vibesql_types::SqlValue::Real(0.0) => false,
            vibesql_types::SqlValue::Real(_) => true,
            vibesql_types::SqlValue::Double(0.0) => false,
            vibesql_types::SqlValue::Double(_) => true,
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

    Ok(Some(filtered_rows))
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
    let mut has_null = false;
    for val_expr in in_values {
        if let vibesql_ast::Expression::Literal(val) = val_expr {
            // Track if we encounter NULL in the list
            if matches!(val, vibesql_types::SqlValue::Null) {
                has_null = true;
            }
            literal_values.push(val.clone());
        } else {
            // Can't optimize if IN list contains non-literals
            return Ok(None);
        }
    }

    // If IN list contains NULL, skip index optimization
    // (same rationale as in try_index_for_in_expr)
    if has_null {
        return Ok(None);
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

/// Apply a single predicate as a post-filter on a set of rows
/// Used when combining index optimization with additional predicates
fn apply_predicate_filter(
    database: &Database,
    rows: &[vibesql_storage::Row],
    predicate: &vibesql_ast::Expression,
    schema: &CombinedSchema,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    use crate::evaluator::CombinedExpressionEvaluator;

    let evaluator = CombinedExpressionEvaluator::with_database(schema, database);

    let filtered_rows: Vec<vibesql_storage::Row> = rows
        .iter()
        .filter(|row| {
            match evaluator.eval(predicate, row) {
                Ok(vibesql_types::SqlValue::Boolean(true)) => true,
                Ok(vibesql_types::SqlValue::Null) => false, // NULL in WHERE is treated as false
                _ => false,
            }
        })
        .cloned()
        .collect();

    Ok(filtered_rows)
}

/// Try to use index for AND expressions (detecting BETWEEN pattern or IN + comparison)
pub(in crate::select::executor) fn try_index_for_and_expr(
    database: &Database,
    left: &vibesql_ast::Expression,
    right: &vibesql_ast::Expression,
    all_rows: &[vibesql_storage::Row],
    schema: &CombinedSchema,
) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
    // Handle nested AND expressions recursively
    // Pattern: comp AND (comp AND IN(...)) - need to recurse into nested AND

    // If right side is a nested AND, optimize it recursively
    if let vibesql_ast::Expression::BinaryOp { left: nested_left, op: vibesql_ast::BinaryOperator::And, right: nested_right } = right {
        // Right is a nested AND expression - try to optimize it recursively
        if let Some(nested_rows) = try_index_for_and_expr(database, nested_left, nested_right, all_rows, schema)? {
            let filtered = apply_predicate_filter(database, &nested_rows, left, schema)?;
            return Ok(Some(filtered));
        }
    }

    // If left side is a nested AND, optimize it recursively
    if let vibesql_ast::Expression::BinaryOp { left: nested_left, op: vibesql_ast::BinaryOperator::And, right: nested_right } = left {
        // Left is a nested AND expression - try to optimize it recursively
        if let Some(nested_rows) = try_index_for_and_expr(database, nested_left, nested_right, all_rows, schema)? {
            // Successfully optimized nested AND, now apply right predicate as post-filter
            return Ok(Some(apply_predicate_filter(database, &nested_rows, right, schema)?));
        }
    }

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

    // Validate bounds: start_val must be <= end_val for a valid range
    // If start > end, the BETWEEN range is empty (no values can satisfy it)
    let gt_result = crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
        &start_val,
        &vibesql_ast::BinaryOperator::GreaterThan,
        &end_val,
    )?;
    if let vibesql_types::SqlValue::Boolean(true) = gt_result {
        // start_val > end_val: empty range, return no rows
        return Ok(Some(Vec::new()));
    }

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
    let mut has_null = false;
    for val_expr in values {
        if let vibesql_ast::Expression::Literal(val) = val_expr {
            // Track if we encounter NULL in the list
            if matches!(val, vibesql_types::SqlValue::Null) {
                has_null = true;
            }
            literal_values.push(val.clone());
        } else {
            return Ok(None); // Not all values are literals
        }
    }

    // If IN list contains NULL, skip index optimization
    // Rationale: per SQL three-valued logic, when NULL is in the IN list:
    // - value IN (..., NULL) when value doesn't match → NULL (not FALSE)
    // The index lookup can't represent this NULL result, so we must fall back
    // to regular evaluation which handles three-valued logic correctly
    if has_null {
        return Ok(None);
    }

    // Find an index on this table and column
    let index_name = find_index_for_where(database, &table_name, column_name)?;
    if index_name.is_none() {
        return Ok(None);
    }
    let index_name = index_name.unwrap();

    // Get the index metadata to check if it's a multi-column index
    let index_metadata = match database.get_index(&index_name) {
        Some(metadata) => metadata,
        None => return Ok(None),
    };

    // Get the index data
    let index_data = match database.get_index_data(&index_name) {
        Some(data) => data,
        None => return Ok(None),
    };

    // For multi-column indexes, we need to use range scans for each value
    // because multi_lookup expects exact matches on composite keys
    let matching_row_indices = if index_metadata.columns.len() > 1 {
        // Multi-column index: use range scans for prefix matching
        // For a multi-column index (col0, col1, col3), the index stores composite keys
        // like [93, 1.5, 42]. To look up col0 IN (93, 63, ...), we use range scans
        // where both start and end are the same value, relying on range_scan's
        // implementation to compare only the first column of composite keys.
        let mut all_indices = Vec::new();
        for value in &literal_values {
            let row_indices = index_data.range_scan(
                Some(value),  // Start: col0 = value
                Some(value),  // End: col0 = value
                true,         // Inclusive start
                true,         // Inclusive end
            );
            all_indices.extend(row_indices);
        }
        all_indices
    } else {
        // Single-column index: use multi_lookup as before
        index_data.multi_lookup(&literal_values)
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

/// Find an index that can be used for WHERE clause filtering
///
/// NOTE: This function is restricted to single-column indexes only because callers
/// like try_index_for_binary_op() create single-element search keys (vec![value]).
/// Multi-column indexes would require composite keys and are handled separately
/// in try_index_for_in_expr() which uses range scans for prefix matching.
pub(in crate::select::executor) fn find_index_for_where(
    database: &Database,
    table_name: &str,
    column_name: &str,
) -> Result<Option<String>, ExecutorError> {
    // Look through all indexes for one on this table and column
    // ONLY single-column indexes (multi-column indexes require special handling)
    let all_indexes = database.list_indexes();
    for index_name in all_indexes {
        if let Some(metadata) = database.get_index(&index_name) {
            if metadata.table_name == table_name
                && metadata.columns.len() == 1
                && metadata.columns[0].column_name == column_name
            {
                // Found a single-column index on this column
                return Ok(Some(index_name));
            }
        }
    }
    Ok(None)
}

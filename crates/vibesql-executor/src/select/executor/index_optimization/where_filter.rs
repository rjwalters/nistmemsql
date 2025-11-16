//! Index-based WHERE clause filtering optimization

use vibesql_storage::database::Database;

use crate::{errors::ExecutorError, schema::CombinedSchema};

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

    // Find an index on this table and column (supports multi-column indexes via prefix matching)
    let index_name = find_index_with_prefix(database, &table_name, &column_name)?;
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

    // Find an index on this table and column (supports multi-column indexes)
    // Use find_index_with_prefix because this function can handle both single-column
    // and multi-column indexes correctly via range scans
    let index_name = find_index_with_prefix(database, &table_name, column_name)?;
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

/// Find a SINGLE-COLUMN index that can be used for WHERE clause filtering
/// For multi-column index support, use find_index_with_prefix() instead
pub(in crate::select::executor) fn find_index_for_where(
    database: &Database,
    table_name: &str,
    column_name: &str,
) -> Result<Option<String>, ExecutorError> {
    // Look through all indexes for a SINGLE-COLUMN index on this table and column
    // Multi-column indexes are intentionally excluded because most index operations
    // (equality lookups, range scans) expect single-element keys
    let all_indexes = database.list_indexes();
    for index_name in all_indexes {
        if let Some(metadata) = database.get_index(&index_name) {
            if metadata.table_name == table_name
                && metadata.columns.len() == 1
                && metadata.columns[0].column_name == column_name
            {
                // Found a single-column index
                return Ok(Some(index_name));
            }
        }
    }
    Ok(None)
}

/// Find an index (single or multi-column) where the target column is the first column
/// This supports prefix matching on composite indexes
fn find_index_with_prefix(
    database: &Database,
    table_name: &str,
    column_name: &str,
) -> Result<Option<String>, ExecutorError> {
    // Look through all indexes for one where the target column is the FIRST column
    // This works for both single-column and multi-column indexes
    let all_indexes = database.list_indexes();
    for index_name in all_indexes {
        if let Some(metadata) = database.get_index(&index_name) {
            if metadata.table_name == table_name
                && !metadata.columns.is_empty()
                && metadata.columns[0].column_name == column_name
            {
                // Found an index where our column is the first column
                return Ok(Some(index_name));
            }
        }
    }
    Ok(None)
}

/// Check if WHERE clause would use multi-column IN optimization, requiring predicate pushdown to be disabled
///
/// Returns true if:
/// - WHERE contains an IN expression
/// - The column has a MULTI-column index (> 1 columns)
/// - This indicates we need ALL rows for correct index-based filtering
///
/// For single-column indexes or non-IN predicates, predicate pushdown can safely be enabled.
pub(in crate::select::executor) fn requires_predicate_pushdown_disable(
    database: &Database,
    where_expr: Option<&vibesql_ast::Expression>,
    from_clause: Option<&vibesql_ast::FromClause>,
) -> bool {
    let where_expr = match where_expr {
        Some(expr) => expr,
        None => return false, // No WHERE clause, predicate pushdown doesn't matter
    };

    // Check if this is an IN expression
    if let vibesql_ast::Expression::InList { expr, negated: false, .. } = where_expr {
        // Extract table and column name from the IN expression
        if let vibesql_ast::Expression::ColumnRef { table, column } = expr.as_ref() {
            // Get table name: either from the column ref or from FROM clause
            let table_name = if let Some(tbl) = table {
                tbl.clone()
            } else {
                // Extract first table from FROM clause
                match from_clause {
                    Some(vibesql_ast::FromClause::Table { name, .. }) => name.clone(),
                    Some(vibesql_ast::FromClause::Join { left, .. }) => {
                        // For joins, check the left table
                        if let vibesql_ast::FromClause::Table { name, .. } = left.as_ref() {
                            name.clone()
                        } else {
                            return false; // Complex FROM, can't determine table
                        }
                    }
                    _ => { return false }, // Can't determine table name
                }
            };

            // Check if there's a MULTI-column index on this column
            if let Ok(Some(index_name)) = find_index_with_prefix(database, &table_name, column) {
                if let Some(metadata) = database.get_index(&index_name) {
                    // Only disable predicate pushdown for MULTI-column indexes
                    // Single-column indexes work fine with predicate pushdown
                    return metadata.columns.len() > 1;
                }
            }
        }
    }

    // For all other cases (binary ops, AND, OR, etc.), predicate pushdown is safe
    false
}

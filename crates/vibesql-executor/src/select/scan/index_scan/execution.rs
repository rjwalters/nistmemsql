//! Index scan execution
//!
//! Executes index scans to retrieve rows from tables using indexes.

use vibesql_ast::Expression;
use vibesql_storage::{Database, Row};

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::predicate::{extract_index_predicate, IndexPredicate};

/// Execute an index scan
///
/// Uses the specified index to retrieve matching rows, then fetches full rows from the table.
/// This implements the "index scan + fetch" strategy with optimized range scans.
///
/// If sorted_columns is provided, the function preserves index order and returns results
/// marked as pre-sorted, allowing the caller to skip ORDER BY sorting.
///
/// # Performance Optimization
/// When the WHERE clause can be fully satisfied by the index predicate (e.g., simple
/// predicates like `WHERE col = 5` or `WHERE col BETWEEN 10 AND 20`), we skip redundant
/// WHERE clause re-evaluation, significantly improving performance for large result sets.
pub(crate) fn execute_index_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: Option<&Expression>,
    sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    database: &Database,
) -> Result<super::super::FromResult, ExecutorError> {
    // Get table and index
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let index_metadata = database
        .get_index(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    let index_data = database
        .get_index_data(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    // Get the first indexed column (for predicate extraction)
    let indexed_column = index_metadata
        .columns
        .first()
        .map(|col| col.column_name.as_str())
        .unwrap_or("");

    // Try to extract index predicate (range or IN) for the indexed column
    let index_predicate = where_clause.and_then(|expr| extract_index_predicate(expr, indexed_column));

    // Performance optimization: Determine if WHERE filtering can be skipped
    // Check if the index predicate fully satisfies the WHERE clause
    let need_where_filter = match (&where_clause, &index_predicate) {
        (Some(where_expr), Some(_)) => {
            // Only skip WHERE filtering if we're certain the index handles everything
            !where_clause_fully_satisfied_by_index(where_expr, indexed_column, &index_predicate)
        }
        (Some(_), None) => true,  // WHERE present but no index predicate extracted
        (None, _) => false,         // No WHERE clause
    };

    // Determine if this is a multi-column index
    let is_multi_column_index = index_metadata.columns.len() > 1;

    // Get row indices using the appropriate index operation
    let matching_row_indices: Vec<usize> = match index_predicate {
        Some(IndexPredicate::Range(range)) => {
            // Validate bounds: if start > end, the range is empty
            if let (Some(start_val), Some(end_val)) = (&range.start, &range.end) {
                let gt_result = crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
                    start_val,
                    &vibesql_ast::BinaryOperator::GreaterThan,
                    end_val,
                )?;
                if let vibesql_types::SqlValue::Boolean(true) = gt_result {
                    // start_val > end_val: empty range, return no rows
                    Vec::new()
                } else {
                    // Valid range, use storage layer's optimized range_scan
                    index_data.range_scan(
                        range.start.as_ref(),
                        range.end.as_ref(),
                        range.inclusive_start,
                        range.inclusive_end,
                    )
                }
            } else {
                // Use storage layer's optimized range_scan for >, <, >=, <=, BETWEEN
                index_data.range_scan(
                    range.start.as_ref(),
                    range.end.as_ref(),
                    range.inclusive_start,
                    range.inclusive_end,
                )
            }
        }
        Some(IndexPredicate::In(values)) => {
            // For multi-column indexes, use prefix matching to find all rows
            // where the first column matches any of the IN values
            if is_multi_column_index {
                // Use prefix_multi_lookup which performs range scans to match
                // partial keys (e.g., [10] matches [10, 20], [10, 30], etc.)
                index_data.prefix_multi_lookup(&values)
            } else {
                // For single-column indexes, use regular exact match lookup
                index_data.multi_lookup(&values)
            }
        }
        None => {
            // Full index scan - collect all row indices from the index in index key order
            // (Will be sorted by row index later if needed, see lines 425-427)
            index_data
                .values()
                .flatten()
                .copied()
                .collect()
        }
    };

    // If we're not returning sorted results, ensure rows are in table order (by row index)
    // This is important when the index doesn't satisfy the ORDER BY clause.
    // Without this, rows would be returned in index key order, which would cause
    // incorrect results when ORDER BY specifies a different column.
    let mut matching_row_indices = matching_row_indices;
    if sorted_columns.is_none() {
        matching_row_indices.sort_unstable();
    }

    // Fetch rows from table
    let all_rows = table.scan();
    let mut rows: Vec<Row> = matching_row_indices
        .into_iter()
        .filter_map(|idx| all_rows.get(idx).cloned())
        .collect();

    // Reverse rows if needed for DESC ORDER BY
    // BTreeMap iteration is always ascending, but for DESC ORDER BY we need descending order
    // Check if we're using this index for ORDER BY and if the first ORDER BY column is DESC
    if let Some(ref sorted_cols) = sorted_columns {
        if let Some((_, first_order_direction)) = sorted_cols.first() {
            if *first_order_direction == vibesql_ast::OrderDirection::Desc {
                rows.reverse();
            }
        }
    }

    // Build schema
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

    // Apply WHERE clause predicates if needed
    // Performance optimization: Skip WHERE clause evaluation if the index already
    // guarantees all rows satisfy the predicate (e.g., simple predicates like
    // `WHERE col = 5` or `WHERE col BETWEEN 10 AND 20`).
    //
    // We still need to filter when:
    // - Predicates involve non-indexed columns
    // - Complex predicates that couldn't be fully pushed to index
    // - OR predicates (not yet optimized for index pushdown)
    // - Multi-column predicates where only first column was indexed
    if need_where_filter {
        if let Some(where_expr) = where_clause {
            // Use predicate decomposition to extract only table-local predicates
            // This prevents ColumnNotFound errors when WHERE clause references columns
            // from other tables in a JOIN (e.g., FROM t1, t2 WHERE t1.a = 5 AND t2.b = 10)
            use crate::optimizer::decompose_where_clause;

            let decomposition = decompose_where_clause(Some(where_expr), &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            // Extract predicates that apply to this table only
            let table_local_preds: Option<&Vec<vibesql_ast::Expression>> =
                decomposition.table_local_predicates.get(table_name);

            // Only apply filtering if there are table-local predicates
            if let Some(preds) = table_local_preds {
                if !preds.is_empty() {
                    use crate::evaluator::CombinedExpressionEvaluator;

                    // Combine predicates with AND
                    let combined_where = super::super::predicates::combine_predicates_with_and(preds.clone());

                    // Create evaluator for filtering
                    let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

                    // Apply table-local predicates only
                    let mut filtered_rows = Vec::new();
                    for row in rows {
                        // Clear CSE cache before evaluating each row
                        evaluator.clear_cse_cache();

                        let eval_result = evaluator.eval(&combined_where, &row)?;
                        let include_row = match eval_result {
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
                    rows = filtered_rows;
                }
            }
        }
    }

    // Return results with sorting metadata if available
    // If WHERE clause was fully handled by index (!need_where_filter), indicate this
    // so the executor doesn't redundantly re-apply WHERE filtering
    if !need_where_filter {
        Ok(super::super::FromResult::from_rows_where_filtered(schema, rows, sorted_columns))
    } else {
        match sorted_columns {
            Some(sorted) => Ok(super::super::FromResult::from_rows_sorted(schema, rows, sorted)),
            None => Ok(super::super::FromResult::from_rows(schema, rows)),
        }
    }
}

/// Determines if the WHERE clause is fully satisfied by the index predicate
///
/// Returns true only when we're 100% certain that the index has already filtered
/// rows exactly according to the WHERE clause, making WHERE re-evaluation redundant.
///
/// # Conservative Approach
/// This function is intentionally conservative - it only returns true for simple cases
/// where we can prove the index predicate exactly matches the WHERE semantics.
/// When in doubt, we return false to ensure correctness.
///
/// # Safe Cases (returns true)
/// - `WHERE col = value` with extracted equality predicate
/// - `WHERE col BETWEEN a AND b` with extracted BETWEEN predicate
/// - `WHERE col >= a AND col <= b` with extracted range predicate
/// - `WHERE col > a` / `WHERE col < b` with extracted range predicate
/// - `WHERE col IN (...)` with extracted IN predicate
///
/// # Unsafe Cases (returns false)
/// - OR predicates: `WHERE col1 = 5 OR col2 = 10`
/// - AND with multiple columns: `WHERE col1 = 5 AND col2 = 10` (only first column indexed)
/// - Complex predicates: `WHERE col = 5 AND func(col2) = 1`
/// - Negations: `NOT IN`, `NOT BETWEEN`, `!=`
/// - Any case where the WHERE clause structure doesn't exactly match the extracted predicate
fn where_clause_fully_satisfied_by_index(
    where_expr: &Expression,
    indexed_column: &str,
    index_predicate: &Option<IndexPredicate>,
) -> bool {
    use super::super::super::scan::index_scan::selection::is_column_reference;
    use vibesql_ast::BinaryOperator;

    let Some(pred) = index_predicate else {
        return false;  // No index predicate, can't be satisfied
    };

    match where_expr {
        // Simple equality: WHERE col = value
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check if this is exactly "indexed_column = literal"
            let is_indexed_col_equals_literal =
                (is_column_reference(left, indexed_column) && matches!(right.as_ref(), Expression::Literal(_)))
                || (is_column_reference(right, indexed_column) && matches!(left.as_ref(), Expression::Literal(_)));

            if !is_indexed_col_equals_literal {
                return false;
            }

            // Verify the index predicate is a matching equality range
            matches!(pred, IndexPredicate::Range(range)
                if range.start.is_some() && range.end.is_some()
                && range.start == range.end
                && range.inclusive_start && range.inclusive_end)
        }

        // BETWEEN: WHERE col BETWEEN low AND high
        Expression::Between { expr, negated: false, .. } => {
            // Must be our indexed column
            if !is_column_reference(expr, indexed_column) {
                return false;
            }

            // Verify the index predicate is a BETWEEN-compatible range
            matches!(pred, IndexPredicate::Range(range)
                if range.start.is_some() && range.end.is_some()
                && range.inclusive_start && range.inclusive_end)
        }

        // Simple range: WHERE col > value, WHERE col >= value, etc.
        Expression::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual => {
                    // Check if this is "indexed_column <op> literal" or "literal <op> indexed_column"
                    let is_simple_range =
                        (is_column_reference(left, indexed_column) && matches!(right.as_ref(), Expression::Literal(_)))
                        || (is_column_reference(right, indexed_column) && matches!(left.as_ref(), Expression::Literal(_)));

                    if !is_simple_range {
                        return false;
                    }

                    // Verify the index predicate is a range (any range is fine for simple comparisons)
                    matches!(pred, IndexPredicate::Range(_))
                }

                // AND: Only safe if it's "col >= a AND col <= b" forming a complete BETWEEN
                BinaryOperator::And => {
                    // This is only safe if both sides reference the same indexed column
                    // and together form a complete range that matches our index predicate
                    // For now, be conservative and reject AND unless it's obviously safe
                    // The predicate extraction already handles simple "col >= a AND col <= b" cases

                    // Check if this is exactly the pattern: indexed_col >= val AND indexed_col <= val
                    match (left.as_ref(), right.as_ref()) {
                        (
                            Expression::BinaryOp { left: l_left, op: l_op, right: l_right },
                            Expression::BinaryOp { left: r_left, op: r_op, right: r_right },
                        ) => {
                            // Both sides must reference our indexed column
                            let left_has_col = is_column_reference(l_left, indexed_column) || is_column_reference(l_right, indexed_column);
                            let right_has_col = is_column_reference(r_left, indexed_column) || is_column_reference(r_right, indexed_column);

                            if !left_has_col || !right_has_col {
                                return false;  // Not both sides on our column
                            }

                            // Both sides must be range operators
                            let is_range_op = |op: &BinaryOperator| matches!(op,
                                BinaryOperator::GreaterThan
                                | BinaryOperator::GreaterThanOrEqual
                                | BinaryOperator::LessThan
                                | BinaryOperator::LessThanOrEqual
                            );

                            if !is_range_op(l_op) || !is_range_op(r_op) {
                                return false;
                            }

                            // Must have extracted a range with both bounds
                            matches!(pred, IndexPredicate::Range(range)
                                if range.start.is_some() && range.end.is_some())
                        }
                        _ => false,  // Not the right structure
                    }
                }

                _ => false,  // Other binary operators not handled
            }
        }

        // IN: WHERE col IN (value1, value2, ...)
        Expression::InList { expr, negated: false, .. } => {
            // Must be our indexed column
            if !is_column_reference(expr, indexed_column) {
                return false;
            }

            // Verify the index predicate is an IN predicate
            matches!(pred, IndexPredicate::In(_))
        }

        // Anything else is unsafe
        _ => false,
    }
}

//! Index scan execution
//!
//! This module provides index-based table scanning for improved query performance.
//! It integrates with the index catalog to use user-defined indexes when beneficial.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Determines if an index scan is beneficial for the given query
///
/// Returns Some((index_name, sorted_columns)) if an index should be used, None otherwise.
/// The sorted_columns vector indicates which columns are pre-sorted by the index scan.
pub(crate) fn should_use_index_scan(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
    // We use indexes in three scenarios:
    // 1. WHERE clause references an indexed column (with or without ORDER BY)
    // 2. ORDER BY references an indexed column (even without WHERE)
    // 3. Both WHERE and ORDER BY use the same index
    //
    // Note: Index scans can provide partial optimization even for complex
    // predicates (including OR expressions). The full WHERE clause is always
    // applied as a post-filter in execute_index_scan() to ensure correctness.

    // Get all indexes for this table
    let _table = database.get_table(table_name)?;
    let indexes = database.list_indexes_for_table(table_name);

    if indexes.is_empty() {
        return None;
    }

    // Try to find an index that satisfies both WHERE and ORDER BY (if present)
    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            let first_indexed_column = index_metadata.columns.first()?;

            // Check if this index can be used for WHERE clause
            let can_use_for_where = where_clause
                .map(|expr| expression_filters_column(expr, &first_indexed_column.column_name))
                .unwrap_or(false);

            // Check if this index can be used for ORDER BY clause
            let can_use_for_order = if let Some(order_items) = order_by {
                // For now, only optimize if:
                // 1. ORDER BY has exactly one column
                // 2. That column matches the first column of the index
                // 3. It's a simple column reference (not an expression)
                if order_items.len() == 1 {
                    match &order_items[0].expr {
                        Expression::ColumnRef { table: None, column } => {
                            column == &first_indexed_column.column_name
                        }
                        _ => false,
                    }
                } else {
                    false
                }
            } else {
                false
            };

            // Use this index if it satisfies WHERE, ORDER BY, or both
            if can_use_for_where || can_use_for_order {
                // Build sorted_columns metadata if ORDER BY can be satisfied
                let sorted_columns = if can_use_for_order {
                    let order_item = &order_by.unwrap()[0];
                    Some(vec![(
                        first_indexed_column.column_name.clone(),
                        order_item.direction.clone(),
                    )])
                } else {
                    None
                };

                return Some((index_name.clone(), sorted_columns));
            }
        }
    }

    None
}

/// Check if an expression filters a specific column
///
/// Returns true if the expression contains a predicate on the given column
/// For example: "WHERE age = 25" filters column "age"
fn expression_filters_column(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            // Check for comparison operators
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Check if either side is our column
                    if is_column_reference(left, column_name) || is_column_reference(right, column_name)
                    {
                        return true;
                    }
                }
                vibesql_ast::BinaryOperator::And | vibesql_ast::BinaryOperator::Or => {
                    // Recursively check sub-expressions for AND/OR
                    return expression_filters_column(left, column_name)
                        || expression_filters_column(right, column_name);
                }
                _ => {}
            }
            false
        }
        // IN with value list: col IN (1, 2, 3)
        Expression::InList { expr, .. } => is_column_reference(expr, column_name),
        // IN with subquery: col IN (SELECT ...)
        Expression::In { expr, .. } => is_column_reference(expr, column_name),
        // BETWEEN: col BETWEEN low AND high
        Expression::Between { expr, .. } => is_column_reference(expr, column_name),
        _ => false,
    }
}

/// Check if an expression is a reference to a specific column
fn is_column_reference(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef { column, .. } => column == column_name,
        _ => false,
    }
}

/// Range predicate information extracted from WHERE clause
#[derive(Debug)]
struct RangePredicate {
    start: Option<SqlValue>,
    end: Option<SqlValue>,
    inclusive_start: bool,
    inclusive_end: bool,
}

/// Index predicate types that can be pushed down to storage layer
#[derive(Debug)]
enum IndexPredicate {
    /// Range scan with optional bounds (>, <, >=, <=, BETWEEN)
    Range(RangePredicate),
    /// Multi-value lookup (IN predicate)
    In(Vec<SqlValue>),
}

/// Extract range predicate bounds for an indexed column from WHERE clause
///
/// This extracts comparison operators (>, <, >=, <=, BETWEEN) that can be
/// pushed down to the storage layer's range_scan() method.
///
/// Returns None if no suitable range predicate found for the column.
fn extract_range_predicate(expr: &Expression, column_name: &str) -> Option<RangePredicate> {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                // Handle simple comparisons: col > value, col < value, etc.
                BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual => {
                    // Check if left side is our column and right side is a literal
                    if is_column_reference(left, column_name) {
                        if let Expression::Literal(value) = right.as_ref() {
                            return Some(match op {
                                BinaryOperator::GreaterThan => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::GreaterThanOrEqual => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: true,
                                    inclusive_end: false,
                                },
                                BinaryOperator::LessThan => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::LessThanOrEqual => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: true,
                                },
                                _ => unreachable!(),
                            });
                        }
                    }
                    // Check if right side is our column and left side is a literal (flipped comparison)
                    else if is_column_reference(right, column_name) {
                        if let Expression::Literal(value) = left.as_ref() {
                            return Some(match op {
                                // Flip the comparison: value > col means col < value
                                BinaryOperator::GreaterThan => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::GreaterThanOrEqual => RangePredicate {
                                    start: None,
                                    end: Some(value.clone()),
                                    inclusive_start: false,
                                    inclusive_end: true,
                                },
                                BinaryOperator::LessThan => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: false,
                                    inclusive_end: false,
                                },
                                BinaryOperator::LessThanOrEqual => RangePredicate {
                                    start: Some(value.clone()),
                                    end: None,
                                    inclusive_start: true,
                                    inclusive_end: false,
                                },
                                _ => unreachable!(),
                            });
                        }
                    }
                }
                // Handle AND: can combine range predicates (e.g., col > 10 AND col < 20)
                BinaryOperator::And => {
                    let left_range = extract_range_predicate(left, column_name);
                    let right_range = extract_range_predicate(right, column_name);

                    // Merge ranges if both sides have predicates on our column
                    match (left_range, right_range) {
                        (Some(mut l), Some(r)) => {
                            // Merge the bounds
                            if l.start.is_none() {
                                l.start = r.start;
                                l.inclusive_start = r.inclusive_start;
                            }
                            if l.end.is_none() {
                                l.end = r.end;
                                l.inclusive_end = r.inclusive_end;
                            }
                            return Some(l);
                        }
                        (Some(l), None) => return Some(l),
                        (None, Some(r)) => return Some(r),
                        (None, None) => {}
                    }
                }
                _ => {}
            }
        }
        // Handle BETWEEN: col BETWEEN low AND high
        Expression::Between { expr: col_expr, low, high, negated, .. } => {
            if !negated && is_column_reference(col_expr, column_name) {
                if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                    (low.as_ref(), high.as_ref())
                {
                    return Some(RangePredicate {
                        start: Some(low_val.clone()),
                        end: Some(high_val.clone()),
                        inclusive_start: true,
                        inclusive_end: true,
                    });
                }
            }
        }
        _ => {}
    }

    None
}

/// Extract index predicate (range or IN) for an indexed column from WHERE clause
///
/// This extracts predicates that can be pushed down to the storage layer:
/// - Range predicates: >, <, >=, <=, BETWEEN
/// - IN predicates: IN (value1, value2, ...)
///
/// Returns None if no suitable predicate found for the column.
fn extract_index_predicate(expr: &Expression, column_name: &str) -> Option<IndexPredicate> {
    // First try to extract a range predicate
    if let Some(range) = extract_range_predicate(expr, column_name) {
        return Some(IndexPredicate::Range(range));
    }

    // Then try to extract an IN predicate
    match expr {
        // Handle IN with value list: col IN (1, 2, 3)
        Expression::InList { expr: col_expr, values: value_list, negated } => {
            if !negated && is_column_reference(col_expr, column_name) {
                // Extract literal values from the IN list
                let mut values = Vec::new();
                for item in value_list {
                    if let Expression::Literal(value) = item {
                        values.push(value.clone());
                    } else {
                        // If any item is not a literal, we can't optimize
                        return None;
                    }
                }
                if !values.is_empty() {
                    return Some(IndexPredicate::In(values));
                }
            }
        }
        // Handle AND: try both sides
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            // Try left side first
            if let Some(pred) = extract_index_predicate(left, column_name) {
                return Some(pred);
            }
            // Then try right side
            if let Some(pred) = extract_index_predicate(right, column_name) {
                return Some(pred);
            }
        }
        _ => {}
    }

    None
}

/// Execute an index scan
///
/// Uses the specified index to retrieve matching rows, then fetches full rows from the table.
/// This implements the "index scan + fetch" strategy with optimized range scans.
///
/// If sorted_columns is provided, the function preserves index order and returns results
/// marked as pre-sorted, allowing the caller to skip ORDER BY sorting.
pub(crate) fn execute_index_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: Option<&Expression>,
    sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    database: &Database,
) -> Result<super::FromResult, ExecutorError> {
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
            // Use storage layer's multi_lookup for IN predicates
            index_data.multi_lookup(&values)
        }
        None => {
            // Full index scan - collect all row indices from the index
            // Note: We do NOT sort by row index here - we preserve the order from BTreeMap iteration
            // which gives us results sorted by index key value (the correct semantic ordering)
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

    // Build schema
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

    // Apply WHERE clause predicates
    // Even with index range scan, we still need to filter for:
    // - Predicates on non-indexed columns
    // - Complex predicates that couldn't be pushed to index
    // - OR predicates (not yet optimized)
    //
    // IMPORTANT: We must apply the FULL WHERE clause here, not just table-local predicates.
    // The table-local predicate decomposition doesn't work correctly for unqualified column
    // references, causing predicates to be classified as "complex" and not applied at all.
    if let Some(where_expr) = where_clause {
        use crate::evaluator::CombinedExpressionEvaluator;

        // Create evaluator for filtering
        let evaluator = CombinedExpressionEvaluator::with_database(&schema, database);

        // Apply full WHERE clause to each row
        let mut filtered_rows = Vec::new();
        for row in rows {
            // Clear CSE cache before evaluating each row
            evaluator.clear_cse_cache();

            let include_row = match evaluator.eval(where_expr, &row)? {
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

    // Return results with sorting metadata if available
    match sorted_columns {
        Some(sorted) => Ok(super::FromResult::from_rows_sorted(schema, rows, sorted)),
        None => Ok(super::FromResult::from_rows(schema, rows)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{BinaryOperator, Expression};

    #[test]
    fn test_expression_filters_column_simple() {
        use vibesql_ast::BinaryOperator;
        use vibesql_types::SqlValue;

        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "age".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(25))),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_expression_filters_column_and() {
        use vibesql_ast::BinaryOperator;
        use vibesql_types::SqlValue;

        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "age".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(18))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "city".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Varchar("Boston".to_string()))),
            }),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(expression_filters_column(&expr, "city"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference() {
        let expr = Expression::ColumnRef {
            table: None,
            column: "age".to_string(),
        };

        assert!(is_column_reference(&expr, "age"));
        assert!(!is_column_reference(&expr, "name"));
    }

    #[test]
    fn test_extract_range_predicate_greater_than() {
        use vibesql_types::SqlValue;

        let expr = Expression::BinaryOp {
            op: BinaryOperator::GreaterThan,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(60))),
        };

        let range = extract_range_predicate(&expr, "col0").unwrap();
        assert_eq!(range.start, Some(SqlValue::Integer(60)));
        assert_eq!(range.end, None);
        assert_eq!(range.inclusive_start, false);
    }

    #[test]
    fn test_extract_range_predicate_less_than_or_equal() {
        use vibesql_types::SqlValue;

        let expr = Expression::BinaryOp {
            op: BinaryOperator::LessThanOrEqual,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(43))),
        };

        let range = extract_range_predicate(&expr, "col0").unwrap();
        assert_eq!(range.start, None);
        assert_eq!(range.end, Some(SqlValue::Integer(43)));
        assert_eq!(range.inclusive_end, true);
    }

    #[test]
    fn test_extract_range_predicate_between() {
        use vibesql_types::SqlValue;

        let expr = Expression::Between {
            expr: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
            low: Box::new(Expression::Literal(SqlValue::Integer(10))),
            high: Box::new(Expression::Literal(SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
        };

        let range = extract_range_predicate(&expr, "col0").unwrap();
        assert_eq!(range.start, Some(SqlValue::Integer(10)));
        assert_eq!(range.end, Some(SqlValue::Integer(20)));
        assert_eq!(range.inclusive_start, true);
        assert_eq!(range.inclusive_end, true);
    }

    #[test]
    fn test_extract_range_predicate_combined_and() {
        use vibesql_types::SqlValue;

        // col0 > 10 AND col0 < 20
        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "col0".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::LessThan,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "col0".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(20))),
            }),
        };

        let range = extract_range_predicate(&expr, "col0").unwrap();
        assert_eq!(range.start, Some(SqlValue::Integer(10)));
        assert_eq!(range.end, Some(SqlValue::Integer(20)));
        assert_eq!(range.inclusive_start, false);
        assert_eq!(range.inclusive_end, false);
    }

    #[test]
    fn test_extract_range_predicate_flipped_comparison() {
        use vibesql_types::SqlValue;

        // 60 < col0 (same as col0 > 60)
        let expr = Expression::BinaryOp {
            op: BinaryOperator::LessThan,
            left: Box::new(Expression::Literal(SqlValue::Integer(60))),
            right: Box::new(Expression::ColumnRef {
                table: None,
                column: "col0".to_string(),
            }),
        };

        let range = extract_range_predicate(&expr, "col0").unwrap();
        assert_eq!(range.start, Some(SqlValue::Integer(60)));
        assert_eq!(range.end, None);
        assert_eq!(range.inclusive_start, false);
    }
}

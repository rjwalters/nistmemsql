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
/// Returns Some(index_name) if an index should be used, None otherwise
pub(crate) fn should_use_index_scan(
    table_name: &str,
    where_clause: Option<&Expression>,
    database: &Database,
) -> Option<String> {
    // For now, we only use indexes if:
    // 1. There's a WHERE clause
    // 2. The table has user-defined indexes
    // 3. The WHERE clause references an indexed column

    let where_expr = where_clause?;

    // Get all indexes for this table
    let _table = database.get_table(table_name)?;
    let indexes = database.list_indexes_for_table(table_name);

    if indexes.is_empty() {
        return None;
    }

    // Check if WHERE clause can use any index
    // For now, we look for simple equality predicates: column = value
    // Future: support range scans, IN predicates, etc.

    for index_name in indexes {
        if let Some(index_metadata) = database.get_index(&index_name) {
            // Check if WHERE clause references the first column of this index
            // (For composite indexes, we can only use the index if the WHERE
            //  clause filters on the leftmost prefix of indexed columns)

            if let Some(first_indexed_column) = index_metadata.columns.first() {
                if expression_filters_column(where_expr, &first_indexed_column.column_name) {
                    return Some(index_name);
                }
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

/// Execute an index scan
///
/// Uses the specified index to retrieve matching rows, then fetches full rows from the table.
/// This implements the "index scan + fetch" strategy with optimized range scans.
pub(crate) fn execute_index_scan(
    table_name: &str,
    index_name: &str,
    alias: Option<&String>,
    where_clause: Option<&Expression>,
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

    // Get the first indexed column (for range predicate extraction)
    let indexed_column = index_metadata
        .columns
        .first()
        .map(|col| col.column_name.as_str())
        .unwrap_or("");

    // Try to extract range predicate for the indexed column
    let range_predicate = where_clause.and_then(|expr| extract_range_predicate(expr, indexed_column));

    // Get row indices using range scan if we have a range predicate, otherwise full scan
    let matching_row_indices: Vec<usize> = if let Some(range) = range_predicate {
        // Use storage layer's optimized range_scan
        index_data.range_scan(
            range.start.as_ref(),
            range.end.as_ref(),
            range.inclusive_start,
            range.inclusive_end,
        )
    } else {
        // Full index scan - collect all row indices from the index
        // Note: We do NOT sort by row index here - we preserve the order from BTreeMap iteration
        // which gives us results sorted by index key value (the correct semantic ordering)
        index_data
            .values()
            .flatten()
            .copied()
            .collect()
    };

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
    if let Some(where_expr) = where_clause {
        use super::predicates::apply_table_local_predicates;
        rows = apply_table_local_predicates(rows, schema.clone(), where_expr, table_name, database)?;
    }

    Ok(super::FromResult::from_rows(schema, rows))
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

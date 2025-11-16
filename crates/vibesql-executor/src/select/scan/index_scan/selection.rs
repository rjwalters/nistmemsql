//! Index selection logic
//!
//! Determines when and which index to use for query optimization.

use vibesql_ast::Expression;
use vibesql_storage::Database;

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
                // Check if ORDER BY columns match the index columns
                // Support multi-column ORDER BY matching
                can_use_index_for_order_by(order_items, &index_metadata.columns)
            } else {
                false
            };

            // Use this index if it satisfies WHERE, ORDER BY, or both
            if can_use_for_where || can_use_for_order {
                // Build sorted_columns metadata if ORDER BY can be satisfied
                let sorted_columns = if can_use_for_order {
                    // Build sorted_columns from all ORDER BY columns that match the index
                    let order_items = order_by.unwrap();
                    Some(
                        order_items
                            .iter()
                            .map(|item| {
                                let col_name = match &item.expr {
                                    Expression::ColumnRef { column, .. } => column.clone(),
                                    _ => unreachable!("can_use_index_for_order_by ensures simple column refs"),
                                };
                                (col_name, item.direction.clone())
                            })
                            .collect(),
                    )
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
pub(super) fn expression_filters_column(expr: &Expression, column_name: &str) -> bool {
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
pub(super) fn is_column_reference(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef { column, .. } => column == column_name,
        _ => false,
    }
}

/// Check if an index can be used to satisfy an ORDER BY clause
///
/// Returns true if the ORDER BY columns match a prefix of the index columns
/// and the sort directions are compatible (considering ASC/DESC on both sides).
///
/// Examples:
/// - ORDER BY col0 ASC can use index (col0 ASC)
/// - ORDER BY col0 DESC can use index (col0 DESC)
/// - ORDER BY col0, col1 can use index (col0, col1)
/// - ORDER BY col0 cannot use index (col0 DESC) - wrong direction
fn can_use_index_for_order_by(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
) -> bool {
    // ORDER BY must not have more columns than the index
    if order_items.len() > index_columns.len() {
        return false;
    }

    // Check each ORDER BY column against corresponding index column
    for (order_item, index_col) in order_items.iter().zip(index_columns.iter()) {
        // ORDER BY expression must be a simple column reference
        let order_col_name = match &order_item.expr {
            Expression::ColumnRef { table: None, column } => column,
            _ => return false, // Complex expressions not supported
        };

        // Column names must match
        if order_col_name != &index_col.column_name {
            return false;
        }

        // Sort directions must be compatible
        // Note: IndexColumn uses OrderDirection enum, same as OrderByItem
        if order_item.direction != index_col.direction {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;
    use vibesql_types::SqlValue;

    #[test]
    fn test_expression_filters_column_simple() {
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
}

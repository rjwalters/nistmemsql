//! Index scan execution
//!
//! This module provides index-based table scanning for improved query performance.
//! It integrates with the index catalog to use user-defined indexes when beneficial.

use vibesql_ast::Expression;
use vibesql_storage::{Database, Row};

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

/// Execute an index scan
///
/// Uses the specified index to retrieve matching rows, then fetches full rows from the table.
/// This implements the "index scan + fetch" strategy.
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

    let index_data = database
        .get_index_data(index_name)
        .ok_or_else(|| ExecutorError::IndexNotFound(index_name.to_string()))?;

    // For now, we perform a full index scan and then apply predicates
    // Future optimization: push predicates into index scan for range queries

    // Get all row indices from the index
    let mut matching_row_indices: Vec<usize> = index_data
        .data
        .values()
        .flatten()
        .copied()
        .collect();

    // Sort for deterministic order
    matching_row_indices.sort_unstable();

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
    // (Even with index scan, we still need to filter for complex predicates)
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
}

//! Join condition analyzer for detecting equi-join opportunities
//!
//! This module analyzes join conditions to identify equi-joins (equality-based joins)
//! which can be optimized using hash join algorithms instead of nested loop joins.

use vibesql_ast::{BinaryOperator, Expression};

use crate::schema::CombinedSchema;

/// Information about an equi-join condition
#[derive(Debug, Clone)]
pub struct EquiJoinInfo {
    /// Column index in the left table
    pub left_col_idx: usize,
    /// Column index in the right table
    pub right_col_idx: usize,
}

/// Analyze a join condition to detect if it's a simple equi-join
///
/// Returns Some(EquiJoinInfo) if the condition is an equi-join like:
/// - `t1.col = t2.col`
/// - `t2.col = t1.col`
/// - `(t1.col = t2.col AND ...) OR (t1.col = t2.col AND ...)`  (common equijoin in OR branches)
/// - `t1.col = t2.col AND ...` (equijoin in AND expression)
///
/// Returns None if:
/// - The condition doesn't contain any equi-join
/// - The condition doesn't reference columns from both sides
pub fn analyze_equi_join(
    condition: &Expression,
    schema: &CombinedSchema,
    left_column_count: usize,
) -> Option<EquiJoinInfo> {
    match condition {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Try to extract column references from both sides
            let left_idx_opt = extract_column_index(left, schema);
            let right_idx_opt = extract_column_index(right, schema);

            if std::env::var("JOIN_DEBUG").is_ok() {
                eprintln!("[JOIN_ANALYZER] Analyzing equijoin: {:?} = {:?}", left, right);
                eprintln!(
                    "[JOIN_ANALYZER] left_idx={:?}, right_idx={:?}, left_col_count={}",
                    left_idx_opt, right_idx_opt, left_column_count
                );
            }

            if let (Some(left_idx), Some(right_idx)) = (left_idx_opt, right_idx_opt) {
                // Check if one is from left table and one is from right table
                if left_idx < left_column_count && right_idx >= left_column_count {
                    // Left column from left table, right column from right table
                    if std::env::var("JOIN_DEBUG").is_ok() {
                        eprintln!(
                            "[JOIN_ANALYZER] SUCCESS: left_col={}, right_col={}",
                            left_idx,
                            right_idx - left_column_count
                        );
                    }
                    return Some(EquiJoinInfo {
                        left_col_idx: left_idx,
                        right_col_idx: right_idx - left_column_count,
                    });
                } else if right_idx < left_column_count && left_idx >= left_column_count {
                    // Left column from right table, right column from left table (swapped)
                    if std::env::var("JOIN_DEBUG").is_ok() {
                        eprintln!(
                            "[JOIN_ANALYZER] SUCCESS (swapped): left_col={}, right_col={}",
                            right_idx,
                            left_idx - left_column_count
                        );
                    }
                    return Some(EquiJoinInfo {
                        left_col_idx: right_idx,
                        right_col_idx: left_idx - left_column_count,
                    });
                } else {
                    if std::env::var("JOIN_DEBUG").is_ok() {
                        eprintln!("[JOIN_ANALYZER] FAIL: both columns on same side");
                    }
                }
            } else {
                if std::env::var("JOIN_DEBUG").is_ok() {
                    eprintln!("[JOIN_ANALYZER] FAIL: couldn't resolve column indices");
                }
            }
            None
        }
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // For AND conditions, recursively check left side first, then right
            // This allows us to find equijoin in expressions like: t1.id = t2.id AND t1.x > 5
            analyze_equi_join(left, schema, left_column_count)
                .or_else(|| analyze_equi_join(right, schema, left_column_count))
        }
        Expression::BinaryOp { op: BinaryOperator::Or, left, right } => {
            // For OR conditions, check if there's a common equijoin in all branches
            // This handles cases like: (t1.id = t2.id AND ...) OR (t1.id = t2.id AND ...)
            let left_equijoin = analyze_equi_join(left, schema, left_column_count);
            let right_equijoin = analyze_equi_join(right, schema, left_column_count);

            // Only return the equijoin if both sides have the SAME equijoin
            match (left_equijoin, right_equijoin) {
                (Some(left_info), Some(right_info))
                    if left_info.left_col_idx == right_info.left_col_idx
                        && left_info.right_col_idx == right_info.right_col_idx =>
                {
                    Some(left_info)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Check if a condition is a complex expression (contains AND/OR/other operators beyond simple
/// equality)
///
/// Returns true if the expression is anything other than a simple `col1 = col2` equality.
/// This is used to determine if a condition should still be applied as a post-join filter
/// even after extracting an equijoin from it.
pub fn is_complex_condition(expr: &Expression) -> bool {
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Equal, left, right } => {
            // Simple equality is only non-complex if both sides are column references
            !matches!(left.as_ref(), Expression::ColumnRef { .. })
                || !matches!(right.as_ref(), Expression::ColumnRef { .. })
        }
        Expression::BinaryOp { op: BinaryOperator::And, .. }
        | Expression::BinaryOp { op: BinaryOperator::Or, .. } => true,
        Expression::BinaryOp { .. } => true, // Other operators (>, <, LIKE, etc.)
        Expression::ColumnRef { .. } => false,
        _ => true, // Function calls, subqueries, etc. are complex
    }
}

/// Extract column index from an expression if it's a simple column reference
fn extract_column_index(expr: &Expression, schema: &CombinedSchema) -> Option<usize> {
    match expr {
        Expression::ColumnRef { table, column } => {
            // Resolve column to index in combined schema
            schema.get_column_index(table.as_deref(), column)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_types::DataType;

    use super::*;
    use crate::schema::CombinedSchema;

    /// Helper to create a test schema with two tables
    fn create_test_schema() -> CombinedSchema {
        let t1 = TableSchema::new(
            "t1".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, true),
                ColumnSchema::new("value".to_string(), DataType::Integer, true),
            ],
        );

        let t2 = TableSchema::new(
            "t2".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, true),
                ColumnSchema::new("data".to_string(), DataType::Integer, true),
            ],
        );

        let mut schema = CombinedSchema::from_table("t1".to_string(), t1);
        schema = CombinedSchema::combine(schema, "t2".to_string(), t2);
        schema
    }

    #[test]
    fn test_analyze_simple_equijoin() {
        let schema = create_test_schema();

        // t1.id = t2.id
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        };

        let result = analyze_equi_join(&expr, &schema, 2); // t1 has 2 columns
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.left_col_idx, 0); // t1.id is column 0
        assert_eq!(info.right_col_idx, 0); // t2.id is column 0
    }

    #[test]
    fn test_analyze_equijoin_in_and() {
        let schema = create_test_schema();

        // t1.id = t2.id AND t1.value > 5
        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                right: Box::new(Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "id".to_string(),
                }),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "value".to_string(),
                }),
                right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(5))),
            }),
        };

        let result = analyze_equi_join(&expr, &schema, 2);
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.left_col_idx, 0); // t1.id
        assert_eq!(info.right_col_idx, 0); // t2.id
    }

    #[test]
    fn test_analyze_equijoin_in_or_common() {
        let schema = create_test_schema();

        // (t1.id = t2.id AND t1.value > 5) OR (t1.id = t2.id AND t1.value < 10)
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "id".to_string(),
                    }),
                    right: Box::new(Expression::ColumnRef {
                        table: Some("t2".to_string()),
                        column: "id".to_string(),
                    }),
                }),
                right: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::GreaterThan,
                    left: Box::new(Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "value".to_string(),
                    }),
                    right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(5))),
                }),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::And,
                left: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "id".to_string(),
                    }),
                    right: Box::new(Expression::ColumnRef {
                        table: Some("t2".to_string()),
                        column: "id".to_string(),
                    }),
                }),
                right: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::LessThan,
                    left: Box::new(Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "value".to_string(),
                    }),
                    right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(10))),
                }),
            }),
        };

        let result = analyze_equi_join(&expr, &schema, 2);
        assert!(result.is_some());
        let info = result.unwrap();
        assert_eq!(info.left_col_idx, 0); // t1.id
        assert_eq!(info.right_col_idx, 0); // t2.id
    }

    #[test]
    fn test_analyze_equijoin_in_or_different() {
        let schema = create_test_schema();

        // (t1.id = t2.id) OR (t1.value = t2.data) - different equijoins
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                right: Box::new(Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "id".to_string(),
                }),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "value".to_string(),
                }),
                right: Box::new(Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "data".to_string(),
                }),
            }),
        };

        let result = analyze_equi_join(&expr, &schema, 2);
        // Should return None because the equijoins are different
        assert!(result.is_none());
    }

    #[test]
    fn test_is_complex_condition_simple_equijoin() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        };

        assert!(!is_complex_condition(&expr));
    }

    #[test]
    fn test_is_complex_condition_and() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            right: Box::new(Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        };

        assert!(is_complex_condition(&expr));
    }

    #[test]
    fn test_is_complex_condition_or() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(1))),
            right: Box::new(Expression::Literal(vibesql_types::SqlValue::Integer(2))),
        };

        assert!(is_complex_condition(&expr));
    }
}

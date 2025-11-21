//! Selectivity estimation for WHERE clause predicates
//!
//! This module estimates the fraction of rows that will pass a given predicate,
//! enabling cost-based predicate ordering to minimize evaluation costs.

use vibesql_ast::{BinaryOperator, Expression};
use vibesql_storage::statistics::TableStatistics;

/// Order predicates by selectivity (most selective first)
///
/// This function analyzes each predicate, estimates its selectivity using table statistics,
/// and reorders them so the most selective predicates are evaluated first. This minimizes
/// the number of rows that need to be checked by subsequent predicates.
///
/// # Arguments
/// * `predicates` - Vector of predicate expressions to order
/// * `stats` - Table statistics for selectivity estimation
///
/// # Returns
/// A new vector with predicates ordered by selectivity (most selective first)
///
/// # Example
/// ```ignore
/// // Before: [expensive_function(x), y = 5]
/// // After:  [y = 5, expensive_function(x)]
/// // If y = 5 filters 99% of rows, expensive_function only runs on 1%
/// ```
pub fn order_predicates_by_selectivity(
    predicates: Vec<Expression>,
    stats: &TableStatistics,
) -> Vec<Expression> {
    if predicates.is_empty() {
        return predicates;
    }

    // Calculate selectivity for each predicate
    let mut pred_with_selectivity: Vec<(f64, Expression)> = predicates
        .into_iter()
        .map(|pred| {
            let selectivity = estimate_selectivity(&pred, stats);
            (selectivity, pred)
        })
        .collect();

    // Sort by selectivity (ascending order - most selective first)
    // Lower selectivity = filters more rows = should run first
    pred_with_selectivity.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Extract ordered predicates
    pred_with_selectivity
        .into_iter()
        .map(|(_, pred)| pred)
        .collect()
}

/// Estimate the selectivity of a predicate (0.0 = filters all rows, 1.0 = keeps all rows)
///
/// This function analyzes predicates and uses table statistics to estimate
/// what fraction of rows will satisfy the predicate.
///
/// # Arguments
/// * `predicate` - The WHERE clause predicate expression
/// * `stats` - Table statistics for the table being filtered
///
/// # Returns
/// A value between 0.0 and 1.0 representing the estimated selectivity.
/// Lower values mean more selective (filters more rows).
pub fn estimate_selectivity(predicate: &Expression, stats: &TableStatistics) -> f64 {
    match predicate {
        // Boolean literals
        Expression::Literal(vibesql_types::SqlValue::Boolean(true)) => 1.0,
        Expression::Literal(vibesql_types::SqlValue::Boolean(false)) => 0.0,

        // AND: product of selectivities (assumes independence)
        // Must come BEFORE general BinaryOp to avoid being caught by it
        Expression::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            let left_sel = estimate_selectivity(left.as_ref(), stats);
            let right_sel = estimate_selectivity(right.as_ref(), stats);
            left_sel * right_sel
        }

        // OR: 1 - ((1 - s1) * (1 - s2)) (probability theory)
        // Must come BEFORE general BinaryOp to avoid being caught by it
        Expression::BinaryOp {
            op: BinaryOperator::Or,
            left,
            right,
        } => {
            let left_sel = estimate_selectivity(left.as_ref(), stats);
            let right_sel = estimate_selectivity(right.as_ref(), stats);
            1.0 - ((1.0 - left_sel) * (1.0 - right_sel))
        }

        // Binary operations (=, !=, <, >, <=, >=)
        // This catch-all must come AFTER specific AND/OR handling
        Expression::BinaryOp { op, left, right } => {
            estimate_binary_op_selectivity(op, left, right, stats)
        }

        // NOT: inverse selectivity
        Expression::UnaryOp {
            op: vibesql_ast::UnaryOperator::Not,
            expr,
        } => 1.0 - estimate_selectivity(expr, stats),

        // IS NULL / IS NOT NULL
        Expression::IsNull { expr, negated: false } => {
            estimate_is_null_selectivity(expr, stats)
        }
        Expression::IsNull { expr, negated: true } => {
            1.0 - estimate_is_null_selectivity(expr, stats)
        }

        // BETWEEN: treated as range (0.33 default)
        Expression::Between { .. } => 0.33,

        // IN: depends on number of values
        Expression::InList { values, .. } => {
            // Rough estimate: each value has 1/n_distinct chance
            let n_values = values.len() as f64;
            (n_values / 100.0).min(1.0) // Cap at 100% but rough heuristic
        }

        // LIKE: depends on pattern
        Expression::Like { pattern, .. } => estimate_like_selectivity(pattern),

        // Complex expressions: conservative estimate
        _ => 0.5,
    }
}

/// Estimate selectivity for binary operations (=, !=, <, >, <=, >=)
fn estimate_binary_op_selectivity(
    op: &BinaryOperator,
    left: &Expression,
    right: &Expression,
    stats: &TableStatistics,
) -> f64 {
    match op {
        BinaryOperator::Equal => estimate_equality_selectivity(left, right, stats),
        BinaryOperator::NotEqual => 1.0 - estimate_equality_selectivity(left, right, stats),
        BinaryOperator::LessThan | BinaryOperator::GreaterThan => {
            estimate_range_selectivity(op, left, right, stats)
        }
        BinaryOperator::LessThanOrEqual | BinaryOperator::GreaterThanOrEqual => {
            estimate_range_selectivity(op, left, right, stats)
        }
        _ => 0.5, // Other operators: conservative estimate
    }
}

/// Estimate selectivity for equality predicates (col = value)
fn estimate_equality_selectivity(
    left: &Expression,
    right: &Expression,
    stats: &TableStatistics,
) -> f64 {
    // Try to extract column name and literal value
    let (column_name, literal_value) = match (left, right) {
        (Expression::ColumnRef { column, .. }, Expression::Literal(val)) => (column, val),
        (Expression::Literal(val), Expression::ColumnRef { column, .. }) => (column, val),
        _ => return 0.1, // Can't analyze: default selectivity
    };

    // Look up column statistics (case-insensitive)
    let col_stats = stats.columns.get(column_name)
        .or_else(|| stats.columns.get(&column_name.to_uppercase()))
        .or_else(|| stats.columns.get(&column_name.to_lowercase()));

    if let Some(col_stats) = col_stats {
        col_stats.estimate_eq_selectivity(literal_value)
    } else {
        // No stats available: assume uniform distribution over 10 distinct values
        0.1
    }
}

/// Estimate selectivity for range predicates (col < value, col > value, etc.)
fn estimate_range_selectivity(
    op: &BinaryOperator,
    left: &Expression,
    right: &Expression,
    stats: &TableStatistics,
) -> f64 {
    // Extract column and value
    let (column_name, literal_value, operator_str) = match (op, left, right) {
        (BinaryOperator::LessThan, Expression::ColumnRef { column, .. }, Expression::Literal(val)) => {
            (column, val, "<")
        }
        (BinaryOperator::GreaterThan, Expression::ColumnRef { column, .. }, Expression::Literal(val)) => {
            (column, val, ">")
        }
        (BinaryOperator::LessThanOrEqual, Expression::ColumnRef { column, .. }, Expression::Literal(val)) => {
            (column, val, "<=")
        }
        (BinaryOperator::GreaterThanOrEqual, Expression::ColumnRef { column, .. }, Expression::Literal(val)) => {
            (column, val, ">=")
        }
        _ => return 0.33, // Default range selectivity
    };

    // Look up column statistics (case-insensitive)
    let col_stats = stats.columns.get(column_name)
        .or_else(|| stats.columns.get(&column_name.to_uppercase()))
        .or_else(|| stats.columns.get(&column_name.to_lowercase()));

    if let Some(col_stats) = col_stats {
        col_stats.estimate_range_selectivity(literal_value, operator_str)
    } else {
        // No stats: conservative estimate
        0.33
    }
}

/// Estimate selectivity for IS NULL
fn estimate_is_null_selectivity(expr: &Expression, stats: &TableStatistics) -> f64 {
    // Extract column name
    let column_name = match expr {
        Expression::ColumnRef { column, .. } => column,
        _ => return 0.1, // Default if not a column reference
    };

    // Look up null ratio from column statistics (case-insensitive)
    let col_stats = stats.columns.get(column_name)
        .or_else(|| stats.columns.get(&column_name.to_uppercase()))
        .or_else(|| stats.columns.get(&column_name.to_lowercase()));

    if let Some(col_stats) = col_stats {
        let total_rows = stats.row_count;
        if total_rows > 0 {
            col_stats.null_count as f64 / total_rows as f64
        } else {
            0.0
        }
    } else {
        // No stats: assume 10% nulls
        0.1
    }
}

/// Estimate selectivity for LIKE patterns
fn estimate_like_selectivity(pattern: &Expression) -> f64 {
    // Extract pattern string
    let pattern_str = match pattern {
        Expression::Literal(vibesql_types::SqlValue::Varchar(s)) => s,
        _ => return 0.5, // Unknown pattern
    };

    // Analyze pattern for wildcards
    if pattern_str.starts_with('%') && pattern_str.ends_with('%') {
        // %pattern% - very unselective
        0.8
    } else if pattern_str.starts_with('%') || pattern_str.ends_with('%') {
        // %pattern or pattern% - moderately selective
        0.5
    } else if pattern_str.contains('%') || pattern_str.contains('_') {
        // Has wildcards in middle
        0.3
    } else {
        // Exact match (equivalent to equality)
        0.1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    fn create_test_stats() -> TableStatistics {
        let schema = TableSchema::new(
            "test_table".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("status".to_string(), DataType::Varchar { max_length: Some(20) }, true),
            ],
        );

        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("active".to_string())]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("inactive".to_string())]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("active".to_string())]),
            Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar("active".to_string())]),
            Row::new(vec![SqlValue::Integer(5), SqlValue::Null]),
        ];

        TableStatistics::compute(&rows, &schema)
    }

    #[test]
    fn test_equality_selectivity() {
        let stats = create_test_stats();

        // status = 'active' should be 3/4 = 75% (of non-null values)
        let pred = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "status".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Varchar("active".to_string()))),
        };

        let selectivity = estimate_selectivity(&pred, &stats);
        assert!((selectivity - 0.75).abs() < 0.01);
    }

    #[test]
    fn test_and_selectivity() {
        let stats = create_test_stats();

        // (status = 'active') AND (id < 10)
        let pred = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "status".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Varchar("active".to_string()))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::LessThan,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
        };

        let selectivity = estimate_selectivity(&pred, &stats);

        // Should be product of individual selectivities
        // Note: id < 10 has selectivity 1.0 for this test data (all ids are < 10)
        // So: 0.75 * 1.0 = 0.75
        assert!((selectivity - 0.75).abs() < 0.01,
            "Expected selectivity 0.75, got {}", selectivity);
    }

    #[test]
    fn test_or_selectivity() {
        let stats = create_test_stats();

        // (status = 'active') OR (id < 10)
        let pred = Expression::BinaryOp {
            op: BinaryOperator::Or,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "status".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Varchar("active".to_string()))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::LessThan,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
        };

        let selectivity = estimate_selectivity(&pred, &stats);

        // OR formula: 1 - ((1 - 0.75) * (1 - 1.0)) = 1 - (0.25 * 0) = 1.0
        // Note: id < 10 has selectivity 1.0 for this test data (all ids are < 10)
        assert!((selectivity - 1.0).abs() < 0.01,
            "Expected selectivity 1.0, got {}", selectivity);
    }

    #[test]
    fn test_nested_and_or_selectivity() {
        let stats = create_test_stats();

        // (status = 'active' OR status = 'inactive') AND (id < 3)
        let pred = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Or,
                left: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "status".to_string(),
                    }),
                    right: Box::new(Expression::Literal(SqlValue::Varchar("active".to_string()))),
                }),
                right: Box::new(Expression::BinaryOp {
                    op: BinaryOperator::Equal,
                    left: Box::new(Expression::ColumnRef {
                        table: None,
                        column: "status".to_string(),
                    }),
                    right: Box::new(Expression::Literal(SqlValue::Varchar("inactive".to_string()))),
                }),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::LessThan,
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "id".to_string(),
                }),
                right: Box::new(Expression::Literal(SqlValue::Integer(3))),
            }),
        };

        let selectivity = estimate_selectivity(&pred, &stats);

        // Nested: (0.75 OR 0.25) AND 0.33
        // OR part: 1 - ((1 - 0.75) * (1 - 0.25)) = 1 - (0.25 * 0.75) = 1 - 0.1875 = 0.8125
        // AND with 0.33: 0.8125 * 0.33 ≈ 0.27
        assert!(selectivity >= 0.25 && selectivity <= 0.30,
            "Expected nested selectivity ~0.27, got {}", selectivity);
    }
}

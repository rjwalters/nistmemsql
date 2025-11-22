//! Columnar aggregation - high-performance aggregate computation

use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::scan::ColumnarScan;
use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Aggregate operation type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateOp {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

/// Compute an aggregate over a column with optional filtering
///
/// This is the core columnar aggregation function that processes
/// columns directly without materializing Row objects.
///
/// # Arguments
///
/// * `scan` - Columnar scan over the data
/// * `column_idx` - Index of the column to aggregate
/// * `op` - Aggregate operation (SUM, COUNT, etc.)
/// * `filter_bitmap` - Optional bitmap of which rows to include
///
/// # Returns
///
/// The aggregated SqlValue
pub fn compute_columnar_aggregate(
    scan: &ColumnarScan,
    column_idx: usize,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => compute_sum(scan, column_idx, filter_bitmap),
        AggregateOp::Count => compute_count(scan, filter_bitmap),
        AggregateOp::Avg => compute_avg(scan, column_idx, filter_bitmap),
        AggregateOp::Min => compute_min(scan, column_idx, filter_bitmap),
        AggregateOp::Max => compute_max(scan, column_idx, filter_bitmap),
    }
}

/// Compute SUM aggregate on a column
fn compute_sum(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let mut sum = 0.0;
    let mut count = 0;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Add to sum
        if let Some(value) = value_opt {
            match value {
                SqlValue::Integer(v) => sum += *v as f64,
                SqlValue::Bigint(v) => sum += *v as f64,
                SqlValue::Smallint(v) => sum += *v as f64,
                SqlValue::Float(v) => sum += *v as f64,
                SqlValue::Double(v) => sum += v,
                SqlValue::Numeric(v) => sum += v,
                SqlValue::Null => {} // NULL values don't contribute to sum
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Cannot compute SUM on non-numeric value: {:?}",
                        value
                    )))
                }
            }
            count += 1;
        }
    }

    // Return appropriate type based on input
    // For now, always return Double for simplicity
    Ok(if count > 0 { SqlValue::Double(sum) } else { SqlValue::Null })
}

/// Compute COUNT aggregate
fn compute_count(
    scan: &ColumnarScan,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let count = if let Some(bitmap) = filter_bitmap {
        bitmap.iter().filter(|&&pass| pass).count()
    } else {
        scan.len()
    };

    Ok(SqlValue::Integer(count as i64))
}

/// Compute AVG aggregate on a column
fn compute_avg(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let sum_result = compute_sum(scan, column_idx, filter_bitmap)?;
    let count_result = compute_count(scan, filter_bitmap)?;

    match (sum_result, count_result) {
        (SqlValue::Double(sum), SqlValue::Integer(count)) if count > 0 => {
            Ok(SqlValue::Double(sum / count as f64))
        }
        (SqlValue::Null, _) | (_, SqlValue::Integer(0)) => Ok(SqlValue::Null),
        _ => Err(ExecutorError::UnsupportedExpression("Invalid AVG computation".to_string())),
    }
}

/// Compute MIN aggregate on a column
fn compute_min(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let mut min_value: Option<SqlValue> = None;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        if let Some(value) = value_opt {
            if !matches!(value, SqlValue::Null) {
                min_value = Some(match &min_value {
                    None => value.clone(),
                    Some(current_min) => {
                        if compare_for_min_max(value, current_min) {
                            value.clone()
                        } else {
                            current_min.clone()
                        }
                    }
                });
            }
        }
    }

    Ok(min_value.unwrap_or(SqlValue::Null))
}

/// Compute MAX aggregate on a column
fn compute_max(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let mut max_value: Option<SqlValue> = None;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        if let Some(value) = value_opt {
            if !matches!(value, SqlValue::Null) {
                max_value = Some(match &max_value {
                    None => value.clone(),
                    Some(current_max) => {
                        if compare_for_min_max(current_max, value) {
                            value.clone()
                        } else {
                            current_max.clone()
                        }
                    }
                });
            }
        }
    }

    Ok(max_value.unwrap_or(SqlValue::Null))
}

/// Compare two values for MIN/MAX (returns true if a < b)
fn compare_for_min_max(a: &SqlValue, b: &SqlValue) -> bool {
    use std::cmp::Ordering;

    let ordering = match (a, b) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Bigint(a), SqlValue::Bigint(b)) => a.cmp(b),
        (SqlValue::Smallint(a), SqlValue::Smallint(b)) => a.cmp(b),
        (SqlValue::Float(a), SqlValue::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Double(a), SqlValue::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    };

    ordering == Ordering::Less
}

/// Compute multiple aggregates in a single pass over the data
///
/// This is more efficient than computing each aggregate separately
/// as it only scans the data once.
pub fn compute_multiple_aggregates(
    rows: &[Row],
    aggregates: &[(usize, AggregateOp)],
    filter_bitmap: Option<&[bool]>,
) -> Result<Vec<SqlValue>, ExecutorError> {
    let scan = ColumnarScan::new(rows);
    let mut results = Vec::with_capacity(aggregates.len());

    for (column_idx, op) in aggregates {
        let result = compute_columnar_aggregate(&scan, *column_idx, *op, filter_bitmap)?;
        results.push(result);
    }

    Ok(results)
}

/// Extract aggregate operations from AST expressions
///
/// Converts aggregate function expressions to (column_idx, AggregateOp) tuples
/// that can be used with columnar execution.
///
/// Currently supports:
/// - SUM(column) → (col_idx, AggregateOp::Sum)
/// - COUNT(*) or COUNT(column) → (0, AggregateOp::Count)
/// - AVG(column) → (col_idx, AggregateOp::Avg)
/// - MIN(column) → (col_idx, AggregateOp::Min)
/// - MAX(column) → (col_idx, AggregateOp::Max)
///
/// Returns None if the expression contains unsupported patterns:
/// - DISTINCT aggregates
/// - Multiple arguments
/// - Complex expressions as arguments (only simple column refs supported)
/// - Non-aggregate expressions
///
/// # Arguments
///
/// * `exprs` - The SELECT list expressions
/// * `schema` - The schema to resolve column names to indices
///
/// # Returns
///
/// Some(aggregates) if all expressions can be converted to simple aggregates,
/// None if any expression is too complex for columnar optimization.
pub fn extract_aggregates(
    exprs: &[Expression],
    schema: &CombinedSchema,
) -> Option<Vec<(usize, AggregateOp)>> {
    let mut aggregates = Vec::new();

    for expr in exprs {
        match expr {
            Expression::AggregateFunction { name, distinct, args } => {
                // DISTINCT not supported for columnar optimization
                if *distinct {
                    return None;
                }

                let op = match name.to_uppercase().as_str() {
                    "SUM" => AggregateOp::Sum,
                    "COUNT" => AggregateOp::Count,
                    "AVG" => AggregateOp::Avg,
                    "MIN" => AggregateOp::Min,
                    "MAX" => AggregateOp::Max,
                    _ => return None, // Unsupported aggregate function
                };

                // Handle COUNT(*)
                if op == AggregateOp::Count && args.is_empty() {
                    // For COUNT(*), use column 0 (the column index is ignored by compute_count)
                    aggregates.push((0, op));
                    continue;
                }

                // Handle COUNT(*) with wildcard argument
                if op == AggregateOp::Count && args.len() == 1 {
                    if matches!(args[0], Expression::Wildcard) {
                        aggregates.push((0, op));
                        continue;
                    }
                }

                // Extract column reference for other aggregates
                if args.len() != 1 {
                    return None; // Multiple arguments not supported
                }

                let column_idx = match &args[0] {
                    Expression::ColumnRef { table, column } => {
                        schema.get_column_index(table.as_deref(), column)?
                    }
                    _ => return None, // Complex expressions not supported
                };

                aggregates.push((column_idx, op));
            }
            _ => return None, // Non-aggregate expressions not supported
        }
    }

    Some(aggregates)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_rows() -> Vec<Row> {
        vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.5)]),
        ]
    }

    #[test]
    fn test_sum_aggregate() {
        let rows = make_test_rows();
        let scan = ColumnarScan::new(&rows);

        let result = compute_sum(&scan, 0, None).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 60.0).abs() < 0.001));

        let result = compute_sum(&scan, 1, None).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 7.5).abs() < 0.001));
    }

    #[test]
    fn test_count_aggregate() {
        let rows = make_test_rows();
        let scan = ColumnarScan::new(&rows);

        let result = compute_count(&scan, None).unwrap();
        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn test_sum_with_filter() {
        let rows = make_test_rows();
        let scan = ColumnarScan::new(&rows);
        let filter = vec![true, false, true]; // Include rows 0 and 2

        let result = compute_sum(&scan, 0, Some(&filter)).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 40.0).abs() < 0.001));
    }

    #[test]
    fn test_multiple_aggregates() {
        let rows = make_test_rows();
        let aggregates = vec![(0, AggregateOp::Sum), (1, AggregateOp::Avg)];

        let results = compute_multiple_aggregates(&rows, &aggregates, None).unwrap();
        assert_eq!(results.len(), 2);
        assert!(matches!(results[0], SqlValue::Double(sum) if (sum - 60.0).abs() < 0.001));
        assert!(matches!(results[1], SqlValue::Double(avg) if (avg - 2.5).abs() < 0.001));
    }

    #[test]
    fn test_extract_aggregates_simple() {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        use crate::schema::CombinedSchema;

        // Create a simple schema with two columns
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("col1".to_string(), DataType::Integer, false),
                ColumnSchema::new("col2".to_string(), DataType::DoublePrecision, false),
            ],
        );

        let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

        // Test SUM(col1)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ColumnRef { table: None, column: "col1".to_string() }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 1);
        assert_eq!(aggregates[0], (0, AggregateOp::Sum));

        // Test COUNT(*)
        let exprs = vec![Expression::AggregateFunction {
            name: "COUNT".to_string(),
            distinct: false,
            args: vec![Expression::Wildcard],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 1);
        assert_eq!(aggregates[0], (0, AggregateOp::Count));

        // Test multiple aggregates: SUM(col1), AVG(col2)
        let exprs = vec![
            Expression::AggregateFunction {
                name: "SUM".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef { table: None, column: "col1".to_string() }],
            },
            Expression::AggregateFunction {
                name: "AVG".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef { table: None, column: "col2".to_string() }],
            },
        ];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 2);
        assert_eq!(aggregates[0], (0, AggregateOp::Sum));
        assert_eq!(aggregates[1], (1, AggregateOp::Avg));
    }

    #[test]
    fn test_extract_aggregates_unsupported() {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        use crate::schema::CombinedSchema;

        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("col1".to_string(), DataType::Integer, false)],
        );

        let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

        // Test DISTINCT aggregate (should return None)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: true,
            args: vec![Expression::ColumnRef { table: None, column: "col1".to_string() }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());

        // Test non-aggregate expression (should return None)
        let exprs = vec![Expression::ColumnRef { table: None, column: "col1".to_string() }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());

        // Test complex expression in aggregate (should return None)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "col1".to_string() }),
                op: vibesql_ast::BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());
    }
}

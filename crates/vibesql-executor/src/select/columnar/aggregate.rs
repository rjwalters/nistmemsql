//! Columnar aggregation - high-performance aggregate computation

use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::scan::ColumnarScan;

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
/// Automatically detects when SIMD optimization is available (Int64/Float64 columns)
/// and falls back to scalar implementation for other types.
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
    // Try SIMD path for numeric columns (5-10x speedup)
    #[cfg(feature = "simd")]
    {
        use super::simd_aggregate::{can_use_simd_for_column, simd_aggregate_f64, simd_aggregate_i64};

        // Detect if column is SIMD-compatible
        if let Some(is_integer) = can_use_simd_for_column(scan, column_idx) {
            // Use SIMD implementation for Int64/Float64 columns
            return if is_integer {
                simd_aggregate_i64(scan, column_idx, op, filter_bitmap)
            } else {
                simd_aggregate_f64(scan, column_idx, op, filter_bitmap)
            };
        }
        // Fall through to scalar path for non-SIMD types
    }

    // Scalar fallback path (always available, used for String, Date, etc.)
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
                SqlValue::Null => {}, // NULL values don't contribute to sum
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        format!("Cannot compute SUM on non-numeric value: {:?}", value)
                    ))
                }
            }
            count += 1;
        }
    }

    // Return appropriate type based on input
    // For now, always return Double for simplicity
    Ok(if count > 0 {
        SqlValue::Double(sum)
    } else {
        SqlValue::Null
    })
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
        (SqlValue::Float(a), SqlValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Double(a), SqlValue::Double(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
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
            Expression::AggregateFunction {
                name,
                distinct,
                args,
            } => {
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

/// Compute aggregates with GROUP BY using columnar execution
///
/// This function implements hash-based grouping on columnar data, enabling
/// TPC-H Q1 and similar queries to use the columnar execution path.
///
/// # Algorithm
///
/// 1. Build hash table mapping group keys → (row indices in that group)
/// 2. For each group, compute aggregates over the grouped rows
/// 3. Return results as rows with (group_key_cols, aggregate_cols)
///
/// # Arguments
///
/// * `rows` - Input rows to group and aggregate
/// * `group_cols` - Indices of columns to group by
/// * `agg_cols` - List of (column_index, aggregate_op) pairs to compute
/// * `filter_bitmap` - Optional filter to apply before grouping
///
/// # Returns
///
/// Vec of Row objects, each containing group key values followed by aggregate results
///
/// # Example
///
/// ```rust,ignore
/// // SELECT l_returnflag, SUM(l_extendedprice)
/// // FROM lineitem
/// // GROUP BY l_returnflag
///
/// let rows = vec![
///     Row::new(vec![SqlValue::Text("A".to_string()), SqlValue::Double(100.0)]),
///     Row::new(vec![SqlValue::Text("B".to_string()), SqlValue::Double(200.0)]),
///     Row::new(vec![SqlValue::Text("A".to_string()), SqlValue::Double(150.0)]),
/// ];
///
/// let group_cols = vec![0]; // Group by first column (l_returnflag)
/// let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(l_extendedprice)
///
/// let result = columnar_group_by(&rows, &group_cols, &agg_cols, None)?;
/// // Returns:
/// // Row["A", 250.0]
/// // Row["B", 200.0]
/// ```
pub fn columnar_group_by(
    rows: &[Row],
    group_cols: &[usize],
    agg_cols: &[(usize, AggregateOp)],
    filter_bitmap: Option<&[bool]>,
) -> Result<Vec<Row>, ExecutorError> {
    use std::collections::HashMap;

    // Early return for empty input
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Create columnar scan for efficient column access
    let scan = ColumnarScan::new(rows);

    // Phase 1: Build hash table mapping group keys to row indices
    // HashMap<Vec<SqlValue>, Vec<usize>>
    // Key: group key values, Value: indices of rows in that group
    let mut groups: HashMap<Vec<SqlValue>, Vec<usize>> = HashMap::new();

    for row_idx in 0..rows.len() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Extract group key values for this row
        let mut group_key = Vec::with_capacity(group_cols.len());
        for &col_idx in group_cols {
            if let Some(value) = scan.column(col_idx).nth(row_idx).flatten() {
                group_key.push(value.clone());
            } else {
                // NULL in group key
                group_key.push(SqlValue::Null);
            }
        }

        // Add row index to this group
        groups.entry(group_key).or_insert_with(Vec::new).push(row_idx);
    }

    // Phase 2: Compute aggregates for each group
    let mut result_rows = Vec::with_capacity(groups.len());

    for (group_key, row_indices) in groups {
        // Create a bitmap for rows in this group
        let mut group_bitmap = vec![false; rows.len()];
        for &idx in &row_indices {
            group_bitmap[idx] = true;
        }

        // Compute aggregates for this group
        let mut result_values = Vec::with_capacity(group_key.len() + agg_cols.len());

        // First, add group key values
        result_values.extend(group_key);

        // Then, compute each aggregate
        for (col_idx, agg_op) in agg_cols {
            let agg_result = compute_columnar_aggregate(&scan, *col_idx, *agg_op, Some(&group_bitmap))?;
            result_values.push(agg_result);
        }

        result_rows.push(Row::new(result_values));
    }

    Ok(result_rows)
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
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

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
            args: vec![Expression::ColumnRef {
                table: None,
                column: "col1".to_string(),
            }],
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
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "col1".to_string(),
                }],
            },
            Expression::AggregateFunction {
                name: "AVG".to_string(),
                distinct: false,
                args: vec![Expression::ColumnRef {
                    table: None,
                    column: "col2".to_string(),
                }],
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
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("col1".to_string(), DataType::Integer, false)],
        );

        let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

        // Test DISTINCT aggregate (should return None)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: true,
            args: vec![Expression::ColumnRef {
                table: None,
                column: "col1".to_string(),
            }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());

        // Test non-aggregate expression (should return None)
        let exprs = vec![Expression::ColumnRef {
            table: None,
            column: "col1".to_string(),
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());

        // Test complex expression in aggregate (should return None)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "col1".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());
    }

    // GROUP BY tests

    #[test]
    fn test_columnar_group_by_simple() {
        // Test simple GROUP BY with one group column
        // SELECT status, SUM(amount) FROM test GROUP BY status
        let rows = vec![
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(100.0)]),
            Row::new(vec![SqlValue::Varchar("B".to_string()), SqlValue::Double(200.0)]),
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(150.0)]),
            Row::new(vec![SqlValue::Varchar("B".to_string()), SqlValue::Double(50.0)]),
        ];

        let group_cols = vec![0]; // Group by status
        let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(amount)

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 2 groups: A and B
        assert_eq!(result.len(), 2);

        // Sort results by group key for deterministic testing
        let mut sorted = result;
        sorted.sort_by(|a, b| {
            let a_key = a.get(0).unwrap();
            let b_key = b.get(0).unwrap();
            a_key.partial_cmp(b_key).unwrap()
        });

        // Check group A: SUM = 250.0
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Varchar("A".to_string())));
        assert!(matches!(sorted[0].get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001));

        // Check group B: SUM = 250.0
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Varchar("B".to_string())));
        assert!(matches!(sorted[1].get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001));
    }

    #[test]
    fn test_columnar_group_by_multiple_group_keys() {
        // Test GROUP BY with multiple columns
        // SELECT status, category, COUNT(*) FROM test GROUP BY status, category
        let rows = vec![
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Varchar("B".to_string()), SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Varchar("B".to_string()), SqlValue::Integer(2)]),
        ];

        let group_cols = vec![0, 1]; // Group by status, category
        let agg_cols = vec![(0, AggregateOp::Count)]; // COUNT(*)

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 4 groups: (A,1), (A,2), (B,1), (B,2)
        assert_eq!(result.len(), 4);

        // Verify we have correct counts
        for row in &result {
            let status = row.get(0).unwrap();
            let category = row.get(1).unwrap();
            let count = row.get(2).unwrap();

            match (status, category) {
                (&SqlValue::Varchar(ref s), &SqlValue::Integer(1)) if s == "A" => {
                    assert_eq!(count, &SqlValue::Integer(2)); // Two rows with A,1
                }
                (&SqlValue::Varchar(ref s), &SqlValue::Integer(2)) if s == "A" => {
                    assert_eq!(count, &SqlValue::Integer(1)); // One row with A,2
                }
                (&SqlValue::Varchar(ref s), &SqlValue::Integer(1)) if s == "B" => {
                    assert_eq!(count, &SqlValue::Integer(1)); // One row with B,1
                }
                (&SqlValue::Varchar(ref s), &SqlValue::Integer(2)) if s == "B" => {
                    assert_eq!(count, &SqlValue::Integer(1)); // One row with B,2
                }
                _ => panic!("Unexpected group key: {:?}, {:?}", status, category),
            }
        }
    }

    #[test]
    fn test_columnar_group_by_multiple_aggregates() {
        // Test GROUP BY with multiple aggregate functions
        // SELECT category, SUM(price), AVG(quantity), COUNT(*) FROM test GROUP BY category
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(100.0), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Double(200.0), SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(150.0), SqlValue::Integer(15)]),
        ];

        let group_cols = vec![0]; // Group by category
        let agg_cols = vec![
            (1, AggregateOp::Sum),   // SUM(price)
            (2, AggregateOp::Avg),   // AVG(quantity)
            (0, AggregateOp::Count), // COUNT(*)
        ];

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 2 groups
        assert_eq!(result.len(), 2);

        // Sort by category for deterministic testing
        let mut sorted = result;
        sorted.sort_by(|a, b| {
            let a_key = a.get(0).unwrap();
            let b_key = b.get(0).unwrap();
            a_key.partial_cmp(b_key).unwrap()
        });

        // Group 1: SUM=250, AVG=12.5, COUNT=2
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Integer(1)));
        assert!(matches!(sorted[0].get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001));
        assert!(matches!(sorted[0].get(2), Some(&SqlValue::Double(avg)) if (avg - 12.5).abs() < 0.001));
        assert_eq!(sorted[0].get(3), Some(&SqlValue::Integer(2)));

        // Group 2: SUM=200, AVG=20.0, COUNT=1
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Integer(2)));
        assert!(matches!(sorted[1].get(1), Some(&SqlValue::Double(sum)) if (sum - 200.0).abs() < 0.001));
        assert!(matches!(sorted[1].get(2), Some(&SqlValue::Double(avg)) if (avg - 20.0).abs() < 0.001));
        assert_eq!(sorted[1].get(3), Some(&SqlValue::Integer(1)));
    }

    #[test]
    fn test_columnar_group_by_with_filter() {
        // Test GROUP BY with pre-filtering
        // SELECT status, SUM(amount) FROM test WHERE amount > 100 GROUP BY status
        let rows = vec![
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(100.0)]),
            Row::new(vec![SqlValue::Varchar("B".to_string()), SqlValue::Double(200.0)]),
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(150.0)]),
            Row::new(vec![SqlValue::Varchar("B".to_string()), SqlValue::Double(50.0)]),
        ];

        // Filter: amount > 100 (rows 1 and 2)
        let filter = vec![false, true, true, false];

        let group_cols = vec![0]; // Group by status
        let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(amount)

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, Some(&filter)).unwrap();

        // Should have 2 groups (only rows passing filter)
        assert_eq!(result.len(), 2);

        // Sort results by group key
        let mut sorted = result;
        sorted.sort_by(|a, b| {
            let a_key = a.get(0).unwrap();
            let b_key = b.get(0).unwrap();
            a_key.partial_cmp(b_key).unwrap()
        });

        // Check group A: only row 2 (150.0) passes filter
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Varchar("A".to_string())));
        assert!(matches!(sorted[0].get(1), Some(&SqlValue::Double(sum)) if (sum - 150.0).abs() < 0.001));

        // Check group B: only row 1 (200.0) passes filter
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Varchar("B".to_string())));
        assert!(matches!(sorted[1].get(1), Some(&SqlValue::Double(sum)) if (sum - 200.0).abs() < 0.001));
    }

    #[test]
    fn test_columnar_group_by_empty_input() {
        let rows: Vec<Row> = vec![];
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Sum)];

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should return empty result for empty input
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_columnar_group_by_null_in_group_key() {
        // Test that NULL values in group keys are handled correctly
        let rows = vec![
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(100.0)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(200.0)]),
            Row::new(vec![SqlValue::Varchar("A".to_string()), SqlValue::Double(150.0)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(50.0)]),
        ];

        let group_cols = vec![0]; // Group by first column
        let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();

        // Should have 2 groups: "A" and NULL
        assert_eq!(result.len(), 2);

        // Find the groups
        let a_group = result.iter().find(|r| matches!(r.get(0), Some(&SqlValue::Varchar(ref s)) if s == "A"));
        let null_group = result.iter().find(|r| matches!(r.get(0), Some(&SqlValue::Null)));

        assert!(a_group.is_some());
        assert!(null_group.is_some());

        // Check "A" group: 100 + 150 = 250
        assert!(matches!(a_group.unwrap().get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001));

        // Check NULL group: 200 + 50 = 250
        assert!(matches!(null_group.unwrap().get(1), Some(&SqlValue::Double(sum)) if (sum - 250.0).abs() < 0.001));
    }
}

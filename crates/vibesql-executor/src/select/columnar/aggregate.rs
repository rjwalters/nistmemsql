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

/// Source of data for an aggregate - either a simple column or an expression
#[derive(Debug, Clone)]
pub enum AggregateSource {
    /// Simple column reference (fast path) - just a column index
    Column(usize),
    /// Complex expression that needs evaluation (e.g., a * b)
    Expression(Expression),
}

/// A complete aggregate specification
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    pub op: AggregateOp,
    pub source: AggregateSource,
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

/// Evaluate a simple arithmetic expression for a single row
///
/// This is a lightweight evaluator for the subset of expressions we support
/// in columnar aggregates (column references and binary operations).
fn eval_simple_expr(
    expr: &Expression,
    row: &Row,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    match expr {
        Expression::ColumnRef { table, column } => {
            let col_idx = schema.get_column_index(table.as_deref(), column)
                .ok_or_else(|| ExecutorError::UnsupportedExpression(
                    format!("Column not found: {}", column)
                ))?;
            Ok(row.get(col_idx).cloned().unwrap_or(SqlValue::Null))
        }
        Expression::Literal(val) => Ok(val.clone()),
        Expression::BinaryOp { left, op, right } => {
            let left_val = eval_simple_expr(left, row, schema)?;
            let right_val = eval_simple_expr(right, row, schema)?;

            // Use the evaluator's operators module
            use crate::evaluator::operators::OperatorRegistry;
            OperatorRegistry::eval_binary_op(&left_val, op, &right_val, vibesql_types::SqlMode::default())
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "Complex expressions not supported in columnar aggregates".to_string()
        )),
    }
}

/// Compute multiple aggregates in a single pass over the data
///
/// This is more efficient than computing each aggregate separately
/// as it only scans the data once.
pub fn compute_multiple_aggregates(
    rows: &[Row],
    aggregates: &[AggregateSpec],
    filter_bitmap: Option<&[bool]>,
    schema: Option<&CombinedSchema>,
) -> Result<Vec<SqlValue>, ExecutorError> {
    let scan = ColumnarScan::new(rows);
    let mut results = Vec::with_capacity(aggregates.len());

    for spec in aggregates {
        let result = match &spec.source {
            // Fast path: direct column aggregation
            AggregateSource::Column(column_idx) => {
                compute_columnar_aggregate(&scan, *column_idx, spec.op, filter_bitmap)?
            }
            // Expression path: evaluate expression for each row, then aggregate
            AggregateSource::Expression(expr) => {
                let schema = schema.ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(
                        "Schema required for expression aggregates".to_string()
                    )
                })?;
                compute_expression_aggregate(rows, expr, spec.op, filter_bitmap, schema)?
            }
        };
        results.push(result);
    }

    Ok(results)
}

/// Compute an aggregate over an expression (e.g., SUM(a * b))
///
/// Evaluates the expression for each row, then aggregates the results.
fn compute_expression_aggregate(
    rows: &[Row],
    expr: &Expression,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
    schema: &CombinedSchema,
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => {
            let mut sum = 0.0;
            let mut count = 0;

            for (row_idx, row) in rows.iter().enumerate() {
                // Check filter bitmap
                if let Some(bitmap) = filter_bitmap {
                    if !bitmap.get(row_idx).copied().unwrap_or(false) {
                        continue;
                    }
                }

                // Evaluate expression for this row
                let value = eval_simple_expr(expr, row, schema)?;

                // Add to sum
                if !matches!(value, SqlValue::Null) {
                    match value {
                        SqlValue::Integer(v) => sum += v as f64,
                        SqlValue::Bigint(v) => sum += v as f64,
                        SqlValue::Smallint(v) => sum += v as f64,
                        SqlValue::Float(v) => sum += v as f64,
                        SqlValue::Double(v) => sum += v,
                        SqlValue::Numeric(v) => sum += v,
                        SqlValue::Null => {}, // Already checked above
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                format!("Cannot compute SUM on non-numeric value: {:?}", value)
                            ))
                        }
                    }
                    count += 1;
                }
            }

            Ok(if count > 0 {
                SqlValue::Double(sum)
            } else {
                SqlValue::Null
            })
        }
        AggregateOp::Count => {
            // COUNT of expression counts non-NULL results
            let mut count = 0;
            for (row_idx, row) in rows.iter().enumerate() {
                if let Some(bitmap) = filter_bitmap {
                    if !bitmap.get(row_idx).copied().unwrap_or(false) {
                        continue;
                    }
                }
                let value = eval_simple_expr(expr, row, schema)?;
                if !matches!(value, SqlValue::Null) {
                    count += 1;
                }
            }
            Ok(SqlValue::Integer(count))
        }
        AggregateOp::Avg => {
            // AVG(expr) = SUM(expr) / COUNT(expr)
            let sum_result = compute_expression_aggregate(rows, expr, AggregateOp::Sum, filter_bitmap, schema)?;
            let count_result = compute_expression_aggregate(rows, expr, AggregateOp::Count, filter_bitmap, schema)?;

            match (sum_result, count_result) {
                (SqlValue::Double(sum), SqlValue::Integer(count)) if count > 0 => {
                    Ok(SqlValue::Double(sum / count as f64))
                }
                _ => Ok(SqlValue::Null),
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            let mut result_value: Option<SqlValue> = None;

            for (row_idx, row) in rows.iter().enumerate() {
                if let Some(bitmap) = filter_bitmap {
                    if !bitmap.get(row_idx).copied().unwrap_or(false) {
                        continue;
                    }
                }

                let value = eval_simple_expr(expr, row, schema)?;
                if !matches!(value, SqlValue::Null) {
                    result_value = Some(match &result_value {
                        None => value,
                        Some(current) => {
                            let should_update = if op == AggregateOp::Min {
                                compare_for_min_max(&value, current)
                            } else {
                                compare_for_min_max(current, &value)
                            };
                            if should_update {
                                value
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }

            Ok(result_value.unwrap_or(SqlValue::Null))
        }
    }
}

/// Extract aggregate operations from AST expressions
///
/// Converts aggregate function expressions to AggregateSpec objects
/// that can be used with columnar execution.
///
/// Currently supports:
/// - SUM(column) → Column aggregate (fast path)
/// - SUM(a * b) → Expression aggregate (evaluates expression per row)
/// - COUNT(*) or COUNT(column) → Column aggregate
/// - AVG(column) or AVG(expr) → Column/Expression aggregate
/// - MIN(column) or MIN(expr) → Column/Expression aggregate
/// - MAX(column) or MAX(expr) → Column/Expression aggregate
///
/// Supported expression types:
/// - Simple column references (fast path)
/// - Binary operations (+, -, *, /) with column references
///
/// Returns None if the expression contains unsupported patterns:
/// - DISTINCT aggregates
/// - Multiple arguments
/// - Complex expressions (subqueries, function calls, etc.)
/// - Non-aggregate expressions
///
/// # Arguments
///
/// * `exprs` - The SELECT list expressions
/// * `schema` - The schema to resolve column names to indices
///
/// # Returns
///
/// Some(aggregates) if all expressions can be converted to aggregates,
/// None if any expression is too complex for columnar optimization.
pub fn extract_aggregates(
    exprs: &[Expression],
    schema: &CombinedSchema,
) -> Option<Vec<AggregateSpec>> {
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
                    aggregates.push(AggregateSpec {
                        op,
                        source: AggregateSource::Column(0),
                    });
                    continue;
                }

                // Handle COUNT(*) with wildcard argument
                if op == AggregateOp::Count && args.len() == 1 {
                    if matches!(args[0], Expression::Wildcard) {
                        aggregates.push(AggregateSpec {
                            op,
                            source: AggregateSource::Column(0),
                        });
                        continue;
                    }
                }

                // Extract source (column or expression) for other aggregates
                if args.len() != 1 {
                    return None; // Multiple arguments not supported
                }

                let source = match &args[0] {
                    // Fast path: simple column reference
                    Expression::ColumnRef { table, column } => {
                        let column_idx = schema.get_column_index(table.as_deref(), column)?;
                        AggregateSource::Column(column_idx)
                    }
                    // New: support binary operations like a * b
                    Expression::BinaryOp { left, op: bin_op, right } => {
                        // Check if this is a simple binary operation we can handle
                        if is_simple_arithmetic_expr(&args[0], schema).is_some() {
                            AggregateSource::Expression(args[0].clone())
                        } else {
                            return None; // Complex expression not supported
                        }
                    }
                    _ => return None, // Other expression types not supported
                };

                aggregates.push(AggregateSpec { op, source });
            }
            _ => return None, // Non-aggregate expressions not supported
        }
    }

    Some(aggregates)
}

/// Check if an expression is a simple arithmetic expression we can optimize
///
/// Returns Some(()) if the expression only contains column references and
/// arithmetic operations (+, -, *, /), which we can efficiently evaluate.
/// Returns None if the expression contains unsupported operations.
fn is_simple_arithmetic_expr(expr: &Expression, schema: &CombinedSchema) -> Option<()> {
    match expr {
        Expression::ColumnRef { table, column } => {
            // Verify column exists
            schema.get_column_index(table.as_deref(), column)?;
            Some(())
        }
        Expression::Literal(_) => Some(()),
        Expression::BinaryOp { left, op, right } => {
            // Only support arithmetic operations
            use vibesql_ast::BinaryOperator::*;
            match op {
                Plus | Minus | Multiply | Divide => {
                    is_simple_arithmetic_expr(left, schema)?;
                    is_simple_arithmetic_expr(right, schema)?;
                    Some(())
                }
                _ => None, // Comparison ops, logical ops, etc. not supported
            }
        }
        _ => None, // Function calls, subqueries, etc. not supported
    }
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
        let aggregates = vec![
            AggregateSpec { op: AggregateOp::Sum, source: AggregateSource::Column(0) },
            AggregateSpec { op: AggregateOp::Avg, source: AggregateSource::Column(1) },
        ];

        let results = compute_multiple_aggregates(&rows, &aggregates, None, None).unwrap();
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
        assert!(matches!(aggregates[0].op, AggregateOp::Sum));
        assert!(matches!(aggregates[0].source, AggregateSource::Column(0)));

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
        assert!(matches!(aggregates[0].op, AggregateOp::Count));
        assert!(matches!(aggregates[0].source, AggregateSource::Column(0)));

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
        assert!(matches!(aggregates[0].op, AggregateOp::Sum));
        assert!(matches!(aggregates[0].source, AggregateSource::Column(0)));
        assert!(matches!(aggregates[1].op, AggregateOp::Avg));
        assert!(matches!(aggregates[1].source, AggregateSource::Column(1)));
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

        // Test subquery in aggregate (should return None - not supported)
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::ScalarSubquery(Box::new(vibesql_ast::SelectStmt {
                with_clause: None,
                distinct: false,
                select_list: vec![],
                into_table: None,
                into_variables: None,
                from: None,
                where_clause: None,
                group_by: None,
                having: None,
                order_by: None,
                limit: None,
                offset: None,
                set_operation: None,
            }))],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_aggregates_with_expression() {
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        // Create a simple schema with two columns
        let schema = TableSchema::new(
            "test".to_string(),
            vec![
                ColumnSchema::new("price".to_string(), DataType::DoublePrecision, false),
                ColumnSchema::new("discount".to_string(), DataType::DoublePrecision, false),
            ],
        );

        let combined_schema = CombinedSchema::from_table("test".to_string(), schema);

        // Test SUM(price * discount) - simple binary operation
        let exprs = vec![Expression::AggregateFunction {
            name: "SUM".to_string(),
            distinct: false,
            args: vec![Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Multiply,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "discount".to_string(),
                }),
            }],
        }];

        let result = extract_aggregates(&exprs, &combined_schema);
        assert!(result.is_some());
        let aggregates = result.unwrap();
        assert_eq!(aggregates.len(), 1);
        assert!(matches!(aggregates[0].op, AggregateOp::Sum));
        assert!(matches!(aggregates[0].source, AggregateSource::Expression(_)));
    }
}

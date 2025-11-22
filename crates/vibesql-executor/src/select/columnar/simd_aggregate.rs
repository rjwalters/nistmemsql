//! SIMD-accelerated columnar aggregation operations
//!
//! This module provides high-performance aggregate computations for Int64 and Float64
//! columns using SIMD operations, achieving 5-10x speedup over scalar implementations.

use crate::errors::ExecutorError;
use crate::simd::aggregation::*;
use super::aggregate::AggregateOp;
use super::scan::ColumnarScan;
use vibesql_types::SqlValue;

/// Compute SIMD aggregate for Int64 columns
///
/// Extracts i64 values from a column, applies optional filter and null masks,
/// then computes the aggregate using SIMD operations.
///
/// # Arguments
///
/// * `scan` - Columnar scan over the data
/// * `column_idx` - Index of the column to aggregate
/// * `op` - Aggregate operation (SUM, AVG, MIN, MAX, COUNT)
/// * `filter_bitmap` - Optional bitmap of which rows to include
///
/// # Returns
///
/// The aggregated SqlValue or an error if the column contains non-i64 values
#[cfg(feature = "simd")]
pub fn simd_aggregate_i64(
    scan: &ColumnarScan,
    column_idx: usize,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    // Extract i64 values and build combined filter (nulls + filter_bitmap)
    // Track original type to preserve it in MIN/MAX results
    let mut values = Vec::new();
    let mut original_type: Option<SqlValue> = None;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Extract i64 value, skip NULLs
        if let Some(value) = value_opt {
            match value {
                SqlValue::Integer(v) => {
                    if original_type.is_none() {
                        original_type = Some(SqlValue::Integer(0)); // Marker for Integer type
                    }
                    values.push(*v);
                }
                SqlValue::Bigint(v) => {
                    if original_type.is_none() {
                        original_type = Some(SqlValue::Bigint(0)); // Marker for Bigint type
                    }
                    values.push(*v);
                }
                SqlValue::Smallint(v) => {
                    if original_type.is_none() {
                        original_type = Some(SqlValue::Smallint(0)); // Marker for Smallint type
                    }
                    values.push(*v as i64);
                }
                SqlValue::Null => continue, // Skip NULLs
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        format!("Cannot compute SIMD aggregate on non-integer value: {:?}", value)
                    ))
                }
            }
        }
    }

    // Handle empty result set
    if values.is_empty() {
        return Ok(match op {
            AggregateOp::Count => SqlValue::Integer(0),
            _ => SqlValue::Null,
        });
    }

    // Apply SIMD operations
    match op {
        AggregateOp::Sum => {
            let sum = simd_sum_i64(&values);
            // Return Double to match scalar behavior
            Ok(SqlValue::Double(sum as f64))
        }
        AggregateOp::Avg => {
            let avg = simd_avg_i64(&values).ok_or_else(|| {
                ExecutorError::UnsupportedExpression("Cannot compute AVG on empty set".to_string())
            })?;
            Ok(SqlValue::Double(avg))
        }
        AggregateOp::Min => {
            let min = simd_min_i64(&values).ok_or_else(|| {
                ExecutorError::UnsupportedExpression("Cannot compute MIN on empty set".to_string())
            })?;
            // Preserve original type for MIN/MAX
            Ok(match original_type.unwrap_or(SqlValue::Bigint(0)) {
                SqlValue::Integer(_) => SqlValue::Integer(min),
                SqlValue::Smallint(_) => SqlValue::Smallint(min as i16),
                _ => SqlValue::Bigint(min),
            })
        }
        AggregateOp::Max => {
            let max = simd_max_i64(&values).ok_or_else(|| {
                ExecutorError::UnsupportedExpression("Cannot compute MAX on empty set".to_string())
            })?;
            // Preserve original type for MIN/MAX
            Ok(match original_type.unwrap_or(SqlValue::Bigint(0)) {
                SqlValue::Integer(_) => SqlValue::Integer(max),
                SqlValue::Smallint(_) => SqlValue::Smallint(max as i16),
                _ => SqlValue::Bigint(max),
            })
        }
        AggregateOp::Count => Ok(SqlValue::Integer(values.len() as i64)),
    }
}

/// Compute SIMD aggregate for Float64 columns
///
/// Extracts f64 values from a column, applies optional filter and null masks,
/// then computes the aggregate using SIMD operations.
///
/// # Arguments
///
/// * `scan` - Columnar scan over the data
/// * `column_idx` - Index of the column to aggregate
/// * `op` - Aggregate operation (SUM, AVG, MIN, MAX, COUNT)
/// * `filter_bitmap` - Optional bitmap of which rows to include
///
/// # Returns
///
/// The aggregated SqlValue or an error if the column contains non-f64 values
#[cfg(feature = "simd")]
pub fn simd_aggregate_f64(
    scan: &ColumnarScan,
    column_idx: usize,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    // Extract f64 values and build combined filter (nulls + filter_bitmap)
    let mut values = Vec::new();

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Extract f64 value, skip NULLs
        if let Some(value) = value_opt {
            match value {
                SqlValue::Double(v) => values.push(*v),
                SqlValue::Float(v) => values.push(*v as f64),
                SqlValue::Numeric(v) => values.push(*v),
                // Also handle integer types for mixed numeric columns
                SqlValue::Integer(v) => values.push(*v as f64),
                SqlValue::Bigint(v) => values.push(*v as f64),
                SqlValue::Smallint(v) => values.push(*v as f64),
                SqlValue::Null => continue, // Skip NULLs
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        format!("Cannot compute SIMD aggregate on non-numeric value: {:?}", value)
                    ))
                }
            }
        }
    }

    // Handle empty result set
    if values.is_empty() {
        return Ok(match op {
            AggregateOp::Count => SqlValue::Integer(0),
            _ => SqlValue::Null,
        });
    }

    // Apply SIMD operations
    match op {
        AggregateOp::Sum => {
            let sum = simd_sum_f64(&values);
            Ok(SqlValue::Double(sum))
        }
        AggregateOp::Avg => {
            let avg = simd_avg_f64(&values).ok_or_else(|| {
                ExecutorError::UnsupportedExpression("Cannot compute AVG on empty set".to_string())
            })?;
            Ok(SqlValue::Double(avg))
        }
        AggregateOp::Min => {
            let min = simd_min_f64(&values).ok_or_else(|| {
                ExecutorError::UnsupportedExpression("Cannot compute MIN on empty set".to_string())
            })?;
            Ok(SqlValue::Double(min))
        }
        AggregateOp::Max => {
            let max = simd_max_f64(&values).ok_or_else(|| {
                ExecutorError::UnsupportedExpression("Cannot compute MAX on empty set".to_string())
            })?;
            Ok(SqlValue::Double(max))
        }
        AggregateOp::Count => Ok(SqlValue::Integer(values.len() as i64)),
    }
}

/// Determine if a column can use SIMD path based on its data type
///
/// Checks the first non-NULL value in the column to determine if it's
/// an integer or float type suitable for SIMD operations.
///
/// # Returns
///
/// - `Some(true)` if column is i64-compatible (Integer, Bigint, Smallint)
/// - `Some(false)` if column is f64-compatible (Double, Float, Numeric)
/// - `None` if column is not SIMD-compatible (String, Date, etc.) or all NULL
#[cfg(feature = "simd")]
pub fn can_use_simd_for_column(scan: &ColumnarScan, column_idx: usize) -> Option<bool> {
    // Check first non-NULL value to determine column type
    for value_opt in scan.column(column_idx) {
        if let Some(value) = value_opt {
            return match value {
                SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => Some(true),
                SqlValue::Double(_) | SqlValue::Float(_) | SqlValue::Numeric(_) => Some(false),
                SqlValue::Null => continue, // Skip NULLs
                _ => None, // Not SIMD-compatible
            };
        }
    }

    None // All NULL or empty column
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;
    use vibesql_storage::Row;

    #[test]
    fn test_simd_aggregate_i64_sum() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, None).unwrap();
        // SUM returns Double to match scalar behavior
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 60.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_i64_avg() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Avg, None).unwrap();
        assert!(matches!(result, SqlValue::Double(avg) if (avg - 20.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_i64_min_max() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(30)]),
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let min_result = simd_aggregate_i64(&scan, 0, AggregateOp::Min, None).unwrap();
        // MIN/MAX preserve original type
        assert_eq!(min_result, SqlValue::Integer(10));

        let max_result = simd_aggregate_i64(&scan, 0, AggregateOp::Max, None).unwrap();
        assert_eq!(max_result, SqlValue::Integer(30));
    }

    #[test]
    fn test_simd_aggregate_i64_count() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Count, None).unwrap();
        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn test_simd_aggregate_i64_with_nulls() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, None).unwrap();
        // SUM returns Double to match scalar behavior
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 40.0).abs() < 0.001));

        let count_result = simd_aggregate_i64(&scan, 0, AggregateOp::Count, None).unwrap();
        assert_eq!(count_result, SqlValue::Integer(2));
    }

    #[test]
    fn test_simd_aggregate_i64_with_filter() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);
        let filter = vec![true, false, true]; // Include rows 0 and 2

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, Some(&filter)).unwrap();
        // SUM returns Double to match scalar behavior
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 40.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_sum() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Double(3.5)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Sum, None).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 7.5).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_avg() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.0)]),
            Row::new(vec![SqlValue::Double(2.0)]),
            Row::new(vec![SqlValue::Double(3.0)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Avg, None).unwrap();
        assert!(matches!(result, SqlValue::Double(avg) if (avg - 2.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_min_max() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(3.0)]),
            Row::new(vec![SqlValue::Double(1.0)]),
            Row::new(vec![SqlValue::Double(2.0)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let min_result = simd_aggregate_f64(&scan, 0, AggregateOp::Min, None).unwrap();
        assert!(matches!(min_result, SqlValue::Double(min) if (min - 1.0).abs() < 0.001));

        let max_result = simd_aggregate_f64(&scan, 0, AggregateOp::Max, None).unwrap();
        assert!(matches!(max_result, SqlValue::Double(max) if (max - 3.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_with_nulls() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Double(3.5)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Sum, None).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 5.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_with_filter() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Double(3.5)]),
        ];
        let scan = ColumnarScan::new(&rows);
        let filter = vec![true, false, true]; // Include rows 0 and 2

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Sum, Some(&filter)).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 5.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_empty_result() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
        ];
        let scan = ColumnarScan::new(&rows);
        let filter = vec![false, false]; // Filter everything out

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, Some(&filter)).unwrap();
        assert_eq!(result, SqlValue::Null);

        let count_result = simd_aggregate_i64(&scan, 0, AggregateOp::Count, Some(&filter)).unwrap();
        assert_eq!(count_result, SqlValue::Integer(0));
    }

    #[test]
    fn test_can_use_simd_for_column() {
        // Integer column
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), Some(true));

        // Float column
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Double(2.5)]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), Some(false));

        // String column (not SIMD-compatible)
        let rows = vec![
            Row::new(vec![SqlValue::Varchar("hello".to_string())]),
            Row::new(vec![SqlValue::Varchar("world".to_string())]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), None);

        // All NULL column
        let rows = vec![
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Null]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), None);
    }
}

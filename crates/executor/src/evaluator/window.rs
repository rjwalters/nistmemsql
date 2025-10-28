//! Window Function Evaluator
//!
//! This module implements the core window function evaluation engine that:
//! - Partitions rows by PARTITION BY expressions
//! - Sorts partitions by ORDER BY clauses
//! - Calculates frame boundaries (ROWS mode)
//! - Evaluates window functions over frames

use ast::{Expression, FrameBound, FrameUnit, OrderByItem, OrderDirection, WindowFrame};
use std::cmp::Ordering;
use std::ops::Range;
use storage::Row;
use types::SqlValue;

/// Simple expression evaluation for window functions
/// TODO: This is a simplified version that handles basic cases.
/// For full integration, use ExpressionEvaluator with schema context.
fn evaluate_expression(expr: &Expression, row: &Row) -> Result<SqlValue, String> {
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        Expression::ColumnRef { table: _, column } => {
            // For now, try parsing column name as index (e.g., "0", "1")
            // Or use first column if it's not a number
            if let Ok(index) = column.parse::<usize>() {
                row.get(index).cloned().ok_or_else(|| format!("Column index {} out of bounds", index))
            } else {
                // Fallback: assume first column
                row.get(0).cloned().ok_or_else(|| "Row has no columns".to_string())
            }
        }
        _ => Err("Unsupported expression in window function".to_string()),
    }
}

/// A partition of rows for window function evaluation
#[derive(Debug, Clone)]
pub struct Partition {
    pub rows: Vec<Row>,
}

impl Partition {
    pub fn new(rows: Vec<Row>) -> Self {
        Self { rows }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Partition rows by PARTITION BY expressions
///
/// Groups rows into partitions based on partition expressions.
/// If no PARTITION BY clause, all rows go into a single partition.
pub fn partition_rows(rows: Vec<Row>, partition_by: &Option<Vec<Expression>>) -> Vec<Partition> {
    // If no PARTITION BY, return all rows in single partition
    let Some(partition_exprs) = partition_by else {
        return vec![Partition::new(rows)];
    };

    if partition_exprs.is_empty() {
        return vec![Partition::new(rows)];
    }

    // Group rows by partition key values
    let mut partitions_map: std::collections::HashMap<Vec<String>, Vec<Row>> =
        std::collections::HashMap::new();

    for row in rows {
        // Evaluate partition expressions for this row
        let mut partition_key = Vec::new();

        for expr in partition_exprs {
            let value = evaluate_expression(expr, &row).unwrap_or(SqlValue::Null);
            // Convert to string for grouping (handles NULL consistently)
            partition_key.push(format!("{:?}", value));
        }

        partitions_map.entry(partition_key).or_default().push(row);
    }

    // Convert HashMap to Vec<Partition>
    partitions_map.into_values().map(Partition::new).collect()
}

/// Sort a partition by ORDER BY clauses
///
/// Sorts rows within a partition according to ORDER BY specification.
pub fn sort_partition(partition: &mut Partition, order_by: &Option<Vec<OrderByItem>>) {
    // If no ORDER BY, keep original order
    let Some(order_items) = order_by else {
        return;
    };

    if order_items.is_empty() {
        return;
    }

    // Sort rows by order expressions
    partition.rows.sort_by(|a, b| {
        for order_item in order_items {
            let val_a = evaluate_expression(&order_item.expr, a).unwrap_or(SqlValue::Null);
            let val_b = evaluate_expression(&order_item.expr, b).unwrap_or(SqlValue::Null);

            let cmp = compare_values(&val_a, &val_b);

            let cmp = match order_item.direction {
                OrderDirection::Asc => cmp,
                OrderDirection::Desc => cmp.reverse(),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });
}

/// Compare two SQL values for ordering
fn compare_values(a: &SqlValue, b: &SqlValue) -> Ordering {
    match (a, b) {
        (SqlValue::Null, SqlValue::Null) => Ordering::Equal,
        (SqlValue::Null, _) => Ordering::Less, // NULL sorts first
        (_, SqlValue::Null) => Ordering::Greater,

        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Real(a), SqlValue::Real(b)) => {
            // Handle NaN carefully
            if a.is_nan() && b.is_nan() {
                Ordering::Equal
            } else if a.is_nan() {
                Ordering::Greater
            } else if b.is_nan() {
                Ordering::Less
            } else {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
        }
        (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
        (SqlValue::Character(a), SqlValue::Character(b)) => a.cmp(b),
        (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),

        // Type coercion for mixed integer/real (Real is f32)
        (SqlValue::Integer(a), SqlValue::Real(b)) => (*a as f32).partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Real(a), SqlValue::Integer(b)) => a.partial_cmp(&(*b as f32)).unwrap_or(Ordering::Equal),

        // Other type combinations: compare as strings
        _ => format!("{:?}", a).cmp(&format!("{:?}", b)),
    }
}

/// Calculate frame boundaries for a given row in a partition
///
/// Returns a Range<usize> representing the [start, end) indices of rows in the frame.
/// Implements ROWS mode frame semantics.
pub fn calculate_frame(
    partition: &Partition,
    current_row_idx: usize,
    frame_spec: &Option<WindowFrame>,
) -> Range<usize> {
    let partition_size = partition.len();

    // Default frame: RANGE UNBOUNDED PRECEDING to CURRENT ROW
    // For ROWS mode, this is equivalent to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    let frame = match frame_spec {
        Some(f) => f,
        None => {
            // Default: start of partition to current row (inclusive)
            return 0..(current_row_idx + 1);
        }
    };

    // Only support ROWS mode for now
    if !matches!(frame.unit, FrameUnit::Rows) {
        // Fallback to default for unsupported RANGE mode
        return 0..(current_row_idx + 1);
    }

    // Calculate start boundary
    let start_idx = calculate_frame_boundary(&frame.start, current_row_idx, partition_size, true);

    // Calculate end boundary
    let end_idx = match &frame.end {
        Some(end_bound) => calculate_frame_boundary(end_bound, current_row_idx, partition_size, false),
        None => current_row_idx + 1, // Default: CURRENT ROW (inclusive, so +1 for Range)
    };

    // Ensure valid range
    let start = start_idx.min(partition_size);
    let end = end_idx.min(partition_size).max(start);

    start..end
}

/// Calculate a single frame boundary (start or end)
///
/// Returns the index for the boundary.
/// For start boundaries, returns inclusive index.
/// For end boundaries, returns exclusive index (Range semantics).
fn calculate_frame_boundary(
    bound: &FrameBound,
    current_row_idx: usize,
    partition_size: usize,
    is_start: bool,
) -> usize {
    match bound {
        FrameBound::UnboundedPreceding => 0,

        FrameBound::UnboundedFollowing => partition_size,

        FrameBound::CurrentRow => {
            if is_start {
                current_row_idx
            } else {
                current_row_idx + 1 // Exclusive end
            }
        }

        FrameBound::Preceding(offset_expr) => {
            // Evaluate offset expression (should be a constant integer)
            let offset = match offset_expr.as_ref() {
                Expression::Literal(SqlValue::Integer(n)) => *n as usize,
                _ => 0, // Fallback for non-constant (should not happen after validation)
            };

            current_row_idx.saturating_sub(offset)
        }

        FrameBound::Following(offset_expr) => {
            // Evaluate offset expression (should be a constant integer)
            let offset = match offset_expr.as_ref() {
                Expression::Literal(SqlValue::Integer(n)) => *n as usize,
                _ => 0,
            };

            let result = current_row_idx + offset;

            if is_start {
                result.min(partition_size)
            } else {
                (result + 1).min(partition_size) // Exclusive end, +1 for inclusive offset
            }
        }
    }
}

/// Evaluate ROW_NUMBER() window function
///
/// Returns unique sequential integers starting from 1 for each row in the partition.
/// Ties (based on ORDER BY) get arbitrary but consistent ordering.
///
/// Example: [1, 2, 3, 4, 5] regardless of duplicate values
pub fn evaluate_row_number(partition: &Partition) -> Vec<SqlValue> {
    // ROW_NUMBER is simple: just assign 1, 2, 3, ... to each row
    (1..=partition.len())
        .map(|n| SqlValue::Integer(n as i64))
        .collect()
}

/// Evaluate RANK() window function
///
/// Returns rank with gaps when there are ties (based on ORDER BY).
/// Rows with equal ORDER BY values get the same rank.
/// Next rank after tie skips numbers.
///
/// Example for scores [95, 90, 90, 85]: ranks are [1, 2, 2, 4]
///
/// Requires ORDER BY clause - partitions must be pre-sorted.
pub fn evaluate_rank(partition: &Partition, order_by: &Option<Vec<OrderByItem>>) -> Vec<SqlValue> {
    // RANK requires ORDER BY
    if order_by.is_none() || order_by.as_ref().unwrap().is_empty() {
        // Without ORDER BY, all rows get rank 1
        return vec![SqlValue::Integer(1); partition.len()];
    }

    let order_items = order_by.as_ref().unwrap();
    let mut ranks = Vec::new();
    let mut current_rank = 1i64;

    for (idx, row) in partition.rows.iter().enumerate() {
        if idx > 0 {
            // Compare current row with previous row
            let prev_row = &partition.rows[idx - 1];

            // Check if ORDER BY values differ
            let mut values_differ = false;
            for order_item in order_items {
                let val_curr = evaluate_expression(&order_item.expr, row).unwrap_or(SqlValue::Null);
                let val_prev = evaluate_expression(&order_item.expr, prev_row).unwrap_or(SqlValue::Null);

                if compare_values(&val_curr, &val_prev) != Ordering::Equal {
                    values_differ = true;
                    break;
                }
            }

            if values_differ {
                // New rank group - rank becomes row number (1-indexed)
                current_rank = (idx + 1) as i64;
            }
            // else: same rank as previous row
        }

        ranks.push(SqlValue::Integer(current_rank));
    }

    ranks
}

/// Evaluate DENSE_RANK() window function
///
/// Returns rank without gaps when there are ties (based on ORDER BY).
/// Rows with equal ORDER BY values get the same rank.
/// Next rank continues sequentially (no gaps).
///
/// Example for scores [95, 90, 90, 85]: ranks are [1, 2, 2, 3]
///
/// Requires ORDER BY clause - partitions must be pre-sorted.
pub fn evaluate_dense_rank(partition: &Partition, order_by: &Option<Vec<OrderByItem>>) -> Vec<SqlValue> {
    // DENSE_RANK requires ORDER BY
    if order_by.is_none() || order_by.as_ref().unwrap().is_empty() {
        // Without ORDER BY, all rows get rank 1
        return vec![SqlValue::Integer(1); partition.len()];
    }

    let order_items = order_by.as_ref().unwrap();
    let mut ranks = Vec::new();
    let mut current_rank = 1i64;

    for (idx, row) in partition.rows.iter().enumerate() {
        if idx > 0 {
            // Compare current row with previous row
            let prev_row = &partition.rows[idx - 1];

            // Check if ORDER BY values differ
            let mut values_differ = false;
            for order_item in order_items {
                let val_curr = evaluate_expression(&order_item.expr, row).unwrap_or(SqlValue::Null);
                let val_prev = evaluate_expression(&order_item.expr, prev_row).unwrap_or(SqlValue::Null);

                if compare_values(&val_curr, &val_prev) != Ordering::Equal {
                    values_differ = true;
                    break;
                }
            }

            if values_differ {
                // New rank group - increment rank by 1 (dense, no gaps)
                current_rank += 1;
            }
            // else: same rank as previous row
        }

        ranks.push(SqlValue::Integer(current_rank));
    }

    ranks
}

/// Evaluate NTILE(n) window function
///
/// Divides partition into n approximately equal groups (buckets/tiles).
/// Returns the group number (1 to n) for each row.
///
/// If rows don't divide evenly, earlier groups get one extra row.
/// Example: 10 rows with NTILE(4) → groups of [3, 3, 2, 2] rows
///
/// Example: NTILE(4) on 8 rows → [1, 1, 2, 2, 3, 3, 4, 4]
/// Example: NTILE(4) on 10 rows → [1, 1, 1, 2, 2, 2, 3, 3, 4, 4]
pub fn evaluate_ntile(partition: &Partition, n: i64) -> Result<Vec<SqlValue>, String> {
    if n <= 0 {
        return Err(format!("NTILE argument must be positive, got {}", n));
    }

    let total_rows = partition.len();
    let n_buckets = n as usize;

    // If n >= total_rows, each row gets its own bucket
    if n_buckets >= total_rows {
        return Ok((1..=total_rows).map(|i| SqlValue::Integer(i as i64)).collect());
    }

    // Calculate bucket sizes
    // Base size: total_rows / n_buckets (integer division)
    // Remainder: total_rows % n_buckets
    // First 'remainder' buckets get (base_size + 1) rows
    // Remaining buckets get base_size rows

    let base_size = total_rows / n_buckets;
    let remainder = total_rows % n_buckets;

    let mut bucket_numbers = Vec::with_capacity(total_rows);
    let mut current_bucket = 1i64;
    let mut rows_in_current_bucket = 0;

    for _ in 0..total_rows {
        bucket_numbers.push(SqlValue::Integer(current_bucket));
        rows_in_current_bucket += 1;

        // Calculate size of current bucket
        // First 'remainder' buckets get (base_size + 1) rows
        let bucket_size = if (current_bucket as usize) <= remainder {
            base_size + 1
        } else {
            base_size
        };

        // Move to next bucket if current one is full
        if rows_in_current_bucket >= bucket_size {
            current_bucket += 1;
            rows_in_current_bucket = 0;
        }
    }

    Ok(bucket_numbers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_rows(values: Vec<i64>) -> Vec<Row> {
        values
            .into_iter()
            .map(|v| Row::new(vec![SqlValue::Integer(v)]))
            .collect()
    }

    #[test]
    fn test_partition_rows_no_partition_by() {
        let rows = make_test_rows(vec![1, 2, 3]);
        let partitions = partition_rows(rows, &None);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 3);
    }

    #[test]
    fn test_partition_rows_empty_partition_by() {
        let rows = make_test_rows(vec![1, 2, 3]);
        let partitions = partition_rows(rows, &Some(vec![]));

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 3);
    }

    #[test]
    fn test_sort_partition_ascending() {
        let mut partition = Partition::new(make_test_rows(vec![3, 1, 2]));

        let order_by = vec![OrderByItem {
            expr: Expression::ColumnRef {
                table: None,
                column: String::new(), // Will use first column
            },
            direction: OrderDirection::Asc,
        }];

        sort_partition(&mut partition, &Some(order_by));

        // Should be sorted ascending: 1, 2, 3
        assert_eq!(partition.rows[0].values[0], SqlValue::Integer(1));
        assert_eq!(partition.rows[1].values[0], SqlValue::Integer(2));
        assert_eq!(partition.rows[2].values[0], SqlValue::Integer(3));
    }

    #[test]
    fn test_calculate_frame_default() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        // Default frame: UNBOUNDED PRECEDING to CURRENT ROW
        let frame = calculate_frame(&partition, 2, &None);

        assert_eq!(frame, 0..3); // Rows 0, 1, 2 (current row is 2)
    }

    #[test]
    fn test_calculate_frame_unbounded_preceding() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        let frame_spec = WindowFrame {
            unit: FrameUnit::Rows,
            start: FrameBound::UnboundedPreceding,
            end: Some(FrameBound::CurrentRow),
        };

        let frame = calculate_frame(&partition, 2, &Some(frame_spec));

        assert_eq!(frame, 0..3); // Rows 0, 1, 2
    }

    #[test]
    fn test_calculate_frame_preceding() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        let frame_spec = WindowFrame {
            unit: FrameUnit::Rows,
            start: FrameBound::Preceding(Box::new(Expression::Literal(SqlValue::Integer(2)))),
            end: Some(FrameBound::CurrentRow),
        };

        let frame = calculate_frame(&partition, 3, &Some(frame_spec));

        // 2 PRECEDING from row 3 is row 1, so rows 1, 2, 3
        assert_eq!(frame, 1..4);
    }

    #[test]
    fn test_calculate_frame_following() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        let frame_spec = WindowFrame {
            unit: FrameUnit::Rows,
            start: FrameBound::CurrentRow,
            end: Some(FrameBound::Following(Box::new(Expression::Literal(SqlValue::Integer(2))))),
        };

        let frame = calculate_frame(&partition, 1, &Some(frame_spec));

        // Current row 1 to 2 FOLLOWING (row 3), so rows 1, 2, 3
        assert_eq!(frame, 1..4);
    }

    #[test]
    fn test_calculate_frame_unbounded_following() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        let frame_spec = WindowFrame {
            unit: FrameUnit::Rows,
            start: FrameBound::CurrentRow,
            end: Some(FrameBound::UnboundedFollowing),
        };

        let frame = calculate_frame(&partition, 2, &Some(frame_spec));

        // Current row 2 to end: rows 2, 3, 4
        assert_eq!(frame, 2..5);
    }

    // ===== Ranking Function Tests =====

    #[test]
    fn test_row_number_simple() {
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let result = evaluate_row_number(&partition);

        assert_eq!(result.len(), 5);
        assert_eq!(result[0], SqlValue::Integer(1));
        assert_eq!(result[1], SqlValue::Integer(2));
        assert_eq!(result[2], SqlValue::Integer(3));
        assert_eq!(result[3], SqlValue::Integer(4));
        assert_eq!(result[4], SqlValue::Integer(5));
    }

    #[test]
    fn test_rank_with_ties() {
        // Scores: 95, 90, 90, 85
        // Expected ranks: 1, 2, 2, 4
        let partition = Partition::new(make_test_rows(vec![95, 90, 90, 85]));

        let order_by = Some(vec![OrderByItem {
            expr: Expression::ColumnRef {
                table: None,
                column: String::new(), // Will use first column
            },
            direction: OrderDirection::Desc,
        }]);

        let result = evaluate_rank(&partition, &order_by);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], SqlValue::Integer(1)); // 95 -> rank 1
        assert_eq!(result[1], SqlValue::Integer(2)); // 90 -> rank 2
        assert_eq!(result[2], SqlValue::Integer(2)); // 90 -> rank 2 (tie)
        assert_eq!(result[3], SqlValue::Integer(4)); // 85 -> rank 4 (gap at 3)
    }

    #[test]
    fn test_rank_no_ties() {
        let partition = Partition::new(make_test_rows(vec![4, 3, 2, 1]));

        let order_by = Some(vec![OrderByItem {
            expr: Expression::ColumnRef {
                table: None,
                column: String::new(),
            },
            direction: OrderDirection::Desc,
        }]);

        let result = evaluate_rank(&partition, &order_by);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], SqlValue::Integer(1));
        assert_eq!(result[1], SqlValue::Integer(2));
        assert_eq!(result[2], SqlValue::Integer(3));
        assert_eq!(result[3], SqlValue::Integer(4));
    }

    #[test]
    fn test_rank_without_order_by() {
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let result = evaluate_rank(&partition, &None);

        // Without ORDER BY, all rows get rank 1
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], SqlValue::Integer(1));
        assert_eq!(result[1], SqlValue::Integer(1));
        assert_eq!(result[2], SqlValue::Integer(1));
    }

    #[test]
    fn test_dense_rank_with_ties() {
        // Scores: 95, 90, 90, 85
        // Expected ranks: 1, 2, 2, 3 (no gap)
        let partition = Partition::new(make_test_rows(vec![95, 90, 90, 85]));

        let order_by = Some(vec![OrderByItem {
            expr: Expression::ColumnRef {
                table: None,
                column: String::new(),
            },
            direction: OrderDirection::Desc,
        }]);

        let result = evaluate_dense_rank(&partition, &order_by);

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], SqlValue::Integer(1)); // 95 -> rank 1
        assert_eq!(result[1], SqlValue::Integer(2)); // 90 -> rank 2
        assert_eq!(result[2], SqlValue::Integer(2)); // 90 -> rank 2 (tie)
        assert_eq!(result[3], SqlValue::Integer(3)); // 85 -> rank 3 (no gap)
    }

    #[test]
    fn test_dense_rank_multiple_tie_groups() {
        // Scores: 100, 90, 90, 80, 80, 80, 70
        // Expected ranks: 1, 2, 2, 3, 3, 3, 4
        let partition = Partition::new(make_test_rows(vec![100, 90, 90, 80, 80, 80, 70]));

        let order_by = Some(vec![OrderByItem {
            expr: Expression::ColumnRef {
                table: None,
                column: String::new(),
            },
            direction: OrderDirection::Desc,
        }]);

        let result = evaluate_dense_rank(&partition, &order_by);

        assert_eq!(result.len(), 7);
        assert_eq!(result[0], SqlValue::Integer(1));
        assert_eq!(result[1], SqlValue::Integer(2));
        assert_eq!(result[2], SqlValue::Integer(2));
        assert_eq!(result[3], SqlValue::Integer(3));
        assert_eq!(result[4], SqlValue::Integer(3));
        assert_eq!(result[5], SqlValue::Integer(3));
        assert_eq!(result[6], SqlValue::Integer(4));
    }

    #[test]
    fn test_ntile_even_division() {
        // 8 rows, NTILE(4) -> 2 rows per bucket
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5, 6, 7, 8]));

        let result = evaluate_ntile(&partition, 4).unwrap();

        assert_eq!(result.len(), 8);
        assert_eq!(result[0], SqlValue::Integer(1));
        assert_eq!(result[1], SqlValue::Integer(1));
        assert_eq!(result[2], SqlValue::Integer(2));
        assert_eq!(result[3], SqlValue::Integer(2));
        assert_eq!(result[4], SqlValue::Integer(3));
        assert_eq!(result[5], SqlValue::Integer(3));
        assert_eq!(result[6], SqlValue::Integer(4));
        assert_eq!(result[7], SqlValue::Integer(4));
    }

    #[test]
    fn test_ntile_uneven_division() {
        // 10 rows, NTILE(4) -> buckets of 3, 3, 2, 2
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));

        let result = evaluate_ntile(&partition, 4).unwrap();

        assert_eq!(result.len(), 10);
        // First bucket: 3 rows (base_size=2 + 1)
        assert_eq!(result[0], SqlValue::Integer(1));
        assert_eq!(result[1], SqlValue::Integer(1));
        assert_eq!(result[2], SqlValue::Integer(1));
        // Second bucket: 3 rows
        assert_eq!(result[3], SqlValue::Integer(2));
        assert_eq!(result[4], SqlValue::Integer(2));
        assert_eq!(result[5], SqlValue::Integer(2));
        // Third bucket: 2 rows (base_size)
        assert_eq!(result[6], SqlValue::Integer(3));
        assert_eq!(result[7], SqlValue::Integer(3));
        // Fourth bucket: 2 rows
        assert_eq!(result[8], SqlValue::Integer(4));
        assert_eq!(result[9], SqlValue::Integer(4));
    }

    #[test]
    fn test_ntile_n_greater_than_rows() {
        // 3 rows, NTILE(5) -> each row gets own bucket
        let partition = Partition::new(make_test_rows(vec![1, 2, 3]));

        let result = evaluate_ntile(&partition, 5).unwrap();

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], SqlValue::Integer(1));
        assert_eq!(result[1], SqlValue::Integer(2));
        assert_eq!(result[2], SqlValue::Integer(3));
    }

    #[test]
    fn test_ntile_single_bucket() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        let result = evaluate_ntile(&partition, 1).unwrap();

        assert_eq!(result.len(), 5);
        // All rows in bucket 1
        for val in result {
            assert_eq!(val, SqlValue::Integer(1));
        }
    }

    #[test]
    fn test_ntile_invalid_argument() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3]));

        let result = evaluate_ntile(&partition, 0);
        assert!(result.is_err());

        let result = evaluate_ntile(&partition, -1);
        assert!(result.is_err());
    }
}

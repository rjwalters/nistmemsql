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
}

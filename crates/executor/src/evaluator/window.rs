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
pub fn partition_rows<F>(
    rows: Vec<Row>,
    partition_by: &Option<Vec<Expression>>,
    eval_fn: F,
) -> Vec<Partition>
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    // If no PARTITION BY, return all rows in single partition
    let Some(partition_exprs) = partition_by else {
        return vec![Partition::new(rows)];
    };

    if partition_exprs.is_empty() {
        return vec![Partition::new(rows)];
    }

    // Group rows by partition key values (use BTreeMap for deterministic ordering)
    let mut partitions_map: std::collections::BTreeMap<Vec<String>, Vec<Row>> =
        std::collections::BTreeMap::new();

    for row in rows {
        // Evaluate partition expressions for this row
        let mut partition_key = Vec::new();

        for expr in partition_exprs {
            let value = eval_fn(expr, &row).unwrap_or(SqlValue::Null);
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
    order_by: &Option<Vec<ast::OrderByItem>>,
    frame_spec: &Option<WindowFrame>,
) -> Range<usize> {
    let partition_size = partition.len();

    // Default frame depends on whether there's an ORDER BY:
    // - Without ORDER BY: entire partition (all rows)
    // - With ORDER BY: RANGE UNBOUNDED PRECEDING to CURRENT ROW
    let frame = match frame_spec {
        Some(f) => f,
        None => {
            // Check if there's an ORDER BY clause
            let has_order_by = order_by.as_ref().map_or(false, |items| !items.is_empty());

            if has_order_by {
                // Default with ORDER BY: start of partition to current row (inclusive)
                let result = 0..(current_row_idx + 1);
                return result;
            } else {
                // Default without ORDER BY: entire partition
                let result = 0..partition_size;
                return result;
            }
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


/// Evaluate COUNT aggregate window function over a frame
///
/// Counts rows in the frame. Two variants:
/// - COUNT(*): counts all rows in frame
/// - COUNT(expr): counts rows where expr is not NULL
///
/// Example: COUNT(*) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
pub fn evaluate_count_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: Option<&Expression>,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut count = 0i64;

    eprintln!("DEBUG COUNT: frame={:?}, partition.len()={}, arg_expr.is_none()={}",
              frame, partition.len(), arg_expr.is_none());

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        // COUNT(*) - count all rows
        if arg_expr.is_none() {
            count += 1;
            continue;
        }

        // COUNT(expr) - count non-NULL values
        if let Some(expr) = arg_expr {
            if let Ok(val) = eval_fn(expr, row) {
                if !matches!(val, SqlValue::Null) {
                    count += 1;
                }
            }
        }
    }


    SqlValue::Integer(count)
}

/// Evaluate SUM aggregate window function over a frame
///
/// Sums integer values in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
///
/// Example: SUM(amount) OVER (ORDER BY date) for running totals
pub fn evaluate_sum_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut sum = 0i64;
    let mut has_value = false;


    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        if let Ok(val) = eval_fn(arg_expr, row) {
            match val {
                SqlValue::Integer(n) => {
                    sum += n;
                    has_value = true;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}             // Ignore non-integer values
            }
        }
    }


    if has_value {
        SqlValue::Integer(sum)
    } else {
        SqlValue::Null
    }
}

/// Evaluate AVG aggregate window function over a frame
///
/// Computes average of integer values in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
///
/// Example: AVG(temperature) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
/// for 7-day moving average
pub fn evaluate_avg_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut sum = 0i64;
    let mut count = 0i64;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        if let Ok(val) = eval_fn(arg_expr, row) {
            match val {
                SqlValue::Integer(n) => {
                    sum += n;
                    count += 1;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}             // Ignore non-integer values
            }
        }
    }

    if count > 0 {
        SqlValue::Integer(sum / count)
    } else {
        SqlValue::Null
    }
}

/// Evaluate MIN aggregate window function over a frame
///
/// Finds minimum value in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
///
/// Example: MIN(salary) OVER (PARTITION BY department)
pub fn evaluate_min_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut min_val: Option<SqlValue> = None;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        if let Ok(val) = eval_fn(arg_expr, row) {
            if matches!(val, SqlValue::Null) {
                continue; // Ignore NULL
            }

            if let Some(ref current_min) = min_val {
                if compare_values(&val, current_min) == Ordering::Less {
                    min_val = Some(val);
                }
            } else {
                min_val = Some(val);
            }
        }
    }

    min_val.unwrap_or(SqlValue::Null)
}

/// Evaluate MAX aggregate window function over a frame
///
/// Finds maximum value in the frame, ignoring NULLs.
/// Returns NULL if all values are NULL or frame is empty.
///
/// Example: MAX(salary) OVER (PARTITION BY department)
pub fn evaluate_max_window<F>(
    partition: &Partition,
    frame: &Range<usize>,
    arg_expr: &Expression,
    eval_fn: F,
) -> SqlValue
where
    F: Fn(&Expression, &Row) -> Result<SqlValue, String>,
{
    let mut max_val: Option<SqlValue> = None;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        if let Ok(val) = eval_fn(arg_expr, row) {
            if matches!(val, SqlValue::Null) {
                continue; // Ignore NULL
            }

            if let Some(ref current_max) = max_val {
                if compare_values(&val, current_max) == Ordering::Greater {
                    max_val = Some(val);
                }
            } else {
                max_val = Some(val);
            }
        }
    }

    max_val.unwrap_or(SqlValue::Null)
}

/// Evaluate LAG() value window function
///
/// Accesses a value from a previous row in the partition (offset rows back).
/// Returns the value from (current_row_idx - offset).
/// If offset goes before partition start, returns default value (or NULL).
///
/// Signature: LAG(expr [, offset [, default]])
/// - expr: Expression to evaluate on the offset row
/// - offset: Number of rows back (default: 1)
/// - default: Value to return when offset is out of bounds (default: NULL)
///
/// Example: LAG(price, 1) OVER (ORDER BY date)
/// Example: LAG(revenue, 1, 0) OVER (PARTITION BY region ORDER BY month)
///
/// Requires: ORDER BY in window spec (partition must be sorted)
pub fn evaluate_lag(
    partition: &Partition,
    current_row_idx: usize,
    value_expr: &Expression,
    offset: Option<i64>,
    default: Option<&Expression>,
) -> Result<SqlValue, String> {
    let offset_val = offset.unwrap_or(1);

    // Validate offset is non-negative
    if offset_val < 0 {
        return Err(format!("LAG offset must be non-negative, got {}", offset_val));
    }

    // Calculate target row index (current_row_idx - offset)
    let target_idx = if offset_val as usize > current_row_idx {
        // Offset goes before partition start - return default
        return Ok(evaluate_default_value(default)?);
    } else {
        current_row_idx - offset_val as usize
    };

    // Get value from target row
    if let Some(target_row) = partition.rows.get(target_idx) {
        evaluate_expression(value_expr, target_row).map_err(|e| e.to_string())
    } else {
        // Should not happen if bounds check is correct
        Ok(evaluate_default_value(default)?)
    }
}

/// Evaluate LEAD() value window function
///
/// Accesses a value from a subsequent row in the partition (offset rows forward).
/// Returns the value from (current_row_idx + offset).
/// If offset goes past partition end, returns default value (or NULL).
///
/// Signature: LEAD(expr [, offset [, default]])
/// - expr: Expression to evaluate on the offset row
/// - offset: Number of rows forward (default: 1)
/// - default: Value to return when offset is out of bounds (default: NULL)
///
/// Example: LEAD(price, 1) OVER (ORDER BY date)
/// Example: LEAD(sales, 3, 0) OVER (PARTITION BY product ORDER BY quarter)
///
/// Requires: ORDER BY in window spec (partition must be sorted)
pub fn evaluate_lead(
    partition: &Partition,
    current_row_idx: usize,
    value_expr: &Expression,
    offset: Option<i64>,
    default: Option<&Expression>,
) -> Result<SqlValue, String> {
    let offset_val = offset.unwrap_or(1);

    // Validate offset is non-negative
    if offset_val < 0 {
        return Err(format!("LEAD offset must be non-negative, got {}", offset_val));
    }

    // Calculate target row index (current_row_idx + offset)
    let target_idx = current_row_idx + offset_val as usize;

    // Check if target is beyond partition end
    if target_idx >= partition.len() {
        return Ok(evaluate_default_value(default)?);
    }

    // Get value from target row
    if let Some(target_row) = partition.rows.get(target_idx) {
        evaluate_expression(value_expr, target_row).map_err(|e| e.to_string())
    } else {
        // Should not happen if bounds check is correct
        Ok(evaluate_default_value(default)?)
    }
}

/// Helper function to evaluate default value for LAG/LEAD
///
/// If default expression is provided, evaluate it.
/// Otherwise, return NULL.
fn evaluate_default_value(default: Option<&Expression>) -> Result<SqlValue, String> {
    match default {
        Some(expr) => match expr {
            Expression::Literal(val) => Ok(val.clone()),
            _ => Err("Default value must be a literal".to_string()),
        },
        None => Ok(SqlValue::Null),
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
        let partitions = partition_rows(rows, &None, evaluate_expression);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 3);
    }

    #[test]
    fn test_partition_rows_empty_partition_by() {
        let rows = make_test_rows(vec![1, 2, 3]);
        let partitions = partition_rows(rows, &Some(vec![]), evaluate_expression);

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].len(), 3);
    }

    #[test]
    fn test_calculate_frame_default() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        // Default frame WITHOUT ORDER BY: entire partition
        let frame = calculate_frame(&partition, 2, &None, &None);

        assert_eq!(frame, 0..5); // Entire partition (no ORDER BY)
    }

    #[test]
    fn test_calculate_frame_unbounded_preceding() {
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        let frame_spec = WindowFrame {
            unit: FrameUnit::Rows,
            start: FrameBound::UnboundedPreceding,
            end: Some(FrameBound::CurrentRow),
        };

        let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec));

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

        let frame = calculate_frame(&partition, 3, &None, &Some(frame_spec));

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

        let frame = calculate_frame(&partition, 1, &None, &Some(frame_spec));

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

        let frame = calculate_frame(&partition, 2, &None, &Some(frame_spec));

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


    #[test]
    fn test_count_star_window() {
        // COUNT(*) over entire partition
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));
        let frame = 0..5; // All rows

        let result = evaluate_count_window(&partition, &frame, None, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(5));
    }

    #[test]
    fn test_count_window_with_frame() {
        // COUNT(*) over 3-row moving window
        let partition = Partition::new(make_test_rows(vec![1, 2, 3, 4, 5]));

        // Frame: rows 1, 2, 3 (3 rows)
        let frame = 1..4;
        let result = evaluate_count_window(&partition, &frame, None, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn test_count_expr_window() {
        // COUNT(expr) - counts non-NULL values
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));
        let frame = 0..3;

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let result = evaluate_count_window(&partition, &frame, Some(&expr), evaluate_expression);

        // All 3 values are non-NULL
        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn test_sum_window_running_total() {
        // SUM for running total
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Running total at position 2: sum of rows 0, 1, 2
        let frame = 0..3; // 10 + 20 + 30 = 60
        let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(60));
    }

    #[test]
    fn test_sum_window_moving() {
        // SUM over 3-row moving window
        let partition = Partition::new(make_test_rows(vec![5, 10, 15, 20, 25]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Frame: rows 2, 3, 4 (values 15, 20, 25)
        let frame = 2..5;
        let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(60)); // 15 + 20 + 25
    }

    #[test]
    fn test_sum_window_empty_frame() {
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Empty frame
        let frame = 0..0;
        let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_avg_window_simple() {
        // AVG over entire partition
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let frame = 0..5;
        let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

        // Average: (10 + 20 + 30 + 40 + 50) / 5 = 30
        assert_eq!(result, SqlValue::Integer(30));
    }

    #[test]
    fn test_avg_window_moving() {
        // 3-row moving average
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Frame: rows 1, 2, 3 (values 20, 30, 40)
        let frame = 1..4;
        let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

        // Average: (20 + 30 + 40) / 3 = 30
        assert_eq!(result, SqlValue::Integer(30));
    }

    #[test]
    fn test_avg_window_empty_frame() {
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Empty frame
        let frame = 0..0;
        let result = evaluate_avg_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_min_window_partition() {
        // MIN over entire partition
        let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let frame = 0..5;
        let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(10));
    }

    #[test]
    fn test_min_window_moving() {
        // MIN in 3-row window
        let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Frame: rows 1, 2, 3 (values 20, 80, 10)
        let frame = 1..4;
        let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(10));
    }

    #[test]
    fn test_min_window_empty_frame() {
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Empty frame
        let frame = 0..0;
        let result = evaluate_min_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_max_window_partition() {
        // MAX over entire partition
        let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let frame = 0..5;
        let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(80));
    }

    #[test]
    fn test_max_window_moving() {
        // MAX in 3-row window
        let partition = Partition::new(make_test_rows(vec![50, 20, 80, 10, 40]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Frame: rows 2, 3, 4 (values 80, 10, 40)
        let frame = 2..5;
        let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Integer(80));
    }

    #[test]
    fn test_max_window_empty_frame() {
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Empty frame
        let frame = 0..0;
        let result = evaluate_max_window(&partition, &frame, &expr, evaluate_expression);

        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_frame_at_partition_boundaries() {
        // Test frame behavior at partition start and end
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Frame extends beyond partition end
        let frame = 3..10; // Should clamp to 3..5
        let result = evaluate_sum_window(&partition, &frame, &expr, evaluate_expression);

        // Should only sum rows 3, 4 (values 40, 50)
        assert_eq!(result, SqlValue::Integer(90));
    }

    // ===== LAG/LEAD Value Function Tests =====

    #[test]
    fn test_lag_default_offset() {
        // LAG(value) with default offset of 1
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Row 0: LAG should return NULL (no previous row)
        let result = evaluate_lag(&partition, 0, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Null);

        // Row 1: LAG should return 10 (previous row value)
        let result = evaluate_lag(&partition, 1, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Integer(10));

        // Row 2: LAG should return 20
        let result = evaluate_lag(&partition, 2, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Integer(20));

        // Row 4: LAG should return 40
        let result = evaluate_lag(&partition, 4, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Integer(40));
    }

    #[test]
    fn test_lag_custom_offset() {
        // LAG(value, 2) - look back 2 rows
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Row 0: offset 2 goes before partition start -> NULL
        let result = evaluate_lag(&partition, 0, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Null);

        // Row 1: offset 2 goes before partition start -> NULL
        let result = evaluate_lag(&partition, 1, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Null);

        // Row 2: LAG(value, 2) should return 10 (row 0)
        let result = evaluate_lag(&partition, 2, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Integer(10));

        // Row 3: LAG(value, 2) should return 20 (row 1)
        let result = evaluate_lag(&partition, 3, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Integer(20));

        // Row 4: LAG(value, 2) should return 30 (row 2)
        let result = evaluate_lag(&partition, 4, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Integer(30));
    }

    #[test]
    fn test_lag_with_default_value() {
        // LAG(value, 1, 0) - default value of 0 instead of NULL
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let default_expr = Expression::Literal(SqlValue::Integer(0));

        // Row 0: should return 0 (default) instead of NULL
        let result = evaluate_lag(&partition, 0, &value_expr, None, Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(0));

        // Row 1: should return 10 (previous row)
        let result = evaluate_lag(&partition, 1, &value_expr, None, Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(10));

        // Row 2: should return 20 (previous row)
        let result = evaluate_lag(&partition, 2, &value_expr, None, Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(20));
    }

    #[test]
    fn test_lag_offset_beyond_partition_start() {
        // Large offset that goes way before partition start
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Row 2 with offset 100 should return NULL
        let result = evaluate_lag(&partition, 2, &value_expr, Some(100), None).unwrap();
        assert_eq!(result, SqlValue::Null);

        // With default value
        let default_expr = Expression::Literal(SqlValue::Integer(-1));
        let result = evaluate_lag(&partition, 2, &value_expr, Some(100), Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(-1));
    }

    #[test]
    fn test_lead_default_offset() {
        // LEAD(value) with default offset of 1
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Row 0: LEAD should return 20 (next row value)
        let result = evaluate_lead(&partition, 0, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Integer(20));

        // Row 1: LEAD should return 30
        let result = evaluate_lead(&partition, 1, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Integer(30));

        // Row 3: LEAD should return 50
        let result = evaluate_lead(&partition, 3, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Integer(50));

        // Row 4: LEAD should return NULL (no next row)
        let result = evaluate_lead(&partition, 4, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_lead_custom_offset() {
        // LEAD(value, 2) - look forward 2 rows
        let partition = Partition::new(make_test_rows(vec![10, 20, 30, 40, 50]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Row 0: LEAD(value, 2) should return 30 (row 2)
        let result = evaluate_lead(&partition, 0, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Integer(30));

        // Row 1: LEAD(value, 2) should return 40 (row 3)
        let result = evaluate_lead(&partition, 1, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Integer(40));

        // Row 2: LEAD(value, 2) should return 50 (row 4)
        let result = evaluate_lead(&partition, 2, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Integer(50));

        // Row 3: offset 2 goes past partition end -> NULL
        let result = evaluate_lead(&partition, 3, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Null);

        // Row 4: offset 2 goes past partition end -> NULL
        let result = evaluate_lead(&partition, 4, &value_expr, Some(2), None).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_lead_with_default_value() {
        // LEAD(value, 1, 999) - default value of 999 instead of NULL
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let default_expr = Expression::Literal(SqlValue::Integer(999));

        // Row 0: should return 20 (next row)
        let result = evaluate_lead(&partition, 0, &value_expr, None, Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(20));

        // Row 1: should return 30 (next row)
        let result = evaluate_lead(&partition, 1, &value_expr, None, Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(30));

        // Row 2: should return 999 (default) instead of NULL
        let result = evaluate_lead(&partition, 2, &value_expr, None, Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(999));
    }

    #[test]
    fn test_lead_offset_beyond_partition_end() {
        // Large offset that goes way past partition end
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // Row 0 with offset 100 should return NULL
        let result = evaluate_lead(&partition, 0, &value_expr, Some(100), None).unwrap();
        assert_eq!(result, SqlValue::Null);

        // With default value
        let default_expr = Expression::Literal(SqlValue::Integer(-1));
        let result = evaluate_lead(&partition, 0, &value_expr, Some(100), Some(&default_expr)).unwrap();
        assert_eq!(result, SqlValue::Integer(-1));
    }

    #[test]
    fn test_lag_lead_single_row_partition() {
        // Edge case: partition with only one row
        let partition = Partition::new(make_test_rows(vec![42]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // LAG on single row should return NULL
        let result = evaluate_lag(&partition, 0, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Null);

        // LEAD on single row should return NULL
        let result = evaluate_lead(&partition, 0, &value_expr, None, None).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_lag_lead_with_zero_offset() {
        // Special case: offset of 0 should return current row value
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        // LAG(value, 0) should return current row value
        let result = evaluate_lag(&partition, 1, &value_expr, Some(0), None).unwrap();
        assert_eq!(result, SqlValue::Integer(20));

        // LEAD(value, 0) should return current row value
        let result = evaluate_lead(&partition, 1, &value_expr, Some(0), None).unwrap();
        assert_eq!(result, SqlValue::Integer(20));
    }

    #[test]
    fn test_lag_negative_offset_error() {
        // LAG with negative offset should return error
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let result = evaluate_lag(&partition, 1, &value_expr, Some(-1), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("non-negative"));
    }

    #[test]
    fn test_lead_negative_offset_error() {
        // LEAD with negative offset should return error
        let partition = Partition::new(make_test_rows(vec![10, 20, 30]));

        let value_expr = Expression::ColumnRef {
            table: None,
            column: "0".to_string(),
        };

        let result = evaluate_lead(&partition, 1, &value_expr, Some(-1), None);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("non-negative"));
    }
}

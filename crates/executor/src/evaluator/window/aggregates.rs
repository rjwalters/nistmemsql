//! Aggregate window functions
//!
//! Implements COUNT, SUM, AVG, MIN, MAX with frame support.

use ast::Expression;
use std::cmp::Ordering;
use std::ops::Range;
use storage::Row;
use types::SqlValue;

use super::partitioning::Partition;
use super::sorting::compare_values;

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

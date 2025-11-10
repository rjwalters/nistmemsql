//! Aggregate window functions
//!
//! Implements COUNT, SUM, AVG, MIN, MAX with frame support.

use std::{cmp::Ordering, ops::Range};

use vibesql_ast::Expression;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{partitioning::Partition, sorting::compare_values};

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

    SqlValue::Numeric(count as f64)
}

/// Evaluate SUM aggregate window function over a frame
///
/// Sums numeric values in the frame, ignoring NULLs.
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
    let mut sum = 0.0f64;
    let mut has_value = false;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        if let Ok(val) = eval_fn(arg_expr, row) {
            match val {
                SqlValue::Integer(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Smallint(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Bigint(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Numeric(n) => {
                    sum += n;
                    has_value = true;
                }
                SqlValue::Float(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Real(n) => {
                    sum += n as f64;
                    has_value = true;
                }
                SqlValue::Double(n) => {
                    sum += n;
                    has_value = true;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}              // Ignore non-numeric values
            }
        }
    }

    if has_value {
        SqlValue::Numeric(sum)
    } else {
        SqlValue::Null
    }
}

/// Evaluate AVG aggregate window function over a frame
///
/// Computes average of numeric values in the frame, ignoring NULLs.
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
    let mut sum = 0.0f64;
    let mut count = 0i64;

    for idx in frame.clone() {
        if idx >= partition.len() {
            break;
        }

        let row = &partition.rows[idx];

        if let Ok(val) = eval_fn(arg_expr, row) {
            match val {
                SqlValue::Integer(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Smallint(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Bigint(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Numeric(n) => {
                    sum += n;
                    count += 1;
                }
                SqlValue::Float(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Real(n) => {
                    sum += n as f64;
                    count += 1;
                }
                SqlValue::Double(n) => {
                    sum += n;
                    count += 1;
                }
                SqlValue::Null => {} // Ignore NULL
                _ => {}              // Ignore non-numeric values
            }
        }
    }

    if count > 0 {
        SqlValue::Numeric(sum / count as f64)
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

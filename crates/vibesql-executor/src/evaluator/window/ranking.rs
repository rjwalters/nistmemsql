//! Ranking window functions
//!
//! Implements ROW_NUMBER, RANK, DENSE_RANK, and NTILE.

use std::cmp::Ordering;

use vibesql_ast::OrderByItem;
use vibesql_types::SqlValue;

use super::{partitioning::Partition, sorting::compare_values, utils::evaluate_expression};

/// Evaluate ROW_NUMBER() window function
///
/// Returns unique sequential integers starting from 1 for each row in the partition.
/// Ties (based on ORDER BY) get arbitrary but consistent ordering.
///
/// Example: [1, 2, 3, 4, 5] regardless of duplicate values
pub fn evaluate_row_number(partition: &Partition) -> Vec<SqlValue> {
    // ROW_NUMBER is simple: just assign 1, 2, 3, ... to each row
    (1..=partition.len()).map(|n| SqlValue::Integer(n as i64)).collect()
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
                let val_prev =
                    evaluate_expression(&order_item.expr, prev_row).unwrap_or(SqlValue::Null);

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
pub fn evaluate_dense_rank(
    partition: &Partition,
    order_by: &Option<Vec<OrderByItem>>,
) -> Vec<SqlValue> {
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
                let val_prev =
                    evaluate_expression(&order_item.expr, prev_row).unwrap_or(SqlValue::Null);

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
        let bucket_size =
            if (current_bucket as usize) <= remainder { base_size + 1 } else { base_size };

        // Move to next bucket if current one is full
        if rows_in_current_bucket >= bucket_size {
            current_bucket += 1;
            rows_in_current_bucket = 0;
        }
    }

    Ok(bucket_numbers)
}

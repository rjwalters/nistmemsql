//! Partition sorting for window functions
//!
//! Sorts rows within partitions according to ORDER BY specifications.

use ast::{OrderByItem, OrderDirection};
use std::cmp::Ordering;
use types::SqlValue;

use super::partitioning::Partition;
use super::utils::evaluate_expression;

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
pub fn compare_values(a: &SqlValue, b: &SqlValue) -> Ordering {
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

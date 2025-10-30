//! ORDER BY sorting logic

use std::cmp::Ordering;

use crate::errors::ExecutorError;
use crate::evaluator::CombinedExpressionEvaluator;

use super::grouping::compare_sql_values;

/// Row with optional sort keys for ORDER BY
pub(super) type RowWithSortKeys = (storage::Row, Option<Vec<(types::SqlValue, ast::OrderDirection)>>);

/// Apply ORDER BY sorting to rows
///
/// Evaluates ORDER BY expressions for each row and sorts them according to the specified
/// directions (ASC/DESC). Supports multi-column sorting with stable sort behavior.
pub(super) fn apply_order_by(
    mut rows: Vec<RowWithSortKeys>,
    order_by: &[ast::OrderByItem],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<RowWithSortKeys>, ExecutorError> {
    // Evaluate ORDER BY expressions for each row
    for (row, sort_keys) in &mut rows {
        let mut keys = Vec::new();
        for order_item in order_by {
            // Evaluator handles window functions via window_mapping if present
            let key_value = evaluator.eval(&order_item.expr, row)?;
            keys.push((key_value, order_item.direction.clone()));
        }
        *sort_keys = Some(keys);
    }

    // Sort by the evaluated keys
    rows.sort_by(|(_, keys_a), (_, keys_b)| {
        let keys_a = keys_a.as_ref().unwrap();
        let keys_b = keys_b.as_ref().unwrap();

        for ((val_a, dir), (val_b, _)) in keys_a.iter().zip(keys_b.iter()) {
            let cmp = match dir {
                ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });

    Ok(rows)
}

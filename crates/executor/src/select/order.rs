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
///
/// ORDER BY can reference:
/// - Columns from the FROM clause
/// - Aliases from the SELECT list
/// - Arbitrary expressions
pub(super) fn apply_order_by(
    mut rows: Vec<RowWithSortKeys>,
    order_by: &[ast::OrderByItem],
    evaluator: &CombinedExpressionEvaluator,
    select_list: &[ast::SelectItem],
) -> Result<Vec<RowWithSortKeys>, ExecutorError> {
    // Evaluate ORDER BY expressions for each row
    for (row, sort_keys) in &mut rows {
        let mut keys = Vec::new();
        for order_item in order_by {
            // Check if ORDER BY expression is a SELECT list alias
            let expr_to_eval = resolve_order_by_alias(&order_item.expr, select_list);
            let key_value = evaluator.eval(expr_to_eval, row)?;
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

/// Resolve ORDER BY expression that might be a SELECT list alias
///
/// If the ORDER BY expression is a simple column reference (without table qualifier)
/// and it matches a SELECT list alias, return the SELECT list expression.
/// Otherwise, return the original ORDER BY expression.
fn resolve_order_by_alias<'a>(
    order_expr: &'a ast::Expression,
    select_list: &'a [ast::SelectItem],
) -> &'a ast::Expression {
    // Check if ORDER BY expression is a simple column reference (no table qualifier)
    if let ast::Expression::ColumnRef { table: None, column } = order_expr {
        // Search for matching alias in SELECT list
        for item in select_list {
            if let ast::SelectItem::Expression { expr, alias: Some(alias_name) } = item {
                if alias_name == column {
                    // Found matching alias, use the SELECT list expression
                    return expr;
                }
            }
        }
    }

    // Not an alias or no match found, use the original expression
    order_expr
}

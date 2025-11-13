//! ORDER BY sorting logic

use std::cmp::Ordering;

use super::grouping::compare_sql_values;
use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

/// Row with optional sort keys for ORDER BY
pub(super) type RowWithSortKeys =
    (vibesql_storage::Row, Option<Vec<(vibesql_types::SqlValue, vibesql_ast::OrderDirection)>>);

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
    order_by: &[vibesql_ast::OrderByItem],
    evaluator: &CombinedExpressionEvaluator,
    select_list: &[vibesql_ast::SelectItem],
) -> Result<Vec<RowWithSortKeys>, ExecutorError> {
    // Evaluate ORDER BY expressions for each row
    for (row, sort_keys) in &mut rows {
        // Clear CSE cache before evaluating this row's ORDER BY expressions
        // to prevent stale cached column values from previous rows
        evaluator.clear_cse_cache();

        let mut keys = Vec::new();
        for order_item in order_by {
            // Check if ORDER BY expression is a SELECT list alias
            // Evaluator handles window functions via window_mapping if present
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
            // Handle NULLs: always sort last regardless of ASC/DESC
            let cmp = match (val_a.is_null(), val_b.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => return Ordering::Greater, // NULL always sorts last
                (false, true) => return Ordering::Less,    // non-NULL always sorts first
                (false, false) => {
                    // Compare non-NULL values, respecting direction
                    match dir {
                        vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                        vibesql_ast::OrderDirection::Desc => compare_sql_values(val_a, val_b).reverse(),
                    }
                }
            };

            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });

    Ok(rows)
}

/// Resolve ORDER BY expression that might be a SELECT list alias or column position
///
/// Handles three cases:
/// 1. Numeric literal (e.g., ORDER BY 1, 2, 3) - returns the expression from that position in SELECT list
/// 2. Simple column reference that matches a SELECT list alias - returns the SELECT list expression
/// 3. Otherwise - returns the original ORDER BY expression
fn resolve_order_by_alias<'a>(
    order_expr: &'a vibesql_ast::Expression,
    select_list: &'a [vibesql_ast::SelectItem],
) -> &'a vibesql_ast::Expression {
    // Check for numeric column position (ORDER BY 1, 2, 3, etc.)
    if let vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(pos)) = order_expr {
        if *pos > 0 && (*pos as usize) <= select_list.len() {
            // Valid column position, return the expression at that position
            let idx = (*pos as usize) - 1;
            if let vibesql_ast::SelectItem::Expression { expr, .. } = &select_list[idx] {
                return expr;
            }
        }
    }

    // Check if ORDER BY expression is a simple column reference (no table qualifier)
    if let vibesql_ast::Expression::ColumnRef { table: None, column } = order_expr {
        // Search for matching alias in SELECT list
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, alias: Some(alias_name) } = item {
                if alias_name == column {
                    // Found matching alias, use the SELECT list expression
                    return expr;
                }
            }
        }
    }

    // Not an alias or column position, use the original expression
    order_expr
}

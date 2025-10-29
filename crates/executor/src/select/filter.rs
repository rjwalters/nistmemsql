//! WHERE clause filtering logic

use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};

/// Apply WHERE clause filter to rows (Combined evaluator version)
///
/// Same as apply_where_filter but specifically for CombinedExpressionEvaluator.
/// Used in non-aggregation queries.
pub(super) fn apply_where_filter_combined(
    rows: Vec<storage::Row>,
    where_expr: Option<&ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let mut filtered_rows = Vec::new();

    for row in rows {
        let include_row = match evaluator.eval(where_expr, &row)? {
            types::SqlValue::Boolean(true) => true,
            types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "WHERE clause must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        };

        if include_row {
            filtered_rows.push(row);
        }
    }

    Ok(filtered_rows)
}

/// Apply WHERE clause filter to rows (Basic evaluator version)
///
/// Same as apply_where_filter but specifically for ExpressionEvaluator.
/// Used in aggregation queries.
#[allow(dead_code)]
pub(super) fn apply_where_filter_basic(
    rows: Vec<storage::Row>,
    where_expr: Option<&ast::Expression>,
    evaluator: &ExpressionEvaluator,
) -> Result<Vec<storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let mut filtered_rows = Vec::new();

    for row in rows {
        let include_row = match evaluator.eval(where_expr, &row)? {
            types::SqlValue::Boolean(true) => true,
            types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
            other => {
                return Err(ExecutorError::InvalidWhereClause(format!(
                    "WHERE must evaluate to boolean, got: {:?}",
                    other
                )))
            }
        };

        if include_row {
            filtered_rows.push(row);
        }
    }

    Ok(filtered_rows)
}

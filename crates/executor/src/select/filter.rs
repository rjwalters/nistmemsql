//! WHERE clause filtering logic

use crate::errors::ExecutorError;
use crate::evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator};

/// Apply WHERE clause filter to rows (Combined evaluator version)
///
/// Same as apply_where_filter but specifically for CombinedExpressionEvaluator.
/// Used in non-aggregation queries.
/// 
/// Accepts SelectExecutor for timeout enforcement. Timeout is checked every 1000 rows.
pub(super) fn apply_where_filter_combined<'a>(
    rows: Vec<storage::Row>,
    where_expr: Option<&ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let mut filtered_rows = Vec::new();
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for row in rows {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        let include_row = match evaluator.eval(where_expr, &row)? {
            types::SqlValue::Boolean(true) => true,
            types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            types::SqlValue::Integer(0) => false,
            types::SqlValue::Integer(_) => true,
            types::SqlValue::Smallint(0) => false,
            types::SqlValue::Smallint(_) => true,
            types::SqlValue::Bigint(0) => false,
            types::SqlValue::Bigint(_) => true,
            types::SqlValue::Float(f) if f == 0.0 => false,
            types::SqlValue::Float(_) => true,
            types::SqlValue::Real(f) if f == 0.0 => false,
            types::SqlValue::Real(_) => true,
            types::SqlValue::Double(f) if f == 0.0 => false,
            types::SqlValue::Double(_) => true,
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
/// 
/// Accepts SelectExecutor for timeout enforcement. Timeout is checked every 1000 rows.
#[allow(dead_code)]
pub(super) fn apply_where_filter_basic<'a>(
    rows: Vec<storage::Row>,
    where_expr: Option<&ast::Expression>,
    evaluator: &ExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let mut filtered_rows = Vec::new();
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for row in rows {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        let include_row = match evaluator.eval(where_expr, &row)? {
            types::SqlValue::Boolean(true) => true,
            types::SqlValue::Boolean(false) | types::SqlValue::Null => false,
            // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
            types::SqlValue::Integer(0) => false,
            types::SqlValue::Integer(_) => true,
            types::SqlValue::Smallint(0) => false,
            types::SqlValue::Smallint(_) => true,
            types::SqlValue::Bigint(0) => false,
            types::SqlValue::Bigint(_) => true,
            types::SqlValue::Float(f) if f == 0.0 => false,
            types::SqlValue::Float(_) => true,
            types::SqlValue::Real(f) if f == 0.0 => false,
            types::SqlValue::Real(_) => true,
            types::SqlValue::Double(f) if f == 0.0 => false,
            types::SqlValue::Double(_) => true,
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

//! Pre-evaluation of non-correlated scalar subqueries in HAVING clause
//!
//! This optimization detects non-correlated scalar subqueries in HAVING and
//! pre-evaluates them ONCE before the group-by loop. The HAVING expression is
//! then rewritten to use literal values instead of subquery expressions.
//!
//! This eliminates per-group overhead of:
//! - Correlation detection
//! - Cache key hash computation
//! - Cache lookup

use vibesql_ast::Expression;
use vibesql_storage::Database;

use crate::{correlation::is_correlated, errors::ExecutorError, schema::CombinedSchema};

/// Pre-evaluate non-correlated scalar subqueries in a HAVING expression.
///
/// Returns an optimized HAVING expression where non-correlated scalar subqueries
/// have been replaced with their literal values.
pub fn optimize_having_subqueries(
    having: &Expression,
    schema: &CombinedSchema,
    database: &Database,
) -> Result<Expression, ExecutorError> {
    rewrite_expression(having, schema, database)
}

/// Recursively rewrite an expression, replacing non-correlated scalar subqueries
/// with their pre-evaluated literal values.
fn rewrite_expression(
    expr: &Expression,
    schema: &CombinedSchema,
    database: &Database,
) -> Result<Expression, ExecutorError> {
    match expr {
        // Scalar subquery - check if non-correlated and pre-evaluate
        Expression::ScalarSubquery(subquery) => {
            if !is_correlated(subquery, schema) {
                // Non-correlated: evaluate once and replace with literal
                let executor = crate::select::SelectExecutor::new(database);
                let rows = executor.execute(subquery)?;

                // Extract scalar value from result (must be 0-1 rows, 1 column)
                let value = if rows.is_empty() {
                    vibesql_types::SqlValue::Null
                } else if rows.len() > 1 {
                    return Err(ExecutorError::SubqueryReturnedMultipleRows {
                        expected: 1,
                        actual: rows.len(),
                    });
                } else if rows[0].values.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: rows[0].values.len(),
                    });
                } else {
                    rows[0].values[0].clone()
                };

                Ok(Expression::Literal(value))
            } else {
                // Correlated: keep as-is
                Ok(expr.clone())
            }
        }

        // Binary operations - recursively rewrite both sides
        Expression::BinaryOp { left, op, right } => {
            let new_left = rewrite_expression(left, schema, database)?;
            let new_right = rewrite_expression(right, schema, database)?;
            Ok(Expression::BinaryOp {
                left: Box::new(new_left),
                op: op.clone(),
                right: Box::new(new_right),
            })
        }

        // Unary operations - recursively rewrite inner expression
        Expression::UnaryOp { op, expr: inner } => {
            let new_inner = rewrite_expression(inner, schema, database)?;
            Ok(Expression::UnaryOp { op: op.clone(), expr: Box::new(new_inner) })
        }

        // All other expressions - return as-is
        // The cache in CombinedExpressionEvaluator will still help with these
        _ => Ok(expr.clone()),
    }
}

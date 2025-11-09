//! Expression optimization logic for constant folding and dead code elimination

use ast::{CaseWhen, Expression};
use types::SqlValue;

use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Result of WHERE clause optimization
#[derive(Debug, PartialEq)]
pub enum WhereOptimization {
    /// WHERE clause was optimized to always true - can be removed
    AlwaysTrue,
    /// WHERE clause was optimized to always false - return empty result
    AlwaysFalse,
    /// WHERE clause was optimized but still needs evaluation
    Optimized(Expression),
    /// WHERE clause was not optimized
    Unchanged(Option<Expression>),
}

/// Optimize a SELECT statement's WHERE clause
///
/// Performs constant folding and dead code elimination on WHERE expressions.
/// Returns information about whether the WHERE clause can be eliminated entirely.
pub fn optimize_where_clause(
    where_expr: Option<&Expression>,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<WhereOptimization, ExecutorError> {
    match where_expr {
        None => Ok(WhereOptimization::Unchanged(None)),
        Some(expr) => {
            let optimized = optimize_expression(expr, evaluator)?;

            match optimized {
                Expression::Literal(SqlValue::Boolean(true)) => Ok(WhereOptimization::AlwaysTrue),
                Expression::Literal(SqlValue::Boolean(false)) => Ok(WhereOptimization::AlwaysFalse),
                Expression::Literal(SqlValue::Null) => {
                    // WHERE NULL is treated as false
                    Ok(WhereOptimization::AlwaysFalse)
                }
                optimized_expr => {
                    if optimized_expr == *expr {
                        Ok(WhereOptimization::Unchanged(Some(optimized_expr)))
                    } else {
                        Ok(WhereOptimization::Optimized(optimized_expr))
                    }
                }
            }
        }
    }
}

/// Optimize an expression by performing constant folding
///
/// Recursively walks the expression tree and evaluates any subexpressions
/// that don't reference columns (constants).
#[allow(clippy::only_used_in_recursion)]
pub fn optimize_expression(
    expr: &Expression,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Expression, ExecutorError> {
    match expr {
        // Literals are already optimized
        Expression::Literal(_) => Ok(expr.clone()),

        // Column references cannot be optimized
        Expression::ColumnRef { .. } => Ok(expr.clone()),

        // Binary operations - try to fold constants
        Expression::BinaryOp { left, op, right } => {
            let left_opt = optimize_expression(left, evaluator)?;
            let right_opt = optimize_expression(right, evaluator)?;

            // If both sides are literals, evaluate the operation
            if let (Expression::Literal(left_val), Expression::Literal(right_val)) =
                (&left_opt, &right_opt)
            {
                match ExpressionEvaluator::eval_binary_op_static(left_val, op, right_val) {
                    Ok(result) => Ok(Expression::Literal(result)),
                    Err(_) => Ok(Expression::BinaryOp {
                        left: Box::new(left_opt),
                        op: *op,
                        right: Box::new(right_opt),
                    }),
                }
            } else {
                Ok(Expression::BinaryOp {
                    left: Box::new(left_opt),
                    op: *op,
                    right: Box::new(right_opt),
                })
            }
        }

        // Unary operations - cannot optimize easily, keep as-is
        Expression::UnaryOp { op, expr: inner_expr } => {
            let inner_opt = optimize_expression(inner_expr, evaluator)?;
            Ok(Expression::UnaryOp { op: *op, expr: Box::new(inner_opt) })
        }

        // Function calls - cannot optimize generally
        Expression::Function { .. } => Ok(expr.clone()),

        // Aggregate functions - cannot optimize
        Expression::AggregateFunction { .. } => Ok(expr.clone()),

        // IS NULL/NOT NULL - try to fold if operand is literal
        Expression::IsNull { expr: inner_expr, negated } => {
            let inner_opt = optimize_expression(inner_expr, evaluator)?;

            if let Expression::Literal(val) = &inner_opt {
                let is_null = val.is_null();
                let result = if *negated { !is_null } else { is_null };
                Ok(Expression::Literal(SqlValue::Boolean(result)))
            } else {
                Ok(Expression::IsNull { expr: Box::new(inner_opt), negated: *negated })
            }
        }

        // Wildcard - cannot optimize
        Expression::Wildcard => Ok(expr.clone()),

        // CASE expressions - optimize recursively
        Expression::Case { operand, when_clauses, else_result } => {
            let operand_opt =
                operand.as_ref().map(|op| optimize_expression(op, evaluator)).transpose()?;
            let when_clauses_opt: Result<Vec<CaseWhen>, ExecutorError> = when_clauses
                .iter()
                .map(|wc| {
                    let conditions: Result<Vec<Expression>, ExecutorError> = wc
                        .conditions
                        .iter()
                        .map(|cond| optimize_expression(cond, evaluator))
                        .collect();
                    Ok(CaseWhen {
                        conditions: conditions?,
                        result: optimize_expression(&wc.result, evaluator)?,
                    })
                })
                .collect();
            let else_opt =
                else_result.as_ref().map(|er| optimize_expression(er, evaluator)).transpose()?;

            Ok(Expression::Case {
                operand: operand_opt.map(Box::new),
                when_clauses: when_clauses_opt?,
                else_result: else_opt.map(Box::new),
            })
        }

        // IN with subquery - cannot optimize
        Expression::In { .. } => Ok(expr.clone()),
        Expression::InList { .. } => Ok(expr.clone()),

        // BETWEEN - try to optimize operands
        Expression::Between { expr: inner_expr, low, high, negated, symmetric } => {
            let expr_opt = optimize_expression(inner_expr, evaluator)?;
            let low_opt = optimize_expression(low, evaluator)?;
            let high_opt = optimize_expression(high, evaluator)?;

            Ok(Expression::Between {
                expr: Box::new(expr_opt),
                low: Box::new(low_opt),
                high: Box::new(high_opt),
                negated: *negated,
                symmetric: *symmetric,
            })
        }

        // CAST - try to optimize operand
        Expression::Cast { expr: inner_expr, data_type } => {
            let expr_opt = optimize_expression(inner_expr, evaluator)?;

            if let Expression::Literal(_val) = &expr_opt {
                // TODO: Implement cast evaluation at plan time
                Ok(Expression::Cast { expr: Box::new(expr_opt), data_type: data_type.clone() })
            } else {
                Ok(Expression::Cast { expr: Box::new(expr_opt), data_type: data_type.clone() })
            }
        }

        // String functions - cannot optimize generally
        Expression::Position { .. } | Expression::Trim { .. } => Ok(expr.clone()),

        // LIKE - cannot optimize generally
        Expression::Like { .. } => Ok(expr.clone()),

        // EXISTS - cannot optimize
        Expression::Exists { .. } => Ok(expr.clone()),

        // Quantified comparison - cannot optimize
        Expression::QuantifiedComparison { .. } => Ok(expr.clone()),

        // Current date/time - cannot optimize
        Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. } => Ok(expr.clone()),

        // DEFAULT - cannot optimize
        Expression::Default => Ok(expr.clone()),

        // Window functions - cannot optimize
        Expression::WindowFunction { .. } => Ok(expr.clone()),

        // NEXT VALUE - cannot optimize
        Expression::NextValue { .. } => Ok(expr.clone()),

        // Scalar subquery - cannot optimize
        Expression::ScalarSubquery(_) => Ok(expr.clone()),
    }
}

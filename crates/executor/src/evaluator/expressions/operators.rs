//! Operator evaluation
//!
//! This module implements evaluation of operators including unary operators (+, -)

use crate::errors::ExecutorError;
use types::SqlValue;

/// Evaluate a unary operation
///
/// This function is shared by both ExpressionEvaluator and CombinedExpressionEvaluator
/// to avoid code duplication.
pub(crate) fn eval_unary_op(
    op: &ast::UnaryOperator,
    val: &SqlValue,
) -> Result<SqlValue, ExecutorError> {
    use ast::UnaryOperator::*;

    match (op, val) {
        // Unary plus - identity operation (return value unchanged)
        (Plus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(*n)),
        (Plus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(*n)),
        (Plus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(*n)),
        (Plus, SqlValue::Float(n)) => Ok(SqlValue::Float(*n)),
        (Plus, SqlValue::Real(n)) => Ok(SqlValue::Real(*n)),
        (Plus, SqlValue::Double(n)) => Ok(SqlValue::Double(*n)),
        (Plus, SqlValue::Numeric(s)) => Ok(SqlValue::Numeric(*s)),

        // Unary minus - negation
        (Minus, SqlValue::Integer(n)) => Ok(SqlValue::Integer(-n)),
        (Minus, SqlValue::Smallint(n)) => Ok(SqlValue::Smallint(-n)),
        (Minus, SqlValue::Bigint(n)) => Ok(SqlValue::Bigint(-n)),
        (Minus, SqlValue::Float(n)) => Ok(SqlValue::Float(-n)),
        (Minus, SqlValue::Real(n)) => Ok(SqlValue::Real(-n)),
        (Minus, SqlValue::Double(n)) => Ok(SqlValue::Double(-n)),
        (Minus, SqlValue::Numeric(f)) => Ok(SqlValue::Numeric(-*f)),

        // NULL propagation - unary operations on NULL return NULL
        (Plus | Minus, SqlValue::Null) => Ok(SqlValue::Null),

        // Unary NOT - logical negation
        (Not, SqlValue::Boolean(b)) => Ok(SqlValue::Boolean(!b)),
        (Not, SqlValue::Null) => Ok(SqlValue::Null), // NULL propagation for NOT

        // Type errors
        (Plus, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "unary +".to_string(),
            right: SqlValue::Null,
        }),
        (Minus, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "unary -".to_string(),
            right: SqlValue::Null,
        }),
        (Not, val) => Err(ExecutorError::TypeMismatch {
            left: val.clone(),
            op: "NOT".to_string(),
            right: SqlValue::Null,
        }),

        // Other unary operators are handled elsewhere
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Unary operator {:?} not supported in this context",
            op
        ))),
    }
}

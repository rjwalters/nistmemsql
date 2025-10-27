//! SQL Expression types for use in SELECT, WHERE, and other clauses

use crate::{BinaryOperator, SelectStmt, UnaryOperator};
use types::SqlValue;

/// SQL Expression (can appear in SELECT, WHERE, etc.)
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Literal value (42, 'hello', TRUE, NULL)
    Literal(SqlValue),

    /// Column reference (id, users.id)
    ColumnRef {
        table: Option<String>,
        column: String
    },

    /// Binary operation (a + b, x = y, p AND q)
    BinaryOp {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>
    },

    /// Unary operation (NOT x, -5)
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>
    },

    /// Function call (COUNT(*), SUM(x))
    Function {
        name: String,
        args: Vec<Expression>
    },

    /// IS NULL / IS NOT NULL
    IsNull {
        expr: Box<Expression>,
        negated: bool, // false = IS NULL, true = IS NOT NULL
    },

    /// Wildcard (*)
    Wildcard,

    /// CASE expression (both simple and searched forms)
    /// Simple CASE:  CASE x WHEN 1 THEN 'a' ELSE 'b' END
    /// Searched CASE: CASE WHEN x>0 THEN 'pos' ELSE 'neg' END
    Case {
        /// Operand for simple CASE (None for searched CASE)
        operand: Option<Box<Expression>>,

        /// List of WHEN clauses: (condition, result)
        /// - Simple CASE: condition is the comparison value
        /// - Searched CASE: condition is a boolean expression
        when_clauses: Vec<(Expression, Expression)>,

        /// Optional ELSE result (defaults to NULL if None)
        else_result: Option<Box<Expression>>,
    },

    /// Scalar subquery (returns single value)
    /// Example: WHERE salary > (SELECT AVG(salary) FROM employees)
    ScalarSubquery(Box<SelectStmt>),
}

//! SQL Expression types for use in SELECT, WHERE, and other clauses

use crate::{BinaryOperator, UnaryOperator};
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
    // TODO: Add CASE, CAST, subqueries, etc.
}

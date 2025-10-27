//! SQL Expression types for use in SELECT, WHERE, and other clauses

use crate::{BinaryOperator, SelectStmt, UnaryOperator};
use types::SqlValue;

/// SQL Expression (can appear in SELECT, WHERE, etc.)
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Literal value (42, 'hello', TRUE, NULL)
    Literal(SqlValue),

    /// Column reference (id, users.id)
    ColumnRef { table: Option<String>, column: String },

    /// Binary operation (a + b, x = y, p AND q)
    BinaryOp { op: BinaryOperator, left: Box<Expression>, right: Box<Expression> },

    /// Unary operation (NOT x, -5)
    UnaryOp { op: UnaryOperator, expr: Box<Expression> },

    /// Function call (COUNT(*), SUM(x))
    Function { name: String, args: Vec<Expression> },

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

    /// IN operator with subquery
    /// Example: WHERE id IN (SELECT user_id FROM orders)
    /// Example: WHERE status NOT IN (SELECT blocked_status FROM config)
    In {
        expr: Box<Expression>,
        subquery: Box<SelectStmt>,
        negated: bool, // false = IN, true = NOT IN
    },

    /// IN operator with value list
    /// Example: WHERE id IN (1, 2, 3)
    /// Example: WHERE name NOT IN ('admin', 'root')
    InList {
        expr: Box<Expression>,
        values: Vec<Expression>,
        negated: bool, // false = IN, true = NOT IN
    },

    /// BETWEEN predicate
    /// Example: WHERE age BETWEEN 18 AND 65
    /// Example: WHERE price NOT BETWEEN 10.0 AND 20.0
    /// Equivalent to: expr >= low AND expr <= high (or negated)
    Between {
        expr: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        negated: bool, // false = BETWEEN, true = NOT BETWEEN
    },

    /// CAST expression
    /// Example: CAST(value AS INTEGER)
    /// Example: CAST('123' AS NUMERIC(10, 2))
    Cast {
        expr: Box<Expression>,
        data_type: types::DataType,
    },

    /// LIKE pattern matching
    /// Example: name LIKE 'John%'
    /// Example: email NOT LIKE '%spam%'
    /// Pattern wildcards: % (any chars), _ (single char)
    Like {
        expr: Box<Expression>,
        pattern: Box<Expression>,
        negated: bool, // false = LIKE, true = NOT LIKE
    },

    /// EXISTS predicate
    /// Example: WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)
    /// Example: WHERE NOT EXISTS (SELECT 1 FROM orders WHERE customer_id = c.id)
    /// Returns TRUE if subquery returns at least one row, FALSE otherwise
    /// Never returns NULL (unlike most predicates)
    Exists {
        subquery: Box<SelectStmt>,
        negated: bool, // false = EXISTS, true = NOT EXISTS
    },

    /// Quantified comparison (ALL, ANY, SOME)
    /// Example: salary > ALL (SELECT salary FROM dept WHERE dept_id = 10)
    /// Example: price < ANY (SELECT price FROM competitors)
    /// Example: quantity = SOME (SELECT quantity FROM inventory)
    /// SOME is a synonym for ANY
    QuantifiedComparison {
        expr: Box<Expression>,
        op: BinaryOperator,
        quantifier: Quantifier,
        subquery: Box<SelectStmt>,
    },
}

/// Quantifier for quantified comparisons
#[derive(Debug, Clone, PartialEq)]
pub enum Quantifier {
    /// ALL - comparison must be TRUE for all rows
    All,
    /// ANY - comparison must be TRUE for at least one row
    Any,
    /// SOME - synonym for ANY
    Some,
}

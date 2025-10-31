//! Shared utilities for operator testing
//!
//! Provides helper functions to reduce boilerplate in operator edge case tests.

use crate::*;

/// Creates a simple SELECT statement with a single expression
///
/// Example:
/// ```rust
/// let expr = Expression::Literal(SqlValue::Integer(42));
/// let stmt = create_select_stmt(expr, "result");
/// ```
pub fn create_select_stmt(expr: ast::Expression, alias: &str) -> ast::SelectStmt {
    ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression { expr, alias: Some(alias.to_string()) }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    }
}

/// Executes an expression and returns the result
///
/// Example:
/// ```rust
/// let expr = Expression::Literal(SqlValue::Integer(42));
/// let result = execute_expression(&db, expr);
/// ```
pub fn execute_expression(
    db: &storage::Database,
    expr: ast::Expression,
) -> Result<types::SqlValue, ExecutorError> {
    let executor = SelectExecutor::new(db);
    let stmt = create_select_stmt(expr, "result");
    let result = executor.execute(&stmt)?;
    Ok(result[0].values[0].clone())
}

/// Asserts that an expression execution produces the expected value
///
/// Example:
/// ```rust
/// let expr = Expression::Literal(SqlValue::Integer(42));
/// assert_expression_result(&db, expr, SqlValue::Integer(42));
/// ```
#[allow(dead_code)]
pub fn assert_expression_result(
    db: &storage::Database,
    expr: ast::Expression,
    expected: types::SqlValue,
) {
    let result = execute_expression(db, expr).unwrap();
    assert_eq!(result, expected);
}

/// Asserts that an expression execution produces an error
///
/// Example:
/// ```rust
/// let expr = Expression::UnaryOp {
///     op: UnaryOperator::Plus,
///     expr: Box::new(Expression::Literal(SqlValue::Varchar("hello".to_string()))),
/// };
/// assert_expression_error(&db, expr);
/// ```
#[allow(dead_code)]
pub fn assert_expression_error(db: &storage::Database, expr: ast::Expression) {
    let result = execute_expression(db, expr);
    assert!(result.is_err());
}

/// Asserts that an expression execution produces a type mismatch error
///
/// Example:
/// ```rust
/// let expr = Expression::UnaryOp {
///     op: UnaryOperator::Plus,
///     expr: Box::new(Expression::Literal(SqlValue::Varchar("hello".to_string()))),
/// };
/// assert_type_mismatch(&db, expr);
/// ```
#[allow(dead_code)]
pub fn assert_type_mismatch(db: &storage::Database, expr: ast::Expression) {
    let executor = SelectExecutor::new(db);
    let stmt = create_select_stmt(expr, "result");
    let result = executor.execute(&stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::TypeMismatch { .. }));
}

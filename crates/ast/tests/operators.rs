use ast::*;
use types::SqlValue;

// ============================================================================
// BinaryOperator Tests - All SQL operators
// ============================================================================

#[test]
fn test_arithmetic_operators() {
    let _plus = BinaryOperator::Plus;
    let _minus = BinaryOperator::Minus;
    let _multiply = BinaryOperator::Multiply;
    let _divide = BinaryOperator::Divide;
    // If these compile, the operators exist
}

#[test]
fn test_comparison_operators() {
    let _eq = BinaryOperator::Equal;
    let _ne = BinaryOperator::NotEqual;
    let _lt = BinaryOperator::LessThan;
    let _le = BinaryOperator::LessThanOrEqual;
    let _gt = BinaryOperator::GreaterThan;
    let _ge = BinaryOperator::GreaterThanOrEqual;
    // If these compile, the operators exist
}

#[test]
fn test_logical_operators() {
    let _and = BinaryOperator::And;
    let _or = BinaryOperator::Or;
    // If these compile, the operators exist
}

#[test]
fn test_modulo_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Modulo,
        left: Box::new(Expression::Literal(SqlValue::Integer(10))),
        right: Box::new(Expression::Literal(SqlValue::Integer(3))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Modulo, .. } => {} // Success
        _ => panic!("Expected modulo operation"),
    }
}

#[test]
fn test_concat_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Concat,
        left: Box::new(Expression::Literal(SqlValue::Varchar("Hello".to_string()))),
        right: Box::new(Expression::Literal(SqlValue::Varchar("World".to_string()))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Concat, .. } => {} // Success
        _ => panic!("Expected concat operation"),
    }
}

#[test]
fn test_not_equal_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::NotEqual,
        left: Box::new(Expression::ColumnRef { table: None, column: "status".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Varchar("active".to_string()))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::NotEqual, .. } => {} // Success
        _ => panic!("Expected not equal operation"),
    }
}

#[test]
fn test_less_than_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::LessThan,
        left: Box::new(Expression::ColumnRef { table: None, column: "age".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(18))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::LessThan, .. } => {} // Success
        _ => panic!("Expected less than operation"),
    }
}

#[test]
fn test_greater_than_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::GreaterThan,
        left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(50000))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::GreaterThan, .. } => {} // Success
        _ => panic!("Expected greater than operation"),
    }
}

#[test]
fn test_and_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::And,
        left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::And, .. } => {} // Success
        _ => panic!("Expected AND operation"),
    }
}

#[test]
fn test_or_operator() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Or,
        left: Box::new(Expression::Literal(SqlValue::Boolean(true))),
        right: Box::new(Expression::Literal(SqlValue::Boolean(false))),
    };
    match expr {
        Expression::BinaryOp { op: BinaryOperator::Or, .. } => {} // Success
        _ => panic!("Expected OR operation"),
    }
}

// ============================================================================
// UnaryOperator Tests
// ============================================================================

#[test]
fn test_unary_not_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Not,
        expr: Box::new(Expression::Literal(SqlValue::Boolean(true))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Not, .. } => {} // Success
        _ => panic!("Expected NOT operation"),
    }
}

#[test]
fn test_unary_minus_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Minus,
        expr: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Minus, .. } => {} // Success
        _ => panic!("Expected unary minus operation"),
    }
}

#[test]
fn test_unary_plus_operator() {
    let expr = Expression::UnaryOp {
        op: UnaryOperator::Plus,
        expr: Box::new(Expression::Literal(SqlValue::Integer(42))),
    };
    match expr {
        Expression::UnaryOp { op: UnaryOperator::Plus, .. } => {} // Success
        _ => panic!("Expected unary plus operation"),
    }
}

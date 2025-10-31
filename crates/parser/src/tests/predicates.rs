//! Tests for SQL predicates (BETWEEN, LIKE, etc.)

use super::*;

#[test]
fn test_between_integer() {
    let sql = "SELECT * FROM users WHERE age BETWEEN 18 AND 65";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        // Check WHERE clause contains BETWEEN
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        // Verify it's a BETWEEN expression
        match where_expr {
            ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Check expr is 'age'
                match *expr {
                    ast::Expression::ColumnRef { table, column } => {
                        assert_eq!(table, None);
                        assert_eq!(column, "AGE");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }

                // Check low is 18
                match *low {
                    ast::Expression::Literal(types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 18);
                    }
                    _ => panic!("Expected Integer literal for low"),
                }

                // Check high is 65
                match *high {
                    ast::Expression::Literal(types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 65);
                    }
                    _ => panic!("Expected Integer literal for high"),
                }
            }
            _ => panic!("Expected Between expression, got {:?}", where_expr),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between() {
    let sql = "SELECT * FROM products WHERE price NOT BETWEEN 10.0 AND 20.0";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            ast::Expression::Between { expr, low: _, high: _, negated, symmetric } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Check expr is 'price'
                match *expr {
                    ast::Expression::ColumnRef { table, column } => {
                        assert_eq!(table, None);
                        assert_eq!(column, "PRICE");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_with_expressions() {
    let sql = "SELECT * FROM orders WHERE total BETWEEN price * 0.9 AND price * 1.1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated);
                assert!(!symmetric, "Should be ASYMMETRIC (default)");

                // Verify expr is 'total'
                match *expr {
                    ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "TOTAL");
                    }
                    _ => panic!("Expected ColumnRef"),
                }

                // Verify low and high are multiplication expressions
                match *low {
                    ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, ast::BinaryOperator::Multiply);
                    }
                    _ => panic!("Expected BinaryOp for low"),
                }

                match *high {
                    ast::Expression::BinaryOp { op, .. } => {
                        assert_eq!(op, ast::BinaryOperator::Multiply);
                    }
                    _ => panic!("Expected BinaryOp for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    }
}

#[test]
fn test_between_with_column_references() {
    let sql = "SELECT * FROM data WHERE value BETWEEN min_val AND max_val";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            ast::Expression::Between { low, high, .. } => {
                // Verify low is min_val
                match *low {
                    ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "MIN_VAL");
                    }
                    _ => panic!("Expected ColumnRef for low"),
                }

                // Verify high is max_val
                match *high {
                    ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "MAX_VAL");
                    }
                    _ => panic!("Expected ColumnRef for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    }
}

#[test]
fn test_between_asymmetric_explicit() {
    let sql = "SELECT * FROM t WHERE x BETWEEN ASYMMETRIC 1 AND 5";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            ast::Expression::Between { negated, symmetric, .. } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_between_symmetric() {
    let sql = "SELECT * FROM t WHERE x BETWEEN SYMMETRIC 1 AND 5";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            ast::Expression::Between { expr, low, high, negated, symmetric } => {
                assert!(!negated, "Should be BETWEEN, not NOT BETWEEN");
                assert!(symmetric, "Should be SYMMETRIC");

                // Check expr is 'x'
                match *expr {
                    ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "X");
                    }
                    _ => panic!("Expected ColumnRef for expr"),
                }

                // Check low is 1
                match *low {
                    ast::Expression::Literal(types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 1);
                    }
                    _ => panic!("Expected Integer literal for low"),
                }

                // Check high is 5
                match *high {
                    ast::Expression::Literal(types::SqlValue::Integer(val)) => {
                        assert_eq!(val, 5);
                    }
                    _ => panic!("Expected Integer literal for high"),
                }
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between_symmetric() {
    let sql = "SELECT * FROM t WHERE x NOT BETWEEN SYMMETRIC 10 AND 1";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            ast::Expression::Between { negated, symmetric, .. } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(symmetric, "Should be SYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_not_between_asymmetric() {
    let sql = "SELECT * FROM t WHERE x NOT BETWEEN ASYMMETRIC 1 AND 10";
    let stmt = Parser::parse_sql(sql).expect("Parse failed");

    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());
        let where_expr = select.where_clause.unwrap();

        match where_expr {
            ast::Expression::Between { negated, symmetric, .. } => {
                assert!(negated, "Should be NOT BETWEEN");
                assert!(!symmetric, "Should be ASYMMETRIC");
            }
            _ => panic!("Expected Between expression"),
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

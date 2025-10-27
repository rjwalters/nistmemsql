//! Tests for subquery parsing

use crate::Parser;
use ast::Expression;

#[test]
fn test_parse_in_subquery() {
    let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            // Check WHERE clause contains IN expression
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery, negated } => {
                    assert!(!negated);
                    // Check left expression is 'id'
                    match *expr {
                        Expression::ColumnRef { table, column } => {
                            assert_eq!(table, None);
                            assert_eq!(column, "id");
                        }
                        _ => panic!("Expected ColumnRef"),
                    }
                    // Check subquery structure
                    assert_eq!(subquery.select_list.len(), 1);
                }
                _ => panic!("Expected IN expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_not_in_subquery() {
    let sql = "SELECT * FROM users WHERE status NOT IN (SELECT blocked_status FROM config)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            // Check WHERE clause contains NOT IN expression
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery: _, negated } => {
                    assert!(negated); // Should be negated
                    // Check left expression is 'status'
                    match *expr {
                        Expression::ColumnRef { table, column } => {
                            assert_eq!(table, None);
                            assert_eq!(column, "status");
                        }
                        _ => panic!("Expected ColumnRef"),
                    }
                }
                _ => panic!("Expected IN expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_in_subquery_simple_column() {
    // Simpler test - just ensure IN works with a single column
    let sql = "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery: _, negated } => {
                    assert!(!negated);
                    match *expr {
                        Expression::ColumnRef { table, column } => {
                            assert_eq!(table, None);
                            assert_eq!(column, "user_id");
                        }
                        _ => panic!("Expected ColumnRef, got {:?}", expr),
                    }
                }
                other => panic!("Expected IN expression, got {:?}", other),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

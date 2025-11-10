//! Tests for subquery parsing (both IN and scalar subqueries)

use vibesql_ast::Expression;

use crate::Parser;

// ============================================================================
// IN Operator Subquery Tests (from PR #96)
// ============================================================================

#[test]
fn test_parse_in_subquery() {
    let sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Check WHERE clause contains IN expression
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery, negated } => {
                    assert!(!negated);
                    // Check left expression is 'id'
                    match *expr {
                        Expression::ColumnRef { table, column } => {
                            assert_eq!(table, None);
                            assert_eq!(column, "ID");
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
        vibesql_ast::Statement::Select(select) => {
            // Check WHERE clause contains NOT IN expression
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery: _, negated } => {
                    assert!(negated); // Should be negated
                                      // Check left expression is 'status'
                    match *expr {
                        Expression::ColumnRef { table, column } => {
                            assert_eq!(table, None);
                            assert_eq!(column, "STATUS");
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
        vibesql_ast::Statement::Select(select) => {
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::In { expr, subquery: _, negated } => {
                    assert!(!negated);
                    match *expr {
                        Expression::ColumnRef { table, column } => {
                            assert_eq!(table, None);
                            assert_eq!(column, "USER_ID");
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

// ============================================================================
// Scalar Subquery Tests (from PR #100)
// ============================================================================

#[test]
fn test_parse_scalar_subquery_simple() {
    // Test: (SELECT 1)
    let sql = "SELECT (SELECT 1)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, alias: _ } => {
                    // Should be a ScalarSubquery
                    match expr {
                        Expression::ScalarSubquery(subquery) => {
                            // Verify the subquery structure
                            assert_eq!(subquery.select_list.len(), 1);
                            match &subquery.select_list[0] {
                                vibesql_ast::SelectItem::Expression { expr, .. } => {
                                    match expr {
                                        Expression::Literal(_) => {
                                            // Expected literal 1
                                        }
                                        _ => panic!("Expected literal in subquery"),
                                    }
                                }
                                _ => panic!("Expected expression in subquery select list"),
                            }
                        }
                        _ => panic!("Expected ScalarSubquery, got {:?}", expr),
                    }
                }
                _ => panic!("Expected expression in select list"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_scalar_subquery_in_where() {
    // Test: WHERE x > (SELECT AVG(y) FROM t)
    let sql = "SELECT * FROM users WHERE salary > (SELECT AVG(salary) FROM employees)";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Check WHERE clause
            let where_clause = select.where_clause.unwrap();
            match where_clause {
                Expression::BinaryOp { op: _, left: _, right } => {
                    // Right side should be the scalar subquery
                    match *right {
                        Expression::ScalarSubquery(subquery) => {
                            // Verify it's selecting AVG
                            assert_eq!(subquery.select_list.len(), 1);
                            match &subquery.select_list[0] {
                                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                                    Expression::AggregateFunction { name, .. } => {
                                        assert_eq!(name.to_uppercase(), "AVG");
                                    }
                                    _ => panic!("Expected aggregate function call in subquery"),
                                },
                                _ => panic!("Expected expression in subquery"),
                            }
                        }
                        _ => panic!("Expected ScalarSubquery on right side of comparison"),
                    }
                }
                _ => panic!("Expected binary operation in WHERE clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_scalar_subquery_in_select() {
    // Test: SELECT id, (SELECT COUNT(*) FROM t2) FROM t1
    let sql = "SELECT id, (SELECT COUNT(*) FROM orders) FROM users";
    let stmt = Parser::parse_sql(sql).unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            // Should have 2 items in select list
            assert_eq!(select.select_list.len(), 2);

            // First should be id column
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "ID");
                    }
                    _ => panic!("Expected column reference"),
                },
                _ => panic!("Expected expression"),
            }

            // Second should be scalar subquery
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    Expression::ScalarSubquery(subquery) => {
                        // Verify it's COUNT(*)
                        assert_eq!(subquery.select_list.len(), 1);
                        match &subquery.select_list[0] {
                            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                                Expression::AggregateFunction { name, .. } => {
                                    assert_eq!(name.to_uppercase(), "COUNT");
                                }
                                _ => panic!("Expected aggregate function"),
                            },
                            _ => panic!("Expected expression"),
                        }
                    }
                    _ => panic!("Expected ScalarSubquery, got {:?}", expr),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

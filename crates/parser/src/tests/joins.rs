use super::*;

// ========================================================================
// JOIN Operation Tests
// ========================================================================

#[test]
fn test_parse_simple_join() {
    let result = Parser::parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match select.from.as_ref().unwrap() {
                ast::FromClause::Join { join_type, left, right, condition } => {
                    // Default JOIN is INNER JOIN
                    assert_eq!(*join_type, ast::JoinType::Inner);

                    // Left should be users table
                    match **left {
                        ast::FromClause::Table { ref name, .. } if name == "users" => {} // Success
                        _ => panic!("Expected left table to be 'users'"),
                    }

                    // Right should be orders table
                    match **right {
                        ast::FromClause::Table { ref name, .. } if name == "orders" => {} // Success
                        _ => panic!("Expected right table to be 'orders'"),
                    }

                    // Should have ON condition
                    assert!(condition.is_some());
                }
                _ => panic!("Expected JOIN in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_inner_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, ast::JoinType::Inner);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, ast::JoinType::LeftOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_outer_join() {
    let result = Parser::parse_sql(
        "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id;",
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, ast::JoinType::LeftOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_right_join() {
    let result =
        Parser::parse_sql("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => match select.from.as_ref().unwrap() {
            ast::FromClause::Join { join_type, .. } => {
                assert_eq!(*join_type, ast::JoinType::RightOuter);
            }
            _ => panic!("Expected JOIN"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_multiple_joins() {
    let result = Parser::parse_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id JOIN products ON orders.product_id = products.id;"
    );
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            // Should have nested JOINs
            match select.from.as_ref().unwrap() {
                ast::FromClause::Join { left, .. } => {
                    // Left should also be a JOIN
                    match **left {
                        ast::FromClause::Join { .. } => {} // Success - nested JOIN
                        _ => panic!("Expected nested JOIN"),
                    }
                }
                _ => panic!("Expected JOIN"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

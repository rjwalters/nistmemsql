use super::super::*;

// ============================================================================

#[test]
fn test_parse_select_42() {
    let result = Parser::parse_sql("SELECT 42;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        ast::Expression::Literal(types::SqlValue::Integer(42)) => {} // Success
                        _ => panic!("Expected Integer(42), got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
            assert!(select.where_clause.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_string() {
    let result = Parser::parse_sql("SELECT 'hello';");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Literal(types::SqlValue::Varchar(s)) if s == "hello" => {} // Success
                    _ => panic!("Expected Varchar('hello'), got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_arithmetic() {
    let result = Parser::parse_sql("SELECT 1 + 2;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::BinaryOp { op, left, right } => {
                        assert_eq!(*op, ast::BinaryOperator::Plus);
                        match **left {
                            ast::Expression::Literal(types::SqlValue::Integer(1)) => {}
                            _ => panic!("Expected left = 1"),
                        }
                        match **right {
                            ast::Expression::Literal(types::SqlValue::Integer(2)) => {}
                            _ => panic!("Expected right = 2"),
                        }
                    }
                    _ => panic!("Expected BinaryOp, got {:?}", expr),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_star() {
    let result = Parser::parse_sql("SELECT *;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Wildcard => {} // Success
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_from_table() {
    let result = Parser::parse_sql("SELECT * FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match &select.from.as_ref().unwrap() {
                ast::FromClause::Table { name, alias } => {
                    assert_eq!(name, "users");
                    assert!(alias.is_none());
                }
                _ => panic!("Expected table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_columns() {
    let result = Parser::parse_sql("SELECT id, name, age FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // Check first column (id)
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } if column == "id" => {}
                    _ => panic!("Expected id column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check second column (name)
            match &select.select_list[1] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } if column == "name" => {}
                    _ => panic!("Expected name column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check third column (age)
            match &select.select_list[2] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } if column == "age" => {}
                    _ => panic!("Expected age column"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_date() {
    let result = Parser::parse_sql("SELECT CURRENT_DATE;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        ast::Expression::CurrentDate => {
                            // CURRENT_DATE is now a dedicated expression variant
                        }
                        _ => panic!("Expected CurrentDate, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_time() {
    let result = Parser::parse_sql("SELECT CURRENT_TIME;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        ast::Expression::CurrentTime { precision } => {
                            // CURRENT_TIME is now a dedicated expression variant with optional precision
                            assert_eq!(*precision, None);
                        }
                        _ => panic!("Expected CurrentTime, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_current_timestamp() {
    let result = Parser::parse_sql("SELECT CURRENT_TIMESTAMP;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        ast::Expression::CurrentTimestamp { precision } => {
                            // CURRENT_TIMESTAMP is now a dedicated expression variant with optional precision
                            assert_eq!(*precision, None);
                        }
                        _ => panic!("Expected CurrentTimestamp, got {:?}", expr),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
            assert!(select.from.is_none());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

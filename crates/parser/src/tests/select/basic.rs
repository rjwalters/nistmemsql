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
                ast::SelectItem::Wildcard { alias: _ } => {} // Success
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
                    assert_eq!(name, "USERS");
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
                    ast::Expression::ColumnRef { column, .. } if column == "ID" => {}
                    _ => panic!("Expected id column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check second column (name)
            match &select.select_list[1] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } if column == "NAME" => {}
                    _ => panic!("Expected name column"),
                },
                _ => panic!("Expected Expression select item"),
            }

            // Check third column (age)
            match &select.select_list[2] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } if column == "AGE" => {}
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
                        ast::Expression::CurrentDate => {}
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

#[test]
fn test_parse_select_qualified_wildcard() {
    let result = Parser::parse_sql("SELECT table_name.* FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                    assert_eq!(qualifier, "TABLE_NAME");
                }
                _ => panic!(
                    "Expected QualifiedWildcard select item, got {:?}",
                    select.select_list[0]
                ),
            }
            // Check FROM clause exists
            assert!(select.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_qualified_wildcard_alias() {
    let result = Parser::parse_sql("SELECT alias.* FROM table_name AS alias;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::QualifiedWildcard { qualifier, alias: _ } => {
                    assert_eq!(qualifier, "ALIAS");
                }
                _ => panic!("Expected QualifiedWildcard select item"),
            }
            assert!(select.from.is_some());
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ============================================================================
// Tests for SELECT ALL syntax (SQL:1999 E051-01)
// ============================================================================

#[test]
fn test_parse_select_all_literal() {
    let result = Parser::parse_sql("SELECT ALL 42;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            // ALL means include duplicates (distinct = false)
            assert!(!select.distinct);
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
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_all_star() {
    let result = Parser::parse_sql("SELECT ALL *;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(!select.distinct);
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Wildcard { alias: _ } => {} // Success
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_all_from_table() {
    let result = Parser::parse_sql("SELECT ALL A FROM T;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(!select.distinct);
            assert_eq!(select.select_list.len(), 1);
            assert!(select.from.is_some());
            match &select.from.as_ref().unwrap() {
                ast::FromClause::Table { name, alias } => {
                    assert_eq!(name, "T");
                    assert!(alias.is_none());
                }
                _ => panic!("Expected table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_all_multiple_columns() {
    let result = Parser::parse_sql("SELECT ALL id, name FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(!select.distinct);
            assert_eq!(select.select_list.len(), 2);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_select_distinct_still_works() {
    // Verify DISTINCT still works after adding ALL support
    let result = Parser::parse_sql("SELECT DISTINCT id FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(select.distinct);
            assert_eq!(select.select_list.len(), 1);
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ============================================================================
// Tests for derived column lists (SQL:1999 E051-07, E051-08)
// ============================================================================

#[test]
fn test_parse_select_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT * AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_all_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT ALL * AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_distinct_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT DISTINCT * AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(select.distinct);
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_qualified_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT table_name.* AS (C, D) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
                    assert_eq!(qualifier, "TABLE_NAME");
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected QualifiedWildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_alias_wildcard_with_derived_column_list() {
    let result = Parser::parse_sql("SELECT t.* AS (C, D) FROM table_name AS t;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
                    assert_eq!(qualifier, "T");
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 2);
                    assert_eq!(derived_cols[0], "C");
                    assert_eq!(derived_cols[1], "D");
                }
                _ => panic!("Expected QualifiedWildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_wildcard_with_multiple_columns_in_derived_list() {
    let result = Parser::parse_sql("SELECT * AS (A, B, C, D, E) FROM table_name;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Wildcard { alias } => {
                    assert!(alias.is_some());
                    let derived_cols = alias.as_ref().unwrap();
                    assert_eq!(derived_cols.len(), 5);
                    assert_eq!(derived_cols[0], "A");
                    assert_eq!(derived_cols[1], "B");
                    assert_eq!(derived_cols[2], "C");
                    assert_eq!(derived_cols[3], "D");
                    assert_eq!(derived_cols[4], "E");
                }
                _ => panic!("Expected Wildcard select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

// ============================================================================
// Backtick Identifier Tests (MySQL-style)
// ============================================================================

#[test]
fn test_parse_select_with_backtick_column_names() {
    let result = Parser::parse_sql("SELECT `user_id`, `user_name` FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, alias } => {
                    assert!(alias.is_none());
                    match expr {
                        ast::Expression::ColumnRef { column, .. } => {
                            // Backtick identifiers preserve case
                            assert_eq!(column, "user_id");
                        }
                        _ => panic!("Expected ColumnRef"),
                    }
                }
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_backtick_table_name() {
    let result = Parser::parse_sql("SELECT * FROM `user_table`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert!(select.from.is_some());
            match &select.from.as_ref().unwrap() {
                ast::FromClause::Table { name, alias } => {
                    // Backtick identifiers preserve case
                    assert_eq!(name, "user_table");
                    assert!(alias.is_none());
                }
                _ => panic!("Expected Table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_backtick_qualified_column() {
    let result = Parser::parse_sql("SELECT `my_table`.`my_column` FROM `my_table`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, table, .. } => {
                        assert_eq!(column, "my_column");
                        assert_eq!(table.as_ref().unwrap(), "my_table");
                    }
                    _ => panic!("Expected qualified ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_with_backtick_reserved_words() {
    // Reserved words can be used as identifiers when backtick-quoted
    let result = Parser::parse_sql("SELECT `select`, `from` FROM `where`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "select");
                    }
                    _ => panic!("Expected ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
            // Check FROM clause
            match &select.from.as_ref().unwrap() {
                ast::FromClause::Table { name, .. } => {
                    assert_eq!(name, "where");
                }
                _ => panic!("Expected Table in FROM clause"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_parse_select_mixed_backtick_and_regular() {
    let result = Parser::parse_sql("SELECT id, `userName`, status FROM `MyTable`;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);
            // First column (regular identifier - uppercased)
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "ID");
                    }
                    _ => panic!("Expected ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
            // Second column (backtick - preserves case)
            match &select.select_list[1] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::ColumnRef { column, .. } => {
                        assert_eq!(column, "userName");
                    }
                    _ => panic!("Expected ColumnRef"),
                },
                _ => panic!("Expected Expression select item"),
            }
        }
        _ => panic!("Expected SELECT statement"),
    }
}

use super::*;

// ========================================================================
// Aggregate Function Tests
// ========================================================================

#[test]
fn test_parse_count_star() {
    let result = Parser::parse_sql("SELECT COUNT(*) FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::AggregateFunction { name, distinct, args } => {
                        assert_eq!(name, "COUNT");
                        assert!(!(*distinct));
                        assert_eq!(args.len(), 1);
                        // COUNT(*) is represented as a special wildcard expression
                    }
                    _ => panic!("Expected aggregate function call"),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_count_column() {
    let result = Parser::parse_sql("SELECT COUNT(id) FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, distinct, args } => {
                    assert_eq!(name, "COUNT");
                    assert!(!(*distinct));
                    assert_eq!(args.len(), 1);
                    match &args[0] {
                        vibesql_ast::Expression::ColumnRef { column, .. } if column == "ID" => {}
                        _ => panic!("Expected column reference"),
                    }
                }
                _ => panic!("Expected aggregate function call"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_sum_function() {
    let result = Parser::parse_sql("SELECT SUM(amount) FROM orders;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, distinct, args } => {
                    assert_eq!(name, "SUM");
                    assert!(!(*distinct));
                    assert_eq!(args.len(), 1);
                }
                _ => panic!("Expected SUM aggregate function"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_avg_function() {
    let result = Parser::parse_sql("SELECT AVG(price) FROM products;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                vibesql_ast::Expression::AggregateFunction { name, .. } => {
                    assert_eq!(name, "AVG");
                }
                _ => panic!("Expected AVG aggregate function"),
            },
            _ => panic!("Expected expression"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_min_max_functions() {
    let result = Parser::parse_sql("SELECT MIN(price), MAX(price) FROM products;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);

            // Check MIN
            match &select.select_list[0] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "MIN");
                    }
                    _ => panic!("Expected MIN aggregate function"),
                },
                _ => panic!("Expected expression"),
            }

            // Check MAX
            match &select.select_list[1] {
                vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "MAX");
                    }
                    _ => panic!("Expected MAX aggregate function"),
                },
                _ => panic!("Expected expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_aggregate_with_alias() {
    let result = Parser::parse_sql("SELECT COUNT(*) AS total FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias } => {
                match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "COUNT");
                    }
                    _ => panic!("Expected aggregate function"),
                }
                assert_eq!(alias.as_ref().unwrap(), "TOTAL");
            }
            _ => panic!("Expected expression with alias"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_aggregate_with_alias_without_as() {
    let result = Parser::parse_sql("SELECT COUNT(*) total FROM users;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => match &select.select_list[0] {
            vibesql_ast::SelectItem::Expression { expr, alias } => {
                match expr {
                    vibesql_ast::Expression::AggregateFunction { name, .. } => {
                        assert_eq!(name, "COUNT");
                    }
                    _ => panic!("Expected aggregate function"),
                }
                assert_eq!(alias.as_ref().unwrap(), "TOTAL");
            }
            _ => panic!("Expected expression with alias"),
        },
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_multiple_aggregates() {
    let result = Parser::parse_sql("SELECT COUNT(*), SUM(amount), AVG(amount) FROM orders;");
    assert!(result.is_ok());
    let stmt = result.unwrap();

    match stmt {
        vibesql_ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // Verify all are aggregate functions
            for item in &select.select_list {
                match item {
                    vibesql_ast::SelectItem::Expression { expr, .. } => match expr {
                        vibesql_ast::Expression::AggregateFunction { .. } => {} // Success
                        _ => panic!("Expected aggregate function"),
                    },
                    _ => panic!("Expected expression"),
                }
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

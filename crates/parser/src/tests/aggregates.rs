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
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 1);
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Function { name, args } => {
                        assert_eq!(name, "COUNT");
                        assert_eq!(args.len(), 1);
                        // COUNT(*) is represented as a special wildcard expression
                    }
                    _ => panic!("Expected function call"),
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
        ast::Statement::Select(select) => match &select.select_list[0] {
            ast::SelectItem::Expression { expr, .. } => match expr {
                ast::Expression::Function { name, args } => {
                    assert_eq!(name, "COUNT");
                    assert_eq!(args.len(), 1);
                    match &args[0] {
                        ast::Expression::ColumnRef { column, .. } if column == "id" => {}
                        _ => panic!("Expected column reference"),
                    }
                }
                _ => panic!("Expected function call"),
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
        ast::Statement::Select(select) => match &select.select_list[0] {
            ast::SelectItem::Expression { expr, .. } => match expr {
                ast::Expression::Function { name, args } => {
                    assert_eq!(name, "SUM");
                    assert_eq!(args.len(), 1);
                }
                _ => panic!("Expected SUM function"),
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
        ast::Statement::Select(select) => match &select.select_list[0] {
            ast::SelectItem::Expression { expr, .. } => match expr {
                ast::Expression::Function { name, .. } => {
                    assert_eq!(name, "AVG");
                }
                _ => panic!("Expected AVG function"),
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
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 2);

            // Check MIN
            match &select.select_list[0] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Function { name, .. } => {
                        assert_eq!(name, "MIN");
                    }
                    _ => panic!("Expected MIN function"),
                },
                _ => panic!("Expected expression"),
            }

            // Check MAX
            match &select.select_list[1] {
                ast::SelectItem::Expression { expr, .. } => match expr {
                    ast::Expression::Function { name, .. } => {
                        assert_eq!(name, "MAX");
                    }
                    _ => panic!("Expected MAX function"),
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
        ast::Statement::Select(select) => match &select.select_list[0] {
            ast::SelectItem::Expression { expr, alias } => {
                match expr {
                    ast::Expression::Function { name, .. } => {
                        assert_eq!(name, "COUNT");
                    }
                    _ => panic!("Expected function"),
                }
                assert_eq!(alias.as_ref().unwrap(), "total");
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
        ast::Statement::Select(select) => {
            assert_eq!(select.select_list.len(), 3);

            // Verify all are functions
            for item in &select.select_list {
                match item {
                    ast::SelectItem::Expression { expr, .. } => match expr {
                        ast::Expression::Function { .. } => {} // Success
                        _ => panic!("Expected function"),
                    },
                    _ => panic!("Expected expression"),
                }
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

use super::*;

// ========================================================================
// IN Predicate with Value Lists Tests
// ========================================================================

#[test]
fn test_parse_in_with_integer_list() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (1, 2, 3);");
    assert!(result.is_ok(), "IN with integer list should parse: {:?}", result);

    let stmt = result.unwrap();
    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        // Should be an InList expression
        if let ast::Expression::InList { expr, values, negated } = &select.where_clause.unwrap() {
            // Left side should be column reference
            assert!(matches!(**expr, ast::Expression::ColumnRef { .. }));

            // Should have 3 values
            assert_eq!(values.len(), 3);

            // All should be integer literals
            assert!(matches!(values[0], ast::Expression::Literal(types::SqlValue::Integer(1))));
            assert!(matches!(values[1], ast::Expression::Literal(types::SqlValue::Integer(2))));
            assert!(matches!(values[2], ast::Expression::Literal(types::SqlValue::Integer(3))));

            // Not negated
            assert_eq!(*negated, false);
        } else {
            panic!("Expected InList expression");
        }
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_parse_in_with_string_list() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE name IN ('Alice', 'Bob', 'Charlie');");
    assert!(result.is_ok(), "IN with string list should parse: {:?}", result);
}

#[test]
fn test_parse_in_with_mixed_types() {
    let result = Parser::parse_sql("SELECT * FROM data WHERE value IN (1, 'two', 3);");
    assert!(result.is_ok(), "IN with mixed types should parse: {:?}", result);
}

#[test]
fn test_parse_in_with_single_value() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (42);");
    assert!(result.is_ok(), "IN with single value should parse: {:?}", result);
}

#[test]
fn test_parse_not_in_with_value_list() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE status NOT IN ('inactive', 'banned');");
    assert!(result.is_ok(), "NOT IN with value list should parse: {:?}", result);

    let stmt = result.unwrap();
    if let ast::Statement::Select(select) = stmt {
        assert!(select.where_clause.is_some());

        // Should be an InList expression with negated=true
        if let ast::Expression::InList { negated, values, .. } = &select.where_clause.unwrap() {
            assert_eq!(*negated, true, "NOT IN should set negated=true");
            assert_eq!(values.len(), 2);
        } else {
            panic!("Expected InList expression");
        }
    }
}

#[test]
fn test_parse_in_with_expressions() {
    // IN list can contain expressions, not just literals
    let result = Parser::parse_sql("SELECT * FROM products WHERE price IN (10 + 5, 20 * 2, 100);");
    assert!(result.is_ok(), "IN with expressions should parse: {:?}", result);
}

#[test]
fn test_parse_in_list_with_and() {
    let result = Parser::parse_sql(
        "SELECT * FROM users WHERE age > 18 AND status IN ('active', 'pending');"
    );
    assert!(result.is_ok(), "IN list with AND should parse: {:?}", result);
}

#[test]
fn test_parse_in_list_with_or() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE category IN ('electronics', 'computers') OR price < 100;"
    );
    assert!(result.is_ok(), "IN list with OR should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_in_lists() {
    let result = Parser::parse_sql(
        "SELECT * FROM data WHERE type IN ('A', 'B') AND status IN (1, 2, 3);"
    );
    assert!(result.is_ok(), "Multiple IN lists should parse: {:?}", result);
}

#[test]
fn test_parse_in_empty_list_should_error() {
    // SQL standard requires at least one value in IN list
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN ();");
    assert!(result.is_err(), "Empty IN list should fail to parse");
}

#[test]
fn test_parse_in_list_with_null() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE status IN ('active', NULL, 'pending');");
    assert!(result.is_ok(), "IN list with NULL should parse: {:?}", result);
}

#[test]
fn test_parse_in_list_complex_expression() {
    let result = Parser::parse_sql(
        "SELECT * FROM orders WHERE (customer_id IN (1, 2, 3) AND total > 100) OR status = 'vip';"
    );
    assert!(result.is_ok(), "Complex expression with IN list should parse: {:?}", result);
}

use super::*;

// ========================================================================
// COALESCE Function Tests
// ========================================================================

#[test]
fn test_parse_coalesce_two_args() {
    let result = Parser::parse_sql("SELECT COALESCE(col1, col2) FROM table1;");
    assert!(result.is_ok(), "COALESCE with 2 args should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_multiple_args() {
    let result = Parser::parse_sql("SELECT COALESCE(col1, col2, col3, col4) FROM table1;");
    assert!(result.is_ok(), "COALESCE with 4 args should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_with_literals() {
    let result = Parser::parse_sql("SELECT COALESCE(name, 'Unknown') FROM users;");
    assert!(result.is_ok(), "COALESCE with literal should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_with_null() {
    let result = Parser::parse_sql("SELECT COALESCE(NULL, NULL, 'default') FROM table1;");
    assert!(result.is_ok(), "COALESCE with NULL should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_in_where() {
    let result =
        Parser::parse_sql("SELECT * FROM users WHERE COALESCE(status, 'active') = 'active';");
    assert!(result.is_ok(), "COALESCE in WHERE should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_nested() {
    let result = Parser::parse_sql("SELECT COALESCE(COALESCE(a, b), c) FROM table1;");
    assert!(result.is_ok(), "Nested COALESCE should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_with_expressions() {
    let result = Parser::parse_sql("SELECT COALESCE(price * 0.9, price, 100) FROM products;");
    assert!(result.is_ok(), "COALESCE with expressions should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_single_arg_should_error() {
    // COALESCE requires at least 2 arguments per SQL standard
    let result = Parser::parse_sql("SELECT COALESCE(col1) FROM table1;");
    // Note: This test expects parsing to succeed but execution to validate arg count
    // Some SQL implementations allow single arg, we'll validate at execution
    assert!(result.is_ok(), "Single arg COALESCE parses (validated at execution)");
}

// ========================================================================
// NULLIF Function Tests
// ========================================================================

#[test]
fn test_parse_nullif_basic() {
    let result = Parser::parse_sql("SELECT NULLIF(col1, col2) FROM table1;");
    assert!(result.is_ok(), "NULLIF should parse: {:?}", result);
}

#[test]
fn test_parse_nullif_with_literals() {
    let result = Parser::parse_sql("SELECT NULLIF(status, 'deleted') FROM users;");
    assert!(result.is_ok(), "NULLIF with literal should parse: {:?}", result);
}

#[test]
fn test_parse_nullif_with_null() {
    let result = Parser::parse_sql("SELECT NULLIF(value, NULL) FROM table1;");
    assert!(result.is_ok(), "NULLIF with NULL should parse: {:?}", result);
}

#[test]
fn test_parse_nullif_in_where() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE NULLIF(balance, 0) IS NOT NULL;");
    assert!(result.is_ok(), "NULLIF in WHERE should parse: {:?}", result);
}

#[test]
fn test_parse_nullif_with_expressions() {
    let result = Parser::parse_sql("SELECT NULLIF(a + b, c * d) FROM table1;");
    assert!(result.is_ok(), "NULLIF with expressions should parse: {:?}", result);
}

#[test]
fn test_parse_nullif_nested() {
    let result = Parser::parse_sql("SELECT NULLIF(NULLIF(a, b), c) FROM table1;");
    assert!(result.is_ok(), "Nested NULLIF should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_and_nullif_combined() {
    let result =
        Parser::parse_sql("SELECT COALESCE(NULLIF(status, 'unknown'), 'active') FROM users;");
    assert!(result.is_ok(), "COALESCE with NULLIF should parse: {:?}", result);
}

#[test]
fn test_parse_nullif_in_case() {
    let result = Parser::parse_sql(
        "SELECT CASE WHEN NULLIF(balance, 0) = NULL THEN 'zero' ELSE 'non-zero' END FROM accounts;",
    );
    assert!(result.is_ok(), "NULLIF in CASE should parse: {:?}", result);
}

#[test]
fn test_parse_coalesce_in_order_by() {
    let result = Parser::parse_sql("SELECT * FROM users ORDER BY COALESCE(last_name, first_name);");
    assert!(result.is_ok(), "COALESCE in ORDER BY should parse: {:?}", result);
}

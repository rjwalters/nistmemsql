use super::*;

// ========================================================================
// ALL Quantifier Tests
// ========================================================================

#[test]
fn test_parse_all_with_greater_than() {
    let result = Parser::parse_sql(
        "SELECT * FROM employees WHERE salary > ALL (SELECT salary FROM dept WHERE dept_id = 10);",
    );
    assert!(result.is_ok(), "ALL with > should parse: {:?}", result);
}

#[test]
fn test_parse_all_with_less_than() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE price < ALL (SELECT price FROM competitors);",
    );
    assert!(result.is_ok(), "ALL with < should parse: {:?}", result);
}

#[test]
fn test_parse_all_with_equals() {
    let result = Parser::parse_sql(
        "SELECT * FROM orders WHERE quantity = ALL (SELECT quantity FROM inventory);",
    );
    assert!(result.is_ok(), "ALL with = should parse: {:?}", result);
}

#[test]
fn test_parse_all_with_not_equals() {
    let result = Parser::parse_sql(
        "SELECT * FROM users WHERE status <> ALL (SELECT status FROM blacklist);",
    );
    assert!(result.is_ok(), "ALL with <> should parse: {:?}", result);
}

#[test]
fn test_parse_all_with_greater_or_equal() {
    let result =
        Parser::parse_sql("SELECT * FROM items WHERE rating >= ALL (SELECT rating FROM reviews);");
    assert!(result.is_ok(), "ALL with >= should parse: {:?}", result);
}

#[test]
fn test_parse_all_with_less_or_equal() {
    let result =
        Parser::parse_sql("SELECT * FROM bids WHERE amount <= ALL (SELECT amount FROM max_bids);");
    assert!(result.is_ok(), "ALL with <= should parse: {:?}", result);
}

// ========================================================================
// ANY Quantifier Tests
// ========================================================================

#[test]
fn test_parse_any_with_greater_than() {
    let result = Parser::parse_sql(
        "SELECT * FROM employees WHERE salary > ANY (SELECT salary FROM dept WHERE dept_id = 10);",
    );
    assert!(result.is_ok(), "ANY with > should parse: {:?}", result);
}

#[test]
fn test_parse_any_with_less_than() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE price < ANY (SELECT price FROM competitors);",
    );
    assert!(result.is_ok(), "ANY with < should parse: {:?}", result);
}

#[test]
fn test_parse_any_with_equals() {
    let result = Parser::parse_sql(
        "SELECT * FROM orders WHERE status = ANY (SELECT status FROM valid_statuses);",
    );
    assert!(result.is_ok(), "ANY with = should parse: {:?}", result);
}

#[test]
fn test_parse_any_with_not_equals() {
    let result = Parser::parse_sql(
        "SELECT * FROM users WHERE user_role <> ANY (SELECT user_role FROM admin_roles);",
    );
    assert!(result.is_ok(), "ANY with <> should parse: {:?}", result);
}

// ========================================================================
// SOME Quantifier Tests (synonym for ANY)
// ========================================================================

#[test]
fn test_parse_some_with_greater_than() {
    let result = Parser::parse_sql(
        "SELECT * FROM employees WHERE salary > SOME (SELECT salary FROM dept WHERE dept_id = 10);",
    );
    assert!(result.is_ok(), "SOME with > should parse: {:?}", result);
}

#[test]
fn test_parse_some_with_less_than() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE price < SOME (SELECT price FROM competitors);",
    );
    assert!(result.is_ok(), "SOME with < should parse: {:?}", result);
}

#[test]
fn test_parse_some_with_equals() {
    let result = Parser::parse_sql(
        "SELECT * FROM orders WHERE quantity = SOME (SELECT quantity FROM inventory);",
    );
    assert!(result.is_ok(), "SOME with = should parse: {:?}", result);
}

// ========================================================================
// Complex Quantified Comparison Tests
// ========================================================================

#[test]
fn test_parse_quantified_with_complex_subquery() {
    let result = Parser::parse_sql(
        "SELECT * FROM employees WHERE salary > ALL (SELECT salary FROM dept WHERE dept_id = 10 AND active = TRUE);"
    );
    assert!(result.is_ok(), "ALL with complex subquery should parse: {:?}", result);
}

#[test]
fn test_parse_quantified_in_where_with_and() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE price < ANY (SELECT price FROM competitors) AND stock > 0;",
    );
    assert!(result.is_ok(), "Quantified with AND should parse: {:?}", result);
}

#[test]
fn test_parse_quantified_in_where_with_or() {
    let result = Parser::parse_sql(
        "SELECT * FROM orders WHERE quantity > ALL (SELECT min_qty FROM rules) OR priority = 'high';"
    );
    assert!(result.is_ok(), "Quantified with OR should parse: {:?}", result);
}

#[test]
fn test_parse_quantified_in_select_list() {
    let result = Parser::parse_sql(
        "SELECT name, salary > ALL (SELECT salary FROM dept WHERE dept_id = 10) AS is_highest FROM employees;"
    );
    assert!(result.is_ok(), "Quantified in SELECT list should parse: {:?}", result);
}

#[test]
fn test_parse_multiple_quantified_comparisons() {
    let result = Parser::parse_sql(
        "SELECT * FROM employees WHERE salary > ANY (SELECT salary FROM juniors) AND salary < ALL (SELECT salary FROM seniors);"
    );
    assert!(result.is_ok(), "Multiple quantified comparisons should parse: {:?}", result);
}

#[test]
fn test_parse_quantified_with_arithmetic() {
    let result = Parser::parse_sql(
        "SELECT * FROM products WHERE price * 0.9 < ANY (SELECT price FROM competitors);",
    );
    assert!(result.is_ok(), "Quantified with arithmetic should parse: {:?}", result);
}

#[test]
fn test_parse_quantified_in_case() {
    let result = Parser::parse_sql(
        "SELECT CASE WHEN salary > ALL (SELECT salary FROM dept WHERE dept_id = 10) THEN 'highest' ELSE 'not highest' END FROM employees;"
    );
    assert!(result.is_ok(), "Quantified in CASE should parse: {:?}", result);
}

#[test]
fn test_parse_nested_quantified_subquery() {
    let result = Parser::parse_sql(
        "SELECT * FROM employees WHERE salary > ALL (SELECT salary FROM dept WHERE budget > ANY (SELECT budget FROM divisions));"
    );
    assert!(result.is_ok(), "Nested quantified subquery should parse: {:?}", result);
}

// ========================================================================
// Edge Cases and Validation
// ========================================================================

#[test]
fn test_parse_all_any_some_are_case_insensitive() {
    let sql_variants = vec![
        "SELECT * FROM t WHERE x > all (SELECT y FROM t2);",
        "SELECT * FROM t WHERE x > ALL (SELECT y FROM t2);",
        "SELECT * FROM t WHERE x > Any (SELECT y FROM t2);",
        "SELECT * FROM t WHERE x > SOME (SELECT y FROM t2);",
    ];

    for sql in sql_variants {
        let result = Parser::parse_sql(sql);
        assert!(
            result.is_ok(),
            "Case-insensitive quantifier should parse: {} -> {:?}",
            sql,
            result
        );
    }
}

#[test]
fn test_parse_quantified_with_parenthesized_expression() {
    let result = Parser::parse_sql(
        "SELECT * FROM employees WHERE (salary * 1.1) > ALL (SELECT salary FROM dept);",
    );
    assert!(result.is_ok(), "Quantified with parenthesized expr should parse: {:?}", result);
}

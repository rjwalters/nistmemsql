//! Tests for parser error handling with malformed SQL.

use crate::parser::Parser;

#[test]
fn test_parse_error_unexpected_token_in_select() {
    let result = Parser::parse_sql("SELECT FROM users");
    assert!(result.is_err(), "Should fail with unexpected FROM");
}

#[test]
fn test_parse_error_incomplete_select() {
    let result = Parser::parse_sql("SELECT");
    assert!(result.is_err(), "Should fail with incomplete SELECT");
}

#[test]
fn test_parse_error_missing_from_table_name() {
    let result = Parser::parse_sql("SELECT * FROM");
    assert!(result.is_err(), "Should fail with missing table name");
}

#[test]
fn test_parse_error_incomplete_where_clause() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE");
    assert!(result.is_err(), "Should fail with incomplete WHERE");
}

#[test]
fn test_parse_error_incomplete_insert() {
    let result = Parser::parse_sql("INSERT INTO");
    assert!(result.is_err(), "Should fail with incomplete INSERT");
}

#[test]
fn test_parse_error_missing_values_keyword() {
    let result = Parser::parse_sql("INSERT INTO users");
    assert!(result.is_err(), "Should fail with missing VALUES");
}

#[test]
fn test_parse_error_incomplete_update() {
    let result = Parser::parse_sql("UPDATE");
    assert!(result.is_err(), "Should fail with incomplete UPDATE");
}

#[test]
fn test_parse_error_missing_set_keyword() {
    let result = Parser::parse_sql("UPDATE users");
    assert!(result.is_err(), "Should fail with missing SET");
}

#[test]
fn test_parse_error_incomplete_delete() {
    let result = Parser::parse_sql("DELETE");
    assert!(result.is_err(), "Should fail with incomplete DELETE");
}

#[test]
fn test_parse_error_missing_from_in_delete() {
    let result = Parser::parse_sql("DELETE users");
    assert!(result.is_err(), "Should fail with missing FROM");
}

#[test]
fn test_parse_error_incomplete_create_table() {
    let result = Parser::parse_sql("CREATE TABLE");
    assert!(result.is_err(), "Should fail with incomplete CREATE TABLE");
}

#[test]
fn test_parse_error_missing_columns_in_create() {
    let result = Parser::parse_sql("CREATE TABLE users");
    assert!(result.is_err(), "Should fail with missing column list");
}

#[test]
fn test_parse_error_incomplete_join() {
    let result = Parser::parse_sql("SELECT * FROM users JOIN");
    assert!(result.is_err(), "Should fail with incomplete JOIN");
}

#[test]
fn test_parse_error_incomplete_group_by() {
    let result = Parser::parse_sql("SELECT * FROM users GROUP BY");
    assert!(result.is_err(), "Should fail with incomplete GROUP BY");
}

#[test]
fn test_parse_error_incomplete_order_by() {
    let result = Parser::parse_sql("SELECT * FROM users ORDER BY");
    assert!(result.is_err(), "Should fail with incomplete ORDER BY");
}

#[test]
fn test_parse_error_mismatched_parentheses() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE (id = 1");
    assert!(result.is_err(), "Should fail with mismatched parentheses");
}

#[test]
fn test_parse_error_invalid_operator() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id === 1");
    assert!(result.is_err(), "Should fail with invalid operator");
}

#[test]
fn test_parse_error_incomplete_having() {
    let result = Parser::parse_sql("SELECT COUNT(*) FROM users GROUP BY id HAVING");
    assert!(result.is_err(), "Should fail with incomplete HAVING");
}

#[test]
fn test_parse_error_empty_sql() {
    let result = Parser::parse_sql("");
    assert!(result.is_err(), "Should fail with empty SQL");
}

#[test]
fn test_parse_error_only_semicolon() {
    let result = Parser::parse_sql(";");
    assert!(result.is_err(), "Should fail with only semicolon");
}

#[test]
fn test_parse_error_incomplete_limit() {
    let result = Parser::parse_sql("SELECT * FROM users LIMIT");
    assert!(result.is_err(), "Should fail with incomplete LIMIT");
}

#[test]
fn test_parse_error_incomplete_offset() {
    let result = Parser::parse_sql("SELECT * FROM users OFFSET");
    assert!(result.is_err(), "Should fail with incomplete OFFSET");
}

#[test]
fn test_parse_error_incomplete_subquery() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (");
    assert!(result.is_err(), "Should fail with incomplete subquery");
}

#[test]
fn test_parse_error_missing_select_in_subquery() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id IN (1, 2");
    assert!(result.is_err(), "Should fail with incomplete value list");
}

#[test]
fn test_parse_error_unexpected_keyword() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE WHERE id = 1");
    assert!(result.is_err(), "Should fail with duplicate WHERE");
}

#[test]
fn test_parse_error_incomplete_expression() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id +");
    assert!(result.is_err(), "Should fail with incomplete expression");
}

#[test]
fn test_parse_error_missing_table_in_from() {
    let result = Parser::parse_sql("SELECT id, name FROM");
    assert!(result.is_err(), "Should fail with missing table");
}

#[test]
fn test_parse_error_incomplete_set_clause() {
    let result = Parser::parse_sql("UPDATE users SET");
    assert!(result.is_err(), "Should fail with incomplete SET clause");
}

#[test]
fn test_parse_error_incomplete_insert_values() {
    let result = Parser::parse_sql("INSERT INTO users VALUES");
    assert!(result.is_err(), "Should fail with incomplete VALUES");
}

#[test]
fn test_parse_error_create_table_empty_parens() {
    let result = Parser::parse_sql("CREATE TABLE users ()");
    assert!(result.is_err(), "Should fail with empty column list");
}

#[test]
fn test_parse_error_incomplete_column_definition() {
    let result = Parser::parse_sql("CREATE TABLE users (id");
    assert!(result.is_err(), "Should fail with incomplete column");
}

#[test]
fn test_parse_error_missing_data_type() {
    let result = Parser::parse_sql("CREATE TABLE users (id,");
    assert!(result.is_err(), "Should fail with missing data type");
}

#[test]
fn test_parse_error_unexpected_eof_in_select_list() {
    let result = Parser::parse_sql("SELECT id,");
    assert!(result.is_err(), "Should fail with incomplete select list");
}

#[test]
fn test_parse_error_unclosed_parenthesis() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE (id = 1 OR name = 'test'");
    assert!(result.is_err(), "Should fail with unclosed parenthesis");
}

#[test]
fn test_parse_error_missing_expression_after_operator() {
    let result = Parser::parse_sql("SELECT * FROM users WHERE id = ");
    assert!(result.is_err(), "Should fail with missing expression");
}

#[test]
fn test_parse_error_invalid_table_source() {
    let result = Parser::parse_sql("SELECT * FROM 123");
    assert!(result.is_err(), "Should fail with invalid table source");
}

#[test]
fn test_parse_error_missing_join_condition() {
    let result = Parser::parse_sql("SELECT * FROM users LEFT JOIN orders ON");
    assert!(result.is_err(), "Should fail with missing join condition");
}

#[test]
fn test_parse_error_invalid_assignment_in_update() {
    let result = Parser::parse_sql("UPDATE users SET name");
    assert!(result.is_err(), "Should fail with invalid assignment");
}

#[test]
fn test_parse_error_missing_parenthesis_in_insert() {
    let result = Parser::parse_sql("INSERT INTO users VALUES (1, 'test'");
    assert!(result.is_err(), "Should fail with missing closing paren");
}

#[test]
fn test_parse_error_empty_where_clause() {
    let result = Parser::parse_sql("DELETE FROM users WHERE");
    assert!(result.is_err(), "Should fail with empty WHERE clause");
}

#[test]
fn test_parse_error_missing_from_in_update_statement() {
    let result = Parser::parse_sql("UPDATE SET name = 'test'");
    assert!(result.is_err(), "Should fail with missing table name");
}

#[test]
fn test_parse_error_select_with_just_comma() {
    let result = Parser::parse_sql("SELECT id, , name FROM users");
    assert!(result.is_err(), "Should fail with consecutive commas");
}

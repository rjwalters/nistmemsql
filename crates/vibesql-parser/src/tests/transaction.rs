//! Tests for transaction control statement parsing (BEGIN, COMMIT, ROLLBACK)

use crate::Parser;

#[test]
fn test_parse_begin() {
    let result = Parser::parse_sql("BEGIN");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(_) => (),
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_begin_transaction() {
    let result = Parser::parse_sql("BEGIN TRANSACTION");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(_) => (),
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_start_transaction() {
    let result = Parser::parse_sql("START TRANSACTION");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::BeginTransaction(_) => (),
        other => panic!("Expected BeginTransaction, got {:?}", other),
    }
}

#[test]
fn test_parse_commit() {
    let result = Parser::parse_sql("COMMIT");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::Commit(_) => (),
        other => panic!("Expected Commit, got {:?}", other),
    }
}

#[test]
fn test_parse_rollback() {
    let result = Parser::parse_sql("ROLLBACK");
    assert!(result.is_ok());
    match result.unwrap() {
        vibesql_ast::Statement::Rollback(_) => (),
        other => panic!("Expected Rollback, got {:?}", other),
    }
}

#[test]
fn test_transaction_keywords_case_insensitive() {
    // Test case insensitivity
    let result1 = Parser::parse_sql("begin");
    assert!(result1.is_ok());

    let result2 = Parser::parse_sql("COMMIT");
    assert!(result2.is_ok());

    let result3 = Parser::parse_sql("rollback");
    assert!(result3.is_ok());
}

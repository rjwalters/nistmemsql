//! Tests for CREATE TYPE and DROP TYPE (SQL:1999 user-defined types)

use ast::Statement;
use executor::TypeExecutor;
use parser::Parser;
use storage::Database;

fn execute_and_expect_success(db: &mut Database, sql: &str) -> String {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    match stmt {
        Statement::CreateType(s) => {
            TypeExecutor::execute_create_type(&s, db).expect("Execution failed")
        }
        Statement::DropType(s) => {
            TypeExecutor::execute_drop_type(&s, db).expect("Execution failed")
        }
        _ => panic!("Unexpected statement type"),
    }
}

fn execute_and_expect_error(db: &mut Database, sql: &str) -> String {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");
    let result = match stmt {
        Statement::CreateType(s) => TypeExecutor::execute_create_type(&s, db),
        Statement::DropType(s) => TypeExecutor::execute_drop_type(&s, db),
        _ => panic!("Unexpected statement type"),
    };
    result.expect_err("Expected error but got success").to_string()
}

#[test]
fn test_create_distinct_type_integer() {
    let mut db = Database::new();
    let result = execute_and_expect_success(&mut db, "CREATE TYPE age AS DISTINCT INTEGER");
    assert_eq!(result, "Type 'AGE' created");
    assert!(db.catalog.type_exists("AGE"));
}

#[test]
fn test_create_distinct_type_varchar() {
    let mut db = Database::new();
    let result = execute_and_expect_success(&mut db, "CREATE TYPE email AS DISTINCT VARCHAR");
    assert_eq!(result, "Type 'EMAIL' created");
    assert!(db.catalog.type_exists("EMAIL"));
}

#[test]
fn test_create_distinct_type_decimal() {
    let mut db = Database::new();
    let result = execute_and_expect_success(&mut db, "CREATE TYPE money AS DISTINCT DECIMAL");
    assert_eq!(result, "Type 'MONEY' created");
    assert!(db.catalog.type_exists("MONEY"));
}

#[test]
fn test_create_distinct_type_boolean() {
    let mut db = Database::new();
    let result = execute_and_expect_success(&mut db, "CREATE TYPE flag AS DISTINCT BOOLEAN");
    assert_eq!(result, "Type 'FLAG' created");
    assert!(db.catalog.type_exists("FLAG"));
}

#[test]
fn test_create_distinct_type_date() {
    let mut db = Database::new();
    let result = execute_and_expect_success(&mut db, "CREATE TYPE birthdate AS DISTINCT DATE");
    assert_eq!(result, "Type 'BIRTHDATE' created");
    assert!(db.catalog.type_exists("BIRTHDATE"));
}

#[test]
fn test_create_structured_type_single_attr() {
    let mut db = Database::new();
    let result = execute_and_expect_success(&mut db, "CREATE TYPE point AS (x INTEGER)");
    assert_eq!(result, "Type 'POINT' created");
    assert!(db.catalog.type_exists("POINT"));
}

#[test]
fn test_create_structured_type_two_attrs() {
    let mut db = Database::new();
    let result = execute_and_expect_success(&mut db, "CREATE TYPE coord AS (x INTEGER, y INTEGER)");
    assert_eq!(result, "Type 'COORD' created");
    assert!(db.catalog.type_exists("COORD"));
}

#[test]
fn test_create_structured_type_multi_attrs() {
    let mut db = Database::new();
    let sql = "CREATE TYPE address AS (street VARCHAR, city VARCHAR, zip VARCHAR)";
    let result = execute_and_expect_success(&mut db, sql);
    assert_eq!(result, "Type 'ADDRESS' created");
    assert!(db.catalog.type_exists("ADDRESS"));
}

#[test]
fn test_create_structured_type_complex() {
    let mut db = Database::new();
    let sql = "CREATE TYPE person AS (name VARCHAR, age INTEGER, active BOOLEAN, created DATE)";
    let result = execute_and_expect_success(&mut db, sql);
    assert_eq!(result, "Type 'PERSON' created");
    assert!(db.catalog.type_exists("PERSON"));
}

#[test]
fn test_create_type_duplicate_error() {
    let mut db = Database::new();
    execute_and_expect_success(&mut db, "CREATE TYPE mytype AS DISTINCT INTEGER");

    // Try to create the same type again - should fail
    let error = execute_and_expect_error(&mut db, "CREATE TYPE mytype AS DISTINCT INTEGER");
    assert!(error.contains("already exists") || error.contains("MYTYPE"));
}

#[test]
fn test_drop_type_distinct() {
    let mut db = Database::new();
    execute_and_expect_success(&mut db, "CREATE TYPE temperature AS DISTINCT FLOAT");

    let result = execute_and_expect_success(&mut db, "DROP TYPE temperature");
    assert_eq!(result, "Type 'TEMPERATURE' dropped");
    assert!(!db.catalog.type_exists("TEMPERATURE"));
}

#[test]
fn test_drop_type_structured() {
    let mut db = Database::new();
    execute_and_expect_success(&mut db, "CREATE TYPE point AS (x INTEGER, y INTEGER)");

    let result = execute_and_expect_success(&mut db, "DROP TYPE point");
    assert_eq!(result, "Type 'POINT' dropped");
    assert!(!db.catalog.type_exists("POINT"));
}

#[test]
fn test_drop_type_restrict() {
    let mut db = Database::new();
    execute_and_expect_success(&mut db, "CREATE TYPE percentage AS DISTINCT DECIMAL");

    let result = execute_and_expect_success(&mut db, "DROP TYPE percentage RESTRICT");
    assert_eq!(result, "Type 'PERCENTAGE' dropped");
}

#[test]
fn test_drop_type_cascade() {
    let mut db = Database::new();
    execute_and_expect_success(&mut db, "CREATE TYPE score AS DISTINCT INTEGER");

    let result = execute_and_expect_success(&mut db, "DROP TYPE score CASCADE");
    assert_eq!(result, "Type 'SCORE' dropped");
}

#[test]
fn test_drop_type_not_found() {
    let mut db = Database::new();

    let error = execute_and_expect_error(&mut db, "DROP TYPE nonexistent");
    assert!(error.contains("not found") || error.contains("NONEXISTENT"));
}

#[test]
fn test_create_drop_recreate() {
    let mut db = Database::new();

    // Create
    execute_and_expect_success(&mut db, "CREATE TYPE mytype AS DISTINCT INTEGER");
    assert!(db.catalog.type_exists("MYTYPE"));

    // Drop
    execute_and_expect_success(&mut db, "DROP TYPE mytype");
    assert!(!db.catalog.type_exists("MYTYPE"));

    // Recreate with different definition
    execute_and_expect_success(&mut db, "CREATE TYPE mytype AS DISTINCT VARCHAR");
    assert!(db.catalog.type_exists("MYTYPE"));
}

#[test]
fn test_multiple_types() {
    let mut db = Database::new();

    // Create multiple types
    execute_and_expect_success(&mut db, "CREATE TYPE type1 AS DISTINCT INTEGER");
    execute_and_expect_success(&mut db, "CREATE TYPE type2 AS DISTINCT VARCHAR");
    execute_and_expect_success(&mut db, "CREATE TYPE type3 AS (x INTEGER, y INTEGER)");

    // Verify all exist
    assert!(db.catalog.type_exists("TYPE1"));
    assert!(db.catalog.type_exists("TYPE2"));
    assert!(db.catalog.type_exists("TYPE3"));

    // Drop them
    execute_and_expect_success(&mut db, "DROP TYPE type1");
    execute_and_expect_success(&mut db, "DROP TYPE type2");
    execute_and_expect_success(&mut db, "DROP TYPE type3");

    // Verify all dropped
    assert!(!db.catalog.type_exists("TYPE1"));
    assert!(!db.catalog.type_exists("TYPE2"));
    assert!(!db.catalog.type_exists("TYPE3"));
}

#[test]
fn test_create_type_forward_declaration() {
    let mut db = Database::new();

    // Create a forward declaration (without AS clause)
    let result = execute_and_expect_success(&mut db, "CREATE TYPE forward_type");
    assert_eq!(result, "Type 'FORWARD_TYPE' created");
    assert!(db.catalog.type_exists("FORWARD_TYPE"));
}

//! Test CREATE SCHEMA with embedded schema elements

use executor::SchemaExecutor;
use parser::Parser;
use storage::Database;

#[test]
fn test_create_schema_with_embedded_table() {
    let mut db = Database::new();

    // Parse CREATE SCHEMA with embedded CREATE TABLE
    let sql = "CREATE SCHEMA test_schema CREATE TABLE users (id INT, name VARCHAR(50))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let ast::Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);
        assert!(result.is_ok(), "Failed to execute: {:?}", result.err());

        // Verify schema was created
        assert!(db.catalog.schema_exists("test_schema"));

        // Verify table was created in the schema
        assert!(db.catalog.table_exists("test_schema.users"));
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_with_multiple_tables() {
    let mut db = Database::new();

    // Parse CREATE SCHEMA with multiple embedded CREATE TABLE statements
    let sql = "CREATE SCHEMA myschema CREATE TABLE t1 (a INT) CREATE TABLE t2 (b VARCHAR(10))";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let ast::Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);
        assert!(result.is_ok(), "Failed to execute: {:?}", result.err());

        // Verify schema was created
        assert!(db.catalog.schema_exists("myschema"));

        // Verify both tables were created
        assert!(db.catalog.table_exists("myschema.t1"));
        assert!(db.catalog.table_exists("myschema.t2"));
    } else {
        panic!("Expected CreateSchema statement");
    }
}

#[test]
fn test_create_schema_no_elements_still_works() {
    let mut db = Database::new();

    // Parse simple CREATE SCHEMA (no embedded elements)
    let sql = "CREATE SCHEMA simple";
    let stmt = Parser::parse_sql(sql).expect("Failed to parse");

    // Execute the statement
    if let ast::Statement::CreateSchema(create_schema_stmt) = stmt {
        let result = SchemaExecutor::execute_create_schema(&create_schema_stmt, &mut db);
        assert!(result.is_ok(), "Failed to execute: {:?}", result.err());

        // Verify schema was created
        assert!(db.catalog.schema_exists("simple"));
    } else {
        panic!("Expected CreateSchema statement");
    }
}

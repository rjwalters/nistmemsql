//! Tests for database introspection statements (SHOW, DESCRIBE)

use crate::parser::Parser;
use vibesql_ast::*;

#[test]
fn test_show_tables() {
    let stmt = Parser::parse_sql("SHOW TABLES").unwrap();
    assert!(matches!(stmt, Statement::ShowTables(_)));

    if let Statement::ShowTables(show_tables) = stmt {
        assert!(show_tables.database.is_none());
        assert!(show_tables.like_pattern.is_none());
        assert!(show_tables.where_clause.is_none());
    }
}

#[test]
fn test_show_tables_from_database() {
    let stmt = Parser::parse_sql("SHOW TABLES FROM mydb").unwrap();

    if let Statement::ShowTables(show_tables) = stmt {
        assert_eq!(show_tables.database, Some("MYDB".to_string()));
        assert!(show_tables.like_pattern.is_none());
        assert!(show_tables.where_clause.is_none());
    } else {
        panic!("Expected ShowTables statement");
    }
}

#[test]
fn test_show_databases() {
    let stmt = Parser::parse_sql("SHOW DATABASES").unwrap();
    assert!(matches!(stmt, Statement::ShowDatabases(_)));

    if let Statement::ShowDatabases(show_databases) = stmt {
        assert!(show_databases.like_pattern.is_none());
        assert!(show_databases.where_clause.is_none());
    }
}

#[test]
fn test_describe_table() {
    let stmt = Parser::parse_sql("DESCRIBE users").unwrap();
    assert!(matches!(stmt, Statement::Describe(_)));

    if let Statement::Describe(describe) = stmt {
        assert_eq!(describe.table_name, "USERS");
        assert!(describe.column_pattern.is_none());
    }
}

#[test]
fn test_describe_table_with_column() {
    let stmt = Parser::parse_sql("DESCRIBE users name").unwrap();

    if let Statement::Describe(describe) = stmt {
        assert_eq!(describe.table_name, "USERS");
        assert_eq!(describe.column_pattern, Some("NAME".to_string()));
    } else {
        panic!("Expected Describe statement");
    }
}

#[test]
fn test_show_columns() {
    let stmt = Parser::parse_sql("SHOW COLUMNS FROM users").unwrap();

    if let Statement::ShowColumns(show_columns) = stmt {
        assert_eq!(show_columns.table_name, "USERS");
        assert!(!show_columns.full);
        assert!(show_columns.database.is_none());
        assert!(show_columns.like_pattern.is_none());
        assert!(show_columns.where_clause.is_none());
    } else {
        panic!("Expected ShowColumns statement");
    }
}

#[test]
fn test_show_index() {
    let stmt = Parser::parse_sql("SHOW INDEX FROM users").unwrap();

    if let Statement::ShowIndex(show_index) = stmt {
        assert_eq!(show_index.table_name, "USERS");
        assert!(show_index.database.is_none());
    } else {
        panic!("Expected ShowIndex statement");
    }
}

#[test]
fn test_show_create_table() {
    let stmt = Parser::parse_sql("SHOW CREATE TABLE users").unwrap();

    if let Statement::ShowCreateTable(show_create) = stmt {
        assert_eq!(show_create.table_name, "USERS");
    } else {
        panic!("Expected ShowCreateTable statement");
    }
}

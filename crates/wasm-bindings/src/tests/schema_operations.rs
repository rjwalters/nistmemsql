//! Schema DDL operation tests for WASM API
//!
//! Tests CREATE/DROP TABLE, roles, domains, sequences, and other DDL operations

use super::helpers::execute_sql;

#[test]
fn test_execute_create_table() {
    let mut db = storage::Database::new();
    let sql = "CREATE TABLE users (id INTEGER, name VARCHAR(50))";

    let stmt = parser::Parser::parse_sql(sql).expect("Parse failed");
    match stmt {
        ast::Statement::CreateTable(create_stmt) => {
            executor::CreateTableExecutor::execute(&create_stmt, &mut db)
                .expect("Create table failed");
        }
        _ => panic!("Expected CreateTable statement"),
    }

    // Verify table was created (table names are normalized to uppercase)
    assert!(db.get_table("USERS").is_some());
}

#[test]
fn test_execute_drop_table() {
    let mut db = storage::Database::new();

    // Create table first
    let create_sql = "CREATE TABLE temp (id INTEGER)";
    execute_sql(&mut db, create_sql).expect("Setup failed");
    assert!(db.get_table("TEMP").is_some());

    // Drop it
    let drop_sql = "DROP TABLE temp";
    let stmt = parser::Parser::parse_sql(drop_sql).expect("Parse failed");
    match stmt {
        ast::Statement::DropTable(drop_stmt) => {
            executor::DropTableExecutor::execute(&drop_stmt, &mut db).expect("Drop table failed");
        }
        _ => panic!("Expected DropTable statement"),
    }

    // Verify table was dropped (table names are normalized to uppercase)
    assert!(db.get_table("TEMP").is_none());
}

#[test]
fn test_execute_insert() {
    let mut db = storage::Database::new();

    // Create table
    let create_sql = "CREATE TABLE products (id INTEGER, name VARCHAR(50))";
    execute_sql(&mut db, create_sql).expect("Setup failed");

    // Insert data
    let insert_sql = "INSERT INTO products (id, name) VALUES (1, 'Widget')";
    let stmt = parser::Parser::parse_sql(insert_sql).expect("Parse failed");
    match stmt {
        ast::Statement::Insert(insert_stmt) => {
            let row_count =
                executor::InsertExecutor::execute(&mut db, &insert_stmt).expect("Insert failed");
            assert_eq!(row_count, 1);
        }
        _ => panic!("Expected Insert statement"),
    }
}

#[test]
fn test_execute_roles() {
    let mut db = storage::Database::new();

    // Create role
    let create_sql = "CREATE ROLE admin";
    let stmt = parser::Parser::parse_sql(create_sql).expect("Parse failed");
    match stmt {
        ast::Statement::CreateRole(create_role_stmt) => {
            executor::RoleExecutor::execute_create_role(&create_role_stmt, &mut db)
                .expect("Create role failed");
        }
        _ => panic!("Expected CreateRole statement"),
    }

    // Drop role
    let drop_sql = "DROP ROLE admin";
    let stmt = parser::Parser::parse_sql(drop_sql).expect("Parse failed");
    match stmt {
        ast::Statement::DropRole(drop_role_stmt) => {
            executor::RoleExecutor::execute_drop_role(&drop_role_stmt, &mut db)
                .expect("Drop role failed");
        }
        _ => panic!("Expected DropRole statement"),
    }
}

#[test]
fn test_execute_domains() {
    let mut db = storage::Database::new();

    // Create domain
    let create_sql = "CREATE DOMAIN email_address AS VARCHAR(255)";
    let stmt = parser::Parser::parse_sql(create_sql).expect("Parse failed");
    match stmt {
        ast::Statement::CreateDomain(create_domain_stmt) => {
            executor::DomainExecutor::execute_create_domain(&create_domain_stmt, &mut db)
                .expect("Create domain failed");
        }
        _ => panic!("Expected CreateDomain statement"),
    }

    // Drop domain
    let drop_sql = "DROP DOMAIN email_address";
    let stmt = parser::Parser::parse_sql(drop_sql).expect("Parse failed");
    match stmt {
        ast::Statement::DropDomain(drop_domain_stmt) => {
            executor::DomainExecutor::execute_drop_domain(&drop_domain_stmt, &mut db)
                .expect("Drop domain failed");
        }
        _ => panic!("Expected DropDomain statement"),
    }
}

#[test]
fn test_execute_sequences() {
    let mut db = storage::Database::new();

    // Create sequence
    let create_sql = "CREATE SEQUENCE user_id_seq";
    let stmt = parser::Parser::parse_sql(create_sql).expect("Parse failed");
    match stmt {
        ast::Statement::CreateSequence(create_seq_stmt) => {
            executor::advanced_objects::execute_create_sequence(&create_seq_stmt, &mut db)
                .expect("Create sequence failed");
        }
        _ => panic!("Expected CreateSequence statement"),
    }

    // Alter sequence
    let alter_sql = "ALTER SEQUENCE user_id_seq RESTART WITH 100";
    let stmt = parser::Parser::parse_sql(alter_sql).expect("Parse failed");
    match stmt {
        ast::Statement::AlterSequence(alter_seq_stmt) => {
            executor::advanced_objects::execute_alter_sequence(&alter_seq_stmt, &mut db)
                .expect("Alter sequence failed");
        }
        _ => panic!("Expected AlterSequence statement"),
    }

    // Drop sequence
    let drop_sql = "DROP SEQUENCE user_id_seq";
    let stmt = parser::Parser::parse_sql(drop_sql).expect("Parse failed");
    match stmt {
        ast::Statement::DropSequence(drop_seq_stmt) => {
            executor::advanced_objects::execute_drop_sequence(&drop_seq_stmt, &mut db)
                .expect("Drop sequence failed");
        }
        _ => panic!("Expected DropSequence statement"),
    }
}

#[test]
#[ignore] // CREATE TYPE and DROP TYPE support is limited - testing via execute.rs wrapping
fn test_execute_types() {
    // Note: CREATE TYPE with full syntax is not yet supported by the parser
    // The execute.rs module handles these statement types when they ARE supported
    // This test documents the intended functionality for when parser support is added

    // When parser support is ready, this will test:
    // 1. CREATE TYPE custom_type AS ...
    // 2. DROP TYPE custom_type

    // For now, this test is ignored but serves as documentation
}

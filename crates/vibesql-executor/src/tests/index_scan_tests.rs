//! Integration tests for index scan optimization
//!
//! These tests verify that the executor correctly uses indexes for query optimization
//! when appropriate indexes exist and WHERE clauses can benefit from them.

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::select::SelectExecutor;

/// Create a test database with users table
fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create users table
    let users_schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "email".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
            ColumnSchema::new("age".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "city".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    db.create_table(users_schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("alice@example.com".to_string()),
            SqlValue::Integer(25),
            SqlValue::Varchar("Boston".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("bob@example.com".to_string()),
            SqlValue::Integer(30),
            SqlValue::Varchar("New York".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("charlie@example.com".to_string()),
            SqlValue::Integer(25),
            SqlValue::Varchar("Boston".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("diana@example.com".to_string()),
            SqlValue::Integer(35),
            SqlValue::Varchar("Chicago".to_string()),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_index_scan_with_email_index() {
    let mut db = create_test_db();

    // Create index on email column
    db.create_index(
        "idx_users_email".to_string(),
        "users".to_string(),
        false, // not unique
        vec![IndexColumn {
            column_name: "email".to_string(),
                prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query that should use the email index
    let query = "SELECT * FROM users WHERE email = 'alice@example.com'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return exactly 1 row
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].values[0], SqlValue::Integer(1));
        assert_eq!(result[0].values[1], SqlValue::Varchar("alice@example.com".to_string()));
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_scan_with_age_index() {
    let mut db = create_test_db();

    // Create index on age column
    db.create_index(
        "idx_users_age".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "age".to_string(),
                prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query that should use the age index
    let query = "SELECT id, email FROM users WHERE age = 25";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return 2 rows (Alice and Charlie, both age 25)
        assert_eq!(result.len(), 2);

        // Verify we got the correct users
        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&1)); // Alice
        assert!(ids.contains(&3)); // Charlie
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_table_scan_without_index() {
    let db = create_test_db();
    // Note: No index created, should fall back to table scan

    let executor = SelectExecutor::new(&db);

    // Query without any index available
    let query = "SELECT * FROM users WHERE city = 'Boston'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should still work correctly with table scan
        assert_eq!(result.len(), 2); // Alice and Charlie in Boston
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_scan_with_comparison_operator() {
    let mut db = create_test_db();

    // Create index on age column
    db.create_index(
        "idx_users_age".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "age".to_string(),
                prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with comparison operator (should still use index scan path)
    let query = "SELECT id FROM users WHERE age > 28";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return Bob (30) and Diana (35)
        assert_eq!(result.len(), 2);

        let ids: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(id) => *id,
                _ => panic!("Expected integer ID"),
            })
            .collect();

        assert!(ids.contains(&2)); // Bob (age 30)
        assert!(ids.contains(&4)); // Diana (age 35)
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_index_scan_with_and_condition() {
    let mut db = create_test_db();

    // Create index on age column
    db.create_index(
        "idx_users_age".to_string(),
        "users".to_string(),
        false,
        vec![IndexColumn {
            column_name: "age".to_string(),
                prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Query with AND condition (index on age, regular filter on city)
    let query = "SELECT * FROM users WHERE age = 25 AND city = 'Boston'";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Should return 2 rows (Alice and Charlie are both 25 and in Boston)
        assert_eq!(result.len(), 2);
    } else {
        panic!("Expected SELECT statement");
    }
}

#[test]
fn test_unique_index_enforcement() {
    let mut db = create_test_db();

    // Create unique index on email
    let result = db.create_index(
        "idx_users_email_unique".to_string(),
        "users".to_string(),
        true, // unique
        vec![IndexColumn {
            column_name: "email".to_string(),
                prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    );

    // Should succeed initially
    assert!(result.is_ok());

    // Now try to insert a duplicate email
    // The implementation now enforces uniqueness on user-defined indexes
    let duplicate_result = db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("alice@example.com".to_string()), // duplicate
            SqlValue::Integer(40),
            SqlValue::Varchar("Seattle".to_string()),
        ]),
    );

    // Should reject duplicates in unique indexes
    assert!(duplicate_result.is_err(), "Unique indexes should prevent duplicate values");
}

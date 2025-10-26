//! End-to-End Integration Tests
//!
//! These tests exercise the full SQL database pipeline:
//! 1. Parse SQL string → AST
//! 2. Execute AST → Results
//! 3. Verify results match expectations

use catalog::ColumnSchema;
use catalog::TableSchema;
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

// ============================================================================
// Helper Functions
// ============================================================================

/// Execute a SELECT query end-to-end: parse SQL → execute → return results
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    // Parse SQL
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    // Extract SELECT statement
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    // Execute
    let executor = SelectExecutor::new(db);
    executor
        .execute(&select_stmt)
        .map_err(|e| format!("Execution error: {:?}", e))
}

/// Create a simple users table schema
fn create_users_schema() -> TableSchema {
    TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 100 }, true),
            ColumnSchema::new("age".to_string(), DataType::Integer, false),
        ],
    )
}

/// Insert sample users data
fn insert_sample_users(db: &mut Database) {
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(17),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(30),
        ]),
    )
    .unwrap();

    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Diana".to_string()),
            SqlValue::Integer(22),
        ]),
    )
    .unwrap();
}

// ============================================================================
// End-to-End Tests
// ============================================================================

#[test]
fn test_e2e_select_star() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT * FROM users
    let results = execute_select(&db, "SELECT * FROM users").unwrap();

    // Verify
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[0].values[1], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[2], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_with_where() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT * FROM users WHERE age > 17
    let results = execute_select(&db, "SELECT * FROM users WHERE age > 17").unwrap();

    // Verify - should get Alice (25), Charlie (30), Diana (22), but NOT Bob (17)
    assert_eq!(results.len(), 3);

    // Check that all results have age >= 18
    for row in &results {
        if let SqlValue::Integer(age) = row.values[2] {
            assert!(age >= 18, "Found user with age {} which is < 18", age);
        }
    }

    // Verify specific users are included
    let names: Vec<String> = results
        .iter()
        .map(|r| match &r.values[1] {
            SqlValue::Varchar(s) => s.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    assert!(names.contains(&"Diana".to_string()));
    assert!(!names.contains(&"Bob".to_string()));
}

#[test]
fn test_e2e_select_specific_columns() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name, age FROM users
    let results = execute_select(&db, "SELECT name, age FROM users").unwrap();

    // Verify
    assert_eq!(results.len(), 4);

    // Each row should have only 2 columns (name, age)
    for row in &results {
        assert_eq!(row.values.len(), 2);
    }

    // Check first row
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[1], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_with_complex_where() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name FROM users WHERE age > 20 AND age < 30
    let results = execute_select(&db, "SELECT name FROM users WHERE age > 20 AND age < 30").unwrap();

    // Verify - should get Alice (25) and Diana (22), but NOT Bob (17) or Charlie (30)
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    assert!(names.contains(&"Alice".to_string()));
    assert!(names.contains(&"Diana".to_string()));
}

#[test]
fn test_e2e_select_with_arithmetic() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name, age + 10 FROM users WHERE id = 1
    let results = execute_select(&db, "SELECT name, age + 10 FROM users WHERE id = 1").unwrap();

    // Verify
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[1], SqlValue::Integer(35)); // 25 + 10
}

#[test]
fn test_e2e_select_with_or() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name FROM users WHERE age < 20 OR age > 28
    let results = execute_select(&db, "SELECT name FROM users WHERE age < 20 OR age > 28").unwrap();

    // Verify - should get Bob (17) and Charlie (30)
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn test_e2e_empty_result() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT * FROM users WHERE age > 100
    let results = execute_select(&db, "SELECT * FROM users WHERE age > 100").unwrap();

    // Verify - should get no results
    assert_eq!(results.len(), 0);
}

#[test]
fn test_e2e_with_diagnostic_dump() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute query
    let results = execute_select(&db, "SELECT * FROM users WHERE age > 17").unwrap();

    // Use diagnostic tools to verify
    let debug_info = db.debug_info();
    assert!(debug_info.contains("Tables: 1"));
    assert!(debug_info.contains("users"));
    assert!(debug_info.contains("4 rows"));

    // Dump table contents
    let dump = db.dump_table("users").unwrap();
    assert!(dump.contains("Alice"));
    assert!(dump.contains("Bob"));
    assert!(dump.contains("Charlie"));
    assert!(dump.contains("Diana"));
    assert!(dump.contains("(4 rows)"));

    // Verify query results
    assert_eq!(results.len(), 3); // Alice, Charlie, Diana (all age >= 18)
}

#[test]
fn test_e2e_string_comparison() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT * FROM users WHERE name = 'Alice'
    let results = execute_select(&db, "SELECT * FROM users WHERE name = 'Alice'").unwrap();

    // Verify
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[1], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}

#[test]
fn test_e2e_multiple_tables() {
    // Setup database with multiple tables
    let mut db = Database::new();

    // Create users table
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Create products table
    let products_schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 100 }, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(products_schema).unwrap();

    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Widget".to_string()),
            SqlValue::Integer(10),
        ]),
    )
    .unwrap();

    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Gadget".to_string()),
            SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    // Query users table
    let user_results = execute_select(&db, "SELECT * FROM users WHERE age > 20").unwrap();
    assert_eq!(user_results.len(), 3); // Alice, Charlie, Diana

    // Query products table
    let product_results = execute_select(&db, "SELECT name FROM products WHERE price < 15").unwrap();
    assert_eq!(product_results.len(), 1);
    assert_eq!(
        product_results[0].values[0],
        SqlValue::Varchar("Widget".to_string())
    );

    // Verify diagnostic info shows both tables
    let debug = db.debug_info();
    assert!(debug.contains("Tables: 2"));
    assert!(debug.contains("users"));
    assert!(debug.contains("products"));
}

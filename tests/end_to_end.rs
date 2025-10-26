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

    // Execute: SELECT * FROM users WHERE age >= 18
    let results = execute_select(&db, "SELECT * FROM users WHERE age >= 18").unwrap();

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
    let results = execute_select(&db, "SELECT * FROM users WHERE age >= 18").unwrap();

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

// ============================================================================
// ORDER BY Tests
// ============================================================================

#[test]
fn test_e2e_order_by_asc() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name, age FROM users ORDER BY age ASC
    let results = execute_select(&db, "SELECT name, age FROM users ORDER BY age ASC").unwrap();

    // Verify - should be sorted by age ascending: 17, 22, 25, 30
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].values[1], SqlValue::Integer(17)); // Bob
    assert_eq!(results[1].values[1], SqlValue::Integer(22)); // Diana
    assert_eq!(results[2].values[1], SqlValue::Integer(25)); // Alice
    assert_eq!(results[3].values[1], SqlValue::Integer(30)); // Charlie

    // Verify names are in correct order
    assert_eq!(results[0].values[0], SqlValue::Varchar("Bob".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Diana".to_string()));
    assert_eq!(results[2].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[3].values[0], SqlValue::Varchar("Charlie".to_string()));
}

#[test]
fn test_e2e_order_by_desc() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name, age FROM users ORDER BY age DESC
    let results = execute_select(&db, "SELECT name, age FROM users ORDER BY age DESC").unwrap();

    // Verify - should be sorted by age descending: 30, 25, 22, 17
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].values[1], SqlValue::Integer(30)); // Charlie
    assert_eq!(results[1].values[1], SqlValue::Integer(25)); // Alice
    assert_eq!(results[2].values[1], SqlValue::Integer(22)); // Diana
    assert_eq!(results[3].values[1], SqlValue::Integer(17)); // Bob
}

#[test]
fn test_e2e_order_by_string() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name FROM users ORDER BY name ASC
    let results = execute_select(&db, "SELECT name FROM users ORDER BY name ASC").unwrap();

    // Verify - should be sorted alphabetically: Alice, Bob, Charlie, Diana
    assert_eq!(results.len(), 4);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Bob".to_string()));
    assert_eq!(results[2].values[0], SqlValue::Varchar("Charlie".to_string()));
    assert_eq!(results[3].values[0], SqlValue::Varchar("Diana".to_string()));
}

#[test]
fn test_e2e_order_by_with_where() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name, age FROM users WHERE age >= 20 ORDER BY age ASC
    let results = execute_select(&db, "SELECT name, age FROM users WHERE age >= 20 ORDER BY age ASC")
        .unwrap();

    // Verify - should have 3 users (Diana 22, Alice 25, Charlie 30)
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[1], SqlValue::Integer(22)); // Diana
    assert_eq!(results[1].values[1], SqlValue::Integer(25)); // Alice
    assert_eq!(results[2].values[1], SqlValue::Integer(30)); // Charlie
}

// ============================================================================
// Multi-Character Operator Tests
// ============================================================================

#[test]
fn test_e2e_less_than_or_equal() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name FROM users WHERE age <= 22
    let results = execute_select(&db, "SELECT name FROM users WHERE age <= 22").unwrap();

    // Verify - should get Bob (17) and Diana (22), but NOT Alice (25) or Charlie (30)
    assert_eq!(results.len(), 2);

    let names: Vec<String> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Diana".to_string()));
    assert!(!names.contains(&"Alice".to_string()));
    assert!(!names.contains(&"Charlie".to_string()));
}

#[test]
fn test_e2e_not_equal_bang_equal() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name FROM users WHERE age != 25
    let results = execute_select(&db, "SELECT name FROM users WHERE age != 25").unwrap();

    // Verify - should get everyone EXCEPT Alice (25)
    assert_eq!(results.len(), 3);

    let names: Vec<String> = results
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Varchar(s) => s.clone(),
            _ => panic!("Expected varchar"),
        })
        .collect();

    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
    assert!(names.contains(&"Diana".to_string()));
    assert!(!names.contains(&"Alice".to_string()));
}

#[test]
fn test_e2e_not_equal_angle_brackets() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name FROM users WHERE name <> 'Bob'
    let results = execute_select(&db, "SELECT name FROM users WHERE name <> 'Bob'").unwrap();

    // Verify - should get everyone EXCEPT Bob
    assert_eq!(results.len(), 3);

    let names: Vec<String> = results
        .iter()
        .map(|r| match &r.values[0] {
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
fn test_e2e_combined_comparison_operators() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Execute: SELECT name FROM users WHERE age >= 18 AND age <= 25
    let results = execute_select(&db, "SELECT name FROM users WHERE age >= 18 AND age <= 25").unwrap();

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
fn test_e2e_all_comparison_operators() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Test each operator individually
    // = (equal)
    let eq_results = execute_select(&db, "SELECT name FROM users WHERE age = 25").unwrap();
    assert_eq!(eq_results.len(), 1);

    // < (less than)
    let lt_results = execute_select(&db, "SELECT name FROM users WHERE age < 20").unwrap();
    assert_eq!(lt_results.len(), 1); // Bob

    // > (greater than)
    let gt_results = execute_select(&db, "SELECT name FROM users WHERE age > 25").unwrap();
    assert_eq!(gt_results.len(), 1); // Charlie

    // <= (less than or equal)
    let lte_results = execute_select(&db, "SELECT name FROM users WHERE age <= 22").unwrap();
    assert_eq!(lte_results.len(), 2); // Bob, Diana

    // >= (greater than or equal)
    let gte_results = execute_select(&db, "SELECT name FROM users WHERE age >= 25").unwrap();
    assert_eq!(gte_results.len(), 2); // Alice, Charlie

    // != (not equal)
    let ne_results = execute_select(&db, "SELECT name FROM users WHERE age != 25").unwrap();
    assert_eq!(ne_results.len(), 3); // Bob, Charlie, Diana
}

#[test]
fn test_e2e_operators_without_spaces() {
    // Setup database
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    // Test operators work without spaces around them
    // age>=18 (no spaces)
    let results1 = execute_select(&db, "SELECT name FROM users WHERE age>=18").unwrap();
    assert_eq!(results1.len(), 3);

    // age<=25 (no spaces)
    let results2 = execute_select(&db, "SELECT name FROM users WHERE age<=25").unwrap();
    assert_eq!(results2.len(), 3);

    // age!=17 (no spaces)
    let results3 = execute_select(&db, "SELECT name FROM users WHERE age!=17").unwrap();
    assert_eq!(results3.len(), 3);
}

// ============================================================================
// Aggregate Function Tests
// ============================================================================

#[test]
fn test_e2e_count_star() {
    // Test: SELECT COUNT(*) FROM users
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT COUNT(*) FROM users").unwrap();
    assert_eq!(results.len(), 1); // One row for aggregate
    assert_eq!(results[0].values.len(), 1); // One column
    assert_eq!(results[0].values[0], SqlValue::Integer(4)); // 4 users
}

#[test]
fn test_e2e_sum_aggregate() {
    // Test: SELECT SUM(age) FROM users
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT SUM(age) FROM users").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(94)); // 25 + 17 + 30 + 22 = 94
}

#[test]
fn test_e2e_avg_aggregate() {
    // Test: SELECT AVG(age) FROM users
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT AVG(age) FROM users").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(23)); // 94 / 4 = 23 (integer division)
}

#[test]
fn test_e2e_min_aggregate() {
    // Test: SELECT MIN(age) FROM users
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT MIN(age) FROM users").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(17)); // Bob's age
}

#[test]
fn test_e2e_max_aggregate() {
    // Test: SELECT MAX(age) FROM users
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT MAX(age) FROM users").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(30)); // Charlie's age
}

#[test]
fn test_e2e_multiple_aggregates() {
    // Test: SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(
        &db,
        "SELECT COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age) FROM users",
    )
    .unwrap();
    assert_eq!(results.len(), 1); // One row for aggregates
    assert_eq!(results[0].values.len(), 5); // Five aggregate columns
    assert_eq!(results[0].values[0], SqlValue::Integer(4)); // COUNT(*)
    assert_eq!(results[0].values[1], SqlValue::Integer(94)); // SUM(age)
    assert_eq!(results[0].values[2], SqlValue::Integer(23)); // AVG(age)
    assert_eq!(results[0].values[3], SqlValue::Integer(17)); // MIN(age)
    assert_eq!(results[0].values[4], SqlValue::Integer(30)); // MAX(age)
}

#[test]
fn test_e2e_aggregate_with_where() {
    // Test: SELECT COUNT(*), AVG(age) FROM users WHERE age >= 18
    let mut db = Database::new();
    db.create_table(create_users_schema()).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT COUNT(*), AVG(age) FROM users WHERE age >= 18")
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Integer(3)); // 3 users >= 18 (Alice, Charlie, Diana)
    assert_eq!(results[0].values[1], SqlValue::Integer(25)); // (25 + 30 + 22) / 3 = 25.67 → 25
}

//! End-to-end integration tests for the SQL engine.
//!
//! These tests exercise the full pipeline: parse SQL → execute → verify results.

use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

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

fn insert_sample_users(db: &mut Database) {
    let rows = vec![
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(25),
        ]),
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(17),
        ]),
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(30),
        ]),
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Diana".to_string()),
            SqlValue::Integer(22),
        ]),
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Eve".to_string()),
            SqlValue::Integer(35),
        ]),
    ];

    for row in rows {
        db.insert_row("users", row).unwrap();
    }
}

// ========================================================================
// Basic Query Tests
// ========================================================================

#[test]
fn test_e2e_select_star() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT * FROM users").unwrap();
    assert_eq!(results.len(), 5);

    // Verify first row
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[0].values[1], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[2], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_specific_columns() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT name, age FROM users").unwrap();
    assert_eq!(results.len(), 5);

    // Verify structure: should have 2 columns (name, age)
    assert_eq!(results[0].values.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[0].values[1], SqlValue::Integer(25));
}

#[test]
fn test_e2e_select_with_where() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results = execute_select(&db, "SELECT name FROM users WHERE age > 25").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Charlie".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Eve".to_string()));
}

#[test]
fn test_e2e_select_with_complex_where() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM users WHERE age > 20 AND age < 30").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Diana".to_string()));
}

// ========================================================================
// Advanced Query Features
// ========================================================================

#[test]
fn test_e2e_distinct() {
    // Create table with duplicate ages
    let schema = TableSchema::new(
        "people".to_string(),
        vec![
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
            ColumnSchema::new("age".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert rows with duplicate ages
    db.insert_row(
        "people",
        Row::new(vec![SqlValue::Varchar("Alice".to_string()), SqlValue::Integer(25)]),
    )
    .unwrap();
    db.insert_row(
        "people",
        Row::new(vec![SqlValue::Varchar("Bob".to_string()), SqlValue::Integer(30)]),
    )
    .unwrap();
    db.insert_row(
        "people",
        Row::new(vec![SqlValue::Varchar("Charlie".to_string()), SqlValue::Integer(25)]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT DISTINCT age FROM people").unwrap();
    assert_eq!(results.len(), 2, "DISTINCT should return 2 unique ages");
    assert_eq!(results[0].values[0], SqlValue::Integer(25));
    assert_eq!(results[1].values[0], SqlValue::Integer(30));
}

#[test]
fn test_e2e_group_by_count() {
    let schema = TableSchema::new(
        "sales".to_string(),
        vec![
            ColumnSchema::new("product".to_string(), DataType::Varchar { max_length: 50 }, false),
            ColumnSchema::new("quantity".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "sales",
        Row::new(vec![SqlValue::Varchar("Apple".to_string()), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        Row::new(vec![SqlValue::Varchar("Banana".to_string()), SqlValue::Integer(5)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        Row::new(vec![SqlValue::Varchar("Apple".to_string()), SqlValue::Integer(15)]),
    )
    .unwrap();

    let results =
        execute_select(&db, "SELECT product, COUNT(*) FROM sales GROUP BY product").unwrap();
    assert_eq!(results.len(), 2);

    // Find Apple row
    let apple_row = results.iter().find(|r| r.values[0] == SqlValue::Varchar("Apple".to_string()));
    assert!(apple_row.is_some());
    assert_eq!(apple_row.unwrap().values[1], SqlValue::Integer(2));

    // Find Banana row
    let banana_row =
        results.iter().find(|r| r.values[0] == SqlValue::Varchar("Banana".to_string()));
    assert!(banana_row.is_some());
    assert_eq!(banana_row.unwrap().values[1], SqlValue::Integer(1));
}

#[test]
fn test_e2e_limit_offset() {
    let schema = create_users_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_users(&mut db);

    // Test LIMIT
    let results = execute_select(&db, "SELECT name FROM users LIMIT 2").unwrap();
    assert_eq!(results.len(), 2);

    // Test LIMIT with OFFSET
    let results = execute_select(&db, "SELECT name FROM users LIMIT 2 OFFSET 2").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Charlie".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Diana".to_string()));
}

// ========================================================================
// Phase 2: Numeric Type Tests
// ========================================================================

#[test]
fn test_e2e_smallint_type() {
    let schema = TableSchema::new(
        "numbers".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("small_val".to_string(), DataType::Smallint, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert SMALLINT values
    db.insert_row("numbers", Row::new(vec![SqlValue::Integer(1), SqlValue::Smallint(100)]))
        .unwrap();
    db.insert_row("numbers", Row::new(vec![SqlValue::Integer(2), SqlValue::Smallint(-50)]))
        .unwrap();

    let results = execute_select(&db, "SELECT small_val FROM numbers").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Smallint(100));
    assert_eq!(results[1].values[0], SqlValue::Smallint(-50));
}

#[test]
fn test_e2e_bigint_type() {
    let schema = TableSchema::new(
        "numbers".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("big_val".to_string(), DataType::Bigint, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert BIGINT values
    db.insert_row(
        "numbers",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Bigint(9_223_372_036_854_775_807)]),
    )
    .unwrap();
    db.insert_row(
        "numbers",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Bigint(-9_223_372_036_854_775_808)]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT big_val FROM numbers").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Bigint(9_223_372_036_854_775_807));
    assert_eq!(results[1].values[0], SqlValue::Bigint(-9_223_372_036_854_775_808));
}

#[test]
fn test_e2e_float_type() {
    let schema = TableSchema::new(
        "measurements".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Float, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("measurements", Row::new(vec![SqlValue::Integer(1), SqlValue::Float(3.14)]))
        .unwrap();
    db.insert_row("measurements", Row::new(vec![SqlValue::Integer(2), SqlValue::Float(-2.71)]))
        .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Float(3.14));
    assert_eq!(results[1].values[0], SqlValue::Float(-2.71));
}

#[test]
fn test_e2e_real_type() {
    let schema = TableSchema::new(
        "measurements".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Real, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("measurements", Row::new(vec![SqlValue::Integer(1), SqlValue::Real(1.23)]))
        .unwrap();
    db.insert_row("measurements", Row::new(vec![SqlValue::Integer(2), SqlValue::Real(4.56)]))
        .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Real(1.23));
    assert_eq!(results[1].values[0], SqlValue::Real(4.56));
}

#[test]
fn test_e2e_double_precision_type() {
    let schema = TableSchema::new(
        "measurements".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::DoublePrecision, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "measurements",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Double(3.141592653589793)]),
    )
    .unwrap();
    db.insert_row(
        "measurements",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Double(2.718281828459045)]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Double(3.141592653589793));
    assert_eq!(results[1].values[0], SqlValue::Double(2.718281828459045));
}

#[test]
fn test_e2e_numeric_type() {
    let schema = TableSchema::new(
        "financials".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "amount".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "financials",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Numeric("123.45".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "financials",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Numeric("999.99".to_string())]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT amount FROM financials").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Numeric("123.45".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Numeric("999.99".to_string()));
}

#[test]
fn test_e2e_decimal_type() {
    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "price".to_string(),
                DataType::Decimal { precision: 8, scale: 2 },
                false,
            ),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "products",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Numeric("19.99".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Numeric("49.95".to_string())]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT price FROM products").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Numeric("19.99".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Numeric("49.95".to_string()));
}

#[test]
fn test_e2e_all_numeric_types_together() {
    let schema = TableSchema::new(
        "all_numbers".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("s".to_string(), DataType::Smallint, false),
            ColumnSchema::new("b".to_string(), DataType::Bigint, false),
            ColumnSchema::new("f".to_string(), DataType::Float, false),
            ColumnSchema::new("r".to_string(), DataType::Real, false),
            ColumnSchema::new("d".to_string(), DataType::DoublePrecision, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "all_numbers",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Smallint(100),
            SqlValue::Bigint(1000000),
            SqlValue::Float(3.14),
            SqlValue::Real(2.71),
            SqlValue::Double(1.41),
        ]),
    )
    .unwrap();

    let results = execute_select(&db, "SELECT * FROM all_numbers").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values.len(), 6);
}

// Test cross-type numeric comparisons
#[test]
fn test_e2e_numeric_comparison() {
    let schema = TableSchema::new(
        "numbers".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("small_val".to_string(), DataType::Smallint, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "numbers",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Smallint(10)]),
    )
    .unwrap();
    db.insert_row(
        "numbers",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Smallint(20)]),
    )
    .unwrap();
    db.insert_row(
        "numbers",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Smallint(30)]),
    )
    .unwrap();

    // Test comparison with new types
    let results =
        execute_select(&db, "SELECT small_val FROM numbers WHERE small_val >= 20").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Smallint(20));
    assert_eq!(results[1].values[0], SqlValue::Smallint(30));
}

// ========================================================================
// Phase 2: CHAR Type Test
// ========================================================================

#[test]
fn test_e2e_char_type() {
    // Test CHAR fixed-length type with space padding behavior
    let schema = TableSchema::new(
        "codes".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("code".to_string(), DataType::Character { length: 5 }, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Character { length: 10 },
                false,
            ),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert CHAR values - should pad short strings with spaces
    db.insert_row(
        "codes",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Character("ABC".to_string()),   // Will be padded to "ABC  " (5 chars)
            SqlValue::Character("Hello".to_string()), // Will be padded to "Hello     " (10 chars)
        ]),
    )
    .unwrap();

    db.insert_row(
        "codes",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Character("12345".to_string()), // Exact length (5 chars)
            SqlValue::Character("World".to_string()), // Will be padded to "World     " (10 chars)
        ]),
    )
    .unwrap();

    db.insert_row(
        "codes",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Character("TOOLONG".to_string()), // Will be truncated to "TOOLO" (5 chars)
            SqlValue::Character("VeryLongName".to_string()), // Will be truncated to "VeryLongNa" (10 chars)
        ]),
    )
    .unwrap();

    // SELECT all rows
    let results = execute_select(&db, "SELECT id, code, name FROM codes").unwrap();
    assert_eq!(results.len(), 3);

    // Row 1: Check padding
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[0].values[1], SqlValue::Character("ABC  ".to_string()));
    assert_eq!(results[0].values[2], SqlValue::Character("Hello     ".to_string()));

    // Row 2: Check exact length
    assert_eq!(results[1].values[0], SqlValue::Integer(2));
    assert_eq!(results[1].values[1], SqlValue::Character("12345".to_string()));
    assert_eq!(results[1].values[2], SqlValue::Character("World     ".to_string()));

    // Row 3: Check truncation
    assert_eq!(results[2].values[0], SqlValue::Integer(3));
    assert_eq!(results[2].values[1], SqlValue::Character("TOOLO".to_string()));
    assert_eq!(results[2].values[2], SqlValue::Character("VeryLongNa".to_string()));

    // Test WHERE clause with CHAR comparison
    let results = execute_select(&db, "SELECT id FROM codes WHERE code = 'ABC  '").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}

// ========================================================================
// Phase 3: LIKE Pattern Matching Test
// ========================================================================

#[test]
fn test_e2e_like_pattern_matching() {
    // Test LIKE pattern matching with wildcards
    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 100 }, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "products",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Widget Pro".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Gadget Plus".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("Widget Mini".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar("Super Gadget".to_string())]),
    )
    .unwrap();

    // Test LIKE with 'starts with' pattern (Widget%)
    let results = execute_select(&db, "SELECT id FROM products WHERE name LIKE 'Widget%'").unwrap();
    assert_eq!(results.len(), 2, "Should match 'Widget%'");
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[1].values[0], SqlValue::Integer(3));

    // Test LIKE with 'ends with' pattern (%Plus)
    let results = execute_select(&db, "SELECT id FROM products WHERE name LIKE '%Plus'").unwrap();
    assert_eq!(results.len(), 1, "Should match '%Plus'");
    assert_eq!(results[0].values[0], SqlValue::Integer(2));

    // Test LIKE with 'contains' pattern (%Gadget%)
    let results = execute_select(&db, "SELECT id FROM products WHERE name LIKE '%Gadget%'").unwrap();
    assert_eq!(results.len(), 2, "Should match '%Gadget%'");
    assert_eq!(results[0].values[0], SqlValue::Integer(2));
    assert_eq!(results[1].values[0], SqlValue::Integer(4));

    // Test NOT LIKE
    let results = execute_select(&db, "SELECT id FROM products WHERE name NOT LIKE '%Gadget%'").unwrap();
    assert_eq!(results.len(), 2, "Should NOT match '%Gadget%'");
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[1].values[0], SqlValue::Integer(3));

    // Test LIKE with underscore wildcard (Widget ____)
    // 'Widget ____' = 11 chars, matches "Widget Mini" (11 chars) but not "Widget Pro" (10 chars)
    let results = execute_select(&db, "SELECT id FROM products WHERE name LIKE 'Widget ____'").unwrap();
    assert_eq!(results.len(), 1, "Should match 'Widget ____' (only Mini)");
    assert_eq!(results[0].values[0], SqlValue::Integer(3));

    // Test exact match
    let results = execute_select(&db, "SELECT id FROM products WHERE name LIKE 'Widget Pro'").unwrap();
    assert_eq!(results.len(), 1, "Should match exact 'Widget Pro'");
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}

// ========================================================================
// Phase 3: IN List Predicate Test
// ========================================================================

#[test]
fn test_e2e_in_list_predicate() {
    // Test IN predicate with value lists
    let schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
            ColumnSchema::new("category".to_string(), DataType::Varchar { max_length: 20 }, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Widget".to_string()),
            SqlValue::Varchar("electronics".to_string()),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Gadget".to_string()),
            SqlValue::Varchar("electronics".to_string()),
            SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Tool".to_string()),
            SqlValue::Varchar("hardware".to_string()),
            SqlValue::Integer(50),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Device".to_string()),
            SqlValue::Varchar("electronics".to_string()),
            SqlValue::Integer(300),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Hammer".to_string()),
            SqlValue::Varchar("hardware".to_string()),
            SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    // Test 1: IN with integer list
    let results = execute_select(&db, "SELECT name FROM products WHERE id IN (1, 3, 5)").unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Widget".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Tool".to_string()));
    assert_eq!(results[2].values[0], SqlValue::Varchar("Hammer".to_string()));

    // Test 2: IN with string list
    let results = execute_select(&db, "SELECT name FROM products WHERE category IN ('hardware')").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Tool".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Hammer".to_string()));

    // Test 3: NOT IN
    let results = execute_select(&db, "SELECT name FROM products WHERE id NOT IN (2, 4)").unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Widget".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Tool".to_string()));
    assert_eq!(results[2].values[0], SqlValue::Varchar("Hammer".to_string()));

    // Test 4: IN with single value
    let results = execute_select(&db, "SELECT name FROM products WHERE id IN (2)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Gadget".to_string()));

    // Test 5: IN with expressions
    let results = execute_select(&db, "SELECT name FROM products WHERE price IN (50, 100 + 100)").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Gadget".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Tool".to_string()));

    // Test 6: IN combined with AND
    let results = execute_select(&db, "SELECT name FROM products WHERE category IN ('electronics') AND price > 150").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Gadget".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Device".to_string()));

    // Test 7: Empty result
    let results = execute_select(&db, "SELECT name FROM products WHERE id IN (99, 100)").unwrap();
    assert_eq!(results.len(), 0);
}

// ========================================================================
// Phase 3: EXISTS Predicate Test
// ========================================================================

#[test]
fn test_e2e_exists_predicate() {
    // Test EXISTS predicate with correlated and non-correlated subqueries
    let customers_schema = TableSchema::new(
        "customers".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
        ],
    );

    let orders_schema = TableSchema::new(
        "orders".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("total".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(customers_schema).unwrap();
    db.create_table(orders_schema).unwrap();

    // Insert customers
    db.insert_row(
        "customers",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "customers",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "customers",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".to_string())]),
    )
    .unwrap();

    // Insert orders (only for customer 1 and 2)
    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(1), SqlValue::Integer(200)]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(2), SqlValue::Integer(150)]),
    )
    .unwrap();

    // Test 1: Simple EXISTS - check if there are any orders
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders)",
    )
    .unwrap();
    assert_eq!(results.len(), 3, "All customers should be returned when orders exist");

    // Test 2: NOT EXISTS with empty table - all customers should match
    let empty_schema = TableSchema::new(
        "empty_table".to_string(),
        vec![ColumnSchema::new("dummy".to_string(), DataType::Integer, false)],
    );
    db.create_table(empty_schema).unwrap();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM empty_table)",
    )
    .unwrap();
    assert_eq!(results.len(), 3, "All customers should match when NOT EXISTS on empty table");

    // Test 3: EXISTS with WHERE condition in subquery
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE total > 150)",
    )
    .unwrap();
    assert_eq!(results.len(), 3, "All customers returned when orders with total > 150 exist");

    // Test 4: NOT EXISTS with matching rows
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE total > 150)",
    )
    .unwrap();
    assert_eq!(results.len(), 0, "No customers when orders with total > 150 exist");

    // Test 5: EXISTS combined with AND
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE id > 1 AND EXISTS (SELECT 1 FROM orders)",
    )
    .unwrap();
    assert_eq!(results.len(), 2, "Should find customers with id > 1 when orders exist");
    assert_eq!(results[0].values[0], SqlValue::Varchar("Bob".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Charlie".to_string()));

    // Test 6: EXISTS combined with OR
    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE id = 3 OR EXISTS (SELECT 1 FROM orders)",
    )
    .unwrap();
    assert_eq!(results.len(), 3, "Should find all customers (either id=3 or orders exist)");

    // Note: Correlated subqueries with table aliases (e.g., WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id))
    // are not yet fully supported and will be added in a future update.
}

// ========================================================================
// Phase 3: COALESCE and NULLIF Functions Test
// ========================================================================

#[test]
fn test_e2e_coalesce_and_nullif() {
    // Test COALESCE and NULLIF scalar functions
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, false),
            ColumnSchema::new("nickname".to_string(), DataType::Varchar { max_length: 50 }, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data with some NULL values
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Ally".to_string()),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Null, // NULL nickname
            SqlValue::Integer(0),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Varchar("Chuck".to_string()),
            SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    // Test 1: COALESCE with non-NULL value
    let results = execute_select(&db, "SELECT COALESCE(nickname, 'Unknown') FROM users WHERE id = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Ally".to_string()));

    // Test 2: COALESCE with NULL value - returns second argument
    let results = execute_select(&db, "SELECT COALESCE(nickname, 'Unknown') FROM users WHERE id = 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Unknown".to_string()));

    // Test 3: COALESCE with multiple arguments
    let results = execute_select(&db, "SELECT COALESCE(nickname, name, 'Default') FROM users WHERE id = 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Bob".to_string()), "Should return name when nickname is NULL");

    // Test 4: COALESCE all NULL - returns NULL
    let results = execute_select(&db, "SELECT COALESCE(NULL, NULL, NULL) FROM users WHERE id = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);

    // Test 5: NULLIF when values are equal - returns NULL
    let results = execute_select(&db, "SELECT NULLIF(balance, 0) FROM users WHERE id = 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF should return NULL when values are equal");

    // Test 6: NULLIF when values are not equal - returns first value
    let results = execute_select(&db, "SELECT NULLIF(balance, 0) FROM users WHERE id = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(100), "NULLIF should return first value when not equal");

    // Test 7: NULLIF with NULL input
    let results = execute_select(&db, "SELECT NULLIF(nickname, 'test') FROM users WHERE id = 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF with NULL first arg returns NULL");

    // Test 8: Combined COALESCE and NULLIF
    // Use NULLIF to convert 0 balance to NULL, then COALESCE to provide default
    let results = execute_select(&db, "SELECT COALESCE(NULLIF(balance, 0), 999) FROM users").unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Integer(100)); // Alice: 100 != 0
    assert_eq!(results[1].values[0], SqlValue::Integer(999)); // Bob: 0 becomes NULL, COALESCE to 999
    assert_eq!(results[2].values[0], SqlValue::Integer(200)); // Charlie: 200 != 0

    // Test 9: COALESCE in WHERE clause
    let results = execute_select(&db, "SELECT name FROM users WHERE COALESCE(nickname, name) = 'Bob'").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Bob".to_string()));

    // Test 10: NULLIF with string comparison
    let results = execute_select(&db, "SELECT NULLIF(name, 'Alice') FROM users WHERE id = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF('Alice', 'Alice') should return NULL");
}

#[test]
fn test_e2e_quantified_comparisons() {
    let mut db = Database::new();

    // Create employees table: id, name, salary, dept_id
    let employees_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 100 }, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(employees_schema.clone()).unwrap();

    // Insert test data
    // Department 1: salaries 50000, 60000, 70000
    // Department 2: salaries 80000, 90000
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string()), SqlValue::Integer(50000), SqlValue::Integer(1)])).unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string()), SqlValue::Integer(60000), SqlValue::Integer(1)])).unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("Charlie".to_string()), SqlValue::Integer(70000), SqlValue::Integer(1)])).unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar("David".to_string()), SqlValue::Integer(80000), SqlValue::Integer(2)])).unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(5), SqlValue::Varchar("Eve".to_string()), SqlValue::Integer(90000), SqlValue::Integer(2)])).unwrap();

    // Test 1: > ALL - salary greater than all dept 1 salaries
    // Only David (80000) and Eve (90000) have salary > ALL dept 1 salaries (50000, 60000, 70000)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Varchar("David".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Eve".to_string()));

    // Test 2: < ALL - salary less than all dept 2 salaries
    // Alice (50000), Bob (60000), Charlie (70000) have salary < ALL dept 2 salaries (80000, 90000)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary < ALL (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));

    // Test 3: > ANY - salary greater than at least one dept 1 salary
    // Everyone except Alice (50000 is not > any value in dept 1 starting with 50000)
    // Actually, Bob (60000) is > 50000, Charlie (70000) is > 50000 and 60000, etc.
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ANY (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 4); // Bob, Charlie, David, Eve

    // Test 4: < ANY - salary less than at least one dept 2 salary
    // All dept 1 employees (Alice, Bob, Charlie) are < 90000
    // David (80000) is < 90000
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary < ANY (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 4); // Alice, Bob, Charlie, David

    // Test 5: = ANY - salary equals any dept 1 salary
    // Only dept 1 employees match
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary = ANY (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Bob".to_string()));
    assert_eq!(results[2].values[0], SqlValue::Varchar("Charlie".to_string()));

    // Test 6: SOME is synonym for ANY
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary = SOME (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 3); // Same as = ANY

    // Test 7: Empty subquery with ALL (should return TRUE - vacuously true)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ALL (SELECT salary FROM employees WHERE dept_id = 999)"
    ).unwrap();
    assert_eq!(results.len(), 5); // All employees pass

    // Test 8: Empty subquery with ANY (should return FALSE - no rows to match)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ANY (SELECT salary FROM employees WHERE dept_id = 999)"
    ).unwrap();
    assert_eq!(results.len(), 0); // No employees pass

    // Test 9: Quantified comparison in SELECT list
    let results = execute_select(&db,
        "SELECT name, salary > ALL (SELECT salary FROM employees WHERE dept_id = 1) AS is_highest FROM employees WHERE id = 5"
    ).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Eve".to_string()));
    assert_eq!(results[0].values[1], SqlValue::Boolean(true)); // Eve's 90000 > all dept 1 salaries

    // Test 10: Complex query with AND
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary > ANY (SELECT salary FROM employees WHERE dept_id = 1) AND salary < ALL (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 2); // Bob (60000) and Charlie (70000)
    assert_eq!(results[0].values[0], SqlValue::Varchar("Bob".to_string()));
    assert_eq!(results[1].values[0], SqlValue::Varchar("Charlie".to_string()));

    // Test 11: >= ALL
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary >= ALL (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 3); // Charlie, David, Eve (>= 70000)

    // Test 12: <= ANY
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary <= ANY (SELECT salary FROM employees WHERE dept_id = 2)"
    ).unwrap();
    assert_eq!(results.len(), 5); // All employees (<= 90000)

    // Test 13: != ALL (not equal to all values)
    let results = execute_select(&db,
        "SELECT name FROM employees WHERE salary <> ALL (SELECT salary FROM employees WHERE dept_id = 1)"
    ).unwrap();
    assert_eq!(results.len(), 2); // David and Eve (not in dept 1)
}

//! End-to-end integration tests for SQL data types.
//!
//! Tests numeric types, character types, and basic query features like DISTINCT, GROUP BY, LIMIT.

// Allow approximate constants in tests - these are test data values, not mathematical constants
#![allow(clippy::approx_constant)]

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
        "USERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("AGE".to_string(), DataType::Integer, false),
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
        db.insert_row("USERS", row).unwrap();
    }
}

// ========================================================================
// Basic Query Features
// ========================================================================

#[test]
fn test_e2e_distinct() {
    // Create table with duplicate ages
    let schema = TableSchema::new(
        "PEOPLE".to_string(),
        vec![
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("AGE".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert rows with duplicate ages
    db.insert_row(
        "PEOPLE",
        Row::new(vec![SqlValue::Varchar("Alice".to_string()), SqlValue::Integer(25)]),
    )
    .unwrap();
    db.insert_row(
        "PEOPLE",
        Row::new(vec![SqlValue::Varchar("Bob".to_string()), SqlValue::Integer(30)]),
    )
    .unwrap();
    db.insert_row(
        "PEOPLE",
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
        "SALES".to_string(),
        vec![
            ColumnSchema::new(
                "PRODUCT".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("QUANTITY".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "SALES",
        Row::new(vec![SqlValue::Varchar("Apple".to_string()), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
        Row::new(vec![SqlValue::Varchar("Banana".to_string()), SqlValue::Integer(5)]),
    )
    .unwrap();
    db.insert_row(
        "SALES",
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
// Numeric Type Tests
// ========================================================================

#[test]
fn test_e2e_smallint_type() {
    let schema = TableSchema::new(
        "NUMBERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("SMALL_VAL".to_string(), DataType::Smallint, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert SMALLINT values
    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(1), SqlValue::Smallint(100)]))
        .unwrap();
    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(2), SqlValue::Smallint(-50)]))
        .unwrap();

    let results = execute_select(&db, "SELECT small_val FROM numbers").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Smallint(100));
    assert_eq!(results[1].values[0], SqlValue::Smallint(-50));
}

#[test]
fn test_e2e_bigint_type() {
    let schema = TableSchema::new(
        "NUMBERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("BIG_VAL".to_string(), DataType::Bigint, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert BIGINT values
    db.insert_row(
        "NUMBERS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Bigint(9_223_372_036_854_775_807)]),
    )
    .unwrap();
    db.insert_row(
        "NUMBERS",
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
        "MEASUREMENTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("VALUE".to_string(), DataType::Float { precision: 53 }, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(1), SqlValue::Float(3.14)]))
        .unwrap();
    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(2), SqlValue::Float(-2.71)]))
        .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Float(3.14));
    assert_eq!(results[1].values[0], SqlValue::Float(-2.71));
}

#[test]
fn test_e2e_real_type() {
    let schema = TableSchema::new(
        "MEASUREMENTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("VALUE".to_string(), DataType::Real, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(1), SqlValue::Real(1.23)]))
        .unwrap();
    db.insert_row("MEASUREMENTS", Row::new(vec![SqlValue::Integer(2), SqlValue::Real(4.56)]))
        .unwrap();

    let results = execute_select(&db, "SELECT value FROM measurements").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Real(1.23));
    assert_eq!(results[1].values[0], SqlValue::Real(4.56));
}

#[test]
fn test_e2e_double_precision_type() {
    let schema = TableSchema::new(
        "MEASUREMENTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("VALUE".to_string(), DataType::DoublePrecision, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "MEASUREMENTS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Double(3.141592653589793)]),
    )
    .unwrap();
    db.insert_row(
        "MEASUREMENTS",
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
        "FINANCIALS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "AMOUNT".to_string(),
                DataType::Numeric { precision: 10, scale: 2 },
                false,
            ),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "FINANCIALS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Numeric("123.45".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "FINANCIALS",
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
        "PRODUCTS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "PRICE".to_string(),
                DataType::Decimal { precision: 8, scale: 2 },
                false,
            ),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "PRODUCTS",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Numeric("19.99".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "PRODUCTS",
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
        "ALL_NUMBERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("S".to_string(), DataType::Smallint, false),
            ColumnSchema::new("B".to_string(), DataType::Bigint, false),
            ColumnSchema::new("F".to_string(), DataType::Float { precision: 53 }, false),
            ColumnSchema::new("R".to_string(), DataType::Real, false),
            ColumnSchema::new("D".to_string(), DataType::DoublePrecision, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row(
        "ALL_NUMBERS",
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

#[test]
fn test_e2e_numeric_comparison() {
    let schema = TableSchema::new(
        "NUMBERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("SMALL_VAL".to_string(), DataType::Smallint, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(1), SqlValue::Smallint(10)])).unwrap();
    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(2), SqlValue::Smallint(20)])).unwrap();
    db.insert_row("NUMBERS", Row::new(vec![SqlValue::Integer(3), SqlValue::Smallint(30)])).unwrap();

    // Test comparison with new types
    let results =
        execute_select(&db, "SELECT small_val FROM numbers WHERE small_val >= 20").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Smallint(20));
    assert_eq!(results[1].values[0], SqlValue::Smallint(30));
}

// ========================================================================
// Character Type Tests
// ========================================================================

#[test]
fn test_e2e_char_type() {
    // Test CHAR fixed-length type with space padding behavior
    let schema = TableSchema::new(
        "CODES".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new("CODE".to_string(), DataType::Character { length: 5 }, false),
            ColumnSchema::new("NAME".to_string(), DataType::Character { length: 10 }, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert CHAR values - should pad short strings with spaces
    db.insert_row(
        "CODES",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Character("ABC".to_string()), // Will be padded to "ABC  " (5 chars)
            SqlValue::Character("Hello".to_string()), // Will be padded to "Hello     " (10 chars)
        ]),
    )
    .unwrap();

    db.insert_row(
        "CODES",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Character("12345".to_string()), // Exact length (5 chars)
            SqlValue::Character("World".to_string()), // Will be padded to "World     " (10 chars)
        ]),
    )
    .unwrap();

    db.insert_row(
        "CODES",
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

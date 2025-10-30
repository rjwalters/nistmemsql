//! Integration tests to improve code coverage
//!
//! Target modules:
//! - predicates.rs: LIKE pattern matching
//! - subqueries.rs: EXISTS, ANY, ALL, SOME
//! - conversion.rs: CAST and type conversion functions

use catalog::{ColumnSchema, TableSchema};
use executor::SelectExecutor;
use parser::Parser;
use storage::{Database, Row};
use types::{DataType, SqlValue};

fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// ========================================================================
// LIKE Pattern Matching Tests (predicates.rs)
// ========================================================================

fn create_products_schema() -> TableSchema {
    TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                true,
            ),
            ColumnSchema::new("code".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ],
    )
}

fn insert_sample_products(db: &mut Database) {
    let rows = vec![
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Apple iPhone".to_string()),
            SqlValue::Varchar("APPL-001".to_string()),
        ]),
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Samsung Galaxy".to_string()),
            SqlValue::Varchar("SAMS-002".to_string()),
        ]),
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Apple MacBook".to_string()),
            SqlValue::Varchar("APPL-100".to_string()),
        ]),
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Microsoft Surface".to_string()),
            SqlValue::Varchar("MSFT-050".to_string()),
        ]),
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Apple Watch".to_string()),
            SqlValue::Varchar("APPL-200".to_string()),
        ]),
    ];

    for row in rows {
        db.insert_row("products", row).unwrap();
    }
}

#[test]
fn test_like_percent_wildcard() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM products WHERE name LIKE 'Apple%'").unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_like_underscore_wildcard() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT code FROM products WHERE code LIKE 'APPL-_0_'").unwrap();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_like_combined_wildcards() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results = execute_select(&db, "SELECT name FROM products WHERE name LIKE '%a%'").unwrap();
    assert!(results.len() >= 2);
}

#[test]
fn test_not_like() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM products WHERE name NOT LIKE 'Apple%'").unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_like_null_handling() {
    let schema = TableSchema::new(
        "test_nulls".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "value".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("test_nulls", Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();

    let results =
        execute_select(&db, "SELECT id FROM test_nulls WHERE value LIKE '%test%'").unwrap();
    assert_eq!(results.len(), 0);
}

// ========================================================================
// EXISTS Subquery Tests (subqueries.rs)
// ========================================================================

fn create_orders_schema() -> TableSchema {
    TableSchema::new(
        "orders".to_string(),
        vec![
            ColumnSchema::new("order_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Integer, false),
        ],
    )
}

fn create_customers_schema() -> TableSchema {
    TableSchema::new(
        "customers".to_string(),
        vec![
            ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    )
}

fn setup_customers_orders_db() -> Database {
    let mut db = Database::new();
    db.create_table(create_customers_schema()).unwrap();
    db.create_table(create_orders_schema()).unwrap();

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

    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(101), SqlValue::Integer(1), SqlValue::Integer(100)]),
    )
    .unwrap();

    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(102), SqlValue::Integer(1), SqlValue::Integer(200)]),
    )
    .unwrap();

    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(103), SqlValue::Integer(3), SqlValue::Integer(150)]),
    )
    .unwrap();

    db
}

#[test]
fn test_exists_predicate() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE customer_id = 1)",
    )
    .unwrap();

    assert_eq!(results.len(), 3);
}

#[test]
fn test_not_exists_predicate() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE amount > 10000)",
    )
    .unwrap();

    assert_eq!(results.len(), 3);
}

// ========================================================================
// Quantified Comparison Tests (subqueries.rs)
// ========================================================================

#[test]
fn test_any_quantifier() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id = ANY (SELECT customer_id FROM orders WHERE amount > 150)"
    ).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_all_quantifier() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id <> ALL (SELECT customer_id FROM orders WHERE amount < 150)"
    ).unwrap();

    assert!(!results.is_empty());
}

#[test]
fn test_some_quantifier() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id = SOME (SELECT customer_id FROM orders)",
    )
    .unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_any_with_greater_than() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT order_id FROM orders WHERE amount > ANY (SELECT amount FROM orders WHERE customer_id = 1)"
    ).unwrap();

    assert!(!results.is_empty());
}

#[test]
fn test_all_with_less_than() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT order_id FROM orders WHERE amount < ALL (SELECT amount FROM orders WHERE customer_id = 1)"
    ).unwrap();

    assert_eq!(results.len(), 0);
}

// ========================================================================
// CAST Operations Tests (conversion.rs)
// ========================================================================

fn create_mixed_types_schema() -> TableSchema {
    TableSchema::new(
        "mixed_data".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "text_num".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
            ColumnSchema::new("int_val".to_string(), DataType::Integer, true),
            ColumnSchema::new("float_val".to_string(), DataType::DoublePrecision, true),
        ],
    )
}

fn setup_mixed_types_db() -> Database {
    let mut db = Database::new();
    db.create_table(create_mixed_types_schema()).unwrap();

    db.insert_row(
        "mixed_data",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("123".to_string()),
            SqlValue::Integer(456),
            SqlValue::Double(78.9),
        ]),
    )
    .unwrap();

    db.insert_row(
        "mixed_data",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("999".to_string()),
            SqlValue::Integer(111),
            SqlValue::Double(22.3),
        ]),
    )
    .unwrap();

    db
}

#[test]
fn test_cast_varchar_to_integer() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(text_num AS INTEGER) FROM mixed_data WHERE id = 1")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(123));
}

#[test]
fn test_cast_integer_to_varchar() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS VARCHAR(50)) FROM mixed_data WHERE id = 1")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("456".to_string()));
}

#[test]
fn test_cast_varchar_to_double() {
    let db = setup_mixed_types_db();

    let results = execute_select(
        &db,
        "SELECT CAST(text_num AS DOUBLE PRECISION) FROM mixed_data WHERE id = 1",
    )
    .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Double(123.0));
}

#[test]
fn test_cast_integer_to_double() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS DOUBLE) FROM mixed_data WHERE id = 1").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Double(456.0));
}

#[test]
fn test_cast_to_smallint() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS SMALLINT) FROM mixed_data WHERE id = 2")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Smallint(111));
}

#[test]
fn test_cast_to_bigint() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS BIGINT) FROM mixed_data WHERE id = 1").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Bigint(456));
}

#[test]
fn test_cast_to_float() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT CAST(int_val AS FLOAT) FROM mixed_data WHERE id = 1").unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Float(456.0));
}

// ========================================================================
// Combined Tests - Complex Queries
// ========================================================================

#[test]
fn test_like_with_exists() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE name LIKE 'A%' AND EXISTS (SELECT 1 FROM orders WHERE amount > 100)"
    ).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_cast_in_where_clause() {
    let db = setup_mixed_types_db();

    let results =
        execute_select(&db, "SELECT id FROM mixed_data WHERE CAST(text_num AS INTEGER) > 500")
            .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(2));
}

#[test]
fn test_multiple_predicates_combined() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results = execute_select(
        &db,
        "SELECT name FROM products WHERE name LIKE '%Apple%' AND code NOT LIKE 'APPL-2%'",
    )
    .unwrap();

    assert_eq!(results.len(), 2);
}

#[test]
fn test_quantified_comparison_with_cast() {
    let db = setup_mixed_types_db();

    let results = execute_select(
        &db,
        "SELECT id FROM mixed_data WHERE CAST(text_num AS INTEGER) > ALL (SELECT int_val FROM mixed_data WHERE id > 100)"
    ).unwrap();

    assert_eq!(results.len(), 2);
}

// ========================================================================
// Edge Cases
// ========================================================================

#[test]
fn test_exists_empty_subquery() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE EXISTS (SELECT * FROM orders WHERE amount > 10000)",
    )
    .unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_all_empty_subquery() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id > ALL (SELECT customer_id FROM orders WHERE amount > 10000)"
    ).unwrap();

    assert_eq!(results.len(), 3);
}

#[test]
fn test_any_empty_subquery() {
    let db = setup_customers_orders_db();

    let results = execute_select(
        &db,
        "SELECT name FROM customers WHERE customer_id = ANY (SELECT customer_id FROM orders WHERE amount > 10000)"
    ).unwrap();

    assert_eq!(results.len(), 0);
}

#[test]
fn test_like_exact_match() {
    let schema = create_products_schema();
    let mut db = Database::new();
    db.create_table(schema).unwrap();
    insert_sample_products(&mut db);

    let results =
        execute_select(&db, "SELECT name FROM products WHERE name LIKE 'Apple iPhone'").unwrap();
    assert_eq!(results.len(), 1);
}

#[test]
fn test_cast_null_value() {
    let schema = TableSchema::new(
        "null_test".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "nullable_val".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("null_test", Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();

    let results =
        execute_select(&db, "SELECT CAST(nullable_val AS INTEGER) FROM null_test").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);
}

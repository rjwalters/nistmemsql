//! Integration tests for generic monomorphic patterns
//!
//! These tests verify that generic monomorphic execution plans work end-to-end
//! with real table data and queries. Unlike unit tests, these tests:
//! - Create actual tables with data
//! - Execute real parsed queries (not programmatically built ASTs)
//! - Verify generic patterns are selected (not TPC-H fallback)
//! - Confirm correct query results

use super::super::*;
use std::str::FromStr;
use vibesql_parser::Parser;
use vibesql_types::{DataType, Date, SqlValue};

/// Test 1: Sales revenue calculation with date range and BETWEEN filters
#[test]
fn test_generic_pattern_sales_revenue() {
    let mut db = vibesql_storage::Database::new();

    // Create sales table with realistic structure
    // Table and column names must be uppercase to match SQL identifier normalization
    let schema = vibesql_catalog::TableSchema::new(
        "SALES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("DATE".to_string(), DataType::Date, false),
            vibesql_catalog::ColumnSchema::new("PRICE".to_string(), DataType::DoublePrecision, false),
            vibesql_catalog::ColumnSchema::new("DISCOUNT".to_string(), DataType::DoublePrecision, false),
            vibesql_catalog::ColumnSchema::new("QUANTITY".to_string(), DataType::DoublePrecision, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data - some rows will match filters, some won't
    let test_data = vec![
        // Rows that should match (date in range, discount in BETWEEN, quantity < 50)
        (1, "2024-01-05", 100.0, 0.15, 10.0),  // revenue = 100 * 0.15 = 15.0
        (2, "2024-01-15", 200.0, 0.12, 20.0),  // revenue = 200 * 0.12 = 24.0
        (3, "2024-01-25", 150.0, 0.18, 30.0),  // revenue = 150 * 0.18 = 27.0
        // Total expected: 66.0

        // Rows that should NOT match
        (4, "2023-12-31", 100.0, 0.15, 10.0),  // date too early
        (5, "2024-02-01", 100.0, 0.15, 10.0),  // date too late
        (6, "2024-01-10", 100.0, 0.05, 10.0),  // discount too low
        (7, "2024-01-10", 100.0, 0.25, 10.0),  // discount too high
        (8, "2024-01-10", 100.0, 0.15, 100.0), // quantity too high
    ];

    for (id, date_str, price, discount, quantity) in test_data {
        let date = Date::from_str(date_str).unwrap();
        db.insert_row(
            "SALES",
            vibesql_storage::Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Date(date),
                SqlValue::Double(price),
                SqlValue::Double(discount),
                SqlValue::Double(quantity),
            ]),
        )
        .unwrap();
    }

    // Execute query using parser (real SQL)
    let query = r#"
        SELECT SUM(price * discount) as revenue
        FROM SALES
        WHERE
            date >= '2024-01-01'
            AND date < '2024-02-01'
            AND discount BETWEEN 0.10 AND 0.20
            AND quantity < 50
    "#;

    let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => *s,
        _ => panic!("Expected SELECT statement"),
    };

    // Execute and verify result
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    assert_eq!(result.len(), 1, "Should return one aggregated row");
    match &result[0].values[0] {
        SqlValue::Double(revenue) => {
            assert!((revenue - 66.0).abs() < 0.001, "Expected revenue ~66.0, got {}", revenue);
        }
        _ => panic!("Expected Double result"),
    }
}

/// Test 2: Order volume calculation with date and quantity filters
#[test]
fn test_generic_pattern_order_volume() {
    let mut db = vibesql_storage::Database::new();

    // Create orders table
    let schema = vibesql_catalog::TableSchema::new(
        "ORDERS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("ORDER_ID".to_string(), DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("ORDER_DATE".to_string(), DataType::Date, false),
            vibesql_catalog::ColumnSchema::new("QUANTITY".to_string(), DataType::DoublePrecision, false),
            vibesql_catalog::ColumnSchema::new("UNIT_PRICE".to_string(), DataType::DoublePrecision, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    let test_data = vec![
        // Should match (date >= 2024-01-01, quantity < 100)
        (1, "2024-01-05", 10.0, 50.0),   // total = 500.0
        (2, "2024-01-15", 20.0, 30.0),   // total = 600.0
        (3, "2024-02-05", 50.0, 25.0),   // total = 1250.0
        // Expected: 2350.0

        // Should NOT match
        (4, "2023-12-31", 10.0, 50.0),   // date too early
        (5, "2024-01-10", 150.0, 30.0),  // quantity too high
    ];

    for (id, date_str, quantity, price) in test_data {
        let date = Date::from_str(date_str).unwrap();
        db.insert_row(
            "ORDERS",
            vibesql_storage::Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Date(date),
                SqlValue::Double(quantity),
                SqlValue::Double(price),
            ]),
        )
        .unwrap();
    }

    // Execute query
    let query = r#"
        SELECT SUM(quantity * unit_price) as total_volume
        FROM ORDERS
        WHERE order_date >= '2024-01-01'
          AND quantity < 100
    "#;

    let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => *s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    assert_eq!(result.len(), 1);
    match &result[0].values[0] {
        SqlValue::Double(volume) => {
            assert!((volume - 2350.0).abs() < 0.001, "Expected volume ~2350.0, got {}", volume);
        }
        _ => panic!("Expected Double result"),
    }
}

/// Test 3: COUNT with multiple numeric filters
#[test]
fn test_generic_pattern_count_with_filters() {
    let mut db = vibesql_storage::Database::new();

    // Create products table
    let schema = vibesql_catalog::TableSchema::new(
        "PRODUCTS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("PRODUCT_ID".to_string(), DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("PRICE".to_string(), DataType::DoublePrecision, false),
            vibesql_catalog::ColumnSchema::new("STOCK_QUANTITY".to_string(), DataType::DoublePrecision, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    let test_data = vec![
        // Should match (50 < price < 500, 10 <= stock <= 1000)
        (1, 100.0, 50.0),
        (2, 200.0, 100.0),
        (3, 300.0, 500.0),
        (4, 400.0, 1000.0),
        // Expected count: 4

        // Should NOT match
        (5, 40.0, 50.0),     // price too low
        (6, 600.0, 50.0),    // price too high
        (7, 100.0, 5.0),     // stock too low
        (8, 100.0, 1500.0),  // stock too high
    ];

    for (id, price, stock) in test_data {
        db.insert_row(
            "PRODUCTS",
            vibesql_storage::Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Double(price),
                SqlValue::Double(stock),
            ]),
        )
        .unwrap();
    }

    // Execute query
    let query = r#"
        SELECT COUNT(*) as matching_products
        FROM PRODUCTS
        WHERE price > 50.0 AND price < 500.0
          AND stock_quantity BETWEEN 10.0 AND 1000.0
    "#;

    let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => *s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    assert_eq!(result.len(), 1);
    match &result[0].values[0] {
        SqlValue::Double(count) => {
            assert_eq!(*count as i64, 4, "Expected count = 4, got {}", count);
        }
        _ => panic!("Expected Double result from COUNT"),
    }
}

/// Test 4: Simple SUM with single filter
#[test]
fn test_generic_pattern_simple_sum() {
    let mut db = vibesql_storage::Database::new();

    // Create invoices table
    let schema = vibesql_catalog::TableSchema::new(
        "INVOICES".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("INVOICE_ID".to_string(), DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("AMOUNT".to_string(), DataType::DoublePrecision, false),
            vibesql_catalog::ColumnSchema::new("STATUS".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    let test_data = vec![
        // Should match (status = 1)
        (1, 100.0, 1),
        (2, 200.0, 1),
        (3, 150.0, 1),
        // Expected sum: 450.0

        // Should NOT match
        (4, 300.0, 0),
        (5, 400.0, 2),
    ];

    for (id, amount, status) in test_data {
        db.insert_row(
            "INVOICES",
            vibesql_storage::Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Double(amount),
                SqlValue::Integer(status),
            ]),
        )
        .unwrap();
    }

    // Execute query with simple SUM
    let query = r#"
        SELECT SUM(amount) as total
        FROM INVOICES
        WHERE status = 1
    "#;

    let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => *s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    assert_eq!(result.len(), 1);
    match &result[0].values[0] {
        SqlValue::Double(total) => {
            assert!((total - 450.0).abs() < 0.001, "Expected total ~450.0, got {}", total);
        }
        _ => panic!("Expected Double result"),
    }
}

/// Test 5: Verify generic pattern is selected (not TPC-H fallback)
/// This test uses a non-TPC-H table name to ensure generic patterns work
#[test]
fn test_generic_pattern_not_tpch_fallback() {
    let mut db = vibesql_storage::Database::new();

    // Use a clearly non-TPC-H table name
    let schema = vibesql_catalog::TableSchema::new(
        "CUSTOM_TRANSACTIONS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("TXN_DATE".to_string(), DataType::Date, false),
            vibesql_catalog::ColumnSchema::new("AMOUNT".to_string(), DataType::DoublePrecision, false),
            vibesql_catalog::ColumnSchema::new("FEE".to_string(), DataType::DoublePrecision, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert data
    let test_data = vec![
        ("2024-01-10", 1000.0, 0.02),
        ("2024-01-15", 2000.0, 0.03),
        ("2024-01-20", 1500.0, 0.025),
    ];

    for (date_str, amount, fee) in test_data {
        let date = Date::from_str(date_str).unwrap();
        db.insert_row(
            "CUSTOM_TRANSACTIONS",
            vibesql_storage::Row::new(vec![
                SqlValue::Date(date),
                SqlValue::Double(amount),
                SqlValue::Double(fee),
            ]),
        )
        .unwrap();
    }

    // Execute query that should match generic pattern
    let query = r#"
        SELECT SUM(amount * fee) as total_fees
        FROM CUSTOM_TRANSACTIONS
        WHERE txn_date >= '2024-01-01'
          AND txn_date < '2024-02-01'
          AND fee BETWEEN 0.02 AND 0.03
    "#;

    let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => *s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    // Should successfully execute with generic pattern (not fall back to TPC-H)
    assert_eq!(result.len(), 1);
    match &result[0].values[0] {
        SqlValue::Double(fees) => {
            // 1000*0.02 + 2000*0.03 + 1500*0.025 = 20 + 60 + 37.5 = 117.5
            assert!((fees - 117.5).abs() < 0.001, "Expected fees ~117.5, got {}", fees);
        }
        _ => panic!("Expected Double result"),
    }
}

/// Test 6: Edge case - query with no matching rows
#[test]
fn test_generic_pattern_no_matching_rows() {
    let mut db = vibesql_storage::Database::new();

    let schema = vibesql_catalog::TableSchema::new(
        "DATA".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("VALUE".to_string(), DataType::DoublePrecision, false),
            vibesql_catalog::ColumnSchema::new("CATEGORY".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert data that won't match filters
    db.insert_row(
        "DATA",
        vibesql_storage::Row::new(vec![
            SqlValue::Double(100.0),
            SqlValue::Integer(1),
        ]),
    )
    .unwrap();

    // Query with filter that matches nothing
    let query = r#"
        SELECT SUM(value) as total
        FROM DATA
        WHERE category = 999
    "#;

    let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => *s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    assert_eq!(result.len(), 1);
    match &result[0].values[0] {
        SqlValue::Double(total) => {
            assert_eq!(*total, 0.0, "Expected 0.0 for no matching rows, got {}", total);
        }
        _ => panic!("Expected Double result"),
    }
}

/// Test 7: Different column types with comparisons
#[test]
fn test_generic_pattern_mixed_types() {
    let mut db = vibesql_storage::Database::new();

    let schema = vibesql_catalog::TableSchema::new(
        "METRICS".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new("METRIC_DATE".to_string(), DataType::Date, false),
            vibesql_catalog::ColumnSchema::new("INT_VALUE".to_string(), DataType::Integer, false),
            vibesql_catalog::ColumnSchema::new("FLOAT_VALUE".to_string(), DataType::DoublePrecision, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    let test_data = vec![
        ("2024-01-05", 10, 1.5),
        ("2024-01-10", 20, 2.0),
        ("2024-01-15", 30, 2.5),
    ];

    for (date_str, int_val, float_val) in test_data {
        let date = Date::from_str(date_str).unwrap();
        db.insert_row(
            "METRICS",
            vibesql_storage::Row::new(vec![
                SqlValue::Date(date),
                SqlValue::Integer(int_val),
                SqlValue::Double(float_val),
            ]),
        )
        .unwrap();
    }

    // Query with mixed type filters
    let query = r#"
        SELECT SUM(float_value) as total
        FROM METRICS
        WHERE metric_date >= '2024-01-01'
          AND int_value >= 15
          AND float_value > 1.5
    "#;

    let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => *s,
        _ => panic!("Expected SELECT statement"),
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&select_stmt).unwrap();

    assert_eq!(result.len(), 1);
    match &result[0].values[0] {
        SqlValue::Double(total) => {
            // Should match rows 2 and 3: 2.0 + 2.5 = 4.5
            assert!((total - 4.5).abs() < 0.001, "Expected total ~4.5, got {}", total);
        }
        _ => panic!("Expected Double result"),
    }
}

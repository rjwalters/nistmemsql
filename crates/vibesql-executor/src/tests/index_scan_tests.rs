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

#[test]
fn test_index_scan_with_order_by_desc() {
    // Reproduces the EXACT failing pattern from index/orderby/10/slt_good_0.test
    // Query: SELECT pk FROM tab1 WHERE col3 > 221 ORDER BY 1 DESC
    // Expected: 8, 7, 6, 5, 4, 2, 1, 0
    // Actual (buggy): 8, 7, 6, 5, 4, 1, 0  (missing row 2!)

    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    // Create table with exact schema from sqllogictest
    let table_schema = TableSchema::new(
        "tab1".to_string(),
        vec![
            ColumnSchema::new("pk".to_string(), DataType::Integer, false),
            ColumnSchema::new("col0".to_string(), DataType::Integer, true),
            ColumnSchema::new("col1".to_string(), DataType::Real, true),
            ColumnSchema::new("col2".to_string(), DataType::Varchar { max_length: None }, true),
            ColumnSchema::new("col3".to_string(), DataType::Integer, true),
            ColumnSchema::new("col4".to_string(), DataType::Real, true),
            ColumnSchema::new("col5".to_string(), DataType::Varchar { max_length: None }, true),
        ],
    );
    db.create_table(table_schema).unwrap();

    // Create single-column index on col3
    db.create_index(
        "idx_tab1_3".to_string(),
        "tab1".to_string(),
        false,
        vec![IndexColumn {
            column_name: "col3".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    // Insert EXACT data from sqllogictest tab0
    // Row 0: col3 = 846 (> 221) ✓
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(0),
            SqlValue::Integer(544),
            SqlValue::Real(473.59),
            SqlValue::Varchar("lupfg".to_string()),
            SqlValue::Integer(846),
            SqlValue::Real(31.38),
            SqlValue::Varchar("crmer".to_string()),
        ]),
    )
    .unwrap();

    // Row 1: col3 = 562 (> 221) ✓
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Integer(551),
            SqlValue::Real(716.92),
            SqlValue::Varchar("sxtsd".to_string()),
            SqlValue::Integer(562),
            SqlValue::Real(145.36),
            SqlValue::Varchar("xxacr".to_string()),
        ]),
    )
    .unwrap();

    // Row 2: col3 = 652 (> 221) ✓ <-- THIS ROW GOES MISSING!
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Integer(481),
            SqlValue::Real(975.35),
            SqlValue::Varchar("uxrjj".to_string()),
            SqlValue::Integer(652),
            SqlValue::Real(750.52),
            SqlValue::Varchar("rdgic".to_string()),
        ]),
    )
    .unwrap();

    // Row 3: col3 = 51 (< 221) ✗ should be filtered out
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Integer(345),
            SqlValue::Real(371.4),
            SqlValue::Varchar("nxyqi".to_string()),
            SqlValue::Integer(51),
            SqlValue::Real(199.34),
            SqlValue::Varchar("lekdd".to_string()),
        ]),
    )
    .unwrap();

    // Row 4: col3 = 256 (> 221) ✓
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Integer(908),
            SqlValue::Real(772.92),
            SqlValue::Varchar("owifa".to_string()),
            SqlValue::Integer(256),
            SqlValue::Real(154.36),
            SqlValue::Varchar("xophl".to_string()),
        ]),
    )
    .unwrap();

    // Row 5: col3 = 957 (> 221) ✓
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Integer(309),
            SqlValue::Real(912.32),
            SqlValue::Varchar("iganw".to_string()),
            SqlValue::Integer(957),
            SqlValue::Real(707.84),
            SqlValue::Varchar("tewpl".to_string()),
        ]),
    )
    .unwrap();

    // Row 6: col3 = 656 (> 221) ✓
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(6),
            SqlValue::Integer(691),
            SqlValue::Real(521.59),
            SqlValue::Varchar("fstmf".to_string()),
            SqlValue::Integer(656),
            SqlValue::Real(504.37),
            SqlValue::Varchar("ptkph".to_string()),
        ]),
    )
    .unwrap();

    // Row 7: col3 = 294 (> 221) ✓
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(7),
            SqlValue::Integer(823),
            SqlValue::Real(447.70),
            SqlValue::Varchar("rrwze".to_string()),
            SqlValue::Integer(294),
            SqlValue::Real(877.49),
            SqlValue::Varchar("fvoic".to_string()),
        ]),
    )
    .unwrap();

    // Row 8: col3 = 878 (> 221) ✓
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(8),
            SqlValue::Integer(725),
            SqlValue::Real(784.91),
            SqlValue::Varchar("iaoqu".to_string()),
            SqlValue::Integer(878),
            SqlValue::Real(963.31),
            SqlValue::Varchar("razqy".to_string()),
        ]),
    )
    .unwrap();

    // Row 9: col3 = 105 (< 221) ✗ should be filtered out
    db.insert_row(
        "tab1",
        Row::new(vec![
            SqlValue::Integer(9),
            SqlValue::Integer(297),
            SqlValue::Real(993.36),
            SqlValue::Varchar("eoujh".to_string()),
            SqlValue::Integer(105),
            SqlValue::Real(829.18),
            SqlValue::Varchar("kvyce".to_string()),
        ]),
    )
    .unwrap();

    // Debug: Check what's in the index
    let index_data = db.get_index_data("idx_tab1_3").expect("Index should exist");
    println!("=== Index Contents ===");
    for (key, row_indices) in index_data.iter() {
        println!("  Key: {:?} -> Rows: {:?}", key, row_indices);
    }

    // Test range scan directly
    println!("\n=== Range Scan Test (col3 > 221) ===");
    let matching_indices = index_data.range_scan(
        Some(&SqlValue::Integer(221)),
        None,
        false, // exclusive start
        false,
    );
    println!("Matching row indices from range_scan: {:?}", matching_indices);

    let executor = SelectExecutor::new(&db);

    // The problematic query: WHERE with index + ORDER BY DESC
    let query = "SELECT pk FROM tab1 WHERE col3 > 221 ORDER BY pk DESC";
    let stmt = Parser::parse_sql(query).unwrap();

    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let result = executor.execute(&select_stmt).unwrap();

        // Extract pk values
        let pks: Vec<i64> = result
            .iter()
            .map(|row| match &row.values[0] {
                SqlValue::Integer(pk) => *pk,
                _ => panic!("Expected Integer"),
            })
            .collect();

        println!("Actual PKs: {:?}", pks);

        // Expected: all rows where col3 > 221, ordered by pk DESC
        // Rows with col3 > 221:
        //   pk=0: col3=846 ✓
        //   pk=1: col3=562 ✓
        //   pk=2: col3=652 ✓  <-- CRITICAL: This row goes missing in the bug!
        //   pk=3: col3=51  ✗  (filtered out)
        //   pk=4: col3=256 ✓
        //   pk=5: col3=957 ✓
        //   pk=6: col3=656 ✓
        //   pk=7: col3=294 ✓
        //   pk=8: col3=878 ✓
        let expected = vec![8, 7, 6, 5, 4, 2, 1, 0];

        assert_eq!(
            pks, expected,
            "Query should return all rows where col3 > 221, ordered by pk DESC. \
             Row 2 (col3=652) should be included!"
        );
    } else {
        panic!("Expected SELECT statement");
    }
}

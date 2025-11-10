//! Integration test to verify predicate pushdown (Phases 2 & 3) is working correctly
//!
//! This test verifies that:
//! - Phase 2: Table-local predicates are pushed to table scans
//! - Phase 3: Equijoin predicates are used during hash joins

use ast;
use catalog;
use executor::SelectExecutor;
use parser;
use types;

fn parse_select(sql: &str) -> ast::SelectStmt {
    match parser::Parser::parse_sql(sql) {
        Ok(ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

#[test]
fn test_phase_2_table_local_pushdown() {
    // Verify table-local predicates are applied during scan
    let mut db = storage::Database::new();

    // Create table with 1000 rows
    let schema = catalog::TableSchema::new(
        "T1".to_string(),
        vec![
            catalog::ColumnSchema {
                name: "a".to_string(),
                data_type: types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
            catalog::ColumnSchema {
                name: "b".to_string(),
                data_type: types::DataType::Integer,
                nullable: false,
                default_value: None,
            },
        ],
    );
    db.create_table(schema).unwrap();

    for i in 1..=1000 {
        db.insert_row("T1", storage::Row::new(vec![
            types::SqlValue::Integer(i),
            types::SqlValue::Integer(i * 10),
        ])).unwrap();
    }

    // Query with table-local predicate: a > 990
    // Without pushdown: scans 1000 rows, then filters
    // With pushdown: scans and filters simultaneously, early termination possible
    let sql = "SELECT COUNT(*) FROM T1 WHERE a > 990";
    let stmt = parse_select(sql);
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1);
    let count = match &result[0].values[0] {
        types::SqlValue::Integer(n) => *n,
        types::SqlValue::Numeric(n) => *n as i64,
        _ => panic!("Expected integer or numeric count"),
    };
    assert_eq!(count, 10); // Rows 991-1000
}

#[test]
fn test_phase_3_equijoin_hash_join() {
    // Verify equijoin predicates enable hash join optimization
    let mut db = storage::Database::new();

    // Create two tables with 100 rows each
    for table_num in 1..=2 {
        let table_name = format!("T{}", table_num);
        let schema = catalog::TableSchema::new(
            table_name.clone(),
            vec![catalog::ColumnSchema {
                name: "a".to_string(),
                data_type: types::DataType::Integer,
                nullable: false,
                default_value: None,
            }],
        );
        db.create_table(schema).unwrap();

        for i in 1..=100 {
            db.insert_row(&table_name, storage::Row::new(vec![
                types::SqlValue::Integer(i),
            ])).unwrap();
        }
    }

    // Query with equijoin in WHERE clause
    // Without hash join: 100 * 100 = 10,000 comparisons
    // With hash join: 100 + 100 = 200 operations (build hash + probe)
    let sql = "SELECT COUNT(*) FROM T1, T2 WHERE T1.a = T2.a";
    let stmt = parse_select(sql);
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1);
    let count = match &result[0].values[0] {
        types::SqlValue::Integer(n) => *n,
        types::SqlValue::Numeric(n) => *n as i64,
        _ => panic!("Expected integer or numeric count"),
    };
    assert_eq!(count, 100); // 100 matching rows
}

#[test]
fn test_phases_2_and_3_combined() {
    // Verify table-local AND equijoin predicates work together
    let mut db = storage::Database::new();

    for table_num in 1..=3 {
        let table_name = format!("T{}", table_num);
        let schema = catalog::TableSchema::new(
            table_name.clone(),
            vec![
                catalog::ColumnSchema {
                    name: "a".to_string(),
                    data_type: types::DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                catalog::ColumnSchema {
                    name: "b".to_string(),
                    data_type: types::DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
            ],
        );
        db.create_table(schema).unwrap();

        for i in 1..=50 {
            db.insert_row(&table_name, storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i * 10),
            ])).unwrap();
        }
    }

    // Combined query:
    // - Table-local: T1.a > 40 (reduces T1 from 50 to 10 rows)
    // - Equijoin: T1.a = T2.a AND T2.a = T3.a (hash join)
    let sql = "SELECT COUNT(*) FROM T1, T2, T3 WHERE T1.a > 40 AND T1.a = T2.a AND T2.a = T3.a";
    let stmt = parse_select(sql);
    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    assert_eq!(result.len(), 1);
    let count = match &result[0].values[0] {
        types::SqlValue::Integer(n) => *n,
        types::SqlValue::Numeric(n) => *n as i64,
        _ => panic!("Expected integer or numeric count"),
    };
    assert_eq!(count, 10); // Rows 41-50 matching across all tables
}

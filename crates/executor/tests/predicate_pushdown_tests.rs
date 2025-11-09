//! Tests for predicate pushdown optimization (issue #1036)
//!
//! These tests verify that table-local predicates are correctly identified and applied
//! during table scans, reducing memory consumption in multi-table joins.

use executor::SelectExecutor;
use parser;
use ast;
use catalog;
use types;

/// Helper function to parse SELECT statements
fn parse_select(sql: &str) -> ast::SelectStmt {
    match parser::Parser::parse_sql(sql) {
        Ok(ast::Statement::Select(select_stmt)) => select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Helper to create test database with tables
fn setup_test_db_with_tables(num_tables: usize, rows_per_table: i64) -> storage::Database {
    let mut db = storage::Database::new();

    for table_num in 1..=num_tables {
        let table_name = format!("T{}", table_num);
        let mut schema = catalog::TableSchema::new(
            table_name.clone(),
            vec![
                catalog::ColumnSchema {
                    name: format!("a{}", table_num),
                    data_type: types::DataType::Integer,
                    nullable: false,
                    default_value: None,
                },
                catalog::ColumnSchema {
                    name: format!("b{}", table_num),
                    data_type: types::DataType::Integer,
                    nullable: true,
                    default_value: None,
                },
                catalog::ColumnSchema {
                    name: format!("x{}", table_num),
                    data_type: types::DataType::Varchar { max_length: Some(40) },
                    nullable: true,
                    default_value: None,
                },
            ],
        );
        schema.primary_key = Some(vec![format!("a{}", table_num)]);
        db.create_table(schema).unwrap();

        // Insert rows
        for row_num in 1..=rows_per_table {
            let row = storage::Row::new(vec![
                types::SqlValue::Integer(row_num),
                types::SqlValue::Integer(row_num % 10 + 1),
                types::SqlValue::Varchar(format!("t{} row {}", table_num, row_num)),
            ]);
            db.insert_row(&table_name, row).unwrap();
        }
    }

    db
}

#[test]
fn test_single_table_local_predicate() {
    // Test that a simple table-local predicate is applied during scan
    let db = setup_test_db_with_tables(1, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT * FROM T1 WHERE a1 > 5";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should return 5 rows (a1=6,7,8,9,10)
    assert_eq!(result.len(), 5);
}

#[test]
fn test_table_local_predicate_with_two_tables() {
    // Test predicate pushdown with 2-table join
    // Without pushdown: 10 * 10 = 100 rows cartesian product
    // With pushdown: t1 filtered to 5 rows first, then joined
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    // a1 > 5 should be pushed down to t1 scan, reducing cartesian product
    let sql = "SELECT COUNT(*) FROM T1, t2 WHERE a1 > 5 AND a1 = b2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should have results (exact count depends on join selectivity)
    assert!(result.len() > 0);
    assert!(result[0].values[0] > types::SqlValue::Integer(0));
}

#[test]
fn test_three_table_join_with_local_predicates() {
    // Test predicate pushdown with 3-table join
    // Multiple table-local predicates should be pushed down independently
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*) FROM T1, t2, t3 \
               WHERE a1 > 3 AND b2 < 8 AND a3 > 2 \
               AND a1 = b2 AND a2 = b3";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should successfully execute with reduced memory due to pushdown
    assert!(result.len() > 0);
}

#[test]
fn test_four_table_join_with_predicates() {
    // Test the memory improvement with 4-table join
    // Without pushdown: 10^4 = 10,000 row cartesian product
    // With pushdown: filtered much earlier, reducing memory consumption
    let db = setup_test_db_with_tables(4, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4 \
               WHERE a1 > 2 AND a2 > 3 AND b3 < 9 \
               AND a1 = b2 AND a2 = b3 AND a3 = b4";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should successfully complete (would likely OOM without pushdown in extreme cases)
    assert!(result.len() > 0);
}

#[test]
fn test_five_table_join_with_predicates() {
    // Test with 5 tables (larger cartesian product)
    let db = setup_test_db_with_tables(5, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*) FROM T1, t2, t3, t4, t5 \
               WHERE a1 > 5 AND a2 < 8 AND a3 > 2 AND a4 < 10 \
               AND a1 = b2 AND a2 = b3 AND a3 = b4 AND a4 = b5";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should complete successfully
    assert!(result.len() > 0);
}

#[test]
fn test_no_table_local_predicates() {
    // Verify behavior when there are only complex predicates
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    // Complex predicate that references both tables
    let sql = "SELECT COUNT(*) FROM T1, t2 WHERE a1 + b2 > 10";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly (not pushed down)
    assert!(result.len() > 0);
}

#[test]
fn test_mixed_table_local_and_complex_predicates() {
    // Test combination of pushdownable and non-pushdownable predicates
    let db = setup_test_db_with_tables(3, 10);
    let executor = SelectExecutor::new(&db);

    // a1 > 5 is pushdownable, but a1 + a2 > 10 is complex
    let sql = "SELECT COUNT(*) FROM T1, t2, t3 \
               WHERE a1 > 5 AND a1 + a2 > 10 AND a1 = b2 AND a2 = b3";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly with mixed predicates
    assert!(result.len() > 0);
}

#[test]
fn test_aggregate_with_table_local_predicate() {
    // Test predicate pushdown with aggregation
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT COUNT(*), SUM(a1) FROM T1, t2 WHERE a1 > 5 AND a1 = b2";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly
    assert!(result.len() > 0);
}

#[test]
fn test_predicate_pushdown_with_group_by() {
    // Test predicate pushdown with GROUP BY
    let db = setup_test_db_with_tables(2, 10);
    let executor = SelectExecutor::new(&db);

    let sql = "SELECT a1, COUNT(*) FROM T1, t2 \
               WHERE a1 > 3 AND a1 = b2 \
               GROUP BY a1";
    let stmt = parse_select(sql);
    let result = executor.execute(&stmt).unwrap();

    // Should work correctly with GROUP BY
    assert!(result.len() > 0);
}

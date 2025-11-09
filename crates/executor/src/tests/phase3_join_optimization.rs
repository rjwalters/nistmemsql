//! Tests for Phase 3 Join Optimization
//!
//! Phase 3.1: Enhanced hash join selection for WHERE clause equijoins
//! Phase 3.2: Join condition reordering for optimal execution
//! Phase 3.3: Vectorized equijoin evaluation

use super::super::*;

/// Test that hash join is selected for equijoin predicates in WHERE clause
/// even when there's no ON clause.
///
/// This is Phase 3.1 - Enhanced hash join selection
#[test]
fn test_hash_join_from_where_equijoin_no_on_clause() {
    let mut db = storage::Database::new();

    // Create table t1
    let t1_schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(t1_schema).unwrap();

    // Insert 5 rows
    for i in 1..=5 {
        db.insert_row(
            "t1",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    // Create table t2
    let t2_schema = catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("name".to_string(), types::DataType::Varchar { max_length: Some(50) }, false),
        ],
    );
    db.create_table(t2_schema).unwrap();

    // Insert 5 rows
    for i in 1..=5 {
        db.insert_row(
            "t2",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Varchar(format!("name_{}", i)),
            ]),
        )
        .unwrap();
    }

    // Query with equijoin in WHERE clause, NO ON clause
    // This should use hash join via Phase 3.1
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "t2".to_string(), alias: None }),
            join_type: ast::JoinType::Inner,
            condition: None, // NO ON clause - equijoin is in WHERE
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    
    // Should have 5 rows (one for each matching ID)
    assert_eq!(result.len(), 5, "Expected 5 rows from equijoin");
    
    // Verify data integrity - first row should have t1.id=1, t1.value=10, t2.id=1, t2.name='name_1'
    let first_row = &result[0];
    assert_eq!(first_row.values.len(), 4, "Expected 4 columns");
    assert_eq!(first_row.values[0], types::SqlValue::Integer(1)); // t1.id
    assert_eq!(first_row.values[1], types::SqlValue::Integer(10)); // t1.value
    assert_eq!(first_row.values[2], types::SqlValue::Integer(1)); // t2.id
    assert_eq!(first_row.values[3], types::SqlValue::Varchar("name_1".to_string())); // t2.name
}

/// Test that multiple equijoins are handled correctly when hash join uses one
#[test]
fn test_hash_join_multiple_equijoins_in_where() {
    let mut db = storage::Database::new();

    // Create table t1 with extra column
    let t1_schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(t1_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t1",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    // Create table t2 with matching columns
    let t2_schema = catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(t2_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t2",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i * 20),
            ]),
        )
        .unwrap();
    }

    // Query with TWO equijoins in WHERE clause
    // Hash join should use the first one and filter with the second
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "t2".to_string(), alias: None }),
            join_type: ast::JoinType::Inner,
            condition: None,
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "id".to_string(),
                }),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "value".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "value".to_string(),
                }),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    
    // t1.value = [10, 20, 30] (id 1, 2, 3)
    // t2.value = [20, 40, 60] (id 1, 2, 3)
    // 
    // Both id AND value must match:
    // t1.id=t2.id AND t1.value=t2.value
    // 
    // id=1: t1.value=10 vs t2.value=20 - NO
    // id=2: t1.value=20 vs t2.value=40 - NO
    // id=3: t1.value=30 vs t2.value=60 - NO
    // 
    // So no rows match both conditions
    assert_eq!(result.len(), 0, "Expected 0 rows where both conditions match");
}

/// Test cascading joins with WHERE clause equijoins (simplified select5 scenario)
#[test]
fn test_cascading_joins_with_where_equijoins() {
    let mut db = storage::Database::new();

    // Create 4 tables with 10 rows each
    for table_num in 1..=4 {
        let table_name = format!("t{}", table_num);
        let schema = catalog::TableSchema::new(
            table_name.clone(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert 10 rows per table
        for row_num in 1..=10 {
            db.insert_row(
                &table_name,
                storage::Row::new(vec![
                    types::SqlValue::Integer(row_num as i64),
                    types::SqlValue::Integer(row_num as i64 + table_num as i64),
                ]),
            )
            .unwrap();
        }
    }

    // Query with cascading equijoins in WHERE
    // Without Phase 3.1, this would build: 10*10=100 rows, then 100*10=1000, etc.
    // With Phase 3.1 hash join:
    //   t1 (10 rows) JOIN t2 ON t1.id=t2.id → 10 rows
    //   result (10 rows) JOIN t3 ON t2.id=t3.id → 10 rows
    //   result (10 rows) JOIN t4 ON t3.id=t4.id → 10 rows
    let executor = SelectExecutor::new(&db);
    
    // Build nested join manually
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Join {
                    left: Box::new(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
                    right: Box::new(ast::FromClause::Table { name: "t2".to_string(), alias: None }),
                    join_type: ast::JoinType::Inner,
                    condition: None,
                }),
                right: Box::new(ast::FromClause::Table { name: "t3".to_string(), alias: None }),
                join_type: ast::JoinType::Inner,
                condition: None,
            }),
            right: Box::new(ast::FromClause::Table { name: "t4".to_string(), alias: None }),
            join_type: ast::JoinType::Inner,
            condition: None,
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("t2".to_string()),
                        column: "id".to_string(),
                    }),
                }),
                op: ast::BinaryOperator::And,
                right: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("t2".to_string()),
                        column: "id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("t3".to_string()),
                        column: "id".to_string(),
                    }),
                }),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("t3".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("t4".to_string()),
                    column: "id".to_string(),
                }),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    
    // Should have 10 rows (one for each matching ID 1-10)
    assert_eq!(result.len(), 10, "Expected 10 rows from cascading equijoins");
    
    // Verify first row structure
    let first_row = &result[0];
    assert_eq!(first_row.values.len(), 8, "Expected 8 columns (4 tables × 2 cols each)");
}

/// Test that Phase 3.1 doesn't break when there's an ON clause
#[test]
fn test_hash_join_with_on_clause_and_where_equijoins() {
    let mut db = storage::Database::new();

    let t1_schema = catalog::TableSchema::new(
        "t1".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("key".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(t1_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t1",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    let t2_schema = catalog::TableSchema::new(
        "t2".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("key".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(t2_schema).unwrap();

    for i in 1..=3 {
        db.insert_row(
            "t2",
            storage::Row::new(vec![
                types::SqlValue::Integer(i),
                types::SqlValue::Integer(i * 10),
            ]),
        )
        .unwrap();
    }

    // Query with BOTH ON clause and WHERE equijoin
    // Should prefer ON clause for hash join
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table { name: "t1".to_string(), alias: None }),
            right: Box::new(ast::FromClause::Table { name: "t2".to_string(), alias: None }),
            join_type: ast::JoinType::Inner,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("t2".to_string()),
                    column: "id".to_string(),
                }),
            }),
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "key".to_string(),
            }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "key".to_string(),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    
    // Should have 3 rows (all rows match on both conditions)
    assert_eq!(result.len(), 3, "Expected 3 rows");
}

/// Test star join pattern with 6 tables (simplified select5.test scenario)
///
/// This tests the bug from issue #1053 where hash join crashes with
/// index out of bounds when executing star join patterns.
#[test]
fn test_star_join_select5_pattern() {
    let mut db = storage::Database::new();

    // Create 6 tables with 10 rows each
    // All tables have same schema: (id, value)
    for table_num in 1..=6 {
        let table_name = format!("t{}", table_num);
        let schema = catalog::TableSchema::new(
            table_name.clone(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    format!("val{}", table_num),
                    types::DataType::Integer,
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert 10 rows per table
        for row_num in 1..=10 {
            db.insert_row(
                &table_name,
                storage::Row::new(vec![
                    types::SqlValue::Integer(row_num as i64),
                    types::SqlValue::Integer(row_num as i64 * table_num as i64),
                ]),
            )
            .unwrap();
        }
    }

    // Build star join: all tables join to t1 (hub) via WHERE clause
    // SELECT * FROM t1, t2, t3, t4, t5, t6
    // WHERE t1.id = t2.id
    //   AND t1.id = t3.id
    //   AND t1.id = t4.id
    //   AND t1.id = t5.id
    //   AND t1.id = t6.id
    //
    // This should return 10 rows (one for each id 1-10)
    let executor = SelectExecutor::new(&db);

    // Build nested join structure: ((((t1, t2), t3), t4), t5), t6
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Join {
                left: Box::new(ast::FromClause::Join {
                    left: Box::new(ast::FromClause::Join {
                        left: Box::new(ast::FromClause::Join {
                            left: Box::new(ast::FromClause::Table {
                                name: "t1".to_string(),
                                alias: None,
                            }),
                            right: Box::new(ast::FromClause::Table {
                                name: "t2".to_string(),
                                alias: None,
                            }),
                            join_type: ast::JoinType::Inner,
                            condition: None,
                        }),
                        right: Box::new(ast::FromClause::Table {
                            name: "t3".to_string(),
                            alias: None,
                        }),
                        join_type: ast::JoinType::Inner,
                        condition: None,
                    }),
                    right: Box::new(ast::FromClause::Table {
                        name: "t4".to_string(),
                        alias: None,
                    }),
                    join_type: ast::JoinType::Inner,
                    condition: None,
                }),
                right: Box::new(ast::FromClause::Table {
                    name: "t5".to_string(),
                    alias: None,
                }),
                join_type: ast::JoinType::Inner,
                condition: None,
            }),
            right: Box::new(ast::FromClause::Table {
                name: "t6".to_string(),
                alias: None,
            }),
            join_type: ast::JoinType::Inner,
            condition: None,
        }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::BinaryOp {
                            left: Box::new(ast::Expression::ColumnRef {
                                table: Some("t1".to_string()),
                                column: "id".to_string(),
                            }),
                            op: ast::BinaryOperator::Equal,
                            right: Box::new(ast::Expression::ColumnRef {
                                table: Some("t2".to_string()),
                                column: "id".to_string(),
                            }),
                        }),
                        op: ast::BinaryOperator::And,
                        right: Box::new(ast::Expression::BinaryOp {
                            left: Box::new(ast::Expression::ColumnRef {
                                table: Some("t1".to_string()),
                                column: "id".to_string(),
                            }),
                            op: ast::BinaryOperator::Equal,
                            right: Box::new(ast::Expression::ColumnRef {
                                table: Some("t3".to_string()),
                                column: "id".to_string(),
                            }),
                        }),
                    }),
                    op: ast::BinaryOperator::And,
                    right: Box::new(ast::Expression::BinaryOp {
                        left: Box::new(ast::Expression::ColumnRef {
                            table: Some("t1".to_string()),
                            column: "id".to_string(),
                        }),
                        op: ast::BinaryOperator::Equal,
                        right: Box::new(ast::Expression::ColumnRef {
                            table: Some("t4".to_string()),
                            column: "id".to_string(),
                        }),
                    }),
                }),
                op: ast::BinaryOperator::And,
                right: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: Some("t1".to_string()),
                        column: "id".to_string(),
                    }),
                    op: ast::BinaryOperator::Equal,
                    right: Box::new(ast::Expression::ColumnRef {
                        table: Some("t5".to_string()),
                        column: "id".to_string(),
                    }),
                }),
            }),
            op: ast::BinaryOperator::And,
            right: Box::new(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("t1".to_string()),
                    column: "id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("t6".to_string()),
                    column: "id".to_string(),
                }),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should have 10 rows (one for each matching ID 1-10)
    assert_eq!(result.len(), 10, "Expected 10 rows from star join");

    // Verify first row structure: should have 12 columns (6 tables × 2 cols each)
    let first_row = &result[0];
    assert_eq!(
        first_row.values.len(),
        12,
        "Expected 12 columns (6 tables × 2 cols each)"
    );

    // Verify all rows have id=1 through id=10
    for (idx, row) in result.iter().enumerate() {
        let expected_id = types::SqlValue::Integer((idx + 1) as i64);
        // t1.id should be at index 0
        assert_eq!(row.values[0], expected_id, "Row {} should have id={}", idx, idx + 1);
        // All tables should have matching id
        assert_eq!(row.values[2], expected_id, "t2.id mismatch at row {}", idx); // t2.id
        assert_eq!(row.values[4], expected_id, "t3.id mismatch at row {}", idx); // t3.id
        assert_eq!(row.values[6], expected_id, "t4.id mismatch at row {}", idx); // t4.id
        assert_eq!(row.values[8], expected_id, "t5.id mismatch at row {}", idx); // t5.id
        assert_eq!(row.values[10], expected_id, "t6.id mismatch at row {}", idx); // t6.id
    }
}

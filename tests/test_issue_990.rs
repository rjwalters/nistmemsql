//! Test for issue #990: COUNT(*) not evaluated in arithmetic expressions
//!
//! Reproduces the exact failing case from SQLLogicTest

use executor::SelectExecutor;

#[test]
fn test_issue_990_multiple_unary_plus() {
    // From the issue: SELECT + + 5 + 92 * COUNT( * )
    // Expected: 97 (assuming 1 row: 5 + 92 * 1 = 97)
    // Actual: 5.000 (bug - COUNT(*) returns 0 or is ignored)

    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "id".to_string(),
            types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 1 row
    db.insert_row(
        "test",
        storage::Row::new(vec![types::SqlValue::Integer(1)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Build the exact expression from the issue: + + 5 + 92 * COUNT(*)
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::UnaryOp {
                    op: ast::UnaryOperator::Plus,
                    expr: Box::new(ast::Expression::UnaryOp {
                        op: ast::UnaryOperator::Plus,
                        expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
                    }),
                }),
                op: ast::BinaryOperator::Plus,
                right: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(92))),
                    op: ast::BinaryOperator::Multiply,
                    right: Box::new(ast::Expression::AggregateFunction {
                        name: "COUNT".to_string(),
                        distinct: false,
                        args: vec![ast::Expression::Wildcard],
                    }),
                }),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);

    // Expected: 97 (5 + 92 * 1)
    println!("Result: {:?}", result[0].values[0]);

    match &result[0].values[0] {
        types::SqlValue::Integer(n) => assert_eq!(*n, 97, "Expected 97, got {}", n),
        types::SqlValue::Numeric(n) => {
            // Check if it's close to 97 (allowing for floating point comparison)
            assert!(((*n as f64) - 97.0).abs() < 0.001, "Expected 97, got {}", n);
        }
        other => panic!("Expected Integer or Numeric, got {:?}", other),
    }
}

#[test]
fn test_issue_990_simpler_case() {
    // Simpler case: SELECT 5 + 92 * COUNT(*) FROM test
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "id".to_string(),
            types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 1 row
    db.insert_row(
        "test",
        storage::Row::new(vec![types::SqlValue::Integer(1)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Build: 5 + 92 * COUNT(*)
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
                op: ast::BinaryOperator::Plus,
                right: Box::new(ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(92))),
                    op: ast::BinaryOperator::Multiply,
                    right: Box::new(ast::Expression::AggregateFunction {
                        name: "COUNT".to_string(),
                        distinct: false,
                        args: vec![ast::Expression::Wildcard],
                    }),
                }),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table {
            name: "test".to_string(),
            alias: None,
        }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);

    println!("Simpler case result: {:?}", result[0].values[0]);

    // Expected: 97 (5 + 92 * 1)
    match &result[0].values[0] {
        types::SqlValue::Integer(n) => assert_eq!(*n, 97, "Expected 97, got {}", n),
        types::SqlValue::Numeric(n) => {
            assert!(((*n as f64) - 97.0).abs() < 0.001, "Expected 97, got {}", n);
        }
        other => panic!("Expected Integer or Numeric, got {:?}", other),
    }
}

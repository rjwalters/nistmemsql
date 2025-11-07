//! Tests for COUNT(*) in arithmetic expressions
//!
//! Specific tests for COUNT(*) used in arithmetic contexts,
//! which is the focus of issue #922.

use executor::SelectExecutor;

#[test]
fn test_count_star_in_multiplication() {
    // Test: SELECT -18 * COUNT(*) FROM tab2
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "tab2".to_string(),
        vec![catalog::ColumnSchema::new(
            "col1".to_string(),
            types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 2 rows
    db.insert_row(
        "tab2",
        storage::Row::new(vec![types::SqlValue::Integer(1)]),
    )
    .unwrap();
    db.insert_row(
        "tab2",
        storage::Row::new(vec![types::SqlValue::Integer(2)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(-18))),
                op: ast::BinaryOperator::Multiply,
                right: Box::new(ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                }),
            },
            alias: Some("col1".to_string()),
        }],
        from: Some(ast::FromClause::Table {
            name: "tab2".to_string(),
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
    // -18 * COUNT(*) = -18 * 2 = -36
    assert_eq!(result[0].values[0], types::SqlValue::Integer(-36));
}

#[test]
fn test_count_star_in_addition() {
    // Test: SELECT COUNT(*) + COUNT(*) FROM tab2
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "tab2".to_string(),
        vec![catalog::ColumnSchema::new(
            "col1".to_string(),
            types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 3 rows
    for i in 0..3 {
        db.insert_row(
            "tab2",
            storage::Row::new(vec![types::SqlValue::Integer(i)]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                }),
                op: ast::BinaryOperator::Plus,
                right: Box::new(ast::Expression::AggregateFunction {
                    name: "COUNT".to_string(),
                    distinct: false,
                    args: vec![ast::Expression::Wildcard],
                }),
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table {
            name: "tab2".to_string(),
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
    // COUNT(*) + COUNT(*) = 3 + 3 = 6
    assert_eq!(result[0].values[0], types::SqlValue::Integer(6));
}

#[test]
fn test_count_star_complex_expression() {
    // Test: SELECT 10 + (COUNT(*) * 2) - 5 FROM tab2
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "tab2".to_string(),
        vec![catalog::ColumnSchema::new(
            "col1".to_string(),
            types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert 4 rows
    for i in 0..4 {
        db.insert_row(
            "tab2",
            storage::Row::new(vec![types::SqlValue::Integer(i)]),
        )
        .unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Build: COUNT(*) * 2
    let count_times_two = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::AggregateFunction {
            name: "COUNT".to_string(),
            distinct: false,
            args: vec![ast::Expression::Wildcard],
        }),
        op: ast::BinaryOperator::Multiply,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(2))),
    };

    // Build: 10 + (COUNT(*) * 2)
    let ten_plus_count = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
        op: ast::BinaryOperator::Plus,
        right: Box::new(count_times_two),
    };

    // Build: 10 + (COUNT(*) * 2) - 5
    let full_expr = ast::Expression::BinaryOp {
        left: Box::new(ten_plus_count),
        op: ast::BinaryOperator::Minus,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
    };

    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: full_expr,
            alias: None,
        }],
        from: Some(ast::FromClause::Table {
            name: "tab2".to_string(),
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
    // 10 + (COUNT(*) * 2) - 5 = 10 + (4 * 2) - 5 = 10 + 8 - 5 = 13
    assert_eq!(result[0].values[0], types::SqlValue::Integer(13));
}

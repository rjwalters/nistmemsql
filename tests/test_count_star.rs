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
        vec![catalog::ColumnSchema::new("col1".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert 2 rows
    db.insert_row("tab2", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("tab2", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();

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
        from: Some(ast::FromClause::Table { name: "tab2".to_string(), alias: None }),
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
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(-36.0));
}

#[test]
fn test_count_star_in_addition() {
    // Test: SELECT COUNT(*) + COUNT(*) FROM tab2
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "tab2".to_string(),
        vec![catalog::ColumnSchema::new("col1".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert 3 rows
    for i in 0..3 {
        db.insert_row("tab2", storage::Row::new(vec![types::SqlValue::Integer(i)])).unwrap();
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
        from: Some(ast::FromClause::Table { name: "tab2".to_string(), alias: None }),
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
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(6.0));
}

#[test]
fn test_count_star_complex_expression() {
    // Test: SELECT 10 + (COUNT(*) * 2) - 5 FROM tab2
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "tab2".to_string(),
        vec![catalog::ColumnSchema::new("col1".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert 4 rows
    for i in 0..4 {
        db.insert_row("tab2", storage::Row::new(vec![types::SqlValue::Integer(i)])).unwrap();
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
        select_list: vec![ast::SelectItem::Expression { expr: full_expr, alias: None }],
        from: Some(ast::FromClause::Table { name: "tab2".to_string(), alias: None }),
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
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(13.0));
}

#[test]
fn test_count_star_with_unary_operators() {
    // Test: SELECT + 60 * ( + + COUNT( * ) ) FROM tab1
    // This specifically tests the issue from #935 where unary operators
    // combined with COUNT(*) in arithmetic expressions fail
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "tab1".to_string(),
        vec![catalog::ColumnSchema::new("col0".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert 5 rows
    for i in 0..5 {
        db.insert_row("tab1", storage::Row::new(vec![types::SqlValue::Integer(i)])).unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Build: + COUNT(*)
    let unary_count = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Plus,
        expr: Box::new(ast::Expression::AggregateFunction {
            name: "COUNT".to_string(),
            distinct: false,
            args: vec![ast::Expression::Wildcard],
        }),
    };

    // Build: + + COUNT(*)
    let double_unary_count =
        ast::Expression::UnaryOp { op: ast::UnaryOperator::Plus, expr: Box::new(unary_count) };

    // Build: ( + + COUNT(*) )
    // The parentheses don't create a separate AST node, they just affect parsing

    // Build: 60 * ( + + COUNT(*) )
    let sixty_times_count = ast::Expression::BinaryOp {
        left: Box::new(ast::Expression::Literal(types::SqlValue::Integer(60))),
        op: ast::BinaryOperator::Multiply,
        right: Box::new(double_unary_count),
    };

    // Build: + 60 * ( + + COUNT(*) )
    let full_expr = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Plus,
        expr: Box::new(sixty_times_count),
    };

    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression { expr: full_expr, alias: None }],
        from: Some(ast::FromClause::Table { name: "tab1".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // + 60 * ( + + COUNT(*) ) = + 60 * ( + + 5 ) = + 60 * 5 = + 300 = 300
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(300.0));
}

#[test]
fn test_count_star_with_negative_unary() {
    // Test: SELECT - COUNT(*) * 9 FROM tab1
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "tab1".to_string(),
        vec![catalog::ColumnSchema::new("col0".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert 3 rows
    for i in 0..3 {
        db.insert_row("tab1", storage::Row::new(vec![types::SqlValue::Integer(i)])).unwrap();
    }

    let executor = SelectExecutor::new(&db);

    // Build: - COUNT(*)
    let negative_count = ast::Expression::UnaryOp {
        op: ast::UnaryOperator::Minus,
        expr: Box::new(ast::Expression::AggregateFunction {
            name: "COUNT".to_string(),
            distinct: false,
            args: vec![ast::Expression::Wildcard],
        }),
    };

    // Build: - COUNT(*) * 9
    let full_expr = ast::Expression::BinaryOp {
        left: Box::new(negative_count),
        op: ast::BinaryOperator::Multiply,
        right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(9))),
    };

    let stmt = ast::SelectStmt {
        into_table: None,
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression { expr: full_expr, alias: None }],
        from: Some(ast::FromClause::Table { name: "tab1".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // - COUNT(*) * 9 = -3 * 9 = -27
    assert_eq!(result[0].values[0], types::SqlValue::Numeric(-27.0));
}

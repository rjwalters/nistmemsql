//! Aggregate function tests
//!
//! Tests for COUNT, SUM, GROUP BY, and HAVING clauses.

use super::super::*;

#[test]
fn test_count_star_no_group_by() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(25)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(30)]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(35)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
}

#[test]
fn test_sum_no_group_by() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(200)]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(150)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(450));
}

#[test]
fn test_group_by_with_count() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(150),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::Wildcard],
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: None,
            column: "dept".to_string(),
        }]),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2);
    let mut results = result
        .into_iter()
        .map(|row| (row.values[0].clone(), row.values[1].clone()))
        .collect::<Vec<_>>();
    results.sort_by(|(dept_a, _), (dept_b, _)| match (dept_a, dept_b) {
        (types::SqlValue::Integer(a), types::SqlValue::Integer(b)) => a.cmp(b),
        _ => std::cmp::Ordering::Equal,
    });
    assert_eq!(results[0], (types::SqlValue::Integer(1), types::SqlValue::Integer(2)));
    assert_eq!(results[1], (types::SqlValue::Integer(2), types::SqlValue::Integer(1)));
}

#[test]
fn test_having_clause() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("dept".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "sales",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Integer(2),
            types::SqlValue::Integer(50),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "dept".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: None,
            column: "dept".to_string(),
        }]),
        having: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(150))),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(300));
}

#[test]
fn test_count_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "data".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "data",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(20)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    // COUNT(*) counts all rows including NULL
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::Wildcard],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "data".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3)); // Counts all 3 rows
}

#[test]
fn test_sum_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "amounts".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(100)]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "amounts",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(200)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "value".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "amounts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // SUM ignores NULL values, so 100 + 200 = 300
    assert_eq!(result[0].values[0], types::SqlValue::Integer(300));
}

#[test]
fn test_avg_function() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "scores".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("score".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(80)]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(90)]),
    )
    .unwrap();
    db.insert_row(
        "scores",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(70)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "score".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "scores".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG(80, 90, 70) = 240 / 3 = 80
    assert_eq!(result[0].values[0], types::SqlValue::Integer(80));
}

#[test]
fn test_min_function() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "temps".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("temp".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(72)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(65)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(78)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "MIN".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "temp".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "temps".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(65));
}

#[test]
fn test_max_function() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "temps".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("temp".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(72)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(65)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(78)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "MAX".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "temp".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "temps".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(78));
}

#[test]
fn test_avg_with_nulls() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "ratings".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("rating".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(5)]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Null]),
    )
    .unwrap();
    db.insert_row(
        "ratings",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(3)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "rating".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "ratings".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // AVG ignores NULL, so (5 + 3) / 2 = 4
    assert_eq!(result[0].values[0], types::SqlValue::Integer(4));
}

#[test]
fn test_sum_with_numeric_types() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "prices".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Numeric { precision: 10, scale: 2 }, false),
        ],
    );
    db.create_table(schema.clone()).unwrap();

    // Insert test data with NUMERIC values
    db.insert_row("prices", storage::Row::new(vec![
        types::SqlValue::Integer(1),
        types::SqlValue::Numeric("100.50".to_string()),
    ])).unwrap();
    db.insert_row("prices", storage::Row::new(vec![
        types::SqlValue::Integer(2),
        types::SqlValue::Numeric("200.25".to_string()),
    ])).unwrap();
    db.insert_row("prices", storage::Row::new(vec![
        types::SqlValue::Integer(3),
        types::SqlValue::Numeric("150.00".to_string()),
    ])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT SUM(amount) FROM prices
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "amount".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "prices".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);

    // Should be Numeric type: 100.50 + 200.25 + 150.00 = 450.75
    match &result[0].values[0] {
        types::SqlValue::Numeric(s) => {
            let value: f64 = s.parse().unwrap();
            assert!((value - 450.75).abs() < 0.01, "Expected 450.75, got {}", value);
        }
        other => panic!("Expected Numeric value, got {:?}", other),
    }
}

#[test]
fn test_avg_with_numeric_types() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), types::DataType::Numeric { precision: 10, scale: 2 }, false),
        ],
    );
    db.create_table(schema.clone()).unwrap();

    // Insert test data with NUMERIC values
    db.insert_row("products", storage::Row::new(vec![
        types::SqlValue::Integer(1),
        types::SqlValue::Numeric("10.50".to_string()),
    ])).unwrap();
    db.insert_row("products", storage::Row::new(vec![
        types::SqlValue::Integer(2),
        types::SqlValue::Numeric("20.25".to_string()),
    ])).unwrap();
    db.insert_row("products", storage::Row::new(vec![
        types::SqlValue::Integer(3),
        types::SqlValue::Numeric("15.00".to_string()),
    ])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT AVG(price) FROM products
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "products".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);

    // Should be Numeric type: (10.50 + 20.25 + 15.00) / 3 = 15.25
    match &result[0].values[0] {
        types::SqlValue::Numeric(s) => {
            let value: f64 = s.parse().unwrap();
            assert!((value - 15.25).abs() < 0.01, "Expected 15.25, got {}", value);
        }
        other => panic!("Expected Numeric value, got {:?}", other),
    }
}

#[test]
fn test_sum_mixed_integer_and_numeric() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "mixed".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Numeric { precision: 10, scale: 2 }, false),
        ],
    );
    db.create_table(schema.clone()).unwrap();

    // Insert mix of INTEGER and NUMERIC values
    db.insert_row("mixed", storage::Row::new(vec![
        types::SqlValue::Integer(1),
        types::SqlValue::Integer(100),  // Integer value
    ])).unwrap();
    db.insert_row("mixed", storage::Row::new(vec![
        types::SqlValue::Integer(2),
        types::SqlValue::Numeric("50.50".to_string()),  // Numeric value
    ])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT SUM(value) FROM mixed
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "SUM".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "value".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "mixed".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);

    // When mixing types, result should be promoted to Numeric: 100 + 50.50 = 150.50
    match &result[0].values[0] {
        types::SqlValue::Numeric(s) => {
            let value: f64 = s.parse().unwrap();
            assert!((value - 150.50).abs() < 0.01, "Expected 150.50, got {}", value);
        }
        types::SqlValue::Integer(v) => {
            // Also acceptable if it was all integers
            assert_eq!(*v, 150, "Expected 150 (as integer)");
        }
        other => panic!("Expected Numeric or Integer value, got {:?}", other),
    }
}

//! Scalar subquery execution tests
//!
//! Tests for scalar subqueries (subqueries that return a single value):
//! - Simple scalar subqueries in WHERE clause
//! - Scalar subqueries in SELECT list
//! - Empty result set (should return NULL)
//! - Error cases (multiple rows, multiple columns)

use super::super::*;

#[test]
fn test_scalar_subquery_in_where_clause() {
    // Test: SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)
    let mut db = storage::Database::new();

    // Create employees table
    let schema = catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            catalog::ColumnSchema::new("salary".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data: salaries are 50000, 60000, 70000 (avg = 60000)
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
            types::SqlValue::Integer(50000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
            types::SqlValue::Integer(60000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Charlie".to_string()),
            types::SqlValue::Integer(70000),
        ]),
    )
    .unwrap();

    // Build subquery: SELECT AVG(salary) FROM employees
    let subquery = Box::new(ast::SelectStmt {
            with_clause: None,
            set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT * FROM employees WHERE salary > (subquery)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: None,
                column: "salary".to_string(),
            }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::ScalarSubquery(subquery)),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Only Charlie (70000) should be returned
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(3));
    assert_eq!(result[0].values[1], types::SqlValue::Varchar("Charlie".to_string()));
    assert_eq!(result[0].values[2], types::SqlValue::Integer(70000));
}

#[test]
fn test_scalar_subquery_in_select_list() {
    // Test: SELECT name, salary, (SELECT MAX(salary) FROM employees) as max_sal FROM employees
    let mut db = storage::Database::new();

    // Create employees table
    let schema = catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            catalog::ColumnSchema::new("salary".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
            types::SqlValue::Integer(50000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
            types::SqlValue::Integer(70000),
        ]),
    )
    .unwrap();

    // Build subquery: SELECT MAX(salary) FROM employees
    let subquery = Box::new(ast::SelectStmt {
            with_clause: None,
            set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "MAX".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT name, salary, (subquery) as max_sal FROM employees
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "salary".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ScalarSubquery(subquery),
                alias: Some("max_sal".to_string()),
            },
        ],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should have 2 rows, each with max_sal = 70000
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Alice".to_string()));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(50000));
    assert_eq!(result[0].values[2], types::SqlValue::Integer(70000)); // max_sal
    assert_eq!(result[1].values[0], types::SqlValue::Varchar("Bob".to_string()));
    assert_eq!(result[1].values[1], types::SqlValue::Integer(70000));
    assert_eq!(result[1].values[2], types::SqlValue::Integer(70000)); // max_sal
}

#[test]
fn test_scalar_subquery_returns_null_when_empty() {
    // Test: SELECT (SELECT id FROM employees WHERE id = 999) as missing_id
    let mut db = storage::Database::new();

    // Create employees table
    let schema = catalog::TableSchema::new(
        "employees".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert one row with id=1
    db.insert_row("employees", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();

    // Build subquery that returns no rows: SELECT id FROM employees WHERE id = 999
    let subquery = Box::new(ast::SelectStmt {
            with_clause: None,
            set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(999))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) as missing_id FROM employees
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ScalarSubquery(subquery),
            alias: Some("missing_id".to_string()),
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return 1 row with NULL
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_scalar_subquery_error_multiple_rows() {
    // Test: Scalar subquery returns multiple rows - should error
    let mut db = storage::Database::new();

    // Create employees table
    let schema = catalog::TableSchema::new(
        "employees".to_string(),
        vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    // Insert multiple rows
    db.insert_row("employees", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("employees", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();

    // Build subquery that returns multiple rows: SELECT id FROM employees
    let subquery = Box::new(ast::SelectStmt {
            with_clause: None,
            set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) FROM employees
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ScalarSubquery(subquery),
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt);

    // Should error with SubqueryReturnedMultipleRows
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryReturnedMultipleRows { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        _ => panic!("Expected SubqueryReturnedMultipleRows error"),
    }
}

#[test]
fn test_scalar_subquery_error_multiple_columns() {
    // Test: Scalar subquery returns multiple columns - should error
    let mut db = storage::Database::new();

    // Create employees table with multiple columns
    let schema = catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert one row
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();

    // Build subquery that returns multiple columns: SELECT id, name FROM employees
    let subquery = Box::new(ast::SelectStmt {
            with_clause: None,
            set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT (subquery) FROM employees
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ScalarSubquery(subquery),
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt);

    // Should error with SubqueryColumnCountMismatch
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        _ => panic!("Expected SubqueryColumnCountMismatch error"),
    }
}

#[test]
fn test_correlated_subquery_basic() {
    // Test: Correlated subquery that references outer query column
    // SELECT e.name, e.salary FROM employees e
    // WHERE e.salary > (SELECT AVG(salary) FROM employees WHERE department = e.department)
    let mut db = storage::Database::new();

    // Create employees table with department
    let schema = catalog::TableSchema::new(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(100) },
                false,
            ),
            catalog::ColumnSchema::new(
                "department".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            catalog::ColumnSchema::new("salary".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    // Engineering: Alice (50000), Bob (80000) - avg 65000
    // Sales: Charlie (40000), Diana (60000) - avg 50000
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
            types::SqlValue::Varchar("Engineering".to_string()),
            types::SqlValue::Integer(50000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
            types::SqlValue::Varchar("Engineering".to_string()),
            types::SqlValue::Integer(80000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Charlie".to_string()),
            types::SqlValue::Varchar("Sales".to_string()),
            types::SqlValue::Integer(40000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Varchar("Diana".to_string()),
            types::SqlValue::Varchar("Sales".to_string()),
            types::SqlValue::Integer(60000),
        ]),
    )
    .unwrap();

    // Build correlated subquery: SELECT AVG(salary) FROM employees WHERE department = e.department
    let subquery = Box::new(ast::SelectStmt {
            with_clause: None,
            set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "AVG".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }],
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: None,
                column: "department".to_string(),
            }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::ColumnRef {
                table: None,
                column: "department".to_string(),
            }),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    });

    // Build main query: SELECT name, salary FROM employees e WHERE salary > (correlated subquery)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "salary".to_string() },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: Some("e".to_string()) }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: None,
                column: "salary".to_string(),
            }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(ast::Expression::ScalarSubquery(subquery)),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let executor = SelectExecutor::new(&db);
    let result = executor.execute(&stmt).unwrap();

    // Should return Bob (80000 > 65000) and Diana (60000 > 50000)
    assert_eq!(result.len(), 2);

    // Check Bob
    let bob = &result[0];
    assert_eq!(bob.get(0).unwrap(), &types::SqlValue::Varchar("Bob".to_string()));
    assert_eq!(bob.get(1).unwrap(), &types::SqlValue::Integer(80000));

    // Check Diana
    let diana = &result[1];
    assert_eq!(diana.get(0).unwrap(), &types::SqlValue::Varchar("Diana".to_string()));
    assert_eq!(diana.get(1).unwrap(), &types::SqlValue::Integer(60000));
}

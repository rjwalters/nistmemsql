//! Tests for GROUP BY with JOIN operations and aggregate functions

use crate::SelectExecutor;
use ast::SelectStmt;
use catalog::TableSchema;
use storage::Database;
use types::{DataType, SqlValue};

fn setup_join_test_data(db: &mut Database) {
    // Create departments table
    let dept_schema = TableSchema::new(
        "departments".to_string(),
        vec![
            catalog::ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new(
                "dept_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(dept_schema).unwrap();

    // Create employees table
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("emp_id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new(
                "emp_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            catalog::ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new("salary".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // Insert departments using direct database API
    db.insert_row(
        "departments",
        storage::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Engineering".to_string())]),
    )
    .unwrap();

    db.insert_row(
        "departments",
        storage::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Sales".to_string())]),
    )
    .unwrap();

    db.insert_row(
        "departments",
        storage::Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar("HR".to_string())]),
    )
    .unwrap();

    // Insert employees
    db.insert_row(
        "employees",
        storage::Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(1), // Engineering
            SqlValue::Integer(95000),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        storage::Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(1), // Engineering
            SqlValue::Integer(87000),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        storage::Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(2), // Sales
            SqlValue::Integer(75000),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        storage::Row::new(vec![
            SqlValue::Integer(4),
            SqlValue::Varchar("Diana".to_string()),
            SqlValue::Integer(2), // Sales
            SqlValue::Integer(72000),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        storage::Row::new(vec![
            SqlValue::Integer(5),
            SqlValue::Varchar("Eve".to_string()),
            SqlValue::Integer(3), // HR
            SqlValue::Integer(65000),
        ]),
    )
    .unwrap();
}

#[test]
fn test_inner_join_with_group_by_count() {
    let mut db = Database::new();
    setup_join_test_data(&mut db);

    let executor = SelectExecutor::new(&db);

    // SELECT d.dept_name, COUNT(e.emp_id) as emp_count
    // FROM departments d
    // INNER JOIN employees e ON d.dept_id = e.dept_id
    // GROUP BY d.dept_name
    // ORDER BY emp_count DESC
    let select_stmt = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_name".to_string(),
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: Some("e".to_string()),
                        column: "emp_id".to_string(),
                    }],
                    character_unit: None,
                },
                alias: Some("emp_count".to_string()),
            },
        ],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table {
                name: "departments".to_string(),
                alias: Some("d".to_string()),
            }),
            right: Box::new(ast::FromClause::Table {
                name: "employees".to_string(),
                alias: Some("e".to_string()),
            }),
            join_type: ast::JoinType::Inner,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("e".to_string()),
                    column: "dept_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: Some("d".to_string()),
            column: "dept_name".to_string(),
        }]),
        having: None,
        order_by: None, // ORDER BY with aggregate aliases not yet supported
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Should have 3 departments with employees
    assert_eq!(result.rows.len(), 3);

    // Verify column names
    assert_eq!(result.columns, vec!["dept_name".to_string(), "emp_count".to_string()]);

    // Verify the results (Engineering: 2, Sales: 2, HR: 1)
    // Order is not guaranteed without ORDER BY, so we'll find each department
    let eng_row = result
        .rows
        .iter()
        .find(|r| r.get(0).unwrap() == &SqlValue::Varchar("Engineering".to_string()))
        .unwrap();
    let sales_row = result
        .rows
        .iter()
        .find(|r| r.get(0).unwrap() == &SqlValue::Varchar("Sales".to_string()))
        .unwrap();
    let hr_row = result
        .rows
        .iter()
        .find(|r| r.get(0).unwrap() == &SqlValue::Varchar("HR".to_string()))
        .unwrap();

    assert_eq!(eng_row.get(1).unwrap(), &SqlValue::Integer(2));
    assert_eq!(sales_row.get(1).unwrap(), &SqlValue::Integer(2));
    assert_eq!(hr_row.get(1).unwrap(), &SqlValue::Integer(1));
}

#[test]
fn test_left_join_with_group_by_avg_salary() {
    let mut db = Database::new();
    setup_join_test_data(&mut db);

    let executor = SelectExecutor::new(&db);

    // SELECT d.dept_name, AVG(e.salary) as avg_salary
    // FROM departments d
    // LEFT JOIN employees e ON d.dept_id = e.dept_id
    // GROUP BY d.dept_name
    // ORDER BY avg_salary DESC
    let select_stmt = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_name".to_string(),
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "AVG".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: Some("e".to_string()),
                        column: "salary".to_string(),
                    }],
                    character_unit: None,
                },
                alias: Some("avg_salary".to_string()),
            },
        ],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table {
                name: "departments".to_string(),
                alias: Some("d".to_string()),
            }),
            right: Box::new(ast::FromClause::Table {
                name: "employees".to_string(),
                alias: Some("e".to_string()),
            }),
            join_type: ast::JoinType::LeftOuter,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("e".to_string()),
                    column: "dept_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: Some("d".to_string()),
            column: "dept_name".to_string(),
        }]),
        having: None,
        order_by: None, // ORDER BY with aggregate aliases not yet supported
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Should have all 3 departments (LEFT JOIN includes all departments)
    assert_eq!(result.rows.len(), 3);

    // Verify each department - order not guaranteed
    let engineering_row = result
        .rows
        .iter()
        .find(|r| r.get(0).unwrap() == &SqlValue::Varchar("Engineering".to_string()))
        .unwrap();
    let sales_row = result
        .rows
        .iter()
        .find(|r| r.get(0).unwrap() == &SqlValue::Varchar("Sales".to_string()))
        .unwrap();
    let hr_row = result
        .rows
        .iter()
        .find(|r| r.get(0).unwrap() == &SqlValue::Varchar("HR".to_string()))
        .unwrap();

    // Engineering: (95000 + 87000) / 2 = 91000
    assert_eq!(engineering_row.get(1).unwrap(), &SqlValue::Integer(91000));

    // Sales: (75000 + 72000) / 2 = 73500
    assert_eq!(sales_row.get(1).unwrap(), &SqlValue::Integer(73500));

    // HR: 65000 / 1 = 65000
    assert_eq!(hr_row.get(1).unwrap(), &SqlValue::Integer(65000));
}

#[test]
fn test_join_group_by_with_having() {
    let mut db = Database::new();
    setup_join_test_data(&mut db);

    let executor = SelectExecutor::new(&db);

    // SELECT d.dept_name, COUNT(e.emp_id) as emp_count
    // FROM departments d
    // INNER JOIN employees e ON d.dept_id = e.dept_id
    // GROUP BY d.dept_name
    // HAVING COUNT(e.emp_id) >= 2
    let select_stmt = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_name".to_string(),
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: Some("e".to_string()),
                        column: "emp_id".to_string(),
                    }],
                    character_unit: None,
                },
                alias: Some("emp_count".to_string()),
            },
        ],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table {
                name: "departments".to_string(),
                alias: Some("d".to_string()),
            }),
            right: Box::new(ast::FromClause::Table {
                name: "employees".to_string(),
                alias: Some("e".to_string()),
            }),
            join_type: ast::JoinType::Inner,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("e".to_string()),
                    column: "dept_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: Some("d".to_string()),
            column: "dept_name".to_string(),
        }]),
        having: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::Function {
                name: "COUNT".to_string(),
                args: vec![ast::Expression::ColumnRef {
                    table: Some("e".to_string()),
                    column: "emp_id".to_string(),
                }],
                character_unit: None,
            }),
            op: ast::BinaryOperator::GreaterThanOrEqual,
            right: Box::new(ast::Expression::Literal(SqlValue::Integer(2))),
        }),
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Should have only 2 departments (Engineering and Sales, both with 2+ employees)
    assert_eq!(result.rows.len(), 2);

    // Engineering and Sales should both have count >= 2
    for row in &result.rows {
        let count = row.get(1).unwrap();
        match count {
            SqlValue::Integer(c) => assert!(*c >= 2),
            _ => panic!("Expected integer count"),
        }
    }
}

#[test]
fn test_join_group_by_multiple_aggregates() {
    let mut db = Database::new();
    setup_join_test_data(&mut db);

    let executor = SelectExecutor::new(&db);

    // SELECT d.dept_name, COUNT(*), MIN(e.salary), MAX(e.salary)
    // FROM departments d
    // INNER JOIN employees e ON d.dept_id = e.dept_id
    // GROUP BY d.dept_name
    let select_stmt = SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_name".to_string(),
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::Wildcard],
                    character_unit: None,
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "MIN".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: Some("e".to_string()),
                        column: "salary".to_string(),
                    }],
                    character_unit: None,
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "MAX".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: Some("e".to_string()),
                        column: "salary".to_string(),
                    }],
                    character_unit: None,
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Join {
            left: Box::new(ast::FromClause::Table {
                name: "departments".to_string(),
                alias: Some("d".to_string()),
            }),
            right: Box::new(ast::FromClause::Table {
                name: "employees".to_string(),
                alias: Some("e".to_string()),
            }),
            join_type: ast::JoinType::Inner,
            condition: Some(ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: Some("d".to_string()),
                    column: "dept_id".to_string(),
                }),
                op: ast::BinaryOperator::Equal,
                right: Box::new(ast::Expression::ColumnRef {
                    table: Some("e".to_string()),
                    column: "dept_id".to_string(),
                }),
            }),
        }),
        where_clause: None,
        group_by: Some(vec![ast::Expression::ColumnRef {
            table: Some("d".to_string()),
            column: "dept_name".to_string(),
        }]),
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute_with_columns(&select_stmt).unwrap();

    // Should have 3 departments with employees
    assert_eq!(result.rows.len(), 3);

    // Verify Engineering: count=2, min=87000, max=95000
    let engineering_row = result
        .rows
        .iter()
        .find(|row| row.get(0).unwrap() == &SqlValue::Varchar("Engineering".to_string()))
        .unwrap();
    assert_eq!(engineering_row.get(1).unwrap(), &SqlValue::Integer(2)); // COUNT(*)
    assert_eq!(engineering_row.get(2).unwrap(), &SqlValue::Integer(87000)); // MIN(salary)
    assert_eq!(engineering_row.get(3).unwrap(), &SqlValue::Integer(95000)); // MAX(salary)
}

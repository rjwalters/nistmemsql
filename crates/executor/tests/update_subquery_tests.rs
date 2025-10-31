use ast::{Assignment, BinaryOperator, Expression, UpdateStmt};
use catalog::{ColumnSchema, TableSchema};
use executor::{ExecutorError, UpdateExecutor};
use storage::{Database, Row};
use types::{DataType, SqlValue};

#[test]
fn test_update_with_scalar_subquery_single_value() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE config (max_salary INT)
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("max_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();
    db.insert_row("config", Row::new(vec![SqlValue::Integer(100000)])).unwrap();

    // UPDATE employees SET salary = (SELECT max_salary FROM config)
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,

        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "max_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify salary was updated
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Integer(100000));
}

#[test]
fn test_update_with_scalar_subquery_max_aggregate() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE salaries (amount INT)
    let sal_schema = TableSchema::new(
        "salaries".to_string(),
        vec![ColumnSchema::new("amount".to_string(), DataType::Integer, false)],
    );
    db.create_table(sal_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(60000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(75000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(50000)])).unwrap();

    // UPDATE employees SET salary = (SELECT MAX(amount) FROM salaries)
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "MAX".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "amount".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "salaries".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify salary was updated to MAX
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Integer(75000));
}

#[test]
fn test_update_with_scalar_subquery_min_aggregate() {
    let mut db = Database::new();

    // CREATE TABLE products (id INT, price INT)
    let prod_schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(prod_schema).unwrap();

    // CREATE TABLE prices (amount INT)
    let price_schema = TableSchema::new(
        "prices".to_string(),
        vec![ColumnSchema::new("amount".to_string(), DataType::Integer, false)],
    );
    db.create_table(price_schema).unwrap();

    // Insert data
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]))
        .unwrap();
    db.insert_row("prices", Row::new(vec![SqlValue::Integer(50)])).unwrap();
    db.insert_row("prices", Row::new(vec![SqlValue::Integer(25)])).unwrap();
    db.insert_row("prices", Row::new(vec![SqlValue::Integer(75)])).unwrap();

    // UPDATE products SET price = (SELECT MIN(amount) FROM prices)
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "MIN".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "amount".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "prices".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify price was updated to MIN
    let table = db.get_table("products").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Integer(25));
}

#[test]
fn test_update_with_scalar_subquery_avg_aggregate() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE salaries (amount INT)
    let sal_schema = TableSchema::new(
        "salaries".to_string(),
        vec![ColumnSchema::new("amount".to_string(), DataType::Integer, false)],
    );
    db.create_table(sal_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10000)]))
        .unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(60000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(70000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(50000)])).unwrap();

    // UPDATE employees SET salary = (SELECT AVG(amount) FROM salaries)
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "AVG".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "amount".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "salaries".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify salary was updated to AVG (60000)
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Integer(60000));
}

#[test]
fn test_update_with_scalar_subquery_returns_null() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE config (max_salary INT)
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("max_salary".to_string(), DataType::Integer, true)],
    );
    db.create_table(config_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();
    // Insert NULL value in config
    db.insert_row("config", Row::new(vec![SqlValue::Null])).unwrap();

    // UPDATE employees SET salary = (SELECT max_salary FROM config)
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,

        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "max_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify salary was updated to NULL
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Null);
}

#[test]
fn test_update_with_scalar_subquery_empty_result() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE config (max_salary INT)
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("max_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();

    // Insert employee but NO config rows
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();

    // UPDATE employees SET salary = (SELECT max_salary FROM config) -- returns NULL
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,

        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "max_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify salary was updated to NULL (empty result set)
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Null);
}

#[test]
fn test_update_with_multiple_subqueries() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, min_sal INT, max_sal INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("min_sal".to_string(), DataType::Integer, true),
            ColumnSchema::new("max_sal".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE salaries (amount INT)
    let sal_schema = TableSchema::new(
        "salaries".to_string(),
        vec![ColumnSchema::new("amount".to_string(), DataType::Integer, false)],
    );
    db.create_table(sal_schema).unwrap();

    // Insert data
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Null, SqlValue::Null]),
    )
    .unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(40000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(80000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(60000)])).unwrap();

    // UPDATE employees SET min_sal = (SELECT MIN(amount) FROM salaries), max_sal = (SELECT MAX(amount) FROM salaries)
    let min_subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "MIN".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "amount".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "salaries".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let max_subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "MAX".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "amount".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "salaries".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![
            Assignment {
                column: "min_sal".to_string(),
                value: Expression::ScalarSubquery(min_subquery),
            },
            Assignment {
                column: "max_sal".to_string(),
                value: Expression::ScalarSubquery(max_subquery),
            },
        ],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify both columns were updated
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1).unwrap(), &SqlValue::Integer(40000)); // min_sal
    assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(80000)); // max_sal
}

#[test]
fn test_update_with_subquery_multiple_rows_error() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE salaries (amount INT)
    let sal_schema = TableSchema::new(
        "salaries".to_string(),
        vec![ColumnSchema::new("amount".to_string(), DataType::Integer, false)],
    );
    db.create_table(sal_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(60000)])).unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(70000)])).unwrap();

    // UPDATE employees SET salary = (SELECT amount FROM salaries) -- ERROR: multiple rows
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,

        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "amount".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "salaries".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryReturnedMultipleRows { .. } => {
            // Expected error
        }
        other => panic!("Expected SubqueryReturnedMultipleRows, got {:?}", other),
    }
}

#[test]
fn test_update_with_subquery_multiple_columns_error() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE salaries (min_amt INT, max_amt INT)
    let sal_schema = TableSchema::new(
        "salaries".to_string(),
        vec![
            ColumnSchema::new("min_amt".to_string(), DataType::Integer, false),
            ColumnSchema::new("max_amt".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(sal_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(45000)]))
        .unwrap();
    db.insert_row("salaries", Row::new(vec![SqlValue::Integer(50000), SqlValue::Integer(100000)]))
        .unwrap();

    // UPDATE employees SET salary = (SELECT min_amt, max_amt FROM salaries) -- ERROR: 2 columns
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,

        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "min_amt".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: Expression::ColumnRef { table: None, column: "max_amt".to_string() },
                alias: None,
            },
        ],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "salaries".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::SubqueryColumnCountMismatch { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        other => panic!("Expected SubqueryColumnCountMismatch, got {:?}", other),
    }
}

#[test]
fn test_update_with_subquery_updates_multiple_rows() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE config (base_salary INT)
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("base_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(40000)]))
        .unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(45000)]))
        .unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(50000)]))
        .unwrap();
    db.insert_row("config", Row::new(vec![SqlValue::Integer(55000)])).unwrap();

    // UPDATE employees SET salary = (SELECT base_salary FROM config) -- all rows
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,

        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "base_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 3);

    // Verify all employees have the new salary
    let table = db.get_table("employees").unwrap();
    for row in table.scan() {
        assert_eq!(row.get(1).unwrap(), &SqlValue::Integer(55000));
    }
}

#[test]
fn test_update_with_subquery_and_where_clause() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT)
    let emp_schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(emp_schema).unwrap();

    // CREATE TABLE config (max_salary INT)
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("max_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();

    // Insert data
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(40000)]))
        .unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(50000)]))
        .unwrap();
    db.insert_row("config", Row::new(vec![SqlValue::Integer(45000)])).unwrap();

    // UPDATE employees SET salary = (SELECT max_salary FROM config) WHERE id = 1
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,

        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "max_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(subquery),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify only employee 1 was updated
    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(45000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(50000)); // Not updated
}

// UPDATE WHERE with Subquery Tests (Issue #353)

#[test]
fn test_update_where_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000), SqlValue::Integer(20)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(70000), SqlValue::Integer(10)]),
    )
    .unwrap();

    // Create departments table
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // Build subquery: SELECT dept_id FROM active_depts
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "active_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 80000 WHERE dept_id IN (SELECT dept_id FROM active_depts)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(80000)),
        }],
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 2); // Employees 1 and 3 in dept 10

    // Verify updates
    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(80000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
    assert_eq!(rows[2].get(1).unwrap(), &SqlValue::Integer(80000)); // Updated
}

#[test]
fn test_update_where_not_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Boolean(true), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Boolean(true), SqlValue::Integer(20)]),
    )
    .unwrap();

    // Create active departments
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // Subquery
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "active_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET active = FALSE WHERE dept_id NOT IN (SELECT dept_id FROM active_depts)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "active".to_string(),
            value: Expression::Literal(SqlValue::Boolean(false)),
        }],
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: true,
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 2 not in active depts

    // Verify
    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Boolean(true)); // Not updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Boolean(false)); // Updated
}

#[test]
fn test_update_where_scalar_subquery_equal() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000)]))
        .unwrap();
    db.insert_row("employees", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000)]))
        .unwrap();

    // Create config table
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("min_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();
    db.insert_row("config", Row::new(vec![SqlValue::Integer(50000)])).unwrap();

    // Subquery: SELECT min_salary FROM config
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "min_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 55000 WHERE salary = (SELECT min_salary FROM config)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(55000)),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(55000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
}

#[test]
fn test_update_where_scalar_subquery_less_than() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("bonus".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(40000), SqlValue::Integer(0)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(70000), SqlValue::Integer(0)]),
    )
    .unwrap();

    // Subquery: SELECT AVG(salary) FROM employees
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "AVG".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "salary".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET bonus = 5000 WHERE salary < (SELECT AVG(salary) FROM employees)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            value: Expression::Literal(SqlValue::Integer(5000)),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: ast::BinaryOperator::LessThan,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1 (40000 < 55000)

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Integer(5000)); // Updated
    assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Integer(0)); // Not updated
}

#[test]
fn test_update_where_subquery_empty_result() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10), SqlValue::Boolean(true)]),
    )
    .unwrap();

    // Create empty table
    let dept_schema = TableSchema::new(
        "inactive_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();

    // Subquery returns empty result
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "inactive_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET active = FALSE WHERE dept_id IN (SELECT dept_id FROM inactive_depts)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "active".to_string(),
            value: Expression::Literal(SqlValue::Boolean(false)),
        }],
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 0); // No rows updated (empty IN list)

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Boolean(true)); // Not updated
}

#[test]
fn test_update_where_subquery_returns_null() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row("employees", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000)]))
        .unwrap();

    // Create config table with no rows
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("max_salary".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();

    // Subquery returns NULL (empty result)
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "max_salary".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 60000 WHERE salary < (SELECT max_salary FROM config)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(60000)),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: ast::BinaryOperator::LessThan,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 0); // No rows updated (NULL comparison is always FALSE/UNKNOWN)

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(50000)); // Not updated
}

#[test]
fn test_update_where_subquery_with_aggregate() {
    let mut db = Database::new();

    // Create items table
    let schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("price".to_string(), DataType::Integer, false),
            ColumnSchema::new("discounted".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "items",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100), SqlValue::Boolean(false)]),
    )
    .unwrap();
    db.insert_row(
        "items",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(50), SqlValue::Boolean(false)]),
    )
    .unwrap();
    db.insert_row(
        "items",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(200), SqlValue::Boolean(false)]),
    )
    .unwrap();

    // Subquery: SELECT MAX(price) FROM items
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::Function {
                name: "MAX".to_string(),
                args: vec![Expression::ColumnRef { table: None, column: "price".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "items".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE items SET discounted = TRUE WHERE price = (SELECT MAX(price) FROM items)
    let stmt = ast::UpdateStmt {
        table_name: "items".to_string(),
        assignments: vec![Assignment {
            column: "discounted".to_string(),
            value: Expression::Literal(SqlValue::Boolean(true)),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Item 3 with price 200

    let table = db.get_table("items").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Boolean(false));
    assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Boolean(false));
    assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Boolean(true)); // Updated
}

#[test]
fn test_update_where_complex_subquery_condition() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000), SqlValue::Integer(20)]),
    )
    .unwrap();

    // Create departments table
    let dept_schema = TableSchema::new(
        "departments".to_string(),
        vec![
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("budget".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("departments", Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(100000)]))
        .unwrap();
    db.insert_row("departments", Row::new(vec![SqlValue::Integer(20), SqlValue::Integer(50000)]))
        .unwrap();

    // Subquery: SELECT dept_id FROM departments WHERE budget > 80000
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "departments".to_string(), alias: None }),
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "budget".to_string() }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(80000))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = 70000 WHERE dept_id IN (SELECT dept_id FROM departments WHERE budget > 80000)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Integer(70000)),
        }],
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1 in dept 10

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(70000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
}

#[test]
fn test_update_where_multiple_rows_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("active".to_string(), DataType::Boolean, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10), SqlValue::Boolean(true)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20), SqlValue::Boolean(true)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(30), SqlValue::Boolean(true)]),
    )
    .unwrap();

    // Create departments table with multiple rows
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(20)])).unwrap();

    // Subquery returns multiple rows (valid for IN)
    let subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "active_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET active = FALSE WHERE dept_id IN (SELECT dept_id FROM active_depts)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "active".to_string(),
            value: Expression::Literal(SqlValue::Boolean(false)),
        }],
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 2); // Employees 1 and 2

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Boolean(false)); // Updated
    assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Boolean(false)); // Updated
    assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Boolean(true)); // Not updated
}

#[test]
fn test_update_where_and_set_both_use_subqueries() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10)]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(60000), SqlValue::Integer(20)]),
    )
    .unwrap();

    // Create salary_targets table
    let targets_schema = TableSchema::new(
        "salary_targets".to_string(),
        vec![ColumnSchema::new("target".to_string(), DataType::Integer, false)],
    );
    db.create_table(targets_schema).unwrap();
    db.insert_row("salary_targets", Row::new(vec![SqlValue::Integer(70000)])).unwrap();

    // Create active_depts table
    let dept_schema = TableSchema::new(
        "active_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("active_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // SET subquery
    let set_subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "target".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "salary_targets".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // WHERE subquery
    let where_subquery = Box::new(ast::SelectStmt {
        into_table: None,
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "active_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // UPDATE employees SET salary = (SELECT target FROM salary_targets) WHERE dept_id IN (SELECT dept_id FROM active_depts)
    let stmt = ast::UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::ScalarSubquery(set_subquery),
        }],
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery: where_subquery,
            negated: false,
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1); // Employee 1

    let table = db.get_table("employees").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Integer(70000)); // Updated
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Integer(60000)); // Not updated
}

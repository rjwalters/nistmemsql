use ast::{Assignment, Expression, UpdateStmt};
use catalog::{ColumnSchema, TableSchema};
use executor::UpdateExecutor;
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
            op: ast::BinaryOperator::Equal,
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

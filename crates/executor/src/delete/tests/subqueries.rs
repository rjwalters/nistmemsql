//! DELETE with subquery tests (Issue #353)

use super::super::executor::DeleteExecutor;
use ast::Expression;
use catalog::{ColumnSchema, TableSchema};
use storage::{Database, Row};
use types::{DataType, SqlValue};

#[test]
fn test_delete_where_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(20),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(10),
        ]),
    )
    .unwrap();

    // Create inactive departments table
    let dept_schema = TableSchema::new(
        "inactive_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();
    db.insert_row("inactive_depts", Row::new(vec![SqlValue::Integer(10)])).unwrap();

    // Subquery: SELECT dept_id FROM inactive_depts
    let subquery = Box::new(ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "inactive_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // DELETE FROM employees WHERE dept_id IN (SELECT dept_id FROM inactive_depts)
    let stmt = ast::DeleteStmt {
        table_name: "employees".to_string(),
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        }),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 2); // Alice and Charlie

    // Verify only Bob remains
    let table = db.get_table("employees").unwrap();
    assert_eq!(table.row_count(), 1);
    let remaining = &table.scan()[0];
    assert_eq!(remaining.get(1).unwrap(), &SqlValue::Varchar("Bob".to_string()));
}

#[test]
fn test_delete_where_not_in_subquery() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(20),
        ]),
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
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "active_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // DELETE FROM employees WHERE dept_id NOT IN (SELECT dept_id FROM active_depts)
    let stmt = ast::DeleteStmt {
        table_name: "employees".to_string(),
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: true,
        }),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 1); // Bob in inactive dept

    // Verify only Alice remains
    let table = db.get_table("employees").unwrap();
    assert_eq!(table.row_count(), 1);
    let remaining = &table.scan()[0];
    assert_eq!(remaining.get(1).unwrap(), &SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_delete_where_scalar_subquery_comparison() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(40000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(60000),
        ]),
    )
    .unwrap();
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(70000),
        ]),
    )
    .unwrap();

    // Subquery: SELECT AVG(salary) FROM employees
    let subquery = Box::new(ast::SelectStmt {
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
        from: Some(ast::FromClause::Table { name: "employees".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // DELETE FROM employees WHERE salary < (SELECT AVG(salary) FROM employees)
    let stmt = ast::DeleteStmt {
        table_name: "employees".to_string(),
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: ast::BinaryOperator::LessThan,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        }),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 1); // Alice (40000 < avg 56666)

    // Verify Bob and Charlie remain
    let table = db.get_table("employees").unwrap();
    assert_eq!(table.row_count(), 2);
    let names: Vec<String> = table
        .scan()
        .iter()
        .map(|row| {
            if let SqlValue::Varchar(name) = row.get(1).unwrap() {
                name.clone()
            } else {
                String::new()
            }
        })
        .collect();
    assert!(names.contains(&"Bob".to_string()));
    assert!(names.contains(&"Charlie".to_string()));
}

#[test]
fn test_delete_where_subquery_empty_result() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("dept_id".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(10),
        ]),
    )
    .unwrap();

    // Create empty departments table
    let dept_schema = TableSchema::new(
        "old_depts".to_string(),
        vec![ColumnSchema::new("dept_id".to_string(), DataType::Integer, false)],
    );
    db.create_table(dept_schema).unwrap();

    // Subquery returns empty result
    let subquery = Box::new(ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "dept_id".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "old_depts".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // DELETE FROM employees WHERE dept_id IN (SELECT dept_id FROM old_depts)
    let stmt = ast::DeleteStmt {
        table_name: "employees".to_string(),
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef { table: None, column: "dept_id".to_string() }),
            subquery,
            negated: false,
        }),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 0); // No rows deleted

    let table = db.get_table("employees").unwrap();
    assert_eq!(table.row_count(), 1); // Alice still exists
}

#[test]
fn test_delete_where_subquery_with_aggregate_max() {
    let mut db = Database::new();

    // Create items table
    let schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("price".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "items",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Widget".to_string()),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Gadget".to_string()),
            SqlValue::Integer(200),
        ]),
    )
    .unwrap();
    db.insert_row(
        "items",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Doohickey".to_string()),
            SqlValue::Integer(150),
        ]),
    )
    .unwrap();

    // Subquery: SELECT MAX(price) FROM items
    let subquery = Box::new(ast::SelectStmt {
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
        from: Some(ast::FromClause::Table { name: "items".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // DELETE FROM items WHERE price = (SELECT MAX(price) FROM items)
    let stmt = ast::DeleteStmt {
        table_name: "items".to_string(),
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        }),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 1); // Gadget with price 200

    // Verify Widget and Doohickey remain
    let table = db.get_table("items").unwrap();
    assert_eq!(table.row_count(), 2);
    let prices: Vec<i64> = table
        .scan()
        .iter()
        .map(|row| if let SqlValue::Integer(price) = row.get(2).unwrap() { *price } else { 0 })
        .collect();
    assert!(prices.contains(&100));
    assert!(prices.contains(&150));
}

#[test]
fn test_delete_where_complex_subquery_with_filter() {
    let mut db = Database::new();

    // Create orders table
    let schema = TableSchema::new(
        "orders".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(101), SqlValue::Integer(50)]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(102), SqlValue::Integer(75)]),
    )
    .unwrap();
    db.insert_row(
        "orders",
        Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(103), SqlValue::Integer(120)]),
    )
    .unwrap();

    // Create inactive customers table
    let customer_schema = TableSchema::new(
        "inactive_customers".to_string(),
        vec![
            ColumnSchema::new("customer_id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "status".to_string(),
                DataType::Varchar { max_length: Some(20) },
                false,
            ),
        ],
    );
    db.create_table(customer_schema).unwrap();
    db.insert_row(
        "inactive_customers",
        Row::new(vec![SqlValue::Integer(101), SqlValue::Varchar("inactive".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "inactive_customers",
        Row::new(vec![SqlValue::Integer(102), SqlValue::Varchar("inactive".to_string())]),
    )
    .unwrap();

    // Subquery: SELECT customer_id FROM inactive_customers WHERE status = 'inactive'
    let subquery = Box::new(ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "customer_id".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "inactive_customers".to_string(), alias: None }),
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "status".to_string() }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Varchar("inactive".to_string()))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // DELETE FROM orders WHERE customer_id IN (SELECT customer_id FROM inactive_customers WHERE status = 'inactive')
    let stmt = ast::DeleteStmt {
        table_name: "orders".to_string(),
        where_clause: Some(Expression::In {
            expr: Box::new(Expression::ColumnRef {
                table: None,
                column: "customer_id".to_string(),
            }),
            subquery,
            negated: false,
        }),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 2); // Orders 1 and 2

    // Verify only order 3 remains
    let table = db.get_table("orders").unwrap();
    assert_eq!(table.row_count(), 1);
    let remaining = &table.scan()[0];
    assert_eq!(remaining.get(0).unwrap(), &SqlValue::Integer(3));
}

#[test]
fn test_delete_where_subquery_returns_null() {
    let mut db = Database::new();

    // Create employees table
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new("salary".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(50000),
        ]),
    )
    .unwrap();

    // Create empty config table
    let config_schema = TableSchema::new(
        "config".to_string(),
        vec![ColumnSchema::new("threshold".to_string(), DataType::Integer, false)],
    );
    db.create_table(config_schema).unwrap();

    // Subquery returns NULL (empty result)
    let subquery = Box::new(ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: Expression::ColumnRef { table: None, column: "threshold".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "config".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    });

    // DELETE FROM employees WHERE salary > (SELECT threshold FROM config)
    let stmt = ast::DeleteStmt {
        table_name: "employees".to_string(),
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "salary".to_string() }),
            op: ast::BinaryOperator::GreaterThan,
            right: Box::new(Expression::ScalarSubquery(subquery)),
        }),
    };

    let deleted = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(deleted, 0); // No rows deleted (NULL comparison always FALSE/UNKNOWN)

    let table = db.get_table("employees").unwrap();
    assert_eq!(table.row_count(), 1); // Alice still exists
}

mod common;

use ast::{Assignment, BinaryOperator, Expression, UpdateStmt};
use common::setup_test_table;
use executor::{ExecutorError, UpdateExecutor};
use storage::{Database, Row};
use types::{DataType, SqlValue};

#[test]
fn test_update_not_null_constraint_violation() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Try to set name (NOT NULL column) to NULL
    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("NOT NULL"));
            assert!(msg.contains("name"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_nullable_column_to_null() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Set salary (nullable column) to NULL - should succeed
    let stmt = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "salary".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify salary was set to NULL
    let table = db.get_table("employees").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(2).unwrap(), &SqlValue::Null);
}

#[test]
fn test_update_primary_key_duplicate() {
    let mut db = Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
    let schema = catalog::TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert two rows
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
    )
    .unwrap();

    // Try to update Bob's id to 1 (duplicate)
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "id".to_string(),
            value: Expression::Literal(SqlValue::Integer(1)),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("PRIMARY KEY"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_primary_key_to_unique_value() {
    let mut db = Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
    let schema = catalog::TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert two rows
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
    )
    .unwrap();

    // Update Bob's id to 3 (unique) - should succeed
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "id".to_string(),
            value: Expression::Literal(SqlValue::Integer(3)),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify the update
    let table = db.get_table("users").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[1].get(0).unwrap(), &SqlValue::Integer(3));
}

#[test]
fn test_update_unique_constraint_duplicate() {
    let mut db = storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE)
    let schema = catalog::TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "email".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()]],
    );
    db.create_table(schema).unwrap();

    // Insert two users
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("alice@example.com".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("bob@example.com".to_string())]),
    )
    .unwrap();

    // Try to update Bob's email to Alice's email (should fail - UNIQUE violation)
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "email".to_string(),
            value: Expression::Literal(SqlValue::Varchar("alice@example.com".to_string())),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE"));
            assert!(msg.contains("email"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_unique_constraint_to_unique_value() {
    let mut db = storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE)
    let schema = catalog::TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "email".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()]],
    );
    db.create_table(schema).unwrap();

    // Insert two users
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("alice@example.com".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("bob@example.com".to_string())]),
    )
    .unwrap();

    // Update Bob's email to a new unique value - should succeed
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "email".to_string(),
            value: Expression::Literal(SqlValue::Varchar("robert@example.com".to_string())),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify the update
    let table = db.get_table("users").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Varchar("robert@example.com".to_string()));
}

#[test]
fn test_update_unique_constraint_allows_null() {
    let mut db = storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE)
    let schema = catalog::TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "email".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true, // nullable
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()]],
    );
    db.create_table(schema).unwrap();

    // Insert two users with email
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("alice@example.com".to_string())]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("bob@example.com".to_string())]),
    )
    .unwrap();

    // Update first user's email to NULL - should succeed
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "email".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Update second user's email to NULL too - should succeed (multiple NULLs allowed)
    let stmt2 = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "email".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let count2 = UpdateExecutor::execute(&stmt2, &mut db).unwrap();
    assert_eq!(count2, 1);

    // Verify both emails are NULL
    let table = db.get_table("users").unwrap();
    let rows: Vec<&Row> = table.scan().iter().collect();
    assert_eq!(rows[0].get(1).unwrap(), &SqlValue::Null);
    assert_eq!(rows[1].get(1).unwrap(), &SqlValue::Null);
}

#[test]
fn test_update_unique_constraint_composite() {
    let mut db = storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, first_name VARCHAR(50), last_name VARCHAR(50), UNIQUE(first_name, last_name))
    let schema = catalog::TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "first_name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
            catalog::ColumnSchema::new(
                "last_name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["first_name".to_string(), "last_name".to_string()]],
    );
    db.create_table(schema).unwrap();

    // Insert two users
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Varchar("Smith".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Varchar("Jones".to_string()),
        ]),
    )
    .unwrap();

    // Try to update Bob to have the same first_name and last_name as Alice (should fail)
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![
            Assignment {
                column: "first_name".to_string(),
                value: Expression::Literal(SqlValue::Varchar("Alice".to_string())),
            },
            Assignment {
                column: "last_name".to_string(),
                value: Expression::Literal(SqlValue::Varchar("Smith".to_string())),
            },
        ],
        where_clause: Some(Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_check_constraint_passes() {
    let mut db = Database::new();

    // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), DataType::Integer, false),
        ],
        None,
        Vec::new(),
        vec![(
            "price_positive".to_string(),
            ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(ast::Expression::Literal(SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Insert a row
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Update to valid price (should succeed)
    let stmt = UpdateStmt {
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Integer(100)),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_update_check_constraint_violation() {
    let mut db = Database::new();

    // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), DataType::Integer, false),
        ],
        None,
        Vec::new(),
        vec![(
            "price_positive".to_string(),
            ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(ast::Expression::Literal(SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Insert a row
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Try to update to negative price (should fail)
    let stmt = UpdateStmt {
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Integer(-10)),
        }],
        where_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("CHECK"));
            assert!(msg.contains("price_positive"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_update_check_constraint_with_null() {
    let mut db = Database::new();

    // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), DataType::Integer, true), // nullable
        ],
        None,
        Vec::new(),
        vec![(
            "price_positive".to_string(),
            ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "price".to_string(),
                }),
                op: ast::BinaryOperator::GreaterThanOrEqual,
                right: Box::new(ast::Expression::Literal(SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Insert a row
    db.insert_row("products", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50)])).unwrap();

    // Update to NULL (should succeed - NULL is treated as UNKNOWN which passes CHECK)
    let stmt = UpdateStmt {
        table_name: "products".to_string(),
        assignments: vec![Assignment {
            column: "price".to_string(),
            value: Expression::Literal(SqlValue::Null),
        }],
        where_clause: None,
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_update_check_constraint_with_expression() {
    let mut db = Database::new();

    // CREATE TABLE employees (id INT, salary INT, bonus INT, CHECK (bonus < salary))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new("salary".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new("bonus".to_string(), DataType::Integer, false),
        ],
        None,
        Vec::new(),
        vec![(
            "bonus_less_than_salary".to_string(),
            ast::Expression::BinaryOp {
                left: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "bonus".to_string(),
                }),
                op: ast::BinaryOperator::LessThan,
                right: Box::new(ast::Expression::ColumnRef {
                    table: None,
                    column: "salary".to_string(),
                }),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Insert a row
    db.insert_row(
        "employees",
        Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000), SqlValue::Integer(10000)]),
    )
    .unwrap();

    // Update bonus to still be less than salary (should succeed)
    let stmt1 = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            value: Expression::Literal(SqlValue::Integer(15000)),
        }],
        where_clause: None,
    };
    let count = UpdateExecutor::execute(&stmt1, &mut db).unwrap();
    assert_eq!(count, 1);

    // Try to update bonus to be >= salary (should fail)
    let stmt2 = UpdateStmt {
        table_name: "employees".to_string(),
        assignments: vec![Assignment {
            column: "bonus".to_string(),
            value: Expression::Literal(SqlValue::Integer(60000)),
        }],
        where_clause: None,
    };

    let result = UpdateExecutor::execute(&stmt2, &mut db);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("CHECK"));
            assert!(msg.contains("bonus_less_than_salary"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

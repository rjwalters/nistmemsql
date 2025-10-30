use ast;
use catalog;
use executor::{ExecutorError, InsertExecutor};
use storage;
use types;

fn setup_test_table(db: &mut storage::Database) {
    // CREATE TABLE users (id INT, name VARCHAR(50))
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );
    db.create_table(schema).unwrap();
}

#[test]
fn test_insert_not_null_constraint_with_column_list() {
    let mut db = storage::Database::new();

    // CREATE TABLE test (id INT NOT NULL, name VARCHAR(50) NOT NULL, age INT)
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                false,
            ),
            catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO test (id, age) VALUES (1, 25)
    // name is NOT NULL but not provided, should fail
    let stmt = ast::InsertStmt {
        table_name: "test".to_string(),
        columns: vec!["id".to_string(), "age".to_string()],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(25)),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("NOT NULL constraint violation"));
            assert!(msg.contains("column 'name'"));
            assert!(msg.contains("table 'test'"));
            assert!(msg.contains("cannot be NULL"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_insert_primary_key_duplicate_single_column() {
    let mut db = storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
    let schema = catalog::TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert first row
    let stmt1 = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row with duplicate id
    let stmt2 = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("PRIMARY KEY"));
            assert!(msg.contains("id"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_insert_primary_key_duplicate_composite() {
    let mut db = storage::Database::new();

    // CREATE TABLE order_items (order_id INT, item_id INT, qty INT, PRIMARY KEY (order_id, item_id))
    let schema = catalog::TableSchema::with_primary_key(
        "order_items".to_string(),
        vec![
            catalog::ColumnSchema::new("order_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("item_id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("qty".to_string(), types::DataType::Integer, true),
        ],
        vec!["order_id".to_string(), "item_id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert first row (order_id=1, item_id=100)
    let stmt1 = ast::InsertStmt {
        table_name: "order_items".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
            ast::Expression::Literal(types::SqlValue::Integer(5)),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Insert row with different combination (order_id=1, item_id=200) - should succeed
    let stmt2 = ast::InsertStmt {
        table_name: "order_items".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(200)),
            ast::Expression::Literal(types::SqlValue::Integer(3)),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt2).unwrap();

    // Try to insert duplicate composite key (order_id=1, item_id=100)
    let stmt3 = ast::InsertStmt {
        table_name: "order_items".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
            ast::Expression::Literal(types::SqlValue::Integer(10)),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("PRIMARY KEY"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_insert_primary_key_unique_values() {
    let mut db = storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
    let schema = catalog::TableSchema::with_primary_key(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();

    // Insert rows with unique ids - should all succeed
    for i in 1..=3 {
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(i)),
                ast::Expression::Literal(types::SqlValue::Varchar(format!("User{}", i))),
            ]]),
        };
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Verify all 3 rows inserted
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_insert_unique_constraint_duplicate() {
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

    // Insert first row
    let stmt1 = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("alice@example.com".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row with duplicate email
    let stmt2 = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("alice@example.com".to_string())),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt2);
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
fn test_insert_unique_constraint_allows_null() {
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

    // Insert multiple rows with NULL email - should all succeed
    // (NULL != NULL in SQL, so multiple NULLs are allowed)
    for i in 1..=3 {
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(i)),
                ast::Expression::Literal(types::SqlValue::Null),
            ]]),
        };
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Verify all 3 rows inserted
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn test_insert_unique_constraint_composite() {
    let mut db = storage::Database::new();

    // CREATE TABLE enrollments (student_id INT, course_id INT, UNIQUE (student_id, course_id))
    let schema = catalog::TableSchema::with_unique_constraints(
        "enrollments".to_string(),
        vec![
            catalog::ColumnSchema::new("student_id".to_string(), types::DataType::Integer, true),
            catalog::ColumnSchema::new("course_id".to_string(), types::DataType::Integer, true),
            catalog::ColumnSchema::new("grade".to_string(), types::DataType::Integer, true),
        ],
        vec![vec!["student_id".to_string(), "course_id".to_string()]],
    );
    db.create_table(schema).unwrap();

    // Insert first enrollment (student=1, course=101)
    let stmt1 = ast::InsertStmt {
        table_name: "enrollments".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(101)),
            ast::Expression::Literal(types::SqlValue::Integer(85)),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Insert different combination (student=1, course=102) - should succeed
    let stmt2 = ast::InsertStmt {
        table_name: "enrollments".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(102)),
            ast::Expression::Literal(types::SqlValue::Integer(90)),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt2).unwrap();

    // Try to insert duplicate combination (student=1, course=101)
    let stmt3 = ast::InsertStmt {
        table_name: "enrollments".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(101)),
            ast::Expression::Literal(types::SqlValue::Integer(95)),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_insert_multiple_unique_constraints() {
    let mut db = storage::Database::new();

    // CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(50) UNIQUE, username VARCHAR(50) UNIQUE)
    let schema = catalog::TableSchema::with_all_constraints(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "email".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
            catalog::ColumnSchema::new(
                "username".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
        Some(vec!["id".to_string()]),
        vec![vec!["email".to_string()], vec!["username".to_string()]],
    );
    db.create_table(schema).unwrap();

    // Insert first user
    let stmt1 = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("alice@example.com".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert with duplicate email (different username)
    let stmt2 = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("alice@example.com".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("bob".to_string())),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE"));
            assert!(msg.contains("email"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }

    // Try to insert with duplicate username (different email)
    let stmt3 = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Varchar("bob@example.com".to_string())),
            ast::Expression::Literal(types::SqlValue::Varchar("alice".to_string())),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("UNIQUE"));
            assert!(msg.contains("username"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_insert_check_constraint_passes() {
    let mut db = storage::Database::new();

    // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), types::DataType::Integer, false),
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
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Insert valid row (price >= 0)
    let stmt = ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
        ]]),
    };

    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Verify row inserted
    let table = db.get_table("products").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_check_constraint_violation() {
    let mut db = storage::Database::new();

    // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), types::DataType::Integer, false),
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
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Try to insert row with negative price (should fail)
    let stmt = ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(-10)),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt);
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
fn test_insert_check_constraint_with_null() {
    let mut db = storage::Database::new();

    // CREATE TABLE products (id INT, price INT CHECK (price >= 0))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), types::DataType::Integer, true), // nullable
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
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(0))),
            },
        )],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Insert row with NULL price - should succeed
    // (NULL in CHECK constraint evaluates to UNKNOWN, which is treated as TRUE)
    let stmt = ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Null),
        ]]),
    };

    InsertExecutor::execute(&mut db, &stmt).unwrap();

    // Verify row inserted
    let table = db.get_table("products").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_multiple_check_constraints() {
    let mut db = storage::Database::new();

    // CREATE TABLE products (id INT, price INT, quantity INT,
    //                        CHECK (price >= 0), CHECK (quantity >= 0))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("price".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("quantity".to_string(), types::DataType::Integer, false),
        ],
        None,
        Vec::new(),
        vec![
            (
                "price_positive".to_string(),
                ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: None,
                        column: "price".to_string(),
                    }),
                    op: ast::BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(0))),
                },
            ),
            (
                "quantity_positive".to_string(),
                ast::Expression::BinaryOp {
                    left: Box::new(ast::Expression::ColumnRef {
                        table: None,
                        column: "quantity".to_string(),
                    }),
                    op: ast::BinaryOperator::GreaterThanOrEqual,
                    right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(0))),
                },
            ),
        ],
        Vec::new(),
    );
    db.create_table(schema).unwrap();

    // Insert valid row
    let stmt1 = ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
            ast::Expression::Literal(types::SqlValue::Integer(50)),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row with negative price (violates first CHECK)
    let stmt2 = ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(-10)),
            ast::Expression::Literal(types::SqlValue::Integer(50)),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("CHECK"));
            assert!(msg.contains("price_positive"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }

    // Try to insert row with negative quantity (violates second CHECK)
    let stmt3 = ast::InsertStmt {
        table_name: "products".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(3)),
            ast::Expression::Literal(types::SqlValue::Integer(100)),
            ast::Expression::Literal(types::SqlValue::Integer(-5)),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt3);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("CHECK"));
            assert!(msg.contains("quantity_positive"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

#[test]
fn test_insert_check_constraint_with_expression() {
    let mut db = storage::Database::new();

    // CREATE TABLE employees (id INT, salary INT, bonus INT,
    //                         CHECK (bonus < salary))
    let schema = catalog::TableSchema::with_all_constraint_types(
        "employees".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("salary".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("bonus".to_string(), types::DataType::Integer, false),
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

    // Insert valid row (bonus < salary)
    let stmt1 = ast::InsertStmt {
        table_name: "employees".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Integer(50000)),
            ast::Expression::Literal(types::SqlValue::Integer(10000)),
        ]]),
    };
    InsertExecutor::execute(&mut db, &stmt1).unwrap();

    // Try to insert row where bonus >= salary (should fail)
    let stmt2 = ast::InsertStmt {
        table_name: "employees".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(2)),
            ast::Expression::Literal(types::SqlValue::Integer(50000)),
            ast::Expression::Literal(types::SqlValue::Integer(60000)),
        ]]),
    };

    let result = InsertExecutor::execute(&mut db, &stmt2);
    assert!(result.is_err());
    match result.unwrap_err() {
        ExecutorError::ConstraintViolation(msg) => {
            assert!(msg.contains("CHECK"));
            assert!(msg.contains("bonus_less_than_salary"));
        }
        other => panic!("Expected ConstraintViolation, got {:?}", other),
    }
}

use executor::{UpdateExecutor, ExecutorError};
use storage::{Database, Row};
use catalog::{ColumnSchema, TableSchema};
use types::{DataType, SqlValue};
use ast::{Assignment, BinaryOperator, Expression, UpdateStmt};

fn setup_test_table(db: &mut Database) {
    // Create table schema
    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
            ColumnSchema::new("salary".to_string(), DataType::Integer, true),
            ColumnSchema::new(
                "department".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );

    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
            SqlValue::Integer(45000),
            SqlValue::Varchar("Engineering".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar("Bob".to_string()),
            SqlValue::Integer(48000),
            SqlValue::Varchar("Engineering".to_string()),
        ]),
    )
    .unwrap();

    db.insert_row(
        "employees",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar("Charlie".to_string()),
            SqlValue::Integer(42000),
            SqlValue::Varchar("Sales".to_string()),
        ]),
    )
    .unwrap();
}
    #[test]
    fn test_update_all_rows() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Integer(50000)),
            }],
            where_clause: None,
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 3);

        // Verify all salaries updated
        let table = db.get_table("employees").unwrap();
        for row in table.scan() {
            assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(50000));
        }
    }

    #[test]
    fn test_update_with_where_clause() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Integer(60000)),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "department".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Varchar("Engineering".to_string()))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 2); // Alice and Bob

        // Verify only Engineering employees updated
        let table = db.get_table("employees").unwrap();
        let rows: Vec<&Row> = table.scan().iter().collect();

        // Alice and Bob should have new salary
        assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Integer(60000));
        assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Integer(60000));

        // Charlie should have original salary
        assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Integer(42000));
    }

    #[test]
    fn test_update_multiple_columns() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![
                Assignment {
                    column: "salary".to_string(),
                    value: Expression::Literal(SqlValue::Integer(55000)),
                },
                Assignment {
                    column: "department".to_string(),
                    value: Expression::Literal(SqlValue::Varchar("Sales".to_string())),
                },
            ],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 1);

        // Verify both columns updated for Alice
        let table = db.get_table("employees").unwrap();
        let row = &table.scan()[0];
        assert_eq!(row.get(2).unwrap(), &SqlValue::Integer(55000));
        assert_eq!(row.get(3).unwrap(), &SqlValue::Varchar("Sales".to_string()));
    }

    #[test]
    fn test_update_with_expression() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        // Give everyone a 10% raise: salary = salary * 110 / 100
        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::BinaryOp {
                    left: Box::new(Expression::BinaryOp {
                        left: Box::new(Expression::ColumnRef {
                            table: None,
                            column: "salary".to_string(),
                        }),
                        op: BinaryOperator::Multiply,
                        right: Box::new(Expression::Literal(SqlValue::Integer(110))),
                    }),
                    op: BinaryOperator::Divide,
                    right: Box::new(Expression::Literal(SqlValue::Integer(100))),
                },
            }],
            where_clause: None,
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 3);

        // Verify salaries increased (integer division, so exact values)
        let table = db.get_table("employees").unwrap();
        let rows: Vec<&Row> = table.scan().iter().collect();

        assert_eq!(rows[0].get(2).unwrap(), &SqlValue::Integer(49500)); // 45000 * 1.1
        assert_eq!(rows[1].get(2).unwrap(), &SqlValue::Integer(52800)); // 48000 * 1.1
        assert_eq!(rows[2].get(2).unwrap(), &SqlValue::Integer(46200)); // 42000 * 1.1
    }

    #[test]
    fn test_update_table_not_found() {
        let mut db = Database::new();

        let stmt = UpdateStmt {
            table_name: "nonexistent".to_string(),
            assignments: vec![],
            where_clause: None,
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
    }

    #[test]
    fn test_update_column_not_found() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "nonexistent_column".to_string(),
                value: Expression::Literal(SqlValue::Integer(123)),
            }],
            where_clause: None,
        };

        let result = UpdateExecutor::execute(&stmt, &mut db);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::ColumnNotFound(_)));
    }

    #[test]
    fn test_update_no_matching_rows() {
        let mut db = Database::new();
        setup_test_table(&mut db);

        let stmt = UpdateStmt {
            table_name: "employees".to_string(),
            assignments: vec![Assignment {
                column: "salary".to_string(),
                value: Expression::Literal(SqlValue::Integer(99999)),
            }],
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(SqlValue::Integer(999))),
            }),
        };

        let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
        assert_eq!(count, 0); // No rows matched
    }


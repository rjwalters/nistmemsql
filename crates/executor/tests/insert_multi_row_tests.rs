use executor::{InsertExecutor, ExecutorError};
use storage;
use catalog;
use types;
use ast;


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
    fn test_multi_row_insert_atomic_success() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
        // All should succeed
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(2)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(3)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Charlie".to_string())),
                ],
            ]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 3);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 3);
    }

    #[test]
    fn test_multi_row_insert_atomic_failure() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1, 'Alice'), (NULL, 'Bob'), (3, 'Charlie')
        // Second row violates NOT NULL constraint on id, should fail atomically
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Null), // id = NULL (violates NOT NULL)
                    ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(3)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Charlie".to_string())),
                ],
            ]),
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::ConstraintViolation(_)));

        // No rows should be inserted due to atomicity
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_multi_row_insert_with_column_list() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users (name, id) VALUES ('Alice', 1), ('Bob', 2)
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["name".to_string(), "id".to_string()],
            source: ast::InsertSource::Values(vec![
                vec![
                    ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
                    ast::Expression::Literal(types::SqlValue::Integer(2)),
                ],
            ]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 2);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn test_multi_row_insert_type_mismatch() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1, 'Alice'), ('not_a_number', 'Bob')
        // Second row has type mismatch, should fail atomically
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Varchar("not_a_number".to_string())), // Wrong type for id
                    ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
                ],
            ]),
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());

        // No rows should be inserted due to atomicity
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_multi_row_insert_various_data_types() {
        let mut db = storage::Database::new();

        // CREATE TABLE test_types (id INT NOT NULL, name VARCHAR(50), active BOOLEAN, score FLOAT)
        let schema = catalog::TableSchema::new(
            "test_types".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("name".to_string(), types::DataType::Varchar { max_length: Some(50) }, true),
                catalog::ColumnSchema::new("active".to_string(), types::DataType::Boolean, true),
                catalog::ColumnSchema::new("score".to_string(), types::DataType::Float { precision: 53 }, true),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO test_types VALUES
        //   (1, 'Alice', TRUE, 95.5),
        //   (2, 'Bob', FALSE, 87.2),
        //   (3, NULL, NULL, NULL)
        let stmt = ast::InsertStmt {
            table_name: "test_types".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                    ast::Expression::Literal(types::SqlValue::Boolean(true)),
                    ast::Expression::Literal(types::SqlValue::Float(95.5)),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(2)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
                    ast::Expression::Literal(types::SqlValue::Boolean(false)),
                    ast::Expression::Literal(types::SqlValue::Float(87.2)),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(3)),
                    ast::Expression::Literal(types::SqlValue::Null),
                    ast::Expression::Literal(types::SqlValue::Null),
                    ast::Expression::Literal(types::SqlValue::Null),
                ],
            ]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 3);

        let table = db.get_table("test_types").unwrap();
        assert_eq!(table.row_count(), 3);
    }

    #[test]
    fn test_multi_row_insert_primary_key_violation() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))
        let schema = catalog::TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("name".to_string(), types::DataType::Varchar { max_length: Some(50) }, true),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO users VALUES (1, 'Alice'), (1, 'Bob')
        // Second row violates PRIMARY KEY, should fail atomically
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)),
                    ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                ],
                vec![
                    ast::Expression::Literal(types::SqlValue::Integer(1)), // Duplicate primary key
                    ast::Expression::Literal(types::SqlValue::Varchar("Bob".to_string())),
                ],
            ]),
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::ConstraintViolation(_)));

        // No rows should be inserted due to atomicity
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 0);
    }

    #[test]
    fn test_single_row_insert_no_transaction() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // Single row INSERT should work without implicit transaction
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);
    }



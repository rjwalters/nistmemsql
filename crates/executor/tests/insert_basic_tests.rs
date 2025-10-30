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
    fn test_basic_insert() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![], // No columns specified
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        // Verify row was inserted
        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_multi_row_insert() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')
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
            ]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 2);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn test_insert_with_column_list() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users (name, id) VALUES ('Alice', 1)
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["name".to_string(), "id".to_string()],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
                ast::Expression::Literal(types::SqlValue::Integer(1)),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        let table = db.get_table("users").unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_insert_null_value() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT, name VARCHAR(50))
        // name is nullable
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: Some(50) },
                    true, // nullable
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO users VALUES (1, NULL)
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Null),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);
    }

    #[test]
    fn test_insert_type_mismatch() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES ('not_a_number', 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Varchar("not_a_number".to_string())),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::UnsupportedExpression(_)));
    }

    #[test]
    fn test_insert_column_count_mismatch() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (1)  -- Missing name column
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![ast::Expression::Literal(types::SqlValue::Integer(1))]],)
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_table_not_found() {
        let mut db = storage::Database::new();

        // INSERT INTO nonexistent VALUES (1, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "nonexistent".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::TableNotFound(_)));
    }

    #[test]
    fn test_insert_column_not_found() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users (id, invalid_col) VALUES (1, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["id".to_string(), "invalid_col".to_string()],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ExecutorError::ColumnNotFound(_)));
    }

    #[test]
    fn test_insert_not_null_constraint_violation() {
        let mut db = storage::Database::new();
        setup_test_table(&mut db);

        // INSERT INTO users VALUES (NULL, 'Alice')
        // id column is NOT NULL, so this should fail
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Null),
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let result = InsertExecutor::execute(&mut db, &stmt);
        assert!(result.is_err());
        match result.unwrap_err() {
            ExecutorError::ConstraintViolation(msg) => {
                assert!(msg.contains("NOT NULL constraint violation"));
                assert!(msg.contains("column 'id'"));
                assert!(msg.contains("table 'users'"));
                assert!(msg.contains("cannot be NULL"));
            }
            other => panic!("Expected ConstraintViolation, got {:?}", other),
        }
    }

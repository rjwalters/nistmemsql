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
    fn test_character_varying_column_with_length() {
        let mut db = storage::Database::new();

        // CREATE TABLE test_cv (id INT, description CHARACTER VARYING(100))
        let schema = catalog::TableSchema::new(
            "test_cv".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "description".to_string(),
                    types::DataType::Varchar { max_length: Some(100) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO test_cv VALUES (1, 'Test description')
        let stmt = ast::InsertStmt {
            table_name: "test_cv".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Test description".to_string())),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        // Verify data was inserted correctly
        let table = db.get_table("test_cv").unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_character_varying_column_without_length() {
        let mut db = storage::Database::new();

        // CREATE TABLE test_cv_nolen (id INT, text CHARACTER VARYING)
        let schema = catalog::TableSchema::new(
            "test_cv_nolen".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new(
                    "text".to_string(),
                    types::DataType::Varchar { max_length: None },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO test_cv_nolen VALUES (1, 'Unlimited length text')
        let stmt = ast::InsertStmt {
            table_name: "test_cv_nolen".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Varchar("Unlimited length text".to_string())),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        // Verify data was inserted correctly
        let table = db.get_table("test_cv_nolen").unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn test_insert_with_default_value() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT DEFAULT 999, name VARCHAR(50))
        let mut id_column = catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false);
        id_column.default_value = Some(ast::Expression::Literal(types::SqlValue::Integer(999)));

        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                id_column,
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Default,
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        // Verify default value was used
        let table = db.get_table("users").unwrap();
        let row = &table.scan()[0];
        assert_eq!(row.get(0), Some(&types::SqlValue::Integer(999)));
        assert_eq!(row.get(1), Some(&types::SqlValue::Varchar("Alice".to_string())));
    }

    #[test]
    fn test_insert_default_no_default_value_defined() {
        let mut db = storage::Database::new();

        // CREATE TABLE users (id INT, name VARCHAR(50)) -- no default for id
        let schema = catalog::TableSchema::new(
            "users".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, true), // nullable
                catalog::ColumnSchema::new(
                    "name".to_string(),
                    types::DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // INSERT INTO users (id, name) VALUES (DEFAULT, 'Alice')
        let stmt = ast::InsertStmt {
            table_name: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Default,
                ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
            ]]),
        };

        let rows = InsertExecutor::execute(&mut db, &stmt).unwrap();
        assert_eq!(rows, 1);

        // Verify NULL was used when no default is defined
        let table = db.get_table("users").unwrap();
        let row = &table.scan()[0];
        assert_eq!(row.get(0), Some(&types::SqlValue::Null));
        assert_eq!(row.get(1), Some(&types::SqlValue::Varchar("Alice".to_string())));
    }

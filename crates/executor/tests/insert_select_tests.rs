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
fn test_insert_from_select_basic() {
    let mut db = storage::Database::new();
    setup_test_table(&mut db);

    // First insert some data to select from
    let insert_stmt = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create another table to insert into
    let schema = catalog::TableSchema::new(
        "users_backup".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO users_backup SELECT * FROM users
    let select_stmt = ast::SelectStmt {
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        with_clause: None,
        distinct: false,
        set_operation: None,
    };

    let insert_select_stmt = ast::InsertStmt {
        table_name: "users_backup".to_string(),
        columns: vec![], // No explicit columns, use all
        source: ast::InsertSource::Select(Box::new(select_stmt)),
    };

    let rows = InsertExecutor::execute(&mut db, &insert_select_stmt).unwrap();
    assert_eq!(rows, 1);

    let table = db.get_table("users_backup").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_from_select_with_where() {
    let mut db = storage::Database::new();
    setup_test_table(&mut db);

    // Insert multiple users
    let insert_stmt = ast::InsertStmt {
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
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create backup table
    let schema = catalog::TableSchema::new(
        "active_users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT INTO active_users SELECT * FROM users WHERE id = 1
    let select_stmt = ast::SelectStmt {
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: Some(ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef { table: None, column: "id".to_string() }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        with_clause: None,
        distinct: false,
        set_operation: None,
    };

    let insert_select_stmt = ast::InsertStmt {
        table_name: "active_users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Select(Box::new(select_stmt)),
    };

    let rows = InsertExecutor::execute(&mut db, &insert_select_stmt).unwrap();
    assert_eq!(rows, 1);

    let table = db.get_table("active_users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_insert_from_select_column_mismatch() {
    let mut db = storage::Database::new();
    setup_test_table(&mut db);

    // Insert some data
    let insert_stmt = ast::InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(types::SqlValue::Integer(1)),
            ast::Expression::Literal(types::SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create a table with different column count
    let schema = catalog::TableSchema::new(
        "wrong_table".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new(
                "name".to_string(),
                types::DataType::Varchar { max_length: Some(50) },
                true,
            ),
            catalog::ColumnSchema::new("extra".to_string(), types::DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    // Try to INSERT with wrong column count
    let select_stmt = ast::SelectStmt {
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        with_clause: None,
        distinct: false,
        set_operation: None,
    };

    let insert_select_stmt = ast::InsertStmt {
        table_name: "wrong_table".to_string(),
        columns: vec![], // Should match all columns
        source: ast::InsertSource::Select(Box::new(select_stmt)),
    };

    let result = InsertExecutor::execute(&mut db, &insert_select_stmt);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), ExecutorError::UnsupportedExpression(_)));
}

#[test]
fn test_insert_from_select_with_aggregates() {
    let mut db = storage::Database::new();

    // Create sales table
    let schema = catalog::TableSchema::new(
        "sales".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("amount".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert sales data
    let insert_stmt = ast::InsertStmt {
        table_name: "sales".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![
            vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(100)),
            ],
            vec![
                ast::Expression::Literal(types::SqlValue::Integer(2)),
                ast::Expression::Literal(types::SqlValue::Integer(200)),
            ],
        ]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create summary table
    let summary_schema = catalog::TableSchema::new(
        "summary".to_string(),
        vec![
            catalog::ColumnSchema::new("total".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("count".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(summary_schema).unwrap();

    // INSERT INTO summary SELECT SUM(amount), COUNT(*) FROM sales
    let select_stmt = ast::SelectStmt {
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "SUM".to_string(),
                    args: vec![ast::Expression::ColumnRef {
                        table: None,
                        column: "amount".to_string(),
                    }],
                },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![ast::Expression::Wildcard],
                },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "sales".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        with_clause: None,
        distinct: false,
        set_operation: None,
    };

    let insert_select_stmt = ast::InsertStmt {
        table_name: "summary".to_string(),
        columns: vec![],
        source: ast::InsertSource::Select(Box::new(select_stmt)),
    };
    let rows = InsertExecutor::execute(&mut db, &insert_select_stmt).unwrap();
    assert_eq!(rows, 1);

    // Verify aggregated data was inserted
    let table = db.get_table("summary").unwrap();
    assert_eq!(table.row_count(), 1);
}

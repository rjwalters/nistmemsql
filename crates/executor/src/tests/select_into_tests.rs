//! SELECT INTO tests - SQL:1999 Feature E111

use crate::{CreateTableExecutor, ExecutorError, SelectExecutor, SelectIntoExecutor};

#[test]
fn test_select_into_single_row() {
    let mut db = storage::Database::new();

    // Create source table
    let create_stmt = ast::CreateTableStmt {
        table_name: "source".to_string(),
        columns: vec![
            ast::ColumnDef {
                name: "id".to_string(),
                data_type: types::DataType::Integer,
                nullable: false,
                constraints: vec![],
                default_value: None,
            },
            ast::ColumnDef {
                name: "name".to_string(),
                data_type: types::DataType::Varchar { max_length: Some(50) },
                nullable: true,
                constraints: vec![],
                default_value: None,
            },
        ],
        table_constraints: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert one row
    db.insert_row(
        "source",
        storage::Row {
            values: vec![
                types::SqlValue::Integer(1),
                types::SqlValue::Varchar("Alice".to_string()),
            ],
        },
    )
    .unwrap();

    // Execute SELECT INTO
    let select_stmt = ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "id".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
        ],
        into_table: Some("target".to_string()),
        from: Some(ast::FromClause::Table { name: "source".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_ok(), "SELECT INTO should succeed: {:?}", result.err());

    // Verify table was created
    assert!(db.catalog.table_exists("target"));

    // Verify row was inserted
    let executor = SelectExecutor::new(&db);
    let query = ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "target".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let rows = executor.execute(&query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], types::SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], types::SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_select_into_no_rows_error() {
    let mut db = storage::Database::new();

    // Create source table
    let create_stmt = ast::CreateTableStmt {
        table_name: "source".to_string(),
        columns: vec![ast::ColumnDef {
            name: "id".to_string(),
            data_type: types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
        }],
        table_constraints: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // No rows inserted - table is empty

    // Execute SELECT INTO (should fail - no rows)
    let select_stmt = ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        into_table: Some("target".to_string()),
        from: Some(ast::FromClause::Table { name: "source".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("no rows"));
        }
        _ => panic!("Expected UnsupportedFeature error for no rows"),
    }
}

#[test]
fn test_select_into_multiple_rows_error() {
    let mut db = storage::Database::new();

    // Create source table
    let create_stmt = ast::CreateTableStmt {
        table_name: "source".to_string(),
        columns: vec![ast::ColumnDef {
            name: "id".to_string(),
            data_type: types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
        }],
        table_constraints: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert multiple rows
    db.insert_row("source", storage::Row { values: vec![types::SqlValue::Integer(1)] }).unwrap();
    db.insert_row("source", storage::Row { values: vec![types::SqlValue::Integer(2)] }).unwrap();

    // Execute SELECT INTO (should fail - multiple rows)
    let select_stmt = ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "id".to_string() },
            alias: None,
        }],
        into_table: Some("target".to_string()),
        from: Some(ast::FromClause::Table { name: "source".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_err());
    match result {
        Err(ExecutorError::UnsupportedFeature(msg)) => {
            assert!(msg.contains("2 rows"));
        }
        _ => panic!("Expected UnsupportedFeature error for multiple rows"),
    }
}

#[test]
fn test_select_into_with_expressions() {
    let mut db = storage::Database::new();

    // Create source table
    let create_stmt = ast::CreateTableStmt {
        table_name: "source".to_string(),
        columns: vec![ast::ColumnDef {
            name: "x".to_string(),
            data_type: types::DataType::Integer,
            nullable: false,
            constraints: vec![],
            default_value: None,
        }],
        table_constraints: vec![],
    };
    CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();

    // Insert one row
    db.insert_row("source", storage::Row { values: vec![types::SqlValue::Integer(10)] }).unwrap();

    // Execute SELECT INTO with expression and alias
    let select_stmt = ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::BinaryOp {
                op: ast::BinaryOperator::Plus,
                left: Box::new(ast::Expression::ColumnRef { table: None, column: "x".to_string() }),
                right: Box::new(ast::Expression::Literal(types::SqlValue::Integer(5))),
            },
            alias: Some("y".to_string()),
        }],
        into_table: Some("target".to_string()),
        from: Some(ast::FromClause::Table { name: "source".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };

    let result = SelectIntoExecutor::execute(&select_stmt, "target", &mut db);
    assert!(result.is_ok(), "SELECT INTO with expression should succeed: {:?}", result.err());

    // Verify table was created with correct column name
    let executor = SelectExecutor::new(&db);
    let query = ast::SelectStmt {
        with_clause: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        into_table: None,
        from: Some(ast::FromClause::Table { name: "target".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
    };
    let rows = executor.execute(&query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], types::SqlValue::Integer(15));
}

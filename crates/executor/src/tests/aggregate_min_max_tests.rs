//! MIN and MAX aggregate function tests
//!
//! Tests for MIN/MAX functions on integers and strings.

use super::super::*;

#[test]
fn test_min_function() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "temps".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("temp".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(72)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(65)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(78)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "MIN".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "temp".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "temps".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(65));
}

#[test]
fn test_max_function() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "temps".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("temp".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(1), types::SqlValue::Integer(72)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(2), types::SqlValue::Integer(65)]),
    )
    .unwrap();
    db.insert_row(
        "temps",
        storage::Row::new(vec![types::SqlValue::Integer(3), types::SqlValue::Integer(78)]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "MAX".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "temp".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "temps".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(78));
}

#[test]
fn test_min_max_on_strings() {
    // Edge case: MIN/MAX on VARCHAR values should use lexicographic ordering
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "names".to_string(),
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
    db.insert_row(
        "names",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Zebra".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "names",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Apple".to_string()),
        ]),
    )
    .unwrap();
    db.insert_row(
        "names",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Mango".to_string()),
        ]),
    )
    .unwrap();

    let executor = SelectExecutor::new(&db);

    // Test MIN
    let stmt_min = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "MIN".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "name".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "names".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt_min).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Apple".to_string()));

    // Test MAX
    let stmt_max = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Function {
                name: "MAX".to_string(),
                args: vec![ast::Expression::ColumnRef { table: None, column: "name".to_string() }],
                character_unit: None,
            },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "names".to_string(), alias: None }),
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt_max).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Zebra".to_string()));
}

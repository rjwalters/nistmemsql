//! LIKE and NOT LIKE pattern matching tests
//!
//! Tests for SQL LIKE predicates covering:
//! - Wildcard patterns (% for any sequence, _ for single char)
//! - NOT LIKE negation
//! - NULL pattern and value handling
//! - Pattern matching edge cases

use super::super::super::*;

#[test]
fn test_like_wildcard_percent() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "name".to_string(),
            types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Bob".to_string())]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Alex".to_string())]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE 'Al%'
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Like {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                "Al%".to_string(),
            ))),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // Alice and Alex
}

#[test]
fn test_like_wildcard_underscore() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "name".to_string(),
            types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("cat".to_string())]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("bat".to_string())]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("cart".to_string())]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE '_at'
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Like {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                "_at".to_string(),
            ))),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // cat and bat (cart is 4 chars)
}

#[test]
fn test_not_like() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "name".to_string(),
            types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Alice".to_string())]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Bob".to_string())]))
        .unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Alex".to_string())]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name NOT LIKE 'Al%'
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Like {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                "Al%".to_string(),
            ))),
            negated: true,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // Only Bob
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Bob".to_string()));
}

#[test]
fn test_like_null_pattern() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "name".to_string(),
            types::DataType::Varchar { max_length: Some(50) },
            false,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Alice".to_string())]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE NULL
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Like {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 0); // NULL pattern matches nothing
}

#[test]
fn test_like_null_value() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new(
            "name".to_string(),
            types::DataType::Varchar { max_length: Some(50) },
            true,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Null])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Varchar("Alice".to_string())]))
        .unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE name LIKE 'Al%'
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard { alias: None }],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Like {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                "Al%".to_string(),
            ))),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // NULL value doesn't match, only Alice
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Alice".to_string()));
}

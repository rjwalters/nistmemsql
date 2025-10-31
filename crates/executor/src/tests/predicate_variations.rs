//! Predicate variation tests
//!
//! Comprehensive tests for SQL predicates including:
//! - IN/NOT IN with various values and NULL handling
//! - LIKE/NOT LIKE with patterns and edge cases
//! - BETWEEN/NOT BETWEEN with edge cases (NULL, reversed ranges)
//! - POSITION function
//! - TRIM function variations
//! - CAST expressions

use super::super::*;

// =============================================================================
// IN Predicate Tests
// =============================================================================

#[test]
fn test_in_list_basic() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, 3, 5)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(3)),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 2); // 1 and 5 match
}

#[test]
fn test_not_in_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT IN (1, 3, 5)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(3)),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 1); // Only 2 doesn't match
    assert_eq!(result[0].values[0], types::SqlValue::Integer(2));
}

#[test]
fn test_in_list_with_null_value() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(1)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Null])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, 3, 5)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Integer(3)),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 1); // NULL doesn't match, only 1 matches
    assert_eq!(result[0].values[0], types::SqlValue::Integer(1));
}

#[test]
fn test_in_list_with_null_in_list() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(2)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val IN (1, NULL, 5)
    // Should return no rows because 2 doesn't match 1 or 5, and NULL comparison is unknown
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::InList {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            values: vec![
                ast::Expression::Literal(types::SqlValue::Integer(1)),
                ast::Expression::Literal(types::SqlValue::Null),
                ast::Expression::Literal(types::SqlValue::Integer(5)),
            ],
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
    assert_eq!(result.len(), 0); // NULL in list causes unknown result for non-matching value
}

// =============================================================================
// LIKE Predicate Tests
// =============================================================================

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
        select_list: vec![ast::SelectItem::Wildcard],
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
        select_list: vec![ast::SelectItem::Wildcard],
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
        select_list: vec![ast::SelectItem::Wildcard],
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
        select_list: vec![ast::SelectItem::Wildcard],
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
        select_list: vec![ast::SelectItem::Wildcard],
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

// =============================================================================
// BETWEEN Predicate Edge Cases
// =============================================================================

#[test]
fn test_between_with_null_expr() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Null])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 1 AND 10
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(1))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1); // NULL doesn't match, only 5
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5));
}

#[test]
fn test_not_between() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(5)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(15)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(25)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val NOT BETWEEN 10 AND 20
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            negated: true,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 2); // 5 and 25
}

#[test]
fn test_between_boundary_values() {
    let mut db = storage::Database::new();
    let schema = catalog::TableSchema::new(
        "test".to_string(),
        vec![catalog::ColumnSchema::new("val".to_string(), types::DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(10)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(15)])).unwrap();
    db.insert_row("test", storage::Row::new(vec![types::SqlValue::Integer(20)])).unwrap();

    let executor = SelectExecutor::new(&db);

    // SELECT * FROM test WHERE val BETWEEN 10 AND 20
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "test".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "val".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            negated: false,
            symmetric: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 3); // All values including boundaries
}

// =============================================================================
// POSITION Function Tests
// =============================================================================

#[test]
fn test_position_found() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('world' IN 'hello world')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "world".to_string(),
                ))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello world".to_string(),
                ))),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(7)); // Position is 1-indexed
}

#[test]
fn test_position_not_found() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('xyz' IN 'hello world')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "xyz".to_string(),
                ))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello world".to_string(),
                ))),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(0)); // Not found returns 0
}

#[test]
fn test_position_null_substring() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION(NULL IN 'hello world')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello world".to_string(),
                ))),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_position_null_string() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT POSITION('world' IN NULL)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Position {
                substring: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "world".to_string(),
                ))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                character_unit: None,
            },
            alias: Some("pos".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

// =============================================================================
// TRIM Function Tests
// =============================================================================

#[test]
fn test_trim_both_default() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM('  hello  ')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,     // Defaults to Both
                removal_char: None, // Defaults to space
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "  hello  ".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_trim_leading() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(LEADING FROM '  hello  ')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: Some(ast::TrimPosition::Leading),
                removal_char: None,
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "  hello  ".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("hello  ".to_string()));
}

#[test]
fn test_trim_trailing() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(TRAILING FROM '  hello  ')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: Some(ast::TrimPosition::Trailing),
                removal_char: None,
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "  hello  ".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("  hello".to_string()));
}

#[test]
fn test_trim_custom_char() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM('x' FROM 'xxxhelloxxx')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,
                removal_char: Some(Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "x".to_string(),
                )))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "xxxhelloxxx".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_trim_null_string() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(NULL)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,
                removal_char: None,
                string: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

#[test]
fn test_trim_null_removal_char() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT TRIM(NULL FROM 'hello')
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Trim {
                position: None,
                removal_char: Some(Box::new(ast::Expression::Literal(types::SqlValue::Null))),
                string: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "hello".to_string(),
                ))),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

// =============================================================================
// CAST Expression Tests
// =============================================================================

#[test]
fn test_cast_integer_to_varchar() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(123 AS VARCHAR(10))
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Cast {
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Integer(123))),
                data_type: types::DataType::Varchar { max_length: Some(10) },
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("123".to_string()));
}

#[test]
fn test_cast_varchar_to_integer() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST('456' AS INTEGER)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Cast {
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Varchar(
                    "456".to_string(),
                ))),
                data_type: types::DataType::Integer,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(456));
}

#[test]
fn test_cast_null() {
    let mut db = storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT CAST(NULL AS INTEGER)
    let stmt = ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::Cast {
                expr: Box::new(ast::Expression::Literal(types::SqlValue::Null)),
                data_type: types::DataType::Integer,
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], types::SqlValue::Null);
}

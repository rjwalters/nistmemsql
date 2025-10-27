//! BETWEEN predicate tests

use super::super::*;

#[test]
fn test_between_integer() {
    let mut db = storage::Database::new();

    // Create test table
    let schema = catalog::TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("name".to_string(), types::DataType::Varchar { max_length: 100 }, false),
            catalog::ColumnSchema::new("age".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Alice".to_string()),
            types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Bob".to_string()),
            types::SqlValue::Integer(30),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Charlie".to_string()),
            types::SqlValue::Integer(35),
        ]),
    )
    .unwrap();
    db.insert_row(
        "users",
        storage::Row::new(vec![
            types::SqlValue::Integer(4),
            types::SqlValue::Varchar("David".to_string()),
            types::SqlValue::Integer(40),
        ]),
    )
    .unwrap();

    // Test: age BETWEEN 28 AND 36
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
                alias: None,
            },
            ast::SelectItem::Expression {
                expr: ast::Expression::ColumnRef { table: None, column: "age".to_string() },
                alias: None,
            },
        ],
        from: Some(ast::FromClause::Table { name: "users".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "age".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(28))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(36))),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return Bob (30) and Charlie (35)
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Bob".to_string()));
    assert_eq!(result[0].values[1], types::SqlValue::Integer(30));
    assert_eq!(result[1].values[0], types::SqlValue::Varchar("Charlie".to_string()));
    assert_eq!(result[1].values[1], types::SqlValue::Integer(35));
}

#[test]
fn test_not_between() {
    let mut db = storage::Database::new();

    // Create test table
    let schema = catalog::TableSchema::new(
        "products".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("name".to_string(), types::DataType::Varchar { max_length: 100 }, false),
            catalog::ColumnSchema::new("price".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(1),
            types::SqlValue::Varchar("Cheap".to_string()),
            types::SqlValue::Integer(5),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(2),
            types::SqlValue::Varchar("Mid".to_string()),
            types::SqlValue::Integer(15),
        ]),
    )
    .unwrap();
    db.insert_row(
        "products",
        storage::Row::new(vec![
            types::SqlValue::Integer(3),
            types::SqlValue::Varchar("Expensive".to_string()),
            types::SqlValue::Integer(25),
        ]),
    )
    .unwrap();

    // Test: price NOT BETWEEN 10 AND 20
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "name".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "products".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "price".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            negated: true, // NOT BETWEEN
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return Cheap (5) and Expensive (25)
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Varchar("Cheap".to_string()));
    assert_eq!(result[1].values[0], types::SqlValue::Varchar("Expensive".to_string()));
}

#[test]
fn test_between_boundary_inclusive() {
    let mut db = storage::Database::new();

    // Create test table
    let schema = catalog::TableSchema::new(
        "data".to_string(),
        vec![catalog::ColumnSchema::new(
            "value".to_string(),
            types::DataType::Integer,
            false,
        )],
    );
    db.create_table(schema).unwrap();

    // Insert boundary and middle values
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(9)]))
        .unwrap();
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(10)]))
        .unwrap(); // Lower boundary
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(15)]))
        .unwrap(); // Middle
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(20)]))
        .unwrap(); // Upper boundary
    db.insert_row("data", storage::Row::new(vec![types::SqlValue::Integer(21)]))
        .unwrap();

    // Test: BETWEEN is inclusive of boundaries
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Wildcard],
        from: Some(ast::FromClause::Table { name: "data".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "value".to_string() }),
            low: Box::new(ast::Expression::Literal(types::SqlValue::Integer(10))),
            high: Box::new(ast::Expression::Literal(types::SqlValue::Integer(20))),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return 10, 15, 20 (boundaries included)
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(10));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(15));
    assert_eq!(result[2].values[0], types::SqlValue::Integer(20));
}

#[test]
fn test_between_with_column_references() {
    let mut db = storage::Database::new();

    // Create test table
    let schema = catalog::TableSchema::new(
        "ranges".to_string(),
        vec![
            catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("min_val".to_string(), types::DataType::Integer, false),
            catalog::ColumnSchema::new("max_val".to_string(), types::DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Insert test data with different ranges
    db.insert_row(
        "ranges",
        storage::Row::new(vec![
            types::SqlValue::Integer(5),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ranges",
        storage::Row::new(vec![
            types::SqlValue::Integer(15),
            types::SqlValue::Integer(1),
            types::SqlValue::Integer(10),
        ]),
    )
    .unwrap();
    db.insert_row(
        "ranges",
        storage::Row::new(vec![
            types::SqlValue::Integer(8),
            types::SqlValue::Integer(5),
            types::SqlValue::Integer(20),
        ]),
    )
    .unwrap();

    // Test: value BETWEEN min_val AND max_val
    let executor = SelectExecutor::new(&db);
    let stmt = ast::SelectStmt {
        distinct: false,
        select_list: vec![ast::SelectItem::Expression {
            expr: ast::Expression::ColumnRef { table: None, column: "value".to_string() },
            alias: None,
        }],
        from: Some(ast::FromClause::Table { name: "ranges".to_string(), alias: None }),
        where_clause: Some(ast::Expression::Between {
            expr: Box::new(ast::Expression::ColumnRef { table: None, column: "value".to_string() }),
            low: Box::new(ast::Expression::ColumnRef { table: None, column: "min_val".to_string() }),
            high: Box::new(ast::Expression::ColumnRef { table: None, column: "max_val".to_string() }),
            negated: false,
        }),
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
    };

    let result = executor.execute(&stmt).unwrap();

    // Should return rows where value is within its range
    // Row 1: 5 BETWEEN 1 AND 10 = true
    // Row 2: 15 BETWEEN 1 AND 10 = false
    // Row 3: 8 BETWEEN 5 AND 20 = true
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values[0], types::SqlValue::Integer(5));
    assert_eq!(result[1].values[0], types::SqlValue::Integer(8));
}

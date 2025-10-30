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
fn test_update_with_default_value() {
    let mut db = Database::new();

    // CREATE TABLE users (id INT, name VARCHAR(50) DEFAULT 'Unknown')
    let mut name_column = ColumnSchema::new(
        "name".to_string(),
        DataType::Varchar { max_length: Some(50) },
        false,
    );
    name_column.default_value = Some(Expression::Literal(SqlValue::Varchar("Unknown".to_string())));

    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            name_column,
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT a row
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();

    // UPDATE users SET name = DEFAULT WHERE id = 1
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: Expression::Default,
        }],
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "id".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify default value was used
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1), Some(&SqlValue::Varchar("Unknown".to_string())));
}

#[test]
fn test_update_default_no_default_value_defined() {
    let mut db = Database::new();

    // CREATE TABLE users (id INT, name VARCHAR(50)) -- no default for name
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true, // nullable
            ),
        ],
    );
    db.create_table(schema).unwrap();

    // INSERT a row
    db.insert_row(
        "users",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar("Alice".to_string()),
        ]),
    )
    .unwrap();

    // UPDATE users SET name = DEFAULT WHERE id = 1
    let stmt = UpdateStmt {
        table_name: "users".to_string(),
        assignments: vec![Assignment {
            column: "name".to_string(),
            value: Expression::Default,
        }],
        where_clause: Some(Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "id".to_string(),
            }),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        }),
    };

    let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    assert_eq!(count, 1);

    // Verify NULL was used when no default is defined
    let table = db.get_table("users").unwrap();
    let row = &table.scan()[0];
    assert_eq!(row.get(1), Some(&SqlValue::Null));
}

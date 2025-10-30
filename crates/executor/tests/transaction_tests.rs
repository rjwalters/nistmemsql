//! Transaction tests including SAVEPOINT functionality

use catalog::{ColumnSchema, TableSchema};
use executor::{
    BeginTransactionExecutor, CommitExecutor, RollbackToSavepointExecutor, SavepointExecutor,
};
use storage::Database;
use types::{DataType, SqlValue};

#[test]
fn test_basic_savepoint() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Begin transaction
    let begin_stmt = ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Insert initial row
    let insert_stmt = ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Integer(1000)),
        ]]),
    };
    executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create savepoint
    let savepoint_stmt = ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    // Insert another row
    let insert_stmt2 = ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(2)),
            ast::Expression::Literal(SqlValue::Integer(500)),
        ]]),
    };
    executor::InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Check we have 2 rows
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 2);

    // Rollback to savepoint
    let rollback_to_stmt = ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    // Should only have 1 row now
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    // Commit
    let commit_stmt = ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_nested_savepoints() {
    let mut db = Database::new();

    // Create a test table
    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    // Begin transaction
    let begin_stmt = ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Insert initial row
    let insert_stmt = ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Integer(1000)),
        ]]),
    };
    executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create first savepoint
    let savepoint_stmt1 = ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt1, &mut db).unwrap();

    // Insert second row
    let insert_stmt2 = ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(2)),
            ast::Expression::Literal(SqlValue::Integer(500)),
        ]]),
    };
    executor::InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Create second savepoint
    let savepoint_stmt2 = ast::SavepointStmt { name: "sp2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt2, &mut db).unwrap();

    // Insert third row
    let insert_stmt3 = ast::InsertStmt {
        table_name: "accounts".to_string(),
        columns: vec!["id".to_string(), "balance".to_string()],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(3)),
            ast::Expression::Literal(SqlValue::Integer(200)),
        ]]),
    };
    executor::InsertExecutor::execute(&mut db, &insert_stmt3).unwrap();

    // Check we have 3 rows
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 3);

    // Rollback to sp1 (should destroy sp2 and remove rows after sp1)
    let rollback_to_stmt = ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    // Should only have 1 row now
    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    // Commit
    let commit_stmt = ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

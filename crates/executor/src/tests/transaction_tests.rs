//! Tests for transaction functionality (BEGIN, COMMIT, ROLLBACK)

use crate::{BeginTransactionExecutor, CommitExecutor, RollbackExecutor, InsertExecutor};
use storage::Database;
use catalog::TableSchema;
use types::{DataType, SqlValue};
use ast::{BeginStmt, CommitStmt, InsertStmt, RollbackStmt};

fn setup_test_table(db: &mut Database) {
    // CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(50))
    let schema = TableSchema::new(
        "users".to_string(),
        vec![
            catalog::ColumnSchema::new("id".to_string(), DataType::Integer, false),
            catalog::ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: 50 }, true),
        ],
    );
    db.create_table(schema).unwrap();
}

#[test]
fn test_begin_transaction_success() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Initially no transaction
    assert!(!db.in_transaction());
    assert_eq!(db.transaction_id(), None);

    // Begin transaction
    let result = BeginTransactionExecutor::execute(&BeginStmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Transaction started");

    // Now in transaction
    assert!(db.in_transaction());
    assert_eq!(db.transaction_id(), Some(1));
}

#[test]
fn test_begin_transaction_already_active() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Begin first transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();
    assert!(db.in_transaction());

    // Try to begin another transaction
    let result = BeginTransactionExecutor::execute(&BeginStmt, &mut db);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Transaction already active"));
}

#[test]
fn test_commit_without_transaction() {
    let mut db = Database::new();

    // Try to commit without active transaction
    let result = CommitExecutor::execute(&CommitStmt, &mut db);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("No active transaction to commit"));
}

#[test]
fn test_rollback_without_transaction() {
    let mut db = Database::new();

    // Try to rollback without active transaction
    let result = RollbackExecutor::execute(&RollbackStmt, &mut db);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("No active transaction to rollback"));
}

#[test]
fn test_commit_transaction() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Begin transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();
    assert!(db.in_transaction());

    // Commit transaction
    let result = CommitExecutor::execute(&CommitStmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Transaction committed");

    // No longer in transaction
    assert!(!db.in_transaction());
    assert_eq!(db.transaction_id(), None);
}

#[test]
fn test_rollback_transaction() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Begin transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();
    assert!(db.in_transaction());

    // Rollback transaction
    let result = RollbackExecutor::execute(&RollbackStmt, &mut db);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "Transaction rolled back");

    // No longer in transaction
    assert!(!db.in_transaction());
    assert_eq!(db.transaction_id(), None);
}

#[test]
fn test_transaction_insert_commit() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Begin transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();

    // Insert a row
    let insert_stmt = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    let rows = InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    assert_eq!(rows, 1);

    // Row should be visible within transaction
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    // Commit transaction
    CommitExecutor::execute(&CommitStmt, &mut db).unwrap();

    // Row should still be visible after commit
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_transaction_insert_rollback() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Begin transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();

    // Insert a row
    let insert_stmt = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    let rows = InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    assert_eq!(rows, 1);

    // Row should be visible within transaction
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    // Rollback transaction
    RollbackExecutor::execute(&RollbackStmt, &mut db).unwrap();

    // Row should be gone after rollback
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 0);
}

#[test]
fn test_transaction_multiple_operations_commit() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert initial data outside transaction
    let insert_stmt1 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt1).unwrap();

    // Begin transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();

    // Insert another row
    let insert_stmt2 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(2)),
            ast::Expression::Literal(SqlValue::Varchar("Bob".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Should have 2 rows
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);

    // Commit
    CommitExecutor::execute(&CommitStmt, &mut db).unwrap();

    // Should still have 2 rows
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);
}

#[test]
fn test_transaction_multiple_operations_rollback() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert initial data outside transaction
    let insert_stmt1 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt1).unwrap();

    // Begin transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();

    // Insert another row
    let insert_stmt2 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(2)),
            ast::Expression::Literal(SqlValue::Varchar("Bob".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();

    // Should have 2 rows
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);

    // Rollback
    RollbackExecutor::execute(&RollbackStmt, &mut db).unwrap();

    // Should be back to 1 row
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_transaction_isolation() {
    let mut db1 = Database::new();
    let mut db2 = Database::new();
    setup_test_table(&mut db1);
    setup_test_table(&mut db2);

    // Both start with empty tables
    assert_eq!(db1.get_table("users").unwrap().row_count(), 0);
    assert_eq!(db2.get_table("users").unwrap().row_count(), 0);

    // db1 begins transaction and inserts
    BeginTransactionExecutor::execute(&BeginStmt, &mut db1).unwrap();
    let insert_stmt = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db1, &insert_stmt).unwrap();

    // db1 should see the row
    assert_eq!(db1.get_table("users").unwrap().row_count(), 1);

    // db2 should not see the uncommitted row
    assert_eq!(db2.get_table("users").unwrap().row_count(), 0);

    // db1 commits
    CommitExecutor::execute(&CommitStmt, &mut db1).unwrap();

    // Now both should see the row (but since they're separate databases, db2 still doesn't)
    // This test demonstrates that transactions are per-database connection
    assert_eq!(db1.get_table("users").unwrap().row_count(), 1);
    assert_eq!(db2.get_table("users").unwrap().row_count(), 0);
}

#[test]
fn test_transaction_nested_operations() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Begin transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();

    // Insert multiple rows
    for i in 1..=5 {
        let insert_stmt = InsertStmt {
            table_name: "users".to_string(),
            columns: vec![],
            source: ast::InsertSource::Values(vec![vec![
                ast::Expression::Literal(SqlValue::Integer(i)),
                ast::Expression::Literal(SqlValue::Varchar(format!("User{}", i))),
            ]]),
        };
        InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    // Should have 5 rows
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 5);

    // Commit
    CommitExecutor::execute(&CommitStmt, &mut db).unwrap();

    // Should still have 5 rows
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 5);
}

#[test]
fn test_transaction_empty_rollback() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // Insert data outside transaction
    let insert_stmt = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Begin transaction but don't do anything
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();

    // Rollback
    RollbackExecutor::execute(&RollbackStmt, &mut db).unwrap();

    // Should still have the original data
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);
}

#[test]
fn test_multiple_transactions() {
    let mut db = Database::new();
    setup_test_table(&mut db);

    // First transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();
    let insert_stmt1 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(1)),
            ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt1).unwrap();
    CommitExecutor::execute(&CommitStmt, &mut db).unwrap();

    // Second transaction
    BeginTransactionExecutor::execute(&BeginStmt, &mut db).unwrap();
    let insert_stmt2 = InsertStmt {
        table_name: "users".to_string(),
        columns: vec![],
        source: ast::InsertSource::Values(vec![vec![
            ast::Expression::Literal(SqlValue::Integer(2)),
            ast::Expression::Literal(SqlValue::Varchar("Bob".to_string())),
        ]]),
    };
    InsertExecutor::execute(&mut db, &insert_stmt2).unwrap();
    RollbackExecutor::execute(&RollbackStmt, &mut db).unwrap();

    // Should only have Alice (Bob was rolled back)
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    let rows: Vec<_> = table.scan().into_iter().collect();
    let first_row = &rows[0];
    assert_eq!(first_row.get(0).unwrap(), &SqlValue::Integer(1));
    assert_eq!(first_row.get(1).unwrap(), &SqlValue::Varchar("Alice".to_string()));
}

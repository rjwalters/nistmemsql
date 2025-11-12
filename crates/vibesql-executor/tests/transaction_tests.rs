//! Transaction tests including SAVEPOINT functionality and error recovery scenarios

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::{
    BeginTransactionExecutor, CommitExecutor, RollbackToSavepointExecutor, SavepointExecutor,
};
use vibesql_storage::Database;
use vibesql_types::{DataType, SqlValue};

macro_rules! insert_stmt {
    ($table:expr, $cols:expr, $source:expr) => {
        vibesql_ast::InsertStmt {
            table_name: $table,
            columns: $cols,
            source: $source,
            conflict_clause: None,
        }
    };
}

// ============================================================================
// BASIC SAVEPOINT TESTS
// ============================================================================

#[test]
fn test_basic_savepoint() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "accounts".to_string(),
        vec!["id".to_string(), "balance".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(1000)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "accounts".to_string(),
        vec!["id".to_string(), "balance".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(500)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 2);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_nested_savepoints() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("balance".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "accounts".to_string(),
        vec!["id".to_string(), "balance".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(1000)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt1 = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt1, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "accounts".to_string(),
        vec!["id".to_string(), "balance".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(500)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt2 = vibesql_ast::SavepointStmt { name: "sp2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt2, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "accounts".to_string(),
        vec!["id".to_string(), "balance".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(200)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 3);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// PRIMARY KEY CONSTRAINT VIOLATION TESTS
// ============================================================================

#[test]
fn test_primary_key_violation_insert() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "users".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "users".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "users".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Bob".to_string())),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 1);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_primary_key_violation_with_multiple_inserts() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "products".to_string(),
        vec![
            ColumnSchema::new("product_id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    schema.primary_key = Some(vec!["product_id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    for i in 1..=3 {
        let insert_stmt = insert_stmt!(
            "products".to_string(),
            vec!["product_id".to_string(), "name".to_string()],
            vibesql_ast::InsertSource::Values(vec![vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(i)),
                vibesql_ast::Expression::Literal(SqlValue::Varchar(format!("Product {}", i))),
            ]])
        );
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    let table = db.get_table("products").unwrap();
    assert_eq!(table.row_count(), 3);

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "products".to_string(),
        vec!["product_id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Duplicate".to_string())),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("products").unwrap();
    assert_eq!(table.row_count(), 3);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("products").unwrap();
    assert_eq!(table.row_count(), 3);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// UNIQUE CONSTRAINT VIOLATION TESTS
// ============================================================================

#[test]
fn test_unique_constraint_violation_single_column() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "accounts".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("email".to_string(), DataType::Varchar { max_length: Some(100) }, false),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    schema.unique_constraints = vec![vec!["email".to_string()]];
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "accounts".to_string(),
        vec!["id".to_string(), "email".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("john@example.com".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "accounts".to_string(),
        vec!["id".to_string(), "email".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("john@example.com".to_string())),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("accounts").unwrap();
    assert_eq!(table.row_count(), 1);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_unique_constraint_violation_multiple_inserts() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "subscriptions".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("user_id".to_string(), DataType::Integer, true),
            ColumnSchema::new("subscription_key".to_string(), DataType::Varchar { max_length: Some(100) }, false),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    schema.unique_constraints = vec![vec!["subscription_key".to_string()]];
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    for i in 1..=5 {
        let insert_stmt = insert_stmt!(
            "subscriptions".to_string(),
            vec!["id".to_string(), "user_id".to_string(), "subscription_key".to_string()],
            vibesql_ast::InsertSource::Values(vec![vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(i)),
                vibesql_ast::Expression::Literal(SqlValue::Integer(i * 100)),
                vibesql_ast::Expression::Literal(SqlValue::Varchar(format!("sub_{}", i))),
            ]])
        );
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    let table = db.get_table("subscriptions").unwrap();
    assert_eq!(table.row_count(), 5);

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "subscriptions".to_string(),
        vec!["id".to_string(), "user_id".to_string(), "subscription_key".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(6)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(600)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("sub_3".to_string())),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("subscriptions").unwrap();
    assert_eq!(table.row_count(), 5);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("subscriptions").unwrap();
    assert_eq!(table.row_count(), 5);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// NOT NULL CONSTRAINT VIOLATION TESTS
// ============================================================================

#[test]
fn test_not_null_constraint_violation() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "employees".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, false),
        ],
    );
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "employees".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Alice".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "employees".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Null),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("employees").unwrap();
    assert_eq!(table.row_count(), 1);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("employees").unwrap();
    assert_eq!(table.row_count(), 1);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// NESTED SAVEPOINT ERROR RECOVERY TESTS
// ============================================================================

#[test]
fn test_nested_savepoints_with_errors() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "items".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(100) }, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "items".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Item1".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt1 = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt1, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "items".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Item2".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt2 = vibesql_ast::SavepointStmt { name: "sp2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt2, &mut db).unwrap();

    // Try constraint violation at sp2 level
    let insert_stmt = insert_stmt!(
        "items".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("DupItem".to_string())),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("items").unwrap();
    assert_eq!(table.row_count(), 2);

    // Rollback to sp2 (should remove nothing after sp2)
    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp2".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("items").unwrap();
    assert_eq!(table.row_count(), 2);

    // Insert successfully after rollback
    let insert_stmt = insert_stmt!(
        "items".to_string(),
        vec!["id".to_string(), "name".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Item3".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("items").unwrap();
    assert_eq!(table.row_count(), 3);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_triple_nested_savepoints_with_errors() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "logs".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("message".to_string(), DataType::Varchar { max_length: Some(255) }, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "logs".to_string(),
        vec!["id".to_string(), "message".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Log1".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "logs".to_string(),
        vec!["id".to_string(), "message".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Log2".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "logs".to_string(),
        vec!["id".to_string(), "message".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("Log3".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp3".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    // Try constraint violation at sp3 level
    let insert_stmt = insert_stmt!(
        "logs".to_string(),
        vec!["id".to_string(), "message".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("DupLog".to_string())),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("logs").unwrap();
    assert_eq!(table.row_count(), 3);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp3".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("logs").unwrap();
    assert_eq!(table.row_count(), 3);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("logs").unwrap();
    assert_eq!(table.row_count(), 1);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// SAVEPOINT NAMING AND EDGE CASE TESTS
// ============================================================================

#[test]
fn test_savepoint_basic_operations() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "data".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("value".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    for name in &["sp_first", "sp_second", "sp_third"] {
        let savepoint_stmt = vibesql_ast::SavepointStmt { name: name.to_string() };
        SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();
    }

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_rollback_to_earliest_savepoint() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "version_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("version".to_string(), DataType::Integer, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp_earliest".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    for i in 1..=10 {
        let insert_stmt = insert_stmt!(
            "version_table".to_string(),
            vec!["id".to_string(), "version".to_string()],
            vibesql_ast::InsertSource::Values(vec![vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(i)),
                vibesql_ast::Expression::Literal(SqlValue::Integer(i * 10)),
            ]])
        );
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    let table = db.get_table("version_table").unwrap();
    assert_eq!(table.row_count(), 10);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp_earliest".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("version_table").unwrap();
    assert_eq!(table.row_count(), 0);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_multiple_savepoints_partial_rollback() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "stages".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("stage".to_string(), DataType::Varchar { max_length: Some(50) }, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Stage 1: create savepoint before any inserts
    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp_before_inserts".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    // Insert first row
    let insert_stmt = insert_stmt!(
        "stages".to_string(),
        vec!["id".to_string(), "stage".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("stage_1".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Insert second row
    let insert_stmt = insert_stmt!(
        "stages".to_string(),
        vec!["id".to_string(), "stage".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("stage_2".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    // Create savepoint after first 2 inserts
    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp_after_2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    // Insert third row
    let insert_stmt = insert_stmt!(
        "stages".to_string(),
        vec!["id".to_string(), "stage".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(3)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("stage_3".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("stages").unwrap();
    assert_eq!(table.row_count(), 3);

    // Rollback to sp_after_2 (should remove row 3)
    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp_after_2".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("stages").unwrap();
    assert_eq!(table.row_count(), 2);

    // Insert replacement row
    let insert_stmt = insert_stmt!(
        "stages".to_string(),
        vec!["id".to_string(), "stage".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(4)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("stage_3b".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("stages").unwrap();
    assert_eq!(table.row_count(), 3);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// COMPREHENSIVE MULTI-ROW ERROR RECOVERY TESTS
// ============================================================================

#[test]
fn test_multi_row_insert_with_constraint_failure() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "records".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("code".to_string(), DataType::Varchar { max_length: Some(50) }, false),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    schema.unique_constraints = vec![vec!["code".to_string()]];
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    for i in 1..=3 {
        let insert_stmt = insert_stmt!(
            "records".to_string(),
            vec!["id".to_string(), "code".to_string()],
            vibesql_ast::InsertSource::Values(vec![vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(i)),
                vibesql_ast::Expression::Literal(SqlValue::Varchar(format!("CODE_{}", i))),
            ]])
        );
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();
    }

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "records".to_string(),
        vec!["id".to_string(), "code".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(4)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("CODE_2".to_string())),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("records").unwrap();
    assert_eq!(table.row_count(), 3);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp1".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "records".to_string(),
        vec!["id".to_string(), "code".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(4)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar("CODE_4".to_string())),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("records").unwrap();
    assert_eq!(table.row_count(), 4);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// TRANSACTION CONSISTENCY TESTS
// ============================================================================

#[test]
fn test_transaction_consistency_after_partial_insert() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "transactions".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("amount".to_string(), DataType::Integer, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "transactions".to_string(),
        vec!["id".to_string(), "amount".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(100)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "transactions".to_string(),
        vec!["id".to_string(), "amount".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(200)),
        ]])
    );
    let result = vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt);
    assert!(result.is_err());

    let table = db.get_table("transactions").unwrap();
    assert_eq!(table.row_count(), 1);

    let insert_stmt = insert_stmt!(
        "transactions".to_string(),
        vec!["id".to_string(), "amount".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(150)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("transactions").unwrap();
    assert_eq!(table.row_count(), 2);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

// ============================================================================
// SAVEPOINT SEQUENCE AND ADVANCED SCENARIOS
// ============================================================================

#[test]
fn test_savepoint_create_and_release_sequence() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "sequence_table".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("seq".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    // Create savepoint before any inserts
    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp_start".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    for i in 1..=5 {
        let insert_stmt = insert_stmt!(
            "sequence_table".to_string(),
            vec!["id".to_string(), "seq".to_string()],
            vibesql_ast::InsertSource::Values(vec![vec![
                vibesql_ast::Expression::Literal(SqlValue::Integer(i)),
                vibesql_ast::Expression::Literal(SqlValue::Integer(i * 10)),
            ]])
        );
        vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

        // Create savepoint after each insert
        if i < 5 {
            let savepoint_stmt = vibesql_ast::SavepointStmt { name: format!("sp{}", i) };
            SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();
        }
    }

    let table = db.get_table("sequence_table").unwrap();
    assert_eq!(table.row_count(), 5);

    // Rollback to sp2 (should remove rows 3, 4, 5)
    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp2".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let table = db.get_table("sequence_table").unwrap();
    assert_eq!(table.row_count(), 2);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

#[test]
fn test_error_recovery_with_sequential_constraint_violations() {
    let mut db = Database::new();

    let mut schema = TableSchema::new(
        "sequential".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("ref_id".to_string(), DataType::Integer, true),
        ],
    );
    schema.primary_key = Some(vec!["id".to_string()]);
    db.create_table(schema).unwrap();

    let begin_stmt = vibesql_ast::BeginStmt;
    BeginTransactionExecutor::execute(&begin_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "sequential".to_string(),
        vec!["id".to_string(), "ref_id".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(100)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp1".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "sequential".to_string(),
        vec!["id".to_string(), "ref_id".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(200)),
        ]])
    );
    assert!(vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).is_err());

    let savepoint_stmt = vibesql_ast::SavepointStmt { name: "sp2".to_string() };
    SavepointExecutor::execute(&savepoint_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "sequential".to_string(),
        vec!["id".to_string(), "ref_id".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(300)),
        ]])
    );
    assert!(vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).is_err());

    let table = db.get_table("sequential").unwrap();
    assert_eq!(table.row_count(), 1);

    let rollback_to_stmt = vibesql_ast::RollbackToSavepointStmt { name: "sp2".to_string() };
    RollbackToSavepointExecutor::execute(&rollback_to_stmt, &mut db).unwrap();

    let insert_stmt = insert_stmt!(
        "sequential".to_string(),
        vec!["id".to_string(), "ref_id".to_string()],
        vibesql_ast::InsertSource::Values(vec![vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(200)),
        ]])
    );
    vibesql_executor::InsertExecutor::execute(&mut db, &insert_stmt).unwrap();

    let table = db.get_table("sequential").unwrap();
    assert_eq!(table.row_count(), 2);

    let commit_stmt = vibesql_ast::CommitStmt;
    CommitExecutor::execute(&commit_stmt, &mut db).unwrap();
}

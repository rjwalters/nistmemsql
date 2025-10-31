//! Transaction and savepoint tests

use crate::*;
use super::helpers::execute_sql;

#[test]
fn test_execute_transactions() {
    let mut db = storage::Database::new();

    // Test BEGIN TRANSACTION
    let begin_sql = "BEGIN TRANSACTION";
    let stmt = parser::Parser::parse_sql(begin_sql).expect("Parse failed");
    match stmt {
        ast::Statement::BeginTransaction(begin_stmt) => {
            executor::BeginTransactionExecutor::execute(&begin_stmt, &mut db)
                .expect("Begin transaction failed");
        }
        _ => panic!("Expected BeginTransaction statement"),
    }

    // Test COMMIT
    let commit_sql = "COMMIT";
    let stmt = parser::Parser::parse_sql(commit_sql).expect("Parse failed");
    match stmt {
        ast::Statement::Commit(commit_stmt) => {
            executor::CommitExecutor::execute(&commit_stmt, &mut db).expect("Commit failed");
        }
        _ => panic!("Expected Commit statement"),
    }
}

#[test]
fn test_execute_savepoints() {
    let mut db = storage::Database::new();

    // Begin transaction first
    let begin_sql = "BEGIN TRANSACTION";
    execute_sql(&mut db, begin_sql).expect("Setup failed");

    // Create savepoint
    let savepoint_sql = "SAVEPOINT sp1";
    let stmt = parser::Parser::parse_sql(savepoint_sql).expect("Parse failed");
    match stmt {
        ast::Statement::Savepoint(savepoint_stmt) => {
            executor::SavepointExecutor::execute(&savepoint_stmt, &mut db)
                .expect("Savepoint failed");
        }
        _ => panic!("Expected Savepoint statement"),
    }

    // Rollback to savepoint
    let rollback_sql = "ROLLBACK TO SAVEPOINT sp1";
    let stmt = parser::Parser::parse_sql(rollback_sql).expect("Parse failed");
    match stmt {
        ast::Statement::RollbackToSavepoint(rollback_stmt) => {
            executor::RollbackToSavepointExecutor::execute(&rollback_stmt, &mut db)
                .expect("Rollback to savepoint failed");
        }
        _ => panic!("Expected RollbackToSavepoint statement"),
    }

    // Release savepoint
    let release_sql = "RELEASE SAVEPOINT sp1";
    let stmt = parser::Parser::parse_sql(release_sql).expect("Parse failed");
    match stmt {
        ast::Statement::ReleaseSavepoint(release_stmt) => {
            executor::ReleaseSavepointExecutor::execute(&release_stmt, &mut db)
                .expect("Release savepoint failed");
        }
        _ => panic!("Expected ReleaseSavepoint statement"),
    }
}

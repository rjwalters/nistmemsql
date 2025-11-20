//! Executor for SEQUENCE objects (SQL:1999)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE SEQUENCE statement
pub fn execute_create_sequence(
    stmt: &CreateSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_sequence(
        stmt.sequence_name.clone(),
        stmt.start_with,
        stmt.increment_by,
        stmt.min_value,
        stmt.max_value,
        stmt.cycle,
    )?;
    Ok(())
}

/// Execute DROP SEQUENCE statement
pub fn execute_drop_sequence(
    stmt: &DropSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // Handle CASCADE to remove sequence dependencies from columns
    db.catalog.drop_sequence(&stmt.sequence_name, stmt.cascade)?;
    Ok(())
}

/// Execute ALTER SEQUENCE statement
pub fn execute_alter_sequence(
    stmt: &AlterSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.alter_sequence(
        &stmt.sequence_name,
        stmt.restart_with,
        stmt.increment_by,
        stmt.min_value,
        stmt.max_value,
        stmt.cycle,
    )?;
    Ok(())
}

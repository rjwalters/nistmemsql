//! Executor for COLLATION objects (SQL:1999)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE COLLATION statement
pub fn execute_create_collation(
    stmt: &CreateCollationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_collation(
        stmt.collation_name.clone(),
        stmt.character_set.clone(),
        stmt.source_collation.clone(),
        stmt.pad_space,
    )?;
    Ok(())
}

/// Execute DROP COLLATION statement
pub fn execute_drop_collation(
    stmt: &DropCollationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_collation(&stmt.collation_name)?;
    Ok(())
}

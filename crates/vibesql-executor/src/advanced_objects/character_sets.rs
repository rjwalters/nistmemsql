//! Executor for CHARACTER SET objects (SQL:1999)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE CHARACTER SET statement
pub fn execute_create_character_set(
    stmt: &CreateCharacterSetStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_character_set(
        stmt.charset_name.clone(),
        stmt.source.clone(),
        stmt.collation.clone(),
    )?;
    Ok(())
}

/// Execute DROP CHARACTER SET statement
pub fn execute_drop_character_set(
    stmt: &DropCharacterSetStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_character_set(&stmt.charset_name)?;
    Ok(())
}

//! Executor for TRANSLATION objects (SQL:1999)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE TRANSLATION statement
pub fn execute_create_translation(
    stmt: &CreateTranslationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_translation(
        stmt.translation_name.clone(),
        stmt.source_charset.clone(),
        stmt.target_charset.clone(),
        stmt.translation_source.clone(),
    )?;
    Ok(())
}

/// Execute DROP TRANSLATION statement
pub fn execute_drop_translation(
    stmt: &DropTranslationStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_translation(&stmt.translation_name)?;
    Ok(())
}

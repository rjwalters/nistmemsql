//! Executor for ASSERTION objects (SQL:1999 Feature F671/F672)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE ASSERTION statement (SQL:1999 Feature F671/F672)
pub fn execute_create_assertion(
    stmt: &CreateAssertionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::Assertion;

    let assertion = Assertion::new(stmt.assertion_name.clone(), (*stmt.check_condition).clone());

    db.catalog.create_assertion(assertion)?;
    Ok(())
}

/// Execute DROP ASSERTION statement (SQL:1999 Feature F671/F672)
pub fn execute_drop_assertion(
    stmt: &DropAssertionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_assertion(&stmt.assertion_name, stmt.cascade)?;
    Ok(())
}

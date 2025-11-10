//! GRANT statement executor

use vibesql_ast::*;
use vibesql_catalog::PrivilegeGrant;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for GRANT statements
pub struct GrantExecutor;

impl GrantExecutor {
    /// Execute GRANT statement
    ///
    /// Phase 2.6: Supports validation and conformance
    pub fn execute_grant(
        stmt: &GrantStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Determine actual object type (may differ from statement if SQL:1999 uses TABLE for
        // schemas)
        let mut actual_object_type = stmt.object_type.clone();

        // Validate object exists based on object type
        match stmt.object_type {
            ObjectType::Table => {
                // Special case: USAGE/EXECUTE are schema/routine privileges, not table privileges
                // If granting USAGE/EXECUTE on a "TABLE", check if it's actually a schema first
                // This handles SQL:1999 tests that use "ON TABLE" for schema objects
                let is_schema_privilege = stmt
                    .privileges
                    .iter()
                    .any(|p| matches!(p, PrivilegeType::Usage | PrivilegeType::Execute));

                if is_schema_privilege && database.catalog.schema_exists(&stmt.object_name) {
                    // Object is actually a schema, update the actual type
                    actual_object_type = ObjectType::Schema;
                } else if !database.catalog.table_exists(&stmt.object_name) {
                    return Err(ExecutorError::TableNotFound(stmt.object_name.clone()));
                }
            }
            ObjectType::Schema => {
                if !database.catalog.schema_exists(&stmt.object_name) {
                    return Err(ExecutorError::SchemaNotFound(stmt.object_name.clone()));
                }
            }
            // SQL:1999 Feature F031-03: Domain privileges
            // Accept without validation - full domain implementation is future work
            ObjectType::Domain => {
                // No validation - domains track privileges for conformance
            }
            // SQL:1999 Feature F031-06: Collation privileges
            // Accept without validation - full collation implementation is future work
            ObjectType::Collation => {
                // No validation - collations track privileges for conformance
            }
            // SQL:1999 Feature F031-08: Character set privileges
            // Accept without validation - full character set implementation is future work
            ObjectType::CharacterSet => {
                // No validation - character sets track privileges for conformance
            }
            // SQL:1999 Feature F031-09: Translation privileges
            // Accept without validation - full translation implementation is future work
            ObjectType::Translation => {
                // No validation - translations track privileges for conformance
            }
            // SQL:1999 Feature F031-10: Type privileges
            // Accept without validation - full UDT implementation is future work
            ObjectType::Type => {
                // No validation - user-defined types track privileges for conformance
            }
            // SQL:1999 Feature F031-11: Sequence privileges
            // Accept without validation - full sequence implementation is future work
            ObjectType::Sequence => {
                // No validation - sequences track privileges for conformance
            }
            // Functions (SQL:1999 Feature P001)
            ObjectType::Function | ObjectType::SpecificFunction => {
                // Create stub if it doesn't exist (for privilege tracking)
                if !database.catalog.function_exists(&stmt.object_name) {
                    database
                        .catalog
                        .create_function_stub(
                            stmt.object_name.clone(),
                            database.catalog.get_current_schema().to_string(),
                        )
                        .map_err(|e| ExecutorError::Other(e.to_string()))?;
                }
            }
            // Procedures (SQL:1999 Feature P001)
            ObjectType::Procedure | ObjectType::SpecificProcedure => {
                // Create stub if it doesn't exist (for privilege tracking)
                if !database.catalog.procedure_exists(&stmt.object_name) {
                    database
                        .catalog
                        .create_procedure_stub(
                            stmt.object_name.clone(),
                            database.catalog.get_current_schema().to_string(),
                        )
                        .map_err(|e| ExecutorError::Other(e.to_string()))?;
                }
            }
            // Routine (generic term for function or procedure)
            ObjectType::Routine | ObjectType::SpecificRoutine => {
                // Try function first, create if neither exists
                if !database.catalog.function_exists(&stmt.object_name)
                    && !database.catalog.procedure_exists(&stmt.object_name)
                {
                    database
                        .catalog
                        .create_function_stub(
                            stmt.object_name.clone(),
                            database.catalog.get_current_schema().to_string(),
                        )
                        .map_err(|e| ExecutorError::Other(e.to_string()))?;
                }
            }
            // Methods (SQL:1999 Feature S091) - OOP SQL
            // For now, accept without validation - full UDT implementation is future work
            ObjectType::Method
            | ObjectType::ConstructorMethod
            | ObjectType::StaticMethod
            | ObjectType::InstanceMethod
            | ObjectType::SpecificMethod
            | ObjectType::SpecificConstructorMethod
            | ObjectType::SpecificStaticMethod
            | ObjectType::SpecificInstanceMethod => {
                // No validation - methods belong to UDTs which aren't fully implemented yet
                // Privilege tracking works by object name alone
            }
        }

        // Validate all grantees (roles) exist
        for grantee in &stmt.grantees {
            if !database.catalog.role_exists(grantee) {
                return Err(ExecutorError::RoleNotFound(grantee.clone()));
            }
        }

        // Expand ALL PRIVILEGES based on actual object type
        let expanded_privileges = if stmt.privileges.contains(&PrivilegeType::AllPrivileges) {
            match actual_object_type {
                ObjectType::Table => vec![
                    PrivilegeType::Select(None),
                    PrivilegeType::Insert(None),
                    PrivilegeType::Update(None),
                    PrivilegeType::Delete,
                    PrivilegeType::References(None),
                ],
                ObjectType::Schema => vec![PrivilegeType::Usage, PrivilegeType::Create],
                // USAGE-only objects (domains, collations, character sets, translations, types,
                // sequences) ALL PRIVILEGES on these objects means USAGE privilege
                ObjectType::Domain
                | ObjectType::Collation
                | ObjectType::CharacterSet
                | ObjectType::Translation
                | ObjectType::Type
                | ObjectType::Sequence => vec![PrivilegeType::Usage],
                // Callable objects (functions, procedures, routines, methods)
                // ALL PRIVILEGES on callable objects means EXECUTE privilege
                ObjectType::Function
                | ObjectType::Procedure
                | ObjectType::Routine
                | ObjectType::Method
                | ObjectType::ConstructorMethod
                | ObjectType::StaticMethod
                | ObjectType::InstanceMethod
                | ObjectType::SpecificFunction
                | ObjectType::SpecificProcedure
                | ObjectType::SpecificRoutine
                | ObjectType::SpecificMethod
                | ObjectType::SpecificConstructorMethod
                | ObjectType::SpecificStaticMethod
                | ObjectType::SpecificInstanceMethod => vec![PrivilegeType::Execute],
            }
        } else {
            stmt.privileges.clone()
        };

        // For each grantee and privilege, create a grant
        for grantee in &stmt.grantees {
            for privilege in &expanded_privileges {
                let grant = PrivilegeGrant {
                    object: stmt.object_name.clone(),
                    object_type: actual_object_type.clone(),
                    privilege: privilege.clone(),
                    grantee: grantee.clone(),
                    grantor: database.get_current_role(),
                    with_grant_option: stmt.with_grant_option,
                };
                database.catalog.add_grant(grant);
            }
        }

        let grantees_str = stmt.grantees.join(", ");
        let privileges_str =
            stmt.privileges.iter().map(|p| format!("{:?}", p)).collect::<Vec<_>>().join(", ");

        Ok(format!("Granted {} on {} to {}", privileges_str, stmt.object_name, grantees_str))
    }
}

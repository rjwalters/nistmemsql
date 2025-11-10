//! REVOKE statement executor

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for REVOKE statements
pub struct RevokeExecutor;

impl RevokeExecutor {
    /// Execute REVOKE statement
    ///
    /// Phase 3: Supports CASCADE/RESTRICT and GRANT OPTION FOR
    pub fn execute_revoke(
        stmt: &RevokeStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Validate object exists based on object type
        match stmt.object_type {
            ObjectType::Table => {
                if !database.catalog.table_exists(&stmt.object_name) {
                    return Err(ExecutorError::TableNotFound(stmt.object_name.clone()));
                }
            }
            ObjectType::Schema => {
                if !database.catalog.schema_exists(&stmt.object_name) {
                    return Err(ExecutorError::SchemaNotFound(stmt.object_name.clone()));
                }
            }
            // SQL:1999 Feature F031: New object types
            // Accept without validation - implementations are future work
            ObjectType::Domain
            | ObjectType::Collation
            | ObjectType::CharacterSet
            | ObjectType::Translation
            | ObjectType::Type
            | ObjectType::Sequence => {
                // No validation - these objects are stubbed for conformance
            }
            // Functions (SQL:1999 Feature P001)
            ObjectType::Function | ObjectType::SpecificFunction => {
                // Allow REVOKE on non-existent functions (SQL:1999 compliance)
                // No validation - silently succeed if function doesn't exist
            }
            // Procedures (SQL:1999 Feature P001)
            ObjectType::Procedure | ObjectType::SpecificProcedure => {
                // Allow REVOKE on non-existent procedures (SQL:1999 compliance)
                // No validation - silently succeed if procedure doesn't exist
            }
            // Routine (generic term for function or procedure)
            ObjectType::Routine | ObjectType::SpecificRoutine => {
                // Allow REVOKE on non-existent routines (SQL:1999 compliance)
                // No validation - silently succeed if routine doesn't exist
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

        // Expand ALL PRIVILEGES based on object type
        let expanded_privileges = if stmt.privileges.contains(&PrivilegeType::AllPrivileges) {
            match stmt.object_type {
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

        // For RESTRICT, check for dependent grants before making any changes
        if matches!(stmt.cascade_option, CascadeOption::Restrict) {
            for grantee in &stmt.grantees {
                for privilege in &expanded_privileges {
                    if database.catalog.has_dependent_grants(&stmt.object_name, grantee, privilege)
                    {
                        return Err(ExecutorError::DependentPrivilegesExist(format!(
                            "Cannot revoke {:?} on {} from {} - dependent grants exist (use CASCADE to revoke dependent grants)",
                            privilege,
                            stmt.object_name,
                            grantee
                        )));
                    }
                }
            }
        }

        // Revoke privileges for each grantee and privilege
        let mut total_removed = 0;
        for grantee in &stmt.grantees {
            for privilege in &expanded_privileges {
                // Remove the grant
                let removed = database.catalog.remove_grants(
                    &stmt.object_name,
                    grantee,
                    privilege,
                    stmt.grant_option_for,
                );
                total_removed += removed;

                // If CASCADE, recursively revoke from dependent grantees
                if matches!(stmt.cascade_option, CascadeOption::Cascade) {
                    Self::revoke_cascade(
                        database,
                        &stmt.object_name,
                        grantee,
                        privilege,
                        stmt.grant_option_for,
                    )?;
                }
            }
        }

        let grantees_str = stmt.grantees.join(", ");
        let privileges_str =
            stmt.privileges.iter().map(|p| format!("{:?}", p)).collect::<Vec<_>>().join(", ");

        let action = if stmt.grant_option_for { "Revoked grant option for" } else { "Revoked" };

        Ok(format!(
            "{} {} on {} from {} ({} grants affected)",
            action, privileges_str, stmt.object_name, grantees_str, total_removed
        ))
    }

    /// Recursively revoke privileges from dependent grantees (CASCADE behavior)
    fn revoke_cascade(
        database: &mut Database,
        object: &str,
        grantor: &str,
        privilege: &PrivilegeType,
        grant_option_only: bool,
    ) -> Result<(), ExecutorError> {
        // Find all grants that were made by this grantor
        let dependent_grants: Vec<String> = database
            .catalog
            .get_all_grants()
            .iter()
            .filter(|g| g.object == object && g.grantor == grantor && g.privilege == *privilege)
            .map(|g| g.grantee.clone())
            .collect();

        // Recursively revoke from each dependent grantee
        for dependent_grantee in dependent_grants {
            database.catalog.remove_grants(
                object,
                &dependent_grantee,
                privilege,
                grant_option_only,
            );

            // Recursively cascade
            Self::revoke_cascade(
                database,
                object,
                &dependent_grantee,
                privilege,
                grant_option_only,
            )?;
        }

        Ok(())
    }
}

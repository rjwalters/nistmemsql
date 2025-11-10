//! Domain DDL executor

use vibesql_ast::*;
use vibesql_catalog::{DomainConstraintDef, DomainDefinition};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for domain DDL statements
pub struct DomainExecutor;

impl DomainExecutor {
    /// Execute CREATE DOMAIN
    pub fn execute_create_domain(
        stmt: &CreateDomainStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Convert AST domain constraints to catalog domain constraints
        let constraints = stmt
            .constraints
            .iter()
            .map(|c| DomainConstraintDef { name: c.name.clone(), check: c.check.clone() })
            .collect();

        let domain = DomainDefinition::new(
            stmt.domain_name.clone(),
            stmt.data_type.clone(),
            stmt.default.clone(),
            constraints,
        );

        database
            .catalog
            .create_domain(domain)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;

        Ok(format!("Domain '{}' created", stmt.domain_name))
    }

    /// Execute DROP DOMAIN
    pub fn execute_drop_domain(
        stmt: &DropDomainStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        database
            .catalog
            .drop_domain(&stmt.domain_name, stmt.cascade)
            .map_err(|e| ExecutorError::StorageError(format!("Catalog error: {:?}", e)))?;

        Ok(format!("Domain '{}' dropped", stmt.domain_name))
    }
}

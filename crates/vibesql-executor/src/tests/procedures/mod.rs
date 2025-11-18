//! Tests for stored procedure and function functionality

use super::common::setup_users_table as setup_test_table;
use vibesql_ast::*;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::advanced_objects;
use crate::errors::ExecutorError;

// Helper function to create a simple procedure with defaults for Phase 6 fields
#[allow(dead_code)]
pub(crate) fn create_simple_procedure(
    name: &str,
    parameters: Vec<ProcedureParameter>,
    body: Vec<ProceduralStatement>,
) -> CreateProcedureStmt {
    CreateProcedureStmt {
        procedure_name: name.to_string(),
        parameters,
        body: ProcedureBody::BeginEnd(body),
        sql_security: None,
        comment: None,
        language: None,
    }
}

mod stored_procedures;
mod functions;
mod parameters;
mod control_flow;
mod error_messages;
mod edge_cases;

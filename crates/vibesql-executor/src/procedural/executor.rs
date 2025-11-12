//! Execute individual procedural statements
//!
//! Handles execution of:
//! - DECLARE (variable declarations)
//! - SET (variable assignments)
//! - SQL statements (SELECT, INSERT, UPDATE, DELETE, etc.)
//! - RETURN (return from function/procedure)
//! - Control flow (delegated to control_flow module)

use crate::errors::ExecutorError;
use crate::procedural::{ControlFlow, ExecutionContext};
use vibesql_ast::{ProceduralStatement, Statement};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute a procedural statement
pub fn execute_procedural_statement(
    stmt: &ProceduralStatement,
    ctx: &mut ExecutionContext,
    db: &mut Database,
) -> Result<ControlFlow, ExecutorError> {
    match stmt {
        ProceduralStatement::Declare {
            name,
            data_type,
            default_value,
        } => {
            // Declare a local variable
            let value = if let Some(expr) = default_value {
                // Evaluate default value expression
                evaluate_expression(expr, db, ctx)?
            } else {
                // No default, use NULL
                SqlValue::Null
            };

            // Store variable with type checking
            let typed_value = cast_to_type(value, data_type)?;
            ctx.set_variable(name, typed_value);
            Ok(ControlFlow::Continue)
        }

        ProceduralStatement::Set { name, value } => {
            // Set variable or parameter value
            let new_value = evaluate_expression(value, db, ctx)?;

            // Try to update parameter first (for OUT/INOUT)
            if ctx.has_parameter(name) {
                // Get the parameter and update it
                if let Some(param) = ctx.get_parameter_mut(name) {
                    *param = new_value;
                }
            } else if ctx.has_variable(name) {
                // Update variable
                ctx.set_variable(name, new_value);
            } else {
                return Err(ExecutorError::VariableNotFound(name.clone()));
            }

            Ok(ControlFlow::Continue)
        }

        ProceduralStatement::Return(expr) => {
            // Evaluate and return the expression
            let value = evaluate_expression(expr, db, ctx)?;
            Ok(ControlFlow::Return(value))
        }

        ProceduralStatement::Leave(label) => {
            // Check if label exists
            if !ctx.has_label(label) {
                return Err(ExecutorError::LabelNotFound(label.clone()));
            }
            Ok(ControlFlow::Leave(label.clone()))
        }

        ProceduralStatement::Iterate(label) => {
            // Check if label exists
            if !ctx.has_label(label) {
                return Err(ExecutorError::LabelNotFound(label.clone()));
            }
            Ok(ControlFlow::Iterate(label.clone()))
        }

        ProceduralStatement::If {
            condition,
            then_statements,
            else_statements,
        } => super::control_flow::execute_if(condition, then_statements, else_statements, ctx, db),

        ProceduralStatement::While {
            condition,
            statements,
        } => super::control_flow::execute_while(condition, statements, ctx, db),

        ProceduralStatement::Loop { statements } => {
            super::control_flow::execute_loop(statements, ctx, db)
        }

        ProceduralStatement::Repeat {
            statements,
            condition,
        } => super::control_flow::execute_repeat(statements, condition, ctx, db),

        ProceduralStatement::Sql(sql_stmt) => {
            // Execute SQL statement
            execute_sql_statement(sql_stmt, db, ctx)?;
            Ok(ControlFlow::Continue)
        }
    }
}

/// Evaluate an expression in the procedural context
///
/// This function evaluates expressions with access to local variables and parameters
pub(crate) fn evaluate_expression(
    expr: &vibesql_ast::Expression,
    _db: &mut Database,
    ctx: &ExecutionContext,
) -> Result<SqlValue, ExecutorError> {
    // Handle ColumnRef which can refer to variables/parameters in procedural context
    if let vibesql_ast::Expression::ColumnRef { table: None, column } = expr {
        // Check if this is a variable or parameter
        if let Some(value) = ctx.get_value(column) {
            return Ok(value.clone());
        }
    }

    // Handle literals directly
    if let vibesql_ast::Expression::Literal(value) = expr {
        return Ok(value.clone());
    }

    // For now, return error for complex expressions
    // TODO: Integrate with full expression evaluator in later phases
    Err(ExecutorError::UnsupportedFeature(format!(
        "Complex expression evaluation in procedures not yet fully implemented: {:?}",
        expr
    )))
}

/// Execute a SQL statement within a procedural context
fn execute_sql_statement(
    _stmt: &Statement,
    _db: &mut Database,
    _ctx: &ExecutionContext,
) -> Result<(), ExecutorError> {
    // TODO: Implement full SQL statement execution in procedures
    // This will be implemented in later phases when we integrate with
    // the full SQL execution engine

    // For now, return an error indicating this is not yet supported
    Err(ExecutorError::UnsupportedFeature(
        "SQL statement execution in procedures not yet fully implemented".to_string()
    ))
}

/// Cast a value to a specific data type
fn cast_to_type(value: SqlValue, target_type: &vibesql_types::DataType) -> Result<SqlValue, ExecutorError> {
    use vibesql_types::DataType;

    // If value is already NULL, return NULL regardless of target type
    if matches!(value, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }

    match target_type {
        DataType::Integer => match value {
            SqlValue::Integer(i) => Ok(SqlValue::Integer(i)),
            SqlValue::Bigint(b) => Ok(SqlValue::Integer(b)),
            SqlValue::Smallint(s) => Ok(SqlValue::Integer(s as i64)),
            SqlValue::Varchar(s) | SqlValue::Character(s) => {
                s.parse::<i64>()
                    .map(SqlValue::Integer)
                    .map_err(|_| ExecutorError::TypeError(format!("Cannot convert '{}' to INTEGER", s)))
            }
            _ => Err(ExecutorError::TypeError(format!(
                "Cannot convert {:?} to INTEGER",
                value
            ))),
        },

        DataType::Varchar { .. } | DataType::Character { .. } => {
            // Convert to string
            Ok(SqlValue::Varchar(value.to_string()))
        }

        DataType::Boolean => match value {
            SqlValue::Boolean(b) => Ok(SqlValue::Boolean(b)),
            SqlValue::Integer(i) => Ok(SqlValue::Boolean(i != 0)),
            SqlValue::Varchar(ref s) | SqlValue::Character(ref s) => {
                let s_upper = s.to_uppercase();
                if s_upper == "TRUE" || s_upper == "T" || s_upper == "1" {
                    Ok(SqlValue::Boolean(true))
                } else if s_upper == "FALSE" || s_upper == "F" || s_upper == "0" {
                    Ok(SqlValue::Boolean(false))
                } else {
                    Err(ExecutorError::TypeError(format!(
                        "Cannot convert '{}' to BOOLEAN",
                        s
                    )))
                }
            }
            _ => Err(ExecutorError::TypeError(format!(
                "Cannot convert {:?} to BOOLEAN",
                value
            ))),
        },

        // For other types, accept the value as-is for now
        _ => Ok(value),
    }
}

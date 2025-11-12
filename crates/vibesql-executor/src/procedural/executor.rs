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
            // Set variable, parameter, or session variable value
            let new_value = evaluate_expression(value, db, ctx)?;

            // Check if it's a session variable (starts with @)
            if name.starts_with('@') {
                let var_name = &name[1..]; // Strip @ prefix
                db.set_session_variable(var_name, new_value);
            } else if ctx.has_parameter(name) {
                // Try to update parameter first (for OUT/INOUT)
                if let Some(param) = ctx.get_parameter_mut(name) {
                    *param = new_value;
                }
            } else if ctx.has_variable(name) {
                // Update local variable
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
/// This function evaluates expressions with access to local variables and parameters.
/// Phase 2 supports simple expressions with variable references and basic operations.
pub fn evaluate_expression(
    expr: &vibesql_ast::Expression,
    _db: &mut Database,
    ctx: &ExecutionContext,
) -> Result<SqlValue, ExecutorError> {
    use vibesql_ast::{BinaryOperator, Expression};

    match expr {
        // Variable, parameter, or session variable reference
        Expression::ColumnRef { table: None, column } => {
            // Check if it's a session variable (starts with @)
            if column.starts_with('@') {
                let var_name = &column[1..]; // Strip @ prefix
                _db.get_session_variable(var_name)
                    .cloned()
                    .ok_or_else(|| ExecutorError::VariableNotFound(format!("@{}", var_name)))
            } else {
                // Regular variable or parameter reference
                ctx.get_value(column)
                    .cloned()
                    .ok_or_else(|| ExecutorError::VariableNotFound(column.clone()))
            }
        }

        // Literal values
        Expression::Literal(value) => Ok(value.clone()),

        // Binary operations (basic arithmetic and comparison)
        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate_expression(left, _db, ctx)?;
            let right_val = evaluate_expression(right, _db, ctx)?;

            match op {
                BinaryOperator::Plus => {
                    // Simple addition for integers
                    match (left_val, right_val) {
                        (SqlValue::Integer(l), SqlValue::Integer(r)) => {
                            Ok(SqlValue::Integer(l + r))
                        }
                        _ => Err(ExecutorError::TypeError(
                            "Binary operation only supports integers in Phase 2".to_string()
                        ))
                    }
                }
                BinaryOperator::Minus => {
                    match (left_val, right_val) {
                        (SqlValue::Integer(l), SqlValue::Integer(r)) => {
                            Ok(SqlValue::Integer(l - r))
                        }
                        _ => Err(ExecutorError::TypeError(
                            "Binary operation only supports integers in Phase 2".to_string()
                        ))
                    }
                }
                BinaryOperator::Multiply => {
                    match (left_val, right_val) {
                        (SqlValue::Integer(l), SqlValue::Integer(r)) => {
                            Ok(SqlValue::Integer(l * r))
                        }
                        _ => Err(ExecutorError::TypeError(
                            "Binary operation only supports integers in Phase 2".to_string()
                        ))
                    }
                }
                BinaryOperator::GreaterThan => {
                    match (left_val, right_val) {
                        (SqlValue::Integer(l), SqlValue::Integer(r)) => {
                            Ok(SqlValue::Boolean(l > r))
                        }
                        _ => Err(ExecutorError::TypeError(
                            "Comparison only supports integers in Phase 2".to_string()
                        ))
                    }
                }
                _ => Err(ExecutorError::UnsupportedFeature(format!(
                    "Binary operator {:?} not yet supported in procedural expressions", op
                )))
            }
        }

        // Function calls - basic support for CONCAT
        Expression::Function { name, args, .. } if name.eq_ignore_ascii_case("CONCAT") => {
            let mut result = String::new();
            for arg in args {
                let val = evaluate_expression(arg, _db, ctx)?;
                result.push_str(&val.to_string());
            }
            Ok(SqlValue::Varchar(result))
        }

        // Other expressions not yet supported
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "Expression type not yet supported in procedures: {:?}", expr
        )))
    }
}

/// Execute a SQL statement within a procedural context
///
/// Phase 2 Limitation: SQL statements in procedures are not yet fully supported.
/// Variables work in DECLARE/SET statements and can be passed as procedure parameters,
/// but SQL statement execution (INSERT/UPDATE/DELETE/SELECT) with variable substitution
/// requires threading the procedural context through the entire SQL execution pipeline.
///
/// This will be implemented in Phase 3 when we add control flow support.
fn execute_sql_statement(
    _stmt: &Statement,
    _db: &mut Database,
    _ctx: &ExecutionContext,
) -> Result<(), ExecutorError> {
    // TODO Phase 3: Implement SQL statement execution with procedural context threading
    // This requires:
    // 1. Modifying all SQL executors to accept optional ExecutionContext
    // 2. Updating expression evaluator to check procedural variables
    // 3. Ensuring variable substitution works in WHERE, VALUES, SET clauses

    Err(ExecutorError::UnsupportedFeature(
        "SQL statements in procedure bodies not yet supported in Phase 2. \
         Use DECLARE, SET, and RETURN statements.".to_string()
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

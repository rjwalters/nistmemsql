//! Executor for advanced SQL:1999 objects (SEQUENCE, TYPE, COLLATION, etc.)
//! Note: DOMAIN has a full implementation in domain_ddl module

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

// DOMAIN functions are in domain_ddl module with full implementation

/// Execute CREATE SEQUENCE statement
pub fn execute_create_sequence(
    stmt: &CreateSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.create_sequence(
        stmt.sequence_name.clone(),
        stmt.start_with,
        stmt.increment_by,
        stmt.min_value,
        stmt.max_value,
        stmt.cycle,
    )?;
    Ok(())
}

/// Execute DROP SEQUENCE statement
pub fn execute_drop_sequence(
    stmt: &DropSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // Handle CASCADE to remove sequence dependencies from columns
    db.catalog.drop_sequence(&stmt.sequence_name, stmt.cascade)?;
    Ok(())
}

/// Execute ALTER SEQUENCE statement
pub fn execute_alter_sequence(
    stmt: &AlterSequenceStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.alter_sequence(
        &stmt.sequence_name,
        stmt.restart_with,
        stmt.increment_by,
        stmt.min_value,
        stmt.max_value,
        stmt.cycle,
    )?;
    Ok(())
}

/// Execute CREATE TYPE statement (comprehensive implementation)
pub fn execute_create_type(stmt: &CreateTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    use vibesql_catalog::{TypeAttribute, TypeDefinition, TypeDefinitionKind};

    // Convert AST TypeDefinition to Catalog TypeDefinitionKind
    let catalog_def = match &stmt.definition {
        vibesql_ast::TypeDefinition::Distinct { base_type } => {
            TypeDefinitionKind::Distinct { base_type: base_type.clone() }
        }
        vibesql_ast::TypeDefinition::Structured { attributes } => {
            let catalog_attrs = attributes
                .iter()
                .map(|attr| TypeAttribute {
                    name: attr.name.clone(),
                    data_type: attr.data_type.clone(),
                })
                .collect();
            TypeDefinitionKind::Structured { attributes: catalog_attrs }
        }
        vibesql_ast::TypeDefinition::Forward => TypeDefinitionKind::Forward,
    };

    let type_def = TypeDefinition { name: stmt.type_name.clone(), definition: catalog_def };

    db.catalog.create_type(type_def)?;
    Ok(())
}

/// Execute DROP TYPE statement (comprehensive implementation with CASCADE/RESTRICT)
pub fn execute_drop_type(stmt: &DropTypeStmt, db: &mut Database) -> Result<(), ExecutorError> {
    let cascade = matches!(stmt.behavior, DropBehavior::Cascade);
    db.catalog.drop_type(&stmt.type_name, cascade)?;
    Ok(())
}

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

/// Execute CREATE VIEW statement
pub fn execute_create_view(stmt: &CreateViewStmt, db: &mut Database) -> Result<(), ExecutorError> {
    use vibesql_catalog::ViewDefinition;

    // If no explicit column list is provided, derive column names from the query
    // This ensures views with SELECT * preserve original column names
    let columns = if stmt.columns.is_none() {
        // Execute the query once to derive column names
        use crate::select::SelectExecutor;
        let executor = SelectExecutor::new(db);
        let result = executor.execute_with_columns(&stmt.query)?;
        Some(result.columns)
    } else {
        stmt.columns.clone()
    };

    let view = ViewDefinition::new(
        stmt.view_name.clone(),
        columns,
        (*stmt.query).clone(),
        stmt.with_check_option,
    );

    if stmt.or_replace {
        // DROP the view if it exists, then CREATE
        let _ = db.catalog.drop_view(&stmt.view_name, false);
        db.catalog.create_view(view)?;
    } else {
        // Regular CREATE VIEW (will fail if view already exists)
        db.catalog.create_view(view)?;
    }
    Ok(())
}

/// Execute DROP VIEW statement
pub fn execute_drop_view(stmt: &DropViewStmt, db: &mut Database) -> Result<(), ExecutorError> {
    // Check if view exists
    let view_exists = db.catalog.get_view(&stmt.view_name).is_some();

    // If IF EXISTS is specified and view doesn't exist, succeed silently
    if stmt.if_exists && !view_exists {
        return Ok(());
    }

    // Handle CASCADE to drop dependent views
    db.catalog.drop_view(&stmt.view_name, stmt.cascade)?;
    Ok(())
}

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

/// Execute CREATE PROCEDURE statement (SQL:1999 Feature P001)
///
/// Creates a stored procedure in the database catalog with optional characteristics.
///
/// # Parameters
///
/// Procedures support three parameter modes:
/// - **IN**: Input-only parameters (read by procedure)
/// - **OUT**: Output-only parameters (written by procedure to session variables)
/// - **INOUT**: Both input and output (read and written)
///
/// # Characteristics (Phase 6)
///
/// - **SQL SECURITY**: DEFINER (default) or INVOKER
/// - **COMMENT**: Documentation string
/// - **LANGUAGE**: SQL (only supported language)
///
/// # Example
///
/// ```sql
/// CREATE PROCEDURE calculate_stats(
///   IN input_value INT,
///   OUT sum_result INT,
///   OUT count_result INT
/// )
///   SQL SECURITY INVOKER
///   COMMENT 'Calculate sum and count from data_table'
/// BEGIN
///   SELECT SUM(value), COUNT(*)
///   INTO sum_result, count_result
///   FROM data_table
///   WHERE value > input_value;
/// END;
/// ```
///
/// # Errors
///
/// Returns error if:
/// - Procedure name already exists
/// - Invalid parameter types
/// - Body parsing fails
pub fn execute_create_procedure(
    stmt: &CreateProcedureStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::{Procedure, ProcedureBody, ProcedureParam, ParameterMode, SqlSecurity};

    // Convert AST parameters to catalog parameters
    let catalog_params = stmt
        .parameters
        .iter()
        .map(|param| {
            let mode = match param.mode {
                vibesql_ast::ParameterMode::In => ParameterMode::In,
                vibesql_ast::ParameterMode::Out => ParameterMode::Out,
                vibesql_ast::ParameterMode::InOut => ParameterMode::InOut,
            };
            ProcedureParam {
                mode,
                name: param.name.clone(),
                data_type: param.data_type.clone(),
            }
        })
        .collect();

    // Convert AST body to catalog body
    let catalog_body = match &stmt.body {
        vibesql_ast::ProcedureBody::BeginEnd(statements) => {
            // Store the parsed AST for execution
            ProcedureBody::BeginEnd(statements.clone())
        }
        vibesql_ast::ProcedureBody::RawSql(sql) => ProcedureBody::RawSql(sql.clone()),
    };

    // Convert characteristics (Phase 6)
    let sql_security = stmt.sql_security.as_ref().map(|sec| match sec {
        vibesql_ast::SqlSecurity::Definer => SqlSecurity::Definer,
        vibesql_ast::SqlSecurity::Invoker => SqlSecurity::Invoker,
    }).unwrap_or(SqlSecurity::Definer);

    let procedure = if stmt.sql_security.is_some() || stmt.comment.is_some() || stmt.language.is_some() {
        Procedure::with_characteristics(
            stmt.procedure_name.clone(),
            db.catalog.get_current_schema().to_string(),
            catalog_params,
            catalog_body,
            sql_security,
            stmt.comment.clone(),
            stmt.language.clone().unwrap_or_else(|| "SQL".to_string()),
        )
    } else {
        Procedure::new(
            stmt.procedure_name.clone(),
            db.catalog.get_current_schema().to_string(),
            catalog_params,
            catalog_body,
        )
    };

    db.catalog.create_procedure_with_characteristics(procedure)?;
    Ok(())
}

/// Execute DROP PROCEDURE statement (SQL:1999 Feature P001)
pub fn execute_drop_procedure(
    stmt: &DropProcedureStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // Check if procedure exists
    let procedure_exists = db.catalog.procedure_exists(&stmt.procedure_name);

    // If IF EXISTS is specified and procedure doesn't exist, succeed silently
    if stmt.if_exists && !procedure_exists {
        return Ok(());
    }

    db.catalog.drop_procedure(&stmt.procedure_name)?;
    Ok(())
}

/// Execute CREATE FUNCTION statement (SQL:1999 Feature P001)
///
/// Creates a user-defined function in the database catalog with optional characteristics.
///
/// Functions differ from procedures:
/// - Functions RETURN a value and can be used in expressions
/// - Functions are read-only (cannot modify database tables)
/// - Functions only support IN parameters (no OUT/INOUT)
/// - Functions have recursion depth limiting (max 100 levels)
///
/// # Characteristics (Phase 6)
///
/// - **DETERMINISTIC**: Indicates same input always produces same output
/// - **SQL SECURITY**: DEFINER (default) or INVOKER
/// - **COMMENT**: Documentation string
/// - **LANGUAGE**: SQL (only supported language)
///
/// # Examples
///
/// Simple function with RETURN expression:
/// ```sql
/// CREATE FUNCTION add_ten(x INT) RETURNS INT
///   DETERMINISTIC
///   RETURN x + 10;
/// ```
///
/// Complex function with BEGIN...END body:
/// ```sql
/// CREATE FUNCTION factorial(n INT) RETURNS INT
///   DETERMINISTIC
///   COMMENT 'Calculate factorial of n'
/// BEGIN
///   DECLARE result INT DEFAULT 1;
///   DECLARE i INT DEFAULT 2;
///
///   WHILE i <= n DO
///     SET result = result * i;
///     SET i = i + 1;
///   END WHILE;
///
///   RETURN result;
/// END;
/// ```
///
/// # Errors
///
/// Returns error if:
/// - Function name already exists
/// - Invalid parameter types or return type
/// - Body parsing fails
pub fn execute_create_function(
    stmt: &CreateFunctionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::{Function, FunctionBody, FunctionParam, SqlSecurity};

    // Convert AST parameters to catalog parameters
    let catalog_params = stmt
        .parameters
        .iter()
        .map(|param| FunctionParam {
            name: param.name.clone(),
            data_type: param.data_type.clone(),
        })
        .collect();

    // Convert AST body to catalog body
    let catalog_body = match &stmt.body {
        vibesql_ast::ProcedureBody::BeginEnd(_) => {
            // For now, store as RawSql. Full execution support comes later.
            FunctionBody::RawSql(format!("{:?}", stmt.body))
        }
        vibesql_ast::ProcedureBody::RawSql(sql) => FunctionBody::RawSql(sql.clone()),
    };

    // Convert characteristics (Phase 6)
    let deterministic = stmt.deterministic.unwrap_or(false);
    let sql_security = stmt.sql_security.as_ref().map(|sec| match sec {
        vibesql_ast::SqlSecurity::Definer => SqlSecurity::Definer,
        vibesql_ast::SqlSecurity::Invoker => SqlSecurity::Invoker,
    }).unwrap_or(SqlSecurity::Definer);

    let function = if stmt.deterministic.is_some() || stmt.sql_security.is_some() || stmt.comment.is_some() || stmt.language.is_some() {
        Function::with_characteristics(
            stmt.function_name.clone(),
            db.catalog.get_current_schema().to_string(),
            catalog_params,
            stmt.return_type.clone(),
            catalog_body,
            deterministic,
            sql_security,
            stmt.comment.clone(),
            stmt.language.clone().unwrap_or_else(|| "SQL".to_string()),
        )
    } else {
        Function::new(
            stmt.function_name.clone(),
            db.catalog.get_current_schema().to_string(),
            catalog_params,
            stmt.return_type.clone(),
            catalog_body,
        )
    };

    db.catalog.create_function_with_characteristics(function)?;
    Ok(())
}

/// Execute DROP FUNCTION statement (SQL:1999 Feature P001)
pub fn execute_drop_function(
    stmt: &DropFunctionStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // Check if function exists
    let function_exists = db.catalog.function_exists(&stmt.function_name);

    // If IF EXISTS is specified and function doesn't exist, succeed silently
    if stmt.if_exists && !function_exists {
        return Ok(());
    }

    db.catalog.drop_function(&stmt.function_name)?;
    Ok(())
}

/// Extract variable name from an expression for OUT/INOUT parameter binding
///
/// Valid patterns:
/// - ColumnRef without table qualifier (treated as session variable): @var_name
/// - Function call to session variable function (if we add one)
///
/// Returns the variable name (without @ prefix) or error if expression is not a valid variable reference.
fn extract_variable_name(expr: &Expression) -> Result<String, ExecutorError> {
    match expr {
        Expression::ColumnRef { table: None, column } => {
            // Column reference without table - treat as session variable
            // If it starts with @, strip it; otherwise use as-is
            let var_name = if let Some(stripped) = column.strip_prefix('@') {
                stripped.to_string()
            } else {
                column.clone()
            };
            Ok(var_name)
        }
        Expression::ColumnRef { table: Some(_), column: _ } => {
            Err(ExecutorError::Other(
                "OUT/INOUT parameter target must be a session variable (e.g., @var_name), not a table.column reference".to_string()
            ))
        }
        Expression::Literal(_) => {
            Err(ExecutorError::Other(
                "OUT/INOUT parameter target cannot be a literal value".to_string()
            ))
        }
        _ => {
            Err(ExecutorError::Other(
                "OUT/INOUT parameter target must be a session variable (e.g., @var_name), not a complex expression".to_string()
            ))
        }
    }
}

/// Execute CALL statement (SQL:1999 Feature P001)
///
/// Executes a stored procedure with parameter binding and procedural statement execution.
///
/// # Parameter Binding
///
/// - **IN parameters**: Values are evaluated and passed to the procedure
/// - **OUT parameters**: Must be session variables (@var), initialized to NULL,
///   procedure can assign values that are returned to caller
/// - **INOUT parameters**: Must be session variables (@var), values are passed in
///   and can be modified by the procedure
///
/// # Example
///
/// ```sql
/// -- Create procedure with mixed parameter modes
/// CREATE PROCEDURE update_counter(
///   IN increment INT,
///   INOUT counter INT,
///   OUT message VARCHAR(100)
/// )
/// BEGIN
///   SET counter = counter + increment;
///   SET message = CONCAT('Counter updated to ', counter);
/// END;
///
/// -- Call procedure
/// SET @count = 10;
/// CALL update_counter(5, @count, @msg);
/// SELECT @count, @msg;
/// -- Output: count=15, msg='Counter updated to 15'
/// ```
///
/// # Control Flow
///
/// The procedure body executes sequentially until:
/// - All statements complete (normal exit)
/// - RETURN statement is encountered (early exit)
/// - Error occurs (execution stops)
///
/// After execution, OUT and INOUT parameter values are written back to their
/// corresponding session variables.
///
/// # Errors
///
/// Returns error if:
/// - Procedure not found
/// - Wrong number of arguments
/// - OUT/INOUT parameter is not a session variable
/// - Type mismatch in parameter binding
/// - Execution error in procedure body
pub fn execute_call(
    stmt: &CallStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use crate::procedural::{ExecutionContext, execute_procedural_statement, ControlFlow};

    // 1. Look up the procedure definition and clone what we need
    let (parameters, body) = {
        let procedure = db.catalog.get_procedure(&stmt.procedure_name);

        match procedure {
            Some(proc) => (proc.parameters.clone(), proc.body.clone()),
            None => {
                // Procedure not found - provide helpful error with suggestions
                let schema_name = db.catalog.get_current_schema().to_string();
                let available_procedures = db.catalog.list_procedures();

                return Err(ExecutorError::ProcedureNotFound {
                    procedure_name: stmt.procedure_name.clone(),
                    schema_name,
                    available_procedures,
                });
            }
        }
    };

    // 2. Validate parameter count
    if stmt.arguments.len() != parameters.len() {
        // Build parameter signature string
        let param_sig = parameters
            .iter()
            .map(|p| {
                let mode = match p.mode {
                    vibesql_catalog::ParameterMode::In => "IN",
                    vibesql_catalog::ParameterMode::Out => "OUT",
                    vibesql_catalog::ParameterMode::InOut => "INOUT",
                };
                format!("{} {} {:?}", mode, p.name, p.data_type)
            })
            .collect::<Vec<_>>()
            .join(", ");

        return Err(ExecutorError::ParameterCountMismatch {
            routine_name: stmt.procedure_name.clone(),
            routine_type: "Procedure".to_string(),
            expected: parameters.len(),
            actual: stmt.arguments.len(),
            parameter_signature: param_sig,
        });
    }

    // 3. Create execution context and bind parameters
    let mut ctx = ExecutionContext::new();

    for (param, arg_expr) in parameters.iter().zip(&stmt.arguments) {
        match param.mode {
            vibesql_catalog::ParameterMode::In => {
                // IN parameter: Evaluate and bind the value
                let value = crate::procedural::executor::evaluate_expression(arg_expr, db, &ctx)?;
                ctx.set_parameter(&param.name, value);
            }
            vibesql_catalog::ParameterMode::Out => {
                // OUT parameter: Initialize to NULL
                ctx.set_parameter(&param.name, vibesql_types::SqlValue::Null);

                // Extract and register target variable name
                let var_name = extract_variable_name(arg_expr)?;
                ctx.register_out_parameter(&param.name, var_name);
            }
            vibesql_catalog::ParameterMode::InOut => {
                // INOUT parameter: Evaluate and bind input value
                let value = crate::procedural::executor::evaluate_expression(arg_expr, db, &ctx)?;
                ctx.set_parameter(&param.name, value);

                // Extract and register target variable name
                let var_name = extract_variable_name(arg_expr)?;
                ctx.register_out_parameter(&param.name, var_name);
            }
        }
    }

    // 4. Execute procedure body
    match &body {
        vibesql_catalog::ProcedureBody::BeginEnd(statements) => {
            // Execute each procedural statement sequentially
            for stmt in statements {
                match execute_procedural_statement(stmt, &mut ctx, db)? {
                    ControlFlow::Continue => {
                        // Continue to next statement
                    }
                    ControlFlow::Return(_) => {
                        // RETURN in procedures exits early (functions will handle return value later)
                        break;
                    }
                    ControlFlow::Leave(_) | ControlFlow::Iterate(_) => {
                        // Control flow statements not yet supported in Phase 2
                        return Err(ExecutorError::UnsupportedFeature(
                            "Control flow (LEAVE/ITERATE) not yet supported in Phase 2".to_string()
                        ));
                    }
                }
            }
            Ok(())
        }
        vibesql_catalog::ProcedureBody::RawSql(_) => {
            // RawSql fallback - would require parsing
            Err(ExecutorError::UnsupportedFeature(
                "RawSql procedure bodies require parsing (use BEGIN/END block instead)".to_string()
            ))
        }
    }?;

    // 5. Return output parameter values to session variables
    for (param_name, target_var_name) in ctx.get_out_parameters() {
        // Get the parameter value from the context
        let value = ctx.get_parameter(param_name)
            .cloned()
            .ok_or_else(|| ExecutorError::Other(format!("Parameter '{}' not found", param_name)))?;

        // Store in session variable
        db.set_session_variable(target_var_name, value);
    }

    Ok(())
}

/// Execute CREATE TRIGGER statement
pub fn execute_create_trigger(
    stmt: &CreateTriggerStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::TriggerDefinition;

    let trigger = TriggerDefinition::new(
        stmt.trigger_name.clone(),
        stmt.timing.clone(),
        stmt.event.clone(),
        stmt.table_name.clone(),
        stmt.granularity.clone(),
        stmt.when_condition.clone(),
        stmt.triggered_action.clone(),
    );

    db.catalog.create_trigger(trigger)?;
    Ok(())
}

/// Execute DROP TRIGGER statement
pub fn execute_drop_trigger(
    stmt: &DropTriggerStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    db.catalog.drop_trigger(&stmt.trigger_name)?;
    Ok(())
}

//! Trigger execution logic for firing triggers on DML operations

use vibesql_ast::{TriggerEvent, TriggerGranularity, TriggerTiming};
use vibesql_catalog::TriggerDefinition;
use vibesql_storage::{Database, Row};

use crate::errors::ExecutorError;

/// Helper struct for trigger firing (execution during DML operations)
pub struct TriggerFirer;

impl TriggerFirer {
    /// Find triggers for a table and event
    ///
    /// # Arguments
    /// * `db` - Database reference
    /// * `table_name` - Name of the table to find triggers for
    /// * `timing` - Trigger timing (BEFORE, AFTER, INSTEAD OF)
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    ///
    /// # Returns
    /// Vector of trigger definitions matching the criteria, sorted by creation order
    pub fn find_triggers(
        db: &Database,
        table_name: &str,
        timing: TriggerTiming,
        event: TriggerEvent,
    ) -> Vec<TriggerDefinition> {
        db.catalog
            .get_triggers_for_table(table_name, Some(event.clone()))
            .filter(|trigger| trigger.timing == timing)
            .cloned()
            .collect()
    }

    /// Execute a single trigger
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `trigger` - Trigger definition to execute
    /// * `old_row` - OLD row for UPDATE/DELETE (None for INSERT)
    /// * `new_row` - NEW row for INSERT/UPDATE (None for DELETE)
    ///
    /// # Returns
    /// Ok(()) if trigger executed successfully, Err if execution failed
    ///
    /// # Notes
    /// - For ROW-level triggers, this is called once per affected row
    /// - For STATEMENT-level triggers, this is called once per statement
    /// - WHEN conditions are evaluated here
    pub fn execute_trigger(
        db: &mut Database,
        trigger: &TriggerDefinition,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<(), ExecutorError> {
        // 1. Evaluate WHEN condition (if present)
        if let Some(when_expr) = &trigger.when_condition {
            let condition_result = Self::evaluate_when_condition(
                db,
                &trigger.table_name,
                when_expr,
                old_row,
                new_row,
            )?;

            // Skip trigger execution if WHEN condition is false
            if !condition_result {
                return Ok(());
            }
        }

        // 2. Execute trigger action
        Self::execute_trigger_action(db, trigger, old_row, new_row)?;

        Ok(())
    }

    /// Evaluate WHEN condition for a trigger
    ///
    /// # Arguments
    /// * `db` - Database reference
    /// * `table_name` - Name of the table
    /// * `when_expr` - WHEN condition expression
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(true) if condition evaluates to true, Ok(false) otherwise
    fn evaluate_when_condition(
        db: &Database,
        table_name: &str,
        when_expr: &vibesql_ast::Expression,
        _old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<bool, ExecutorError> {
        // Get table schema
        let schema = db
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        // For now, we'll evaluate the condition with the NEW row context
        // TODO: Phase 5 will add proper OLD/NEW pseudo-table support
        let row = new_row.ok_or_else(|| {
            ExecutorError::UnsupportedExpression("WHEN condition requires NEW row".to_string())
        })?;

        // Create evaluator and evaluate expression
        let evaluator = crate::ExpressionEvaluator::with_database(schema, db);
        let result = evaluator.eval(when_expr, row)?;

        // Convert to boolean
        match result {
            vibesql_types::SqlValue::Boolean(b) => Ok(b),
            vibesql_types::SqlValue::Null => Ok(false),
            _ => Err(ExecutorError::UnsupportedExpression(
                "WHEN condition must evaluate to boolean".to_string(),
            )),
        }
    }

    /// Execute trigger action statements
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `trigger` - Trigger definition
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(()) if action executed successfully, Err if execution failed
    fn execute_trigger_action(
        db: &mut Database,
        trigger: &TriggerDefinition,
        _old_row: Option<&Row>,
        _new_row: Option<&Row>,
    ) -> Result<(), ExecutorError> {
        // Extract SQL from trigger action
        let sql = match &trigger.triggered_action {
            vibesql_ast::TriggerAction::RawSql(sql) => sql.clone(),
        };

        // Parse the trigger action SQL
        // For Phase 3, we'll parse and execute the SQL directly
        // Phase 5 will add OLD/NEW context support
        let statements = Self::parse_trigger_sql(&sql)?;

        // Execute each statement in the trigger body
        for statement in statements {
            Self::execute_statement(db, &statement)?;
        }

        Ok(())
    }

    /// Parse trigger SQL into statements
    ///
    /// # Arguments
    /// * `sql` - Raw SQL string from trigger action
    ///
    /// # Returns
    /// Vector of parsed statements
    fn parse_trigger_sql(sql: &str) -> Result<Vec<vibesql_ast::Statement>, ExecutorError> {
        // Strip BEGIN/END wrapper if present
        let sql = sql.trim();
        let sql = if sql.to_uppercase().starts_with("BEGIN") {
            // Remove BEGIN and END
            let sql = sql[5..].trim();
            if sql.to_uppercase().ends_with("END") {
                &sql[..sql.len() - 3]
            } else {
                sql
            }
        } else {
            sql
        };

        // Split by semicolons and parse each statement
        let mut statements = Vec::new();
        for stmt_sql in sql.split(';') {
            let stmt_sql = stmt_sql.trim();
            if stmt_sql.is_empty() || stmt_sql.starts_with("--") {
                // Skip empty statements or comments
                continue;
            }

            match vibesql_parser::Parser::parse_sql(stmt_sql) {
                Ok(stmt) => statements.push(stmt),
                Err(e) => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Failed to parse trigger SQL: {}",
                        e.message
                    )))
                }
            }
        }

        // If no statements parsed (e.g., trigger body was only comments), that's OK
        // Just return empty vector
        Ok(statements)
    }

    /// Execute a single statement from trigger body
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `statement` - Statement to execute
    ///
    /// # Returns
    /// Ok(()) if statement executed successfully
    fn execute_statement(
        db: &mut Database,
        statement: &vibesql_ast::Statement,
    ) -> Result<(), ExecutorError> {
        use vibesql_ast::Statement;

        match statement {
            Statement::Insert(insert_stmt) => {
                crate::InsertExecutor::execute(db, insert_stmt)?;
                Ok(())
            }
            Statement::Update(update_stmt) => {
                crate::UpdateExecutor::execute(update_stmt, db)?;
                Ok(())
            }
            Statement::Delete(delete_stmt) => {
                crate::DeleteExecutor::execute(delete_stmt, db)?;
                Ok(())
            }
            Statement::Select(select_stmt) => {
                // Execute SELECT but ignore results (useful for side effects)
                let executor = crate::SelectExecutor::new(db);
                executor.execute_with_columns(select_stmt)?;
                Ok(())
            }
            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Statement type not supported in triggers: {:?}",
                statement
            ))),
        }
    }

    /// Execute all BEFORE triggers for an operation
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `table_name` - Name of the table
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(()) if all triggers executed successfully
    pub fn execute_before_triggers(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<(), ExecutorError> {
        let triggers = Self::find_triggers(db, table_name, TriggerTiming::Before, event);

        for trigger in triggers {
            // Only execute ROW-level triggers in this phase
            // STATEMENT-level triggers are future work
            if trigger.granularity == TriggerGranularity::Row {
                Self::execute_trigger(db, &trigger, old_row, new_row)?;
            }
        }

        Ok(())
    }

    /// Execute all AFTER triggers for an operation
    ///
    /// # Arguments
    /// * `db` - Mutable database reference
    /// * `table_name` - Name of the table
    /// * `event` - Trigger event (INSERT, UPDATE, DELETE)
    /// * `old_row` - OLD row (for UPDATE/DELETE)
    /// * `new_row` - NEW row (for INSERT/UPDATE)
    ///
    /// # Returns
    /// Ok(()) if all triggers executed successfully
    pub fn execute_after_triggers(
        db: &mut Database,
        table_name: &str,
        event: TriggerEvent,
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<(), ExecutorError> {
        let triggers = Self::find_triggers(db, table_name, TriggerTiming::After, event);

        for trigger in triggers {
            // Only execute ROW-level triggers in this phase
            // STATEMENT-level triggers are future work
            if trigger.granularity == TriggerGranularity::Row {
                Self::execute_trigger(db, &trigger, old_row, new_row)?;
            }
        }

        Ok(())
    }
}

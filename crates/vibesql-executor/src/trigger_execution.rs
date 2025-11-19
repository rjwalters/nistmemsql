//! Trigger execution logic for firing triggers on DML operations

use std::cell::Cell;

use vibesql_ast::{PseudoTable, TriggerEvent, TriggerGranularity, TriggerTiming};
use vibesql_catalog::{TableSchema, TriggerDefinition};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Maximum trigger recursion depth to prevent infinite loops
const MAX_TRIGGER_RECURSION_DEPTH: usize = 16;

thread_local! {
    /// Current trigger recursion depth for this thread
    static TRIGGER_RECURSION_DEPTH: Cell<usize> = Cell::new(0);
}

/// RAII guard for managing trigger recursion depth
/// Increments depth on creation, decrements on drop
struct RecursionGuard;

impl RecursionGuard {
    /// Create a new recursion guard, incrementing the depth
    ///
    /// # Returns
    /// Ok(RecursionGuard) if depth is within limits, Err if limit exceeded
    fn new() -> Result<Self, ExecutorError> {
        TRIGGER_RECURSION_DEPTH.with(|depth| {
            let current = depth.get();
            if current >= MAX_TRIGGER_RECURSION_DEPTH {
                Err(ExecutorError::UnsupportedExpression(format!(
                    "Trigger recursion depth limit exceeded (max: {}). Possible infinite trigger loop.",
                    MAX_TRIGGER_RECURSION_DEPTH
                )))
            } else {
                depth.set(current + 1);
                Ok(RecursionGuard)
            }
        })
    }
}

impl Drop for RecursionGuard {
    fn drop(&mut self) {
        TRIGGER_RECURSION_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

/// Execution context for triggers with OLD/NEW row access
/// Provides pseudo-variable resolution for trigger bodies
pub struct TriggerContext<'a> {
    /// OLD row - available for UPDATE and DELETE triggers
    pub old_row: Option<&'a Row>,
    /// NEW row - available for INSERT and UPDATE triggers
    pub new_row: Option<&'a Row>,
    /// Table schema for column lookups
    pub table_schema: &'a TableSchema,
}

impl<'a> TriggerContext<'a> {
    /// Resolve a pseudo-variable reference to a SqlValue
    ///
    /// # Arguments
    /// * `pseudo_table` - Which pseudo-table (OLD or NEW)
    /// * `column` - Column name to retrieve
    ///
    /// # Returns
    /// Ok(SqlValue) with the column value, or Err if invalid
    ///
    /// # Errors
    /// - If OLD/NEW is not available for this trigger type
    /// - If column doesn't exist in table schema
    pub fn resolve_pseudo_var(
        &self,
        pseudo_table: PseudoTable,
        column: &str,
    ) -> Result<SqlValue, ExecutorError> {
        // Get the appropriate row
        let row = match pseudo_table {
            PseudoTable::Old => self.old_row.ok_or_else(|| {
                ExecutorError::UnsupportedExpression(
                    "OLD pseudo-variable not available in this trigger context".to_string(),
                )
            })?,
            PseudoTable::New => self.new_row.ok_or_else(|| {
                ExecutorError::UnsupportedExpression(
                    "NEW pseudo-variable not available in this trigger context".to_string(),
                )
            })?,
        };

        // Find column index in schema
        let col_idx = self
            .table_schema
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| ExecutorError::ColumnNotFound {
                column_name: column.to_string(),
                table_name: self.table_schema.name.clone(),
                searched_tables: vec![self.table_schema.name.clone()],
                available_columns: self.table_schema.columns.iter().map(|c| c.name.clone()).collect(),
            })?;

        // Return the value
        Ok(row.values[col_idx].clone())
    }
}

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
        old_row: Option<&Row>,
        new_row: Option<&Row>,
    ) -> Result<bool, ExecutorError> {
        // Get table schema
        let schema = db
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        // Use NEW row as the base row for evaluation (prefer NEW over OLD)
        // The trigger context will handle OLD/NEW pseudo-variable references
        let row = new_row.or(old_row).ok_or_else(|| {
            ExecutorError::UnsupportedExpression("WHEN condition requires a row context".to_string())
        })?;

        // Create trigger context for OLD/NEW pseudo-variable resolution
        let trigger_context = TriggerContext {
            old_row,
            new_row,
            table_schema: schema,
        };

        // Create evaluator with trigger context
        let evaluator = crate::ExpressionEvaluator::with_trigger_context(schema, db, &trigger_context);
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
        // Check recursion depth before executing any triggers
        let _guard = RecursionGuard::new()?;

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
        // Check recursion depth before executing any triggers
        let _guard = RecursionGuard::new()?;

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

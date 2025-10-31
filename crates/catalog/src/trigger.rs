//! Trigger definitions for SQL triggers

use ast::{TriggerAction, TriggerEvent, TriggerGranularity, TriggerTiming};

/// Trigger definition stored in the catalog
#[derive(Debug, Clone)]
pub struct TriggerDefinition {
    /// Name of the trigger
    pub name: String,
    /// Trigger timing (BEFORE, AFTER, INSTEAD OF)
    pub timing: TriggerTiming,
    /// Trigger event (INSERT, UPDATE, DELETE)
    pub event: TriggerEvent,
    /// Table name the trigger is on
    pub table_name: String,
    /// Granularity (ROW or STATEMENT)
    pub granularity: TriggerGranularity,
    /// Optional WHEN condition
    pub when_condition: Option<Box<ast::Expression>>,
    /// Triggered action (procedural SQL)
    pub triggered_action: TriggerAction,
}

impl TriggerDefinition {
    /// Create a new trigger definition
    pub fn new(
        name: String,
        timing: TriggerTiming,
        event: TriggerEvent,
        table_name: String,
        granularity: TriggerGranularity,
        when_condition: Option<Box<ast::Expression>>,
        triggered_action: TriggerAction,
    ) -> Self {
        TriggerDefinition {
            name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
        }
    }
}

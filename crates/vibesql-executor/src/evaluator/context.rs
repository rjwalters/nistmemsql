//! Context management for expression evaluation
//!
//! This module provides context types for different evaluation scenarios:
//! - Trigger context (OLD/NEW pseudo-variables)
//! - Procedural context (stored procedure/function variables)
//! - CTE context (WITH clause results)

/// Contexts available during expression evaluation
/// These provide access to different scopes and variable bindings
pub(super) struct EvaluationContext<'a> {
    /// Trigger context for OLD/NEW pseudo-variable resolution
    pub trigger_context: Option<&'a crate::trigger_execution::TriggerContext<'a>>,
    /// Procedural context for stored procedure/function variable resolution
    pub procedural_context: Option<&'a crate::procedural::ExecutionContext>,
    /// CTE (Common Table Expression) context for accessing WITH clause results
    pub cte_context: Option<&'a std::collections::HashMap<String, crate::select::cte::CteResult>>,
}

impl<'a> EvaluationContext<'a> {
    /// Create a new empty evaluation context
    pub fn new() -> Self {
        Self {
            trigger_context: None,
            procedural_context: None,
            cte_context: None,
        }
    }

    /// Create context with trigger context
    pub fn with_trigger(trigger_context: &'a crate::trigger_execution::TriggerContext<'a>) -> Self {
        Self {
            trigger_context: Some(trigger_context),
            procedural_context: None,
            cte_context: None,
        }
    }

    /// Create context with procedural context
    pub fn with_procedural(procedural_context: &'a crate::procedural::ExecutionContext) -> Self {
        Self {
            trigger_context: None,
            procedural_context: Some(procedural_context),
            cte_context: None,
        }
    }

    /// Create context with CTE context
    pub fn with_cte(cte_context: &'a std::collections::HashMap<String, crate::select::cte::CteResult>) -> Self {
        Self {
            trigger_context: None,
            procedural_context: None,
            cte_context: Some(cte_context),
        }
    }
}

impl<'a> Default for EvaluationContext<'a> {
    fn default() -> Self {
        Self::new()
    }
}

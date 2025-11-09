//! SelectExecutor construction and initialization

use std::time::Instant;

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    pub(super) database: &'a storage::Database,
    pub(super) _outer_row: Option<&'a storage::Row>,
    pub(super) _outer_schema: Option<&'a crate::schema::CombinedSchema>,
    /// Subquery nesting depth (for preventing stack overflow)
    pub(super) subquery_depth: usize,
    /// Execution start time for timeout enforcement
    pub(super) start_time: Instant,
    /// Maximum query execution time in seconds (exposed for testing)
    pub timeout_seconds: u64,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a storage::Database) -> Self {
        SelectExecutor {
            database,
            _outer_row: None,
            _outer_schema: None,
            subquery_depth: 0,
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
        }
    }

    /// Create a new SELECT executor with outer context for correlated subqueries
    pub fn new_with_outer_context(
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
    ) -> Self {
        SelectExecutor {
            database,
            _outer_row: Some(outer_row),
            _outer_schema: Some(outer_schema),
            subquery_depth: 0,
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
        }
    }

    /// Create a new SELECT executor with outer context and explicit depth
    /// Used when creating subquery executors to track nesting depth
    pub fn new_with_outer_context_and_depth(
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
        parent_depth: usize,
    ) -> Self {
        SelectExecutor {
            database,
            _outer_row: Some(outer_row),
            _outer_schema: Some(outer_schema),
            subquery_depth: parent_depth + 1,
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
        }
    }

    /// Check if query has exceeded timeout (exposed for testing)
    pub fn check_timeout(&self) -> Result<(), crate::errors::ExecutorError> {
        let elapsed = self.start_time.elapsed().as_secs();
        if elapsed > self.timeout_seconds {
            return Err(crate::errors::ExecutorError::QueryTimeoutExceeded {
                elapsed_seconds: elapsed,
                max_seconds: self.timeout_seconds,
            });
        }
        Ok(())
    }
}

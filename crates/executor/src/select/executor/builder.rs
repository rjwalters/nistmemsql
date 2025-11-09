//! SelectExecutor construction and initialization

use std::cell::{Cell, RefCell};
use crate::limits::{MAX_MEMORY_BYTES, MEMORY_WARNING_BYTES};
use crate::errors::ExecutorError;

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    pub(super) database: &'a storage::Database,
    pub(super) _outer_row: Option<&'a storage::Row>,
    pub(super) _outer_schema: Option<&'a crate::schema::CombinedSchema>,
    /// Subquery nesting depth (for preventing stack overflow)
    pub(super) subquery_depth: usize,
    /// Memory used by this query execution (in bytes)
    pub(super) memory_used_bytes: Cell<usize>,
    /// Flag to prevent logging the same warning multiple times
    pub(super) memory_warning_logged: Cell<bool>,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a storage::Database) -> Self {
        SelectExecutor {
            database,
            _outer_row: None,
            _outer_schema: None,
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
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
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
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
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
        }
    }

    /// Track memory allocation
    pub(super) fn track_memory_allocation(&self, bytes: usize) -> Result<(), ExecutorError> {
        let mut current = self.memory_used_bytes.get();
        current += bytes;
        self.memory_used_bytes.set(current);

        // Log warning at threshold
        if !self.memory_warning_logged.get() && current > MEMORY_WARNING_BYTES {
            eprintln!(
                "⚠️  Query memory usage: {:.2} GB",
                current as f64 / 1024.0 / 1024.0 / 1024.0
            );
            self.memory_warning_logged.set(true);
        }

        // Hard limit
        if current > MAX_MEMORY_BYTES {
            return Err(ExecutorError::MemoryLimitExceeded {
                used_bytes: current,
                max_bytes: MAX_MEMORY_BYTES,
            });
        }

        Ok(())
    }

    /// Track memory deallocation
    pub(super) fn track_memory_deallocation(&self, bytes: usize) {
        let current = self.memory_used_bytes.get();
        self.memory_used_bytes.set(current.saturating_sub(bytes));
    }
}

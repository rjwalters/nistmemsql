//! SelectExecutor construction and initialization

use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    time::Instant,
};

use crate::{
    errors::ExecutorError,
    limits::{MAX_MEMORY_BYTES, MEMORY_WARNING_BYTES},
};

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    pub(super) database: &'a vibesql_storage::Database,
    pub(super) _outer_row: Option<&'a vibesql_storage::Row>,
    pub(super) _outer_schema: Option<&'a crate::schema::CombinedSchema>,
    /// Procedural context for stored procedure/function variable resolution
    pub(super) procedural_context: Option<&'a crate::procedural::ExecutionContext>,
    /// Subquery nesting depth (for preventing stack overflow)
    pub(super) subquery_depth: usize,
    /// Memory used by this query execution (in bytes)
    pub(super) memory_used_bytes: Cell<usize>,
    /// Flag to prevent logging the same warning multiple times
    pub(super) memory_warning_logged: Cell<bool>,
    /// Query start time (for timeout enforcement)
    pub(super) start_time: Instant,
    /// Timeout in seconds (defaults to MAX_QUERY_EXECUTION_SECONDS)
    pub timeout_seconds: u64,
    /// Cache for aggregate results within a single group
    /// Key: Hash of the aggregate expression (format: "{name}:{distinct}:{arg_debug}")
    /// Value: Cached aggregate result
    /// Scope: Per-group evaluation (cleared between groups)
    pub(super) aggregate_cache: RefCell<HashMap<String, vibesql_types::SqlValue>>,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a vibesql_storage::Database) -> Self {
        SelectExecutor {
            database,
            _outer_row: None,
            _outer_schema: None,
            procedural_context: None,
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Create a new SELECT executor with outer context for correlated subqueries
    pub fn new_with_outer_context(
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
    ) -> Self {
        SelectExecutor {
            database,
            _outer_row: Some(outer_row),
            _outer_schema: Some(outer_schema),
            procedural_context: None,
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Create a new SELECT executor with outer context and explicit depth
    /// Used when creating subquery executors to track nesting depth
    ///
    /// # Note on Timeout Inheritance
    ///
    /// Currently subqueries get their own 60s timeout rather than sharing parent's timeout.
    /// This means a query with N subqueries could run for up to N*60s instead of 60s total.
    ///
    /// However, this is acceptable for the initial fix because:
    /// 1. The main regression (100% timeout) was caused by ZERO timeout enforcement
    /// 2. Having per-subquery timeouts still prevents infinite loops (the core issue)
    /// 3. Most problematic queries cause recursive subquery execution, which IS caught
    /// 4. Threading timeout through evaluators requires extensive refactoring
    ///
    /// Future improvement: Add timeout fields to ExpressionEvaluator and pass through
    /// See: https://github.com/rjwalters/vibesql/issues/1012#subquery-timeout
    pub fn new_with_outer_context_and_depth(
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
        parent_depth: usize,
    ) -> Self {
        SelectExecutor {
            database,
            _outer_row: Some(outer_row),
            _outer_schema: Some(outer_schema),
            procedural_context: None,
            subquery_depth: parent_depth + 1,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Create a new SELECT executor with procedural context for stored procedures/functions
    pub fn new_with_procedural_context(
        database: &'a vibesql_storage::Database,
        procedural_context: &'a crate::procedural::ExecutionContext,
    ) -> Self {
        SelectExecutor {
            database,
            _outer_row: None,
            _outer_schema: None,
            procedural_context: Some(procedural_context),
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: Instant::now(),
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            aggregate_cache: RefCell::new(HashMap::new()),
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
    #[cfg(test)]
    pub(super) fn track_memory_deallocation(&self, bytes: usize) {
        let current = self.memory_used_bytes.get();
        self.memory_used_bytes.set(current.saturating_sub(bytes));
    }

    /// Override default timeout for this query (useful for testing)
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = seconds;
        self
    }

    /// Clear aggregate cache (should be called between group evaluations)
    pub(super) fn clear_aggregate_cache(&self) {
        self.aggregate_cache.borrow_mut().clear();
    }

    /// Get access to the query buffer pool for reducing allocations
    pub(crate) fn query_buffer_pool(&self) -> &vibesql_storage::QueryBufferPool {
        self.database.query_buffer_pool()
    }

    /// Check if query has exceeded timeout
    /// Call this in hot loops to prevent infinite execution
    pub fn check_timeout(&self) -> Result<(), crate::errors::ExecutorError> {
        let elapsed = self.start_time.elapsed().as_secs();
        if elapsed >= self.timeout_seconds {
            return Err(crate::errors::ExecutorError::QueryTimeoutExceeded {
                elapsed_seconds: elapsed,
                max_seconds: self.timeout_seconds,
            });
        }
        Ok(())
    }
}

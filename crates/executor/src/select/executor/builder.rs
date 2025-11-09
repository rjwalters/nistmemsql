//! SelectExecutor construction and initialization

use std::cell::Cell;
use std::time::Instant;
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
    /// Query start time (for timeout enforcement)
    pub(super) start_time: Instant,
    /// Timeout in seconds (defaults to MAX_QUERY_EXECUTION_SECONDS)
    pub(super) timeout_seconds: u64,
    /// Progress tracking fields (for verbose logging)
    pub(super) rows_processed: usize,
    pub(super) last_progress_log: Instant,
    pub(super) verbose: bool,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a storage::Database) -> Self {
        let verbose = std::env::var("SQLLOGICTEST_VERBOSE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let now = Instant::now();
        SelectExecutor {
            database,
            _outer_row: None,
            _outer_schema: None,
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: now,
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            rows_processed: 0,
            last_progress_log: now,
            verbose,
        }
    }

    /// Create a new SELECT executor with outer context for correlated subqueries
    pub fn new_with_outer_context(
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
    ) -> Self {
        let verbose = std::env::var("SQLLOGICTEST_VERBOSE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let now = Instant::now();
        SelectExecutor {
            database,
            _outer_row: Some(outer_row),
            _outer_schema: Some(outer_schema),
            subquery_depth: 0,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: now,
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            rows_processed: 0,
            last_progress_log: now,
            verbose,
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
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a crate::schema::CombinedSchema,
        parent_depth: usize,
    ) -> Self {
        let verbose = std::env::var("SQLLOGICTEST_VERBOSE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let now = Instant::now();
        SelectExecutor {
            database,
            _outer_row: Some(outer_row),
            _outer_schema: Some(outer_schema),
            subquery_depth: parent_depth + 1,
            memory_used_bytes: Cell::new(0),
            memory_warning_logged: Cell::new(false),
            start_time: now,
            timeout_seconds: crate::limits::MAX_QUERY_EXECUTION_SECONDS,
            rows_processed: 0,
            last_progress_log: now,
            verbose,
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

    /// Log progress if enough time has elapsed
    pub(super) fn log_progress_if_needed(&mut self) {
        if !self.verbose {
            return;
        }

        let progress_interval = std::env::var("SQLLOGICTEST_PROGRESS_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        let elapsed_since_last_log = self.last_progress_log.elapsed().as_secs();

        if elapsed_since_last_log >= progress_interval {
            let total_elapsed = self.start_time.elapsed().as_secs_f64();
            eprintln!("  → Processed {} rows ({:.1}s elapsed)",
                self.rows_processed, total_elapsed);
            self.last_progress_log = Instant::now();
        }
    }

    /// Increment row counter and log progress periodically
    pub(super) fn track_row_processed(&mut self) {
        self.rows_processed += 1;

        // Check progress every 10,000 rows
        if self.rows_processed % 10_000 == 0 {
            self.log_progress_if_needed();
        }
    }

    /// Override default timeout for this query (useful for testing)
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout_seconds = seconds;
        self
    }

    /// Check if query has exceeded timeout
    /// Call this in hot loops to prevent infinite execution
    pub(super) fn check_timeout(&self) -> Result<(), crate::errors::ExecutorError> {
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

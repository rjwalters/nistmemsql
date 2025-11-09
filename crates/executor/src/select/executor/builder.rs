//! SelectExecutor construction and initialization

use std::time::Instant;

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    pub(super) database: &'a storage::Database,
    pub(super) _outer_row: Option<&'a storage::Row>,
    pub(super) _outer_schema: Option<&'a crate::schema::CombinedSchema>,
    /// Subquery nesting depth (for preventing stack overflow)
    pub(super) subquery_depth: usize,
    /// Progress tracking fields (for verbose logging)
    pub(super) rows_processed: usize,
    pub(super) start_time: Instant,
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
            rows_processed: 0,
            start_time: now,
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
            rows_processed: 0,
            start_time: now,
            last_progress_log: now,
            verbose,
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
        let verbose = std::env::var("SQLLOGICTEST_VERBOSE")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false);

        let now = Instant::now();
        SelectExecutor {
            database,
            _outer_row: Some(outer_row),
            _outer_schema: Some(outer_schema),
            subquery_depth: parent_depth + 1,
            rows_processed: 0,
            start_time: now,
            last_progress_log: now,
            verbose,
        }
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
}

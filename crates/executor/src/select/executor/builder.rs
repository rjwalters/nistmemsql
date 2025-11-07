//! SelectExecutor construction and initialization

/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    pub(super) database: &'a storage::Database,
    pub(super) _outer_row: Option<&'a storage::Row>,
    pub(super) _outer_schema: Option<&'a crate::schema::CombinedSchema>,
    /// Subquery nesting depth (for preventing stack overflow)
    pub(super) subquery_depth: usize,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a storage::Database) -> Self {
        SelectExecutor {
            database,
            _outer_row: None,
            _outer_schema: None,
            subquery_depth: 0,
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
        }
    }
}

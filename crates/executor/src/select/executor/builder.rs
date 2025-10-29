//! SelectExecutor construction and initialization


/// Executes SELECT queries
pub struct SelectExecutor<'a> {
    pub(super) database: &'a storage::Database,
    pub(super) outer_row: Option<&'a storage::Row>,
    pub(super) outer_schema: Option<&'a catalog::TableSchema>,
}

impl<'a> SelectExecutor<'a> {
    /// Create a new SELECT executor
    pub fn new(database: &'a storage::Database) -> Self {
        SelectExecutor {
            database,
            outer_row: None,
            outer_schema: None,
        }
    }

    /// Create a new SELECT executor with outer context for correlated subqueries
    pub fn new_with_outer_context(
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a catalog::TableSchema,
    ) -> Self {
        SelectExecutor {
            database,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
        }
    }
}

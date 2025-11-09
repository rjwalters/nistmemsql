use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use crate::select::WindowFunctionKey;
use std::cell::RefCell;
use std::collections::HashMap;

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    pub(super) schema: &'a catalog::TableSchema,
    pub(super) outer_row: Option<&'a storage::Row>,
    pub(super) outer_schema: Option<&'a catalog::TableSchema>,
    pub(super) database: Option<&'a storage::Database>,
    /// Current depth in expression tree (for preventing stack overflow)
    pub(super) depth: usize,
}

/// Evaluates expressions with combined schema (for JOINs)
pub struct CombinedExpressionEvaluator<'a> {
    pub(super) schema: &'a CombinedSchema,
    pub(super) database: Option<&'a storage::Database>,
    pub(super) outer_row: Option<&'a storage::Row>,
    pub(super) outer_schema: Option<&'a CombinedSchema>,
    pub(super) window_mapping: Option<&'a std::collections::HashMap<WindowFunctionKey, usize>>,
    /// Cache for column lookups to avoid repeated schema traversals
    column_cache: RefCell<HashMap<(Option<String>, String), usize>>,
    /// Current depth in expression tree (for preventing stack overflow)
    pub(super) depth: usize,
}

impl<'a> ExpressionEvaluator<'a> {
    /// Create a new expression evaluator for a given schema
    pub fn new(schema: &'a catalog::TableSchema) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: None,
            depth: 0,
        }
    }

    /// Create a new expression evaluator with outer query context for correlated subqueries
    pub fn with_outer_context(
        schema: &'a catalog::TableSchema,
        outer_row: &'a storage::Row,
        outer_schema: &'a catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: None,
            depth: 0,
        }
    }

    /// Create a new expression evaluator with database reference for subqueries
    pub fn with_database(
        schema: &'a catalog::TableSchema,
        database: &'a storage::Database,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
            depth: 0,
        }
    }

    /// Create a new expression evaluator with database and outer context (for correlated subqueries)
    pub fn with_database_and_outer_context(
        schema: &'a catalog::TableSchema,
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: Some(database),
            depth: 0,
        }
    }

    /// Evaluate a binary operation
    pub(crate) fn eval_binary_op(
        &self,
        left: &types::SqlValue,
        op: &ast::BinaryOperator,
        right: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        Self::eval_binary_op_static(left, op, right)
    }

    /// Static version of eval_binary_op for shared logic
    ///
    /// Delegates to the new trait-based operator registry for improved modularity.
    pub(crate) fn eval_binary_op_static(
        left: &types::SqlValue,
        op: &ast::BinaryOperator,
        right: &types::SqlValue,
    ) -> Result<types::SqlValue, ExecutorError> {
        super::operators::OperatorRegistry::eval_binary_op(left, op, right)
    }

    /// Compare two SQL values for equality (NULL-safe for simple CASE)
    /// Uses IS NOT DISTINCT FROM semantics where NULL = NULL is TRUE
    pub(crate) fn values_are_equal(left: &types::SqlValue, right: &types::SqlValue) -> bool {
        use types::SqlValue::*;

        // SQL:1999 semantics for CASE equality:
        // - NULL = NULL is TRUE (different from WHERE clause behavior!)
        // - This is "IS NOT DISTINCT FROM" semantics
        match (left, right) {
            (Null, Null) => true,
            (Null, _) | (_, Null) => false,
            (Integer(a), Integer(b)) => a == b,
            (Varchar(a), Varchar(b)) => a == b,
            (Character(a), Character(b)) => a == b,
            (Character(a), Varchar(b)) | (Varchar(a), Character(b)) => a == b,
            (Boolean(a), Boolean(b)) => a == b,
            _ => false, // Type mismatch = not equal
        }
    }

    /// Helper to execute a closure with incremented depth
    pub(super) fn with_incremented_depth<F, T>(&self, f: F) -> Result<T, ExecutorError>
    where
        F: FnOnce(&Self) -> Result<T, ExecutorError>,
    {
        // Create a new evaluator with incremented depth
        let evaluator = ExpressionEvaluator {
            schema: self.schema,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            database: self.database,
            depth: self.depth + 1,
        };
        f(&evaluator)
    }
}

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Create a new combined expression evaluator
    /// Note: Currently unused as all callers use with_database(), but kept for API completeness
    #[allow(dead_code)]
    pub(crate) fn new(schema: &'a CombinedSchema) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: None,
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
        }
    }

    /// Create a new combined expression evaluator with database reference
    pub(crate) fn with_database(
        schema: &'a CombinedSchema,
        database: &'a storage::Database,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
        }
    }

    /// Create a new combined expression evaluator with database and outer context for correlated subqueries
    pub(crate) fn with_database_and_outer_context(
        schema: &'a CombinedSchema,
        database: &'a storage::Database,
        outer_row: &'a storage::Row,
        outer_schema: &'a CombinedSchema,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            window_mapping: None,
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
        }
    }

    /// Create a new combined expression evaluator with database and window mapping
    pub(crate) fn with_database_and_windows(
        schema: &'a CombinedSchema,
        database: &'a storage::Database,
        window_mapping: &'a std::collections::HashMap<WindowFunctionKey, usize>,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: Some(window_mapping),
            column_cache: RefCell::new(HashMap::new()),
            depth: 0,
        }
    }

    /// Get column index with caching to avoid repeated schema lookups
    pub(crate) fn get_column_index_cached(
        &self,
        table: Option<&str>,
        column: &str,
    ) -> Option<usize> {
        let key = (table.map(|s| s.to_string()), column.to_string());

        // Check cache first
        if let Some(&idx) = self.column_cache.borrow().get(&key) {
            return Some(idx);
        }

        // Cache miss: lookup and store
        if let Some(idx) = self.schema.get_column_index(table, column) {
            self.column_cache.borrow_mut().insert(key, idx);
            Some(idx)
        } else {
            None
        }
    }

    /// Helper to execute a closure with incremented depth
    pub(super) fn with_incremented_depth<F, T>(&self, f: F) -> Result<T, ExecutorError>
    where
        F: FnOnce(&Self) -> Result<T, ExecutorError>,
    {
        // Create a new evaluator with incremented depth
        let evaluator = CombinedExpressionEvaluator {
            schema: self.schema,
            database: self.database,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            window_mapping: self.window_mapping,
            // Share the column cache between parent and child evaluators
            column_cache: RefCell::new(self.column_cache.borrow().clone()),
            depth: self.depth + 1,
        };
        f(&evaluator)
    }
}

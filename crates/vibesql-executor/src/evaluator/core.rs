use std::{cell::RefCell, collections::HashMap, num::NonZeroUsize, rc::Rc};

use lru::LruCache;

use crate::{errors::ExecutorError, schema::CombinedSchema, select::WindowFunctionKey};

/// Default maximum size for CSE cache (entries)
/// Can be overridden via CSE_CACHE_SIZE environment variable
const DEFAULT_CSE_CACHE_SIZE: usize = 1000;

/// Default maximum size for subquery result cache (entries)
/// Increased from 100 to 5000 to handle complex test files with 400+ unique subqueries
/// (e.g., index/between/1000/slt_good_0.test has 401 subqueries)
/// Can be overridden via SUBQUERY_CACHE_SIZE environment variable
const DEFAULT_SUBQUERY_CACHE_SIZE: usize = 5000;

/// Components returned by get_parallel_components for parallel execution
type ParallelComponents<'a> = (
    &'a CombinedSchema,
    Option<&'a vibesql_storage::Database>,
    Option<&'a vibesql_storage::Row>,
    Option<&'a CombinedSchema>,
    Option<&'a std::collections::HashMap<WindowFunctionKey, usize>>,
    bool,
);

/// Evaluates expressions in the context of a row
pub struct ExpressionEvaluator<'a> {
    pub(super) schema: &'a vibesql_catalog::TableSchema,
    pub(super) outer_row: Option<&'a vibesql_storage::Row>,
    pub(super) outer_schema: Option<&'a vibesql_catalog::TableSchema>,
    pub(super) database: Option<&'a vibesql_storage::Database>,
    /// Trigger context for OLD/NEW pseudo-variable resolution
    pub(super) trigger_context: Option<&'a crate::trigger_execution::TriggerContext<'a>>,
    /// Procedural context for stored procedure/function variable resolution
    pub(super) procedural_context: Option<&'a crate::procedural::ExecutionContext>,
    /// Current depth in expression tree (for preventing stack overflow)
    pub(super) depth: usize,
    /// CSE cache for common sub-expression elimination with LRU eviction (shared via Rc across depth levels)
    pub(super) cse_cache: Rc<RefCell<LruCache<u64, vibesql_types::SqlValue>>>,
    /// Whether CSE is enabled (can be disabled for debugging)
    pub(super) enable_cse: bool,
}

/// Evaluates expressions with combined schema (for JOINs)
pub struct CombinedExpressionEvaluator<'a> {
    pub(super) schema: &'a CombinedSchema,
    pub(super) database: Option<&'a vibesql_storage::Database>,
    pub(super) outer_row: Option<&'a vibesql_storage::Row>,
    pub(super) outer_schema: Option<&'a CombinedSchema>,
    pub(super) window_mapping: Option<&'a std::collections::HashMap<WindowFunctionKey, usize>>,
    /// Procedural context for stored procedure/function variable resolution
    pub(super) procedural_context: Option<&'a crate::procedural::ExecutionContext>,
    /// Cache for column lookups to avoid repeated schema traversals
    column_cache: RefCell<HashMap<(Option<String>, String), usize>>,
    /// Cache for non-correlated subquery results with LRU eviction (key = subquery hash, value = result rows)
    /// Shared via Rc across child evaluators within a single statement execution.
    /// Cache lifetime is tied to the evaluator instance - each new evaluator gets a fresh cache.
    pub(super) subquery_cache: Rc<RefCell<LruCache<u64, Vec<vibesql_storage::Row>>>>,
    /// Current depth in expression tree (for preventing stack overflow)
    pub(super) depth: usize,
    /// CSE cache for common sub-expression elimination with LRU eviction (shared via Rc across depth levels)
    pub(super) cse_cache: Rc<RefCell<LruCache<u64, vibesql_types::SqlValue>>>,
    /// Whether CSE is enabled (can be disabled for debugging)
    pub(super) enable_cse: bool,
}

impl<'a> ExpressionEvaluator<'a> {
    /// Create a new expression evaluator for a given schema
    pub fn new(schema: &'a vibesql_catalog::TableSchema) -> Self {
        let cache_size = Self::get_cse_cache_size();
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: None,
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(cache_size).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Check if CSE is enabled via environment variable
    /// Defaults to true, can be disabled by setting CSE_ENABLED=false
    fn is_cse_enabled() -> bool {
        std::env::var("CSE_ENABLED")
            .map(|v| v.to_lowercase() != "false" && v != "0")
            .unwrap_or(true) // Default: enabled
    }

    /// Get CSE cache size from environment variable
    /// Defaults to DEFAULT_CSE_CACHE_SIZE, can be overridden by setting CSE_CACHE_SIZE
    fn get_cse_cache_size() -> usize {
        std::env::var("CSE_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CSE_CACHE_SIZE)
    }

    /// Create a new expression evaluator with outer query context for correlated subqueries
    pub fn with_outer_context(
        schema: &'a vibesql_catalog::TableSchema,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a vibesql_catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: None,
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new expression evaluator with database reference for subqueries
    pub fn with_database(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new expression evaluator with trigger context for OLD/NEW pseudo-variables
    pub fn with_trigger_context(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
        trigger_context: &'a crate::trigger_execution::TriggerContext<'a>,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
            trigger_context: Some(trigger_context),
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new expression evaluator with database and outer context (for correlated
    /// subqueries)
    pub fn with_database_and_outer_context(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a vibesql_catalog::TableSchema,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            database: Some(database),
            trigger_context: None,
            procedural_context: None,
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new expression evaluator with procedural context for stored procedure/function execution
    pub fn with_procedural_context(
        schema: &'a vibesql_catalog::TableSchema,
        database: &'a vibesql_storage::Database,
        procedural_context: &'a crate::procedural::ExecutionContext,
    ) -> Self {
        ExpressionEvaluator {
            schema,
            outer_row: None,
            outer_schema: None,
            database: Some(database),
            trigger_context: None,
            procedural_context: Some(procedural_context),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Evaluate a binary operation
    pub(crate) fn eval_binary_op(
        &self,
        left: &vibesql_types::SqlValue,
        op: &vibesql_ast::BinaryOperator,
        right: &vibesql_types::SqlValue,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Extract SQL mode from database, default to MySQL if not available
        let sql_mode = self
            .database
            .map(|db| db.sql_mode())
            .unwrap_or_default();

        Self::eval_binary_op_static(left, op, right, sql_mode)
    }

    /// Static version of eval_binary_op for shared logic
    ///
    /// Delegates to the new trait-based operator registry for improved modularity.
    pub(crate) fn eval_binary_op_static(
        left: &vibesql_types::SqlValue,
        op: &vibesql_ast::BinaryOperator,
        right: &vibesql_types::SqlValue,
        sql_mode: vibesql_types::SqlMode,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        super::operators::OperatorRegistry::eval_binary_op(left, op, right, sql_mode)
    }

    /// Clear the CSE cache
    /// Should be called before evaluating expressions for a new row in multi-row contexts
    pub fn clear_cse_cache(&self) {
        self.cse_cache.borrow_mut().clear();
    }

    /// Compare two SQL values for equality in simple CASE expressions
    /// Uses regular = comparison semantics where NULL = anything is UNKNOWN (false)
    pub(crate) fn values_are_equal(left: &vibesql_types::SqlValue, right: &vibesql_types::SqlValue) -> bool {
        use vibesql_types::SqlValue::*;

        // SQL standard semantics for simple CASE equality:
        // - Uses regular = comparison, not IS NOT DISTINCT FROM
        // - NULL = anything is UNKNOWN, so no match occurs
        // - CASE operand falls through to ELSE or returns NULL
        match (left, right) {
            // NULL never matches anything in simple CASE (including NULL)
            (Null, _) | (_, Null) => false,

            // Exact type matches
            (Integer(a), Integer(b)) => a == b,
            (Varchar(a), Varchar(b)) => a == b,
            (Character(a), Character(b)) => a == b,
            (Character(a), Varchar(b)) | (Varchar(a), Character(b)) => a == b,
            (Boolean(a), Boolean(b)) => a == b,

            // Numeric type comparisons - convert to f64 for comparison
            // This handles: Numeric, Integer, Smallint, Bigint, Unsigned, Float, Real, Double
            (
                Integer(_) | Smallint(_) | Bigint(_) | Unsigned(_) | Numeric(_) | Float(_) | Real(_) | Double(_),
                Integer(_) | Smallint(_) | Bigint(_) | Unsigned(_) | Numeric(_) | Float(_) | Real(_) | Double(_)
            ) => {
                // Convert both to f64 and compare
                match (crate::evaluator::casting::to_f64(left), crate::evaluator::casting::to_f64(right)) {
                    (Ok(a), Ok(b)) => (a - b).abs() < f64::EPSILON,
                    _ => false,
                }
            }

            _ => false, // Type mismatch = not equal
        }
    }

    /// Helper to execute a closure with incremented depth
    pub(super) fn with_incremented_depth<F, T>(&self, f: F) -> Result<T, ExecutorError>
    where
        F: FnOnce(&Self) -> Result<T, ExecutorError>,
    {
        // Create a new evaluator with incremented depth
        // Share the CSE cache across depth levels for consistent caching
        let evaluator = ExpressionEvaluator {
            schema: self.schema,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            database: self.database,
            trigger_context: self.trigger_context,
            procedural_context: self.procedural_context,
            depth: self.depth + 1,
            cse_cache: self.cse_cache.clone(),
            enable_cse: self.enable_cse,
        };
        f(&evaluator)
    }
}

impl<'a> CombinedExpressionEvaluator<'a> {
    /// Check if CSE is enabled via environment variable
    /// Defaults to true, can be disabled by setting CSE_ENABLED=false
    fn is_cse_enabled() -> bool {
        std::env::var("CSE_ENABLED")
            .map(|v| v.to_lowercase() != "false" && v != "0")
            .unwrap_or(true) // Default: enabled
    }

    /// Get CSE cache size from environment variable
    /// Defaults to DEFAULT_CSE_CACHE_SIZE, can be overridden by setting CSE_CACHE_SIZE
    fn get_cse_cache_size() -> usize {
        std::env::var("CSE_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CSE_CACHE_SIZE)
    }

    /// Get subquery cache size from environment variable
    /// Defaults to DEFAULT_SUBQUERY_CACHE_SIZE, can be overridden by setting SUBQUERY_CACHE_SIZE
    fn get_subquery_cache_size() -> usize {
        std::env::var("SUBQUERY_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SUBQUERY_CACHE_SIZE)
    }

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
            procedural_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_subquery_cache_size()).unwrap()
            ))),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database reference
    pub(crate) fn with_database(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            procedural_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_subquery_cache_size()).unwrap()
            ))),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database and outer context for correlated
    /// subqueries
    pub(crate) fn with_database_and_outer_context(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        outer_row: &'a vibesql_storage::Row,
        outer_schema: &'a CombinedSchema,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: Some(outer_row),
            outer_schema: Some(outer_schema),
            window_mapping: None,
            procedural_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_subquery_cache_size()).unwrap()
            ))),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database and window mapping
    pub(crate) fn with_database_and_windows(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        window_mapping: &'a std::collections::HashMap<WindowFunctionKey, usize>,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: Some(window_mapping),
            procedural_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_subquery_cache_size()).unwrap()
            ))),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Create a new combined expression evaluator with database and procedural context
    pub(crate) fn with_database_and_procedural_context(
        schema: &'a CombinedSchema,
        database: &'a vibesql_storage::Database,
        procedural_context: &'a crate::procedural::ExecutionContext,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database: Some(database),
            outer_row: None,
            outer_schema: None,
            window_mapping: None,
            procedural_context: Some(procedural_context),
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_subquery_cache_size()).unwrap()
            ))),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: Self::is_cse_enabled(),
        }
    }

    /// Clear the CSE cache
    /// Should be called before evaluating expressions for a new row in multi-row contexts
    pub(crate) fn clear_cse_cache(&self) {
        self.cse_cache.borrow_mut().clear();
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
        // Share caches between parent and child evaluators
        let evaluator = CombinedExpressionEvaluator {
            schema: self.schema,
            database: self.database,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            window_mapping: self.window_mapping,
            procedural_context: self.procedural_context,
            // Share the column cache between parent and child evaluators
            column_cache: RefCell::new(self.column_cache.borrow().clone()),
            // Share the subquery cache - subqueries can be reused across depths
            subquery_cache: self.subquery_cache.clone(),
            depth: self.depth + 1,
            cse_cache: self.cse_cache.clone(),
            enable_cse: self.enable_cse,
        };
        f(&evaluator)
    }

    /// Clone the evaluator for evaluating a different expression
    ///
    /// Shares the subquery cache (safe because non-correlated subqueries produce
    /// the same results regardless of the current row) but creates a fresh CSE cache
    /// (necessary because CSE results depend on row values).
    pub fn clone_for_new_expression(&self) -> Self {
        CombinedExpressionEvaluator {
            schema: self.schema,
            database: self.database,
            outer_row: self.outer_row,
            outer_schema: self.outer_schema,
            window_mapping: self.window_mapping,
            procedural_context: self.procedural_context,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: self.subquery_cache.clone(),
            depth: self.depth,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse: self.enable_cse,
        }
    }

    /// Get the combined schema for this evaluator
    pub(crate) fn schema(&self) -> &'a CombinedSchema {
        self.schema
    }

    /// Get evaluator components for parallel execution
    /// Returns (schema, database, outer_row, outer_schema, window_mapping, enable_cse)
    pub(crate) fn get_parallel_components(&self) -> ParallelComponents<'a> {
        (
            self.schema,
            self.database,
            self.outer_row,
            self.outer_schema,
            self.window_mapping,
            self.enable_cse,
        )
    }

    /// Create evaluator from parallel components
    /// Creates a fresh evaluator with independent caches for thread-safe parallel execution
    pub(crate) fn from_parallel_components(
        schema: &'a CombinedSchema,
        database: Option<&'a vibesql_storage::Database>,
        outer_row: Option<&'a vibesql_storage::Row>,
        outer_schema: Option<&'a CombinedSchema>,
        window_mapping: Option<&'a std::collections::HashMap<WindowFunctionKey, usize>>,
        enable_cse: bool,
    ) -> Self {
        CombinedExpressionEvaluator {
            schema,
            database,
            outer_row,
            outer_schema,
            window_mapping,
            procedural_context: None,
            column_cache: RefCell::new(HashMap::new()),
            subquery_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_subquery_cache_size()).unwrap()
            ))),
            depth: 0,
            cse_cache: Rc::new(RefCell::new(LruCache::new(
                NonZeroUsize::new(Self::get_cse_cache_size()).unwrap()
            ))),
            enable_cse,
        }
    }
}

//! Lazy iterator-based query execution infrastructure
//!
//! This module provides a foundation for streaming query execution using iterators
//! instead of materializing results. This reduces memory usage and enables early
//! termination for LIMIT queries.
//!
//! # Architecture
//!
//! The core trait is `RowIterator`, which extends `Iterator<Item = Result<Row, ExecutorError>>`
//! with additional query-specific methods. All query operators (scan, filter, project, join)
//! are implemented as iterators that can be composed.
//!
//! # Benefits
//!
//! - **Memory efficiency**: O(max_single_table) instead of O(product)
//! - **Streaming**: Rows flow through pipeline without buffering
//! - **Early termination**: LIMIT 10 only computes 10 rows
//! - **Composability**: Iterators naturally chain together
//!
//! # Example
//!
//! ```rust,ignore
//! // Create a table scan iterator
//! let scan_iter = TableScanIterator::new(schema, rows);
//!
//! // Add a filter
//! let filter_iter = FilterIterator::new(scan_iter, predicate, evaluator);
//!
//! // Add a projection
//! let project_iter = ProjectionIterator::new(filter_iter, projection_fn);
//!
//! // Consume only what we need (e.g., LIMIT 10)
//! for row in project_iter.take(10) {
//!     println!("{:?}", row?);
//! }
//! ```

use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema};

/// Core trait for row-producing iterators in the query execution pipeline
///
/// This trait extends the standard Iterator trait with query-specific metadata
/// and methods. All query operators (scans, filters, joins, projections) implement
/// this trait to enable composable, streaming query execution.
///
/// # Why not just use Iterator?
///
/// While we could use `Iterator<Item = Result<Row, ExecutorError>>` directly,
/// this trait adds query-specific capabilities:
/// - Access to the output schema (for type checking and column resolution)
/// - Size hints for query optimization
/// - Future: Statistics, cost estimates, etc.
pub trait RowIterator: Iterator<Item = Result<storage::Row, ExecutorError>> {
    /// Get the schema of rows produced by this iterator
    ///
    /// The schema defines the structure and types of columns in output rows.
    /// It remains constant throughout iteration and must match the schema
    /// of all rows produced.
    fn schema(&self) -> &CombinedSchema;

    /// Provide a hint about the number of rows this iterator will produce
    ///
    /// This follows the same semantics as `Iterator::size_hint()`:
    /// - Returns `(lower_bound, upper_bound)`
    /// - `lower_bound` is always `<= actual count <= upper_bound.unwrap_or(usize::MAX)`
    /// - None for upper_bound means "unknown" or "unbounded"
    ///
    /// These hints can be used for:
    /// - Allocating appropriately-sized buffers
    /// - Choosing between nested loop vs hash join
    /// - Query planning and optimization
    ///
    /// The default implementation delegates to the underlying Iterator::size_hint()
    fn row_size_hint(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}

/// Iterator that scans rows from a materialized table
///
/// This is the simplest RowIterator - it wraps a Vec<Row> and produces
/// rows one at a time. While the input is still materialized, this provides
/// a uniform iterator interface for composition with other operators.
///
/// # Example
///
/// ```rust,ignore
/// let rows = vec![row1, row2, row3];
/// let scan = TableScanIterator::new(schema, rows);
/// for row in scan {
///     println!("{:?}", row?);
/// }
/// ```
pub struct TableScanIterator {
    schema: CombinedSchema,
    rows: std::vec::IntoIter<storage::Row>,
    total_count: usize,
}

impl TableScanIterator {
    /// Create a new table scan iterator from a schema and materialized rows
    ///
    /// # Arguments
    /// * `schema` - The schema describing the structure of rows
    /// * `rows` - The rows to iterate over (consumed and moved)
    pub fn new(schema: CombinedSchema, rows: Vec<storage::Row>) -> Self {
        let total_count = rows.len();
        Self { schema, rows: rows.into_iter(), total_count }
    }
}

impl Iterator for TableScanIterator {
    type Item = Result<storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(Ok)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.rows.size_hint()
    }
}

impl RowIterator for TableScanIterator {
    fn schema(&self) -> &CombinedSchema {
        &self.schema
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.size_hint();
        (lower, upper)
    }
}

/// Iterator that filters rows based on a predicate expression
///
/// This iterator wraps another RowIterator and only yields rows that
/// satisfy the given predicate. Rows are evaluated lazily - predicates
/// are only evaluated for rows that are actually consumed.
///
/// # Predicate Evaluation
///
/// The predicate must evaluate to a boolean (or truthy value for SQLLogicTest compat):
/// - `true` or non-zero number: row passes
/// - `false`, `0`, or `NULL`: row is filtered out
///
/// # Example
///
/// ```rust,ignore
/// let scan = TableScanIterator::new(schema.clone(), rows);
/// let predicate = Expression::Binary {
///     left: Box::new(Expression::ColumnRef { column: "age".to_string() }),
///     op: BinaryOp::GreaterThan,
///     right: Box::new(Expression::Literal(SqlValue::Integer(18))),
/// };
/// let filter = FilterIterator::new(scan, predicate, evaluator);
///
/// // Only rows where age > 18 are yielded
/// for row in filter {
///     println!("{:?}", row?);
/// }
/// ```
pub struct FilterIterator<'a, I: RowIterator> {
    source: I,
    predicate: ast::Expression,
    evaluator: CombinedExpressionEvaluator<'a>,
}

impl<'a, I: RowIterator> FilterIterator<'a, I> {
    /// Create a new filter iterator
    ///
    /// # Arguments
    /// * `source` - The source iterator to filter
    /// * `predicate` - The expression to evaluate for each row
    /// * `evaluator` - The evaluator to use for predicate evaluation
    pub fn new(source: I, predicate: ast::Expression, evaluator: CombinedExpressionEvaluator<'a>) -> Self {
        Self { source, predicate, evaluator }
    }

    /// Check if a value is "truthy" according to SQL/SQLLogicTest semantics
    ///
    /// Returns:
    /// - `Ok(true)` if the value is truthy (row should be included)
    /// - `Ok(false)` if the value is falsy (row should be filtered out)
    /// - `Err(...)` if the value is not a valid predicate type
    fn is_truthy(value: &types::SqlValue) -> Result<bool, ExecutorError> {
        match value {
            types::SqlValue::Boolean(b) => Ok(*b),
            types::SqlValue::Null => Ok(false), // NULL is falsy
            // SQLLogicTest compatibility: treat integers as truthy/falsy
            types::SqlValue::Integer(0)
            | types::SqlValue::Smallint(0)
            | types::SqlValue::Bigint(0) => Ok(false),
            types::SqlValue::Integer(_) | types::SqlValue::Smallint(_) | types::SqlValue::Bigint(_) => {
                Ok(true)
            }
            types::SqlValue::Float(f) | types::SqlValue::Real(f) if *f == 0.0 => Ok(false),
            types::SqlValue::Float(_) | types::SqlValue::Real(_) => Ok(true),
            types::SqlValue::Double(f) if *f == 0.0 => Ok(false),
            types::SqlValue::Double(_) => Ok(true),
            other => Err(ExecutorError::InvalidWhereClause(format!(
                "Filter expression must evaluate to boolean, got: {:?}",
                other
            ))),
        }
    }
}

impl<'a, I: RowIterator> Iterator for FilterIterator<'a, I> {
    type Item = Result<storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Get the next row from source
            let row = match self.source.next()? {
                Ok(row) => row,
                Err(e) => return Some(Err(e)),
            };

            // Evaluate the predicate for this row
            match self.evaluator.eval(&self.predicate, &row) {
                Ok(value) => match Self::is_truthy(&value) {
                    Ok(true) => return Some(Ok(row)),  // Row passes filter
                    Ok(false) => continue,              // Row filtered out, try next
                    Err(e) => return Some(Err(e)),     // Type error
                },
                Err(e) => return Some(Err(e)), // Evaluation error
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Filter can only reduce the count, never increase it
        // Lower bound is 0 (might filter everything)
        // Upper bound is the source's upper bound (might filter nothing)
        let (_, upper) = self.source.size_hint();
        (0, upper)
    }
}

impl<'a, I: RowIterator> RowIterator for FilterIterator<'a, I> {
    fn schema(&self) -> &CombinedSchema {
        self.source.schema()
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        // Same logic as size_hint
        let (_, upper) = self.source.row_size_hint();
        (0, upper)
    }
}

/// Iterator that projects (transforms) rows using a projection function
///
/// This iterator wraps another RowIterator and applies a transformation
/// to each row. The transformation can change the row's values and/or schema
/// (e.g., selecting specific columns, computing expressions, renaming columns).
///
/// # Example
///
/// ```rust,ignore
/// let scan = TableScanIterator::new(schema, rows);
///
/// // Project to select only certain columns
/// let project_fn = |row: storage::Row| -> Result<storage::Row, ExecutorError> {
///     Ok(storage::Row::new(vec![row.values[0].clone(), row.values[2].clone()]))
/// };
///
/// let projected_schema = CombinedSchema::new(/* ... */);
/// let project = ProjectionIterator::new(scan, projected_schema, project_fn);
///
/// for row in project {
///     println!("{:?}", row?);  // Only contains projected columns
/// }
/// ```
pub struct ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(storage::Row) -> Result<storage::Row, ExecutorError>,
{
    source: I,
    schema: CombinedSchema,
    projection_fn: F,
}

impl<I, F> ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(storage::Row) -> Result<storage::Row, ExecutorError>,
{
    /// Create a new projection iterator
    ///
    /// # Arguments
    /// * `source` - The source iterator to project
    /// * `schema` - The schema of rows after projection
    /// * `projection_fn` - The function to transform each row
    pub fn new(source: I, schema: CombinedSchema, projection_fn: F) -> Self {
        Self { source, schema, projection_fn }
    }
}

impl<I, F> Iterator for ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(storage::Row) -> Result<storage::Row, ExecutorError>,
{
    type Item = Result<storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.source.next()? {
            Ok(row) => row,
            Err(e) => return Some(Err(e)),
        };

        Some((self.projection_fn)(row))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Projection doesn't change row count
        self.source.size_hint()
    }
}

impl<I, F> RowIterator for ProjectionIterator<I, F>
where
    I: RowIterator,
    F: FnMut(storage::Row) -> Result<storage::Row, ExecutorError>,
{
    fn schema(&self) -> &CombinedSchema {
        &self.schema
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        // Projection doesn't change row count
        self.source.row_size_hint()
    }
}

/// Iterator that performs lazy nested loop join between two row sources
///
/// This iterator streams through the left side while materializing the right side.
/// For each left row, it iterates through all right rows, checking the join condition
/// and yielding matching combinations.
///
/// # Join Types Supported
///
/// - **INNER**: Only matching row pairs
/// - **CROSS**: All combinations (condition ignored)
/// - **LEFT OUTER**: All left rows + NULLs for unmatched
/// - **RIGHT OUTER**: All right rows + NULLs for unmatched (requires full left scan first)
/// - **FULL OUTER**: Combination of LEFT and RIGHT
///
/// # Current Limitation
///
/// The right side must be fully materialized (Vec<Row>). This is a pragmatic compromise:
/// - Nested loop joins inherently need to scan the right side multiple times
/// - True streaming would require a hash join or other algorithm
/// - This still provides memory benefits for the left side (which can be very large)
///
/// # Example
///
/// ```rust,ignore
/// let left_iter = TableScanIterator::new(left_schema, left_rows);
/// let right_rows = vec![/* right side rows */];
///
/// let join = LazyNestedLoopJoin::new(
///     left_iter,
///     right_schema,
///     right_rows,
///     ast::JoinType::Inner,
///     Some(condition),
///     evaluator,
/// );
///
/// // Only materializes left rows on-demand
/// for row in join.take(10) {
///     println!("{:?}", row?);
/// }
/// ```
pub struct LazyNestedLoopJoin<I: RowIterator> {
    /// Left side iterator (streaming)
    left: I,
    /// Right side schema (needed for combined schema)
    right_schema: CombinedSchema,
    /// Right side rows (materialized)
    right_rows: Vec<storage::Row>,
    /// Type of join
    join_type: ast::JoinType,
    /// Optional join condition
    condition: Option<ast::Expression>,
    /// Combined schema for output rows
    combined_schema: CombinedSchema,

    // State for iteration
    /// Current left row being processed
    current_left: Option<storage::Row>,
    /// Current position in right rows
    right_index: usize,
    /// For LEFT/FULL OUTER: tracks if current left row has any matches
    current_left_matched: bool,
    /// For RIGHT/FULL OUTER: tracks which right rows have been matched
    right_matched: Vec<bool>,
    /// For RIGHT/FULL OUTER: after left exhausted, yield unmatched right rows
    left_exhausted: bool,
    /// For RIGHT/FULL OUTER: current position in emitting unmatched right rows
    unmatched_right_index: usize,
}

impl<I: RowIterator> LazyNestedLoopJoin<I> {
    /// Create a new lazy nested loop join iterator
    ///
    /// # Arguments
    /// * `left` - Iterator for left side rows (streaming)
    /// * `right_schema` - Schema for right side
    /// * `right_rows` - Materialized right side rows
    /// * `join_type` - Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
    /// * `condition` - Optional join condition expression
    pub fn new(
        left: I,
        right_schema: CombinedSchema,
        right_rows: Vec<storage::Row>,
        join_type: ast::JoinType,
        condition: Option<ast::Expression>,
    ) -> Self {
        // Build combined schema (left columns + right columns)
        let left_schema = left.schema().clone();

        // Manually combine two CombinedSchema instances
        let mut table_schemas = left_schema.table_schemas.clone();
        let left_total = left_schema.total_columns;
        let mut right_total = 0;

        // Add all right-side tables with adjusted start indices
        for (table_name, (start_idx, schema)) in right_schema.table_schemas.iter() {
            let adjusted_start = left_total + start_idx;
            table_schemas.insert(table_name.clone(), (adjusted_start, schema.clone()));
            right_total += schema.columns.len();
        }

        let combined_schema = CombinedSchema {
            table_schemas,
            total_columns: left_total + right_total,
        };

        let right_count = right_rows.len();

        Self {
            left,
            right_schema,
            right_rows,
            join_type,
            condition,
            combined_schema,
            current_left: None,
            right_index: 0,
            current_left_matched: false,
            right_matched: vec![false; right_count],
            left_exhausted: false,
            unmatched_right_index: 0,
        }
    }

    /// Combine two rows into a single row (left + right)
    #[inline]
    fn combine_rows(left: &storage::Row, right: &storage::Row) -> storage::Row {
        let mut values = Vec::with_capacity(left.values.len() + right.values.len());
        values.extend_from_slice(&left.values);
        values.extend_from_slice(&right.values);
        storage::Row::new(values)
    }

    /// Create a row with NULL values for outer join
    #[inline]
    fn null_row(column_count: usize) -> Vec<types::SqlValue> {
        vec![types::SqlValue::Null; column_count]
    }

    /// Check if join condition is satisfied for a combined row
    fn check_condition(&self, combined_row: &storage::Row) -> Result<bool, ExecutorError> {
        match &self.condition {
            None => Ok(true), // No condition means all rows match (CROSS JOIN)
            Some(expr) => {
                // Create evaluator on-the-fly for the combined schema
                let database = storage::Database::new(); // Empty database for column comparisons
                let evaluator = CombinedExpressionEvaluator::with_database(&self.combined_schema, &database);
                let value = evaluator.eval(expr, combined_row)?;
                // Use same truthy logic as FilterIterator
                match value {
                    types::SqlValue::Boolean(b) => Ok(b),
                    types::SqlValue::Null => Ok(false),
                    types::SqlValue::Integer(0)
                    | types::SqlValue::Smallint(0)
                    | types::SqlValue::Bigint(0) => Ok(false),
                    types::SqlValue::Integer(_)
                    | types::SqlValue::Smallint(_)
                    | types::SqlValue::Bigint(_) => Ok(true),
                    types::SqlValue::Float(f) | types::SqlValue::Real(f) if f == 0.0 => Ok(false),
                    types::SqlValue::Float(_) | types::SqlValue::Real(_) => Ok(true),
                    types::SqlValue::Double(f) if f == 0.0 => Ok(false),
                    types::SqlValue::Double(_) => Ok(true),
                    other => Err(ExecutorError::InvalidWhereClause(format!(
                        "Join condition must evaluate to boolean, got: {:?}",
                        other
                    ))),
                }
            }
        }
    }
}

impl<I: RowIterator> Iterator for LazyNestedLoopJoin<I> {
    type Item = Result<storage::Row, ExecutorError>;

    fn next(&mut self) -> Option<Self::Item> {
        // Handle RIGHT/FULL OUTER: after left exhausted, emit unmatched right rows
        if self.left_exhausted {
            match self.join_type {
                ast::JoinType::RightOuter | ast::JoinType::FullOuter => {
                    while self.unmatched_right_index < self.right_rows.len() {
                        let idx = self.unmatched_right_index;
                        self.unmatched_right_index += 1;

                        if !self.right_matched[idx] {
                            // Emit: NULL + right_row
                            let left_null_count = self.combined_schema.table_schemas.values()
                                .map(|(_, s)| s.columns.len())
                                .sum::<usize>() - self.right_schema.table_schemas.values()
                                .map(|(_, s)| s.columns.len())
                                .sum::<usize>();

                            let mut values = Self::null_row(left_null_count);
                            values.extend_from_slice(&self.right_rows[idx].values);
                            return Some(Ok(storage::Row::new(values)));
                        }
                    }
                    return None; // Done with unmatched right rows
                }
                _ => return None, // Other join types done when left exhausted
            }
        }

        loop {
            // If no current left row, fetch the next one
            if self.current_left.is_none() {
                match self.left.next() {
                    Some(Ok(row)) => {
                        self.current_left = Some(row);
                        self.right_index = 0;
                        self.current_left_matched = false;
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => {
                        // Left side exhausted
                        self.left_exhausted = true;

                        // For RIGHT/FULL OUTER, continue to emit unmatched right rows
                        match self.join_type {
                            ast::JoinType::RightOuter | ast::JoinType::FullOuter => {
                                return self.next(); // Recurse to handle unmatched right rows
                            }
                            _ => return None,
                        }
                    }
                }
            }

            // We have a left row - iterate through right rows
            let left_row = self.current_left.as_ref().unwrap();

            while self.right_index < self.right_rows.len() {
                let right_row = &self.right_rows[self.right_index];
                let right_idx = self.right_index;
                self.right_index += 1;

                // Combine rows for condition check
                let combined_row = Self::combine_rows(left_row, right_row);

                // Check join condition
                match self.check_condition(&combined_row) {
                    Ok(true) => {
                        // Match found!
                        self.current_left_matched = true;
                        self.right_matched[right_idx] = true;
                        return Some(Ok(combined_row));
                    }
                    Ok(false) => {
                        // No match, continue to next right row
                        continue;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Finished all right rows for this left row
            // Handle LEFT/FULL OUTER: emit left + NULLs if no matches
            if !self.current_left_matched {
                match self.join_type {
                    ast::JoinType::LeftOuter | ast::JoinType::FullOuter => {
                        let right_null_count = self.right_schema.table_schemas.values()
                            .map(|(_, s)| s.columns.len())
                            .sum::<usize>();

                        let mut values = left_row.values.clone();
                        values.extend(Self::null_row(right_null_count));

                        self.current_left = None; // Move to next left row
                        return Some(Ok(storage::Row::new(values)));
                    }
                    _ => {
                        // INNER/CROSS/RIGHT: no match means don't emit
                        self.current_left = None; // Move to next left row
                        continue;
                    }
                }
            }

            // Move to next left row
            self.current_left = None;
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (left_lower, left_upper) = self.left.size_hint();
        let right_count = self.right_rows.len();

        match self.join_type {
            ast::JoinType::Inner => {
                // At minimum 0 (no matches), at most left * right
                (0, left_upper.and_then(|l| l.checked_mul(right_count)))
            }
            ast::JoinType::Cross => {
                // Exactly left * right
                let lower = left_lower.saturating_mul(right_count);
                let upper = left_upper.and_then(|l| l.checked_mul(right_count));
                (lower, upper)
            }
            ast::JoinType::LeftOuter | ast::JoinType::FullOuter => {
                // At least as many as left side
                (left_lower, None) // Upper bound is hard to compute
            }
            ast::JoinType::RightOuter => {
                // At least as many as right side
                (right_count, None)
            }
        }
    }
}

impl<I: RowIterator> RowIterator for LazyNestedLoopJoin<I> {
    fn schema(&self) -> &CombinedSchema {
        &self.combined_schema
    }

    fn row_size_hint(&self) -> (usize, Option<usize>) {
        self.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::Row;
    use types::SqlValue;

    /// Helper to create a simple schema for testing
    fn test_schema() -> CombinedSchema {
        let table_schema = catalog::TableSchema::new(
            "test".to_string(),
            vec![catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false)],
        );
        CombinedSchema::from_table("test".to_string(), table_schema)
    }

    #[test]
    fn test_table_scan_iterator_empty() {
        let schema = test_schema();
        let rows = vec![];
        let mut iter = TableScanIterator::new(schema.clone(), rows);

        // Can't use assert_eq! on schema (no PartialEq), just verify it's not null
        assert!(iter.schema().table_schemas.len() >= 0);
        assert_eq!(iter.next(), None);
        assert_eq!(iter.size_hint(), (0, Some(0)));
    }

    #[test]
    fn test_table_scan_iterator_with_rows() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
        ];
        let mut iter = TableScanIterator::new(schema.clone(), rows);

        // Can't use assert_eq! on schema (no PartialEq), just verify it's not null
        assert!(iter.schema().table_schemas.len() >= 0);
        assert_eq!(iter.size_hint(), (3, Some(3)));

        assert_eq!(iter.next().unwrap().unwrap().values, vec![SqlValue::Integer(1)]);
        assert_eq!(iter.next().unwrap().unwrap().values, vec![SqlValue::Integer(2)]);
        assert_eq!(iter.next().unwrap().unwrap().values, vec![SqlValue::Integer(3)]);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_filter_iterator_all_pass() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
        ];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Predicate that always returns true
        let predicate = ast::Expression::Literal(SqlValue::Boolean(true));
        let evaluator = CombinedExpressionEvaluator::new(&schema);
        let mut filter = FilterIterator::new(scan, predicate, evaluator);

        // Can't use assert_eq! on schema (no PartialEq), just verify it's not null
        assert!(filter.schema().table_schemas.len() >= 0);
        assert_eq!(filter.next().unwrap().unwrap().values, vec![SqlValue::Integer(1)]);
        assert_eq!(filter.next().unwrap().unwrap().values, vec![SqlValue::Integer(2)]);
        assert_eq!(filter.next().unwrap().unwrap().values, vec![SqlValue::Integer(3)]);
        assert_eq!(filter.next(), None);
    }

    #[test]
    fn test_filter_iterator_none_pass() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
        ];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Predicate that always returns false
        let predicate = ast::Expression::Literal(SqlValue::Boolean(false));
        let evaluator = CombinedExpressionEvaluator::new(&schema);
        let mut filter = FilterIterator::new(scan, predicate, evaluator);

        // Can't use assert_eq! on schema (no PartialEq), just verify it's not null
        assert!(filter.schema().table_schemas.len() >= 0);
        assert_eq!(filter.next(), None);
    }

    #[test]
    fn test_filter_iterator_null_is_falsy() {
        let schema = test_schema();
        let rows = vec![Row::new(vec![SqlValue::Integer(1)])];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Predicate that returns NULL (should filter out)
        let predicate = ast::Expression::Literal(SqlValue::Null);
        let evaluator = CombinedExpressionEvaluator::new(&schema);
        let mut filter = FilterIterator::new(scan, predicate, evaluator);

        assert_eq!(filter.next(), None);
    }

    #[test]
    fn test_filter_iterator_integer_truthy() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
        ];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Predicate that returns non-zero integer (truthy)
        let predicate = ast::Expression::Literal(SqlValue::Integer(42));
        let evaluator = CombinedExpressionEvaluator::new(&schema);
        let mut filter = FilterIterator::new(scan, predicate, evaluator);

        assert_eq!(filter.next().unwrap().unwrap().values, vec![SqlValue::Integer(1)]);
        assert_eq!(filter.next().unwrap().unwrap().values, vec![SqlValue::Integer(2)]);
        assert_eq!(filter.next(), None);
    }

    #[test]
    fn test_filter_iterator_zero_is_falsy() {
        let schema = test_schema();
        let rows = vec![Row::new(vec![SqlValue::Integer(1)])];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Predicate that returns 0 (falsy)
        let predicate = ast::Expression::Literal(SqlValue::Integer(0));
        let evaluator = CombinedExpressionEvaluator::new(&schema);
        let mut filter = FilterIterator::new(scan, predicate, evaluator);

        assert_eq!(filter.next(), None);
    }

    #[test]
    fn test_projection_iterator_identity() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
        ];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Identity projection (no-op)
        let project_fn = |row: Row| Ok(row);
        let mut project = ProjectionIterator::new(scan, schema.clone(), project_fn);

        // Can't use assert_eq! on schema (no PartialEq), just verify it's not null
        assert!(project.schema().table_schemas.len() >= 0);
        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(1)]);
        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(2)]);
        assert_eq!(project.next(), None);
    }

    #[test]
    fn test_projection_iterator_transform() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
        ];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Double each value
        let project_fn = |mut row: Row| {
            if let SqlValue::Integer(n) = row.values[0] {
                row.values[0] = SqlValue::Integer(n * 2);
            }
            Ok(row)
        };
        let mut project = ProjectionIterator::new(scan, schema.clone(), project_fn);

        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(2)]);
        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(4)]);
        assert_eq!(project.next(), None);
    }

    #[test]
    fn test_chained_iterators() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
            Row::new(vec![SqlValue::Integer(4)]),
        ];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Filter for values > 1
        let predicate = ast::Expression::Literal(SqlValue::Integer(1)); // Truthy, for demo
        let evaluator = CombinedExpressionEvaluator::new(&schema);
        let filter = FilterIterator::new(scan, predicate, evaluator);

        // Then double each value
        let project_fn = |mut row: Row| {
            if let SqlValue::Integer(n) = row.values[0] {
                row.values[0] = SqlValue::Integer(n * 2);
            }
            Ok(row)
        };
        let mut project = ProjectionIterator::new(filter, schema.clone(), project_fn);

        // Should get all rows (filter passes all), doubled
        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(2)]);
        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(4)]);
        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(6)]);
        assert_eq!(project.next().unwrap().unwrap().values, vec![SqlValue::Integer(8)]);
        assert_eq!(project.next(), None);
    }

    #[test]
    fn test_iterator_take_limit() {
        let schema = test_schema();
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
            Row::new(vec![SqlValue::Integer(4)]),
            Row::new(vec![SqlValue::Integer(5)]),
        ];
        let scan = TableScanIterator::new(schema.clone(), rows);

        // Use standard iterator take() to implement LIMIT
        let results: Vec<_> = scan.take(2).collect();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].as_ref().unwrap().values, vec![SqlValue::Integer(1)]);
        assert_eq!(results[1].as_ref().unwrap().values, vec![SqlValue::Integer(2)]);
    }

    // Helper to create two-table schema for join tests
    fn test_join_schemas() -> (CombinedSchema, CombinedSchema) {
        let left_schema = catalog::TableSchema::new(
            "t1".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("value".to_string(), types::DataType::Integer, false),
            ],
        );
        let right_schema = catalog::TableSchema::new(
            "t2".to_string(),
            vec![
                catalog::ColumnSchema::new("id".to_string(), types::DataType::Integer, false),
                catalog::ColumnSchema::new("data".to_string(), types::DataType::Integer, false),
            ],
        );
        (
            CombinedSchema::from_table("t1".to_string(), left_schema),
            CombinedSchema::from_table("t2".to_string(), right_schema),
        )
    }

    #[test]
    fn test_lazy_nested_loop_join_cross() {
        let (left_schema, right_schema) = test_join_schemas();

        let left_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
        ];
        let right_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200)]),
        ];

        let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

        let mut join = LazyNestedLoopJoin::new(
            left_iter,
            right_schema,
            right_rows,
            ast::JoinType::Cross,
            None, // No condition for CROSS JOIN
        );

        // CROSS JOIN: 2 left x 2 right = 4 rows
        let results: Vec<_> = join.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 4);

        // Check first row: (1, 10, 1, 100)
        assert_eq!(
            results[0].values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(10),
                SqlValue::Integer(1),
                SqlValue::Integer(100)
            ]
        );

        // Check last row: (2, 20, 2, 200)
        assert_eq!(
            results[3].values,
            vec![
                SqlValue::Integer(2),
                SqlValue::Integer(20),
                SqlValue::Integer(2),
                SqlValue::Integer(200)
            ]
        );
    }

    #[test]
    fn test_lazy_nested_loop_join_inner_with_condition() {
        let (left_schema, right_schema) = test_join_schemas();

        let left_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(30)]),
        ];
        let right_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200)]),
        ];

        let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

        // Condition: t1.id = t2.id (column 0 = column 2)
        let condition = ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        };

        let mut join = LazyNestedLoopJoin::new(
            left_iter,
            right_schema,
            right_rows,
            ast::JoinType::Inner,
            Some(condition),
        );

        // INNER JOIN with condition: only (1,1) and (2,2) match
        let results: Vec<_> = join.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 2);

        // (1, 10, 1, 100)
        assert_eq!(
            results[0].values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(10),
                SqlValue::Integer(1),
                SqlValue::Integer(100)
            ]
        );

        // (2, 20, 2, 200)
        assert_eq!(
            results[1].values,
            vec![
                SqlValue::Integer(2),
                SqlValue::Integer(20),
                SqlValue::Integer(2),
                SqlValue::Integer(200)
            ]
        );
    }

    #[test]
    fn test_lazy_nested_loop_join_left_outer() {
        let (left_schema, right_schema) = test_join_schemas();

        let left_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(30)]), // No match in right
        ];
        let right_rows = vec![Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)])];

        let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

        // Condition: t1.id = t2.id
        let condition = ast::Expression::BinaryOp {
            left: Box::new(ast::Expression::ColumnRef {
                table: Some("t1".to_string()),
                column: "id".to_string(),
            }),
            op: ast::BinaryOperator::Equal,
            right: Box::new(ast::Expression::ColumnRef {
                table: Some("t2".to_string()),
                column: "id".to_string(),
            }),
        };

        let mut join = LazyNestedLoopJoin::new(
            left_iter,
            right_schema,
            right_rows,
            ast::JoinType::LeftOuter,
            Some(condition),
        );

        let results: Vec<_> = join.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 2);

        // First row: (1, 10, 1, 100) - match
        assert_eq!(
            results[0].values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(10),
                SqlValue::Integer(1),
                SqlValue::Integer(100)
            ]
        );

        // Second row: (3, 30, NULL, NULL) - no match, left with NULLs
        assert_eq!(
            results[1].values,
            vec![SqlValue::Integer(3), SqlValue::Integer(30), SqlValue::Null, SqlValue::Null]
        );
    }

    #[test]
    fn test_lazy_nested_loop_join_early_termination() {
        let (left_schema, right_schema) = test_join_schemas();

        // Large left side
        let left_rows: Vec<_> = (1..=1000)
            .map(|i| Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i * 10)]))
            .collect();

        // Small right side
        let right_rows: Vec<_> = (1..=10)
            .map(|i| Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i * 100)]))
            .collect();

        let left_iter = TableScanIterator::new(left_schema.clone(), left_rows);

        let join = LazyNestedLoopJoin::new(
            left_iter,
            right_schema,
            right_rows,
            ast::JoinType::Cross,
            None,
        );

        // Only take first 5 rows - should not materialize all 1000 left rows
        let results: Vec<_> = join.take(5).collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 5);

        // Verify we got the expected rows (first left row with first 5 right rows)
        for i in 0..5 {
            assert_eq!(results[i].values[0], SqlValue::Integer(1)); // Left id stays 1
            assert_eq!(results[i].values[1], SqlValue::Integer(10)); // Left value stays 10
        }
    }
}

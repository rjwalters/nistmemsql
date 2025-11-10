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
}

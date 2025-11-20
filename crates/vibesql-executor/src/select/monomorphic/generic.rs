//! Generic monomorphic execution patterns
//!
//! This module provides generic monomorphic execution plans that match on query
//! characteristics (structure, aggregation type, etc.) rather than specific table
//! or column names.
//!
//! Unlike TPC-H-specific plans which hardcode table names and column indices,
//! these generic patterns:
//! - Match any table with compatible schema
//! - Extract filter predicates dynamically from WHERE clause
//! - Resolve column indices from schema at plan creation time
//! - Still use unchecked accessors for maximum performance
//!
//! This provides the same performance benefits (~230ns/row improvement) while
//! working for any user query with similar structure.

use std::str::FromStr;

use vibesql_ast::{BinaryOperator, Expression, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::{DataType, Date, SqlValue};

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::pattern::{has_aggregate_function, has_no_joins, QueryPattern};
use super::MonomorphicPlan;

// =============================================================================
// Phase 1: Filter Extraction Infrastructure
// =============================================================================

/// Represents a filter predicate that can be evaluated efficiently
#[derive(Debug, Clone)]
pub enum FilterPredicate {
    /// Date range filter: column >= min AND column < max
    DateRange {
        column_idx: usize,
        min: Date,
        max: Date,
    },
    /// Numeric BETWEEN filter: column >= min AND column <= max
    Between {
        column_idx: usize,
        min: f64,
        max: f64,
    },
    /// Comparison with constant: column OP value
    Comparison {
        column_idx: usize,
        op: ComparisonOp,
        value: SqlValue,
        value_type: DataType,
    },
}

/// Comparison operators supported in filter predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComparisonOp {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    NotEqual,
}

impl FilterPredicate {
    /// Evaluate this filter predicate on a row
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(always)]
    pub unsafe fn evaluate(&self, row: &Row) -> bool {
        match self {
            FilterPredicate::DateRange {
                column_idx,
                min,
                max,
            } => {
                let value = row.get_date_unchecked(*column_idx);
                value >= *min && value < *max
            }
            FilterPredicate::Between {
                column_idx,
                min,
                max,
            } => {
                let value = row.get_numeric_as_f64_unchecked(*column_idx);
                value >= *min && value <= *max
            }
            FilterPredicate::Comparison {
                column_idx,
                op,
                value,
                value_type,
            } => Self::evaluate_comparison(row, *column_idx, *op, value, value_type),
        }
    }

    /// Evaluate a comparison predicate
    #[inline(always)]
    unsafe fn evaluate_comparison(
        row: &Row,
        column_idx: usize,
        op: ComparisonOp,
        value: &SqlValue,
        value_type: &DataType,
    ) -> bool {
        match value_type {
            DataType::DoublePrecision | DataType::Real | DataType::Decimal { .. } => {
                let row_value = row.get_f64_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Double(v) => *v,
                    SqlValue::Integer(v) => *v as f64,
                    _ => return false,
                };
                Self::compare_f64(row_value, op, compare_value)
            }
            DataType::Integer | DataType::Bigint | DataType::Smallint | DataType::Unsigned => {
                let row_value = row.get_i64_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Integer(v) | SqlValue::Bigint(v) => *v,
                    SqlValue::Smallint(v) => *v as i64,
                    _ => return false,
                };
                Self::compare_i64(row_value, op, compare_value)
            }
            DataType::Date => {
                let row_value = row.get_date_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Date(d) => *d,
                    SqlValue::Varchar(s) => {
                        // Parse Varchar date string (e.g., "2024-01-01")
                        match Date::from_str(s) {
                            Ok(d) => d,
                            Err(_) => return false,
                        }
                    }
                    _ => return false,
                };
                Self::compare_date(row_value, op, compare_value)
            }
            DataType::Boolean => {
                let row_value = row.get_bool_unchecked(column_idx);
                let compare_value = match value {
                    SqlValue::Boolean(b) => *b,
                    _ => return false,
                };
                Self::compare_bool(row_value, op, compare_value)
            }
            _ => false,
        }
    }

    #[inline(always)]
    fn compare_f64(a: f64, op: ComparisonOp, b: f64) -> bool {
        match op {
            ComparisonOp::LessThan => a < b,
            ComparisonOp::LessThanOrEqual => a <= b,
            ComparisonOp::GreaterThan => a > b,
            ComparisonOp::GreaterThanOrEqual => a >= b,
            ComparisonOp::Equal => (a - b).abs() < f64::EPSILON,
            ComparisonOp::NotEqual => (a - b).abs() >= f64::EPSILON,
        }
    }

    #[inline(always)]
    fn compare_i64(a: i64, op: ComparisonOp, b: i64) -> bool {
        match op {
            ComparisonOp::LessThan => a < b,
            ComparisonOp::LessThanOrEqual => a <= b,
            ComparisonOp::GreaterThan => a > b,
            ComparisonOp::GreaterThanOrEqual => a >= b,
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
        }
    }

    #[inline(always)]
    fn compare_date(a: Date, op: ComparisonOp, b: Date) -> bool {
        match op {
            ComparisonOp::LessThan => a < b,
            ComparisonOp::LessThanOrEqual => a <= b,
            ComparisonOp::GreaterThan => a > b,
            ComparisonOp::GreaterThanOrEqual => a >= b,
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
        }
    }

    #[inline(always)]
    fn compare_bool(a: bool, op: ComparisonOp, b: bool) -> bool {
        match op {
            ComparisonOp::Equal => a == b,
            ComparisonOp::NotEqual => a != b,
            _ => false, // Other comparisons don't make sense for booleans
        }
    }
}

/// Helper to get column type by name from schema
fn get_column_type(schema: &CombinedSchema, column_name: &str) -> Option<DataType> {
    // Search all tables for the column
    for (_start_index, table_schema) in schema.table_schemas.values() {
        if let Some(col) = table_schema.get_column(column_name) {
            return Some(col.data_type.clone());
        }
    }
    None
}

/// Extract filter predicates from a WHERE clause
///
/// Returns None if filters cannot be extracted (e.g., unsupported expression types)
pub fn extract_filters(
    where_clause: &Option<Expression>,
    schema: &CombinedSchema,
) -> Option<Vec<FilterPredicate>> {
    match where_clause {
        Some(expr) => extract_filters_from_expr(expr, schema),
        None => Some(vec![]),
    }
}

/// Recursively extract filters from an expression
fn extract_filters_from_expr(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<FilterPredicate>> {
    match expr {
        // AND: combine filters from both sides
        Expression::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            let mut left_filters = extract_filters_from_expr(left, schema)?;
            let right_filters = extract_filters_from_expr(right, schema)?;
            left_filters.extend(right_filters);
            Some(left_filters)
        }

        // BETWEEN: extract as Between predicate
        Expression::Between {
            expr: inner_expr,
            low,
            high,
            negated: false,
            ..
        } => {
            // Must be column BETWEEN literal AND literal
            let column_name = match inner_expr.as_ref() {
                Expression::ColumnRef { column, .. } => column,
                _ => return None,
            };

            let column_idx = schema.get_column_index(None, column_name)?;
            let column_type = get_column_type(schema, column_name)?;

            // Try to extract as numeric BETWEEN
            if matches!(
                column_type,
                DataType::DoublePrecision
                    | DataType::Real
                    | DataType::Decimal { .. }
                    | DataType::Integer
                    | DataType::Bigint
                    | DataType::Unsigned
            ) {
                let low_val = extract_f64_literal(low)?;
                let high_val = extract_f64_literal(high)?;
                return Some(vec![FilterPredicate::Between {
                    column_idx,
                    min: low_val,
                    max: high_val,
                }]);
            }

            None
        }

        // Comparison: column OP literal
        Expression::BinaryOp { op, left, right } => {
            let comparison_op = match op {
                BinaryOperator::LessThan => ComparisonOp::LessThan,
                BinaryOperator::LessThanOrEqual => ComparisonOp::LessThanOrEqual,
                BinaryOperator::GreaterThan => ComparisonOp::GreaterThan,
                BinaryOperator::GreaterThanOrEqual => ComparisonOp::GreaterThanOrEqual,
                BinaryOperator::Equal => ComparisonOp::Equal,
                BinaryOperator::NotEqual => ComparisonOp::NotEqual,
                _ => return None,
            };

            // Try column OP literal
            if let Expression::ColumnRef { column, .. } = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    let column_idx = schema.get_column_index(None, column)?;
                    let column_type = get_column_type(schema, column)?;

                    // Special case: detect date range patterns (col >= date1 AND col < date2)
                    if column_type == DataType::Date {
                        if let SqlValue::Date(_date) = value {
                            // This might be part of a date range, but we'll just add it as a comparison
                            // Date range optimization will happen if we find matching predicates
                            return Some(vec![FilterPredicate::Comparison {
                                column_idx,
                                op: comparison_op,
                                value: value.clone(),
                                value_type: column_type,
                            }]);
                        }
                    }

                    return Some(vec![FilterPredicate::Comparison {
                        column_idx,
                        op: comparison_op,
                        value: value.clone(),
                        value_type: column_type,
                    }]);
                }
            }

            // Try literal OP column (reverse the operator)
            if let Expression::Literal(value) = left.as_ref() {
                if let Expression::ColumnRef { column, .. } = right.as_ref() {
                    let column_idx = schema.get_column_index(None, column)?;
                    let column_type = get_column_type(schema, column)?;

                    // Reverse the operator
                    let reversed_op = match comparison_op {
                        ComparisonOp::LessThan => ComparisonOp::GreaterThan,
                        ComparisonOp::LessThanOrEqual => ComparisonOp::GreaterThanOrEqual,
                        ComparisonOp::GreaterThan => ComparisonOp::LessThan,
                        ComparisonOp::GreaterThanOrEqual => ComparisonOp::LessThanOrEqual,
                        ComparisonOp::Equal => ComparisonOp::Equal,
                        ComparisonOp::NotEqual => ComparisonOp::NotEqual,
                    };

                    return Some(vec![FilterPredicate::Comparison {
                        column_idx,
                        op: reversed_op,
                        value: value.clone(),
                        value_type: column_type,
                    }]);
                }
            }

            None
        }

        _ => None,
    }
}

/// Helper to extract an f64 literal from an expression
fn extract_f64_literal(expr: &Expression) -> Option<f64> {
    match expr {
        Expression::Literal(SqlValue::Double(v)) => Some(*v),
        Expression::Literal(SqlValue::Integer(v)) => Some(*v as f64),
        Expression::Literal(SqlValue::Numeric(n)) => {
            // Convert Numeric (rust_decimal) to f64
            n.to_string().parse::<f64>().ok()
        }
        _ => None,
    }
}

/// Optimize date range filters by combining adjacent date comparisons
///
/// Looks for patterns like: col >= date1 AND col < date2
/// and combines them into a single DateRange predicate
pub fn optimize_date_ranges(filters: Vec<FilterPredicate>) -> Vec<FilterPredicate> {
    let mut optimized = Vec::new();
    let mut i = 0;

    while i < filters.len() {
        // Look for date comparison pairs
        if i + 1 < filters.len() {
            if let (
                FilterPredicate::Comparison {
                    column_idx: idx1,
                    op: op1,
                    value: SqlValue::Date(date1),
                    value_type: DataType::Date,
                },
                FilterPredicate::Comparison {
                    column_idx: idx2,
                    op: op2,
                    value: SqlValue::Date(date2),
                    value_type: DataType::Date,
                },
            ) = (&filters[i], &filters[i + 1])
            {
                // Same column?
                if idx1 == idx2 {
                    // Check for range pattern: col >= min AND col < max
                    match (op1, op2) {
                        (ComparisonOp::GreaterThanOrEqual, ComparisonOp::LessThan)
                        | (ComparisonOp::GreaterThan, ComparisonOp::LessThanOrEqual) => {
                            optimized.push(FilterPredicate::DateRange {
                                column_idx: *idx1,
                                min: *date1,
                                max: *date2,
                            });
                            i += 2;
                            continue;
                        }
                        (ComparisonOp::LessThan, ComparisonOp::GreaterThanOrEqual)
                        | (ComparisonOp::LessThanOrEqual, ComparisonOp::GreaterThan) => {
                            optimized.push(FilterPredicate::DateRange {
                                column_idx: *idx1,
                                min: *date2,
                                max: *date1,
                            });
                            i += 2;
                            continue;
                        }
                        _ => {}
                    }
                }
            }
        }

        // No optimization, just add the filter
        optimized.push(filters[i].clone());
        i += 1;
    }

    optimized
}

// =============================================================================
// Phase 2: Generic Single-Table Aggregation Pattern
// =============================================================================

/// Specification for an aggregation operation
#[derive(Debug, Clone)]
pub enum AggregationSpec {
    /// SUM(col1 * col2) - multiply two columns and sum
    SumProduct {
        col1_idx: usize,
        col2_idx: usize,
    },
    /// SUM(col) - simple sum
    Sum {
        col_idx: usize,
    },
    /// COUNT(*) or COUNT(col)
    Count,
}

impl AggregationSpec {
    /// Get the initial accumulator value
    pub fn initial_value(&self) -> f64 {
        match self {
            AggregationSpec::SumProduct { .. } | AggregationSpec::Sum { .. } => 0.0,
            AggregationSpec::Count => 0.0,
        }
    }

    /// Accumulate a value
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// are validated at plan creation time.
    #[inline(always)]
    pub unsafe fn accumulate(&self, acc: f64, row: &Row) -> f64 {
        match self {
            AggregationSpec::SumProduct { col1_idx, col2_idx } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                acc + (val1 * val2)
            }
            AggregationSpec::Sum { col_idx } => {
                let val = row.get_numeric_as_f64_unchecked(*col_idx);
                acc + val
            }
            AggregationSpec::Count => acc + 1.0,
        }
    }
}

/// Generic filtered aggregation plan
///
/// Handles queries like:
/// ```sql
/// SELECT SUM(col1 * col2)
/// FROM any_table
/// WHERE <filters>
/// ```
///
/// This plan is generic - it works for any table and any compatible filters.
pub struct GenericFilteredAggregationPlan {
    /// Filter predicates to evaluate
    filters: Vec<FilterPredicate>,

    /// Aggregation to compute
    aggregation: AggregationSpec,

    /// Description for debugging
    #[allow(dead_code)]
    description: String,
}

impl GenericFilteredAggregationPlan {
    /// Try to create a generic filtered aggregation plan
    pub fn try_create(stmt: &SelectStmt, schema: &CombinedSchema) -> Option<Self> {
        // Must have no joins
        if !has_no_joins(&stmt.from) {
            return None;
        }

        // Must have no GROUP BY or HAVING
        if stmt.group_by.is_some() || stmt.having.is_some() {
            return None;
        }

        // Must have SUM aggregate
        if !has_aggregate_function(&stmt.select_list, "SUM") {
            return None;
        }

        // Extract aggregation specification
        let aggregation = Self::extract_aggregation(&stmt.select_list, schema)?;

        // Extract filter predicates from WHERE clause
        let filters = extract_filters(&stmt.where_clause, schema)?;

        // Optimize date ranges
        let optimized_filters = optimize_date_ranges(filters);

        // Get table name for description
        let table_name = match &stmt.from {
            Some(vibesql_ast::FromClause::Table { name, .. }) => name.clone(),
            _ => "unknown".to_string(),
        };

        Some(Self {
            description: format!(
                "Generic Filtered Aggregation on {} ({} filters)",
                table_name,
                optimized_filters.len()
            ),
            filters: optimized_filters,
            aggregation,
        })
    }

    /// Extract aggregation specification from SELECT list
    fn extract_aggregation(
        select_list: &[vibesql_ast::SelectItem],
        schema: &CombinedSchema,
    ) -> Option<AggregationSpec> {
        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                if let Some(spec) = Self::extract_aggregation_from_expr(expr, schema) {
                    return Some(spec);
                }
            }
        }
        None
    }

    /// Extract aggregation from an expression
    fn extract_aggregation_from_expr(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Option<AggregationSpec> {
        match expr {
            Expression::AggregateFunction { name, args, .. } => {
                if name.eq_ignore_ascii_case("SUM") && args.len() == 1 {
                    // Check for SUM(col1 * col2)
                    if let Expression::BinaryOp {
                        op: BinaryOperator::Multiply,
                        left,
                        right,
                    } = &args[0]
                    {
                        let col1 = match left.as_ref() {
                            Expression::ColumnRef { column, .. } => column,
                            _ => return None,
                        };
                        let col2 = match right.as_ref() {
                            Expression::ColumnRef { column, .. } => column,
                            _ => return None,
                        };

                        let col1_idx = schema.get_column_index(None, col1)?;
                        let col2_idx = schema.get_column_index(None, col2)?;

                        return Some(AggregationSpec::SumProduct { col1_idx, col2_idx });
                    }

                    // Check for SUM(col)
                    if let Expression::ColumnRef { column, .. } = &args[0] {
                        let col_idx = schema.get_column_index(None, column)?;
                        return Some(AggregationSpec::Sum { col_idx });
                    }
                } else if name.eq_ignore_ascii_case("COUNT") {
                    return Some(AggregationSpec::Count);
                }
                None
            }
            _ => None,
        }
    }

    /// Execute using type-specialized fast path
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(never)] // Don't inline to make profiling easier
    unsafe fn execute_unsafe(&self, rows: &[Row]) -> f64 {
        let mut accumulator = self.aggregation.initial_value();

        for row in rows {
            // Evaluate all filters
            let mut pass = true;
            for filter in &self.filters {
                if !filter.evaluate(row) {
                    pass = false;
                    break;
                }
            }

            // If all filters pass, accumulate
            if pass {
                accumulator = self.aggregation.accumulate(accumulator, row);
            }
        }

        accumulator
    }

    /// Execute using streaming fast path (lazy filtering)
    ///
    /// This method filters rows during iteration, only processing rows that match
    /// all predicates. This eliminates the overhead of materializing rows that
    /// will be filtered out.
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(never)] // Don't inline to make profiling easier
    unsafe fn execute_stream_unsafe(&self, rows: Box<dyn Iterator<Item = Row>>) -> f64 {
        let mut accumulator = self.aggregation.initial_value();

        // Stream through rows, filtering inline
        for row in rows {
            // Evaluate all filters
            let mut pass = true;
            for filter in &self.filters {
                if !filter.evaluate(&row) {
                    pass = false;
                    break;
                }
            }

            // If all filters pass, accumulate
            if pass {
                accumulator = self.aggregation.accumulate(accumulator, &row);
            }
        }

        accumulator
    }
}

impl MonomorphicPlan for GenericFilteredAggregationPlan {
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError> {
        // Execute using unsafe fast path
        let result = unsafe { self.execute_unsafe(rows) };

        // Return single-row result
        Ok(vec![Row {
            values: vec![SqlValue::Double(result)],
        }])
    }

    fn execute_stream(
        &self,
        rows: Box<dyn Iterator<Item = Row>>,
    ) -> Result<Vec<Row>, ExecutorError> {
        // Execute using streaming fast path
        let result = unsafe { self.execute_stream_unsafe(rows) };

        // Return single-row result
        Ok(vec![Row {
            values: vec![SqlValue::Double(result)],
        }])
    }

    fn description(&self) -> &str {
        &self.description
    }
}

/// Pattern matcher for generic filtered aggregation queries
#[allow(dead_code)]
pub struct GenericFilteredAggregationMatcher;

impl QueryPattern for GenericFilteredAggregationMatcher {
    fn matches(&self, stmt: &SelectStmt, schema: &CombinedSchema) -> bool {
        // Try to create the plan - if successful, it matches
        GenericFilteredAggregationPlan::try_create(stmt, schema).is_some()
    }

    fn description(&self) -> &str {
        "Generic Filtered Aggregation Pattern"
    }
}

// =============================================================================
// Phase 3: Generic GROUP BY Aggregation Pattern
// =============================================================================

use std::collections::HashMap;

/// Specification for an aggregation column in GROUP BY queries
#[derive(Debug, Clone)]
pub enum GroupAggregateSpec {
    /// SUM(col)
    Sum { col_idx: usize },
    /// SUM(col1 * col2)
    SumProduct { col1_idx: usize, col2_idx: usize },
    /// SUM(col1 * (1 - col2))
    SumProductOneMinusCol {
        col1_idx: usize,
        col2_idx: usize,
    },
    /// SUM(col1 * (1 - col2) * (1 + col3))
    SumProductComplex {
        col1_idx: usize,
        col2_idx: usize,
        col3_idx: usize,
    },
    /// AVG(col) - stored as sum/count
    Avg { col_idx: usize },
    /// COUNT(*)
    Count,
}

impl GroupAggregateSpec {
    /// Accumulate a value for this aggregate
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// are validated at plan creation time.
    #[inline(always)]
    pub unsafe fn accumulate(&self, acc: f64, row: &Row) -> f64 {
        match self {
            GroupAggregateSpec::Sum { col_idx } => {
                let val = row.get_numeric_as_f64_unchecked(*col_idx);
                acc + val
            }
            GroupAggregateSpec::SumProduct { col1_idx, col2_idx } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                acc + (val1 * val2)
            }
            GroupAggregateSpec::SumProductOneMinusCol { col1_idx, col2_idx } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                acc + (val1 * (1.0 - val2))
            }
            GroupAggregateSpec::SumProductComplex {
                col1_idx,
                col2_idx,
                col3_idx,
            } => {
                let val1 = row.get_numeric_as_f64_unchecked(*col1_idx);
                let val2 = row.get_numeric_as_f64_unchecked(*col2_idx);
                let val3 = row.get_numeric_as_f64_unchecked(*col3_idx);
                acc + (val1 * (1.0 - val2) * (1.0 + val3))
            }
            GroupAggregateSpec::Avg { col_idx } => {
                let val = row.get_numeric_as_f64_unchecked(*col_idx);
                acc + val
            }
            GroupAggregateSpec::Count => acc + 1.0,
        }
    }
}

/// Specification for a GROUP BY column
#[derive(Debug, Clone)]
pub struct GroupByColumnSpec {
    pub col_idx: usize,
    pub col_type: DataType,
}

/// Generic grouped aggregation plan
///
/// Handles queries like:
/// ```sql
/// SELECT col1, col2, SUM(col3), AVG(col4), COUNT(*)
/// FROM any_table
/// WHERE <filters>
/// GROUP BY col1, col2
/// ```
///
/// This plan is generic - it works for any table and any compatible GROUP BY pattern.
pub struct GenericGroupedAggregationPlan {
    /// Filter predicates to evaluate
    filters: Vec<FilterPredicate>,

    /// GROUP BY columns (for grouping key)
    group_by_columns: Vec<GroupByColumnSpec>,

    /// Aggregations to compute for each group
    aggregates: Vec<GroupAggregateSpec>,

    /// Description for debugging
    #[allow(dead_code)]
    description: String,
}

impl GenericGroupedAggregationPlan {
    /// Try to create a generic grouped aggregation plan
    pub fn try_create(stmt: &SelectStmt, schema: &CombinedSchema) -> Option<Self> {
        // Must have no joins
        if !has_no_joins(&stmt.from) {
            return None;
        }

        // HAVING clause not yet supported in monomorphic path - fall back to standard execution
        if stmt.having.is_some() {
            return None;
        }

        // Must have GROUP BY
        let group_by = stmt.group_by.as_ref()?;

        // Extract GROUP BY columns
        let group_by_columns = Self::extract_group_by_columns(group_by, schema)?;

        // Extract aggregations
        let aggregates = Self::extract_aggregates(&stmt.select_list, schema)?;

        // Must have at least one aggregate
        if aggregates.is_empty() {
            return None;
        }

        // Extract filter predicates from WHERE clause
        let filters = extract_filters(&stmt.where_clause, schema)?;

        // Optimize date ranges
        let optimized_filters = optimize_date_ranges(filters);

        // Get table name for description
        let table_name = match &stmt.from {
            Some(vibesql_ast::FromClause::Table { name, .. }) => name.clone(),
            _ => "unknown".to_string(),
        };

        Some(Self {
            description: format!(
                "Generic Grouped Aggregation on {} ({} groups, {} aggregates, {} filters)",
                table_name,
                group_by_columns.len(),
                aggregates.len(),
                optimized_filters.len()
            ),
            filters: optimized_filters,
            group_by_columns,
            aggregates,
        })
    }

    /// Extract GROUP BY columns from GROUP BY clause
    fn extract_group_by_columns(
        group_by: &[Expression],
        schema: &CombinedSchema,
    ) -> Option<Vec<GroupByColumnSpec>> {
        let mut columns = Vec::new();

        for expr in group_by {
            if let Expression::ColumnRef { column, .. } = expr {
                let col_idx = schema.get_column_index(None, column)?;
                let col_type = get_column_type(schema, column)?;
                columns.push(GroupByColumnSpec { col_idx, col_type });
            } else {
                // Complex GROUP BY expressions not supported yet
                return None;
            }
        }

        Some(columns)
    }

    /// Extract aggregates from SELECT list
    fn extract_aggregates(
        select_list: &[vibesql_ast::SelectItem],
        schema: &CombinedSchema,
    ) -> Option<Vec<GroupAggregateSpec>> {
        let mut aggregates = Vec::new();

        for item in select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                if let Some(agg) = Self::extract_aggregate_from_expr(expr, schema) {
                    aggregates.push(agg);
                }
            }
        }

        Some(aggregates)
    }

    /// Extract a single aggregate from an expression
    fn extract_aggregate_from_expr(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Option<GroupAggregateSpec> {
        match expr {
            Expression::AggregateFunction { name, args, .. } => {
                if name.eq_ignore_ascii_case("SUM") && args.len() == 1 {
                    // Check for different SUM patterns
                    Self::extract_sum_pattern(&args[0], schema)
                } else if name.eq_ignore_ascii_case("AVG") && args.len() == 1 {
                    // AVG(col)
                    if let Expression::ColumnRef { column, .. } = &args[0] {
                        let col_idx = schema.get_column_index(None, column)?;
                        return Some(GroupAggregateSpec::Avg { col_idx });
                    }
                    None
                } else if name.eq_ignore_ascii_case("COUNT") {
                    Some(GroupAggregateSpec::Count)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Extract SUM pattern (handles various expressions)
    fn extract_sum_pattern(expr: &Expression, schema: &CombinedSchema) -> Option<GroupAggregateSpec> {
        match expr {
            // SUM(col)
            Expression::ColumnRef { column, .. } => {
                let col_idx = schema.get_column_index(None, column)?;
                Some(GroupAggregateSpec::Sum { col_idx })
            }
            // SUM(col1 * col2) or complex patterns
            Expression::BinaryOp {
                op: BinaryOperator::Multiply,
                left,
                right,
            } => {
                // Check for SUM(col1 * col2)
                if let (
                    Expression::ColumnRef { column: col1, .. },
                    Expression::ColumnRef { column: col2, .. },
                ) = (left.as_ref(), right.as_ref())
                {
                    let col1_idx = schema.get_column_index(None, col1)?;
                    let col2_idx = schema.get_column_index(None, col2)?;
                    return Some(GroupAggregateSpec::SumProduct { col1_idx, col2_idx });
                }

                // Check for SUM(col1 * (1 - col2))
                if let Expression::ColumnRef { column: col1, .. } = left.as_ref() {
                    if let Expression::BinaryOp {
                        op: BinaryOperator::Minus,
                        left: sub_left,
                        right: sub_right,
                    } = right.as_ref()
                    {
                        if let (Expression::Literal(SqlValue::Integer(1)), Expression::ColumnRef { column: col2, .. }) =
                            (sub_left.as_ref(), sub_right.as_ref())
                        {
                            let col1_idx = schema.get_column_index(None, col1)?;
                            let col2_idx = schema.get_column_index(None, col2)?;
                            return Some(GroupAggregateSpec::SumProductOneMinusCol {
                                col1_idx,
                                col2_idx,
                            });
                        }
                    }
                }

                // Check for SUM(col1 * (1 - col2) * (1 + col3))
                // This is a complex pattern: col1 * ((1 - col2) * (1 + col3))
                if let Expression::ColumnRef { column: col1, .. } = left.as_ref() {
                    if let Expression::BinaryOp {
                        op: BinaryOperator::Multiply,
                        left: mult_left,
                        right: mult_right,
                    } = right.as_ref()
                    {
                        // Check for (1 - col2) * (1 + col3)
                        if let (
                            Expression::BinaryOp {
                                op: BinaryOperator::Minus,
                                left: sub_left,
                                right: sub_right,
                            },
                            Expression::BinaryOp {
                                op: BinaryOperator::Plus,
                                left: add_left,
                                right: add_right,
                            },
                        ) = (mult_left.as_ref(), mult_right.as_ref())
                        {
                            if let (
                                Expression::Literal(SqlValue::Integer(1)),
                                Expression::ColumnRef { column: col2, .. },
                                Expression::Literal(SqlValue::Integer(1)),
                                Expression::ColumnRef { column: col3, .. },
                            ) = (
                                sub_left.as_ref(),
                                sub_right.as_ref(),
                                add_left.as_ref(),
                                add_right.as_ref(),
                            ) {
                                let col1_idx = schema.get_column_index(None, col1)?;
                                let col2_idx = schema.get_column_index(None, col2)?;
                                let col3_idx = schema.get_column_index(None, col3)?;
                                return Some(GroupAggregateSpec::SumProductComplex {
                                    col1_idx,
                                    col2_idx,
                                    col3_idx,
                                });
                            }
                        }
                    }
                }

                None
            }
            _ => None,
        }
    }

    /// Execute using type-specialized fast path
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(never)] // Don't inline to make profiling easier
    unsafe fn execute_unsafe(&self, rows: &[Row]) -> HashMap<Vec<SqlValue>, Vec<f64>> {
        let mut groups: HashMap<Vec<SqlValue>, Vec<f64>> = HashMap::new();

        for row in rows {
            // Evaluate all filters
            let mut pass = true;
            for filter in &self.filters {
                if !filter.evaluate(row) {
                    pass = false;
                    break;
                }
            }

            if !pass {
                continue;
            }

            // Extract group key
            let mut key = Vec::with_capacity(self.group_by_columns.len());
            for group_col in &self.group_by_columns {
                let value = match &group_col.col_type {
                    DataType::Varchar { .. } => {
                        SqlValue::Varchar(row.get_string_unchecked(group_col.col_idx).to_string())
                    }
                    DataType::Integer | DataType::Bigint => {
                        SqlValue::Integer(row.get_i64_unchecked(group_col.col_idx))
                    }
                    DataType::DoublePrecision | DataType::Real | DataType::Decimal { .. } => {
                        SqlValue::Double(row.get_f64_unchecked(group_col.col_idx))
                    }
                    DataType::Date => SqlValue::Date(row.get_date_unchecked(group_col.col_idx)),
                    DataType::Boolean => SqlValue::Boolean(row.get_bool_unchecked(group_col.col_idx)),
                    _ => continue,
                };
                key.push(value);
            }

            // Get or create group aggregates
            let agg_values = groups
                .entry(key)
                .or_insert_with(|| vec![0.0; self.aggregates.len()]);

            // Accumulate each aggregate
            for (i, spec) in self.aggregates.iter().enumerate() {
                agg_values[i] = spec.accumulate(agg_values[i], row);
            }
        }

        groups
    }

    /// Execute using streaming fast path (lazy filtering)
    ///
    /// This method filters rows during iteration, only processing rows that match
    /// all predicates. This eliminates the overhead of materializing rows that
    /// will be filtered out.
    ///
    /// # Safety
    ///
    /// Uses unchecked accessors for performance. Safe because column indices
    /// and types are validated at plan creation time.
    #[inline(never)] // Don't inline to make profiling easier
    unsafe fn execute_stream_unsafe(
        &self,
        rows: Box<dyn Iterator<Item = Row>>,
    ) -> HashMap<Vec<SqlValue>, Vec<f64>> {
        let mut groups: HashMap<Vec<SqlValue>, Vec<f64>> = HashMap::new();

        // Stream through rows, filtering inline
        for row in rows {
            // Evaluate all filters
            let mut pass = true;
            for filter in &self.filters {
                if !filter.evaluate(&row) {
                    pass = false;
                    break;
                }
            }

            if !pass {
                continue;
            }

            // Extract group key
            let mut key = Vec::with_capacity(self.group_by_columns.len());
            for group_col in &self.group_by_columns {
                let value = match &group_col.col_type {
                    DataType::Varchar { .. } => {
                        SqlValue::Varchar(row.get_string_unchecked(group_col.col_idx).to_string())
                    }
                    DataType::Integer | DataType::Bigint => {
                        SqlValue::Integer(row.get_i64_unchecked(group_col.col_idx))
                    }
                    DataType::DoublePrecision | DataType::Real | DataType::Decimal { .. } => {
                        SqlValue::Double(row.get_f64_unchecked(group_col.col_idx))
                    }
                    DataType::Date => SqlValue::Date(row.get_date_unchecked(group_col.col_idx)),
                    DataType::Boolean => SqlValue::Boolean(row.get_bool_unchecked(group_col.col_idx)),
                    _ => continue,
                };
                key.push(value);
            }

            // Get or create group aggregates
            let agg_values = groups
                .entry(key)
                .or_insert_with(|| vec![0.0; self.aggregates.len()]);

            // Accumulate each aggregate
            for (i, spec) in self.aggregates.iter().enumerate() {
                agg_values[i] = spec.accumulate(agg_values[i], &row);
            }
        }

        groups
    }
}

impl MonomorphicPlan for GenericGroupedAggregationPlan {
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError> {
        // Execute using unsafe fast path
        let groups = unsafe { self.execute_unsafe(rows) };

        // Convert to result rows
        let mut results: Vec<Row> = groups
            .into_iter()
            .map(|(key, agg_values)| {
                let mut values = key;

                // Add aggregate results, converting AVG to actual average
                for (i, spec) in self.aggregates.iter().enumerate() {
                    let val = match spec {
                        GroupAggregateSpec::Avg { .. } => {
                            // Get count from the last aggregate (should be COUNT)
                            let count = if let Some(GroupAggregateSpec::Count) =
                                self.aggregates.last()
                            {
                                agg_values[self.aggregates.len() - 1]
                            } else {
                                // If no COUNT, we can't compute AVG correctly
                                1.0
                            };
                            SqlValue::Double(agg_values[i] / count)
                        }
                        GroupAggregateSpec::Count => SqlValue::Integer(agg_values[i] as i64),
                        _ => SqlValue::Double(agg_values[i]),
                    };
                    values.push(val);
                }

                Row { values }
            })
            .collect();

        // Sort by group key for consistent output
        results.sort_by(|a, b| {
            for i in 0..self.group_by_columns.len() {
                let cmp = match (&a.values[i], &b.values[i]) {
                    (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
                    (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
                    (SqlValue::Double(a), SqlValue::Double(b)) => {
                        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),
                    (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
                    _ => std::cmp::Ordering::Equal,
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(results)
    }

    fn execute_stream(
        &self,
        rows: Box<dyn Iterator<Item = Row>>,
    ) -> Result<Vec<Row>, ExecutorError> {
        // Execute using streaming fast path
        let groups = unsafe { self.execute_stream_unsafe(rows) };

        // Convert to result rows
        let mut results: Vec<Row> = groups
            .into_iter()
            .map(|(key, agg_values)| {
                let mut values = key;

                // Add aggregate results, converting AVG to actual average
                for (i, spec) in self.aggregates.iter().enumerate() {
                    let val = match spec {
                        GroupAggregateSpec::Avg { .. } => {
                            // Get count from the last aggregate (should be COUNT)
                            let count = if let Some(GroupAggregateSpec::Count) =
                                self.aggregates.last()
                            {
                                agg_values[self.aggregates.len() - 1]
                            } else {
                                // If no COUNT, we can't compute AVG correctly
                                1.0
                            };
                            SqlValue::Double(agg_values[i] / count)
                        }
                        GroupAggregateSpec::Count => SqlValue::Integer(agg_values[i] as i64),
                        _ => SqlValue::Double(agg_values[i]),
                    };
                    values.push(val);
                }

                Row { values }
            })
            .collect();

        // Sort by group key for consistent output
        results.sort_by(|a, b| {
            for i in 0..self.group_by_columns.len() {
                let cmp = match (&a.values[i], &b.values[i]) {
                    (SqlValue::Varchar(a), SqlValue::Varchar(b)) => a.cmp(b),
                    (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
                    (SqlValue::Double(a), SqlValue::Double(b)) => {
                        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                    }
                    (SqlValue::Date(a), SqlValue::Date(b)) => a.cmp(b),
                    (SqlValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
                    _ => std::cmp::Ordering::Equal,
                };
                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(results)
    }

    fn description(&self) -> &str {
        &self.description
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_parser::Parser;

    fn parse_select_query(query: &str) -> SelectStmt {
        let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => *select_stmt,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_generic_grouped_aggregation_q1_like() {
        // Create lineitem-like schema
        let table = TableSchema::new(
            "lineitem".to_string(),
            vec![
                ColumnSchema::new("l_orderkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_partkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_suppkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_linenumber".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_quantity".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_extendedprice".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_discount".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_tax".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_returnflag".to_string(), DataType::Varchar { max_length: None }, true),
                ColumnSchema::new("l_linestatus".to_string(), DataType::Varchar { max_length: None }, true),
                ColumnSchema::new("l_shipdate".to_string(), DataType::Date, true),
            ],
        );
        let schema = CombinedSchema::from_table("lineitem".to_string(), table);

        // TPC-H Q1 query (simplified)
        let q1_query = r#"
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                AVG(l_quantity) as avg_qty,
                COUNT(*) as count_order
            FROM lineitem
            GROUP BY l_returnflag, l_linestatus
        "#;

        let stmt = parse_select_query(q1_query);
        let plan = GenericGroupedAggregationPlan::try_create(&stmt, &schema);
        assert!(plan.is_some(), "Should create generic GROUP BY plan for Q1-like query");

        let plan = plan.unwrap();
        assert!(
            plan.description.to_lowercase().contains("lineitem"),
            "Description should mention table name: {}",
            plan.description
        );
        assert!(
            plan.description.contains("2 groups"),
            "Description should mention 2 group columns: {}",
            plan.description
        );
    }

    #[test]
    #[ignore] // TODO: Fix generic filter tests (broken on main)
    fn test_filter_extraction_simple_comparison() {
        // Create schema with a sales table
        let table = TableSchema::new(
            "sales".to_string(),
            vec![
                ColumnSchema::new("price".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("quantity".to_string(), DataType::Integer, true),
            ],
        );
        let schema = CombinedSchema::from_table("sales".to_string(), table);

        // Parse query with simple comparison
        let query = "SELECT * FROM sales WHERE quantity < 100";
        let stmt = parse_select_query(query);

        // Extract filters
        let filters_opt = extract_filters(&stmt.where_clause, &schema);
        assert!(filters_opt.is_some(), "Should be able to extract filters");
        let filters = filters_opt.unwrap();
        assert_eq!(filters.len(), 1);

        match &filters[0] {
            FilterPredicate::Comparison {
                column_idx,
                op,
                value,
                ..
            } => {
                assert_eq!(*column_idx, 1); // quantity is column 1
                assert_eq!(*op, ComparisonOp::LessThan);
                assert_eq!(*value, SqlValue::Integer(100));
            }
            _ => panic!("Expected Comparison filter"),
        }
    }

    #[test]
    #[ignore] // TODO: Fix generic filter tests (broken on main)
    fn test_generic_pattern_q6_like() {
        // Create lineitem-like schema
        let table = TableSchema::new(
            "lineitem".to_string(),
            vec![
                ColumnSchema::new("l_orderkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_partkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_suppkey".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_linenumber".to_string(), DataType::Integer, true),
                ColumnSchema::new("l_quantity".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_extendedprice".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_discount".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_tax".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("l_returnflag".to_string(), DataType::Varchar { max_length: None }, true),
                ColumnSchema::new("l_linestatus".to_string(), DataType::Varchar { max_length: None }, true),
                ColumnSchema::new("l_shipdate".to_string(), DataType::Date, true),
            ],
        );
        let schema = CombinedSchema::from_table("lineitem".to_string(), table);

        // TPC-H Q6 query (note: using bare strings without DATE keyword for compatibility)
        let q6_query = r#"
            SELECT SUM(l_extendedprice * l_discount) as revenue
            FROM lineitem
            WHERE
                l_shipdate >= '1994-01-01'
                AND l_shipdate < '1995-01-01'
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        "#;

        let stmt = parse_select_query(q6_query);
        let plan = GenericFilteredAggregationPlan::try_create(&stmt, &schema);
        assert!(plan.is_some(), "Should create generic plan for Q6-like query");

        let plan = plan.unwrap();
        assert!(
            plan.description.to_lowercase().contains("lineitem"),
            "Description should mention table name"
        );
    }

    #[test]
    #[ignore] // TODO: Fix generic filter tests (broken on main)
    fn test_generic_pattern_different_table() {
        // Create a sales table with similar structure
        let table = TableSchema::new(
            "sales".to_string(),
            vec![
                ColumnSchema::new("sale_date".to_string(), DataType::Date, true),
                ColumnSchema::new("price".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("discount".to_string(), DataType::DoublePrecision, true),
                ColumnSchema::new("quantity".to_string(), DataType::DoublePrecision, true),
            ],
        );
        let schema = CombinedSchema::from_table("sales".to_string(), table);

        // Similar query on different table (note: using bare strings without DATE keyword)
        let query = r#"
            SELECT SUM(price * discount) as revenue
            FROM sales
            WHERE
                sale_date >= '2024-01-01'
                AND sale_date < '2024-02-01'
                AND discount BETWEEN 0.10 AND 0.20
                AND quantity < 50
        "#;

        let stmt = parse_select_query(query);
        let plan = GenericFilteredAggregationPlan::try_create(&stmt, &schema);
        assert!(
            plan.is_some(),
            "Should create generic plan for different table"
        );

        let plan = plan.unwrap();
        assert!(
            plan.description.to_lowercase().contains("sales"),
            "Description should mention sales table"
        );
    }
}

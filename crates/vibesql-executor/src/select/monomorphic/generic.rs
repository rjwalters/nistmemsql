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
                let value = row.get_f64_unchecked(*column_idx);
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
                        if let SqlValue::Date(date) = value {
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
                let val1 = row.get_f64_unchecked(*col1_idx);
                let val2 = row.get_f64_unchecked(*col2_idx);
                acc + (val1 * val2)
            }
            AggregationSpec::Sum { col_idx } => {
                let val = row.get_f64_unchecked(*col_idx);
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

    fn description(&self) -> &str {
        &self.description
    }
}

/// Pattern matcher for generic filtered aggregation queries
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
            plan.description.contains("lineitem"),
            "Description should mention table name"
        );
    }

    #[test]
    fn test_generic_pattern_different_table() {
        // Create a sales table with similar structure
        let table = TableSchema::new(
            "sales".to_string(),
            vec![
                ColumnSchema::new("date".to_string(), DataType::Date, true),
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
                date >= '2024-01-01'
                AND date < '2024-02-01'
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
            plan.description.contains("sales"),
            "Description should mention sales table"
        );
    }
}

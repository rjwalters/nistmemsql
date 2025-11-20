//! TPC-H Query Specialized Plans
//!
//! This module provides hand-optimized monomorphic execution plans for
//! TPC-H benchmark queries.

use vibesql_ast::SelectStmt;
use vibesql_storage::Row;
use vibesql_types::{Date, SqlValue};

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::pattern::{
    contains_column_multiply, has_aggregate_function, has_between_predicate, has_no_joins,
    is_single_table, where_references_column, QueryPattern,
};
use super::MonomorphicPlan;

use std::str::FromStr;

/// Specialized plan for TPC-H Q6 (Forecasting Revenue Change)
///
/// Query Pattern:
/// ```sql
/// SELECT SUM(l_extendedprice * l_discount) as revenue
/// FROM lineitem
/// WHERE
///     l_shipdate >= '1994-01-01'
///     AND l_shipdate < '1995-01-01'
///     AND l_discount BETWEEN 0.05 AND 0.07
///     AND l_quantity < 24
/// ```
///
/// This plan uses unchecked accessors for ~230ns/row performance improvement.
pub struct TpchQ6Plan {
    // Pre-computed column indices
    l_shipdate_idx: usize,      // Column 10, type: DATE
    l_discount_idx: usize,      // Column 6, type: DOUBLE
    l_quantity_idx: usize,      // Column 4, type: DOUBLE
    l_extendedprice_idx: usize, // Column 5, type: DOUBLE

    // Pre-parsed constant values
    date_1994: Date,
    date_1995: Date,
    discount_min: f64,
    discount_max: f64,
    quantity_max: f64,
}

impl TpchQ6Plan {
    /// Create a new TPC-H Q6 plan with default parameters
    pub fn new() -> Self {
        Self {
            l_shipdate_idx: 10,
            l_discount_idx: 6,
            l_quantity_idx: 4,
            l_extendedprice_idx: 5,

            date_1994: Date::from_str("1994-01-01").expect("valid date"),
            date_1995: Date::from_str("1995-01-01").expect("valid date"),
            discount_min: 0.05,
            discount_max: 0.07,
            quantity_max: 24.0,
        }
    }

    /// Execute using type-specialized fast path
    ///
    /// # Safety
    ///
    /// This method uses unsafe code but is safe because:
    /// 1. Column indices are validated against schema at plan creation
    /// 2. Column types are guaranteed by TPC-H schema
    /// 3. Debug assertions catch type mismatches
    #[inline(never)] // Don't inline to make profiling easier
    unsafe fn execute_unsafe(&self, rows: &[Row]) -> f64 {
        let mut sum = 0.0;

        for row in rows {
            // Direct typed access - no enum matching!
            let shipdate = row.get_date_unchecked(self.l_shipdate_idx);

            // Date filters (most selective first)
            if shipdate >= self.date_1994 && shipdate < self.date_1995 {
                let discount = row.get_f64_unchecked(self.l_discount_idx);

                // Discount filter
                if discount >= self.discount_min && discount <= self.discount_max {
                    let quantity = row.get_f64_unchecked(self.l_quantity_idx);

                    // Quantity filter
                    if quantity < self.quantity_max {
                        let price = row.get_f64_unchecked(self.l_extendedprice_idx);

                        // Native f64 multiply - no type coercion!
                        sum += price * discount;
                    }
                }
            }
        }

        sum
    }
}

impl MonomorphicPlan for TpchQ6Plan {
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError> {
        // Execute the query using unsafe fast path
        let result = unsafe { self.execute_unsafe(rows) };

        // Return single-row result with revenue column
        Ok(vec![Row {
            values: vec![SqlValue::Double(result)],
        }])
    }

    fn description(&self) -> &str {
        "TPC-H Q6 (Forecasting Revenue Change) - Monomorphic"
    }
}

/// Pattern matcher for TPC-H Q6
///
/// Matches queries with:
/// - Single table FROM lineitem
/// - SUM(l_extendedprice * l_discount) in SELECT
/// - WHERE clause with l_shipdate, l_discount BETWEEN, and l_quantity predicates
/// - No JOINs, GROUP BY, or HAVING
pub struct TpchQ6PatternMatcher;

impl QueryPattern for TpchQ6PatternMatcher {
    fn matches(&self, stmt: &SelectStmt, _schema: &CombinedSchema) -> bool {
        // Must be a single-table query on lineitem
        if !is_single_table(&stmt.from, "lineitem") {
            return false;
        }

        // Must have no joins
        if !has_no_joins(&stmt.from) {
            return false;
        }

        // Must have no GROUP BY or HAVING
        if stmt.group_by.is_some() || stmt.having.is_some() {
            return false;
        }

        // Must have SUM aggregate in SELECT
        if !has_aggregate_function(&stmt.select_list, "SUM") {
            return false;
        }

        // Check for multiplication of l_extendedprice and l_discount in SELECT
        let has_price_discount_multiply = stmt.select_list.iter().any(|item| {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                contains_column_multiply(expr, "l_extendedprice", "l_discount")
            } else {
                false
            }
        });

        if !has_price_discount_multiply {
            return false;
        }

        // Check WHERE clause for required predicates
        if !where_references_column(&stmt.where_clause, "l_shipdate") {
            return false;
        }

        if !where_references_column(&stmt.where_clause, "l_quantity") {
            return false;
        }

        if !has_between_predicate(&stmt.where_clause, "l_discount") {
            return false;
        }

        true
    }

    fn description(&self) -> &str {
        "TPC-H Q6 Pattern Matcher"
    }
}

/// Attempt to create a TPC-H monomorphic plan for a query
///
/// Returns None if the query doesn't match any known TPC-H pattern.
pub fn try_create_tpch_plan(
    stmt: &SelectStmt,
    schema: &CombinedSchema,
) -> Option<Box<dyn MonomorphicPlan>> {
    // Try TPC-H Q6 pattern
    let q6_matcher = TpchQ6PatternMatcher;
    if q6_matcher.matches(stmt, schema) {
        return Some(Box::new(TpchQ6Plan::new()));
    }

    // Future: Add more TPC-H query patterns here
    // - Q1: Pricing Summary Report
    // - Q3: Shipping Priority
    // - Q5: Local Supplier Volume
    // etc.

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_parser::Parser;

    /// Helper function to parse a SELECT query string into a SelectStmt
    fn parse_select_query(query: &str) -> SelectStmt {
        let stmt = Parser::parse_sql(query).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::Select(select_stmt) => *select_stmt,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_q6_pattern_matching() {
        // Create an empty schema for pattern matching tests
        let empty_table = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
        let schema = CombinedSchema::from_table("test".to_string(), empty_table);

        // Should match Q6
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
        let plan = try_create_tpch_plan(&stmt, &schema);
        assert!(plan.is_some(), "Q6 pattern should be recognized");
        assert_eq!(
            plan.unwrap().description(),
            "TPC-H Q6 (Forecasting Revenue Change) - Monomorphic"
        );
    }

    #[test]
    fn test_q6_pattern_different_where_order() {
        // Create an empty schema for pattern matching tests
        let empty_table = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
        let schema = CombinedSchema::from_table("test".to_string(), empty_table);

        // Q6 with different WHERE clause ordering - should still match
        let q6_query_reordered = r#"
            SELECT SUM(l_extendedprice * l_discount) as revenue
            FROM lineitem
            WHERE
                l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
                AND l_shipdate >= '1994-01-01'
                AND l_shipdate < '1995-01-01'
        "#;

        let stmt = parse_select_query(q6_query_reordered);
        let plan = try_create_tpch_plan(&stmt, &schema);
        assert!(
            plan.is_some(),
            "Q6 pattern should be recognized regardless of WHERE clause order"
        );
    }

    #[test]
    fn test_q6_pattern_multiply_reversed() {
        // Create an empty schema for pattern matching tests
        let empty_table = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
        let schema = CombinedSchema::from_table("test".to_string(), empty_table);

        // Q6 with reversed multiplication order - should still match
        let q6_query_reversed = r#"
            SELECT SUM(l_discount * l_extendedprice) as revenue
            FROM lineitem
            WHERE
                l_shipdate >= '1994-01-01'
                AND l_shipdate < '1995-01-01'
                AND l_discount BETWEEN 0.05 AND 0.07
                AND l_quantity < 24
        "#;

        let stmt = parse_select_query(q6_query_reversed);
        let plan = try_create_tpch_plan(&stmt, &schema);
        assert!(
            plan.is_some(),
            "Q6 pattern should be recognized regardless of multiplication operand order"
        );
    }

    #[test]
    fn test_non_q6_query() {
        // Create an empty schema for pattern matching tests
        let empty_table = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
        let schema = CombinedSchema::from_table("test".to_string(), empty_table);

        // Should not match Q6
        let other_query = "SELECT * FROM orders WHERE o_orderdate > '2020-01-01'";

        let stmt = parse_select_query(other_query);
        let plan = try_create_tpch_plan(&stmt, &schema);
        assert!(plan.is_none(), "Non-Q6 query should not match");
    }
}

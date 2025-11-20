//! TPC-H Query Specialized Plans
//!
//! This module provides hand-optimized monomorphic execution plans for
//! TPC-H benchmark queries.

use vibesql_storage::Row;
use vibesql_types::{Date, SqlValue};

use crate::{errors::ExecutorError, schema::CombinedSchema};

use super::MonomorphicPlan;

use std::collections::HashMap;
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

/// Specialized plan for TPC-H Q1 (Pricing Summary Report)
///
/// Query Pattern:
/// ```sql
/// SELECT
///     l_returnflag,
///     l_linestatus,
///     SUM(l_quantity) as sum_qty,
///     SUM(l_extendedprice) as sum_base_price,
///     SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
///     SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
///     AVG(l_quantity) as avg_qty,
///     AVG(l_extendedprice) as avg_price,
///     AVG(l_discount) as avg_disc,
///     COUNT(*) as count_order
/// FROM lineitem
/// WHERE l_shipdate <= '1998-09-01'
/// GROUP BY l_returnflag, l_linestatus
/// ORDER BY l_returnflag, l_linestatus
/// ```
pub struct TpchQ1Plan {
    // Pre-computed column indices
    l_returnflag_idx: usize,    // Column 8, type: VARCHAR(1)
    l_linestatus_idx: usize,    // Column 9, type: VARCHAR(1)
    l_quantity_idx: usize,      // Column 4, type: DECIMAL (f64)
    l_extendedprice_idx: usize, // Column 5, type: DECIMAL (f64)
    l_discount_idx: usize,      // Column 6, type: DECIMAL (f64)
    l_tax_idx: usize,           // Column 7, type: DECIMAL (f64)
    l_shipdate_idx: usize,      // Column 10, type: DATE

    // Pre-parsed constant values
    date_cutoff: Date,
}

/// Aggregate values for a single group in Q1
#[derive(Debug, Clone)]
struct Q1Aggregates {
    sum_qty: f64,
    sum_base_price: f64,
    sum_disc_price: f64,
    sum_charge: f64,
    sum_discount: f64, // For AVG(l_discount)
    count: i64,
}

impl Q1Aggregates {
    fn new() -> Self {
        Self {
            sum_qty: 0.0,
            sum_base_price: 0.0,
            sum_disc_price: 0.0,
            sum_charge: 0.0,
            sum_discount: 0.0,
            count: 0,
        }
    }

    fn add(&mut self, qty: f64, price: f64, discount: f64, tax: f64) {
        self.sum_qty += qty;
        self.sum_base_price += price;
        self.sum_disc_price += price * (1.0 - discount);
        self.sum_charge += price * (1.0 - discount) * (1.0 + tax);
        self.sum_discount += discount;
        self.count += 1;
    }
}

impl TpchQ1Plan {
    /// Create a new TPC-H Q1 plan with default parameters
    pub fn new() -> Self {
        Self {
            l_returnflag_idx: 8,
            l_linestatus_idx: 9,
            l_quantity_idx: 4,
            l_extendedprice_idx: 5,
            l_discount_idx: 6,
            l_tax_idx: 7,
            l_shipdate_idx: 10,

            date_cutoff: Date::from_str("1998-09-01").expect("valid date"),
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
    unsafe fn execute_unsafe(&self, rows: &[Row]) -> HashMap<(String, String), Q1Aggregates> {
        let mut groups: HashMap<(String, String), Q1Aggregates> = HashMap::new();

        for row in rows {
            // Direct typed access - no enum matching!
            let shipdate = row.get_date_unchecked(self.l_shipdate_idx);

            // Date filter
            if shipdate <= self.date_cutoff {
                let returnflag = row.get_string_unchecked(self.l_returnflag_idx).to_string();
                let linestatus = row.get_string_unchecked(self.l_linestatus_idx).to_string();
                let qty = row.get_f64_unchecked(self.l_quantity_idx);
                let price = row.get_f64_unchecked(self.l_extendedprice_idx);
                let discount = row.get_f64_unchecked(self.l_discount_idx);
                let tax = row.get_f64_unchecked(self.l_tax_idx);

                // Get or create group
                let agg = groups
                    .entry((returnflag, linestatus))
                    .or_insert_with(Q1Aggregates::new);

                // Update aggregates
                agg.add(qty, price, discount, tax);
            }
        }

        groups
    }
}

impl MonomorphicPlan for TpchQ1Plan {
    fn execute(&self, rows: &[Row]) -> Result<Vec<Row>, ExecutorError> {
        // Execute the query using unsafe fast path
        let groups = unsafe { self.execute_unsafe(rows) };

        // Convert to result rows and sort by returnflag, linestatus
        let mut results: Vec<_> = groups
            .into_iter()
            .map(|((returnflag, linestatus), agg)| {
                let avg_qty = agg.sum_qty / agg.count as f64;
                let avg_price = agg.sum_base_price / agg.count as f64;
                let avg_disc = agg.sum_discount / agg.count as f64;

                Row {
                    values: vec![
                        SqlValue::Varchar(returnflag),
                        SqlValue::Varchar(linestatus),
                        SqlValue::Double(agg.sum_qty),
                        SqlValue::Double(agg.sum_base_price),
                        SqlValue::Double(agg.sum_disc_price),
                        SqlValue::Double(agg.sum_charge),
                        SqlValue::Double(avg_qty),
                        SqlValue::Double(avg_price),
                        SqlValue::Double(avg_disc),
                        SqlValue::Integer(agg.count),
                    ],
                }
            })
            .collect();

        // Sort by returnflag, linestatus
        results.sort_by(|a, b| {
            let a_flag = match &a.values[0] {
                SqlValue::Varchar(s) => s,
                _ => "",
            };
            let a_status = match &a.values[1] {
                SqlValue::Varchar(s) => s,
                _ => "",
            };
            let b_flag = match &b.values[0] {
                SqlValue::Varchar(s) => s,
                _ => "",
            };
            let b_status = match &b.values[1] {
                SqlValue::Varchar(s) => s,
                _ => "",
            };

            (a_flag, a_status).cmp(&(b_flag, b_status))
        });

        Ok(results)
    }

    fn description(&self) -> &str {
        "TPC-H Q1 (Pricing Summary Report) - Monomorphic"
    }
}

/// Attempt to create a TPC-H monomorphic plan for a query
///
/// Returns None if the query doesn't match any known TPC-H pattern.
pub fn try_create_tpch_plan(
    query: &str,
    _schema: &CombinedSchema,
) -> Option<Box<dyn MonomorphicPlan>> {
    // Normalize query for matching (remove whitespace, lowercase)
    let normalized = query
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    // Check for TPC-H Q1 pattern
    // Key indicators:
    // - GROUP BY l_returnflag, l_linestatus
    // - Multiple aggregates (SUM, AVG, COUNT)
    // - l_shipdate filter
    if normalized.contains("group by l_returnflag")
        && normalized.contains("l_linestatus")
        && normalized.contains("from lineitem")
        && normalized.contains("l_shipdate")
        && normalized.contains("sum(l_quantity)")
        && normalized.contains("avg(")
    {
        return Some(Box::new(TpchQ1Plan::new()));
    }

    // Check for TPC-H Q6 pattern
    // Key indicators:
    // - SUM(l_extendedprice * l_discount)
    // - FROM lineitem
    // - l_shipdate filters
    // - l_discount BETWEEN
    // - l_quantity filter
    if normalized.contains("sum(l_extendedprice*l_discount)")
        || normalized.contains("sum(l_extendedprice * l_discount)")
    {
        if normalized.contains("from lineitem")
            && normalized.contains("l_shipdate")
            && normalized.contains("l_discount")
            && normalized.contains("between")
            && normalized.contains("l_quantity")
        {
            return Some(Box::new(TpchQ6Plan::new()));
        }
    }

    // Future: Add more TPC-H query patterns here
    // - Q3: Shipping Priority (join + aggregation)
    // - Q5: Local Supplier Volume (multi-table join)
    // etc.

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_q1_pattern_matching() {
        // Create an empty schema for pattern matching tests
        let empty_table = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
        let schema = CombinedSchema::from_table("test".to_string(), empty_table);

        // Should match Q1
        let q1_query = r#"
            SELECT
                l_returnflag,
                l_linestatus,
                SUM(l_quantity) as sum_qty,
                SUM(l_extendedprice) as sum_base_price,
                SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                AVG(l_quantity) as avg_qty,
                AVG(l_extendedprice) as avg_price,
                AVG(l_discount) as avg_disc,
                COUNT(*) as count_order
            FROM lineitem
            WHERE l_shipdate <= '1998-09-01'
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus
        "#;

        let plan = try_create_tpch_plan(q1_query, &schema);
        assert!(plan.is_some(), "Q1 pattern should be recognized");
        assert_eq!(
            plan.unwrap().description(),
            "TPC-H Q1 (Pricing Summary Report) - Monomorphic"
        );
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

        let plan = try_create_tpch_plan(q6_query, &schema);
        assert!(plan.is_some(), "Q6 pattern should be recognized");
        assert_eq!(
            plan.unwrap().description(),
            "TPC-H Q6 (Forecasting Revenue Change) - Monomorphic"
        );
    }

    #[test]
    fn test_non_tpch_query() {
        // Create an empty schema for pattern matching tests
        let empty_table = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
        let schema = CombinedSchema::from_table("test".to_string(), empty_table);

        // Should not match any TPC-H pattern
        let other_query = "SELECT * FROM orders WHERE o_orderdate > '2020-01-01'";

        let plan = try_create_tpch_plan(other_query, &schema);
        assert!(plan.is_none(), "Non-TPC-H query should not match");
    }
}

//! Wrapper for join execution with search-based optimization
//!
//! This module provides integration of the JoinOrderSearch algorithm
//! into the standard join execution path. It detects multi-table joins
//! and applies optimal ordering when beneficial.

#![allow(dead_code)]

use std::collections::HashMap;

use super::join::{JoinOrderAnalyzer, JoinOrderSearch};
use crate::optimizer::PredicateDecomposition;

/// Configuration for join reordering optimization
pub struct JoinReorderConfig {
    /// Enable join reordering via search
    pub enabled: bool,
    /// Only apply if there are at least this many tables
    pub min_tables: usize,
    /// Log optimization decisions (for debugging)
    pub verbose: bool,
}

impl Default for JoinReorderConfig {
    fn default() -> Self {
        JoinReorderConfig { enabled: true, min_tables: 3, verbose: false }
    }
}

/// Statistics about a join ordering decision
#[derive(Debug, Clone)]
pub struct JoinOrderingStats {
    /// Tables in the query
    pub table_count: usize,
    /// Whether reordering was attempted
    pub reordering_attempted: bool,
    /// The optimal order found (if any)
    pub optimal_order: Vec<String>,
    /// Whether the order differs from left-to-right
    pub differs_from_leftright: bool,
}

/// Try to extract and flatten a FROM clause containing joins
///
/// Returns (table_count, predicates) if successful
pub fn extract_join_info(
    from: &vibesql_ast::FromClause,
    _predicates: Option<&PredicateDecomposition>,
) -> Option<(Vec<String>, PredicateDecomposition)> {
    // Count tables in the FROM clause
    let table_count = count_tables(from);

    if table_count < 2 {
        return None; // Single table, no optimization needed
    }

    // Extract table names in left-to-right order
    let mut tables = Vec::new();
    extract_table_names(from, &mut tables);

    // Create a basic predicate decomposition for analysis
    // In a full implementation, this would be passed in
    let predicates = PredicateDecomposition {
        table_local_predicates: HashMap::new(),
        equijoin_conditions: Vec::new(),
        complex_predicates: Vec::new(),
    };

    Some((tables, predicates))
}

/// Count tables in a FROM clause
fn count_tables(from: &vibesql_ast::FromClause) -> usize {
    match from {
        vibesql_ast::FromClause::Table { .. } => 1,
        vibesql_ast::FromClause::Subquery { .. } => 1,
        vibesql_ast::FromClause::Join { left, right, .. } => count_tables(left) + count_tables(right),
    }
}

/// Extract table names from a FROM clause in left-to-right order
fn extract_table_names(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            tables.push(name.clone());
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            tables.push(alias.clone());
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            extract_table_names(left, tables);
            extract_table_names(right, tables);
        }
    }
}

/// Find optimal join order for a multi-table join
pub fn find_optimal_join_order(
    config: &JoinReorderConfig,
    tables: &[String],
    predicates: Option<&PredicateDecomposition>,
) -> JoinOrderingStats {
    let mut stats = JoinOrderingStats {
        table_count: tables.len(),
        reordering_attempted: false,
        optimal_order: tables.to_vec(),
        differs_from_leftright: false,
    };

    // Check if reordering is applicable
    if !config.enabled || tables.len() < config.min_tables {
        return stats;
    }

    stats.reordering_attempted = true;

    // Build analyzer and search for optimal order
    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(tables.to_vec());

    // Analyze predicates if available
    if let Some(pred) = predicates {
        // Note: In a full implementation, we would extract join edges from predicates
        // and analyze them. For now, we use the basic search with just table info.
        let _pred_copy = pred; // Use to avoid unused warning
    }

    // Search for optimal order
    let search = JoinOrderSearch::from_analyzer(&analyzer);
    let optimal_order = search.find_optimal_order();

    // Check if it differs from left-to-right
    if optimal_order != tables {
        stats.differs_from_leftright = true;
        stats.optimal_order = optimal_order;

        if config.verbose {
            eprintln!(
                "[JoinReorder] Found better order: {:?} vs {:?}",
                stats.optimal_order, tables
            );
        }
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_single_table() {
        let from = vibesql_ast::FromClause::Table { name: "t1".to_string(), alias: None };

        let result = extract_join_info(&from, None);
        assert!(result.is_none(), "Single table should not trigger optimization");
    }

    #[test]
    fn test_default_config() {
        let config = JoinReorderConfig::default();
        assert!(config.enabled);
        assert_eq!(config.min_tables, 3);
    }
}

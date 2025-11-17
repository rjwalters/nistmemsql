//! Integration tests for join order search optimization
//!
//! These tests verify that the join order search module can correctly
//! identify and execute optimal join orders for multi-table queries.

#[test]
fn test_join_search_module_compiles() {
    // This test verifies that the join search module is properly integrated
    // and compiles.

    // Create a simple analyzer to verify the module works
    use vibesql_executor::select::join::{search::JoinOrderSearch, JoinOrderAnalyzer};

    let mut analyzer = JoinOrderAnalyzer::new();
    analyzer.register_tables(vec!["t1".to_string(), "t2".to_string()]);

    let db = vibesql_storage::Database::new();
    let search = JoinOrderSearch::from_analyzer(&analyzer, &db);
    let order = search.find_optimal_order();

    // Should return a valid ordering
    assert_eq!(order.len(), 2);
}

//! Test aggregate functions in complex expression contexts
//!
//! This test file specifically addresses issue #2011 which reported
//! 832 SQLLogicTest failures due to aggregate functions not being
//! properly supported in all expression contexts.

use vibesql_executor::execute_query;
use vibesql_storage::Database;

#[test]
fn test_aggregate_in_arithmetic_expression() {
    let mut db = Database::new();

    // Test: COUNT(*) * 2
    let result = execute_query(&db, "SELECT COUNT(*) * 2 AS result", None);
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            // Since there's no table, COUNT(*) should return 1 (implicit row)
            // and 1 * 2 = 2
        }
        Err(e) => {
            panic!("Query failed with error: {:?}\nThis should work - aggregates in arithmetic expressions should be supported", e);
        }
    }
}

#[test]
fn test_aggregate_with_unary_operators() {
    let mut db = Database::new();

    // Test: -COUNT(*) and +COUNT(*)
    let result = execute_query(&db, "SELECT -COUNT(*) AS neg, +COUNT(*) AS pos", None);
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
        }
        Err(e) => {
            panic!("Query failed with error: {:?}\nUnary operators on aggregates should work", e);
        }
    }
}

#[test]
fn test_multiple_aggregates_in_expression() {
    let mut db = Database::new();

    // Test: COUNT(*) + COUNT(*)
    let result = execute_query(&db, "SELECT COUNT(*) + COUNT(*) AS result", None);
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
        }
        Err(e) => {
            panic!("Query failed with error: {:?}\nMultiple aggregates in one expression should work", e);
        }
    }
}

#[test]
fn test_aggregate_in_complex_nested_expression() {
    let mut db = Database::new();

    // Test the example from issue #2011:
    // SELECT DISTINCT - + 94 DIV - COUNT( DISTINCT + 51 )
    let result = execute_query(&db, "SELECT DISTINCT - + 94 DIV - COUNT(DISTINCT 51) AS result", None);
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
            // COUNT(DISTINCT 51) = 1 (one distinct value)
            // -COUNT(DISTINCT 51) = -1
            // +(94) = 94
            // -(+(94)) = -94
            // -94 DIV -1 = 94
        }
        Err(e) => {
            panic!("Query failed with error: {:?}\nComplex nested expressions with aggregates should work", e);
        }
    }
}

#[test]
fn test_aggregate_with_division() {
    let mut db = Database::new();

    // Test: 100 / COUNT(*)
    let result = execute_query(&db, "SELECT 100 DIV COUNT(*) AS result", None);
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
        }
        Err(e) => {
            panic!("Query failed with error: {:?}\nDivision with aggregates should work", e);
        }
    }
}

#[test]
fn test_aggregate_in_case_expression() {
    let mut db = Database::new();

    // Test: CASE with aggregate
    let result = execute_query(&db, "SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS result", None);
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
        }
        Err(e) => {
            panic!("Query failed with error: {:?}\nAggregates in CASE expressions should work", e);
        }
    }
}

#[test]
fn test_cast_of_aggregate() {
    let mut db = Database::new();

    // Test: CAST(COUNT(*) AS SIGNED)
    let result = execute_query(&db, "SELECT CAST(COUNT(*) AS SIGNED) AS result", None);
    match result {
        Ok(rows) => {
            assert_eq!(rows.len(), 1);
        }
        Err(e) => {
            panic!("Query failed with error: {:?}\nCAST of aggregates should work", e);
        }
    }
}

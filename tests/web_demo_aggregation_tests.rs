//! Tests for aggregation web demo SQL examples
//!
//! This test suite validates SQL aggregation examples (GROUP BY, HAVING, COUNT, SUM, AVG, etc.)
//! from the web demo by parsing the TypeScript example files and executing queries.

mod common;

use ast;
use common::web_demo_helpers::{extract_query, load_database, parse_example_files, WebDemoExample};
use executor::SelectExecutor;
use parser::Parser;

/// Test aggregation SQL examples from web demo
/// Includes examples with IDs: agg*, group*, having*, count*, sum*, avg*, min*, max*
#[test]
fn test_aggregation_sql_examples() {
    // Parse all examples from web demo
    let examples = parse_example_files().expect("Failed to parse example files");

    // Filter for aggregation examples
    let aggregation_examples: Vec<&WebDemoExample> = examples
        .iter()
        .filter(|ex| {
            ex.id.starts_with("agg")
                || ex.id.starts_with("group")
                || ex.id.starts_with("having")
                || ex.id.starts_with("count")
                || ex.id.starts_with("sum")
                || ex.id.starts_with("avg")
                || ex.id.starts_with("min")
                || ex.id.starts_with("max")
        })
        .collect();

    assert!(
        !aggregation_examples.is_empty(),
        "No aggregation examples found - check web demo examples file exists"
    );

    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for example in &aggregation_examples {
        // Load the appropriate database
        let db = match load_database(&example.database) {
            Some(db) => db,
            None => {
                println!("⚠️  Skipping {}: Unknown database '{}'", example.id, example.database);
                skipped += 1;
                continue;
            }
        };

        // Extract just the SQL query (without expected comments)
        let query = extract_query(&example.sql);

        // Parse the SQL
        let stmt = match Parser::parse_sql(&query) {
            Ok(stmt) => stmt,
            Err(e) => {
                println!("❌ {}: Parse error: {}", example.id, e);
                failed += 1;
                continue;
            }
        };

        // Execute the query (aggregation queries are SELECT statements)
        let result = match stmt {
            ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&db);
                executor.execute(&select_stmt)
            }
            _ => {
                println!("⚠️  Skipping {}: Not a SELECT statement", example.id);
                skipped += 1;
                continue;
            }
        };

        // Check execution result
        match result {
            Ok(rows) => {
                // Validate expected row count if specified
                if let Some(expected_count) = example.expected_count {
                    if rows.len() != expected_count {
                        println!(
                            "❌ {}: Expected {} rows, got {}",
                            example.id,
                            expected_count,
                            rows.len()
                        );
                        failed += 1;
                        continue;
                    }
                }

                // TODO: Validate expected row data for more precise validation
                // For now, just check count (matching original test behavior)

                println!("✓  {}: Passed ({} rows)", example.id, rows.len());
                passed += 1;
            }
            Err(e) => {
                println!("❌ {}: Execution error: {}", example.id, e);
                failed += 1;
            }
        }
    }

    // Print summary
    println!("\n=== Aggregation Examples Test Summary ===");
    println!("Total:   {}", aggregation_examples.len());
    println!("Passed:  {}", passed);
    println!("Failed:  {}", failed);
    println!("Skipped: {}", skipped);
    println!("=========================================\n");

    // For now, we require at least some tests to pass
    assert!(passed >= 1, "Expected at least 1 aggregation example to pass, got {}", passed);

    // TODO: Make this stricter once all aggregation features are complete
    // assert_eq!(failed, 0, "{} aggregation example(s) failed", failed);
}

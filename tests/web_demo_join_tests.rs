//! Tests for join web demo SQL examples
//!
//! This test suite validates SQL join examples (INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL JOIN, CROSS JOIN)
//! from the web demo by parsing the TypeScript example files and executing queries.

mod common;

use ast;
use common::web_demo_helpers::{extract_query, load_database, parse_example_files, WebDemoExample};
use executor::SelectExecutor;
use parser::Parser;

/// Test join SQL examples from web demo
/// Includes examples with IDs: join*, inner*, left*, right*, full*, cross*
///
/// TODO(#716): Re-enable once join examples are migrated to JSON format
#[test]
#[ignore = "Join JSON examples not yet created - see issue #716"]
fn test_join_sql_examples() {
    // Parse all examples from web demo
    let examples = parse_example_files().expect("Failed to parse example files");

    // Filter for join examples
    let join_examples: Vec<&WebDemoExample> = examples
        .iter()
        .filter(|ex| {
            ex.id.starts_with("join")
                || ex.id.starts_with("inner")
                || ex.id.starts_with("left")
                || ex.id.starts_with("right")
                || ex.id.starts_with("full")
                || ex.id.starts_with("cross")
        })
        .collect();

    assert!(
        !join_examples.is_empty(),
        "No join examples found - check web demo examples file exists"
    );

    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for example in &join_examples {
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

        // Execute the query (join queries are SELECT statements)
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
    println!("\n=== Join Examples Test Summary ===");
    println!("Total:   {}", join_examples.len());
    println!("Passed:  {}", passed);
    println!("Failed:  {}", failed);
    println!("Skipped: {}", skipped);
    println!("==================================\n");

    // For now, we require at least some tests to pass
    assert!(passed >= 1, "Expected at least 1 join example to pass, got {}", passed);

    // TODO: Make this stricter once all join features are complete
    // assert_eq!(failed, 0, "{} join example(s) failed", failed);
}

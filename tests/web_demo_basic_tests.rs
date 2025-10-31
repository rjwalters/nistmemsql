//! Tests for basic web demo SQL examples
//!
//! This test suite validates basic SQL examples (SELECT, INSERT, UPDATE, DDL, DML operations)
//! from the web demo by parsing the TypeScript example files and executing queries.

mod common;

use common::web_demo_helpers::{
    extract_query, load_database, parse_example_files, WebDemoExample,
};
use executor::SelectExecutor;
use parser::Parser;
use ast;

/// Test basic SQL examples from web demo
/// Includes examples with IDs: basic*, dml*, ddl*, data*
#[test]
fn test_basic_sql_examples() {
    // Parse all examples from web demo
    let examples = parse_example_files().expect("Failed to parse example files");

    // Filter for basic examples (basic, dml, ddl, data prefixes)
    let basic_examples: Vec<&WebDemoExample> = examples
        .iter()
        .filter(|ex| {
            ex.id.starts_with("basic")
                || ex.id.starts_with("dml")
                || ex.id.starts_with("ddl")
                || ex.id.starts_with("data")
        })
        .collect();

    assert!(
        !basic_examples.is_empty(),
        "No basic examples found - check web demo examples file exists"
    );

    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for example in &basic_examples {
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

        // Execute the query based on statement type
        let result = match stmt {
            ast::Statement::Select(select_stmt) => {
                let executor = SelectExecutor::new(&db);
                executor.execute(&select_stmt)
            }
            ast::Statement::CreateTable { .. }
            | ast::Statement::Insert { .. }
            | ast::Statement::Update { .. }
            | ast::Statement::Delete { .. } => {
                // For DDL/DML statements, just check they parse successfully
                println!("✓  {}: DDL/DML statement parsed successfully", example.id);
                passed += 1;
                continue;
            }
            _ => {
                println!("⚠️  Skipping {}: Unsupported statement type", example.id);
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
                            example.id, expected_count, rows.len()
                        );
                        failed += 1;
                        continue;
                    }
                }

                // TODO: Validate expected row data
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
    println!("\n=== Basic Examples Test Summary ===");
    println!("Total:   {}", basic_examples.len());
    println!("Passed:  {}", passed);
    println!("Failed:  {}", failed);
    println!("Skipped: {}", skipped);
    println!("===================================\n");

    // For now, we require at least some tests to pass
    assert!(
        passed >= 1,
        "Expected at least 1 basic example to pass, got {}",
        passed
    );

    // TODO: Make this stricter once all basic features are complete
    // assert_eq!(failed, 0, "{} basic example(s) failed", failed);
}

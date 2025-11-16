//! Comprehensive SQLLogicTest suite runner using the dolthub/sqllogictest submodule.
//!
//! This test suite runs ALL ~5.9 million SQL tests from the official SQLLogicTest corpus.
//!
//! Tests are organized by category:
//! - select1-5.test: Basic SELECT queries
//! - evidence/: Core SQL language features
//! - index/: Index and ordering tests
//! - random/: Randomized query tests
//! - ddl/: Data Definition Language tests

mod sqllogictest;

use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::Write,
    path::PathBuf,
    time::Instant,
};

use sqllogictest::{
    execution::{run_test_file_with_details, TestError},
    stats::{TestFailure, TestStats},
};

/// Run SQLLogicTest files from the submodule - all files, no sampling
fn run_test_suite() -> (HashMap<String, TestStats>, usize) {
    let test_dir = PathBuf::from("third_party/sqllogictest/test");
    let mut results = HashMap::new();

    let start_time = Instant::now();

    // Check if we're filtering to specific files (for parallel workers)
    let filter_files: Option<HashSet<String>> = env::var("SQLLOGICTEST_FILES")
        .ok()
        .map(|files_str| {
            files_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        });

    // Find all .test files
    let pattern = format!("{}/**/*.test", test_dir.display());
    let mut all_test_files: Vec<PathBuf> =
        glob::glob(&pattern).expect("Failed to read test pattern").filter_map(Result::ok).collect();

    // Filter to specific files if SQLLOGICTEST_FILES is set
    if let Some(ref filter) = filter_files {
        all_test_files.retain(|test_file| {
            let relative_path = test_file
                .strip_prefix(&test_dir)
                .unwrap_or(test_file)
                .to_string_lossy()
                .to_string();
            filter.contains(&relative_path)
        });
    }

    let total_available_files = all_test_files.len();

    if let Some(worker_id) = env::var("SQLLOGICTEST_WORKER_ID").ok() {
        println!("\n=== SQLLogicTest Suite (Worker {}) ===", worker_id);
    } else {
        println!("\n=== SQLLogicTest Suite (Full Run) ===");
    }
    println!("Total test files: {}", total_available_files);
    println!("Starting test run...\n");

    let mut files_tested = 0;

    for test_file in all_test_files {
        files_tested += 1;

        // Get relative path from test_dir
        let relative_path = test_file
            .strip_prefix(&test_dir)
            .unwrap_or(&test_file)
            .to_string_lossy()
            .to_string();

        // Determine category from path
        let category = if relative_path.starts_with("select") {
            "select"
        } else if relative_path.starts_with("evidence/") {
            "evidence"
        } else if relative_path.starts_with("index/") {
            "index"
        } else if relative_path.starts_with("random/") {
            "random"
        } else if relative_path.starts_with("ddl/") {
            "ddl"
        } else {
            "other"
        }
        .to_string();

        let stats = results.entry(category.clone()).or_insert_with(TestStats::default);
        stats.total += 1;
        stats.tested_files.insert(relative_path.clone());

        // Read and run test file
        let contents = match std::fs::read_to_string(&test_file) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("✗ {} - Failed to read file: {}", relative_path, e);
                stats.errors += 1;
                continue;
            }
        };

        // Log test file start
        eprintln!("[Worker] Starting: {}", relative_path);
        let _test_start = Instant::now();

        // Create a new database for each test file and run with detailed failure capture
        let (test_result, detailed_failures) =
            run_test_file_with_details(&contents, &relative_path);

        match test_result {
            Ok(_) => {
                stats.passed += 1;
            }
            Err(TestError::Timeout { file, timeout_seconds }) => {
                eprintln!("⏱️  TIMEOUT: {} exceeded {}s", file, timeout_seconds);
                stats.failed += 1;
                // Always track failed files, even if detailed_failures is empty
                // This ensures accurate pass/fail reporting in JSON output
                stats.detailed_failures.push((relative_path.clone(), detailed_failures));
            }
            Err(TestError::Execution(e)) => {
                eprintln!("✗ {} - {}", relative_path, e);
                stats.failed += 1;
                // Always track failed files, even if detailed_failures is empty
                // This ensures accurate pass/fail reporting in JSON output
                stats.detailed_failures.push((relative_path.clone(), detailed_failures));
            }
        }
    }

    println!(
        "\n✅ All {} test files completed!",
        total_available_files
    );
    println!("Total time: {:.1} seconds", start_time.elapsed().as_secs_f64());
    println!("Files tested: {}\n", files_tested);

    (results, total_available_files)
}

fn main() {
    // If SELECT1_ONLY is set, run only select1.test
    if env::var("SELECT1_ONLY").is_ok() {
        let test_file = PathBuf::from("third_party/sqllogictest/test/select1.test");
        let contents = std::fs::read_to_string(&test_file).expect("Failed to read select1.test");
        let (test_result, detailed_failures) =
            run_test_file_with_details(&contents, "select1.test");

        match test_result {
            Ok(_) => {
                // Success message already printed
            }
            Err(_) => {
                // Error message already printed
                for failure in detailed_failures {
                    println!("  SQL: {}", failure.sql_statement);
                    println!("  Expected: {:?}", failure.expected_result);
                    println!("  Actual: {:?}", failure.actual_result);
                    println!("  Error: {}", failure.error_message);
                    println!("  Line: {:?}", failure.line_number);
                    println!();
                }
                panic!("select1.test failed");
            }
        }
        return;
    }

    // Check if submodule is initialized
    let test_dir = PathBuf::from("third_party/sqllogictest/test");
    if !test_dir.exists() {
        panic!(
            "SQLLogicTest submodule not initialized. Run:\n  git submodule update --init --recursive"
        );
    }

    let (results, total_available_files) = run_test_suite();

    // Print summary
    println!("\n=== Test Results Summary ===");
    println!(
        "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>10}",
        "Category", "Total", "Passed", "Failed", "Errors", "Skipped", "Pass Rate"
    );
    println!("{}", "-".repeat(80));

    let mut grand_total = TestStats::default();
    let mut all_tested_files = HashSet::new();

    for category in ["select", "evidence", "index", "random", "ddl", "other"] {
        if let Some(stats) = results.get(category) {
            println!(
                "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>9.1}%",
                category,
                stats.total,
                stats.passed,
                stats.failed,
                stats.errors,
                stats.skipped,
                stats.pass_rate()
            );
            grand_total.total += stats.total;
            grand_total.passed += stats.passed;
            grand_total.failed += stats.failed;
            grand_total.errors += stats.errors;
            grand_total.skipped += stats.skipped;
            all_tested_files.extend(stats.tested_files.clone());
        }
    }

    println!("{}", "-".repeat(80));
    println!(
        "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>9.1}%",
        "TOTAL",
        grand_total.total,
        grand_total.passed,
        grand_total.failed,
        grand_total.errors,
        grand_total.skipped,
        grand_total.pass_rate()
    );

    println!(
        "\nNote: This test suite runs ~5.9 million test cases across {} files.",
        total_available_files
    );
    println!("Some failures are expected as we continue implementing SQL:1999 features.");

    // Write results to JSON file for CI/badge generation
    let tested_files_vec: Vec<String> = all_tested_files.into_iter().collect();

    // Collect all detailed failures across categories
    let mut all_detailed_failures: Vec<(String, Vec<TestFailure>)> = Vec::new();
    for stats in results.values() {
        all_detailed_failures.extend(stats.detailed_failures.clone());
    }

    let results_json = serde_json::json!({
        "summary": {
            "total": grand_total.total,
            "passed": grand_total.passed,
            "failed": grand_total.failed,
            "errors": grand_total.errors,
            "skipped": grand_total.skipped,
            "pass_rate": grand_total.pass_rate(),
            "total_available_files": total_available_files,
            "tested_files": tested_files_vec.len(),
        },
        "tested_files": {
            "passed": results.values().flat_map(|s| &s.tested_files).filter(|f| {
                // Check if this file passed (not in detailed_failures)
                !all_detailed_failures.iter().any(|(path, _)| path == *f)
            }).collect::<Vec<_>>(),
            "failed": results.values().flat_map(|s| &s.tested_files).filter(|f| {
                // Check if this file failed (in detailed_failures)
                all_detailed_failures.iter().any(|(path, _)| path == *f)
            }).collect::<Vec<_>>(),
        },
        "categories": {
            "select": results.get("select").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "evidence": results.get("evidence").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "index": results.get("index").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "random": results.get("random").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "ddl": results.get("ddl").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
            "other": results.get("other").map(|s| serde_json::json!({
                "total": s.total,
                "passed": s.passed,
                "failed": s.failed,
                "errors": s.errors,
                "skipped": s.skipped,
                "pass_rate": s.pass_rate()
            })),
        },
        "detailed_failures": all_detailed_failures.iter().map(|(file_path, failures)| {
            serde_json::json!({
                "file_path": file_path,
                "failures": failures.iter().map(|f| {
                    serde_json::json!({
                        "sql_statement": f.sql_statement,
                        "expected_result": f.expected_result,
                        "actual_result": f.actual_result,
                        "error_message": f.error_message,
                        "line_number": f.line_number
                    })
                }).collect::<Vec<_>>()
            })
        }).collect::<Vec<_>>()
    });

    // Ensure target directory exists
    fs::create_dir_all("target").ok();

    // Determine output filename - use worker-specific name if in parallel mode
    let output_file = if let Ok(worker_id) = env::var("SQLLOGICTEST_WORKER_ID") {
        format!("target/sqllogictest_results_worker_{}.json", worker_id)
    } else {
        "target/sqllogictest_results.json".to_string()
    };

    // Write JSON results
    if let Ok(mut file) = fs::File::create(&output_file) {
        let _ = file.write_all(serde_json::to_string_pretty(&results_json).unwrap().as_bytes());
        println!("\n✓ Results written to {}", output_file);
    }
}

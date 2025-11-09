//! Comprehensive SQLLogicTest suite runner using the dolthub/sqllogictest submodule.
//!
//! This test suite runs ~5.9 million SQL tests from the official SQLLogicTest corpus.
//! Tests are randomly selected each run to progressively build coverage over time.
//! Results are merged with historical data to track: tested/passed, tested/failed, not-yet-tested.
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
    time::{Duration, Instant},
};

use sqllogictest::{
    execution::{run_test_file_with_details, TestError},
    stats::{TestFailure, TestStats},
    work_queue::WorkQueue,
};

/// Run SQLLogicTest files from the submodule (prioritized by failure history, then randomly
/// selected with time budget)
fn run_test_suite() -> (HashMap<String, TestStats>, usize) {
    let test_dir = PathBuf::from("third_party/sqllogictest/test");
    let mut results = HashMap::new();

    // Get time budget from environment (default: 5 minutes = 300 seconds)
    let time_budget_secs: u64 =
        env::var("SQLLOGICTEST_TIME_BUDGET").ok().and_then(|s| s.parse().ok()).unwrap_or(300);
    let time_budget = Duration::from_secs(time_budget_secs);
    let start_time = Instant::now();

    // Find all .test files
    let pattern = format!("{}/**/*.test", test_dir.display());
    let all_test_files: Vec<PathBuf> =
        glob::glob(&pattern).expect("Failed to read test pattern").filter_map(Result::ok).collect();

    let total_available_files = all_test_files.len();

    // Work queue mode: Claim files dynamically until queue is empty
    println!("\n=== SQLLogicTest Suite (Work Queue Mode) ===");
    println!("Total available test files: {}", total_available_files);
    println!("Time budget: {} seconds", time_budget_secs);
    println!("Starting test run...\n");

    let work_queue = WorkQueue::from_env().expect("Failed to initialize work queue");
    let mut files_tested = 0;

    loop {
        // Check time budget
        if start_time.elapsed() >= time_budget {
            println!(
                "\n⏱️  Time budget exhausted after {:.1} seconds",
                start_time.elapsed().as_secs_f64()
            );
            println!("Files tested: {}", files_tested);
            println!("Files remaining in queue: {}", work_queue.pending_count());
            println!("Files completed: {}\n", work_queue.completed_count());
            return (results, total_available_files);
        }

        // Try to claim next file from work queue (returns relative path)
        let Some(rel_path_buf) = work_queue.claim_next_file() else {
            println!(
                "\n✅ Work queue empty! All {} files have been tested.",
                total_available_files
            );
            println!("Total time: {:.1} seconds", start_time.elapsed().as_secs_f64());
            println!("Files tested: {}\n", files_tested);
            return (results, total_available_files);
        };

        files_tested += 1;
        let relative_path = rel_path_buf.to_string_lossy().to_string();
        // Prepend test_dir to get absolute path
        let test_file = test_dir.join(&rel_path_buf);

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
                if !detailed_failures.is_empty() {
                    stats.detailed_failures.push((relative_path.clone(), detailed_failures));
                }
            }
            Err(TestError::Execution(e)) => {
                eprintln!("✗ {} - {}", relative_path, e);
                stats.failed += 1;
                if !detailed_failures.is_empty() {
                    stats.detailed_failures.push((relative_path.clone(), detailed_failures));
                }
            }
        }
    }
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
        "\nNote: This test suite randomly samples from ~5.9 million test cases across {} files.",
        total_available_files
    );
    println!("Results from multiple CI runs are merged to progressively build complete coverage.");
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

    // Write JSON results
    if let Ok(mut file) = fs::File::create("target/sqllogictest_results.json") {
        let _ = file.write_all(serde_json::to_string_pretty(&results_json).unwrap().as_bytes());
        println!("\n✓ Results written to target/sqllogictest_results.json");
    }
}

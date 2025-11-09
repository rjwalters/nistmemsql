-- ============================================================================
-- SQLLogicTest Results Schema
-- ============================================================================
--
-- Database schema for storing VibeSQL SQLLogicTest conformance test results.
-- This schema supports tracking test execution history, analyzing failure
-- patterns, and monitoring progress over time.
--
-- Part of the dogfooding initiative: VibeSQL stores its own test results
-- in VibeSQL, demonstrating real-world database usage.
--
-- Related: docs/planning/PERSISTENCE_AND_DOGFOODING.md

-- ============================================================================
-- Table: test_files
-- ============================================================================
--
-- Tracks the current status of each SQLLogicTest file.
-- Each file represents a test suite covering specific SQL functionality.
--
-- Design decisions:
-- - file_path is PRIMARY KEY (natural key, stable identifier)
-- - category/subcategory enable grouping and filtering
-- - status tracks current state: 'PASS', 'FAIL', 'UNTESTED'
-- - last_tested and last_passed track temporal state changes

CREATE TABLE test_files (
    file_path VARCHAR(500) PRIMARY KEY,
    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50),
    status VARCHAR(20) NOT NULL,  -- 'PASS', 'FAIL', 'UNTESTED'
    last_tested TIMESTAMP,
    last_passed TIMESTAMP
);

-- ============================================================================
-- Table: test_runs
-- ============================================================================
--
-- Metadata for each test execution run.
-- Enables tracking progress over time and correlating results with code changes.
--
-- Design decisions:
-- - run_id is surrogate PRIMARY KEY (enables efficient joins)
-- - completed_at can be NULL for in-progress runs
-- - git_commit links results to specific code versions
-- - ci_run_id enables correlation with CI/CD systems

CREATE TABLE test_runs (
    run_id INTEGER PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    total_files INTEGER,
    passed INTEGER,
    failed INTEGER,
    untested INTEGER,
    git_commit VARCHAR(40),
    ci_run_id VARCHAR(100)
);

-- ============================================================================
-- Table: test_results
-- ============================================================================
--
-- Individual test execution results for each file in each run.
-- Provides detailed history for failure pattern analysis.
--
-- Design decisions:
-- - result_id is surrogate PRIMARY KEY
-- - Foreign keys enforce referential integrity
-- - duration_ms enables performance tracking
-- - error_message stores failure details (VARCHAR(2000) may truncate very long errors)

CREATE TABLE test_results (
    result_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    file_path VARCHAR(500) NOT NULL,
    status VARCHAR(20) NOT NULL,
    tested_at TIMESTAMP NOT NULL,
    duration_ms INTEGER,
    error_message VARCHAR(2000),
    FOREIGN KEY (run_id) REFERENCES test_runs(run_id),
    FOREIGN KEY (file_path) REFERENCES test_files(file_path)
);

-- ============================================================================
-- Example Queries
-- ============================================================================
--
-- These queries demonstrate the schema's analytical capabilities and validate
-- that the design supports the dogfooding use case.

-- Query 1: Current status summary by category
-- Shows overall test coverage and pass rate per category
--
-- Example output:
--   category | total | passed | failed | untested
--   ---------|-------|--------|--------|----------
--   select   | 150   | 120    | 25     | 5
--   join     | 80    | 75     | 5      | 0
--
-- SELECT
--     category,
--     COUNT(*) as total,
--     SUM(CASE WHEN status='PASS' THEN 1 ELSE 0 END) as passed,
--     SUM(CASE WHEN status='FAIL' THEN 1 ELSE 0 END) as failed,
--     SUM(CASE WHEN status='UNTESTED' THEN 1 ELSE 0 END) as untested
-- FROM test_files
-- GROUP BY category
-- ORDER BY category;

-- Query 2: Progress over time
-- Tracks test pass rate trends across recent runs
--
-- Example output:
--   date       | passed | failed | pass_rate
--   -----------|--------|--------|----------
--   2025-01-15 | 205    | 30     | 87.2
--   2025-01-14 | 200    | 35     | 85.1
--
-- SELECT
--     DATE(completed_at) as date,
--     passed,
--     failed,
--     ROUND(100.0 * passed / total_files, 1) as pass_rate
-- FROM test_runs
-- WHERE completed_at IS NOT NULL
-- ORDER BY completed_at DESC
-- LIMIT 30;

-- Query 3: Most problematic files
-- Identifies test files with frequent failures for investigation
--
-- Example output:
--   file_path              | failure_count
--   -----------------------|--------------
--   select/aggregates.test | 15
--   join/outer.test        | 12
--
-- SELECT
--     file_path,
--     COUNT(*) as failure_count
-- FROM test_results
-- WHERE status = 'FAIL'
-- GROUP BY file_path
-- HAVING COUNT(*) > 5
-- ORDER BY failure_count DESC
-- LIMIT 20;

-- Query 4: Recent failures with details
-- Shows the latest test failures with error messages for debugging
--
-- SELECT
--     tf.file_path,
--     tf.category,
--     tr.error_message,
--     tr.tested_at
-- FROM test_results tr
-- JOIN test_files tf ON tr.file_path = tf.file_path
-- WHERE tr.status = 'FAIL'
-- ORDER BY tr.tested_at DESC
-- LIMIT 50;

-- Query 5: Test execution performance
-- Identifies slow-running tests that may need optimization
--
-- SELECT
--     file_path,
--     AVG(duration_ms) as avg_duration,
--     MAX(duration_ms) as max_duration,
--     COUNT(*) as run_count
-- FROM test_results
-- WHERE duration_ms IS NOT NULL
-- GROUP BY file_path
-- HAVING AVG(duration_ms) > 1000
-- ORDER BY avg_duration DESC
-- LIMIT 20;

-- ============================================================================
-- Usage Notes
-- ============================================================================
--
-- Loading this schema:
--   1. Execute this entire file as a SQL script
--   2. Tables will be created in order (test_runs, test_files, test_results)
--   3. Foreign key constraints will be enforced
--
-- Populating data:
--   See scripts/generate_punchlist.py for Python integration
--   See tests/test_results_schema.rs for Rust usage examples
--
-- Exporting data:
--   Use Database::save_sql_dump() to export as portable SQL
--   Dump can be loaded into web demo for live querying
--
-- Schema evolution:
--   For now, schema is immutable (recreate from scratch each time)
--   Future: Add migration support when schema stabilizes

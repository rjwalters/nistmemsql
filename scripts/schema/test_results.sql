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

-- ============================================================================
-- Analysis Views for Failure Pattern Detection
-- ============================================================================
--
-- These views power the `./scripts/sqllogictest analyze` command to help
-- prioritize which test failures to fix first based on impact and effort.
--
-- Views:
-- - latest_run_summary: Summary of most recent test run
-- - failure_patterns: Groups failures by error type for pattern analysis
-- - fix_opportunities: Prioritizes fixes by impact ratio (tests affected / effort)
-- - failure_examples: Shows example failures for each pattern

-- View: Latest test run summary
-- Shows summary statistics for the most recent test run
CREATE VIEW latest_run_summary AS
SELECT
    run_id,
    started_at,
    completed_at,
    total_files,
    passed,
    failed,
    untested,
    ROUND(100.0 * passed / total_files, 1) as pass_rate,
    git_commit
FROM test_runs
ORDER BY run_id DESC
LIMIT 1;

-- View: Failure patterns grouped by error type
-- Analyzes error messages to identify common failure patterns
CREATE VIEW failure_patterns AS
SELECT
    -- Extract error type from error message
    CASE
        WHEN tr.error_message LIKE '%query result mismatch%' THEN 'Result Mismatch'
        WHEN tr.error_message LIKE '%ColumnNotFound%' THEN 'Column Not Found'
        WHEN tr.error_message LIKE '%TypeMismatch%' THEN 'Type Mismatch'
        WHEN tr.error_message LIKE '%Parse%Error%' THEN 'Parse Error'
        WHEN tr.error_message LIKE '%UnsupportedExpression%' THEN 'Unsupported Expression'
        WHEN tr.error_message LIKE '%DivisionByZero%' THEN 'Division By Zero'
        WHEN tr.error_message LIKE '%Not implemented%' THEN 'Not Implemented'
        WHEN tr.error_message LIKE '%InvalidLine%' THEN 'Invalid Line'
        ELSE 'Other'
    END as error_type,
    COUNT(*) as failure_count,
    COUNT(DISTINCT tr.file_path) as affected_files,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM test_results WHERE run_id = tr.run_id AND status = 'FAIL'), 1) as pct_of_failures,
    -- Extract specific error patterns
    CASE
        WHEN tr.error_message LIKE '%query result mismatch%' AND tr.error_message LIKE '%.000%' THEN 'Decimal formatting (N.000 vs N)'
        WHEN tr.error_message LIKE '%ColumnNotFound%' AND tr.error_message LIKE '%COL%' THEN 'Case-sensitive column resolution'
        WHEN tr.error_message LIKE '%TypeMismatch%' AND tr.error_message LIKE '%NOT%' THEN 'NOT NULL type mismatch'
        WHEN tr.error_message LIKE '%Parse%Error%' THEN 'SQL parse error'
        WHEN tr.error_message LIKE '%UnsupportedExpression%NULLIF%' THEN 'Missing function: NULLIF'
        WHEN tr.error_message LIKE '%UnsupportedExpression%COALESCE%' THEN 'Missing function: COALESCE'
        WHEN tr.error_message LIKE '%DivisionByZero%' THEN 'Division by zero'
        WHEN tr.error_message LIKE '%Not implemented%' THEN 'Feature not implemented'
        WHEN tr.error_message LIKE '%InvalidLine%onlyif%' THEN 'onlyif directive'
        WHEN tr.error_message LIKE '%InvalidLine%skipif%' THEN 'skipif directive'
        ELSE 'Other error'
    END as error_pattern,
    tr.run_id
FROM test_results tr
WHERE tr.status = 'FAIL'
GROUP BY tr.run_id, error_type, error_pattern
ORDER BY failure_count DESC;

-- View: High-impact fix opportunities
-- Prioritizes fixes based on impact ratio (failures fixed per unit of effort)
CREATE VIEW fix_opportunities AS
SELECT
    ROW_NUMBER() OVER (ORDER BY (failure_count * 1.0 / effort_score) DESC) as rank,
    error_pattern as pattern,
    failure_count as tests_affected,
    pct_of_failures as pct_failures,
    effort,
    ROUND(failure_count * 1.0 / effort_score, 1) as impact_ratio,
    CASE
        WHEN ROW_NUMBER() OVER (ORDER BY (failure_count * 1.0 / effort_score) DESC) <= 3 THEN 'P0'
        WHEN ROW_NUMBER() OVER (ORDER BY (failure_count * 1.0 / effort_score) DESC) <= 7 THEN 'P1'
        ELSE 'P2'
    END as priority
FROM (
    SELECT
        error_pattern,
        SUM(failure_count) as failure_count,
        ROUND(AVG(pct_of_failures), 1) as pct_of_failures,
        -- Effort estimation based on pattern
        CASE
            WHEN error_pattern LIKE '%formatting%' THEN 'Low'
            WHEN error_pattern LIKE '%Missing function%' THEN 'Medium'
            WHEN error_pattern LIKE '%Case-sensitive%' THEN 'Medium'
            WHEN error_pattern LIKE '%directive%' THEN 'Medium'
            WHEN error_pattern LIKE '%parse error%' THEN 'High'
            WHEN error_pattern LIKE '%type mismatch%' THEN 'High'
            ELSE 'Medium'
        END as effort,
        CASE
            WHEN error_pattern LIKE '%formatting%' THEN 1
            WHEN error_pattern LIKE '%Missing function%' THEN 2
            WHEN error_pattern LIKE '%Case-sensitive%' THEN 2
            WHEN error_pattern LIKE '%directive%' THEN 2
            WHEN error_pattern LIKE '%parse error%' THEN 3
            WHEN error_pattern LIKE '%type mismatch%' THEN 3
            ELSE 2
        END as effort_score
    FROM failure_patterns
    WHERE error_pattern != 'Other error'
    GROUP BY error_pattern
) AS grouped
ORDER BY impact_ratio DESC
LIMIT 10;

-- View: Example failures for each pattern
-- Shows sample test files and error messages for each failure pattern
CREATE VIEW failure_examples AS
SELECT
    error_pattern,
    file_path,
    error_type,
    SUBSTR(error_message, 1, 150) as error_message_preview
FROM (
    SELECT
        CASE
            WHEN tr.error_message LIKE '%query result mismatch%' AND tr.error_message LIKE '%.000%' THEN 'Decimal formatting (N.000 vs N)'
            WHEN tr.error_message LIKE '%ColumnNotFound%' AND tr.error_message LIKE '%COL%' THEN 'Case-sensitive column resolution'
            WHEN tr.error_message LIKE '%TypeMismatch%' AND tr.error_message LIKE '%NOT%' THEN 'NOT NULL type mismatch'
            WHEN tr.error_message LIKE '%Parse%Error%' THEN 'SQL parse error'
            WHEN tr.error_message LIKE '%UnsupportedExpression%NULLIF%' THEN 'Missing function: NULLIF'
            WHEN tr.error_message LIKE '%UnsupportedExpression%COALESCE%' THEN 'Missing function: COALESCE'
            WHEN tr.error_message LIKE '%DivisionByZero%' THEN 'Division by zero'
            WHEN tr.error_message LIKE '%Not implemented%' THEN 'Feature not implemented'
            WHEN tr.error_message LIKE '%InvalidLine%onlyif%' THEN 'onlyif directive'
            WHEN tr.error_message LIKE '%InvalidLine%skipif%' THEN 'skipif directive'
            ELSE 'Other error'
        END as error_pattern,
        CASE
            WHEN tr.error_message LIKE '%query result mismatch%' THEN 'Result Mismatch'
            WHEN tr.error_message LIKE '%ColumnNotFound%' THEN 'Column Not Found'
            WHEN tr.error_message LIKE '%TypeMismatch%' THEN 'Type Mismatch'
            WHEN tr.error_message LIKE '%Parse%Error%' THEN 'Parse Error'
            WHEN tr.error_message LIKE '%UnsupportedExpression%' THEN 'Unsupported Expression'
            WHEN tr.error_message LIKE '%DivisionByZero%' THEN 'Division By Zero'
            WHEN tr.error_message LIKE '%Not implemented%' THEN 'Not Implemented'
            WHEN tr.error_message LIKE '%InvalidLine%' THEN 'Invalid Line'
            ELSE 'Other'
        END as error_type,
        tr.file_path,
        tr.error_message,
        ROW_NUMBER() OVER (PARTITION BY
            CASE
                WHEN tr.error_message LIKE '%query result mismatch%' AND tr.error_message LIKE '%.000%' THEN 'Decimal formatting (N.000 vs N)'
                WHEN tr.error_message LIKE '%ColumnNotFound%' AND tr.error_message LIKE '%COL%' THEN 'Case-sensitive column resolution'
                WHEN tr.error_message LIKE '%TypeMismatch%' AND tr.error_message LIKE '%NOT%' THEN 'NOT NULL type mismatch'
                WHEN tr.error_message LIKE '%Parse%Error%' THEN 'SQL parse error'
                WHEN tr.error_message LIKE '%UnsupportedExpression%NULLIF%' THEN 'Missing function: NULLIF'
                WHEN tr.error_message LIKE '%UnsupportedExpression%COALESCE%' THEN 'Missing function: COALESCE'
                WHEN tr.error_message LIKE '%DivisionByZero%' THEN 'Division by zero'
                WHEN tr.error_message LIKE '%Not implemented%' THEN 'Feature not implemented'
                WHEN tr.error_message LIKE '%InvalidLine%onlyif%' THEN 'onlyif directive'
                WHEN tr.error_message LIKE '%InvalidLine%skipif%' THEN 'skipif directive'
                ELSE 'Other error'
            END
        ORDER BY tr.file_path) as rn
    FROM test_results tr
    WHERE tr.status = 'FAIL'
    AND tr.run_id = (SELECT MAX(run_id) FROM test_runs)
) AS ranked
WHERE rn <= 3;

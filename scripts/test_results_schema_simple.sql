-- SQLLogicTest Results Database Schema (Simplified for vibesql dogfooding)
-- Using only currently supported vibesql features

-- Test Runs: Metadata about each parallel test execution
CREATE TABLE test_runs (
    run_id INTEGER,
    timestamp TEXT,
    workers INTEGER,
    time_budget INTEGER,
    seed INTEGER,
    total_files INTEGER,
    passed_files INTEGER,
    failed_files INTEGER,
    pass_rate REAL
);

-- Test Results: Individual test file outcomes
CREATE TABLE test_results (
    result_id INTEGER,
    run_id INTEGER,
    file_path TEXT,
    status TEXT,
    error_type TEXT,
    error_message TEXT,
    execution_time_ms INTEGER,
    worker_id INTEGER
);

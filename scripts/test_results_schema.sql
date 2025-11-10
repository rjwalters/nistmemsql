-- SQLLogicTest Results Database Schema
-- Dogfooding vibesql for test result analysis and reporting

-- Test Runs: Metadata about each parallel test execution
CREATE TABLE test_runs (
    run_id INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL,
    workers INTEGER NOT NULL,
    time_budget INTEGER NOT NULL,
    seed INTEGER NOT NULL,
    total_files INTEGER,
    passed_files INTEGER,
    failed_files INTEGER,
    pass_rate REAL
);

-- Test Results: Individual test file outcomes
CREATE TABLE test_results (
    result_id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('passed', 'failed', 'skipped', 'timeout')),
    error_type TEXT,
    error_message TEXT,
    execution_time_ms INTEGER,
    worker_id INTEGER,
    FOREIGN KEY (run_id) REFERENCES test_runs(run_id)
);

-- Indexes for efficient querying
CREATE INDEX idx_test_results_run ON test_results(run_id);
CREATE INDEX idx_test_results_status ON test_results(status);
CREATE INDEX idx_test_results_error_type ON test_results(error_type);
CREATE INDEX idx_test_results_file ON test_results(file_path);

-- View: Latest test run summary
CREATE VIEW latest_run_summary AS
SELECT
    run_id,
    timestamp,
    workers,
    total_files,
    passed_files,
    failed_files,
    pass_rate,
    printf('%.1f%%', pass_rate) as pass_rate_formatted
FROM test_runs
ORDER BY run_id DESC
LIMIT 1;

-- View: Failure patterns grouped by error type
CREATE VIEW failure_patterns AS
SELECT
    tr.error_type,
    COUNT(*) as failure_count,
    COUNT(DISTINCT tr.file_path) as affected_files,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM test_results WHERE run_id = tr.run_id AND status = 'failed'), 1) as pct_of_failures,
    -- Extract common error patterns
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
WHERE tr.status = 'failed'
GROUP BY tr.run_id, tr.error_type, error_pattern
ORDER BY failure_count DESC;

-- View: High-impact fix opportunities
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
) grouped
ORDER BY impact_ratio DESC
LIMIT 10;

-- View: Example failures for each pattern
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
        tr.file_path,
        tr.error_type,
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
    WHERE tr.status = 'failed'
    AND tr.run_id = (SELECT MAX(run_id) FROM test_runs)
) ranked
WHERE rn <= 3;

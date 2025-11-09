-- SQLLogicTest Conformance Results
-- Mock data for development (Issue #996 will generate real data)

-- Test files tracking table
CREATE TABLE test_files (
    file_path VARCHAR(500),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    status VARCHAR(20),
    last_tested VARCHAR(30),
    last_passed VARCHAR(30)
);

-- Test runs metadata table
CREATE TABLE test_runs (
    run_id INTEGER,
    started_at VARCHAR(30),
    completed_at VARCHAR(30),
    total_files INTEGER,
    passed INTEGER,
    failed INTEGER,
    untested INTEGER,
    git_commit VARCHAR(40),
    ci_run_id VARCHAR(100)
);

-- Individual test results table
CREATE TABLE test_results (
    result_id INTEGER,
    run_id INTEGER,
    file_path VARCHAR(500),
    status VARCHAR(20),
    tested_at VARCHAR(30),
    duration_ms INTEGER,
    error_message VARCHAR(2000)
);

-- Sample test files data
INSERT INTO test_files VALUES ('test/select1.test', 'select', 'basic', 'PASS', '2025-01-08 10:00:00', '2025-01-08 10:00:00');
INSERT INTO test_files VALUES ('test/select2.test', 'select', 'aggregates', 'PASS', '2025-01-08 10:00:01', '2025-01-08 10:00:01');
INSERT INTO test_files VALUES ('test/select3.test', 'select', 'joins', 'PASS', '2025-01-08 10:00:02', '2025-01-08 10:00:02');
INSERT INTO test_files VALUES ('test/select4.test', 'select', 'subqueries', 'FAIL', '2025-01-08 10:00:03', '2025-01-05 09:30:00');
INSERT INTO test_files VALUES ('test/index/1.test', 'index', 'btree', 'FAIL', '2025-01-08 10:00:04', '2025-01-01 08:00:00');
INSERT INTO test_files VALUES ('test/index/2.test', 'index', 'hash', 'FAIL', '2025-01-08 10:00:05', NULL);
INSERT INTO test_files VALUES ('test/evidence/join1.test', 'evidence', 'inner_join', 'PASS', '2025-01-08 10:00:06', '2025-01-08 10:00:06');
INSERT INTO test_files VALUES ('test/evidence/join2.test', 'evidence', 'left_join', 'PASS', '2025-01-08 10:00:07', '2025-01-08 10:00:07');
INSERT INTO test_files VALUES ('test/evidence/agg1.test', 'evidence', 'aggregates', 'PASS', '2025-01-08 10:00:08', '2025-01-08 10:00:08');
INSERT INTO test_files VALUES ('test/random/1.test', 'random', 'mixed', 'PASS', '2025-01-08 10:00:09', '2025-01-08 10:00:09');
INSERT INTO test_files VALUES ('test/random/2.test', 'random', 'mixed', 'FAIL', '2025-01-08 10:00:10', '2025-01-07 15:00:00');
INSERT INTO test_files VALUES ('test/random/3.test', 'random', 'mixed', 'PASS', '2025-01-08 10:00:11', '2025-01-08 10:00:11');

-- Sample test runs data
INSERT INTO test_runs VALUES (1, '2025-01-08 10:00:00', '2025-01-08 10:05:00', 12, 9, 3, 0, 'abc123def456', 'ci-run-001');
INSERT INTO test_runs VALUES (2, '2025-01-07 15:00:00', '2025-01-07 15:04:30', 12, 8, 4, 0, 'def789ghi012', 'ci-run-002');
INSERT INTO test_runs VALUES (3, '2025-01-06 09:00:00', '2025-01-06 09:05:15', 12, 7, 5, 0, 'ghi345jkl678', 'ci-run-003');

-- Sample test results data
INSERT INTO test_results VALUES (1, 1, 'test/select1.test', 'PASS', '2025-01-08 10:00:00', 45, NULL);
INSERT INTO test_results VALUES (2, 1, 'test/select2.test', 'PASS', '2025-01-08 10:00:01', 52, NULL);
INSERT INTO test_results VALUES (3, 1, 'test/select3.test', 'PASS', '2025-01-08 10:00:02', 68, NULL);
INSERT INTO test_results VALUES (4, 1, 'test/select4.test', 'FAIL', '2025-01-08 10:00:03', 120, 'Subquery not supported');
INSERT INTO test_results VALUES (5, 1, 'test/index/1.test', 'FAIL', '2025-01-08 10:00:04', 89, 'Index creation failed');
INSERT INTO test_results VALUES (6, 1, 'test/index/2.test', 'FAIL', '2025-01-08 10:00:05', 95, 'Hash index not implemented');
INSERT INTO test_results VALUES (7, 1, 'test/evidence/join1.test', 'PASS', '2025-01-08 10:00:06', 78, NULL);
INSERT INTO test_results VALUES (8, 1, 'test/evidence/join2.test', 'PASS', '2025-01-08 10:00:07', 82, NULL);
INSERT INTO test_results VALUES (9, 1, 'test/evidence/agg1.test', 'PASS', '2025-01-08 10:00:08', 56, NULL);
INSERT INTO test_results VALUES (10, 1, 'test/random/1.test', 'PASS', '2025-01-08 10:00:09', 44, NULL);
INSERT INTO test_results VALUES (11, 1, 'test/random/2.test', 'FAIL', '2025-01-08 10:00:10', 110, 'Parser error');
INSERT INTO test_results VALUES (12, 1, 'test/random/3.test', 'PASS', '2025-01-08 10:00:11', 48, NULL);

-- Test to verify the index optimization fix
-- 1. Binary WHERE with multi-column index should NOT use index (fall back to table scan)
-- 2. IN clause with multi-column index SHOULD use index

-- Create test table
CREATE TABLE test_table (
    pk INTEGER PRIMARY KEY,
    col0 INTEGER,
    col1 INTEGER,
    col2 INTEGER
);

-- Create multi-column index
CREATE INDEX idx_multi ON test_table (col0, col1, col2);

-- Insert test data
INSERT INTO test_table VALUES (1, 100, 200, 300);
INSERT INTO test_table VALUES (2, 100, 250, 350);
INSERT INTO test_table VALUES (3, 150, 200, 300);
INSERT INTO test_table VALUES (4, 200, 200, 300);
INSERT INTO test_table VALUES (5, 200, 250, 350);

-- Test 1: Binary WHERE with multi-column index
-- Should return rows 1 and 2 (col0 = 100)
-- This should work correctly by falling back to table scan
SELECT pk FROM test_table WHERE col0 = 100 ORDER BY pk;

-- Test 2: IN clause with multi-column index
-- Should return rows 1, 2, 3 (col0 IN (100, 150))
-- This should use the multi-column index (prefix matching)
SELECT pk FROM test_table WHERE col0 IN (100, 150) ORDER BY pk;

-- Test 3: ORDER BY should work correctly
SELECT pk FROM test_table WHERE col0 = 200 ORDER BY pk DESC;

-- Test 4: IN with ORDER BY
SELECT pk FROM test_table WHERE col0 IN (100, 200) ORDER BY pk;

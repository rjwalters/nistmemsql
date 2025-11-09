-- Test Phase 3.2.2 WHERE clause equijoin integration
-- This verifies that WHERE clause equijoins are properly detected and used for hash joins

-- Create test tables
CREATE TABLE t1 (id INTEGER, name TEXT);
CREATE TABLE t2 (id INTEGER, user_id INTEGER, value TEXT);
CREATE TABLE t3 (id INTEGER, t2_id INTEGER, data TEXT);

-- Insert test data
INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO t2 VALUES (100, 1, 'val1'), (101, 2, 'val2'), (102, 3, 'val3');
INSERT INTO t3 VALUES (1000, 100, 'data1'), (1001, 101, 'data2'), (1002, 102, 'data3');

-- Test 1: Simple WHERE equijoin
SELECT t1.id, t2.id 
FROM t1, t2 
WHERE t1.id = t2.user_id
ORDER BY t1.id;

-- Expected: 3 rows (each t1 joins with its corresponding t2)

-- Test 2: Multi-table WHERE equijoins
SELECT t1.id, t2.id, t3.id
FROM t1, t2, t3
WHERE t1.id = t2.user_id
  AND t2.id = t3.t2_id
ORDER BY t1.id;

-- Expected: 3 rows (t1 -> t2 -> t3 chain)

-- Test 3: WHERE equijoin with local predicate
SELECT t1.id, t2.id
FROM t1, t2
WHERE t1.id > 1
  AND t1.id = t2.user_id
ORDER BY t1.id;

-- Expected: 2 rows (Bob and Charlie)

-- Test 4: Mixed ON and WHERE conditions (ON takes precedence)
SELECT t1.id, t2.id
FROM t1 INNER JOIN t2 ON t1.id = t2.user_id
WHERE t1.id > 1
ORDER BY t1.id;

-- Expected: 2 rows (Bob and Charlie)

-- Clean up
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

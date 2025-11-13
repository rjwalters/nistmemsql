-- Minimal test case for index scan bug
CREATE TABLE tab1 (pk INTEGER PRIMARY KEY, col0 INTEGER, col3 INTEGER);
INSERT INTO tab1 VALUES (0, 5, 1);
INSERT INTO tab1 VALUES (1, 719, 5);
INSERT INTO tab1 VALUES (2, 720, 10);

-- Create index on col0
CREATE INDEX idx_col0 ON tab1(col0);

-- Test 1: Simple range query (should return 2 rows: pk=1, pk=2)
SELECT pk FROM tab1 WHERE col0 > 718;

-- Test 2: Simple equality (should return 1 row: pk=2)
SELECT pk FROM tab1 WHERE col3 > 2;

-- Test 3: BETWEEN (should return 1 row: pk=0)
SELECT pk FROM tab1 WHERE col0 >= 2 AND col0 <= 8;

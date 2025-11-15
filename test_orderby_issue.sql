-- Test case to reproduce index/orderby failures
-- Based on error patterns from issue #1838

CREATE TABLE tab1 (
    pk INTEGER PRIMARY KEY,
    col0 INTEGER,
    col1 REAL,
    col3 INTEGER
);

CREATE INDEX idx_col0 ON tab1(col0);
CREATE INDEX idx_col1 ON tab1(col1);
CREATE INDEX idx_col3 ON tab1(col3);

-- Insert some test data
INSERT INTO tab1 VALUES (1, 100, 500.0, 200);
INSERT INTO tab1 VALUES (2, 150, 600.0, 250);
INSERT INTO tab1 VALUES (3, 200, 700.0, 300);
INSERT INTO tab1 VALUES (4, 250, 800.0, 350);
INSERT INTO tab1 VALUES (5, 300, 900.0, 400);

-- Test queries from the issue examples
-- These should return rows in correct order

-- Test 1: col3 > 221 ORDER BY pk DESC
SELECT pk FROM tab1 WHERE col3 > 221 ORDER BY 1 DESC;
-- Expected: 5, 4, 3, 2 (in descending order)

-- Test 2: col1 <= 857.86 ORDER BY pk DESC
SELECT pk FROM tab1 WHERE col1 <= 857.86 ORDER BY 1 DESC;
-- Expected: 5, 4, 3, 2, 1 (in descending order)

-- Test 3: col0 >= 214 ORDER BY pk DESC
SELECT pk FROM tab1 WHERE col0 >= 150 ORDER BY 1 DESC;
-- Expected: 5, 4, 3, 2 (in descending order)

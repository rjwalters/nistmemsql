-- Mimics the exact pattern from the failing sqllogictest
-- 1. Create empty table
CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER);

-- 2. Create index on EMPTY table (this is the key!)
CREATE INDEX idx_col0 ON tab1 (col0);

-- 3. Create source table with data
CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER);
INSERT INTO tab0 VALUES (1, 100);
INSERT INTO tab0 VALUES (2, 200);
INSERT INTO tab0 VALUES (3, 300);
INSERT INTO tab0 VALUES (4, 400);
INSERT INTO tab0 VALUES (5, 500);

-- 4. Insert data into tab1 via INSERT...SELECT (AFTER index was created)
INSERT INTO tab1 SELECT * FROM tab0;

-- 5. Query using the index (should return 3 rows: 300, 400, 500)
SELECT pk FROM tab1 WHERE col0 > 250;

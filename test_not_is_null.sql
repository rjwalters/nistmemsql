-- Test case for NOT ... IS NULL parsing issue
-- Expected: NOT (col0 IS NULL)
-- Current: (NOT col0) IS NULL

CREATE TABLE tab0 (col0 INTEGER);
INSERT INTO tab0 VALUES (99);
INSERT INTO tab0 VALUES (100);
INSERT INTO tab0 VALUES (NULL);

-- This should return rows where col0 IS NULL evaluates to FALSE (i.e., non-NULL rows)
-- NOT (FALSE) = TRUE for rows 1 and 2
-- NOT (TRUE) = FALSE for row 3
SELECT * FROM tab0 WHERE NOT col0 IS NULL;
-- Expected: 2 rows (99, 100)

-- This should be equivalent
SELECT * FROM tab0 WHERE col0 IS NOT NULL;
-- Expected: 2 rows (99, 100)

-- Test case for Issue #939: IS NULL with unary operators

-- Setup test table
CREATE TABLE tab2 (col0 INTEGER, col1 INTEGER);
INSERT INTO tab2 VALUES (10, 20);
INSERT INTO tab2 VALUES (30, NULL);

-- Simple IS NULL (should work)
SELECT * FROM tab2 WHERE col1 IS NULL;

-- Unary operator with IS NULL (should work)
SELECT * FROM tab2 WHERE +col1 IS NULL;

-- Complex expression with IS NULL (FAILING CASE)
SELECT DISTINCT * FROM tab2 AS cor0 WHERE + col1 + + - col0 IS NULL;

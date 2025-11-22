-- Test cross-type IN clause with >3 items (verifies type coercion fix)
-- This should match with Float→Int coercion

CREATE TABLE test_int (int_col INTEGER);
INSERT INTO test_int VALUES (1), (2), (3);

-- Test 1: Float values in IN clause with >3 items should coerce to Integer
-- Should return all 3 rows (with type coercion)
SELECT * FROM test_int WHERE int_col IN (1.0, 2.0, 3.0, 4.0);

-- Test 2: String→Date coercion with >3 items
CREATE TABLE test_dates (date_col DATE);
INSERT INTO test_dates VALUES ('2024-01-01'), ('2024-01-02');

-- Should return 2 rows (with String→Date coercion)
SELECT * FROM test_dates WHERE date_col IN ('2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04');

-- Test 3: Cross-numeric types with >3 items
CREATE TABLE test_numeric (num_col NUMERIC);
INSERT INTO test_numeric VALUES (10), (20), (30);

-- Should return all 3 rows (NUMERIC → INTEGER coercion)
SELECT * FROM test_numeric WHERE num_col IN (10, 20, 30, 40);

DROP TABLE test_int;
DROP TABLE test_dates;
DROP TABLE test_numeric;

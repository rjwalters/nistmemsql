CREATE TABLE t1(x INTEGER);
INSERT INTO t1 VALUES(1);

CREATE VIEW v1 AS SELECT x FROM t1;

-- Verify view works
SELECT x FROM v1;

-- Drop view
DROP VIEW v1;

-- Try to use dropped view (should error)
SELECT x FROM v1;

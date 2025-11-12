CREATE TABLE t1(x INTEGER);
CREATE VIEW v1 AS SELECT x FROM t1;

-- Drop with IF EXISTS (should succeed)
DROP VIEW IF EXISTS v1;

-- Drop again with IF EXISTS (should also succeed)
DROP VIEW IF EXISTS v1;

-- Drop without IF EXISTS (should fail)
DROP VIEW v1;

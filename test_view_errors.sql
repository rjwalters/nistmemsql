CREATE TABLE t1(x INTEGER, y VARCHAR(8));
INSERT INTO t1 VALUES(1, 'one');

-- Create view
CREATE VIEW v1 AS SELECT x FROM t1;
SELECT x FROM v1;

-- Try to create duplicate view (should error)
CREATE VIEW v1 AS SELECT x FROM t1;

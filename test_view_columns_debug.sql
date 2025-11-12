CREATE TABLE t1(x INTEGER, y VARCHAR(8));
INSERT INTO t1 VALUES(1, 'one');

-- Create view with explicit column names
CREATE VIEW v1(a, b) AS SELECT x, y FROM t1;

-- Check what columns the view schema has
SELECT x, y FROM t1;
SELECT a, b FROM v1;

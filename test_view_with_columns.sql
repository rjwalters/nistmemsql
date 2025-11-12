CREATE TABLE t1(x INTEGER, y VARCHAR(8));
INSERT INTO t1 VALUES(1, 'one');
INSERT INTO t1 VALUES(2, 'two');

-- Create view with explicit column names
CREATE VIEW v1(a, b) AS SELECT x, y FROM t1;

-- Query the view
SELECT a, b FROM v1;
SELECT a FROM v1;

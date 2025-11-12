CREATE TABLE t1(x INTEGER);
INSERT INTO t1 VALUES(1);

-- Use exact case as it will be stored
CREATE VIEW V1(col_a) AS SELECT x FROM t1;

SELECT col_a FROM V1;

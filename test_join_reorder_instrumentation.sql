-- Test script to verify join reordering instrumentation is working
-- Run with: VIBESQL_DEBUG_JOIN_REORDER=1 cargo test --lib

CREATE TABLE t1 (id INT, val INT);
CREATE TABLE t2 (id INT, val INT);
CREATE TABLE t3 (id INT, val INT);

INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300);
INSERT INTO t3 VALUES (1, 1000), (2, 2000);

-- This should trigger the instrumentation (3+ tables with joins)
SELECT * FROM t1
  JOIN t2 ON t1.id = t2.id
  JOIN t3 ON t2.id = t3.id;

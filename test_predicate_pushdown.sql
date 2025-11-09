-- Test predicate pushdown with multi-table joins from select5.test pattern
CREATE TABLE t1(a1 INTEGER PRIMARY KEY, b1 INTEGER, x1 VARCHAR(40));
CREATE TABLE t2(a2 INTEGER PRIMARY KEY, b2 INTEGER, x2 VARCHAR(40));
CREATE TABLE t3(a3 INTEGER PRIMARY KEY, b3 INTEGER, x3 VARCHAR(40));
CREATE TABLE t4(a4 INTEGER PRIMARY KEY, b4 INTEGER, x4 VARCHAR(40));

-- Insert 10 rows into each table  
INSERT INTO t1 VALUES (1, 1, 'row 1'), (2, 2, 'row 2'), (3, 3, 'row 3'), (4, 4, 'row 4'), (5, 5, 'row 5'),
                      (6, 6, 'row 6'), (7, 7, 'row 7'), (8, 8, 'row 8'), (9, 9, 'row 9'), (10, 10, 'row 10');
INSERT INTO t2 VALUES (1, 1, 'row 1'), (2, 2, 'row 2'), (3, 3, 'row 3'), (4, 4, 'row 4'), (5, 5, 'row 5'),
                      (6, 6, 'row 6'), (7, 7, 'row 7'), (8, 8, 'row 8'), (9, 9, 'row 9'), (10, 10, 'row 10');
INSERT INTO t3 VALUES (1, 1, 'row 1'), (2, 2, 'row 2'), (3, 3, 'row 3'), (4, 4, 'row 4'), (5, 5, 'row 5'),
                      (6, 6, 'row 6'), (7, 7, 'row 7'), (8, 8, 'row 8'), (9, 9, 'row 9'), (10, 10, 'row 10');
INSERT INTO t4 VALUES (1, 1, 'row 1'), (2, 2, 'row 2'), (3, 3, 'row 3'), (4, 4, 'row 4'), (5, 5, 'row 5'),
                      (6, 6, 'row 6'), (7, 7, 'row 7'), (8, 8, 'row 8'), (9, 9, 'row 9'), (10, 10, 'row 10');

-- Simple query with equijoin conditions 
-- Without predicate pushdown: 10^4 = 10000 rows before filtering
-- With predicate pushdown: should filter earlier
SELECT x1, x2, x3, x4
FROM t1, t2, t3, t4
WHERE a1 = b2  -- Equijoin: t1.a1 = t2.b2
  AND a2 = b3  -- Equijoin: t2.a2 = t3.b3
  AND a3 = b4  -- Equijoin: t3.a3 = t4.b4
  AND a1 = 5   -- Table-local: filter t1 by a1 = 5
ORDER BY x1, x2, x3, x4;

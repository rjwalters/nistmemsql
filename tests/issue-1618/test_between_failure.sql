-- Reproduction for index/between/10/slt_good_0.test failure
-- Expected: Returns pk=6
-- Actual: Returns no rows

CREATE TABLE tab0(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
INSERT INTO tab0 VALUES(0,22,43.96,'yoyca',0,80.14,'eoenc');
INSERT INTO tab0 VALUES(1,51,34.90,'zeqhw',44,13.49,'easox');
INSERT INTO tab0 VALUES(2,42,59.76,'ylshk',15,4.45,'xgrvy');
INSERT INTO tab0 VALUES(3,67,90.66,'rnadc',77,50.36,'knooo');
INSERT INTO tab0 VALUES(4,48,53.10,'txhlv',75,9.77,'gvudx');
INSERT INTO tab0 VALUES(5,18,40.58,'wgfxz',96,12.5,'mmxbj');
INSERT INTO tab0 VALUES(6,84,24.24,'ttodp',31,73.0,'wujjl');
INSERT INTO tab0 VALUES(7,86,67.45,'mwgbl',38,10.48,'ypcha');
INSERT INTO tab0 VALUES(8,68,38.47,'kaoqh',8,41.5,'fyhzl');
INSERT INTO tab0 VALUES(9,29,19.6,'kbenw',20,19.58,'gsszq');

CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);
CREATE INDEX idx_tab1_0 ON tab1 (col0);
CREATE INDEX idx_tab1_1 ON tab1 (col1);
CREATE INDEX idx_tab1_3 ON tab1 (col3);
CREATE INDEX idx_tab1_4 ON tab1 (col4);

-- This should populate indexes (our fix)
INSERT INTO tab1 SELECT * FROM tab0;

-- Test 1: Verify row 6 exists
SELECT pk FROM tab1 WHERE pk = 6;
-- Expected: 6

-- Test 2: Check col0 index
SELECT pk FROM tab1 WHERE col0 >= 79;
-- Expected: 6, 7

-- Test 3: Check col4 index
SELECT pk FROM tab1 WHERE col4 >= 56.46 AND col4 <= 83.9;
-- Expected: 0, 6

-- Test 4: Combined query (THE FAILING ONE)
SELECT pk FROM tab1 WHERE (col0 >= 79) AND (col4 >= 56.46 AND col4 <= 83.9);
-- Expected: 6
-- Actual: ???

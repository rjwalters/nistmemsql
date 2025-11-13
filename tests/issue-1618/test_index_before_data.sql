CREATE TABLE test_table(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 TEXT);
CREATE INDEX idx_col0 ON test_table (col0);
INSERT INTO test_table VALUES(1, 100, 'a');
INSERT INTO test_table VALUES(2, 200, 'b');
INSERT INTO test_table VALUES(3, 300, 'c');
INSERT INTO test_table VALUES(4, 400, 'd');
INSERT INTO test_table VALUES(5, 500, 'e');
SELECT pk FROM test_table WHERE col0 > 250;

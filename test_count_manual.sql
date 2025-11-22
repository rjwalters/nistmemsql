CREATE TABLE lineitem (
    l_quantity FLOAT,
    l_extendedprice FLOAT,
    l_discount FLOAT,
    l_shipdate DATE
);

INSERT INTO lineitem VALUES (10.0, 1000.0, 0.06, '1994-06-15');
INSERT INTO lineitem VALUES (15.0, 2000.0, 0.05, '1994-12-31');
INSERT INTO lineitem VALUES (20.0, 1500.0, 0.07, '1994-01-01');

-- This should return 3
SELECT COUNT(*) FROM lineitem
WHERE l_shipdate >= '1994-01-01'
  AND l_shipdate < '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24;

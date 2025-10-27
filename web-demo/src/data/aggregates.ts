export interface AggregateFunction {
  name: string
  category: 'Basic' | 'Grouping' | 'Filtering' | 'Planned'
  status: 'implemented' | 'planned'
  description: string
  syntax: string
  useCase: string
}

export const AGGREGATE_FUNCTIONS: AggregateFunction[] = [
  // Basic Aggregates
  {
    name: 'COUNT(*)',
    category: 'Basic',
    status: 'implemented',
    description: 'Counts all rows including NULLs',
    syntax: 'SELECT COUNT(*) FROM table',
    useCase: 'Getting total number of rows in a table or group',
  },
  {
    name: 'COUNT(column)',
    category: 'Basic',
    status: 'implemented',
    description: 'Counts non-NULL values in column',
    syntax: 'SELECT COUNT(column) FROM table',
    useCase: 'Counting how many rows have a value for specific column',
  },
  {
    name: 'SUM',
    category: 'Basic',
    status: 'implemented',
    description: 'Calculates sum of numeric values (excludes NULLs)',
    syntax: 'SELECT SUM(column) FROM table',
    useCase: 'Totaling numeric columns like sales or quantities',
  },
  {
    name: 'AVG',
    category: 'Basic',
    status: 'implemented',
    description: 'Calculates average of numeric values (excludes NULLs)',
    syntax: 'SELECT AVG(column) FROM table',
    useCase: 'Finding mean values like average price or salary',
  },
  {
    name: 'MIN',
    category: 'Basic',
    status: 'implemented',
    description: 'Finds minimum value (excludes NULLs)',
    syntax: 'SELECT MIN(column) FROM table',
    useCase: 'Finding lowest value like cheapest price',
  },
  {
    name: 'MAX',
    category: 'Basic',
    status: 'implemented',
    description: 'Finds maximum value (excludes NULLs)',
    syntax: 'SELECT MAX(column) FROM table',
    useCase: 'Finding highest value like most expensive price',
  },

  // Grouping
  {
    name: 'GROUP BY (single column)',
    category: 'Grouping',
    status: 'implemented',
    description: 'Groups rows by single column values',
    syntax: 'SELECT col, COUNT(*) FROM table GROUP BY col',
    useCase: 'Aggregating data by category or dimension',
  },
  {
    name: 'GROUP BY (multiple columns)',
    category: 'Grouping',
    status: 'implemented',
    description: 'Groups rows by combination of column values',
    syntax: 'SELECT col1, col2, COUNT(*) FROM table GROUP BY col1, col2',
    useCase: 'Multi-dimensional aggregation like region and product',
  },

  // Filtering
  {
    name: 'HAVING',
    category: 'Filtering',
    status: 'implemented',
    description: 'Filters groups after aggregation (unlike WHERE which filters before)',
    syntax: 'SELECT col, COUNT(*) FROM table GROUP BY col HAVING COUNT(*) > 10',
    useCase: 'Filtering aggregated results like groups with totals above threshold',
  },

  // Planned Features
  {
    name: 'COUNT(DISTINCT column)',
    category: 'Planned',
    status: 'planned',
    description: 'Counts unique non-NULL values',
    syntax: 'SELECT COUNT(DISTINCT column) FROM table',
    useCase: 'Counting unique values like number of different products',
  },
  {
    name: 'Advanced HAVING',
    category: 'Planned',
    status: 'planned',
    description: 'Complex HAVING conditions with multiple aggregates',
    syntax: 'HAVING COUNT(*) > 5 AND AVG(price) > 100',
    useCase: 'Multi-criteria group filtering',
  },
]

export const AGGREGATE_EXAMPLES = {
  basicAggregates: `CREATE TABLE sales (
    id INTEGER,
    product VARCHAR(50),
    quantity INTEGER,
    price FLOAT,
    region VARCHAR(20)
);

INSERT INTO sales VALUES (1, 'Laptop', 5, 999.99, 'North');
INSERT INTO sales VALUES (2, 'Mouse', 50, 19.99, 'North');
INSERT INTO sales VALUES (3, 'Laptop', 3, 999.99, 'South');
INSERT INTO sales VALUES (4, 'Keyboard', 25, 79.99, 'South');
INSERT INTO sales VALUES (5, 'Monitor', NULL, 299.99, 'East');

-- COUNT
SELECT COUNT(*) AS total_sales FROM sales;
SELECT COUNT(quantity) AS sales_with_qty FROM sales;

-- SUM
SELECT SUM(quantity) AS total_units FROM sales;

-- AVG (excludes NULLs)
SELECT AVG(quantity) AS avg_units FROM sales;

-- MIN and MAX
SELECT MIN(price) AS cheapest, MAX(price) AS most_expensive FROM sales;`,

  groupBySingle: `-- Group by region
SELECT region, COUNT(*) AS sales_count, SUM(quantity) AS total_units
FROM sales
GROUP BY region;

-- Group by product
SELECT product, AVG(price) AS avg_price, SUM(quantity) AS total_sold
FROM sales
GROUP BY product;`,

  groupByMultiple: `-- Group by region and product
SELECT region, product, COUNT(*) AS count, AVG(price) AS avg_price
FROM sales
GROUP BY region, product;`,

  having: `-- Filter groups after aggregation
SELECT region, SUM(quantity) AS total_units
FROM sales
GROUP BY region
HAVING SUM(quantity) > 20;

-- Complex HAVING conditions
SELECT product, COUNT(*) AS sales_count, AVG(price) AS avg_price
FROM sales
GROUP BY product
HAVING COUNT(*) > 1 AND AVG(price) > 50.0;`,

  orderBy: `-- Order aggregated results
SELECT region, SUM(quantity) AS total_units
FROM sales
GROUP BY region
ORDER BY total_units DESC;

-- Multiple sort columns
SELECT product, COUNT(*) AS count, AVG(price) AS avg_price
FROM sales
GROUP BY product
ORDER BY count DESC, avg_price ASC;`,

  nullHandling: `-- Aggregates exclude NULL values (except COUNT(*))
SELECT
    COUNT(*) AS all_rows,
    COUNT(quantity) AS non_null_qty,
    AVG(quantity) AS avg_qty,
    SUM(quantity) AS total_qty
FROM sales;

-- Expected: all_rows=5, non_null_qty=4, avg_qty and total_qty exclude NULL`,
}

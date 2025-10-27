export interface DMLOperation {
  name: string
  category: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE'
  status: 'implemented' | 'partial' | 'planned'
  description: string
  syntax: string
}

export const DML_OPERATIONS: DMLOperation[] = [
  // SELECT Operations
  {
    name: 'SELECT - Basic',
    category: 'SELECT',
    status: 'implemented',
    description: 'Retrieve data from tables with optional filtering',
    syntax: 'SELECT columns FROM table WHERE condition',
  },
  {
    name: 'SELECT with ORDER BY',
    category: 'SELECT',
    status: 'implemented',
    description: 'Sort query results in ascending or descending order',
    syntax: 'SELECT columns FROM table ORDER BY column ASC|DESC',
  },
  {
    name: 'SELECT with LIMIT/OFFSET',
    category: 'SELECT',
    status: 'implemented',
    description: 'Limit number of results and implement pagination',
    syntax: 'SELECT columns FROM table LIMIT n OFFSET m',
  },
  {
    name: 'SELECT DISTINCT',
    category: 'SELECT',
    status: 'implemented',
    description: 'Remove duplicate rows from result set',
    syntax: 'SELECT DISTINCT columns FROM table',
  },

  // INSERT Operations
  {
    name: 'INSERT - Single Row',
    category: 'INSERT',
    status: 'implemented',
    description: 'Insert a single row of data into a table',
    syntax: 'INSERT INTO table VALUES (values)',
  },
  {
    name: 'INSERT with Columns',
    category: 'INSERT',
    status: 'implemented',
    description: 'Insert data specifying column names',
    syntax: 'INSERT INTO table (columns) VALUES (values)',
  },
  {
    name: 'INSERT Multiple Rows',
    category: 'INSERT',
    status: 'planned',
    description: 'Insert multiple rows in a single statement',
    syntax: 'INSERT INTO table VALUES (v1), (v2), (v3)',
  },
  {
    name: 'INSERT from SELECT',
    category: 'INSERT',
    status: 'planned',
    description: 'Insert data from query results',
    syntax: 'INSERT INTO table SELECT columns FROM other_table',
  },

  // UPDATE Operations
  {
    name: 'UPDATE with WHERE',
    category: 'UPDATE',
    status: 'implemented',
    description: 'Update rows matching a condition',
    syntax: 'UPDATE table SET column = value WHERE condition',
  },
  {
    name: 'UPDATE Multiple Columns',
    category: 'UPDATE',
    status: 'implemented',
    description: 'Update several columns in one statement',
    syntax: 'UPDATE table SET col1 = v1, col2 = v2 WHERE condition',
  },
  {
    name: 'Positioned UPDATE',
    category: 'UPDATE',
    status: 'planned',
    description: 'Update via cursor position',
    syntax: 'UPDATE table SET column = value WHERE CURRENT OF cursor',
  },

  // DELETE Operations
  {
    name: 'DELETE with WHERE',
    category: 'DELETE',
    status: 'implemented',
    description: 'Delete rows matching a condition',
    syntax: 'DELETE FROM table WHERE condition',
  },
  {
    name: 'Positioned DELETE',
    category: 'DELETE',
    status: 'planned',
    description: 'Delete via cursor position',
    syntax: 'DELETE FROM table WHERE CURRENT OF cursor',
  },
]

export const DML_EXAMPLES = {
  selectBasic: `-- Create sample table
CREATE TABLE employees (
    id INTEGER,
    name VARCHAR(100),
    department VARCHAR(50),
    salary INTEGER
);

-- Insert sample data
INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000);
INSERT INTO employees VALUES (2, 'Bob', 'Sales', 75000);
INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 105000);
INSERT INTO employees VALUES (4, 'Diana', 'Marketing', 82000);

-- Basic SELECT queries
SELECT * FROM employees;
SELECT name, salary FROM employees WHERE department = 'Engineering';
SELECT name FROM employees WHERE salary > 80000;`,

  selectOrdering: `-- ORDER BY examples
SELECT * FROM employees ORDER BY salary DESC;
SELECT name, department FROM employees ORDER BY name ASC;

-- LIMIT and OFFSET (pagination)
SELECT * FROM employees ORDER BY salary DESC LIMIT 2;
SELECT * FROM employees ORDER BY id LIMIT 2 OFFSET 1;

-- DISTINCT
SELECT DISTINCT department FROM employees;`,

  insertOperations: `-- Single row insert
INSERT INTO employees VALUES (5, 'Eve', 'HR', 72000);

-- Insert with column specification
INSERT INTO employees (id, name, department) VALUES (6, 'Frank', 'Sales');

-- Insert NULL values
INSERT INTO employees VALUES (7, 'Grace', 'Engineering', NULL);

-- Verify inserts
SELECT * FROM employees WHERE id >= 5;`,

  updateOperations: `-- Update single column
UPDATE employees SET salary = 98000 WHERE id = 1;

-- Update multiple columns
UPDATE employees SET department = 'Engineering', salary = 95000 WHERE id = 2;

-- Conditional update
UPDATE employees SET salary = salary * 1.1 WHERE department = 'Sales';

-- Verify updates
SELECT * FROM employees ORDER BY id;`,

  deleteOperations: `-- Delete specific row
DELETE FROM employees WHERE id = 7;

-- Conditional delete
DELETE FROM employees WHERE salary < 75000;

-- Delete by department
DELETE FROM employees WHERE department = 'Marketing';

-- Verify remaining rows
SELECT * FROM employees;`,

  combined: `-- Full CRUD workflow
-- 1. Create and populate table
CREATE TABLE products (
    id INTEGER,
    name VARCHAR(100),
    price FLOAT,
    stock INTEGER
);

INSERT INTO products VALUES (1, 'Laptop', 999.99, 15);
INSERT INTO products VALUES (2, 'Mouse', 24.99, 50);
INSERT INTO products VALUES (3, 'Keyboard', 79.99, 30);

-- 2. Read data
SELECT * FROM products WHERE price < 100;

-- 3. Update inventory
UPDATE products SET stock = stock - 5 WHERE id = 1;

-- 4. Remove out of stock items
DELETE FROM products WHERE stock = 0;

-- 5. View final state
SELECT * FROM products ORDER BY price DESC;`,
}

/**
 * SQL:1999 Feature Showcase - Example Queries
 *
 * Comprehensive library of SQL examples organized by feature category.
 * Each example demonstrates specific SQL:1999 capabilities using the
 * Northwind and Employees example databases.
 */

export interface QueryExample {
  id: string
  title: string
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
  sql: string
  description: string
  sqlFeatures: string[]
}

export interface ExampleCategory {
  id: string
  title: string
  description: string
  queries: QueryExample[]
}

export const exampleCategories: ExampleCategory[] = [
  {
    id: 'basic',
    title: 'Basic Queries',
    description: 'SELECT, WHERE, ORDER BY, LIMIT fundamentals',
    queries: [
      {
        id: 'basic-1',
        title: 'Simple SELECT',
        database: 'northwind',
        sql: 'SELECT * FROM products LIMIT 5;',
        description: 'Retrieve first 5 products',
        sqlFeatures: ['SELECT', 'LIMIT'],
      },
      {
        id: 'basic-2',
        title: 'WHERE clause filtering',
        database: 'northwind',
        sql: `SELECT product_name, unit_price
FROM products
WHERE unit_price > 20
ORDER BY unit_price DESC;`,
        description: 'Filter products by price with sorting',
        sqlFeatures: ['SELECT', 'WHERE', 'ORDER BY'],
      },
      {
        id: 'basic-3',
        title: 'Column aliases',
        database: 'northwind',
        sql: `SELECT
  product_name AS name,
  unit_price AS price,
  unit_price * 1.2 AS price_with_tax
FROM products
LIMIT 10;`,
        description: 'Use aliases for columns and calculated fields',
        sqlFeatures: ['SELECT', 'AS', 'Expressions'],
      },
      {
        id: 'basic-4',
        title: 'DISTINCT values',
        database: 'northwind',
        sql: `SELECT DISTINCT category_id
FROM products
ORDER BY category_id;`,
        description: 'Get unique category IDs',
        sqlFeatures: ['SELECT', 'DISTINCT', 'ORDER BY'],
      },
    ],
  },

  {
    id: 'string',
    title: 'String Functions',
    description: 'UPPER, LOWER, SUBSTRING, TRIM, CONCAT, LENGTH, and more',
    queries: [
      {
        id: 'string-1',
        title: 'Text Case Conversion',
        database: 'northwind',
        sql: `SELECT
  product_name,
  UPPER(product_name) AS uppercase,
  LOWER(product_name) AS lowercase
FROM products
LIMIT 5;`,
        description: 'Convert text to uppercase and lowercase',
        sqlFeatures: ['UPPER', 'LOWER'],
      },
      {
        id: 'string-2',
        title: 'String Extraction',
        database: 'employees',
        sql: `SELECT
  first_name,
  last_name,
  SUBSTRING(first_name, 1, 1) AS first_initial,
  LENGTH(last_name) AS last_name_length
FROM employees
LIMIT 8;`,
        description: 'Extract substrings and measure string length',
        sqlFeatures: ['SUBSTRING', 'LENGTH'],
      },
      {
        id: 'string-3',
        title: 'Text Trimming',
        database: 'northwind',
        sql: `SELECT
  '  ' || product_name || '  ' AS padded,
  TRIM('  ' || product_name || '  ') AS trimmed,
  LENGTH('  ' || product_name || '  ') AS padded_length,
  LENGTH(TRIM('  ' || product_name || '  ')) AS trimmed_length
FROM products
LIMIT 5;`,
        description: 'Remove leading and trailing whitespace',
        sqlFeatures: ['TRIM', 'LENGTH', '||'],
      },
      {
        id: 'string-4',
        title: 'String Concatenation',
        database: 'employees',
        sql: `SELECT
  first_name,
  last_name,
  first_name || ' ' || last_name AS full_name,
  CONCAT(last_name, ', ', first_name) AS formatted_name
FROM employees
LIMIT 8;`,
        description: 'Combine strings using || operator and CONCAT function',
        sqlFeatures: ['CONCAT', '||'],
      },
      {
        id: 'string-5',
        title: 'LEFT and RIGHT Extraction',
        database: 'northwind',
        sql: `SELECT
  product_name,
  LEFT(product_name, 5) AS first_5_chars,
  RIGHT(product_name, 5) AS last_5_chars
FROM products
WHERE LENGTH(product_name) >= 10
LIMIT 6;`,
        description: 'Extract characters from start and end of strings',
        sqlFeatures: ['LEFT', 'RIGHT', 'LENGTH'],
      },
      {
        id: 'string-6',
        title: 'String Replacement',
        database: 'northwind',
        sql: `SELECT
  product_name,
  REPLACE(product_name, 'a', '@') AS replaced,
  REPLACE(product_name, ' ', '_') AS underscored
FROM products
WHERE product_name LIKE '%a%'
LIMIT 6;`,
        description: 'Replace characters or substrings within text',
        sqlFeatures: ['REPLACE', 'LIKE'],
      },
      {
        id: 'string-7',
        title: 'String Search',
        database: 'employees',
        sql: `SELECT
  first_name,
  last_name,
  POSITION('a' IN LOWER(first_name)) AS first_a_position,
  POSITION('e' IN LOWER(last_name)) AS first_e_position
FROM employees
WHERE POSITION('a' IN LOWER(first_name)) > 0
LIMIT 8;`,
        description: 'Find position of substring within text',
        sqlFeatures: ['POSITION', 'LOWER'],
      },
      {
        id: 'string-8',
        title: 'Text Reversal',
        database: 'northwind',
        sql: `SELECT
  product_name,
  REVERSE(product_name) AS reversed,
  UPPER(REVERSE(product_name)) AS reversed_upper
FROM products
LIMIT 5;`,
        description: 'Reverse string character order',
        sqlFeatures: ['REVERSE', 'UPPER'],
      },
    ],
  },

  {
    id: 'joins',
    title: 'JOIN Operations',
    description: 'INNER, LEFT, RIGHT, FULL OUTER, CROSS joins',
    queries: [
      {
        id: 'join-1',
        title: 'INNER JOIN',
        database: 'northwind',
        sql: `SELECT
  p.product_name,
  c.category_name,
  p.unit_price
FROM products p
INNER JOIN categories c ON p.category_id = c.category_id
ORDER BY c.category_name, p.product_name;`,
        description: 'Join products with their categories',
        sqlFeatures: ['INNER JOIN', 'ON', 'ORDER BY'],
      },
      {
        id: 'join-2',
        title: 'LEFT OUTER JOIN',
        database: 'northwind',
        sql: `SELECT
  c.category_name,
  COUNT(p.product_id) as product_count
FROM categories c
LEFT OUTER JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY product_count DESC;`,
        description: 'Show all categories with product counts (including empty categories)',
        sqlFeatures: ['LEFT OUTER JOIN', 'GROUP BY', 'COUNT'],
      },
      {
        id: 'join-3',
        title: 'Multiple JOINs',
        database: 'employees',
        sql: `SELECT
  e.first_name || ' ' || e.last_name AS employee,
  e.title,
  m.first_name || ' ' || m.last_name AS manager
FROM employees e
INNER JOIN employees m ON e.manager_id = m.employee_id
WHERE e.department = 'Engineering'
ORDER BY e.last_name;`,
        description: 'Self-join to show employees and their managers',
        sqlFeatures: ['INNER JOIN', 'Self-join', 'String concatenation'],
      },
      {
        id: 'join-4',
        title: 'CROSS JOIN',
        database: 'northwind',
        sql: `SELECT
  c1.category_name AS category1,
  c2.category_name AS category2
FROM categories c1
CROSS JOIN categories c2
WHERE c1.category_id < c2.category_id
LIMIT 10;`,
        description: 'Cartesian product of categories (all pairs)',
        sqlFeatures: ['CROSS JOIN', 'WHERE', 'LIMIT'],
      },
    ],
  },

  {
    id: 'aggregates',
    title: 'Aggregate Functions',
    description: 'COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING',
    queries: [
      {
        id: 'agg-1',
        title: 'Basic aggregation',
        database: 'northwind',
        sql: `SELECT
  COUNT(*) as total_products,
  AVG(unit_price) as avg_price,
  MIN(unit_price) as min_price,
  MAX(unit_price) as max_price
FROM products;`,
        description: 'Aggregate statistics for all products',
        sqlFeatures: ['COUNT', 'AVG', 'MIN', 'MAX'],
      },
      {
        id: 'agg-2',
        title: 'GROUP BY with COUNT',
        database: 'northwind',
        sql: `SELECT
  c.category_name,
  COUNT(p.product_id) as product_count,
  AVG(p.unit_price) as avg_price
FROM categories c
INNER JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_name
ORDER BY product_count DESC;`,
        description: 'Count and average price of products by category',
        sqlFeatures: ['GROUP BY', 'COUNT', 'AVG', 'INNER JOIN'],
      },
      {
        id: 'agg-3',
        title: 'HAVING clause',
        database: 'northwind',
        sql: `SELECT
  c.category_name,
  COUNT(p.product_id) as product_count
FROM categories c
LEFT JOIN products p ON c.category_id = p.category_id
GROUP BY c.category_name
HAVING COUNT(p.product_id) >= 2
ORDER BY product_count DESC;`,
        description: 'Filter aggregated results - categories with 2+ products',
        sqlFeatures: ['GROUP BY', 'HAVING', 'COUNT'],
      },
      {
        id: 'agg-4',
        title: 'Multiple grouping columns',
        database: 'employees',
        sql: `SELECT
  department,
  title,
  COUNT(*) as employee_count,
  AVG(salary) as avg_salary
FROM employees
GROUP BY department, title
HAVING COUNT(*) > 1
ORDER BY department, avg_salary DESC;`,
        description: 'Group by department and title with salary aggregates',
        sqlFeatures: ['GROUP BY', 'HAVING', 'AVG', 'COUNT'],
      },
    ],
  },

  {
    id: 'recursive',
    title: 'Recursive Queries',
    description: 'WITH RECURSIVE - SQL:1999 common table expressions',
    queries: [
      {
        id: 'rec-1',
        title: 'Employee hierarchy',
        database: 'employees',
        sql: `WITH RECURSIVE employee_hierarchy AS (
  -- Base case: CEO (no manager)
  SELECT
    employee_id,
    first_name,
    last_name,
    title,
    manager_id,
    1 as level,
    first_name || ' ' || last_name as path
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  -- Recursive case: employees with managers
  SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    e.title,
    e.manager_id,
    eh.level + 1,
    eh.path || ' > ' || e.first_name || ' ' || e.last_name
  FROM employees e
  INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT
  level,
  first_name || ' ' || last_name as employee,
  title,
  path as reporting_chain
FROM employee_hierarchy
ORDER BY level, last_name
LIMIT 15;`,
        description: 'Build complete org chart using recursive CTE',
        sqlFeatures: ['WITH RECURSIVE', 'UNION ALL', 'INNER JOIN'],
      },
      {
        id: 'rec-2',
        title: 'Count hierarchy levels',
        database: 'employees',
        sql: `WITH RECURSIVE hierarchy AS (
  SELECT
    employee_id,
    1 as level
  FROM employees
  WHERE manager_id IS NULL

  UNION ALL

  SELECT
    e.employee_id,
    h.level + 1
  FROM employees e
  INNER JOIN hierarchy h ON e.manager_id = h.employee_id
)
SELECT
  level,
  COUNT(*) as employee_count
FROM hierarchy
GROUP BY level
ORDER BY level;`,
        description: 'Count employees at each level of the org chart',
        sqlFeatures: ['WITH RECURSIVE', 'UNION ALL', 'GROUP BY'],
      },
    ],
  },

  {
    id: 'subqueries',
    title: 'Subqueries',
    description: 'Scalar, table, and IN/EXISTS subqueries',
    queries: [
      {
        id: 'sub-1',
        title: 'Scalar subquery',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  (SELECT AVG(unit_price) FROM products) as avg_price
FROM products
WHERE unit_price > (SELECT AVG(unit_price) FROM products)
ORDER BY unit_price DESC;`,
        description: 'Find products above average price',
        sqlFeatures: ['Subquery', 'AVG', 'WHERE', 'SELECT'],
      },
      {
        id: 'sub-2',
        title: 'IN subquery',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price
FROM products
WHERE category_id IN (
  SELECT category_id
  FROM categories
  WHERE category_name IN ('Beverages', 'Condiments')
)
ORDER BY unit_price DESC;`,
        description: 'Find products in specific categories using IN',
        sqlFeatures: ['IN', 'Subquery', 'WHERE'],
      },
      {
        id: 'sub-3',
        title: 'Subquery in FROM clause',
        database: 'employees',
        sql: `SELECT
  department,
  avg_salary,
  CASE
    WHEN avg_salary > 100000 THEN 'High'
    WHEN avg_salary > 50000 THEN 'Medium'
    ELSE 'Low'
  END as salary_bracket
FROM (
  SELECT
    department,
    AVG(salary) as avg_salary
  FROM employees
  GROUP BY department
) dept_salaries
ORDER BY avg_salary DESC;`,
        description: 'Use subquery as derived table for further processing',
        sqlFeatures: ['Subquery in FROM', 'CASE', 'AVG', 'GROUP BY'],
      },
    ],
  },

  {
    id: 'case',
    title: 'CASE Expressions',
    description: 'Conditional logic in queries',
    queries: [
      {
        id: 'case-1',
        title: 'Simple CASE',
        database: 'northwind',
        sql: `SELECT
  product_name,
  unit_price,
  CASE
    WHEN unit_price < 10 THEN 'Budget'
    WHEN unit_price < 50 THEN 'Standard'
    ELSE 'Premium'
  END as price_category
FROM products
ORDER BY unit_price;`,
        description: 'Categorize products by price range',
        sqlFeatures: ['CASE', 'WHEN', 'ELSE', 'END'],
      },
      {
        id: 'case-2',
        title: 'CASE in aggregation',
        database: 'employees',
        sql: `SELECT
  department,
  COUNT(*) as total_employees,
  COUNT(CASE WHEN salary > 100000 THEN 1 END) as high_earners,
  COUNT(CASE WHEN salary <= 100000 THEN 1 END) as other_earners
FROM employees
GROUP BY department
ORDER BY department;`,
        description: 'Conditional counting using CASE expressions',
        sqlFeatures: ['CASE', 'COUNT', 'GROUP BY'],
      },
      {
        id: 'case-3',
        title: 'Multiple CASE expressions',
        database: 'employees',
        sql: `SELECT
  first_name || ' ' || last_name as employee,
  salary,
  CASE
    WHEN salary >= 200000 THEN 'Executive'
    WHEN salary >= 140000 THEN 'Senior Management'
    WHEN salary >= 100000 THEN 'Management'
    ELSE 'Staff'
  END as pay_grade,
  CASE
    WHEN department = 'Engineering' THEN 'Tech'
    WHEN department = 'Sales' THEN 'Revenue'
    ELSE 'Operations'
  END as division
FROM employees
ORDER BY salary DESC
LIMIT 10;`,
        description: 'Multiple CASE statements for complex categorization',
        sqlFeatures: ['CASE', 'String concatenation', 'LIMIT'],
      },
    ],
  },

  {
    id: 'set',
    title: 'Set Operations',
    description: 'UNION, INTERSECT, EXCEPT for combining queries',
    queries: [
      {
        id: 'set-1',
        title: 'UNION',
        database: 'employees',
        sql: `SELECT department FROM employees WHERE salary > 150000
UNION
SELECT department FROM employees WHERE title LIKE '%Director%'
ORDER BY department;`,
        description: 'Combine unique departments from two criteria',
        sqlFeatures: ['UNION', 'WHERE', 'ORDER BY'],
      },
      {
        id: 'set-2',
        title: 'UNION ALL',
        database: 'northwind',
        sql: `SELECT 'Expensive' as category, product_name, unit_price
FROM products
WHERE unit_price > 50
UNION ALL
SELECT 'Cheap' as category, product_name, unit_price
FROM products
WHERE unit_price < 10
ORDER BY unit_price DESC;`,
        description: 'Combine expensive and cheap products (with duplicates)',
        sqlFeatures: ['UNION ALL', 'WHERE', 'String literals'],
      },
      {
        id: 'set-3',
        title: 'Column count in UNION',
        database: 'northwind',
        sql: `SELECT category_name as name, 'category' as type
FROM categories
UNION
SELECT product_name as name, 'product' as type
FROM products
LIMIT 15;`,
        description: 'Union different tables with matching column structures',
        sqlFeatures: ['UNION', 'AS', 'LIMIT'],
      },
    ],
  },

  {
    id: 'company',
    title: 'Company Database',
    description: 'Multi-table JOINs and business analytics',
    queries: [
      {
        id: 'company-1',
        title: 'Department headcount and salaries',
        database: 'company',
        sql: `SELECT
  d.dept_name,
  COUNT(e.emp_id) AS headcount,
  AVG(e.salary) AS avg_salary,
  MIN(e.salary) AS min_salary,
  MAX(e.salary) AS max_salary
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
GROUP BY d.dept_name
ORDER BY avg_salary DESC;`,
        description: 'Show employee count and salary statistics by department',
        sqlFeatures: ['LEFT JOIN', 'GROUP BY', 'COUNT', 'AVG', 'MIN', 'MAX'],
      },
      {
        id: 'company-2',
        title: 'Total project budget by department',
        database: 'company',
        sql: `SELECT
  d.dept_name,
  d.location,
  COUNT(p.project_id) AS project_count,
  SUM(p.budget) AS total_budget
FROM departments d
LEFT JOIN projects p ON d.dept_id = p.dept_id
GROUP BY d.dept_name, d.location
HAVING SUM(p.budget) > 0
ORDER BY total_budget DESC;`,
        description: 'Calculate total project budgets and counts per department',
        sqlFeatures: ['LEFT JOIN', 'GROUP BY', 'COUNT', 'SUM', 'HAVING'],
      },
      {
        id: 'company-3',
        title: 'Employee project assignments',
        database: 'company',
        sql: `SELECT
  e.name AS employee,
  d.dept_name AS department,
  p.project_name,
  p.budget
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id
LEFT JOIN projects p ON e.dept_id = p.dept_id
WHERE e.dept_id IS NOT NULL
ORDER BY d.dept_name, e.name
LIMIT 15;`,
        description: 'Show employees with their departments and assigned projects',
        sqlFeatures: ['LEFT JOIN', 'Multi-table JOIN', 'WHERE', 'LIMIT'],
      },
      {
        id: 'company-4',
        title: 'Budget efficiency analysis',
        database: 'company',
        sql: `SELECT
  d.dept_name,
  COUNT(e.emp_id) AS headcount,
  SUM(p.budget) AS project_budget,
  CASE
    WHEN COUNT(e.emp_id) = 0 THEN 0
    ELSE SUM(p.budget) / COUNT(e.emp_id)
  END AS budget_per_employee
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
LEFT JOIN projects p ON d.dept_id = p.dept_id
GROUP BY d.dept_name
ORDER BY budget_per_employee DESC;`,
        description: 'Calculate budget efficiency (budget per employee) by department',
        sqlFeatures: ['LEFT JOIN', 'Multi-table JOIN', 'GROUP BY', 'CASE', 'Calculated fields'],
      },
      {
        id: 'company-5',
        title: 'High-value projects report',
        database: 'company',
        sql: `SELECT
  p.project_name,
  p.budget,
  d.dept_name,
  d.location,
  CASE
    WHEN p.budget >= 400000 THEN 'Critical'
    WHEN p.budget >= 200000 THEN 'Major'
    WHEN p.budget >= 100000 THEN 'Standard'
    ELSE 'Minor'
  END AS priority
FROM projects p
LEFT JOIN departments d ON p.dept_id = d.dept_id
WHERE p.budget IS NOT NULL
ORDER BY p.budget DESC;`,
        description: 'List projects with budget categorization and department info',
        sqlFeatures: ['LEFT JOIN', 'WHERE', 'CASE', 'ORDER BY'],
      },
    ],
  },

  {
    id: 'university',
    title: 'University Database Examples',
    description: 'Complex relationships, correlated subqueries, and multi-table JOINs',
    queries: [
      {
        id: 'uni-1',
        title: 'Student GPA Calculation',
        database: 'university',
        sql: `SELECT s.name, s.major, s.gpa AS declared_gpa,
       (SELECT AVG(CASE
           WHEN e.grade = 'A' THEN 4.0
           WHEN e.grade = 'B' THEN 3.0
           WHEN e.grade = 'C' THEN 2.0
           WHEN e.grade = 'D' THEN 1.0
           WHEN e.grade = 'F' THEN 0.0
       END)
        FROM enrollments e
        WHERE e.student_id = s.student_id AND e.grade IS NOT NULL) AS calculated_gpa
FROM students s
LIMIT 10;`,
        description: 'Calculate GPA from enrollments using correlated subquery',
        sqlFeatures: ['Correlated subquery', 'CASE', 'AVG', 'NULL handling'],
      },
      {
        id: 'uni-2',
        title: 'Course Enrollment Statistics',
        database: 'university',
        sql: `SELECT c.course_name, c.department, COUNT(e.student_id) AS enrollment_count
FROM courses c
LEFT JOIN enrollments e ON c.course_id = e.course_id
GROUP BY c.course_name, c.department
ORDER BY enrollment_count DESC;`,
        description: 'Count enrollments by course with LEFT JOIN',
        sqlFeatures: ['LEFT JOIN', 'COUNT', 'GROUP BY', 'ORDER BY'],
      },
      {
        id: 'uni-3',
        title: 'Department Analysis',
        database: 'university',
        sql: `SELECT c.department,
       COUNT(DISTINCT s.student_id) AS student_count,
       COUNT(DISTINCT c.course_id) AS course_count,
       AVG(s.gpa) AS avg_student_gpa
FROM courses c
JOIN enrollments e ON c.course_id = e.course_id
JOIN students s ON e.student_id = s.student_id
GROUP BY c.department
ORDER BY student_count DESC;`,
        description: 'Multi-table JOIN to analyze departments',
        sqlFeatures: ['Multi-table JOIN', 'COUNT DISTINCT', 'AVG', 'GROUP BY'],
      },
      {
        id: 'uni-4',
        title: 'Grade Distribution',
        database: 'university',
        sql: `SELECT grade, COUNT(*) AS count
FROM enrollments
WHERE grade IS NOT NULL
GROUP BY grade
ORDER BY grade;`,
        description: 'Count enrollments by grade (excluding NULLs)',
        sqlFeatures: ['COUNT', 'GROUP BY', 'WHERE', 'NULL filtering'],
      },
      {
        id: 'uni-5',
        title: 'High-Performing Students',
        database: 'university',
        sql: `SELECT s.name, s.major, s.gpa
FROM students s
WHERE s.gpa > (
    SELECT AVG(s2.gpa)
    FROM students s2
    WHERE s2.major = s.major AND s2.gpa IS NOT NULL
)
ORDER BY s.gpa DESC;`,
        description: 'Students with GPA above their major average',
        sqlFeatures: ['Correlated subquery', 'AVG', 'WHERE', 'NULL handling'],
      },
      {
        id: 'uni-6',
        title: 'Courses with No Enrollments',
        database: 'university',
        sql: `SELECT c.course_name, c.department
FROM courses c
LEFT JOIN enrollments e ON c.course_id = e.course_id
WHERE e.student_id IS NULL;`,
        description: 'Find courses with zero enrollment using LEFT JOIN',
        sqlFeatures: ['LEFT JOIN', 'WHERE', 'NULL filtering'],
      },
    ],
  },

  {
    id: 'ddl',
    title: 'DDL & Constraints',
    description: 'CREATE TABLE with PRIMARY KEY, UNIQUE, CHECK constraints',
    queries: [
      {
        id: 'ddl-1',
        title: 'Basic CREATE TABLE',
        database: 'empty',
        sql: `CREATE TABLE students (
    student_id INTEGER,
    name VARCHAR(100),
    age INTEGER,
    gpa FLOAT
);

INSERT INTO students VALUES (1, 'Alice Johnson', 20, 3.8);
INSERT INTO students VALUES (2, 'Bob Smith', 21, 3.5);

SELECT * FROM students;`,
        description: 'Create a simple table with basic column definitions',
        sqlFeatures: ['CREATE TABLE', 'INTEGER', 'VARCHAR', 'FLOAT'],
      },
      {
        id: 'ddl-2',
        title: 'PRIMARY KEY Constraint',
        database: 'empty',
        sql: `CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(50),
    email VARCHAR(100)
);

INSERT INTO users VALUES (1, 'alice', 'alice@example.com');
INSERT INTO users VALUES (2, 'bob', 'bob@example.com');

SELECT * FROM users ORDER BY user_id;`,
        description: 'Create table with PRIMARY KEY to ensure unique identification',
        sqlFeatures: ['CREATE TABLE', 'PRIMARY KEY', 'INSERT'],
      },
      {
        id: 'ddl-3',
        title: 'PRIMARY KEY Violation',
        database: 'empty',
        sql: `CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(50)
);

INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (1, 'bob');`,
        description: 'Attempt to insert duplicate PRIMARY KEY value (will fail)',
        sqlFeatures: ['PRIMARY KEY', 'Constraint Enforcement'],
      },
      {
        id: 'ddl-4',
        title: 'UNIQUE Constraint',
        database: 'empty',
        sql: `CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    balance FLOAT
);

INSERT INTO accounts VALUES (1, 'alice@example.com', 1000.0);
INSERT INTO accounts VALUES (2, 'bob@example.com', 500.0);

SELECT * FROM accounts ORDER BY account_id;`,
        description: 'Create table with UNIQUE constraint on email column',
        sqlFeatures: ['UNIQUE', 'PRIMARY KEY', 'Constraint Enforcement'],
      },
      {
        id: 'ddl-5',
        title: 'UNIQUE Constraint Violation',
        database: 'empty',
        sql: `CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    email VARCHAR(100) UNIQUE
);

INSERT INTO accounts VALUES (1, 'alice@example.com');
INSERT INTO accounts VALUES (2, 'alice@example.com');`,
        description: 'Attempt to insert duplicate UNIQUE value (will fail)',
        sqlFeatures: ['UNIQUE', 'Constraint Enforcement'],
      },
      {
        id: 'ddl-6',
        title: 'CHECK Constraint',
        database: 'empty',
        sql: `CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    price FLOAT CHECK (price > 0.0),
    quantity INTEGER CHECK (quantity >= 0)
);

INSERT INTO products VALUES (1, 'Laptop', 999.99, 10);
INSERT INTO products VALUES (2, 'Mouse', 25.50, 50);

SELECT * FROM products ORDER BY product_id;`,
        description: 'Create table with CHECK constraints to validate data ranges',
        sqlFeatures: ['CHECK', 'Constraint Enforcement', 'Expressions'],
      },
      {
        id: 'ddl-7',
        title: 'CHECK Constraint Violation',
        database: 'empty',
        sql: `CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    price FLOAT CHECK (price > 0.0)
);

INSERT INTO products VALUES (1, -10.0);`,
        description: 'Attempt to insert value that violates CHECK constraint (will fail)',
        sqlFeatures: ['CHECK', 'Constraint Enforcement'],
      },
      {
        id: 'ddl-8',
        title: 'Multiple Constraints',
        database: 'empty',
        sql: `CREATE TABLE employees (
    emp_id INTEGER PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    salary FLOAT CHECK (salary > 0),
    age INTEGER CHECK (age >= 18 AND age <= 65)
);

INSERT INTO employees VALUES (1, 'alice@company.com', 75000.0, 28);
INSERT INTO employees VALUES (2, 'bob@company.com', 82000.0, 35);

SELECT * FROM employees ORDER BY emp_id;`,
        description: 'Combine PRIMARY KEY, UNIQUE, and multiple CHECK constraints',
        sqlFeatures: ['PRIMARY KEY', 'UNIQUE', 'CHECK', 'Multiple Constraints'],
      },
      {
        id: 'ddl-9',
        title: 'UPDATE with Constraint Checks',
        database: 'empty',
        sql: `CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    balance FLOAT CHECK (balance >= 0)
);

INSERT INTO accounts VALUES (1, 1000.0);
INSERT INTO accounts VALUES (2, 500.0);

UPDATE accounts SET balance = balance + 100 WHERE account_id = 1;

SELECT * FROM accounts ORDER BY account_id;`,
        description: 'Constraints are enforced during UPDATE operations',
        sqlFeatures: ['UPDATE', 'CHECK', 'Constraint Enforcement'],
      },
      {
        id: 'ddl-10',
        title: 'UPDATE Constraint Violation',
        database: 'empty',
        sql: `CREATE TABLE accounts (
    account_id INTEGER PRIMARY KEY,
    balance FLOAT CHECK (balance >= 0)
);

INSERT INTO accounts VALUES (1, 1000.0);

UPDATE accounts SET balance = -500.0 WHERE account_id = 1;`,
        description: 'UPDATE that violates CHECK constraint (will fail)',
        sqlFeatures: ['UPDATE', 'CHECK', 'Constraint Enforcement'],
      },
      {
        id: 'ddl-11',
        title: 'Composite PRIMARY KEY',
        database: 'empty',
        sql: `CREATE TABLE enrollments (
    student_id INTEGER,
    course_id INTEGER,
    grade VARCHAR(2),
    PRIMARY KEY (student_id, course_id)
);

INSERT INTO enrollments VALUES (1, 101, 'A');
INSERT INTO enrollments VALUES (1, 102, 'B');
INSERT INTO enrollments VALUES (2, 101, 'A');

SELECT * FROM enrollments ORDER BY student_id, course_id;`,
        description: 'PRIMARY KEY spanning multiple columns for unique combinations',
        sqlFeatures: ['PRIMARY KEY', 'Composite Key', 'Multi-column Constraints'],
      },
      {
        id: 'ddl-12',
        title: 'Real-World: User Registration',
        database: 'empty',
        sql: `CREATE TABLE user_registrations (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(50) UNIQUE,
    email VARCHAR(100) UNIQUE,
    age INTEGER CHECK (age >= 13),
    account_balance FLOAT CHECK (account_balance >= 0)
);

INSERT INTO user_registrations VALUES
    (1, 'alice_j', 'alice@example.com', 25, 0.0);
INSERT INTO user_registrations VALUES
    (2, 'bob_smith', 'bob@example.com', 30, 100.50);

SELECT user_id, username, email, age
FROM user_registrations
ORDER BY user_id;`,
        description: 'Real-world user registration table with multiple constraint types',
        sqlFeatures: ['PRIMARY KEY', 'UNIQUE', 'CHECK', 'Business Logic'],
      },
    ],
  },
]

/**
 * Get all examples flattened from all categories
 */
export function getAllExamples(): QueryExample[] {
  return exampleCategories.flatMap(cat => cat.queries)
}

/**
 * Find an example by ID
 */
export function findExample(id: string): QueryExample | undefined {
  return getAllExamples().find(ex => ex.id === id)
}

/**
 * Get examples for a specific database
 */
export function getExamplesForDatabase(
  database: 'northwind' | 'employees' | 'company' | 'university' | 'empty'
): QueryExample[] {
  return getAllExamples().filter(ex => ex.database === database)
}
